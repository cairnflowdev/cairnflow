// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Acceptance tests of M4: the core without flags ⇒ every configuration
//! exhausted, every property holds, every `sometimes` has an example;
//! re-injecting each historical bug into the core **itself**
//! (`cairn_lifecycle::Bugs`) ⇒ the checker finds a counterexample of exactly the
//! property it breaks (`matrix`).
//!
//! The counterexamples printed (`--nocapture`) are the **shortest** (single-threaded
//! BFS, as in `tests/m2.rs`). Heavier configurations (bigger budgets) are `--ignored` tests.

// Measure the exploration time with the real clock (only to print the report).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

use {
    cairn_lifecycle::Bugs,
    cairn_model::m4::{self, Action, Config, Start, WakeBy, M4},
    stateright::{Checker, HasDiscoveries, Model},
};

fn report(name: &str, c: &impl Checker<M4>, started: std::time::Instant) {
    println!(
        "{name}: {} unique states, {} generated, depth {}, done = {}, {:.1?}",
        c.unique_state_count(),
        c.state_count(),
        c.max_depth(),
        c.is_done(),
        started.elapsed()
    );
}

/// BFS over the **whole** state space.
fn check_all(name: &str, bugs: Bugs, cfg: Config) -> impl Checker<M4> {
    let started = std::time::Instant::now();
    let c = M4::new(bugs, cfg)
        .checker()
        .threads(std::thread::available_parallelism().map_or(1, |n| n.get()))
        .finish_when(HasDiscoveries::AllFailures)
        .spawn_bfs()
        .join();
    report(name, &c, started);
    assert!(c.is_done(), "{name}: the BFS must be exhaustive");
    c
}

/// The shortest counterexample of `prop` (single-threaded BFS until found).
fn counterexample(name: &str, prop: &'static str, bugs: Bugs, cfg: Config) -> Vec<Action> {
    let started = std::time::Instant::now();
    let c = M4::new(bugs, cfg)
        .checker()
        .finish_when(HasDiscoveries::AnyOf([prop].into()))
        .spawn_bfs()
        .join();
    report(name, &c, started);
    let path = c
        .discovery(prop)
        .unwrap_or_else(|| panic!("{name}: no counterexample found for {prop:?}"));
    let actions = path.into_actions();
    println!(
        "{name}: counterexample of {prop:?} ({} steps):",
        actions.len()
    );
    for (i, a) in actions.iter().enumerate() {
        println!("  {:>2}. {a:?}", i + 1);
    }
    actions
}

// ───────────────────────────────────────── configurations

/// Two invocations on one object (EXCLUSIVE): one client turn, one `/send`
/// turn; the SDK fails retryably, suspends waiting for a timer; throttling and
/// one leader change; `maxAttempts` 2 (reachable).
const EXCLUSIVE: Config = Config {
    exclusive: true,
    start: [Start::Client, Start::Send],
    fails: [1, 1],
    errors: [0, 0],
    unfinished: [0, 0],
    sleeps: [Some(WakeBy::Timer), None],
    max_attempts: Some(2),
    throttles: 1,
    leader_changes: 1,
    cancels: [false, false],
    resumes: 0,
    stuck: 0,
    undeclared: false,
    durable: false,
    refusals: 0,
    late: [false, false],
    signals: [false, false],
    gate_errors: 0,
};

/// Like [`EXCLUSIVE`] on an unkeyed queue: `Running` back to `Inbox` on a leader
/// change.
const SHARED: Config = Config {
    exclusive: false,
    ..EXCLUSIVE
};

/// Two wakes for one suspension (a timer + a drainer completion) and an
/// infrastructure error after `Running`.
const WAKES: Config = Config {
    exclusive: true,
    start: [Start::Send, Start::Client],
    fails: [0, 1],
    errors: [1, 0],
    unfinished: [0, 0],
    sleeps: [Some(WakeBy::Both), Some(WakeBy::Drain)],
    max_attempts: None,
    throttles: 1,
    leader_changes: 1,
    cancels: [false, false],
    resumes: 0,
    stuck: 0,
    undeclared: false,
    durable: false,
    refusals: 0,
    late: [false, false],
    signals: [false, false],
    gate_errors: 0,
};

/// Admin: cancel, `paused` 575 then `resume`, the rescue net's `pause_stuck`.
const ADMIN: Config = Config {
    exclusive: true,
    start: [Start::Client, Start::Send],
    fails: [2, 0],
    errors: [0, 1],
    unfinished: [0, 0],
    sleeps: [None, Some(WakeBy::Timer)],
    max_attempts: Some(1),
    throttles: 0,
    leader_changes: 1,
    cancels: [false, true],
    resumes: 1,
    stuck: 1,
    undeclared: false,
    durable: false,
    refusals: 0,
    late: [false, false],
    signals: [false, false],
    gate_errors: 0,
};

/// Suspending on **two** drainer completions: `ctx.all` in invocation 0,
/// `ctx.race` in invocation 1; one leader change loses the RAM wait tree between
/// the two completions (or before the first).
const TWO: Config = Config {
    exclusive: true,
    start: [Start::Send, Start::Client],
    fails: [0, 0],
    errors: [0, 0],
    unfinished: [0, 0],
    sleeps: [
        Some(WakeBy::Two { all: true }),
        Some(WakeBy::Two { all: false }),
    ],
    max_attempts: None,
    throttles: 0,
    leader_changes: 1,
    cancels: [false, false],
    resumes: 0,
    stuck: 0,
    undeclared: false,
    durable: false,
    refusals: 0,
    late: [false, false],
    signals: [false, false],
    gate_errors: 0,
};

/// The durability model: one outgoing call whose result comes back through the
/// drainer (receiving, `200`, the drainer on a non-durable message), one leader
/// change dropping the non-durable tail.
const MSG: Config = Config {
    exclusive: true,
    start: [Start::Client, Start::Client],
    fails: [0, 0],
    errors: [0, 0],
    unfinished: [0, 0],
    sleeps: [Some(WakeBy::Drain), None],
    max_attempts: None,
    throttles: 0,
    leader_changes: 1,
    cancels: [false, false],
    resumes: 0,
    stuck: 0,
    undeclared: false,
    durable: true,
    refusals: 0,
    late: [false, false],
    signals: [false, false],
    gate_errors: 0,
};

/// The tenant is suspended and lifted once, in any gap: a turn waiting for a
/// permit, holding a permit before preflight, past preflight but not yet calling
/// the SDK (waiting for durability), or between two attempts. The SDK fails once,
/// suspends waiting for a timer; throttling and one leader change as in [`EXCLUSIVE`].
const REFUSED: Config = Config {
    refusals: 1,
    ..EXCLUSIVE
};

/// Rebuilding the VO locks in chunks while serving commands: invocation 0
/// suspends waiting for a timer holding the lock and can be cancelled at any
/// time; invocation 1 is created (`/send`) at any time — including between two
/// chunks after a leader change. Two leader changes: the lease lost during the
/// rebuild, a second rebuild.
const RESTORE: Config = Config {
    exclusive: true,
    start: [Start::Client, Start::Send],
    fails: [0, 0],
    errors: [0, 0],
    unfinished: [0, 0],
    sleeps: [Some(WakeBy::Timer), None],
    max_attempts: None,
    throttles: 0,
    leader_changes: 2,
    cancels: [true, false],
    resumes: 0,
    stuck: 0,
    undeclared: false,
    durable: false,
    refusals: 0,
    late: [false, true],
    signals: [false, false],
    gate_errors: 0,
};

/// The permit queue is data: invocation 0 (client) suspends waiting for a timer
/// holding the VO lock; invocation 1 (`/send`) parks a ticket behind that lock or
/// behind the tenant suspension, and can be cancelled at any time — including
/// while the ticket is parked (its invocation lock is free); one leader change
/// (the ticket is lost with the RAM).
const PARK: Config = Config {
    exclusive: true,
    start: [Start::Client, Start::Send],
    fails: [0, 0],
    errors: [0, 0],
    unfinished: [0, 0],
    sleeps: [Some(WakeBy::Timer), None],
    max_attempts: None,
    throttles: 0,
    leader_changes: 1,
    cancels: [false, true],
    resumes: 0,
    stuck: 0,
    undeclared: false,
    durable: false,
    refusals: 1,
    late: [false, false],
    signals: [false, false],
    gate_errors: 0,
};

/// A parked ticket carrying two wakes: invocation 1 (`/send`) suspends waiting
/// for a timer **and** a drainer completion; the timer turn parks a ticket while
/// the tenant is suspended, the completion arrives while the ticket is parked;
/// the SDK fails retryably and with an infrastructure error.
const PARK_WAKES: Config = Config {
    exclusive: false,
    start: [Start::Client, Start::Send],
    fails: [0, 1],
    errors: [0, 1],
    unfinished: [0, 0],
    sleeps: [None, Some(WakeBy::Both)],
    max_attempts: None,
    throttles: 0,
    leader_changes: 0,
    cancels: [false, false],
    resumes: 0,
    stuck: 0,
    undeclared: false,
    durable: false,
    refusals: 1,
    late: [false, false],
    signals: [false, false],
    gate_errors: 0,
};

/// A parked ticket with a retry: invocation 1's first turn (`/send`) fails
/// retryably; the retry's fire hands over a turn and the turn parks a ticket
/// (the tenant is suspended), the next retry comes due while the ticket is parked
/// (the retry tick moves the row far out as a watch row), then a leader change
/// loses the ticket.
const PARK_RETRY: Config = Config {
    exclusive: false,
    start: [Start::Client, Start::Send],
    fails: [0, 1],
    errors: [0, 0],
    unfinished: [0, 0],
    sleeps: [None, None],
    max_attempts: Some(3),
    throttles: 0,
    leader_changes: 1,
    cancels: [false, false],
    resumes: 0,
    stuck: 0,
    undeclared: false,
    durable: false,
    refusals: 1,
    late: [false, false],
    signals: [false, false],
    gate_errors: 0,
};

/// An external signal to an invocation with a parked ticket: invocation 1
/// (`/send`) parks a ticket behind the tenant suspension or behind invocation 0's
/// VO lock (client); lifting the suspension grants the ticket a permit; a signal
/// reaches invocation 1 **at any time** — including in the window "ticket
/// granted, turn has not taken the invocation lock" ([`m4::Pc::Handed`]). An
/// EXCLUSIVE queue: two invocations compete for one VO lock.
const SIGNAL: Config = Config {
    exclusive: true,
    start: [Start::Client, Start::Send],
    fails: [0, 0],
    errors: [0, 0],
    unfinished: [0, 0],
    sleeps: [None, None],
    max_attempts: None,
    throttles: 0,
    leader_changes: 0,
    cancels: [false, false],
    resumes: 0,
    stuck: 0,
    undeclared: false,
    durable: false,
    refusals: 1,
    late: [false, false],
    signals: [false, true],
    gate_errors: 0,
};

/// A ticket turn with a pre-planned retry failing at the gate after the permit:
/// like [`PARK_RETRY`] — invocation 1's first turn fails retryably, the fire
/// hands over a turn, the turn parks a ticket behind the suspension, the next
/// retry comes due while the ticket is parked (the row moved far out as a watch
/// row) — but no leader change (which would hide the bug), and one failed status
/// read at the gate after the permit.
const PARK_FAIL: Config = Config {
    leader_changes: 0,
    gate_errors: 1,
    ..PARK_RETRY
};

/// Configuration `c` with the durability model on.
const fn durable(c: Config) -> Config {
    Config { durable: true, ..c }
}

// ───────────────────────────────────────── (a) the core without flags

/// Newer properties the product shell still breaks, and the fix for each: the
/// shared tests skip them until the fix removes the matching line. Empty now:
/// `ONE_PERMIT` was fixed — matrix row `granted_out_of_ledger`; `WAKE_BOUND` —
/// row `planned_keeps_watch_row`.
const RED_TODAY: &[(&str, &str)] = &[];

fn red_today(prop: &str) -> bool {
    RED_TODAY.iter().any(|(p, _)| *p == prop)
}

/// (a) The core as the target code: every configuration exhausted, every
/// property holds, every `sometimes` has an example in a configuration that reaches it.
#[test]
fn core_holds_every_property() {
    // Last column: the model explores the gap "a ticket is parked while another
    // task holds its invocation lock" — configurations where a background turn
    // parks a ticket (someone else's VO lock, the grant gate closed after a
    // leader change, a suspended tenant) and another task holds that lock (a
    // timer turn handed over while the gate holds the lock, a cancel).
    //
    // Column `adopted`: a new request adopts a **granted** ticket whose turn has
    // not taken the lock yet (the window of the permit outside the ledger) —
    // every configuration with a parked ticket and another turn of the same invocation.
    for (name, cfg, done, exhausted, early, parked_locked, adopted) in [
        ("EXCLUSIVE", EXCLUSIVE, true, true, false, true, true),
        ("SHARED", SHARED, true, true, false, true, true),
        ("WAKES", WAKES, true, false, false, true, true),
        ("ADMIN", ADMIN, false, true, false, true, true),
        ("TWO", TWO, true, false, false, false, false),
        // The durability model: small configurations here, all of them in `--ignored`.
        ("MSG", MSG, true, false, true, false, false),
        (
            "EXCLUSIVE durable",
            durable(EXCLUSIVE),
            true,
            true,
            false,
            true,
            true,
        ),
        ("ADMIN dur", durable(ADMIN), false, true, false, true, true),
        // Suspend/lift in every gap of a turn.
        ("REFUSED", REFUSED, true, true, false, true, true),
        (
            "REFUSED durable",
            durable(REFUSED),
            true,
            true,
            false,
            true,
            true,
        ),
        // Chunked restore: create/cancel commands between two chunks, the lease lost half-way.
        ("RESTORE", RESTORE, true, false, false, true, true),
        (
            "RESTORE durable",
            durable(RESTORE),
            true,
            false,
            false,
            true,
            true,
        ),
        // Parked tickets: parked behind the VO lock / the suspension, cancelled while
        // parked, a completion arriving while parked.
        ("PARK", PARK, true, false, false, true, true),
        ("PARK dur", durable(PARK), true, false, false, true, true),
        ("PARK_WAKES", PARK_WAKES, false, false, false, true, true),
        // No other task holds invocation 1's lock while its ticket is parked: no
        // cancel, no timer (a timer turn handed over while busy), no client turn
        // of its own; a fire while the ticket exists is busy and hands nothing
        // over (FL-20). Not because of moving the row far out: the old behaviour
        // (`parked_defer_short`) is `false` too, and adding a cancel of invocation
        // 1 makes it `true` (measured when written). This gap is covered in PARK,
        // PARK_WAKES and every configuration of (a) with a leader change.
        ("PARK_RETRY", PARK_RETRY, true, false, false, false, false),
    ] {
        let c = check_all(name, Bugs::NONE, cfg);
        let refusals = cfg.refusals > 0;
        for p in M4::new(Bugs::NONE, cfg).properties() {
            let found = c.discovery(p.name).is_some();
            match p.name {
                m4::SOMETIMES_DONE => assert_eq!(found, done, "{name}: {}", p.name),
                m4::SOMETIMES_EXHAUSTED => assert_eq!(found, exhausted, "{name}: {}", p.name),
                m4::SOMETIMES_EARLY_DRAIN => assert_eq!(found, early, "{name}: {}", p.name),
                // The model explores the refused-tenant gap (a turn that already
                // had a permit stopped before the SDK) — instead of stepping around it.
                m4::SOMETIMES_REFUSED_AFTER_PERMIT => {
                    assert_eq!(found, refusals, "{name}: {}", p.name)
                }
                // Every configuration with a leader change and a cancel or a late
                // create explores the gap "a command while the locks are rebuilt".
                m4::SOMETIMES_SERVED_RESTORING => assert_eq!(
                    found,
                    cfg.leader_changes > 0
                        && (cfg.cancels.iter().any(|c| *c) || cfg.late.iter().any(|l| *l)),
                    "{name}: {}",
                    p.name
                ),
                m4::SOMETIMES_PARKED_LOCKED => {
                    assert_eq!(found, parked_locked, "{name}: {}", p.name)
                }
                m4::SOMETIMES_ADOPTED_HANDED => assert_eq!(found, adopted, "{name}: {}", p.name),
                n if red_today(n) => {}
                _ => assert!(
                    !found,
                    "{name}: {} breaks: {:?}",
                    p.name,
                    c.discovery(p.name).map(|d| d.into_actions())
                ),
            }
        }
    }
}

/// (a') The product is the core itself (`Bugs::PRODUCTION == Bugs::NONE`, the
/// shell `turn/life.rs` calls `step`): every property of (a) holds for it.
#[test]
fn production_is_the_core() {
    assert_eq!(Bugs::PRODUCTION, Bugs::NONE);
}

/// (a'') The product **before** the lifecycle core (`Bugs::BEFORE_STEP_3`, every
/// row the core changed set back to the code just before it) breaks exactly the
/// properties the core fixed — no other:
/// - `ATTEMPTS`, `NEW_ATTEMPT`: counting before `Running`, its reverse, a retry
///   keeping its old position;
/// - `VALID`: an `Inbox` entry of a keyed queue carrying a counted attempt
///   without entering `Running`, an entry of a cancelled invocation (cancel in
///   two commits);
/// - `LIVE`: `paused` 575 while the SDK has not been called `maxAttempts` times;
/// - `NO_DUPLICATE`: the drainer hands over a duplicate turn (the
///   timer-ignores-queued-turn shape).
///
/// The shell of that time: a background turn waits for its permit in the task
/// and is throttled (`BEFORE_W6Q`) — the counting bugs break through the
/// throttle path.
#[test]
fn before_step_3_broke_only_what_step_3_fixed() {
    const BEFORE: Bugs = Bugs {
        permit_wait_in_task: true,
        ..Bugs::BEFORE_STEP_3
    };
    use m4::{ATTEMPTS, LIVE, NEW_ATTEMPT, NO_DUPLICATE, VALID};
    for (name, cfg, want) in [
        (
            "EXCLUSIVE",
            EXCLUSIVE,
            &[ATTEMPTS, NEW_ATTEMPT, VALID, LIVE][..],
        ),
        (
            "WAKES",
            WAKES,
            &[ATTEMPTS, NEW_ATTEMPT, VALID, NO_DUPLICATE][..],
        ),
        ("ADMIN", ADMIN, &[ATTEMPTS, NEW_ATTEMPT, VALID][..]),
    ] {
        let c = check_all(name, BEFORE, cfg);
        let mut broken: Vec<&str> = M4::new(BEFORE, cfg)
            .properties()
            .into_iter()
            .filter(|p| !p.name.starts_with("sometimes"))
            .filter(|p| c.discovery(p.name).is_some())
            .map(|p| p.name)
            .collect();
        broken.sort();
        let mut want = want.to_vec();
        want.sort();
        assert_eq!(broken, want, "{name}: before the core breaks");
    }
}

/// (a‴) The product **before** wait trees (`Bugs::BEFORE_3G_C`: the drainer wakes
/// on every completion) is right but slow: every property holds in the
/// two-completion configuration. Wait trees change the number of turns, not correctness.
#[test]
fn before_3g_c_holds_every_property() {
    let c = check_all("TWO before wait trees", Bugs::BEFORE_3G_C, TWO);
    for p in M4::new(Bugs::BEFORE_3G_C, TWO).properties() {
        if !p.name.starts_with("sometimes") {
            assert!(
                c.discovery(p.name).is_none(),
                "before wait trees: {} breaks",
                p.name
            );
        }
    }
}

/// (a⁗) The product **before** chunked restore (`Bugs::BEFORE_W5R`: rebuild every
/// VO lock in one hold, before the first command — the old FL-8) is right but
/// slow: every property holds. Chunking changes when the first command is
/// served, not correctness.
#[test]
fn before_w5r_holds_every_property() {
    for (name, cfg) in [
        ("RESTORE before chunking", RESTORE),
        ("EXCLUSIVE before chunking", EXCLUSIVE),
        ("ADMIN before chunking", ADMIN),
    ] {
        let c = check_all(name, Bugs::BEFORE_W5R, cfg);
        for p in M4::new(Bugs::BEFORE_W5R, cfg).properties() {
            if p.name == m4::SOMETIMES_SERVED_RESTORING {
                assert!(
                    c.discovery(p.name).is_none(),
                    "{name}: before chunking no command is served while the locks are rebuilt"
                );
            } else if !p.name.starts_with("sometimes") && !red_today(p.name) {
                assert!(c.discovery(p.name).is_none(), "{name}: {} breaks", p.name);
            }
        }
    }
}

/// (a⁵) The product **before** parked tickets (`Bugs::BEFORE_W6Q`: a background
/// turn waits for its permit in the task, holding the invocation lock, and when
/// blocked by the budget its slice runs out, it gives up and retries later) is
/// right but slow: every property holds — and it keeps the coverage of the
/// throttle path (`Event::Throttled` at the permit request) under interleaving, a
/// path the new shell only keeps for `resume`/external signals. No ticket parks:
/// the "ticket parked, lock free" gap does not exist.
#[test]
fn before_w6q_holds_every_property() {
    for (name, cfg) in [
        ("EXCLUSIVE before tickets", EXCLUSIVE),
        ("WAKES before tickets", WAKES),
        ("ADMIN before tickets", ADMIN),
        ("REFUSED before tickets", REFUSED),
        ("PARK before tickets", PARK),
        ("PARK_WAKES before tickets", PARK_WAKES),
    ] {
        let c = check_all(name, Bugs::BEFORE_W6Q, cfg);
        for p in M4::new(Bugs::BEFORE_W6Q, cfg).properties() {
            if p.name == m4::SOMETIMES_PARKED_LOCKED {
                assert!(
                    c.discovery(p.name).is_none(),
                    "{name}: before tickets no ticket parks"
                );
            } else if !p.name.starts_with("sometimes") {
                assert!(
                    c.discovery(p.name).is_none(),
                    "{name}: {} breaks: {:?}",
                    p.name,
                    c.discovery(p.name).map(|d| d.into_actions())
                );
            }
        }
    }
}

/// Every `always` property of `m` holds on `cfg` (exhaustive).
fn holds_every_safety_property(name: &str, bugs: Bugs, cfg: Config) -> impl Checker<M4> {
    let c = check_all(name, bugs, cfg);
    let broken: Vec<String> = M4::new(bugs, cfg)
        .properties()
        .into_iter()
        .filter(|p| !p.name.starts_with("sometimes"))
        .filter_map(|p| {
            c.discovery(p.name)
                .map(|d| format!("{}: {:?}", p.name, d.into_actions()))
        })
        .collect();
    assert!(broken.is_empty(), "{name} breaks:\n{}", broken.join("\n"));
    c
}

/// (a⁶) **A permit outside the ledger**: an external signal to an invocation
/// whose ticket has been granted while the ticket's turn has not taken the
/// invocation lock. The signal's request adopts the ticket (`Scheduler::adopt`)
/// and takes its very permit. Before the fix (flag `granted_out_of_ledger`) the
/// ticket's permit sat outside the ledger, the signal was granted a **second**
/// permit (`ONE_PERMIT`), the ticket's task returned its permit per
/// `held_before = false` and released the VO lock the second permit was using ⇒
/// invocation 0 entered the SDK at the same time (`VO_EXCLUSIVE`). Must see:
/// every property holds, and the model explores that gap.
#[test]
fn signal_to_a_granted_ticket_holds_every_property() {
    let c = holds_every_safety_property("SIGNAL", Bugs::NONE, SIGNAL);
    assert!(
        c.discovery(m4::SOMETIMES_ADOPTED_HANDED).is_some(),
        "SIGNAL: the gap of an adopted granted ticket is not explored"
    );
}

/// (a⁷) **A watch row moved far out**: the turn of a ticket with a pre-planned
/// retry, its row moved far out (a watch row +24 h), fails at the gate after the
/// permit before `Running`. Before the fix (flag `planned_keeps_watch_row`) the
/// wake duty was acked from the start (a `planned` turn) and nothing rewrote the
/// row ⇒ the invocation is `running`, no task, no rescue mark, its only retry a
/// day away (`WAKE_BOUND`). The model has no time so the watch row still fires:
/// `LIVE` does not see it, and a leader change (`recover`) hides it.
/// Must see: every property holds.
#[test]
fn failed_ticket_turn_keeps_a_near_wake() {
    holds_every_safety_property("PARK_FAIL", Bugs::NONE, PARK_FAIL);
}

/// Flag `granted_out_of_ledger` (the earlier shell) breaks `ONE_PERMIT` even in a
/// configuration without a signal (a timer turn adopts a granted ticket).
#[test]
fn granted_out_of_ledger_breaks_one_permit_without_signals() {
    let bugs = Bugs {
        granted_out_of_ledger: true,
        ..Bugs::NONE
    };
    counterexample("WAKES, stray permit", m4::ONE_PERMIT, bugs, WAKES);
}

// ───────────────────────────────────────── (b) the injected-bug matrix

type Knock = fn(&mut Bugs);

/// Each row: name, injected flag, a configuration that reaches it, the property that must break.
fn matrix() -> Vec<(&'static str, Knock, Config, &'static str)> {
    // One suspension with a VO lock, the wake turn throttled right after its grant.
    let f018 = Config {
        start: [Start::Send, Start::Client],
        fails: [0, 0],
        sleeps: [Some(WakeBy::Timer), None],
        max_attempts: None,
        throttles: 1,
        leader_changes: 0,
        ..EXCLUSIVE
    };
    // The first turn throttled; the retry throttled or cancelled while waiting for a permit.
    let throttled = Config {
        start: [Start::Send, Start::Client],
        fails: [1, 0],
        sleeps: [None, None],
        max_attempts: Some(2),
        throttles: 2,
        leader_changes: 0,
        ..EXCLUSIVE
    };
    // Throttling and a cancel while waiting for a permit together: the first fix of the attempt-refund bug only covered the former.
    let cancelled = Config {
        throttles: 1,
        leader_changes: 1,
        ..throttled
    };
    // No deployment declares the service: each turn stops at preflight `NotFound`.
    let undeclared = Config {
        fails: [0, 0],
        sleeps: [None, None],
        max_attempts: None,
        throttles: 0,
        undeclared: true,
        ..EXCLUSIVE
    };
    let crash = Config {
        start: [Start::Client, Start::Client],
        fails: [1, 0],
        sleeps: [None, None],
        max_attempts: None,
        throttles: 0,
        leader_changes: 1,
        ..EXCLUSIVE
    };
    // One outgoing call whose result comes back through the drainer, one leader
    // change, durability.
    let msg = MSG;
    vec![
        // (fixed: `Running` with the lock goes to `Suspended`): a turn that has read
        // state dies half-way, the new leader sends `Running` back to `Inbox`
        // without keeping the lock ⇒ the next one runs in between.
        (
            "running back to inbox",
            |b| b.f017_running_to_inbox = true,
            crash,
            m4::VO_EXCLUSIVE,
        ),
        // (fixed: release only the permit): dropping a freshly granted permit also
        // released the VO lock held from before.
        //
        // The four throttle-path rows at the permit request (this one, counting at
        // fire time, FL-21, counting before `Running`) run on the earlier shell
        // (`permit_wait_in_task`): background turns are no longer throttled there
        // (the ticket parks in the queue), so the core's bug only shows through the
        // old shell — the `Event::Throttled` path remains for `resume`/external signals.
        (
            "held lock dropped",
            |b| {
                b.f018_drop_held_lock = true;
                b.permit_wait_in_task = true;
            },
            f018,
            m4::VO_EXCLUSIVE,
        ),
        // (before the first refund fix): the fire counts the attempt, throttling does not refund it.
        (
            "counted at fire",
            |b| {
                b.f023_count_at_fire = true;
                b.permit_wait_in_task = true;
            },
            throttled,
            m4::ATTEMPTS,
        ),
        // (the first refund fix): only `Throttled` turns are refunded; a turn cancelled while
        // waiting for a permit (leader change) is not.
        (
            "refund only throttled",
            |b| b.f023_refund_only_throttled = true,
            cancelled,
            m4::ATTEMPTS,
        ),
        // FL-20: the retry tick only asks the lock.
        (
            "FL-20",
            |b| b.fl20_lock_only = true,
            throttled,
            m4::NO_DUPLICATE,
        ),
        // FL-21 regression: the fire does not count `fires`.
        (
            "FL-21",
            |b| {
                b.fl21_no_fires = true;
                b.permit_wait_in_task = true;
            },
            throttled,
            m4::BACKOFF,
        ),
        // Suspicion A: a timer hands over a turn while a turn is already queued.
        (
            "timer ignores queued turn",
            |b| b.a_timer_ignores_queued = true,
            WAKES,
            m4::NO_DUPLICATE,
        ),
        // the same shape at the drainer — before the core.
        (
            "drainer ignores queued turn",
            |b| b.drain_ignores_queued = true,
            WAKES,
            m4::NO_DUPLICATE,
        ),
        // counting before `Running` — before the core.
        (
            "counted before Running",
            |b| {
                b.b_count_before_running = true;
                b.permit_wait_in_task = true;
            },
            throttled,
            m4::ATTEMPTS,
        ),
        // the reverse of the previous row — before the core.
        (
            "uncounted watch",
            |b| b.uncounted_watch = true,
            crash,
            m4::ATTEMPTS,
        ),
        // a pre-planned retry keeping its old position — before the core (M4 found it).
        (
            "stale position",
            |b| b.planned_stale_position = true,
            WAKES,
            m4::ATTEMPTS,
        ),
        // cancel in two commits — before the core.
        (
            "cancel in two commits",
            |b| b.d1_cancel_two_commits = true,
            ADMIN,
            m4::VALID,
        ),
        // Wait trees (injected): the drainer has no wait tree (leader change) yet
        // keeps `suspended` ⇒ the last completion of `ctx.all` is swallowed, the caller is stuck.
        (
            "wait tree: unknown = unsatisfied",
            |b| b.waiting_unknown_unsatisfied = true,
            TWO,
            m4::LIVE,
        ),
        // Wait trees (injected): evaluating the tree on the ledger **before** the
        // completion that just arrived ⇒ the completion that satisfies the tree never wakes.
        (
            "wait tree: missing the new completion",
            |b| b.waiting_without_new = true,
            TWO,
            m4::LIVE,
        ),
        // Preflight `NotFound` (fixed after a bug report): 404 without writing a
        // status ⇒ `running` without a queue entry.
        (
            "preflight NotFound",
            |b| b.notfound_keeps_running = true,
            undeclared,
            m4::VALID,
        ),
        // (injected, shell): `200` before the receive commit is durable ⇒ a leader
        // change drops the tail, the message is lost on both sides, the caller hangs forever.
        (
            "ack before durable (injected)",
            |b| b.ack_before_durable = true,
            msg,
            m4::LIVE,
        ),
        // (injected, shell): calling the SDK before `Running` is durable ⇒ a leader
        // change drops a counted attempt while the SDK was called.
        (
            "effect before durable (injected)",
            |b| b.effect_before_durable = true,
            durable(crash),
            m4::ATTEMPTS,
        ),
        // (shell, found by fuzzing): the SDK ends a turn without `End` and the shell
        // reports `Failed` ⇒ removes the queue entry of an invocation still `running`.
        (
            "unfinished turn reported terminal",
            |b| b.unfinished_turn_terminal = true,
            Config {
                unfinished: [1, 0],
                fails: [0, 0],
                sleeps: [None, None],
                max_attempts: None,
                throttles: 0,
                leader_changes: 0,
                ..EXCLUSIVE
            },
            m4::VALID,
        ),
        // (injected, shell): the sender resends while the first receive commit is
        // not durable; dedup asks only the durable store ⇒ two rows for one result.
        (
            "dedup only on the durable store (injected)",
            |b| b.accept_checks_durable_only = true,
            msg,
            m4::ONE_DELIVERY,
        ),
        // (found in a soak run): the tenant state was asked only at grant time ⇒ a
        // turn holding a permit from before the suspension calls the SDK after the node applied it.
        (
            "permit granted before the suspension still calls the SDK",
            |b| b.refused_calls_sdk = true,
            Config {
                start: [Start::Client, Start::Client],
                fails: [0, 0],
                sleeps: [None, None],
                max_attempts: None,
                throttles: 0,
                leader_changes: 0,
                ..REFUSED
            },
            m4::NO_CALL_REFUSED,
        ),
        // Chunked restore (injected, shell): the scheduler grants a permit before
        // the lock rebuild has finished ⇒ another invocation's turn enters the SDK
        // while the lock holder (in a chunk not yet scanned) is still suspended half-way.
        (
            "chunked restore: grant before the locks are rebuilt (injected)",
            |b| b.grant_before_restored = true,
            Config {
                cancels: [false, false],
                leader_changes: 1,
                late: [false, false],
                ..RESTORE
            },
            m4::VO_EXCLUSIVE,
        ),
        // Chunked restore (injected, shell): a chunk classifies, then releases the
        // engine before building the RAM lock ⇒ a cancel slips in, the lock of an
        // invocation already `failed` gets built, the next one waits forever.
        (
            "chunked restore: lock built after releasing the engine (injected)",
            |b| b.restore_lock_after_release = true,
            Config {
                leader_changes: 1,
                late: [false, false],
                ..RESTORE
            },
            m4::LIVE,
        ),
        // Chunked drain (injected, shell): the drainer asks "is anyone on the target"
        // in the hold before the release point between two chunks, then delivers in
        // the hold after ⇒ a turn handed over in that gap (a timer) is not seen, the wake hands over a second turn.
        (
            "chunked drain: drainer asks before the release point, delivers after (injected)",
            |b| b.drain_attendance_across_release = true,
            WAKES,
            m4::NO_DUPLICATE,
        ),
        // Parked tickets (M4 found it while building the shell, then injected): a
        // new request does not adopt the parked ticket ⇒ a timer turn handed over
        // while the pre-planned turn still holds the lock (the gate) runs before the
        // ticket, an infrastructure error counts one attempt; the ticket counts again
        // from 0 at the same journal position.
        (
            "parked tickets: a new request does not adopt the ticket",
            |b| b.adopt_keeps_ticket = true,
            WAKES,
            m4::ATTEMPTS,
        ),
        // Parked tickets (injected): the ticket is granted while a cancel holds the
        // invocation lock ⇒ two tasks hold the lock.
        (
            "parked tickets: grant while the invocation lock is busy",
            |b| b.parked_grant_ignores_lock = true,
            PARK,
            m4::ONE_TURN,
        ),
        // Parked tickets (injected): a grant that lands while the drainer holds the
        // target's lock (nack) drops the ticket too — the drainer saw "a turn is
        // handed over" so it does not hand one over ⇒ a `running` invocation with nothing left to wake it.
        (
            "parked tickets: a nack loses the ticket",
            |b| b.nack_drops_ticket = true,
            PARK_WAKES,
            m4::LIVE,
        ),
        // Parked tickets (injected): the ticket stays in the queue after the grant ⇒ two handed-over turns.
        (
            "parked tickets: ticket granted twice",
            |b| b.parked_granted_twice = true,
            PARK,
            m4::NO_DUPLICATE,
        ),
        // Parked tickets (injected): the ticket's turn does not re-read the status
        // after the permit ⇒ the ticket of an invocation cancelled while parked
        // still runs, writing stage `Running` for a `failed` invocation (also breaks
        // `ATTEMPTS`, `LIVE`).
        // Parked tickets (injected): the retry tick moving a parked ticket's row far
        // out clears `fires` ⇒ the ticket is lost on a leader change, the next fire
        // falls back to a lower step than the previous one.
        (
            "parked tickets: moving the ticket's row clears fires",
            |b| b.parked_defer_resets_fires = true,
            PARK_RETRY,
            m4::BACKOFF,
        ),
        (
            "parked tickets: the ticket skips the gate after the permit",
            |b| b.parked_grant_skips_gate = true,
            PARK,
            m4::VALID,
        ),
        // (the earlier product): a granted ticket's permit outside the ledger ⇒ a
        // signal adopts the ticket and asks for another permit; the ticket's task
        // returns its permit and releases the VO lock in use ⇒ another invocation
        // enters the SDK at the same time.
        (
            "a granted ticket's permit outside the ledger",
            |b| b.granted_out_of_ledger = true,
            SIGNAL,
            m4::VO_EXCLUSIVE,
        ),
        // (the earlier core): a ticket's `planned` turn failing at the gate after the
        // permit keeps a watch row the background tick moved far out because of the
        // parked ticket ⇒ nobody wakes the invocation for a day.
        (
            "a failed ticket turn keeps a watch row moved far out",
            |b| b.planned_keeps_watch_row = true,
            PARK_FAIL,
            m4::WAKE_BOUND,
        ),
    ]
}

/// (b) Each historical bug injected into the core turns exactly its property red,
/// with the shortest counterexample.
#[test]
fn every_historical_bug_turns_m4_red() {
    for (name, knock, cfg, prop) in matrix() {
        let mut bugs = Bugs::NONE;
        knock(&mut bugs);
        counterexample(name, prop, bugs, cfg);
    }
}

/// The attempt-refund bug: the first fix (refunding the attempt for
/// `Throttled` turns) is enough with throttling only — the second root cause only
/// shows when a turn is **cancelled** while waiting for a permit (a leader change).
/// The first root cause is red with throttling alone.
#[test]
fn f023_first_fix_only_covers_throttling() {
    let only_throttle = Config {
        start: [Start::Send, Start::Client],
        fails: [1, 0],
        sleeps: [None, None],
        max_attempts: Some(2),
        throttles: 2,
        leader_changes: 0,
        ..EXCLUSIVE
    };
    // The earlier shell: the throttle path at the permit request (see the refund rows of `matrix`).
    let root4 = Bugs {
        f023_refund_only_throttled: true,
        ..Bugs::BEFORE_W6Q
    };
    let c = check_all("root 4, throttling", root4, only_throttle);
    assert!(
        c.discovery(m4::ATTEMPTS).is_none(),
        "the first fix is enough for throttling"
    );
    let root3 = Bugs {
        f023_count_at_fire: true,
        ..Bugs::BEFORE_W6Q
    };
    let c = check_all("root 3, throttling", root3, only_throttle);
    assert!(
        c.discovery(m4::ATTEMPTS).is_some(),
        "counting at fire is red with throttling alone"
    );
}

/// The configuration of each matrix row, core without flags: green — a red row is
/// due to the flag, not to the configuration.
#[test]
fn matrix_configs_hold_without_bugs() {
    for (name, _, cfg, _) in matrix() {
        let c = check_all(name, Bugs::NONE, cfg);
        for p in M4::new(Bugs::NONE, cfg).properties() {
            if !p.name.starts_with("sometimes") && !red_today(p.name) {
                assert!(c.discovery(p.name).is_none(), "{name}: {} breaks", p.name);
            }
        }
    }
}

// ───────────────────────────────────────── heavy

/// The durability model on **every** configuration of (a) (`--ignored`, release:
/// around ten million states per configuration). The same results as (a), plus
/// `SOMETIMES_EARLY_DRAIN` in configurations with a drainer.
///
/// **The chunked-restore shell multiplies the state space** after each leader
/// change (any action between two chunks): `EXCLUSIVE durable` 0.76 → 1.57
/// million; `SHARED durable` (43 million with the old shell, 48 s) exceeds 5 GB
/// of RAM with the new one. The three biggest configurations run with the old
/// shell (`Bugs::BEFORE_W5R`: rebuild every lock before the first command —
/// keeping the coverage of turn/durability interleavings as before chunking);
/// the chunked rebuild's interleavings are covered by `RESTORE durable` below, in
/// every configuration of (a), in the matrix and in
/// [`restore_holds_with_bigger_budgets`].
#[test]
#[ignore = "heavy — run by hand: cargo test --release -p cairn-model --test m4 -- --ignored"]
fn core_holds_with_durability_on_every_config() {
    let old = Bugs::BEFORE_W5R;
    let now = Bugs::NONE;
    for (name, bugs, cfg, done, exhausted, early) in [
        ("EXCLUSIVE dur", now, durable(EXCLUSIVE), true, true, false),
        (
            "SHARED durable (pre-chunking shell)",
            old,
            durable(SHARED),
            true,
            true,
            false,
        ),
        (
            "WAKES durable (pre-chunking shell)",
            old,
            durable(WAKES),
            true,
            false,
            true,
        ),
        ("ADMIN durable", now, durable(ADMIN), false, true, false),
        ("TWO durable", now, durable(TWO), true, false, true),
        // Suspend/lift plus two wakes, infrastructure errors.
        (
            "WAKES durable + suspension (pre-chunking shell)",
            old,
            Config {
                refusals: 1,
                ..durable(WAKES)
            },
            true,
            false,
            true,
        ),
    ] {
        let c = check_all(name, bugs, cfg);
        for p in M4::new(bugs, cfg).properties() {
            let found = c.discovery(p.name).is_some();
            match p.name {
                m4::SOMETIMES_DONE => assert_eq!(found, done, "{name}: {}", p.name),
                m4::SOMETIMES_EXHAUSTED => assert_eq!(found, exhausted, "{name}: {}", p.name),
                m4::SOMETIMES_EARLY_DRAIN => assert_eq!(found, early, "{name}: {}", p.name),
                m4::SOMETIMES_REFUSED_AFTER_PERMIT => {
                    assert_eq!(found, cfg.refusals > 0, "{name}: {}", p.name)
                }
                m4::SOMETIMES_SERVED_RESTORING => assert_eq!(
                    found,
                    bugs == now
                        && cfg.leader_changes > 0
                        && (cfg.cancels.iter().any(|c| *c) || cfg.late.iter().any(|l| *l)),
                    "{name}: {}",
                    p.name
                ),
                // The parked-ticket gap — covered in (a); only printed here.
                m4::SOMETIMES_PARKED_LOCKED | m4::SOMETIMES_ADOPTED_HANDED => {
                    println!("{name}: {} = {found}", p.name)
                }
                n if red_today(n) => {}
                _ => assert!(
                    !found,
                    "{name}: {} breaks: {:?}",
                    p.name,
                    c.discovery(p.name).map(|d| d.into_actions())
                ),
            }
        }
    }
}

/// Bigger budgets (`--ignored`): throttle 2, leader changes 2, infrastructure
/// errors, two wakes, cancel and `resume`.
#[test]
#[ignore = "heavy — run by hand: cargo test -p cairn-model --test m4 -- --ignored"]
fn core_holds_with_bigger_budgets() {
    let big = Config {
        exclusive: true,
        start: [Start::Send, Start::Client],
        fails: [2, 1],
        errors: [1, 0],
        unfinished: [0, 0],
        sleeps: [Some(WakeBy::Both), Some(WakeBy::Timer)],
        max_attempts: Some(3),
        throttles: 2,
        leader_changes: 2,
        cancels: [false, true],
        resumes: 1,
        stuck: 1,
        undeclared: false,
        durable: false,
        refusals: 0,
        late: [false, false],
        signals: [false, false],
        gate_errors: 0,
    };
    // An unkeyed queue blows up the state space much faster (two turns running
    // in parallel): the `big` budget exceeds 16 GB of RAM — one throttle, one
    // leader change, no infrastructure error.
    let big_shared = Config {
        exclusive: false,
        throttles: 1,
        leader_changes: 1,
        errors: [0, 0],
        unfinished: [0, 0],
        ..big
    };
    // The chunked-restore shell multiplies the space after each leader change
    // (see `core_holds_with_durability_on_every_config`): the two existing
    // configurations run with the old shell; the chunked rebuild with big budgets
    // is in [`restore_holds_with_bigger_budgets`].
    for (name, bugs, cfg) in [
        ("BIG (old restore)", Bugs::BEFORE_W5R, big),
        ("BIG shared (old restore)", Bugs::BEFORE_W5R, big_shared),
    ] {
        let c = check_all(name, bugs, cfg);
        for p in M4::new(bugs, cfg).properties() {
            if !p.name.starts_with("sometimes") && !red_today(p.name) {
                assert!(c.discovery(p.name).is_none(), "{name}: {} breaks", p.name);
            }
        }
    }
}

/// The chunked VO-lock rebuild with bigger budgets than `RESTORE` (`--ignored`,
/// release): retryable failures, infrastructure errors, throttling, the rescue
/// net's `pause_stuck`; the durable variant with one leader change and no rescue
/// net (two leader changes exceed 6 GB of RAM). Every property holds.
#[test]
#[ignore = "heavy — run by hand: cargo test --release -p cairn-model --test m4 -- --ignored"]
fn restore_holds_with_bigger_budgets() {
    let big = Config {
        fails: [1, 0],
        errors: [0, 1],
        throttles: 1,
        stuck: 1,
        ..RESTORE
    };
    // Durability multiplies further: the durable variant keeps only throttling and one retryable failure.
    let big_durable = durable(Config {
        fails: [1, 0],
        errors: [0, 1],
        throttles: 1,
        leader_changes: 1,
        ..RESTORE
    });
    for (name, cfg) in [("RESTORE thr", big), ("RESTORE dur thr", big_durable)] {
        let c = check_all(name, Bugs::NONE, cfg);
        for p in M4::new(Bugs::NONE, cfg).properties() {
            if !p.name.starts_with("sometimes") && !red_today(p.name) {
                assert!(
                    c.discovery(p.name).is_none(),
                    "{name}: {} breaks: {:?}",
                    p.name,
                    c.discovery(p.name).map(|d| d.into_actions())
                );
            }
        }
        assert!(
            c.discovery(m4::SOMETIMES_SERVED_RESTORING).is_some(),
            "{name}: the gap of a command while the locks are rebuilt is not explored"
        );
    }
}
