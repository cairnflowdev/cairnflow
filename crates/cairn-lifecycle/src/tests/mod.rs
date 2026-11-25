// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The core's row-by-row tables.
//!
//! The decision tables of `turn/decide.rs` and
//! `drainer.rs::rescue_net_and_recover_table` moved here **row for row**: each
//! row keeps its view and action as in the original, a translator in each file
//! turns them into `View`/`Event` and reads the `Plan` back into the old
//! action. Each row names the FL-n it keeps in its first column.
//!
//! Rows where the core chose the fixed behaviour (suspicion B, the reverse of
//! B, D, policy, drainer) pin **two** expectations: the core (`Bugs::NONE` —
//! also the product since the shell moved onto the core, `Bugs::PRODUCTION`)
//! and the behaviour from before (`Bugs::BEFORE_STEP_3`, as a historical bug) —
//! the "changed with the core" column.

#![allow(
    clippy::type_complexity,
    reason = "row-by-row tables: one tuple per row, read across"
)]

mod admin;
mod codec;
mod drain;
mod restore;
mod retry;
mod turn;
mod valid;

use crate::{Attendance, Bugs, Event, Knobs, Plan, View};

/// The `now` of every table.
pub(super) const NOW: u64 = 1_000;

pub(super) const KNOBS: Knobs = Knobs {
    defer_ms: 5_000,
    watch_after_ms: 65_000,
    parked_defer_ms: 500_000,
};

pub(super) fn view(attendance: Attendance) -> View {
    View::new(NOW, KNOBS, attendance)
}

pub(super) fn idle() -> View {
    view(Attendance::IDLE)
}

pub(super) fn run(v: &View, e: Event, b: Bugs) -> Plan {
    crate::step_mutated(v, &e, &b)
}

/// A row unchanged by the core: the core and the behaviour before it give the
/// same result.
pub(super) fn both(v: &View, e: Event) -> Plan {
    let core = run(v, e, Bugs::NONE);
    let before = run(v, e, Bugs::BEFORE_STEP_3);
    assert_eq!(
        core, before,
        "unchanged row yet core ≠ before the core: {v:?} {e:?}"
    );
    // `step` without flags is `Bugs::NONE`.
    assert_eq!(crate::step(v, &e), core);
    core
}

/// The answers the fake shell has ready for each [`crate::Need`]; a `None`
/// cell = not read (when a table says "read X" the core must ask for exactly
/// that cell).
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Answers {
    pub status: Option<crate::Status>,
    pub slot: Option<Option<crate::Slot>>,
    pub retry: Option<Option<crate::RetryRow>>,
    pub journal_len: Option<u32>,
    pub contract: Option<crate::Contract>,
    /// Backoff per policy for **every** step; `Some(None)` = the handler declares none.
    pub policy: Option<Option<u64>>,
    pub waiting: Option<crate::Waiting>,
}

/// The fake shell: ask the core, answer every `Need` present in `a`, ask again;
/// return the first `Plan` that asks for nothing, or that asks for a cell `a`
/// lacks.
pub(super) fn ask(v: View, e: Event, a: Answers, b: Bugs) -> Plan {
    use crate::Need;
    let mut v = v;
    for _ in 0..8 {
        let p = run(&v, e, b);
        let Some(n) = p.need else {
            return p;
        };
        let filled = match n {
            Need::Status => a.status.map(|s| v.status = Some(s)),
            Need::Slot => a.slot.map(|s| v.slot = Some(s)),
            Need::Retry => a.retry.map(|r| v.retry = Some(r)),
            Need::JournalLen => a.journal_len.map(|n| v.journal_len = Some(n)),
            Need::Contract => a.contract.map(|c| v.contract = Some(c)),
            Need::Policy { step } => a.policy.map(|d| v = v.with_policy(step, d)),
            Need::Waiting => a.waiting.map(|w| v.waiting = Some(w)),
        };
        if filled.is_none() {
            return p;
        }
    }
    panic!("the core keeps asking for reads: {v:?} {e:?}");
}

/// [`ask`] when the core and the behaviour before it agree.
pub(super) fn ask_both(v: View, e: Event, a: Answers) -> Plan {
    let core = ask(v, e, a, Bugs::NONE);
    let before = ask(v, e, a, Bugs::BEFORE_STEP_3);
    assert_eq!(
        core, before,
        "unchanged row yet core ≠ before the core: {v:?} {e:?} {a:?}"
    );
    core
}

/// The product is the core itself: the shell calls `step`, with no flag.
#[test]
fn production_is_the_core() {
    assert_eq!(Bugs::PRODUCTION, Bugs::NONE);
    assert_ne!(Bugs::BEFORE_STEP_3, Bugs::NONE);
}
