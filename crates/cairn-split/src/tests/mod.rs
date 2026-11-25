// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The row-by-row tables of the split/merge core.
//!
//! Each row: a name (the bug or invariant it keeps), a view, an event, the
//! core's decision (`Bugs::NONE`), and — in rows where a historical-bug flag
//! changes the result — the decision with that flag on (the behaviour before
//! the fix). A row pins only the core and the flags it lists; where a flag
//! shows up as a counterexample is M2's business
//! (`crates/cairn-model/tests/m2.rs`).

#![allow(
    clippy::type_complexity,
    reason = "row-by-row tables: one tuple per row, read across"
)]

mod coordinator;
mod discard;
mod finder;
mod ledger_reap;
mod phase;
mod reap;

use crate::{Bugs, Event, Plan, View};

pub(super) type Flip = fn(&mut Bugs);

/// A row: name, view, event, the core's decision, then (flag, decision with
/// the flag on).
pub(super) struct Row {
    pub name: &'static str,
    pub view: View,
    pub event: Event,
    pub core: Plan,
    pub bugs: Vec<(Flip, Plan)>,
}

pub(super) fn row(name: &'static str, view: View, event: Event, core: Plan) -> Row {
    Row {
        name,
        view,
        event,
        core,
        bugs: Vec::new(),
    }
}

impl Row {
    pub fn with(mut self, flip: Flip, plan: Plan) -> Row {
        self.bugs.push((flip, plan));
        self
    }
}

pub(super) fn check(rows: Vec<Row>) {
    for r in rows {
        assert_eq!(
            crate::step(&r.view, &r.event),
            r.core,
            "{}: core ({:?} | {:?})",
            r.name,
            r.view,
            r.event
        );
        assert_eq!(
            crate::step_mutated(&r.view, &r.event, &Bugs::NONE),
            r.core,
            "{}: step ≠ step_mutated(NONE)",
            r.name
        );
        for (flip, want) in &r.bugs {
            let mut b = Bugs::NONE;
            flip(&mut b);
            assert_eq!(
                crate::step_mutated(&r.view, &r.event, &b),
                *want,
                "{}: flag {b:?}",
                r.name
            );
        }
    }
}

/// The product today is exactly the behaviour recorded in `Bugs::PRODUCTION` —
/// changing the shell means changing this constant in the same commit.
#[test]
fn production_is_the_core() {
    assert_eq!(Bugs::PRODUCTION, Bugs::NONE);
    const { assert!(Bugs::BEFORE_3_1.no_ledger && Bugs::BEFORE_3_1.no_roll_forward) };
    const { assert!(!Bugs::BEFORE_3_3.no_ledger && Bugs::BEFORE_3_3.ledger_not_proof) };
}
