// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The pure decision core of range split/merge.
//!
//! A split/merge is a transaction over several objects (intent, close mark,
//! clone, owner record, commit, tombstone) in which, before this core, everyone
//! entitled to roll back, roll forward or delete wrote their own rules — six
//! rollers-back, four ways of cleaning up children, three ways of removing the
//! close mark. Each historical bug of this group (D1, D2, D5, R1-i, SM-11, L1,
//! L3, L4, the failed hold, R6.3-1, R6.3-6, mixed snapshots in D-d, the orphan
//! Db after two kills) was an actor deciding on reasoning that another actor
//! did not keep. Here every one of those decisions is **one** function:
//! [`step`].
//!
//! One call is: **the shell reads the [`View`] → `step(view, event)` → the
//! shell carries out the [`Plan`]** (or reads the [`Need`] cell as well and
//! asks again), like `cairn-lifecycle`. M2 (`crates/cairn-model/src/m2.rs`)
//! uses this very function as its transition function; the row-by-row tables
//! in `tests/` pin each decision, and each historical bug is a [`Bugs`] flag
//! (feature `mutations`) that M2 must go red on when it is planted back.
//!
//! The durable op ledger (`ops/<key>`) is the [`Phase`]: a phase is written
//! **before** its side effects, the transition rules are in
//! [`Phase::advance`]. `Cleaned` is the positive proof for the child cleanup
//! paths; `Prepared` lets the reconciliation round **roll forward**.
//!
//! - [`op`]: [`Op`], [`Ranges`], [`Phase`].
//! - [`view`]: the durable facts the shell reads.
//! - [`event`]: each actor's question.
//! - [`plan`]: the answer.

#![forbid(unsafe_code)]

mod bugs;
pub mod event;
pub mod op;
pub mod plan;
mod step;
pub mod view;

#[cfg(any(test, feature = "mutations"))]
pub use bugs::Bugs;
#[cfg(any(test, feature = "mutations"))]
pub use step::step_mutated;
pub use {
    event::{Cas, Event, Finder, Remover, Stage, Who},
    op::{Advance, Op, Phase, Range, Ranges},
    plan::{Keep, Need, Plan, Reopen},
    step::step,
    view::{
        Child, Entry, Gate, Orphan, Owner, Pins, Read, ReadErr, Refs, Remains, Retired, Standing,
        View,
    },
};

#[cfg(test)]
mod tests;
