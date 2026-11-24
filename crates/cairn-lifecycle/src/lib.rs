// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The invocation lifecycle core.
//!
//! Every lifecycle state change of the product goes through [`step`]: the
//! shell is `crates/cairn-server/src/turn/life.rs` (reads the `View`, carries
//! out the `Plan`), and source check 5 (`scripts/lint-sources.sh`) blocks
//! primitive calls outside the shell. The row-by-row test tables pin the target
//! behaviour and — in the "changed with the core" rows — the behaviour from
//! before the shell moved onto the core as a historical bug
//! (`Bugs::BEFORE_STEP_3`); M4 (`crates/cairn-model/src/m4.rs`) checks the
//! [`step`] function itself under interleavings, deaths and leader changes.
//!
//! One call is: **the shell reads the [`View`] → `step(view, event)` → the
//! shell carries out the [`Plan`] → the shell reports the next event**. A plan
//! is one commit (status, stage, retry row, wake-ups), RAM work after the
//! commit, [`Side`] effects and the next step; or "read X as well" ([`Need`]).
//!
//! - [`step`]: the transition table — `step.rs` lists the product functions it
//!   merges.
//! - [`valid`]: the set of valid durable states.
//! - [`codec`]: the one encoding of the retry row, byte-for-byte equal to the
//!   engine's.
//! - [`drain`]: the decisions of the drainer and of the permit-waiting turn.
//!
//! Hot path: the core runs on every turn — no allocation, no logging, no locks
//! (every type is `Copy`).

#![forbid(unsafe_code)]

mod bugs;
pub mod codec;
pub mod drain;
mod event;
mod plan;
mod step;
mod valid;
mod view;

#[cfg(any(test, feature = "mutations"))]
pub use bugs::Bugs;
#[cfg(any(test, feature = "mutations"))]
pub use step::step_mutated;
pub use {
    codec::{RetryRow, RowKind},
    event::{Event, FailedAt, Finish, Outcome, Planned, Turn, TurnErr, WakeKind},
    plan::{
        Duty, Mark, Need, Plan, Ram, Release, Reply, RowChange, Seal, Side, StageChange, Then, Vo,
        WakeWrite,
    },
    step::{backoff_ms, step, RETRY_BASE_MS, RETRY_CAP_MS, RETRY_SOON_MS},
    valid::{valid, Invalid},
    view::{
        Attendance, Contract, Knobs, OnMax, Pause, Permit, Pinned, Policy, ReadErr, Slot, Stage,
        Status, View, Waiting, PARKED_DEFER_MS,
    },
};

#[cfg(test)]
mod tests;
