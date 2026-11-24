// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Cairnflow's journal and invocation state machine.
//!
//! Two modules, one boundary:
//!
//! - [`journal`] — append-only, two index systems (entry index and command index).
//! - [`machine`] — a **pure** state machine: `Event` in, `Effect` out. No
//!   storage, no network, no clock, no randomness.
//!
//! That boundary exists so that section 10 of the spec can run. Mandatory test
//! number 1 is *"kill at every command index, replay, identical journal and
//! output"*; it is only feasible if rebuilding the machine from a journal is
//! cheap and deterministic.
//!
//! The driver is what connects [`machine::Effect`] to `cairn-store` and HTTP.
//! Two contracts it must keep:
//!
//! 1. **`Persist` before `Send`.** Sending before durability promises the SDK
//!    something not yet certain — the same kind of mistake as acking before
//!    `await_durable` (INV-5).
//! 2. **`Task` must be idempotent.** When an invocation restarts, the machine
//!    re-issues tasks for every pending completion.

pub mod journal;
pub mod machine;
pub mod replay;

pub use {
    journal::{Appended, Journal, JournalEntry, JournalError},
    machine::{
        suspension_has_leaves, suspension_satisfied, suspension_satisfied_by, Completion,
        CompletionResult, Effect, Event, FailureAction, InvocationId, InvocationMachine,
        MachineError, Outcome, Phase, SettledSignals, StartParams, Task,
    },
};
