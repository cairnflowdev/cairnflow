// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The conformance trace generator (`conformance/`).
//!
//! - [`sdk`] — the reference SDK, emitting exactly the bytes a real SDK must emit.
//! - [`sim`] — the real state machine + a deterministic driver + the transport rules.
//! - [`scenarios`] — scenarios, taken from `mandatory.rs` and `phase4.rs`.
//! - [`trace`] / [`json`] — JSON output per `conformance/trace.schema.json`.

pub mod json;
pub mod scenarios;
pub mod sdk;
pub mod sim;
pub mod trace;
