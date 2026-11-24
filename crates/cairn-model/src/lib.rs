// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! cairnflow's protocol models, exhaustively checked with `stateright`.
//!
//! Each model is an abstract [`stateright::Model`] that does **not** depend on
//! product code: actions are the atomic steps the real code takes, named after
//! the real functions; the existing fences are configuration flags that can be
//! switched off/on. The acceptance criterion of each model: re-inject a
//! historical bug (switch off the fence that fixed it) ⇒ the checker finds a
//! counterexample; all fences on ⇒ exhaustive search, every invariant holds.
//!
//! - [`m1`]: lease, session, range table, opening a Db.
//! - [`m2`]: split/merge, GC, fork — every path that deletes a Db or an owner
//!   record around an operation.
//! - [`m3`]: the per-node shared WAL — sealing, taking over ranges, floor GC,
//!   scanning and finally removing dead logs (NW-7, C2, C3′, C4, C7).
//! - [`m4`]: the invocation lifecycle — unlike M1–M3, the transition function
//!   is `cairn_lifecycle::step` **itself**.

pub mod m1;
pub mod m2;
pub mod m3;
pub mod m4;
