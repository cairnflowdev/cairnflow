// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The node's query layer: the registry of read handles of the ranges this node
//! currently leads ([`QueryRanges`]), and the coordination of SQL statements
//! through `cairn-query`.

pub mod http;
pub mod isolate;
pub(crate) mod list;
mod ranges;
pub mod views;

pub use {
    http::QueryService,
    isolate::{QueryWorkerConfig, DEFAULT_WORKER_MEMORY_MB},
    ranges::{QueryRangeGuard, QueryRanges},
};
