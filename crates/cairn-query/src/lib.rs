// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **The cairnflow query layer**: DataFusion over the declared index framework
//! (`cairn_store::query_index`).
//!
//! - [`ctx`]: who asks, which ranges are read, how much is read, warnings.
//! - [`filter`]: the `Exact` filter shape — chooses the path and is re-applied at the leaf.
//! - [`invocation`]: `sys_invocation` — path chooser by descriptor + counters.
//! - [`counts`]: `sys_invocation_counts` — RAM, does not read the store.
//! - [`artifact_bytes`]: `sys_artifact_bytes` — RAM, does not read the store.
//! - [`keyed`]: tables by key (`sys_journal`, `sys_state`, …) — a key is required.
//! - [`remote`]: scanning another node's ranges — request, response, serving side.
//! - [`view`]: RAM/bucket tables mirroring the REST read endpoints (`sys_deployment`, …).
//! - [`session`]: locked-down session, caps, running a query.

pub mod answer;
pub mod artifact_bytes;
pub mod catalog;
pub mod counts;
pub mod ctx;
pub mod filter;
pub mod invocation;
pub mod journal_json;
pub mod keyed;
pub mod remote;
pub mod session;
pub mod sql_check;
pub mod view;
pub mod worker;

/// The DataFusion this crate pins (`=55.2.0`) — users read results with
/// exactly that Arrow version.
pub use datafusion;

pub use {
    ctx::{QueryCtx, ReadStats, TenantScope, Warning},
    session::{
        check_sql, check_sql_text, explain, run, run_with_params, tables, Limits, Param,
        QueryOutput, MAX_SQL_BLOCKS, MAX_SQL_BYTES, MAX_SQL_OPERATORS, MAX_SQL_RELATIONS,
    },
    sql_check::{MAX_SQL_CELLS, MAX_SQL_EXPRESSIONS, MAX_SQL_GROUPING_SETS, MAX_SQL_WIDTH},
};

/// The error of one query. [`Self::is_client`] separates the asker's errors
/// (`400`) from the system's (`500`).
#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    /// Wrong SQL, missing table/column, query refused by the lockdown.
    #[error("{0}")]
    Sql(String),
    /// Over a cap (reads, memory, time, row count).
    #[error("query_budget: {0}")]
    Budget(String),
    #[error("{0}")]
    Internal(String),
}

impl QueryError {
    pub fn is_client(&self) -> bool {
        matches!(self, Self::Sql(_) | Self::Budget(_))
    }
}

impl From<datafusion::error::DataFusionError> for QueryError {
    fn from(e: datafusion::error::DataFusionError) -> Self {
        use datafusion::error::DataFusionError as E;
        let msg = e.to_string();
        if msg.contains("query_budget") {
            return Self::Budget(msg);
        }
        match e.find_root() {
            E::ResourcesExhausted(_) => Self::Budget(msg),
            E::SQL(..) | E::Plan(_) | E::SchemaError(..) | E::NotImplemented(_) => Self::Sql(msg),
            E::Execution(_) if msg.contains("not supported") || msg.contains("DDL") => {
                Self::Sql(msg)
            }
            _ => Self::Internal(msg),
        }
    }
}

impl From<cairn_store::StoreError> for QueryError {
    fn from(e: cairn_store::StoreError) -> Self {
        Self::Internal(e.to_string())
    }
}
