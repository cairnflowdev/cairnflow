// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Context of one query: who asks (tenant), which ranges are read, how much has
//! been read, and a side channel for warnings (`partial`) that DataFusion lacks
//! (it has no warning channel at run time).

use {
    cairn_store::range_read::RangeReadHandle,
    std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

/// Default read-unit cap per query: one index row or one point read is one
/// unit.
pub const DEFAULT_READ_BUDGET: u64 = 200_000;

/// Tenant scope of the asker — the **structure** of every scan, not a filter:
/// index keys start with the tenant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TenantScope {
    /// `admin` of one tenant: sees only that tenant.
    One(String),
    /// `cluster-admin`: every tenant.
    All,
}

/// A warning of the query: a missing range (`partial`) or an index being built.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Warning {
    pub range: Option<u32>,
    pub reason: String,
}

/// Read counters of one query — tests assert on the cost ("newest page: 0
/// point reads"), the `cairnflow_query_*` metrics read from here.
#[derive(Debug, Default)]
pub struct ReadStats {
    /// Number of scans opened (seeks).
    pub seeks: AtomicU64,
    /// Number of index / table rows read through scans.
    pub rows: AtomicU64,
    /// Number of point reads (metadata, status).
    pub point_reads: AtomicU64,
    /// Reads where the scan path touched the hot region instead of the index (fallback).
    pub fallback_rows: AtomicU64,
}

impl ReadStats {
    pub fn units(&self) -> u64 {
        self.rows.load(Ordering::Relaxed) + self.point_reads.load(Ordering::Relaxed)
    }

    pub fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.seeks.load(Ordering::Relaxed),
            self.rows.load(Ordering::Relaxed),
            self.point_reads.load(Ordering::Relaxed),
        )
    }
}

/// Query context — goes with every table of one session.
#[derive(Clone)]
pub struct QueryCtx {
    pub tenant: TenantScope,
    /// Read handles of the ranges this query scans (local to the coordinating node).
    pub ranges: Vec<RangeReadHandle>,
    /// Other nodes holding the query's remaining ranges. Empty on a remote node.
    pub remotes: Vec<Arc<dyn crate::remote::RemoteScan>>,
    /// Source of the view tables. `None` ⇒ view tables are not registered.
    pub views: Option<Arc<dyn crate::view::ViewSource>>,
    pub stats: Arc<ReadStats>,
    pub warnings: Arc<Mutex<Vec<Warning>>>,
    /// The query's read-unit cap.
    pub read_budget: u64,
    /// Counters of the coordinating node's local ranges for the path chooser,
    /// when the query runs in the child process: `Some(None)` = some range has
    /// not finished building its counters. `None` = read `ranges` as usual.
    pub class_counts: Option<Option<Vec<crate::invocation::ClassCount>>>,
}

impl QueryCtx {
    pub fn new(tenant: TenantScope, ranges: Vec<RangeReadHandle>) -> Self {
        Self {
            tenant,
            ranges,
            remotes: Vec::new(),
            views: None,
            stats: Arc::default(),
            warnings: Arc::default(),
            read_budget: DEFAULT_READ_BUDGET,
            class_counts: None,
        }
    }

    /// Add the remote nodes that hold the rest of the query.
    pub fn with_remotes(mut self, remotes: Vec<Arc<dyn crate::remote::RemoteScan>>) -> Self {
        self.remotes = remotes;
        self
    }

    /// Add the source of the view tables.
    pub fn with_views(mut self, source: Arc<dyn crate::view::ViewSource>) -> Self {
        self.views = Some(source);
        self
    }

    pub fn warn(&self, range: Option<u32>, reason: impl Into<String>) {
        let w = Warning {
            range,
            reason: reason.into(),
        };
        let mut ws = self.warnings.lock().unwrap_or_else(|p| p.into_inner());
        if !ws.contains(&w) {
            ws.push(w);
        }
    }

    /// Range `range` could not be read because of `e`: the query answers the
    /// rest with a `partial` warning. The text of `e` (a store error: bucket,
    /// host, object path; the node asked) goes only to the log — the warning
    /// goes to the tenant.
    pub fn warn_partial(&self, range: Option<u32>, e: &dyn std::fmt::Display) {
        tracing::warn!(range, detail = %e, "query range read failed; answering without it");
        self.warn(range, "partial: the range could not be read — retry later");
    }

    pub fn warnings(&self) -> Vec<Warning> {
        self.warnings
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Whether the tenant may see rows of `tenant`.
    pub fn sees(&self, tenant: &str) -> bool {
        match &self.tenant {
            TenantScope::One(t) => t == tenant,
            TenantScope::All => true,
        }
    }

    /// Record `n` read units; over the cap ⇒ error `query_budget`.
    pub fn charge(&self, rows: u64, points: u64) -> Result<(), crate::QueryError> {
        self.stats.rows.fetch_add(rows, Ordering::Relaxed);
        self.stats.point_reads.fetch_add(points, Ordering::Relaxed);
        if self.stats.units() > self.read_budget {
            return Err(crate::QueryError::Budget(format!(
                "the query read more than {} units (index rows + point reads)",
                self.read_budget
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A typical store error text: bucket name, host, object path.
    const LEAK: &str = "Generic S3 error: GET https://customer-bucket.s3.tigris.dev/cell-1/ranges/3/manifest/00000000000000000042.manifest";

    /// The `partial` warning of `/query` that reaches the tenant carries no
    /// store detail (bucket, host, object path); the detail goes only to the
    /// log.
    #[test]
    fn partial_warning_does_not_leak_store_details() {
        let ctx = QueryCtx::new(TenantScope::All, Vec::new());
        ctx.warn_partial(Some(3), &crate::QueryError::Internal(LEAK.into()));
        let ws = ctx.warnings();
        assert_eq!(ws.len(), 1);
        assert_eq!(ws[0].range, Some(3));
        assert!(ws[0].reason.starts_with("partial: "), "{ws:?}");
        for part in ["customer-bucket", "tigris", "ranges/3", "manifest"] {
            assert!(
                !ws[0].reason.contains(part),
                "warning leaks {part:?}: {ws:?}"
            );
        }
    }
}
