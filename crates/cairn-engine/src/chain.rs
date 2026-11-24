// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Tenants with the tamper-evident hash chain enabled.
//!
//! Off by default — every journal write of an enabled tenant costs one extra
//! 36-byte row (the chain head, [`cairn_store::Table::Chain`]). The list lives
//! in the bucket; the node re-reads it periodically and replaces this set, and
//! the engine asks it when **creating** an invocation: an invocation created
//! while its tenant is enabled carries the chain for its whole life (the chain
//! head is in the store), one created while disabled does not — toggling midway
//! never produces a journal that is half chained and half not.

use cairn_store::entropy::HashSet;
use {
    cairn_store::TenantId,
    std::sync::{Arc, RwLock},
};

/// The set of tenants with the hash chain enabled, shared between the node (the
/// refresher) and every engine (the readers).
#[derive(Clone, Default)]
pub struct ChainedTenants(Arc<RwLock<HashSet<TenantId>>>);

impl ChainedTenants {
    pub fn contains(&self, t: &TenantId) -> bool {
        self.0.read().map(|s| s.contains(t)).unwrap_or(false)
    }

    /// Replaces the whole set (the refresh loop reads it from the bucket).
    pub fn replace(&self, tenants: HashSet<TenantId>) {
        if let Ok(mut s) = self.0.write() {
            *s = tenants;
        }
    }

    /// Snapshot of the current set.
    pub fn snapshot(&self) -> Vec<TenantId> {
        self.0
            .read()
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Enables/disables one tenant right away on this node (after a successful write).
    pub fn set(&self, t: &TenantId, on: bool) {
        if let Ok(mut s) = self.0.write() {
            if on {
                s.insert(t.clone());
            } else {
                s.remove(t);
            }
        }
    }
}
