// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The node's engine set — [`Partitions`].
//!
//! Separate from `ingress`: `split`, `gc`, `balance`, `slow` use this type, and
//! previously had to reach back into the HTTP layer to get it.

use {crate::actor::EngineHandle, std::sync::Arc};

/// The ranges this node runs engines for, by range id.
///
/// The node opens engines for **every** range of the table, not only those it currently
/// leads: an engine that cannot take its lease answers `NotLeader` and stands by, and it
/// must be ready there to take leadership when the node holding it dies.
///
/// **One shared set, changeable at run time**: the node adds engines for new ranges
/// appearing in the table and drops the engines of ranges that disappeared. Every `clone`
/// (ingress, balancer, `RunningNode`) sees the same set. Reads return copies of the
/// handles — holding a handle does not hold the lock.
///
/// The price is one thread per range — including ranges the node does not lead (leadership
/// balancing splits *write authority*, not *threads*). Folding engines onto fewer threads
/// is a later step.
#[derive(Clone, Default)]
pub struct Partitions {
    handles: Arc<std::sync::RwLock<std::collections::BTreeMap<u32, EngineHandle>>>,
}

impl Partitions {
    pub fn new(handles: std::collections::BTreeMap<u32, EngineHandle>) -> Self {
        Self {
            handles: Arc::new(std::sync::RwLock::new(handles)),
        }
    }

    pub fn single(handle: EngineHandle) -> Self {
        Self::new([(0u32, handle)].into_iter().collect())
    }

    pub fn get(&self, p: cairn_store::PartitionId) -> Option<EngineHandle> {
        self.handles.read().ok()?.get(&p.0).cloned()
    }

    /// Snapshot of the engine set at call time.
    pub fn snapshot(&self) -> std::collections::BTreeMap<u32, EngineHandle> {
        self.handles.read().map(|m| m.clone()).unwrap_or_default()
    }

    /// Iterate over a snapshot — ranges added/removed meanwhile do not affect it.
    pub fn iter(&self) -> impl Iterator<Item = (u32, EngineHandle)> {
        self.snapshot().into_iter()
    }

    pub fn contains(&self, p: cairn_store::PartitionId) -> bool {
        self.handles.read().is_ok_and(|m| m.contains_key(&p.0))
    }

    pub fn insert(&self, p: cairn_store::PartitionId, handle: EngineHandle) {
        if let Ok(mut m) = self.handles.write() {
            m.insert(p.0, handle);
        }
    }

    pub fn remove(&self, p: cairn_store::PartitionId) -> Option<EngineHandle> {
        self.handles.write().ok()?.remove(&p.0)
    }

    /// Tell every engine of the node "the deployment registry changed" — **in parallel, each
    /// engine bounded** by [`crate::actor::NOTIFY_WAIT`].
    ///
    /// This command is only a reminder: an engine that is serving answers at once and
    /// reconciles later; an engine not yet in its serving loop (recovering) runs the
    /// start-of-term reconciliation, reading the registry at that point. So giving up on a slow
    /// engine loses nothing — whereas waiting for it sequentially would stall the node's whole
    /// refresh round (key revocation, tenants, captures) behind one range.
    pub async fn deployments_changed(&self) {
        let notify = self.iter().map(|(_, h)| async move {
            let _ = tokio::time::timeout(crate::actor::NOTIFY_WAIT, h.deployments_changed()).await;
        });
        futures::future::join_all(notify).await;
    }
}
