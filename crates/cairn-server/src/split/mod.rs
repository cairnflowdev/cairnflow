// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The **split/merge** coordinator (protocol D24, D-e).
//!
//! Runs on the node leading the parent range P = `[a, c]`, splitting at `b`:
//!
//! ```text
//!  0. precondition: no intent; P's L0 has room — counting views (F4)
//!  1. table CAS: P → Splitting{b, L, R}, L, R allocated from next_range_id  ← decision point
//!  2. flush the memtable while still serving
//!  3. Quiesce P — answers once the Db is closed, lease still held
//!  4. (F3 in SlateDB: clone refuses a source with WAL its manifest does not cover)
//!  5. clone L, R — a projection of two entity regions per child
//!  6. take owners/range-L, owners/range-R in the node's session
//!  7. table CAS: drop P, add L(gen+1), R(gen+1), version+1                 ← commit point
//!  8. open L, R (the node's engine; missing DERIVED_READY ⇒ rebuild the derived region)
//!  9. P retires: write the tombstone, the engine exits for good
//! ```
//!
//! **Roll-back** ([`roll_back`]): whoever takes a range and sees the intent of a
//! split/merge that never committed — its coordinator died — CASes the **intent
//! away first** (the decision point: that CAS and the commit CAS, exactly one
//! wins), and only then cleans up the children's Db and owner records. Cleaning
//! first and CASing afterwards would let a commit that wins in between lose the
//! children already cleaned. A cleanup that fails halfway only leaves garbage
//! (the Db/record of an id never reused), loses nothing.
//!
//! The coordinator rolls back by itself when one of its steps fails; it marks the
//! ranges it is working on in [`ActiveIntents`] so that the engine factory of
//! **this very node** does not think it dead and roll back from under it.
//!
//! **Operations belong to the node, not to the request** (SM-15):
//! [`RangeOps::start`] runs the operation in a task of the node and records it in
//! the operation book ([`OpBook`], `GET /ranges/ops`); the HTTP handler only
//! waits for the result, so a client disconnect cancels nothing. The intent on
//! the table is the source of truth. Every path that leaves an intent behind
//! while the source's leader is alive has someone cleaning it up: a failed
//! roll-back is retried in the background ([`RangeOps::settle_in_background`]),
//! a quiesced source is tied to the coordinator's lifetime (the quiesce hold
//! wire, SM-15), and the last net is the intent reconciliation round of the D-d
//! runner ([`IntentReconciler`]).

// The runtime's clock: outside simulation it is `std::time::Instant`, inside
// deterministic simulation it is the virtual clock.
pub use cairn_split::Who;
use cairn_split::{Phase, Remover, Reopen, Stage};
use tokio::time::Instant;
use {
    crate::{
        actor::{ActorError, EngineHandle},
        partitions::Partitions,
        range_stats::{self, RangeStats, RangeTimings},
        routing::RoutingTable,
    },
    cairn_store::{
        delete_owner, Claim, ClusterConfig, LeaseKeeper, ObjectStores, PartitionId, PartitionKey,
        PartitionTable, RangeEntry, RangeIntent, Session, StoreError, TableUpdate,
    },
    serde::Serialize,
    std::{
        collections::{BTreeMap, BTreeSet, VecDeque},
        sync::{Arc, Mutex},
        time::Duration,
    },
};

mod active;
pub use active::*;
mod error;
pub use error::*;
mod report;
pub use report::*;
mod book;
pub use book::*;
mod ops;
use ops::*;
mod merge;
mod rollback;
mod steps;
pub use rollback::*;
mod reconcile;
pub use reconcile::*;
pub mod core;
pub(crate) mod forward;
pub use forward::roll_forward;
mod ledger;
pub(crate) use ledger::mark_ledger;

/// Limit on waiting for the parent range's in-flight work when quiescing (step 3 above).
pub const QUIESCE_GRACE: Duration = Duration::from_secs(10);

/// Limit on one clone/union while the source is quiesced. The source does not
/// serve during that time and SlateDB retries I/O forever: past the limit we roll
/// back (the source reopens, the half-made children are cleaned up per SM-14) —
/// the caller retries later.
const CLONE_BOUND: Duration = Duration::from_secs(30);

/// Limit on taking the child ranges in the session: wait for a live incarnation,
/// read and `Create` the owner records. Same size as one ordinary range take plus a few retries.
const CLAIM_BOUND: Duration = Duration::from_secs(10);

/// Wait for `fut` at most `bound`; past the limit ⇒ [`RangeOpError::Busy`] — a
/// transient error, the coordinator rolls back as for any failed step.
async fn bounded<T>(
    op: &'static cairn_store::retry::Op,
    bound: Duration,
    what: &str,
    fut: impl std::future::Future<Output = OpResult<T>>,
) -> OpResult<T> {
    cairn_store::retry::deadline(op, bound, fut)
        .await
        .map_err(|_| RangeOpError::Busy(format!("{what} over {bound:?}: rolled back, retry")))?
}

/// Interval at which we ask the bucket again whether the intent is still ours
/// while waiting to gather a range (`gather_here`): one GET of `cluster.conf` per
/// tick, only while the range is in someone else's hands.
const INTENT_RECHECK: Duration = Duration::from_millis(500);

/// Coordinates split/merge on this node. `Clone` is cheap.
#[derive(Clone)]
pub struct RangeOps {
    pub(crate) cluster: ClusterConfig,
    pub(crate) stores: ObjectStores,
    pub(crate) session: Session,
    pub(crate) partitions: Partitions,
    pub(crate) routing: RoutingTable,
    pub(crate) active: ActiveIntents,
    pub(crate) client: reqwest::Client,
    /// Cluster secret: the request to hand a range over goes through the old owner's internal port, signed.
    pub(crate) secret: crate::auth::ClusterSecret,
    /// Limit on waiting for a child range to have its leader on this node (step 8
    /// above), and on waiting to gather the other range here for a merge (D-e).
    pub(crate) settle: Duration,
    /// Range lifecycle measurements of the node — for `report`.
    pub(crate) stats: RangeStats,
    /// The node's identity — prefix of operation ids, and to know which leader is
    /// us when reconciling intents.
    pub(crate) node_id: String,
    /// The node's operation book.
    pub(crate) book: OpBook,
    /// Lineage depth limit for auto-split
    /// (`LineageConfig::max_auto_split_depth`).
    pub(crate) lineage: crate::lineage::LineageConfig,
}

/// Compile-time fence: split/merge futures must be `Send` — axum handlers run
/// them on a thread pool. The grant of a `Claim` is `!Send`; holding it across an
/// `await` is an error here rather than in the router.
#[allow(dead_code)]
fn _futures_are_send(ops: RangeOps) {
    fn is_send<T: Send>(_: T) {}
    is_send(ops.merge(PartitionId(0), PartitionId(1)));
    is_send(ops.split(PartitionId(0), None));
    is_send(ops.split_exclusive(PartitionId(0), 1));
    is_send(ops.split_blocker(PartitionId(0), 1));
}

#[cfg(test)]
mod tests;
