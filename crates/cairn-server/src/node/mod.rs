// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Assembles a complete node: store, one engine per partition, routing, HTTP.
//!
//! All the assembly used to live in `main.rs`, so only the binary could build
//! a real node — tests had to reassemble the pieces themselves, and
//! reassembling meant assembling *differently*: one partition, no transport,
//! no routing table. Bugs that only show when the pieces talk to each other
//! (an inter-partition deadlock is the example) were out of reach of every
//! in-process test.
//!
//! This module is **the only assembly path**: the binary calls [`start`], and
//! so do tests. A multi-partition node with real HTTP transport between the
//! partitions can be built inside one `#[tokio::test]`.
//!
//! ## Lifecycle
//!
//! [`RunningNode`] owns **every** background task it spawns (heartbeat,
//! presence scan, routing-table refresh, release-when-over-share loop,
//! deployment registry refresh, HTTP server). [`RunningNode::shutdown`] stops
//! them all and then shuts the engines down; dropping without `shutdown`
//! still cancels the background tasks and the server, the engines just are
//! not sealed cleanly.
//!
//! One known exception: the sampling task of each follower reader
//! ([`crate::follower`]) is spawned by `FollowerReads` itself and ends on its
//! own when the reader is abandoned — the node holds no handle to them.

use {
    crate::{
        actor::{EngineHandle, Leadership, Readiness, TurnLimits},
        balance::Balancer,
        engine_pool::EnginePool,
        follower::FollowerReads,
        ingress::AppState,
        partitions::Partitions,
        registry::Registry,
        router,
        routing::RoutingTable,
        transport::Transport,
    },
    cairn_engine::{Engine, EngineError},
    cairn_store::{
        acquire_shared_with_hint, ClusterConfig, DeploymentStore, LeaseConfig, NodeWalConfig,
        NodeWals, ObjectStores, PartitionId, PartitionTable, PartitionWriter, RuleStore, Session,
        StoreError, WalKind,
    },
    std::{net::SocketAddr, sync::Arc, time::Duration},
    tokio::{sync::oneshot, task::JoinHandle},
};

mod config;
mod http;
pub use config::*;
mod running;
pub use running::*;
mod start;
pub use start::*;
mod setup;
pub use setup::check_advertise_host;
use setup::*;
mod refresh;
pub(crate) use refresh::refresh_registry;
pub use refresh::*;
mod spawner;
pub use spawner::*;

/// Startup error. The message is written for the operator and is often
/// several lines (the D12 gate, the CAS gate, a mismatched partition count) —
/// print it with `Display`.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Deployment shape.
///
/// **Cluster only**: a node without `--cluster` is a **one-node** cluster
/// ([`Self::single`]) — the same sessions, leases, owner records, shared WAL,
/// routing and background loops as a multi-node cluster. The single-process
/// mode without a lease layer (`SingleProcess`) was removed: its separate
/// branch was where a whole class of bugs came from (a retired range nobody
/// wrote a tombstone for).
///
/// Every range contends for its lease with the other nodes on the same
/// bucket — even when this node is the only one.
#[derive(Clone, Debug)]
pub struct NodeMode {
    pub partitions: u16,
    pub lease_ttl: Duration,
    /// Node identity in the cluster. `None` ⇒ generated per process.
    pub node_id: Option<String>,
}

/// Default lease TTL — also the default of `--lease-ttl-ms`. See
/// [`LeaseConfig::new`] for why it is 10 s.
pub const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(10);

impl NodeMode {
    /// The binary's node without `--cluster`: a one-node cluster, default TTL,
    /// node id generated per process.
    pub fn single(partitions: u16) -> Self {
        Self {
            partitions,
            lease_ttl: DEFAULT_LEASE_TTL,
            node_id: None,
        }
    }

    pub fn partitions(&self) -> u16 {
        self.partitions
    }
}

#[cfg(test)]
mod tests;
