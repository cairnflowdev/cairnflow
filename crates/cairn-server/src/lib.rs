// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! HTTP ingress, discovery and the invoker.
//!
//! The architecture here is **dictated by `!Send`**, not by taste:
//! [`cairn_store::PartitionWriter`] is deliberately not `Send` (INV-4), so the
//! engine cannot enter axum handlers running on a thread pool. It lives on its
//! own thread and talks over channels ([`actor`]). That is exactly what we want
//! — the writer lives in exactly one task, and the compiler guarantees it
//! instead of human discipline.
//!
//! | Module | Contents |
//! |---|---|
//! | [`discovery`] | Manifest, negotiation of the two version axes |
//! | [`invoker`] | Calling the SDK, `REQUEST_RESPONSE` mode |
//! | [`turn`] | The turn loop: load journal → call SDK → judge the result |
//! | [`flow`] | Counters, VO locks, the scheduler — concurrency caps per scope |
//! | [`actor`] | The `!Send` boundary |
//! | [`ingress`] | HTTP: register deployments, call services, inspect state |
//! | [`routing`] | Who holds which partition — read straight from the lease object |
//! | [`transport`] | Sending messages to another partition, over the HTTP server itself |
//! | [`node`] | Assembling a complete node — the assembly path shared by the binary and tests |

pub mod actor;
mod actor_types;
pub mod artifact_sweep;
pub mod audit;
pub mod auth;
pub mod autosplit;
pub mod balance;
pub mod boot;
pub mod capture;
pub(crate) mod clock_rate;
pub mod control;
pub mod ctl;
pub mod discovery;
pub mod drainer;
pub mod egress;
pub mod engine_pool;
pub mod flow;
pub mod follower;
pub mod gc;
pub mod host;
pub mod identity;
pub mod ingress;
pub mod invoker;
pub mod l0;
pub mod lease;
pub mod lineage;
pub mod metering;
pub mod metrics;
#[cfg(feature = "testkit")]
pub mod netfilter;
pub mod node;
pub mod otel;
pub mod owner_notice;
pub mod partitions;
pub mod pricing;
pub mod quarantine;
pub mod query;
pub mod range_stats;
pub mod rate;
pub mod registry;
pub mod registry_gen;
pub mod registry_sync;
pub mod replay;
pub mod replay_budget;
pub mod routing;
pub mod schedules;
pub mod scrub;
pub mod slow;
pub mod slowlog;
pub mod split;
pub mod stats;
pub mod stats_history;
pub(crate) mod store_check;
#[cfg(feature = "testkit")]
pub mod store_profile;
pub mod streams;
pub mod tenant_purge;
pub mod tenant_usage;
pub mod tenants;
pub mod transport;
pub mod turn;
pub mod ui;
pub mod value_output;
pub mod value_sweep;
pub mod value_urls;
pub mod windows;

/// The `--version` line of every binary: `<bin> <package version>
/// (data=<DATA_FORMAT>, knows=<KNOWS>)`. The package version does not change
/// between builds; what decides whether two binaries can share one cluster is
/// `knows=` (the number of migration steps the binary knows,
/// `cairn_store::migrate::KNOWS`) and `data=`.
pub fn version_line(bin: &str) -> String {
    format!(
        "{bin} {} (data={}, knows={})",
        env!("CARGO_PKG_VERSION"),
        cairn_store::DATA_FORMAT,
        cairn_store::migrate::KNOWS
    )
}

pub use {
    actor::{ActorError, EngineFactory, EngineHandle, InvokeOutcome},
    discovery::{Deployment, DiscoveryError, EndpointManifest, ProtocolMode},
    ingress::{router, AppState},
    invoker::INVOCATION_V1,
    registry::Registry,
    routing::{Leader, RoutingTable},
    transport::{Transport, TransportError},
};
