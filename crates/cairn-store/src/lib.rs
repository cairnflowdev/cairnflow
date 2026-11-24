// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Cairnflow's storage layer.
//!
//! The first phase ran **one** SlateDB instance with the object store pointing at the local
//! filesystem: no partitions, no leader, no follower. But the shape of the types here was the
//! shape of the distributed phase, on purpose: *"cheap when done early, very expensive to
//! retrofit"*.
//!
//! Three things were enforced from the very first commit, each derived from a result measured
//! during the initial validation experiments:
//!
//! | Invariant | Enforced by | Why we know it is needed |
//! |---|---|---|
//! | **INV-3** — all pending work lives in the state tables, not in RAM | [`Mutations`] has no arbitrary `put`; only named tables ([`keys::Table`]) can be written, through two key builders ([`keys::entity`], [`keys::index`]) | Redo replication only carries KV mutations. Pending work held in RAM disappears on failover, and no simple test catches it |
//! | **INV-4** — at most one valid writer per partition | [`PartitionWriter`] is `!Clone`/`!Send`, `open` requires a [`LeadershipGrant`], `seal` consumes it | The store is a single copy on object storage; one mistake corrupts it, there is no replica to absorb it |
//! | **INV-5** — ack only once durable | [`PartitionWriter::commit`] returns [`Committed`] only after `await_durable` | Measured: a writer that has already been fenced still gets a successful `put`. Acking after `put` acks a zombie leader's write |
//!
//! The hardest part the original design planned to write by hand — overlaying the manifest for
//! follower reads — **was removed**: experiments showed that SlateDB's `DbReader` does it itself,
//! and cuts the overlay at `D'ᵢ` rather than at `D`.

pub mod archive;
pub mod artifact;
pub mod artifact_bytes;
pub mod audit;
pub mod backup;
pub mod cas;
pub mod clock;
mod cluster;
pub mod cluster_history;
mod config;
pub mod dbstats;
mod deployments;
pub mod disk_cache;
pub mod entropy;
mod error;
pub mod failpoint;
pub mod failstop;
mod format;
pub mod gc_barrier;
pub mod gc_swept;
pub mod health;
mod ids;
pub mod inspect;
pub mod io_source;
mod keyspace;
pub mod ledger;
pub mod local;
pub mod metered;
pub mod migrate;
pub mod nodewal;
pub mod ops_report;
mod owner;
pub mod pace;
mod partition_table;
pub mod probe;
pub mod proof;
pub mod query_index;
pub mod quiet_lease;
pub mod range_read;
mod reader;
pub mod retry;
mod rules;
mod scope;
mod session;
pub mod store_clock;
pub mod store_probe;
pub mod tenant_purge;
#[cfg(any(test, feature = "testkit"))]
pub mod testkit;
pub mod trash;
pub mod units;
pub mod value;
mod writer;

pub use {
    cluster::{
        callee_invocation_id, callee_partition_key, decode_table, encode_table,
        idempotent_invocation_id, make_invocation_id, partition_key_of, schedule_id,
        schedule_manual_run_id, schedule_run_id, workflow_invocation_id, ClusterConfig,
        TableUpdate, MAX_PARTITIONS,
    },
    config::{
        atomic_put_by_scheme, split_keeps_l0_headroom, verified_s3_endpoint, verified_s3_server,
        CloseMark, CompactionWatch, FullCompaction, ObjectStores, PollIntervals, RangeCheckpoint,
        RangePins, RangeRefs, RangeSize, StoreLocation, DEFAULT_FLUSH_INTERVAL,
        DEFAULT_OPEN_DB_BOUND, DEFAULT_POLLS, S3_CLIENT_RETRY_TIMEOUT,
    },
    deployments::{
        check_id as check_deployment_id, CatalogDoc, DeploymentStore, LoadedCatalog,
        RETIRING_HEADER as DEPLOYMENT_RETIRING_HEADER,
    },
    error::{Result, StoreError},
    format::{
        newer_header, object_policy, path_matches, policy_of, unlisted_object, ObjectPolicy,
        ReadMode, DATA_FORMAT, LIBRARY_LAYOUTS, OBJECT_POLICIES,
    },
    keyspace::{
        id_from_hex, id_from_slice, id_hex, InboundKind, InvocationId, QueueName, Stage, Table,
        ID_LEN,
    },
    nodewal::{
        acquire_shared, acquire_shared_with_hint, NodeTailers, NodeWal, NodeWalConfig, NodeWals,
        DEFAULT_LINGER, MAX_LINGER, SHARED_WAL_NAME,
    },
    owner::{
        delete_owner, list_retired, read_owner, retired_ranges, LogId, OwnerRecord, RetiredRange,
        SessionId, WalKind, WalSegment,
    },
    partition_table::{
        partition_key_for_callee, partition_key_for_callee_prefix, partition_key_for_schedule,
        partition_key_for_target, PartitionKey, PartitionTable, RangeEntry, RangeId, RangeIntent,
        CALLEE_PREFIX_MASK,
    },
    reader::{Freshness, PartitionReader, ReaderHealth, StallDetector},
    rules::{
        Limit, LoadedRules, Published as PublishedRules, Rate, Rule, RuleBook, RuleClear, RuleEdit,
        RuleStore, TenantRule, STAR_WARNING,
    },
    scope::{CounterPath, IdentError, LimitKey, RulePattern, Scope, TenantId, MAX_COMPONENT},
    session::{
        delegation_stats, expire_sessions_under_lock, pause_stats, read_fence_stats, rearm_stats,
        roles as coordinator_roles, Claim, CoordinatorRecord, CoordinatorSeen, CrashHandle, Holder,
        LeaseAnswer, LeaseConfig, LeaseKeeper, LeaseView, Membership, Observed, Observing,
        OwnerWal, SelfReport, Session, SessionMode, SessionRecord, SessionState, SessionTracker,
        ViewMember, COORDINATOR_OBJECT, DEFAULT_OWNER_WATCH, LIST_NET, REDELEGATE_AFTER_TTLS,
        RETAKE_AFTER_TTLS, SPARSE_AFTER_TTLS,
    },
    writer::{
        Batch, Committed, Durability, Flush, Hinted, LeadershipGrant, Mutations, PartitionId,
        PartitionWriter, Pending, RangeIter, RangeStore, RebuildStats, Rider, RiderChunk, Row,
        ScanCounts, ScanHow, SendDurability, SendTicket, SeqHeads, SlateDb, SlateIter, StoreStatus,
        ValidityHandle, WriteStallTotals, WriterHealth, WRITE_STALL_MIN,
    },
};

#[cfg(any(test, feature = "testkit"))]
pub use writer::ModelStatus;

#[doc(hidden)]
pub use fail as __fail;

/// The keyspace — public so the runtime can build its own scans, but the write path still goes
/// only through [`Mutations`].
pub mod keys {
    pub use crate::keyspace::*;
}
