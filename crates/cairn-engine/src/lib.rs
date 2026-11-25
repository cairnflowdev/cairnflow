// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Driver — connects [`cairn_runtime`] to [`cairn_store`].
//!
//! The pure state machine returns [`Effect`]s; here they become real I/O. The
//! two contracts the machine can only *describe* the driver must **enforce**:
//!
//! 1. **`Persist` before `Send`.** Everything of a step that must be durable is
//!    gathered into **one** commit, and nothing is sent down to the SDK until
//!    that commit is done.
//! 2. **Pending work lives in the state table, in the same commit as the journal
//!    entry that produced it** (INV-3). See [`Engine::step`] for why "same
//!    commit" is required and not excess caution.

mod chain;
mod engine;
mod interpartition;
pub mod lineage;
mod next_due;
mod outbox;
pub mod output;
mod promise;
pub mod schedule;
mod status;
mod tenant_limits;
pub mod units;

pub use {
    chain::ChainedTenants,
    engine::{
        input_head, lifecycle_slot, lifecycle_stage, lifecycle_status, signal_notification,
        CancelOutcome, CreateOptions, Engine, EngineError, InboundKey, Invocation, InvocationId,
        InvocationKind, JournalRow, ListFilter, ListScan, Listing, OutboxKey, OutboxRef,
        PromiseOutcome, PurgeOutcome, ReplyTo, RestartOutcome, Result, ResumeOutcome, RowWrite,
        ScheduleStamp, ScheduleWrite, SignalRef, Source, StageWrite, Target, VQueueRow, VQueueSlot,
        CANCELLED, HDR_STATE_LOCK, INFRA_STUCK, LIST_SCAN_BUDGET, LIST_SCAN_MAX, LIST_TAG_CHUNK,
        MAX_ATTEMPTS_EXHAUSTED, NO_OBJECT_KEY, QUARANTINED, RECOVER_CHUNK, RESTORE_CHUNK,
    },
    engine::{set_warm_check, warm_check_on, AuditGuard, WarmUse, WARM_CAP_BYTES},
    engine::{target_of_meta, BuildCursor, BuildStep, HintCursor, IndexState, QUERY_BUILD_CHUNK},
    engine::{TenantPurgeCursor, TenantRowsStep},
    interpartition::{
        tenant_on_wire, wire_tenant, CompletionPayload, DeliverCancel, DeliverCompletion,
        DeliverSignal, InterPartition, SignalId, StartCall, Watch,
    },
    lineage::Lineage,
    next_due::NextDue,
    outbox::{
        OutboundCall, OutboundCancel, OutboundFire, OutboundReply, OutboundWatch, OutboxEntry,
        OutboxError, ReplyResult,
    },
    output::{output_of, output_owner, InvocationOutput, SegmentedOutput, MAX_ASSEMBLE},
    promise::PromiseRecord,
    status::{InvocationStatus, Times},
    tenant_limits::{
        TenantLimit, TenantLimits, TenantLimitsSource, TenantRefusal, TenantState, TenantUsage,
        ARTIFACTS_LIMIT_CODE, JOURNAL_BYTES_LIMIT, JOURNAL_BYTES_LIMIT_CODE, JOURNAL_TOO_LONG,
        JOURNAL_TOO_LONG_CODE, PAYLOAD_TOO_LARGE, PAYLOAD_TOO_LARGE_CODE, STATE_KEYS_LIMIT_CODE,
        TENANT_LIMIT, TIMERS_LIMIT_CODE, VALUE_BYTES_LIMIT_CODE,
    },
};
