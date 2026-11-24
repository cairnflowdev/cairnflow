// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `SplitBlock`, `RangeOpError`: errors and blocking reasons of split/merge.

use super::*;

/// What blocks an auto-split right now ([`RangeOps::split_blocker`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SplitBlock {
    /// The table still has an unfinished split/merge (one intent cluster-wide).
    Intent,
    /// The cluster is upgrading.
    Upgrading(String),
    /// A child's L0 would hit the limit.
    L0(String),
    /// The range stands at or above the ancestor depth limit: splitting further
    /// lengthens the chain — wait for the child-standalone round to compact it.
    Depth(String),
}

impl SplitBlock {
    /// Label in `blockedBy` of `/ranges/suggestions`.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Intent => "intent",
            Self::Upgrading(_) => "upgrading",
            Self::L0(_) => "l0",
            Self::Depth(_) => "depth",
        }
    }
}

/// Error of one split/merge — classified by how the caller must handle it.
#[derive(Clone, Debug, thiserror::Error)]
pub enum RangeOpError {
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    BadRequest(String),
    /// This node does not lead the range — ask the node that leads it.
    #[error("this node does not lead range {range}")]
    NotLeader { range: u32, leader: Option<String> },
    /// The table changed midway, or the range has other unfinished work.
    #[error("{0}")]
    Conflict(String),
    /// Temporarily impossible (L0 has no room yet, the range is changing owner) — retry later.
    #[error("{0}")]
    Busy(String),
    /// A step failed; the intent has been rolled back.
    #[error("{0}")]
    Failed(String),
}

impl From<StoreError> for RangeOpError {
    fn from(e: StoreError) -> Self {
        match e {
            // A transient store I/O error, or a write/take that lost the lease midway (the
            // range just changed owner, another node took it): a flaky store or a legitimate race —
            // the coordinator rolls back, the caller retries.
            StoreError::Io(_)
            | StoreError::LeaseIo(_)
            | StoreError::LeaseLost
            | StoreError::Fenced
            | StoreError::LeaseUnavailable { .. }
            | StoreError::Closed => Self::Busy(format!(
                "store did not answer in time or the range just changed owner ({e}) — retry"
            )),
            StoreError::Slate(ref s) if matches!(s.kind(), slatedb::ErrorKind::Unavailable) => {
                Self::Busy(format!("store did not answer in time ({e}) — retry"))
            }
            // The range just retired: another split/merge has committed on it.
            StoreError::RangeRetired { .. } => {
                Self::Conflict(format!("range table changed midway ({e})"))
            }
            e => Self::Failed(e.to_string()),
        }
    }
}

impl From<ActorError> for RangeOpError {
    fn from(e: ActorError) -> Self {
        match e {
            // The range just changed owner (the other node died, its lease expired) or the
            // table changed midway: transient — the intent has been rolled back, the caller retries.
            ActorError::NotLeader | ActorError::Stopped | ActorError::WrongRange => {
                Self::Busy(format!("range is changing owner or table ({e}) — retry"))
            }
            // The source range's seal timed out or failed during quiesce (slow store): the Db
            // has not been cloned (SM-4) and the intent has been rolled back — transient (SM-7).
            ActorError::SealFailed(_) => {
                Self::Busy(format!("source range not closed yet ({e}) — retry"))
            }
            // A store error through the engine — including a memtable flush past its limit
            // and a lease lost during flush/quiesce: classified like a store error.
            ActorError::Engine(cairn_engine::EngineError::Store(e)) => Self::from(e),
            ActorError::Overloaded { .. } => Self::Busy(format!("{e} — retry")),
            e => Self::Failed(e.to_string()),
        }
    }
}

impl RangeOpError {
    /// Label in the operation book (`GET /ranges/ops`).
    pub fn label(&self) -> &'static str {
        match self {
            Self::NotFound(_) => "notFound",
            Self::BadRequest(_) => "badRequest",
            Self::NotLeader { .. } => "notLeader",
            Self::Conflict(_) => "conflict",
            Self::Busy(_) => "busy",
            Self::Failed(_) => "failed",
        }
    }
}

pub(super) type OpResult<T> = std::result::Result<T, RangeOpError>;
