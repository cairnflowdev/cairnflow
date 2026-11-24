// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Actor error and outcome types — `ActorError`, `Result`, `InvokeOutcome` —
//! and the default SDK wait limit.
//!
//! Split out of `actor` (and then lifted to crate level) so that `turn` can
//! use them without `crate::actor`: the `actor`↔`turn` cycle is cut at module
//! level. `actor` re-exports everything here with `pub use`, so
//! `crate::actor::ActorError` still works.

use {crate::invoker, cairn_engine::EngineError, std::time::Duration};

#[derive(Debug, thiserror::Error)]
pub enum ActorError {
    #[error("engine stopped")]
    Stopped,
    /// This node is not the leader of the partition — principle N3.
    ///
    /// **Retryable**, and the client should retry on another node. This is the
    /// known price of the single-writer model: the leader handover window
    /// shows outside as an error, and it must be part of the API contract
    /// rather than hidden.
    #[error("this node is not the partition leader")]
    NotLeader,
    #[error(transparent)]
    Engine(#[from] EngineError),
    #[error(transparent)]
    Invoke(#[from] invoker::InvokeError),
    #[error("cannot spawn the engine thread: {0}")]
    Spawn(String),
    /// The command is about a key **outside the range** of this engine, or the
    /// range is retiring (D23). Nothing was done — the caller reloads the range
    /// table and sends it to the right engine; retrying here would fail the
    /// same way.
    #[error("command is outside this engine's range (the range changed or is retiring)")]
    WrongRange,
    /// Stepping down could not seal the Db (or could not write the tombstone):
    /// the Db **may still be open**. Whoever waits for "closed" — the split
    /// protocol — must not clone it.
    #[error("cannot close the range Db: {0}")]
    SealFailed(String),
    /// The range is overloaded: L0 is near its cap because compaction fell
    /// behind. **Nothing was created** — no invocation, no id. Resending after
    /// `retry_after_ms` is safe; the same idempotency key matches.
    ///
    /// Only commands that create new work at ingress get this error. Never on
    /// `/internal/inbound`: the sender would mark the whole destination range
    /// `down` and completions to it would get stuck too (`StartCall` is
    /// accepted durably and then deferred).
    #[error(
        "range is overloaded (L0 near its limit, compaction behind) — nothing created, retry later"
    )]
    Overloaded { retry_after_ms: u64 },
    /// The shared admission path refused the command
    /// (`turn::admit::Admission::Rejected`): the command asks for something
    /// that does not exist — **nothing was done**. `400`.
    #[error("{0}")]
    Rejected(String),
    /// Tenant `limited`/`suspended`, or it hit `max-live=` —
    /// `Admission::Refused`: new work is refused, **nothing was created**, no
    /// id. `429 tenant_limited`, `403 tenant_suspended`, `429
    /// live_invocations_limit`.
    #[error("tenant is not accepting new work: {0:?}")]
    TenantRefused(cairn_engine::TenantRefusal),
}

/// An error of the durability step ([`Lease::sync`]) goes straight into the
/// `Engine(Store(..))` branch.
///
/// It must be **that** branch, not a new variant: [`ActorError::is_fenced`]
/// recognises a zombie by exactly this shape, and `sync` is where zombies get
/// caught. A separate variant would look like an infrastructure error, and
/// infrastructure errors are retried — that is, retrying a fenced commit,
/// forever.
impl From<cairn_store::StoreError> for ActorError {
    fn from(e: cairn_store::StoreError) -> Self {
        Self::Engine(EngineError::Store(e))
    }
}

impl ActorError {
    /// Whether this error means "we just lost write authority".
    ///
    /// Telling these apart is mandatory: an infrastructure error is retried,
    /// while lost write authority **must not** be retried — drop the writer and
    /// return `NotLeader`. Retrying a fenced commit repeats forever something
    /// that can never succeed.
    pub fn is_fenced(&self) -> bool {
        match self {
            Self::NotLeader => true,
            Self::Engine(EngineError::Store(e)) => e.is_fenced(),
            _ => false,
        }
    }

    /// The error text the tenant reads: `lastFailure` of a failed attempt. A
    /// store error (and an error building/closing the range's Db, which carries
    /// a store error) names the bucket, host and object path — the cell's
    /// infrastructure: a fixed sentence, details in the node's log. Errors
    /// caused by the command itself (key too long, bad id) keep their text.
    pub fn tenant_text(&self) -> String {
        use cairn_store::StoreError as S;
        match self {
            Self::Engine(EngineError::Store(
                S::KeyTooLong { .. }
                | S::Ident(_)
                | S::BadDeploymentId { .. }
                | S::DeploymentExists { .. }
                | S::CatalogRejected { .. },
            )) => self.to_string(),
            Self::Engine(EngineError::Store(_)) | Self::Spawn(_) | Self::SealFailed(_) => {
                "the store is temporarily unavailable — the turn will be retried".into()
            }
            _ => self.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Typical store error text: bucket name, host, object path.
    const LEAK: &str = "Generic S3 error: GET https://customer-bucket.s3.tigris.dev/cell-1/ranges/3/manifest/00000000000000000042.manifest";

    /// A store error of a failed turn put into `lastFailure` (which the tenant
    /// reads through `GET /invocations/{id}` and `/query`) carries no bucket,
    /// host or object path; other errors keep their text.
    #[test]
    fn tenant_text_hides_store_details() {
        use cairn_store::StoreError as S;
        for e in [
            ActorError::Engine(EngineError::Store(S::Io(LEAK.into()))),
            ActorError::Engine(EngineError::Store(S::Corrupt(LEAK.into()))),
            ActorError::Spawn(LEAK.into()),
            ActorError::SealFailed(LEAK.into()),
        ] {
            let t = e.tenant_text();
            for part in ["customer-bucket", "tigris", "ranges/3", "manifest"] {
                assert!(!t.contains(part), "{e:?}: tenant text leaks {part:?}: {t}");
            }
        }
        assert_eq!(
            ActorError::Rejected("bad input".into()).tenant_text(),
            "bad input"
        );
    }
}

pub type Result<T> = std::result::Result<T, ActorError>;

/// Default SDK wait limit.
///
/// Wide enough for nearly every real handler, narrow enough that a hung
/// process does not sit there forever. There is deliberately **only one**
/// tier: a second "abort" tier only makes sense when there is a "please stop
/// gracefully" step in between, and mid-turn cairn has no channel to ask
/// through. Inventing two numbers for one step would be imitating a form.
pub const DEFAULT_SDK_TIMEOUT: Duration = Duration::from_secs(60);

/// Result of one invocation turn from ingress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvokeOutcome {
    /// Segmented output carries a ref + the source owner — ingress assembles it
    /// outside the engine before replying.
    Completed(cairn_engine::InvocationOutput),
    /// Waiting for something outside (a timer, or the result of `ctx.call`).
    Waiting,
    /// The last turn failed in a retryable way; the next attempt is scheduled.
    Retry {
        delay_ms: Option<u64>,
        /// `(code, message)` of the failed attempt — reported by the SDK, or an
        /// infrastructure error of the turn (`500`). Written durably in the same
        /// commit that schedules the next attempt
        /// (`cairn_store::keys::LastFailure`); `None` = the turn never reached
        /// the SDK.
        failure: Option<(u32, String)>,
    },
    /// The wait limit for a slot in the queue ran out.
    ///
    /// **The invocation still exists, is still durable, is still queued**, and
    /// has been scheduled to run again — the only thing dropped is this turn.
    /// For the client this is `429` with `Retry-After` *and* the id to ask
    /// again; it differs from `Waiting` in that it says **why** it has to wait.
    ///
    /// Not `503`: in cairn `503` already means "not the leader"
    /// ([`crate::ingress`]), and clients handle the two cases differently.
    Throttled {
        retry_after_ms: u64,
    },
    Failed {
        code: u32,
        message: String,
    },
    Paused {
        code: u32,
        message: String,
    },
    /// This workflow id already has a run. **Nothing was created.**
    ///
    /// A separate outcome rather than `Failed`: `Failed` means "your
    /// invocation ran and failed", while this means "your invocation never
    /// existed". Clients handle the two differently — for this one they
    /// `attach` to the running run, for the other there is nothing to attach
    /// to.
    ///
    /// Carries no id: the id of the run holding the workflow key **is** the id
    /// the caller just computed (it is deterministic), so returning it here is
    /// redundant.
    AlreadyInvoked,
}
