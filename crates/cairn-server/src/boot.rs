// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **The boot path retries transient I/O errors.**
//!
//! Previously any transient error in one boot step (the probe gate, `format`,
//! `sessions/`, `cluster-version`, `cluster.conf`, `auth/`, `tenants`,
//! `range-windows`) made the node exit with code 1 — a 4 h soak under the
//! `s3-standard-faults` profile saw 31 failed restarts.
//!
//! Every step now goes through [`cairn_store::retry::run`] with its own `Op`
//! (`boot.*` in `retry::ops`), and all steps share **one** time limit
//! ([`BOOT_IO_DEADLINE`], measured from entering the first gate): a later step
//! only gets what the earlier steps left of it. Only transient errors are
//! retried; permanent errors (a store without CAS, a bucket with the wrong
//! format, a binary older than the cluster) return immediately as before.
//! Every step must be **repeatable**: a read, or a read-then-`Create` that
//! resolves ambiguity by reading back (rule 7).

use {
    cairn_store::retry::{self, Op, Policy, Retryable, Verdict},
    std::{future::Future, time::Duration},
    tokio::time::Instant,
};

/// Shared limit for all retries on the boot path. Long enough for a few of the
/// S3 client's own retries (`S3_CLIENT_RETRY_TIMEOUT`, 30 s per request) on a
/// flaky store; past the limit the node exits with the last error — an error
/// misclassified as transient (`PermissionDenied` wrapped in `StoreError::Io`,
/// see [`cairn_store::retry::store_verdict`]) costs this much before it shows.
pub const BOOT_IO_DEADLINE: Duration = Duration::from_secs(120);

const FIRST_PAUSE: Duration = Duration::from_millis(100);
const MAX_PAUSE: Duration = Duration::from_secs(5);

/// The boot path's clock: each step takes a retry policy with the remaining budget.
pub(crate) struct BootClock {
    end: Instant,
}

impl BootClock {
    pub(crate) fn start() -> Self {
        Self {
            end: Instant::now() + BOOT_IO_DEADLINE,
        }
    }

    /// Policy for the next step: total limit = what remains. Once the budget is
    /// spent, a single uncapped attempt — the step still runs, it just is not
    /// retried any more.
    pub(crate) fn policy(&self) -> Policy {
        let left = self.end.saturating_duration_since(Instant::now());
        if left.is_zero() {
            Policy::new(FIRST_PAUSE, MAX_PAUSE, 1)
        } else {
            Policy::for_duration(FIRST_PAUSE, MAX_PAUSE, left)
        }
    }

    /// Runs one boot step through [`retry::run`] with self-classified errors.
    pub(crate) async fn run<T, E, F, Fut>(&self, op: &'static Op, f: F) -> Result<T, E>
    where
        E: Retryable + std::fmt::Display,
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        retry::run(op, &self.policy(), retry::Never, f).await
    }

    /// Like [`Self::run`] for steps that return string errors (`auth`,
    /// `tenants`, `range-windows`) — classified by [`text_verdict`].
    pub(crate) async fn run_text<T, F, Fut>(&self, op: &'static Op, mut f: F) -> Result<T, String>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, String>>,
    {
        self.run(op, || {
            let fut = f();
            async move { fut.await.map_err(TextFault) }
        })
        .await
        .map_err(|e| e.0)
    }
}

/// String error of the cluster-level loaders.
struct TextFault(String);

impl std::fmt::Display for TextFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Retryable for TextFault {
    fn verdict(&self) -> Verdict {
        text_verdict(&self.0)
    }
    fn timed_out(op: &'static str, after: Duration) -> Self {
        TextFault(format!("cannot read ({op}): over {after:?}"))
    }
}

/// The loaders of `auth/keys`, `tenants`, `range-windows` and the cluster
/// secret return string errors: their I/O errors start with "cannot read" /
/// "cannot create" (a failed GET or `Create` — see `auth.rs`, `tenants.rs`,
/// `windows.rs`); everything else is corrupt content (decoding, a secret too
/// short) — retrying cannot change it.
pub(crate) fn text_verdict(msg: &str) -> Verdict {
    const TRANSIENT: [&str; 2] = ["cannot read", "cannot create"];
    if TRANSIENT.iter().any(|p| msg.starts_with(p)) {
        Verdict::Transient
    } else {
        Verdict::Permanent
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_chain_classified_by_error_kind() {
        for transient in [
            "cannot read tenants: Generic",
            "cannot read range-windows: Generic S3 error: 503",
            "cannot read cluster-version: Generic",
            "cannot read (boot): over 30s",
            "cannot read auth/keys: Generic S3 error: 503",
            "cannot create cluster secret auth/cluster-secret: Generic S3 error",
            "cannot read cluster secret auth/cluster-secret: Generic",
        ] {
            assert_eq!(text_verdict(transient), Verdict::Transient, "{transient}");
        }
        for permanent in [
            "cluster secret too short",
            "cluster secret auth/cluster-secret is too short (3 bytes)",
            "corrupt key",
            "unknown line in tenants: x",
        ] {
            assert_eq!(text_verdict(permanent), Verdict::Permanent, "{permanent}");
        }
    }

    /// Shared limit: a later step only gets what earlier steps left of the
    /// budget; once it is spent, a single attempt.
    #[tokio::test(start_paused = true)]
    async fn shared_deadline_of_steps() {
        let clock = BootClock::start();
        assert_eq!(clock.policy().total, Some(BOOT_IO_DEADLINE));
        tokio::time::advance(Duration::from_secs(100)).await;
        assert_eq!(clock.policy().total, Some(Duration::from_secs(20)));
        tokio::time::advance(Duration::from_secs(30)).await;
        let p = clock.policy();
        assert_eq!((p.attempts, p.total), (1, None));
    }
}
