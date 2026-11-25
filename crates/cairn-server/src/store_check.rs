// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **`atomic_put` must be declared explicitly on an unverified `s3://` store** (the S3-2
//! assumption, `docs/design/decisions.md`).
//!
//! The shared WAL resolves an ambiguous log PUT by **reading back** when the WAL tier
//! declares `atomic_put` (NW-2). On a store that tears objects (MinIO with erasure coding,
//! when the client is killed mid-PUT), reading back may acknowledge a PUT that the sealer
//! already saw as empty: **data loss**. The per-scheme default treats every `s3://` as
//! atomic, so before running with that default the node identifies the store:
//!
//! 1. `--wal-atomic-put true|false` declared ⇒ use the declaration, ask nothing;
//! 2. a scheme other than `s3`/`s3a` ⇒ the per-scheme default (`memory://`,
//!    `file://` atomic; other schemes not);
//! 3. the client's endpoint is AWS S3 (no custom endpoint, or
//!    `*.amazonaws.com`) or another endpoint on the verified list ⇒ run ([`cairn_store::verified_s3_endpoint`]);
//! 4. otherwise GET the endpoint's root (unsigned — every answer, `403` included, carries
//!    a `Server` header) and read the `Server` header: SeaweedFS ⇒ run
//!    ([`cairn_store::verified_s3_server`]);
//! 5. otherwise — MinIO, Garage, Ceph, an unknown gateway, no header, unreadable — ⇒
//!    **refuse to start**, with an error pointing at S3-2 and `docs/self-hosting.md`.
//!
//! Identification only chooses between "run with the default" and "require a declaration":
//! it never changes a capability **implicitly** (a proxy that changes the `Server` header
//! makes the node require a declaration — fail closed).

use {
    cairn_store::{
        retry::{self, Policy, Retryable, Verdict},
        StoreLocation,
    },
    std::time::Duration,
};

/// Limit on one probe GET.
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// Retry policy of the probe: transient network errors, four attempts (~1.5 s of pauses in total).
/// The store answered the D12 gate right before, so an endpoint not answering here is
/// unusual — fail closed instead of waiting out the whole boot limit.
const PROBE_POLICY: Policy = Policy::new(Duration::from_millis(200), Duration::from_secs(1), 4)
    .with_attempt_bound(PROBE_TIMEOUT);

/// Why the node may run with the WAL tier's current `atomic_put` — for the log.
pub(crate) async fn vet_wal_atomic_put(wal: &StoreLocation) -> Result<String, String> {
    if wal.atomic_put_declared() {
        return Ok(format!(
            "declared --wal-atomic-put {} (store {})",
            wal.atomic_put(),
            wal.uri()
        ));
    }
    if !wal.needs_atomic_put_vetting() {
        return Ok(format!("by scheme ({})", wal.uri()));
    }
    if let Some(kind) = cairn_store::verified_s3_endpoint(wal.endpoint()) {
        return Ok(format!(
            "{kind} by endpoint {} — S3-2 verified store",
            wal.endpoint().unwrap_or("client default")
        ));
    }
    let endpoint = wal.endpoint().unwrap_or_default().to_string();
    let server = retry::run(
        &retry::ops::BOOT_STORE_ID,
        &PROBE_POLICY,
        retry::Never,
        || server_header(&endpoint),
    )
    .await;
    if let Ok(Some(s)) = &server {
        if let Some(kind) = cairn_store::verified_s3_server(s) {
            return Ok(format!(
                "{kind} by Server header {s:?} of {endpoint} — S3-2 verified store"
            ));
        }
    }
    let seen = match &server {
        Ok(Some(s)) => format!("header Server {s:?}"),
        Ok(None) => "no Server header".into(),
        Err(e) => format!("cannot read the Server header: {}", e.0),
    };
    let minio = matches!(&server, Ok(Some(s)) if s.trim_start().starts_with("MinIO"));
    Err(format!(
        "WAL store {} (endpoint {endpoint}, {seen}) is not on the list of stores verified for atomic PUT (AWS S3, Tigris, SeaweedFS). On a store that can tear objects, the default `atomic_put` of s3:// lets the shared WAL ack a PUT that the sealer saw as empty — data loss (assumption S3-2, docs/reliability/irreversible.md §0.3). Set --wal-atomic-put false explicitly (safe on every store, only less live when a log PUT is ambiguous) or --wal-atomic-put true (only after running s3_store_contract S3-2 on this very store) — see docs/self-hosting.md (store table).{}",
        wal.uri(),
        if minio {
            " MinIO EC was measured to tear objects: use --wal-atomic-put false."
        } else {
            ""
        }
    ))
}

/// Error of one probe GET — every network error is transient.
#[derive(Debug)]
struct ProbeFault(String);

impl std::fmt::Display for ProbeFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Retryable for ProbeFault {
    fn verdict(&self) -> Verdict {
        Verdict::Transient
    }
    fn timed_out(op: &'static str, after: Duration) -> Self {
        ProbeFault(format!("{op}: exceeded {after:?}"))
    }
}

/// GET the root of `endpoint`, returning the `Server` header (absent ⇒ `None`). Every status
/// code counts as an answer: the request is unsigned, so S3 answers `403`.
async fn server_header(endpoint: &str) -> Result<Option<String>, ProbeFault> {
    let client = reqwest::Client::builder()
        .timeout(PROBE_TIMEOUT)
        .build()
        .map_err(|e| ProbeFault(e.to_string()))?;
    let resp = crate::egress::send(&client, client.get(endpoint))
        .await
        .map_err(|e| ProbeFault(e.to_string()))?;
    Ok(resp
        .headers()
        .get(reqwest::header::SERVER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string))
}
