// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **`atomic_put` must be declared explicitly on an unverified `s3://` store**
//! (assumption S3-2 of `docs/reliability/irreversible.md` §0.3).
//!
//! The shared WAL tier resolves an ambiguous log PUT by **reading back** when
//! the store declares `atomic_put` (NW-2). On a store that tears objects (local
//! MinIO with erasure coding tore lease objects in half when the client was
//! killed mid-CAS), reading back can ack a PUT that a sealer already saw as
//! absent (`cx-torn-resolve-by-read`): data loss. Every `s3://` used to default
//! to atomic. Now an `s3://` node runs without `--wal-atomic-put` only when the
//! store is on the verified list — AWS S3 (no custom endpoint), Tigris (by
//! endpoint), SeaweedFS (by the `Server` header) — and refuses to start
//! otherwise.
//!
//! The test's store is an `InMemory` with an `s3://…` URI and an endpoint
//! pointing at a fake HTTP server that only returns a `Server` header (the node
//! reads that header at startup; every object operation goes to `InMemory`).

// Tests run on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

mod common;

use {
    anyhow::{anyhow, ensure, Result},
    cairn_server::node::{self, NodeMode},
    cairn_store::{ObjectStores, StoreLocation},
    std::time::Duration,
};

/// A fake HTTP server that answers `403` to every request, with
/// `Server: <server>` (absent ⇒ no header). Returns the base URL.
async fn fake_endpoint(server: Option<&'static str>) -> Result<String> {
    let app = axum::Router::new().fallback(move || async move {
        let mut r = axum::response::Response::new(axum::body::Body::from("AccessDenied"));
        *r.status_mut() = axum::http::StatusCode::FORBIDDEN;
        if let Some(s) = server {
            r.headers_mut()
                .insert(axum::http::header::SERVER, s.parse().expect("header"));
        }
        r
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(format!("http://{addr}"))
}

/// One single-range cluster node on `bucket` (`InMemory`) with URI
/// `s3://walbench/<name>`, endpoint `endpoint`, declaring `declared`
/// (`--wal-atomic-put`).
async fn start_on(
    bucket: &common::Bucket,
    name: &str,
    endpoint: Option<String>,
    declared: Option<bool>,
) -> Result<node::RunningNode, String> {
    let loc = StoreLocation::from_store((**bucket).clone(), "", &format!("s3://walbench/{name}"))
        .with_endpoint(endpoint);
    let stores = ObjectStores::single(loc).with_wal_atomic_put(declared);
    let mut cfg = common::test_config(NodeMode {
        partitions: 1,
        lease_ttl: Duration::from_secs(2),
        node_id: Some(name.into()),
    });
    cfg.stores = stores;
    tokio::time::timeout(Duration::from_secs(60), node::start(cfg))
        .await
        .map_err(|_| "startup hung for over 60 s".to_string())?
        .map_err(|e| e.to_string())
}

/// A startup error must point the operator at the assumption and the docs.
fn points_to_docs(msg: &str) -> Result<()> {
    ensure!(
        msg.contains("S3-2") && msg.contains("self-hosting.md") && msg.contains("--wal-atomic-put"),
        "the error does not point to S3-2 / self-hosting.md / --wal-atomic-put: {msg}"
    );
    Ok(())
}

/// **Unknown endpoint, nothing declared ⇒ no startup.**
///
/// 1. The fake server returns `Server: MinIO` (a store measured to tear
///    objects, S3-2).
/// 2. The fake server returns no `Server` header (unrecognised).
/// 3. An endpoint nobody listens on (the header cannot be read).
///
/// Must see: all three startups fail, the message points to S3-2,
/// `self-hosting.md`, `--wal-atomic-put`. Before the fix: all three ran with
/// `atomic_put` = yes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unverified_s3_endpoint_without_declaration_refuses_to_start() -> Result<()> {
    let closed = {
        let l = std::net::TcpListener::bind("127.0.0.1:0")?;
        format!("http://{}", l.local_addr()?)
    };
    for (what, endpoint) in [
        ("MinIO", fake_endpoint(Some("MinIO")).await?),
        ("no header", fake_endpoint(None).await?),
        ("nobody listening", closed),
    ] {
        match start_on(&common::bucket(), "la", Some(endpoint), None).await {
            Ok(n) => {
                n.shutdown().await;
                return Err(anyhow!(
                    "{what}: the node started without declaring --wal-atomic-put"
                ));
            }
            Err(msg) => points_to_docs(&msg).map_err(|e| anyhow!("{what}: {e}"))?,
        }
    }
    Ok(())
}

/// **Declared explicitly ⇒ runs, on every endpoint**: `--wal-atomic-put false`
/// (safe on a store that tears objects) and `true` (the operator verified S3-2
/// on their store).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn declared_atomic_put_starts_on_any_endpoint() -> Result<()> {
    for declared in [false, true] {
        let endpoint = fake_endpoint(Some("MinIO")).await?;
        let bucket = common::bucket();
        let n = start_on(&bucket, "declared", Some(endpoint), Some(declared))
            .await
            .map_err(|e| anyhow!("--wal-atomic-put {declared}: {e}"))?;
        n.shutdown().await;
    }
    Ok(())
}

/// **Verified stores run as before, without declaring**: AWS S3 (no custom
/// endpoint, or an `*.amazonaws.com` endpoint), Tigris (endpoint), SeaweedFS
/// (the `Server` header of SeaweedFS 4.40 measured 2026-10-19: `SeaweedFS 30GB 4.40`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verified_s3_stores_start_without_declaration() -> Result<()> {
    for (what, endpoint) in [
        ("AWS S3", None),
        (
            "AWS S3 regional endpoint",
            Some("https://s3.us-east-1.amazonaws.com".to_string()),
        ),
        ("Tigris", Some("https://t3.storage.dev".to_string())),
        (
            "Tigris fly",
            Some("https://fly.storage.tigris.dev".to_string()),
        ),
        (
            "SeaweedFS",
            Some(fake_endpoint(Some("SeaweedFS 30GB 4.40")).await?),
        ),
    ] {
        let bucket = common::bucket();
        let n = start_on(&bucket, "verified", endpoint, None)
            .await
            .map_err(|e| anyhow!("{what}: {e}"))?;
        n.shutdown().await;
    }
    Ok(())
}
