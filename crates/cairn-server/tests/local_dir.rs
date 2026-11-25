// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **A node on a local directory** (`--data-dir`, `file://`).
//!
//! `LocalFileSystem` has `PutMode::Create` (SlateDB's `writer_epoch` fence)
//! but no CAS on etags, while the lease tier renews and takes ranges by CAS.
//! A local directory cannot be shared between machines, so a node on it is the only
//! node of its bucket — and the node holds an **exclusive lock** on the directory so that
//! this is a fact rather than an assumption: CAS emulated inside the process is correct
//! because no other process writes, and a session still "live" on the bucket at start-up
//! belongs to an earlier run that died (the operating system releases the lock when the
//! process dies, even on `kill -9`).

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{anyhow, ensure, Result},
    cairn_server::node::{self, NodeConfig, NodeMode},
    cairn_store::{ObjectStores, StoreLocation},
    std::{
        path::PathBuf,
        time::{Duration, Instant},
    },
};

/// TTL as in the binary's default: restarting while having to wait for the old session to
/// expire would mean waiting about this long.
const TTL: Duration = Duration::from_secs(10);

struct TempDir(PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn temp_dir(tag: &str) -> TempDir {
    let dir = std::env::temp_dir().join(format!(
        "cairnflow-local-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    TempDir(dir)
}

fn config(dir: &TempDir) -> Result<NodeConfig> {
    let stores = ObjectStores::single(StoreLocation::parse(&format!(
        "file://{}",
        dir.0.display()
    ))?);
    let mut cfg = NodeConfig::new(
        stores,
        NodeMode {
            partitions: 2,
            lease_ttl: TTL,
            node_id: None,
        },
    );
    cfg.tick_interval = Some(Duration::from_millis(50));
    cfg.flush_interval = Some(Duration::from_millis(5));
    Ok(cfg)
}

/// A node on a local directory runs the lease tier, and comes back after `kill -9`
/// without waiting for the old session to expire.
///
/// 1. Build node A (node id generated per process) on a fresh directory, wait for it to
///    lead both ranges.
/// 2. While A runs, build node B on the same directory.
/// 3. Drop A without `shutdown` — like `kill -9`: its session is still `live` on the
///    bucket, its leases not yielded.
/// 4. Build node C (another node id) on the same directory.
///
/// Must see: A starts (a store without CAS does not block a lone node); B is
/// refused because the directory is held; C leads both ranges well within one TTL.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_dir_node_leads_and_restarts_without_waiting_ttl() -> Result<()> {
    let dir = temp_dir("restart");
    let a = common::start_led(config(&dir)?).await?;

    let b = node::start(config(&dir)?).await;
    ensure!(
        b.is_err(),
        "a second node started on a local directory that is held"
    );
    let e = b.err().map(|e| e.to_string()).unwrap_or_default();
    ensure!(e.contains("lock"), "the error does not say why: {e}");

    let a_session = a
        .session
        .current()
        .ok_or_else(|| anyhow!("node A has no session"))?;
    drop(a);
    let a_record = std::fs::read_to_string(
        dir.0
            .join("sessions")
            .join(&a_session.node)
            .join(a_session.inc.to_string()),
    )?;
    ensure!(
        a_record.lines().any(|l| l == "state=live"),
        "after kill -9 A's session must still be live on the bucket:\n{a_record}"
    );
    let t0 = Instant::now();
    let c = common::start_led(config(&dir)?)
        .await
        .map_err(|e| anyhow!("could not restart after kill -9: {e}"))?;
    let took = t0.elapsed();
    c.shutdown().await;
    ensure!(
        took < TTL / 2,
        "restart after kill -9 took {took:?} — waited for the old session to expire (TTL {TTL:?})"
    );
    Ok(())
}

/// A node on a local directory has no peers: revoking a key returns `204` at once,
/// and close-when-unsure does not close by mistake —
/// the node confirms the registry is fresh by GETting its own generation.
///
/// 1. A node on a fresh directory, auth on, registry refresh every 100 ms (freshness
///    limit 200 ms). Create key K.
/// 2. Idle for 1.5 s (seven times the limit), asking with K every 100 ms.
/// 3. Revoke K.
///
/// Must see: (2) K always passes (`404` for a service that does not exist, never
/// `503`); (3) `204`, then K is refused (`401`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_dir_node_confirms_its_own_registry_and_revokes_at_once() -> Result<()> {
    const ADMIN: &str = "cf_test_local_dir_bootstrap_key_0123456";
    let dir = temp_dir("fresh");
    let mut cfg = config(&dir)?;
    cfg.deploy_refresh = Duration::from_millis(100);
    cfg.auth = cairn_server::auth::AuthConfig {
        required: true,
        admin_key: Some(ADMIN.into()),
        cluster_secret: None,
        metrics_key: None,
        admin_key_sha256: None,
        provision_key_sha256: None,
    };
    let n = common::start_led(cfg).await?;
    let url = n.url();
    let http = reqwest::Client::new();
    let r = async {
        let v: serde_json::Value = http
            .post(format!("{url}/admin/keys"))
            .bearer_auth(ADMIN)
            .json(&serde_json::json!({ "tenant": "default", "role": "invoke" }))
            .send()
            .await?
            .json()
            .await?;
        let (id, key) = (
            v["id"].as_str().unwrap_or_default().to_string(),
            v["secret"].as_str().unwrap_or_default().to_string(),
        );
        let ask = || {
            http.post(format!("{url}/invoke/NoSuchService/run"))
                .bearer_auth(&key)
                .send()
        };
        let t0 = Instant::now();
        while t0.elapsed() < Duration::from_millis(1500) {
            let code = ask().await?.status().as_u16();
            ensure!(
                code == 404,
                "--data-dir node returned {code} for a valid key while idle"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let code = http
            .delete(format!("{url}/admin/keys/{id}"))
            .bearer_auth(ADMIN)
            .send()
            .await?
            .status()
            .as_u16();
        ensure!(code == 204, "revoke on a node without peers: {code}");
        let code = ask().await?.status().as_u16();
        ensure!(code == 401, "a revoked key still passes: {code}");
        Ok(())
    }
    .await;
    n.shutdown().await;
    r.map_err(|e: anyhow::Error| anyhow!("{e}"))
}
