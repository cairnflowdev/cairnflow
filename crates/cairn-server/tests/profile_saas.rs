// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The SaaS profile: `--profile saas` turns on the whole set of safe defaults
//! for a shared cell and refuses to start when a precondition is missing.
//!
//! Two layers:
//!
//! - **the real binary** ([`Binary`]): refusing to start (exit code, message, bucket
//!   untouched), and a node that starts with every precondition has strict egress +
//!   mandatory `https://`, a session cookie carrying `Secure`, the cluster secret not in the
//!   bucket;
//! - **an in-process node**: `deploy_require_identity` (a flag the profile turns on)
//!   refuses an SDK that does not declare `requestIdentity` — a binary with strict
//!   `--egress-*` cannot reach a fake SDK on `127.0.0.1`.
//!
//! The part where the profile turns off the user's OTLP: the unit tests of `main.rs`
//! (`saas_profile_turns_on_every_safe_default`) and `otel.rs` (feature `otel`).

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{ensure, Result},
    cairn_server::{
        auth::AuthConfig,
        node::{NodeConfig, NodeMode, RunningNode},
    },
    cairn_store::{ObjectStores, StoreLocation},
    common::{
        binary::Binary,
        fake_sdk::{FakeSdk, Step},
    },
    reqwest::StatusCode,
    serde_json::{json, Value},
    std::{path::PathBuf, time::Duration},
};

const ADMIN_KEY: &str = "cf_test_profile_saas_bootstrap_key_0123456789";
const SECRET: &str = "cf_test_cluster_secret_profile_saas_0123456789";
const MASTER: &str = "cf_test_identity_master_profile_saas_0123";

/// A binary's temporary data directory; drop ⇒ deleted.
struct TempDir(PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn temp_dir(tag: &str) -> TempDir {
    let dir = std::env::temp_dir().join(format!(
        "cairnflow-profile-saas-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    TempDir(dir)
}

/// Every file under `dir` (relative paths).
fn files(dir: &std::path::Path) -> Vec<String> {
    fn walk(root: &std::path::Path, at: &std::path::Path, out: &mut Vec<String>) {
        let Ok(rd) = std::fs::read_dir(at) else {
            return;
        };
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(root, &p, out);
            } else if let Ok(rel) = p.strip_prefix(root) {
                out.push(rel.to_string_lossy().into_owned());
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out);
    out
}

/// Each missing precondition ⇒ the binary exits with a non-zero code, says which
/// precondition, and **does not touch the bucket** (the data directory is not created).
#[test]
fn refuses_to_start_without_its_conditions() -> Result<()> {
    let full = [
        ("CAIRNFLOW_CLUSTER_SECRET", SECRET),
        ("CAIRNFLOW_IDENTITY_MASTER", MASTER),
        ("CAIRNFLOW_ADMIN_KEY", ADMIN_KEY),
    ];
    let deny = ["--egress-deny-cidr", "203.0.113.7"];
    /// (environment, extra flags, fragments that must be in the error).
    type Case<'a> = (Vec<(&'a str, &'a str)>, Vec<&'a str>, Vec<&'a str>);
    let cases: Vec<Case> = vec![
        (
            vec![("CAIRNFLOW_ADMIN_KEY", ADMIN_KEY)],
            vec![],
            vec![
                "missing CAIRNFLOW_CLUSTER_SECRET",
                "missing CAIRNFLOW_IDENTITY_MASTER",
                "missing --egress-deny-cidr",
            ],
        ),
        (
            vec![
                ("CAIRNFLOW_CLUSTER_SECRET", "too-short"),
                ("CAIRNFLOW_IDENTITY_MASTER", MASTER),
            ],
            deny.to_vec(),
            vec!["CAIRNFLOW_CLUSTER_SECRET is too short"],
        ),
        (
            vec![("CAIRNFLOW_CLUSTER_SECRET", SECRET)],
            deny.to_vec(),
            vec!["missing CAIRNFLOW_IDENTITY_MASTER"],
        ),
        (
            full.to_vec(),
            [&deny[..], &["--no-auth"]].concat(),
            vec!["--no-auth is not allowed"],
        ),
        (
            full.to_vec(),
            [&deny[..], &["--ui-cookie-secure", "off"]].concat(),
            vec!["--ui-cookie-secure off is not allowed"],
        ),
        (full.to_vec(), vec![], vec!["missing --egress-deny-cidr"]),
        // Tenant SQL in a child process: it may not
        // run inside the node, and a missing worker binary ⇒ no start.
        (
            full.to_vec(),
            [&deny[..], &["--query-isolation", "inline"]].concat(),
            vec!["--query-isolation inline is not allowed"],
        ),
        (
            full.to_vec(),
            [
                &deny[..],
                &["--query-worker", "/nonexistent/cairn-query-worker"],
            ]
            .concat(),
            vec!["cannot find the cairn-query-worker binary"],
        ),
    ];
    for (env, extra, want) in cases {
        let dir = temp_dir("refuse");
        let data = dir.0.to_string_lossy().into_owned();
        let mut args = vec!["--profile", "saas", "--data-dir", &data, "--port", "0"];
        args.extend(extra.iter().copied());
        let (exit, out) = Binary::run_to_exit(&args, &env, Duration::from_secs(30))?;
        ensure!(!exit.success(), "{args:?} must refuse to start:\n{out}");
        for w in &want {
            ensure!(out.contains(w), "{args:?}: missing {w:?} in:\n{out}");
        }
        ensure!(
            files(&dir.0).is_empty(),
            "{args:?}: refused yet wrote to the bucket: {:?}",
            files(&dir.0)
        );
    }
    Ok(())
}

/// Every precondition met ⇒ the node runs with every default of the profile:
///
/// 1. `http://` is refused (`--egress-require-https`), even to a public IP;
/// 2. `https://` to loopback is refused (`--egress-deny-private`);
/// 3. `https://` to the cell's ingress address is refused (`--egress-deny-cidr`);
/// 4. the console session cookie carries `Secure` even though `--host` is loopback;
/// 5. the bucket has no `auth/cluster-secret` (the cluster secret comes only from env).
#[tokio::test]
async fn running_node_has_every_safe_default() -> Result<()> {
    let dir = temp_dir("run");
    let data = dir.0.to_string_lossy().into_owned();
    let node = Binary::spawn(
        &[
            "--profile",
            "saas",
            "--data-dir",
            &data,
            "--port",
            "0",
            "--egress-deny-cidr",
            "203.0.113.7",
        ],
        &[
            ("CAIRNFLOW_CLUSTER_SECRET", SECRET),
            ("CAIRNFLOW_IDENTITY_MASTER", MASTER),
            ("CAIRNFLOW_ADMIN_KEY", ADMIN_KEY),
        ],
    )?;
    // The two threads reading `Binary`'s stdout/stderr run in parallel: the profile line may
    // reach the log after the port line `spawn` waits for (seen in a Ready run). Wait
    // with a limit.
    let t0 = std::time::Instant::now();
    while !node.dump().contains("profile:     saas") && t0.elapsed() < Duration::from_secs(5) {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    ensure!(
        node.dump().contains("profile:     saas"),
        "the start-up line does not mention the profile:\n{}",
        node.dump()
    );
    while !node.dump().contains("  query:") && t0.elapsed() < Duration::from_secs(5) {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    ensure!(
        node.dump().contains("cairn-query-worker processes"),
        "saas profile, yet /query does not run in a child process:\n{}",
        node.dump()
    );
    let http = reqwest::Client::new();
    for uri in [
        "http://198.51.100.1:9080/",
        "https://127.0.0.1:9080/",
        "https://203.0.113.7/",
    ] {
        let r = http
            .post(format!("{}/deployments", node.url))
            .bearer_auth(ADMIN_KEY)
            .json(&json!({ "uri": uri }))
            .send()
            .await?;
        let code = r.status();
        let v: Value = r.json().await.unwrap_or(Value::Null);
        ensure!(
            code == StatusCode::BAD_REQUEST && v["code"] == "egress_denied",
            "{uri}: {code} {v}\n{}",
            node.dump()
        );
    }
    let r = http
        .post(format!("{}/ui/session", node.url))
        .header(reqwest::header::ORIGIN, node.url.trim_end_matches('/'))
        .json(&json!({ "key": ADMIN_KEY }))
        .send()
        .await?;
    let code = r.status();
    let cookie = r
        .headers()
        .get(reqwest::header::SET_COOKIE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    ensure!(
        code == StatusCode::OK && cookie.split(';').any(|a| a.trim() == "Secure"),
        "console login: {code} {cookie:?}"
    );
    let written = files(&dir.0);
    ensure!(
        !written.iter().any(|f| f.ends_with("cluster-secret")),
        "the SaaS profile wrote the cluster secret to the bucket: {written:?}"
    );
    ensure!(!written.is_empty(), "node running, bucket empty?");
    Ok(())
}

/// The profile's default limits: a `--profile saas` binary
/// with an empty `tenants` book still returns every tenant limit as non-`null` on `GET
/// /tenant` — no tenant is "unlimited" on a shared cell. Including
/// `maxValueBytes` 64 MiB.
#[tokio::test]
async fn running_node_defaults_every_tenant_cap() -> Result<()> {
    let dir = temp_dir("caps");
    let data = dir.0.to_string_lossy().into_owned();
    let node = Binary::spawn(
        &[
            "--profile",
            "saas",
            "--data-dir",
            &data,
            "--port",
            "0",
            "--egress-deny-cidr",
            "203.0.113.7",
        ],
        &[
            ("CAIRNFLOW_CLUSTER_SECRET", SECRET),
            ("CAIRNFLOW_IDENTITY_MASTER", MASTER),
            ("CAIRNFLOW_ADMIN_KEY", ADMIN_KEY),
        ],
    )?;
    let r = reqwest::Client::new()
        .get(format!("{}/tenant", node.url))
        .bearer_auth(ADMIN_KEY)
        .send()
        .await?;
    let code = r.status();
    let v: Value = r.json().await.unwrap_or(Value::Null);
    let l = &v["limits"];
    ensure!(
        code == StatusCode::OK
            && l["maxLive"] == 1_000
            && l["maxDeployments"] == 20
            && l["maxSchedules"] == 50
            && l["maxTimers"] == 1_000
            && l["maxStreams"] == 64
            && l["maxStateKeys"] == 10_000
            && l["maxArtifactBytes"] == 1u64 << 30
            && l["maxJournalBytes"] == 32u64 << 20
            && l["maxPayload"] == 1u64 << 20
            && l["maxValueBytes"] == 64u64 << 20
            && l["maxJournal"] == 10_000,
        "GET /tenant: {code} {v}\n{}",
        node.dump()
    );
    Ok(())
}

/// A proxy in front of the fake SDK, editing the `/discover` manifest: `identity` =
/// `Some("v1")` ⇒ the SDK declares verification (`requestIdentity`).
async fn discover_proxy(
    sdk: &FakeSdk,
    identity: Option<&'static str>,
) -> Result<(String, tokio::task::JoinHandle<()>)> {
    discover_proxy_with(sdk, move |m| {
        if let Some(v) = identity {
            m["requestIdentity"] = json!(v);
        }
    })
    .await
}

/// A proxy in front of `sdk` editing the `GET /discover` manifest with `edit`.
async fn discover_proxy_with(
    sdk: &FakeSdk,
    edit: impl Fn(&mut Value) + Clone + Send + Sync + 'static,
) -> Result<(String, tokio::task::JoinHandle<()>)> {
    let to = sdk.url.clone();
    let app = axum::Router::new().fallback(move |req: axum::extract::Request| {
        let to = to.clone();
        let edit = edit.clone();
        async move {
            let r = reqwest::Client::new()
                .get(format!("{to}{}", req.uri()))
                .send()
                .await
                .unwrap();
            let ct = r
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            let mut m: Value = r.json().await.unwrap();
            edit(&mut m);
            (
                [(axum::http::header::CONTENT_TYPE, ct)],
                serde_json::to_vec(&m).unwrap(),
            )
        }
    });
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", l.local_addr()?);
    let task = tokio::spawn(async move {
        let _ = axum::serve(l, app).await;
    });
    Ok((url, task))
}

async fn node_requiring_identity(bucket: &common::Bucket) -> Result<RunningNode> {
    let stores = ObjectStores::single(StoreLocation::from_store(
        (**bucket).clone(),
        "",
        "memory://",
    ));
    let mut cfg = NodeConfig::new(stores, NodeMode::single(1));
    cfg.auth = AuthConfig {
        required: true,
        admin_key: Some(ADMIN_KEY.into()),
        cluster_secret: Some(SECRET.into()),
        metrics_key: None,
        admin_key_sha256: None,
        provision_key_sha256: None,
    };
    cfg.deploy_require_identity = true;
    common::start_led(cfg).await
}

/// `deploy_require_identity`: an SDK that does not declare
/// `requestIdentity: v1` ⇒ `400 identity_required`, no deployment is
/// written; declaring it ⇒ registration works.
#[tokio::test]
async fn unverified_deployment_is_refused() -> Result<()> {
    let bucket = common::bucket();
    let node = node_requiring_identity(&bucket).await?;
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("\"x\"")])
        .spawn()
        .await?;
    let http = reqwest::Client::new();
    let register = |uri: String| {
        let http = http.clone();
        let url = node.url();
        async move {
            let r = http
                .post(format!("{url}/deployments"))
                .bearer_auth(ADMIN_KEY)
                .json(&json!({ "uri": uri }))
                .send()
                .await?;
            let code = r.status();
            let v: Value = r.json().await.unwrap_or(Value::Null);
            anyhow::Ok((code, v))
        }
    };
    for identity in [None, Some("v2")] {
        let (url, task) = discover_proxy(&sdk, identity).await?;
        let (code, v) = register(url).await?;
        task.abort();
        ensure!(
            code == StatusCode::BAD_REQUEST && v["code"] == "identity_required",
            "{identity:?}: {code} {v}"
        );
    }
    let listed: Value = http
        .get(format!("{}/deployments", node.url()))
        .bearer_auth(ADMIN_KEY)
        .send()
        .await?
        .json()
        .await?;
    ensure!(
        listed.as_array().is_some_and(Vec::is_empty),
        "refused yet wrote a deployment: {listed}"
    );
    let (url, task) = discover_proxy(&sdk, Some("v1")).await?;
    let (code, v) = register(url).await?;
    task.abort();
    ensure!(code == StatusCode::OK, "SDK with verification: {code} {v}");
    Ok(())
}

/// `deploy_require_compression` (SaaS profile): a `REQUEST_RESPONSE` endpoint whose
/// SDK does not declare `requestCompression: gzip` ⇒ `400 compression_required`,
/// no deployment is written — a shared cell pays egress for every full
/// replay, and an uncompressed body is the case where it loses money on that. Declaring
/// `gzip` ⇒ registration works; `BIDI_STREAM` does not replay every turn ⇒
/// not checked.
#[tokio::test]
async fn uncompressed_request_response_deployment_is_refused() -> Result<()> {
    let bucket = common::bucket();
    let stores = ObjectStores::single(StoreLocation::from_store(
        (*bucket).clone(),
        "",
        "memory://",
    ));
    let mut cfg = NodeConfig::new(stores, NodeMode::single(1));
    cfg.auth = AuthConfig {
        required: true,
        admin_key: Some(ADMIN_KEY.into()),
        cluster_secret: Some(SECRET.into()),
        metrics_key: None,
        admin_key_sha256: None,
        provision_key_sha256: None,
    };
    cfg.deploy_require_compression = true;
    let node = common::start_led(cfg).await?;
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("\"x\"")])
        .spawn()
        .await?;
    let http = reqwest::Client::new();
    let register = |uri: String| {
        let http = http.clone();
        let url = node.url();
        async move {
            let r = http
                .post(format!("{url}/deployments"))
                .bearer_auth(ADMIN_KEY)
                .json(&json!({ "uri": uri }))
                .send()
                .await?;
            let code = r.status();
            let v: Value = r.json().await.unwrap_or(Value::Null);
            anyhow::Ok((code, v))
        }
    };
    // The fake SDK declares `REQUEST_RESPONSE`; a missing or wrong compression value ⇒ refused.
    for compression in [None, Some("br")] {
        let (url, task) = discover_proxy_with(&sdk, move |m| {
            m["protocolMode"] = json!("REQUEST_RESPONSE");
            match compression {
                Some(c) => m["requestCompression"] = json!(c),
                None => {
                    m.as_object_mut().unwrap().remove("requestCompression");
                }
            }
        })
        .await?;
        let (code, v) = register(url).await?;
        task.abort();
        ensure!(
            code == StatusCode::BAD_REQUEST && v["code"] == "compression_required",
            "{compression:?}: {code} {v}"
        );
    }
    let listed: Value = http
        .get(format!("{}/deployments", node.url()))
        .bearer_auth(ADMIN_KEY)
        .send()
        .await?
        .json()
        .await?;
    ensure!(
        listed.as_array().is_some_and(Vec::is_empty),
        "refused yet wrote a deployment: {listed}"
    );
    let (url, task) = discover_proxy_with(&sdk, |m| {
        m["protocolMode"] = json!("REQUEST_RESPONSE");
        m["requestCompression"] = json!("gzip");
    })
    .await?;
    let (code, v) = register(url).await?;
    task.abort();
    ensure!(code == StatusCode::OK, "RR with compression: {code} {v}");
    let (url, task) = discover_proxy_with(&sdk, |m| {
        m["protocolMode"] = json!("BIDI_STREAM");
        m.as_object_mut().unwrap().remove("requestCompression");
    })
    .await?;
    let (code, v) = register(url).await?;
    task.abort();
    ensure!(code == StatusCode::OK, "BIDI, no compression: {code} {v}");
    Ok(())
}

/// `--profile saas`: the node is **not dumpable**
/// (`PR_SET_DUMPABLE 0`) — a `/query` child process of the same user cannot read
/// `/proc/<node>/environ` (cluster secret, store keys). Linux only; self-hosting keeps the
/// usual debug/core dump behaviour.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn saas_node_environ_is_not_readable_by_its_user() -> Result<()> {
    let dir = temp_dir("dumpable");
    let data = dir.0.to_string_lossy().into_owned();
    let node = Binary::spawn(
        &[
            "--profile",
            "saas",
            "--data-dir",
            &data,
            "--port",
            "0",
            "--egress-deny-cidr",
            "203.0.113.7",
        ],
        &[
            ("CAIRNFLOW_CLUSTER_SECRET", SECRET),
            ("CAIRNFLOW_IDENTITY_MASTER", MASTER),
            ("CAIRNFLOW_ADMIN_KEY", ADMIN_KEY),
        ],
    )?;
    let environ = std::fs::read(format!("/proc/{}/environ", node.pid()));
    ensure!(
        environ.is_err(),
        "the same user can read the saas node's environ ({} bytes)",
        environ.map(|b| b.len()).unwrap_or_default()
    );
    // Self-hosted: unchanged.
    let plain = Binary::spawn(
        &[
            "--main-store",
            "memory://",
            "--port",
            "0",
            "--internal-port",
            "0",
        ],
        &[("CAIRNFLOW_ADMIN_KEY", ADMIN_KEY)],
    )?;
    ensure!(
        std::fs::read(format!("/proc/{}/environ", plain.pid())).is_ok(),
        "self-hosted node lost dumpable"
    );
    Ok(())
}
