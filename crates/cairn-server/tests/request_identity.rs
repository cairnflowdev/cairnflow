// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Request identity toward customer endpoints:
//! a node with a master (`CAIRNFLOW_IDENTITY_MASTER`) signs every `/discover` and `/invoke`
//! with the **tenant's** Ed25519 key, derived from the master; the public keys are on
//! `GET /identity`; rotation goes through four stages with `cairn-ctl identity …`.
//!
//! The fake SDK stands behind a proxy that records the headers and body of every request the
//! runtime sends down ([`Capture`]). The SDK side (verification, `401`, `jti` matched against `Start`)
//! is in `sdk/typescript/test/identity.test.ts`.
//!
//! Each test opens with the ordering it reproduces (see AGENTS.md).

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
    cairn_server::{
        auth::AuthConfig,
        ctl::{Ctl, IdentityStep, RepairOptions},
        identity::{self, Master},
        node::{NodeConfig, NodeMode, RunningNode},
    },
    cairn_store::{ObjectStores, StoreLocation},
    common::fake_sdk::{FakeSdk, Step},
    serde_json::{json, Value},
    std::{
        sync::{Arc, Mutex},
        time::Duration,
    },
};

const ADMIN_KEY: &str = "cf_test_request_identity_bootstrap_0123456789";

fn master() -> Master {
    Master::new(b"master-for-test-request-identity-32b+").expect("≥ 32 byte")
}

/// One request the runtime sent down to the SDK.
#[derive(Clone, Debug)]
struct Seen {
    path: String,
    scheme: Option<String>,
    jwt: Option<String>,
    /// The `Host` header the request carried (the `host` claim must equal it).
    host: Option<String>,
    body: bytes::Bytes,
}

/// A proxy in front of the fake SDK: records each request then forwards it verbatim. With
/// `identity` it adds `requestIdentity` to the manifest (the SDK declares verification).
struct Capture {
    url: String,
    seen: Arc<Mutex<Vec<Seen>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Capture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Capture {
    async fn spawn(sdk: &FakeSdk, identity: Option<&'static str>) -> Result<Self> {
        let seen: Arc<Mutex<Vec<Seen>>> = Arc::default();
        let (to, log) = (sdk.url.clone(), Arc::clone(&seen));
        let app = axum::Router::new().fallback(move |req: axum::extract::Request| {
            let (to, log) = (to.clone(), Arc::clone(&log));
            async move {
                let (parts, body) = req.into_parts();
                let body = axum::body::to_bytes(body, usize::MAX).await.unwrap();
                let h = |k: &str| {
                    parts
                        .headers
                        .get(k)
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_string)
                };
                log.lock().unwrap().push(Seen {
                    path: parts.uri.path().to_string(),
                    scheme: h(identity::SCHEME_HEADER),
                    jwt: h(identity::JWT_HEADER),
                    host: h("host"),
                    body: body.clone(),
                });
                let mut rb = reqwest::Client::new()
                    .request(parts.method.clone(), format!("{to}{}", parts.uri))
                    .body(body);
                for k in ["content-type", "accept"] {
                    if let Some(v) = h(k) {
                        rb = rb.header(k, v);
                    }
                }
                let r = rb.send().await.unwrap();
                let status = r.status();
                let ct = r
                    .headers()
                    .get("content-type")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                let mut out = r.bytes().await.unwrap();
                if let (Some(v), "/discover") = (identity, parts.uri.path()) {
                    let mut m: Value = serde_json::from_slice(&out).unwrap();
                    m["requestIdentity"] = json!(v);
                    out = serde_json::to_vec(&m).unwrap().into();
                }
                (status, [(axum::http::header::CONTENT_TYPE, ct)], out)
            }
        });
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", l.local_addr()?);
        let task = tokio::spawn(async move {
            let _ = axum::serve(l, app).await;
        });
        Ok(Self { url, seen, task })
    }

    fn seen(&self, path: &str) -> Vec<Seen> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.path == path)
            .cloned()
            .collect()
    }
}

async fn node_on(bucket: &common::Bucket, master: Option<Master>) -> Result<RunningNode> {
    let stores = ObjectStores::single(StoreLocation::from_store(
        (**bucket).clone(),
        "",
        "memory://",
    ));
    let mut cfg = NodeConfig::new(stores, NodeMode::single(1));
    cfg.tick_interval = Some(Duration::from_millis(50));
    cfg.flush_interval = Some(Duration::from_millis(5));
    cfg.deploy_refresh = Duration::from_millis(100);
    cfg.auth = AuthConfig {
        required: true,
        admin_key: Some(ADMIN_KEY.into()),
        cluster_secret: Some("cf_test_cluster_secret_request_identity".into()),
        metrics_key: None,
        admin_key_sha256: None,
        provision_key_sha256: None,
    };
    cfg.identity_master = master;
    common::start_led(cfg).await
}

fn http() -> reqwest::Client {
    reqwest::Client::new()
}

/// A `role` key of `tenant`, created with the bootstrap key.
async fn key(node: &RunningNode, tenant: &str, role: &str) -> Result<String> {
    let r = http()
        .post(format!("{}/admin/keys", node.url()))
        .bearer_auth(ADMIN_KEY)
        .json(&json!({ "tenant": tenant, "role": role }))
        .send()
        .await?;
    ensure!(r.status().is_success(), "create key: {}", r.status());
    let v: Value = r.json().await?;
    Ok(v["secret"].as_str().unwrap_or_default().to_string())
}

async fn register(node: &RunningNode, bearer: &str, uri: &str) -> Result<Value> {
    let r = http()
        .post(format!("{}/deployments", node.url()))
        .bearer_auth(bearer)
        .json(&json!({ "uri": uri }))
        .send()
        .await?;
    let code = r.status();
    let v: Value = r.json().await.unwrap_or(Value::Null);
    ensure!(code.is_success(), "register: {code} {v}");
    Ok(v)
}

async fn invoke(node: &RunningNode, bearer: &str) -> Result<String> {
    let r = http()
        .post(format!("{}/invoke/Echo/run", node.url()))
        .bearer_auth(bearer)
        .body("\"x\"")
        .send()
        .await?;
    let code = r.status();
    let v: Value = r.json().await.unwrap_or(Value::Null);
    ensure!(
        code == 200 && v["status"] == "completed",
        "invoke: {code} {v}"
    );
    Ok(v["id"].as_str().unwrap_or_default().to_string())
}

/// `GET /identity` with the `bearer` key.
async fn published(node: &RunningNode, bearer: &str) -> Result<Value> {
    let r = http()
        .get(format!("{}/identity", node.url()))
        .bearer_auth(bearer)
        .send()
        .await?;
    let code = r.status();
    let v: Value = r.json().await.unwrap_or(Value::Null);
    ensure!(code == 200, "GET /identity: {code} {v}");
    Ok(v)
}

fn kids(v: &Value) -> Vec<(u64, String, bool)> {
    v["keys"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|k| {
                    (
                        k["generation"].as_u64().unwrap_or(0),
                        k["kid"].as_str().unwrap_or("").to_string(),
                        k["active"] == true,
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

/// `(hex id, attempt)` of the `Start` in the body of an `/invoke` request.
fn start_of(body: &bytes::Bytes) -> Result<(String, u32)> {
    use prost::Message as _;
    let mut buf = bytes::BytesMut::from(&body[..]);
    let f = cairn_protocol::FrameDecoder::new()
        .decode(&mut buf)?
        .ok_or_else(|| anyhow!("empty body"))?;
    ensure!(
        f.ty == cairn_protocol::MessageType::START,
        "the first frame is not Start"
    );
    let s = cairn_protocol::pb::StartMessage::decode(f.payload)?;
    Ok((
        cairn_store::id_hex(&s.id.as_ref().try_into()?),
        s.retry_count_since_last_stored_entry,
    ))
}

/// Verifies `seen` with `kid`: returns the claims.
fn claims(seen: &Seen, kid: &str) -> Result<Value> {
    ensure!(
        seen.scheme.as_deref() == Some(identity::SCHEME_V1),
        "{}: missing {}",
        seen.path,
        identity::SCHEME_HEADER
    );
    let jwt = seen
        .jwt
        .as_deref()
        .ok_or_else(|| anyhow!("{}: missing {}", seen.path, identity::JWT_HEADER))?;
    identity::verify(jwt, kid).map_err(|e| anyhow!("{}: {e}", seen.path))
}

/// **Discovery and invoke carry the tenant's signature, with the right claims.**
///
/// 1. A node with a master; tenant `acme` (an `admin` key) registers the fake SDK through the
///    header-recording proxy.
/// 2. `acme`'s `GET /identity` ⇒ one key, generation 1, signing.
/// 3. Invoke once.
///
/// Must see: `/discover` carries a JWT verifiable with that `kid`, `aud`
/// `/discover`, `sub` `acme`, `jti` `discover`; `/invoke/Echo/run` carries a JWT
/// with `aud` = the path, `jti` = `<id>:<attempt>` matching the request's own `Start`,
/// `exp − iat = iat − nbf = 60`; every request carries its own 16-byte `nonce`. The deployment
/// records that the SDK turned on verification (`requestIdentity`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn requests_carry_the_tenants_signature() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("done")])
        .spawn()
        .await?;
    let cap = Capture::spawn(&sdk, Some("v1")).await?;
    let bucket = common::bucket();
    let node = node_on(&bucket, Some(master())).await?;
    let r = common::within(Duration::from_secs(60), "signature", async {
        let admin = key(&node, "acme", "admin").await?;
        let d = register(&node, &admin, &cap.url).await?;
        ensure!(
            d["requestIdentity"] == "v1",
            "the deployment records the SDK's verification: {d}"
        );
        let p = published(&node, &admin).await?;
        ensure!(p["tenant"] == "acme" && p["signing"] == true, "{p}");
        let k = kids(&p);
        ensure!(
            k.len() == 1 && k[0].0 == 1 && k[0].2,
            "one key, generation 1: {p}"
        );
        let kid = &k[0].1;
        ensure!(kid.starts_with(identity::KID_PREFIX), "{kid}");

        let disc = cap.seen("/discover");
        ensure!(!disc.is_empty(), "no /discover seen");
        let c = claims(&disc[0], kid)?;
        ensure!(
            c["aud"] == "/discover" && c["sub"] == "acme" && c["jti"] == identity::DISCOVER_JTI,
            "{c}"
        );

        let id = invoke(&node, &admin).await?;
        let inv = cap.seen("/invoke/Echo/run");
        ensure!(!inv.is_empty(), "no /invoke seen");
        for s in &inv {
            let c = claims(s, kid)?;
            let (sid, retry) = start_of(&s.body)?;
            ensure!(sid == id, "the request's Start belongs to {sid}, not {id}");
            ensure!(
                c["aud"] == "/invoke/Echo/run"
                    && c["sub"] == "acme"
                    && c["jti"] == format!("{sid}:{retry}"),
                "{c}"
            );
            let (iat, nbf, exp) = (
                c["iat"].as_u64().unwrap_or(0),
                c["nbf"].as_u64().unwrap_or(0),
                c["exp"].as_u64().unwrap_or(0),
            );
            ensure!(exp - iat == 60 && iat - nbf == 60, "{c}");
        }
        // Every signed request carries its own `nonce` (16 bytes, base64url): no two
        // requests share a JWT.
        let all: Vec<Seen> = cap.seen.lock().unwrap().clone();
        let mut nonces = std::collections::HashSet::new();
        for s in &all {
            let c = claims(s, kid)?;
            let n = c["nonce"].as_str().unwrap_or_default().to_string();
            ensure!(n.len() == 22, "{}: nonce {n:?}", s.path);
            ensure!(nonces.insert(n), "{}: a nonce seen twice", s.path);
        }
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **The `/invoke` JWT is bound to the request's own body and host.**
///
/// 1. A node with a master; tenant `acme` registers the fake SDK through the header-recording proxy.
/// 2. Invoke once.
///
/// Must see: every JWT of `/invoke/Echo/run` carries `bsh` = base64url(sha256 of the
/// body before compression) and `host` = the request's `Host` header; `/discover` carries `host`
/// (no body ⇒ no `bsh`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invoke_jwt_binds_body_and_host() -> Result<()> {
    use base64::Engine as _;
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("done")])
        .spawn()
        .await?;
    let cap = Capture::spawn(&sdk, Some("v1")).await?;
    let bucket = common::bucket();
    let node = node_on(&bucket, Some(master())).await?;
    let r = common::within(Duration::from_secs(60), "body and host", async {
        let admin = key(&node, "acme", "admin").await?;
        register(&node, &admin, &cap.url).await?;
        let kid = kids(&published(&node, &admin).await?)[0].1.clone();
        let disc = cap.seen("/discover");
        ensure!(!disc.is_empty(), "no /discover seen");
        let c = claims(&disc[0], &kid)?;
        ensure!(
            c["host"].as_str() == disc[0].host.as_deref() && c.get("bsh").is_none(),
            "{c}"
        );
        invoke(&node, &admin).await?;
        let inv = cap.seen("/invoke/Echo/run");
        ensure!(!inv.is_empty(), "no /invoke seen");
        for s in &inv {
            let c = claims(s, &kid)?;
            let want = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(ring::digest::digest(&ring::digest::SHA256, &s.body).as_ref());
            ensure!(
                c["bsh"] == want.as_str(),
                "bsh {} ≠ sha256 of the body {want}",
                c["bsh"]
            );
            ensure!(
                c["host"].as_str() == s.host.as_deref(),
                "host {} ≠ Host {:?}",
                c["host"],
                s.host
            );
        }
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **Tenant B cannot carry tenant A's signature.**
///
/// 1. A node with a master; two tenants `acme`, `beta` register **the same** SDK URL.
/// 2. Each tenant invokes once.
///
/// Must see: the two tenants' `kid`s differ; `beta`'s requests verify
/// with `beta`'s `kid` and **not** with `acme`'s `kid` (and vice
/// versa); `sub` is the request's own tenant.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tenant_b_cannot_carry_tenant_a_signature() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("done")])
        .spawn()
        .await?;
    let cap = Capture::spawn(&sdk, None).await?;
    let bucket = common::bucket();
    let node = node_on(&bucket, Some(master())).await?;
    let r = common::within(Duration::from_secs(60), "two tenants", async {
        let a = key(&node, "acme", "admin").await?;
        let b = key(&node, "beta", "admin").await?;
        let kid_a = kids(&published(&node, &a).await?)[0].1.clone();
        let kid_b = kids(&published(&node, &b).await?)[0].1.clone();
        ensure!(kid_a != kid_b, "two tenants share one key");
        register(&node, &a, &cap.url).await?;
        let id_a = invoke(&node, &a).await?;
        register(&node, &b, &cap.url).await?;
        let id_b = invoke(&node, &b).await?;
        for s in cap.seen("/invoke/Echo/run") {
            let (sid, _) = start_of(&s.body)?;
            let (mine, other, tenant) = if sid == id_a {
                (&kid_a, &kid_b, "acme")
            } else if sid == id_b {
                (&kid_b, &kid_a, "beta")
            } else {
                return Err(anyhow!("unknown Start {sid}"));
            };
            let c = claims(&s, mine)?;
            ensure!(c["sub"] == tenant, "{c}");
            ensure!(
                claims(&s, other).is_err(),
                "a request of {tenant} verifies with the other tenant's key"
            );
        }
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

fn ctl(bucket: &common::Bucket) -> Ctl {
    Ctl::new(ObjectStores::single(StoreLocation::from_store(
        (**bucket).clone(),
        "",
        "memory://",
    )))
}

fn apply(command: &str) -> RepairOptions {
    RepairOptions {
        apply: true,
        force: false,
        who: "test".into(),
        command: command.into(),
        expect_version: None,
        now_ms: common::wall_ms(),
    }
}

/// Waits for `bearer`'s `GET /identity` to satisfy `want` (the node re-reads the `tenants`
/// book at the `deploy_refresh` cadence).
async fn until_published(
    node: &RunningNode,
    bearer: &str,
    what: &str,
    want: impl Fn(&[(u64, String, bool)]) -> bool,
) -> Result<Vec<(u64, String, bool)>> {
    let t0 = std::time::Instant::now();
    loop {
        let k = kids(&published(node, bearer).await?);
        if want(&k) {
            return Ok(k);
        }
        ensure!(t0.elapsed() < Duration::from_secs(10), "{what}: {k:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// **Four-stage key rotation, never "sign with the newest key" right away.**
///
/// 1. `acme` signs with generation 1.
/// 2. `cairn-ctl identity publish acme` ⇒ `GET /identity` publishes two keys,
///    generation 1 still signing: new requests still verify with `kid` 1.
/// 3. (The customer adds `kid` 2 to the SDK.) `identity activate acme` ⇒ new requests
///    verify with `kid` 2, not with `kid` 1.
/// 4. `identity retire acme` ⇒ only `kid` 2 is left.
///
/// Must see: each stage exactly as above; each command leaves an audit
/// record `audit/ctl/…`; `activate` with nothing published and `publish` while a
/// key is pending are both refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rotation_goes_through_four_phases() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("done")])
        .spawn()
        .await?;
    let cap = Capture::spawn(&sdk, None).await?;
    let bucket = common::bucket();
    let node = node_on(&bucket, Some(master())).await?;
    let ctl = ctl(&bucket);
    let r = common::within(Duration::from_secs(90), "key rotation", async {
        let a = key(&node, "acme", "admin").await?;
        register(&node, &a, &cap.url).await?;
        let g1 = kids(&published(&node, &a).await?)[0].1.clone();
        ensure!(
            ctl.identity(
                "acme",
                IdentityStep::Activate,
                &apply("identity activate acme")
            )
            .await
            .is_err(),
            "activate before a new key is published must be refused"
        );

        let rep = ctl
            .identity(
                "acme",
                IdentityStep::Publish,
                &apply("identity publish acme"),
            )
            .await
            .map_err(|e| anyhow!("publish: {e}"))?;
        ensure!(rep.applied && rep.audit.is_some(), "{rep:?}");
        let k = until_published(&node, &a, "two keys published", |k| k.len() == 2).await?;
        ensure!(
            k[0] == (1, g1.clone(), true) && k[1].0 == 2 && !k[1].2,
            "{k:?}"
        );
        let g2 = k[1].1.clone();
        ensure!(
            ctl.identity(
                "acme",
                IdentityStep::Publish,
                &apply("identity publish acme")
            )
            .await
            .is_err(),
            "publish while a key is pending must be refused"
        );
        let before = cap.seen("/invoke/Echo/run").len();
        invoke(&node, &a).await?;
        for s in &cap.seen("/invoke/Echo/run")[before..] {
            claims(s, &g1)?;
        }

        ctl.identity(
            "acme",
            IdentityStep::Activate,
            &apply("identity activate acme"),
        )
        .await
        .map_err(|e| anyhow!("activate: {e}"))?;
        until_published(&node, &a, "activated", |k| k.len() == 2 && k[1].2).await?;
        let before = cap.seen("/invoke/Echo/run").len();
        invoke(&node, &a).await?;
        for s in &cap.seen("/invoke/Echo/run")[before..] {
            claims(s, &g2)?;
            ensure!(
                claims(s, &g1).is_err(),
                "activated yet still signing with generation 1"
            );
        }

        ctl.identity("acme", IdentityStep::Retire, &apply("identity retire acme"))
            .await
            .map_err(|e| anyhow!("retire: {e}"))?;
        let k = until_published(&node, &a, "old key retired", |k| k.len() == 1).await?;
        ensure!(k[0] == (2, g2.clone(), true), "{k:?}");

        let audits = common::memory_has_prefix(&bucket, "audit/ctl/").await?;
        ensure!(audits, "no audit record");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **No master ⇒ no signing** (self-hosting as before request identity existed).
///
/// Must see: requests to the SDK carry no identity header; `GET
/// /identity` returns `signing: false`, no key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_master_nothing_is_signed() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("done")])
        .spawn()
        .await?;
    let cap = Capture::spawn(&sdk, None).await?;
    let bucket = common::bucket();
    let node = node_on(&bucket, None).await?;
    let r = common::within(Duration::from_secs(60), "no master", async {
        register(&node, ADMIN_KEY, &cap.url).await?;
        invoke(&node, ADMIN_KEY).await?;
        for s in cap.seen.lock().unwrap().iter() {
            ensure!(
                s.scheme.is_none() && s.jwt.is_none(),
                "{}: an identity header without a master",
                s.path
            );
        }
        let p = published(&node, ADMIN_KEY).await?;
        ensure!(p["signing"] == false && kids(&p).is_empty(), "{p}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

// ───────────────────────────────────── a verifying BIDI SDK (one JWT, one stream)

/// A hand-written `BIDI_STREAM` SDK that keeps the ledger every real SDK keeps when it
/// verifies (TS `serve.ts::UsedTokens`, Python, Go): **one JWT opens one stream**, keyed
/// by the JWT's signature. A second stream carrying a JWT already seen gets `401` before
/// the handler runs. Every turn it accepts ends with `Error(behavior = PAUSE)`.
///
/// h1 serves `/discover`; h2c (prior knowledge, sniffed on the preface) serves the
/// runtime's h2 probe of `/discover` and the `/invoke` streams.
struct PausingBidi {
    url: String,
    shared: Arc<BidiShared>,
}

#[derive(Default)]
struct BidiShared {
    /// Every `/invoke` stream: (the JWT it carried, accepted).
    streams: Mutex<Vec<(String, bool)>>,
    /// Signatures already used to open a stream.
    used: Mutex<std::collections::HashSet<String>>,
    tasks: Mutex<Vec<tokio::task::AbortHandle>>,
}

impl Drop for PausingBidi {
    fn drop(&mut self) {
        for h in self.shared.tasks.lock().unwrap().drain(..) {
            h.abort();
        }
    }
}

impl PausingBidi {
    async fn spawn() -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let shared = Arc::new(BidiShared::default());
        let sh = Arc::clone(&shared);
        let accept = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let h = tokio::spawn(bidi_conn(socket, Arc::clone(&sh)));
                sh.tasks.lock().unwrap().push(h.abort_handle());
            }
        });
        shared.tasks.lock().unwrap().push(accept.abort_handle());
        Ok(Self { url, shared })
    }

    fn streams(&self) -> Vec<(String, bool)> {
        self.shared.streams.lock().unwrap().clone()
    }
}

fn bidi_manifest() -> String {
    use cairn_server::discovery::{
        EndpointManifest, HandlerManifest, ProtocolMode, ServiceManifest, ServiceType,
    };
    let m = EndpointManifest {
        protocol_mode: ProtocolMode::BidiStream,
        min_protocol_version: 1,
        max_protocol_version: 4,
        services: vec![ServiceManifest {
            name: "Warehouse".into(),
            ty: ServiceType::Service,
            handlers: vec![HandlerManifest {
                name: "ship".into(),
                ty: None,
                contract: Default::default(),
            }],
            contract: Default::default(),
        }],
    };
    let mut v = serde_json::to_value(&m).expect("manifest");
    v["requestIdentity"] = json!("v1");
    v.to_string()
}

async fn bidi_conn(socket: tokio::net::TcpStream, sh: Arc<BidiShared>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    // h2c opens with `PRI * HTTP/2.0`; look at 4 bytes.
    let mut first = [0u8; 4];
    loop {
        match socket.peek(&mut first).await {
            Ok(0) | Err(_) => return,
            Ok(4) => break,
            Ok(_) => tokio::task::yield_now().await,
        }
    }
    if &first == b"PRI " {
        let Ok(mut conn) = h2::server::handshake(socket).await else {
            return;
        };
        while let Some(Ok((req, respond))) = conn.accept().await {
            let h = tokio::spawn(bidi_h2(req, respond, Arc::clone(&sh)));
            sh.tasks.lock().unwrap().push(h.abort_handle());
        }
        return;
    }
    // h1: only `/discover`.
    let mut socket = socket;
    let mut buf = Vec::new();
    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
        let mut chunk = [0u8; 4096];
        match socket.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    }
    if !buf.starts_with(b"GET /discover") {
        return;
    }
    let body = bidi_manifest();
    let reply = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        cairn_server::discovery::DISCOVERY_V1,
        body.len()
    );
    let _ = socket.write_all(reply.as_bytes()).await;
    let _ = socket.shutdown().await;
}

async fn bidi_h2(
    req: http::Request<h2::RecvStream>,
    mut respond: h2::server::SendResponse<bytes::Bytes>,
    sh: Arc<BidiShared>,
) {
    use cairn_protocol::{pb, Message};
    let respond_with = |respond: &mut h2::server::SendResponse<bytes::Bytes>,
                        status: u16,
                        ct: &str,
                        body: bytes::Bytes| {
        let resp = http::Response::builder()
            .status(status)
            .header("content-type", ct)
            .body(())
            .expect("valid header");
        if let Ok(mut out) = respond.send_response(resp, false) {
            let _ = out.send_data(body, true);
        }
    };
    if req.uri().path() == "/discover" {
        respond_with(
            &mut respond,
            200,
            cairn_server::discovery::DISCOVERY_V1,
            bidi_manifest().into(),
        );
        return;
    }
    let jwt = req
        .headers()
        .get(identity::JWT_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let signature = jwt.rsplit('.').next().unwrap_or_default().to_string();
    let fresh = !signature.is_empty() && sh.used.lock().unwrap().insert(signature);
    sh.streams.lock().unwrap().push((jwt, fresh));
    if !fresh {
        respond_with(
            &mut respond,
            401,
            "application/json",
            bytes::Bytes::from_static(
                br#"{"error":"this request identity token has already opened a stream"}"#,
            ),
        );
        return;
    }
    // Headers at once, before reading the body, as the protocol requires; then wait for `Start` and
    // ask for a pause.
    let resp = http::Response::builder()
        .status(200)
        .header("content-type", cairn_server::invoker::INVOCATION_V1)
        .body(())
        .expect("valid header");
    let Ok(mut out) = respond.send_response(resp, false) else {
        return;
    };
    let mut body = req.into_body();
    let dec = cairn_protocol::FrameDecoder::new();
    let mut buf = bytes::BytesMut::new();
    while let Some(Ok(chunk)) = body.data().await {
        let _ = body.flow_control().release_capacity(chunk.len());
        buf.extend_from_slice(&chunk);
        if let Ok(Some(_start)) = dec.decode(&mut buf) {
            let pause = Message::Error(pb::ErrorMessage {
                code: 500,
                message: "out of stock".into(),
                behavior: pb::ErrorBehavior::Pause as i32,
                ..Default::default()
            });
            let _ = out.send_data(cairn_server::invoker::encode_stream(&[pause]), true);
            return;
        }
    }
}

/// `POST /invocations/{id}/resume` with `bearer`: (status code, body).
async fn resume(node: &RunningNode, bearer: &str, id: &str) -> Result<(u16, Value)> {
    let r = http()
        .post(format!("{}/invocations/{id}/resume", node.url()))
        .bearer_auth(bearer)
        .send()
        .await?;
    let code = r.status().as_u16();
    Ok((code, r.json().await.unwrap_or(Value::Null)))
}

/// **Two quick resumes of a paused `BIDI_STREAM` invocation each open their own stream.**
///
/// 1. A node with a master; tenant `acme` registers a `BIDI_STREAM` SDK that verifies and
///    keeps the "one JWT, one stream" ledger; every turn asks for `PAUSE`.
/// 2. Invoke ⇒ `paused`.
/// 3. Wait for a fresh wall-clock second, then resume twice back to back (both within that
///    second): neither turn grows the journal, so both turns open with the same `Start`
///    (same id, same `retry_count`, same body) inside the same `iat` second.
///
/// Must see: every `/invoke` stream carries a JWT never seen before (the SDK accepts each
/// one), and both resumes answer `200` with the invocation `paused` again — not a refusal
/// from the SDK's ledger turned into a failed resume.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quick_bidi_resumes_each_carry_a_fresh_jwt() -> Result<()> {
    let sdk = PausingBidi::spawn().await?;
    let bucket = common::bucket();
    let node = node_on(&bucket, Some(master())).await?;
    let r = common::within(Duration::from_secs(60), "quick resumes", async {
        let admin = key(&node, "acme", "admin").await?;
        let d = register(&node, &admin, &sdk.url).await?;
        ensure!(
            d.to_string().contains("BIDI_STREAM"),
            "the deployment is not BIDI: {d}"
        );
        let r = http()
            .post(format!("{}/invoke/Warehouse/ship", node.url()))
            .bearer_auth(&admin)
            .body("{}")
            .send()
            .await?;
        let code = r.status();
        let v: Value = r.json().await.unwrap_or(Value::Null);
        ensure!(code == 409, "the SDK asks for PAUSE ⇒ 409: {code} {v}");
        let id = v["id"].as_str().unwrap_or_default().to_string();

        // The start of a fresh second: both resumes below fall inside it.
        let second = cairn_store::entropy::now_ms() / 1000;
        while cairn_store::entropy::now_ms() / 1000 == second {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let before = cairn_store::entropy::now_ms();
        let first = resume(&node, &admin, &id).await?;
        let again = resume(&node, &admin, &id).await?;
        let elapsed = cairn_store::entropy::now_ms() - before;

        let streams = sdk.streams();
        let refused: Vec<_> = streams.iter().filter(|(_, ok)| !ok).collect();
        ensure!(
            refused.is_empty(),
            "the SDK refused {} of {} streams as an already used JWT \
             (resumes took {elapsed} ms; first {first:?}, second {again:?})",
            refused.len(),
            streams.len()
        );
        for (n, (code, v)) in [first, again].into_iter().enumerate() {
            ensure!(
                code == 200 && v["status"] == "paused",
                "resume {}: {code} {v}",
                n + 1
            );
        }
        ensure!(streams.len() >= 3, "streams: {streams:?}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}
