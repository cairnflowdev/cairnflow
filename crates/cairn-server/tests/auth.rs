// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Auth: per-tenant bearer API keys + roles on the ingress port; a separate
//! internal port for `/internal/*`, every request between nodes signed with an
//! HMAC under the cluster secret.
//!
//! Runs the real **binary**: what needs checking is the process's default
//! configuration (auth on, which port serves what), not a hand-built router.

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
    common::binary::Binary,
    reqwest::{Client, StatusCode},
    sha2::{Digest, Sha256},
    std::time::Duration,
};

const ADMIN_KEY: &str = "cf_test_bootstrap_admin_key_0123456789";
const SECRET: &str = "cf_test_cluster_secret_auth_32_bytes";

/// A single-process node, auth on through the bootstrap key, a fixed cluster secret.
fn node() -> Result<Binary> {
    Binary::spawn(
        &[
            "--main-store",
            "memory://",
            "--port",
            "0",
            "--internal-port",
            "0",
        ],
        &[
            ("CAIRNFLOW_ADMIN_KEY", ADMIN_KEY),
            ("CAIRNFLOW_CLUSTER_SECRET", SECRET),
        ],
    )
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// HMAC-SHA256 per RFC 2104 — rewritten here, independent of the server's
/// implementation: the test checks the **protocol**, not that the server agrees with itself.
fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut k = [0u8; 64];
    if key.len() > 64 {
        k[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let pad = |b: u8| k.iter().map(|x| x ^ b).collect::<Vec<u8>>();
    let inner = Sha256::new()
        .chain_update(pad(0x36))
        .chain_update(msg)
        .finalize();
    Sha256::new()
        .chain_update(pad(0x5c))
        .chain_update(inner)
        .finalize()
        .into()
}

/// The two signature headers of an internal request: `method \n path?query \n ts \n
/// hex(sha256(body))`.
fn sign(
    secret: &[u8],
    method: &str,
    path: &str,
    body: &[u8],
    ts: u64,
) -> [(&'static str, String); 2] {
    let msg = format!("{method}\n{path}\n{ts}\n{}", hex(&Sha256::digest(body)));
    [
        ("x-cairn-timestamp", ts.to_string()),
        (
            "x-cairn-signature",
            hex(&hmac_sha256(secret, msg.as_bytes())),
        ),
    ]
}

fn now_s() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// The ingress port does not serve `/internal/*` — not even with a cluster-admin key.
/// Those routes exist only on the internal port, and there they do exist (an
/// unsigned request gets `401`, not `404`).
#[tokio::test]
async fn internal_routes_unreachable_on_ingress_port() -> Result<()> {
    let n = node()?;
    let http = Client::new();
    for (method, path) in [
        ("POST", "/internal/inbound"),
        ("GET", "/internal/deployments/usage"),
        ("POST", "/internal/ranges/0/resign"),
        ("POST", "/internal/session/changed"),
    ] {
        let m = reqwest::Method::from_bytes(method.as_bytes())?;
        let r = http
            .request(m.clone(), format!("{}{path}", n.url))
            .bearer_auth(ADMIN_KEY)
            .send()
            .await?;
        ensure!(
            r.status() == StatusCode::NOT_FOUND,
            "{method} {path} on the ingress port returned {}, expected 404",
            r.status()
        );
        let r = http
            .request(m, format!("{}{path}", n.internal_url))
            .send()
            .await?;
        ensure!(
            r.status() == StatusCode::UNAUTHORIZED,
            "{method} {path} unsigned on the internal port returned {}, expected 401",
            r.status()
        );
    }
    // The internal port does not serve the customer API.
    let r = http
        .get(format!("{}/invocations", n.internal_url))
        .bearer_auth(ADMIN_KEY)
        .send()
        .await?;
    ensure!(
        r.status() == StatusCode::NOT_FOUND || r.status() == StatusCode::UNAUTHORIZED,
        "GET /invocations on the internal port returned {}",
        r.status()
    );
    Ok(())
}

/// A cluster secret from `CAIRNFLOW_CLUSTER_SECRET` shorter than 32 bytes ⇒ the
/// node refuses to start, and says why (found in a security review). Before the
/// fix `CAIRNFLOW_CLUSTER_SECRET=dev` was accepted, while the bucket read path
/// demanded ≥ 16 bytes.
#[tokio::test]
async fn short_env_cluster_secret_refuses_to_start() -> Result<()> {
    let (exit, out) = Binary::run_to_exit(
        &[
            "--main-store",
            "memory://",
            "--port",
            "0",
            "--internal-port",
            "0",
        ],
        &[
            ("CAIRNFLOW_ADMIN_KEY", ADMIN_KEY),
            // 31 byte.
            (
                "CAIRNFLOW_CLUSTER_SECRET",
                "cf_test_secret_shorter_than_32b",
            ),
        ],
        Duration::from_secs(30),
    )?;
    ensure!(
        !exit.success() && out.contains("CAIRNFLOW_CLUSTER_SECRET"),
        "31-byte secret: the binary must refuse to start and name the variable, it exited {exit}:\n{out}"
    );
    Ok(())
}

/// No key, a wrong key, or a broken header ⇒ `401` on every route except `/health`.
/// No key configured and no `--no-auth` ⇒ the binary refuses to start.
#[tokio::test]
async fn request_without_key_is_401() -> Result<()> {
    let (exit, out) = Binary::run_to_exit(
        &[
            "--main-store",
            "memory://",
            "--port",
            "0",
            "--internal-port",
            "0",
        ],
        &[],
        Duration::from_secs(30),
    )?;
    ensure!(
        !exit.success(),
        "no key, no --no-auth: the binary must refuse to start, it exited {exit}:\n{out}"
    );

    let n = node()?;
    let http = Client::new();
    let probes: [(&str, &str); 8] = [
        ("GET", "/invocations"),
        ("POST", "/invoke/Svc/run"),
        ("GET", "/cluster"),
        ("GET", "/deployments"),
        ("POST", "/admin/tick"),
        ("GET", "/rules"),
        ("GET", "/admin/keys"),
        ("GET", "/no-such-route"),
    ];
    for (method, path) in probes {
        let m = reqwest::Method::from_bytes(method.as_bytes())?;
        let url = format!("{}{path}", n.url);
        for (what, req) in [
            ("no key", http.request(m.clone(), &url)),
            (
                "wrong key",
                http.request(m.clone(), &url)
                    .bearer_auth("cf_test_not_a_key"),
            ),
            (
                "broken header",
                http.request(m.clone(), &url)
                    .header("authorization", ADMIN_KEY),
            ),
        ] {
            let r = req.send().await?;
            ensure!(
                r.status() == StatusCode::UNAUTHORIZED,
                "{method} {path} ({what}) returned {}, expected 401",
                r.status()
            );
        }
    }
    let r = http.get(format!("{}/health", n.url)).send().await?;
    ensure!(
        r.status() == StatusCode::OK,
        "/health must be open: {}",
        r.status()
    );
    let r = http
        .get(format!("{}/cluster", n.url))
        .bearer_auth(ADMIN_KEY)
        .send()
        .await?;
    ensure!(
        r.status() == StatusCode::OK,
        "bootstrap key reads /cluster: {}",
        r.status()
    );
    Ok(())
}

/// An `invoke` key can call services but cannot touch the admin API (`403`);
/// an `admin` key administers its tenant but cannot touch the cluster API. A key
/// created through `/admin/keys` reveals its secret only once, and deleting a key
/// means the key stops working.
#[tokio::test]
async fn invoke_key_cannot_call_admin() -> Result<()> {
    let n = node()?;
    let http = Client::new();
    let create = |role: &'static str| {
        let http = http.clone();
        let url = n.url.clone();
        async move {
            let r = http
                .post(format!("{url}/admin/keys"))
                .bearer_auth(ADMIN_KEY)
                .json(&serde_json::json!({ "tenant": "default", "role": role }))
                .send()
                .await?;
            ensure!(
                r.status() == StatusCode::OK,
                "create {role} key: {}",
                r.status()
            );
            let v: serde_json::Value = r.json().await?;
            Ok::<_, anyhow::Error>((
                v["id"].as_str().unwrap_or_default().to_string(),
                v["secret"].as_str().unwrap_or_default().to_string(),
            ))
        }
    };
    let (invoke_id, invoke_key) = create("invoke").await?;
    let (_, admin_key) = create("admin").await?;
    ensure!(invoke_key.starts_with("cf_"), "key secret: {invoke_key:?}");

    let status = |method: &'static str, path: &str, key: String| {
        let http = http.clone();
        let url = format!("{}{path}", n.url);
        async move {
            let m = reqwest::Method::from_bytes(method.as_bytes())?;
            Ok::<_, anyhow::Error>(http.request(m, url).bearer_auth(key).send().await?.status())
        }
    };
    // invoke: passes the auth layer on the call path (service does not exist ⇒ not
    // 401/403), blocked on everything administrative.
    let s = status("POST", "/invoke/NoSuchService/run", invoke_key.clone()).await?;
    ensure!(
        s != StatusCode::UNAUTHORIZED && s != StatusCode::FORBIDDEN,
        "invoke key calling /invoke blocked: {s}"
    );
    for (method, path) in [
        ("GET", "/deployments"),
        ("POST", "/deployments"),
        ("GET", "/invocations"),
        ("GET", "/cluster"),
        ("POST", "/admin/tick"),
        ("GET", "/admin/keys"),
        ("POST", "/ranges/merge"),
    ] {
        let s = status(method, path, invoke_key.clone()).await?;
        ensure!(
            s == StatusCode::FORBIDDEN,
            "invoke key: {method} {path} returned {s}, expected 403"
        );
    }
    // A hard cancel stays `Admin`; signal and cooperative cancel: see
    // `invoke_key_signals_and_cancels_cooperatively`.
    const ID: &str = "/invocations/0123456789abcdef0123456789abcdef0123456789abcdef";
    let s = status("POST", &format!("{ID}/cancel"), invoke_key.clone()).await?;
    ensure!(
        s == StatusCode::FORBIDDEN,
        "invoke key: hard cancel returned {s}, expected 403"
    );
    // admin: may administer the tenant, not the cluster.
    let s = status("GET", "/deployments", admin_key.clone()).await?;
    ensure!(s == StatusCode::OK, "admin key GET /deployments: {s}");
    for (method, path) in [
        ("GET", "/cluster"),
        ("POST", "/admin/tick"),
        ("GET", "/admin/keys"),
    ] {
        let s = status(method, path, admin_key.clone()).await?;
        ensure!(
            s == StatusCode::FORBIDDEN,
            "admin key: {method} {path} returned {s}, expected 403"
        );
    }

    // The key list never reveals secrets.
    let listed = http
        .get(format!("{}/admin/keys", n.url))
        .bearer_auth(ADMIN_KEY)
        .send()
        .await?
        .text()
        .await?;
    ensure!(
        !listed.contains(&invoke_key),
        "GET /admin/keys reveals a secret: {listed}"
    );
    ensure!(
        listed.contains(&invoke_id),
        "GET /admin/keys lacks the key just created: {listed}"
    );

    // Deleting a key ⇒ the key stops working.
    let r = http
        .delete(format!("{}/admin/keys/{invoke_id}", n.url))
        .bearer_auth(ADMIN_KEY)
        .send()
        .await?;
    ensure!(r.status().is_success(), "delete key: {}", r.status());
    let s = status("POST", "/invoke/NoSuchService/run", invoke_key).await?;
    ensure!(
        s == StatusCode::UNAUTHORIZED,
        "deleted key still works: {s}"
    );
    Ok(())
}

/// **Signal and cooperative cancel with an `invoke` key**: an `invoke` key that
/// can resolve an awakeable (signal by index) can also send a signal by name,
/// and can stop its own invocation with `cancel?mode=signal` (the Vercel
/// transport needs exactly that to stop a chat). A hard cancel stays `admin`.
///
/// Must see: `signal` and `cancel?mode=signal` with an `invoke` key pass the auth
/// layer (missing id ⇒ the handler's own `404`, not `403`); `cancel` without
/// `mode` is still `403`; another tenant's `invoke` key ⇒ `404`.
#[tokio::test]
async fn invoke_key_signals_and_cancels_cooperatively() -> Result<()> {
    let n = node()?;
    let http = Client::new();
    let key_of = |tenant: &'static str| {
        let http = http.clone();
        let url = n.url.clone();
        async move {
            let r = http
                .post(format!("{url}/admin/keys"))
                .bearer_auth(ADMIN_KEY)
                .json(&serde_json::json!({ "tenant": tenant, "role": "invoke" }))
                .send()
                .await?;
            let v: serde_json::Value = r.json().await?;
            Ok::<_, anyhow::Error>(v["secret"].as_str().unwrap_or_default().to_string())
        }
    };
    let key = key_of("default").await?;
    const ID: &str = "/invocations/0123456789abcdef0123456789abcdef0123456789abcdef";
    let post = |path: String, key: String, body: serde_json::Value| {
        let http = http.clone();
        let url = format!("{}{path}", n.url);
        async move {
            Ok::<_, anyhow::Error>(
                http.post(url)
                    .bearer_auth(key)
                    .json(&body)
                    .send()
                    .await?
                    .status(),
            )
        }
    };
    let s = post(
        format!("{ID}/signal"),
        key.clone(),
        serde_json::json!({ "signal": "approve" }),
    )
    .await?;
    ensure!(s == StatusCode::NOT_FOUND, "signal with an invoke key: {s}");
    let s = post(
        format!("{ID}/cancel?mode=signal"),
        key.clone(),
        serde_json::json!({}),
    )
    .await?;
    ensure!(
        s == StatusCode::NOT_FOUND,
        "cooperative cancel with an invoke key: {s}"
    );
    let s = post(format!("{ID}/cancel"), key.clone(), serde_json::json!({})).await?;
    ensure!(s == StatusCode::FORBIDDEN, "hard cancel, invoke key: {s}");
    let s = post(format!("{ID}/cancel?mode=hard"), key, serde_json::json!({})).await?;
    ensure!(
        s == StatusCode::FORBIDDEN,
        "an unknown mode must not lower the role: {s}"
    );
    Ok(())
}

/// **A repeated `mode` does not open hard cancel to an `invoke` key** (found in a
/// security review).
///
/// 1. An `invoke` key sends `POST /invocations/{id}/cancel?mode=signal&mode=hard`.
/// 2. The auth layer sees a `mode=signal` pair ⇒ (before the fix) lowers the role
///    to `invoke`; the handler reads `Query<HashMap>`, the last value wins ⇒ `hard`.
/// 3. The same with `POST /invocations/batch/cancel?mode=signal…` — up to 1 000 ids
///    at once.
///
/// Must see: all three requests ⇒ `403` at the auth layer (missing id ⇒ if it got
/// past auth it would be the handler's `404`/`200`, not `403`).
#[tokio::test]
async fn invoke_key_cannot_hard_cancel_with_repeated_mode() -> Result<()> {
    let n = node()?;
    let http = Client::new();
    let r = http
        .post(format!("{}/admin/keys", n.url))
        .bearer_auth(ADMIN_KEY)
        .json(&serde_json::json!({ "tenant": "default", "role": "invoke" }))
        .send()
        .await?;
    let v: serde_json::Value = r.json().await?;
    let key = v["secret"].as_str().unwrap_or_default().to_string();
    const ID: &str = "0123456789abcdef0123456789abcdef0123456789abcdef";
    for (path, body) in [
        (
            format!("/invocations/{ID}/cancel?mode=signal&mode=hard"),
            serde_json::json!({}),
        ),
        (
            format!("/invocations/{ID}/cancel?mode=hard&mode=signal&mode=hard"),
            serde_json::json!({}),
        ),
        (
            "/invocations/batch/cancel?mode=signal&mode=hard".to_string(),
            serde_json::json!({ "ids": [ID] }),
        ),
    ] {
        let s = http
            .post(format!("{}{path}", n.url))
            .bearer_auth(&key)
            .json(&body)
            .send()
            .await?
            .status();
        ensure!(s == StatusCode::FORBIDDEN, "{path} with an invoke key: {s}");
    }
    Ok(())
}

/// The internal port accepts only requests signed with the cluster secret: unsigned,
/// signed with another secret, signed correctly but with an edited body, or signed
/// correctly but too old ⇒ `401`. Signed correctly passes the auth layer (garbage
/// body ⇒ the handler's own `400`, not `401`).
#[tokio::test]
async fn forged_inbound_without_cluster_secret_is_rejected() -> Result<()> {
    let n = node()?;
    let http = Client::new();
    let path = "/internal/inbound?range=0&table=1";
    let url = format!("{}{path}", n.internal_url);
    let body = b"not-an-interrange-message".to_vec();
    let send = |headers: Option<[(&'static str, String); 2]>, body: Vec<u8>| {
        let mut req = http.post(&url).body(body);
        if let Some(hs) = headers {
            for (k, v) in hs {
                req = req.header(k, v);
            }
        }
        async move { Ok::<_, anyhow::Error>(req.send().await?.status()) }
    };

    let s = send(None, body.clone()).await?;
    ensure!(s == StatusCode::UNAUTHORIZED, "unsigned: {s}");
    let forged = sign(
        b"cf_test_not_the_cluster_secret",
        "POST",
        path,
        &body,
        now_s(),
    );
    let s = send(Some(forged), body.clone()).await?;
    ensure!(s == StatusCode::UNAUTHORIZED, "foreign secret: {s}");
    let signed = sign(SECRET.as_bytes(), "POST", path, &body, now_s());
    let s = send(Some(signed), b"tampered-body".to_vec()).await?;
    ensure!(s == StatusCode::UNAUTHORIZED, "edited body: {s}");
    let stale = sign(SECRET.as_bytes(), "POST", path, &body, now_s() - 600);
    let s = send(Some(stale), body.clone()).await?;
    ensure!(s == StatusCode::UNAUTHORIZED, "signature 10 min old: {s}");

    let good = sign(SECRET.as_bytes(), "POST", path, &body, now_s());
    let s = send(Some(good), body).await?;
    ensure!(
        s == StatusCode::BAD_REQUEST,
        "signed correctly, garbage body: must reach the handler (400), returned {s}"
    );
    Ok(())
}

/// Two in-process nodes, **auth on**, the cluster secret taken from the bucket (the
/// first node creates it, the second reads it): every path between nodes still
/// works — `ctx.call` into the other node's range (a signed inter-range message),
/// merging two ranges of two nodes (a signed yield request), splitting the merged
/// range again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authed_cluster_still_talks_and_moves_ranges() -> Result<()> {
    use common::{
        fake_sdk::{FakeSdk, Step},
        load_of, single_leaders, start_cluster_node_tweaked, wait_probe,
    };
    let shared = common::bucket();
    let ttl = Duration::from_secs(1);
    let authed = |cfg: &mut cairn_server::node::NodeConfig| {
        cfg.auth = cairn_server::auth::AuthConfig {
            required: true,
            admin_key: Some(ADMIN_KEY.into()),
            cluster_secret: None,
            metrics_key: None,
            admin_key_sha256: None,
            provision_key_sha256: None,
        };
    };
    let nodes = vec![
        start_cluster_node_tweaked(&shared, "node-alpha", 2, ttl, authed).await?,
        start_cluster_node_tweaked(&shared, "node-beta", 2, ttl, authed).await?,
    ];
    let refs: Vec<_> = nodes.iter().collect();
    let probe = wait_probe(
        &refs,
        Duration::from_secs(20),
        "exactly one range per node",
        |p| single_leaders(p).is_some_and(|m| load_of(&m, 2) == [1, 1]),
    )
    .await?;
    let leaders = single_leaders(&probe).expect("balanced");
    let r_a = *leaders
        .iter()
        .find(|(_, n)| **n == 0)
        .expect("alpha holds a range")
        .0;
    let r_b = *leaders
        .iter()
        .find(|(_, n)| **n == 1)
        .expect("beta holds a range")
        .0;
    let (a, b) = (&nodes[0].node, &nodes[1].node);

    let http = Client::new();
    let post = |url: String, body: serde_json::Value| {
        let http = http.clone();
        async move {
            let r = http
                .post(url)
                .bearer_auth(ADMIN_KEY)
                .json(&body)
                .send()
                .await?;
            let code = r.status();
            let text = r.text().await?;
            Ok::<_, anyhow::Error>((
                code,
                serde_json::from_str::<serde_json::Value>(&text)
                    .unwrap_or(serde_json::Value::String(text)),
            ))
        }
    };
    let callee_key = common::key_on_partition(b, "Callee", r_b, "callee");
    let caller_key = common::key_on_partition(a, "Caller", r_a, "caller");
    let sdk = FakeSdk::builder()
        .object(
            "Caller",
            "run",
            vec![
                Step::call("Callee", "echo", Some(&callee_key)),
                Step::EchoCallResult,
            ],
        )
        .object("Callee", "echo", vec![Step::output("from-beta")])
        .spawn()
        .await?;

    let r = common::within(Duration::from_secs(60), "cluster with auth", async {
        let (code, body) = post(
            format!("{}/deployments", a.url()),
            serde_json::json!({ "uri": sdk.url }),
        )
        .await?;
        ensure!(code == StatusCode::OK, "register: {code} {body}");
        // No key ⇒ 401, the cluster API included.
        let r = http
            .post(format!("{}/ranges/merge?left=0&right=1", a.url()))
            .send()
            .await?;
        ensure!(
            r.status() == StatusCode::UNAUTHORIZED,
            "merge without a key: {}",
            r.status()
        );

        // An inter-range message A → B and the reply B → A, signed with the cluster secret.
        let call = |tag: &'static str| {
            let url = format!("{}/invoke/Caller/run?key={caller_key}", a.url());
            let http = http.clone();
            async move {
                let mut last = serde_json::Value::Null;
                for _ in 0..100 {
                    let r = http
                        .post(&url)
                        .bearer_auth(ADMIN_KEY)
                        .body("x")
                        .send()
                        .await?;
                    let code = r.status();
                    let body: serde_json::Value = r.json().await.unwrap_or_default();
                    if code == StatusCode::SERVICE_UNAVAILABLE {
                        last = body;
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                    ensure!(
                        code == StatusCode::OK && body["output"] == "from-beta",
                        "{tag}: {code} {body}"
                    );
                    return Ok(());
                }
                Err(anyhow!("{tag}: still 503 after 10 s: {last}"))
            }
        };
        call("before").await?;

        // Merge two ranges of two nodes: A asks B to yield through the internal port.
        let (left, right) = if r_a < r_b { (r_a, r_b) } else { (r_b, r_a) };
        let (code, view) = post(
            format!("{}/ranges/merge?left={left}&right={right}", a.url()),
            serde_json::Value::Null,
        )
        .await?;
        ensure!(code == StatusCode::OK, "merge: {code} {view}");
        let merged = view["ranges"].as_array().ok_or_else(|| anyhow!("{view}"))?;
        ensure!(merged.len() == 1, "{view}");
        let id = merged[0]["range"].as_u64().unwrap_or(0);
        // `address` in `/cluster` is the **ingress** port — clients send there.
        let addr = merged[0]["address"].as_str().unwrap_or_default();
        ensure!(
            addr == a.url() || addr == b.url(),
            "the leader's address must be the ingress port: {addr} (a={}, b={})",
            a.url(),
            b.url()
        );

        // Split again.
        let (code, view) = post(
            format!("{}/ranges/{id}/split", a.url()),
            serde_json::Value::Null,
        )
        .await?;
        ensure!(code == StatusCode::OK, "split: {code} {view}");
        ensure!(view["ranges"].as_array().map(Vec::len) == Some(2), "{view}");
        Ok(())
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// On a directory store (`--data-dir`, `LocalFileSystem` — no CAS on the etag):
/// the key registry and the tenant registry must be writable **many times**. The
/// lack of CAS already blocks forming a cluster, so overwriting here is safe —
/// the same rule as the rule book and the catalog. The bug let `--data-dir`
/// create exactly one key and no more.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn key_and_tenant_books_work_on_local_dir_store() -> anyhow::Result<()> {
    use cairn_server::node::{self, NodeConfig, NodeMode};
    let dir = std::env::temp_dir().join(format!("cairnflow-auth-local-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;
    let stores = cairn_store::ObjectStores::single(cairn_store::StoreLocation::parse(&format!(
        "file://{}",
        dir.display()
    ))?);
    let mut cfg = NodeConfig::new(stores, NodeMode::single(1));
    let admin = "cf_test_local_dir_bootstrap_key_000000001";
    cfg.auth = cairn_server::auth::AuthConfig {
        required: true,
        admin_key: Some(admin.into()),
        cluster_secret: Some("cf_test_cluster_secret_key_and_tenant_32b".into()),
        metrics_key: None,
        admin_key_sha256: None,
        provision_key_sha256: None,
    };
    let node = node::start(cfg).await.map_err(|e| anyhow::anyhow!("{e}"))?;
    let url = node.url();
    let http = reqwest::Client::new();
    let r = async {
        for tenant in ["acme", "globex", "acme"] {
            let resp = http
                .post(format!("{url}/admin/keys"))
                .bearer_auth(admin)
                .json(&serde_json::json!({ "tenant": tenant, "role": "admin" }))
                .send()
                .await?;
            anyhow::ensure!(
                resp.status().is_success(),
                "key {tenant}: {}",
                resp.text().await?
            );
        }
        for tenant in ["acme", "globex"] {
            let resp = http
                .put(format!("{url}/admin/tenants/{tenant}"))
                .bearer_auth(admin)
                .json(&serde_json::json!({ "hashChain": true }))
                .send()
                .await?;
            anyhow::ensure!(
                resp.status().is_success(),
                "tenant {tenant}: {}",
                resp.text().await?
            );
        }
        Ok(())
    }
    .await;
    node.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
    r
}

/// A binary node with auth on, extra flags `extra`.
fn node_with(extra: &[&str]) -> Result<Binary> {
    let mut args = vec![
        "--main-store",
        "memory://",
        "--port",
        "0",
        "--internal-port",
        "0",
        "--ui-cookie-secure",
        "off",
    ];
    args.extend_from_slice(extra);
    Binary::spawn(
        &args,
        &[
            ("CAIRNFLOW_ADMIN_KEY", ADMIN_KEY),
            ("CAIRNFLOW_CLUSTER_SECRET", SECRET),
        ],
    )
}

/// `POST /ui/session` through a "proxy": `headers` set verbatim. Returns (code,
/// `Set-Cookie`).
async fn proxied_login(
    n: &Binary,
    key: &str,
    headers: &[(&str, &str)],
) -> Result<(StatusCode, Option<String>)> {
    let mut req = Client::new()
        .post(format!("{}/ui/session", n.url))
        .json(&serde_json::json!({ "key": key }));
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    let r = req.send().await?;
    let set = r
        .headers()
        .get(reqwest::header::SET_COOKIE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    Ok((r.status(), set))
}

/// `X-Forwarded-Host`/`X-Forwarded-Proto` (and `X-Forwarded-For` for the per-IP
/// login limit) are trusted only when the peer is inside `--trust-proxy <cidr>`.
///
/// Order:
/// 1. Node `--trust-proxy 127.0.0.0/8`. A TLS proxy rewrites `Host` to the inner
///    address, keeps the real host in `X-Forwarded-Host`, `X-Forwarded-Proto: https`.
///    An old browser (no `Sec-Fetch-Site`) sends the real host's `Origin`.
/// 2. The same request with an `Origin` of the wrong scheme (`http://`).
/// 3. Ten wrong-key attempts from client `198.51.100.7` (through `X-Forwarded-For`),
///    then one from `198.51.100.8`.
/// 4. A node without the flag, and a node `--trust-proxy 10.0.0.0/8` (the loopback
///    peer is outside the range): the same request 1.
///
/// Must see: (1) `200`, the cookie carries `Secure` despite `--ui-cookie-secure off`;
/// (2) `403`; (3) the first client `429`, the second client still checked
/// (`401`); (4) `403` — `X-Forwarded-*` headers from an untrusted peer are ignored.
#[tokio::test]
async fn forwarded_headers_only_from_trusted_proxy() -> Result<()> {
    let behind_proxy = [
        ("host", "10.0.0.5:8080"),
        ("x-forwarded-host", "console.cairnflow.test"),
        ("x-forwarded-proto", "https"),
        ("origin", "https://console.cairnflow.test"),
    ];
    // (1)
    let n = node_with(&["--trust-proxy", "127.0.0.0/8"])?;
    let (code, set) = proxied_login(&n, ADMIN_KEY, &behind_proxy).await?;
    let set = set.unwrap_or_default();
    ensure!(code == StatusCode::OK, "behind a trusted proxy: {code}");
    ensure!(
        set.contains("; Secure"),
        "X-Forwarded-Proto https but the cookie is not Secure: {set}"
    );
    // (2)
    let mut wrong_scheme = behind_proxy;
    wrong_scheme[3] = ("origin", "http://console.cairnflow.test");
    let (code, _) = proxied_login(&n, ADMIN_KEY, &wrong_scheme).await?;
    ensure!(code == StatusCode::FORBIDDEN, "Origin wrong scheme: {code}");
    // (3)
    let from = |ip: &'static str| {
        let mut h = behind_proxy.to_vec();
        h.push(("x-forwarded-for", ip));
        h
    };
    for i in 0..10 {
        let key = format!("cf_{i:02x}{:046x}", 9);
        let (code, _) = proxied_login(&n, &key, &from("198.51.100.7")).await?;
        ensure!(code == StatusCode::UNAUTHORIZED, "bad attempt {i}: {code}");
    }
    let (code, _) = proxied_login(&n, "cf_ffffffff", &from("198.51.100.7")).await?;
    ensure!(
        code == StatusCode::TOO_MANY_REQUESTS,
        "client out of attempts: {code}"
    );
    let (code, _) = proxied_login(&n, "cf_eeeeeeee", &from("198.51.100.8")).await?;
    ensure!(
        code == StatusCode::UNAUTHORIZED,
        "another client behind the same proxy blocked by the proxy's IP: {code}"
    );
    drop(n);
    // (4)
    for extra in [&[][..], &["--trust-proxy", "10.0.0.0/8"][..]] {
        let n = node_with(extra)?;
        let (code, _) = proxied_login(&n, ADMIN_KEY, &behind_proxy).await?;
        ensure!(
            code == StatusCode::FORBIDDEN,
            "{extra:?}: X-Forwarded-* from an untrusted peer must be ignored: {code}"
        );
    }
    Ok(())
}

fn sha_hex(k: &str) -> String {
    hex(&Sha256::digest(k.as_bytes()))
}

const PROVISION_KEY: &str = "cf_test_control_plane_provision_key_0123456789";
const BREAK_GLASS_KEY: &str = "cf_test_break_glass_cluster_admin_0123456789";

/// A cell with no cluster-admin key in its env: the node knows only the **hash**
/// of the `provision` key and the hash of the break-glass cluster-admin key.
///
/// Order:
/// 1. Build a node with only `CAIRNFLOW_PROVISION_KEY_SHA256` (no
///    `CAIRNFLOW_ADMIN_KEY`, empty registry).
/// 2. The `provision` key calls the control-plane paths, mints an `admin` key for
///    a tenant; calls the cluster and run-data paths; logs in to the console.
/// 3. Build a node with `CAIRNFLOW_ADMIN_KEY_SHA256` too; the operator uses the
///    plaintext from the break-glass vault.
///
/// Must see: (1) the node starts; (2) `whoami` = `provision`/`default`,
/// `/admin/cluster-version` and `POST /admin/keys` (role `admin`) `200`,
/// `/cluster` and `/invocations` `403`, `/ui/session` `401`, the hash string itself
/// as Bearer `401`; (3) the plaintext is cluster-admin (`/cluster` `200`) and can
/// open the console.
#[tokio::test]
async fn hashed_env_keys_run_a_cell_without_admin_plaintext() -> Result<()> {
    let base = [
        "--main-store",
        "memory://",
        "--port",
        "0",
        "--internal-port",
        "0",
        "--ui-cookie-secure",
        "off",
    ];
    let prov_hash = sha_hex(PROVISION_KEY);
    let n = Binary::spawn(
        &base,
        &[
            ("CAIRNFLOW_PROVISION_KEY_SHA256", prov_hash.as_str()),
            ("CAIRNFLOW_CLUSTER_SECRET", SECRET),
        ],
    )?;
    let http = Client::new();
    let get = |path: &str, key: &str| http.get(format!("{}{path}", n.url)).bearer_auth(key).send();
    let who: serde_json::Value = get("/auth/whoami", PROVISION_KEY).await?.json().await?;
    ensure!(
        who["role"] == "provision" && who["tenant"] == "default",
        "whoami of the provision key by hash: {who}"
    );
    for (path, want) in [
        ("/admin/cluster-version", StatusCode::OK),
        ("/admin/tenants", StatusCode::OK),
        ("/cluster", StatusCode::FORBIDDEN),
        ("/invocations", StatusCode::FORBIDDEN),
        ("/metrics", StatusCode::FORBIDDEN),
    ] {
        let got = get(path, PROVISION_KEY).await?.status();
        ensure!(got == want, "provision GET {path}: {got}, expected {want}");
    }
    let r = get("/admin/cluster-version", &prov_hash).await?.status();
    ensure!(
        r == StatusCode::UNAUTHORIZED,
        "the hash string itself as Bearer: {r}"
    );
    let made = http
        .post(format!("{}/admin/keys", n.url))
        .bearer_auth(PROVISION_KEY)
        .json(&serde_json::json!({ "tenant": "acme", "role": "admin" }))
        .send()
        .await?;
    ensure!(
        made.status() == StatusCode::OK || made.status() == StatusCode::CREATED,
        "provision mints an admin key: {}",
        made.text().await?
    );
    for role in ["cluster-admin", "provision"] {
        let r = http
            .post(format!("{}/admin/keys", n.url))
            .bearer_auth(PROVISION_KEY)
            .json(&serde_json::json!({ "tenant": "default", "role": role }))
            .send()
            .await?
            .status();
        ensure!(r == StatusCode::FORBIDDEN, "provision mints {role}: {r}");
    }
    let (code, cookie) = proxied_login(&n, PROVISION_KEY, &[("origin", n.url.as_str())]).await?;
    ensure!(
        code == StatusCode::UNAUTHORIZED && cookie.is_none(),
        "provision key logs in to the console: {code} {cookie:?}"
    );
    drop(n);

    let n = Binary::spawn(
        &base,
        &[
            ("CAIRNFLOW_PROVISION_KEY_SHA256", prov_hash.as_str()),
            ("CAIRNFLOW_ADMIN_KEY_SHA256", &sha_hex(BREAK_GLASS_KEY)),
            ("CAIRNFLOW_CLUSTER_SECRET", SECRET),
        ],
    )?;
    let r = http
        .get(format!("{}/cluster", n.url))
        .bearer_auth(BREAK_GLASS_KEY)
        .send()
        .await?
        .status();
    ensure!(r == StatusCode::OK, "break-glass GET /cluster: {r}");
    let (code, cookie) = proxied_login(&n, BREAK_GLASS_KEY, &[("origin", n.url.as_str())]).await?;
    ensure!(
        code == StatusCode::OK && cookie.is_some(),
        "break-glass logs in to the console: {code}"
    );
    Ok(())
}

/// A broken hash, two roles with the same secret, or both the plaintext and the
/// hash of cluster-admin ⇒ the binary refuses to start and names the variable —
/// rather than silently running with fewer keys than the operator thinks.
#[tokio::test]
async fn bad_env_key_hash_refuses_to_start() -> Result<()> {
    let args = [
        "--main-store",
        "memory://",
        "--port",
        "0",
        "--internal-port",
        "0",
    ];
    let prov = sha_hex(PROVISION_KEY);
    let boot = sha_hex(ADMIN_KEY);
    /// (case, environment variable, text the error must contain).
    type Case<'a> = (&'a str, Vec<(&'a str, &'a str)>, &'a str);
    let cases: [Case; 4] = [
        (
            "63-digit hash",
            vec![("CAIRNFLOW_PROVISION_KEY_SHA256", &prov[..63])],
            "CAIRNFLOW_PROVISION_KEY_SHA256",
        ),
        (
            "plaintext put in the hash variable by mistake",
            vec![("CAIRNFLOW_ADMIN_KEY_SHA256", ADMIN_KEY)],
            "CAIRNFLOW_ADMIN_KEY_SHA256",
        ),
        (
            "provision equals the bootstrap key",
            vec![
                ("CAIRNFLOW_ADMIN_KEY", ADMIN_KEY),
                ("CAIRNFLOW_PROVISION_KEY_SHA256", boot.as_str()),
            ],
            "are the same API key",
        ),
        (
            "both plaintext and hash of cluster-admin",
            vec![
                ("CAIRNFLOW_ADMIN_KEY", ADMIN_KEY),
                ("CAIRNFLOW_ADMIN_KEY_SHA256", boot.as_str()),
            ],
            "pick one",
        ),
    ];
    for (what, mut envs, needle) in cases {
        envs.push(("CAIRNFLOW_CLUSTER_SECRET", SECRET));
        let (exit, out) = Binary::run_to_exit(&args, &envs, Duration::from_secs(30))?;
        ensure!(
            !exit.success() && out.contains(needle),
            "{what}: the binary must refuse to start and name {needle:?}, it exited {exit}:\n{out}"
        );
        ensure!(
            !out.contains(ADMIN_KEY) && !out.contains(PROVISION_KEY),
            "{what}: output reveals a secret:\n{out}"
        );
    }
    Ok(())
}

/// **A key registry changing constantly underneath ⇒ `503 keys_busy`, not `500`**
/// (seen in a benchmark: 16 parallel `POST /admin/keys` streams got `500` once
/// the CAS loop on the registry ran out of attempts).
///
/// 1. A cluster node with auth; every PUT to `auth/keys` fails with `Precondition`
///    (someone else just wrote — the read-back is not ours).
/// 2. `POST /admin/keys` and `DELETE /admin/keys/{id}`.
///
/// Must see: `503`, `code: keys_busy`, a `Retry-After` header, an English body
/// carrying no object path; with the fault rule removed the same commands pass
/// (`200`/`204`). On the old code: `500` with a masked body.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn contended_key_registry_is_503_keys_busy() -> Result<()> {
    use cairn_store::testkit::{Action, OpSet, PathMatch, Rule};
    let shared = common::bucket();
    let n = common::start_cluster_node_tweaked(&shared, "n1", 1, Duration::from_secs(2), |cfg| {
        cfg.auth = cairn_server::auth::AuthConfig {
            required: true,
            admin_key: Some(ADMIN_KEY.into()),
            cluster_secret: Some(SECRET.into()),
            metrics_key: None,
            admin_key_sha256: None,
            provision_key_sha256: None,
        };
    })
    .await?;
    let url = n.node.url();
    let http = Client::new();
    let create = || {
        http.post(format!("{url}/admin/keys"))
            .bearer_auth(ADMIN_KEY)
            .json(&serde_json::json!({ "tenant": "acme", "role": "invoke" }))
            .send()
    };
    // An existing key to try revoking.
    let r = create().await?;
    ensure!(r.status() == StatusCode::OK, "first key: {}", r.status());
    let id = r.json::<serde_json::Value>().await?["id"]
        .as_str()
        .ok_or_else(|| anyhow!("no id"))?
        .to_string();
    let rule = n.store.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains("auth/keys".into()),
        Action::Precondition,
    ));
    let delete = || {
        http.delete(format!("{url}/admin/keys/{id}"))
            .bearer_auth(ADMIN_KEY)
            .send()
    };
    for (what, r) in [("create", create().await?), ("revoke", delete().await?)] {
        let code = r.status();
        let retry_after = r.headers().get("retry-after").cloned();
        let text = r.text().await?;
        let v: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
        ensure!(
            code == StatusCode::SERVICE_UNAVAILABLE && v["code"] == "keys_busy",
            "{what} while the key registry keeps changing: {code} {text}"
        );
        ensure!(retry_after.is_some(), "{what}: no Retry-After");
        ensure!(
            !text.contains("auth/keys"),
            "{what}: body reveals an object path: {text}"
        );
    }
    n.store.remove_rule(rule);
    let r = create().await?;
    ensure!(r.status() == StatusCode::OK, "create again: {}", r.status());
    let r = delete().await?;
    ensure!(
        r.status() == StatusCode::NO_CONTENT || r.status() == StatusCode::ACCEPTED,
        "revoke again: {}",
        r.status()
    );
    Ok(())
}
