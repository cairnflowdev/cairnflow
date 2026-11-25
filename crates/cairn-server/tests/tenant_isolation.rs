// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The tenant isolation suite: tenant `alpha` holds its `admin` and `invoke`
//! keys, knows (guesses correctly) every identifier of tenant `bravo` —
//! invocation ids, deployment ids, service names, workflow keys, schedule
//! names, stream names, artifact shas, awakeable ids — and calls **every route**
//! of the ingress port with those identifiers. Must see:
//!
//! - no response contains `bravo`'s data ([`Fixture::markers`]: output, input,
//!   state, stream records, artifact content, schedule input, SDK address);
//! - no route returns `5xx`; cluster routes return `403`; per-tenant routes
//!   return exactly the code pinned in [`MATRIX`] (mostly `404`, as if it did
//!   not exist);
//! - `bravo`'s state does not change ([`Snapshot`]): a suspended invocation
//!   stays suspended, a completed one is not cleaned up, schedules are not
//!   paused or deleted, deployments and revisions stay the same;
//! - every `sys_*` table of `/query` (through `TenantScope`) has no row of
//!   `bravo`'s; `?tenant=bravo` does not open another tenant — with an `admin`
//!   key **and** with a console SSO session (the `__Host-cairn_ui` cookie: a
//!   fake issuer `common::oidc`, `alpha`'s client with the `admin` role);
//! - a cross-range `StartCall` of `bravo` to a service with the same name as one
//!   of `alpha`'s stays inside `bravo` (`alpha`'s SDK gets none of its turns);
//! - `alpha`'s SDK commands by id — `attach`, `getOutput`, `signal` (CANCEL,
//!   awakeable, named signal) — to `bravo`'s invocations, in the same range and
//!   another range, answer byte for byte as for a random id and change nothing
//!   of `bravo`'s ([`alpha_sdk_cannot_reach_bravo_by_id`]; across nodes:
//!   `interrange.rs`).
//!
//! The matrix comes from the `.route(` calls of `ingress/mod.rs`: a new route
//! without a row ⇒ [`every_route_has_a_row`] goes red. The audit log has rows in
//! the matrix (`GET /audit`, `sys_audit`); ledger metering and per-tenant OTLP
//! do not exist yet — they get covered when they land.
//!
//! Order of [`alpha_cannot_reach_bravo`]:
//!
//! 1. `bravo` registers two SDKs (a scripted fake + a hand-written
//!    REQUEST_RESPONSE one with streams and artifacts), runs every kind of
//!    invocation, sets schedules, purges one into the archive; `alpha` registers
//!    an SDK with an `Echo` service of the same name.
//! 2. Take a [`Snapshot`] of `bravo` with `bravo`'s own key; control check:
//!    `bravo`'s key **sees** its own markers (only then does the "does not see"
//!    check below mean anything).
//! 3. `alpha` scans every `sys_*` table (filtered by `bravo`'s identifiers when
//!    the table requires a filter) with a key and then with an SSO session, then
//!    calls every row of the matrix (`admin` key, then `invoke`, with and without
//!    `?tenant=bravo`).
//! 4. Take `bravo`'s [`Snapshot`] again: it must be identical to step 2.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterminism"
)]

mod common;

use {
    anyhow::{anyhow, ensure, Result},
    bytes::{Bytes, BytesMut},
    cairn_protocol::{encode_message, pb, Flags, MessageType},
    cairn_server::{
        auth::AuthConfig,
        discovery::{
            EndpointManifest, HandlerManifest, ProtocolMode, ServiceManifest, ServiceType,
            DISCOVERY_V1,
        },
        invoker::INVOCATION_V1,
        node::{NodeConfig, NodeMode, RunningNode},
    },
    cairn_store::{ObjectStores, StoreLocation},
    common::{
        fake_sdk::{FakeSdk, Step, HOLD_SIGNAL},
        oidc::{login, FakeIssuer},
        within,
    },
    reqwest::{Client, Method},
    serde_json::{json, Value},
    std::{collections::BTreeMap, time::Duration},
};

const BOOT: &str = "cf_test_tenant_isolation_bootstrap_0123456789";
/// Token that only reads metrics (`CAIRNFLOW_METRICS_KEY`).
const METRICS: &str = "cf_test_tenant_isolation_metrics_only_0123456789";
const SECRET: &str = "cf_test_cluster_secret_tenant_isolation_32b";
const PARTITIONS: u16 = 8;
/// All data `bravo` writes carries this prefix; `alpha` never sends it.
const MARK: &str = "bravo-secret";
/// Content of `bravo`'s artifact.
const ARTIFACT: &[u8] = b"bravo-secret-artifact";

// ─────────────────────────────────────────────────────────────── matrix

/// How a route of the ingress port is handled.
#[derive(Clone, Copy, Debug)]
enum Rule {
    /// Carries no tenant data (`/health`).
    Open,
    /// Cluster work: a tenant's `admin`/`invoke` key ⇒ `403`.
    Cluster,
    /// Control-plane work (the `Provision` capability): a tenant key ⇒ `403`
    /// like [`Rule::Cluster`]; a `provision` key may call it.
    Provision,
    /// Per-tenant data: `alpha` calls with `bravo`'s identifiers, the returned
    /// code is in `expect`.
    Tenant(&'static [u16]),
    /// No API key needed, the right is in a token in the path (segment read
    /// URLs): a fake token ⇒ `403` for every caller. The token's MAC covers the
    /// tenant — one tenant's token cannot open another tenant's segment
    /// (`value_urls::tests`).
    Capability,
}

use Rule::{Capability, Cluster, Open, Provision, Tenant};

/// `404` as if it did not exist.
const NF: &[u16] = &[404];
/// Listing / reading within the caller's own tenant.
const OWN: &[u16] = &[200];

/// Every `(method, path)` of `router()` in `ingress/mod.rs`, and how it is handled.
const MATRIX: &[(&str, &str, Rule)] = &[
    ("GET", "/health", Open),
    ("GET", "/values/v1/{token}", Capability),
    ("GET", "/auth/whoami", Tenant(OWN)),
    ("GET", "/identity", Tenant(OWN)),
    ("GET", "/tenant", Tenant(OWN)),
    ("POST", "/deployments", Tenant(&[400])),
    ("GET", "/deployments", Tenant(OWN)),
    ("GET", "/deployments/{id}", Tenant(NF)),
    ("DELETE", "/deployments/{id}", Tenant(NF)),
    ("PUT", "/services/{service}/revision", Tenant(NF)),
    ("GET", "/services", Tenant(OWN)),
    ("POST", "/invoke/{service}/{handler}", Tenant(NF)),
    ("POST", "/invoke/{service}/{handler}/send", Tenant(NF)),
    (
        "POST",
        "/scope/{scope}/invoke/{service}/{handler}",
        Tenant(NF),
    ),
    (
        "POST",
        "/scope/{scope}/invoke/{service}/{handler}/send",
        Tenant(NF),
    ),
    ("POST", "/workflow/{service}/{key}/attach", Tenant(NF)),
    ("GET", "/workflow/{service}/{key}/output", Tenant(NF)),
    ("GET", "/workflow/{service}/{key}/id", Tenant(OWN)),
    (
        "POST",
        "/scope/{scope}/workflow/{service}/{key}/attach",
        Tenant(NF),
    ),
    (
        "GET",
        "/scope/{scope}/workflow/{service}/{key}/output",
        Tenant(NF),
    ),
    (
        "GET",
        "/scope/{scope}/workflow/{service}/{key}/id",
        Tenant(OWN),
    ),
    // Promises by `(tenant, scope, service, key, name)`: alpha sets and reads
    // **its own** promise with the same name; bravo's is in the Snapshot.
    (
        "POST",
        "/scope/{scope}/promises/{service}/{key}/{name}/resolve",
        Tenant(OWN),
    ),
    (
        "POST",
        "/scope/{scope}/promises/{service}/{key}/{name}/reject",
        Tenant(OWN),
    ),
    (
        "GET",
        "/scope/{scope}/promises/{service}/{key}/{name}",
        Tenant(OWN),
    ),
    ("GET", "/rules", Provision),
    ("POST", "/rules", Provision),
    ("DELETE", "/rules", Provision),
    ("GET", "/rules/{*pattern}", Cluster),
    ("DELETE", "/rules/{*pattern}", Cluster),
    ("GET", "/limits", Cluster),
    ("GET", "/vqueues", Cluster),
    ("GET", "/scheduler", Cluster),
    ("GET", "/invocations", Tenant(OWN)),
    ("POST", "/query", Tenant(OWN)),
    ("GET", "/invocations/{id}", Tenant(NF)),
    ("GET", "/invocations/{id}/journal", Tenant(NF)),
    ("GET", "/invocations/{id}/streams/{name}", Tenant(NF)),
    ("GET", "/invocations/{id}/logs", Tenant(NF)),
    ("GET", "/invocations/{id}/spans", Tenant(NF)),
    ("GET", "/invocations/{id}/artifacts", Tenant(NF)),
    ("GET", "/invocations/{id}/artifacts/{sha}", Tenant(NF)),
    ("POST", "/invocations/{id}/cancel", Tenant(NF)),
    ("POST", "/invocations/{id}/purge", Tenant(NF)),
    ("POST", "/invocations/{id}/signal", Tenant(NF)),
    ("POST", "/invocations/{id}/attach", Tenant(NF)),
    ("POST", "/invocations/{id}/resume", Tenant(NF)),
    ("POST", "/invocations/{id}/replay", Tenant(NF)),
    ("POST", "/invocations/{id}/migrate", Tenant(NF)),
    ("POST", "/invocations/{id}/restart", Tenant(NF)),
    ("POST", "/invocations/batch/{op}", Tenant(OWN)),
    ("POST", "/deployments/{id}/replay-check", Tenant(NF)),
    ("GET", "/schedules", Tenant(OWN)),
    ("GET", "/schedules/{name}", Tenant(NF)),
    ("PUT", "/schedules/{name}", Tenant(NF)),
    ("DELETE", "/schedules/{name}", Tenant(NF)),
    ("POST", "/schedules/{name}/pause", Tenant(NF)),
    ("POST", "/schedules/{name}/resume", Tenant(NF)),
    ("POST", "/schedules/{name}/trigger", Tenant(NF)),
    ("GET", "/scope/{scope}/schedules", Tenant(OWN)),
    ("GET", "/scope/{scope}/schedules/{name}", Tenant(NF)),
    ("PUT", "/scope/{scope}/schedules/{name}", Tenant(NF)),
    ("DELETE", "/scope/{scope}/schedules/{name}", Tenant(NF)),
    ("POST", "/scope/{scope}/schedules/{name}/pause", Tenant(NF)),
    ("POST", "/scope/{scope}/schedules/{name}/resume", Tenant(NF)),
    (
        "POST",
        "/scope/{scope}/schedules/{name}/trigger",
        Tenant(NF),
    ),
    ("GET", "/metering", Tenant(OWN)),
    ("GET", "/metering/hours", Tenant(OWN)),
    ("GET", "/ranges/windows", Cluster),
    ("GET", "/ranges/stats", Cluster),
    ("GET", "/ranges/suggestions", Cluster),
    ("PUT", "/ranges/{id}/window", Cluster),
    ("POST", "/awakeables/{id}/resolve", Tenant(NF)),
    ("POST", "/awakeables/{id}/reject", Tenant(NF)),
    (
        "POST",
        "/promises/{service}/{key}/{name}/resolve",
        Tenant(OWN),
    ),
    (
        "POST",
        "/promises/{service}/{key}/{name}/reject",
        Tenant(OWN),
    ),
    ("GET", "/promises/{service}/{key}/{name}", Tenant(OWN)),
    ("GET", "/cluster", Cluster),
    ("GET", "/metrics", Cluster),
    ("POST", "/ranges/{id}/split", Cluster),
    ("POST", "/ranges/merge", Cluster),
    ("GET", "/ranges/ops", Cluster),
    ("GET", "/ranges/ops/{id}", Cluster),
    ("POST", "/admin/tick", Cluster),
    ("POST", "/admin/keys", Provision),
    ("GET", "/admin/keys", Provision),
    ("GET", "/admin/tenants", Provision),
    ("PUT", "/admin/tenants/{tenant}", Provision),
    ("PUT", "/admin/tenants/{tenant}/state", Provision),
    ("GET", "/admin/tenants/{tenant}/purge", Provision),
    ("GET", "/audit", Tenant(OWN)),
    ("GET", "/archive", Tenant(OWN)),
    ("GET", "/archive/{id}", Tenant(NF)),
    ("DELETE", "/admin/keys/{id}", Provision),
    ("GET", "/admin/keys/{id}/revocation", Provision),
    // OIDC relying party: the control plane's configuration.
    ("GET", "/admin/oidc", Provision),
    ("PUT", "/admin/oidc", Provision),
    ("PUT", "/admin/oidc/clients/{id}", Provision),
    ("DELETE", "/admin/oidc/clients/{id}", Provision),
    ("POST", "/admin/oidc/revoked", Provision),
    ("GET", "/stats/handlers", Tenant(OWN)),
    ("GET", "/stats/slow-runs", Tenant(OWN)),
    ("GET", "/stats/history", Tenant(OWN)),
    ("GET", "/cluster/nodes", Cluster),
    ("GET", "/admin/nodewal", Cluster),
    ("GET", "/admin/scrub", Cluster),
    ("GET", "/admin/trash", Cluster),
    ("GET", "/metering/pricing", Cluster),
    ("POST", "/admin/capture", Cluster),
    ("GET", "/admin/capture", Cluster),
    ("GET", "/admin/capture/{tenant}/{id}", Cluster),
    ("GET", "/admin/quarantine", Cluster),
    (
        "DELETE",
        "/admin/quarantine/{tenant}/{service}/{handler}",
        Cluster,
    ),
    ("GET", "/admin/cluster-version", Provision),
    ("GET", "/admin/ops-reports", Provision),
    ("GET", "/admin/ops-reports/{id}", Provision),
    ("GET", "/admin/gc/plan", Cluster),
    ("GET", "/admin/gc/barriers", Cluster),
    ("GET", "/admin/gc/barriers/{id}", Cluster),
    ("PUT", "/admin/gc/barriers/{id}", Cluster),
    ("POST", "/admin/failpoints", Cluster),
    ("GET", "/admin/failpoints", Cluster),
];

/// `(method, path)` declared in a router source fragment: every `.route("…", …)`,
/// the method taken from the `get(`/`post(`/`put(`/`delete(` of that call.
fn routes_in(src: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for part in src.split(".route(").skip(1) {
        let Some(path) = part
            .trim_start()
            .strip_prefix('"')
            .and_then(|p| p.split('"').next())
        else {
            continue;
        };
        // The call up to the next `.route(`, skipping comment lines.
        let call: String = part
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");
        for m in ["get", "post", "put", "delete", "patch"] {
            let needle = format!("{m}(");
            let found = call.match_indices(&needle).any(|(i, _)| {
                // `get(` follows `(`, `.`, `:` or whitespace — not the tail of
                // another name (`budget(`).
                i == 0 || matches!(call.as_bytes()[i - 1], b'(' | b'.' | b':' | b' ' | b'\n')
            });
            if found {
                out.push((m.to_ascii_uppercase(), path.to_string()));
            }
        }
    }
    out
}

fn router_src() -> (&'static str, &'static str) {
    let src = include_str!("../src/ingress/mod.rs");
    let start = src.find("pub fn router(state: AppState)").expect("router");
    let end = src.find("pub fn internal_router").expect("internal_router");
    (&src[start..end], &src[end..])
}

/// **A new route must have a row**: every `(method, path)` of `router()` is in
/// [`MATRIX`], and vice versa.
#[test]
fn every_route_has_a_row() {
    let (public, _) = router_src();
    let routes = routes_in(public);
    assert!(routes.len() > 100, "route parsing broken: {routes:?}");
    for (m, p) in &routes {
        assert!(
            MATRIX.iter().any(|(mm, pp, _)| mm == m && pp == p),
            "{m} {p} has no row in the MATRIX of tests/tenant_isolation.rs — classify it \
             (Open/Cluster/Tenant) and have alpha try it with bravo's identifiers"
        );
    }
    for (m, p, _) in MATRIX {
        assert!(
            routes.iter().any(|(mm, pp)| mm == m && pp == p),
            "MATRIX still has {m} {p}, which ingress does not"
        );
    }
}

/// The concrete `(method, path)` of a matrix row for the capability table.
fn concrete(m: &str, p: &str) -> (Method, String) {
    let method = Method::from_bytes(m.as_bytes()).expect("method");
    (method, p.replace("{*pattern}", "x").replace(['{', '}'], ""))
}

/// **Every route declares its capabilities**: each `(method, path)` of
/// `router()` has its own line in the [`declared_caps`] table — no route lives
/// off the default closed branch. A new route must be classified (read, invoke,
/// change, provision or cluster) when it is added, not silently become
/// `Cluster`.
///
/// [`declared_caps`]: cairn_server::auth::declared_caps
#[test]
fn every_route_declares_capability() {
    let (public, _) = router_src();
    let routes = routes_in(public);
    assert!(routes.len() > 100, "route parsing broken: {routes:?}");
    for (m, p) in &routes {
        let (method, path) = concrete(m, p);
        assert!(
            cairn_server::auth::declared_caps(&method, &path).is_some(),
            "{m} {p} declares no capability in auth::declared_caps"
        );
    }
}

/// The matrix class matches the capability set the route declares
/// ([`required_caps`]) — and vice versa: the capability table and the matrix do
/// not drift apart.
///
/// - `Open` ⇔ open;
/// - `Cluster` ⇔ no tenant capability and no `Provision`;
/// - `Provision` ⇔ has `Provision`, no tenant capability;
/// - `Tenant` ⇔ has at least one tenant capability (`Read`/`Invoke`/`Operate`),
///   and `Provision` only in [`PROVISION_READS`].
///
/// [`required_caps`]: cairn_server::auth::required_caps
#[test]
fn cluster_rows_match_required_caps() {
    use cairn_server::auth::{required_caps, Capability, Need, TENANT_CAPS};
    for (m, p, rule) in MATRIX {
        let (method, path) = concrete(m, p);
        let need = required_caps(&method, &path);
        let caps = match need {
            Need::Open => {
                assert!(
                    matches!(rule, Open | Capability),
                    "{m} {p}: MATRIX says {rule:?}, the route is open"
                );
                continue;
            }
            Need::Any(c) => c,
        };
        let tenant = caps.meets(TENANT_CAPS);
        let provision = caps.has(Capability::Provision);
        let reads = PROVISION_READS.contains(&(*m, *p));
        let ok = match rule {
            Open | Capability => false,
            Cluster => !tenant && !provision,
            Provision => provision && !tenant,
            Tenant(_) => tenant && provision == reads,
        };
        assert!(
            ok,
            "{m} {p}: MATRIX says {rule:?}, the route declares {:?}",
            caps.names()
        );
    }
}

// ───────────────────────────────────────────────── hand-written REQUEST_RESPONSE SDK

/// `bravo`'s SDK with streams and artifacts (wire v4): `Agent/art` writes one
/// durable record to stream `notes`, PUTs an artifact, then returns its output.
async fn agent_sdk() -> Result<(String, tokio::task::JoinHandle<()>)> {
    let manifest = serde_json::to_string(&EndpointManifest {
        protocol_mode: ProtocolMode::RequestResponse,
        min_protocol_version: 1,
        max_protocol_version: 4,
        services: vec![ServiceManifest {
            name: "Agent".into(),
            ty: ServiceType::Service,
            handlers: vec![HandlerManifest {
                name: "art".into(),
                ty: None,
                contract: Default::default(),
            }],
            contract: Default::default(),
        }],
    })?;
    let app = axum::Router::new()
        .route(
            "/discover",
            axum::routing::get(move || {
                let m = manifest.clone();
                async move { ([(axum::http::header::CONTENT_TYPE, DISCOVERY_V1)], m) }
            }),
        )
        .route(
            "/invoke/Agent/art",
            axum::routing::post(|| async {
                (
                    [(axum::http::header::CONTENT_TYPE, INVOCATION_V1)],
                    agent_frames(),
                )
            }),
        );
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", l.local_addr()?);
    let task = tokio::spawn(async move {
        let _ = axum::serve(l, app).await;
    });
    Ok((url, task))
}

fn artifact_sha() -> String {
    use sha2::Digest;
    sha2::Sha256::digest(ARTIFACT)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn agent_frames() -> Bytes {
    use sha2::Digest;
    let mut b = BytesMut::new();
    b.extend_from_slice(&encode_message(
        MessageType::STREAM_APPEND,
        Flags::NONE,
        &pb::StreamAppendMessage {
            name: "notes".into(),
            durable: true,
            records: vec![pb::StreamRecord {
                step: 1,
                attempt: 0,
                k: 0,
                payload: Bytes::from(format!("{{\"note\":\"{MARK}-stream\"}}")),
            }],
            close: false,
        },
    ));
    let sha: [u8; 32] = sha2::Sha256::digest(ARTIFACT).into();
    b.extend_from_slice(&encode_message(
        MessageType::ARTIFACT_PUT,
        Flags::NONE,
        &pb::ArtifactPutMessage {
            sha256: Bytes::copy_from_slice(&sha),
            content: Bytes::from_static(ARTIFACT),
            content_type: "text/plain".into(),
        },
    ));
    b.extend_from_slice(&encode_message(
        MessageType::OUTPUT,
        Flags::NONE,
        &pb::OutputCommandMessage {
            result: Some(pb::output_command_message::Result::Value(
                cairn_protocol::value::inline(Bytes::from(format!("\"{MARK}-agent\""))),
            )),
            name: "output".into(),
        },
    ));
    b.extend_from_slice(&encode_message(
        MessageType::END,
        Flags::NONE,
        &pb::EndMessage {},
    ));
    b.freeze()
}

// ─────────────────────────────────────────────────────────────── fixture

/// HTTP with a key — or with a console session cookie when `cookie` is set
/// (with the node's `Origin` like a browser, for the CSRF gate of `authenticate`).
#[derive(Clone)]
struct As {
    url: String,
    key: String,
    cookie: Option<String>,
    http: Client,
    /// Ask to be forwarded to the leader (`x-cairn-forward: leader`) — like the
    /// console behind a load balancer.
    forward: bool,
}

impl As {
    async fn call(&self, method: &str, path: &str, body: Option<&str>) -> Result<(u16, String)> {
        let method = Method::from_bytes(method.as_bytes())?;
        let mut rb = self
            .http
            .request(method, format!("{}{path}", self.url))
            .timeout(Duration::from_secs(15));
        rb = match &self.cookie {
            Some(c) => rb
                .header(reqwest::header::COOKIE, c)
                .header(reqwest::header::ORIGIN, &self.url),
            None => rb.bearer_auth(&self.key),
        };
        if self.forward {
            rb = rb.header("x-cairn-forward", "leader");
        }
        if let Some(b) = body {
            rb = rb
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(b.to_string());
        }
        let r = rb.send().await?;
        let code = r.status().as_u16();
        Ok((code, r.text().await.unwrap_or_default()))
    }

    async fn json(&self, method: &str, path: &str, body: Option<&str>) -> Result<Value> {
        let (code, text) = self.call(method, path, body).await?;
        ensure!((200..300).contains(&code), "{method} {path}: {code} {text}");
        Ok(serde_json::from_str(&text).unwrap_or(Value::String(text)))
    }

    /// `POST /invoke…` waiting for the result; returns the id.
    async fn invoke(&self, path: &str, input: &str) -> Result<String> {
        let v = self.json("POST", path, Some(input)).await?;
        let id = v["id"].as_str().unwrap_or_default().to_string();
        ensure!(!id.is_empty(), "{path}: {v}");
        Ok(id)
    }

    /// Wait for invocation `id` to reach `status`.
    async fn wait_status(&self, id: &str, status: &str) -> Result<()> {
        within(
            Duration::from_secs(30),
            &format!("{id} reaches {status}"),
            async {
                loop {
                    let (code, text) = self
                        .call("GET", &format!("/invocations/{id}"), None)
                        .await?;
                    if code == 200 {
                        let v: Value = serde_json::from_str(&text)?;
                        if v["status"] == status {
                            return Ok(());
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            },
        )
        .await
    }
}

/// Every identifier of `bravo`'s that `alpha` "guesses correctly".
struct Fixture {
    /// A completed Echo (its output carries the marker).
    done: String,
    /// Suspended waiting for signal [`HOLD_SIGNAL`].
    held: String,
    /// `Agent/art`: has stream `notes` and an artifact.
    art: String,
    /// Purged by hand ⇒ only left in the archive.
    archived: String,
    /// A cross-range call.
    callers: Vec<String>,
    deployments: Vec<String>,
    key_id: String,
    /// Strings the responses to `alpha` must never contain.
    markers: Vec<String>,
}

impl Fixture {
    fn ids(&self) -> Vec<&str> {
        let mut v = vec![
            self.done.as_str(),
            self.held.as_str(),
            self.art.as_str(),
            self.archived.as_str(),
        ];
        v.extend(self.callers.iter().map(String::as_str));
        v
    }

    /// Awakeable id pointing at the suspended invocation (the awakeable's first index).
    fn awakeable(&self) -> String {
        format!("awk_{}{:08x}", self.held, 17)
    }
}

/// `bravo`'s state as seen with its own key.
#[derive(Debug, PartialEq)]
struct Snapshot {
    statuses: BTreeMap<String, (u16, String)>,
    schedules: Vec<(String, bool)>,
    deployments: Vec<String>,
    services: Vec<(String, String)>,
    archive: u16,
    promise: String,
    archive_list: String,
}

async fn snapshot(b: &As, fx: &Fixture) -> Result<Snapshot> {
    let mut statuses = BTreeMap::new();
    for id in fx.ids() {
        let (code, text) = b.call("GET", &format!("/invocations/{id}"), None).await?;
        let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        statuses.insert(
            id.to_string(),
            (code, v["status"].as_str().unwrap_or("").to_string()),
        );
    }
    let s = b.json("GET", "/schedules", None).await?;
    let mut schedules: Vec<(String, bool)> = s["schedules"]
        .as_array()
        .or(s.as_array())
        .into_iter()
        .flatten()
        .map(|x| {
            (
                x["name"].as_str().unwrap_or("").to_string(),
                x["paused"].as_bool().unwrap_or(false),
            )
        })
        .collect();
    schedules.sort();
    let d = b.json("GET", "/deployments", None).await?;
    let mut deployments: Vec<String> = d
        .as_array()
        .into_iter()
        .flatten()
        .map(|x| x["id"].as_str().unwrap_or("").to_string())
        .collect();
    deployments.sort();
    let sv = b.json("GET", "/services", None).await?;
    let mut services: Vec<(String, String)> = sv
        .as_array()
        .into_iter()
        .flatten()
        .map(|x| (x["name"].to_string(), x["revision"].to_string()))
        .collect();
    services.sort();
    let (archive, _) = b
        .call("GET", &format!("/archive/{}", fx.archived), None)
        .await?;
    let (_, promise) = b.call("GET", "/promises/Flow/wf1/p", None).await?;
    let (_, archive_list) = b
        .call("GET", &format!("/archive?date={}", today()), None)
        .await?;
    Ok(Snapshot {
        statuses,
        schedules,
        deployments,
        services,
        archive,
        promise,
        archive_list,
    })
}

async fn node_on(bucket: &common::Bucket) -> Result<RunningNode> {
    node_with_metrics(bucket, None).await
}

/// Like [`node_on`], with the metrics token `metrics`.
async fn node_with_metrics(bucket: &common::Bucket, metrics: Option<&str>) -> Result<RunningNode> {
    common::start_led(config_on(bucket, metrics)).await
}

/// Node configuration of the isolation suite on `bucket`.
fn config_on(bucket: &common::Bucket, metrics: Option<&str>) -> NodeConfig {
    let stores = ObjectStores::single(StoreLocation::from_store(
        (**bucket).clone(),
        "",
        "memory://",
    ));
    let mut cfg = NodeConfig::new(stores, NodeMode::single(PARTITIONS));
    cfg.tick_interval = Some(Duration::from_millis(50));
    cfg.flush_interval = Some(Duration::from_millis(5));
    cfg.deploy_refresh = Duration::from_millis(100);
    cfg.auth = AuthConfig {
        required: true,
        admin_key: Some(BOOT.into()),
        cluster_secret: Some(SECRET.into()),
        metrics_key: metrics.map(str::to_string),
        admin_key_sha256: None,
        provision_key_sha256: None,
    };
    cfg
}

/// A `role` key of `tenant`: `(id, secret)`.
async fn key(boot: &As, tenant: &str, role: &str) -> Result<(String, String)> {
    let v = boot
        .json(
            "POST",
            "/admin/keys",
            Some(&json!({ "tenant": tenant, "role": role }).to_string()),
        )
        .await?;
    Ok((
        v["id"].as_str().unwrap_or_default().to_string(),
        v["secret"].as_str().unwrap_or_default().to_string(),
    ))
}

/// A console session cookie of `tenant` (`admin` role) through a real OIDC flow
/// with a fake issuer: configure issuer + client for the node's hostname, log
/// in, then `whoami` must say `via: sso` with the right tenant (only then does
/// the cookie scan mean anything).
async fn sso_login(boot: &As, tenant: &str) -> Result<(String, FakeIssuer)> {
    let issuer = FakeIssuer::start().await?;
    let host = reqwest::Url::parse(&boot.url)?
        .host_str()
        .unwrap_or_default()
        .to_string();
    boot.json(
        "PUT",
        "/admin/oidc",
        Some(&json!({ "issuer": issuer.url, "displayName": "Cairnflow Cloud" }).to_string()),
    )
    .await?;
    boot.json(
        "PUT",
        &format!("/admin/oidc/clients/env_{tenant}"),
        Some(
            &json!({ "hostname": host, "tenant": tenant, "maxRole": "admin",
                "portalUrl": "https://app.example.test/environments" })
            .to_string(),
        ),
    )
    .await?;
    let cookie = login(&boot.url, None)
        .await?
        .cookie
        .ok_or_else(|| anyhow!("SSO login gave no cookie"))?;
    let me = As {
        cookie: Some(cookie.clone()),
        ..boot.clone()
    }
    .json("GET", "/auth/whoami", None)
    .await?;
    ensure!(
        me["via"] == "sso" && me["tenant"] == tenant && me["role"] == "admin",
        "SSO session: {me}"
    );
    Ok((cookie, issuer))
}

/// A second SSO cookie of the same client, with role `role`: the fake issuer
/// gives a different `cairn_role` to a different user `sub`; `whoami` must name
/// exactly that role.
async fn sso_login_as(boot: &As, issuer: &FakeIssuer, sub: &str, role: &str) -> Result<As> {
    issuer.set_user(sub, Some(role));
    let cookie = login(&boot.url, None)
        .await?
        .cookie
        .ok_or_else(|| anyhow!("SSO login with role {role} gave no cookie"))?;
    let me = As {
        cookie: Some(cookie),
        ..boot.clone()
    };
    let who = me.json("GET", "/auth/whoami", None).await?;
    ensure!(
        who["via"] == "sso" && who["role"] == role,
        "SSO session with role {role}: {who}"
    );
    Ok(me)
}

/// [`MARK`] verbatim, in hex (the journal returns payloads as hex) and in base64
/// at all three offsets (dropping the first/last character tied to neighbouring
/// bytes).
fn encoded_marks() -> Vec<String> {
    use base64::Engine as _;
    let mut out = vec![
        MARK.to_string(),
        MARK.bytes().map(|b| format!("{b:02x}")).collect(),
    ];
    for k in 0..3 {
        let padded = format!("{}{MARK}", "x".repeat(k));
        let enc = base64::engine::general_purpose::STANDARD.encode(padded.as_bytes());
        let skip = [0, 2, 3][k];
        out.push(enc[skip..enc.len() - 4].to_string());
    }
    out
}

/// Step 1 of the order at the top of the file.
async fn build(
    b: &As,
    a: &As,
    b_sdk: &FakeSdk,
    a_sdk: &FakeSdk,
    agent_url: &str,
    key_id: String,
) -> Result<Fixture> {
    let mut deployments = Vec::new();
    for uri in [b_sdk.url.as_str(), agent_url] {
        let v = b
            .json(
                "POST",
                "/deployments",
                Some(&json!({ "uri": uri }).to_string()),
            )
            .await?;
        deployments.push(v["id"].as_str().unwrap_or_default().to_string());
    }
    a.json(
        "POST",
        "/deployments",
        Some(&json!({ "uri": a_sdk.url }).to_string()),
    )
    .await?;

    let done = b
        .invoke("/invoke/Echo/run", &format!("\"{MARK}-input\""))
        .await?;
    // Cross-range `StartCall`: 8 ranges, each caller in a random range, callee
    // `spread` — landing in the same range all six times has probability (1/8)^6.
    let mut callers = Vec::new();
    for _ in 0..6 {
        let id = b.invoke("/invoke/Caller/run", "\"x\"").await?;
        let v = b.json("GET", &format!("/invocations/{id}"), None).await?;
        ensure!(
            v["output"].to_string().contains(MARK),
            "bravo's call must reach bravo's Echo: {v}"
        );
        callers.push(id);
    }
    ensure!(
        a_sdk.starts_of("Echo", "run").is_empty(),
        "alpha's SDK got a turn of bravo's: {:?}",
        a_sdk.starts_of("Echo", "run")
    );
    b.invoke("/invoke/Obj/set?key=k1", "\"x\"").await?;
    b.invoke("/scope/bscope/invoke/Echo/run", "\"x\"").await?;
    b.invoke("/invoke/Flow/run?key=wf1", "\"x\"").await?;
    b.json(
        "POST",
        "/promises/Flow/wf1/p/resolve",
        Some(&format!("\"{MARK}-promise\"")),
    )
    .await?;
    let v = b
        .json("POST", "/invoke/Hold/run/send", Some("\"x\""))
        .await?;
    let held = v["id"].as_str().unwrap_or_default().to_string();
    b.wait_status(&held, "suspended").await?;
    let art = b.invoke("/invoke/Agent/art", "\"x\"").await?;
    let archived = b.invoke("/invoke/Echo/run", "\"x\"").await?;
    b.json("POST", &format!("/invocations/{archived}/purge"), None)
        .await?;
    b.json(
        "PUT",
        "/schedules/nightly",
        Some(
            &json!({
                "everyMs": 3_600_000,
                "target": {"service": "Echo", "handler": "run"},
                "input": format!("{MARK}-sched"),
            })
            .to_string(),
        ),
    )
    .await?;
    // `alpha` has an invocation of its own (for `/invocations/{alpha's id}/
    // artifacts/{bravo's sha}`).
    a.invoke("/invoke/Echo/run", "\"x\"").await?;

    let mut markers = encoded_marks();
    for uri in [b_sdk.url.as_str(), agent_url] {
        markers.push(uri.trim_start_matches("http://").to_string());
    }
    Ok(Fixture {
        done,
        held,
        art,
        archived,
        callers,
        deployments,
        key_id,
        markers,
    })
}

// ─────────────────────────────────────────────────────────────── probes

/// The concrete path of `p` with `bravo`'s identifiers — one or several variants.
fn probes(
    m: &str,
    p: &str,
    rule: Rule,
    fx: &Fixture,
    a_own: &str,
) -> Vec<(String, Option<String>)> {
    let sub = |s: &str, id: &str| -> String {
        let svc = if s.contains("/workflow/") || s.contains("/promises/") {
            "Flow"
        } else {
            "Hold"
        };
        let name = if s.contains("/streams/") {
            "notes"
        } else if s.contains("/promises/") {
            "p"
        } else {
            "nightly"
        };
        s.replace("{scope}", "bscope")
            .replace("{service}", svc)
            .replace("{handler}", "run")
            .replace("{key}", "wf1")
            .replace("{name}", name)
            .replace("{sha}", &artifact_sha())
            .replace("{op}", "cancel")
            .replace("{token}", "bogus")
            .replace("{tenant}", "bravo")
            .replace("{*pattern}", "x")
            .replace("{id}", id)
    };
    let body = |path: &str| -> Option<String> {
        Some(match (m, path) {
            ("POST", "/deployments") => json!({ "uri": "not a url" }).to_string(),
            ("PUT", p) if p.ends_with("/revision") => {
                json!({ "deployment": fx.deployments[0] }).to_string()
            }
            ("POST", "/query") => json!({ "sql": "SELECT * FROM sys_invocation" }).to_string(),
            ("POST", "/invocations/batch/{op}") => json!({ "ids": fx.ids() }).to_string(),
            ("POST", p) if p.ends_with("/signal") => {
                json!({ "signal": HOLD_SIGNAL, "value": 1 }).to_string()
            }
            ("PUT", p) if p.contains("/schedules/") => json!({
                "everyMs": 3_600_000,
                "target": {"service": "Hold", "handler": "run"}
            })
            .to_string(),
            ("POST", p) if p.contains("/reject") => json!({ "message": "x" }).to_string(),
            ("POST" | "PUT", _) => "\"x\"".to_string(),
            _ => return None,
        })
    };
    let b = body(p);
    if !matches!(rule, Tenant(_)) {
        // The role gate comes before the handler: one probe is enough.
        return vec![(sub(p, &fx.key_id), b)];
    }
    if p.starts_with("/deployments/{id}") {
        // `replay-check` requires `?service=` before looking at the deployment.
        let q = if p.ends_with("/replay-check") {
            "?service=Hold"
        } else {
            ""
        };
        return fx
            .deployments
            .iter()
            .map(|d| (format!("{}{q}", sub(p, d)), b.clone()))
            .collect();
    }
    match p {
        "/archive" => return vec![(format!("/archive?date={}", today()), b)],
        "/metering/hours" => return vec![(format!("/metering/hours?from={}T00", today()), b)],
        _ => {}
    }
    if p.starts_with("/awakeables/") {
        return vec![(sub(p, &fx.awakeable()), b)];
    }
    if p == "/archive/{id}" {
        return vec![(sub(p, &fx.archived), b)];
    }
    if p == "/invocations/{id}/artifacts/{sha}" {
        return vec![
            (sub(p, &fx.art), b.clone()),
            // alpha's own id + bravo's sha: a content-addressed artifact must
            // not open across tenants.
            (sub(p, a_own), b),
        ];
    }
    if p.contains("{id}") {
        // `migrate` requires `?deployment=` before looking at the invocation.
        let q = if p.ends_with("/migrate") {
            format!("?deployment={}", fx.deployments[0])
        } else {
            String::new()
        };
        return fx
            .ids()
            .into_iter()
            .map(|id| (format!("{}{q}", sub(p, id)), b.clone()))
            .collect();
    }
    vec![(sub(p, ""), b)]
}

/// The query for table `t`: filtered by bravo's identifiers when the table
/// requires a key filter; otherwise the whole table.
fn table_queries(t: &str, fx: &Fixture) -> Vec<String> {
    match t {
        "sys_journal" | "sys_artifact" | "sys_log" | "sys_timer" | "sys_stream"
        | "sys_attachment" | "sys_span" | "sys_outbox" | "sys_inbound" => fx
            .ids()
            .iter()
            .map(|id| format!("SELECT * FROM {t} WHERE id = '{id}'"))
            .collect(),
        "sys_stream_record" => vec![format!(
            "SELECT * FROM {t} WHERE id = '{}' AND name = 'notes'",
            fx.art
        )],
        "sys_state" | "sys_value" | "sys_promise" | "sys_promise_waiter" => {
            [("Obj", "k1"), ("Flow", "wf1")]
                .iter()
                .map(|(s, k)| format!("SELECT * FROM {t} WHERE service = '{s}' AND key = '{k}'"))
                .collect()
        }
        "sys_vqueue_entry" | "sys_object" => ["Obj", "Flow", "Hold", "Echo"]
            .iter()
            .map(|s| format!("SELECT * FROM {t} WHERE service = '{s}'"))
            .collect(),
        "sys_archive" => vec![format!("SELECT * FROM {t} WHERE date = '{}'", today())],
        _ => vec![format!("SELECT * FROM {t}")],
    }
}

/// Today's UTC date, `yyyy-mm-dd` (Gregorian calendar from the day count since the epoch).
fn today() -> String {
    let days = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time after the epoch")
        .as_secs()
        / 86_400) as i64;
    // Howard Hinnant, `civil_from_days`.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Append `?tenant=bravo` (or `&tenant=bravo`).
fn with_tenant(path: &str) -> String {
    if path.contains('?') {
        format!("{path}&tenant=bravo")
    } else {
        format!("{path}?tenant=bravo")
    }
}

fn leaks(text: &str, fx: &Fixture) -> Option<String> {
    fx.markers
        .iter()
        .find(|m| text.contains(m.as_str()))
        .cloned()
}

/// The node, keys and SDKs of step 1 (order at the top of the file), before [`build`].
struct World {
    node: RunningNode,
    boot: As,
    a: As,
    a_inv: As,
    /// Console SSO session, tenant `alpha`, role `admin`.
    a_sso: As,
    /// SSO sessions of `alpha` with roles `invoke` and `view`.
    a_sso_invoke: As,
    a_sso_view: As,
    b: As,
    b_sdk: FakeSdk,
    a_sdk: FakeSdk,
    agent_url: String,
    agent_task: tokio::task::JoinHandle<()>,
    b_key_id: String,
}

impl World {
    async fn start(bucket: &common::Bucket) -> Result<World> {
        let node = node_on(bucket).await?;
        let http = Client::new();
        let boot = As {
            url: node.url(),
            key: BOOT.into(),
            cookie: None,
            http: http.clone(),
            forward: false,
        };
        let (b_key_id, b_admin) = key(&boot, "bravo", "admin").await?;
        let (_, a_admin) = key(&boot, "alpha", "admin").await?;
        let (_, a_invoke) = key(&boot, "alpha", "invoke").await?;
        let b = As {
            key: b_admin,
            ..boot.clone()
        };
        let a = As {
            key: a_admin,
            ..boot.clone()
        };
        let a_inv = As {
            key: a_invoke,
            ..boot.clone()
        };
        let (cookie, issuer) = sso_login(&boot, "alpha").await?;
        let a_sso = As {
            cookie: Some(cookie),
            ..boot.clone()
        };
        let a_sso_invoke = sso_login_as(&boot, &issuer, "user-invoke", "invoke").await?;
        let a_sso_view = sso_login_as(&boot, &issuer, "user-view", "view").await?;
        let echo = format!("\"{MARK}-out\"");
        let b_sdk = FakeSdk::builder()
            .service("Echo", "run", vec![Step::output(&echo)])
            .service(
                "Caller",
                "run",
                vec![Step::call_spread("Echo", "run"), Step::EchoCallResult],
            )
            .object(
                "Obj",
                "set",
                vec![
                    Step::SetState {
                        key: "note".into(),
                        value: Bytes::from(format!("\"{MARK}-state\"")),
                    },
                    // A segmented value (≥ `seal_bytes` 256 KiB): bravo's
                    // `sys_value` has a real segment so the table's row can be
                    // checked.
                    Step::SetState {
                        key: "history".into(),
                        value: Bytes::from(format!("\"{MARK}-{}\"", "h".repeat(300 << 10))),
                    },
                    Step::output("\"ok\""),
                ],
            )
            .workflow("Flow", "run", vec![Step::output(&echo)])
            .service("Hold", "run", vec![Step::Suspend, Step::output("\"ok\"")])
            .spawn()
            .await?;
        let a_sdk = FakeSdk::builder()
            .service("Echo", "run", vec![Step::output("\"alpha-out\"")])
            .spawn()
            .await?;
        let (agent_url, agent_task) = agent_sdk().await?;

        Ok(World {
            node,
            boot,
            a,
            a_inv,
            a_sso,
            a_sso_invoke,
            a_sso_view,
            b,
            b_sdk,
            a_sdk,
            agent_url,
            agent_task,
            b_key_id,
        })
    }

    /// Step 1 ([`build`]), plus the id of one of `alpha`'s own invocations.
    async fn build(&self) -> Result<(Fixture, String)> {
        let fx = build(
            &self.b,
            &self.a,
            &self.b_sdk,
            &self.a_sdk,
            &self.agent_url,
            self.b_key_id.clone(),
        )
        .await?;
        let a_own = self
            .a
            .json("GET", "/invocations", None)
            .await?
            .as_array()
            .and_then(|v| v.first().cloned())
            .and_then(|v| v["id"].as_str().map(str::to_string))
            .ok_or_else(|| anyhow!("alpha does not see its own invocation"))?;
        Ok((fx, a_own))
    }

    async fn shutdown(self) {
        self.agent_task.abort();
        self.node.shutdown().await;
    }
}

/// Step 3b: every identity in `who` calls every row of [`MATRIX`] with `bravo`'s
/// identifiers — codes outside the rule, `5xx` or `bravo`'s markers go into
/// `bad`. Returns the code of each probe by `"<who> <method> <path>"` (compared
/// between two nodes).
async fn matrix_pass(
    who: &[(&str, &As)],
    fx: &Fixture,
    a_own: &str,
    table: &mut Vec<String>,
    bad: &mut Vec<String>,
) -> Result<BTreeMap<String, u16>> {
    let mut codes = BTreeMap::new();
    for (m, p, rule) in MATRIX {
        for (who, client) in who.iter().copied() {
            for (path, body) in probes(m, p, *rule, fx, a_own) {
                let mut variants = vec![path.clone()];
                if *m == "GET" {
                    variants.push(with_tenant(&path));
                }
                for path in variants {
                    let (code, text) = client.call(m, &path, body.as_deref()).await?;
                    table.push(format!("{who:6} {code} {m} {path}"));
                    codes.insert(format!("{who} {m} {path}"), code);
                    if let Some(mark) = leaks(&text, fx) {
                        bad.push(format!("{who} {m} {path}: {code} leaks {mark:?}: {text}"));
                    }
                    if code >= 500 {
                        bad.push(format!("{who} {m} {path}: {code} {text}"));
                    }
                    let tenant_q = path.contains("tenant=bravo");
                    // A role lacking the capability for this row ⇒ `403`: `invoke` on
                    // every `Tenant` row that is not an invoke, `view` on every write row.
                    let lacks_role =
                        who.ends_with("invoke") || who == "sso-view" && !view_reads(m, p);
                    let ok = match rule {
                        Open => code == 200,
                        Capability | Cluster | Provision => code == 403,
                        // `?tenant=` with a key that is not cluster-admin ⇒
                        // 403 or ignored.
                        Tenant(expect) => {
                            expect.contains(&code) || code == 403 && (lacks_role || tenant_q)
                        }
                    };
                    if !ok {
                        bad.push(format!("{who} {m} {path}: {code} outside {rule:?}: {text}"));
                    }
                }
            }
        }
    }
    Ok(codes)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alpha_cannot_reach_bravo() -> Result<()> {
    let bucket = common::bucket();
    let w = World::start(&bucket).await?;
    let World {
        node,
        a,
        a_inv,
        a_sso,
        a_sso_invoke,
        a_sso_view,
        b,
        a_sdk,
        ..
    } = &w;
    let r = within(Duration::from_secs(240), "tenant isolation", async {
        // ── 1. build.
        let (fx, a_own) = w.build().await?;

        // ── 2. snapshot, control check.
        let before = snapshot(b, &fx).await?;
        for (id, (code, status)) in &before.statuses {
            ensure!(
                *code == 200 || id == &fx.archived,
                "bravo does not see its own invocation {id}: {code}"
            );
            ensure!(id != &fx.held || status == "suspended", "{before:?}");
        }
        ensure!(before.archive == 200, "bravo's archive: {before:?}");
        ensure!(
            before.promise.contains(MARK) && before.archive_list.contains(&fx.archived),
            "bravo's promise and archive listing: {before:?}"
        );
        ensure!(
            before.schedules == vec![("nightly".into(), false)],
            "{before:?}"
        );
        let mut seen = String::new();
        for path in [
            format!("/invocations/{}/journal", fx.done),
            format!("/invocations/{}/artifacts/{}", fx.art, artifact_sha()),
            format!("/archive/{}", fx.archived),
            "/deployments".to_string(),
        ] {
            seen.push_str(&b.call("GET", &path, None).await?.1);
        }
        ensure!(
            seen.contains(MARK)
                // the journal returns payloads as hex.
                && seen.contains(fx.markers[1].as_str())
                && fx.markers[fx.markers.len() - 2..]
                    .iter()
                    .all(|m| seen.contains(m.as_str())),
            "control check: bravo's key must see its own markers: {seen}"
        );

        let mut bad = Vec::new();
        let mut table = Vec::new();
        // ── 3a. every `sys_*` table through `TenantScope`, filtered by bravo's
        // identifiers when the table requires a filter, with the `admin` key and
        // with alpha's SSO session. Control check: the same query with bravo's
        // key must run (200) — only then is alpha's `400` not meaningless — or
        // the table is cluster-only (both get blocked).
        let mut b_seen = String::new();
        for t in cairn_query::tables() {
            for sql in table_queries(t, &fx) {
                let sql = json!({ "sql": sql }).to_string();
                let (b_code, b_text) = b.call("POST", "/query", Some(&sql)).await?;
                b_seen.push_str(&b_text);
                let cluster_only = b_code == 400
                    && (b_text.contains("lacks the capability for")
                        || b_text.contains("reserved for cluster-admin"));
                if b_code != 200 && !cluster_only {
                    bad.push(format!("control: bravo runs {sql}: {b_code} {b_text}"));
                }
                for (via, client) in [("key", a), ("sso", a_sso)] {
                    for path in ["/query", "/query?tenant=bravo"] {
                        let (code, text) = client.call("POST", path, Some(&sql)).await?;
                        table.push(format!(
                            "query  {via:3} {code} {t} {path} {}",
                            if code == 200 { "" } else { text.as_str() }
                        ));
                        if text.contains("bravo") || leaks(&text, &fx).is_some() {
                            bad.push(format!(
                                "/query {via} {t} ({path}): {code} leaks bravo: {text}"
                            ));
                        }
                        if code >= 500 {
                            bad.push(format!("/query {via} {t} ({path}): {code} {text}"));
                        }
                        if code != b_code && !(code == 403 && path.contains("tenant=")) {
                            bad.push(format!(
                                "/query {via} {sql} ({path}): alpha {code}, bravo {b_code}: {text}"
                            ));
                        }
                    }
                }
            }
        }
        for m in ["-out", "-input", "-state", "-stream", "-agent", "-promise"] {
            if !b_seen.contains(&format!("{MARK}{m}")) {
                bad.push(format!(
                    "control check: the sys_* tables do not let bravo see {MARK}{m}"
                ));
            }
        }

        // ── 3b. alpha calls every row of the matrix — after the scan, since a
        // leaking route could change bravo's state. With a key and with a
        // console SSO session in three roles: the cookie goes through the same
        // capability gate, so the same expected table per role applies.
        let identities = [
            ("admin", a),
            ("invoke", a_inv),
            ("sso-admin", a_sso),
            ("sso-invoke", a_sso_invoke),
            ("sso-view", a_sso_view),
        ];
        matrix_pass(&identities, &fx, &a_own, &mut table, &mut bad).await?;

        // ── 3c. the internal port: `/internal/*` routes do not exist on the
        // ingress port (a tenant key ⇒ `403` like every cluster path), and on the
        // internal port a request not signed with the cluster secret ⇒ `401`
        // even with a key.
        let (_, internal) = router_src();
        let internal_routes = routes_in(internal);
        if internal_routes.len() < 10 {
            bad.push(format!("bad internal routes: {internal_routes:?}"));
        }
        let inside = As {
            url: node.internal_url(),
            ..a.clone()
        };
        for (m, p) in &internal_routes {
            let path = p.replace(['{', '}'], "");
            for (port, client, want) in [
                ("ingress", a, 403),
                ("ingress-sso", a_sso, 403),
                ("internal", &inside, 401),
            ] {
                let (code, text) = client.call(m, &path, Some("{}")).await?;
                table.push(format!("{port:6} {code} {m} {path}"));
                if code != want || leaks(&text, &fx).is_some() {
                    bad.push(format!("{port} {m} {path}: {code} (want {want}): {text}"));
                }
            }
        }

        // ── 4. bravo unchanged.
        let after = snapshot(b, &fx).await?;
        if after != before {
            bad.push(format!(
                "bravo's state changed:\n  before {before:?}\n  after  {after:?}"
            ));
        }
        if a_sdk.starts_of("Echo", "run").len() != 1 {
            bad.push(format!(
                "alpha's SDK got a foreign turn: {:?}",
                a_sdk.starts_of("Echo", "run")
            ));
        }
        // `CAIRN_ISOLATION_TABLE=1` prints the table of codes returned by every probe.
        if std::env::var("CAIRN_ISOLATION_TABLE").is_ok() {
            eprintln!("{}", table.join("\n"));
        }
        if !bad.is_empty() {
            return Err(anyhow!(
                "{} isolation violations:\n{}\n\ntable:\n{}",
                bad.len(),
                bad.join("\n"),
                table.join("\n")
            ));
        }
        Ok(())
    })
    .await;
    w.shutdown().await;
    r
}

/// Per-tenant routes a `provision` key (the control plane) may also call: a
/// tenant's request identity, metering and audit log (`?tenant=`), and
/// `whoami`. The audit log is who did what — no run data.
const PROVISION_READS: &[(&str, &str)] = &[
    ("GET", "/auth/whoami"),
    ("GET", "/identity"),
    ("GET", "/metering"),
    ("GET", "/metering/hours"),
    ("GET", "/audit"),
];

/// Whether the `view` role may call this `Tenant` row: read only — `GET`, and
/// `POST /query` (a read-only query).
fn view_reads(m: &str, p: &str) -> bool {
    m == "GET" || (m == "POST" && p == "/query")
}

/// **`provision` and `view` keys across the whole matrix.**
///
/// The control plane holds every cell's `provision` key instead of a
/// cluster-admin key: exposing the control plane must not mean exposing
/// customers' run data. The `view` role reads everything `admin` reads in its
/// own tenant, but calls nothing and changes nothing.
///
/// 1. Build like [`alpha_cannot_reach_bravo`] (step 1); the bootstrap key adds a
///    pattern rule whose description carries `bravo`'s marker, a `provision` key
///    (tenant `default`), a `view` key of `alpha`, and a `cluster-admin` key.
///    Take a [`Snapshot`] of `bravo`.
/// 2. The `provision` key calls every row of [`MATRIX`] with `bravo`'s
///    identifiers (`GET` both with and without `?tenant=bravo`; `DELETE
///    /admin/keys/{id}` with an id that does not exist — revoking `bravo`'s
///    keys is its job, but it would change the [`Snapshot`]).
/// 3. The control plane does its job: `GET /identity?tenant=bravo`,
///    `GET /rules`, `POST /rules {tenant}`, create and then revoke an `admin` key
///    of `bravo`.
/// 4. Escalation: the `provision` key creates a `cluster-admin` or `provision`
///    key, revokes the `cluster-admin` key, edits a pattern rule.
/// 5. `alpha`'s `view` key calls every row with `bravo`'s identifiers, then
///    reads and tries to change `alpha`'s own data.
/// 6. Take the [`Snapshot`] again.
///
/// Must see: (2) `Open` rows `200`; `Provision` rows not `403`, not `5xx`;
/// `Tenant` rows `403` except [`PROVISION_READS`] (`200`); `Cluster` rows `403`;
/// no response contains `bravo`'s marker (including the pattern rule in
/// `GET /rules`). (3) all succeed. (4) all `403`. (5) `Tenant` read rows behave
/// like `alpha`'s `admin` key (code in `expect`, `403` with `?tenant=`),
/// `Tenant` write rows and every `Cluster`/`Provision` row `403`; the `view` key
/// sees `alpha`'s invocations but cannot call, cancel or register. (6)
/// identical to (1).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn provision_and_view_keys_over_the_matrix() -> Result<()> {
    let bucket = common::bucket();
    let w = World::start(&bucket).await?;
    let r = within(Duration::from_secs(240), "provision and view", async {
        // ── 1.
        let (fx, a_own) = w.build().await?;
        let boot = &w.boot;
        boot.json(
            "POST",
            "/rules",
            Some(
                &json!({ "pattern": "Hold", "concurrency": 1000, "description": format!("{MARK}-rule") })
                    .to_string(),
            ),
        )
        .await?;
        let (_, p_secret) = key(boot, "default", "provision").await?;
        let (_, v_secret) = key(boot, "alpha", "view").await?;
        let (ca_id, _) = key(boot, "default", "cluster-admin").await?;
        let p = As {
            key: p_secret,
            ..boot.clone()
        };
        let v = As {
            key: v_secret,
            ..boot.clone()
        };
        let before = snapshot(&w.b, &fx).await?;
        let mut bad = Vec::new();
        let mut table = Vec::new();

        // ── 2.
        for (m, path, rule) in MATRIX {
            let reads = PROVISION_READS.contains(&(*m, *path));
            for (probe, body) in probes(m, path, *rule, &fx, &a_own) {
                let probe = if *m == "DELETE" && path.starts_with("/admin/keys/") {
                    "/admin/keys/k_missing".to_string()
                } else {
                    probe
                };
                let mut variants = vec![probe.clone()];
                if *m == "GET" {
                    variants.push(with_tenant(&probe));
                }
                for probe in variants {
                    let (code, text) = p.call(m, &probe, body.as_deref()).await?;
                    table.push(format!("provision {code} {m} {probe}"));
                    if let Some(mark) = leaks(&text, &fx) {
                        bad.push(format!("provision {m} {probe}: {code} leaks {mark:?}: {text}"));
                    }
                    let ok = match rule {
                        Open => code == 200,
                        Provision => code != 403 && code < 500,
                        Tenant(_) if reads => code == 200,
                        Capability | Tenant(_) | Cluster => code == 403,
                    };
                    if !ok {
                        bad.push(format!("provision {m} {probe}: {code} outside {rule:?}: {text}"));
                    }
                }
            }
        }

        // ── 3.
        let id = p.json("GET", "/identity?tenant=bravo", None).await?;
        if id["tenant"] != "bravo" {
            bad.push(format!("GET /identity?tenant=bravo: {id}"));
        }
        let rules = p.json("GET", "/rules", None).await?;
        if rules["rules"].as_array().is_none_or(|r| !r.is_empty()) {
            bad.push(format!("provision sees the pattern rule: {rules}"));
        }
        let put = p
            .json(
                "POST",
                "/rules",
                Some(&json!({ "tenant": "bravo", "rate": "unlimited", "description": "beta" }).to_string()),
            )
            .await?;
        if !put["tenants"].to_string().contains("bravo") || put.to_string().contains(MARK) {
            bad.push(format!("POST /rules {{tenant}} with provision: {put}"));
        }
        let made = p
            .json(
                "POST",
                "/admin/keys",
                Some(&json!({ "tenant": "bravo", "role": "admin" }).to_string()),
            )
            .await?;
        let made_id = made["id"].as_str().unwrap_or_default().to_string();
        let (code, text) = p.call("DELETE", &format!("/admin/keys/{made_id}"), None).await?;
        if !(code == 204 || code == 202) {
            bad.push(format!("provision revokes the admin key it just created: {code} {text}"));
        }
        let (code, text) = p.call("GET", "/admin/cluster-version", None).await?;
        if code != 200 {
            bad.push(format!("provision GET /admin/cluster-version: {code} {text}"));
        }

        // ── 4.
        for role in ["cluster-admin", "provision"] {
            let (code, text) = p
                .call(
                    "POST",
                    "/admin/keys",
                    Some(&json!({ "tenant": "default", "role": role }).to_string()),
                )
                .await?;
            if code != 403 {
                bad.push(format!("provision creates a {role} key: {code} {text}"));
            }
        }
        let (code, text) = p.call("DELETE", &format!("/admin/keys/{ca_id}"), None).await?;
        if code != 403 {
            bad.push(format!("provision revokes the cluster-admin key: {code} {text}"));
        }
        let (code, text) = p
            .call(
                "POST",
                "/rules",
                Some(&json!({ "pattern": "Hold", "concurrency": 1 }).to_string()),
            )
            .await?;
        if code != 403 {
            bad.push(format!("provision edits the pattern rule: {code} {text}"));
        }

        // ── 5.
        for (m, path, rule) in MATRIX {
            for (probe, body) in probes(m, path, *rule, &fx, &a_own) {
                let mut variants = vec![probe.clone()];
                if *m == "GET" {
                    variants.push(with_tenant(&probe));
                }
                for probe in variants {
                    let (code, text) = v.call(m, &probe, body.as_deref()).await?;
                    table.push(format!("view   {code} {m} {probe}"));
                    if let Some(mark) = leaks(&text, &fx) {
                        bad.push(format!("view {m} {probe}: {code} leaks {mark:?}: {text}"));
                    }
                    if code >= 500 {
                        bad.push(format!("view {m} {probe}: {code} {text}"));
                    }
                    let ok = match rule {
                        Open => code == 200,
                        Capability | Cluster | Provision => code == 403,
                        Tenant(expect) if view_reads(m, path) => {
                            expect.contains(&code) || code == 403 && probe.contains("tenant=bravo")
                        }
                        Tenant(_) => code == 403,
                    };
                    if !ok {
                        bad.push(format!("view {m} {probe}: {code} outside {rule:?}: {text}"));
                    }
                }
            }
        }
        let own = v.json("GET", &format!("/invocations/{a_own}"), None).await?;
        if own["id"] != a_own.as_str() {
            bad.push(format!("view cannot read alpha's invocation: {own}"));
        }
        for (m, path, body) in [
            ("POST", "/invoke/Echo/run".to_string(), Some("\"x\"")),
            ("POST", format!("/invocations/{a_own}/cancel"), None),
            ("POST", format!("/invocations/{a_own}/cancel?mode=signal"), None),
            ("POST", "/deployments".to_string(), Some("{\"uri\":\"http://127.0.0.1:1\"}")),
        ] {
            let (code, text) = v.call(m, &path, body).await?;
            if code != 403 {
                bad.push(format!("view {m} {path} on alpha's data: {code} {text}"));
            }
        }

        // ── 6.
        let after = snapshot(&w.b, &fx).await?;
        if after != before {
            bad.push(format!(
                "bravo's state changed:\n  before {before:?}\n  after  {after:?}"
            ));
        }
        if std::env::var("CAIRN_ISOLATION_TABLE").is_ok() {
            eprintln!("{}", table.join("\n"));
        }
        ensure!(
            bad.is_empty(),
            "{} violations:\n{}\n\ntable:\n{}",
            bad.len(),
            bad.join("\n"),
            table.join("\n")
        );
        Ok(())
    })
    .await;
    w.shutdown().await;
    r
}

/// **The metrics token only opens `GET /metrics`.**
///
/// Prometheus holds this token (`CAIRNFLOW_METRICS_KEY`) instead of a
/// cluster-admin key. The `metrics` role sits outside the chain `invoke ⊂ admin
/// ⊂ cluster-admin`: metrics see across every tenant, but the token may do
/// nothing else.
///
/// 1. A node with a bootstrap key and a metrics token.
/// 2. Call **every** row of [`MATRIX`] with the metrics token (fake path
///    parameters, empty body); plus `HEAD /metrics`, `/scope/x/metrics`,
///    `/metrics/`, an unknown path.
/// 3. Log in to the console (`POST /ui/session`) with the metrics token.
/// 4. The bootstrap key creates a `metrics`-role key in the registry
///    (`POST /admin/keys`).
/// 5. Build a node with a metrics token shorter than 32 bytes, then one equal to
///    the bootstrap key.
///
/// Must see: `GET /metrics` `200` with metrics; `GET /health` `200` as for
/// everyone; every other row and every variant in (2) `403`; (3) `401`, no
/// `Set-Cookie` — the cookie branch treats an absent `key_id` as bootstrap, so a
/// metrics token cookie would be a cluster-admin cookie; (4) `400` — the
/// `metrics` role never goes into `auth/keys` (an old binary would fail to read
/// the whole registry); (5) the node does not start.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metrics_key_reaches_only_metrics() -> Result<()> {
    let bucket = common::bucket();
    let node = node_with_metrics(&bucket, Some(METRICS)).await?;
    let http = Client::new();
    let m = As {
        url: node.url(),
        key: METRICS.into(),
        cookie: None,
        http: http.clone(),
        forward: false,
    };
    let r = within(Duration::from_secs(60), "metrics token", async {
        let mut wrong = Vec::new();
        for (method, path, rule) in MATRIX {
            let concrete = path.replace("{*pattern}", "x");
            let concrete: String = {
                let mut out = String::new();
                let mut skip = false;
                for c in concrete.chars() {
                    match c {
                        '{' => {
                            skip = true;
                            out.push('x');
                        }
                        '}' => skip = false,
                        _ if skip => {}
                        _ => out.push(c),
                    }
                }
                out
            };
            let (code, text) = m.call(method, &concrete, None).await?;
            let want = match (method, *path, rule) {
                (&"GET", "/metrics", _) => 200,
                (_, _, Open) => 200,
                _ => 403,
            };
            if code != want {
                wrong.push(format!(
                    "{method} {concrete}: {code} (expected {want}) {text:.80}"
                ));
            }
            if want == 200 && *path == "/metrics" && !text.contains("cairnflow_") {
                wrong.push(format!("GET /metrics has no metrics: {text:.80}"));
            }
        }
        for (method, path) in [
            ("HEAD", "/metrics"),
            ("GET", "/scope/x/metrics"),
            ("GET", "/metrics/"),
            ("GET", "/missing"),
            ("POST", "/metrics"),
        ] {
            let (code, _) = m.call(method, path, None).await?;
            if code != 403 {
                wrong.push(format!("{method} {path}: {code} (expected 403)"));
            }
        }
        ensure!(
            wrong.is_empty(),
            "the metrics token goes beyond `GET /metrics`:\n{}",
            wrong.join("\n")
        );

        let login = http
            .post(format!("{}/ui/session", node.url()))
            .header(reqwest::header::ORIGIN, node.url())
            .json(&json!({ "key": METRICS }))
            .send()
            .await?;
        ensure!(
            login.status() == 401 && login.headers().get(reqwest::header::SET_COOKIE).is_none(),
            "console login with the metrics token: {} {:?}",
            login.status(),
            login.headers().get(reqwest::header::SET_COOKIE)
        );

        let boot = As {
            key: BOOT.into(),
            ..m.clone()
        };
        let (code, text) = boot
            .call(
                "POST",
                "/admin/keys",
                Some(&json!({ "tenant": "default", "role": "metrics" }).to_string()),
            )
            .await?;
        ensure!(code == 400, "metrics-role key in registry: {code} {text}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r?;

    for (bad, why) in [
        ("cf_short", "is too short"),
        (BOOT, "equals CAIRNFLOW_ADMIN_KEY"),
    ] {
        let bucket = common::bucket();
        match node_with_metrics(&bucket, Some(bad)).await {
            Ok(n) => {
                n.shutdown().await;
                anyhow::bail!("node started with metrics token {bad:?}");
            }
            Err(e) => ensure!(e.to_string().contains(why), "{bad:?}: {e}"),
        }
    }
    Ok(())
}

/// **SSE and the forwarding path across two nodes.**
///
/// 1. Build like [`alpha_cannot_reach_bravo`] (step 1) on node one.
/// 2. SSE: `bravo` opens stream `notes` of its agent invocation (control check —
///    really reads up to the marked record); `alpha` (key and SSO session) opens
///    the same stream.
/// 3. Node two comes up on the same bucket: it leads no range yet, so every
///    per-range path to it is **forwarded** to node one
///    (`x-cairn-forward: leader`, like the console behind a load balancer).
///    Wait until `GET /invocations/<alpha's>` through node two comes back with
///    `x-cairn-forwarded-by`.
/// 4. The REST matrix (step 3b) with `alpha`'s five identities through node
///    one, then through node two asking to be forwarded.
///
/// Must see: (2) `bravo` gets the record with its own marker; `alpha` gets
/// `404`, not a byte of `bravo`'s. (4) every code through node two equals the
/// code through node one (a range node two takes midway runs locally — the code
/// must still be equal); no response leaks `bravo`'s marker; `bravo`'s state
/// does not change.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sse_and_forwarding_keep_tenants_apart() -> Result<()> {
    let bucket = common::bucket();
    let w = World::start(&bucket).await?;
    let mut second: Option<RunningNode> = None;
    let r = within(Duration::from_secs(240), "SSE and forwarding", async {
        let (fx, a_own) = w.build().await?;
        let before = snapshot(&w.b, &fx).await?;
        // ── 2. SSE.
        let stream = format!("/invocations/{}/streams/notes", fx.art);
        let mut resp =
            w.b.http
                .get(format!("{}{stream}", w.b.url))
                .bearer_auth(&w.b.key)
                .header("accept", "text/event-stream")
                .send()
                .await?;
        ensure!(
            resp.status() == 200,
            "bravo opens its own stream: {}",
            resp.status()
        );
        let mut seen = String::new();
        within(
            common::TEST_DEADLINE,
            "bravo reads its own stream record",
            async {
                while !seen.contains(&format!("{MARK}-stream")) {
                    let chunk = resp
                        .chunk()
                        .await?
                        .ok_or_else(|| anyhow!("bravo's SSE closed before the record: {seen}"))?;
                    seen.push_str(&String::from_utf8_lossy(&chunk));
                }
                Ok(())
            },
        )
        .await?;
        drop(resp);
        for (via, client) in [("key", &w.a), ("sso", &w.a_sso)] {
            let (code, text) = client.call("GET", &stream, None).await?;
            ensure!(
                code == 404 && leaks(&text, &fx).is_none() && !text.contains(MARK),
                "alpha ({via}) opens bravo's stream: {code} {text}"
            );
        }
        // ── 3. Node two.
        // Do not wait for leadership: node one holds every range, node two only forwards.
        second = Some(
            cairn_server::node::start(config_on(&bucket, None))
                .await
                .map_err(|e| anyhow!("node two: {e}"))?,
        );
        let url2 = second.as_ref().map(|n| n.url()).unwrap_or_default();
        let via2 = |c: &As| As {
            url: url2.clone(),
            forward: true,
            ..c.clone()
        };
        let a2 = via2(&w.a);
        // Bounded by its own deadline (not `within`) so a timeout says what node two
        // last answered: forwarding refused, the registry not loaded yet, or the range
        // already served locally.
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            let last = match a2
                .http
                .get(format!("{}/invocations/{a_own}", a2.url))
                .bearer_auth(&a2.key)
                .header("x-cairn-forward", "leader")
                .send()
                .await
            {
                Ok(r) => {
                    let code = r.status();
                    let by = r.headers().get("x-cairn-forwarded-by").cloned();
                    if code == 200 && by.is_some() {
                        break;
                    }
                    let body = r.text().await.unwrap_or_default();
                    format!(
                        "{code}, forwarded-by {by:?}, {}",
                        body.chars().take(300).collect::<String>()
                    )
                }
                Err(e) => format!("request failed: {e}"),
            };
            ensure!(
                std::time::Instant::now() < deadline,
                "node two did not forward within 60 s; last answer: {last}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        // ── 4. The matrix across two nodes.
        let (mut table, mut bad) = (Vec::new(), Vec::new());
        let one = [
            ("admin", &w.a),
            ("invoke", &w.a_inv),
            ("sso-admin", &w.a_sso),
            ("sso-invoke", &w.a_sso_invoke),
            ("sso-view", &w.a_sso_view),
        ];
        let codes1 = matrix_pass(&one, &fx, &a_own, &mut table, &mut bad).await?;
        let two_owned: Vec<(&str, As)> = one.iter().map(|(n, c)| (*n, via2(c))).collect();
        let two: Vec<(&str, &As)> = two_owned.iter().map(|(n, c)| (*n, c)).collect();
        let codes2 = matrix_pass(&two, &fx, &a_own, &mut table, &mut bad).await?;
        for (k, c1) in &codes1 {
            let c2 = codes2.get(k).copied().unwrap_or(0);
            if *c1 != c2 {
                bad.push(format!("{k}: node one {c1}, through forwarding {c2}"));
            }
        }
        let after = snapshot(&w.b, &fx).await?;
        if after != before {
            bad.push(format!(
                "bravo's state changed:\n  before {before:?}\n  after  {after:?}"
            ));
        }
        if std::env::var("CAIRN_ISOLATION_TABLE").is_ok() {
            eprintln!("{}", table.join("\n"));
        }
        ensure!(
            bad.is_empty(),
            "{} violations through forwarding:\n{}",
            bad.len(),
            bad.join("\n")
        );
        Ok(())
    })
    .await;
    if let Some(n) = second {
        n.shutdown().await;
    }
    w.shutdown().await;
    r
}

// ─────────────────────────────────────── alpha's SDK holding bravo's ids

/// An object key of `tenant`'s `service` that falls into range `range`.
fn key_on(node: &RunningNode, tenant: &str, service: &str, range: u32, prefix: &str) -> String {
    let t = cairn_store::TenantId::new(tenant).expect("valid tenant");
    (0..10_000)
        .map(|i| format!("{prefix}-{i}"))
        .find(|k| node.cluster.partition_of(&t, service, k).0 == range)
        .unwrap_or_else(|| panic!("no key of {service} for {tenant} in range {range}"))
}

/// A hex id with the same partition key (first 8 bytes) as `id` — the same
/// target range —, the rest random: an invocation that does not exist right
/// where `id` is.
fn random_twin(id: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let salt = nanos ^ u128::from(N.fetch_add(1, Ordering::Relaxed)) << 64;
    format!(
        "{}{:032x}",
        &id[..16],
        salt.wrapping_mul(0x9e37_79b9_7f4a_7c15_f39c_c060_5ced_c835)
    )
}

/// The range of invocation `id` (hex).
fn range_of(node: &RunningNode, id: &str) -> u32 {
    let raw: Vec<u8> = (0..id.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&id[i..i + 2], 16).expect("hex id"))
        .collect();
    let raw: cairn_store::InvocationId = raw.try_into().expect("24-byte id");
    node.cluster.partition_of_id(&raw).0
}

/// The string output of `GET /invocations/{id}`.
async fn output_of(who: &As, id: &str) -> Result<String> {
    let v = who.json("GET", &format!("/invocations/{id}"), None).await?;
    let raw = match &v["output"] {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    Ok(serde_json::from_str::<String>(&raw).unwrap_or(raw))
}

/// A security review finding: `alpha`'s handler knows the invocation ids of
/// `bravo` and calls `attach`, `getOutput`, `signal` on them — the SDK path,
/// not through the ingress's `ensure_visible`. Runs two passes: probing in the
/// same range as the target (`task_attach`/`task_peek_output`,
/// `settle_signal_local` through the signal outbox) and in another range
/// (`Watch`/`DeliverSignal` messages through `/internal/inbound`,
/// `inbound_attach`/`inbound_peek`).
///
/// Interleaving, each pass:
///
/// 1. `bravo` runs object `Echo` (key in range P) to completion — its output
///    carries the marker — and object `Hold` (key in P) to `suspended` waiting
///    for [`HOLD_SIGNAL`]. Snapshot `Hold`'s journal and the number of SDK turns
///    `bravo` has received for it.
/// 2. `alpha` builds object `Fence` (key in P) suspended waiting for
///    [`HOLD_SIGNAL`] — a fence: a signal to it goes **after** every signal to
///    `Hold` from the same sender, in the same target range.
/// 3. `alpha` runs object `Probe` (key in P in the same-range pass, in Q ≠ P in
///    the other-range pass): `getOutput`/`attach` to `bravo`'s `Echo` and then
///    to a random id with the same partition key; the same with `Hold`; then
///    `signal` idx 1 (CANCEL), idx 17 (awakeable) and [`HOLD_SIGNAL`] to `Hold`;
///    finally [`HOLD_SIGNAL`] to `Fence`.
/// 4. Wait until `Probe` completes and `Fence` completes (the fence has passed).
///
/// Must see: each result with `bravo`'s id equals, byte for byte, the result
/// with the random id (the id in messages replaced by `<id>`); no marker of
/// `bravo`'s in `alpha`'s output; `Hold` is still `suspended`, its journal is
/// identical to step 1, and `bravo`'s SDK received no more of its turns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alpha_sdk_cannot_reach_bravo_by_id() -> Result<()> {
    let bucket = common::bucket();
    let node = node_on(&bucket).await?;
    let boot = As {
        url: node.url(),
        key: BOOT.into(),
        cookie: None,
        http: Client::new(),
        forward: false,
    };
    let (_, b_key) = key(&boot, "bravo", "admin").await?;
    let (_, a_key) = key(&boot, "alpha", "admin").await?;
    let b = As {
        key: b_key,
        ..boot.clone()
    };
    let a = As {
        key: a_key,
        ..boot.clone()
    };
    let echo = format!("\"{MARK}-out\"");
    let b_sdk = FakeSdk::builder()
        .object("Echo", "run", vec![Step::output(&echo)])
        .object("Hold", "run", vec![Step::Suspend, Step::output(&echo)])
        .spawn()
        .await?;
    let cell = || std::sync::Arc::new(std::sync::Mutex::new(String::new()));
    let (done, done_twin, held, held_twin, fence) = (cell(), cell(), cell(), cell(), cell());
    let a_sdk = FakeSdk::builder()
        .object(
            "Probe",
            "run",
            vec![
                Step::GetOutput(done.clone()),
                Step::GetOutput(done_twin.clone()),
                Step::Attach(done.clone()),
                Step::Attach(done_twin.clone()),
                Step::GetOutput(held.clone()),
                Step::GetOutput(held_twin.clone()),
                Step::Attach(held.clone()),
                Step::Attach(held_twin.clone()),
                Step::SignalIdx(held.clone(), 1),
                Step::SignalIdx(held.clone(), 17),
                Step::SendSignal(held.clone()),
                Step::SendSignal(fence.clone()),
                Step::OutputResults,
            ],
        )
        .object(
            "Fence",
            "run",
            vec![Step::Suspend, Step::output("\"fence\"")],
        )
        .spawn()
        .await?;
    b.json(
        "POST",
        "/deployments",
        Some(&json!({ "uri": b_sdk.url }).to_string()),
    )
    .await?;
    a.json(
        "POST",
        "/deployments",
        Some(&json!({ "uri": a_sdk.url }).to_string()),
    )
    .await?;

    let p = 3u32;
    let q = 6u32;
    let r = within(Duration::from_secs(120), "alpha's SDK probes bravo's ids", async {
        for (pass, probe_range) in [("same range", p), ("other range", q)] {
            // ── 1.
            let k = key_on(&node, "bravo", "Echo", p, &format!("e{probe_range}"));
            let done_id = b.invoke(&format!("/invoke/Echo/run?key={k}"), "\"x\"").await?;
            let k = key_on(&node, "bravo", "Hold", p, &format!("h{probe_range}"));
            let v = b
                .json("POST", &format!("/invoke/Hold/run/send?key={k}"), Some("\"x\""))
                .await?;
            let held_id = v["id"].as_str().unwrap_or_default().to_string();
            b.wait_status(&held_id, "suspended").await?;
            ensure!(
                range_of(&node, &done_id) == p && range_of(&node, &held_id) == p,
                "bravo's targets must be in range {p}"
            );
            let journal_before = b
                .call("GET", &format!("/invocations/{held_id}/journal"), None)
                .await?;
            let starts_before = b_sdk.starts_of("Hold", "run").len();
            // ── 2.
            let k = key_on(&node, "alpha", "Fence", p, &format!("f{probe_range}"));
            let v = a
                .json("POST", &format!("/invoke/Fence/run/send?key={k}"), Some("\"x\""))
                .await?;
            let fence_id = v["id"].as_str().unwrap_or_default().to_string();
            a.wait_status(&fence_id, "suspended").await?;
            // ── 3.
            *done.lock().unwrap() = done_id.clone();
            *done_twin.lock().unwrap() = random_twin(&done_id);
            *held.lock().unwrap() = held_id.clone();
            *held_twin.lock().unwrap() = random_twin(&held_id);
            *fence.lock().unwrap() = fence_id.clone();
            let k = key_on(&node, "alpha", "Probe", probe_range, &format!("p{probe_range}"));
            let probe = a.invoke(&format!("/invoke/Probe/run?key={k}"), "\"x\"").await?;
            ensure!(
                (range_of(&node, &probe) == p) == (probe_range == p),
                "{pass}: the probe turn is in range {} rather than {probe_range}",
                range_of(&node, &probe)
            );
            // ── 4.
            a.wait_status(&fence_id, "completed").await?;
            let out = output_of(&a, &probe).await?;
            let lines: Vec<String> = out
                .lines()
                .map(|l| {
                    [&done_id, &held_id, &*done_twin.lock().unwrap(), &*held_twin.lock().unwrap()]
                        .iter()
                        .fold(l.to_string(), |l, id| l.replace(id.as_str(), "<id>"))
                })
                .collect();
            ensure!(lines.len() == 8, "{pass}: output of the probe turn: {out:?}");
            for (i, pair) in lines.chunks(2).enumerate() {
                ensure!(
                    pair[0] == pair[1],
                    "{pass}: probe {i} with bravo's id differs from the random id:\n  bravo: {}\n  random: {}",
                    pair[0],
                    pair[1]
                );
            }
            ensure!(!out.contains(MARK), "{pass}: bravo's marker leaked: {out}");
            let st = b.json("GET", &format!("/invocations/{held_id}"), None).await?;
            ensure!(
                st["status"] == "suspended",
                "{pass}: bravo's Hold is no longer suspended: {st}"
            );
            let journal_after = b
                .call("GET", &format!("/invocations/{held_id}/journal"), None)
                .await?;
            ensure!(
                journal_after == journal_before,
                "{pass}: Hold's journal changed:\n  before {journal_before:?}\n  after  {journal_after:?}"
            );
            ensure!(
                b_sdk.starts_of("Hold", "run").len() == starts_before,
                "{pass}: bravo's SDK got more turns for Hold"
            );
        }
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}
