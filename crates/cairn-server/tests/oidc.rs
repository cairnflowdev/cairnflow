// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The cell as an OIDC relying party: the code + PKCE flow against
//! an in-process fake issuer, the v2 session cookie `__Host-cairn_ui`, the
//! `auth/oidc` + `auth/revoked` books through the `Provision` API with an ACK.
//!
//! The fake issuer ([`FakeIssuer`], `tests/common/oidc.rs`) is a real axum server on loopback: discovery,
//! JWKS (RS256, a fixed test key), `/authorize` auto-consenting for the "next user"
//! the test sets, `/token` checking `code`, `client_id`, `redirect_uri` and PKCE S256
//! like a control plane's provider (`oidc-provider`). Cookies are set by hand in the
//! header — the test checks the protocol, not the browser (`ui/e2e` checks that part).
//!
//! # Running against an external issuer
//!
//! `external_issuer` runs the same flow against a real issuer when the environment
//! variables are set (no `CAIRN_OIDC_ISSUER` ⇒ skipped):
//!
//! - `CAIRN_OIDC_ISSUER` — the issuer (compared verbatim with discovery and `iss`);
//! - `CAIRN_OIDC_CLIENT_ID` — a client created beforehand at the provider, a public client, PKCE
//!   S256, redirect exactly `http://localhost:${CAIRN_OIDC_PORT}/ui/login/oidc/callback`;
//! - `CAIRN_OIDC_TENANT` — the client's tenant (default = the client id);
//! - `CAIRN_OIDC_PORT` — the fixed port the node listens on (default 18480);
//! - `CAIRN_OIDC_LOGIN` — a program that logs the test user in: takes the authorize
//!   URL in `argv[1]`, does **not** follow the redirect back to the cell, prints the final
//!   callback URL to stdout (`…/ui/login/oidc/callback?code=…&state=…`);
//! - `CAIRN_OIDC_EXPECT_ROLE` — optional, the expected role after clamping (`admin`/`view`).
//!
//! ```sh
//! CAIRN_OIDC_ISSUER=… CAIRN_OIDC_CLIENT_ID=… CAIRN_OIDC_LOGIN='<your login program>' \
//!   cargo test -p cairn-server --test oidc external_issuer -- --nocapture
//! ```

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{anyhow, ensure, Context, Result},
    cairn_server::{
        auth::AuthConfig,
        node::{NodeConfig, NodeMode, RunningNode},
        ui::UiConfig,
    },
    common::oidc::*,
    reqwest::{header, StatusCode},
    serde_json::{json, Value},
    std::{sync::Arc, time::Duration},
};
const BOOT: &str = "cf_test_oidc_bootstrap_key_0123456789abcdef";
const SECRET: &str = "cf_test_cluster_secret_oidc_32_bytes";
const HOST: &str = "127.0.0.1";
const CLIENT: &str = "env_acme";
const TENANT: &str = "acme";

// ───────────────────────────────────────────────────────────── cell

fn auth_config() -> AuthConfig {
    AuthConfig {
        required: true,
        admin_key: Some(BOOT.into()),
        cluster_secret: Some(SECRET.into()),
        metrics_key: None,
        admin_key_sha256: None,
        provision_key_sha256: None,
    }
}

async fn start_node() -> Result<RunningNode> {
    let mut cfg = common::test_config(NodeMode::single(1));
    cfg.auth = auth_config();
    cfg.ui = UiConfig::default();
    common::start_led(cfg).await
}

/// An admin command with the bootstrap key. Returns (code, JSON).
async fn admin(
    node: &str,
    method: reqwest::Method,
    path: &str,
    body: Option<Value>,
) -> Result<(StatusCode, Value)> {
    let mut r = http()
        .request(method, format!("{node}{path}"))
        .bearer_auth(BOOT);
    if let Some(b) = body {
        r = r.json(&b);
    }
    let r = r.send().await?;
    let code = r.status();
    Ok((code, r.json().await.unwrap_or(Value::Null)))
}

/// The cell's OIDC configuration: the fake issuer, one client for `127.0.0.1` → `acme`.
async fn configure(node: &str, issuer: &str, max_role: &str) -> Result<()> {
    let (c, v) = admin(
        node,
        reqwest::Method::PUT,
        "/admin/oidc",
        Some(json!({"issuer": issuer, "displayName": "Cairnflow Cloud"})),
    )
    .await?;
    ensure!(c == StatusCode::NO_CONTENT, "PUT /admin/oidc: {c} {v}");
    let (c, v) = admin(
        node,
        reqwest::Method::PUT,
        &format!("/admin/oidc/clients/{CLIENT}"),
        Some(
            json!({"hostname": HOST, "tenant": TENANT, "maxRole": max_role,
            "portalUrl": "https://app.example.test/environments"}),
        ),
    )
    .await?;
    ensure!(c == StatusCode::NO_CONTENT, "PUT client: {c} {v}");
    Ok(())
}

/// One way to edit the fake issuer's "next user".
type UserEdit = Box<dyn Fn(&mut NextUser)>;

async fn whoami(node: &str, cookie: &str) -> Result<(StatusCode, Value)> {
    let r = http()
        .get(format!("{node}/auth/whoami"))
        .header(header::COOKIE, cookie)
        .send()
        .await?;
    let code = r.status();
    Ok((code, r.json().await.unwrap_or(Value::Null)))
}

/// Signs a v2 cookie with the cluster secret — as someone holding the cluster secret could.
fn forge(claims: &Value) -> String {
    use ring::hmac;
    let k0 = hmac::Key::new(hmac::HMAC_SHA256, SECRET.as_bytes());
    let derived = hmac::sign(&k0, b"cairn-ui-session/v2-oidc");
    let key = hmac::Key::new(hmac::HMAC_SHA256, derived.as_ref());
    let payload = claims.to_string();
    let hex = |b: &[u8]| b.iter().map(|x| format!("{x:02x}")).collect::<String>();
    format!(
        "__Host-cairn_ui={}.{}",
        hex(payload.as_bytes()),
        hex(hmac::sign(&key, payload.as_bytes()).as_ref())
    )
}

async fn with_node<F, Fut>(name: &str, f: F) -> Result<()>
where
    F: FnOnce(RunningNode, FakeIssuer) -> Fut,
    Fut: std::future::Future<Output = (RunningNode, Result<()>)>,
{
    let issuer = FakeIssuer::start().await?;
    let node = start_node().await?;
    let (node, r) = match tokio::time::timeout(common::TEST_DEADLINE, f(node, issuer)).await {
        Ok(out) => out,
        Err(_) => return Err(anyhow!("over the limit: {name}")),
    };
    node.shutdown().await;
    r
}

// ───────────────────────────────────────────────────────────── test

/// The code + PKCE flow end to end: authorize carries `S256`, `nonce`,
/// `state`, scope `openid email profile`, `redirect_uri` exactly the callback; the token
/// endpoint sees a matching `code_verifier`; a `__Host-` session cookie (`Secure`,
/// `HttpOnly`, `Strict`, `Path=/`, no `Domain`); `whoami` reports the tenant **of the
/// configuration**, role, user, `via: sso`, the portal link; logout clears both
/// kinds of cookie.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn code_flow_with_pkce() -> Result<()> {
    with_node("code flow", |node, issuer| async move {
        let r = async {
            let url = node.url();
            // Not configured ⇒ no SSO button; an SSO login lands back on the console with an error.
            let opts: Value = http()
                .get(format!("{url}/ui/login/options"))
                .send()
                .await?
                .json()
                .await?;
            ensure!(opts["sso"] == false, "{opts}");
            let r = http().get(format!("{url}/ui/login/oidc")).send().await?;
            ensure!(
                location(&r) == "/ui/login?sso_error=not_configured",
                "{}",
                location(&r)
            );

            configure(&url, &issuer.url, "admin").await?;
            let opts: Value = http()
                .get(format!("{url}/ui/login/options"))
                .send()
                .await?
                .json()
                .await?;
            ensure!(
                opts["sso"] == true
                    && opts["displayName"] == "Cairnflow Cloud"
                    && opts["portalUrl"] == "https://app.example.test/environments",
                "{opts}"
            );
            let l = login(&url, Some("/ui/invocations?status=failed")).await?;
            let q = issuer.state.lock().unwrap().last_authorize.clone();
            for (k, v) in [
                ("response_type", "code"),
                ("client_id", CLIENT),
                ("scope", "openid email profile"),
                ("code_challenge_method", "S256"),
            ] {
                ensure!(
                    q.get(k).map(String::as_str) == Some(v),
                    "authorize {k}: {q:?}"
                );
            }
            ensure!(
                q.get("redirect_uri") == Some(&format!("{url}/ui/login/oidc/callback")),
                "{q:?}"
            );
            ensure!(q.get("nonce").is_some_and(|n| n.len() >= 43) && q.contains_key("state"));
            ensure!(l.authorize.starts_with(&issuer.url), "{}", l.authorize);
            ensure!(l.landed == "/ui/invocations?status=failed", "{}", l.landed);
            let cookie = l.cookie.context("no session cookie")?;
            let (code, me) = whoami(&url, &cookie).await?;
            ensure!(code == StatusCode::OK, "{code} {me}");
            ensure!(
                me["tenant"] == TENANT
                    && me["role"] == "admin"
                    && me["via"] == "sso"
                    && me["bootstrap"] == false
                    && me["keyId"].is_null()
                    && me["user"]["sub"] == "user-1"
                    && me["user"]["email"] == "user-1@example.test"
                    && me["user"]["client"] == CLIENT
                    && me["sso"] == true
                    && me["portalUrl"] == "https://app.example.test/environments"
                    && me["sessionExpiresAt"].as_u64().is_some(),
                "{me}"
            );
            // The tenant's `admin` role: can read, cannot administer the cluster.
            let r = http()
                .get(format!("{url}/invocations"))
                .header(header::COOKIE, &cookie)
                .send()
                .await?;
            ensure!(r.status() == StatusCode::OK, "{}", r.status());
            let r = http()
                .get(format!("{url}/admin/oidc"))
                .header(header::COOKIE, &cookie)
                .send()
                .await?;
            ensure!(r.status() == StatusCode::FORBIDDEN, "{}", r.status());
            // Writing with a cookie still needs the same origin.
            let r = http()
                .post(format!("{url}/invocations/batch/cancel"))
                .header(header::COOKIE, &cookie)
                .header(header::ORIGIN, "http://evil.test")
                .json(&json!({"ids": []}))
                .send()
                .await?;
            ensure!(r.status() == StatusCode::FORBIDDEN, "CSRF: {}", r.status());
            // Logout clears both cookies.
            let r = http()
                .delete(format!("{url}/ui/session"))
                .header(header::ORIGIN, &url)
                .header(header::COOKIE, &cookie)
                .send()
                .await?;
            let cleared: Vec<_> = r
                .headers()
                .get_all(header::SET_COOKIE)
                .iter()
                .filter_map(|v| v.to_str().ok())
                .collect();
            ensure!(
                cleared
                    .iter()
                    .any(|c| c.starts_with("__Host-cairn_ui=;") && c.contains("Max-Age=0"))
                    && cleared.iter().any(|c| c.starts_with("cairn_ui=;")),
                "{cleared:?}"
            );
            Ok(())
        }
        .await;
        (node, r)
    })
    .await
}

/// Login CSRF: a callback whose `state` does not match the temporary cookie, with no
/// temporary cookie, or with another flow's temporary cookie ⇒ no session (`invalid_state`),
/// the token endpoint is not called.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrong_state_is_rejected() -> Result<()> {
    with_node("state", |node, issuer| async move {
        let r = async {
            let url = node.url();
            configure(&url, &issuer.url, "admin").await?;
            let (authorize, flow) = start_login(&url, None).await?;
            let r = http().get(&authorize).send().await?;
            let callback = location(&r);
            // The attacker: their code, the victim's temporary cookie (another flow).
            let (_, victim_flow) = start_login(&url, None).await?;
            for (cb, cookie) in [
                (callback.clone(), victim_flow.clone()),
                (callback.clone(), String::new()),
                (callback.replace("state=", "state=x"), flow.clone()),
            ] {
                let (c, landed) = finish_login(&cb, &cookie).await?;
                ensure!(
                    c.is_none() && landed == "/ui/login?sso_error=invalid_state",
                    "{landed}"
                );
            }
            ensure!(
                issuer.state.lock().unwrap().tokens == 0,
                "the token endpoint was called"
            );
            // The right cookie ⇒ passes.
            let (c, _) = finish_login(&callback, &flow).await?;
            ensure!(c.is_some());
            Ok(())
        }
        .await;
        (node, r)
    })
    .await
}

/// A bad `id_token`: another `nonce`, an `aud` other than the client, another `iss`, expired by
/// the clock (`exp` beyond the allowed skew), `iat` in the future, `cairn_tenant` differing
/// from the configuration, no `cairn_role` ⇒ no session. The IdP denying the user
/// (`access_denied`) ⇒ the console reports exactly that code.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bad_tokens_are_rejected() -> Result<()> {
    with_node("wrong token", |node, issuer| async move {
        let r = async {
            let url = node.url();
            configure(&url, &issuer.url, "admin").await?;
            let now = now_s();
            let cases: Vec<(&str, UserEdit)> = vec![
                (
                    "nonce",
                    Box::new(|u| {
                        u.overrides.insert("nonce".into(), "other".into());
                    }),
                ),
                (
                    "aud",
                    Box::new(|u| {
                        u.overrides.insert("aud".into(), "env_other".into());
                    }),
                ),
                (
                    "iss",
                    Box::new(|u| {
                        u.overrides.insert("iss".into(), "http://evil.test".into());
                    }),
                ),
                (
                    "exp",
                    Box::new(move |u| {
                        u.overrides.insert("exp".into(), (now - 120).into());
                    }),
                ),
                (
                    "iat",
                    Box::new(move |u| {
                        u.overrides.insert("iat".into(), (now + 600).into());
                    }),
                ),
                ("tenant", Box::new(|u| u.tenant_claim = Some("beta".into()))),
                ("role", Box::new(|u| u.role = None)),
                ("billing", Box::new(|u| u.role = Some("billing".into()))),
                (
                    "callback iss",
                    Box::new(|u| u.callback_iss = Some("http://evil.test".into())),
                ),
            ];
            for (what, edit) in cases {
                issuer.set_user("user-1", Some("admin"));
                issuer.edit_user(edit);
                let l = login(&url, None).await?;
                ensure!(
                    l.cookie.is_none() && l.landed == "/ui/login?sso_error=token_rejected",
                    "{what}: {}",
                    l.landed
                );
            }
            // A tenant claim **matching** the configuration ⇒ passes.
            issuer.set_user("user-1", Some("admin"));
            issuer.edit_user(|u| u.tenant_claim = Some(TENANT.into()));
            ensure!(login(&url, None).await?.cookie.is_some(), "tenant matches");
            issuer.edit_user(|u| u.deny = true);
            let l = login(&url, None).await?;
            ensure!(
                l.landed == "/ui/login?sso_error=access_denied",
                "{}",
                l.landed
            );
            Ok(())
        }
        .await;
        (node, r)
    })
    .await
}

/// The role from `cairn_role` is clamped by the client's `max_role` — at login and
/// at every check: a `cluster-admin` claim becomes `admin`; for a client capped at `view`,
/// `admin` becomes `view` and writes are blocked; lowering `max_role` after login
/// clamps an existing session at its next check.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn role_capped() -> Result<()> {
    with_node("role clamp", |node, issuer| async move {
        let r = async {
            let url = node.url();
            configure(&url, &issuer.url, "admin").await?;
            issuer.set_user("root", Some("cluster-admin"));
            let cookie = login(&url, None)
                .await?
                .cookie
                .context("cluster-admin claim")?;
            let (_, me) = whoami(&url, &cookie).await?;
            ensure!(me["role"] == "admin", "{me}");
            let r = http()
                .get(format!("{url}/cluster"))
                .header(header::COOKIE, &cookie)
                .send()
                .await?;
            ensure!(
                r.status() == StatusCode::FORBIDDEN,
                "cluster: {}",
                r.status()
            );

            // Lower the cap to `view`: the old session is clamped at once; a new session is `view` too.
            configure(&url, &issuer.url, "view").await?;
            let (_, me) = whoami(&url, &cookie).await?;
            ensure!(
                me["role"] == "view" && me["capabilities"] == json!(["read"]),
                "{me}"
            );
            issuer.set_user("user-1", Some("admin"));
            let cookie = login(&url, None).await?.cookie.context("view")?;
            let (_, me) = whoami(&url, &cookie).await?;
            ensure!(me["role"] == "view", "{me}");
            let r = http()
                .post(format!("{url}/invocations/batch/cancel"))
                .header(header::COOKIE, &cookie)
                .header(header::ORIGIN, &url)
                .json(&json!({"ids": []}))
                .send()
                .await?;
            ensure!(
                r.status() == StatusCode::FORBIDDEN,
                "view writes: {}",
                r.status()
            );
            // `invoke` under a `view` cap: no intersection ⇒ no session.
            issuer.set_user("user-2", Some("invoke"));
            ensure!(login(&url, None).await?.cookie.is_none(), "invoke ∩ view");
            Ok(())
        }
        .await;
        (node, r)
    })
    .await
}

/// A v2 cookie **correctly signed** with the cluster secret but carrying `cluster-admin` (or
/// `provision`) ⇒ refused: the signature is not the only thing guarding the highest privilege.
/// Signed the same way, a real client `admin` role ⇒ passes (a correctly forged cookie).
/// A correctly signed cookie sent to a hostname other than the client's ⇒ refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forged_cluster_admin_cookie_is_rejected() -> Result<()> {
    with_node("forged cookie", |node, issuer| async move {
        let r = async {
            let url = node.url();
            configure(&url, &issuer.url, "admin").await?;
            let now = now_s();
            let claims = |role: &str| {
                json!({"v": 2, "src": "oidc", "iss": issuer.url, "sub": "x", "email": null,
                    "name": null, "tenant": TENANT, "role": role, "client": CLIENT,
                    "iat": now, "exp": now + 600, "sid": "0123456789abcdef0123456789abcdef"})
            };
            let (code, me) = whoami(&url, &forge(&claims("admin"))).await?;
            ensure!(
                code == StatusCode::OK && me["role"] == "admin",
                "control: {code} {me}"
            );
            for role in ["cluster-admin", "provision", "metrics"] {
                let (code, me) = whoami(&url, &forge(&claims(role))).await?;
                ensure!(code == StatusCode::UNAUTHORIZED, "{role}: {code} {me}");
            }
            // A tenant other than the client's, or another `src`/`v`.
            let mut c = claims("admin");
            c["tenant"] = "default".into();
            ensure!(whoami(&url, &forge(&c)).await?.0 == StatusCode::UNAUTHORIZED);
            // Another hostname (`localhost` instead of `127.0.0.1`) ⇒ not the client's.
            let other = url.replace("127.0.0.1", "localhost");
            let (code, _) = whoami(&other, &forge(&claims("admin"))).await?;
            ensure!(code == StatusCode::UNAUTHORIZED, "other hostname: {code}");
            Ok(())
        }
        .await;
        (node, r)
    })
    .await
}

/// Removing a client ⇒ its sessions die at the next request (`DELETE` returns `204` once
/// every node has applied it); removing again ⇒ `204`. Revocation on a role change: the control
/// plane writes `auth/revoked` ⇒ old sessions die; that person logging in again before the mark ⇒ blocked
/// (`revoked`); other people of the same client are unaffected.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn removed_client_and_revocation_kill_sessions() -> Result<()> {
    with_node("remove, revoke", |node, issuer| async move {
        let r = async {
            let url = node.url();
            configure(&url, &issuer.url, "admin").await?;
            let alice = login(&url, None).await?.cookie.context("alice")?;
            issuer.set_user("bob", Some("view"));
            let bob = login(&url, None).await?.cookie.context("bob")?;

            let (c, v) = admin(
                &url,
                reqwest::Method::POST,
                "/admin/oidc/revoked",
                Some(json!({"client": CLIENT, "sub": "user-1", "beforeMs": now_s() * 1000})),
            )
            .await?;
            ensure!(c == StatusCode::NO_CONTENT, "revoke: {c} {v}");
            ensure!(
                whoami(&url, &alice).await?.0 == StatusCode::UNAUTHORIZED,
                "alice still alive"
            );
            ensure!(
                whoami(&url, &bob).await?.0 == StatusCode::OK,
                "bob hit as collateral"
            );
            issuer.set_user("user-1", Some("view"));
            let again = login(&url, None).await?;
            ensure!(
                again.cookie.is_none() && again.landed == "/ui/login?sso_error=revoked",
                "logged in again before the mark: {}",
                again.landed
            );
            // The book shows the revocation row, mark ≥ cell time + 60 s; resending does not slide it.
            let (_, book) = admin(&url, reqwest::Method::GET, "/admin/oidc", None).await?;
            let before = book["revoked"][0]["beforeMs"].as_u64().context("revoked")?;
            ensure!(before >= now_s() * 1000 + 50_000, "{book}");
            admin(
                &url,
                reqwest::Method::POST,
                "/admin/oidc/revoked",
                Some(json!({"client": CLIENT, "sub": "user-1", "beforeMs": before})),
            )
            .await?;
            let (_, book) = admin(&url, reqwest::Method::GET, "/admin/oidc", None).await?;
            ensure!(
                book["revoked"][0]["beforeMs"] == before,
                "the mark slid: {book}"
            );

            let del = |id: &str| {
                let (url, path) = (url.clone(), format!("/admin/oidc/clients/{id}"));
                async move { admin(&url, reqwest::Method::DELETE, &path, None).await }
            };
            let (c, _) = del(CLIENT).await?;
            ensure!(c == StatusCode::NO_CONTENT, "remove: {c}");
            ensure!(
                whoami(&url, &bob).await?.0 == StatusCode::UNAUTHORIZED,
                "bob after removal"
            );
            let (c, _) = del(CLIENT).await?;
            ensure!(c == StatusCode::NO_CONTENT, "remove again: {c}");
            Ok(())
        }
        .await;
        (node, r)
    })
    .await
}

/// The view tables of `/query` (`sys_metering`, `sys_handler_history`) call the ingress
/// router back in process with the question's identity: an SSO session must open the
/// inner call — it carries the effective `Host` of the original request, otherwise the
/// SSO cookie's hostname binding returns `401` and the question fails (the console's Usage
/// screen with an SSO session).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sso_session_reads_query_views() -> Result<()> {
    with_node("view tables", |node, issuer| async move {
        let r = async {
            let url = node.url();
            configure(&url, &issuer.url, "admin").await?;
            let cookie = login(&url, None).await?.cookie.context("session")?;
            for sql in [
                "SELECT count(*) AS n FROM sys_metering",
                "SELECT count(*) AS n FROM sys_handler_history",
            ] {
                let r = http()
                    .post(format!("{url}/query"))
                    .header(header::COOKIE, &cookie)
                    .header(header::ORIGIN, &url)
                    .json(&json!({"sql": sql}))
                    .send()
                    .await?;
                let code = r.status();
                let v: Value = r.json().await.unwrap_or(Value::Null);
                ensure!(code == StatusCode::OK, "{sql}: {code} {v}");
            }
            Ok(())
        }
        .await;
        (node, r)
    })
    .await
}

/// A `return_to` outside the origin (`//evil`, an absolute URL, `/\evil`) ⇒ dropped, back to
/// `/ui/`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn return_to_outside_origin_is_dropped() -> Result<()> {
    with_node("return_to", |node, issuer| async move {
        let r = async {
            let url = node.url();
            configure(&url, &issuer.url, "admin").await?;
            for bad in [
                "//evil.test/x",
                "https://evil.test/",
                "/\\evil.test",
                "javascript:alert(1)",
            ] {
                let l = login(&url, Some(bad)).await?;
                ensure!(
                    l.cookie.is_some() && l.landed == "/ui/",
                    "{bad}: {}",
                    l.landed
                );
            }
            Ok(())
        }
        .await;
        (node, r)
    })
    .await
}

/// The issuer is down ⇒ existing sessions and API keys keep working; only new logins
/// fail (`token_rejected` — discovery is still in RAM). A new node that never saw the
/// issuer ⇒ `issuer_unavailable`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn issuer_down_keeps_sessions_and_keys() -> Result<()> {
    with_node("issuer down", |node, issuer| async move {
        let r = async {
            let url = node.url();
            configure(&url, &issuer.url, "admin").await?;
            let cookie = login(&url, None).await?.cookie.context("session")?;
            issuer.set_down(true);
            ensure!(
                whoami(&url, &cookie).await?.0 == StatusCode::OK,
                "the session died with the issuer"
            );
            let (c, _) = admin(&url, reqwest::Method::GET, "/admin/oidc", None).await?;
            ensure!(c == StatusCode::OK, "API key: {c}");
            let (authorize, flow) = start_login(&url, None).await?;
            ensure!(authorize.starts_with(&issuer.url), "{authorize}");
            // The fake IdP is down: build the callback by hand as if the user had consented.
            let state = url::Url::parse(&authorize)?
                .query_pairs()
                .find(|(k, _)| k == "state")
                .map(|(_, v)| v.into_owned())
                .context("state")?;
            let (c, landed) = finish_login(
                &format!("{url}/ui/login/oidc/callback?code=x&state={state}"),
                &flow,
            )
            .await?;
            ensure!(
                c.is_none() && landed == "/ui/login?sso_error=token_rejected",
                "{landed}"
            );
            Ok(())
        }
        .await;
        (node, r)
    })
    .await?;
    // A new node, with the issuer down from the start.
    let issuer = FakeIssuer::start().await?;
    issuer.set_down(true);
    let node = start_node().await?;
    let r = async {
        let url = node.url();
        configure(&url, &issuer.url, "admin").await?;
        let r = http().get(format!("{url}/ui/login/oidc")).send().await?;
        ensure!(
            location(&r) == "/ui/login?sso_error=issuer_unavailable",
            "{}",
            location(&r)
        );
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}

/// Two nodes on one bucket, same cluster secret: a session logged in on node A works on
/// node B; removing the client on A returns `204` only once B has applied it ⇒ B refuses the session at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_nodes_share_sessions_and_revocations() -> Result<()> {
    let issuer = FakeIssuer::start().await?;
    let tweak = |cfg: &mut NodeConfig| {
        cfg.auth = auth_config();
        cfg.ui = UiConfig::default();
    };
    let (_bucket, nodes) =
        common::cluster_nodes_tweaked(&["oidc-a", "oidc-b"], 2, Duration::from_secs(3), tweak)
            .await?;
    let (a, b) = (nodes[0].node.url(), nodes[1].node.url());
    let r = common::within(common::TEST_DEADLINE, "two nodes", async {
        configure(&a, &issuer.url, "admin").await?;
        let cookie = login(&a, None).await?.cookie.context("session on A")?;
        // B may have no session yet when A writes — wait, bounded, until B accepts.
        let t0 = std::time::Instant::now();
        loop {
            let (code, me) = whoami(&b, &cookie).await?;
            if code == StatusCode::OK {
                ensure!(me["user"]["sub"] == "user-1", "{me}");
                break;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(10),
                "B does not accept A's session: {code}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let (c, v) = admin(
            &a,
            reqwest::Method::DELETE,
            &format!("/admin/oidc/clients/{CLIENT}"),
            None,
        )
        .await?;
        ensure!(c == StatusCode::NO_CONTENT, "remove on A: {c} {v}");
        ensure!(
            whoami(&b, &cookie).await?.0 == StatusCode::UNAUTHORIZED,
            "B has not applied the removal"
        );
        Ok(())
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// The admin API contract: no book ⇒ `issuer: null`; each field is checked (a
/// `javascript:` portal link, a role outside the tenant, a hostname with a port, `clientSecret`
/// not accepted, a TTL out of range); a duplicate hostname ⇒ `409`; a tenant key ⇒ `403`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_api_contract() -> Result<()> {
    with_node("admin API", |node, issuer| async move {
        let r = async {
            let url = node.url();
            let (c, v) = admin(&url, reqwest::Method::GET, "/admin/oidc", None).await?;
            ensure!(
                c == StatusCode::OK
                    && v == json!({"issuer": null, "sessionTtlMs": 3_600_000, "displayName": null,
                        "clients": [], "revoked": []}),
                "{c} {v}"
            );
            for body in [
                json!({"issuer": "ftp://x"}),
                json!({"issuer": issuer.url, "sessionTtlMs": 1}),
                json!({"issuer": issuer.url, "extra": 1}),
                json!({"issuer": issuer.url, "displayName": ""}),
                json!({"issuer": issuer.url, "displayName": "a\nb"}),
            ] {
                let (c, _) = admin(&url, reqwest::Method::PUT, "/admin/oidc", Some(body.clone())).await?;
                ensure!(c.is_client_error(), "{body}: {c}");
            }
            configure(&url, &issuer.url, "admin").await?;
            let put = |id: &str, body: Value| {
                let (url, path) = (url.clone(), format!("/admin/oidc/clients/{id}"));
                async move { admin(&url, reqwest::Method::PUT, &path, Some(body)).await }
            };
            for body in [
                json!({"hostname": "b.test", "tenant": "beta", "portalUrl": "javascript:alert(1)"}),
                json!({"hostname": "b.test", "tenant": "beta", "maxRole": "cluster-admin"}),
                json!({"hostname": "b.test", "tenant": "beta", "maxRole": "provision"}),
                json!({"hostname": "b.test:443", "tenant": "beta"}),
                json!({"hostname": "B.test", "tenant": "beta"}),
                json!({"hostname": "b.test", "tenant": "beta", "clientSecret": "s"}),
                json!({"hostname": "b.test", "tenant": "not valid"}),
            ] {
                let (c, _) = put("env_b", body.clone()).await?;
                ensure!(c.is_client_error(), "{body}: {c}");
            }
            let (c, _) = put("env_b", json!({"hostname": HOST, "tenant": "beta"})).await?;
            ensure!(c == StatusCode::CONFLICT, "duplicate hostname: {c}");
            let (c, _) = put("env_b", json!({"hostname": "b.test", "tenant": "beta"})).await?;
            ensure!(c == StatusCode::NO_CONTENT, "{c}");
            // A repeated PUT with the same content ⇒ 204 (ack of the current book).
            let (c, _) = put("env_b", json!({"hostname": "b.test", "tenant": "beta"})).await?;
            ensure!(c == StatusCode::NO_CONTENT, "{c}");
            let (_, v) = admin(&url, reqwest::Method::GET, "/admin/oidc", None).await?;
            ensure!(
                v["issuer"] == issuer.url.as_str()
                    && v["clients"][1] == json!({"clientId": "env_b", "hostname": "b.test",
                        "tenant": "beta", "maxRole": "admin", "portalUrl": null}),
                "{v}"
            );
            let (c, _) = admin(
                &url,
                reqwest::Method::POST,
                "/admin/oidc/revoked",
                Some(json!({"client": "env_b", "sub": "u", "beforeMs": now_s() * 1000 + 2 * 86_400_000})),
            )
            .await?;
            ensure!(c == StatusCode::BAD_REQUEST, "mark too far: {c}");
            // A tenant's `admin` key cannot change the OIDC configuration.
            let v: Value = http()
                .post(format!("{url}/admin/keys"))
                .bearer_auth(BOOT)
                .json(&json!({"tenant": TENANT, "role": "admin"}))
                .send()
                .await?
                .json()
                .await?;
            let key = v["secret"].as_str().context("key")?;
            let r = http()
                .put(format!("{url}/admin/oidc"))
                .bearer_auth(key)
                .json(&json!({"issuer": "http://evil.test"}))
                .send()
                .await?;
            ensure!(r.status() == StatusCode::FORBIDDEN, "{}", r.status());
            Ok(())
        }
        .await;
        (node, r)
    })
    .await
}

// ───────────────────────────────────────────── audit

/// The audit records of `tenant` (bootstrap key: may choose the tenant).
async fn audit_of(node: &str, tenant: &str) -> Result<Vec<Value>> {
    let (c, v) = admin(
        node,
        reqwest::Method::GET,
        &format!("/audit?tenant={tenant}"),
        None,
    )
    .await?;
    ensure!(c == StatusCode::OK, "GET /audit: {c} {v}");
    Ok(v["records"].as_array().cloned().unwrap_or_default())
}

/// The latest `intent`/`done` pair of `action` (the shared name stem).
fn pair<'a>(records: &'a [Value], action: &str) -> Option<(&'a Value, &'a Value)> {
    let mut intents: Vec<&Value> = records
        .iter()
        .filter(|r| r["kind"] == "intent" && r["action"] == action)
        .collect();
    intents.sort_by_key(|r| r["ts"].as_u64());
    intents.iter().rev().find_map(|i| {
        let stem = i["name"].as_str()?.strip_suffix(".intent")?;
        let done = records
            .iter()
            .find(|r| r["name"].as_str() == Some(&format!("{stem}.done")))?;
        Some((*i, done))
    })
}

/// **Console login/logout is in the tenant's audit**: both the SSO path and the key
/// path, intent before outcome after, like every action on an object.
///
/// 1. SSO user `user-1` logs in through client `env_acme` ⇒ a
///    `session.login` pair in **acme**'s audit (the client's tenant), the actor
///    `{type: user, id: sub, email, role, client}`, the target `{via: sso, client}`.
/// 2. That person logs out with their cookie ⇒ a `session.logout` pair, same
///    actor.
/// 3. The bootstrap key logs in with `<base>/session` ⇒ `session.login` in the
///    audit of the key's tenant (`default`), the actor is the key, the target `{via: key}`;
///    logout ⇒ `session.logout`.
/// 4. Logout without a cookie (or with a broken cookie) ⇒ still `204`, no record
///    (there is nobody to record).
///
/// Must see: each login/logout is exactly one pair, outcome `ok`, no
/// key or cookie leaked into a record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn console_sign_in_and_out_are_audited() -> Result<()> {
    with_node("login audit", |node, issuer| async move {
        let r = async {
            let url = node.url();
            configure(&url, &issuer.url, "admin").await?;
            let l = login(&url, None).await?;
            let cookie = l.cookie.context("SSO set no cookie")?;
            let a = audit_of(&url, TENANT).await?;
            let (i, d) =
                pair(&a, "session.login").context(format!("missing session.login: {a:#?}"))?;
            ensure!(i["outcome"] == "pending" && d["outcome"] == "ok", "{i} {d}");
            ensure!(
                i["actor"]
                    == json!({"type": "user", "id": "user-1", "sub": "user-1",
                        "email": "user-1@example.test", "role": "admin", "client": CLIENT}),
                "SSO actor: {}",
                i["actor"]
            );
            ensure!(
                i["target"] == json!({"via": "sso", "client": CLIENT}),
                "{}",
                i["target"]
            );
            ensure!(i["tenant"] == TENANT, "{i}");
            let r = http()
                .delete(format!("{url}/ui/session"))
                .header(header::ORIGIN, &url)
                .header(header::COOKIE, &cookie)
                .send()
                .await?;
            ensure!(r.status() == StatusCode::NO_CONTENT, "{}", r.status());
            let a = audit_of(&url, TENANT).await?;
            let (i, d) =
                pair(&a, "session.logout").context(format!("missing session.logout: {a:#?}"))?;
            ensure!(
                d["outcome"] == "ok"
                    && i["actor"]["type"] == "user"
                    && i["actor"]["id"] == "user-1",
                "{i} {d}"
            );

            // The key path.
            let r = http()
                .post(format!("{url}/ui/session"))
                .header(header::ORIGIN, &url)
                .json(&json!({"key": BOOT}))
                .send()
                .await?;
            ensure!(
                r.status() == StatusCode::OK,
                "key login status: {}",
                r.status()
            );
            let (key_cookie, _) =
                set_cookie(&r, "cairn_ui").context("the key login set no session cookie")?;
            let a = audit_of(&url, "default").await?;
            let (i, d) =
                pair(&a, "session.login").context(format!("missing key session.login: {a:#?}"))?;
            ensure!(
                d["outcome"] == "ok"
                    && i["actor"]["type"] == "key"
                    && i["actor"]["id"] == "bootstrap:cluster-admin"
                    && i["target"] == json!({"via": "key"}),
                "{i} {d}"
            );
            let text = serde_json::to_string(&a)?;
            ensure!(!text.contains(BOOT), "an audit record leaks the key");
            let r = http()
                .delete(format!("{url}/ui/session"))
                .header(header::ORIGIN, &url)
                .header(header::COOKIE, &key_cookie)
                .send()
                .await?;
            ensure!(r.status() == StatusCode::NO_CONTENT, "{}", r.status());
            let a = audit_of(&url, "default").await?;
            ensure!(
                pair(&a, "session.logout").is_some(),
                "missing key session.logout: {a:#?}"
            );

            // No cookie ⇒ nobody to record.
            let before = audit_of(&url, "default").await?.len();
            let r = http()
                .delete(format!("{url}/ui/session"))
                .header(header::ORIGIN, &url)
                .send()
                .await?;
            ensure!(r.status() == StatusCode::NO_CONTENT, "{}", r.status());
            ensure!(
                audit_of(&url, "default").await?.len() == before,
                "an anonymous logout was recorded"
            );
            let a = audit_of(&url, TENANT).await?;
            let text = serde_json::to_string(&a)?;
            ensure!(
                !text.contains(cookie.split('=').nth(1).unwrap_or("?")),
                "a record leaks the cookie"
            );
            Ok(())
        }
        .await;
        (node, r)
    })
    .await
}

/// **OIDC configuration writes are in the audit of the client's tenant** ("actions on the
/// `auth/oidc` object"): clients and session revocations belong to an
/// environment, so a control plane reading `GET /audit?tenant=<t>` must see them.
///
/// 1. `PUT /admin/oidc` (the issuer, cell-wide) ⇒ `oidc.config` in the actor's
///    audit (`default`).
/// 2. `PUT /admin/oidc/clients/env_acme {tenant: acme}` ⇒ `oidc.client.put`
///    in **acme**'s audit.
/// 3. `POST /admin/oidc/revoked {client: env_acme}` ⇒ `oidc.revoke` in acme.
/// 4. `DELETE /admin/oidc/clients/env_acme` ⇒ `oidc.client.remove` in acme
///    (the tenant is read from the book **before** the client disappears).
///
/// Must see: each command is one pair with outcome `ok`/`pending` in the right tenant, no
/// pair from 2–4 leaks into `default`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oidc_writes_are_audited_in_the_client_tenant() -> Result<()> {
    with_node("audit OIDC", |node, issuer| async move {
        let r = async {
            let url = node.url();
            configure(&url, &issuer.url, "admin").await?;
            let (c, v) = admin(
                &url,
                reqwest::Method::POST,
                "/admin/oidc/revoked",
                Some(json!({"client": CLIENT, "sub": "user-1", "beforeMs": now_s() * 1000})),
            )
            .await?;
            ensure!(c.is_success(), "revoke: {c} {v}");
            let (c, v) = admin(
                &url,
                reqwest::Method::DELETE,
                &format!("/admin/oidc/clients/{CLIENT}"),
                None,
            )
            .await?;
            ensure!(c.is_success(), "remove client: {c} {v}");
            let own = audit_of(&url, "default").await?;
            let acme = audit_of(&url, TENANT).await?;
            ensure!(pair(&own, "oidc.config").is_some(), "oidc.config: {own:#?}");
            for action in ["oidc.client.put", "oidc.revoke", "oidc.client.remove"] {
                let (i, d) =
                    pair(&acme, action).context(format!("{action} not in acme: {acme:#?}"))?;
                ensure!(
                    matches!(d["outcome"].as_str(), Some("ok" | "pending")),
                    "{action}: {i} {d}"
                );
                ensure!(pair(&own, action).is_none(), "{action} leaked into default");
            }
            let (i, _) = pair(&acme, "oidc.client.remove").context("remove")?;
            ensure!(i["target"]["id"] == CLIENT, "{i}");
            Ok(())
        }
        .await;
        (node, r)
    })
    .await
}

/// **No intent written ⇒ no login** ("there is never an action without an
/// intent"), but logout still works (giving up privilege is never
/// blocked by the audit).
///
/// 1. The main store fails every PUT under `audit/tenant/`.
/// 2. SSO login ⇒ lands on `<base>/login?sso_error=unavailable`, no cookie.
/// 3. Key login ⇒ `503`, no cookie.
/// 4. Logout with an existing SSO cookie ⇒ `204`, the cookie is cleared.
/// 5. Remove the fault ⇒ SSO login works again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sign_in_needs_the_audit_intent() -> Result<()> {
    use cairn_store::{
        testkit::{Action, FaultyStore, OpSet, PathMatch, Rule},
        ObjectStores, StoreLocation,
    };
    let issuer = FakeIssuer::start().await?;
    let store = FaultyStore::wrap(Arc::new(slatedb::object_store::memory::InMemory::new()));
    let mut cfg = common::test_config(NodeMode::single(1));
    cfg.stores = ObjectStores::single(StoreLocation::from_store(store.clone(), "", "memory://"));
    cfg.auth = auth_config();
    cfg.ui = UiConfig::default();
    let node = common::start_led(cfg).await?;
    let r = async {
        let url = node.url();
        configure(&url, &issuer.url, "admin").await?;
        let cookie = login(&url, None).await?.cookie.context("first login")?;
        let rule = store.add_rule(Rule::new(
            OpSet::PUT,
            PathMatch::Contains("audit/tenant/".into()),
            Action::Fail { times: None },
        ));
        let l = login(&url, None).await?;
        ensure!(
            l.cookie.is_none() && l.landed.ends_with("/ui/login?sso_error=unavailable"),
            "SSO login without an intent: {:?} {}",
            l.cookie,
            l.landed
        );
        let r = http()
            .post(format!("{url}/ui/session"))
            .header(header::ORIGIN, &url)
            .json(&json!({"key": BOOT}))
            .send()
            .await?;
        ensure!(
            r.status() == StatusCode::SERVICE_UNAVAILABLE && set_cookie(&r, "cairn_ui").is_none(),
            "key login without an intent: {}",
            r.status()
        );
        let r = http()
            .delete(format!("{url}/ui/session"))
            .header(header::ORIGIN, &url)
            .header(header::COOKIE, &cookie)
            .send()
            .await?;
        ensure!(
            r.status() == StatusCode::NO_CONTENT,
            "logout: {}",
            r.status()
        );
        ensure!(
            r.headers()
                .get_all(header::SET_COOKIE)
                .iter()
                .any(|v| v.to_str().is_ok_and(|v| v.starts_with("__Host-cairn_ui=;"))),
            "logout did not clear the cookie"
        );
        store.remove_rule(rule);
        ensure!(
            login(&url, None).await?.cookie.is_some(),
            "fault removed yet login still fails"
        );
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}

/// The contract with a real issuer: see the top-of-file doc. Variables missing ⇒ skipped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_issuer() -> Result<()> {
    let Ok(issuer) = std::env::var("CAIRN_OIDC_ISSUER") else {
        eprintln!("skipped: CAIRN_OIDC_ISSUER not set");
        return Ok(());
    };
    let client = std::env::var("CAIRN_OIDC_CLIENT_ID").context("CAIRN_OIDC_CLIENT_ID")?;
    let tenant = std::env::var("CAIRN_OIDC_TENANT").unwrap_or_else(|_| client.clone());
    let port: u16 = std::env::var("CAIRN_OIDC_PORT")
        .ok()
        .map(|p| p.parse())
        .transpose()?
        .unwrap_or(18480);
    let login_cmd = std::env::var("CAIRN_OIDC_LOGIN").context("CAIRN_OIDC_LOGIN")?;
    let mut cfg = common::test_config(NodeMode::single(1));
    cfg.auth = auth_config();
    cfg.ui = UiConfig::default();
    cfg.host = "127.0.0.1".into();
    cfg.port = port;
    let node = common::start_led(cfg).await?;
    let url = format!("http://localhost:{port}");
    let r = async {
        let (c, v) = admin(
            &url,
            reqwest::Method::PUT,
            "/admin/oidc",
            Some(json!({"issuer": issuer})),
        )
        .await?;
        ensure!(c == StatusCode::NO_CONTENT, "{c} {v}");
        let (c, v) = admin(
            &url,
            reqwest::Method::PUT,
            &format!("/admin/oidc/clients/{client}"),
            Some(json!({"hostname": "localhost", "tenant": tenant, "maxRole": "admin"})),
        )
        .await?;
        ensure!(c == StatusCode::NO_CONTENT, "{c} {v}");
        let (authorize, flow) = start_login(&url, None).await?;
        ensure!(
            authorize.contains("code_challenge_method=S256"),
            "{authorize}"
        );
        let out = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(format!("{login_cmd} \"$1\""))
            .arg("sh")
            .arg(&authorize)
            .output()
            .await?;
        ensure!(
            out.status.success(),
            "CAIRN_OIDC_LOGIN failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let callback = String::from_utf8(out.stdout)?.trim().to_string();
        ensure!(
            callback.starts_with(&format!("{url}/ui/login/oidc/callback?")),
            "CAIRN_OIDC_LOGIN must print the callback URL, printed: {callback}"
        );
        let (cookie, landed) = finish_login(&callback, &flow).await?;
        let cookie = cookie.with_context(|| format!("no session, landed on {landed}"))?;
        let (code, me) = whoami(&url, &cookie).await?;
        ensure!(
            code == StatusCode::OK && me["tenant"] == tenant.as_str() && me["via"] == "sso",
            "{code} {me}"
        );
        if let Ok(role) = std::env::var("CAIRN_OIDC_EXPECT_ROLE") {
            ensure!(me["role"] == role.as_str(), "{me}");
        }
        eprintln!("external_issuer: {me}");
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}

// ─────────────────────────────── login-flow flooding

/// A node trusting a loopback proxy: `X-Forwarded-For` sets the client IP of each request.
async fn start_proxied_node() -> Result<RunningNode> {
    let mut cfg = common::test_config(NodeMode::single(1));
    cfg.auth = auth_config();
    cfg.ui = UiConfig {
        trust_proxy: cairn_server::auth::TrustedProxies::parse("127.0.0.1")
            .map_err(|e| anyhow!(e))?,
        ..UiConfig::default()
    };
    common::start_led(cfg).await
}

/// One SSO login from client `ip` (through a trusted proxy): the code of
/// `/login/oidc`, then (on `303`) the code and `Location` of the callback.
async fn login_from(node: &str, ip: &str) -> Result<(StatusCode, Option<(StatusCode, String)>)> {
    let r = http()
        .get(format!("{node}/ui/login/oidc"))
        .header("x-forwarded-for", ip)
        .send()
        .await?;
    if r.status() != StatusCode::SEE_OTHER {
        return Ok((r.status(), None));
    }
    let (flow, _) = set_cookie(&r, "__Host-cairn_oidc").context("missing temporary cookie")?;
    let callback = location(&http().get(location(&r)).send().await?);
    let r = http()
        .get(&callback)
        .header(header::COOKIE, &flow)
        .header("x-forwarded-for", ip)
        .send()
        .await?;
    Ok((StatusCode::SEE_OTHER, Some((r.status(), location(&r)))))
}

/// **A replayed callback.** One successful login; someone holding the
/// callback URL and the flow cookie resends exactly that pair twice more.
///
/// Must see: the replays land on `invalid_state`, no session; the issuer's token endpoint
/// receives **exactly one** request — the flow cookie is signed statelessly but
/// `state` is single-use, and is refused **before** `exchange`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replayed_callback_reaches_the_issuer_once() -> Result<()> {
    with_node("callback replay", |node, issuer| async move {
        let r = async {
            let url = node.url();
            configure(&url, &issuer.url, "admin").await?;
            let (authorize, flow) = start_login(&url, None).await?;
            let callback = location(&http().get(&authorize).send().await?);
            let (c, _) = finish_login(&callback, &flow).await?;
            ensure!(c.is_some(), "the first login must succeed");
            for i in 0..2 {
                let (c, landed) = finish_login(&callback, &flow).await?;
                ensure!(
                    c.is_none() && landed == "/ui/login?sso_error=invalid_state",
                    "replay {i}: {landed}"
                );
            }
            let tokens = issuer.state.lock().unwrap().tokens;
            ensure!(tokens == 1, "the token endpoint received {tokens} requests");
            Ok(())
        }
        .await;
        (node, r)
    })
    .await
}

/// **The `exchange` limit of an OIDC client (per client).** 40
/// clients with different IPs (one login per IP, no IP hitting its own limit) flood
/// logins into one client, then a real user from another IP.
///
/// Must see: the issuer receives no more than 20 + (seconds elapsed) + 1 `exchange`s;
/// some callbacks get `429` with `Retry-After`; the real user can log in once the
/// node has refilled (retrying per `Retry-After`, bounded).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exchange_flood_is_capped_per_node() -> Result<()> {
    let issuer = FakeIssuer::start().await?;
    let node = start_proxied_node().await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(60), "exchange flood", async {
        configure(&url, &issuer.url, "admin").await?;
        let t0 = std::time::Instant::now();
        let mut limited = 0;
        for i in 0..40 {
            let (start, cb) = login_from(&url, &format!("198.51.100.{}", i + 1)).await?;
            ensure!(start == StatusCode::SEE_OTHER, "start {i}: {start}");
            let (code, _) = cb.context("callback")?;
            if code == StatusCode::TOO_MANY_REQUESTS {
                limited += 1;
            }
        }
        let tokens = issuer.state.lock().unwrap().tokens;
        let cap = 20 + t0.elapsed().as_secs() as u32 + 1;
        ensure!(tokens <= cap, "issuer got {tokens} exchanges, cap {cap}");
        ensure!(limited > 0, "no callback got 429");
        // The real user, another IP: retry until the node refills.
        loop {
            let (start, cb) = login_from(&url, "203.0.113.9").await?;
            ensure!(
                start == StatusCode::SEE_OTHER,
                "real user's login start: {start}"
            );
            match cb.context("callback")? {
                (StatusCode::SEE_OTHER, landed) if landed == "/ui/" => return Ok(()),
                (StatusCode::TOO_MANY_REQUESTS, _) => {
                    tokio::time::sleep(Duration::from_millis(500)).await
                }
                other => anyhow::bail!("real user: {other:?}"),
            }
        }
    })
    .await;
    node.shutdown().await;
    r
}

/// Like [`login_from`], for hostname `host` (through the trusted proxy's `X-Forwarded-Host`):
/// the issuer's callback points back to `host`, so send it to the node with the same
/// header.
async fn login_as(
    node: &str,
    ip: &str,
    host: &str,
) -> Result<(StatusCode, Option<(StatusCode, String)>)> {
    let r = http()
        .get(format!("{node}/ui/login/oidc"))
        .header("x-forwarded-for", ip)
        .header("x-forwarded-host", host)
        .send()
        .await?;
    if r.status() != StatusCode::SEE_OTHER {
        return Ok((r.status(), None));
    }
    let (flow, _) = set_cookie(&r, "__Host-cairn_oidc").context("missing temporary cookie")?;
    let callback = location(&http().get(location(&r)).send().await?);
    let path = callback
        .split_once(host)
        .map(|(_, p)| p.to_string())
        .context("the callback does not point to the client's hostname")?;
    let r = http()
        .get(format!("{node}{path}"))
        .header(header::COOKIE, &flow)
        .header("x-forwarded-for", ip)
        .header("x-forwarded-host", host)
        .send()
        .await?;
    Ok((StatusCode::SEE_OTHER, Some((r.status(), location(&r)))))
}

/// **The `exchange` limit is per OIDC client** (security review): the node's `exchange`
/// limit used to be **one** bucket for every tenant, so a few dozen IPs (one
/// /56 is 256 /64s) flooding logins into one tenant's client pushed the SSO of every
/// tenant on the node into `429`.
///
/// Order: two OIDC clients on one node — `env_acme` (hostname `127.0.0.1`)
/// and `env_bravo` (hostname `bravo.example.test`, through `X-Forwarded-Host`); 40
/// different IPs flood logins into `env_acme`; right after, one user
/// logs in to `env_bravo`.
///
/// Must see: `env_acme` has callbacks getting `429`; the `env_bravo` user gets in
/// on the first try (`303` to `/ui/`), no `429`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exchange_flood_on_one_client_spares_the_others() -> Result<()> {
    let issuer = FakeIssuer::start().await?;
    let node = start_proxied_node().await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(60), "flood on one client", async {
        configure(&url, &issuer.url, "admin").await?;
        let (c, v) = admin(
            &url,
            reqwest::Method::PUT,
            "/admin/oidc/clients/env_bravo",
            Some(json!({"hostname": "bravo.example.test", "tenant": "bravo",
                "maxRole": "admin", "portalUrl": "https://app.example.test/environments"})),
        )
        .await?;
        ensure!(c == StatusCode::NO_CONTENT, "PUT client bravo: {c} {v}");
        let mut limited = 0;
        for i in 0..40 {
            let (start, cb) = login_from(&url, &format!("198.51.100.{}", i + 1)).await?;
            ensure!(start == StatusCode::SEE_OTHER, "start {i}: {start}");
            if cb.context("callback")?.0 == StatusCode::TOO_MANY_REQUESTS {
                limited += 1;
            }
        }
        ensure!(limited > 0, "client acme: no callback got 429");
        let (start, cb) = login_as(&url, "203.0.113.9", "bravo.example.test").await?;
        ensure!(start == StatusCode::SEE_OTHER, "bravo start: {start}");
        let (code, landed) = cb.context("callback bravo")?;
        ensure!(
            code == StatusCode::SEE_OTHER && landed == "/ui/",
            "bravo's user blocked because of client acme: {code} {landed}"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **A per-IP limit on the SSO flow.** One client floods `/login/oidc`;
/// different IPv6 addresses in the same /64 count as one client.
///
/// Must see: the first 20 get `303`, the 21st `429` + `Retry-After`; the 21st from another
/// address **in the same /64** also `429`; an address in another /64 still `303`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn login_start_is_limited_per_client_and_per_v6_64() -> Result<()> {
    let issuer = FakeIssuer::start().await?;
    let node = start_proxied_node().await?;
    let url = node.url();
    let r = common::within(common::TEST_DEADLINE, "per-IP limit", async {
        configure(&url, &issuer.url, "admin").await?;
        let start = |ip: String| {
            let url = url.clone();
            async move {
                let r = http()
                    .get(format!("{url}/ui/login/oidc"))
                    .header("x-forwarded-for", ip)
                    .send()
                    .await?;
                anyhow::Ok((r.status(), r.headers().contains_key(header::RETRY_AFTER)))
            }
        };
        for i in 0..20 {
            let (code, _) = start(format!("2001:db8:1:2::{:x}", i + 1)).await?;
            ensure!(code == StatusCode::SEE_OTHER, "attempt {i}: {code}");
        }
        let (code, retry) = start("2001:db8:1:2:ffff::1".into()).await?;
        ensure!(
            code == StatusCode::TOO_MANY_REQUESTS && retry,
            "attempt 21, same /64: {code} retry-after={retry}"
        );
        let (code, _) = start("2001:db8:1:3::1".into()).await?;
        ensure!(code == StatusCode::SEE_OTHER, "other /64: {code}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// One SSO login from `ip` whose callback carries a junk `code` instead of the
/// issuer's: the node holds a valid flow, so it calls the token endpoint, which
/// refuses. Returns the callback's status and `Location`.
async fn junk_code_from(node: &str, ip: &str) -> Result<(StatusCode, String)> {
    let r = http()
        .get(format!("{node}/ui/login/oidc"))
        .header("x-forwarded-for", ip)
        .send()
        .await?;
    ensure!(r.status() == StatusCode::SEE_OTHER, "start: {}", r.status());
    let (flow, _) = set_cookie(&r, "__Host-cairn_oidc").context("missing temporary cookie")?;
    let mut callback = url::Url::parse(&location(&http().get(location(&r)).send().await?))?;
    let pairs: Vec<(String, String)> = callback
        .query_pairs()
        .map(|(k, v)| {
            let v = if k == "code" {
                "junk".into()
            } else {
                v.into_owned()
            };
            (k.into_owned(), v)
        })
        .collect();
    callback.query_pairs_mut().clear().extend_pairs(pairs);
    let r = http()
        .get(callback.as_str())
        .header(header::COOKIE, &flow)
        .header("x-forwarded-for", ip)
        .send()
        .await?;
    Ok((r.status(), location(&r)))
}

/// **Junk codes from a few IPs do not lock a client's users out** (security
/// review). Each callback with a valid flow costs one slot of the client's
/// `exchange` cap before the issuer says whether the code is real, so:
///
/// 1. six IPs each start a login and send the callback with a junk `code`, eight
///    times (under their own per-IP caps of 20 starts and 10 failed callbacks);
/// 2. every one of those 48 callbacks used to take a slot of the client's cap
///    (burst 20, one a second), which was then empty;
/// 3. a real user from another IP logs in right after.
///
/// Must see: the attackers' callbacks end on `token_rejected` or `429`; the real
/// user gets in on the first try (`303` to `/ui/`). An IP whose callback failed
/// draws on its own lane of the client's cap, apart from IPs with no failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn junk_codes_from_a_few_ips_do_not_lock_out_the_client() -> Result<()> {
    let issuer = FakeIssuer::start().await?;
    let node = start_proxied_node().await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(60), "junk codes", async {
        configure(&url, &issuer.url, "admin").await?;
        for round in 0..8 {
            for i in 0..6 {
                let (code, landed) = junk_code_from(&url, &format!("198.51.100.{}", i + 1)).await?;
                ensure!(
                    code == StatusCode::TOO_MANY_REQUESTS
                        || landed == "/ui/login?sso_error=token_rejected",
                    "attacker {i}, round {round}: {code} {landed}"
                );
            }
        }
        let (start, cb) = login_from(&url, "203.0.113.9").await?;
        ensure!(start == StatusCode::SEE_OTHER, "real user's start: {start}");
        let (code, landed) = cb.context("callback")?;
        ensure!(
            code == StatusCode::SEE_OTHER && landed == "/ui/",
            "the real user was locked out by junk codes: {code} {landed}"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **A user's own failed callbacks do not push the next user of that IP onto the
/// drained lane** (security review of the two-lane `exchange` cap). Real users
/// behind one NAT share an IP; a callback that fails before any exchange (a stale
/// link, a login cancelled at the issuer) says nothing about junk codes:
///
/// 1. from the office IP, a stale callback without a flow cookie, then a login
///    cancelled at the issuer (`error=access_denied` with a valid flow);
/// 2. six other IPs send junk codes until the client's other lane is empty;
/// 3. a colleague logs in from the office IP.
///
/// Must see: the colleague gets in on the first try (`303` to `/ui/`). Only an
/// exchange takes one of the IP's clean-lane tickets.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failures_before_an_exchange_keep_the_ips_clean_lane() -> Result<()> {
    let issuer = FakeIssuer::start().await?;
    let node = start_proxied_node().await?;
    let url = node.url();
    let office = "203.0.113.9";
    let r = common::within(Duration::from_secs(60), "office login", async {
        configure(&url, &issuer.url, "admin").await?;
        let r = http()
            .get(format!("{url}/ui/login/oidc/callback?code=x&state=y"))
            .header("x-forwarded-for", office)
            .send()
            .await?;
        ensure!(
            location(&r) == "/ui/login?sso_error=invalid_state",
            "stale link: {}",
            r.status()
        );
        let r = http()
            .get(format!("{url}/ui/login/oidc"))
            .header("x-forwarded-for", office)
            .send()
            .await?;
        let (flow, _) = set_cookie(&r, "__Host-cairn_oidc").context("missing temporary cookie")?;
        let state = url::Url::parse(&location(&r))?
            .query_pairs()
            .find(|(k, _)| k == "state")
            .map(|(_, v)| v.into_owned())
            .context("no state")?;
        let r = http()
            .get(format!(
                "{url}/ui/login/oidc/callback?error=access_denied&state={state}"
            ))
            .header(header::COOKIE, &flow)
            .header("x-forwarded-for", office)
            .send()
            .await?;
        ensure!(
            location(&r) == "/ui/login?sso_error=access_denied",
            "cancelled login: {}",
            location(&r)
        );
        for round in 0..8 {
            for i in 0..6 {
                let (code, landed) = junk_code_from(&url, &format!("198.51.100.{}", i + 1)).await?;
                ensure!(
                    code == StatusCode::TOO_MANY_REQUESTS
                        || landed == "/ui/login?sso_error=token_rejected",
                    "attacker {i}, round {round}: {code} {landed}"
                );
            }
        }
        let (start, cb) = login_from(&url, office).await?;
        ensure!(start == StatusCode::SEE_OTHER, "colleague's start: {start}");
        let (code, landed) = cb.context("callback")?;
        ensure!(
            code == StatusCode::SEE_OTHER && landed == "/ui/",
            "the colleague was pushed onto the drained lane: {code} {landed}"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **Broken callbacks are capped per client.** One client sends
/// junk callbacks (no flow cookie); another client logs in for real many times.
///
/// Must see: the same client's 11th junk callback ⇒ `429` + `Retry-After`;
/// 12 **successful** logins from another client are not blocked — users
/// behind a shared NAT do not eat each other's quota.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_callbacks_are_limited_per_client_not_successful_ones() -> Result<()> {
    let issuer = FakeIssuer::start().await?;
    let node = start_proxied_node().await?;
    let url = node.url();
    let r = common::within(common::TEST_DEADLINE, "broken callback limit", async {
        configure(&url, &issuer.url, "admin").await?;
        let junk = || async {
            let r = http()
                .get(format!("{url}/ui/login/oidc/callback?code=x&state=y"))
                .header("x-forwarded-for", "198.51.100.66")
                .send()
                .await?;
            anyhow::Ok((r.status(), r.headers().contains_key(header::RETRY_AFTER)))
        };
        for i in 0..10 {
            let (code, _) = junk().await?;
            ensure!(code == StatusCode::SEE_OTHER, "junk callback {i}: {code}");
        }
        let (code, retry) = junk().await?;
        ensure!(
            code == StatusCode::TOO_MANY_REQUESTS && retry,
            "11th junk callback: {code} retry-after={retry}"
        );
        for i in 0..12 {
            let (start, cb) = login_from(&url, "203.0.113.50").await?;
            ensure!(start == StatusCode::SEE_OTHER, "login {i}: {start}");
            let (code, landed) = cb.context("callback")?;
            ensure!(
                code == StatusCode::SEE_OTHER && landed == "/ui/",
                "real login {i}: {code} {landed}"
            );
        }
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **An SSO logout revokes exactly that session.** Formerly
/// `DELETE <base>/session` only cleared the cookie in the browser: a cookie captured before
/// the logout (a shared machine, a proxy log) kept opening the API until it expired (up to
/// 24 h with SSO).
///
/// 1. Log in with SSO twice ⇒ two cookies (two sessions of the same person).
/// 2. Log out with the first cookie.
///
/// Must see: the first cookie gets `401` at once; the second still `200` (revocation is per
/// session, not per person); logging in again right after works (no floor mark).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sso_logout_revokes_that_session_only() -> Result<()> {
    with_node("SSO logout", |node, issuer| async move {
        let r = async {
            let url = node.url();
            configure(&url, &issuer.url, "admin").await?;
            let first = login(&url, None)
                .await?
                .cookie
                .context("SSO set no cookie")?;
            let second = login(&url, None)
                .await?
                .cookie
                .context("SSO set no cookie")?;
            for c in [&first, &second] {
                let (code, v) = whoami(&url, c).await?;
                ensure!(code == StatusCode::OK, "before logout: {code} {v}");
            }
            let r = http()
                .delete(format!("{url}/ui/session"))
                .header(header::ORIGIN, &url)
                .header(header::COOKIE, &first)
                .send()
                .await?;
            ensure!(r.status() == StatusCode::NO_CONTENT, "{}", r.status());
            let (code, v) = whoami(&url, &first).await?;
            ensure!(
                code == StatusCode::UNAUTHORIZED,
                "a logged-out cookie still opens the session: {code} {v}"
            );
            let (code, v) = whoami(&url, &second).await?;
            ensure!(
                code == StatusCode::OK,
                "another session of the same person died too: {code} {v}"
            );
            let again = login(&url, None).await?.cookie.context("log in again")?;
            let (code, v) = whoami(&url, &again).await?;
            ensure!(
                code == StatusCode::OK,
                "logging in again right after logout: {code} {v}"
            );
            Ok(())
        }
        .await;
        (node, r)
    })
    .await
}
