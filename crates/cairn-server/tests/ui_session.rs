// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The web console session: exchange an API key for a stateless `HttpOnly`
//! cookie, `GET /auth/whoami`, CSRF protection for cookie requests.
//!
//! A real in-process node, auth enabled with the bootstrap key and a fixed
//! cluster secret. Cookies are set by hand in the header (the client has no
//! cookie jar) — the test checks the protocol, not the browser.

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

mod common;

use {
    anyhow::{anyhow, ensure, Result},
    cairn_server::{
        auth::AuthConfig,
        node::{NodeMode, RunningNode},
        ui::UiConfig,
    },
    reqwest::{header, Client, StatusCode},
    serde_json::{json, Value},
    std::time::Duration,
};

const BOOT: &str = "cf_test_ui_session_bootstrap_key_0123456789";
const SECRET: &str = "cf_test_cluster_secret_ui_session_32b";

async fn start_with(admin_key: Option<&str>, ui: UiConfig) -> Result<RunningNode> {
    let mut cfg = common::test_config(NodeMode::single(1));
    cfg.auth = AuthConfig {
        required: admin_key.is_some(),
        admin_key: admin_key.map(str::to_string),
        cluster_secret: Some(SECRET.into()),
        metrics_key: None,
        admin_key_sha256: None,
        provision_key_sha256: None,
    };
    cfg.ui = ui;
    common::start_led(cfg).await
}

async fn start() -> Result<RunningNode> {
    start_with(Some(BOOT), UiConfig::default()).await
}

fn origin(node: &RunningNode) -> String {
    node.url().to_string()
}

/// `POST /ui/session` from the node's own origin. Returns (code, JSON, cookie).
async fn login(node: &RunningNode, key: &str) -> Result<(StatusCode, Value, Option<String>)> {
    let r = Client::new()
        .post(format!("{}/ui/session", node.url()))
        .header(header::ORIGIN, origin(node))
        .json(&json!({ "key": key }))
        .send()
        .await?;
    let code = r.status();
    let set = r
        .headers()
        .get(header::SET_COOKIE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let v = r.json::<Value>().await.unwrap_or(Value::Null);
    Ok((code, v, set))
}

/// The `cairn_ui=…` value from `Set-Cookie`.
fn cookie_of(set: &str) -> String {
    set.split(';').next().unwrap_or_default().to_string()
}

async fn login_cookie(node: &RunningNode, key: &str) -> Result<String> {
    let (code, v, set) = login(node, key).await?;
    ensure!(code == StatusCode::OK, "login: {code} {v}");
    Ok(cookie_of(
        &set.ok_or_else(|| anyhow!("Set-Cookie is missing"))?,
    ))
}

async fn get_with_cookie(
    node: &RunningNode,
    path: &str,
    cookie: &str,
) -> Result<(StatusCode, Value)> {
    let r = Client::new()
        .get(format!("{}{path}", node.url()))
        .header(header::COOKIE, cookie)
        .send()
        .await?;
    let code = r.status();
    Ok((code, r.json::<Value>().await.unwrap_or(Value::Null)))
}

async fn create_key(node: &RunningNode, tenant: &str, role: &str) -> Result<(String, String)> {
    let v: Value = Client::new()
        .post(format!("{}/admin/keys", node.url()))
        .bearer_auth(BOOT)
        .json(&json!({ "tenant": tenant, "role": role }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok((
        v["id"].as_str().unwrap_or_default().to_string(),
        v["secret"].as_str().unwrap_or_default().to_string(),
    ))
}

/// Every role through the cookie and through Bearer: `whoami` reports the right
/// tenant, role and `via`; permissions follow the role as with Bearer. The
/// cookie does not contain the key, carries `HttpOnly; SameSite=Strict; Path=/`,
/// and no `Secure` on loopback.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_role_by_cookie_and_bearer() -> Result<()> {
    let node = start().await?;
    let r = common::within(common::TEST_DEADLINE, "roles", async {
        for (role, list_code) in [
            ("invoke", StatusCode::FORBIDDEN),
            ("admin", StatusCode::OK),
            ("cluster-admin", StatusCode::OK),
        ] {
            let (id, secret) = create_key(&node, "acme", role).await?;
            let (code, v, set) = login(&node, &secret).await?;
            ensure!(code == StatusCode::OK, "{role}: {code} {v}");
            ensure!(
                v["role"] == role && v["tenant"] == "acme" && v["keyId"] == id.as_str(),
                "{v}"
            );
            let set = set.unwrap_or_default();
            for part in [
                "cairn_ui=",
                "HttpOnly",
                "SameSite=Strict",
                "Path=/",
                "Max-Age=43200",
            ] {
                ensure!(set.contains(part), "{role}: {set}");
            }
            ensure!(!set.contains("Secure"), "loopback ⇒ no Secure: {set}");
            ensure!(!set.contains(&secret), "cookie holds no key");
            let cookie = cookie_of(&set);

            let (code, me) = get_with_cookie(&node, "/auth/whoami", &cookie).await?;
            ensure!(code == StatusCode::OK, "{role}: whoami {code} {me}");
            ensure!(
                me["role"] == role && me["tenant"] == "acme" && me["via"] == "cookie",
                "{me}"
            );
            ensure!(
                me["keyId"] == id.as_str() && me["bootstrap"] == false && me["noAuth"] == false,
                "{me}"
            );
            ensure!(
                me["version"] == env!("CARGO_PKG_VERSION") && me["node"].is_string(),
                "{me}"
            );
            ensure!(me["sessionExpiresAt"].as_u64().is_some(), "{me}");
            ensure!(me["clusterVersion"].is_number(), "{me}");
            let (code, _) = get_with_cookie(&node, "/invocations", &cookie).await?;
            ensure!(
                code == list_code,
                "{role}: GET /invocations with a cookie {code}"
            );

            let me: Value = Client::new()
                .get(format!("{}/auth/whoami", node.url()))
                .bearer_auth(&secret)
                .send()
                .await?
                .json()
                .await?;
            ensure!(
                me["via"] == "bearer" && me["role"] == role && me["sessionExpiresAt"].is_null(),
                "{me}"
            );
        }
        let me: Value = Client::new()
            .get(format!("{}/auth/whoami", node.url()))
            .bearer_auth(BOOT)
            .send()
            .await?
            .json()
            .await?;
        ensure!(
            me["bootstrap"] == true && me["keyId"].is_null() && me["role"] == "cluster-admin",
            "{me}"
        );
        let anon = Client::new()
            .get(format!("{}/auth/whoami", node.url()))
            .send()
            .await?;
        ensure!(
            anon.status() == StatusCode::UNAUTHORIZED,
            "{}",
            anon.status()
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **The `view` and `provision` roles in the console.**
///
/// 1. The bootstrap key creates `view` and `provision` keys for `acme`.
/// 2. Log in to the console with the `view` key; `whoami` through the cookie;
///    read `/invocations`; cancel an invocation with the cookie (same origin).
/// 3. Log in to the console with the `provision` key.
///
/// Must see: (2) `whoami` says `role: view`, `capabilities: ["read"]` (the
/// console hides actions by this set); the read is `200`; the cancel `403`.
/// The `cluster-admin` key has all five capabilities. (3) `401`, no
/// `Set-Cookie` — the control plane does not use the console, and a cookie for
/// it would only be one more place to leak.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn view_and_provision_keys_at_the_console() -> Result<()> {
    let node = start().await?;
    let r = common::within(common::TEST_DEADLINE, "view/provision", async {
        let (_, view) = create_key(&node, "acme", "view").await?;
        let (_, provision) = create_key(&node, "acme", "provision").await?;
        let cookie = login_cookie(&node, &view).await?;
        let (code, me) = get_with_cookie(&node, "/auth/whoami", &cookie).await?;
        ensure!(code == StatusCode::OK, "{code} {me}");
        ensure!(
            me["role"] == "view" && me["capabilities"] == json!(["read"]),
            "{me}"
        );
        let (code, _) = get_with_cookie(&node, "/invocations", &cookie).await?;
        ensure!(code == StatusCode::OK, "view reads /invocations: {code}");
        let r = Client::new()
            .post(format!("{}/invocations/inv_x/cancel", node.url()))
            .header(header::COOKIE, &cookie)
            .header(header::ORIGIN, origin(&node))
            .send()
            .await?;
        ensure!(
            r.status() == StatusCode::FORBIDDEN,
            "view cancels with a cookie: {}",
            r.status()
        );
        let me: Value = Client::new()
            .get(format!("{}/auth/whoami", node.url()))
            .bearer_auth(BOOT)
            .send()
            .await?
            .json()
            .await?;
        ensure!(
            me["capabilities"] == json!(["read", "invoke", "operate", "provision", "cluster"]),
            "{me}"
        );
        let (code, v, set) = login(&node, &provision).await?;
        ensure!(
            code == StatusCode::UNAUTHORIZED && set.is_none(),
            "a provision key opens the console: {code} {v} {set:?}"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// A cookie with one byte changed ⇒ `401`; a revoked key ⇒ the session dies; a
/// wrong Bearer does not fall back to a valid cookie; a wrong key at login ⇒
/// `401`; logout clears the cookie.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tamper_revoke_and_logout() -> Result<()> {
    let node = start().await?;
    let r = common::within(common::TEST_DEADLINE, "tamper/revoke", async {
        let (id, secret) = create_key(&node, "acme", "admin").await?;
        let cookie = login_cookie(&node, &secret).await?;
        let (code, _) = get_with_cookie(&node, "/auth/whoami", &cookie).await?;
        ensure!(code == StatusCode::OK);

        let mut bytes = cookie.clone().into_bytes();
        let last = bytes.len() - 1;
        bytes[last] = if bytes[last] == b'0' { b'1' } else { b'0' };
        let (code, _) = get_with_cookie(&node, "/auth/whoami", &String::from_utf8(bytes)?).await?;
        ensure!(
            code == StatusCode::UNAUTHORIZED,
            "cookie with one byte changed: {code}"
        );

        let wrong_bearer = Client::new()
            .get(format!("{}/auth/whoami", node.url()))
            .header(header::COOKIE, &cookie)
            .bearer_auth("cf_bad")
            .send()
            .await?;
        ensure!(
            wrong_bearer.status() == StatusCode::UNAUTHORIZED,
            "a wrong Bearer does not fall back to the cookie"
        );

        let (code, _, set) = login(&node, "cf_test_no_such_key").await?;
        ensure!(code == StatusCode::UNAUTHORIZED && set.is_none(), "{code}");

        let out = Client::new()
            .delete(format!("{}/ui/session", node.url()))
            .header(header::ORIGIN, origin(&node))
            .header(header::COOKIE, &cookie)
            .send()
            .await?;
        ensure!(out.status() == StatusCode::NO_CONTENT, "{}", out.status());
        let cleared = out
            .headers()
            .get(header::SET_COOKIE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        ensure!(
            cleared.starts_with("cairn_ui=;") && cleared.contains("Max-Age=0"),
            "{cleared}"
        );

        let gone = Client::new()
            .delete(format!("{}/admin/keys/{id}", node.url()))
            .bearer_auth(BOOT)
            .send()
            .await?;
        ensure!(gone.status().is_success(), "{}", gone.status());
        let (code, _) = get_with_cookie(&node, "/auth/whoami", &cookie).await?;
        ensure!(
            code == StatusCode::UNAUTHORIZED,
            "the key was revoked but the session is still alive: {code}"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// CSRF: `POST /query` with a cookie and a foreign `Origin` ⇒ `403`,
/// `Sec-Fetch-Site: cross-site` ⇒ `403`, no origin evidence ⇒ `403`; same
/// origin ⇒ passes. The same request with Bearer ⇒ not checked. Login from a
/// foreign origin ⇒ `403`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cookie_requests_must_be_same_origin() -> Result<()> {
    let node = start().await?;
    let r = common::within(common::TEST_DEADLINE, "CSRF", async {
        let cookie = login_cookie(&node, BOOT).await?;
        let host = node.url().trim_start_matches("http://").to_string();
        let query = |extra: &[(&str, String)], with_cookie: bool| {
            let mut req = Client::new()
                .post(format!("{}/query", node.url()))
                .json(&json!({ "sql": "SELECT 1" }));
            req = if with_cookie {
                req.header(header::COOKIE, &cookie)
            } else {
                req.bearer_auth(BOOT)
            };
            for (k, v) in extra {
                req = req.header(*k, v);
            }
            req.send()
        };
        let evil = "http://127.0.0.1:1".to_string();
        for (extra, want_forbidden) in [
            (vec![("origin", evil.clone())], true),
            (
                vec![
                    ("sec-fetch-site", "cross-site".to_string()),
                    ("origin", origin(&node)),
                ],
                true,
            ),
            (vec![("sec-fetch-site", "same-site".to_string())], true),
            (vec![], true),
            (vec![("origin", origin(&node))], false),
            (vec![("sec-fetch-site", "same-origin".to_string())], false),
        ] {
            let r = query(&extra, true).await?;
            ensure!(
                (r.status() == StatusCode::FORBIDDEN) == want_forbidden,
                "cookie + {extra:?}: {}",
                r.status()
            );
        }
        let bearer = query(&[("origin", evil.clone())], false).await?;
        ensure!(
            bearer.status() != StatusCode::FORBIDDEN,
            "Bearer is not origin-checked: {}",
            bearer.status()
        );
        let get = get_with_cookie(&node, "/invocations", &cookie).await?.0;
        ensure!(
            get == StatusCode::OK,
            "a GET with a cookie needs no origin: {get}"
        );

        let cross_login = Client::new()
            .post(format!("{}/ui/session", node.url()))
            .header(header::ORIGIN, evil)
            .header(header::HOST, host)
            .json(&json!({ "key": BOOT }))
            .send()
            .await?;
        ensure!(
            cross_login.status() == StatusCode::FORBIDDEN,
            "{}",
            cross_login.status()
        );
        let bare_login = Client::new()
            .post(format!("{}/ui/session", node.url()))
            .json(&json!({ "key": BOOT }))
            .send()
            .await?;
        ensure!(
            bare_login.status() == StatusCode::FORBIDDEN,
            "{}",
            bare_login.status()
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// Stateless sessions: another node with the same cluster secret and the same
/// bootstrap key accepts the cookie; a node that changes `CAIRNFLOW_ADMIN_KEY`
/// kills the old bootstrap session.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stateless_across_nodes_and_bootstrap_rotation() -> Result<()> {
    let a = start().await?;
    let cookie = match login_cookie(&a, BOOT).await {
        Ok(c) => c,
        Err(e) => {
            a.shutdown().await;
            return Err(e);
        }
    };
    a.shutdown().await;
    let twin = start().await?;
    let r = get_with_cookie(&twin, "/auth/whoami", &cookie).await;
    twin.shutdown().await;
    let (code, me) = r?;
    ensure!(
        code == StatusCode::OK && me["bootstrap"] == true,
        "sibling node: {code} {me}"
    );

    let rotated = start_with(
        Some("cf_test_ui_session_rotated_key_99999999"),
        UiConfig::default(),
    )
    .await?;
    let r = get_with_cookie(&rotated, "/auth/whoami", &cookie).await;
    rotated.shutdown().await;
    let (code, _) = r?;
    ensure!(
        code == StatusCode::UNAUTHORIZED,
        "the bootstrap key changed but the old session is still alive: {code}"
    );
    Ok(())
}

/// Session expiry: expired ⇒ `401`. `--ui-cookie-secure on` ⇒ the cookie carries `Secure`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expiry_and_secure_flag() -> Result<()> {
    let node = start_with(
        Some(BOOT),
        UiConfig {
            session_ttl: Duration::from_secs(1),
            cookie_secure: Some(true),
            ..Default::default()
        },
    )
    .await?;
    let r = common::within(common::TEST_DEADLINE, "expiry", async {
        let (code, _, set) = login(&node, BOOT).await?;
        let set = set.unwrap_or_default();
        ensure!(code == StatusCode::OK && set.contains("; Secure"), "{set}");
        let cookie = cookie_of(&set);
        // A 1 s expiry counted in Unix seconds: wait (with a cap) until the session dies.
        let t0 = std::time::Instant::now();
        loop {
            let (code, _) = get_with_cookie(&node, "/auth/whoami", &cookie).await?;
            if code == StatusCode::UNAUTHORIZED {
                break;
            }
            ensure!(code == StatusCode::OK, "{code}");
            ensure!(
                t0.elapsed() < Duration::from_secs(5),
                "a session with a 1 s expiry is still alive after 5 s"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// `--no-auth`: login returns `noAuth` without a cookie; `whoami` says `noAuth`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_auth_needs_no_session() -> Result<()> {
    let node = start_with(None, UiConfig::default()).await?;
    let r = common::within(common::TEST_DEADLINE, "--no-auth", async {
        let (code, v, set) = login(&node, "anything").await?;
        ensure!(
            code == StatusCode::OK && v["noAuth"] == true && set.is_none(),
            "{code} {v}"
        );
        let me: Value = Client::new()
            .get(format!("{}/auth/whoami", node.url()))
            .send()
            .await?
            .json()
            .await?;
        ensure!(
            me["noAuth"] == true && me["via"] == "none" && me["role"] == "cluster-admin",
            "{me}"
        );
        ensure!(me["bootstrap"] == false, "{me}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// The view tables of `/query` run `GET` on the node's router with the
/// identity of the query. A query made with a session cookie must carry that
/// identity down — before the fix only `Authorization` was forwarded, so
/// `SELECT … FROM sys_deployment` with a cookie was `400 GET /deployments: 401`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_views_work_with_a_session_cookie() -> Result<()> {
    let node = start().await?;
    let r = common::within(common::TEST_DEADLINE, "view with a cookie", async {
        let (_, secret) = create_key(&node, "acme", "admin").await?;
        let cookie = login_cookie(&node, &secret).await?;
        let r = Client::new()
            .post(format!("{}/query", node.url()))
            .header(header::COOKIE, &cookie)
            .header(header::ORIGIN, origin(&node))
            .json(&json!({ "sql": "SELECT id FROM sys_deployment" }))
            .send()
            .await?;
        let code = r.status();
        let v: Value = r.json().await?;
        ensure!(code == StatusCode::OK, "{code} {v}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **Environment label**: `--ui-env-label` shows in `whoami.envLabel` for every
/// key; unset ⇒ `null`; a bad label ⇒ the node does not start.
#[tokio::test]
async fn whoami_carries_env_label() -> Result<()> {
    let plain = start().await?;
    let labelled = start_with(
        Some(BOOT),
        UiConfig {
            env_label: Some("staging-eu".into()),
            ..UiConfig::default()
        },
    )
    .await?;
    let r = common::within(common::TEST_DEADLINE, "envLabel", async {
        for (node, want) in [(&plain, Value::Null), (&labelled, json!("staging-eu"))] {
            let me: Value = Client::new()
                .get(format!("{}/auth/whoami", node.url()))
                .bearer_auth(BOOT)
                .send()
                .await?
                .json()
                .await?;
            ensure!(me["envLabel"] == want, "{me}");
        }
        Ok(())
    })
    .await;
    plain.shutdown().await;
    labelled.shutdown().await;
    r?;
    let bad = start_with(
        Some(BOOT),
        UiConfig {
            env_label: Some(" prod".into()),
            ..UiConfig::default()
        },
    )
    .await;
    ensure!(bad.is_err(), "leading-space label still starts");
    Ok(())
}

/// `POST /ui/session` with key `key` and `extra` extra headers (the Origin is
/// the node's). Returns (code, `Retry-After`).
async fn login_raw(
    node: &RunningNode,
    key: &str,
    extra: &[(&str, &str)],
) -> Result<(StatusCode, Option<u64>)> {
    let mut req = Client::new()
        .post(format!("{}/ui/session", node.url()))
        .header(header::ORIGIN, origin(node))
        .json(&json!({ "key": key }));
    for (k, v) in extra {
        req = req.header(*k, *v);
    }
    let r = req.send().await?;
    let retry = r
        .headers()
        .get(header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok());
    Ok((r.status(), retry))
}

/// **IPv6 grouped by /64.** An IPv6 machine has a whole /64 to rotate
/// addresses through; a per-IP limit must count the whole /64 as one client.
///
/// Order (the node trusts a loopback proxy, client IP via `X-Forwarded-For`):
/// 1. 10 wrong keys, each from a different address in `2001:db8:1:2::/64`,
///    each with a different key prefix (the prefix cap is not reached).
/// 2. A right key from another address in the same /64.
/// 3. A right key from `2001:db8:1:3::1` (another /64).
///
/// Must see: (2) `429`; (3) `200`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn login_limit_counts_an_ipv6_64_as_one_client() -> Result<()> {
    let node = start_with(
        Some(BOOT),
        UiConfig {
            trust_proxy: cairn_server::auth::TrustedProxies::parse("127.0.0.1")
                .map_err(|e| anyhow::anyhow!(e))?,
            ..UiConfig::default()
        },
    )
    .await?;
    let r = common::within(common::TEST_DEADLINE, "group /64", async {
        for i in 0..10 {
            let ip = format!("2001:db8:1:2::{:x}", i + 1);
            let key = format!("cf_{i:08x}{:040x}", 3);
            let (code, _) = login_raw(&node, &key, &[("x-forwarded-for", &ip)]).await?;
            ensure!(code == StatusCode::UNAUTHORIZED, "wrong try {i}: {code}");
        }
        let (code, _) =
            login_raw(&node, BOOT, &[("x-forwarded-for", "2001:db8:1:2:ab::cd")]).await?;
        ensure!(
            code == StatusCode::TOO_MANY_REQUESTS,
            "another address in the same /64 was not counted together: {code}"
        );
        let (code, _) = login_raw(&node, BOOT, &[("x-forwarded-for", "2001:db8:1:3::1")]).await?;
        ensure!(code == StatusCode::OK, "another /64: {code}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// Console login is rate-limited per **key prefix** and per **IP** (GCRA,
/// only failed attempts count). Out of quota ⇒ `429` with `Retry-After`,
/// **before** the key is checked — even a right key, so a prober gets no test
/// at all.
///
/// Order:
/// 1. 5 wrong keys with the same prefix `cf_deadbeef` ⇒ `401`; the 6th ⇒ `429`.
/// 2. Wrong keys with other prefixes, same IP ⇒ still `401` up to the IP cap
///    (10 failures).
/// 3. The next attempt from that IP, with a right key ⇒ `429`.
///
/// Must see: `Retry-After` ≥ 1 on every `429`; a right key from another IP
/// (another node) still logs in — the limit does not lock the whole cluster.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn login_is_rate_limited_per_key_prefix_and_ip() -> Result<()> {
    let node = start().await?;
    let r = common::within(common::TEST_DEADLINE, "login limit", async {
        // (1)
        for i in 0..5 {
            let (code, _) = login_raw(&node, &format!("cf_deadbeef{i:040x}"), &[]).await?;
            ensure!(code == StatusCode::UNAUTHORIZED, "wrong try {i}: {code}");
        }
        let (code, retry) = login_raw(&node, "cf_deadbeef_sixth_try", &[]).await?;
        ensure!(
            code == StatusCode::TOO_MANY_REQUESTS,
            "6th try with the same prefix: {code}"
        );
        ensure!(
            retry.is_some_and(|s| s >= 1),
            "429 without Retry-After: {retry:?}"
        );
        // (2)
        for i in 0..5 {
            let (code, _) = login_raw(&node, &format!("cf_{i:02x}{:046x}", 7), &[]).await?;
            ensure!(
                code == StatusCode::UNAUTHORIZED,
                "other prefix, the IP's wrong try {i}: {code}"
            );
        }
        // (3)
        let (code, retry) = login_raw(&node, BOOT, &[]).await?;
        ensure!(
            code == StatusCode::TOO_MANY_REQUESTS,
            "the IP is out of quota yet a right key was still checked: {code}"
        );
        ensure!(
            retry.is_some_and(|s| s >= 1),
            "429 without Retry-After: {retry:?}"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r?;
    let other = start().await?;
    let r = login_raw(&other, BOOT, &[]).await;
    other.shutdown().await;
    let (code, _) = r?;
    ensure!(code == StatusCode::OK, "another node: {code}");
    Ok(())
}

/// **A Bearer `401` is counted, not blocked.** Three API requests with a wrong
/// key, then a right key from the same IP.
///
/// Must see: `cairnflow_auth_failures_total{kind="bearer"}` rises by ≥ 3; the
/// right key still gets `200` — a 256-bit key cannot be guessed, and blocking
/// per IP behind a proxy only hurts customers behind NAT.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bearer_failures_are_counted_not_blocked() -> Result<()> {
    let node = start().await?;
    let r = common::within(common::TEST_DEADLINE, "count 401", async {
        let failures = |m: &str| -> f64 {
            m.lines()
                .find(|l| l.starts_with("cairnflow_auth_failures_total{kind=\"bearer\"}"))
                .and_then(|l| l.split_whitespace().last())
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0)
        };
        let metrics = || async {
            anyhow::Ok(
                Client::new()
                    .get(format!("{}/metrics", node.url()))
                    .bearer_auth(BOOT)
                    .send()
                    .await?
                    .text()
                    .await?,
            )
        };
        let before = failures(&metrics().await?);
        for i in 0..3 {
            let r = Client::new()
                .get(format!("{}/invocations", node.url()))
                .bearer_auth(format!("cf_bad_{i:040x}"))
                .send()
                .await?;
            ensure!(
                r.status() == StatusCode::UNAUTHORIZED,
                "wrong key {i}: {}",
                r.status()
            );
        }
        let after = failures(&metrics().await?);
        ensure!(after - before >= 3.0, "count {before} → {after}");
        let r = Client::new()
            .get(format!("{}/invocations", node.url()))
            .bearer_auth(BOOT)
            .send()
            .await?;
        ensure!(
            r.status() == StatusCode::OK,
            "right key after 401: {}",
            r.status()
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

// ─────────────────────────── low-severity findings

/// The control plane's `provision` key: only its hash lives in the env.
const PROV: &str = "cf_test_ui_session_provision_key_0123456789";

async fn start_with_provision() -> Result<RunningNode> {
    use sha2::Digest;
    let mut cfg = common::test_config(NodeMode::single(1));
    cfg.auth = AuthConfig {
        required: true,
        admin_key: Some(BOOT.into()),
        cluster_secret: Some(SECRET.into()),
        metrics_key: None,
        admin_key_sha256: None,
        provision_key_sha256: Some(
            sha2::Sha256::digest(PROV.as_bytes())
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect(),
        ),
    };
    common::start_led(cfg).await
}

async fn bearer_json(
    method: reqwest::Method,
    url: String,
    key: &str,
    body: Option<Value>,
) -> Result<(StatusCode, Value)> {
    let mut r = Client::new().request(method, url).bearer_auth(key);
    if let Some(b) = body {
        r = r.json(&b);
    }
    let r = r.send().await?;
    let code = r.status();
    Ok((code, r.json().await.unwrap_or(Value::Null)))
}

/// **`whoami` says where the key lives.** Three keys: the bootstrap
/// cluster-admin (env), `provision` (hash in the env), one key in the registry.
///
/// Must see: `kind` is `bootstrap`, `env`, `registry` in turn; `bootstrap` is
/// `true` only for the real bootstrap key — the console does not mistake the
/// `provision` key for cluster-admin.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn whoami_kind_names_where_the_key_lives() -> Result<()> {
    let node = start_with_provision().await?;
    let url = node.url();
    let r = common::within(common::TEST_DEADLINE, "whoami kind", async {
        let (code, v) = bearer_json(
            reqwest::Method::POST,
            format!("{url}/admin/keys"),
            BOOT,
            Some(json!({ "tenant": "acme", "role": "admin" })),
        )
        .await?;
        ensure!(code == StatusCode::OK, "create key: {code} {v}");
        let registry = v["secret"].as_str().unwrap_or_default().to_string();
        for (key, kind, bootstrap) in [
            (BOOT, "bootstrap", true),
            (PROV, "env", false),
            (registry.as_str(), "registry", false),
        ] {
            let (code, me) = bearer_json(
                reqwest::Method::GET,
                format!("{url}/auth/whoami"),
                key,
                None,
            )
            .await?;
            ensure!(code == StatusCode::OK, "{kind}: {code} {me}");
            ensure!(
                me["kind"] == kind && me["bootstrap"] == bootstrap,
                "{kind}: {me}"
            );
        }
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **`GET /admin/tenants` does not return isolation rules to a `provision`
/// key.** Rules may carry a customer's service/handler names; `/rules` already
/// hides patterns from `provision`.
///
/// Must see: cluster-admin sees `4/2/60`; `provision` gets `200` without that
/// string anywhere in the body.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_tenants_hides_quarantine_rule_from_provision() -> Result<()> {
    let node = start_with_provision().await?;
    let url = node.url();
    let r = common::within(common::TEST_DEADLINE, "quarantineRule", async {
        let (code, v) = bearer_json(
            reqwest::Method::PUT,
            format!("{url}/admin/tenants/acme"),
            BOOT,
            Some(json!({ "quarantine": "hold", "quarantineRule": "4/2/60" })),
        )
        .await?;
        ensure!(code.is_success(), "PUT: {code} {v}");
        let (code, v) = bearer_json(
            reqwest::Method::GET,
            format!("{url}/admin/tenants"),
            BOOT,
            None,
        )
        .await?;
        ensure!(
            code == StatusCode::OK && v.to_string().contains("4/2/60"),
            "cluster-admin: {code} {v}"
        );
        let (code, v) = bearer_json(
            reqwest::Method::GET,
            format!("{url}/admin/tenants"),
            PROV,
            None,
        )
        .await?;
        ensure!(code == StatusCode::OK, "provision: {code} {v}");
        ensure!(
            !v.to_string().contains("4/2/60"),
            "provision sees the rules: {v}"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **The session cookie is locked with the `__Host-` prefix when `Secure`, and
/// two different sessions in one request are refused.**
///
/// 1. A node with `--ui-cookie-secure on`, log in with the `admin` key of `acme`
///    and of `zeta`.
/// 2. Before the fix the cookie was named `cairn_ui` (no prefix): a sibling
///    origin could set `cairn_ui=<its session>; Domain=<parent>`, the browser
///    sends both copies, and the server took the **first** one that verified —
///    the victim acts inside the setter's tenant.
///
/// Must see: `Set-Cookie` starts with `__Host-`, `Secure`, `Path=/`, no
/// `Domain`; a plain `cairn_ui` cookie does not open a session when `Secure`; a
/// request carrying two valid sessions of different keys ⇒ `401`, each session
/// alone ⇒ `200`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn secure_key_session_cookie_is_host_prefixed_and_unambiguous() -> Result<()> {
    let node = start_with(
        Some(BOOT),
        UiConfig {
            cookie_secure: Some(true),
            ..Default::default()
        },
    )
    .await?;
    let r = common::within(common::TEST_DEADLINE, "cookie __Host-", async {
        let (_, acme) = create_key(&node, "acme", "admin").await?;
        let (_, zeta) = create_key(&node, "zeta", "admin").await?;
        let (code, _, set) = login(&node, &acme).await?;
        let set = set.unwrap_or_default();
        ensure!(
            code == StatusCode::OK
                && set.starts_with("__Host-")
                && set.contains("; Secure")
                && set.contains("Path=/")
                && !set.to_ascii_lowercase().contains("domain="),
            "the session's Set-Cookie is locked when Secure: {set}"
        );
        let a = cookie_of(&set);
        let b = login_cookie(&node, &zeta).await?;
        for one in [&a, &b] {
            let (code, _) = get_with_cookie(&node, "/auth/whoami", one).await?;
            ensure!(code == StatusCode::OK, "one session: {code}");
        }
        let (name, value) = a.split_once('=').unwrap_or_default();
        let plain = format!("cairn_ui={value}");
        let (code, _) = get_with_cookie(&node, "/auth/whoami", &plain).await?;
        ensure!(
            name != "cairn_ui" && code == StatusCode::UNAUTHORIZED,
            "a plain cairn_ui cookie opened a session when Secure: {code}"
        );
        let both = format!("{b}; {a}");
        let (code, v) = get_with_cookie(&node, "/auth/whoami", &both).await?;
        ensure!(
            code == StatusCode::UNAUTHORIZED,
            "two sessions of different keys in one request: {code} {v}"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **Wrong Bearers are capped per IP.** The Bearer path had no cap on failed
/// attempts (it only counted): a prober avoids `<base>/session` (10 tries, then
/// 1 per 6 s) and probes here, one SHA-256 per try.
///
/// Must see: 150 back-to-back requests with a wrong key from one IP ⇒ some
/// `429` (with `Retry-After`) after a large burst (≥ 100 first `401`s — out of
/// reach of a mistyped key or a NAT); the code is only ever `401` or `429`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bearer_failures_from_one_ip_are_capped() -> Result<()> {
    let node = start().await?;
    let r = common::within(common::TEST_DEADLINE, "wrong Bearer cap", async {
        let client = Client::new();
        let (mut denied, mut capped, mut first_cap) = (0, 0, None);
        for i in 0..150 {
            let r = client
                .get(format!("{}/invocations", node.url()))
                .bearer_auth(format!("cf_bad_{i:040x}"))
                .send()
                .await?;
            match r.status() {
                StatusCode::UNAUTHORIZED => denied += 1,
                StatusCode::TOO_MANY_REQUESTS => {
                    ensure!(
                        r.headers().contains_key(header::RETRY_AFTER),
                        "429 without Retry-After"
                    );
                    capped += 1;
                    first_cap.get_or_insert(i);
                }
                other => anyhow::bail!("unexpected code {other}"),
            }
        }
        ensure!(
            capped > 0,
            "150 wrong Bearers from one IP and not one was blocked"
        );
        ensure!(
            first_cap.is_some_and(|i| i >= 100),
            "blocked too early (try {first_cap:?}, {denied} 401s)"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **A valid key is never hit by the per-IP Bearer cap.** Blocking **every**
/// Bearer of an IP that ran out of quota is a DoS door: anyone behind the same
/// NAT/proxy (or every customer when `--trust-proxy` is missing and the peer is
/// the proxy) could lock out customers who hold a right key — the same reason
/// Bearer `401`s are only counted.
///
/// Must see: after 150 back-to-back wrong Bearers from one IP (quota exhausted,
/// failures get `429`), a right key from the same IP still gets `200`; the next
/// failure still gets `429`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn valid_key_is_never_capped_by_ip_failures() -> Result<()> {
    let node = start().await?;
    let r = common::within(common::TEST_DEADLINE, "right key after cap", async {
        let client = Client::new();
        let get = |key: String| {
            let client = client.clone();
            let url = format!("{}/invocations", node.url());
            async move { anyhow::Ok(client.get(url).bearer_auth(key).send().await?.status()) }
        };
        let mut capped = false;
        for i in 0..150 {
            capped |= get(format!("cf_bad_{i:040x}")).await? == StatusCode::TOO_MANY_REQUESTS;
        }
        ensure!(capped, "150 wrong Bearers did not reach the cap");
        let ok = get(BOOT.to_string()).await?;
        ensure!(ok == StatusCode::OK, "right key, IP out of quota: {ok}");
        let again = get(format!("cf_bad_{:040x}", 999)).await?;
        ensure!(
            again == StatusCode::TOO_MANY_REQUESTS,
            "wrong try after the right key: {again}"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **Logout revokes the session on the server side.** Before the fix sessions
/// were stateless: `DELETE <base>/session` only cleared the cookie in the
/// browser, and a copy captured earlier lived until it expired (12 h).
///
/// Must see: after logging out with cookie A, cookie A gets `401`; cookie B
/// (another login, same key) still gets `200`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn logout_revokes_the_key_session() -> Result<()> {
    let node = start().await?;
    let r = common::within(common::TEST_DEADLINE, "logout revokes", async {
        let a = login_cookie(&node, BOOT).await?;
        let b = login_cookie(&node, BOOT).await?;
        let out = Client::new()
            .delete(format!("{}/ui/session", node.url()))
            .header(header::ORIGIN, origin(&node))
            .header(header::COOKIE, &a)
            .send()
            .await?;
        ensure!(out.status() == StatusCode::NO_CONTENT, "{}", out.status());
        let (code, _) = get_with_cookie(&node, "/auth/whoami", &a).await?;
        ensure!(
            code == StatusCode::UNAUTHORIZED,
            "logged-out cookie: {code}"
        );
        let (code, _) = get_with_cookie(&node, "/auth/whoami", &b).await?;
        ensure!(code == StatusCode::OK, "another session, same key: {code}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **An open SSE stream re-checks its permissions periodically.** Before the
/// fix SSE authenticated once at open and lived until `max_life` (600 s): a
/// revoked key kept receiving data.
///
/// 1. A node with a 300 ms re-check tick. An `admin` key of `acme`; a hanging
///    invocation.
/// 2. Open SSE on its `out` stream with that key; revoke the key.
///
/// Must see: the stream closes within ≤ 5 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_sse_closes_after_its_key_is_revoked() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    let mut cfg = common::test_config(NodeMode::single(1));
    cfg.auth = AuthConfig {
        required: true,
        admin_key: Some(BOOT.into()),
        cluster_secret: Some(SECRET.into()),
        metrics_key: None,
        admin_key_sha256: None,
        provision_key_sha256: None,
    };
    cfg.streams.recheck = Duration::from_millis(300);
    let node = common::start_led(cfg).await?;
    let sdk = FakeSdk::builder()
        .service("Hold", "run", vec![Step::Suspend, Step::output("x")])
        .spawn()
        .await?;
    let r = common::within(common::TEST_DEADLINE, "SSE after revoke", async {
        let (id, key) = create_key(&node, "acme", "admin").await?;
        let http = Client::new();
        let r = http
            .post(format!("{}/deployments", node.url()))
            .bearer_auth(&key)
            .json(&json!({ "uri": sdk.url }))
            .send()
            .await?;
        ensure!(r.status().is_success(), "deployment: {}", r.status());
        let v: Value = http
            .post(format!("{}/invoke/Hold/run/send", node.url()))
            .bearer_auth(&key)
            .body("{}")
            .send()
            .await?
            .json()
            .await?;
        let inv = v["id"].as_str().unwrap_or_default().to_string();
        let mut sse = http
            .get(format!("{}/invocations/{inv}/streams/out", node.url()))
            .bearer_auth(&key)
            .send()
            .await?;
        ensure!(sse.status() == StatusCode::OK, "open SSE: {}", sse.status());
        let gone = http
            .delete(format!("{}/admin/keys/{id}", node.url()))
            .bearer_auth(BOOT)
            .send()
            .await?;
        ensure!(gone.status().is_success(), "revoke: {}", gone.status());
        let closed = tokio::time::timeout(Duration::from_secs(5), async {
            while let Ok(Some(_)) = sse.chunk().await {}
        })
        .await;
        ensure!(closed.is_ok(), "SSE of a revoked key still open after 5 s");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **The wrong-Bearer cap groups IPv6 by /64.** The login limiter groups a /64
/// into one client; the wrong-Bearer cap keyed on the bare IP, so a machine
/// with a whole /64 that switches address every burst of 100 failures was never
/// blocked.
///
/// Order (the node trusts a loopback proxy, client IP via `X-Forwarded-For`):
/// 1. 150 wrong Bearers, each from a different address in `2001:db8:1:2::/64`.
/// 2. One wrong Bearer from `2001:db8:1:3::1` (another /64).
///
/// Must see: (1) `429` from the 101st try on; (2) `401`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bearer_failures_group_ipv6_by_64() -> Result<()> {
    let node = start_with(
        Some(BOOT),
        UiConfig {
            trust_proxy: cairn_server::auth::TrustedProxies::parse("127.0.0.1")
                .map_err(|e| anyhow::anyhow!(e))?,
            ..UiConfig::default()
        },
    )
    .await?;
    let r = common::within(common::TEST_DEADLINE, "wrong Bearer per /64", async {
        let client = Client::new();
        let mut first_cap = None;
        for i in 0..150u32 {
            let r = client
                .get(format!("{}/invocations", node.url()))
                .bearer_auth(format!("cf_bad_{i:040x}"))
                .header("x-forwarded-for", format!("2001:db8:1:2::{:x}", i + 1))
                .send()
                .await?;
            if r.status() == StatusCode::TOO_MANY_REQUESTS {
                first_cap.get_or_insert(i);
            }
        }
        ensure!(
            first_cap.is_some_and(|i| i >= 100),
            "150 failures from one /64 and the first block was {first_cap:?}"
        );
        let r = client
            .get(format!("{}/invocations", node.url()))
            .bearer_auth("cf_test_wrong")
            .header("x-forwarded-for", "2001:db8:1:3::1")
            .send()
            .await?;
        ensure!(
            r.status() == StatusCode::UNAUTHORIZED,
            "another /64: {}",
            r.status()
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **Logout is rate-limited per IP.** Every effective logout is a write to the
/// cluster-wide session registry (and bumps the registry generation); a right
/// login costs no quota, so a client could mint and log out sessions
/// continuously.
///
/// Must see: 40 back-to-back login/logout rounds from one IP ⇒ some `429` (with
/// `Retry-After`) before the 40th.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn logout_is_rate_limited_per_client() -> Result<()> {
    let node = start().await?;
    let r = common::within(common::TEST_DEADLINE, "logout cap", async {
        let mut capped = None;
        for i in 0..40 {
            let c = login_cookie(&node, BOOT).await?;
            let out = Client::new()
                .delete(format!("{}/ui/session", node.url()))
                .header(header::ORIGIN, origin(&node))
                .header(header::COOKIE, &c)
                .send()
                .await?;
            if out.status() == StatusCode::TOO_MANY_REQUESTS {
                ensure!(
                    out.headers().contains_key(header::RETRY_AFTER),
                    "429 without Retry-After"
                );
                capped = Some(i);
                break;
            }
        }
        ensure!(
            capped.is_some(),
            "40 back-to-back logouts from one IP and not one was blocked"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}
