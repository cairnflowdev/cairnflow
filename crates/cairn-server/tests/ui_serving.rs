// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The web console on the ingress port: static files under the base path,
//! outside the auth layer; everything else keeps its old behaviour.
//!
//! A real in-process node, auth enabled with the bootstrap key. The file set is
//! injected through `UiConfig::files` to check assets, compression and caching
//! without depending on whether `ui/dist` was built; a separate test runs the
//! embedded copy.

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
    std::sync::Arc,
};

const ADMIN_KEY: &str = "cf_test_ui_serving_bootstrap_key_0123456789";
const HTML: &str = "text/html,application/xhtml+xml,*/*;q=0.8";
const INDEX: &str = r##"<!doctype html><html><head><script src="./theme-init.js"></script><script type="module" src="./assets/app-1a2b.js"></script></head><body><a href="#main">skip</a></body></html>"##;

fn files() -> Vec<(String, Vec<u8>)> {
    [
        ("index.html", INDEX.as_bytes()),
        ("theme-init.js", b"/* theme */".as_slice()),
        ("assets/app-1a2b.js", b"console.log(1)".as_slice()),
        ("assets/app-1a2b.js.br", b"BR-BYTES".as_slice()),
        ("assets/app-1a2b.js.gz", b"GZ-BYTES".as_slice()),
        ("assets/plain-9f.css", b"body{}".as_slice()),
    ]
    .into_iter()
    .map(|(n, b)| (n.to_string(), b.to_vec()))
    .collect()
}

fn ui(base: &str) -> UiConfig {
    UiConfig {
        enabled: true,
        base_path: base.into(),
        files: Some(Arc::new(files())),
        ..Default::default()
    }
}

async fn start(ui: UiConfig) -> Result<RunningNode> {
    let mut cfg = common::test_config(NodeMode::single(1));
    cfg.auth = AuthConfig {
        required: true,
        admin_key: Some(ADMIN_KEY.into()),
        cluster_secret: None,
        metrics_key: None,
        admin_key_sha256: None,
        provision_key_sha256: None,
    };
    cfg.ui = ui;
    common::start_led(cfg).await
}

fn http() -> Client {
    Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("client")
}

async fn get(
    node: &RunningNode,
    path: &str,
    headers: &[(&str, &str)],
) -> Result<reqwest::Response> {
    let mut req = http().get(format!("{}{path}", node.url()));
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    Ok(req.send().await?)
}

fn h(r: &reqwest::Response, name: &str) -> String {
    r.headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string()
}

/// Every UI response carries a CSP without a nonce and three protective headers.
fn secured(r: &reqwest::Response) -> Result<()> {
    let csp = h(r, "content-security-policy");
    for part in [
        "default-src 'self'",
        "script-src 'self'",
        "style-src 'self'",
        "style-src-attr 'unsafe-inline'",
        "frame-ancestors 'none'",
        "base-uri 'none'",
    ] {
        ensure!(csp.contains(part), "CSP lacks {part}: {csp}");
    }
    ensure!(!csp.contains("nonce"), "CSP uses no nonce: {csp}");
    ensure!(h(r, "x-content-type-options") == "nosniff");
    ensure!(h(r, "referrer-policy") == "same-origin");
    ensure!(h(r, "cross-origin-opener-policy") == "same-origin");
    Ok(())
}

/// Assets: `immutable`, `ETag`, `If-None-Match` ⇒ `304`, the compressed copy
/// is picked by `Accept-Encoding` with `Vary`; a missing asset ⇒ `404` even
/// when the browser asks for HTML; a file outside `assets/` ⇒ `no-cache`. No
/// key needed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn assets_cache_and_compression() -> Result<()> {
    let node = start(ui("/ui")).await?;
    let r = common::within(common::TEST_DEADLINE, "asset", async {
        let plain = get(&node, "/ui/assets/app-1a2b.js", &[]).await?;
        ensure!(plain.status() == StatusCode::OK, "{}", plain.status());
        secured(&plain)?;
        ensure!(h(&plain, "cache-control") == "public, max-age=31536000, immutable");
        ensure!(
            h(&plain, "content-type").contains("javascript"),
            "{}",
            h(&plain, "content-type")
        );
        ensure!(h(&plain, "vary") == "Accept-Encoding");
        ensure!(h(&plain, "content-encoding").is_empty());
        let etag = h(&plain, "etag");
        ensure!(
            etag.starts_with('"') && etag.len() == 66,
            "ETag sha256: {etag}"
        );
        ensure!(plain.bytes().await?.as_ref() == b"console.log(1)");

        let br = get(
            &node,
            "/ui/assets/app-1a2b.js",
            &[("accept-encoding", "gzip, deflate, br")],
        )
        .await?;
        ensure!(h(&br, "content-encoding") == "br", "{:?}", br.headers());
        ensure!(h(&br, "vary") == "Accept-Encoding");
        ensure!(
            h(&br, "content-type").contains("javascript"),
            "type of the original"
        );
        let br_etag = h(&br, "etag");
        ensure!(br_etag != etag, "one ETag per compressed copy");
        ensure!(br.bytes().await?.as_ref() == b"BR-BYTES");

        let gz = get(
            &node,
            "/ui/assets/app-1a2b.js",
            &[("accept-encoding", "br;q=0, gzip")],
        )
        .await?;
        ensure!(h(&gz, "content-encoding") == "gzip");
        ensure!(gz.bytes().await?.as_ref() == b"GZ-BYTES");

        let again = get(&node, "/ui/assets/app-1a2b.js", &[("if-none-match", &etag)]).await?;
        ensure!(
            again.status() == StatusCode::NOT_MODIFIED,
            "{}",
            again.status()
        );
        ensure!(h(&again, "etag") == etag);
        ensure!(again.bytes().await?.is_empty());
        let weak = get(
            &node,
            "/ui/assets/app-1a2b.js",
            &[
                ("if-none-match", &format!("W/{br_etag}")),
                ("accept-encoding", "br"),
            ],
        )
        .await?;
        ensure!(
            weak.status() == StatusCode::NOT_MODIFIED,
            "a weak ETag matches too: {}",
            weak.status()
        );

        let css = get(
            &node,
            "/ui/assets/plain-9f.css",
            &[("accept-encoding", "br")],
        )
        .await?;
        ensure!(css.status() == StatusCode::OK);
        ensure!(h(&css, "vary").is_empty(), "no compressed copy ⇒ no Vary");
        ensure!(h(&css, "content-encoding").is_empty());

        let missing = get(&node, "/ui/assets/gone-0000.js", &[("accept", HTML)]).await?;
        ensure!(
            missing.status() == StatusCode::NOT_FOUND,
            "missing asset: {}",
            missing.status()
        );
        secured(&missing)?;

        let theme = get(&node, "/ui/theme-init.js", &[]).await?;
        ensure!(theme.status() == StatusCode::OK);
        ensure!(
            h(&theme, "cache-control") == "no-cache",
            "a file without a hashed name must revalidate"
        );

        let head = http()
            .head(format!("{}/ui/assets/app-1a2b.js", node.url()))
            .send()
            .await?;
        ensure!(head.status() == StatusCode::OK && !h(&head, "etag").is_empty());
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// SPA fallback only for `text/html`: a deep link returns the rewritten
/// `index.html` (`"./` ⇒ `"/ui/`, base/version meta tags, `no-store`); no
/// `text/html` ⇒ `404`. `/ui` ⇒ `302 /ui/`; `GET /` with `text/html` ⇒
/// `302 /ui/`, without ⇒ old behaviour (`401` without a key).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn html_fallback_and_redirects() -> Result<()> {
    let node = start(ui("/ui")).await?;
    let r = common::within(common::TEST_DEADLINE, "fallback", async {
        for path in ["/ui/", "/ui/invocations/abc", "/ui/index.html"] {
            let r = get(&node, path, &[("accept", HTML)]).await?;
            ensure!(r.status() == StatusCode::OK, "{path}: {}", r.status());
            secured(&r)?;
            ensure!(h(&r, "cache-control") == "no-store", "{path}");
            ensure!(h(&r, "content-type").starts_with("text/html"), "{path}");
            let body = r.text().await?;
            ensure!(
                body.contains(r#"src="/ui/assets/app-1a2b.js""#),
                "{path}: {body}"
            );
            ensure!(
                body.contains(r#"src="/ui/theme-init.js""#),
                "{path}: {body}"
            );
            ensure!(
                body.contains(r##"href="#main""##),
                "in-page anchors stay unchanged: {body}"
            );
            ensure!(
                body.contains(r#"<meta name="cairn-base" content="/ui">"#),
                "{body}"
            );
            ensure!(
                body.contains(&format!(
                    r#"<meta name="cairn-version" content="{}">"#,
                    env!("CARGO_PKG_VERSION")
                )),
                "{body}"
            );
            ensure!(!body.contains("<base"), "does not use <base href>: {body}");
        }
        let json = get(
            &node,
            "/ui/invocations/abc",
            &[("accept", "application/json")],
        )
        .await?;
        ensure!(
            json.status() == StatusCode::NOT_FOUND,
            "no text/html ⇒ 404: {}",
            json.status()
        );

        let bare = get(&node, "/ui", &[("accept", HTML)]).await?;
        ensure!(
            bare.status() == StatusCode::FOUND && h(&bare, "location") == "/ui/",
            "{}",
            bare.status()
        );
        let root = get(&node, "/", &[("accept", HTML)]).await?;
        ensure!(
            root.status() == StatusCode::FOUND && h(&root, "location") == "/ui/",
            "{}",
            root.status()
        );
        let api_root = get(&node, "/", &[]).await?;
        ensure!(
            api_root.status() == StatusCode::UNAUTHORIZED,
            "GET / without HTML keeps the old behaviour: {}",
            api_root.status()
        );
        let lookalike = get(&node, "/uix", &[("accept", HTML)]).await?;
        ensure!(
            lookalike.status() == StatusCode::UNAUTHORIZED,
            "/uix does not belong to the UI: {}",
            lookalike.status()
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// The UI is open but the API stays locked: `GET /ui/…` needs no key;
/// `/invocations` without a key ⇒ `401`, with a key ⇒ `200`; methods other
/// than `GET`/`HEAD` under the base path (except the session) go into auth
/// like any unknown path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auth_still_guards_the_api() -> Result<()> {
    let node = start(ui("/ui")).await?;
    let r = common::within(common::TEST_DEADLINE, "auth", async {
        let page = get(&node, "/ui/", &[("accept", HTML)]).await?;
        ensure!(page.status() == StatusCode::OK);
        let anon = get(&node, "/invocations", &[("accept", HTML)]).await?;
        ensure!(
            anon.status() == StatusCode::UNAUTHORIZED,
            "{}",
            anon.status()
        );
        let keyed = http()
            .get(format!("{}/invocations", node.url()))
            .bearer_auth(ADMIN_KEY)
            .send()
            .await?;
        ensure!(keyed.status() == StatusCode::OK, "{}", keyed.status());
        let post = http()
            .post(format!("{}/ui/invocations", node.url()))
            .header(header::ACCEPT, HTML)
            .send()
            .await?;
        ensure!(
            post.status() == StatusCode::UNAUTHORIZED,
            "POST under /ui (outside /ui/session) does not belong to the UI: {}",
            post.status()
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// `--ui-base-path /console`: the UI lives at `/console/`, `index.html` points
/// assets at that base; `/ui/` is no longer the UI (falls through to the API ⇒
/// `401`). `X-Forwarded-Prefix` is not read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn custom_base_path() -> Result<()> {
    let node = start(ui("/console")).await?;
    let r = common::within(common::TEST_DEADLINE, "base path", async {
        let page = get(
            &node,
            "/console/runs",
            &[("accept", HTML), ("x-forwarded-prefix", "/evil\"><script>")],
        )
        .await?;
        ensure!(page.status() == StatusCode::OK, "{}", page.status());
        let body = page.text().await?;
        ensure!(
            body.contains(r#"src="/console/assets/app-1a2b.js""#),
            "{body}"
        );
        ensure!(
            body.contains(r#"<meta name="cairn-base" content="/console">"#),
            "{body}"
        );
        ensure!(
            !body.contains("evil"),
            "the header never reaches the HTML: {body}"
        );
        let asset = get(&node, "/console/assets/app-1a2b.js", &[]).await?;
        ensure!(asset.status() == StatusCode::OK);
        let old = get(&node, "/ui/", &[("accept", HTML)]).await?;
        ensure!(old.status() == StatusCode::UNAUTHORIZED, "{}", old.status());
        let root = get(&node, "/", &[("accept", HTML)]).await?;
        ensure!(h(&root, "location") == "/console/", "{:?}", root.headers());
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// `--no-ui`: nothing under `/ui` is served, `GET /` does not redirect — the
/// node answers as it did before the UI existed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_ui_restores_old_behaviour() -> Result<()> {
    let mut cfg = ui("/ui");
    cfg.enabled = false;
    let node = start(cfg).await?;
    let r = common::within(common::TEST_DEADLINE, "--no-ui", async {
        for path in ["/ui/", "/ui/assets/app-1a2b.js", "/"] {
            let r = get(&node, path, &[("accept", HTML)]).await?;
            ensure!(
                r.status() == StatusCode::UNAUTHORIZED,
                "{path}: {}",
                r.status()
            );
            ensure!(h(&r, "content-security-policy").is_empty(), "{path}");
        }
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// A bad base path is a startup error — even when the UI is off.
#[tokio::test]
async fn bad_base_path_is_rejected_at_startup() -> Result<()> {
    for (base, enabled) in [
        ("ui", true),
        ("/ui/", true),
        ("/a/../b", true),
        ("/u i", true),
        ("/invocations", true),
        ("/admin", false),
    ] {
        let mut cfg = common::test_config(NodeMode::single(1));
        cfg.ui = UiConfig {
            enabled,
            base_path: base.into(),
            files: Some(Arc::new(files())),
            ..Default::default()
        };
        match cairn_server::node::start(cfg).await {
            Err(e) => ensure!(e.to_string().contains("--ui-base-path"), "{base}: {e}"),
            Ok(n) => {
                n.shutdown().await;
                return Err(anyhow!("base path {base:?} must be refused"));
            }
        }
    }
    Ok(())
}

/// The real embedded copy (the placeholder page when `ui/dist` is not built):
/// `index.html` has the base meta tag.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn embedded_build_serves_index() -> Result<()> {
    let node = start(UiConfig::default()).await?;
    let r = common::within(common::TEST_DEADLINE, "embedded copy", async {
        let r = get(&node, "/ui/", &[("accept", HTML)]).await?;
        ensure!(r.status() == StatusCode::OK, "{}", r.status());
        secured(&r)?;
        let body = r.text().await?;
        ensure!(
            body.contains(r#"<meta name="cairn-base" content="/ui">"#),
            "{body}"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// `RESERVED` covers the first segment of every route on the ingress and
/// internal ports: a UI base path that equals a route would swallow the
/// browser's `GET`s to the API (the UI layer sits in front of auth).
#[test]
fn reserved_covers_every_route() {
    let src = include_str!("../src/ingress/mod.rs");
    for part in src.split(".route(").skip(1) {
        let Some(path) = part
            .trim_start()
            .strip_prefix("\"/")
            .and_then(|p| p.split('"').next())
        else {
            continue;
        };
        let first = path.split('/').next().unwrap_or("");
        assert!(
            cairn_server::ui::RESERVED.contains(&first),
            "route /{path}: segment /{first} is not in ui::RESERVED"
        );
    }
}
