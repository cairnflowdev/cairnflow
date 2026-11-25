// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Choosing the transport mode when a deployment is registered.
//!
//! `BIDI_STREAM` only works when HTTP/2 goes all the way to the SDK; behind a
//! proxy that only speaks h1 (Render, Railway, Heroku, an edge that buffers
//! request bodies) every BIDI turn fails at the transport. The default keeps
//! the earlier behaviour (`declared`: the mode the SDK declares);
//! `transport: "auto"` (or `--deploy-transport auto`) probes `/discover` once
//! through an h2 client when the SDK declares BIDI — a failed probe ⇒ the
//! deployment settles on `REQUEST_RESPONSE`, and the reason is in the
//! deployment's `transport`.

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterminism"
)]

mod common;

use {
    anyhow::{ensure, Result},
    bytes::Bytes,
    cairn_server::{
        discovery::{
            EndpointManifest, HandlerManifest, ProtocolMode, ServiceManifest, ServiceType,
            DISCOVERY_V1,
        },
        node::NodeMode,
    },
    common::fake_sdk::{FakeSdk, Step},
    serde_json::{json, Value},
    tokio::io::{AsyncReadExt, AsyncWriteExt},
};

async fn register(node: &str, sdk: &str, transport: Option<&str>) -> Result<(u16, Value)> {
    let mut body = json!({ "uri": sdk });
    if let Some(t) = transport {
        body["transport"] = t.into();
    }
    let r = reqwest::Client::new()
        .post(format!("{node}/deployments"))
        .json(&body)
        .send()
        .await?;
    Ok((r.status().as_u16(), r.json().await.unwrap_or(Value::Null)))
}

/// Default (`declared`): the mode is the mode the SDK declares — as before
/// transport probing existed — and the deployment records the real HTTP version
/// of the `/discover` call.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declared_is_default_and_records_http_version() -> Result<()> {
    let node = common::start_single_process(1).await?;
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("\"ok\"")])
        .declare_bidi()
        .spawn()
        .await?;
    let (code, d) = register(&node.url(), &sdk.url, None).await?;
    node.shutdown().await;
    ensure!(code == 200, "{code} {d}");
    ensure!(d["protocolMode"] == "BIDI_STREAM", "{d}");
    ensure!(
        d["transport"]
            == json!({"policy": "declared", "declared": "BIDI_STREAM", "discoveredHttp": "HTTP/1.1"}),
        "{d}"
    );
    Ok(())
}

/// `auto` + an SDK that declares BIDI but only speaks h1 (behind an h1 proxy):
/// the h2 probe fails ⇒ the deployment settles on `REQUEST_RESPONSE` and the
/// invocation runs over h1. Without `auto`, the same SDK fails every turn at the
/// transport (h2c does not get through).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auto_downgrades_bidi_behind_h1_only() -> Result<()> {
    let node = common::start_single_process(1).await?;
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("\"ok\"")])
        .declare_bidi()
        .spawn()
        .await?;
    let r = async {
        let (code, d) = register(&node.url(), &sdk.url, Some("auto")).await?;
        ensure!(code == 200, "{code} {d}");
        ensure!(d["protocolMode"] == "REQUEST_RESPONSE", "{d}");
        ensure!(
            d["transport"]["policy"] == "auto" && d["transport"]["declared"] == "BIDI_STREAM",
            "{d}"
        );
        ensure!(
            d["transport"]["bidiProbe"]
                .as_str()
                .is_some_and(|p| p.starts_with("failed")),
            "{d}"
        );
        let (code, out) = common::invoke(&node.url(), "Echo", "run", None, "null").await?;
        ensure!(
            code == 200 && out["status"] == "completed" && out["output"] == "\"ok\"",
            "invoke: {code} {out}"
        );
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}

/// `auto` + an SDK that speaks h2c ⇒ keeps `BIDI_STREAM`, the probe records `ok`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auto_keeps_bidi_when_h2_reachable() -> Result<()> {
    let node = common::start_single_process(1).await?;
    let sdk = DualSdk::spawn().await?;
    let (code, d) = register(&node.url(), &sdk, Some("auto")).await?;
    node.shutdown().await;
    ensure!(code == 200, "{code} {d}");
    ensure!(d["protocolMode"] == "BIDI_STREAM", "{d}");
    ensure!(d["transport"]["bidiProbe"] == "ok", "{d}");
    Ok(())
}

/// An unknown value ⇒ `400`, the SDK is not called; the node's
/// `--deploy-transport auto` makes `auto` the default, and a request can still
/// override it back to `declared`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_default_and_bad_value() -> Result<()> {
    let mut cfg = common::test_config(NodeMode::single(1));
    cfg.deploy_transport = cairn_server::discovery::TransportPolicy::Auto;
    let node = common::start_led(cfg).await?;
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("\"ok\"")])
        .declare_bidi()
        .spawn()
        .await?;
    let r = async {
        let (code, d) = register(&node.url(), &sdk.url, Some("bidi-please")).await?;
        ensure!(code == 400, "{code} {d}");
        let (code, d) = register(&node.url(), &sdk.url, None).await?;
        ensure!(
            code == 200 && d["protocolMode"] == "REQUEST_RESPONSE",
            "{code} {d}"
        );
        let (code, d) = register(&node.url(), &sdk.url, Some("declared")).await?;
        ensure!(
            code == 200 && d["protocolMode"] == "BIDI_STREAM",
            "{code} {d}"
        );
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}

// ───────────────────────────────────────────────── SDK speaking both h1 and h2c

/// A fake SDK that serves `/discover` (declaring BIDI) over both h1 and h2c,
/// like the TS SDK's `serve()`: sniff the `PRI ` preface, then branch.
struct DualSdk;

impl DualSdk {
    async fn spawn() -> Result<String> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                tokio::spawn(serve_conn(socket));
            }
        });
        Ok(url)
    }
}

fn manifest() -> Bytes {
    Bytes::from(
        serde_json::to_vec(&EndpointManifest {
            protocol_mode: ProtocolMode::BidiStream,
            min_protocol_version: 1,
            max_protocol_version: 1,
            services: vec![ServiceManifest {
                name: "Echo".into(),
                ty: ServiceType::Service,
                handlers: vec![HandlerManifest {
                    name: "run".into(),
                    ty: None,
                    contract: Default::default(),
                }],
                contract: Default::default(),
            }],
        })
        .expect("manifest encodes"),
    )
}

async fn serve_conn(mut socket: tokio::net::TcpStream) {
    let mut first = [0u8; 4];
    let mut n = 0;
    while n < 4 {
        match socket.peek(&mut first).await {
            Ok(0) | Err(_) => return,
            Ok(k) => n = k,
        }
        if n < 4 {
            tokio::task::yield_now().await;
        }
    }
    if &first == b"PRI " {
        let Ok(mut conn) = h2::server::handshake(socket).await else {
            return;
        };
        while let Some(Ok((_req, mut respond))) = conn.accept().await {
            let resp = http::Response::builder()
                .status(200)
                .header("content-type", DISCOVERY_V1)
                .body(())
                .expect("valid header");
            if let Ok(mut out) = respond.send_response(resp, false) {
                let _ = out.send_data(manifest(), true);
            }
        }
        return;
    }
    let mut buf = Vec::new();
    loop {
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        let mut chunk = [0u8; 4096];
        match socket.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    }
    let body = manifest();
    let head = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: {DISCOVERY_V1}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    let _ = socket.write_all(head.as_bytes()).await;
    let _ = socket.write_all(&body).await;
    let _ = socket.shutdown().await;
}
