// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Unknown frames and side channels by **negotiated wire version** —
//! `docs/protocol-v1.md` §9.2.
//!
//! A fake `REQUEST_RESPONSE` SDK declares `maxProtocolVersion` = 1, 2, then 3;
//! each handler returns a response body with exactly one non-command frame
//! inserted before `Output` + `End`. This is the contract of
//! `docs/protocol-v1.md` §9.2 at the level of a real turn
//! (`turn/sdk.rs::drive_rr` + the state machine), not of the machine alone
//! (`cairn-runtime/tests/side_channel.rs`):
//!
//! | Frame | Every wire version |
//! |---|---|
//! | `Keepalive` `0x0008` | ignored — side channel, even below v3 (lenient runtime) |
//! | custom `0xFC01` | ignored (RR has no forwarding path) — side channel, does not become an entry |
//! | unknown code `0x0420`, reserved code `0x0006` | 571, invocation `failed`, the journal only has `Input` |
//! | `ReplayReport` `0x0009` outside a trial replay turn | 571 |
//!
//! The wire version only changes what the SDK **may send**; it does not change
//! how the runtime treats what it does not understand. A new side-channel frame
//! (e.g. v4's `StreamAppend`) must therefore carry a **new code**: a runtime
//! older than it sees an unknown code and returns 571, rather than silently
//! recording it wrong — and negotiation never lets an SDK send it down to such
//! a runtime.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterminism"
)]

mod common;

use {
    anyhow::Result,
    axum::{
        extract::{Path as AxPath, State},
        http::header,
        routing::{get, post},
        Router,
    },
    bytes::{Bytes, BytesMut},
    cairn_protocol::{encode_frame, encode_message, pb, Flags, MessageType},
    cairn_server::{
        discovery::{
            EndpointManifest, HandlerManifest, ProtocolMode, ServiceManifest, ServiceType,
            DISCOVERY_V1,
        },
        invoker::INVOCATION_V1,
    },
    common::{register, send, start_single_process, wait_status, within, TEST_DEADLINE},
};

/// Handler → the frame inserted before `Output` + `End`.
const CASES: &[(&str, u16, &str)] = &[
    ("keepalive", 0x0008, "completed"),
    ("custom", 0xFC01, "completed"),
    ("unknown", 0x0420, "failed"),
    ("reserved", 0x0006, "failed"),
    ("replayReport", 0x0009, "failed"),
];

async fn discover(State(wire): State<u32>) -> ([(header::HeaderName, &'static str); 1], String) {
    let manifest = EndpointManifest {
        protocol_mode: ProtocolMode::RequestResponse,
        min_protocol_version: 1,
        max_protocol_version: wire,
        services: vec![ServiceManifest {
            name: "Side".into(),
            ty: ServiceType::Service,
            handlers: CASES
                .iter()
                .map(|(h, _, _)| HandlerManifest {
                    name: h.to_string(),
                    ty: None,
                    contract: Default::default(),
                })
                .collect(),
            contract: Default::default(),
        }],
    };
    (
        [(header::CONTENT_TYPE, DISCOVERY_V1)],
        serde_json::to_string(&manifest).unwrap(),
    )
}

async fn invoke(
    AxPath((_service, handler)): AxPath<(String, String)>,
) -> ([(header::HeaderName, &'static str); 1], Bytes) {
    let (_, ty, _) = CASES.iter().find(|(h, _, _)| *h == handler).unwrap();
    let extra = match *ty {
        0x0008 => encode_message(
            MessageType::KEEPALIVE,
            Flags::NONE,
            &pb::KeepaliveMessage {},
        ),
        0x0009 => encode_message(
            MessageType::REPLAY_REPORT,
            Flags::NONE,
            &pb::ReplayReportMessage {
                matched: 1,
                ..Default::default()
            },
        ),
        other => encode_frame(MessageType(other), Flags::NONE, b"\x08\x01"),
    };
    let mut body = BytesMut::from(&extra[..]);
    body.extend_from_slice(&encode_message(
        MessageType::OUTPUT,
        Flags::NONE,
        &pb::OutputCommandMessage {
            result: Some(pb::output_command_message::Result::Value(
                cairn_protocol::value::inline(Bytes::from_static(b"1")),
            )),
            name: "output".into(),
        },
    ));
    body.extend_from_slice(&encode_message(
        MessageType::END,
        Flags::NONE,
        &pb::EndMessage {},
    ));
    ([(header::CONTENT_TYPE, INVOCATION_V1)], body.freeze())
}

async fn spawn_sdk(wire: u32) -> Result<(String, tokio::task::JoinHandle<()>)> {
    let app = Router::new()
        .route("/discover", get(discover))
        .route("/invoke/{service}/{handler}", post(invoke))
        .with_state(wire);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok((url, server))
}

#[tokio::test]
async fn unknown_frames_and_side_channel_by_wire_version() -> Result<()> {
    for wire in 1..=3u32 {
        let node = start_single_process(1).await?;
        let (sdk, server) = spawn_sdk(wire).await?;
        let reg = register(&node.url(), &sdk).await?;
        assert_eq!(
            reg["wireVersion"], wire,
            "negotiated a different version: {reg}"
        );

        for (handler, ty, want) in CASES {
            let at = format!("wire v{wire}, {handler} (0x{ty:04X})");
            let (code, r) = send(&node.url(), "Side", handler, None, "null").await?;
            assert!(code < 300, "{at}: {code} {r}");
            let id = r["id"].as_str().unwrap().to_string();
            let st = within(TEST_DEADLINE, &at, wait_status(&node.url(), &id, want)).await?;

            let journal: serde_json::Value =
                reqwest::get(format!("{}/invocations/{id}/journal", node.url()))
                    .await?
                    .json()
                    .await?;
            let types: Vec<&str> = journal
                .as_array()
                .unwrap()
                .iter()
                .map(|e| e["ty"].as_str().unwrap())
                .collect();
            if *want == "completed" {
                // Side channel: does not become an entry, `Output` is still entry 1.
                assert_eq!(types, ["0x0400", "0x0401"], "{at}: {journal}");
                assert_eq!(st["output"], "1", "{at}: {st}");
            } else {
                // 571 before reaching `Output`: nothing recorded besides `Input`.
                assert_eq!(types, ["0x0400"], "{at}: {journal}");
                assert!(
                    st.to_string().contains("571"),
                    "{at}: the failure does not carry 571: {st}"
                );
            }
        }
        server.abort();
        node.shutdown().await;
    }
    Ok(())
}

/// `StreamAppendMessage { name: "ui", records: [{ step: 1, payload: "\"hi\"" }] }`
/// encoded by hand — the test runs on a binary without its protobuf type.
const STREAM_APPEND_UI: &[u8] = b"\x0a\x02ui\x1a\x08\x08\x01\x22\x04\"hi\"";

async fn invoke_v4(
    AxPath((_service, handler)): AxPath<(String, String)>,
) -> ([(header::HeaderName, &'static str); 1], Bytes) {
    let extra = match handler.as_str() {
        "streamAppend" => encode_frame(MessageType(0x000B), Flags::NONE, STREAM_APPEND_UI),
        // A broken `ArtifactPut` (1-byte sha): a protocol violation ⇒ 571 on every
        // wire (at v4 the execution path rejects it).
        _ => encode_frame(MessageType(0x000C), Flags::NONE, b"\x0a\x01\x00"),
    };
    let mut body = BytesMut::from(&extra[..]);
    body.extend_from_slice(&encode_message(
        MessageType::OUTPUT,
        Flags::NONE,
        &pb::OutputCommandMessage {
            result: Some(pb::output_command_message::Result::Value(
                cairn_protocol::value::inline(Bytes::from_static(b"1")),
            )),
            name: "output".into(),
        },
    ));
    body.extend_from_slice(&encode_message(
        MessageType::END,
        Flags::NONE,
        &pb::EndMessage {},
    ));
    ([(header::CONTENT_TYPE, INVOCATION_V1)], body.freeze())
}

async fn discover_v4(State(wire): State<u32>) -> ([(header::HeaderName, &'static str); 1], String) {
    let manifest = EndpointManifest {
        protocol_mode: ProtocolMode::RequestResponse,
        min_protocol_version: 1,
        max_protocol_version: wire,
        services: vec![ServiceManifest {
            name: "Side".into(),
            ty: ServiceType::Service,
            handlers: ["streamAppend", "artifactPut"]
                .iter()
                .map(|h| HandlerManifest {
                    name: h.to_string(),
                    ty: None,
                    contract: Default::default(),
                })
                .collect(),
            contract: Default::default(),
        }],
    };
    (
        [(header::CONTENT_TYPE, DISCOVERY_V1)],
        serde_json::to_string(&manifest).unwrap(),
    )
}

/// **Wire v4.** `StreamAppend` (`0x000B`) is a side-channel frame opened at v4:
/// below v4 the SDK may not send it, so the runtime returns 571 as for an
/// unknown code (a binary older than this frame sees exactly an unknown code —
/// the same 571); at v4 it does not become an entry, `Output` is still entry 1,
/// the invocation is `completed`. A **broken** `ArtifactPut` (`0x000C`) (1-byte
/// sha): 571 on every wire — below v4 as an unknown code, at v4 the execution
/// path rejects it. A correct `ArtifactPut` at v4:
/// `tests/streams.rs::artifact_put_lands_before_the_output_and_reads_back`.
///
/// | wire | `streamAppend` | `artifactPut` |
/// |---|---|---|
/// | 1–3 | `failed` 571, journal `[Input]` | `failed` 571 |
/// | 4 | `completed`, journal `[Input, Output]` | `failed` 571 |
#[tokio::test]
async fn stream_append_is_side_channel_from_wire_v4() -> Result<()> {
    for wire in 1..=4u32 {
        let node = start_single_process(1).await?;
        let app = Router::new()
            .route("/discover", get(discover_v4))
            .route("/invoke/{service}/{handler}", post(invoke_v4))
            .with_state(wire);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let sdk = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let reg = register(&node.url(), &sdk).await?;
        assert_eq!(reg["wireVersion"], wire, "negotiated: {reg}");
        for (handler, want) in [
            (
                "streamAppend",
                if wire >= 4 { "completed" } else { "failed" },
            ),
            ("artifactPut", "failed"),
        ] {
            let at = format!("wire v{wire}, {handler}");
            let (code, r) = send(&node.url(), "Side", handler, None, "null").await?;
            assert!(code < 300, "{at}: {code} {r}");
            let id = r["id"].as_str().unwrap().to_string();
            let st = within(TEST_DEADLINE, &at, wait_status(&node.url(), &id, want)).await?;
            let journal: serde_json::Value =
                reqwest::get(format!("{}/invocations/{id}/journal", node.url()))
                    .await?
                    .json()
                    .await?;
            let types: Vec<&str> = journal
                .as_array()
                .unwrap()
                .iter()
                .map(|e| e["ty"].as_str().unwrap())
                .collect();
            if want == "completed" {
                assert_eq!(types, ["0x0400", "0x0401"], "{at}: {journal}");
            } else {
                assert_eq!(types, ["0x0400"], "{at}: {journal}");
                assert!(st.to_string().contains("571"), "{at}: {st}");
            }
        }
        server.abort();
        node.shutdown().await;
    }
    Ok(())
}
