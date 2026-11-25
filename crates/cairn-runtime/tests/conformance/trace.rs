// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Trace → JSON per `conformance/trace.schema.json`.

use {
    super::{
        json::{hex, J},
        sim::{Close, Final, From, Trace, TraceFrame, CLOCK_MS},
    },
    cairn_protocol::{decode, pb, Frame, Message, MessageType},
    cairn_runtime::{FailureAction, JournalEntry},
};

pub const FORMAT: &str = "cairnflow-conformance/1";
/// The service holding every conformance handler (README, Handlers section).
pub const SERVICE: &str = "Conformance";

fn type_hex(ty: MessageType) -> String {
    format!("0x{:04x}", ty.raw())
}

/// The message name (`Sleep`, `ProposeRunCompletionAck`, …), without the code.
fn type_name(ty: MessageType) -> String {
    let s = ty.to_string();
    s.split('(').next().unwrap_or(&s).to_string()
}

fn summary(f: &Frame) -> String {
    match decode(f) {
        Ok(Message::Custom { ty, payload }) => {
            format!("Custom {{ ty: {}, payload: {payload:?} }}", type_hex(ty))
        }
        // The two v5 fields of `Start` (spec section 9.3) at their default
        // values are not on the wire — the decoded summary of pre-v5 traces does
        // not mention them either, so old traces do not change by a byte. The v6
        // field `segment_urls` likewise: empty means not on the wire, and old
        // traces do not mention it.
        Ok(m @ Message::Start(pb::StartMessage { replay_from: 0, .. })) => format!("{m:?}")
            .replace(", segment_urls: [] }", " }")
            .replace(", replay_from: 0, prefix_digest: b\"\" }", " }"),
        Ok(m @ Message::Start(_)) => format!("{m:?}").replace(", segment_urls: [] }", " }"),
        Ok(m) => format!("{m:?}"),
        Err(e) => format!("<cannot decode: {e}>"),
    }
}

/// The side channel: frames that travel on the connection but never become
/// journal entries, wait for no durable ack, and change no journal wherever
/// they are inserted (spec section 9.2).
pub fn channel(ty: MessageType) -> &'static str {
    if ty == MessageType::KEEPALIVE
        || ty == MessageType::STREAM_APPEND
        || ty == MessageType::ARTIFACT_PUT
        || ty.raw() >= 0xFC00
    {
        "side"
    } else {
        "journal"
    }
}

/// The comparison for one SDK → runtime frame. `bytes`, except where the
/// payload carries text the SDK chooses itself (error messages) — there only
/// the protocol-meaningful part is pinned.
fn match_rule(f: &Frame) -> Vec<(&'static str, J)> {
    match decode(f) {
        Ok(Message::Error(e)) => vec![
            ("match", J::str("error")),
            ("error_code", J::num(e.code)),
            ("error_behavior", J::num(e.behavior)),
        ],
        Ok(Message::Output(pb::OutputCommandMessage {
            result: Some(pb::output_command_message::Result::Failure(_)),
            ..
        })) => vec![("match", J::str("type"))],
        _ => vec![],
    }
}

fn frame_json(tf: &TraceFrame) -> J {
    let f = &tf.frame;
    let mut fields = vec![
        (
            "from",
            J::str(match tf.from {
                From::Runtime => "runtime",
                From::Sdk => "sdk",
            }),
        ),
        ("channel", J::str(channel(f.ty))),
        ("type", J::str(type_hex(f.ty))),
        ("name", J::str(type_name(f.ty))),
        ("flags", J::str(format!("0x{:04x}", f.flags.raw()))),
        ("hex", J::str(hex(&tf.wire))),
    ];
    if tf.from == From::Sdk {
        fields.extend(match_rule(f));
    }
    fields.push(("summary", J::str(summary(f))));
    J::Obj(fields)
}

fn entry_json(i: usize, e: &JournalEntry) -> J {
    let f = Frame::new(e.ty, cairn_protocol::Flags::NONE, e.payload.clone());
    let mut fields = vec![
        ("index", J::num(i as i64)),
        ("type", J::str(type_hex(e.ty))),
        ("name", J::str(type_name(e.ty))),
        ("payload_hex", J::str(hex(&e.payload))),
    ];
    let rule = match_rule(&f);
    if !rule.is_empty() {
        fields.push(("match", J::str("type")));
    }
    fields.push(("summary", J::str(summary(&f))));
    J::Obj(fields)
}

fn outcome_json(o: &Final) -> J {
    match o {
        Final::Completed(out) => match &out.result {
            Some(pb::output_command_message::Result::Value(v)) => J::Obj(vec![
                ("kind", J::str("completed")),
                // The **logical** bytes (segments ‖ tail) — a segmented `Output`
                // (v6) carries refs; inline is exactly `content` as before.
                (
                    "value_hex",
                    J::str(hex(&super::sim::logical_bytes(
                        &cairn_protocol::value::Logical::of(Some(v)).expect("a valid Value"),
                    ))),
                ),
            ]),
            Some(pb::output_command_message::Result::Failure(f)) => J::Obj(vec![
                ("kind", J::str("completed")),
                ("failure_code", J::num(f.code)),
            ]),
            None => J::Obj(vec![("kind", J::str("completed"))]),
        },
        Final::Failed { code, action } => J::Obj(vec![
            ("kind", J::str("failed")),
            ("code", J::num(*code)),
            (
                "action",
                J::str(match action {
                    FailureAction::Retry { .. } => "retry",
                    FailureAction::Pause => "pause",
                    FailureAction::Fail => "fail",
                }),
            ),
        ]),
        Final::Refused => J::Obj(vec![("kind", J::str("refused"))]),
        Final::ReplayReport(r) => J::Obj(vec![
            ("kind", J::str("replay_report")),
            ("matched", J::num(r.matched)),
            ("mismatch", J::Bool(r.mismatch)),
        ]),
    }
}

pub fn to_json(t: &Trace) -> J {
    let sc = &t.scenario;
    J::Obj(
        [
            ("format", J::str(FORMAT)),
            ("name", J::str(sc.name.clone())),
            ("description", J::str(sc.description.clone())),
            (
                "source",
                J::Arr(sc.source.iter().map(|s| J::str(s.clone())).collect()),
            ),
            ("spec", J::Arr(sc.spec.iter().map(|s| J::str(*s)).collect())),
            (
                "matrix_cells",
                J::Arr(sc.cells.iter().map(|c| J::num(*c)).collect()),
            ),
            ("wire_version", J::num(sc.wire)),
        ]
        .into_iter()
        .chain(
            sc.sdk_max_wire
                .map(|m| ("sdk_max_protocol_version", J::num(m))),
        )
        .chain([
            ("mode", J::str(sc.transport.mode())),
            (
                "handler",
                J::Obj(vec![
                    ("service", J::str(SERVICE)),
                    ("name", J::str(sc.handler)),
                ]),
            ),
            ("clock_ms", J::num(CLOCK_MS)),
            ("initial_journal_len", J::num(t.initial_len as i64)),
            (
                "connections",
                J::Arr(
                    t.conns
                        .iter()
                        .map(|c| {
                            let mut fields = vec![
                                ("verify", J::Bool(c.verify)),
                                (
                                    "close",
                                    J::str(match c.close {
                                        Close::Sdk => "sdk",
                                        Close::Runtime => "runtime",
                                    }),
                                ),
                            ];
                            // Wire v5 (spec section 9.3): only v5 traces have this
                            // field — old traces do not change by a byte.
                            if let Some(r) = &c.replay {
                                let mut o = vec![];
                                if r.sdk_restart {
                                    o.push(("sdk_restart", J::Bool(true)));
                                }
                                if r.miss {
                                    o.push(("miss", J::Bool(true)));
                                }
                                if let Some(k) = r.cache {
                                    o.push(("cache", J::num(k)));
                                }
                                if r.stale_sdk {
                                    o.push(("stale_sdk", J::Bool(true)));
                                }
                                if let Some((k, h)) = &r.held {
                                    o.push(("held", J::num(*k)));
                                    o.push(("held_digest", J::str(hex(h))));
                                }
                                fields.push(("replay", J::Obj(o)));
                            }
                            if let Some(status) = c.status {
                                fields.push(("status", J::num(status)));
                            }
                            fields.push((
                                "frames",
                                J::Arr(c.frames.iter().map(frame_json).collect()),
                            ));
                            J::Obj(fields)
                        })
                        .collect(),
                ),
            ),
            (
                "journal",
                J::Arr(
                    t.journal
                        .iter()
                        .enumerate()
                        .map(|(i, e)| entry_json(i, e))
                        .collect(),
                ),
            ),
            ("outcome", outcome_json(&t.outcome)),
        ])
        // Wire v6: the segment table the runner serves (URL ⇒ bytes) — only
        // traces with segmented state.
        .chain((!sc.seg_state.is_empty()).then(|| {
            (
                "segments",
                J::Arr(
                    sc.seg_state
                        .iter()
                        .map(|(_, d, _)| {
                            J::Obj(vec![
                                ("url", J::str(d.url())),
                                ("hex", J::str(hex(d.bytes))),
                            ])
                        })
                        .collect(),
                ),
            )
        }))
        .collect(),
    )
}
