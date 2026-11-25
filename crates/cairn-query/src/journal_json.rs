// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Journal decoded for readers — the JSON shape shared by the `entry_lite`/`body`
//! columns of `sys_journal` and `GET /invocations/{id}/journal?decode=1`.
//!
//! The projector ([`cairn_protocol::journal_view`]) says what an entry is and
//! where its body is; this adds two things visible only with the **whole**
//! journal: which commands are still waiting (`completed: false` — "what is it
//! waiting for") and the callee id of a `Call` (the
//! `CallInvocationIdCompletion` notification sits in another entry).

use {
    bytes::Bytes,
    cairn_protocol::{
        journal_view::{pending, view, EntryView, SignalRef},
        MessageType,
    },
    serde_json::{json, Map, Value},
    std::collections::BTreeMap,
};

/// Body of an entry: `{encoding, size, truncated, text | base64}`.
///
/// - `json`: the whole body is valid JSON — `text` is verbatim (the reader
///   parses it itself; no parse-and-print round trip changes numbers or key
///   order);
/// - `utf8`: text that is not JSON, cut at a character boundary;
/// - `base64`: the rest.
///
/// Never a number array like a plain serde of bytes (3–4 characters per byte).
pub fn body_json(b: &[u8], max: usize) -> Value {
    let truncated = b.len() > max;
    let head = &b[..b.len().min(max)];
    match std::str::from_utf8(b) {
        Ok(text) if !truncated && serde_json::from_str::<serde::de::IgnoredAny>(text).is_ok() => {
            json!({ "encoding": "json", "size": b.len(), "truncated": false, "text": text })
        }
        _ => match utf8_prefix(head, truncated) {
            Some(text) => {
                json!({ "encoding": "utf8", "size": b.len(), "truncated": truncated, "text": text })
            }
            None => {
                json!({ "encoding": "base64", "size": b.len(), "truncated": truncated, "base64": base64(head) })
            }
        },
    }
}

/// `head` is UTF-8 — or, when cut, UTF-8 up to just before a partial character.
fn utf8_prefix(head: &[u8], truncated: bool) -> Option<&str> {
    match std::str::from_utf8(head) {
        Ok(s) => Some(s),
        // Cut in the middle of a multi-byte character: the error is only in the last ≤ 3 bytes.
        Err(e) if truncated && e.error_len().is_none() && head.len() - e.valid_up_to() < 4 => {
            std::str::from_utf8(&head[..e.valid_up_to()]).ok()
        }
        Err(_) => None,
    }
}

/// Standard base64 (RFC 4648, with `=`). Twenty lines, not worth a crate.
fn base64(b: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(b.len().div_ceil(3) * 4);
    for c in b.chunks(3) {
        let n = (u32::from(c[0]) << 16)
            | (u32::from(*c.get(1).unwrap_or(&0)) << 8)
            | u32::from(*c.get(2).unwrap_or(&0));
        for i in 0..4 {
            if i <= c.len() {
                out.push(A[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// The head of an entry, as camelCase JSON, absent fields omitted. `completed`
/// only for commands that wait for a result; `callee` is the invocation id a
/// `Call`/`OneWayCall` creates.
fn lite_json(v: &EntryView, completed: Option<bool>, callee: Option<&str>) -> Value {
    let l = &v.lite;
    let mut m = Map::new();
    let mut put = |k: &str, val: Value| {
        if !val.is_null() {
            m.insert(k.into(), val);
        }
    };
    put("kind", json!(l.kind));
    put("name", json!(l.name));
    put("service", json!(l.service));
    put("handler", json!(l.handler));
    put("key", json!(l.key));
    put("invocationId", json!(l.invocation_id.as_deref().or(callee)));
    put("wakeUpAtMs", json!(l.wake_up_at_ms));
    put("promise", json!(l.promise));
    put("stateKey", json!(l.state_key));
    put(
        "signal",
        match &l.signal {
            Some(SignalRef::Index(i)) => json!({ "index": i }),
            Some(SignalRef::Name(n)) => json!({ "name": n }),
            None => Value::Null,
        },
    );
    put(
        "failure",
        match &l.failure {
            Some((code, message)) => json!({ "code": code, "message": message }),
            None => Value::Null,
        },
    );
    put("awaits", json!(l.awaits));
    put("completes", json!(l.completes));
    put("completed", json!(completed));
    put("hasBody", json!(v.body.is_some()));
    Value::Object(m)
}

/// One decoded entry: head and body (if any).
pub struct Decoded {
    pub lite: Value,
    pub body: Option<Value>,
}

/// Decode a whole journal `(ty, payload)` in write order. Bodies cut at `max` bytes.
pub fn decode_journal(entries: &[(u16, Bytes)], max: usize) -> Vec<Decoded> {
    let views: Vec<EntryView> = entries
        .iter()
        .map(|(ty, p)| view(MessageType(*ty), p))
        .collect();
    let waiting: std::collections::BTreeSet<usize> = pending(&views).into_iter().collect();
    // completion id ⇒ invocation id carried by the `CallInvocationIdCompletion` notification.
    let ids: BTreeMap<u32, &str> = views
        .iter()
        .filter_map(|v| Some((v.lite.completes?, v.lite.invocation_id.as_deref()?)))
        .collect();
    views
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let completed = v.lite.awaits.map(|_| !waiting.contains(&i));
            let callee = v
                .lite
                .awaits_invocation_id
                .and_then(|c| ids.get(&c).copied());
            Decoded {
                lite: lite_json(v, completed, callee),
                body: v.body.as_deref().map(|b| body_json(b, max)),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use {super::*, cairn_protocol::pb, prost::Message as _};

    #[test]
    fn body_encodings() {
        let j = body_json(br#"{"a": [1, 2]}"#, 1024);
        assert_eq!(j["encoding"], "json");
        assert_eq!(j["text"], r#"{"a": [1, 2]}"#);
        let t = body_json("hello €".as_bytes(), 1024);
        assert_eq!(
            (t["encoding"].as_str(), t["text"].as_str()),
            (Some("utf8"), Some("hello €"))
        );
        let b = body_json(&[0xff, 0x00, 0x10], 1024);
        assert_eq!(
            (b["encoding"].as_str(), b["base64"].as_str()),
            (Some("base64"), Some("/wAQ"))
        );
        assert_eq!(b["size"], 3);
        // Cut: long JSON becomes utf8 cut at a character boundary, with `truncated`.
        let long = format!("\"{}\"", "€".repeat(10));
        let c = body_json(long.as_bytes(), 6);
        assert_eq!(c["encoding"], "utf8");
        assert_eq!(c["truncated"], true);
        assert_eq!(c["text"], "\"€");
        assert_eq!(c["size"], long.len());
    }

    #[test]
    fn base64_matches_rfc4648() {
        for (raw, want) in [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
            ("fooba", "Zm9vYmE="),
            ("foobar", "Zm9vYmFy"),
        ] {
            assert_eq!(base64(raw.as_bytes()), want, "{raw}");
        }
    }

    /// A `Sleep` command without a notification ⇒ `completed: false` ("what is
    /// it waiting for"); `Call` carries the callee id from the notification in
    /// a later entry.
    #[test]
    fn waiting_and_callee() {
        let e = |ty: MessageType, m: &dyn Fn() -> Vec<u8>| (ty.raw(), Bytes::from(m()));
        let journal = vec![
            e(MessageType::CALL, &|| {
                pb::CallCommandMessage {
                    service_name: "Payment".into(),
                    handler_name: "auth".into(),
                    invocation_id_notification_idx: 1,
                    result_completion_id: 2,
                    ..Default::default()
                }
                .encode_to_vec()
            }),
            e(MessageType::CALL_INVOCATION_ID_COMPLETION, &|| {
                pb::CallInvocationIdCompletionNotificationMessage {
                    completion_id: 1,
                    invocation_id: "9be1".into(),
                }
                .encode_to_vec()
            }),
            e(MessageType::SLEEP, &|| {
                pb::SleepCommandMessage {
                    wake_up_time: 99,
                    result_completion_id: 3,
                    name: "nap".into(),
                }
                .encode_to_vec()
            }),
        ];
        let d = decode_journal(&journal, 1024);
        assert_eq!(d[0].lite["kind"], "Call");
        assert_eq!(d[0].lite["invocationId"], "9be1");
        assert_eq!(d[0].lite["completed"], false, "call result has not arrived");
        assert!(
            d[1].lite.get("completed").is_none(),
            "notification waits for nothing"
        );
        assert_eq!(d[2].lite["completed"], false);
        assert_eq!(d[2].lite["wakeUpAtMs"], 99);
        assert_eq!(d[2].lite["name"], "nap");
    }
}
