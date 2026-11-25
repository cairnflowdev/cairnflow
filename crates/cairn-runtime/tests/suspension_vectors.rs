// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **CG-4:** the runtime's satisfaction rule for suspension trees
//! (`suspension_satisfied`) matches the SDK's rule on **every** tree the SDK
//! emits.
//!
//! Shared vectors in `conformance/suspension-vectors.json`: the
//! `SuspensionMessage` payload bytes exactly as the TS SDK encodes them (the TS
//! runner checks that), and every combination of arrived leaves with the
//! expected verdict. Here: decode the bytes with `prost`, ask the real state
//! machine. A disagreement ⇒ the SDK reruns the handler when the runtime thinks
//! it is not satisfied (an extra turn), or — worse — the SDK waits forever for
//! what the runtime considers satisfied (a hung stream; see the SDK's
//! `SUSPEND_STALL_MS` warning).

use {
    cairn_protocol::pb::{self, signal_notification_message::SignalId},
    cairn_runtime::machine::{suspension_satisfied, SettledSignals},
    prost::Message,
};

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

#[test]
fn runtime_and_sdk_agree_on_every_vector() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../conformance/suspension-vectors.json"
    );
    let doc: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path).expect("reading the vectors"))
            .expect("JSON");
    let vectors = doc["vectors"].as_array().expect("vectors");
    assert!(vectors.len() >= 10, "too few vectors: {}", vectors.len());
    let mut checked = 0;
    for v in vectors {
        let name = v["name"].as_str().unwrap_or("?");
        let msg = pb::SuspensionMessage::decode(&unhex(v["hex"].as_str().expect("hex"))[..])
            .unwrap_or_else(|e| panic!("{name}: the payload does not decode: {e}"));
        for s in v["states"].as_array().expect("states") {
            let completions: Vec<u32> = s["completions"]
                .as_array()
                .expect("completions")
                .iter()
                .map(|c| c.as_u64().expect("number") as u32)
                .collect();
            let mut signals: Vec<SignalId> = s["signals"]
                .as_array()
                .expect("signals")
                .iter()
                .map(|i| SignalId::Idx(i.as_u64().expect("number") as u32))
                .collect();
            signals.extend(
                s["names"]
                    .as_array()
                    .expect("names")
                    .iter()
                    .map(|n| SignalId::Name(n.as_str().expect("string").to_owned())),
            );
            let got =
                suspension_satisfied(&msg, &completions, SettledSignals { signals: &signals });
            assert_eq!(
                got,
                s["satisfied"].as_bool().expect("satisfied"),
                "{name}: the runtime says {got} for {s}"
            );
            checked += 1;
        }
    }
    assert!(checked > 70, "only {checked} states checked");
}
