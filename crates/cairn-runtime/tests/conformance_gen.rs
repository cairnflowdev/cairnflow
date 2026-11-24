// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The conformance trace generator — `conformance/traces/*.json`.
//!
//! Two tests, like the sample-bucket generator of `format_fixture.rs`:
//!
//! - [`traces_match_runtime`] (runs normally): regenerates every trace **in
//!   memory** and compares it character by character with the committed file.
//!   Red ⇒ the runtime (state machine, transport rules) or the reference SDK
//!   has changed its wire behaviour. If the change is deliberate: regenerate,
//!   then run the runner of every SDK (`sdk/typescript/test/conformance.test.ts`).
//! - [`regenerate_traces`] (`#[ignore]`): overwrites the trace directory, **only**
//!   with `CAIRNFLOW_REGENERATE_CONFORMANCE=1`:
//!
//! ```sh
//! CAIRNFLOW_REGENERATE_CONFORMANCE=1 cargo test -p cairn-runtime --test conformance_gen --
//! --ignored
//! ```

// Tests run on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

mod conformance;

use {
    conformance::{scenarios, trace},
    std::{
        collections::BTreeMap,
        path::{Path, PathBuf},
    },
};

fn traces_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../conformance/traces")
}

/// File name → contents, of every trace the runtime produces today.
fn generated() -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for t in scenarios::all() {
        let file = format!("{}.json", t.scenario.name);
        let text = trace::to_json(&t).pretty();
        assert!(
            out.insert(file.clone(), text).is_none(),
            "duplicate trace name {file}"
        );
    }
    out
}

fn committed() -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for e in std::fs::read_dir(traces_dir()).expect("conformance/traces is missing") {
        let p = e.unwrap().path();
        if p.extension().is_some_and(|x| x == "json") {
            let name = p.file_name().unwrap().to_string_lossy().into_owned();
            out.insert(name, std::fs::read_to_string(&p).unwrap());
        }
    }
    out
}

const REGEN: &str = "CAIRNFLOW_REGENERATE_CONFORMANCE=1 cargo test -p cairn-runtime \
                     --test conformance_gen -- --ignored";

#[test]
fn traces_match_runtime() {
    let want = generated();
    let have = committed();
    let missing: Vec<_> = want.keys().filter(|k| !have.contains_key(*k)).collect();
    let stale: Vec<_> = have.keys().filter(|k| !want.contains_key(*k)).collect();
    assert!(
        missing.is_empty() && stale.is_empty(),
        "the trace set is off: missing {missing:?}, stale {stale:?} — regenerate with `{REGEN}`"
    );
    for (name, text) in &want {
        if have[name] != *text {
            let line = have[name]
                .lines()
                .zip(text.lines())
                .position(|(a, b)| a != b)
                .map_or(0, |i| i + 1);
            panic!(
                "{name} drifted from the runtime from line {line}. The wire behaviour has changed; \
                 if that is deliberate, regenerate with `{REGEN}` and then run the runner of every \
                 SDK"
            );
        }
    }
}

#[test]
#[ignore = "only writes with CAIRNFLOW_REGENERATE_CONFORMANCE=1"]
fn regenerate_traces() {
    if std::env::var("CAIRNFLOW_REGENERATE_CONFORMANCE").as_deref() != Ok("1") {
        eprintln!("skipped: set CAIRNFLOW_REGENERATE_CONFORMANCE=1 to write conformance/traces");
        return;
    }
    let dir = traces_dir();
    std::fs::create_dir_all(&dir).unwrap();
    for name in committed().keys() {
        std::fs::remove_file(dir.join(name)).unwrap();
    }
    for (name, text) in generated() {
        std::fs::write(dir.join(&name), text).unwrap();
    }
}

/// On **every** `REQUEST_RESPONSE` connection of every trace, for **every** mark
/// `0 < B ≤ N`, the delta body (`replay::Opening::body`) spliced with the prefix
/// the SDK kept (`[0, B)` of the full replay) gives exactly the frame sequence
/// of the full replay, byte for byte — except the two v5 fields of `Start`, and
/// `prefix_digest` equals the chain the SDK computes itself over the bytes it
/// received.
#[test]
fn delta_rejoins_to_full_replay_on_every_rr_trace() {
    use {
        bytes::Bytes,
        cairn_protocol::{decode, Message},
        cairn_runtime::replay,
        conformance::sim::From,
    };
    let mut checked = 0usize;
    for t in scenarios::all() {
        if t.scenario.transport != conformance::sdk::Transport::Rr {
            continue;
        }
        for (ci, conn) in t.conns.iter().enumerate() {
            let full: Vec<_> = conn
                .frames
                .iter()
                .filter(|f| f.from == From::Runtime)
                .collect();
            let msgs: Vec<Message> = full.iter().map(|f| decode(&f.frame).unwrap()).collect();
            // A v5 trace's connection already carries a delta body: its splice
            // is the trace itself (the runner compares every byte); here only
            // whole bodies are considered.
            if matches!(&msgs[0], Message::Start(s) if s.replay_from > 0) {
                continue;
            }
            let opening = replay::Opening::new(&msgs).expect("a turn opening starts with Start");
            let n = opening.known_entries();
            let Message::Start(start) = &msgs[0] else {
                unreachable!()
            };
            // The whole body: exactly the v4 bytes recorded in the trace.
            let wire: Vec<u8> = full.iter().flat_map(|f| f.wire.to_vec()).collect();
            assert_eq!(
                opening.body(0).as_ref(),
                wire.as_slice(),
                "{} connection {ci}",
                t.scenario.name
            );
            for b in 1..=n {
                let delta = split_frames(opening.body(b));
                let Message::Start(ds) = decode(&delta[0]).unwrap() else {
                    panic!("the delta does not open with Start")
                };
                assert_eq!(ds.replay_from, b);
                // The SDK: the chain over exactly the bytes it received in the full replay.
                let sdk_chain = replay::chain(
                    &start.id,
                    full[1..=b as usize].iter().map(|f| f.wire.clone()),
                );
                assert_eq!(ds.prefix_digest.as_ref(), &sdk_chain[b as usize][..]);
                let mut plain = ds.clone();
                plain.replay_from = 0;
                plain.prefix_digest = Bytes::new();
                assert_eq!(
                    &plain, start,
                    "Start apart from the two v5 fields must stay the same"
                );
                // Splice: the cache's prefix ‖ the rest of the delta body.
                let rejoined: Vec<Bytes> = full[1..=b as usize]
                    .iter()
                    .map(|f| f.wire.clone())
                    .chain(
                        delta[1..]
                            .iter()
                            .map(|f| cairn_protocol::encode_frame(f.ty, f.flags, &f.payload)),
                    )
                    .collect();
                let want: Vec<Bytes> = full[1..].iter().map(|f| f.wire.clone()).collect();
                assert_eq!(
                    rejoined, want,
                    "{} connection {ci}, B = {b}: the spliced delta differs from the full replay",
                    t.scenario.name
                );
                checked += 1;
            }
        }
    }
    assert!(
        checked > 100,
        "only {checked} (connection, B) pairs checked"
    );
}

/// Splits a body into frames (like `invoker::decode_stream`, without pulling in `cairn-server`).
fn split_frames(body: bytes::Bytes) -> Vec<cairn_protocol::Frame> {
    let dec = cairn_protocol::FrameDecoder::new();
    let mut buf = bytes::BytesMut::from(&body[..]);
    let mut out = Vec::new();
    while let Some(f) = dec.decode(&mut buf).unwrap() {
        out.push(f);
    }
    assert!(buf.is_empty(), "leftover bytes");
    out
}
