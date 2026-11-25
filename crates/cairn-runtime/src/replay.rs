// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Incremental replay for `REQUEST_RESPONSE` — wire v5 (`protocol-v1.md`
//! section 9.3).
//!
//! **Pure, like the state machine:** no I/O, no clock. The input is the turn
//! opening the machine emitted (`Event::Start` ⇒ `Start` ‖ `N` replayed entries
//! ‖ notifications produced while opening the turn); the output is the request
//! body bytes — whole, or omitting the first `B` entries together with
//! `Start.replay_from = B` and `Start.prefix_digest = h_B`.
//!
//! The state machine **does not change**: a delta is only one way of cutting
//! the byte stream it already emitted. The SDK splices `cache[0, B) ‖ the rest`
//! back together and sees exactly the frame sequence of a full replay — which
//! `tests/replay_delta.rs` pins on every `REQUEST_RESPONSE` trace.
//!
//! **The digest covers exactly the bytes sent.** `hᵢ₊₁ = SHA-256(hᵢ ‖
//! u32be(len fᵢ) ‖ fᵢ)` where `fᵢ` is the whole frame (8-byte header + payload)
//! of entry `i`, before compression. `Start` is not part of the chain: it is
//! sent whole on every turn. SHA-256 because correctness depends on it — a
//! collision means the SDK runs on the wrong prefix. Hashed through `ring`
//! (hardware SHA instructions on x86_64 and aarch64): the server hashes the
//! whole replayed part on every turn — the pure-software `sha2` is ~9× slower
//! on aarch64.

use {
    bytes::{Bytes, BytesMut},
    cairn_protocol::{Flags, Message},
    ring::digest::{Context, SHA256},
};

/// One link of the chain.
pub type Digest = [u8; 32];

/// Domain separation for the chain's root.
pub const DOMAIN: &[u8] = b"cairn-replay-v1";

/// `h₀` of invocation `id`.
pub fn chain_root(id: &[u8]) -> Digest {
    sha256(&[DOMAIN, &(id.len() as u32).to_be_bytes(), id])
}

fn sha256(parts: &[&[u8]]) -> Digest {
    let mut c = Context::new(&SHA256);
    for p in parts {
        c.update(p);
    }
    c.finish().as_ref().try_into().expect("SHA-256 is 32 bytes")
}

/// `hᵢ₊₁` from `hᵢ` and the whole frame of entry `i`.
pub fn chain_step(h: &Digest, frame: &[u8]) -> Digest {
    sha256(&[h, &(frame.len() as u32).to_be_bytes(), frame])
}

/// The chain `h₀ … h_n` over `frames` (n elements) of invocation `id`.
pub fn chain<F: AsRef<[u8]>>(id: &[u8], frames: impl IntoIterator<Item = F>) -> Vec<Digest> {
    let mut out = vec![chain_root(id)];
    for f in frames {
        let next = chain_step(out.last().expect("has a root"), f.as_ref());
        out.push(next);
    }
    out
}

/// An encoded RR turn opening, with the digest chain of the replayed part.
///
/// Encoded **once**: the whole body and the delta body are built from the same
/// hashed frames, so the digest sent covers exactly the bytes the SDK will
/// splice.
#[derive(Clone, Debug)]
pub struct Opening {
    start: cairn_protocol::pb::StartMessage,
    /// The whole frames of every message after `Start`, in order.
    rest: Vec<Bytes>,
    /// `N` = `Start.known_entries`: `rest[0..N)` are the replayed journal entries.
    known: u32,
    /// `h₀ … h_N`.
    chain: Vec<Digest>,
}

impl Opening {
    /// `None` when `opening` does not open with `Start` or is shorter than
    /// `known_entries` — not a turn opening the machine emitted; the caller
    /// sends it whole as in v4.
    pub fn new(opening: &[Message]) -> Option<Self> {
        let (Message::Start(start), rest) = opening.split_first()? else {
            return None;
        };
        let known = start.known_entries;
        if rest.len() < known as usize {
            return None;
        }
        let rest: Vec<Bytes> = rest.iter().map(|m| m.encode(Flags::NONE)).collect();
        let chain = chain(&start.id, &rest[..known as usize]);
        Some(Self {
            start: start.clone(),
            rest,
            known,
            chain,
        })
    }

    /// `N` — the number of journal entries in the replayed part.
    pub fn known_entries(&self) -> u32 {
        self.known
    }

    /// `h_b` (`b ≤ N`), or `None` when beyond.
    pub fn digest(&self, b: u32) -> Option<&Digest> {
        self.chain.get(b as usize)
    }

    /// The size of the whole body (`body(0).len()`) without building the body.
    pub fn full_len(&self) -> usize {
        Message::Start(self.start.clone()).encode(Flags::NONE).len()
            + self.rest.iter().map(Bytes::len).sum::<usize>()
    }

    /// The bytes of the replayed part `[0, b)` (whole frames) — what the delta body omits.
    pub fn prefix_bytes(&self, b: u32) -> usize {
        self.rest[..(b.min(self.known) as usize)]
            .iter()
            .map(Bytes::len)
            .sum()
    }

    /// The request body: `from = 0` ⇒ whole, byte-identical to v4's
    /// `encode_stream`; `0 < from ≤ N` ⇒ `Start{replay_from, prefix_digest}` ‖
    /// `rest[from..]`. `from > N` is never sent: clamped to whole.
    pub fn body(&self, from: u32) -> Bytes {
        let (start, skip) = match self.digest(from) {
            Some(h) if from > 0 => {
                let mut s = self.start.clone();
                s.replay_from = from;
                s.prefix_digest = Bytes::copy_from_slice(h);
                (Message::Start(s), from as usize)
            }
            _ => (Message::Start(self.start.clone()), 0),
        };
        let mut out = BytesMut::new();
        out.extend_from_slice(&start.encode(Flags::NONE));
        for f in &self.rest[skip..] {
            out.extend_from_slice(f);
        }
        out.freeze()
    }
}

#[cfg(test)]
mod tests {
    use {super::*, cairn_protocol::pb};

    fn opening(n: usize) -> Vec<Message> {
        let mut out = vec![Message::Start(pb::StartMessage {
            id: Bytes::from_static(&[7; 24]),
            known_entries: n as u32,
            ..Default::default()
        })];
        for i in 0..n + 1 {
            out.push(Message::Sleep(pb::SleepCommandMessage {
                wake_up_time: i as u64,
                result_completion_id: i as u32 + 1,
                name: format!("s{i}"),
            }));
        }
        out
    }

    /// The CPU cost of hashing + encoding the turn opening in the agent cell 50 ×
    /// 100 KiB. Run by hand, release build:
    /// `cargo test --release -p cairn-runtime --lib replay::tests::cost -- --ignored --nocapture`.
    #[test]
    #[ignore = "manual measurement"]
    #[allow(
        clippy::disallowed_methods,
        clippy::disallowed_types,
        reason = "measures real time, outside the deterministic simulation"
    )]
    fn cost_of_hashing_a_50_step_journal() {
        let payload = Bytes::from(vec![b'x'; 100 << 10]);
        let mut o = vec![Message::Start(pb::StartMessage {
            id: Bytes::from_static(&[7; 24]),
            known_entries: 101,
            ..Default::default()
        })];
        o.push(Message::Input(pb::InputCommandMessage::default()));
        for i in 0..50u32 {
            o.push(Message::Run(pb::RunCommandMessage {
                result_completion_id: i + 1,
                name: format!("s{i}"),
            }));
            o.push(Message::RunCompletion(
                pb::RunCompletionNotificationMessage {
                    completion_id: i + 1,
                    result: Some(pb::run_completion_notification_message::Result::Value(
                        cairn_protocol::value::inline(payload.clone()),
                    )),
                },
            ));
        }
        let n = 30;
        let t = std::time::Instant::now();
        for _ in 0..n {
            let mut b = BytesMut::new();
            for m in &o {
                b.extend_from_slice(&m.encode(Flags::NONE));
            }
            std::hint::black_box(b);
        }
        let plain = t.elapsed() / n;
        let t = std::time::Instant::now();
        for _ in 0..n {
            let e = Opening::new(&o).unwrap();
            std::hint::black_box(e.body(99));
        }
        let delta = t.elapsed() / n;
        let bytes: usize = o.iter().map(|m| m.encode(Flags::NONE).len()).sum();
        println!(
            "journal {bytes} bytes: whole encoding (v4) {plain:?}, encoding + hashing + delta body {delta:?}"
        );
    }

    /// `from = 0` is exactly the v4 body — old traces do not change by a byte.
    #[test]
    fn full_body_is_v4_body() {
        let o = opening(3);
        let mut want = BytesMut::new();
        for m in &o {
            want.extend_from_slice(&m.encode(Flags::NONE));
        }
        let e = Opening::new(&o).unwrap();
        assert_eq!(e.full_len(), want.len());
        assert_eq!(e.body(0), want.freeze());
        assert_eq!(e.body(99), e.body(0), "beyond N ⇒ whole");
    }

    /// The golden vector — the same value as in `sdk/typescript/test/replay.test.ts`.
    /// Changing it changes the wire (protocol section 9.3): every SDK changes in
    /// the same commit.
    #[test]
    fn golden_vector_is_stable() {
        fn hex(d: &Digest) -> String {
            d.iter().map(|b| format!("{b:02x}")).collect()
        }
        let input = [
            0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x72, 0x07, 0x0a, 0x05, 0x22, 0x72,
            0x65, 0x71, 0x22,
        ];
        let c = chain(&[0x11; 24], [input.as_slice()]);
        assert_eq!(
            hex(&c[0]),
            "1f64388c3fc28f0042a5c57665b4b9a00b95f335a30260a54323acffdd41bc74"
        );
        assert_eq!(
            hex(&c[1]),
            "caedb1a740dbe983c6a7f0a4be6a7f94e8f0b48fd0324664f1b51c6d3994367a"
        );
    }

    /// The chain depends on the id, the order and every byte.
    #[test]
    fn chain_binds_id_order_and_bytes() {
        let a = chain(&[1; 24], [b"x".as_slice(), b"y"]);
        assert_ne!(a, chain(&[2; 24], [b"x".as_slice(), b"y"]));
        assert_ne!(a, chain(&[1; 24], [b"y".as_slice(), b"x"]));
        assert_ne!(a, chain(&[1; 24], [b"x".as_slice(), b"z"]));
        // Frame boundaries are inside the hash: "xy" ‖ "" differs from "x" ‖ "y".
        assert_ne!(a[2], chain(&[1; 24], [b"xy".as_slice(), b""])[2]);
        assert_eq!(a.len(), 3);
    }
}
