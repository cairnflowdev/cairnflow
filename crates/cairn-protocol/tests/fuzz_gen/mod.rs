// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Shared generators for protocol fuzzing.
//!
//! Lives in `cairn-protocol`'s `tests/` and is pulled into other crates
//! (`cairn-runtime`, `cairn-engine`) with `#[path = …] mod fuzz_gen;`: it is
//! only test code, not worth a crate feature.

#![allow(dead_code)]

use {
    cairn_protocol::MessageType,
    proptest::{prelude::*, test_runner::Config},
};

/// A number in a protobuf field: biased towards small numbers (completion ids
/// and entry indexes must collide to reach the duplicate/missing checks),
/// otherwise any `u64`.
pub fn number() -> impl Strategy<Value = u64> {
    prop_oneof![3 => 0u64..8, 1 => any::<u64>()]
}

pub fn budget(cases: u32) -> Config {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(cases);
    Config {
        cases,
        failure_persistence: None,
        ..Config::default()
    }
}

/// A message code: half known codes (to get deep into prost), half any `u16`.
pub fn message_type() -> impl Strategy<Value = MessageType> {
    prop_oneof![
        (0u16..=0x000F).prop_map(MessageType),
        (0x0400u16..=0x0414).prop_map(MessageType),
        prop::sample::select(vec![
            0x8002u16, 0x8006, 0x8009, 0x800A, 0x800B, 0x800C, 0x800D, 0x800E, 0x8011, 0x8012,
            0x8013, 0xFBFF,
        ])
        .prop_map(MessageType),
        any::<u16>().prop_map(MessageType),
    ]
}

/// A "near-protobuf" payload: a sequence of `(field number, wire type, value)`
/// fields that is syntactically right at the raw level, with truncations/byte
/// flips. Pure random bytes rarely get past prost's first tag, so most of the
/// budget goes here.
pub fn protobuf_like() -> impl Strategy<Value = Vec<u8>> {
    let field = (
        1u32..24,
        0u8..6,
        prop::collection::vec(any::<u8>(), 0..24),
        number(),
    );
    (
        prop::collection::vec(field, 0..12),
        any::<Option<(usize, u8)>>(),
        any::<Option<usize>>(),
    )
        .prop_map(|(fields, flip, cut)| {
            let mut out = Vec::new();
            for (no, wt, bytes, n) in fields {
                varint(&mut out, (u64::from(no) << 3) | u64::from(wt));
                match wt {
                    0 => varint(&mut out, n),
                    1 => out.extend_from_slice(&n.to_le_bytes()),
                    2 => {
                        // The declared length sometimes lies.
                        let len = if n % 7 == 0 {
                            n % 4096
                        } else {
                            bytes.len() as u64
                        };
                        varint(&mut out, len);
                        out.extend_from_slice(&bytes);
                    }
                    5 => out.extend_from_slice(&(n as u32).to_le_bytes()),
                    _ => out.extend_from_slice(&bytes),
                }
            }
            if let Some((at, x)) = flip {
                if !out.is_empty() {
                    let i = at % out.len();
                    out[i] ^= x;
                }
            }
            if let Some(c) = cut {
                if !out.is_empty() {
                    out.truncate(c % out.len());
                }
            }
            out
        })
}

pub fn varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

pub fn payload() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        1 => prop::collection::vec(any::<u8>(), 0..256),
        3 => protobuf_like(),
    ]
}
