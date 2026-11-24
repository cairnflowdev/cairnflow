// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The retry row: the engine's four widths, byte for byte. The sample bytes
//! are copied by hand from `engine/step.rs::{encode_retry, encode_watch,
//! encode_retry_fired}`; the comparison with the real engine (written through
//! `Engine`, read raw from the store, and the other way round) is in
//! `crates/cairn-engine/tests/retry_codec.rs`.

use crate::codec::{CodecError, RetryRow, RowKind, Width, WATCH_FLAG};

#[test]
fn four_widths_byte_for_byte() {
    #[rustfmt::skip]
    let rows: &[(&str, RetryRow, Width, &[u8])] = &[
        // `encode_retry(3, 10)` — schedule_retry, begin_turn, end_turn
        ("8 B",  RetryRow::retry(0, 3, 10, 0), Width::W8,  &[0,0,0,3, 0,0,0,10]),
        // `encode_watch()` — begin_turn's watch row (FL-17)
        ("9 B",  RetryRow::legacy_watch(0),    Width::W9,  &[0,0,0,0, 255,255,255,255, WATCH_FLAG]),
        // `encode_retry_fired(3, 10, 2)` — reschedule_retry, defer_retries (FL-21)
        ("13 B", RetryRow::retry(0, 3, 10, 2), Width::W13, &[0,0,0,3, 0,0,0,10, 0, 0,0,0,2]),
        // `defer_retries` writes 13 bytes even when fires = 0
        ("13 B", RetryRow::retry(0, 3, 10, 0), Width::W13, &[0,0,0,3, 0,0,0,10, 0, 0,0,0,0]),
        // an old binary's 4-byte row: journal_len unknown
        ("4 B",  RetryRow::retry(0, 7, u32::MAX, 0), Width::W4, &[0,0,0,7]),
        // a watch row carrying the attempt (core, the reverse of B) — the same 9-byte layout
        ("9 B",  RetryRow::watch(0, 2, 5),     Width::W9,  &[0,0,0,2, 0,0,0,5, WATCH_FLAG]),
    ];
    for (name, row, w, bytes) in rows {
        let enc = row
            .encode_as(*w)
            .unwrap_or_else(|| panic!("{name}: {w:?} can carry {row:?}"));
        assert_eq!(enc.as_slice(), *bytes, "{name}: encode");
        assert_eq!(RetryRow::decode(0, bytes), Ok(*row), "{name}: decode");
    }
    // The canonical width is the width the engine writes for the same row.
    assert_eq!(RetryRow::retry(0, 3, 10, 0).canonical_width(), Width::W8);
    assert_eq!(RetryRow::legacy_watch(0).canonical_width(), Width::W9);
    assert_eq!(RetryRow::retry(0, 3, 10, 1).canonical_width(), Width::W13);
}

/// Reads like the engine: `attempt` the first 4 bytes; `journal_len` bytes
/// 4..8 (absent = `u32::MAX`); watch ⇔ byte 9 = 1; `fires` bytes 9..13
/// (absent = 0).
#[test]
fn decodes_like_the_engine() {
    let engine_read = |raw: &[u8]| {
        let u = |r: std::ops::Range<usize>| {
            raw.get(r)
                .map(|b| u32::from_be_bytes(b.try_into().unwrap()))
        };
        (
            u(0..4).unwrap_or(0),
            u(4..8).unwrap_or(u32::MAX),
            raw.get(8) == Some(&WATCH_FLAG),
            u(9..13).unwrap_or(0),
        )
    };
    for row in [
        RetryRow::retry(5, 0, 0, 0),
        RetryRow::retry(5, 9, 33, 0),
        RetryRow::retry(5, 9, 33, 4),
        RetryRow::legacy_watch(5),
        RetryRow::watch(5, 3, 8),
        RetryRow {
            fires: 2,
            ..RetryRow::watch(5, 3, 8)
        },
    ] {
        let p = row.encode();
        assert_eq!(
            engine_read(p.as_slice()),
            (row.attempt, row.journal_len, row.is_watch(), row.fires),
            "{row:?}"
        );
        assert_eq!(RetryRow::decode(5, p.as_slice()), Ok(row));
        assert_eq!(
            RetryRow::decode_value(row.encode_value().as_slice()),
            Ok(row)
        );
    }
}

#[test]
fn strict_decode_rejects_odd_lengths() {
    for n in [0usize, 1, 3, 5, 7, 10, 12] {
        assert_eq!(
            RetryRow::decode(0, &vec![0; n]),
            Err(CodecError::Length(n)),
            "{n}"
        );
    }
    assert_eq!(
        RetryRow::decode(0, &[0, 0, 0, 1, 0, 0, 0, 2, 7]),
        Err(CodecError::Flag(7))
    );
    // Longer than 13: read the first 13 bytes, drop the tail (new bytes are only
    // ever appended).
    let mut long = RetryRow::retry(0, 1, 2, 3)
        .encode_as(Width::W13)
        .unwrap()
        .as_slice()
        .to_vec();
    long.extend_from_slice(&[9, 9]);
    assert_eq!(RetryRow::decode(0, &long), Ok(RetryRow::retry(0, 1, 2, 3)));
    assert_eq!(RetryRow::decode_value(&[0; 7]), Err(CodecError::Length(7)));
    // A width that cannot carry the row.
    assert!(RetryRow::legacy_watch(0).encode_as(Width::W8).is_none());
    assert!(RetryRow::retry(0, 1, 2, 3).encode_as(Width::W9).is_none());
    assert!(RetryRow::retry(0, 1, 2, 0).encode_as(Width::W4).is_none());
    assert_eq!(RetryRow::retry(0, 1, 2, 3).kind, RowKind::Retry);
}
