// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The retry-row encoding — in **one** place, replacing
//! `encode_retry`/`encode_watch`/`encode_retry_fired`/`is_watch`/`retry_fires`
//! of `cairn-engine/src/engine/step.rs` and the 4/8/9/13-byte branches scattered
//! through `due.rs` (`due_retries_fired`, `pending_retry`, `undefer_retry`).
//!
//! The value of the entity row `retries|id` is `due (u64 BE) ‖ payload`
//! (`cairn_store::keys::encode_due`). The payload, by length:
//!
//! | Bytes | Layout | Who writes it today |
//! |---|---|---|
//! | 4 | `attempt` | old binaries (before `journal_len`) — `journal_len` reads as `u32::MAX` |
//! | 8 | `attempt ‖ journal_len` | `schedule_retry`, `begin_turn`, `end_turn` |
//! | 9 | `attempt ‖ journal_len ‖ flag` | `begin_turn` (watch row: `0 ‖ u32::MAX ‖ 1`) |
//! | 13 | `attempt ‖ journal_len ‖ flag ‖ fires` | `reschedule_retry`, `defer_retries` (flag 0, FL-21) |
//!
//! Every integer is big-endian; flag `1` = watch row (FL-17), `0` = an ordinary
//! retry. `DATA_FORMAT` does not change (D27): no byte is added, only gathered.
//!
//! **Unknown lengths are read strictly**: the four lengths above decode; longer
//! than 13 reads the first 13 bytes and drops the tail (the lenient-read
//! precedent of `journal_len` and then `fires`: new bytes are only ever
//! **appended**); any other length, or a flag outside `{0, 1}`, is a
//! [`CodecError`]. The engine today reads even those lengths leniently (missing
//! bytes ⇒ default value) — a deliberate difference.

/// The flag in the payload's 9th byte: watch row (`engine/step.rs::WATCH_FLAG`).
pub const WATCH_FLAG: u8 = 1;

/// Row kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RowKind {
    /// A retry with a backoff step — `pending_retry` counts it.
    Retry,
    /// The watch row of a turn that has entered `Running` (FL-17) —
    /// `pending_retry` does not count it: recovery on becoming leader and the
    /// rescue net do not wait for it.
    Watch,
}

/// A decoded retry row. An invocation has at most one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RetryRow {
    pub due: u64,
    /// Number of **counted** attempts (turns that entered `Running`) since the
    /// journal last grew (FL-19).
    pub attempt: u32,
    /// Journal length when scheduled; `u32::MAX` = unknown (4-byte rows, the
    /// watch rows of today's binary) — the progress comparison never recounts.
    pub journal_len: u32,
    /// Number of fires that did not become an attempt (FL-21). The backoff
    /// step is `attempt + fires`.
    pub fires: u32,
    pub kind: RowKind,
}

impl RetryRow {
    pub const fn retry(due: u64, attempt: u32, journal_len: u32, fires: u32) -> Self {
        Self {
            due,
            attempt,
            journal_len,
            fires,
            kind: RowKind::Retry,
        }
    }

    pub const fn watch(due: u64, attempt: u32, journal_len: u32) -> Self {
        Self {
            due,
            attempt,
            journal_len,
            fires: 0,
            kind: RowKind::Watch,
        }
    }

    /// The watch row exactly as `begin_turn` writes it today (`encode_watch`):
    /// `attempt` 0, `journal_len` unknown.
    pub const fn legacy_watch(due: u64) -> Self {
        Self::watch(due, 0, u32::MAX)
    }

    pub fn is_watch(&self) -> bool {
        self.kind == RowKind::Watch
    }

    /// The backoff step of the next fire (FL-21).
    pub fn step(&self) -> u32 {
        self.attempt.saturating_add(self.fires)
    }

    /// The canonical width: watch without `fires` ⇒ 9; ordinary without
    /// `fires` ⇒ 8; otherwise ⇒ 13 (the flag in byte 9 still tells the kind).
    pub fn canonical_width(&self) -> Width {
        match (self.kind, self.fires) {
            (RowKind::Watch, 0) => Width::W9,
            (RowKind::Retry, 0) => Width::W8,
            _ => Width::W13,
        }
    }

    /// The payload at the canonical width.
    pub fn encode(&self) -> Payload {
        self.encode_as(self.canonical_width())
            .expect("the canonical width always fits the row")
    }

    /// The payload at width `w` — allows a byte-by-byte comparison with the
    /// existing write paths (`defer_retries` writes 13 bytes even when `fires`
    /// = 0). `None` when `w` cannot carry the row: 4/8 bytes have no flag or
    /// `fires`, 9 bytes have no `fires`.
    pub fn encode_as(&self, w: Width) -> Option<Payload> {
        let watch = self.is_watch();
        let fits = match w {
            Width::W4 => !watch && self.fires == 0 && self.journal_len == u32::MAX,
            Width::W8 => !watch && self.fires == 0,
            Width::W9 => self.fires == 0,
            Width::W13 => true,
        };
        if !fits {
            return None;
        }
        let mut b = [0u8; 13];
        b[..4].copy_from_slice(&self.attempt.to_be_bytes());
        b[4..8].copy_from_slice(&self.journal_len.to_be_bytes());
        if watch {
            b[8] = WATCH_FLAG;
        }
        b[9..].copy_from_slice(&self.fires.to_be_bytes());
        Some(Payload {
            bytes: b,
            len: w.bytes(),
        })
    }

    /// The full value of the entity row: `due ‖ payload`, canonical.
    pub fn encode_value(&self) -> Value {
        let p = self.encode();
        let mut b = [0u8; 21];
        b[..8].copy_from_slice(&self.due.to_be_bytes());
        b[8..8 + p.len as usize].copy_from_slice(p.as_slice());
        Value {
            bytes: b,
            len: 8 + p.len,
        }
    }

    /// Decodes the payload of a row due at `due`.
    pub fn decode(due: u64, payload: &[u8]) -> Result<Self, CodecError> {
        let u32_at =
            |i: usize| u32::from_be_bytes(payload[i..i + 4].try_into().expect("enough bytes"));
        let n = payload.len();
        match n {
            4 => Ok(Self::retry(due, u32_at(0), u32::MAX, 0)),
            8 => Ok(Self::retry(due, u32_at(0), u32_at(4), 0)),
            9 | 13.. => {
                let kind = match payload[8] {
                    0 => RowKind::Retry,
                    WATCH_FLAG => RowKind::Watch,
                    flag => return Err(CodecError::Flag(flag)),
                };
                Ok(Self {
                    due,
                    attempt: u32_at(0),
                    journal_len: u32_at(4),
                    fires: if n >= 13 { u32_at(9) } else { 0 },
                    kind,
                })
            }
            _ => Err(CodecError::Length(n)),
        }
    }

    /// Decodes the full value `due ‖ payload`.
    pub fn decode_value(raw: &[u8]) -> Result<Self, CodecError> {
        if raw.len() < 8 {
            return Err(CodecError::Length(raw.len()));
        }
        let due = u64::from_be_bytes(raw[..8].try_into().expect("8 byte"));
        Self::decode(due, &raw[8..])
    }
}

/// Payload width.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Width {
    W4,
    W8,
    W9,
    W13,
}

impl Width {
    pub const fn bytes(self) -> u8 {
        match self {
            Width::W4 => 4,
            Width::W8 => 8,
            Width::W9 => 9,
            Width::W13 => 13,
        }
    }
}

/// An encoded payload (no allocation: the core runs on every turn).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Payload {
    bytes: [u8; 13],
    len: u8,
}

impl Payload {
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }
}

/// `due ‖ payload`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Value {
    bytes: [u8; 21],
    len: u8,
}

impl Value {
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CodecError {
    /// A payload length outside 4/8/9/≥13 (or a value shorter than `due`).
    Length(usize),
    /// A flag in byte 9 outside `{0, 1}`.
    Flag(u8),
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CodecError::Length(n) => write!(f, "retry row is {n} bytes — not 4/8/9/13"),
            CodecError::Flag(b) => write!(f, "unknown retry row flag: {b}"),
        }
    }
}

impl std::error::Error for CodecError {}
