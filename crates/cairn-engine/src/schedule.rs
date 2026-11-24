// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Server-side schedule rows: the `Schedules` table (`0x1A`).
//!
//! The value in the store is `due ‖ row` ([`cairn_store::keys::encode_due`]);
//! this module handles the **row** part — read strictly, version byte first
//! (D27: a strictly read object/row that changes layout changes its version).
//! Schedule arithmetic lives in `cairn-schedule`; here there is only the
//! durable data and its deterministic derivations (spec hash, jitter, the run
//! header).

use {
    crate::engine::InvocationId,
    bytes::{Buf, BufMut, Bytes, BytesMut},
};

/// The row version we write and read. Strict read: another version ⇒ error.
const ROW_VERSION: u8 = 1;

/// How many recent fires the row keeps (`last`).
pub const LAST_KEEP: usize = 10;

/// Byte cap of a schedule's input.
pub const MAX_INPUT: usize = 64 * 1024;

/// Entry-0 header of a run fired by a schedule: `<name>@<instant ms>` — the SDK
/// reads it as `ctx.schedule`. `restart` copies headers, so the restarted run
/// still belongs to that instant.
pub const HDR_SCHEDULE: &str = "cairnflow-schedule";

/// The stored spec — the source form, re-parsed with `cairn-schedule` on every use.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SpecRow {
    Cron { expr: String, time_zone: String },
    Every(u64),
}

impl SpecRow {
    /// Parses with `cairn-schedule`. `min_period_ms` applies only to `every`.
    pub fn parse(
        &self,
        min_period_ms: u64,
    ) -> Result<cairn_schedule::Spec, cairn_schedule::SpecError> {
        match self {
            SpecRow::Cron { expr, time_zone } => cairn_schedule::Spec::cron(expr, time_zone),
            SpecRow::Every(ms) => cairn_schedule::Spec::every(*ms, min_period_ms),
        }
    }
}

/// Whether two runs of one schedule may overlap.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Overlap {
    /// The previous run has not finished ⇒ skip this instant, `skipped += 1` (CG-15). Default.
    Skip,
    /// Always fire. A virtual-object target queues by key anyway.
    Allow,
}

/// The outcome of one instant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LastKind {
    Fired,
    Skipped,
}

/// A recent fire (or skip).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LastFire {
    /// The nominal instant (without jitter), ms.
    pub at: u64,
    /// The run id (when skipped, the id that would have been created).
    pub id: InvocationId,
    pub kind: LastKind,
}

/// A schedule.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScheduleRow {
    pub name: String,
    /// `None` = `default`.
    pub tenant: Option<String>,
    pub scope: Option<String>,
    pub spec: SpecRow,
    pub service: String,
    pub handler: String,
    /// Object key of the target; empty = unkeyed.
    pub key: String,
    pub limit_key: Option<String>,
    pub input: Bytes,
    pub paused: bool,
    pub overlap: Overlap,
    pub jitter_ms: u64,
    /// The next nominal instant, ms (0 when paused and there never was one).
    pub next_at: u64,
    /// Number of fires written — the key of `Fire` outbox rows (`(schedule id, fire_seq)`).
    pub fire_seq: u32,
    /// Newest last, at most [`LAST_KEEP`].
    pub last: Vec<LastFire>,
    pub fired: u64,
    pub missed: u64,
    pub skipped: u64,
    /// Hash of spec + target + input + overlap + jitter: a `PUT` with the same hash is a no-op.
    pub hash: [u8; 16],
    pub created_ms: u64,
    pub updated_ms: u64,
}

/// Error reading a schedule row.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RowError {
    #[error("schedule row version {0}; this binary reads {ROW_VERSION}")]
    UnknownVersion(u8),
    #[error("truncated schedule row")]
    Truncated,
    #[error("corrupt schedule row: {0}")]
    Corrupt(&'static str),
}

impl ScheduleRow {
    /// A keyed target (object key or scope) ⇒ the run is on **another** range
    /// (by the target's key), through the `Fire` outbox. Unkeyed ⇒ the run is
    /// on the same range.
    pub fn keyed(&self) -> bool {
        !self.key.is_empty() || self.scope.is_some()
    }

    /// Records an instant in `last`, keeping the [`LAST_KEEP`] newest.
    pub fn push_last(&mut self, f: LastFire) {
        self.last.push(f);
        if self.last.len() > LAST_KEEP {
            let drop = self.last.len() - LAST_KEEP;
            self.last.drain(..drop);
        }
    }

    /// The most recent **fire** (skipped instants ignored).
    pub fn last_fired(&self) -> Option<&LastFire> {
        self.last.iter().rev().find(|l| l.kind == LastKind::Fired)
    }

    /// Hash of the schedule's "definition" — what `PUT` compares to see whether anything changed.
    pub fn definition_hash(&self) -> [u8; 16] {
        use sha2::{Digest, Sha256};
        let mut b = BytesMut::new();
        put_spec(&mut b, &self.spec);
        put_str(&mut b, &self.service);
        put_str(&mut b, &self.handler);
        put_str(&mut b, &self.key);
        put_opt(&mut b, self.limit_key.as_deref());
        put_bytes(&mut b, &self.input);
        b.put_u8(match self.overlap {
            Overlap::Skip => 0,
            Overlap::Allow => 1,
        });
        b.put_u64(self.jitter_ms);
        let mut h = Sha256::new();
        h.update(b"cairnflow-sched-def/1");
        h.update(&b);
        let d = h.finalize();
        let mut out = [0u8; 16];
        out.copy_from_slice(&d[..16]);
        out
    }

    /// The real fire time of instant `at`: `at + H(schedule id, at) mod jitter_ms`,
    /// clamped below the next instant `next`. Same inputs ⇒ every leader gets
    /// the same result.
    pub fn fire_due(&self, id: &InvocationId, at: u64, next: Option<u64>) -> u64 {
        use sha2::{Digest, Sha256};
        if self.jitter_ms == 0 {
            return at;
        }
        let mut h = Sha256::new();
        h.update(b"cairnflow-sched-jitter/1");
        h.update(id);
        h.update(at.to_be_bytes());
        let d = h.finalize();
        let hash = u64::from_be_bytes(d[..8].try_into().expect("8 byte"));
        let n = next.map(|n| n as i64);
        cairn_schedule::jittered(at as i64, n, self.jitter_ms, hash).max(0) as u64
    }

    pub fn encode(&self) -> Bytes {
        let mut b = BytesMut::with_capacity(128 + self.input.len());
        b.put_u8(ROW_VERSION);
        put_str(&mut b, &self.name);
        put_opt(&mut b, self.tenant.as_deref());
        put_opt(&mut b, self.scope.as_deref());
        put_spec(&mut b, &self.spec);
        put_str(&mut b, &self.service);
        put_str(&mut b, &self.handler);
        put_str(&mut b, &self.key);
        put_opt(&mut b, self.limit_key.as_deref());
        put_bytes(&mut b, &self.input);
        b.put_u8(u8::from(self.paused));
        b.put_u8(match self.overlap {
            Overlap::Skip => 0,
            Overlap::Allow => 1,
        });
        b.put_u64(self.jitter_ms);
        b.put_u64(self.next_at);
        b.put_u32(self.fire_seq);
        b.put_u8(self.last.len() as u8);
        for l in &self.last {
            b.put_u64(l.at);
            b.put_slice(&l.id);
            b.put_u8(match l.kind {
                LastKind::Fired => 0,
                LastKind::Skipped => 1,
            });
        }
        b.put_u64(self.fired);
        b.put_u64(self.missed);
        b.put_u64(self.skipped);
        b.put_slice(&self.hash);
        b.put_u64(self.created_ms);
        b.put_u64(self.updated_ms);
        b.freeze()
    }

    pub fn decode(raw: &[u8]) -> Result<Self, RowError> {
        let mut b = raw;
        let version = u8_of(&mut b)?;
        if version != ROW_VERSION {
            return Err(RowError::UnknownVersion(version));
        }
        let name = str_of(&mut b)?;
        let tenant = opt_of(&mut b)?;
        let scope = opt_of(&mut b)?;
        let spec = match u8_of(&mut b)? {
            0 => SpecRow::Cron {
                expr: str_of(&mut b)?,
                time_zone: str_of(&mut b)?,
            },
            1 => SpecRow::Every(u64_of(&mut b)?),
            _ => return Err(RowError::Corrupt("spec")),
        };
        let service = str_of(&mut b)?;
        let handler = str_of(&mut b)?;
        let key = str_of(&mut b)?;
        let limit_key = opt_of(&mut b)?;
        let input = Bytes::copy_from_slice(&bytes_of(&mut b)?);
        let paused = match u8_of(&mut b)? {
            0 => false,
            1 => true,
            _ => return Err(RowError::Corrupt("paused")),
        };
        let overlap = match u8_of(&mut b)? {
            0 => Overlap::Skip,
            1 => Overlap::Allow,
            _ => return Err(RowError::Corrupt("overlap")),
        };
        let jitter_ms = u64_of(&mut b)?;
        let next_at = u64_of(&mut b)?;
        let fire_seq = u32_of(&mut b)?;
        let n = u8_of(&mut b)? as usize;
        if n > LAST_KEEP {
            return Err(RowError::Corrupt("last"));
        }
        let mut last = Vec::with_capacity(n);
        for _ in 0..n {
            let at = u64_of(&mut b)?;
            if b.remaining() < 24 {
                return Err(RowError::Truncated);
            }
            let mut id = [0u8; 24];
            b.copy_to_slice(&mut id);
            let kind = match u8_of(&mut b)? {
                0 => LastKind::Fired,
                1 => LastKind::Skipped,
                _ => return Err(RowError::Corrupt("last.kind")),
            };
            last.push(LastFire { at, id, kind });
        }
        let fired = u64_of(&mut b)?;
        let missed = u64_of(&mut b)?;
        let skipped = u64_of(&mut b)?;
        if b.remaining() < 16 {
            return Err(RowError::Truncated);
        }
        let mut hash = [0u8; 16];
        b.copy_to_slice(&mut hash);
        let created_ms = u64_of(&mut b)?;
        let updated_ms = u64_of(&mut b)?;
        if b.has_remaining() {
            return Err(RowError::Corrupt("trailing bytes"));
        }
        Ok(Self {
            name,
            tenant,
            scope,
            spec,
            service,
            handler,
            key,
            limit_key,
            input,
            paused,
            overlap,
            jitter_ms,
            next_at,
            fire_seq,
            last,
            fired,
            missed,
            skipped,
            hash,
            created_ms,
            updated_ms,
        })
    }
}

/// The [`HDR_SCHEDULE`] header value for instant `at`.
pub fn header_value(name: &str, at: u64) -> String {
    format!("{name}@{at}")
}

/// The schedule name in an [`HDR_SCHEDULE`] header value (`<name>@<instant>`)
/// — the `$schedule` attribute of the query index. Schedule names contain no
/// `@`, so split at the last `@`.
pub fn stamp_name(header: &str) -> &str {
    header.rsplit_once('@').map_or(header, |(name, _)| name)
}

/// The idempotency key of a cross-range fire: the target range deduplicates on
/// it, so redelivering the same `Fire` row does not create a second run
/// (CG-10).
pub fn fire_idempotency_key(name: &str, at: u64) -> String {
    format!("schedule/{name}/{at}")
}

/// The idempotency key of a **manual** cross-range fire: by request id —
/// resending the same request does not create a second run.
pub fn manual_idempotency_key(name: &str, request: &str) -> String {
    format!("schedule/{name}/manual/{request}")
}

fn put_spec(b: &mut BytesMut, s: &SpecRow) {
    match s {
        SpecRow::Cron { expr, time_zone } => {
            b.put_u8(0);
            put_str(b, expr);
            put_str(b, time_zone);
        }
        SpecRow::Every(ms) => {
            b.put_u8(1);
            b.put_u64(*ms);
        }
    }
}

fn put_str(b: &mut BytesMut, s: &str) {
    put_bytes(b, s.as_bytes());
}

fn put_bytes(b: &mut BytesMut, v: &[u8]) {
    b.put_u32(v.len() as u32);
    b.put_slice(v);
}

fn put_opt(b: &mut BytesMut, s: Option<&str>) {
    match s {
        Some(v) => {
            b.put_u8(1);
            put_str(b, v);
        }
        None => b.put_u8(0),
    }
}

fn u8_of(b: &mut &[u8]) -> Result<u8, RowError> {
    if b.remaining() < 1 {
        return Err(RowError::Truncated);
    }
    Ok(b.get_u8())
}

fn u32_of(b: &mut &[u8]) -> Result<u32, RowError> {
    if b.remaining() < 4 {
        return Err(RowError::Truncated);
    }
    Ok(b.get_u32())
}

fn u64_of(b: &mut &[u8]) -> Result<u64, RowError> {
    if b.remaining() < 8 {
        return Err(RowError::Truncated);
    }
    Ok(b.get_u64())
}

fn bytes_of(b: &mut &[u8]) -> Result<Vec<u8>, RowError> {
    let n = u32_of(b)? as usize;
    if b.remaining() < n {
        return Err(RowError::Truncated);
    }
    let v = b[..n].to_vec();
    b.advance(n);
    Ok(v)
}

fn str_of(b: &mut &[u8]) -> Result<String, RowError> {
    String::from_utf8(bytes_of(b)?).map_err(|_| RowError::Corrupt("utf-8"))
}

fn opt_of(b: &mut &[u8]) -> Result<Option<String>, RowError> {
    match u8_of(b)? {
        0 => Ok(None),
        1 => Ok(Some(str_of(b)?)),
        _ => Err(RowError::Corrupt("presence flag")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row() -> ScheduleRow {
        let mut r = ScheduleRow {
            name: "nightly".into(),
            tenant: None,
            scope: Some("acme".into()),
            spec: SpecRow::Cron {
                expr: "0 3 * * *".into(),
                time_zone: "Europe/Berlin".into(),
            },
            service: "Reports".into(),
            handler: "build".into(),
            key: String::new(),
            limit_key: None,
            input: Bytes::from_static(b"{\"x\":1}"),
            paused: false,
            overlap: Overlap::Skip,
            jitter_ms: 500,
            next_at: 1_700_000_000_000,
            fire_seq: 3,
            last: vec![LastFire {
                at: 1,
                id: [7; 24],
                kind: LastKind::Skipped,
            }],
            fired: 2,
            missed: 1,
            skipped: 1,
            hash: [0; 16],
            created_ms: 5,
            updated_ms: 6,
        };
        r.hash = r.definition_hash();
        r
    }

    #[test]
    fn encode_decode_and_strict_read() {
        let r = row();
        let raw = r.encode();
        assert_eq!(ScheduleRow::decode(&raw).unwrap(), r);
        let mut bad = raw.to_vec();
        bad[0] = 9;
        assert_eq!(ScheduleRow::decode(&bad), Err(RowError::UnknownVersion(9)));
        assert_eq!(
            ScheduleRow::decode(&raw[..raw.len() - 1]),
            Err(RowError::Truncated)
        );
        let mut long = raw.to_vec();
        long.push(0);
        assert!(ScheduleRow::decode(&long).is_err());
    }

    #[test]
    fn hashes_definition_and_last() {
        let a = row();
        let mut b = row();
        b.fire_seq = 99;
        b.paused = true;
        assert_eq!(
            a.definition_hash(),
            b.definition_hash(),
            "state is not part of the hash"
        );
        b.input = Bytes::from_static(b"{}");
        assert_ne!(a.definition_hash(), b.definition_hash());
        let mut r = row();
        for i in 0..15 {
            r.push_last(LastFire {
                at: i,
                id: [0; 24],
                kind: LastKind::Fired,
            });
        }
        assert_eq!(r.last.len(), LAST_KEEP);
        assert_eq!(r.last.first().unwrap().at, 5);
        assert_eq!(r.last_fired().unwrap().at, 14);
    }

    #[test]
    fn jitter_deterministic_and_clamped() {
        let r = row();
        let id = [1u8; 24];
        let d = r.fire_due(&id, 1_000, Some(10_000));
        assert!((1_000..1_500).contains(&d));
        assert_eq!(d, r.fire_due(&id, 1_000, Some(10_000)));
        assert_eq!(r.fire_due(&id, 1_000, Some(1_001)), 1_000);
        assert!(row().keyed());
    }
}
