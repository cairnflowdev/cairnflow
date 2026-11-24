// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Layout of one object of the shared WAL log.
//!
//! ```text
//! prologue  magic "CFNW" · version u16 · kind u8 (1 = data, 2 = seal) · header_len u32
//! header    data: acked_upto i64 · n u32 · n × entry · crc32 of the header
//!           seal: floor u64 · sealer (len u16 + utf8) · crc32
//! entry     range u32 · epoch u64 · wal_file_id u64 · first_seq u64 · last_seq u64
//!           · offset u32 · len u32 · crc32 of the frame
//! body      the frames, grouped contiguously by range, in entry order
//! frame     n u32 · n × row
//! row       seq u64 · create_ts (tag u8 + i64) · expire_ts (tag u8 + i64)
//!           · key (len u32 + byte) · value (tag u8: 0 value, 1 merge, 2 tombstone; len u32 + byte)
//! ```
//!
//! All numbers are little-endian. The header has an `(offset, len)` index of each frame so
//! that a reader needing only one range can read it with a byte-range GET; the first
//! version reads the whole object.
//!
//! **The log index** (`<log>/index`) is an entirely different object, with its own magic:
//!
//! ```text
//! magic "CFNI" · version u16 · floor u64 · upto u64 · n u32
//! · n × (seq u64 · k u16 · k × (range u32 · wal_file_id u64)) · crc32 of every byte before
//! ```
//!
//! `n` contiguous entries from `floor` to `upto`: object `seq` carries frames of those
//! ranges with those wal ids. Read leniently: absent, damaged or unknown ⇒ as if missing.

use {
    crate::error::{Result, StoreError},
    bytes::{BufMut, Bytes, BytesMut},
    slatedb::{RowEntry, ValueDeletable},
    std::collections::BTreeMap,
};

const MAGIC: &[u8; 4] = b"CFNW";
const INDEX_MAGIC: &[u8; 4] = b"CFNI";
const INDEX_VERSION: u16 = 1;
const VERSION: u16 = 1;
const KIND_DATA: u8 = 1;
const KIND_SEAL: u8 = 2;
const PROLOGUE: usize = 4 + 2 + 1 + 4;

/// A range's part of one object = one of the range's "WAL files" in the sense of SlateDB
/// RFC 0030.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Frame {
    pub range: u32,
    /// `writer_epoch` of the Db that wrote the frame — for diagnosis and to check ordering.
    pub epoch: u64,
    pub wal_file_id: u64,
    pub rows: Vec<RowEntry>,
}

/// A decoded data object.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DataObject {
    /// Highest seq the writer had acked when it cut the object; −1 = none yet.
    pub acked_upto: i64,
    pub frames: Vec<Frame>,
}

/// One object of the log: data or a seal.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Decoded {
    Data(DataObject),
    /// Seal placed by `sealer`, after reading the GC floor `floor`.
    Seal {
        sealer: String,
        floor: u64,
    },
}

pub(crate) fn encode_data(acked_upto: i64, frames: &[Frame]) -> Bytes {
    let bodies: Vec<Bytes> = frames.iter().map(|f| encode_rows(&f.rows)).collect();
    let mut header = BytesMut::new();
    header.put_i64_le(acked_upto);
    header.put_u32_le(frames.len() as u32);
    let mut offset = 0u32;
    for (f, body) in frames.iter().zip(&bodies) {
        let first_seq = f.rows.iter().map(|r| r.seq).min().unwrap_or(0);
        let last_seq = f.rows.iter().map(|r| r.seq).max().unwrap_or(0);
        header.put_u32_le(f.range);
        header.put_u64_le(f.epoch);
        header.put_u64_le(f.wal_file_id);
        header.put_u64_le(first_seq);
        header.put_u64_le(last_seq);
        header.put_u32_le(offset);
        header.put_u32_le(body.len() as u32);
        header.put_u32_le(crc32fast::hash(body));
        offset += body.len() as u32;
    }
    let header_crc = crc32fast::hash(&header);
    header.put_u32_le(header_crc);
    let mut out = BytesMut::with_capacity(PROLOGUE + header.len() + offset as usize);
    put_prologue(&mut out, KIND_DATA, header.len());
    out.put_slice(&header);
    for body in bodies {
        out.put_slice(&body);
    }
    out.freeze()
}

pub(crate) fn encode_seal(sealer: &str, floor: u64) -> Bytes {
    let mut header = BytesMut::new();
    header.put_u64_le(floor);
    header.put_u16_le(sealer.len() as u16);
    header.put_slice(sealer.as_bytes());
    let crc = crc32fast::hash(&header);
    header.put_u32_le(crc);
    let mut out = BytesMut::with_capacity(PROLOGUE + header.len());
    put_prologue(&mut out, KIND_SEAL, header.len());
    out.put_slice(&header);
    out.freeze()
}

/// The index of a log: the (range, wal id) frames of each data object, from `floor` to
/// `upto`, contiguous.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct LogIndex {
    pub entries: BTreeMap<u64, Vec<(u32, u64)>>,
}

/// Encodes `entries` (must be contiguous). `None` when empty — nothing to write.
pub(crate) fn encode_index(entries: &BTreeMap<u64, Vec<(u32, u64)>>) -> Option<Bytes> {
    let (&floor, _) = entries.iter().next()?;
    let (&upto, _) = entries.iter().next_back()?;
    debug_assert_eq!(upto - floor + 1, entries.len() as u64, "index has a gap");
    let mut out = BytesMut::new();
    out.put_slice(INDEX_MAGIC);
    out.put_u16_le(INDEX_VERSION);
    out.put_u64_le(floor);
    out.put_u64_le(upto);
    out.put_u32_le(entries.len() as u32);
    for (seq, frames) in entries {
        out.put_u64_le(*seq);
        out.put_u16_le(frames.len() as u16);
        for (range, id) in frames {
            out.put_u32_le(*range);
            out.put_u64_le(*id);
        }
    }
    let crc = crc32fast::hash(&out);
    out.put_u32_le(crc);
    Some(out.freeze())
}

/// Decodes an index; anything off (magic, version, crc, entries not contiguous from `floor`
/// to `upto`) is an error — the caller treats it as no index.
pub(crate) fn decode_index(bytes: &[u8]) -> Result<LogIndex> {
    let body_len = bytes
        .len()
        .checked_sub(4)
        .ok_or_else(|| bad("index truncated"))?;
    let (body, crc) = bytes.split_at(body_len);
    if crc32fast::hash(body) != u32::from_le_bytes(crc.try_into().unwrap()) {
        return Err(bad("index crc mismatch"));
    }
    let mut c = Cursor { buf: body, at: 0 };
    if c.take(4)? != INDEX_MAGIC {
        return Err(bad("index missing magic"));
    }
    let version = c.u16()?;
    if version != INDEX_VERSION {
        return Err(bad(&format!("unknown index version {version}")));
    }
    let floor = c.u64()?;
    let upto = c.u64()?;
    let n = c.u32()? as u64;
    if upto < floor || upto - floor + 1 != n {
        return Err(bad("index: entry count does not match the span"));
    }
    let mut entries = BTreeMap::new();
    for want in floor..=upto {
        let seq = c.u64()?;
        if seq != want {
            return Err(bad("index not contiguous"));
        }
        let k = c.u16()? as usize;
        let mut frames = Vec::with_capacity(k);
        for _ in 0..k {
            frames.push((c.u32()?, c.u64()?));
        }
        entries.insert(seq, frames);
    }
    if c.at != body.len() {
        return Err(bad("index has trailing bytes"));
    }
    Ok(LogIndex { entries })
}

fn put_prologue(out: &mut BytesMut, kind: u8, header_len: usize) {
    out.put_slice(MAGIC);
    out.put_u16_le(VERSION);
    out.put_u8(kind);
    out.put_u32_le(header_len as u32);
}

fn encode_rows(rows: &[RowEntry]) -> Bytes {
    let mut out = BytesMut::new();
    out.put_u32_le(rows.len() as u32);
    for r in rows {
        out.put_u64_le(r.seq);
        put_opt_i64(&mut out, r.create_ts);
        put_opt_i64(&mut out, r.expire_ts);
        out.put_u32_le(r.key.len() as u32);
        out.put_slice(&r.key);
        match &r.value {
            ValueDeletable::Value(v) => {
                out.put_u8(0);
                out.put_u32_le(v.len() as u32);
                out.put_slice(v);
            }
            ValueDeletable::Merge(v) => {
                out.put_u8(1);
                out.put_u32_le(v.len() as u32);
                out.put_slice(v);
            }
            ValueDeletable::Tombstone => {
                out.put_u8(2);
                out.put_u32_le(0);
            }
        }
    }
    out.freeze()
}

fn put_opt_i64(out: &mut BytesMut, v: Option<i64>) {
    match v {
        Some(x) => {
            out.put_u8(1);
            out.put_i64_le(x);
        }
        None => {
            out.put_u8(0);
            out.put_i64_le(0);
        }
    }
}

/// Sequential reader; every read past the end is a data error.
struct Cursor<'a> {
    buf: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.at.checked_add(n).filter(|&e| e <= self.buf.len());
        let end = end.ok_or_else(|| bad("shared WAL object truncated"))?;
        let s = &self.buf[self.at..end];
        self.at = end;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn opt_i64(&mut self) -> Result<Option<i64>> {
        let tag = self.u8()?;
        let v = self.i64()?;
        match tag {
            0 => Ok(None),
            1 => Ok(Some(v)),
            t => Err(bad(&format!("unknown timestamp tag {t}"))),
        }
    }
}

fn bad(what: &str) -> StoreError {
    StoreError::Corrupt(format!("shared WAL: {what}"))
}

pub(crate) fn decode(bytes: &Bytes) -> Result<Decoded> {
    let mut c = Cursor { buf: bytes, at: 0 };
    if c.take(4)? != MAGIC {
        return Err(bad("missing magic"));
    }
    let version = c.u16()?;
    if version != VERSION {
        return Err(bad(&format!("unknown version {version}")));
    }
    let kind = c.u8()?;
    let header_len = c.u32()? as usize;
    let header_start = c.at;
    let header = c.take(header_len)?;
    let (covered, crc) =
        header.split_at(header_len.checked_sub(4).ok_or_else(|| bad("bad header"))?);
    if crc32fast::hash(covered) != u32::from_le_bytes(crc.try_into().unwrap()) {
        return Err(bad("header crc mismatch"));
    }
    let mut h = Cursor {
        buf: covered,
        at: 0,
    };
    match kind {
        KIND_SEAL => {
            let floor = h.u64()?;
            let n = h.u16()? as usize;
            let sealer = String::from_utf8(h.take(n)?.to_vec())
                .map_err(|_| bad("sealer is not valid UTF-8"))?;
            Ok(Decoded::Seal { sealer, floor })
        }
        KIND_DATA => {
            let acked_upto = h.i64()?;
            let n = h.u32()? as usize;
            let body_start = header_start + header_len;
            let mut frames = Vec::with_capacity(n);
            for _ in 0..n {
                let range = h.u32()?;
                let epoch = h.u64()?;
                let wal_file_id = h.u64()?;
                let _first_seq = h.u64()?;
                let _last_seq = h.u64()?;
                let offset = h.u32()? as usize;
                let len = h.u32()? as usize;
                let crc = h.u32()?;
                let start = body_start
                    .checked_add(offset)
                    .filter(|s| s.checked_add(len).is_some_and(|e| e <= bytes.len()))
                    .ok_or_else(|| bad("frame outside the object"))?;
                let body = bytes.slice(start..start + len);
                if crc32fast::hash(&body) != crc {
                    return Err(bad("frame crc mismatch"));
                }
                frames.push(Frame {
                    range,
                    epoch,
                    wal_file_id,
                    rows: decode_rows(&body)?,
                });
            }
            Ok(Decoded::Data(DataObject { acked_upto, frames }))
        }
        k => Err(bad(&format!("unknown object kind {k}"))),
    }
}

fn decode_rows(body: &Bytes) -> Result<Vec<RowEntry>> {
    let mut c = Cursor { buf: body, at: 0 };
    let n = c.u32()? as usize;
    let mut rows = Vec::with_capacity(n);
    for _ in 0..n {
        let seq = c.u64()?;
        let create_ts = c.opt_i64()?;
        let expire_ts = c.opt_i64()?;
        let klen = c.u32()? as usize;
        let key_start = c.at;
        c.take(klen)?;
        let key = body.slice(key_start..key_start + klen);
        let tag = c.u8()?;
        let vlen = c.u32()? as usize;
        let v_start = c.at;
        c.take(vlen)?;
        let v = body.slice(v_start..v_start + vlen);
        let value = match tag {
            0 => ValueDeletable::Value(v),
            1 => ValueDeletable::Merge(v),
            2 => ValueDeletable::Tombstone,
            t => return Err(bad(&format!("unknown value tag {t}"))),
        };
        rows.push(RowEntry {
            key,
            value,
            seq,
            create_ts,
            expire_ts,
        });
    }
    if c.at != body.len() {
        return Err(bad("frame has trailing bytes"));
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(key: &str, seq: u64, value: ValueDeletable) -> RowEntry {
        RowEntry {
            key: Bytes::copy_from_slice(key.as_bytes()),
            value,
            seq,
            create_ts: Some(seq as i64 * 10),
            expire_ts: if seq.is_multiple_of(2) {
                Some(-5)
            } else {
                None
            },
        }
    }

    /// Index: encode/decode round-trips; anything off (truncated, crc, not contiguous) is
    /// refused — the reader treats it as no index.
    #[test]
    fn log_index_roundtrips_and_rejects_damage() {
        let entries: BTreeMap<u64, Vec<(u32, u64)>> =
            [(7, vec![(0, 3)]), (8, vec![]), (9, vec![(0, 4), (2, 11)])]
                .into_iter()
                .collect();
        let bytes = encode_index(&entries).expect("has entries");
        assert_eq!(decode_index(&bytes).unwrap().entries, entries);
        assert!(encode_index(&BTreeMap::new()).is_none());
        assert!(decode_index(&bytes[..bytes.len() - 1]).is_err());
        let mut flipped = bytes.to_vec();
        flipped[20] ^= 1;
        assert!(decode_index(&flipped).is_err());
        let gap: BTreeMap<u64, Vec<(u32, u64)>> = [(7, vec![]), (9, vec![])].into_iter().collect();
        // A build with debug_assert stops at encode; decoding a hand-built index with a hole
        // must be refused.
        let mut raw = BytesMut::new();
        raw.put_slice(INDEX_MAGIC);
        raw.put_u16_le(INDEX_VERSION);
        raw.put_u64_le(7);
        raw.put_u64_le(8);
        raw.put_u32_le(2);
        for seq in gap.keys() {
            raw.put_u64_le(*seq);
            raw.put_u16_le(0);
        }
        let crc = crc32fast::hash(&raw);
        raw.put_u32_le(crc);
        assert!(decode_index(&raw).is_err());
        // A data object is never read as an index, and vice versa.
        assert!(decode(&bytes).is_err());
    }

    #[test]
    fn data_object_round_trips() {
        let frames = vec![
            Frame {
                range: 3,
                epoch: 7,
                wal_file_id: 41,
                rows: vec![
                    row("a", 1, ValueDeletable::Value(Bytes::from_static(b"x"))),
                    row("b", 2, ValueDeletable::Tombstone),
                ],
            },
            Frame {
                range: 9,
                epoch: 2,
                wal_file_id: 5,
                rows: vec![row("c", 8, ValueDeletable::Merge(Bytes::from_static(b"m")))],
            },
            Frame {
                range: 10,
                epoch: 1,
                wal_file_id: 1,
                rows: vec![],
            },
        ];
        let bytes = encode_data(12, &frames);
        assert_eq!(
            decode(&bytes).unwrap(),
            Decoded::Data(DataObject {
                acked_upto: 12,
                frames
            })
        );
    }

    #[test]
    fn seal_round_trips() {
        let bytes = encode_seal("b/3", 17);
        assert_eq!(
            decode(&bytes).unwrap(),
            Decoded::Seal {
                sealer: "b/3".into(),
                floor: 17
            }
        );
    }

    /// One damaged byte anywhere is an error, never a different valid object — replaying a
    /// damaged frame would resurrect rows that were never written.
    #[test]
    fn any_flipped_byte_is_rejected() {
        let bytes = encode_data(
            -1,
            &[Frame {
                range: 1,
                epoch: 1,
                wal_file_id: 1,
                rows: vec![row(
                    "key",
                    4,
                    ValueDeletable::Value(Bytes::from_static(b"value")),
                )],
            }],
        );
        for i in 0..bytes.len() {
            let mut b = bytes.to_vec();
            b[i] ^= 0x40;
            assert!(decode(&Bytes::from(b)).is_err(), "byte {i}");
        }
        for len in 0..bytes.len() {
            assert!(decode(&bytes.slice(..len)).is_err(), "truncated at {len}");
        }
    }
}
