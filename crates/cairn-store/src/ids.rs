// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Leaf identifiers: range id ([`PartitionId`]) and invocation id ([`InvocationId`]).
//!
//! Split out of `writer` and `keyspace` so that `partition_table`, `cluster`, `owner`… can use
//! them without a dependency cycle. The old paths (`writer::PartitionId`,
//! `keyspace::InvocationId`…) are kept through `pub use`.

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PartitionId(pub u32);

/// Length of an invocation id: 8 bytes of **partition key** (big-endian) + 16 bytes of
/// identifier.
pub const ID_LEN: usize = 24;

/// Invocation id, fixed length (protocol v1 `StartMessage.id` is `bytes`). Fixed so that journal
/// keys sort by `(invocation, index)` without a length prefix; the partition key comes first so
/// that the byte order of ids matches the numeric order of keys — a key range is a contiguous
/// range of ids.
pub type InvocationId = [u8; ID_LEN];

/// Id in hex — the exact form the HTTP API, the journal and the CLI use.
pub fn id_hex(id: &InvocationId) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

/// Inverse of [`id_hex`]. `None` unless it is exactly `2 × ID_LEN` hex characters.
pub fn id_from_hex(s: &str) -> Option<InvocationId> {
    if s.len() != 2 * ID_LEN || !s.is_ascii() {
        return None;
    }
    let mut out = [0u8; ID_LEN];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(out)
}

/// Id from a byte slice of the right length.
pub fn id_from_slice(b: &[u8]) -> Option<InvocationId> {
    b.try_into().ok()
}
