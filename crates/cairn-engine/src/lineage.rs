// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **The lineage of a call**: the root of the call tree and the user tags the
//! callee receives in its metadata (`$root`, the `ByAttr` tags).
//!
//! Travels with the `Call` outbox row ([`crate::OutboundCall`]) and the
//! `StartCall` message ([`crate::StartCall`]) as a **tail block**: appended
//! after the last field of the current layout, without bumping the version. An
//! older binary stops reading at the last field it knows and ignores the rest
//! (neither decoder checks for trailing bytes — pinned by
//! `outbox::tests::call_lineage_is_a_tail`, `interpartition::tests::lineage_is_a_tail`),
//! so the call still runs, only without its lineage. Block absent ⇒ default (no
//! root, no tags).
//!
//! Layout: a sequence of `tag(u8) ‖ len(u16) ‖ bytes`; unknown tags are skipped
//! (a newer writer), a broken block reads as empty — lineage is metadata and
//! never breaks a call.

use {
    bytes::{Buf, BufMut, BytesMut},
    cairn_store::InvocationId,
};

const ROOT: u8 = 1;
const TAG: u8 = 2;

/// The root and tags of a call.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Lineage {
    /// The root of the call tree: the caller's `root_id`, or the caller itself
    /// when it is the root.
    pub root_id: Option<InvocationId>,
    /// Validated user tags (`query_index::check_tag`).
    pub tags: Vec<(String, String)>,
}

impl Lineage {
    pub fn is_empty(&self) -> bool {
        self.root_id.is_none() && self.tags.is_empty()
    }

    /// Appends the tail block (nothing when empty — the old layout byte for byte).
    pub fn encode_tail(&self, b: &mut BytesMut) {
        if let Some(r) = &self.root_id {
            b.put_u8(ROOT);
            b.put_u16(r.len() as u16);
            b.put_slice(r);
        }
        for (k, v) in &self.tags {
            let mut body = BytesMut::new();
            body.put_u16(k.len() as u16);
            body.put_slice(k.as_bytes());
            body.put_u16(v.len() as u16);
            body.put_slice(v.as_bytes());
            b.put_u8(TAG);
            b.put_u16(body.len() as u16);
            b.put_slice(&body);
        }
    }

    /// Reads the tail block from the rest of `b`. Broken ⇒ what was read up to there.
    pub fn decode_tail(b: &mut impl Buf) -> Self {
        let mut out = Self::default();
        while b.remaining() >= 3 {
            let tag = b.get_u8();
            let len = b.get_u16() as usize;
            if b.remaining() < len {
                break;
            }
            let body = b.copy_to_bytes(len);
            match tag {
                ROOT if body.len() == cairn_store::ID_LEN => {
                    let mut id = [0u8; cairn_store::ID_LEN];
                    id.copy_from_slice(&body);
                    out.root_id = Some(id);
                }
                TAG => {
                    let mut c = &body[..];
                    let mut s = || -> Option<String> {
                        if c.remaining() < 2 {
                            return None;
                        }
                        let n = c.get_u16() as usize;
                        if c.remaining() < n {
                            return None;
                        }
                        let v = String::from_utf8(c[..n].to_vec()).ok();
                        c.advance(n);
                        v
                    };
                    if let (Some(k), Some(v)) = (s(), s()) {
                        out.tags.push((k, v));
                    }
                }
                _ => {}
            }
        }
        out
    }

    /// The valid tags of `raw` (from the SDK): drops invalid tags and those over the tag-count cap.
    pub fn checked_tags(raw: &[(String, String)]) -> Vec<(String, String)> {
        raw.iter()
            .filter(|(k, v)| cairn_store::query_index::check_tag(k, v).is_ok())
            .take(cairn_store::query_index::MAX_TAGS)
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use {super::*, bytes::Bytes};

    #[test]
    fn round_trips_and_skips_unknown_tags() {
        let l = Lineage {
            root_id: Some([7; 24]),
            tags: vec![("team".into(), "pay".into()), ("env".into(), "prod".into())],
        };
        let mut b = BytesMut::new();
        b.put_u8(9);
        b.put_u16(2);
        b.put_slice(b"xx");
        l.encode_tail(&mut b);
        let mut r = b.freeze();
        assert_eq!(Lineage::decode_tail(&mut r), l);
        assert!(Lineage::decode_tail(&mut Bytes::new()).is_empty());
    }

    /// A tail block cut short reads as what precedes it, without panicking.
    #[test]
    fn truncated_tail_is_lenient() {
        let l = Lineage {
            root_id: Some([7; 24]),
            tags: vec![("team".into(), "pay".into())],
        };
        let mut b = BytesMut::new();
        l.encode_tail(&mut b);
        let full = b.freeze();
        let mut cut = full.slice(..full.len() - 2);
        assert_eq!(
            Lineage::decode_tail(&mut cut),
            Lineage {
                root_id: Some([7; 24]),
                tags: vec![]
            }
        );
    }
}
