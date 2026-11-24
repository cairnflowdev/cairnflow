// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The output of an invocation, possibly segmented.
//!
//! An `Output` larger than `seal_bytes` is sealed by the shell like `SetState`:
//! the journal, the `completed` status row, the `Reply` outbox row and the
//! cross-range `Completion` message carry only ref + tail **and the source
//! owner** `(tenant, pk, owner)` — the owner of the target object if keyed,
//! otherwise of the id itself (like the holder `Invocation(id)` of the
//! `Output` entry).
//!
//! **The engine never assembles.** Every place that hands an output to someone
//! else goes through the shell: ingress assembles outside the engine (capped at
//! [`MAX_ASSEMBLE`]), a caller receives it inline or as a COPY under its own
//! owner (`turn/values.rs`).
//!
//! Layout of a [`SegmentedOutput`] (shared by all three places above):
//! `u32 len ‖ tenant ‖ u64 pk ‖ owner[32] ‖ u32 len ‖ pb::Value` (prost). Only
//! written once the `values/segmented` gate is on — every binary in the cluster
//! can read it.

use {
    bytes::{Buf, BufMut, Bytes, BytesMut},
    cairn_protocol::value::{Logical, SegmentRef},
    cairn_store::{
        value::{Owner, Segment},
        PartitionKey, TenantId,
    },
};

/// Cap on assembling outside the engine: a larger logical output ⇒ `413 value_too_large`.
pub const MAX_ASSEMBLE: u64 = 64 << 20;

/// The output of a `completed` invocation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InvocationOutput {
    Inline(Bytes),
    Segmented(SegmentedOutput),
}

impl From<Bytes> for InvocationOutput {
    fn from(b: Bytes) -> Self {
        Self::Inline(b)
    }
}

impl InvocationOutput {
    /// Logical length.
    pub fn len(&self) -> u64 {
        match self {
            Self::Inline(b) => b.len() as u64,
            Self::Segmented(s) => s.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Inline bytes; `None` when segmented — the caller assembles outside the
    /// engine and never reads the tail as the whole value.
    pub fn inline(&self) -> Option<&Bytes> {
        match self {
            Self::Inline(b) => Some(b),
            Self::Segmented(_) => None,
        }
    }

    /// The `Value` of the `Output` entry — ref + tail when segmented.
    pub fn to_value(&self) -> cairn_protocol::pb::Value {
        match self {
            Self::Inline(b) => cairn_protocol::value::inline(b.clone()),
            Self::Segmented(s) => s.value.encode(),
        }
    }

    /// The result returned to waiters (the `Reply` outbox).
    pub fn into_reply(self) -> crate::ReplyResult {
        match self {
            Self::Inline(b) => crate::ReplyResult::Value(b),
            Self::Segmented(s) => crate::ReplyResult::Segmented(s),
        }
    }
}

/// A segmented output with the source owner of its segments.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentedOutput {
    pub tenant: TenantId,
    pub pk: PartitionKey,
    pub owner: Owner,
    /// Always `Logical::Segmented` (non-empty ref).
    pub value: Logical,
}

impl SegmentedOutput {
    pub fn len(&self) -> u64 {
        self.value.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn refs(&self) -> &[SegmentRef] {
        self.value.refs()
    }

    /// The tail (bytes after the last segment).
    pub fn tail(&self) -> &Bytes {
        self.value.carried()
    }

    /// The object of ref `r` under the source owner.
    pub fn segment(&self, r: &SegmentRef) -> Segment {
        Segment {
            tenant: self.tenant.as_str().to_string(),
            pk: self.pk,
            owner: self.owner,
            sha: r.sha256,
            gen: r.gen,
        }
    }

    pub fn encode_into(&self, b: &mut BytesMut) {
        let t = self.tenant.as_str().as_bytes();
        b.put_u32(t.len() as u32);
        b.put_slice(t);
        b.put_u64(self.pk.0);
        b.put_slice(&self.owner.0);
        let v = prost::Message::encode_to_vec(&self.value.encode());
        b.put_u32(v.len() as u32);
        b.put_slice(&v);
    }

    pub fn encode(&self) -> Bytes {
        let mut b = BytesMut::with_capacity(96);
        self.encode_into(&mut b);
        b.freeze()
    }

    /// Inverse of [`Self::encode_into`]; `None` = broken (the caller treats the
    /// row as broken — never guesses).
    pub fn decode_from(b: &mut Bytes) -> Option<Self> {
        let n = take_u32(b)? as usize;
        if b.remaining() < n {
            return None;
        }
        let tenant = std::str::from_utf8(&b.copy_to_bytes(n)).ok()?.to_string();
        let tenant = TenantId::new(&tenant).ok()?;
        if b.remaining() < 8 + 32 {
            return None;
        }
        let pk = PartitionKey(b.get_u64());
        let mut owner = [0u8; 32];
        b.copy_to_slice(&mut owner);
        let n = take_u32(b)? as usize;
        if b.remaining() < n {
            return None;
        }
        let raw = b.copy_to_bytes(n);
        let v: cairn_protocol::pb::Value = prost::Message::decode(raw).ok()?;
        let value = Logical::of(Some(&v)).ok()?;
        if value.refs().is_empty() {
            return None;
        }
        Some(Self {
            tenant,
            pk,
            owner: Owner(owner),
            value,
        })
    }

    pub fn decode(mut b: Bytes) -> Option<Self> {
        let s = Self::decode_from(&mut b)?;
        b.is_empty().then_some(s)
    }
}

/// The owner of values in the journal and output of invocation `id` targeting
/// `t`: a keyed/scoped object ⇒ the object's owner; otherwise ⇒ the owner of
/// the id itself. The same rule as the holder `Invocation(id)`.
pub fn output_owner(t: &crate::Target, id: &crate::InvocationId) -> (PartitionKey, Owner) {
    cairn_store::value::object_owner(&t.tenant, t.scope(), &t.service, &t.object_key)
        .unwrap_or_else(|| cairn_store::value::invocation_owner(id))
}

/// The output of `Output.value`: inline as before; segmented ⇒ ref + tail with
/// the source owner. Broken layout ⇒ error (the machine checked the frame
/// earlier).
pub fn output_of(
    v: &cairn_protocol::pb::Value,
    t: &crate::Target,
    id: &crate::InvocationId,
) -> Result<InvocationOutput, cairn_protocol::value::ValueError> {
    Ok(match Logical::of(Some(v))? {
        Logical::Inline(b) => InvocationOutput::Inline(b),
        l @ Logical::Segmented { .. } => {
            let (pk, owner) = output_owner(t, id);
            InvocationOutput::Segmented(SegmentedOutput {
                tenant: t.tenant.clone(),
                pk,
                owner,
                value: l,
            })
        }
    })
}

fn take_u32(b: &mut Bytes) -> Option<u32> {
    (b.remaining() >= 4).then(|| b.get_u32())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> SegmentedOutput {
        SegmentedOutput {
            tenant: TenantId::new("acme").unwrap(),
            pk: PartitionKey(0x1234),
            owner: Owner([7; 32]),
            value: Logical::Segmented {
                refs: vec![SegmentRef {
                    sha256: [9; 32],
                    len: 300_000,
                    gen: 42,
                }],
                tail: Bytes::from_static(b"\"]"),
                len: 300_002,
            },
        }
    }

    #[test]
    fn round_trips() {
        let s = sample();
        assert_eq!(SegmentedOutput::decode(s.encode()), Some(s));
    }

    #[test]
    fn rejects_truncated_and_trailing_bytes() {
        let raw = sample().encode();
        for n in 0..raw.len() {
            assert_eq!(SegmentedOutput::decode(raw.slice(..n)), None, "cut at {n}");
        }
        let mut long = BytesMut::from(&raw[..]);
        long.put_u8(0);
        assert_eq!(SegmentedOutput::decode(long.freeze()), None);
    }
}
