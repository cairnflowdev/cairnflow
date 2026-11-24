// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Encoding of the `invocation_status` table.
//!
//! The layout is deliberately crude: 1 status byte, then a tail that depends on
//! the status. Enough for the current needs and readable by eye when inspecting
//! the store. Switch to protobuf when introspection needs more fields.
//!
//! **Format 10**: `completed`, `failed`, `paused` carry the time they entered
//! class `t` (u64 BE, ms) right after the tag byte — `tag ‖ t ‖ tail`. `running`
//! and `suspended` do not: the `active` class takes `t` from the metadata (at
//! creation). The three statuses carrying `t` are only written by paths that
//! know the class; waking only writes `running`, so it never loses `t`.

use {
    bytes::{Buf, BufMut, Bytes, BytesMut},
    cairn_store::query_index::Class,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InvocationStatus {
    Running,
    /// The result was returned to the caller. A segmented output carries ref +
    /// tail and the source owner — tag [`COMPLETED_SEGMENTED`].
    Completed(crate::InvocationOutput),
    /// Waiting for a notification; will be woken.
    Suspended,
    Failed {
        code: u32,
        message: String,
    },
    /// Stuck, waiting for manual intervention.
    Paused {
        code: u32,
        message: String,
    },
}

const RUNNING: u8 = 0;
const COMPLETED: u8 = 1;
const SUSPENDED: u8 = 2;
const FAILED: u8 = 3;
const PAUSED: u8 = 4;
/// `completed` carrying a segmented output: `tag ‖ t ‖ SegmentedOutput`. Only
/// written once the `values/segmented` gate is on (an old binary never meets it).
const COMPLETED_SEGMENTED: u8 = 5;

impl InvocationStatus {
    /// The tag byte of a status that is **final** for an `/invoke` waiter:
    /// `completed`, `failed`, `paused` — the first byte of [`Self::encode`].
    pub fn is_settled_tag(tag: u8) -> bool {
        matches!(tag, COMPLETED | COMPLETED_SEGMENTED | FAILED | PAUSED)
    }

    /// Encodes, with `at_ms` = the time it entered the class (ignored for `running`/`suspended`).
    pub fn encode(&self, at_ms: u64) -> Bytes {
        let mut b = BytesMut::with_capacity(40);
        match self {
            Self::Running => b.put_u8(RUNNING),
            Self::Suspended => b.put_u8(SUSPENDED),
            Self::Completed(crate::InvocationOutput::Inline(v)) => {
                b.put_u8(COMPLETED);
                b.put_u64(at_ms);
                b.put_slice(v);
            }
            Self::Completed(crate::InvocationOutput::Segmented(s)) => {
                b.put_u8(COMPLETED_SEGMENTED);
                b.put_u64(at_ms);
                s.encode_into(&mut b);
            }
            Self::Failed { code, message } => {
                b.put_u8(FAILED);
                b.put_u64(at_ms);
                b.put_u32(*code);
                b.put_slice(message.as_bytes());
            }
            Self::Paused { code, message } => {
                b.put_u8(PAUSED);
                b.put_u64(at_ms);
                b.put_u32(*code);
                b.put_slice(message.as_bytes());
            }
        }
        b.freeze()
    }

    /// The time an encoded value entered class `t` — `None` for `running`/
    /// `suspended` (class `active`: `t` = creation time) or when broken.
    pub fn decode_at(raw: &[u8]) -> Option<u64> {
        match raw.first()? {
            &(COMPLETED | COMPLETED_SEGMENTED | FAILED | PAUSED) => {
                Some(u64::from_be_bytes(raw.get(1..9)?.try_into().ok()?))
            }
            _ => None,
        }
    }

    /// The query-index class.
    pub fn class(&self) -> Class {
        match self {
            Self::Running | Self::Suspended => Class::Active,
            Self::Paused { .. } => Class::Paused,
            Self::Completed(_) => Class::Completed,
            Self::Failed { .. } => Class::Failed,
        }
    }

    /// The class of a **status tag** (the first byte). `None` = unknown tag.
    pub fn tag_class(tag: u8) -> Option<Class> {
        Some(match tag {
            RUNNING | SUSPENDED => Class::Active,
            PAUSED => Class::Paused,
            COMPLETED | COMPLETED_SEGMENTED => Class::Completed,
            FAILED => Class::Failed,
            _ => return None,
        })
    }

    pub fn decode(mut b: Bytes) -> Option<Self> {
        if b.is_empty() {
            return None;
        }
        let tag = b.get_u8();
        if matches!(tag, COMPLETED | COMPLETED_SEGMENTED | FAILED | PAUSED) {
            if b.len() < 8 {
                return None;
            }
            b.advance(8);
        }
        Some(match tag {
            RUNNING => Self::Running,
            SUSPENDED => Self::Suspended,
            COMPLETED => Self::Completed(crate::InvocationOutput::Inline(b)),
            COMPLETED_SEGMENTED => Self::Completed(crate::InvocationOutput::Segmented(
                crate::SegmentedOutput::decode(b)?,
            )),
            FAILED | PAUSED => {
                if b.len() < 4 {
                    return None;
                }
                let code = b.get_u32();
                let message = String::from_utf8_lossy(&b).into_owned();
                if tag == FAILED {
                    Self::Failed { code, message }
                } else {
                    Self::Paused { code, message }
                }
            }
            _ => return None,
        })
    }

    /// Whether the invocation has finished for good (not counting `Suspended` — that one will continue).
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed(_) | Self::Failed { .. })
    }

    /// The status name in the API, and what `?status=` matches against.
    ///
    /// It lives here and not in the HTTP layer because the `?status=` filter
    /// must run **inside** the engine — see [`Engine::list_invocations`]. Two
    /// copies of the same mapping table are two copies that will drift apart
    /// when a sixth status is added.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Suspended => "suspended",
            Self::Completed(_) => "completed",
            Self::Failed { .. } => "failed",
            Self::Paused { .. } => "paused",
        }
    }

    /// Every valid name for `?status=`. Used to reject a typo right at the
    /// entry boundary, instead of returning an empty list that looks like an
    /// answer.
    pub const NAMES: [&'static str; 5] = ["running", "suspended", "completed", "failed", "paused"];

    /// The name of a **status tag** — the first byte of the encoded value,
    /// which the store's `Invocations` index copies (see
    /// `cairn_store::keys::ix_invocation`). `None` = unknown tag.
    pub fn tag_name(tag: u8) -> Option<&'static str> {
        match tag {
            RUNNING => Some("running"),
            SUSPENDED => Some("suspended"),
            COMPLETED | COMPLETED_SEGMENTED => Some("completed"),
            FAILED => Some("failed"),
            PAUSED => Some("paused"),
            _ => None,
        }
    }

    /// Whether this status tag is an invocation that **has not finished and
    /// will continue on its own** — `Running` or `Suspended`. `Paused` waits
    /// for an operator so it is not included. (`recover` reloads only `Running`
    /// — see [`Self::tag_is_runnable`].)
    pub fn tag_is_live(tag: u8) -> bool {
        matches!(tag, RUNNING | SUSPENDED)
    }

    /// Whether this status tag is an invocation that **has work to run right
    /// now** — only `Running`. See `Engine::runnable_invocations_unbounded`.
    pub fn tag_is_runnable(tag: u8) -> bool {
        tag == RUNNING
    }

    /// Whether this status tag is an invocation that **has ended** —
    /// `completed`, `failed`. Rebuilding VO locks on becoming leader skips
    /// them (`Engine::restore_chunk`): ending removes the queue entry in the
    /// same commit. Unknown tag ⇒ `false` (read the entry to be sure).
    pub fn tag_is_finished(tag: u8) -> bool {
        matches!(tag, COMPLETED | COMPLETED_SEGMENTED | FAILED)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_status_roundtrips() {
        for s in [
            InvocationStatus::Running,
            InvocationStatus::Suspended,
            InvocationStatus::Completed(Bytes::from_static(b"result").into()),
            InvocationStatus::Completed(Bytes::new().into()),
            // Segmented output: its own tag, same class.
            InvocationStatus::Completed(crate::InvocationOutput::Segmented(
                crate::SegmentedOutput {
                    tenant: cairn_store::TenantId::new("acme").unwrap(),
                    pk: cairn_store::PartitionKey(9),
                    owner: cairn_store::value::Owner([3; 32]),
                    value: cairn_protocol::value::Logical::Segmented {
                        refs: vec![cairn_protocol::value::SegmentRef {
                            sha256: [5; 32],
                            len: 1 << 20,
                            gen: 7,
                        }],
                        tail: Bytes::from_static(b"\""),
                        len: (1 << 20) + 1,
                    },
                },
            )),
            InvocationStatus::Failed {
                code: 570,
                message: "journal differs from code".into(),
            },
            InvocationStatus::Paused {
                code: 571,
                message: String::new(),
            },
        ] {
            let raw = s.encode(1_700_000_000_123);
            assert_eq!(
                InvocationStatus::decode(raw.clone()),
                Some(s.clone()),
                "{s:?}"
            );
            let at = InvocationStatus::decode_at(&raw);
            match s.class() {
                Class::Active => assert_eq!(at, None),
                _ => assert_eq!(at, Some(1_700_000_000_123)),
            }
        }
    }

    #[test]
    fn empty_or_unknown_bytes_do_not_panic() {
        assert_eq!(InvocationStatus::decode(Bytes::new()), None);
        assert_eq!(InvocationStatus::decode(Bytes::from_static(&[99])), None);
        // FAILED missing the `t` stamp, or with the stamp but missing the 4-byte code.
        assert_eq!(
            InvocationStatus::decode(Bytes::from_static(&[3, 0, 1])),
            None
        );
        assert_eq!(
            InvocationStatus::decode(Bytes::from_static(&[3, 0, 0, 0, 0, 0, 0, 0, 1, 0])),
            None
        );
    }
}

/// The timestamps of an invocation: the `create_ts` the store attaches to
/// entry 0 (at the admit commit) and to the status row (at the last status
/// change) — epoch ms, the clock of the node holding the range at commit time,
/// monotonic within one Db (G20). Not a byte of cairn's own, not in the
/// journal, not through the state machine: replay does not change it. `None`
/// = the row carries no timestamp (absent or `0`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Times {
    /// `createdAt`: unchanged for the invocation's whole life (CG-6).
    pub created_ms: Option<u64>,
    /// `modifiedAt`.
    pub modified_ms: Option<u64>,
}

impl Times {
    /// A raw store timestamp ⇒ `None` when absent or not positive.
    pub fn stamp(raw: Option<i64>) -> Option<u64> {
        raw.filter(|t| *t > 0).map(|t| t as u64)
    }

    /// `endedAt`: `modified_ms` when `status` has ended (`completed`/`failed`).
    pub fn ended_ms(&self, status: &InvocationStatus) -> Option<u64> {
        if status.is_terminal() {
            self.modified_ms
        } else {
            None
        }
    }
}
