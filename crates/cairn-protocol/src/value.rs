// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The **logical** value of a `pb::Value`.
//!
//! `Value` takes two forms:
//!
//! - **Inline**: `segments` empty, `content` is the whole value — the original
//!   layout; the bytes on the wire and in the journal **do not change** (`len`
//!   left at 0).
//! - **Segmented**: `segments` non-empty; the logical value = the segments
//!   concatenated in order followed by `content` (called the *tail*), `len` =
//!   the total.
//!
//! **Every read of `Value.content` goes through this module** ([`Logical::of`]).
//! Reading `content` directly reads the tail as if it were the whole value — a
//! silent breakage, a bug class that no behaviour test fully catches; the
//! source rule `value_content_goes_through_logical` (`tests/value_source.rs`)
//! FAILs when `.content` appears outside the allow list.
//!
//! The module does not know where a segment lives or who holds it
//! (`cairn_store::value`): it only decodes and checks the layout.

use {crate::pb, bytes::Bytes};

/// The sha256 length of a segment.
pub const SHA_LEN: usize = 32;

/// A reference to an immutable segment: `values/…/<sha>-<gen>`.
///
/// `gen` is new at **every** seal (from `entropy`): a `(sha, gen)` that no
/// longer has a holder never comes back, so a deletion proof is permanent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SegmentRef {
    pub sha256: [u8; SHA_LEN],
    pub len: u64,
    pub gen: u64,
}

/// A logical value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Logical {
    Inline(Bytes),
    Segmented {
        refs: Vec<SegmentRef>,
        tail: Bytes,
        /// `Σ refs.len + tail.len`, checked when decoded.
        len: u64,
    },
}

/// A `Value` that cannot be decoded into a logical value.
///
/// English text: this error can become the message of a failed run that a
/// customer reads (runtime user-facing text).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ValueError {
    #[error("segment {index} has a {len}-byte sha256, expected 32")]
    BadSha { index: usize, len: usize },
    #[error("value declares {declared} bytes but its parts sum to {actual}")]
    LenMismatch { declared: u64, actual: u64 },
    #[error("value length overflows u64")]
    Overflow,
    #[error("segmented values are not accepted from this SDK")]
    SegmentsNotAllowed,
    #[error("value is segmented; this path only takes inline values")]
    NotInline,
}

impl Logical {
    /// Decodes a `Value`. `None` (absent field) = empty inline, like
    /// `unwrap_or_default().content` used to be.
    pub fn of(v: Option<&pb::Value>) -> Result<Logical, ValueError> {
        let Some(v) = v else {
            return Ok(Logical::Inline(Bytes::new()));
        };
        if v.segments.is_empty() {
            // The old layout leaves `len` = 0; new writers do too. Non-zero
            // must be exactly the length — no guessing.
            if v.len != 0 && v.len != v.content.len() as u64 {
                return Err(ValueError::LenMismatch {
                    declared: v.len,
                    actual: v.content.len() as u64,
                });
            }
            return Ok(Logical::Inline(v.content.clone()));
        }
        let mut refs = Vec::with_capacity(v.segments.len());
        let mut sum = v.content.len() as u64;
        for (index, s) in v.segments.iter().enumerate() {
            let sha: [u8; SHA_LEN] =
                s.sha256
                    .as_ref()
                    .try_into()
                    .map_err(|_| ValueError::BadSha {
                        index,
                        len: s.sha256.len(),
                    })?;
            sum = sum.checked_add(s.len).ok_or(ValueError::Overflow)?;
            refs.push(SegmentRef {
                sha256: sha,
                len: s.len,
                gen: s.gen,
            });
        }
        if v.len != sum {
            return Err(ValueError::LenMismatch {
                declared: v.len,
                actual: sum,
            });
        }
        Ok(Logical::Segmented {
            refs,
            tail: v.content.clone(),
            len: sum,
        })
    }

    /// Decodes a `Value` **from the SDK**. Before wire v6 an SDK has no valid
    /// way to send `segments`: an SDK that sends them refers to a segment the
    /// runtime never checked (sha, holder) ⇒ a protocol violation.
    pub fn from_sdk(v: Option<&pb::Value>, segments_allowed: bool) -> Result<Logical, ValueError> {
        let l = Self::of(v)?;
        if !segments_allowed && matches!(l, Logical::Segmented { .. }) {
            return Err(ValueError::SegmentsNotAllowed);
        }
        Ok(l)
    }

    /// The logical length.
    pub fn len(&self) -> u64 {
        match self {
            Logical::Inline(b) => b.len() as u64,
            Logical::Segmented { len, .. } => *len,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The bytes that must be carried **inside** the frame or entry: the whole
    /// value when inline, only the tail when segmented.
    pub fn carried(&self) -> &Bytes {
        match self {
            Logical::Inline(b) => b,
            Logical::Segmented { tail, .. } => tail,
        }
    }

    pub fn refs(&self) -> &[SegmentRef] {
        match self {
            Logical::Inline(_) => &[],
            Logical::Segmented { refs, .. } => refs,
        }
    }

    /// The whole value when inline; segmented ⇒ [`ValueError::NotInline`]. For
    /// the paths that do not accept segmented values — a clear error instead of
    /// reading the tail as the whole value.
    pub fn into_inline(self) -> Result<Bytes, ValueError> {
        match self {
            Logical::Inline(b) => Ok(b),
            Logical::Segmented { .. } => Err(ValueError::NotInline),
        }
    }

    /// Re-encodes. Inline ⇒ exactly [`inline`] (bytes unchanged from the original layout).
    pub fn encode(&self) -> pb::Value {
        match self {
            Logical::Inline(b) => inline(b.clone()),
            Logical::Segmented { refs, tail, len } => pb::Value {
                content: tail.clone(),
                segments: refs
                    .iter()
                    .map(|r| pb::SegmentRef {
                        sha256: Bytes::copy_from_slice(&r.sha256),
                        len: r.len,
                        gen: r.gen,
                    })
                    .collect(),
                len: *len,
            },
        }
    }
}

/// An inline `Value` — the **only place** that builds a `pb::Value` from bytes.
pub fn inline(content: Bytes) -> pb::Value {
    pb::Value {
        content,
        segments: Vec::new(),
        len: 0,
    }
}

/// The protobuf encoded size of a `Value`.
pub fn encoded_len(v: &pb::Value) -> usize {
    prost::Message::encoded_len(v)
}

/// The bytes of a `Value` that is **certainly inline** (a value read from a row
/// cairn built inline itself, or a path that does not accept segmented values):
/// like [`Logical::into_inline`].
pub fn inline_content(v: Option<&pb::Value>) -> Result<Bytes, ValueError> {
    Logical::of(v)?.into_inline()
}

/// Every `Value` that message `m` carries, in field order. An exhaustive
/// `match`: a new message carrying a `Value` that is not listed here does not
/// compile.
macro_rules! values_of {
    ($m:expr, $as:ident) => {{
        use crate::{pb, Message as M};
        let mut out = Vec::new();
        match $m {
            M::Input(i) => out.extend(i.value.$as()),
            M::Output(o) => {
                if let Some(pb::output_command_message::Result::Value(v)) = o.result.$as() {
                    out.push(v)
                }
            }
            M::SetState(s) => out.extend(s.value.$as()),
            M::GetEagerState(g) => {
                if let Some(pb::get_eager_state_command_message::Result::Value(v)) = g.result.$as()
                {
                    out.push(v)
                }
            }
            M::CompletePromise(c) => {
                if let Some(pb::complete_promise_command_message::Completion::CompletionValue(v)) =
                    c.completion.$as()
                {
                    out.push(v)
                }
            }
            M::SendSignal(s) => {
                if let Some(pb::send_signal_command_message::Result::Value(v)) = s.result.$as() {
                    out.push(v)
                }
            }
            M::GetStateCompletion(c) => {
                if let Some(pb::get_state_completion_notification_message::Result::Value(v)) =
                    c.result.$as()
                {
                    out.push(v)
                }
            }
            M::GetPromiseCompletion(c) => {
                if let Some(pb::get_promise_completion_notification_message::Result::Value(v)) =
                    c.result.$as()
                {
                    out.push(v)
                }
            }
            M::PeekPromiseCompletion(c) => {
                if let Some(pb::peek_promise_completion_notification_message::Result::Value(v)) =
                    c.result.$as()
                {
                    out.push(v)
                }
            }
            M::CallCompletion(c) => {
                if let Some(pb::call_completion_notification_message::Result::Value(v)) =
                    c.result.$as()
                {
                    out.push(v)
                }
            }
            M::AttachInvocationCompletion(c) => {
                if let Some(pb::attach_invocation_completion_notification_message::Result::Value(
                    v,
                )) = c.result.$as()
                {
                    out.push(v)
                }
            }
            M::GetInvocationOutputCompletion(c) => {
                if let Some(
                    pb::get_invocation_output_completion_notification_message::Result::Value(v),
                ) = c.result.$as()
                {
                    out.push(v)
                }
            }
            M::RunCompletion(c) => {
                if let Some(pb::run_completion_notification_message::Result::Value(v)) =
                    c.result.$as()
                {
                    out.push(v)
                }
            }
            M::Signal(c) => {
                if let Some(pb::signal_notification_message::Result::Value(v)) = c.result.$as() {
                    out.push(v)
                }
            }
            // `bytes value` (14) is not a `Value`; only field 16 carries refs.
            M::ProposeRunCompletion(p) => {
                if let Some(pb::propose_run_completion_message::Result::SegmentedValue(v)) =
                    p.result.$as()
                {
                    out.push(v)
                }
            }
            M::Start(_)
            | M::Suspension(_)
            | M::Error(_)
            | M::End(_)
            | M::CommandAck(_)
            | M::ProposeRunCompletionAck(_)
            | M::Keepalive(_)
            | M::ReplayReport(_)
            | M::ContinueAsNew(_)
            | M::StreamAppend(_)
            | M::ArtifactPut(_)
            | M::SegmentUrls(_)
            | M::GetState(_)
            | M::ClearState(_)
            | M::ClearAllState(_)
            | M::GetStateKeys(_)
            | M::GetEagerStateKeys(_)
            | M::GetPromise(_)
            | M::PeekPromise(_)
            | M::Sleep(_)
            | M::Call(_)
            | M::OneWayCall(_)
            | M::Run(_)
            | M::AttachInvocation(_)
            | M::GetInvocationOutput(_)
            | M::GetStateKeysCompletion(_)
            | M::CompletePromiseCompletion(_)
            | M::SleepCompletion(_)
            | M::CallInvocationIdCompletion(_)
            | M::Custom { .. } => {}
        }
        out
    }};
}

/// Every `Value` of `m` (read).
pub fn values(m: &crate::Message) -> Vec<&pb::Value> {
    values_of!(m, as_ref)
}

/// Every `Value` of `m` (mutable) — the shell replaces a segmented `Value` with
/// an assembled inline one before sending it down to an SDK ≤ v5.
pub fn values_mut(m: &mut crate::Message) -> Vec<&mut pb::Value> {
    values_of!(m, as_mut)
}

/// Every segment ref that `m` carries (layout checked) — the `Invocation(id)`
/// holder of a journal entry. A broken layout ⇒ an error.
pub fn refs_in(m: &crate::Message) -> Result<Vec<SegmentRef>, ValueError> {
    let mut out = Vec::new();
    for v in values(m) {
        out.extend_from_slice(Logical::of(Some(v))?.refs());
    }
    Ok(out)
}

/// Every `Value` of a message **sent up by the SDK** must be inline before wire
/// v6: an SDK ≤ v5 has no valid way to send `segments`, so an SDK sending them
/// points at a segment whose sha and holder the runtime never checked. Also
/// checks the layout (`len`).
pub fn sdk_values_inline(m: &crate::Message) -> Result<(), ValueError> {
    for v in values(m) {
        Logical::from_sdk(Some(v), false)?;
    }
    Ok(())
}

/// Like [`sdk_values_inline`] for a frame **the shell has sealed**
/// (`Event::Sealed`): only `SetState.value`, `GetEagerState.result`,
/// `Output.value` and `ProposeRunCompletion.segmented_value` may be segmented;
/// every other `Value` must still be inline. Whether the refs exist is the
/// engine's business (checked under the engine, in the same commit step).
pub fn sealed_values_ok(m: &crate::Message) -> Result<(), ValueError> {
    use crate::Message as M;
    match m {
        M::SetState(_) | M::GetEagerState(_) | M::Output(_) | M::ProposeRunCompletion(_) => {
            for v in values(m) {
                Logical::of(Some(v))?;
            }
            Ok(())
        }
        _ => Err(ValueError::SegmentsNotAllowed),
    }
}

#[cfg(test)]
mod tests {
    use {super::*, prost::Message as _};

    fn seg(b: u8, len: u64, gen: u64) -> SegmentRef {
        SegmentRef {
            sha256: [b; 32],
            len,
            gen,
        }
    }

    #[test]
    fn inline_encoding_is_byte_identical_to_the_old_layout() {
        // The old layout: `message Value { bytes content = 1; }`.
        let v = inline(Bytes::from_static(b"hello"));
        assert_eq!(v.encode_to_vec(), [0x0a, 5, b'h', b'e', b'l', b'l', b'o']);
        assert_eq!(inline(Bytes::new()).encode_to_vec(), Vec::<u8>::new());
    }

    #[test]
    fn absent_is_empty_inline() {
        assert_eq!(Logical::of(None), Ok(Logical::Inline(Bytes::new())));
    }

    #[test]
    fn segmented_round_trips() {
        let l = Logical::Segmented {
            refs: vec![seg(1, 10, 7), seg(2, 20, 8)],
            tail: Bytes::from_static(b"abc"),
            len: 33,
        };
        let wire = l.encode().encode_to_vec();
        let back = pb::Value::decode(&wire[..]).unwrap();
        assert_eq!(Logical::of(Some(&back)), Ok(l.clone()));
        assert_eq!(l.len(), 33);
        assert_eq!(l.carried(), &Bytes::from_static(b"abc"));
        assert_eq!(l.into_inline(), Err(ValueError::NotInline));
    }

    #[test]
    fn length_must_match_parts() {
        let mut v = Logical::Segmented {
            refs: vec![seg(1, 10, 7)],
            tail: Bytes::from_static(b"x"),
            len: 11,
        }
        .encode();
        v.len = 12;
        assert_eq!(
            Logical::of(Some(&v)),
            Err(ValueError::LenMismatch {
                declared: 12,
                actual: 11
            })
        );
        let mut i = inline(Bytes::from_static(b"xy"));
        i.len = 2;
        assert!(Logical::of(Some(&i)).is_ok());
        i.len = 3;
        assert!(Logical::of(Some(&i)).is_err());
    }

    #[test]
    fn short_sha_is_rejected() {
        let mut v = Logical::Segmented {
            refs: vec![seg(1, 10, 7)],
            tail: Bytes::new(),
            len: 10,
        }
        .encode();
        v.segments[0].sha256 = Bytes::from_static(b"short");
        assert_eq!(
            Logical::of(Some(&v)),
            Err(ValueError::BadSha { index: 0, len: 5 })
        );
    }

    #[test]
    fn overflow_is_rejected() {
        let mut v = Logical::Segmented {
            refs: vec![seg(1, u64::MAX, 7), seg(2, 1, 8)],
            tail: Bytes::new(),
            len: 0,
        }
        .encode();
        v.len = 0;
        assert_eq!(Logical::of(Some(&v)), Err(ValueError::Overflow));
    }

    #[test]
    fn sdk_message_with_segments_is_rejected() {
        let seg = Logical::Segmented {
            refs: vec![seg(1, 10, 7)],
            tail: Bytes::new(),
            len: 10,
        }
        .encode();
        let set = crate::Message::SetState(pb::SetStateCommandMessage {
            key: Bytes::from_static(b"k"),
            value: Some(seg.clone()),
            name: String::new(),
        });
        assert_eq!(sdk_values_inline(&set), Err(ValueError::SegmentsNotAllowed));
        let out = crate::Message::Output(pb::OutputCommandMessage {
            result: Some(pb::output_command_message::Result::Value(seg)),
            ..Default::default()
        });
        assert_eq!(sdk_values_inline(&out), Err(ValueError::SegmentsNotAllowed));
        let ok = crate::Message::SetState(pb::SetStateCommandMessage {
            key: Bytes::from_static(b"k"),
            value: Some(inline(Bytes::from_static(b"v"))),
            name: String::new(),
        });
        assert_eq!(sdk_values_inline(&ok), Ok(()));
    }

    #[test]
    fn sdk_before_v6_may_not_send_segments() {
        let v = Logical::Segmented {
            refs: vec![seg(1, 10, 7)],
            tail: Bytes::new(),
            len: 10,
        }
        .encode();
        assert_eq!(
            Logical::from_sdk(Some(&v), false),
            Err(ValueError::SegmentsNotAllowed)
        );
        assert!(Logical::from_sdk(Some(&v), true).is_ok());
        assert!(Logical::from_sdk(Some(&inline(Bytes::from_static(b"a"))), false).is_ok());
    }
}
