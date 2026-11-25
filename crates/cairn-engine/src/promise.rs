// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The record of a durable promise — the `Promises` table.
//!
//! The record carries the **completer** next to the result, and that is not
//! decoration: `CompletePromise` must be idempotent across replay. The state
//! machine replays the task for every completion still pending, so an
//! invocation that dies right after writing the result will complete the same
//! promise *again* on its next run — and if it did not recognise itself, it
//! would see "promise already has a result" and report an error for work it
//! already finished.
//!
//! Same version discipline as `outbox`: the version byte comes first, durable
//! data must stay readable after a binary upgrade.

use {
    crate::outbox::{OutboxError, ReplyResult},
    bytes::{Buf, BufMut, Bytes, BytesMut},
    cairn_store::InvocationId,
};

const VERSION: u8 = 2;
const VERSION_MIN: u8 = 2;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PromiseRecord {
    pub result: ReplyResult,
    /// Who completed it: `(invocation, completion id)` of the `CompletePromise` command.
    pub completer: (InvocationId, u32),
}

impl PromiseRecord {
    pub fn encode(&self) -> Bytes {
        let mut b = BytesMut::with_capacity(64);
        b.put_u8(VERSION);
        b.put_slice(&self.completer.0);
        b.put_u32(self.completer.1);
        match &self.result {
            ReplyResult::Value(v) => {
                b.put_u8(0);
                b.put_u32(v.len() as u32);
                b.put_slice(v);
            }
            ReplyResult::Failure { code, message } => {
                b.put_u8(1);
                b.put_u32(*code);
                b.put_u32(message.len() as u32);
                b.put_slice(message.as_bytes());
            }
            // A promise value is always inline: no path builds a segmented
            // promise record — written as a failure instead of panicking on
            // the engine thread.
            ReplyResult::Segmented(_) => {
                // Tripwire: a promise record carrying a segmented value ⇒ the
                // engine would have to assemble it on delivery.
                cairn_store::failpoint!("value::engine_needs_bytes");
                let message = "segmented promise value";
                b.put_u8(1);
                b.put_u32(500);
                b.put_u32(message.len() as u32);
                b.put_slice(message.as_bytes());
            }
        }
        b.freeze()
    }

    pub fn decode(mut b: Bytes) -> Result<Self, OutboxError> {
        if b.remaining() < 2 {
            return Err(OutboxError::Empty);
        }
        let version = b.get_u8();
        if !(VERSION_MIN..=VERSION).contains(&version) {
            return Err(OutboxError::UnknownVersion(version));
        }
        if b.remaining() < cairn_store::ID_LEN + 4 + 1 {
            return Err(OutboxError::Truncated);
        }
        let mut id = [0u8; cairn_store::ID_LEN];
        b.copy_to_slice(&mut id);
        let cid = b.get_u32();
        let result = match b.get_u8() {
            0 => {
                let n = get_len(&mut b)?;
                ReplyResult::Value(b.split_to(n))
            }
            1 => {
                if b.remaining() < 4 {
                    return Err(OutboxError::Truncated);
                }
                let code = b.get_u32();
                let n = get_len(&mut b)?;
                let raw = b.split_to(n);
                ReplyResult::Failure {
                    code,
                    message: String::from_utf8_lossy(&raw).into_owned(),
                }
            }
            other => return Err(OutboxError::UnknownKind(other)),
        };
        Ok(Self {
            result,
            completer: (id, cid),
        })
    }
}

fn get_len(b: &mut Bytes) -> Result<usize, OutboxError> {
    if b.remaining() < 4 {
        return Err(OutboxError::Truncated);
    }
    let n = b.get_u32() as usize;
    if b.remaining() < n {
        return Err(OutboxError::Truncated);
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        for result in [
            ReplyResult::Value(Bytes::from_static(b"value")),
            ReplyResult::Value(Bytes::new()),
            ReplyResult::Failure {
                code: 500,
                message: "broken".into(),
            },
        ] {
            let r = PromiseRecord {
                result,
                completer: ([7u8; 24], 3),
            };
            assert_eq!(PromiseRecord::decode(r.encode()).unwrap(), r);
        }
    }

    #[test]
    fn truncated_does_not_panic() {
        let r = PromiseRecord {
            result: ReplyResult::Value(Bytes::from_static(b"x")),
            completer: ([1u8; 24], 1),
        };
        let full = r.encode();
        for n in 0..full.len() {
            let _ = PromiseRecord::decode(full.slice(..n));
        }
    }
}
