// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The journal — append-only, per principle P1 of `protocol-v1.md`.
//!
//! **Two indexes, do not mix them up.**
//!
//! - *entry index* — the position in the journal, counting **both** commands
//!   and notifications. `StartMessage.known_entries` counts by this index, and
//!   replay follows it.
//! - *command index* — counts only commands. `CommandAckMessage.command_index`
//!   and `ErrorMessage.related_command_index` speak in this index (spec section
//!   8.1).
//!
//! Mixing them up is a silent bug: an ack points at the wrong entry, and it
//! only shows when an invocation has notifications between its commands — that
//! is, every invocation with a `ctx.sleep`.

use {
    bytes::{Buf, BufMut, Bytes, BytesMut},
    cairn_protocol::{Frame, MessageKind, MessageType},
};

/// A durable entry. Stores `(type, payload)`, **not** the flags.
///
/// The `REQUESTED_ACK` flag is an agreement of *one connection* — the SDK asks
/// for an ack for this run. Storing it in the journal and resending it on
/// replay would send an ack for a request that no longer exists.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalEntry {
    pub ty: MessageType,
    pub payload: Bytes,
}

impl JournalEntry {
    pub fn new(ty: MessageType, payload: Bytes) -> Self {
        Self { ty, payload }
    }

    pub fn from_frame(frame: &Frame) -> Self {
        Self::new(frame.ty, frame.payload.clone())
    }

    pub fn is_command(&self) -> bool {
        self.ty.kind() == MessageKind::Command
    }

    pub fn is_notification(&self) -> bool {
        self.ty.kind() == MessageKind::Notification
    }

    /// The durable form: a big-endian `u16` type followed by the payload.
    pub fn encode(&self) -> Bytes {
        let mut b = BytesMut::with_capacity(2 + self.payload.len());
        b.put_u16(self.ty.raw());
        b.put_slice(&self.payload);
        b.freeze()
    }

    pub fn decode(mut bytes: Bytes) -> Result<Self, JournalError> {
        if bytes.len() < 2 {
            return Err(JournalError::Truncated { len: bytes.len() });
        }
        let ty = MessageType(bytes.get_u16());
        Ok(Self::new(ty, bytes))
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum JournalError {
    #[error("corrupt entry: {len} bytes, need at least 2")]
    Truncated { len: usize },
}

/// The journal of one invocation.
///
/// Only `append` — no `set`, no `remove`. P1 is append-only, and the cheapest
/// way to keep that promise is not to write the functions that would break it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Journal {
    entries: Vec<JournalEntry>,
    /// The number of command entries, to allocate `command_index` without rescanning.
    command_count: u32,
}

impl Journal {
    pub fn new() -> Self {
        Self::default()
    }

    /// Rebuilds from the durable entries, **in entry-index order**.
    pub fn from_entries(entries: Vec<JournalEntry>) -> Self {
        let command_count = entries.iter().filter(|e| e.is_command()).count() as u32;
        Self {
            entries,
            command_count,
        }
    }

    /// The number of entries — exactly the value put into `StartMessage.known_entries`.
    pub fn len(&self) -> u32 {
        self.entries.len() as u32
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn entries(&self) -> &[JournalEntry] {
        &self.entries
    }

    pub fn get(&self, entry_index: u32) -> Option<&JournalEntry> {
        self.entries.get(entry_index as usize)
    }

    /// The `command_index` the next entry will get if it is a command.
    pub fn next_command_index(&self) -> u32 {
        self.command_count
    }

    /// Appends an entry. Returns `(entry_index, command_index if it is a command)`.
    pub fn append(&mut self, entry: JournalEntry) -> Appended {
        let entry_index = self.len();
        let command_index = entry.is_command().then(|| {
            let i = self.command_count;
            self.command_count += 1;
            i
        });
        self.entries.push(entry);
        Appended {
            entry_index,
            command_index,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Appended {
    pub entry_index: u32,
    /// `None` if the entry is a notification — notifications have no command index.
    pub command_index: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd(ty: MessageType) -> JournalEntry {
        JournalEntry::new(ty, Bytes::new())
    }

    #[test]
    fn entry_roundtrip() {
        let e = JournalEntry::new(MessageType::CALL, Bytes::from_static(b"payload"));
        assert_eq!(JournalEntry::decode(e.encode()).unwrap(), e);
    }

    #[test]
    fn empty_entry_still_roundtrips() {
        // EndMessage{} and Void{} encode to a 0-byte payload.
        let e = JournalEntry::new(MessageType::END, Bytes::new());
        assert_eq!(JournalEntry::decode(e.encode()).unwrap(), e);
    }

    #[test]
    fn too_short_entry_is_rejected() {
        assert_eq!(
            JournalEntry::decode(Bytes::from_static(b"x")),
            Err(JournalError::Truncated { len: 1 })
        );
    }

    #[test]
    fn command_index_skips_notifications() {
        // This is the module's easiest place to go wrong: notifications between commands.
        let mut j = Journal::new();
        let a = j.append(cmd(MessageType::INPUT)); // entry 0, command 0
        let b = j.append(cmd(MessageType::SLEEP)); // entry 1, command 1
        let n = j.append(cmd(MessageType::SLEEP_COMPLETION)); // entry 2, NOT a command
        let c = j.append(cmd(MessageType::OUTPUT)); // entry 3, command 2

        assert_eq!((a.entry_index, a.command_index), (0, Some(0)));
        assert_eq!((b.entry_index, b.command_index), (1, Some(1)));
        assert_eq!((n.entry_index, n.command_index), (2, None));
        assert_eq!(
            (c.entry_index, c.command_index),
            (3, Some(2)),
            "a notification used up a command index"
        );
        assert_eq!(j.len(), 4, "known_entries must count notifications too");
    }

    #[test]
    fn rebuilding_from_entries_gives_same_result() {
        let mut j = Journal::new();
        for ty in [
            MessageType::INPUT,
            MessageType::SLEEP,
            MessageType::SLEEP_COMPLETION,
            MessageType::OUTPUT,
        ] {
            j.append(cmd(ty));
        }
        let rebuilt = Journal::from_entries(j.entries().to_vec());
        assert_eq!(rebuilt, j);
        assert_eq!(rebuilt.next_command_index(), j.next_command_index());
    }
}
