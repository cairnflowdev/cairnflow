// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Fuzzing the invocation state machine with arbitrary SDK messages.
//!
//! `InvocationMachine` is the first place a tenant's messages are *understood*
//! rather than only split into frames: completion ids, entry indexes, signals,
//! nested suspensions, `run` result proposals. A panic here takes down the
//! range's engine thread, i.e. every invocation of every tenant on that range.
//!
//! Two passes per case:
//!
//! 1. a new machine (empty journal) receives `Start` and then an arbitrary
//!    event sequence — SDK frames (codes and near-protobuf payloads), runtime
//!    completions (small ids so they collide, every kind of result), signals;
//! 2. the journal pass 1 itself wrote (`Effect::Persist`) — i.e. a journal the
//!    tenant *can write* — plus a few junk entries, replayed on a second machine
//!    which then receives another event sequence. A broken journal must become
//!    `MachineError::CorruptJournal`.
//!
//! Must see: no panic; every `Persist` carries an `entry_index` consecutive
//! from the journal length at replay.

#[path = "../../cairn-protocol/tests/fuzz_gen/mod.rs"]
mod fuzz_gen;

use {
    bytes::Bytes,
    cairn_protocol::{pb, Flags, Frame, MessageType},
    cairn_runtime::{
        Completion, CompletionResult, Effect, Event, InvocationMachine, Journal, JournalEntry,
        StartParams,
    },
    fuzz_gen::{budget, message_type, number, payload},
    proptest::prelude::*,
};

fn bytes() -> impl Strategy<Value = Bytes> {
    prop::collection::vec(any::<u8>(), 0..16).prop_map(Bytes::from)
}

fn failure() -> impl Strategy<Value = pb::Failure> {
    (any::<u32>(), ".{0,8}").prop_map(|(code, message)| pb::Failure {
        code,
        message,
        metadata: vec![],
    })
}

fn completion_result() -> impl Strategy<Value = CompletionResult> {
    prop_oneof![
        prop::option::of(bytes().prop_map(cairn_protocol::value::Logical::Inline))
            .prop_map(CompletionResult::State),
        prop::collection::vec(bytes(), 0..4).prop_map(CompletionResult::StateKeys),
        Just(CompletionResult::Timer),
        Just(CompletionResult::Void),
        bytes().prop_map(CompletionResult::CallValue),
        failure().prop_map(CompletionResult::CallFailure),
        ".{0,12}".prop_map(CompletionResult::CallInvocationId),
    ]
}

/// The code of an SDK frame: biased towards commands and control (the real SDK → runtime path).
fn sdk_type() -> impl Strategy<Value = MessageType> {
    prop_oneof![
        3 => (0x0400u16..=0x0413).prop_map(MessageType),
        2 => (0u16..=0x000C).prop_map(MessageType),
        1 => message_type(),
    ]
}

fn event() -> impl Strategy<Value = Event> {
    prop_oneof![
        6 => (sdk_type(), prop_oneof![Just(0u16), Just(0x8000), any::<u16>()], payload())
            .prop_map(|(ty, flags, p)| {
                Event::FromSdk(Frame::new(ty, Flags::from_raw(flags), Bytes::from(p)))
            }),
        2 => (number(), completion_result()).prop_map(|(id, result)| {
            Event::Completion(Completion { id: id as u32, result })
        }),
        1 => payload().prop_map(|p| {
            Event::Signal(prost::Message::decode(&p[..]).unwrap_or_default())
        }),
        1 => Just(Event::Start),
    ]
}

/// Runs `events` on `m`; returns the entries the machine asks to write, checking consecutive
/// indexes.
fn run(m: &mut InvocationMachine, events: Vec<Event>) -> Vec<JournalEntry> {
    let mut persisted = Vec::new();
    for ev in events {
        let Ok(fx) = m.handle(ev) else {
            // A journal broken at replay: a named error, the caller stops the invocation.
            return persisted;
        };
        for e in fx {
            if let Effect::Persist { entry_index, entry } = e {
                assert_eq!(
                    entry_index,
                    m.journal().len() - 1,
                    "Persist is not the last entry of the journal"
                );
                persisted.push(entry);
            }
        }
    }
    persisted
}

fn params(read_only: bool) -> StartParams {
    StartParams {
        state_read_only: read_only,
        ..StartParams::default()
    }
}

proptest! {
    #![proptest_config(budget(2048))]

    #[test]
    fn machine_survives_any_sdk_stream(
        first in prop::collection::vec(event(), 0..24),
        junk in prop::collection::vec((sdk_type(), payload()), 0..3),
        junk_at in any::<prop::sample::Index>(),
        second in prop::collection::vec(event(), 0..24),
        read_only in any::<bool>(),
    ) {
        let id = [0x42; 24];
        let mut m = InvocationMachine::resume(id, params(read_only), Journal::new());
        let mut entries = run(&mut m, std::iter::once(Event::Start).chain(first).collect());

        for (ty, p) in junk {
            let at = junk_at.index(entries.len() + 1);
            entries.insert(at, JournalEntry::new(ty, Bytes::from(p)));
        }
        let mut m = InvocationMachine::resume(id, params(read_only), Journal::from_entries(entries));
        run(&mut m, std::iter::once(Event::Start).chain(second).collect());
    }
}
