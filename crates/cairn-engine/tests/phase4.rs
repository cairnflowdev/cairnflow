// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Promises, attach and the rest on real storage: durable promises,
//! attach/get-output, eager state, clear-all — through the engine (real
//! commits, real outbox), no mocks.

// Tests run on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterministic sources"
)]

use {
    anyhow::Result,
    bytes::Bytes,
    cairn_engine::{CreateOptions, Engine, Invocation, OutboxEntry, ReplyResult, Target},
    cairn_protocol::{encode_message, pb, Flags, Frame, FrameDecoder, Message, MessageType},
    cairn_runtime::{Completion, CompletionResult, Event, StartParams},
    cairn_store::{
        make_invocation_id, LeadershipGrant, PartitionId, PartitionKey, PartitionWriter,
    },
    slatedb::{
        config::Settings,
        object_store::{local::LocalFileSystem, ObjectStore},
        Db,
    },
    std::{path::PathBuf, sync::Arc, time::Duration},
};

const SERVICE: &str = "Workflow";
const OBJECT: &str = "don-7";

fn target() -> Target {
    Target {
        tenant: Default::default(),
        scope: None,
        service: SERVICE.into(),
        handler: "run".into(),
        object_key: OBJECT.into(),
    }
}

fn inv_id(n: u8) -> [u8; 24] {
    make_invocation_id(PartitionKey(0), [n; 16])
}

fn hex(id: &[u8; 24]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

fn frame<M: prost::Message>(ty: MessageType, m: &M) -> Frame {
    let wire = encode_message(ty, Flags::NONE, m);
    let mut buf = bytes::BytesMut::from(&wire[..]);
    FrameDecoder::new().decode(&mut buf).unwrap().unwrap()
}

fn fresh_root(name: &str) -> Result<PathBuf> {
    let root = std::env::temp_dir().join(format!("cairnflow-phase4-{name}"));
    if root.exists() {
        std::fs::remove_dir_all(&root)?;
    }
    std::fs::create_dir_all(&root)?;
    Ok(root)
}

async fn open_engine(root: &PathBuf) -> Result<Engine> {
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(root)?);
    let settings = Settings {
        flush_interval: Some(Duration::from_millis(10)),
        ..Default::default()
    };
    let db = Db::builder("partition-0", store)
        .with_settings(settings)
        .build()
        .await?;
    let writer = PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db).await?;
    Ok(Engine::open(writer).await?)
}

async fn start(engine: &mut Engine, n: u8) -> Result<(Invocation, Vec<Message>)> {
    let id = inv_id(n);
    let mut inv = engine
        .create(
            id,
            &target(),
            CreateOptions::default(),
            StartParams::default(),
            Bytes::new(),
        )
        .await?;
    let opening = engine.step(&mut inv, Event::Start).await?;
    Ok((inv, opening))
}

fn types(msgs: &[Message]) -> Vec<MessageType> {
    msgs.iter().map(|m| m.message_type()).collect()
}

/// A full promise round: one invocation waits, another invocation of the same
/// object completes it — the result reaches the waiter through the outbox, and
/// a second completion gets 409.
#[tokio::test]
async fn promise_wait_and_complete() -> Result<()> {
    let root = fresh_root("promise")?;
    let mut engine = open_engine(&root).await?;

    // A waits for promise "approve".
    let (mut a, _) = start(&mut engine, 1).await?;
    let msgs = engine
        .step(
            &mut a,
            Event::FromSdk(frame(
                MessageType::GET_PROMISE,
                &pb::GetPromiseCommandMessage {
                    key: "approve".into(),
                    result_completion_id: 1,
                    name: String::new(),
                },
            )),
        )
        .await?;
    assert!(msgs.is_empty(), "no result yet ⇒ nothing to send");

    // B (same object) peeks first: no result yet → void.
    let (mut b, _) = start(&mut engine, 2).await?;
    let msgs = engine
        .step(
            &mut b,
            Event::FromSdk(frame(
                MessageType::PEEK_PROMISE,
                &pb::PeekPromiseCommandMessage {
                    key: "approve".into(),
                    result_completion_id: 1,
                    name: String::new(),
                },
            )),
        )
        .await?;
    assert_eq!(types(&msgs), vec![MessageType::PEEK_PROMISE_COMPLETION]);
    let Message::PeekPromiseCompletion(n) = &msgs[0] else {
        unreachable!()
    };
    assert!(matches!(
        n.result,
        Some(pb::peek_promise_completion_notification_message::Result::Void(_))
    ));

    // B completes → gets void, and the result for A is in the outbox.
    let msgs = engine
        .step(
            &mut b,
            Event::FromSdk(frame(
                MessageType::COMPLETE_PROMISE,
                &pb::CompletePromiseCommandMessage {
                    key: "approve".into(),
                    completion: Some(
                        pb::complete_promise_command_message::Completion::CompletionValue(
                            cairn_protocol::value::inline(Bytes::from_static(b"\"ok\"")),
                        ),
                    ),
                    result_completion_id: 2,
                    name: String::new(),
                },
            )),
        )
        .await?;
    assert_eq!(types(&msgs), vec![MessageType::COMPLETE_PROMISE_COMPLETION]);

    let pending = engine.pending_outbox(16).await?;
    assert_eq!(pending.len(), 1, "one waiter = one outbox row");
    let (row, entry) = &pending[0];
    let OutboxEntry::Reply(r) = entry else {
        panic!("must be Reply, got {entry:?}")
    };
    assert_eq!(r.caller, inv_id(1));
    assert_eq!(r.completion_id, 1);
    assert_eq!(r.result, ReplyResult::Value(Bytes::from_static(b"\"ok\"")));

    // Deliver to A — as the server's drain_outbox would.
    let msgs = engine
        .deliver(
            &mut a,
            Completion {
                id: 1,
                result: CompletionResult::CallValue(Bytes::from_static(b"\"ok\"")),
            },
            cairn_engine::Source::Outbox { row: *row },
        )
        .await?;
    assert_eq!(types(&msgs), vec![MessageType::GET_PROMISE_COMPLETION]);

    // C completes again → 409, the promise keeps the first result.
    let (mut c, _) = start(&mut engine, 3).await?;
    let msgs = engine
        .step(
            &mut c,
            Event::FromSdk(frame(
                MessageType::COMPLETE_PROMISE,
                &pb::CompletePromiseCommandMessage {
                    key: "approve".into(),
                    completion: Some(
                        pb::complete_promise_command_message::Completion::CompletionValue(
                            cairn_protocol::value::inline(Bytes::from_static(b"\"late\"")),
                        ),
                    ),
                    result_completion_id: 1,
                    name: String::new(),
                },
            )),
        )
        .await?;
    let Message::CompletePromiseCompletion(n) = &msgs[0] else {
        panic!("must be CompletePromiseCompletion")
    };
    let Some(pb::complete_promise_completion_notification_message::Result::Failure(f)) = &n.result
    else {
        panic!("a second completion must fail")
    };
    assert_eq!(f.code, 409);

    // D gets after the result exists → receives it right away, no wait queue.
    let (mut d, _) = start(&mut engine, 4).await?;
    let msgs = engine
        .step(
            &mut d,
            Event::FromSdk(frame(
                MessageType::GET_PROMISE,
                &pb::GetPromiseCommandMessage {
                    key: "approve".into(),
                    result_completion_id: 1,
                    name: String::new(),
                },
            )),
        )
        .await?;
    assert_eq!(types(&msgs), vec![MessageType::GET_PROMISE_COMPLETION]);
    Ok(())
}

/// Attach to a finished invocation receives the result right away; get-output
/// on a running invocation returns void; attach to a nonexistent id (same
/// partition) returns 404.
#[tokio::test]
async fn attach_and_get_output_same_partition() -> Result<()> {
    let root = fresh_root("attach")?;
    let mut engine = open_engine(&root).await?;

    // A runs to completion with an output.
    let (mut a, _) = start(&mut engine, 1).await?;
    engine
        .step(
            &mut a,
            Event::FromSdk(frame(
                MessageType::OUTPUT,
                &pb::OutputCommandMessage {
                    result: Some(pb::output_command_message::Result::Value(
                        cairn_protocol::value::inline(Bytes::from_static(b"\"done\"")),
                    )),
                    name: String::new(),
                },
            )),
        )
        .await?;
    engine
        .step(
            &mut a,
            Event::FromSdk(frame(MessageType::END, &pb::EndMessage {})),
        )
        .await?;

    // B attaches to A → the value comes back right away (A is terminal, same partition).
    let (mut b, _) = start(&mut engine, 2).await?;
    let msgs = engine
        .step(
            &mut b,
            Event::FromSdk(frame(
                MessageType::ATTACH_INVOCATION,
                &pb::AttachInvocationCommandMessage {
                    invocation_id: hex(&inv_id(1)),
                    result_completion_id: 1,
                    name: String::new(),
                },
            )),
        )
        .await?;
    assert_eq!(
        types(&msgs),
        vec![MessageType::ATTACH_INVOCATION_COMPLETION]
    );
    let Message::AttachInvocationCompletion(n) = &msgs[0] else {
        unreachable!()
    };
    let Some(pb::attach_invocation_completion_notification_message::Result::Value(v)) = &n.result
    else {
        panic!("attach to a finished invocation must receive a value")
    };
    assert_eq!(v.content, Bytes::from_static(b"\"done\""));

    // B peeks at itself (running) → void.
    let msgs = engine
        .step(
            &mut b,
            Event::FromSdk(frame(
                MessageType::GET_INVOCATION_OUTPUT,
                &pb::GetInvocationOutputCommandMessage {
                    invocation_id: hex(&inv_id(2)),
                    result_completion_id: 2,
                    name: String::new(),
                },
            )),
        )
        .await?;
    let Message::GetInvocationOutputCompletion(n) = &msgs[0] else {
        unreachable!()
    };
    assert!(matches!(
        n.result,
        Some(pb::get_invocation_output_completion_notification_message::Result::Void(_))
    ));

    // Attach to a nonexistent id → 404.
    let msgs = engine
        .step(
            &mut b,
            Event::FromSdk(frame(
                MessageType::ATTACH_INVOCATION,
                &pb::AttachInvocationCommandMessage {
                    invocation_id: hex(&inv_id(9)),
                    result_completion_id: 3,
                    name: String::new(),
                },
            )),
        )
        .await?;
    let Message::AttachInvocationCompletion(n) = &msgs[0] else {
        unreachable!()
    };
    let Some(pb::attach_invocation_completion_notification_message::Result::Failure(f)) = &n.result
    else {
        panic!("attach to a nonexistent id must fail")
    };
    assert_eq!(f.code, 404);
    Ok(())
}

/// Eager state: the previous invocation's `SetState` shows up in the next
/// invocation's `state_map`; `ClearAllState` really clears; `GetStateKeys`
/// reads the right list.
#[tokio::test]
async fn eager_state_and_clear_all() -> Result<()> {
    let root = fresh_root("eager")?;
    let mut engine = open_engine(&root).await?;

    let (mut a, opening) = start(&mut engine, 1).await?;
    // First invocation: the object has no state yet → empty map, not partial.
    let Message::Start(s) = &opening[0] else {
        panic!("must open with Start")
    };
    assert!(s.state_map.is_empty());
    assert!(!s.partial_state);

    for (k, v) in [("color", "\"red\""), ("size", "\"7\"")] {
        engine
            .step(
                &mut a,
                Event::FromSdk(frame(
                    MessageType::SET_STATE,
                    &pb::SetStateCommandMessage {
                        key: Bytes::copy_from_slice(k.as_bytes()),
                        value: Some(cairn_protocol::value::inline(Bytes::copy_from_slice(
                            v.as_bytes(),
                        ))),
                        name: String::new(),
                    },
                )),
            )
            .await?;
    }

    // The next invocation of the same object sees both keys in Start.
    let (mut b, opening) = start(&mut engine, 2).await?;
    let Message::Start(s) = &opening[0] else {
        panic!()
    };
    assert_eq!(s.state_map.len(), 2);
    assert!(!s.partial_state);

    // GetStateKeys returns exactly the list.
    let msgs = engine
        .step(
            &mut b,
            Event::FromSdk(frame(
                MessageType::GET_STATE_KEYS,
                &pb::GetStateKeysCommandMessage {
                    result_completion_id: 1,
                    name: String::new(),
                },
            )),
        )
        .await?;
    let Message::GetStateKeysCompletion(n) = &msgs[0] else {
        panic!()
    };
    let keys = &n.state_keys.as_ref().unwrap().keys;
    assert_eq!(keys.len(), 2);

    // ClearAllState really clears: the next invocation opens with an empty map.
    engine
        .step(
            &mut b,
            Event::FromSdk(frame(
                MessageType::CLEAR_ALL_STATE,
                &pb::ClearAllStateCommandMessage {
                    name: String::new(),
                },
            )),
        )
        .await?;
    let (_, opening) = start(&mut engine, 3).await?;
    let Message::Start(s) = &opening[0] else {
        panic!()
    };
    assert!(s.state_map.is_empty(), "clearAll must clear all state");
    Ok(())
}

/// Per the protocol, `0xFC00+` is **forwarded as raw bytes and not journaled**.
///
/// This is what `BIDI_STREAM` unlocks: under `REQUEST_RESPONSE` the engine also
/// produces this message but has no way to send it (the response body is
/// already built), so the layer above drops it and says so loudly. What the
/// engine must get right is the same in both modes, and it is two things:
///
/// 1. The payload passes through **intact**, even when it is not valid
///    protobuf — the engine must not parse what it does not understand.
/// 2. It takes **no** place in the journal. Journaling a third party's
///    extension would shift every command after it, and an SDK without that
///    extension would replay out of step at the very next entry.
#[tokio::test]
async fn custom_message_passes_raw_bytes_and_skips_journal() -> Result<()> {
    let root = fresh_root("custom")?;
    let mut engine = open_engine(&root).await?;
    let (mut inv, opening) = start(&mut engine, 9).await?;
    assert_eq!(
        types(&opening),
        vec![MessageType::START, MessageType::INPUT]
    );
    let len_before = engine.journal_entries(&inv.id).await?.len();

    // Deliberate garbage: does not decode as any v1 message.
    let rac = Bytes::from_static(&[0xFF, 0x00, 0x13, 0x37]);
    let custom = Frame::new(MessageType(0xFC01), Flags::NONE, rac.clone());

    let out = engine.step(&mut inv, Event::FromSdk(custom)).await?;
    assert_eq!(
        out,
        vec![Message::Custom {
            ty: MessageType(0xFC01),
            payload: rac,
        }],
        "the custom message did not pass through intact"
    );
    assert_eq!(
        engine.journal_entries(&inv.id).await?.len(),
        len_before,
        "the custom message was journaled — every later entry would shift"
    );

    // And the next command still lands in its right place.
    let out = engine
        .step(
            &mut inv,
            Event::FromSdk(frame(
                MessageType::SET_STATE,
                &pb::SetStateCommandMessage {
                    key: Bytes::from_static(b"k"),
                    value: Some(cairn_protocol::value::inline(Bytes::from_static(b"1"))),
                    name: "set".into(),
                },
            )),
        )
        .await?;
    assert!(out.is_empty());
    let journal = engine.journal_entries(&inv.id).await?;
    assert_eq!(journal.len(), len_before + 1);
    assert_eq!(journal[len_before].1, MessageType::SET_STATE.raw());
    Ok(())
}

/// **Regression.** Two sources produce a notification for the same completion id.
///
/// The state machine replays `Task::PromiseGet` for every completion still
/// pending after replay (`machine.rs`, `replay_task_for`) — the recovery path
/// after failover. If the promise got its result *while* the invocation was
/// suspended, that replay produces a notification, while the outbox row
/// `complete_promise_external` queued for the waiter is still there.
/// Delivering that row too is a **second** notification for the same
/// completion id — forbidden by the service protocol, and the invocation dies
/// with `571`.
///
/// This window is real: anything that wakes the invocation before the outbox
/// row is drained (a timer firing, another `ctx.call` returning) opens it.
#[tokio::test]
async fn promise_replay_must_not_give_completion_two_notifications() -> Result<()> {
    let root = fresh_root("promise-replay-dup")?;
    let mut engine = open_engine(&root).await?;

    // A: waits for a promise (completion 1) AND a `ctx.call` not yet back
    // (completion 2). The second is what will wake A before the outbox row is
    // drained.
    let (mut a, _) = start(&mut engine, 1).await?;
    engine
        .step(
            &mut a,
            Event::FromSdk(frame(
                MessageType::GET_PROMISE,
                &pb::GetPromiseCommandMessage {
                    key: "approve".into(),
                    result_completion_id: 1,
                    name: String::new(),
                },
            )),
        )
        .await?;
    engine
        .step(
            &mut a,
            Event::FromSdk(frame(
                MessageType::SLEEP,
                &pb::SleepCommandMessage {
                    wake_up_time: 1,
                    result_completion_id: 2,
                    name: "deadline".into(),
                },
            )),
        )
        .await?;

    // The outside system approves while A is suspended → the result is queued in the outbox for A.
    let outcome = engine
        .complete_promise_external(
            &cairn_store::TenantId::default(),
            None,
            SERVICE,
            OBJECT,
            "approve",
            ReplyResult::Value(Bytes::from_static(b"\"ok\"")),
        )
        .await?;
    assert!(matches!(
        outcome,
        cairn_engine::PromiseOutcome::Completed { woken: 1 }
    ));

    let pending = engine.pending_outbox(16).await?;
    assert_eq!(pending.len(), 1, "one waiter = one outbox row");
    let (outbox_row, _) = pending[0];

    // The timer fires BEFORE the outbox row is drained. `load` replays the
    // journal, sees completion 1 still pending, replays `Task::PromiseGet` —
    // and the promise now has a result.
    let mut a2 = engine.load(a.id, StartParams::default()).await?;
    let replay = engine.step(&mut a2, Event::Start).await?;
    let msgs = engine
        .deliver(
            &mut a2,
            Completion {
                id: 2,
                result: CompletionResult::Timer,
            },
            cairn_engine::Source::Timer { due_ms: 1 },
        )
        .await?;
    let produced: Vec<MessageType> = types(&replay).into_iter().chain(types(&msgs)).collect();
    assert!(
        produced
            .iter()
            .filter(|t| **t == MessageType::GET_PROMISE_COMPLETION)
            .count()
            >= 1,
        "the task replay produced a notification for completion 1: {produced:?}"
    );

    // The outbox row is still there, and delivering it is a second
    // notification for completion 1. This is where it must NOT blow up.
    // The next turn — exactly the server's `drive` path: load, replay, and
    // only then feed what woke it.
    let mut a3 = engine.load(a.id, StartParams::default()).await?;
    engine.step(&mut a3, Event::Start).await?;
    assert!(
        a3.settled_completions().contains(&1),
        "completion 1 must be settled after the replay"
    );
    let msgs = engine
        .deliver(
            &mut a3,
            Completion {
                id: 1,
                result: CompletionResult::CallValue(Bytes::from_static(b"\"ok\"")),
            },
            cairn_engine::Source::Outbox { row: outbox_row },
        )
        .await?;
    assert!(
        !msgs.iter().any(|m| matches!(m, Message::Error(_))),
        "a duplicate delivery must be a no-op, not 571: {:?}",
        types(&msgs)
    );
    // The real proof is in the durable status: a protocol violation ends the
    // invocation with `Failed{571}`, not with a message sent down to the SDK.
    let status = engine.status(&a.id).await?;
    assert!(
        !matches!(
            status,
            Some(cairn_engine::InvocationStatus::Failed { code: 571, .. })
        ),
        "the invocation died with 571 on a second notification for completion 1: {status:?}"
    );
    Ok(())
}

/// A turn in flight must not overwrite a status that is already terminal.
///
/// This test calls `Engine::step` directly, i.e. **without** the invocation
/// lock that `turn::cancel` holds — so it checks exactly what it means to
/// check: the engine layer's invariant, independent of whether the layer above
/// locks correctly. Through the server's paths today this ordering does not
/// happen; the safety net exists for the next call site.
///
/// The most expensive loss without the net is not a wrong `status` number:
/// `cancel` has queued a **failure** for the caller through the outbox, keyed
/// by `(waiter, completion)`, so a successful `Reply` overwrites it — the
/// caller gets the result of a cancelled invocation and carries on as if
/// nothing happened.
#[tokio::test]
async fn in_flight_turn_must_not_overwrite_cancelled_status() -> Result<()> {
    let root = fresh_root("finish-after-cancel")?;
    let mut engine = open_engine(&root).await?;

    // An invocation spawned by `ctx.call`: a caller is waiting for the result.
    let caller = inv_id(9);
    let reply_to = cairn_engine::ReplyTo {
        caller,
        completion_id: 7,
    };
    let id = inv_id(1);
    let mut inv = engine
        .create(
            id,
            &target(),
            CreateOptions {
                reply_to: Some(&reply_to),
                ..CreateOptions::default()
            },
            StartParams::default(),
            Bytes::new(),
        )
        .await?;
    engine.step(&mut inv, Event::Start).await?;

    // Cancel while `inv` (the turn in flight) is still in our hands.
    let outcome = engine.cancel(&id, "cancelled by the operator").await?;
    assert!(matches!(outcome, cairn_engine::CancelOutcome::Cancelled));
    let after_cancel = engine.pending_outbox(16).await?;
    assert_eq!(
        after_cancel.len(),
        1,
        "cancel must queue a failure for the caller"
    );

    // The turn reaches its end with a successful Output — late.
    engine
        .step(
            &mut inv,
            Event::FromSdk(frame(
                MessageType::OUTPUT,
                &pb::OutputCommandMessage {
                    result: Some(pb::output_command_message::Result::Value(
                        cairn_protocol::value::inline(Bytes::from_static(b"\"too late\"")),
                    )),
                    name: String::new(),
                },
            )),
        )
        .await?;
    engine
        .step(
            &mut inv,
            Event::FromSdk(frame(MessageType::END, &pb::EndMessage {})),
        )
        .await?;

    // The durable status stays cancelled.
    match engine.status(&id).await? {
        Some(cairn_engine::InvocationStatus::Failed { code, .. }) => {
            assert_eq!(code, cairn_engine::CANCELLED, "must be 572 cancelled");
        }
        other => panic!("the status was overwritten by the late turn: {other:?}"),
    }

    // And the caller still gets a **failure**, not a value.
    let pending = engine.pending_outbox(16).await?;
    assert_eq!(pending.len(), 1, "no extra caller row allowed");
    let OutboxEntry::Reply(r) = &pending[0].1 else {
        panic!("must be Reply, got {:?}", pending[0].1)
    };
    assert_eq!(r.caller, caller);
    assert!(
        matches!(r.result, ReplyResult::Failure { code, .. } if code == cairn_engine::CANCELLED),
        "the caller got the result of a cancelled invocation: {:?}",
        r.result
    );
    Ok(())
}

// ───────────────── eager state loading stops scanning at the cap

/// Writes `(key, value)` directly into the state of object [`target`].
async fn seed_state(
    engine: &mut Engine,
    rows: impl Iterator<Item = (String, Vec<u8>)>,
) -> Result<()> {
    let t = target();
    let owner = t.object_owner().expect("keyed object");
    let mut muts = cairn_store::Mutations::new();
    for (k, v) in rows {
        muts.set_state(
            owner,
            &t.tenant,
            t.scope(),
            &t.service,
            &t.object_key,
            k.as_bytes(),
            &v,
        );
    }
    engine.writer_mut().commit(muts).await?;
    Ok(())
}

/// The number of keys the writer's scans have read (every zone).
fn keys_read(engine: &Engine) -> u64 {
    let c = engine.writer().scan_counts();
    c.hot + c.derived + c.bulk
}

/// Creates invocation `n` on `t` and opens its first turn: `(Start, keys scanned)`.
async fn opening_reads(engine: &mut Engine, n: u8, t: &Target) -> Result<(pb::StartMessage, u64)> {
    let before = keys_read(engine);
    let mut inv = engine
        .create(
            inv_id(n),
            t,
            CreateOptions::default(),
            StartParams::default(),
            Bytes::new(),
        )
        .await?;
    let opening = engine.step(&mut inv, Event::Start).await?;
    let read = keys_read(engine) - before;
    let Some(Message::Start(s)) = opening.into_iter().next() else {
        panic!("must open with Start")
    };
    Ok((s, read))
}

/// The number of **state** keys the opening turn of invocation `n` on
/// [`target`] reads: minus what an identical opening turn on an object without
/// state reads (the journal, the side rows of
/// `create`).
async fn state_reads(engine: &mut Engine, n: u8) -> Result<(pb::StartMessage, u64)> {
    let vacant = Target {
        object_key: "vacant".into(),
        ..target()
    };
    let (_, base) = opening_reads(engine, n + 100, &vacant).await?;
    let (s, read) = opening_reads(engine, n, &target()).await?;
    Ok((s, read.saturating_sub(base)))
}

/// **An object with 100 000 keys: the opening turn reads ≤ cap + 1 state
/// rows.** Before the fix `with_eager_state` read **every** key of the object
/// into RAM and only then cut at 64 keys / 256 KiB — every turn of that object
/// paid 100 000 rows to send 64.
///
/// Must see: `Start` carries exactly the first 64 keys in byte order,
/// `partial_state`; the opening turn reads ≤ 64 + 1 state keys. On the old
/// code: 100 000.
#[tokio::test]
async fn eager_state_stops_scanning_at_the_entry_cap() -> Result<()> {
    const KEYS: usize = 100_000;
    const MAX_ENTRIES: usize = 64;
    let root = fresh_root("eager-cap-entries")?;
    let mut engine = open_engine(&root).await?;
    seed_state(
        &mut engine,
        (0..KEYS).map(|i| (format!("k{i:06}"), b"1".to_vec())),
    )
    .await?;

    let (s, read) = state_reads(&mut engine, 1).await?;
    let keys: Vec<String> = s
        .state_map
        .iter()
        .map(|e| String::from_utf8_lossy(&e.key).into_owned())
        .collect();
    let want: Vec<String> = (0..MAX_ENTRIES).map(|i| format!("k{i:06}")).collect();
    assert_eq!(keys, want, "the first 64 keys in byte order");
    assert!(s.partial_state, "object over the cap ⇒ partial_state");
    assert!(
        read <= (MAX_ENTRIES + 1) as u64,
        "the opening turn read {read} state keys (cap {MAX_ENTRIES} + 1)"
    );
    Ok(())
}

/// **The byte cap stops the scan too**: five 100 KiB keys, a 256 KiB cap ⇒
/// `Start` carries two keys, `partial_state`, and the scan stops at the third
/// key (the one that goes over the cap), not reading the two after it. On the
/// old code: 5.
#[tokio::test]
async fn eager_state_stops_scanning_at_the_byte_cap() -> Result<()> {
    let root = fresh_root("eager-cap-bytes")?;
    let mut engine = open_engine(&root).await?;
    seed_state(
        &mut engine,
        (0..5).map(|i| (format!("big{i}"), vec![b'x'; 100 * 1024])),
    )
    .await?;
    let (s, read) = state_reads(&mut engine, 1).await?;
    assert_eq!(s.state_map.len(), 2, "two 100 KiB keys fit the 256 KiB cap");
    assert!(s.partial_state);
    assert!(read <= 3, "opening read {read} keys (want ≤ 3)");
    Ok(())
}
