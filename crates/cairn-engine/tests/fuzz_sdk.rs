// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Fuzzes the engine's effect-application path with arbitrary SDK messages.
//!
//! The layer after `cairn-runtime`: values the tenant writes (the wake time of a
//! `Sleep`, the run time of a `OneWayCall`, the service/handler/key names of a
//! `Call`, the target id of a `SendSignal`/`AttachInvocation`, state keys,
//! promise names, tags) go into table keys, timer rows, the outbox and indexes,
//! and are then committed. The engine runs on the store model (`RangeModel`,
//! the same contract as SlateDB, `tests/range_model.rs`) with a manual clock,
//! so one case costs only a few ms.
//!
//! Each case: create an invocation (with or without an object key, random
//! tenant limits — retention, payload, journal), `Start`, push a sequence of
//! SDK frames — well-typed commands with edge values, mixed with
//! near-protobuf frames —, then jump the clock to the end and fire every due
//! timer.
//!
//! Must see: no panic. An `Err` from `step` is a named error the shell
//! (`turn/`) handles; any case returning an `Err` not caused by the store is
//! printed for inspection
//! (`CAIRNFLOW_FUZZ_VERBOSE=1`).

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterministic sources"
)]

#[path = "../../cairn-protocol/tests/fuzz_gen/mod.rs"]
mod fuzz_gen;

use {
    bytes::{Bytes, BytesMut},
    cairn_engine::{CreateOptions, Engine, Target},
    cairn_protocol::{encode_frame, encode_message, pb, Flags, Frame, FrameDecoder, MessageType},
    cairn_runtime::{Event, StartParams},
    cairn_store::{
        clock::ManualClock, make_invocation_id, testkit::RangeBucket, LeadershipGrant, PartitionId,
        PartitionKey, PartitionWriter, TenantId,
    },
    fuzz_gen::{budget, message_type, number, payload},
    proptest::prelude::*,
    std::sync::Arc,
};

const T0: u64 = 1_700_000_000_000;

fn frame<M: prost::Message>(ty: MessageType, flags: Flags, m: &M) -> Frame {
    let wire = encode_message(ty, flags, m);
    FrameDecoder::new()
        .decode(&mut BytesMut::from(&wire[..]))
        .unwrap()
        .unwrap()
}

/// Tenant-written strings: empty, ASCII, Unicode, control characters, `/`, long.
fn text() -> impl Strategy<Value = String> {
    prop_oneof![
        Just(String::new()),
        "[a-z]{1,6}",
        ".{0,12}",
        "[\\x00-\\x1f/\\\\%]{1,6}",
        "[a-z]{200,300}",
    ]
}

/// Timestamps: past, near, far, `u64` edges.
fn time_ms() -> impl Strategy<Value = u64> {
    prop_oneof![
        Just(0u64),
        Just(u64::MAX),
        Just(i64::MAX as u64),
        (T0 - 1000)..(T0 + 100_000),
        any::<u64>(),
    ]
}

/// Completion ids: mostly new (few duplicates, so commands go deep to the
/// commit), sometimes small numbers or edges (to enter the duplicate/missing
/// branches).
fn small() -> impl Strategy<Value = u32> {
    prop_oneof![4 => 1u32..100_000, 1 => number().prop_map(|n| n as u32)]
}

fn header() -> impl Strategy<Value = pb::Header> {
    (text(), text()).prop_map(|(key, value)| pb::Header { key, value })
}

fn bytes() -> impl Strategy<Value = Bytes> {
    prop::collection::vec(any::<u8>(), 0..32).prop_map(Bytes::from)
}

fn value() -> impl Strategy<Value = Option<pb::Value>> {
    prop::option::of(bytes().prop_map(cairn_protocol::value::inline))
}

/// Well-typed SDK commands, each field carrying an edge value.
fn single() -> impl Strategy<Value = Frame> {
    let ack = prop_oneof![Just(Flags::NONE), Just(Flags::REQUESTED_ACK)];
    prop_oneof![
        (time_ms(), small(), text(), ack.clone()).prop_map(|(t, id, name, f)| frame(
            MessageType::SLEEP,
            f,
            &pb::SleepCommandMessage {
                wake_up_time: t,
                result_completion_id: id,
                name
            },
        )),
        (
            text(),
            text(),
            bytes(),
            text(),
            prop::option::of(text()),
            prop::option::of(text()),
            prop::option::of(text()),
            small(),
            small(),
            prop::collection::vec(header(), 0..3)
        )
            .prop_map(|(s, h, p, k, idem, scope, limit, idc, rc, tags)| frame(
                MessageType::CALL,
                Flags::NONE,
                &pb::CallCommandMessage {
                    service_name: s,
                    handler_name: h,
                    parameter: p,
                    key: k,
                    idempotency_key: idem,
                    scope,
                    limit_key: limit,
                    invocation_id_notification_idx: idc,
                    result_completion_id: rc,
                    tags,
                    ..Default::default()
                },
            )),
        (
            text(),
            text(),
            time_ms(),
            text(),
            prop::option::of(text()),
            small()
        )
            .prop_map(|(s, h, t, k, idem, idc)| frame(
                MessageType::ONE_WAY_CALL,
                Flags::NONE,
                &pb::OneWayCallCommandMessage {
                    service_name: s,
                    handler_name: h,
                    invoke_time: t,
                    key: k,
                    idempotency_key: idem,
                    invocation_id_notification_idx: idc,
                    ..Default::default()
                },
            )),
        (bytes(), small()).prop_map(|(key, id)| frame(
            MessageType::GET_STATE,
            Flags::NONE,
            &pb::GetStateCommandMessage {
                key,
                result_completion_id: id,
                name: String::new()
            },
        )),
        (bytes(), value()).prop_map(|(key, value)| frame(
            MessageType::SET_STATE,
            Flags::NONE,
            &pb::SetStateCommandMessage {
                key,
                value,
                name: String::new()
            },
        )),
        bytes().prop_map(|key| frame(
            MessageType::CLEAR_STATE,
            Flags::NONE,
            &pb::ClearStateCommandMessage {
                key,
                name: String::new()
            },
        )),
        small().prop_map(|id| frame(
            MessageType::GET_STATE_KEYS,
            Flags::NONE,
            &pb::GetStateKeysCommandMessage {
                result_completion_id: id,
                name: String::new()
            },
        )),
        (text(), small()).prop_map(|(key, id)| frame(
            MessageType::GET_PROMISE,
            Flags::NONE,
            &pb::GetPromiseCommandMessage {
                key,
                result_completion_id: id,
                ..Default::default()
            },
        )),
        (text(), small()).prop_map(|(key, id)| frame(
            MessageType::PEEK_PROMISE,
            Flags::NONE,
            &pb::PeekPromiseCommandMessage {
                key,
                result_completion_id: id,
                ..Default::default()
            },
        )),
        (text(), small(), bytes()).prop_map(|(key, id, v)| frame(
            MessageType::COMPLETE_PROMISE,
            Flags::NONE,
            &pb::CompletePromiseCommandMessage {
                key,
                result_completion_id: id,
                completion: Some(
                    pb::complete_promise_command_message::Completion::CompletionValue(
                        cairn_protocol::value::inline(v)
                    ),
                ),
                name: String::new(),
            },
        )),
        (small(), bytes()).prop_map(|(id, v)| frame(
            MessageType::PROPOSE_RUN_COMPLETION,
            Flags::REQUESTED_ACK,
            &pb::ProposeRunCompletionMessage {
                result_completion_id: id,
                result: Some(pb::propose_run_completion_message::Result::Value(v)),
            },
        )),
        (text(), small()).prop_map(|(target, id)| frame(
            MessageType::ATTACH_INVOCATION,
            Flags::NONE,
            &pb::AttachInvocationCommandMessage {
                invocation_id: target,
                result_completion_id: id,
                name: String::new(),
            },
        )),
        (text(), small()).prop_map(|(target, id)| frame(
            MessageType::GET_INVOCATION_OUTPUT,
            Flags::NONE,
            &pb::GetInvocationOutputCommandMessage {
                invocation_id: target,
                result_completion_id: id,
                name: String::new(),
            },
        )),
        text().prop_map(|target| frame(
            MessageType::SEND_SIGNAL,
            Flags::NONE,
            &pb::SendSignalCommandMessage {
                target_invocation_id: target,
                ..Default::default()
            },
        )),
        (value(), text()).prop_map(|(v, name)| frame(
            MessageType::OUTPUT,
            Flags::NONE,
            &pb::OutputCommandMessage {
                result: v.map(pb::output_command_message::Result::Value),
                name,
            },
        )),
        (
            prop::collection::vec(small(), 0..3),
            prop::collection::vec(small(), 0..2),
            prop::collection::vec(text(), 0..2)
        )
            .prop_map(|(c, s, n)| frame(
                MessageType::SUSPENSION,
                Flags::NONE,
                &pb::SuspensionMessage {
                    waiting_completions: c,
                    waiting_signals: s,
                    waiting_named_signals: n,
                    nested_futures: vec![],
                    ..Default::default()
                },
            )),
        (any::<u32>(), text(), prop::option::of(any::<u64>())).prop_map(|(code, message, d)| {
            frame(
                MessageType::ERROR,
                Flags::NONE,
                &pb::ErrorMessage {
                    code,
                    message,
                    next_retry_delay: d,
                    ..Default::default()
                },
            )
        }),
        Just(frame(MessageType::END, Flags::NONE, &pb::EndMessage {})),
    ]
}

/// `run` with (or without) a proposed result for exactly that id, like a real SDK.
fn run_pair() -> impl Strategy<Value = Vec<Frame>> {
    let ack = prop_oneof![Just(Flags::NONE), Just(Flags::REQUESTED_ACK)];
    (small(), text(), ack.clone(), prop::option::of(bytes())).prop_map(|(id, name, f, v)| {
        let mut out = vec![frame(
            MessageType::RUN,
            f,
            &pb::RunCommandMessage {
                result_completion_id: id,
                name,
            },
        )];
        // Propose the result for exactly the `run` just declared, like a real SDK.
        if let Some(v) = v {
            out.push(frame(
                MessageType::PROPOSE_RUN_COMPLETION,
                Flags::REQUESTED_ACK,
                &pb::ProposeRunCompletionMessage {
                    result_completion_id: id,
                    result: Some(pb::propose_run_completion_message::Result::Value(v)),
                },
            ));
        }
        out
    })
}

fn command() -> impl Strategy<Value = Vec<Frame>> {
    prop_oneof![8 => single().prop_map(|f| vec![f]), 2 => run_pair()]
}

/// Near-protobuf frames with an arbitrary code (the same generator as `cairn-protocol`).
fn raw_frame() -> impl Strategy<Value = Frame> {
    (message_type(), payload()).prop_map(|(ty, p)| {
        let wire = encode_frame(ty, Flags::NONE, &p);
        let mut buf = BytesMut::from(&wire[..]);
        FrameDecoder::with_max_frame_len(u32::MAX)
            .decode(&mut buf)
            .unwrap()
            .unwrap()
    })
}

fn sdk_frames() -> impl Strategy<Value = Vec<Frame>> {
    prop_oneof![9 => command(), 1 => raw_frame().prop_map(|f| vec![f])]
}

fn target(keyed: bool, tenant: bool) -> Target {
    Target {
        tenant: if tenant {
            TenantId::new("acme").unwrap()
        } else {
            TenantId::default()
        },
        scope: None,
        service: "Svc".into(),
        handler: "h".into(),
        object_key: if keyed { "k".into() } else { String::new() },
    }
}

async fn one_case(frames: Vec<Frame>, keyed: bool, tenant: bool, limit: cairn_engine::TenantLimit) {
    let bucket = RangeBucket::new();
    let w = PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), bucket.open())
        .await
        .unwrap();
    let clock = Arc::new(ManualClock::new(T0));
    let mut engine = Engine::open(w)
        .await
        .unwrap()
        .with_clock(clock.clone())
        .with_streams(true)
        .with_retention(Some(60_000))
        // Per-tenant limits for both tenants: the journal-cut and
        // payload-rejection paths of `step`.
        .with_tenant_limits(Arc::new(cairn_engine::TenantLimits::new(
            [TenantId::default(), TenantId::new("acme").unwrap()]
                .into_iter()
                .map(|t| (t, limit))
                .collect(),
        )));
    let id = make_invocation_id(PartitionKey(0), [7; 16]);
    let mut inv = engine
        .create(
            id,
            &target(keyed, tenant),
            CreateOptions::default(),
            StartParams::default(),
            Bytes::from_static(b"{}"),
        )
        .await
        .unwrap();
    let verbose = std::env::var_os("CAIRNFLOW_FUZZ_VERBOSE").is_some();
    let report = |what: &str, e: anyhow::Error| {
        if verbose {
            eprintln!("fuzz_sdk: {what}: {e:#}");
        }
    };
    if let Err(e) = engine.step(&mut inv, Event::Start).await {
        return report("Start", e.into());
    }
    let mut steps = 0;
    for f in frames {
        if inv.outcome().is_some() {
            break;
        }
        steps += 1;
        if let Err(e) = engine.step(&mut inv, Event::FromSdk(f)).await {
            return report("FromSdk", e.into());
        }
    }
    // The clock jumps to the end: every timer (including `u64::MAX`) and purge deadline comes due.
    clock.set_wall_ms(u64::MAX);
    match engine.due_timers(u64::MAX, usize::MAX).await {
        Ok(due) => {
            for (due_ms, who, cid) in due {
                if who == inv.id && inv.outcome().is_none() {
                    if let Err(e) = engine.fire_timer(&mut inv, due_ms, cid).await {
                        return report("fire_timer", e.into());
                    }
                }
            }
        }
        Err(e) => report("due_timers", e.into()),
    }
    if verbose {
        let end = match inv.outcome() {
            None => "open".to_string(),
            Some(cairn_runtime::Outcome::Failed { code, message, .. }) => {
                format!(
                    "failed {code} {}",
                    message.chars().take(60).collect::<String>()
                )
            }
            Some(o) => format!("{o:?}").chars().take(12).collect(),
        };
        eprintln!("fuzz_sdk: case steps={steps} end={end}");
    }
}

proptest! {
    #![proptest_config(budget(512))]

    #[test]
    fn engine_survives_any_sdk_program(
        frames in prop::collection::vec(sdk_frames(), 0..20)
            .prop_map(|v| v.into_iter().flatten().collect::<Vec<_>>()),
        keyed in any::<bool>(),
        tenant in any::<bool>(),
        limit in (
            prop::option::of(0u64..120_000),
            prop::option::of(1u64..64),
            prop::option::of(1u32..12),
        )
            .prop_map(|(retention_ms, max_payload, max_journal)| cairn_engine::TenantLimit {
                retention_ms,
                max_payload,
                max_journal,
                ..Default::default()
            }),
    ) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(one_case(frames, keyed, tenant, limit));
    }
}
