// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Billing units at the engine layer: **counted at commit, only when durable**,
//! once per entry. Runs on the store model (`RangeModel`) — a death between
//! submitting and durability can be built deterministically
//! (`RangeBucket::crash`).
//!
//! Read with `cairn_store::units::collect_tenant` (one tenant per test: tests
//! run in parallel in one process, sharing the process's ledger).

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

use {
    bytes::{Bytes, BytesMut},
    cairn_engine::{CreateOptions, Engine, Target},
    cairn_protocol::{encode_message, pb, Flags, Frame, FrameDecoder, MessageType},
    cairn_runtime::{Event, StartParams},
    cairn_store::{
        clock::ManualClock, make_invocation_id, testkit::RangeBucket, InvocationId,
        LeadershipGrant, PartitionId, PartitionKey, PartitionWriter, TenantId,
    },
    std::sync::Arc,
};

const T0: u64 = 1_700_000_000_000;
const KIB: usize = 1024;

type Eng = Engine<cairn_store::testkit::RangeModel>;

fn frame<M: prost::Message>(ty: MessageType, m: &M) -> Frame {
    let wire = encode_message(ty, Flags::NONE, m);
    FrameDecoder::new()
        .decode(&mut BytesMut::from(&wire[..]))
        .unwrap()
        .unwrap()
}

fn run(cid: u32) -> Frame {
    frame(
        MessageType::RUN,
        &pb::RunCommandMessage {
            result_completion_id: cid,
            name: String::new(),
        },
    )
}

fn propose(cid: u32, n: usize) -> Frame {
    frame(
        MessageType::PROPOSE_RUN_COMPLETION,
        &pb::ProposeRunCompletionMessage {
            result_completion_id: cid,
            result: Some(pb::propose_run_completion_message::Result::Value(
                Bytes::from(vec![b'x'; n]),
            )),
        },
    )
}

fn output(n: usize) -> Frame {
    frame(
        MessageType::OUTPUT,
        &pb::OutputCommandMessage {
            result: Some(pb::output_command_message::Result::Value(
                cairn_protocol::value::inline(Bytes::from(vec![b'y'; n])),
            )),
            name: String::new(),
        },
    )
}

fn end() -> Frame {
    frame(MessageType::END, &pb::EndMessage {})
}

async fn open(bucket: &RangeBucket) -> Eng {
    let w = PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), bucket.open())
        .await
        .unwrap();
    Engine::open(w)
        .await
        .unwrap()
        .with_clock(Arc::new(ManualClock::new(T0)))
}

fn target(tenant: &str) -> Target {
    Target {
        tenant: TenantId::new(tenant).unwrap(),
        scope: None,
        service: "Svc".into(),
        handler: "h".into(),
        object_key: String::new(),
    }
}

fn fresh_id() -> InvocationId {
    let mut seed = [0u8; 16];
    cairn_store::entropy::fill(&mut seed);
    make_invocation_id(PartitionKey(0), seed)
}

/// The durable units of `tenant` (drained).
fn units(tenant: &str) -> u64 {
    cairn_store::units::collect_tenant(tenant).values().sum()
}

/// **A death between submitting and durability counts nothing; redoing counts
/// exactly once.**
///
/// 1. Create an invocation: submitted, not durable ⇒ 0; a WAL flush tick ⇒ 1.
/// 2. `Start`, `Run` + a 100 KiB proposal — submitted, **not** durable (nobody
///    waits) ⇒ 0. The process dies, the non-durable WAL tail is lost.
/// 3. Reopen, load the invocation, redo `Run` + the proposal, `Output` 64 KiB +
///    1, `End`; a WAL flush tick.
///
/// Must see: after 2 nothing more (even when the range closes); after 3 two
/// more (the 100 KiB `run`) + 1 (the excess of `Output`) + 1 (the end) — `run`
/// counted exactly once although the dead life had submitted it. Replaying the
/// journal afterwards adds nothing.
#[tokio::test]
async fn unflushed_batches_lost_in_a_crash_are_counted_once_on_redo() {
    let t = "units-crash";
    let bucket = RangeBucket::new();
    let id = fresh_id();
    {
        let mut e = open(&bucket).await;
        let mut inv = e
            .create(
                id,
                &target(t),
                CreateOptions::default(),
                StartParams::default(),
                Bytes::from_static(b"{}"),
            )
            .await
            .unwrap();
        assert_eq!(
            units(t),
            0,
            "create submitted, not durable ⇒ not counted yet"
        );
        bucket.flush();
        assert_eq!(units(t), 1, "create durable");
        e.step(&mut inv, Event::Start).await.unwrap();
        e.step(&mut inv, Event::FromSdk(run(1))).await.unwrap();
        e.step(&mut inv, Event::FromSdk(propose(1, 100 * KIB)))
            .await
            .unwrap();
        assert_eq!(units(t), 0, "run submitted, not durable ⇒ not counted yet");
        assert_eq!(
            e.unit_meter().pending(),
            2,
            "run 100 KiB waiting for durability"
        );
        bucket.crash(0);
        assert_eq!(units(t), 0, "the non-durable tail is lost with the process");
    }
    assert_eq!(
        units(t),
        0,
        "closing the range leaves nothing of the lost tail"
    );
    let mut e = open(&bucket).await;
    let mut inv = e.load(id, StartParams::default()).await.unwrap();
    e.step(&mut inv, Event::Start).await.unwrap();
    e.step(&mut inv, Event::FromSdk(run(1))).await.unwrap();
    e.step(&mut inv, Event::FromSdk(propose(1, 100 * KIB)))
        .await
        .unwrap();
    e.step(&mut inv, Event::FromSdk(output(64 * KIB + 1)))
        .await
        .unwrap();
    e.step(&mut inv, Event::FromSdk(end())).await.unwrap();
    bucket.flush();
    assert_eq!(units(t), 2 + 1 + 1);
    // Reload and replay the journal (a new turn, a leader change): no entry is written.
    let mut again = e.load(id, StartParams::default()).await.unwrap();
    e.step(&mut again, Event::Start).await.unwrap();
    bucket.flush();
    assert_eq!(units(t), 0, "replay does not count");
}

/// **Cancel** (an end without an `Output`): 1 create + 1 end, written at the
/// status commit — not through any entry.
#[tokio::test]
async fn cancel_counts_the_end_without_an_output_entry() {
    let t = "units-cancel";
    let bucket = RangeBucket::new();
    let mut e = open(&bucket).await;
    let id = fresh_id();
    let mut inv = e
        .create(
            id,
            &target(t),
            CreateOptions::default(),
            StartParams::default(),
            Bytes::from_static(b"{}"),
        )
        .await
        .unwrap();
    e.step(&mut inv, Event::Start).await.unwrap();
    e.cancel(&id, "cancel").await.unwrap();
    // Cancelling an invocation that has already ended: nothing written, nothing counted.
    e.cancel(&id, "cancel again").await.unwrap();
    bucket.flush();
    assert_eq!(units(t), 2);
}

/// **A new artifact counts by bytes, once per `(id, sha)`.**
///
/// A 64 KiB + 1 byte row ⇒ 2; writing exactly that row again (a retry PUTting
/// the same artifact again) ⇒ 0; a 0-byte row with another sha ⇒ 1.
#[tokio::test]
async fn new_artifact_rows_count_by_size_once() {
    let t = "units-artifact";
    let bucket = RangeBucket::new();
    let mut e = open(&bucket).await;
    let id = fresh_id();
    let mut inv = e
        .create(
            id,
            &target(t),
            CreateOptions::default(),
            StartParams::default(),
            Bytes::from_static(b"{}"),
        )
        .await
        .unwrap();
    e.step(&mut inv, Event::Start).await.unwrap();
    bucket.flush();
    assert_eq!(units(t), 1, "create");
    let row = |size: u64| cairn_store::keys::ArtifactRow {
        epoch: 7,
        size,
        content_type: "application/octet-stream".into(),
    };
    for (sha, size) in [
        ([1u8; 32], 64 * 1024 + 1),
        ([1u8; 32], 64 * 1024 + 1),
        ([2u8; 32], 0),
    ] {
        let mut muts = cairn_store::Mutations::new();
        e.stage_artifact_put(&mut muts, &id, &sha, &row(size))
            .await
            .unwrap();
        e.writer_mut().commit(muts).await.unwrap();
    }
    bucket.flush();
    assert_eq!(units(t), 2 + 1);
}
