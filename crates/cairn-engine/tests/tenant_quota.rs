// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Per-invocation / per-object / per-tenant resource limits that `Engine::step`
//! enforces: over the limit ⇒ the invocation is `failed` with code 579
//! (`TENANT_LIMIT`), the message starting with the limit's string code, in the
//! very commit of the command that went over. Another tenant (without limits)
//! running the same program is unaffected.
//!
//! Runs on the store model (`RangeModel`) with a manual clock, like `fuzz_sdk.rs`.

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterministic sources"
)]

use {
    bytes::{Bytes, BytesMut},
    cairn_engine::{CreateOptions, Engine, Target, TenantLimit, TenantLimits, TenantUsage},
    cairn_protocol::{encode_message, pb, Flags, Frame, FrameDecoder, MessageType},
    cairn_runtime::{Event, Outcome, StartParams},
    cairn_store::{
        clock::ManualClock, make_invocation_id, testkit::RangeBucket, LeadershipGrant, PartitionId,
        PartitionKey, PartitionWriter, TenantId,
    },
    std::{collections::BTreeMap, sync::Arc},
};

const T0: u64 = 1_700_000_000_000;

type Eng = Engine<cairn_store::testkit::RangeModel>;

fn frame<M: prost::Message>(ty: MessageType, m: &M) -> Frame {
    let wire = encode_message(ty, Flags::NONE, m);
    FrameDecoder::new()
        .decode(&mut BytesMut::from(&wire[..]))
        .unwrap()
        .unwrap()
}

fn sleep(id: u32) -> Frame {
    frame(
        MessageType::SLEEP,
        &pb::SleepCommandMessage {
            wake_up_time: T0 + 3_600_000,
            result_completion_id: id,
            name: String::new(),
        },
    )
}

fn set_state(key: &str) -> Frame {
    frame(
        MessageType::SET_STATE,
        &pb::SetStateCommandMessage {
            key: Bytes::copy_from_slice(key.as_bytes()),
            value: Some(cairn_protocol::value::inline(Bytes::from_static(b"1"))),
            name: String::new(),
        },
    )
}

fn artifact() -> Frame {
    let content = b"x";
    let sha = cairn_store::artifact::sha256(content);
    frame(
        MessageType::ARTIFACT_PUT,
        &pb::ArtifactPutMessage {
            sha256: Bytes::copy_from_slice(&sha),
            content: Bytes::from_static(content),
            content_type: "text/plain".into(),
        },
    )
}

/// A `SetState` frame carrying an `n`-byte value.
fn set_state_sized(key: &str, n: usize) -> Frame {
    frame(
        MessageType::SET_STATE,
        &pb::SetStateCommandMessage {
            key: Bytes::copy_from_slice(key.as_bytes()),
            value: Some(cairn_protocol::value::inline(Bytes::from(vec![b'v'; n]))),
            name: String::new(),
        },
    )
}

/// Runs `frames` for an invocation of `tenant` (target keyed by `key`, or not),
/// on an engine with limit `limit` for `acme` and usage counts `usage`;
/// returns the outcome (if it has ended).
async fn run(
    tenant: &str,
    key: Option<&str>,
    limit: TenantLimit,
    usage: TenantUsage,
    frames: Vec<Frame>,
    // An earlier run on the same engine (so the state already exists).
    engine: Option<Eng>,
) -> (Option<Outcome>, Eng) {
    let (o, e, _) = run_id(tenant, key, limit, usage, frames, engine).await;
    (o, e)
}

/// Like [`run`], with the invocation's id.
async fn run_id(
    tenant: &str,
    key: Option<&str>,
    limit: TenantLimit,
    usage: TenantUsage,
    frames: Vec<Frame>,
    engine: Option<Eng>,
) -> (Option<Outcome>, Eng, cairn_engine::InvocationId) {
    let mut engine = match engine {
        Some(e) => e,
        None => {
            let bucket = RangeBucket::new();
            let w =
                PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), bucket.open())
                    .await
                    .unwrap();
            let acme = TenantId::new("acme").unwrap();
            Engine::open(w)
                .await
                .unwrap()
                .with_clock(Arc::new(ManualClock::new(T0)))
                .with_streams(true)
                .with_tenant_limits(Arc::new(
                    TenantLimits::new(BTreeMap::from([(acme.clone(), limit)]))
                        .with_usage(BTreeMap::from([(acme, usage)])),
                ))
        }
    };
    let target = Target {
        tenant: TenantId::new(tenant).unwrap(),
        scope: None,
        service: "Svc".into(),
        handler: "h".into(),
        object_key: key.unwrap_or_default().into(),
    };
    let mut seed = [0u8; 16];
    cairn_store::entropy::fill(&mut seed);
    let id = make_invocation_id(PartitionKey(0), seed);
    let mut inv = engine
        .create(
            id,
            &target,
            CreateOptions::default(),
            StartParams::default(),
            Bytes::from_static(b"{}"),
        )
        .await
        .unwrap();
    engine.step(&mut inv, Event::Start).await.unwrap();
    for f in frames {
        if inv.outcome().is_some() {
            break;
        }
        engine.step(&mut inv, Event::FromSdk(f)).await.unwrap();
    }
    (inv.outcome().cloned(), engine, id)
}

fn failed_with(o: &Option<Outcome>, code_str: &str) -> bool {
    matches!(o, Some(Outcome::Failed { code, message, .. })
        if *code == cairn_engine::TENANT_LIMIT && message.starts_with(code_str))
}

/// `max-timers=1`: a second timer pending at the same time ⇒ `failed` 579
/// `timers_limit`; one timer is fine; another tenant with two timers is still open.
#[tokio::test]
async fn timers_per_invocation() {
    let limit = TenantLimit {
        max_timers: Some(1),
        ..Default::default()
    };
    let (o, _) = run(
        "acme",
        None,
        limit,
        TenantUsage::default(),
        vec![sleep(1)],
        None,
    )
    .await;
    assert!(o.is_none(), "one timer under the limit: {o:?}");
    let (o, _) = run(
        "acme",
        None,
        limit,
        TenantUsage::default(),
        vec![sleep(1), sleep(2)],
        None,
    )
    .await;
    assert!(failed_with(&o, cairn_engine::TIMERS_LIMIT_CODE), "{o:?}");
    let (o, _) = run(
        "other",
        None,
        limit,
        TenantUsage::default(),
        vec![sleep(1), sleep(2)],
        None,
    )
    .await;
    assert!(o.is_none(), "another tenant without limits: {o:?}");
}

/// `max-state-keys=2`: an object's third key ⇒ `failed` 579
/// `state_keys_limit`; rewriting an existing key does not count; another object
/// of the same tenant has its own quota.
#[tokio::test]
async fn state_keys_per_object() {
    let limit = TenantLimit {
        max_state_keys: Some(2),
        ..Default::default()
    };
    let u = TenantUsage::default();
    let (o, e) = run(
        "acme",
        Some("k"),
        limit,
        u,
        vec![set_state("a"), set_state("b"), set_state("a")],
        None,
    )
    .await;
    assert!(o.is_none(), "two keys, rewriting an old key: {o:?}");
    let (o, e) = run("acme", Some("k"), limit, u, vec![set_state("c")], Some(e)).await;
    assert!(
        failed_with(&o, cairn_engine::STATE_KEYS_LIMIT_CODE),
        "{o:?}"
    );
    let (o, e) = run("acme", Some("k"), limit, u, vec![set_state("b")], Some(e)).await;
    assert!(o.is_none(), "existing key: {o:?}");
    let (o, e) = run(
        "acme",
        Some("k2"),
        limit,
        u,
        vec![set_state("x"), set_state("y")],
        Some(e),
    )
    .await;
    assert!(o.is_none(), "another object: {o:?}");
    // Three new keys within the same turn are counted too.
    let (o, _) = run(
        "acme",
        Some("k3"),
        limit,
        u,
        vec![set_state("p"), set_state("q"), set_state("r")],
        Some(e),
    )
    .await;
    assert!(
        failed_with(&o, cairn_engine::STATE_KEYS_LIMIT_CODE),
        "{o:?}"
    );
}

/// `max-artifact-bytes`: a tenant already at its limit (the background loop's
/// count) ⇒ `ArtifactPut` makes the invocation `failed` 579 `artifacts_limit`;
/// under the limit the frame goes on as usual (in the engine: to the machine).
#[tokio::test]
async fn artifacts_over_tenant_bytes() {
    let limit = TenantLimit {
        max_artifact_bytes: Some(100),
        ..Default::default()
    };
    let over = TenantUsage {
        artifact_bytes: 100,
        ..Default::default()
    };
    let (o, _) = run("acme", None, limit, over, vec![artifact()], None).await;
    assert!(failed_with(&o, cairn_engine::ARTIFACTS_LIMIT_CODE), "{o:?}");
    let under = TenantUsage {
        artifact_bytes: 10,
        ..Default::default()
    };
    let (o, _) = run("acme", None, limit, under, vec![artifact()], None).await;
    assert!(
        !failed_with(&o, cairn_engine::ARTIFACTS_LIMIT_CODE),
        "under the limit: {o:?}"
    );
}

/// `max-journal-bytes=4096`: ten 1 KiB `SetState`s ⇒ the invocation is
/// **`paused`** with code 580 `journal_bytes_limit` (not `failed`: the limit
/// can be raised and then `resume`), and the durable journal does **not**
/// exceed the limit — the entry that would push the total over is not written,
/// so the next load does not read past the limit. Another tenant (without
/// limits) running the same program is still open.
#[tokio::test]
async fn journal_bytes_per_invocation() {
    let max = 4096u64;
    let limit = TenantLimit {
        max_journal_bytes: Some(max),
        ..Default::default()
    };
    let frames = || {
        (0..10)
            .map(|i| set_state_sized(&format!("k{i}"), 1024))
            .collect::<Vec<_>>()
    };
    let (o, e, id) = run_id(
        "acme",
        Some("k"),
        limit,
        TenantUsage::default(),
        frames(),
        None,
    )
    .await;
    assert!(
        matches!(&o, Some(Outcome::Failed { code, message, action })
            if *code == cairn_engine::JOURNAL_BYTES_LIMIT
                && message.starts_with(cairn_engine::JOURNAL_BYTES_LIMIT_CODE)
                && matches!(action, cairn_runtime::FailureAction::Pause)),
        "{o:?}"
    );
    let stored: u64 = e
        .journal_entries(&id)
        .await
        .unwrap()
        .iter()
        .map(|(_, _, payload)| 2 + payload.len() as u64)
        .sum();
    assert!(stored <= max, "durable journal {stored} B > limit {max}");
    assert!(stored > max / 2, "entries below the limit kept: {stored}");
    let (o, _) = run(
        "other",
        Some("k"),
        limit,
        TenantUsage::default(),
        frames(),
        None,
    )
    .await;
    assert!(o.is_none(), "another tenant without limits: {o:?}");
}
