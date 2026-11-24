// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The durable status tells which invocations **have work to run**.
//!
//! `recover` (on every leader change — split, merge, failover) used to hand an
//! SDK turn to every `suspended` invocation, even one suspended on a one-hour
//! timer — because the durable status did not distinguish "suspended, nothing
//! has woken it" from "suspended, the completion is already in the journal but
//! the turn has not run yet". Once they are distinguished, `recover` only reruns
//! the latter: feeding a completion/signal into a `suspended` invocation must
//! move it to `running` **in the same commit** that writes the
//! notification.

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
    cairn_engine::{CreateOptions, Engine, InvocationStatus, Source, Target},
    cairn_protocol::{encode_message, pb, Flags, Frame, FrameDecoder, MessageType},
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

const WAKE_AT: u64 = 4_000_000_000_000;

fn target() -> Target {
    Target {
        tenant: Default::default(),
        scope: None,
        service: "Svc".into(),
        handler: "run".into(),
        object_key: String::new(),
    }
}

fn frame<M: prost::Message>(ty: MessageType, m: &M) -> Frame {
    let wire = encode_message(ty, Flags::NONE, m);
    let mut buf = bytes::BytesMut::from(&wire[..]);
    FrameDecoder::new().decode(&mut buf).unwrap().unwrap()
}

fn fresh_root(name: &str) -> Result<PathBuf> {
    let root =
        std::env::temp_dir().join(format!("cairnflow-runnable-{name}-{}", std::process::id()));
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

/// An invocation that sleeps for an hour and then suspends: `sleep(1)` → `Suspension([1])`.
async fn suspended_on_timer(engine: &mut Engine, n: u8) -> Result<[u8; 24]> {
    let id = make_invocation_id(PartitionKey(0), [n; 16]);
    let mut inv = engine
        .create(
            id,
            &target(),
            CreateOptions::default(),
            StartParams::default(),
            Bytes::new(),
        )
        .await?;
    engine.step(&mut inv, Event::Start).await?;
    engine
        .step(
            &mut inv,
            Event::FromSdk(frame(
                MessageType::SLEEP,
                &pb::SleepCommandMessage {
                    wake_up_time: WAKE_AT,
                    result_completion_id: 1,
                    name: String::new(),
                },
            )),
        )
        .await?;
    engine
        .step(
            &mut inv,
            Event::FromSdk(frame(
                MessageType::SUSPENSION,
                &pb::SuspensionMessage {
                    waiting_completions: vec![1],
                    ..Default::default()
                },
            )),
        )
        .await?;
    Ok(id)
}

/// A timer fires for a suspended invocation ⇒ the durable status is `running`
/// right in the commit that writes the notification. Suspended with nothing to
/// wake it, it is still
/// `suspended`.
#[tokio::test]
async fn delivering_to_a_suspended_invocation_marks_it_running() -> Result<()> {
    let root = fresh_root("deliver")?;
    let mut engine = open_engine(&root).await?;
    let woken = suspended_on_timer(&mut engine, 1).await?;
    let waiting = suspended_on_timer(&mut engine, 2).await?;
    assert_eq!(
        engine.status(&woken).await?,
        Some(InvocationStatus::Suspended)
    );

    // Exactly the server's path (`settle_completion`): reload, replay, deliver.
    let mut inv = engine.load(woken, StartParams::default()).await?;
    engine.step(&mut inv, Event::Start).await?;
    engine
        .deliver(
            &mut inv,
            Completion {
                id: 1,
                result: CompletionResult::Timer,
            },
            Source::Timer { due_ms: WAKE_AT },
        )
        .await?;

    assert_eq!(
        engine.status(&woken).await?,
        Some(InvocationStatus::Running),
        "the completion is already in the journal — the invocation has work to run"
    );
    assert_eq!(
        engine.status(&waiting).await?,
        Some(InvocationStatus::Suspended),
        "with nothing to wake it, it stays suspended"
    );

    // The status index — what `recover` reads — sees exactly the same.
    assert_eq!(engine.runnable_invocations_unbounded().await?, vec![woken]);
    let mut live = engine.unfinished_invocations_unbounded().await?;
    live.sort();
    assert_eq!(live, vec![woken, waiting]);
    Ok(())
}

/// The SDK suspends waiting for something **already** in the journal — written
/// in this very turn (a `ctx.run` result just proposed, a lazy `ctx.get`). The
/// turn will rerun right away (the server's `settle_turn`), so the durable
/// status must be `running`: writing `suspended` would let a leader change
/// exactly between the two turns leave the invocation lying still forever —
/// `recover` only reruns `running`, and nothing else would wake it. (Caught by
/// `durability.test.ts`: killing the runtime on the third turn.)
#[tokio::test]
async fn suspension_already_satisfied_is_stored_as_running() -> Result<()> {
    let root = fresh_root("satisfied")?;
    let mut engine = open_engine(&root).await?;
    let id = make_invocation_id(PartitionKey(0), [4; 16]);
    let mut inv = engine
        .create(
            id,
            &target(),
            CreateOptions::default(),
            StartParams::default(),
            Bytes::new(),
        )
        .await?;
    engine.step(&mut inv, Event::Start).await?;
    for f in [
        frame(
            MessageType::RUN,
            &pb::RunCommandMessage {
                result_completion_id: 1,
                name: "step-1".into(),
            },
        ),
        frame(
            MessageType::PROPOSE_RUN_COMPLETION,
            &pb::ProposeRunCompletionMessage {
                result_completion_id: 1,
                result: Some(pb::propose_run_completion_message::Result::Value(
                    Bytes::from_static(b"42"),
                )),
            },
        ),
        frame(
            MessageType::SUSPENSION,
            &pb::SuspensionMessage {
                waiting_completions: vec![1],
                ..Default::default()
            },
        ),
    ] {
        engine.step(&mut inv, Event::FromSdk(f)).await?;
    }
    assert!(
        inv.settled_completions().contains(&1),
        "the run result must already be in the journal"
    );
    assert_eq!(engine.status(&id).await?, Some(InvocationStatus::Running));
    assert_eq!(engine.runnable_invocations_unbounded().await?, vec![id]);

    // Control: suspended waiting for a timer that has not fired is still `suspended`.
    let waiting = suspended_on_timer(&mut engine, 5).await?;
    assert_eq!(
        engine.status(&waiting).await?,
        Some(InvocationStatus::Suspended)
    );
    Ok(())
}
