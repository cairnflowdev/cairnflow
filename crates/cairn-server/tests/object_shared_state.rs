// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! A virtual object's `SHARED` handler **cannot write state** (D-W5).
//!
//! A `SHARED` handler runs without holding the object lock (`turn::is_exclusive`), in
//! parallel with an `EXCLUSIVE` handler on the same key. Letting it write opens exactly the
//! lost-update door the `EXCLUSIVE` lock closes: both sides read the old value and both
//! overwrite. Before the fix, only the TS SDK's `SharedContext` type blocked it; the runtime let it through.
//!
//! A one-range runtime (`EngineHandle`, like `lifecycle.rs`) with a scripted fake SDK
//! (`common::fake_sdk`). Where an invocation "created by an old binary" is needed, the test
//! stops the runtime, rewrites entry 0 without the rule mark ([`STATE_LOCK_HEADER`]) with
//! exactly the engine's journal write, then reopens on the same directory — a binary
//! upgrade while the invocation is waiting.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{anyhow, bail, ensure, Result},
    bytes::Bytes,
    cairn_engine::{CompletionPayload, Engine, InvocationStatus, SignalId, Target},
    cairn_protocol::{Flags, Frame, Message, MessageType},
    cairn_server::{
        actor::{EngineHandle, Leadership, Readiness, TurnLimits},
        discovery::Deployment,
        Registry,
    },
    cairn_store::{LeadershipGrant, PartitionId, PartitionWriter},
    common::fake_sdk::{FakeSdk, Step, HOLD_SIGNAL},
    slatedb::{
        config::Settings,
        object_store::{local::LocalFileSystem, ObjectStore},
        Db,
    },
    std::{
        path::{Path, PathBuf},
        sync::Arc,
        time::Duration,
    },
};

/// The limit for every wait.
const TIMEOUT: Duration = Duration::from_secs(10);

/// The entry 0 header marking an invocation as living under the rule "a virtual object's
/// `SHARED` handler does not write state". Binaries from before the fix do not write it.
const STATE_LOCK_HEADER: &str = cairn_engine::HDR_STATE_LOCK;

// ───────────────────────────────────────────────────────────── runtime

fn fresh_root(name: &str) -> Result<PathBuf> {
    let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(name);
    if root.exists() {
        std::fs::remove_dir_all(&root)?;
    }
    std::fs::create_dir_all(&root)?;
    Ok(root)
}

async fn open_db(root: &Path) -> Result<Db> {
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(root)?);
    Ok(Db::builder("partition-0", store)
        .with_settings(Settings {
            flush_interval: Some(Duration::from_millis(10)),
            ..Default::default()
        })
        .build()
        .await?)
}

/// A new leader on `root` (calling it again on the same `root` is a leader change).
async fn start(root: &Path, deployment: &Deployment) -> Result<EngineHandle> {
    let registry = Registry::new();
    let dir = root.to_path_buf();
    let rt = EngineHandle::spawn_with_transport(
        Box::new(move || {
            let dir = dir.clone();
            Box::pin(async move {
                let db = open_db(&dir).await.expect("open SlateDB");
                let writer =
                    PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db).await?;
                Ok(Leadership {
                    engine: Engine::open(writer).await?,
                    keeper: None,
                })
            })
        }),
        registry.clone(),
        None,
        Readiness::WhenLeader,
        cairn_server::transport::Transport::disconnected(),
        TurnLimits {
            sdk_timeout: None,
            queue_wait: None,
            ..TurnLimits::default()
        },
        None,
    )?;
    registry.upsert(deployment.clone());
    rt.deployments_changed().await?;
    Ok(rt)
}

/// A **stopped** runtime: open `Engine` directly on `root`, do `f`, seal.
async fn with_engine<T>(root: &Path, f: impl AsyncFnOnce(&mut Engine) -> Result<T>) -> Result<T> {
    let db = open_db(root).await?;
    let writer = PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db).await?;
    let mut engine = Engine::open(writer).await?;
    let out = f(&mut engine).await?;
    engine.seal().await?;
    Ok(out)
}

async fn deployment_of(sdk: &FakeSdk) -> Result<Deployment> {
    let mut d = cairn_server::discovery::discover(
        &cairn_server::egress::TenantHttp::h1(Default::default()),
        &sdk.url,
        None,
    )
    .await
    .map_err(|e| anyhow!("discover fake SDK: {e:?}"))?;
    // A fixed id: invocations are bound to a deployment by id, and the next leader rebuilds the registry.
    d.id = "dp_object_shared".into();
    Ok(d)
}

fn target(handler: &str, key: &str) -> Target {
    Target {
        tenant: Default::default(),
        scope: None,
        service: "Cart".into(),
        handler: handler.into(),
        object_key: key.into(),
    }
}

fn id(n: u8) -> [u8; 24] {
    let mut b = [0u8; 24];
    b[15] = n;
    b
}

/// Waits until the status of `id` satisfies `ok`; returns that status.
async fn wait_status(
    rt: &EngineHandle,
    id: [u8; 24],
    what: &str,
    ok: impl Fn(&InvocationStatus) -> bool,
) -> Result<InvocationStatus> {
    let deadline = std::time::Instant::now() + TIMEOUT;
    loop {
        let s = rt.status(id).await?;
        if let Some(s) = s.as_ref().filter(|s| ok(s)) {
            return Ok(s.clone());
        }
        if std::time::Instant::now() > deadline {
            bail!("{what}: after {TIMEOUT:?} still {s:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn settled(s: &InvocationStatus) -> bool {
    matches!(
        s,
        InvocationStatus::Completed(_) | InvocationStatus::Failed { .. }
    )
}

fn is_suspended(s: &InvocationStatus) -> bool {
    matches!(s, InvocationStatus::Suspended)
}

async fn hold_signal(rt: &EngineHandle, id: [u8; 24]) -> Result<()> {
    rt.external_signal(
        id,
        SignalId::Name(HOLD_SIGNAL.into()),
        CompletionPayload::Void,
    )
    .await?;
    Ok(())
}

/// Runs `handler` on `key` until it completes; returns the final status.
async fn run(rt: &EngineHandle, n: u8, handler: &str, key: &str) -> Result<InvocationStatus> {
    rt.submit(
        id(n),
        target(handler, key),
        None,
        None,
        Bytes::from_static(b"{}"),
    )
    .await?;
    wait_status(rt, id(n), handler, settled).await
}

/// The eager state carried by invocation `n`'s `Start`: the object's state when it
/// was loaded — exactly what a real SDK reads.
fn state_seen_by(sdk: &FakeSdk, n: u8) -> Vec<(String, String)> {
    let starts = sdk.starts();
    let start = starts
        .iter()
        .rfind(|s| s.id.as_ref() == id(n).as_slice())
        .expect("the invocation ran at least one turn");
    start
        .state
        .iter()
        .map(|(k, v)| {
            (
                String::from_utf8_lossy(k).into_owned(),
                String::from_utf8_lossy(v).into_owned(),
            )
        })
        .collect()
}

fn pair(k: &str, v: &str) -> (String, String) {
    (k.into(), v.into())
}

fn is_violation(s: &InvocationStatus) -> bool {
    matches!(
        s,
        InvocationStatus::Failed { code, message }
            if *code == cairn_protocol::PROTOCOL_VIOLATION && message.contains("SHARED")
    )
}

/// Rewrites `id`'s entry 0 without the `key` header — building exactly the entry 0 that a
/// binary not yet aware of that header wrote. The same journal write as the engine
/// (`Mutations::append_journal` overwrites exactly the entry 0 slot).
async fn strip_entry0_header(e: &mut Engine, id: &[u8; 24], key: &str) -> Result<()> {
    let rows = e.writer().journal_unbounded(id).await?;
    let (idx, raw) = rows.first().ok_or_else(|| anyhow!("no journal"))?;
    ensure!(*idx == 0, "the first entry must be entry 0");
    let entry = cairn_runtime::JournalEntry::decode(raw.clone())?;
    let frame = Frame::new(entry.ty, Flags::NONE, entry.payload.clone());
    let Message::Input(mut input) = cairn_protocol::decode(&frame)? else {
        bail!("entry 0 is not an Input");
    };
    input.headers.retain(|h| h.key != key);
    let wire = Message::Input(input).encode(Flags::NONE);
    let entry0 = cairn_runtime::JournalEntry::new(
        MessageType::INPUT,
        wire.slice(cairn_protocol::HEADER_LEN..),
    );
    let mut m = cairn_store::Mutations::new();
    m.append_journal(id, 0, &entry0.encode());
    e.writer_mut().commit(m).await?;
    Ok(())
}

/// The shared `Cart` script: `add` (`EXCLUSIVE`) writes `items=1`; `peek`
/// (`SHARED`) tries to write `items=hijacked`; `view` (`SHARED`) only reads; `late`
/// (`SHARED`) waits for a signal before trying to write.
async fn cart_sdk() -> Result<FakeSdk> {
    let set = |k: &str, v: &'static str| Step::SetState {
        key: k.into(),
        value: Bytes::from_static(v.as_bytes()),
    };
    FakeSdk::builder()
        .object(
            "Cart",
            "add",
            vec![set("items", "1"), Step::output("\"added\"")],
        )
        .shared_object(
            "Cart",
            "peek",
            vec![set("items", "hijacked"), Step::output("\"wrote\"")],
        )
        .shared_object("Cart", "view", vec![Step::output("\"viewed\"")])
        .shared_object(
            "Cart",
            "late",
            vec![Step::Suspend, set("late", "1"), Step::output("\"wrote\"")],
        )
        .spawn()
        .await
}

// ───────────────────────────────────────────────────────────── test

/// An `EXCLUSIVE` handler still writes state as before — the new rule does not touch it.
///
/// Must see: `add` completes; a later invocation (`view`) sees `items=1`.
#[tokio::test]
async fn exclusive_object_handler_still_writes_state() -> Result<()> {
    let sdk = cart_sdk().await?;
    let d = deployment_of(&sdk).await?;
    let root = fresh_root("object-shared-exclusive")?;
    let rt = start(&root, &d).await?;

    let s = run(&rt, 1, "add", "k").await?;
    ensure!(matches!(s, InvocationStatus::Completed(_)), "add: {s:?}");
    let s = run(&rt, 2, "view", "k").await?;
    ensure!(matches!(s, InvocationStatus::Completed(_)), "view: {s:?}");
    ensure!(
        state_seen_by(&sdk, 2) == vec![pair("items", "1")],
        "state after add: {:?}",
        state_seen_by(&sdk, 2)
    );
    rt.shutdown().await?;
    Ok(())
}

/// A virtual object's `SHARED` handler writing state ⇒ the invocation is `failed`
/// PROTOCOL_VIOLATION (571), with an English message saying why, and the state
/// is **not** changed. A `SHARED` handler that does not write still runs.
///
/// Must see: `add` writes `items=1`; `peek` fails with 571 (the message mentions `SHARED`);
/// `view` completes and sees `items=1`, not `hijacked`.
#[tokio::test]
async fn shared_object_handler_cannot_write_state() -> Result<()> {
    let sdk = cart_sdk().await?;
    let d = deployment_of(&sdk).await?;
    let root = fresh_root("object-shared-refused")?;
    let rt = start(&root, &d).await?;

    let s = run(&rt, 1, "add", "k").await?;
    ensure!(matches!(s, InvocationStatus::Completed(_)), "add: {s:?}");
    let s = run(&rt, 2, "peek", "k").await?;
    ensure!(is_violation(&s), "peek (SHARED writes state): {s:?}");
    let s = run(&rt, 3, "view", "k").await?;
    ensure!(matches!(s, InvocationStatus::Completed(_)), "view: {s:?}");
    ensure!(
        state_seen_by(&sdk, 3) == vec![pair("items", "1")],
        "state after the refused peek: {:?}",
        state_seen_by(&sdk, 3)
    );
    rt.shutdown().await?;
    Ok(())
}

/// **An invocation in flight during a binary upgrade keeps the old rule; an invocation
/// created under the new rule keeps the new rule across a leader change.**
///
/// Order:
/// 1. `late` (`SHARED`) runs twice, `A` on key `a`, `B` on key `b`: both wait for a
///    signal **before** writing state. The runtime stops.
/// 2. `A`'s entry 0 is rewritten without [`STATE_LOCK_HEADER`] — `A` is now an
///    invocation created by an old binary, waiting during the upgrade. `B` is unchanged.
/// 3. A new leader opens on the same directory; the signal reaches both.
///
/// Must see: `A` writes and completes (the rule is fixed at creation: the old binary
/// promised nothing, so running code does not change behaviour midway); `B` fails with 571 —
/// the mark in entry 0 survives the leader change, and the turn after the leader change still applies the rule.
#[tokio::test]
async fn in_flight_shared_handler_keeps_the_rule_it_was_created_under() -> Result<()> {
    let sdk = cart_sdk().await?;
    let d = deployment_of(&sdk).await?;
    let root = fresh_root("object-shared-in-flight")?;
    let (a, b) = (1u8, 2u8);
    {
        let rt = start(&root, &d).await?;
        for (n, key) in [(a, "a"), (b, "b")] {
            rt.submit(
                id(n),
                target("late", key),
                None,
                None,
                Bytes::from_static(b"{}"),
            )
            .await?;
            wait_status(&rt, id(n), "late waits before writing", is_suspended).await?;
        }
        rt.shutdown().await?;
    }
    with_engine(&root, async |e: &mut Engine| {
        strip_entry0_header(e, &id(a), STATE_LOCK_HEADER).await
    })
    .await?;

    let rt = start(&root, &d).await?;
    hold_signal(&rt, id(a)).await?;
    hold_signal(&rt, id(b)).await?;
    let sa = wait_status(&rt, id(a), "A (old binary) completes", settled).await?;
    ensure!(
        matches!(sa, InvocationStatus::Completed(_)),
        "A, created before the fix, must write as before: {sa:?}"
    );
    let sb = wait_status(&rt, id(b), "B fails", settled).await?;
    ensure!(is_violation(&sb), "B created under the new rule: {sb:?}");

    // State: `a` has `late=1`, `b` has nothing.
    let s = run(&rt, 3, "view", "a").await?;
    ensure!(matches!(s, InvocationStatus::Completed(_)), "view a: {s:?}");
    ensure!(
        state_seen_by(&sdk, 3) == vec![pair("late", "1")],
        "state of a: {:?}",
        state_seen_by(&sdk, 3)
    );
    let s = run(&rt, 4, "view", "b").await?;
    ensure!(matches!(s, InvocationStatus::Completed(_)), "view b: {s:?}");
    ensure!(
        state_seen_by(&sdk, 4).is_empty(),
        "state of b must be empty: {:?}",
        state_seen_by(&sdk, 4)
    );
    rt.shutdown().await?;
    Ok(())
}
