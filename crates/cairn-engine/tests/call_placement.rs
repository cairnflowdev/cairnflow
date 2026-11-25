// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The deliberate spread path `placement: "spread"` at the engine layer: the
//! rule of a `spread` call belongs to the **caller**, fixed when the caller is
//! created (the `cairnflow-call-placement` mark in entry 0), and every rewrite
//! of the outbox row yields the same row.
//!
//! Why pin it: the state machine replays `Task::Invoke` of every call still
//! waiting for an id on **every** replay, and the engine overwrites the outbox
//! row (D-O2). A `spread` call that already sent its `StartCall` under the old
//! rule (a v5 row) and is rewritten under the grouping rule (v6) is **a second
//! callee** — IR-9. The test reads the row version from the first byte of
//! `encode()`, like `call_prefix.rs`.

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
    cairn_engine::{CreateOptions, Engine, Invocation, OutboxEntry, Target},
    cairn_protocol::{encode_message, pb, Flags, Frame, FrameDecoder, MessageType},
    cairn_runtime::{Event, StartParams},
    cairn_store::{
        make_invocation_id, partition_key_for_callee_prefix, LeadershipGrant, PartitionId,
        PartitionKey, PartitionWriter,
    },
    slatedb::{
        config::Settings,
        object_store::{local::LocalFileSystem, ObjectStore},
        Db,
    },
    std::{path::PathBuf, sync::Arc, time::Duration},
};

/// The caller's pk: high 32 bits that are easy to recognise.
const CALLER_PK: u64 = 0x2345_6789_0000_0042;
/// The run time of a one-way call scheduled in advance — far in the future.
const LATER: u64 = 4_000_000_000_000;

const SPREAD: i32 = pb::CallPlacement::Spread as i32;
const DEFAULT: i32 = pb::CallPlacement::Default as i32;

fn caller() -> [u8; 24] {
    make_invocation_id(PartitionKey(CALLER_PK), [9; 16])
}

fn target() -> Target {
    Target {
        tenant: Default::default(),
        scope: None,
        service: "Caller".into(),
        handler: "run".into(),
        object_key: String::new(),
    }
}

fn frame<M: prost::Message>(ty: MessageType, m: &M) -> Frame {
    let wire = encode_message(ty, Flags::NONE, m);
    let mut buf = bytes::BytesMut::from(&wire[..]);
    FrameDecoder::new().decode(&mut buf).unwrap().unwrap()
}

/// `ctx.call` to `Leaf/echo`: the id at completion `id_cid`, the result at `id_cid + 1`.
fn call(id_cid: u32, key: &str, idem: Option<&str>, placement: i32) -> Frame {
    frame(
        MessageType::CALL,
        &pb::CallCommandMessage {
            service_name: "Leaf".into(),
            handler_name: "echo".into(),
            key: key.into(),
            idempotency_key: idem.map(Into::into),
            result_completion_id: id_cid + 1,
            invocation_id_notification_idx: id_cid,
            placement,
            ..Default::default()
        },
    )
}

/// An unkeyed `ctx.send` scheduled at [`LATER`]: sits in the `delayed` table until due.
fn send_later(id_cid: u32, placement: i32) -> Frame {
    frame(
        MessageType::ONE_WAY_CALL,
        &pb::OneWayCallCommandMessage {
            service_name: "Leaf".into(),
            handler_name: "echo".into(),
            invoke_time: LATER,
            invocation_id_notification_idx: id_cid,
            placement,
            ..Default::default()
        },
    )
}

fn fresh_root(name: &str) -> Result<PathBuf> {
    let root = std::env::temp_dir().join(format!(
        "cairnflow-callplacement-{name}-{}",
        std::process::id()
    ));
    if root.exists() {
        std::fs::remove_dir_all(&root)?;
    }
    std::fs::create_dir_all(&root)?;
    Ok(root)
}

/// An engine on `root` with the two call gates as at range open.
async fn open_engine(root: &PathBuf, call_prefix: bool, call_placement: bool) -> Result<Engine> {
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
    Ok(Engine::open(writer)
        .await?
        .with_call_prefix(call_prefix)
        .with_call_placement(call_placement))
}

async fn start(engine: &mut Engine) -> Result<Invocation> {
    let mut inv = engine
        .create(
            caller(),
            &target(),
            CreateOptions::default(),
            StartParams::default(),
            Bytes::new(),
        )
        .await?;
    engine.step(&mut inv, Event::Start).await?;
    Ok(inv)
}

/// Replays the caller like recovery after a failover: load from the journal, `Start`.
async fn replay(engine: &mut Engine) -> Result<()> {
    let mut again = engine.load(caller(), StartParams::default()).await?;
    engine.step(&mut again, Event::Start).await?;
    Ok(())
}

/// `(completion id, version, last 8 bytes)` of every pending `Call` row of
/// caller `who`.
async fn call_rows(engine: &mut Engine, who: [u8; 24]) -> Result<Vec<(u32, u8, u64)>> {
    let mut out: Vec<(u32, u8, u64)> = engine
        .pending_outbox(64)
        .await?
        .into_iter()
        .filter_map(|(_, e)| match &e {
            OutboxEntry::Call(c) if c.caller == who => {
                // The v5/v6 layout without the lineage tail block.
                let raw = OutboxEntry::Call(cairn_engine::OutboundCall {
                    lineage: Default::default(),
                    ..c.clone()
                })
                .encode();
                let tail = u64::from_be_bytes(raw[raw.len() - 8..].try_into().unwrap());
                Some((c.id_completion, raw[0], tail))
            }
            _ => None,
        })
        .collect();
    out.sort_unstable();
    Ok(out)
}

fn versions(rows: &[(u32, u8, u64)]) -> Vec<(u32, u8)> {
    rows.iter().map(|r| (r.0, r.1)).collect()
}

fn prefix_pk(id_cid: u32) -> u64 {
    partition_key_for_callee_prefix(PartitionKey(CALLER_PK), &caller(), id_cid).0
}

/// **The `spread` rule after the `calls/placement` gate.** A caller created
/// with both gates on:
/// - an unkeyed `spread` call ⇒ a **v5** row (the old rule `H(caller, cid)`,
///   computed at drain time) — including a `ctx.send` scheduled in advance,
///   once due;
/// - an unkeyed default call ⇒ v6 by prefix (grouped);
/// - `spread` with an object key or idempotency key ⇒ ignored: the keyed tier's
///   rule (v5, as without `spread`).
///
/// Then replay twice, and reopen the range with the `calls/placement` gate
/// **off** (the rule reads the caller's mark, not the gate) and replay: every
/// row keeps its last bytes exactly.
#[tokio::test]
async fn spread_calls_of_a_marked_caller_keep_the_old_rule() -> Result<()> {
    let root = fresh_root("marked")?;
    let want = vec![(1, 5), (3, 6), (5, 5), (7, 5)];
    let first = {
        let mut engine = open_engine(&root, true, true).await?;
        let mut a = start(&mut engine).await?;
        for f in [
            call(1, "", None, SPREAD),
            call(3, "", None, DEFAULT),
            call(5, "k", None, SPREAD),
            call(7, "", Some("i"), SPREAD),
            send_later(9, SPREAD),
        ] {
            engine.step(&mut a, Event::FromSdk(f)).await?;
        }
        let rows = call_rows(&mut engine, caller()).await?;
        assert_eq!(versions(&rows), want, "{rows:x?}");
        assert_eq!(rows[1].2, prefix_pk(3), "the default call groups by prefix");
        for _ in 0..2 {
            replay(&mut engine).await?;
            let again = call_rows(&mut engine, caller()).await?;
            assert_eq!(again, rows, "replay changed rows");
        }
        engine.writer().wait_durable().await?;
        rows
    };
    let mut engine = open_engine(&root, true, false).await?;
    replay(&mut engine).await?;
    let rows = call_rows(&mut engine, caller()).await?;
    assert_eq!(
        rows, first,
        "replay with the calls/placement gate off changed a marked caller's rule"
    );
    assert_eq!(engine.promote_delayed(LATER + 1, 16).await?, 1);
    let rows = call_rows(&mut engine, caller()).await?;
    let sent = rows
        .iter()
        .find(|r| r.0 == 9)
        .expect("the delayed row has moved");
    assert_eq!(
        sent.1, 5,
        "a ctx.send spread scheduled in advance must follow the old rule: {rows:x?}"
    );
    Ok(())
}

/// **`spread` is ignored for an unmarked caller** — created while the
/// `calls/placement` gate was off (before it was turned on, or a cluster that
/// has not turned it on): a `spread` call groups like a default call (v6, the
/// same prefix pk). Reopen the range with the gate on: replay keeps v6, and a
/// **new** `spread` call of the same caller is v6 too.
///
/// This is the rule a gate-step-4 binary applies (it does not know
/// `placement`), so a journal a gate-step-4 binary has processed never yields
/// a different row under this binary.
#[tokio::test]
async fn spread_is_ignored_for_callers_created_before_the_placement_gate() -> Result<()> {
    let root = fresh_root("unmarked")?;
    {
        let mut engine = open_engine(&root, true, false).await?;
        let mut a = start(&mut engine).await?;
        engine
            .step(&mut a, Event::FromSdk(call(1, "", None, SPREAD)))
            .await?;
        let rows = call_rows(&mut engine, caller()).await?;
        assert_eq!(rows, vec![(1, 6, prefix_pk(1))], "{rows:x?}");
        engine.writer().wait_durable().await?;
    }
    let mut engine = open_engine(&root, true, true).await?;
    let mut a = engine.load(caller(), StartParams::default()).await?;
    engine.step(&mut a, Event::Start).await?;
    engine
        .step(&mut a, Event::FromSdk(call(3, "", None, SPREAD)))
        .await?;
    let rows = call_rows(&mut engine, caller()).await?;
    assert_eq!(
        rows,
        vec![(1, 6, prefix_pk(1)), (3, 6, prefix_pk(3))],
        "a caller created before the calls/placement gate must ignore spread forever: {rows:x?}"
    );
    Ok(())
}

/// **With the grouping gate off, `spread` changes nothing.** An old cluster
/// that has turned no gate on: every unkeyed call already follows the old rule
/// (v5), `spread` or not. The engine turns only `calls/placement` on (no
/// grouping gate — only buildable at the engine layer): still v5 for both.
#[tokio::test]
async fn spread_without_the_prefix_gate_changes_nothing() -> Result<()> {
    for (name, placement) in [("off", false), ("placement-only", true)] {
        let root = fresh_root(name)?;
        let mut engine = open_engine(&root, false, placement).await?;
        let mut a = start(&mut engine).await?;
        engine
            .step(&mut a, Event::FromSdk(call(1, "", None, SPREAD)))
            .await?;
        engine
            .step(&mut a, Event::FromSdk(call(3, "", None, DEFAULT)))
            .await?;
        let rows = call_rows(&mut engine, caller()).await?;
        assert_eq!(versions(&rows), vec![(1, 5), (3, 5)], "{name}: {rows:x?}");
    }
    Ok(())
}

/// **`restart` copies the `cairnflow-call-placement` mark** (like the
/// `cairnflow-call-prefix` mark). A marked caller issues a `spread` call (a v5
/// row), then is rebuilt (`restart`, keeping the `Call` command that has no id
/// yet) on a range opened with the `calls/placement` gate **off**.
///
/// Must see: the new copy's `Call` row is still v5 — the rule follows the
/// lineage, not the gate at rebuild time.
#[tokio::test]
async fn restart_keeps_the_placement_marker() -> Result<()> {
    let root = fresh_root("restart")?;
    {
        let mut engine = open_engine(&root, true, true).await?;
        let mut a = start(&mut engine).await?;
        engine
            .step(&mut a, Event::FromSdk(call(1, "", None, SPREAD)))
            .await?;
        engine.writer().wait_durable().await?;
    }
    let mut engine = open_engine(&root, true, false).await?;
    let new = make_invocation_id(PartitionKey(CALLER_PK), [8; 16]);
    let keep = engine.writer().journal_unbounded(&caller()).await?.len() as u32;
    let q = cairn_store::QueueName {
        tenant: Default::default(),
        scope: None,
        limit: None,
        exclusive: false,
        service: "Caller".into(),
        object_key: String::new(),
    };
    let seq = engine.next_queue_seq(&q).await?;
    let out = engine
        .restart(&caller(), &new, keep, None, None, Some((&q, seq)))
        .await?;
    assert!(
        matches!(out, cairn_engine::RestartOutcome::Restarted { .. }),
        "{out:?}"
    );
    let mut b = engine.load(new, StartParams::default()).await?;
    engine.step(&mut b, Event::Start).await?;
    let rows = call_rows(&mut engine, new).await?;
    assert_eq!(
        versions(&rows),
        vec![(1, 5)],
        "the rebuilt copy must keep the old copy's spread rule: {rows:x?}"
    );
    Ok(())
}
