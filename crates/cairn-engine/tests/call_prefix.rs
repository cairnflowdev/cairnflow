// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The gate grouping call trees by pk prefix, at the engine layer: the rule of
//! a call is **fixed when the outbox row is written**, and replaying the task
//! does not change it.
//!
//! The state machine replays `Task::Invoke` for every call still waiting for an
//! id on **every** replay, and the engine rewrites the outbox row (D-O2: same
//! `seq`, overwrite). A row written before the gate was turned on (v5, the old
//! rule) that is rewritten after it as v6 (the prefix rule) is **a second
//! callee** if the old row has already sent its `StartCall` — exactly what IR-9
//! forbids. The test reads the row version from the first byte of `encode()`
//! (v6 appends `callee_pk`: a presence flag + 8 bytes).

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
const CALLER_PK: u64 = 0x1234_5678_0000_0042;
/// The run time of a one-way call scheduled in advance — far in the future.
const LATER: u64 = 4_000_000_000_000;

fn caller() -> [u8; 24] {
    make_invocation_id(PartitionKey(CALLER_PK), [7; 16])
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
fn call(id_cid: u32, key: &str, scope: Option<&str>, idem: Option<&str>) -> Frame {
    frame(
        MessageType::CALL,
        &pb::CallCommandMessage {
            service_name: "Leaf".into(),
            handler_name: "echo".into(),
            key: key.into(),
            parameter: Bytes::new(),
            headers: vec![],
            idempotency_key: idem.map(Into::into),
            result_completion_id: id_cid + 1,
            invocation_id_notification_idx: id_cid,
            name: String::new(),
            scope: scope.map(Into::into),
            limit_key: None,
            placement: 0,
            tags: vec![],
        },
    )
}

/// An unkeyed `ctx.send` scheduled at [`LATER`]: sits in the `delayed` table until due.
fn send_later(id_cid: u32) -> Frame {
    frame(
        MessageType::ONE_WAY_CALL,
        &pb::OneWayCallCommandMessage {
            service_name: "Leaf".into(),
            handler_name: "echo".into(),
            parameter: Bytes::new(),
            invoke_time: LATER,
            headers: vec![],
            key: String::new(),
            idempotency_key: None,
            scope: None,
            limit_key: None,
            invocation_id_notification_idx: id_cid,
            name: String::new(),
            placement: 0,
            tags: vec![],
        },
    )
}

fn fresh_root(name: &str) -> Result<PathBuf> {
    let root = std::env::temp_dir().join(format!(
        "cairnflow-callprefix-{name}-{}",
        std::process::id()
    ));
    if root.exists() {
        std::fs::remove_dir_all(&root)?;
    }
    std::fs::create_dir_all(&root)?;
    Ok(root)
}

/// An engine on `root` with the `call_prefix` gate as at range open.
async fn open_engine(root: &PathBuf, call_prefix: bool) -> Result<Engine> {
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
    Ok(Engine::open(writer).await?.with_call_prefix(call_prefix))
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

/// `(completion id, version, last 8 bytes)` of every pending `Call` row.
async fn call_rows(engine: &mut Engine) -> Result<Vec<(u32, u8, u64)>> {
    let mut out: Vec<(u32, u8, u64)> = engine
        .pending_outbox(64)
        .await?
        .into_iter()
        .filter_map(|(_, e)| match &e {
            OutboxEntry::Call(c) => {
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

fn prefix_pk(id_cid: u32) -> u64 {
    partition_key_for_callee_prefix(PartitionKey(CALLER_PK), &caller(), id_cid).0
}

/// **IR-9 across turning the gate on.** A `Call` row (and the `delayed` row of
/// a `ctx.send` scheduled in advance) written while the gate is off ⇒ v5.
/// Reopen the range **with the gate on** and replay the caller — the state
/// machine replays both `Task::Invoke`s — both rows stay at v5 (the old rule),
/// including the `delayed` row when it comes due and moves to the outbox.
///
/// Before the prefix rule existed the engine always wrote v5, so the test was
/// trivially green; it guards **the way it was fixed**: computing the rule from
/// the gate at write time while forgetting existing rows spawns a second callee.
#[tokio::test]
async fn rows_written_before_the_gate_keep_the_old_rule_on_replay() -> Result<()> {
    let root = fresh_root("before")?;
    {
        let mut engine = open_engine(&root, false).await?;
        let mut a = start(&mut engine).await?;
        engine
            .step(&mut a, Event::FromSdk(call(1, "", None, None)))
            .await?;
        engine.step(&mut a, Event::FromSdk(send_later(3))).await?;
        let rows = call_rows(&mut engine).await?;
        assert_eq!(
            rows.iter().map(|r| (r.0, r.1)).collect::<Vec<_>>(),
            vec![(1, 5)]
        );
        // Durable, then a sudden death (no `seal`), like the old leader before a failover.
        engine.writer().wait_durable().await?;
    }
    let mut engine = open_engine(&root, true).await?;
    for _ in 0..2 {
        replay(&mut engine).await?;
        let rows = call_rows(&mut engine).await?;
        assert_eq!(
            rows.iter().map(|r| (r.0, r.1)).collect::<Vec<_>>(),
            vec![(1, 5)],
            "replay with the gate on changed the rule of a row written before the gate: {rows:x?}"
        );
    }
    assert_eq!(engine.promote_delayed(LATER + 1, 16).await?, 1);
    replay(&mut engine).await?;
    let rows = call_rows(&mut engine).await?;
    assert_eq!(
        rows.iter().map(|r| (r.0, r.1)).collect::<Vec<_>>(),
        vec![(1, 5), (3, 5)],
        "a `delayed` row written before the gate must keep the old rule: {rows:x?}"
    );
    Ok(())
}

/// **The prefix rule after turning the gate on.** An unkeyed, unscoped call
/// without an idempotency key ⇒ a v6 row carrying `callee_pk` = the caller's
/// high 32 bits | a hash of `(caller, completion)`; replay keeps it. Keyed tiers
/// (object key, scope, idempotency key) keep the existing rule: v5 (IR-10). A
/// `ctx.send` scheduled in advance follows the prefix rule too, also once due.
#[tokio::test]
async fn unkeyed_calls_after_the_gate_carry_the_callers_prefix() -> Result<()> {
    let root = fresh_root("after")?;
    let mut engine = open_engine(&root, true).await?;
    let mut a = start(&mut engine).await?;
    engine
        .step(&mut a, Event::FromSdk(call(1, "", None, None)))
        .await?;
    engine
        .step(&mut a, Event::FromSdk(call(3, "k", None, None)))
        .await?;
    engine
        .step(&mut a, Event::FromSdk(call(5, "", Some("acme"), None)))
        .await?;
    engine
        .step(&mut a, Event::FromSdk(call(7, "", None, Some("i"))))
        .await?;
    engine.step(&mut a, Event::FromSdk(send_later(9))).await?;
    let want_v = vec![(1, 6), (3, 5), (5, 5), (7, 5)];
    for round in 0..2 {
        let rows = call_rows(&mut engine).await?;
        assert_eq!(
            rows.iter().map(|r| (r.0, r.1)).collect::<Vec<_>>(),
            want_v,
            "round {round}: {rows:x?}"
        );
        assert_eq!(rows[0].2, prefix_pk(1), "callee_pk of the unkeyed call");
        assert_eq!(rows[0].2 >> 32, CALLER_PK >> 32);
        replay(&mut engine).await?;
    }
    assert_eq!(engine.promote_delayed(LATER + 1, 16).await?, 1);
    let rows = call_rows(&mut engine).await?;
    let sent = rows
        .iter()
        .find(|r| r.0 == 9)
        .expect("the delayed row has moved");
    assert_eq!((sent.1, sent.2), (6, prefix_pk(9)), "{rows:x?}");
    Ok(())
}

/// **An absent row whose id has not been delivered** (found in review). The
/// rule of a call must not be "keep it if the row is still there, follow the
/// gate if it is gone": the in-RAM D-O2 map removes the row when the deleting
/// commit is *staged*, so a failed id-delivery commit with the engine living
/// on leaves a v5 row in the store that the map does not know — and the next
/// replay sees "no row".
///
/// Model: the caller is created with the gate off, a v5 `Call` row is written;
/// the row is dropped from the map and the store while the id has **not** been
/// delivered (`drop_outbox` — the same "absent" shape an off map shows); the
/// range is reopened with the gate on; the caller is replayed.
///
/// Must see: the rewritten row is still v5 — the rule belongs to **the
/// caller** (created before the gate), not to the gate state at rewrite time.
/// Before the fix: v6, i.e. a second callee if the v5 row's `StartCall` had
/// already gone out.
#[tokio::test]
async fn row_gone_before_its_id_is_delivered_keeps_the_callers_rule() -> Result<()> {
    let root = fresh_root("gone")?;
    {
        let mut engine = open_engine(&root, false).await?;
        let mut a = start(&mut engine).await?;
        engine
            .step(&mut a, Event::FromSdk(call(1, "", None, None)))
            .await?;
        let rows = engine.pending_outbox(16).await?;
        assert_eq!(rows.len(), 1);
        engine.drop_outbox(rows[0].0).await?;
        assert!(call_rows(&mut engine).await?.is_empty());
        engine.writer().wait_durable().await?;
    }
    let mut engine = open_engine(&root, true).await?;
    replay(&mut engine).await?;
    let rows = call_rows(&mut engine).await?;
    assert_eq!(
        rows.iter().map(|r| (r.0, r.1)).collect::<Vec<_>>(),
        vec![(1, 5)],
        "caller created before the gate: replay after the row is gone must keep the old rule: {rows:x?}"
    );
    Ok(())
}

/// **The rule belongs to the caller, fixed when it is created.** A caller
/// created with the gate off; the range reopens with the gate on; the caller
/// issues a **new** unkeyed `ctx.call` ⇒ still v5 (the old rule). A caller
/// created after the gate is on ⇒ v6. One rule per caller, unchanged across
/// range opens — so no rewrite of any call can change it.
#[tokio::test]
async fn callers_created_before_the_gate_keep_the_old_rule_for_new_calls() -> Result<()> {
    let root = fresh_root("percaller")?;
    {
        let mut engine = open_engine(&root, false).await?;
        let mut a = start(&mut engine).await?;
        engine
            .step(&mut a, Event::FromSdk(call(1, "", None, None)))
            .await?;
        engine.writer().wait_durable().await?;
    }
    let mut engine = open_engine(&root, true).await?;
    let mut a = engine.load(caller(), StartParams::default()).await?;
    engine.step(&mut a, Event::Start).await?;
    engine
        .step(&mut a, Event::FromSdk(call(3, "", None, None)))
        .await?;
    let rows = call_rows(&mut engine).await?;
    assert_eq!(
        rows.iter().map(|r| (r.0, r.1)).collect::<Vec<_>>(),
        vec![(1, 5), (3, 5)],
        "{rows:x?}"
    );
    Ok(())
}

/// **`restart` copies the old copy's mark** (found in review). A caller created
/// before the gate issues an unkeyed `ctx.call` (a v5 row), then is rebuilt
/// (`restart`, keeping the `Call` command that has no id yet) after the range
/// opened with the gate on; the new copy replays that call.
///
/// Must see: the new copy's `Call` row is still v5 — the rule follows the
/// invocation's lineage, not the gate at rebuild time. Call sites today always
/// issue a new id for the rebuilt copy (`turn::retry::successor_id`), so its
/// callee differs from the old copy's callee id under either rule; copying the
/// mark keeps the rule from depending on that.
#[tokio::test]
async fn restart_keeps_the_rule_of_the_invocation_it_restarts() -> Result<()> {
    let root = fresh_root("restart")?;
    {
        let mut engine = open_engine(&root, false).await?;
        let mut a = start(&mut engine).await?;
        engine
            .step(&mut a, Event::FromSdk(call(1, "", None, None)))
            .await?;
        engine.writer().wait_durable().await?;
    }
    let mut engine = open_engine(&root, true).await?;
    let new = make_invocation_id(PartitionKey(CALLER_PK), [8; 16]);
    let keep = engine.writer().journal_unbounded(&caller()).await?.len() as u32;
    // The new copy enters the queue the way the real shell does
    // (`turn::retry::restart`): `Running` without a place in a queue is a
    // tripwire of `lifecycle-check`.
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
    let rows: Vec<(u32, u8)> = engine
        .pending_outbox(64)
        .await?
        .into_iter()
        .filter_map(|(_, e)| match &e {
            OutboxEntry::Call(c) if c.caller == new => Some((c.id_completion, e.encode()[0])),
            _ => None,
        })
        .collect();
    assert_eq!(rows, vec![(1, 5)], "rebuilt copy keeps old rule");
    Ok(())
}
