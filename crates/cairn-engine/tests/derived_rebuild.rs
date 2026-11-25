// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The derived zone is rebuildable.
//!
//! After a split or merge, the child Db sees the `0x01` zone **empty** — the
//! projection hides it, the union filters it. The only mechanism that brings
//! it back is rebuilding it from the hot entity zone, **before serving**,
//! whenever the `DERIVED_READY` mark is missing or belongs to another range.
//! The tests here build exactly that situation by deleting the whole derived
//! zone by hand.

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
    cairn_engine::{
        CompletionPayload, CreateOptions, DeliverCompletion, Engine, InterPartition, Invocation,
        Target,
    },
    cairn_protocol::{encode_message, pb, Flags, Frame, FrameDecoder, MessageType},
    cairn_runtime::{Event, StartParams},
    cairn_store::{
        keys::{self, Index},
        make_invocation_id, partition_key_for_target, LeadershipGrant, Mutations, PartitionId,
        PartitionKey, PartitionWriter, QueueName, Stage,
    },
    slatedb::{
        config::{FlushOptions, FlushType, Settings},
        object_store::{local::LocalFileSystem, ObjectStore},
        Db,
    },
    std::{path::PathBuf, sync::Arc, time::Duration},
};

const DB: &str = "partition-0";
const DEPLOYMENT: &str = "dp_test";

fn frame<M: prost::Message>(ty: MessageType, m: &M) -> Frame {
    let wire = encode_message(ty, Flags::NONE, m);
    let mut buf = bytes::BytesMut::from(&wire[..]);
    FrameDecoder::new().decode(&mut buf).unwrap().unwrap()
}

fn hex(id: &[u8; 24]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

fn fresh_root(name: &str) -> Result<PathBuf> {
    let root = std::env::temp_dir().join(format!("cairnflow-derived-{name}"));
    if root.exists() {
        std::fs::remove_dir_all(&root)?;
    }
    std::fs::create_dir_all(&root)?;
    Ok(root)
}

fn store_at(root: &PathBuf) -> Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(LocalFileSystem::new_with_prefix(root)?))
}

fn settings() -> Settings {
    Settings {
        flush_interval: Some(Duration::from_millis(10)),
        ..Default::default()
    }
}

async fn open_engine(store: &Arc<dyn ObjectStore>) -> Result<Engine> {
    let db = Db::builder(DB, store.clone())
        .with_settings(settings())
        .build()
        .await?;
    let writer = PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db).await?;
    Ok(Engine::open(writer).await?.with_retention(Some(3_600_000)))
}

fn cart() -> Target {
    Target {
        tenant: Default::default(),
        scope: None,
        service: "Cart".into(),
        handler: "add".into(),
        object_key: "k1".into(),
    }
}

fn pk_cart() -> PartitionKey {
    partition_key_for_target(&cairn_store::TenantId::default(), None, "Cart", "k1", None).unwrap()
}

fn queue() -> QueueName {
    QueueName {
        tenant: cairn_store::TenantId::default(),
        scope: None,
        limit: None,
        exclusive: true,
        service: "Cart".into(),
        object_key: "k1".into(),
    }
}

async fn create(engine: &mut Engine, n: u8) -> Result<Invocation> {
    let id = make_invocation_id(pk_cart(), [n; 16]);
    let q = queue();
    let seq = engine.next_queue_seq(&q).await?;
    let mut inv = engine
        .create(
            id,
            &cart(),
            CreateOptions {
                queue: Some((&q, seq)),
                // Deployment pinning: the `Pins` row + the `Deployment` index
                // must be rebuilt like every other index.
                deployment: Some(DEPLOYMENT),
                ..Default::default()
            },
            StartParams::default(),
            Bytes::new(),
        )
        .await?;
    engine.step(&mut inv, Event::Start).await?;
    Ok(inv)
}

fn sleep(at: u64, id: u32) -> Frame {
    frame(
        MessageType::SLEEP,
        &pb::SleepCommandMessage {
            wake_up_time: at,
            result_completion_id: id,
            name: String::new(),
        },
    )
}

fn call(result: u32, id_completion: u32) -> Frame {
    frame(
        MessageType::CALL,
        &pb::CallCommandMessage {
            service_name: "Payment".into(),
            handler_name: "charge".into(),
            key: "acct".into(),
            result_completion_id: result,
            invocation_id_notification_idx: id_completion,
            ..Default::default()
        },
    )
}

/// A scenario with every kind of indexed row: running and finished invocations
/// (scheduled purge), queues, timers, outbox, signals, delayed calls, retries,
/// inbound.
async fn scenario(store: &Arc<dyn ObjectStore>) -> Result<Vec<[u8; 24]>> {
    let mut engine = open_engine(store).await?;
    let mut running = Vec::new();
    for n in 1..=3u8 {
        let mut inv = create(&mut engine, n).await?;
        engine.step(&mut inv, Event::FromSdk(sleep(1, 1))).await?;
        engine.step(&mut inv, Event::FromSdk(call(2, 3))).await?;
        engine
            .step(
                &mut inv,
                Event::FromSdk(frame(
                    MessageType::SEND_SIGNAL,
                    &pb::SendSignalCommandMessage {
                        target_invocation_id: hex(&make_invocation_id(PartitionKey(9), [9; 16])),
                        signal_id: Some(pb::send_signal_command_message::SignalId::Name(
                            "go".into(),
                        )),
                        result: Some(pb::send_signal_command_message::Result::Void(pb::Void {})),
                        entry_name: String::new(),
                    },
                )),
            )
            .await?;
        engine
            .step(
                &mut inv,
                Event::FromSdk(frame(
                    MessageType::ONE_WAY_CALL,
                    &pb::OneWayCallCommandMessage {
                        service_name: "Mailer".into(),
                        handler_name: "send".into(),
                        invoke_time: u64::MAX / 2,
                        invocation_id_notification_idx: 4,
                        ..Default::default()
                    },
                )),
            )
            .await?;
        running.push(inv.id);
    }
    engine.schedule_retry(&running[0], 1, 2, 3).await?;
    let mut done = create(&mut engine, 9).await?;
    engine
        .step(
            &mut done,
            Event::FromSdk(frame(
                MessageType::OUTPUT,
                &pb::OutputCommandMessage {
                    result: Some(pb::output_command_message::Result::Value(
                        cairn_protocol::value::inline(Bytes::from_static(b"1")),
                    )),
                    name: String::new(),
                },
            )),
        )
        .await?;
    engine
        .step(
            &mut done,
            Event::FromSdk(frame(MessageType::END, &pb::EndMessage {})),
        )
        .await?;
    for cid in 10..13 {
        engine
            .accept_message(&InterPartition::Completion(DeliverCompletion {
                target: running[1],
                completion_id: cid,
                result: CompletionPayload::Void,
            }))
            .await?;
    }
    engine.seal().await?;
    Ok(running)
}

/// Every derived-zone row (except the mark, the query-index mark and the
/// durable progress watermarks — progress state, not an index), in key order.
async fn derived(store: &Arc<dyn ObjectStore>) -> Result<Vec<(Bytes, Bytes)>> {
    let db = Db::builder(DB, store.clone())
        .with_settings(settings())
        .build()
        .await?;
    let mut it = db.scan(vec![0x01u8]..vec![0x02u8]).await?;
    let mut out = vec![];
    while let Some(kv) = it.next().await? {
        if kv.key.as_ref() != keys::derived_ready().as_slice()
            && kv.key.as_ref() != keys::floors().as_slice()
            && kv.key.as_ref() != cairn_store::query_index::ready_key().as_slice()
        {
            out.push((kv.key, kv.value));
        }
    }
    db.close().await?;
    Ok(out)
}

/// Simulates a child Db just born from a split: empty derived zone, no mark.
async fn wipe_derived(store: &Arc<dyn ObjectStore>) -> Result<usize> {
    let db = Db::builder(DB, store.clone())
        .with_settings(settings())
        .build()
        .await?;
    let mut it = db.scan(vec![0x01u8]..vec![0x02u8]).await?;
    let mut keys = vec![];
    while let Some(kv) = it.next().await? {
        keys.push(kv.key);
    }
    for k in &keys {
        db.delete(k).await?;
    }
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await?;
    db.close().await?;
    Ok(keys.len())
}

/// Normalises for comparison: ordering indexes (`seq ‖ entity key`) are rebuilt
/// with `seq` reissued in the same order, so compare **the order of entity
/// keys**, not the numbers.
fn normalize(rows: &[(Bytes, Bytes)]) -> Vec<(Vec<u8>, Vec<u8>)> {
    rows.iter()
        .map(|(k, v)| {
            for ix in [Index::Outbox, Index::Signal, Index::Inbound] {
                if let Some((_, ekey)) = keys::parse_ix_seq(k, ix) {
                    return ([&[0x01, ix as u8][..], ekey].concat(), v.to_vec());
                }
            }
            (k.to_vec(), v.to_vec())
        })
        .collect()
}

/// **The derived zone's exit criterion.** Delete the derived zone, reopen ⇒ the
/// same content (except `seq` reissued in the same order). The query index is
/// not rebuilt at open but by the background build: run it to completion
/// before comparing — it must recompute **exactly** every key the live writes
/// created.
#[tokio::test]
async fn derived_region_rebuilds_to_same_bytes() -> Result<()> {
    let root = fresh_root("same-bytes")?;
    let store = store_at(&root)?;
    scenario(&store).await?;
    let before = derived(&store).await?;
    assert!(
        before.len() > 10,
        "the scenario must produce every kind of index row: {}",
        before.len()
    );

    assert_eq!(
        wipe_derived(&store).await?,
        before.len() + 2,
        "delete the mark and the durable progress watermarks too"
    );
    let mut engine = open_engine(&store).await?;
    let mut cursor = cairn_engine::BuildCursor::default();
    while engine.query_build_step(&mut cursor, 7).await? != cairn_engine::BuildStep::Idle {}
    engine.seal().await?;
    let after = derived(&store).await?;
    assert_eq!(normalize(&after), normalize(&before));
    Ok(())
}

/// Opening a Db without the mark ⇒ rebuilt **before** the engine returns: the
/// very first read already sees all pending work.
#[tokio::test]
async fn open_without_marker_rebuilds_before_serving() -> Result<()> {
    let root = fresh_root("open-rebuilds")?;
    let store = store_at(&root)?;
    let mut running = scenario(&store).await?;
    wipe_derived(&store).await?;

    let mut engine = open_engine(&store).await?;
    let mut unfinished = engine.unfinished_invocations_unbounded().await?;
    unfinished.sort();
    running.sort();
    assert_eq!(unfinished, running, "recover misses unfinished ones");
    assert_eq!(
        engine.due_timers(10, usize::MAX).await?.len(),
        3,
        "timer vanished"
    );
    assert_eq!(
        engine.pending_outbox(64).await?.len(),
        3,
        "pending call vanished"
    );
    assert_eq!(
        engine.signals_from(0, 64).await?.len(),
        3,
        "signal waiting to be sent vanished"
    );
    assert_eq!(
        engine.inbound_from(0, 64).await?.len(),
        3,
        "acked message vanished"
    );
    assert_eq!(
        engine.due_retries(10, usize::MAX).await?.len(),
        1,
        "scheduled retry vanished"
    );
    // 3, not 4: a finished invocation leaves the queue **within** its ending
    // commit.
    assert_eq!(engine.all_vqueues().await?.len(), 3, "queues are empty");
    engine.seal().await?;
    Ok(())
}

/// `rebuild_derived_restores_deployment_index`: after the derived zone is
/// deleted (split/merge), the count "who still runs on this deployment" is
/// still right — ended invocations are not counted.
#[tokio::test]
async fn rebuild_derived_restores_deployment_index() -> Result<()> {
    let root = fresh_root("deployment-index")?;
    let store = store_at(&root)?;
    let mut running = scenario(&store).await?;
    {
        let engine = open_engine(&store).await?;
        let (n, _) = engine.pinned_to(DEPLOYMENT, 10).await?;
        assert_eq!(
            n, 3,
            "3 unfinished invocations pinned to the deployment, not the finished one"
        );
        engine.seal().await?;
    }
    wipe_derived(&store).await?;
    let engine = open_engine(&store).await?;
    let (n, mut ids) = engine.pinned_to(DEPLOYMENT, 10).await?;
    ids.sort();
    running.sort();
    assert_eq!(n, 3);
    assert_eq!(ids, running);
    assert_eq!(engine.pinned_counts().await?.get(DEPLOYMENT), Some(&3));
    engine.seal().await?;
    Ok(())
}

/// The rebuild scans only the **hot zone** — the journal and state (bulk zone)
/// are not read, so the cost is proportional to the number of small rows, not
/// to history. And a matching mark rebuilds nothing at all.
#[tokio::test]
async fn rebuild_does_not_read_bulk_region() -> Result<()> {
    let root = fresh_root("no-bulk")?;
    let store = store_at(&root)?;
    scenario(&store).await?;
    wipe_derived(&store).await?;

    let engine = open_engine(&store).await?;
    let used = engine.writer().scan_counts();
    assert!(used.hot > 0, "the rebuild must scan the hot zone: {used:?}");
    assert_eq!(used.bulk, 0, "the rebuild read the bulk zone: {used:?}");
    engine.seal().await?;

    // The mark exists and matches ⇒ reopening scans no entity zone.
    let engine = open_engine(&store).await?;
    let used = engine.writer().scan_counts();
    assert_eq!(
        (used.hot, used.bulk),
        (0, 0),
        "mark matches but still rebuilt: {used:?}"
    );
    engine.seal().await?;
    Ok(())
}

/// An index row is a **hint**: an `Index::Timer` row without an entity row is
/// not fired, and is cleaned up.
#[tokio::test]
async fn stale_index_row_is_ignored() -> Result<()> {
    let root = fresh_root("stale")?;
    let store = store_at(&root)?;
    let mut engine = open_engine(&store).await?;
    let ghost = make_invocation_id(pk_cart(), [7; 16]);
    let mut m = Mutations::new();
    m.raw_put(keys::ix_timer(5, &ghost, 1), b"");
    engine.writer_mut().commit(m).await?;

    assert!(
        engine.due_timers(10, usize::MAX).await?.is_empty(),
        "ghost timer fired"
    );
    engine.seal().await?;
    let left: Vec<_> = derived(&store)
        .await?
        .into_iter()
        .filter(|(k, _)| keys::parse_ix_timer(k).is_some())
        .collect();
    assert!(
        left.is_empty(),
        "the stale index row was not cleaned up: {left:?}"
    );
    Ok(())
}

/// Queue index rows of two invocations with the same `seq` (two Dbs merged) do
/// not overwrite each other — and the rebuild keeps both.
#[tokio::test]
async fn vqueue_index_keeps_both_rows_with_same_seq() -> Result<()> {
    let root = fresh_root("vq-same-seq")?;
    let store = store_at(&root)?;
    let mut engine = open_engine(&store).await?;
    let q = queue().encode();
    let a = make_invocation_id(pk_cart(), [1; 16]);
    let b = make_invocation_id(pk_cart(), [2; 16]);
    let mut m = Mutations::new();
    m.push_vqueue(&q, Stage::Inbox, 1, &a)
        .push_vqueue(&q, Stage::Inbox, 1, &b);
    engine.writer_mut().commit(m).await?;
    let rows = engine.writer().vqueue_entries_unbounded(&q).await?;
    assert_eq!(rows.len(), 2, "{rows:?}");
    engine.seal().await?;
    Ok(())
}
