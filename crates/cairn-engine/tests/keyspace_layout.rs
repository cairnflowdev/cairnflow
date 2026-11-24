// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The entity/derived keyspace on real storage.
//!
//! Partitioning by range is only correct if **every row lives under the
//! partition key of its local owner**: projecting a Db onto the range of one
//! pk must keep every row of that pk and let in no row of another pk. Two
//! places used to be wrong: the outbox's `Reply` rows were keyed by the
//! recipient, and five of the six inbound kinds were keyed by the sender —
//! after a split, neither sender nor recipient saw them, while the sender had
//! already deleted its outbox ⇒ an acked message was lost.
//!
//! The projection here is SlateDB's real projection (F4: several ranges per
//! source), not a hand-written key filter — the test checks exactly what a
//! split will do.

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
        CompletionPayload, CreateOptions, DeliverCancel, DeliverCompletion, DeliverSignal, Engine,
        InterPartition, Invocation, InvocationStatus, ReplyTo, SignalId, StartCall, Target, Watch,
    },
    cairn_protocol::{encode_message, pb, Flags, Frame, FrameDecoder, MessageType},
    cairn_runtime::{Event, StartParams},
    cairn_store::{
        keys::{self, InboundKind, Table},
        make_invocation_id, partition_key_for_target, LeadershipGrant, PartitionId, PartitionKey,
        PartitionWriter, Scope,
    },
    slatedb::{
        admin::AdminBuilder,
        config::{FlushOptions, FlushType, Settings},
        object_store::{local::LocalFileSystem, ObjectStore},
        CloneSourceSpec, Db,
    },
    std::{collections::BTreeSet, ops::Bound, path::PathBuf, sync::Arc, time::Duration},
};

const DB: &str = "partition-0";

fn frame<M: prost::Message>(ty: MessageType, m: &M) -> Frame {
    let wire = encode_message(ty, Flags::NONE, m);
    let mut buf = bytes::BytesMut::from(&wire[..]);
    FrameDecoder::new().decode(&mut buf).unwrap().unwrap()
}

fn hex(id: &[u8; 24]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

fn fresh_root(name: &str) -> Result<PathBuf> {
    let root = std::env::temp_dir().join(format!("cairnflow-keyspace-{name}"));
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

fn id_at(pk: PartitionKey, n: u8) -> [u8; 24] {
    make_invocation_id(pk, [n; 16])
}

fn obj(scope: Option<&Scope>, service: &str, key: &str) -> PartitionKey {
    partition_key_for_target(&cairn_store::TenantId::default(), scope, service, key, None)
        .expect("has a scope or an object key")
}

async fn create(
    engine: &mut Engine,
    id: [u8; 24],
    target: &Target,
    reply_to: Option<&ReplyTo>,
    idem: Option<&str>,
) -> Result<Invocation> {
    let mut inv = engine
        .create(
            id,
            target,
            CreateOptions {
                reply_to,
                idempotency_key: idem,
                ..Default::default()
            },
            StartParams::default(),
            Bytes::new(),
        )
        .await?;
    engine.step(&mut inv, Event::Start).await?;
    Ok(inv)
}

fn set_state(key: &'static str) -> Frame {
    frame(
        MessageType::SET_STATE,
        &pb::SetStateCommandMessage {
            key: Bytes::from_static(key.as_bytes()),
            value: Some(cairn_protocol::value::inline(Bytes::from_static(b"1"))),
            name: String::new(),
        },
    )
}

fn output() -> Frame {
    frame(
        MessageType::OUTPUT,
        &pb::OutputCommandMessage {
            result: Some(pb::output_command_message::Result::Value(
                cairn_protocol::value::inline(Bytes::from_static(b"\"done\"")),
            )),
            name: String::new(),
        },
    )
}

/// The byte ranges a partition holding pk `[pk, pk+1)` owns — exactly the
/// keyspace layout: one range in the hot zone `0x00`, one in the bulk zone
/// `0x02`. Written by hand on purpose: the test is the specification, it must
/// not borrow functions of the very thing it checks.
fn ranges_of(pk: PartitionKey) -> Vec<(Bound<Bytes>, Bound<Bytes>)> {
    [0x00u8, 0x02]
        .into_iter()
        .map(|region| {
            let at = |p: u64| {
                let mut v = vec![region];
                v.extend_from_slice(&p.to_be_bytes());
                Bytes::from(v)
            };
            let end = match pk.0.checked_add(1) {
                Some(next) => Bound::Excluded(at(next)),
                None => Bound::Excluded(Bytes::from(vec![region + 1])),
            };
            (Bound::Included(at(pk.0)), end)
        })
        .collect()
}

/// Flushes the memtable to L0 and then closes — a clone (F3) rejects a source with data only in the WAL.
async fn settle(store: &Arc<dyn ObjectStore>) -> Result<()> {
    let db = Db::builder(DB, store.clone())
        .with_settings(settings())
        .build()
        .await?;
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await?;
    db.close().await?;
    Ok(())
}

async fn scan_keys(store: &Arc<dyn ObjectStore>, path: &str) -> Result<Vec<Bytes>> {
    let db = Db::builder(path, store.clone())
        .with_settings(settings())
        .build()
        .await?;
    let mut it = db.scan::<std::ops::RangeFull>(..).await?;
    let mut out = vec![];
    while let Some(kv) = it.next().await? {
        out.push(kv.key);
    }
    db.close().await?;
    Ok(out)
}

/// Every key the child Db (the projection onto `pk`'s range) sees.
async fn projected(store: &Arc<dyn ObjectStore>, pk: PartitionKey) -> Result<Vec<Bytes>> {
    let child = format!("child-{:016x}", pk.0);
    AdminBuilder::new(child.as_str(), store.clone())
        .build()
        .create_clone_builder_from_source(
            CloneSourceSpec::new(DB).with_projection_ranges(ranges_of(pk)),
        )
        .build()
        .await?;
    scan_keys(store, &child).await
}

fn tables(keys: &[Bytes]) -> BTreeSet<u8> {
    keys.iter()
        .filter_map(|k| keys::table_of(k).map(|t| t as u8))
        .collect()
}

/// **The keyspace's exit criterion.** Build a Db with every kind of entity row
/// — replies to waiters in another partition, all six kinds of unprocessed
/// inbound, state and promises of scoped and unscoped objects, idempotency,
/// attachments, signals, delayed calls, timers, retries, scheduled purges —
/// then project onto the range of each pk:
///
/// 1. each pk sees all of its rows;
/// 2. a pk only *mentioned* in keys (a reply's recipient, an inbound's sender)
///    sees **nothing**;
/// 3. the children together see exactly every entity row of the parent — no
///    row lives under an owner outside the list.
#[tokio::test]
async fn projection_keeps_every_owned_row() -> Result<()> {
    let root = fresh_root("projection")?;
    let store = store_at(&root)?;
    let mut engine = open_engine(&store).await?;

    let bob = Scope::new("bob").unwrap();
    // Object `Cart/k1` — its invocations live under the object's pk.
    let cart = Target {
        tenant: Default::default(),
        scope: None,
        service: "Cart".into(),
        handler: "add".into(),
        object_key: "k1".into(),
    };
    let pk_o = obj(None, "Cart", "k1");
    let o = id_at(pk_o, 1);
    // The same object key, with a scope ⇒ a different object, the scope's pk.
    let scoped = Target {
        tenant: Default::default(),
        scope: Some(bob.clone()),
        ..cart.clone()
    };
    let pk_s = obj(Some(&bob), "Cart", "k1");
    let s = id_at(pk_s, 2);
    // An unkeyed service with an idempotency key ⇒ the pk of the key set.
    let plain = Target {
        tenant: Default::default(),
        scope: None,
        service: "Mailer".into(),
        handler: "send".into(),
        object_key: String::new(),
    };
    let pk_i = partition_key_for_target(
        &cairn_store::TenantId::default(),
        None,
        "Mailer",
        "",
        Some("idem-1"),
    )
    .unwrap();
    let i = id_at(pk_i, 3);
    // Reply producer X and waiter W — W is in another partition and has no rows.
    let pk_x = PartitionKey(0x1111_0000_0000_0000);
    let pk_w = PartitionKey(0x2222_0000_0000_0000);
    let x = id_at(pk_x, 4);
    let w = id_at(pk_w, 5);
    // Cross-partition messages: target D (ours), sender R (another partition).
    let pk_d = PartitionKey(0x3333_0000_0000_0000);
    let pk_r = PartitionKey(0x4444_0000_0000_0000);
    let d = id_at(pk_d, 6);
    let r = id_at(pk_r, 7);
    // Attachment target T, the attacher in another partition (W).
    let pk_t = PartitionKey(0x5555_0000_0000_0000);
    let t = id_at(pk_t, 8);

    // O: state, a completed promise, waiting for another promise, a timer, a
    // call, sending a signal, a delayed call.
    let mut inv_o = create(&mut engine, o, &cart, None, None).await?;
    for f in [
        set_state("s"),
        frame(
            MessageType::COMPLETE_PROMISE,
            &pb::CompletePromiseCommandMessage {
                key: "p1".into(),
                completion: Some(
                    pb::complete_promise_command_message::Completion::CompletionValue(
                        cairn_protocol::value::inline(Bytes::from_static(b"1")),
                    ),
                ),
                result_completion_id: 1,
                name: String::new(),
            },
        ),
        frame(
            MessageType::GET_PROMISE,
            &pb::GetPromiseCommandMessage {
                key: "p2".into(),
                result_completion_id: 2,
                name: String::new(),
            },
        ),
        frame(
            MessageType::SLEEP,
            &pb::SleepCommandMessage {
                wake_up_time: u64::MAX / 2,
                result_completion_id: 3,
                name: String::new(),
            },
        ),
        frame(
            MessageType::CALL,
            &pb::CallCommandMessage {
                service_name: "Payment".into(),
                handler_name: "charge".into(),
                key: "acct".into(),
                result_completion_id: 4,
                invocation_id_notification_idx: 5,
                ..Default::default()
            },
        ),
        frame(
            MessageType::SEND_SIGNAL,
            &pb::SendSignalCommandMessage {
                target_invocation_id: hex(&w),
                signal_id: Some(pb::send_signal_command_message::SignalId::Name("go".into())),
                result: Some(pb::send_signal_command_message::Result::Void(pb::Void {})),
                entry_name: String::new(),
            },
        ),
        frame(
            MessageType::ONE_WAY_CALL,
            &pb::OneWayCallCommandMessage {
                service_name: "Mailer".into(),
                handler_name: "send".into(),
                invoke_time: u64::MAX / 2,
                invocation_id_notification_idx: 6,
                ..Default::default()
            },
        ),
    ] {
        engine.step(&mut inv_o, Event::FromSdk(f)).await?;
    }
    engine.schedule_retry(&o, u64::MAX / 2, 1, 3).await?;

    // S: state of a scoped object.
    let mut inv_s = create(&mut engine, s, &scoped, None, None).await?;
    engine
        .step(&mut inv_s, Event::FromSdk(set_state("s")))
        .await?;

    // I: an idempotency row.
    create(&mut engine, i, &plain, None, Some("idem-1")).await?;

    // X answers W and then ends ⇒ a Reply row, and a scheduled purge.
    let reply_to = ReplyTo {
        caller: w,
        completion_id: 9,
    };
    let mut inv_x = create(&mut engine, x, &plain, Some(&reply_to), None).await?;
    engine.step(&mut inv_x, Event::FromSdk(output())).await?;
    engine
        .step(
            &mut inv_x,
            Event::FromSdk(frame(MessageType::END, &pb::EndMessage {})),
        )
        .await?;
    assert!(matches!(
        engine.status(&x).await?,
        Some(InvocationStatus::Completed(_))
    ));

    // T has a waiter in another partition.
    create(&mut engine, t, &plain, None, None).await?;
    engine.attach(&t, &w, 3).await?;

    // Six kinds of messages to D, all unprocessed.
    for msg in [
        InterPartition::StartCall(StartCall {
            callee: d,
            service: "Mailer".into(),
            handler: "send".into(),
            object_key: String::new(),
            parameter: Bytes::new(),
            reply_to: Some((r, 1)),
            idempotency_key: None,
            scope: None,
            limit_key: None,
            tenant: None,
            sender: (r, 2),
            schedule: None,
            lineage: Default::default(),
        }),
        InterPartition::Completion(DeliverCompletion {
            target: d,
            completion_id: 3,
            result: CompletionPayload::Void,
        }),
        InterPartition::Attach(Watch {
            target: d,
            caller: r,
            completion_id: 4,
            tenant: None,
        }),
        InterPartition::PeekOutput(Watch {
            target: d,
            caller: r,
            completion_id: 5,
            tenant: None,
        }),
        InterPartition::Signal(DeliverSignal {
            target: d,
            sender: r,
            entry_index: 6,
            signal: SignalId::Name("go".into()),
            result: CompletionPayload::Void,
            tenant: None,
        }),
        InterPartition::Cancel(DeliverCancel {
            target: d,
            parent: r,
            id_completion: 7,
        }),
    ] {
        engine.accept_message(&msg).await?;
    }
    // O's durable stream chunks: bulk zone, the owner is the pk
    // in the id.
    let mut muts = cairn_store::Mutations::new();
    muts.put_stream_chunk(&o, "$log", 0, &keys::StreamChunk::default());
    // O's artifact row: hot zone, same owner.
    muts.put_artifact(
        &o,
        &[1; 32],
        &keys::ArtifactRow {
            epoch: 1,
            size: 1,
            content_type: String::new(),
        },
    );
    engine.writer_mut().commit(muts).await?;

    engine.seal().await?;
    settle(&store).await?;
    let all = scan_keys(&store, DB).await?;

    // ── 1. Every owner sees all of its rows.
    let seen_o = projected(&store, pk_o).await?;
    let want_o = [
        Table::InvocationStatus,
        Table::Journal,
        Table::State,
        Table::Promises,
        Table::PromiseWaiters,
        Table::Timers,
        Table::Outbox,
        Table::SignalOutbox,
        Table::Delayed,
        Table::Retries,
        Table::Streams,
        Table::Artifacts,
    ];
    let got = tables(&seen_o);
    for t in want_o {
        assert!(
            got.contains(&(t as u8)),
            "the object's pk is missing row {t:?}: {got:?}"
        );
    }
    let seen_s = projected(&store, pk_s).await?;
    assert!(
        seen_s
            .iter()
            .any(|k| keys::parse_state_user_key(k) == Some(&b"s"[..])),
        "a scoped object's state must live under the scope's pk"
    );
    let seen_i = projected(&store, pk_i).await?;
    assert!(tables(&seen_i).contains(&(Table::Idempotency as u8)));
    let seen_x = projected(&store, pk_x).await?;
    assert!(
        seen_x.iter().any(|k| keys::parse_outbox(k) == Some((w, 9))),
        "Reply rows must live under the PRODUCER's pk"
    );
    assert!(
        tables(&seen_x).contains(&(Table::Purges as u8)),
        "X's scheduled purge"
    );
    let seen_t = projected(&store, pk_t).await?;
    assert!(seen_t
        .iter()
        .any(|k| keys::parse_attachment(k) == Some((w, 3))));
    let seen_d = projected(&store, pk_d).await?;
    let kinds: BTreeSet<u8> = seen_d
        .iter()
        .filter_map(|k| keys::parse_inbound(k).map(|(kind, _, _)| kind as u8))
        .collect();
    assert_eq!(
        kinds,
        [
            InboundKind::StartCall,
            InboundKind::Completion,
            InboundKind::Attach,
            InboundKind::PeekOutput,
            InboundKind::Signal,
            InboundKind::Cancel,
        ]
        .into_iter()
        .map(|k| k as u8)
        .collect(),
        "all six inbound kinds must live under the TARGET's pk"
    );

    // ── 2. A pk only mentioned in keys sees nothing.
    assert_eq!(
        projected(&store, pk_w).await?,
        Vec::<Bytes>::new(),
        "waiter W"
    );
    assert_eq!(
        projected(&store, pk_r).await?,
        Vec::<Bytes>::new(),
        "sender R"
    );

    // ── 3. Together exactly every entity row of the parent.
    let union: BTreeSet<Bytes> = [&seen_o, &seen_s, &seen_i, &seen_x, &seen_t, &seen_d]
        .into_iter()
        .flatten()
        .cloned()
        .collect();
    let entity: BTreeSet<Bytes> = all
        .iter()
        .filter(|k| matches!(k.first(), Some(0x00) | Some(0x02)))
        .cloned()
        .collect();
    assert!(
        !entity.is_empty(),
        "there are no entity rows in zones 0x00/0x02"
    );
    let orphans: Vec<_> = entity.difference(&union).collect();
    assert!(
        orphans.is_empty(),
        "{} entity rows live under an owner outside the list: {orphans:?}",
        orphans.len()
    );
    Ok(())
}

/// **Becoming leader reads only the derived zone.** The path to leadership
/// (`recover` + `restore_flow` + outbox seeding + the timer scan) scans only
/// the **derived zone**. Status rows are scattered among the journal once the
/// pk comes before the table code; scanning them on every failover would scan
/// all the data.
#[tokio::test]
async fn recover_reads_only_derived_region() -> Result<()> {
    let root = fresh_root("recover")?;
    let store = store_at(&root)?;
    let mut engine = open_engine(&store).await?;
    let cart = Target {
        tenant: Default::default(),
        scope: None,
        service: "Cart".into(),
        handler: "add".into(),
        object_key: "k1".into(),
    };
    let pk_o = obj(None, "Cart", "k1");
    let mut running = Vec::new();
    for n in 1..=3u8 {
        let id = id_at(pk_o, n);
        let mut inv = create(&mut engine, id, &cart, None, None).await?;
        engine
            .step(&mut inv, Event::FromSdk(set_state("s")))
            .await?;
        engine
            .step(
                &mut inv,
                Event::FromSdk(frame(
                    MessageType::SLEEP,
                    &pb::SleepCommandMessage {
                        wake_up_time: 1,
                        result_completion_id: 1,
                        name: String::new(),
                    },
                )),
            )
            .await?;
        running.push(id);
    }
    let done = id_at(pk_o, 9);
    let mut inv = create(&mut engine, done, &cart, None, None).await?;
    engine.step(&mut inv, Event::FromSdk(output())).await?;
    engine
        .step(
            &mut inv,
            Event::FromSdk(frame(MessageType::END, &pb::EndMessage {})),
        )
        .await?;
    engine.seal().await?;

    let mut engine = open_engine(&store).await?;
    let before = engine.writer().scan_counts();
    let mut unfinished = engine.unfinished_invocations_unbounded().await?;
    engine.all_vqueues().await?;
    engine.outbox_high_water().await?;
    let timers = engine.due_timers(u64::MAX / 2, usize::MAX).await?;
    let used = engine.writer().scan_counts().since(&before);

    unfinished.sort();
    assert_eq!(
        unfinished, running,
        "recover must see exactly the unfinished invocations"
    );
    assert_eq!(timers.len(), 3, "three timers due");
    assert_eq!(used.hot, 0, "recover scanned the hot entity zone: {used:?}");
    assert_eq!(used.bulk, 0, "recover scanned the bulk zone: {used:?}");
    assert!(used.derived > 0, "recover reads derived zone: {used:?}");
    engine.seal().await?;
    Ok(())
}

/// **D-a.** The state of an **unkeyed, unscoped** object has no pk to own it —
/// previously each partition kept its own copy, silently wrong. Touching it is
/// a terminal error, not a made-up owner.
#[tokio::test]
async fn unkeyed_unscoped_state_fails_terminally() -> Result<()> {
    let root = fresh_root("unkeyed-state")?;
    let store = store_at(&root)?;
    let mut engine = open_engine(&store).await?;
    let plain = Target {
        tenant: Default::default(),
        scope: None,
        service: "Mailer".into(),
        handler: "send".into(),
        object_key: String::new(),
    };
    let id = id_at(PartitionKey(42), 1);
    let mut inv = create(&mut engine, id, &plain, None, None).await?;
    engine
        .step(&mut inv, Event::FromSdk(set_state("s")))
        .await?;
    match engine.status(&id).await? {
        Some(InvocationStatus::Failed { code, .. }) => {
            assert_eq!(code, cairn_engine::NO_OBJECT_KEY)
        }
        other => panic!("ownerless state must fail the invocation for good, got {other:?}"),
    }
    engine.seal().await?;
    Ok(())
}

/// **D-a**, the external path: a promise of an unkeyed, unscoped object is
/// rejected — no partition is the right home for it.
#[tokio::test]
async fn unkeyed_unscoped_promise_is_refused_from_outside() -> Result<()> {
    let root = fresh_root("unkeyed-promise")?;
    let store = store_at(&root)?;
    let mut engine = open_engine(&store).await?;
    let r = engine
        .complete_promise_external(
            &cairn_store::TenantId::default(),
            None,
            "Mailer",
            "",
            "p",
            cairn_engine::ReplyResult::Value(Bytes::from_static(b"1")),
        )
        .await;
    assert!(
        r.is_err(),
        "completing an ownerless promise must be rejected: {r:?}"
    );
    let r = engine
        .peek_promise_external(&cairn_store::TenantId::default(), None, "Mailer", "", "p")
        .await;
    assert!(r.is_err(), "ownerless promise read allowed: {r:?}");
    engine.seal().await?;
    Ok(())
}

/// A scope without an object key **does** have an owner — the scope's pk. D-a
/// must not block this case by mistake.
#[tokio::test]
async fn scoped_unkeyed_state_is_allowed() -> Result<()> {
    let root = fresh_root("unkeyed-scoped")?;
    let store = store_at(&root)?;
    let mut engine = open_engine(&store).await?;
    let target = Target {
        tenant: Default::default(),
        scope: Some(Scope::new("bob").unwrap()),
        service: "Mailer".into(),
        handler: "send".into(),
        object_key: String::new(),
    };
    let id = id_at(PartitionKey(42), 1);
    let mut inv = create(&mut engine, id, &target, None, None).await?;
    engine
        .step(&mut inv, Event::FromSdk(set_state("s")))
        .await?;
    assert_eq!(engine.status(&id).await?, Some(InvocationStatus::Running));
    engine.seal().await?;
    Ok(())
}
