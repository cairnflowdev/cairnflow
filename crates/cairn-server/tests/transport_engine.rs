// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Sending a cross-partition message **must not hold the engine**.
//!
//! The receiving side of a cross-partition message must take *its own* engine
//! to write the message down before it acks. If the sending side held its
//! engine for the whole send, two partitions sending to each other at the same
//! time would wait for each other forever: A holds engine A waiting for B's ack,
//! B holds engine B waiting for A's ack, and each side's receive handler waits
//! for exactly the engine that is being held. The HTTP client has no time limit,
//! so nothing breaks that cycle.
//!
//! The test here measures exactly that link, deterministically: partition 1 is
//! a fake server. When it receives a message from partition 0, before acking it
//! asks partition 0 a question that needs the engine (`status`). If the sending
//! side still holds the engine ⇒ that question times out.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterminism"
)]

use {
    anyhow::Result,
    axum::{extract::State, http::StatusCode, routing::post, Router},
    cairn_engine::{Engine, InterPartition, Watch},
    cairn_server::{
        actor::{EngineHandle, Leadership, Readiness, TurnLimits},
        routing::RoutingTable,
        transport::Transport,
        Registry,
    },
    cairn_store::{
        make_invocation_id, ClusterConfig, LeadershipGrant, PartitionId, PartitionKey,
        PartitionWriter,
    },
    slatedb::{
        config::Settings,
        object_store::{memory::InMemory, path::Path, ObjectStore},
        Db,
    },
    std::{
        sync::{Arc, Mutex},
        time::Duration,
    },
};

/// Fake partition 1: receives a message, asks partition 0 back a question that
/// needs the engine, and only then acks. Records whether that question was
/// answered.
#[derive(Clone)]
struct Stub {
    p0: Arc<Mutex<Option<EngineHandle>>>,
    answered: Arc<Mutex<Vec<bool>>>,
}

async fn on_inbound(State(stub): State<Stub>) -> StatusCode {
    let handle = stub.p0.lock().unwrap().clone();
    let ok = match handle {
        Some(h) => tokio::time::timeout(
            Duration::from_secs(2),
            h.status(make_invocation_id(PartitionKey(0), [9; 16])),
        )
        .await
        .is_ok(),
        None => false,
    };
    stub.answered.lock().unwrap().push(ok);
    StatusCode::OK
}

#[tokio::test(flavor = "multi_thread")]
async fn transport_send_does_not_hold_engine() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let prefix = Path::from("");
    let cluster = ClusterConfig::open(&store, &prefix, 2).await?;

    // The fake server for partition 1.
    let stub = Stub {
        p0: Arc::new(Mutex::new(None)),
        answered: Arc::new(Mutex::new(Vec::new())),
    };
    let app = Router::new()
        .route("/internal/inbound", post(on_inbound))
        .with_state(stub.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let stub_url = format!("http://{}", listener.local_addr()?);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    // The routing table declares partition 1 as belonging to the fake server.
    let routing = RoutingTable::new();
    routing.insert(
        PartitionId(1),
        cairn_server::routing::Leader {
            node_id: "stub".into(),
            address: Some(stub_url.clone()),
            ingress: None,
            term: 1,
        },
    );

    // The real partition 0.
    let db_store = Arc::clone(&store);
    let engine_cluster = cluster.clone();
    let handle = EngineHandle::spawn_with_transport(
        Box::new(move || {
            let store = Arc::clone(&db_store);
            let cluster = engine_cluster.clone();
            Box::pin(async move {
                let db = Db::builder("partition-0", store)
                    .with_settings(Settings {
                        flush_interval: Some(Duration::from_millis(10)),
                        ..Default::default()
                    })
                    .build()
                    .await
                    .expect("open SlateDB");
                let writer =
                    PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db).await?;
                Ok(Leadership {
                    engine: Engine::open_in_cluster(writer, cluster).await?,
                    keeper: None,
                })
            })
        }),
        Registry::new(),
        None,
        Readiness::WhenLeader,
        Transport::new(reqwest::Client::new(), routing, cluster.clone()),
        TurnLimits::default(),
        None,
    )?;
    *stub.p0.lock().unwrap() = Some(handle.clone());

    // A `PeekOutput` message to partition 0, asking about an invocation that does
    // not exist; the answer must go back to the asker in partition 1 — that is, a
    // cross-partition send from partition 0.
    handle
        .inbound(InterPartition::PeekOutput(Watch {
            target: make_invocation_id(PartitionKey(0), [1; 16]),
            caller: make_invocation_id(PartitionKey(u64::MAX), [2; 16]),
            completion_id: 1,
            tenant: None,
        }))
        .await?;
    // The partition's drainer processes the `inbound` row as soon as the message
    // arrives and sends the answer to partition 1 — wait until the fake server gets it.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while stub.answered.lock().unwrap().is_empty() && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let answered = stub.answered.lock().unwrap().clone();
    assert!(
        !answered.is_empty(),
        "partition 0 sent no PeekOutput answer to partition 1"
    );
    assert!(
        answered.iter().all(|ok| *ok),
        "while partition 0 was sending a message, a question that needs its engine was not \
         answered within 2s — the sending side holds the engine across the send. Two \
         partitions sending to each other at the same time would deadlock forever"
    );
    Ok(())
}

/// The fake server of an **old binary**: only `/internal/inbound`, no batch
/// route — the router returns `404` for every other path, like a binary from
/// before batched sends.
#[derive(Clone, Default)]
struct OldBinary {
    /// `completion_id` of each `Completion` message received over the single-message route.
    singles: Arc<Mutex<Vec<u32>>>,
    /// Unknown paths that were asked (each batch-route probe is one entry).
    probes: Arc<Mutex<Vec<String>>>,
}

async fn old_inbound(State(old): State<OldBinary>, body: axum::body::Bytes) -> StatusCode {
    // As slow as a durable write: while one message is in flight, the following
    // rows pile up into a batch on the next drain round.
    tokio::time::sleep(Duration::from_millis(100)).await;
    match InterPartition::decode(body) {
        Ok(InterPartition::Completion(c)) => {
            old.singles.lock().unwrap().push(c.completion_id);
            StatusCode::OK
        }
        _ => StatusCode::BAD_REQUEST,
    }
}

async fn old_fallback(State(old): State<OldBinary>, uri: axum::http::Uri) -> StatusCode {
    old.probes.lock().unwrap().push(uri.path().to_string());
    StatusCode::NOT_FOUND
}

/// Wait until the fake server has received results `first..first+6`.
async fn wait_arrived(old: &OldBinary, first: u32) -> Result<()> {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let got = old.singles.lock().unwrap().clone();
        if (first..first + 6).all(|cid| got.contains(&cid)) {
            return Ok(());
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "results {first}..{} not at the fake server yet: {got:?}",
            first + 6
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// **The destination is an old binary: probe once, remember, send one message
/// at a time.**
///
/// Order:
/// 1. Range 0 is real; range 1 is [`OldBinary`] (no batch route).
/// 2. Six `Attach` messages to range 0, each asking about a missing invocation,
///    with the waiter in range 1 ⇒ six outbox `Reply` rows to range 1. The fake
///    server answers slowly, so the rows pile up and the drainer sends them in
///    batches.
/// 3. The first batch gets `404` on the batch route ⇒ `Transport` remembers
///    that address and sends one message at a time within the same send; every
///    later batch to the same address goes straight one by one.
/// 4. The routing table changes (range 1's leader moves to a new term, same
///    address) ⇒ forget what was remembered, the next batch probes again exactly
///    once.
///
/// Must see: every result reaches the fake server over the single-message
/// route; exactly **one** batch-route probe before the table changes and
/// exactly one after. Before the fix: no probe at all.
#[tokio::test(flavor = "multi_thread")]
async fn batches_to_an_old_binary_fall_back_to_single_messages() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let prefix = Path::from("");
    let cluster = ClusterConfig::open(&store, &prefix, 2).await?;

    let old = OldBinary::default();
    let app = Router::new()
        .route("/internal/inbound", post(old_inbound))
        .fallback(old_fallback)
        .with_state(old.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let old_url = format!("http://{}", listener.local_addr()?);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let leader = |term| cairn_server::routing::Leader {
        node_id: "old".into(),
        address: Some(old_url.clone()),
        ingress: None,
        term,
    };
    let routing = RoutingTable::new();
    routing.insert(PartitionId(1), leader(1));

    let db_store = Arc::clone(&store);
    let engine_cluster = cluster.clone();
    let handle = EngineHandle::spawn_with_transport(
        Box::new(move || {
            let store = Arc::clone(&db_store);
            let cluster = engine_cluster.clone();
            Box::pin(async move {
                let db = Db::builder("partition-0", store)
                    .with_settings(Settings {
                        flush_interval: Some(Duration::from_millis(10)),
                        ..Default::default()
                    })
                    .build()
                    .await
                    .expect("open SlateDB");
                let writer =
                    PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db).await?;
                Ok(Leadership {
                    engine: Engine::open_in_cluster(writer, cluster).await?,
                    keeper: None,
                })
            })
        }),
        Registry::new(),
        None,
        Readiness::WhenLeader,
        Transport::new(reqwest::Client::new(), routing.clone(), cluster.clone()),
        TurnLimits::default(),
        None,
    )?;

    // Six messages asking range 0, waiter in range 1; `completion_id` from `first`.
    let burst = |first: u32| {
        let handle = handle.clone();
        async move {
            for cid in first..first + 6 {
                handle
                    .inbound(InterPartition::Attach(Watch {
                        target: make_invocation_id(PartitionKey(0), [1; 16]),
                        caller: make_invocation_id(PartitionKey(u64::MAX), [2; 16]),
                        completion_id: cid,
                        tenant: None,
                    }))
                    .await?;
            }
            anyhow::Ok(())
        }
    };
    burst(1).await?;
    wait_arrived(&old, 1).await?;
    let probes = old.probes.lock().unwrap().clone();
    anyhow::ensure!(
        probes == ["/internal/inbound/batch"],
        "must probe the batch route exactly once, then remember: {probes:?}"
    );

    // Range 1's leader moves to a new term ⇒ the routing table changes ⇒ probe again.
    routing.insert(PartitionId(1), leader(2));
    burst(101).await?;
    wait_arrived(&old, 101).await?;
    let probes = old.probes.lock().unwrap().clone();
    anyhow::ensure!(
        probes.len() == 2,
        "a routing table change probes again exactly once: {probes:?}"
    );
    Ok(())
}
