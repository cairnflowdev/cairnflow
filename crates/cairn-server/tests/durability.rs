// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! INV-5 at the server layer: **no answer may run ahead of the durability of what it
//! says.**
//!
//! The old bug: `Lease::sync` *took away* the writer's pending WAL handle. A second
//! task arriving later found the slot empty and got its ticket at once — while the
//! data it had just read (SlateDB reads at `Memory` level by default) was not yet
//! durable. A status query could therefore return an invocation that, had the process
//! died at that moment, would never have existed.
//!
//! How it is measured (without relying on the real clock): every WAL PUT goes through
//! a `FaultyStore` gate. Close the gate, `/send` creates an invocation — its write
//! stands at the gate (`Gate::blocked` ≥ 1), i.e. written but not durable. Meanwhile
//! another query asks about that very invocation: it **must not** be answered while
//! the gate is closed, and must be answered (seeing the invocation) as soon as the
//! gate opens.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

use {
    anyhow::{bail, ensure, Result},
    bytes::Bytes,
    cairn_engine::{Engine, Target},
    cairn_server::{
        actor::{EngineHandle, Leadership, Readiness, TurnLimits},
        Registry,
    },
    cairn_store::{
        testkit::{Action, FaultyStore, Gate, Rule},
        LeadershipGrant, PartitionId, PartitionWriter,
    },
    slatedb::{
        config::Settings,
        object_store::{memory::InMemory, ObjectStore},
        Db,
    },
    std::{sync::Arc, time::Duration},
};

/// The window for observing "the query has not answered while the gate is closed".
/// Not "surely done by now": a query answering early (wrongly) answers within a few ms.
const HELD: Duration = Duration::from_millis(300);
/// The bound for every awaited event in the file.
const LIMIT: Duration = Duration::from_secs(10);

fn spawn_runtime(store: Arc<FaultyStore>) -> Result<EngineHandle> {
    EngineHandle::spawn_with_transport(
        Box::new(move || {
            let store: Arc<dyn ObjectStore> = store.clone();
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
                    engine: Engine::open(writer).await?,
                    keeper: None,
                })
            })
        }),
        Registry::new(),
        None,
        Readiness::WhenLeader,
        cairn_server::transport::Transport::disconnected(),
        TurnLimits::default(),
        None,
    )
    .map_err(Into::into)
}

fn target() -> Target {
    Target {
        tenant: Default::default(),
        scope: None,
        service: "Svc".into(),
        handler: "work".into(),
        object_key: String::new(),
    }
}

fn id(n: u8) -> [u8; 24] {
    let mut b = [0u8; 24];
    b[0] = 0; // partition 0
    b[15] = n;
    b
}

/// The interleaving shared by both tests:
/// 1. The engine opens; the first `status` answers ⇒ every write made while opening is durable.
/// 2. Close the WAL PUT gate; `submit` invocation `inv` — wait until its write stands
///    at the gate (written, not durable).
/// 3. Send the query `ask`; keep the gate closed for [`HELD`] more.
/// 4. Open the gate.
///
/// Must see: at (3) the query has not answered; after (4) it answers within the bound
/// and sees `inv` (`saw`), and `submit` finishes too.
async fn reply_waits_for_durability<T: Send + 'static>(
    ask: impl FnOnce(EngineHandle) -> tokio::task::JoinHandle<cairn_server::actor::Result<T>>,
    saw: impl FnOnce(&T) -> bool,
) -> Result<()> {
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let handle = spawn_runtime(Arc::clone(&faulty))?;
    let _ = handle.status(id(250)).await;

    let gate = Gate::closed();
    faulty.add_rule(Rule::wal_put(Action::Gate(gate.clone())));
    let inv = id(1);
    let submitter = handle.clone();
    let submit = tokio::spawn(async move {
        submitter
            .submit(inv, target(), None, None, Bytes::from_static(b"{}"))
            .await
    });
    let t0 = std::time::Instant::now();
    while gate.blocked() == 0 {
        ensure!(t0.elapsed() < LIMIT, "submit's write missed the WAL gate");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    let asked = ask(handle.clone());
    let t0 = std::time::Instant::now();
    while t0.elapsed() < HELD {
        if asked.is_finished() {
            gate.open();
            bail!(
                "the query answered after {:?} while the invocation's write still stood at the WAL gate — \
                 an answer based on data not yet durable: INV-5 broken",
                t0.elapsed()
            );
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    gate.open();
    let got = tokio::time::timeout(LIMIT, asked).await???;
    ensure!(
        saw(&got),
        "the gate is open but the query does not see the invocation just created"
    );
    tokio::time::timeout(LIMIT, submit).await???;
    Ok(())
}

/// A `Status` about an invocation just created but **not yet durable** ⇒ must not be
/// answered before it is durable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_does_not_reply_before_creation_is_durable() -> Result<()> {
    reply_waits_for_durability(
        |h| tokio::spawn(async move { h.status(id(1)).await }),
        |st| st.is_some(),
    )
    .await
}

/// The same property, through the listing path — a read-only query quite different in
/// shape but going through the same answering gate.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_does_not_reply_before_creation_is_durable() -> Result<()> {
    reply_waits_for_durability(
        |h| tokio::spawn(async move { h.list_invocations(10, None).await }),
        |rows| rows.iter().any(|(i, _, _)| *i == id(1)),
    )
    .await
}
