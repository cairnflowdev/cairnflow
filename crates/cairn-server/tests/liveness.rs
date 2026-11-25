// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! A partition must **always answer**, even while its store is failing.
//!
//! "Answer" here may be `NotLeader`: that is the right answer of a node that is
//! stepping down (principle N3). What is not allowed is silence — when a node is
//! silent the client does not know where to go, and the operator does not know why.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

use {
    anyhow::Result,
    bytes::Bytes,
    cairn_engine::{Engine, Target},
    cairn_server::{
        actor::{ActorError, EngineHandle, Leadership, Readiness, TurnLimits},
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
    b[15] = n;
    b
}

/// A one-node partition on a `FaultyStore`, with a short flush cadence.
fn spawn(store: Arc<FaultyStore>, limits: TurnLimits) -> Result<EngineHandle> {
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
        limits,
        None,
    )
    .map_err(Into::into)
}

/// Stepping down while the WAL cannot flush: `seal` waits for durability forever. Meanwhile
/// the partition must still answer commands — with `NotLeader` — rather than stay silent.
///
/// Interleaving (each step waits for an event with a limit, never a `sleep`):
/// 1. The engine opens; the first `status` answers ⇒ every write made at open is durable
///    (FL-2: an answer requires a durability ticket).
/// 2. Close the gate on every WAL PUT. `submit` an invocation: its write stops at the gate
///    (`Gate::blocked` ≥ 1).
/// 3. `resign`: `seal` waits for that write to become durable — forever. `Resign` answers
///    only **after** the seal, so do not wait for it.
/// 4. Poll `flow` (answered on the spot, no ticket wait) until it returns `NotLeader` —
///    the event "the engine has entered step-down". Each poll must be answered within
///    500 ms.
///
/// Must see: `status` returns `NotLeader` within 500 ms, and `resign` has not answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commands_answered_while_seal_hangs() -> Result<()> {
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let handle = spawn(Arc::clone(&faulty), TurnLimits::default())?;
    // (1) The engine has finished opening and everything written at open is durable.
    let _ = handle.status(id(250)).await;

    // (2) From here the WAL cannot flush any more; one write is pending: `/send` writes then
    // waits for durability.
    let gate = Gate::closed();
    faulty.add_rule(Rule::wal_put(Action::Gate(gate.clone())));
    let submitter = handle.clone();
    let _pending = tokio::spawn(async move {
        submitter
            .submit(id(1), target(), None, None, Bytes::from_static(b"{}"))
            .await
    });
    let t0 = std::time::Instant::now();
    while gate.blocked() == 0 {
        anyhow::ensure!(
            t0.elapsed() < Duration::from_secs(5),
            "submit's write did not reach the WAL gate"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    // (3) Step down.
    let resigner = handle.clone();
    let resigned = tokio::spawn(async move { resigner.resign().await });

    // (4) Wait for the engine to enter step-down; every question must get an answer.
    let t0 = std::time::Instant::now();
    loop {
        match tokio::time::timeout(Duration::from_millis(500), handle.flow()).await {
            Ok(Err(ActorError::NotLeader)) => break,
            Ok(_) => {}
            Err(_) => anyhow::bail!("`flow` silent 500 ms after resign — no one reads commands"),
        }
        anyhow::ensure!(
            t0.elapsed() < Duration::from_secs(5),
            "the engine did not step down after resign"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    let started = std::time::Instant::now();
    let r = tokio::time::timeout(Duration::from_millis(500), handle.status(id(2))).await;
    anyhow::ensure!(
        !resigned.is_finished(),
        "Resign answered while the seal was still pending — the caller would think the Db is closed"
    );
    gate.open();
    match r {
        Ok(Err(ActorError::NotLeader)) => Ok(()),
        Ok(other) => anyhow::bail!("the answer is not NotLeader: {other:?}"),
        Err(_) => anyhow::bail!(
            "partition silent for {:?} while stepping down — `seal` waits on a WAL that cannot flush and \
             nobody reads the command channel",
            started.elapsed()
        ),
    }
}

/// A partition on a dead store must **step down**, not stay on as a leader that
/// cannot write anything.
///
/// A WAL flush error past the retry count ⇒ SlateDB enters a permanently failed state: every
/// later commit fails. That error is not `Fenced`, so formerly nothing pulled the
/// partition down — the lease kept renewing, and the partition was stuck until the process
/// restarted. Measured by how often the engine factory is called: stepping down and
/// claiming again calls it once more.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_steps_down_when_db_closes() -> Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let opens = Arc::new(AtomicUsize::new(0));
    let (store, counter) = (Arc::clone(&faulty), Arc::clone(&opens));
    let handle = EngineHandle::spawn_with_transport(
        Box::new(move || {
            let store: Arc<dyn ObjectStore> = store.clone();
            let counter = Arc::clone(&counter);
            Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
                let db = Db::builder("partition-0", store)
                    .with_settings(Settings {
                        flush_interval: Some(Duration::from_millis(10)),
                        // No unbounded retry: the WAL flush error must surface as the
                        // store's failed state, exactly the case to measure.
                        object_store_max_retries: Some(0),
                        ..Default::default()
                    })
                    .build()
                    .await
                    .map_err(|e| {
                        cairn_engine::EngineError::Store(cairn_store::StoreError::Slate(e))
                    })?;
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
    )?;
    let _ = handle.status(id(250)).await;
    assert_eq!(opens.load(Ordering::SeqCst), 1);

    // From here every WAL flush fails.
    faulty.add_rule(Rule::wal_put(Action::Fail { times: None }));
    let _ = tokio::time::timeout(
        Duration::from_secs(2),
        handle.submit(id(1), target(), None, None, Bytes::from_static(b"{}")),
    )
    .await;

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while opens.load(Ordering::SeqCst) < 2 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    anyhow::ensure!(
        opens.load(Ordering::SeqCst) >= 2,
        "the partition's store failed for good yet the partition did not step down within 5s"
    );
    Ok(())
}

/// A write is not durable and `durable_seq` stands still past the limit ⇒ step down.
///
/// SlateDB retries object store errors forever by default, so an unreachable store shows
/// up as *waiting for durability forever* — there is no error to catch. The closed WAL
/// gate here plays exactly that case.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_steps_down_on_durability_stall() -> Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let opens = Arc::new(AtomicUsize::new(0));
    let gate = Gate::closed();
    let (store, counter) = (Arc::clone(&faulty), Arc::clone(&opens));
    let handle = EngineHandle::spawn_with_transport(
        Box::new(move || {
            let store: Arc<dyn ObjectStore> = store.clone();
            let counter = Arc::clone(&counter);
            Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
                let db = Db::builder("partition-0", store)
                    .with_settings(Settings {
                        flush_interval: Some(Duration::from_millis(10)),
                        ..Default::default()
                    })
                    .build()
                    .await
                    .map_err(|e| {
                        cairn_engine::EngineError::Store(cairn_store::StoreError::Slate(e))
                    })?;
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
        TurnLimits {
            durability_stall: Some(Duration::from_millis(500)),
            ..TurnLimits::default()
        },
        None,
    )?;
    let _ = handle.status(id(250)).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(opens.load(Ordering::SeqCst), 1);

    faulty.add_rule(Rule::wal_put(Action::Gate(gate.clone())));
    let submitter = handle.clone();
    let _pending = tokio::spawn(async move {
        submitter
            .submit(id(1), target(), None, None, Bytes::from_static(b"{}"))
            .await
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while opens.load(Ordering::SeqCst) < 2 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    gate.open();
    anyhow::ensure!(
        opens.load(Ordering::SeqCst) >= 2,
        "durable_seq stood still yet the partition did not step down within 5s (limit 500ms)"
    );
    Ok(())
}

/// A factory that fails forever (a long-lasting store 503): the leadership loop must **back
/// off** between claims, not reclaim every fixed 0.5 s — each failure is a claim PUT
/// plus a yield PUT feeding the very throttling.
/// While backing off, commands are still answered at once (`NotLeader`), and `Shutdown`
/// does not have to wait out the pause.
///
/// Order:
/// 1. Build the engine with `Readiness::Immediately`; the factory always returns an I/O
///    error and counts its calls.
/// 2. After 6 s (real time — the engine runs on its own pool thread): count.
/// 3. Send `status`: must get `NotLeader` within 200 ms.
/// 4. `shutdown`: must answer within 1 s, even in the middle of a long pause.
///
/// Must see: ≤ 6 factory calls in 6 s (backoff 0.5 s doubling, jitter in the upper
/// half ⇒ the 6th call at the earliest at ~7.75 s); a fixed 0.5 s pause gives ~13 calls.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failing_factory_backs_off_and_keeps_answering() -> Result<()> {
    use std::sync::atomic::{AtomicU32, Ordering};
    let calls = Arc::new(AtomicU32::new(0));
    let counted = Arc::clone(&calls);
    let handle = EngineHandle::spawn_with_transport(
        Box::new(move || {
            counted.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                Err(cairn_engine::EngineError::Store(
                    cairn_store::StoreError::Io("503 SlowDown".into()),
                ))
            })
        }),
        Registry::new(),
        None,
        Readiness::Immediately,
        cairn_server::transport::Transport::disconnected(),
        TurnLimits::default(),
        None,
    )?;
    tokio::time::sleep(Duration::from_secs(6)).await;
    let n = calls.load(Ordering::SeqCst);
    anyhow::ensure!(
        n <= 6,
        "the failing factory was called {n} times in 6 s — the leadership loop does not back off"
    );

    let t0 = std::time::Instant::now();
    let r = tokio::time::timeout(Duration::from_millis(200), handle.status(id(1)))
        .await
        .map_err(|_| anyhow::anyhow!("status not answered during the pause"))?;
    anyhow::ensure!(
        matches!(r, Err(ActorError::NotLeader)),
        "must be NotLeader, got {r:?} after {:?}",
        t0.elapsed()
    );

    tokio::time::timeout(Duration::from_secs(1), handle.shutdown())
        .await
        .map_err(|_| anyhow::anyhow!("Shutdown had to wait out the pause"))??;
    Ok(())
}

// ───────────────────────────────────────── boot path

mod common;

/// One step of the boot path and the rule that fails exactly one of its requests.
struct BootFault {
    step: &'static str,
    ops: cairn_store::testkit::OpSet,
    path: cairn_store::testkit::PathMatch,
    /// How many matching requests pass before the failed request.
    skip: u32,
}

fn boot_faults() -> Vec<BootFault> {
    use cairn_store::testkit::{OpSet, PathMatch};
    let at = |step, ops, path: &str, skip| BootFault {
        step,
        ops,
        path: PathMatch::Contains(path.into()),
        skip,
    };
    let exact = |step, ops, path: &str| BootFault {
        step,
        ops,
        path: PathMatch::Prefix(path.into()),
        skip: 0,
    };
    vec![
        at(
            "fence gate: cleanup first",
            OpSet::DELETE,
            ".cairnflow-fence-probe/",
            0,
        ),
        at(
            "fence gate: first Create",
            OpSet::PUT,
            ".cairnflow-fence-probe/",
            0,
        ),
        at(
            "fence gate: second Create",
            OpSet::PUT,
            ".cairnflow-fence-probe/",
            1,
        ),
        at(
            "fence gate: read back",
            OpSet::GET,
            ".cairnflow-fence-probe/",
            0,
        ),
        at(
            "CAS gate: cleanup first",
            OpSet::DELETE,
            ".cairnflow-cas-probe/",
            0,
        ),
        at("CAS gate: Create", OpSet::PUT, ".cairnflow-cas-probe/", 0),
        at(
            "CAS gate: right etag",
            OpSet::PUT,
            ".cairnflow-cas-probe/",
            1,
        ),
        at("CAS gate: old etag", OpSet::PUT, ".cairnflow-cas-probe/", 2),
        exact("format mark: GET", OpSet::GET, "format"),
        exact("format mark: PUT", OpSet::PUT, "format"),
        at("session: LIST", OpSet::LIST, "sessions/", 0),
        at("session: PUT", OpSet::PUT, "sessions/", 0),
        exact("cluster-version: GET", OpSet::GET, "cluster-version"),
        exact("cluster-version: PUT", OpSet::PUT, "cluster-version"),
        exact("cluster.conf: GET", OpSet::GET, "cluster.conf"),
        exact("cluster.conf: PUT", OpSet::PUT, "cluster.conf"),
        exact("cluster secret: PUT", OpSet::PUT, "auth/cluster-secret"),
        exact("cluster secret: GET", OpSet::GET, "auth/cluster-secret"),
        exact("key store: GET", OpSet::GET, "auth/keys"),
        exact("tenant flags: GET", OpSet::GET, "tenants"),
        exact("flush windows: GET", OpSet::GET, "range-windows"),
    ]
}

/// **The boot path retries transient I/O errors** (a 4 h soak under the
/// `s3-standard-faults` profile saw 31 failed restarts before this).
///
/// 1. A fresh bucket — or a bucket that already has a cluster (node `seed` came up then
///    shut down), to also hit the "read back what exists" branch (cluster secret,
///    `cluster.conf`, format mark).
/// 2. Node `b`'s store fails **exactly one** request of one boot step: a 503
///    before reaching the store (`FailNth`).
/// 3. `node::start(b)`.
///
/// Must see: `start` returns `Ok` at every step, and the rule really fired (a step that
/// touches no request in that scenario is skipped, not counted as passing). Before the
/// fix: the node exited — the probe gate still reported a transient error as "the store
/// does not honor conditional PUT/CAS".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boot_survives_one_transient_fault_at_each_step() -> Result<()> {
    use cairn_store::testkit::{Action, Rule};
    let mut failed = Vec::new();
    let mut fired = 0;
    for existing in [false, true] {
        for f in boot_faults() {
            let bucket = common::bucket();
            if existing {
                let seed = FaultyStore::wrap(bucket.clone());
                common::start_cluster_node_on(&seed, "seed", 2, Duration::from_secs(2), |_| {})
                    .await?
                    .shutdown()
                    .await;
            }
            let store = FaultyStore::wrap(bucket.clone());
            store.add_rule(Rule::new(
                f.ops,
                f.path.clone(),
                Action::FailNth { skip: f.skip },
            ));
            let started = tokio::time::timeout(
                Duration::from_secs(60),
                common::start_cluster_node_on(&store, "b", 2, Duration::from_secs(2), |_| {}),
            )
            .await;
            let hit = store.injected().0 > 0;
            fired += usize::from(hit);
            match started {
                Ok(Ok(node)) => node.shutdown().await,
                Ok(Err(e)) => failed.push(format!(
                    "{} ({}): {e}",
                    f.step,
                    if existing {
                        "bucket with a cluster"
                    } else {
                        "fresh bucket"
                    }
                )),
                Err(_) => failed.push(format!("{}: boot hung over 60 s", f.step)),
            }
        }
    }
    anyhow::ensure!(
        failed.is_empty(),
        "boot failed on one transient error at {} steps:\n  {}",
        failed.len(),
        failed.join("\n  ")
    );
    anyhow::ensure!(
        fired >= 30,
        "only {fired} scenarios fired an error — rules miss the paths?"
    );
    Ok(())
}

/// A net against the boot retry going too far: a store that **really** cannot do CAS
/// on etags (`LocalFileSystem` returns `not yet implemented` for `PutMode::Update`)
/// is still refused at once, with the right message — not retried until the boot limit.
///
/// Built with `StoreLocation::from_store`, not `parse("file://…")`: a node on a
/// `file://` directory holds an exclusive lock and emulates
/// CAS (`ObjectStores::exclusive_local`), whereas a non-CAS store built directly has
/// no directory to lock — exactly the "store without CAS" shape this test needs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn boot_still_rejects_store_without_cas_quickly() -> Result<()> {
    let root = std::env::temp_dir().join(format!(
        "cairnflow-boot-nocas-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    std::fs::create_dir_all(&root)?;
    let stores = cairn_store::ObjectStores::single(cairn_store::StoreLocation::from_store(
        std::sync::Arc::new(slatedb::object_store::local::LocalFileSystem::new_with_prefix(&root)?),
        "",
        &format!("file://{}", root.display()),
    ));
    let cfg = cairn_server::node::NodeConfig::new(
        stores,
        cairn_server::node::NodeMode {
            partitions: 1,
            lease_ttl: Duration::from_secs(2),
            node_id: Some("nocas".into()),
        },
    );
    let t0 = std::time::Instant::now();
    let res = tokio::time::timeout(Duration::from_secs(30), cairn_server::node::start(cfg)).await;
    let _ = std::fs::remove_dir_all(&root);
    let err = match res {
        Err(_) => anyhow::bail!("boot on a non-CAS store hung over 30 s"),
        Ok(Ok(_)) => anyhow::bail!("a cluster node came up on file:// — the CAS gate leaks"),
        Ok(Err(e)) => e.to_string(),
    };
    anyhow::ensure!(
        err.contains("cannot do CAS on etag"),
        "wrong message: {err}"
    );
    anyhow::ensure!(
        t0.elapsed() < Duration::from_secs(5),
        "slow refusal ({:?}) — a permanent error was retried",
        t0.elapsed()
    );
    Ok(())
}
