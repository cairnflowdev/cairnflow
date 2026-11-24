// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Smoke test of the shared assembly path: a real multi-partition node, built in process.
//!
//! Two questions, one test each: (1) does the node route by key to the right
//! partition, and can an invocation be looked up on both; (2) does a `ctx.call`
//! **crossing partitions** — over the real HTTP transport — arrive.
//! Both have a time limit: hanging is red, not a stalled CI.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{ensure, Result},
    common::{
        fake_sdk::{FakeSdk, Step},
        TEST_DEADLINE,
    },
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_partitions_route_by_key() -> Result<()> {
    let sdk = FakeSdk::builder()
        .object("Counter", "get", vec![Step::output("ok")])
        .spawn()
        .await?;
    let node = common::start_single_process(2).await?;
    let url = node.url();

    let result = common::within(TEST_DEADLINE, "two invocations on two partitions", async {
        common::register(&url, &sdk.url).await?;

        let mut ids = Vec::new();
        for p in 0..2 {
            let key = common::key_on_partition(&node, "Counter", p, "k");
            let (code, body) = common::invoke(&url, "Counter", "get", Some(&key), "x").await?;
            ensure!(code == 200, "partition {p}, key {key}: {code} {body}");
            ensure!(body["output"] == "ok", "partition {p}: {body}");
            ids.push((p, body["id"].as_str().unwrap_or_default().to_string()));
        }
        for (p, id) in &ids {
            let (code, st) = common::invocation(&url, id).await?;
            ensure!(code == 200, "/invocations/{id}: {code} {st}");
            ensure!(st["status"] == "completed", "/invocations/{id}: {st}");
            let got = common::partition_of(&url, id).await?;
            ensure!(got == *p, "{id} is on partition {got}, not {p}");
        }
        // Each key exactly one turn, and the two turns carry different keys — i.e. exactly
        // two invocations, not one run twice.
        let starts = sdk.starts_of("Counter", "get");
        ensure!(starts.len() == 2, "turns: {starts:?}");
        ensure!(starts[0].key != starts[1].key, "{starts:?}");
        Ok(())
    })
    .await;

    node.shutdown().await;
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_partition_call_completes() -> Result<()> {
    let node = common::start_single_process(2).await?;
    let callee_key = common::key_on_partition(&node, "Callee", 1, "callee");
    let caller_key = common::key_on_partition(&node, "Caller", 0, "caller");
    let sdk = FakeSdk::builder()
        .object(
            "Caller",
            "run",
            vec![
                Step::call("Callee", "echo", Some(&callee_key)),
                Step::EchoCallResult,
            ],
        )
        .object("Callee", "echo", vec![Step::output("from-partition-1")])
        .spawn()
        .await?;
    let url = node.url();

    let result = common::within(TEST_DEADLINE, "ctx.call from partition 0 to 1", async {
        common::register(&url, &sdk.url).await?;
        let (code, body) = common::invoke(&url, "Caller", "run", Some(&caller_key), "x").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        let id = body["id"].as_str().unwrap_or_default().to_string();
        let st = common::wait_status(&url, &id, "completed").await?;
        // The callee's result reaches the right caller — the completion travelled back
        // over the transport, not just the call going forth.
        ensure!(st["output"] == "from-partition-1", "{st}");
        ensure!(
            common::partition_of(&url, &id).await? == 0,
            "the caller must be on partition 0"
        );
        let callee = sdk.starts_of("Callee", "echo");
        ensure!(callee.len() == 1, "{:?}", sdk.starts());
        ensure!(
            common::partition_of(&url, &callee[0].id_hex()).await? == 1,
            "the callee must be on partition 1: {callee:?}"
        );
        Ok(())
    })
    .await;

    node.shutdown().await;
    result
}

/// A self-check of the fake SDK: the gate holds turns, `Sleep` waits then continues thanks
/// to the background tick, `Suspend` leaves the invocation idle. The later tests rely on these three.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fake_sdk_gate_sleep_and_suspend() -> Result<()> {
    let sdk = FakeSdk::builder()
        .gated_service("Slow", "nap", vec![Step::Sleep(30), Step::output("slept")])
        .service("Idle", "hold", vec![Step::Suspend])
        .spawn()
        .await?;
    let node = common::start_single_process(1).await?;
    let url = node.url();

    let result = common::within(TEST_DEADLINE, "gate, sleep, suspend", async {
        common::register(&url, &sdk.url).await?;

        // Gate closed: the first turn reaches the SDK and stops there.
        let call = tokio::spawn({
            let url = url.clone();
            async move { common::invoke(&url, "Slow", "nap", None, "x").await }
        });
        while sdk.starts_of("Slow", "nap").is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        ensure!(!call.is_finished(), "gate closed yet the turn completed");
        sdk.gate().open();
        let (code, body) = call.await??;
        let id = body["id"].as_str().unwrap_or_default().to_string();
        ensure!(code == 200 || code == 202, "{code} {body}");
        let st = common::wait_status(&url, &id, "completed").await?;
        ensure!(st["output"] == "slept", "{st}");
        // Two turns: one up to the sleep, one after the timer fires.
        ensure!(
            sdk.starts_of("Slow", "nap").len() == 2,
            "{:?}",
            sdk.starts()
        );

        let (code, body) = common::send(&url, "Idle", "hold", None, "x").await?;
        ensure!(code == 202 || code == 200, "{code} {body}");
        let id = body["id"].as_str().unwrap_or_default().to_string();
        common::wait_status(&url, &id, "suspended").await?;
        Ok(())
    })
    .await;

    node.shutdown().await;
    result
}

/// A format 5 bucket (the range table, the old keyspace — table code
/// first in the key) must be refused: this binary reads keys in three zones by owner, and
/// misreading an old bucket reports no error at all — it just sees every table empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_refuses_format_5_bucket() -> Result<()> {
    use {
        cairn_server::node::{self, NodeConfig, NodeMode},
        cairn_store::{ObjectStores, StoreError, StoreLocation},
        slatedb::object_store::{path::Path, ObjectStoreExt},
        std::time::Duration,
    };
    let bucket = common::bucket();
    bucket
        .put(
            &Path::from("format"),
            "cairnflow-format\nversion=5\n".into(),
        )
        .await?;
    let stores = ObjectStores::single(StoreLocation::from_store(bucket.clone(), "", "memory://"));
    let started = node::start(NodeConfig::new(
        stores,
        NodeMode {
            partitions: 1,
            lease_ttl: Duration::from_secs(1),
            node_id: Some("format-5".into()),
        },
    ))
    .await;
    match started {
        Ok(node) => {
            node.shutdown().await;
            anyhow::bail!("node started on a format 5 bucket (old keyspace)");
        }
        Err(e) => {
            let want = StoreError::DataFormat {
                found: Some(5),
                expected: cairn_store::DATA_FORMAT,
            }
            .to_string();
            ensure!(
                e.to_string().contains(&want),
                "refused, but not because of the format: {e}"
            );
        }
    }
    Ok(())
}

/// A format 7 bucket must be refused: the meaning of the durable `suspended` status has
/// changed. This binary re-runs only `running` invocations on becoming leader, and trusts
/// that an invocation that already has something to wake it is `running`. Old binaries
/// wrote `suspended` even for invocations whose completion was already in the journal
/// (died between the commit and the turn) — opening that bucket leaves them idle forever, with no error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_refuses_format_7_bucket() -> Result<()> {
    use {
        cairn_server::node::{self, NodeConfig, NodeMode},
        cairn_store::{ObjectStores, StoreError, StoreLocation},
        slatedb::object_store::{path::Path, ObjectStoreExt},
        std::time::Duration,
    };
    let bucket = common::bucket();
    bucket
        .put(
            &Path::from("format"),
            "cairnflow-format\nversion=7\n".into(),
        )
        .await?;
    let stores = ObjectStores::single(StoreLocation::from_store(bucket.clone(), "", "memory://"));
    let started = node::start(NodeConfig::new(
        stores,
        NodeMode {
            partitions: 1,
            lease_ttl: Duration::from_secs(1),
            node_id: Some("format-7".into()),
        },
    ))
    .await;
    match started {
        Ok(node) => {
            node.shutdown().await;
            anyhow::bail!("node started on a format 7 bucket (`suspended` with the old meaning)");
        }
        Err(e) => {
            let want = StoreError::DataFormat {
                found: Some(7),
                expected: cairn_store::DATA_FORMAT,
            }
            .to_string();
            ensure!(
                e.to_string().contains(&want),
                "refused, but not because of the format: {e}"
            );
        }
    }
    Ok(())
}

/// A format 8 bucket must be refused: state / promise / idempotency /
/// queue keys carry the tenant, the partition-key hash covers the
/// tenant, the deployment registry became a catalog + immutable deployments, entry 0 carries
/// the deployment and tenant. This binary reading a format 8 bucket misreads every key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_refuses_format_8_bucket() -> Result<()> {
    use {
        cairn_server::node::{self, NodeConfig, NodeMode},
        cairn_store::{ObjectStores, StoreError, StoreLocation},
        slatedb::object_store::{path::Path, ObjectStoreExt},
        std::time::Duration,
    };
    let bucket = common::bucket();
    bucket
        .put(
            &Path::from("format"),
            "cairnflow-format\nversion=8\n".into(),
        )
        .await?;
    let stores = ObjectStores::single(StoreLocation::from_store(bucket.clone(), "", "memory://"));
    let started = node::start(NodeConfig::new(
        stores,
        NodeMode {
            partitions: 1,
            lease_ttl: Duration::from_secs(1),
            node_id: Some("format-8".into()),
        },
    ))
    .await;
    match started {
        Ok(node) => {
            node.shutdown().await;
            anyhow::bail!("node started on a format 8 bucket (keys without tenant)");
        }
        Err(e) => {
            let want = StoreError::DataFormat {
                found: Some(8),
                expected: cairn_store::DATA_FORMAT,
            }
            .to_string();
            ensure!(
                e.to_string().contains(&want),
                "refused, but not because of the format: {e}"
            );
        }
    }
    Ok(())
}

/// A cluster node on a bucket of **another format** must refuse to start at once — it
/// must not claim a lease or write anything to the bucket and then loop forever on the store-open error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_node_refuses_other_format_bucket_at_start() -> Result<()> {
    use {
        cairn_server::node::{self, NodeConfig, NodeMode},
        cairn_store::{
            testkit::{FaultyStore, Op, PathClass},
            ObjectStores, StoreLocation, DATA_FORMAT,
        },
        slatedb::object_store::{path::Path, ObjectStoreExt},
        std::time::Duration,
    };
    let bucket = common::bucket();
    bucket
        .put(
            &Path::from("format"),
            format!("cairnflow-format\nversion={}\n", DATA_FORMAT - 1).into(),
        )
        .await?;
    let store = FaultyStore::wrap(bucket.clone());
    let stores = ObjectStores::single(StoreLocation::from_store(store.clone(), "", "memory://"));
    let started = node::start(NodeConfig::new(
        stores,
        NodeMode {
            partitions: 2,
            lease_ttl: Duration::from_secs(1),
            node_id: Some("format-check".into()),
        },
    ))
    .await;
    // Give a node that started by mistake time to do the wrong thing.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let lease_writes = store.counts().get(Op::Put, PathClass::Lease);
    if let Ok(node) = started {
        node.shutdown().await;
        anyhow::bail!("cluster node booted on format {} bucket", DATA_FORMAT - 1);
    }
    ensure!(
        lease_writes == 0,
        "node refused but wrote {lease_writes} times to the lease tier"
    );
    Ok(())
}

/// **A node without `--cluster` is a one-node cluster**: the same
/// code path as a cluster — session, leases by owner
/// record, shared WAL for new ranges — instead of a separate branch without a lease tier (that
/// branch produced a class of bugs: a retired range nobody tombstoned).
///
/// 1. Build the node with the binary's `NodeMode` when there is no `--cluster`, two ranges,
///    on a test bucket kept at hand.
/// 2. Wait for the node to lead both ranges.
///
/// Must see: the node has a session; each range's owner record points at that session,
/// not yielded, `term` ≥ 1, `wal_kind=shared`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]

async fn node_without_cluster_flag_is_a_one_node_cluster() -> Result<()> {
    use cairn_store::{ObjectStores, StoreLocation};
    let mem = std::sync::Arc::new(slatedb::object_store::memory::InMemory::new());
    let mut cfg = common::test_config(cairn_server::node::NodeMode::single(2));
    cfg.stores = ObjectStores::single(StoreLocation::from_store(mem.clone(), "", "memory://"));
    let node = common::start_led(cfg).await?;
    let me = node
        .session
        .current()
        .ok_or_else(|| anyhow::anyhow!("node without --cluster has no session"));
    let store: std::sync::Arc<dyn slatedb::object_store::ObjectStore> = mem.clone();
    let r = async {
        let me = me?;
        for p in 0..2 {
            let rec = cairn_store::read_owner(
                &store,
                &slatedb::object_store::path::Path::default(),
                cairn_store::PartitionId(p),
            )
            .await?
            .ok_or_else(|| anyhow::anyhow!("range {p}: no owner record"))?;
            ensure!(
                rec.session == me && !rec.released && rec.term >= 1,
                "range {p}: owner record {rec:?} does not belong to session {me}"
            );
            ensure!(
                rec.wal_kind == cairn_store::WalKind::Shared,
                "range {p}: a new range of a one-node cluster uses {:?}",
                rec.wal_kind
            );
        }
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}

/// **`--node-id` is a safe name.** The node id goes into object
/// keys (lease, session, shared WAL); it is checked in `node::start` to cover the binary,
/// tests and simulation alike.
///
/// Must see: `a/b`, `../x`, empty, with a space, 65 characters ⇒ `start` refuses, the error
/// mentions `node id`; `pod.cairn-0` (a DNS name, `$(POD_NAME)`), a hex machine-style id and
/// `cairn-1_x` start fine.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_id_must_be_a_safe_name() -> Result<()> {
    use {
        cairn_server::node::{self, NodeMode},
        std::time::Duration,
    };
    let start = |id: String| async move {
        let mut cfg = common::test_config(NodeMode {
            partitions: 1,
            lease_ttl: Duration::from_secs(2),
            node_id: Some(id),
        });
        cfg.tick_interval = Some(Duration::from_millis(50));
        node::start(cfg).await
    };
    for bad in [
        "a/b".to_string(),
        "../x".into(),
        String::new(),
        "node a".into(),
        "x".repeat(65),
    ] {
        match start(bad.clone()).await {
            Ok(n) => {
                n.shutdown().await;
                anyhow::bail!("node id {bad:?} accepted");
            }
            Err(e) => ensure!(e.to_string().contains("node id"), "{bad:?}: {e}"),
        }
    }
    for good in ["pod.cairn-0", "e784e1f3a9d8", "cairn-1_x"] {
        let n = start(good.into())
            .await
            .map_err(|e| anyhow::anyhow!("node id {good:?} refused: {e}"))?;
        n.shutdown().await;
    }
    Ok(())
}
