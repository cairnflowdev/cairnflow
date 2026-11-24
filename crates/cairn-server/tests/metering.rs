// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Per-tenant metering: billing figures must not be lost when a node restarts or shuts
//! down gracefully.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{anyhow, bail, Result},
    cairn_server::{
        metering::{self, Usage},
        node::{self, NodeConfig, NodeMode},
    },
    common::memory_stores,
    slatedb::object_store::{path::Path, ObjectStore, ObjectStoreExt, PutPayload},
    std::{sync::Arc, time::Duration},
};

fn config(stores: &cairn_store::ObjectStores, flush: Duration) -> NodeConfig {
    let mut cfg = NodeConfig::new(stores.clone(), NodeMode::single(1));
    cfg.tick_interval = Some(Duration::from_millis(50));
    cfg.flush_interval = Some(Duration::from_millis(5));
    cfg.metering_flush = flush;
    cfg
}

/// **A restart with a fixed `--node-id` overwrote that hour's figures.**
///
/// 1. An earlier process of node `n` flushed `metering/<t>/<hour>/<n>` with
///    100 steps. This process just opened: its in-RAM counter starts at 0.
/// 2. Node `n` (this process) counts 3 steps of tenant `t` then flushes.
///
/// Must see: `GET /metering` for that hour reads 103 steps. Before the fix, the first
/// flush of the new process **overwrote** the old process's object with its own total
/// (3): one rolling deployment lost up to an hour of billing figures per
/// tenant.
#[tokio::test]
async fn restarted_node_does_not_overwrite_the_hour_of_its_previous_life() -> Result<()> {
    let stores = memory_stores();
    let running = node::start(config(&stores, Duration::from_millis(100)))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let store: Arc<dyn ObjectStore> = Arc::clone(stores.main().store());
    let prefix = stores.main().prefix().clone();
    let tenant = "restart";
    let hour = metering::hour_now();
    let segment: String = running
        .node_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let earlier = Usage {
        invocations: 0,
        steps: 100,
        run_ms: 0,
        cu: 100,
        ..Usage::default()
    };
    let path = if prefix.as_ref().is_empty() {
        format!("metering/{tenant}/{hour}/{segment}")
    } else {
        format!("{}/metering/{tenant}/{hour}/{segment}", prefix.as_ref())
    };
    store
        .put(
            &Path::from(path),
            PutPayload::from(serde_json::to_vec(&earlier)?),
        )
        .await?;

    metering::steps(tenant, 3);
    let mut last = Usage::default();
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        last = metering::read(&store, &prefix, tenant, &hour)
            .await
            .map_err(|e| anyhow!(e))?;
        if last.steps == 103 {
            running.shutdown().await;
            return Ok(());
        }
    }
    running.shutdown().await;
    if metering::hour_now() != hour {
        // The hour changed midway: the counts are split over two hours, nothing to conclude.
        return Ok(());
    }
    bail!(
        "after the node restart, hour {hour} reads {} steps, must be 103",
        last.steps
    )
}

/// **Graceful shutdown did not do a final flush.**
///
/// 1. The node counts 7 steps of tenant `t`, flush period one hour (no tick arrives).
/// 2. The node shuts down gracefully (`RunningNode::shutdown`, the SIGTERM path).
///
/// Must see: the bucket has 7 steps of `t` in that hour. Before the fix, graceful
/// shutdown cancelled the flush task and lost everything counted since the last flush.
#[tokio::test]
async fn graceful_shutdown_flushes_metering() -> Result<()> {
    let stores = memory_stores();
    let running = node::start(config(&stores, Duration::from_secs(3600)))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let store: Arc<dyn ObjectStore> = Arc::clone(stores.main().store());
    let prefix = stores.main().prefix().clone();
    let tenant = "tat-em";
    let hour = metering::hour_now();
    metering::steps(tenant, 7);
    running.shutdown().await;
    let got = metering::read(&store, &prefix, tenant, &hour)
        .await
        .map_err(|e| anyhow!(e))?;
    if got.steps != 7 && metering::hour_now() == hour {
        bail!(
            "graceful shutdown lost figures: hour {hour} reads {} steps, must be 7",
            got.steps
        );
    }
    Ok(())
}

/// **History per hour and per model.**
///
/// 1. The bucket has metering objects of tenant `default` in three hours: `T05` (the
///    format from before per-model figures, no `models`), `T06` (two processes — one with
///    `models`, one without), and one hour outside the range.
/// 2. `GET /metering/hours?from=…T05&to=…` (a range of ≤ 31 days).
///
/// Must see: exactly two hours in the range, ordered by hour; `T06` sums both
/// processes; the new object's `models` come back right, the old object reads as empty;
/// a range over 31 days and a malformed hour ⇒ `400`.
#[tokio::test]
async fn metering_hours_reads_a_range_with_models() -> Result<()> {
    let stores = memory_stores();
    let running = node::start(config(&stores, Duration::from_secs(3600)))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let store: Arc<dyn ObjectStore> = Arc::clone(stores.main().store());
    let prefix = stores.main().prefix().clone();
    let root = if prefix.as_ref().is_empty() {
        "metering/default".to_string()
    } else {
        format!("{}/metering/default", prefix.as_ref())
    };
    let put = |path: String, body: &'static str| {
        let store = Arc::clone(&store);
        async move {
            store
                .put(&Path::from(path), PutPayload::from(body.as_bytes()))
                .await
        }
    };
    put(
        format!("{root}/2026-10-01T05/a.1"),
        r#"{"invocations":2,"steps":3,"runMs":4,"inputTokens":10,"outputTokens":1,"costMicros":5}"#,
    )
    .await?;
    put(
        format!("{root}/2026-10-01T06/a.1"),
        r#"{"invocations":1,"steps":1,"runMs":1,"inputTokens":7,"outputTokens":2,"costMicros":3,"models":{"m1":{"inputTokens":7,"outputTokens":2,"costMicros":3}}}"#,
    )
    .await?;
    put(
        format!("{root}/2026-10-01T06/b.2"),
        r#"{"invocations":1,"steps":1,"runMs":1,"inputTokens":4,"outputTokens":0,"costMicros":0}"#,
    )
    .await?;
    put(
        format!("{root}/2026-10-05T00/a.1"),
        r#"{"invocations":9,"steps":9,"runMs":9,"inputTokens":9,"outputTokens":9,"costMicros":9}"#,
    )
    .await?;
    let http = reqwest::Client::new();
    let get = |q: &str| {
        http.get(format!("{}/metering/hours?{q}", running.url()))
            .send()
    };
    let r = get("from=2026-10-01T05&to=2026-10-02T00").await?;
    let code = r.status();
    let v: serde_json::Value = r.json().await?;
    let result = (|| {
        anyhow::ensure!(code == 200, "{code} {v}");
        let hours = v["hours"].as_array().ok_or_else(|| anyhow!("{v}"))?;
        anyhow::ensure!(hours.len() == 2, "{v}");
        anyhow::ensure!(
            hours[0]["hour"] == "2026-10-01T05" && hours[0]["models"] == serde_json::json!({}),
            "{v}"
        );
        anyhow::ensure!(
            hours[1]["hour"] == "2026-10-01T06"
                && hours[1]["inputTokens"] == 11
                && hours[1]["invocations"] == 2
                && hours[1]["models"]["m1"]["inputTokens"] == 7,
            "{v}"
        );
        Ok(())
    })();
    for bad in [
        "from=2026-10-01T00&to=2026-11-01T00",
        "from=2026-02-30T00",
        "to=2026-10-01T00",
    ] {
        let code = get(bad).await?.status();
        if code != 400 {
            running.shutdown().await;
            bail!("{bad}: {code}, must be 400");
        }
    }
    running.shutdown().await;
    result
}
