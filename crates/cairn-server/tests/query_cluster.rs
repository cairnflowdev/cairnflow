// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `POST /query` across a cluster: ranges on another node go through
//! `POST /internal/query/scan`.

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{anyhow, ensure, Result},
    common::{
        fake_sdk::{FakeSdk, Step},
        key_on_partition, load_of, single_leaders, start_cluster_node_tweaked, wait_probe,
    },
    reqwest::Client,
    serde_json::{json, Value},
    std::time::Duration,
};

/// **A query spanning two nodes**:
///
/// 1. Two nodes, two ranges, each node leads exactly one range.
/// 2. Three invocations (virtual objects, keys chosen per range) on each range, called
///    on the node leading that range.
/// 3. `POST /query` on **both** nodes.
///
/// Must see: each node returns all six rows, three per range, `partial = false`; the
/// counters add up both sides; `LIMIT` cuts correctly after merging.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_spans_nodes() -> Result<()> {
    let bucket = common::bucket();
    let ttl = Duration::from_secs(1);
    let nodes = vec![
        start_cluster_node_tweaked(&bucket, "node-alpha", 2, ttl, |_| {}).await?,
        start_cluster_node_tweaked(&bucket, "node-beta", 2, ttl, |_| {}).await?,
    ];
    let sdk = FakeSdk::builder()
        .object("Obj", "run", vec![Step::output("ok")])
        .spawn()
        .await?;
    let r = common::within(Duration::from_secs(90), "query spanning two nodes", async {
        let refs: Vec<_> = nodes.iter().collect();
        let probe = wait_probe(
            &refs,
            Duration::from_secs(20),
            "each node leads one range",
            |p| single_leaders(p).is_some_and(|m| load_of(&m, 2) == [1, 1]),
        )
        .await?;
        let leaders = single_leaders(&probe).expect("balanced");
        let a = &nodes[0].node;
        common::register(&a.url(), &sdk.url).await?;
        // Registered on `node-alpha` only: `node-beta` serves `Obj` once its registry
        // has re-read the bucket after the change notification.
        let urls: Vec<_> = nodes.iter().map(|n| n.node.url()).collect();
        common::wait_services_known(&urls, &["Obj"]).await?;
        let http = Client::new();
        for (&range, &leader) in &leaders {
            for i in 0..3 {
                let key = key_on_partition(a, "Obj", range, &format!("r{range}-{i}"));
                let r = http
                    .post(format!(
                        "{}/invoke/Obj/run?key={key}",
                        nodes[leader].node.url()
                    ))
                    .body("{}")
                    .send()
                    .await?;
                ensure!(r.status().is_success(), "invoke: {}", r.text().await?);
            }
        }
        let sql = "SELECT range, count(*) AS n FROM sys_invocation GROUP BY range ORDER BY range";
        let want: Vec<Value> = leaders.keys().map(|r| json!([r, 3])).collect();
        for n in &nodes {
            let v = query_until(&http, &n.node.url(), sql, |v| v["rows"] == json!(want)).await?;
            ensure!(v["warnings"] == json!([]), "{v}");
        }
        let v = query_until(
            &http,
            &a.url(),
            "SELECT sum(count) FROM sys_invocation_counts",
            |v| v["rows"] == json!([[6]]),
        )
        .await?;
        ensure!(v["partial"] == false, "{v}");
        // `LIMIT` after merging: the four newest rows of the whole cluster, in order.
        let v = query_until(
            &http,
            &nodes[1].node.url(),
            "SELECT id, created_at FROM sys_invocation ORDER BY created_at DESC, id LIMIT 4",
            |v| v["rows"].as_array().is_some_and(|r| r.len() == 4),
        )
        .await?;
        let all = query_until(
            &http,
            &a.url(),
            "SELECT id, created_at FROM sys_invocation ORDER BY created_at DESC, id",
            |v| v["rows"].as_array().is_some_and(|r| r.len() == 6),
        )
        .await?;
        ensure!(
            v["rows"].as_array().map(|r| r[..].to_vec())
                == all["rows"].as_array().map(|r| r[..4].to_vec()),
            "{v}\n{all}"
        );
        Ok(())
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// Asks until `ok` (the background build and the `Remote` level trail the commit by one
/// tick) and the answer is not `partial`.
async fn query_until(
    http: &Client,
    url: &str,
    sql: &str,
    ok: impl Fn(&Value) -> bool,
) -> Result<Value> {
    let t0 = std::time::Instant::now();
    loop {
        let r = http
            .post(format!("{url}/query"))
            .json(&json!({ "sql": sql }))
            .send()
            .await?;
        let code = r.status();
        let v: Value = r.json().await?;
        if code.is_success() && v["partial"] == false && ok(&v) {
            return Ok(v);
        }
        if t0.elapsed() > Duration::from_secs(30) {
            return Err(anyhow!("{sql}: {code} {v}"));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// **A query during a split**:
///
/// 1. One node, one range, eight completed invocations.
/// 2. A task asks `count(*)`, `count(DISTINCT id)` continuously.
/// 3. Midway, `POST /ranges/0/split`.
///
/// Must see: every non-`partial` answer counts exactly eight, without duplicates
/// (the parent range is never read together with a child range); a `partial` answer
/// counts ≤ eight; after the split, complete answers return (the child ranges rebuild
/// their index) with two ranges.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_during_split() -> Result<()> {
    let node = common::start_led(common::solo_config(1)).await?;
    let url = node.url();
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("ok")])
        .spawn()
        .await?;
    let r = common::within(Duration::from_secs(90), "query during a split", async {
        common::register(&url, &sdk.url).await?;
        let http = Client::new();
        for _ in 0..8 {
            let r = http
                .post(format!("{url}/invoke/Echo/run"))
                .body("{}")
                .send()
                .await?;
            ensure!(r.status().is_success(), "invoke: {}", r.text().await?);
        }
        let sql = "SELECT count(*), count(DISTINCT id), count(DISTINCT range) FROM sys_invocation";
        query_until(&http, &url, sql, |v| v["rows"] == json!([[8, 8, 1]])).await?;

        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let watcher = {
            let (http, url, stop) = (http.clone(), url.clone(), stop.clone());
            tokio::spawn(async move {
                let mut seen = (0usize, 0usize);
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let v: Value = http
                        .post(format!("{url}/query"))
                        .json(&json!({ "sql": sql }))
                        .send()
                        .await?
                        .json()
                        .await?;
                    let Some(row) = v["rows"].get(0) else {
                        // A transient error (the range is changing) is not a wrong answer.
                        continue;
                    };
                    let (n, distinct) =
                        (row[0].as_u64().unwrap_or(0), row[1].as_u64().unwrap_or(0));
                    ensure!(n == distinct, "duplicate rows: {v}");
                    if v["partial"] == false {
                        ensure!(n == 8, "complete answer, wrong count: {v}");
                        seen.0 += 1;
                    } else {
                        ensure!(n <= 8, "partial answer counts too many: {v}");
                        seen.1 += 1;
                    }
                }
                Ok::<_, anyhow::Error>(seen)
            })
        };
        tokio::time::sleep(Duration::from_millis(200)).await;
        let r = http.post(format!("{url}/ranges/0/split")).send().await?;
        ensure!(r.status().is_success(), "split: {}", r.text().await?);
        query_until(&http, &url, sql, |v| v["rows"] == json!([[8, 8, 2]])).await?;
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let (full, partial) = watcher.await??;
        ensure!(full > 0, "no complete answer ({partial} partial)");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **A query on a deposed leader**:
///
/// 1. Two nodes, two ranges, one range per node; three invocations per range.
/// 2. Alpha yields its range (`resign`): that range's read handle on alpha is
///    removed before the seal; a node (possibly alpha itself) claims it again.
/// 3. A new invocation in that range, at the new leader, acked.
/// 4. Ask `count(*)` on alpha until the answer is not `partial`.
///
/// Must see: every non-`partial` answer from the ack (3) on counts all seven — never
/// a stale image from the deposed handle reported as complete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_on_deposed_leader() -> Result<()> {
    let bucket = common::bucket();
    let ttl = Duration::from_secs(1);
    let nodes = vec![
        start_cluster_node_tweaked(&bucket, "node-alpha", 2, ttl, |_| {}).await?,
        start_cluster_node_tweaked(&bucket, "node-beta", 2, ttl, |_| {}).await?,
    ];
    let sdk = FakeSdk::builder()
        .object("Obj", "run", vec![Step::output("ok")])
        .spawn()
        .await?;
    let r = common::within(Duration::from_secs(90), "query on deposed leader", async {
        let refs: Vec<_> = nodes.iter().collect();
        let probe = wait_probe(
            &refs,
            Duration::from_secs(20),
            "each node leads one range",
            |p| single_leaders(p).is_some_and(|m| load_of(&m, 2) == [1, 1]),
        )
        .await?;
        let leaders = single_leaders(&probe).expect("balanced");
        let a = &nodes[0].node;
        common::register(&a.url(), &sdk.url).await?;
        // Registered on `node-alpha` only: wait until `node-beta` serves `Obj` too.
        let urls: Vec<_> = nodes.iter().map(|n| n.node.url()).collect();
        common::wait_services_known(&urls, &["Obj"]).await?;
        let http = Client::new();
        let invoke = |leader: usize, range: u32, tag: String| {
            let http = http.clone();
            let url = nodes[leader].node.url();
            let key = key_on_partition(a, "Obj", range, &tag);
            async move {
                let r = http
                    .post(format!("{url}/invoke/Obj/run?key={key}"))
                    .body("{}")
                    .send()
                    .await?;
                ensure!(r.status().is_success(), "invoke: {}", r.text().await?);
                Ok::<_, anyhow::Error>(())
            }
        };
        for (&range, &leader) in &leaders {
            for i in 0..3 {
                invoke(leader, range, format!("r{range}-{i}")).await?;
            }
        }
        let sql = "SELECT count(*) FROM sys_invocation";
        query_until(&http, &a.url(), sql, |v| v["rows"] == json!([[6]])).await?;

        let p = *leaders
            .iter()
            .find(|(_, n)| **n == 0)
            .expect("alpha holds a range")
            .0;
        a.partitions
            .get(cairn_store::PartitionId(p))
            .expect("p has an engine")
            .resign()
            .await?;
        let probe = wait_probe(
            &refs,
            ttl * 3 + Duration::from_secs(5),
            "p has a leader again",
            |pr| pr[&p].len() == 1,
        )
        .await?;
        let now = probe[&p][0];
        invoke(now, p, "after-yield".into()).await?;
        let t0 = std::time::Instant::now();
        loop {
            let v: Value = http
                .post(format!("{}/query", a.url()))
                .json(&json!({ "sql": sql }))
                .send()
                .await?
                .json()
                .await?;
            if v["partial"] == false && v["rows"].is_array() {
                ensure!(
                    v["rows"] == json!([[7]]),
                    "complete answer but wrong after the yield: {v}"
                );
                break;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(30),
                "complete answers did not return: {v}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(())
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}
