// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Versioned deployments.
//!
//! Formerly the registry was keyed by endpoint URL, and when two deployments declared
//! the same service **the first element of an in-RAM `Vec` won** — that order was the
//! registration order on this node plus the LIST order of the refresh loop, so two
//! nodes could call two different deployments for the same service. And invocations
//! were bound to no deployment: registering a new version made every unfinished
//! invocation replay on the new code.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{anyhow, Result},
    common::{
        fake_sdk::{FakeSdk, Step},
        register, send, start_single_process, wait_status, within, TEST_DEADLINE,
    },
    std::time::Duration,
};

async fn sdk(output: &str, sleep_ms: Option<u64>) -> Result<FakeSdk> {
    let mut steps = Vec::new();
    if let Some(ms) = sleep_ms {
        steps.push(Step::Sleep(ms));
    }
    steps.push(Step::output(output));
    FakeSdk::builder().object("Job", "run", steps).spawn().await
}

async fn get_json(url: &str) -> Result<serde_json::Value> {
    Ok(reqwest::get(url).await?.json().await?)
}

/// Registering the **same** endpoint again is a new deployment with a new id — not an
/// overwrite: invocations are bound to ids, so the old id must remain for them.
#[tokio::test]
async fn registering_same_endpoint_twice_yields_two_ids() -> Result<()> {
    let node = start_single_process(1).await?;
    let sdk = sdk("x", None).await?;
    let a = register(&node.url(), &sdk.url).await?;
    let b = register(&node.url(), &sdk.url).await?;
    let (ida, idb) = (a["id"].as_str(), b["id"].as_str());
    assert!(ida.is_some_and(|s| s.starts_with("dp_")), "{a}");
    assert!(idb.is_some_and(|s| s.starts_with("dp_")), "{b}");
    assert_ne!(ida, idb, "one endpoint registered twice needs two ids");

    let all = get_json(&format!("{}/deployments", node.url())).await?;
    let all = all.as_array().ok_or_else(|| anyhow!("{all}"))?;
    assert_eq!(all.len(), 2, "{all:?}");
    // Only the new one is the current revision.
    let current: Vec<_> = all
        .iter()
        .filter(|d| d["currentFor"].as_array().is_some_and(|s| !s.is_empty()))
        .map(|d| d["id"].as_str().unwrap_or_default().to_string())
        .collect();
    assert_eq!(current, vec![idb.unwrap().to_string()]);
    node.shutdown().await;
    Ok(())
}

/// `invocation_finishes_on_its_own_deployment`: created on v1, suspended; register v2
/// for the same service; the timer fires ⇒ it continues **on v1**. A new invocation
/// runs on v2.
#[tokio::test]
async fn invocation_finishes_on_its_own_deployment() -> Result<()> {
    let node = start_single_process(1).await?;
    let v1 = sdk("v1", Some(400)).await?;
    let v2 = sdk("v2", Some(1)).await?;
    register(&node.url(), &v1.url).await?;

    let (code, r) = send(&node.url(), "Job", "run", Some("k1"), "{}").await?;
    assert!(code < 300, "{code} {r}");
    let old = r["id"].as_str().unwrap().to_string();
    within(
        TEST_DEADLINE,
        "k1 suspended on its sleep",
        wait_status(&node.url(), &old, "suspended"),
    )
    .await?;

    register(&node.url(), &v2.url).await?;
    let done = within(
        TEST_DEADLINE,
        "k1 done",
        wait_status(&node.url(), &old, "completed"),
    )
    .await?;
    assert_eq!(
        done["output"], "v1",
        "an unfinished invocation must continue on its own deployment"
    );
    assert!(
        v2.starts().iter().all(|s| s.id_hex() != old),
        "v2 must not see v1's invocation"
    );

    let (code, r) = send(&node.url(), "Job", "run", Some("k2"), "{}").await?;
    assert!(code < 300, "{code} {r}");
    let new = r["id"].as_str().unwrap().to_string();
    let done = within(
        TEST_DEADLINE,
        "k2 done",
        wait_status(&node.url(), &new, "completed"),
    )
    .await?;
    assert_eq!(
        done["output"], "v2",
        "a new invocation runs on the current revision"
    );
    node.shutdown().await;
    Ok(())
}

/// Moving the revision pointer through the API (rollback): with an `expect` condition
/// exactly one caller wins; a wrong `expect` ⇒ `409`. The current revision cannot be removed.
#[tokio::test]
async fn revision_rollback_is_conditional() -> Result<()> {
    let node = start_single_process(1).await?;
    let v1 = sdk("v1", None).await?;
    let v2 = sdk("v2", None).await?;
    let id1 = register(&node.url(), &v1.url).await?["id"]
        .as_str()
        .unwrap()
        .to_string();
    let id2 = register(&node.url(), &v2.url).await?["id"]
        .as_str()
        .unwrap()
        .to_string();
    let client = reqwest::Client::new();
    let put = |deployment: &str, expect: &str| {
        client
            .put(format!("{}/services/Job/revision", node.url()))
            .json(&serde_json::json!({ "deployment": deployment, "expect": expect }))
            .send()
    };
    // Wrong condition: the pointer is at id2, not at id1.
    assert_eq!(put(&id1, &id1).await?.status().as_u16(), 409);
    // The current revision cannot be removed.
    let del = client
        .delete(format!("{}/deployments/{id2}", node.url()))
        .send()
        .await?;
    assert_eq!(del.status().as_u16(), 409);
    // A rollback with the right condition.
    assert!(put(&id1, &id2).await?.status().is_success());
    let (_, r) = send(&node.url(), "Job", "run", Some("k"), "{}").await?;
    let id = r["id"].as_str().unwrap().to_string();
    let done = within(
        TEST_DEADLINE,
        "done",
        wait_status(&node.url(), &id, "completed"),
    )
    .await?;
    assert_eq!(done["output"], "v1");
    // id2 is no longer the revision ⇒ it can be removed.
    let del = client
        .delete(format!("{}/deployments/{id2}", node.url()))
        .send()
        .await?;
    assert_eq!(del.status().as_u16(), 204);
    node.shutdown().await;
    Ok(())
}

/// `two_nodes_resolve_same_deployment`: two nodes on the same bucket; A registered on
/// node a, then B (the same service) on node b. Once both have seen both deployments,
/// **every** invocation — whichever node creates it — runs on B.
#[tokio::test]
async fn two_nodes_resolve_same_deployment() -> Result<()> {
    let (_shared, nodes) =
        common::cluster_nodes(&["a", "b"], 2, Duration::from_millis(1_500)).await?;
    let (na, nb) = (&nodes[0].node, &nodes[1].node);
    let sdk_a = sdk("A", None).await?;
    let sdk_b = sdk("B", None).await?;
    register(&na.url(), &sdk_a.url).await?;
    register(&nb.url(), &sdk_b.url).await?;

    // Wait for both nodes' registry refresh loops to see both deployments.
    within(
        Duration::from_secs(20),
        "both nodes see both deployments",
        async {
            loop {
                let ca = get_json(&format!("{}/deployments", na.url())).await?;
                let cb = get_json(&format!("{}/deployments", nb.url())).await?;
                let n = |v: &serde_json::Value| v.as_array().map_or(0, Vec::len);
                if n(&ca) == 2 && n(&cb) == 2 {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        },
    )
    .await?;

    // Each range has a leader; send to node a, on a wrong leader (503) go to node b.
    // Enough keys for both nodes to create invocations.
    let mut outputs = Vec::new();
    let mut served_by = std::collections::BTreeSet::new();
    for i in 0..24 {
        let key = format!("k{i}");
        let mut done = None;
        for _ in 0..50 {
            for (name, n) in [("a", na), ("b", nb)] {
                let (code, r) = send(&n.url(), "Job", "run", Some(&key), "{}").await?;
                if code < 300 {
                    done = Some((name, n.url(), r["id"].as_str().unwrap().to_string()));
                    break;
                }
            }
            if done.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let (name, url, id) = done.ok_or_else(|| anyhow!("no node accepted {key}"))?;
        served_by.insert(name);
        let st = within(TEST_DEADLINE, "done", wait_status(&url, &id, "completed")).await?;
        outputs.push(st["output"].as_str().unwrap_or_default().to_string());
    }
    assert!(
        outputs.iter().all(|o| o == "B"),
        "both nodes must pick the same deployment (B, registered later): {outputs:?} (node: {served_by:?})"
    );
    for n in nodes {
        n.node.shutdown().await;
    }
    Ok(())
}

/// `deployment_in_use_cannot_be_removed`: an old deployment — no longer the revision —
/// still cannot be removed while unfinished invocations are bound to it; `GET` reports
/// that number. Once the invocations finish it can be removed.
#[tokio::test]
async fn deployment_in_use_cannot_be_removed() -> Result<()> {
    let node = start_single_process(2).await?;
    let v1 = sdk("v1", Some(1_500)).await?;
    let v2 = sdk("v2", None).await?;
    let id1 = register(&node.url(), &v1.url).await?["id"]
        .as_str()
        .unwrap()
        .to_string();
    let (_, r) = send(&node.url(), "Job", "run", Some("k1"), "{}").await?;
    let inv = r["id"].as_str().unwrap().to_string();
    within(
        TEST_DEADLINE,
        "k1 suspended",
        wait_status(&node.url(), &inv, "suspended"),
    )
    .await?;
    register(&node.url(), &v2.url).await?;

    let one = get_json(&format!("{}/deployments/{id1}", node.url())).await?;
    assert_eq!(one["pinnedInvocations"], 1, "{one}");
    assert_eq!(one["currentFor"], serde_json::json!([]), "{one}");
    let client = reqwest::Client::new();
    let del = client
        .delete(format!("{}/deployments/{id1}", node.url()))
        .send()
        .await?;
    assert_eq!(
        del.status().as_u16(),
        409,
        "removed while invocations are still bound to it"
    );

    within(
        TEST_DEADLINE,
        "k1 done",
        wait_status(&node.url(), &inv, "completed"),
    )
    .await?;
    let one = get_json(&format!("{}/deployments/{id1}", node.url())).await?;
    assert_eq!(one["pinnedInvocations"], 0, "{one}");
    let del = client
        .delete(format!("{}/deployments/{id1}", node.url()))
        .send()
        .await?;
    assert_eq!(del.status().as_u16(), 204);
    let gone = reqwest::get(format!("{}/deployments/{id1}", node.url())).await?;
    assert_eq!(gone.status().as_u16(), 404);
    node.shutdown().await;
    Ok(())
}

/// A failed read of **one** new deployment must not make a node answer `404` for a
/// service it already has — found by `sim_cluster` (seed 29).
///
/// 1. Two nodes `a`, `b`, registry poll every 30 s. Register the SDK on `a` (revision
///    1); wait until `b` can send.
/// 2. On `b`: a GET of a `deployments/` object fails exactly once.
/// 3. Register the SDK again on `a` (revision 2, the catalog points to it). `a` tells
///    `b`; `b` reads the new catalog and GETs revision 2 — which fails.
///
/// Must see: sending through `b` does not return `404` (the service still has revision
/// 1 until `b` manages to load revision 2), and `b` loads revision 2 within 3 s —
/// without waiting for the 30 s poll. Before the fix: the catalog pointed `b` at a
/// revision 2 it had not loaded, and `b` answered `404` to every send until the next poll.
#[tokio::test(flavor = "multi_thread")]
async fn failed_deployment_read_does_not_turn_a_known_service_into_404() -> Result<()> {
    use cairn_store::testkit::{Action, OpSet, PathMatch, Rule};
    let bucket = common::bucket();
    let mut nodes = Vec::new();
    for id in ["a", "b"] {
        nodes.push(
            common::start_cluster_node_tweaked(&bucket, id, 2, Duration::from_secs(2), |cfg| {
                cfg.deploy_refresh = Duration::from_secs(30)
            })
            .await?,
        );
    }
    let (a, b) = (&nodes[0], &nodes[1]);
    let sdk = sdk("ok", None).await?;
    register(&a.node.url(), &sdk.url).await?;
    within(TEST_DEADLINE, "b can send after registration", async {
        loop {
            let (code, _) = send(&b.node.url(), "Job", "run", Some("k0"), "{}").await?;
            if code != 404 {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?;
    let failed_before = b.store.injected().0;
    b.store.add_rule(Rule::new(
        OpSet::GET,
        PathMatch::Contains("deployments/".into()),
        Action::Fail { times: Some(1) },
    ));
    register(&a.node.url(), &sdk.url).await?;
    within(Duration::from_secs(5), "b fails on revision 2", async {
        while b.store.injected().0 == failed_before {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    })
    .await?;
    for i in 0..20 {
        let (code, body) = send(&b.node.url(), "Job", "run", Some(&format!("k{i}")), "{}").await?;
        anyhow::ensure!(code != 404, "send {i} through b returned 404: {body}");
    }
    within(Duration::from_secs(3), "b loads revision 2", async {
        loop {
            let listed = get_json(&format!("{}/deployments", b.node.url())).await?;
            if listed.as_array().is_some_and(|v| v.len() == 2) {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?;
    for n in nodes {
        n.node.shutdown().await;
    }
    Ok(())
}

/// "Safe deploy": `POST /deployments?activate=false` creates a deployment **without**
/// moving the revision — new invocations still run the old version.
/// `GET /deployments/{id}` returns the manifest (effective types, contracts) to compare
/// with the current version; the move is a `PUT /services/{s}/revision` with `expect`.
#[tokio::test]
async fn staged_deployment_moves_revision_only_when_asked() -> Result<()> {
    let node = start_single_process(1).await?;
    let v1 = sdk("v1", None).await?;
    let v2 = FakeSdk::builder()
        .object("Job", "run", vec![Step::output("v2")])
        .contract(
            "Job",
            "run",
            cairn_server::discovery::Contract {
                retry_policy_max_attempts: Some(7),
                ..Default::default()
            },
        )
        .spawn()
        .await?;
    let id1 = register(&node.url(), &v1.url).await?["id"]
        .as_str()
        .unwrap()
        .to_string();
    let client = reqwest::Client::new();
    let stage = |q: &str| {
        client
            .post(format!("{}/deployments?activate={q}", node.url()))
            .json(&serde_json::json!({ "uri": v2.url }))
            .send()
    };
    assert_eq!(stage("maybe").await?.status().as_u16(), 400);
    let staged: serde_json::Value = stage("false").await?.error_for_status()?.json().await?;
    let id2 = staged["id"].as_str().unwrap().to_string();
    assert_eq!(staged["currentFor"], serde_json::json!([]), "{staged}");

    let run = |want: &'static str| {
        let url = node.url();
        async move {
            let (_, r) = send(&url, "Job", "run", Some("k"), "{}").await?;
            let id = r["id"].as_str().unwrap().to_string();
            let done = within(TEST_DEADLINE, "done", wait_status(&url, &id, "completed")).await?;
            assert_eq!(done["output"], want);
            anyhow::Ok(())
        }
    };
    // Not moved yet: new invocations still run the current version.
    run("v1").await?;

    let d2 = get_json(&format!("{}/deployments/{id2}", node.url())).await?;
    let svc = &d2["manifest"][0];
    assert_eq!(svc["name"], "Job", "{d2}");
    assert_eq!(svc["ty"], "virtualObject");
    assert_eq!(svc["handlers"][0]["name"], "run");
    assert_eq!(svc["handlers"][0]["ty"], "exclusive", "effective type");
    assert_eq!(svc["handlers"][0]["retryPolicyMaxAttempts"], 7);
    // The list does not carry manifests (heavy, only needed for comparing).
    let all = get_json(&format!("{}/deployments", node.url())).await?;
    assert!(all[0].get("manifest").is_none(), "{all}");

    let moved = client
        .put(format!("{}/services/Job/revision", node.url()))
        .json(&serde_json::json!({ "deployment": id2, "expect": id1 }))
        .send()
        .await?;
    assert!(moved.status().is_success());
    run("v2").await?;
    node.shutdown().await;
    Ok(())
}

/// The node's `cairnflow_deployment_pinned_scans_total`.
async fn pinned_scans(url: &str) -> Result<u64> {
    let text = reqwest::get(format!("{url}/metrics")).await?.text().await?;
    text.lines()
        .find_map(|l| l.strip_prefix("cairnflow_deployment_pinned_scans_total "))
        .and_then(|v| v.trim().parse().ok())
        .ok_or_else(|| anyhow!("no cairnflow_deployment_pinned_scans_total"))
}

/// **A deployment read request does not scan the index of every range each time.**
///
/// 1. A lone node with 2 ranges, one deployment.
/// 2. 20 concurrent `GET /deployments/{id}` (a `view` key may do it).
///
/// Formerly each request ran `Command::PinnedCounts` on **every** range — a scan
/// O(unfinished invocations of every tenant) under the engine — so 20 requests were
/// 40 scans.
///
/// Must see: ≤ 12 scans (this test alone: ≤ 2 cluster-wide counts × 2 ranges; the rest
/// is left for other tests in the same process under `cargo test`); every request `200`
/// with `pinnedInvocations`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_deployment_reads_share_one_pinned_count() -> Result<()> {
    let node = start_single_process(2).await?;
    let sdk = sdk("x", None).await?;
    let url = node.url();
    let r = within(TEST_DEADLINE, "shared pinned count", async {
        let d = register(&url, &sdk.url).await?;
        let id = d["id"].as_str().unwrap_or_default().to_string();
        let before = pinned_scans(&url).await?;
        let reads: Vec<_> = (0..20)
            .map(|_| {
                let u = format!("{url}/deployments/{id}");
                tokio::spawn(async move { get_json(&u).await })
            })
            .collect();
        for r in reads {
            let v = r.await??;
            anyhow::ensure!(v["pinnedInvocations"].is_u64(), "{v}");
        }
        let scans = pinned_scans(&url).await? - before;
        // This test alone: ≤ 2 counts × 2 ranges = 4. Under `cargo test` other tests of
        // the file (deployment GET/DELETE) add to the same process-wide counter — leave
        // 8; formerly it was 40.
        anyhow::ensure!(scans <= 12, "20 deployment reads made {scans} scans");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **A refused `DELETE /deployments/{id}` does not scan every range each time either.**
///
/// 1. A lone node with 2 ranges; a deployment with one invocation parked on a long sleep.
/// 2. 20 concurrent `DELETE /deployments/{id}` (any key with `operate` may loop it).
///
/// The count before a removal is the same scan as the read's — O(unfinished invocations
/// of every tenant) under the engine of every range — and `DELETE` ran it once per
/// request: 40 scans for 20 requests that all answer `409` and write nothing.
///
/// Must see: every request `409`; ≤ 12 scans (≤ 2 cluster-wide counts × 2 ranges, the
/// rest left for other tests of the process, as in the read test above).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_refused_deletes_share_one_pinned_count() -> Result<()> {
    let node = start_single_process(2).await?;
    let sdk = sdk("x", Some(120_000)).await?;
    let url = node.url();
    let r = within(TEST_DEADLINE, "shared pinned count for DELETE", async {
        let d = register(&url, &sdk.url).await?;
        let id = d["id"].as_str().unwrap_or_default().to_string();
        let (_, r) = send(&url, "Job", "run", Some("k1"), "{}").await?;
        let inv = r["id"].as_str().unwrap_or_default().to_string();
        wait_status(&url, &inv, "suspended").await?;
        let before = pinned_scans(&url).await?;
        let client = reqwest::Client::new();
        let deletes: Vec<_> = (0..20)
            .map(|_| {
                let req = client.delete(format!("{url}/deployments/{id}"));
                tokio::spawn(async move { anyhow::Ok(req.send().await?.status().as_u16()) })
            })
            .collect();
        for d in deletes {
            let code = d.await??;
            anyhow::ensure!(code == 409, "a deployment with a parked invocation: {code}");
        }
        let scans = pinned_scans(&url).await? - before;
        anyhow::ensure!(scans <= 12, "20 refused deletes made {scans} scans");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **The "retiring" mark written ambiguously (rule 7).** The PUT of the mark
/// `deployments/<t>/<id>.retiring` is applied and then returns an error (a 5xx or a
/// timeout although it actually wrote).
///
/// Must see: `DELETE` reads back, finds exactly its own mark and goes on — `204`, the
/// deployment gone, no mark left on the bucket.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ambiguous_retiring_mark_still_removes_the_deployment() -> Result<()> {
    use cairn_store::testkit::{Ambiguity, Rule};
    use futures::TryStreamExt;
    use slatedb::object_store::{path::Path, ObjectStore};
    let bucket = common::bucket();
    let n = common::start_cluster_node_tweaked(
        &bucket,
        "a",
        1,
        std::time::Duration::from_secs(60),
        |_| {},
    )
    .await?;
    common::wait_probe(&[&n], TEST_DEADLINE, "a leads range 0", |p| {
        p.get(&0).is_some_and(|v| v == &vec![0])
    })
    .await?;
    let url = n.node.url();
    let sdk = sdk("x", None).await?;
    let r = within(TEST_DEADLINE, "ambiguous mark", async {
        let x = register(&url, &sdk.url).await?["id"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        register(&url, &sdk.url).await?;
        n.store
            .add_rule(Rule::ambiguous_put(".retiring", Ambiguity::Error));
        let del = reqwest::Client::new()
            .delete(format!("{url}/deployments/{x}"))
            .send()
            .await?;
        anyhow::ensure!(del.status().as_u16() == 204, "DELETE: {}", del.status());
        let gone = reqwest::get(format!("{url}/deployments/{x}")).await?;
        anyhow::ensure!(
            gone.status().as_u16() == 404,
            "GET after DELETE: {}",
            gone.status()
        );
        let left: Vec<_> = bucket
            .list(Some(&Path::from("deployments")))
            .try_collect()
            .await?;
        anyhow::ensure!(
            !left
                .iter()
                .any(|m| m.location.as_ref().ends_with(".retiring")),
            "mark left behind: {:?}",
            left.iter()
                .map(|m| m.location.to_string())
                .collect::<Vec<_>>()
        );
        Ok(())
    })
    .await;
    n.node.shutdown().await;
    r
}
