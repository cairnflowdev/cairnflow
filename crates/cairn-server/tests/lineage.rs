// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Call lineage**: the SDK's tags, the parent of a `ctx.send`, the root of the call
//! tree — recorded in the callee's metadata and queryable.

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{ensure, Result},
    common::fake_sdk::{FakeSdk, Step},
    serde_json::{json, Value},
    std::time::Duration,
};

async fn query(url: &str, sql: &str) -> Result<Value> {
    Ok(reqwest::Client::new()
        .post(format!("{url}/query"))
        .json(&json!({ "sql": sql }))
        .send()
        .await?
        .json()
        .await?)
}

/// **A two-level tree, four ranges**:
///
/// 1. `Root/run` (ingress — the root) calls `Mid/run` **waiting** with tag `team=pay`,
///    then `ctx.send`s `Leaf/run` with tag `team=ops`, then completes.
/// 2. `Mid/run` `ctx.send`s `Leaf/run` (no tag) then completes.
///
/// Must see: `Mid` has `parent_id = Root`, `root_id = Root`, tag `team=pay`;
/// `Root`'s `Leaf` has `parent_id = Root` (even with `send`), tag `team=ops`;
/// `Mid`'s `Leaf` has `parent_id = Mid`, `root_id = Root` (inherited across two
/// levels); only `Root` is a root; `WHERE root_id = Root` sees exactly three children.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn call_lineage_reaches_the_callee() -> Result<()> {
    let tag = |v: &str| vec![("team".to_string(), v.to_string())];
    let sdk = FakeSdk::builder()
        .service(
            "Root",
            "run",
            vec![
                Step::TaggedCall {
                    service: "Mid".into(),
                    handler: "run".into(),
                    tags: tag("pay"),
                },
                Step::Send {
                    service: "Leaf".into(),
                    handler: "run".into(),
                    tags: tag("ops"),
                },
                Step::output("ok"),
            ],
        )
        .service(
            "Mid",
            "run",
            vec![
                Step::Send {
                    service: "Leaf".into(),
                    handler: "run".into(),
                    tags: vec![],
                },
                Step::output("mid"),
            ],
        )
        .service("Leaf", "run", vec![Step::output("leaf")])
        .spawn()
        .await?;
    let node = common::start_led(common::solo_config(4)).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(60), "lineage", async {
        common::register(&url, &sdk.url).await?;
        let (code, v) = common::invoke(&url, "Root", "run", None, "{}").await?;
        ensure!(code == 200, "{code} {v}");
        let root = v["id"].as_str().unwrap_or_default().to_string();
        let sql = "SELECT service, parent_id, root_id, root, tags FROM sys_invocation \
                   ORDER BY service, tags";
        let t0 = std::time::Instant::now();
        let rows = loop {
            let v = query(&url, sql).await?;
            let rows = v["rows"].as_array().cloned().unwrap_or_default();
            if rows.len() == 4 {
                break rows;
            }
            ensure!(t0.elapsed() < Duration::from_secs(30), "not four yet: {v}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        let mid = rows
            .iter()
            .find(|r| r[0] == "Mid")
            .ok_or_else(|| anyhow::anyhow!("no Mid: {rows:?}"))?;
        let mid_id = {
            let v = query(&url, "SELECT id FROM sys_invocation WHERE service = 'Mid'").await?;
            v["rows"][0][0].as_str().unwrap_or_default().to_string()
        };
        ensure!(
            mid[1] == root.as_str()
                && mid[2] == root.as_str()
                && mid[3] == false
                && mid[4] == r#"{"team":"pay"}"#,
            "Mid: {mid:?}"
        );
        let leaves: Vec<&Value> = rows.iter().filter(|r| r[0] == "Leaf").collect();
        ensure!(leaves.len() == 2, "{rows:?}");
        let of_root = leaves
            .iter()
            .find(|r| r[1] == root.as_str())
            .ok_or_else(|| anyhow::anyhow!("Root's send lost its parent: {rows:?}"))?;
        ensure!(
            of_root[2] == root.as_str() && of_root[3] == false && of_root[4] == r#"{"team":"ops"}"#,
            "Root's Leaf: {of_root:?}"
        );
        let of_mid = leaves
            .iter()
            .find(|r| r[1] == mid_id.as_str())
            .ok_or_else(|| anyhow::anyhow!("Mid's send lost its parent: {rows:?}"))?;
        ensure!(
            of_mid[2] == root.as_str() && of_mid[3] == false && of_mid[4].is_null(),
            "Mid's Leaf: {of_mid:?}"
        );
        let r_row = rows.iter().find(|r| r[0] == "Root").unwrap();
        ensure!(r_row[3] == true && r_row[2].is_null(), "Root: {r_row:?}");
        // The whole tree of a root: one `ByAttr[$root]` seek per range.
        let v = query(
            &url,
            &format!("SELECT count(*) FROM sys_invocation WHERE root_id = '{root}'"),
        )
        .await?;
        ensure!(v["rows"] == json!([[3]]), "{v}");
        let v = query(
            &url,
            &format!("EXPLAIN SELECT id FROM sys_invocation WHERE root_id = '{root}'"),
        )
        .await?;
        ensure!(v.to_string().contains("ByAttr"), "did not use ByAttr: {v}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}
