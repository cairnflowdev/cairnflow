// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! User tags at ingress: the header
//! `cairnflow-tag: k=v` (≤ 8) on `/invoke`, `/send`, `/workflow` goes into the metadata
//! row (TLV `attr`) and the `ByAttr` index, immutable; `GET
//! /invocations/{id}` returns `tags`. A tag breaking the rules is refused with `400` before
//! anything is created.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{anyhow, ensure, Result},
    common::fake_sdk::{FakeSdk, Step},
    std::time::Duration,
};

async fn send_tagged(url: &str, key: &str, tags: &[&str]) -> Result<(u16, serde_json::Value)> {
    let mut req = reqwest::Client::new()
        .post(format!("{url}/invoke/Hold/wait/send?key={key}"))
        .body("{}");
    for t in tags {
        req = req.header("cairnflow-tag", *t);
    }
    let resp = req.send().await?;
    let code = resp.status().as_u16();
    let text = resp.text().await?;
    Ok((
        code,
        serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text)),
    ))
}

/// **Tags at ingress reach the metadata.**
///
/// 1. `send` with two `cairnflow-tag` headers (`env=prod`, `team=a`).
/// 2. `GET /invocations/{id}` once it is waiting.
///
/// Must see: `tags` = `{"env":"prod","team":"a"}` — read from the metadata row,
/// the source of `ByAttr` (the index row is derived from it, `query_meta.rs`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ingress_tags_land_in_meta_and_by_attr() -> Result<()> {
    let sdk = FakeSdk::builder()
        .object("Hold", "wait", vec![Step::Suspend, Step::output("done")])
        .spawn()
        .await?;
    let node = common::start_led(common::solo_config(1)).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(60), "tags at ingress", async {
        common::register(&url, &sdk.url).await?;
        let (code, body) = send_tagged(&url, "k", &["env=prod", "team=a"]).await?;
        ensure!(code == 202, "{code} {body}");
        let id = body["id"].as_str().unwrap_or_default().to_string();
        common::wait_status(&url, &id, "suspended").await?;
        let st: serde_json::Value = reqwest::get(format!("{url}/invocations/{id}"))
            .await?
            .json()
            .await?;
        ensure!(
            st["tags"] == serde_json::json!({"env": "prod", "team": "a"}),
            "tags: {st}"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **A tag breaking the rules ⇒ `400`, nothing created.** An upper-case key, a key starting
/// with `$`, a key longer than 32 bytes, a value longer than 64 bytes, a missing `=`, nine tags.
///
/// Must see: `400` every time; the invocation list stays empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bad_tag_is_400() -> Result<()> {
    let sdk = FakeSdk::builder()
        .object("Hold", "wait", vec![Step::Suspend, Step::output("done")])
        .spawn()
        .await?;
    let node = common::start_led(common::solo_config(1)).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(60), "wrong tag", async {
        common::register(&url, &sdk.url).await?;
        let long_key = format!("{}=v", "k".repeat(33));
        let long_value = format!("k={}", "v".repeat(65));
        let nine: Vec<String> = (0..9).map(|i| format!("k{i}=v")).collect();
        let nine: Vec<&str> = nine.iter().map(String::as_str).collect();
        let cases: Vec<Vec<&str>> = vec![
            vec!["Env=prod"],
            vec!["$parent=x"],
            vec![long_key.as_str()],
            vec![long_value.as_str()],
            vec!["no-table"],
            nine,
        ];
        for (i, tags) in cases.iter().enumerate() {
            let (code, body) = send_tagged(&url, &format!("k{i}"), tags).await?;
            ensure!(code == 400, "{tags:?}: {code} {body}");
        }
        let rows: serde_json::Value = reqwest::get(format!("{url}/invocations?limit=100"))
            .await?
            .json()
            .await?;
        let n = rows
            .as_array()
            .ok_or_else(|| anyhow!("not an array: {rows}"))?
            .len();
        ensure!(n == 0, "a bad tag still created an invocation: {rows}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}
