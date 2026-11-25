// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! An ingress create command resent with the same id.
//!
//! The router resends a command after `Stopped` (the range quiesces to split)
//! with the **same id**, relying on "every command by id is idempotent"
//! (`ingress/router.rs`). An ingress create without an idempotency key used not
//! to check for an existing id: if the first attempt had committed and only then
//! the range stopped, the resend created **over** it — entry 0 and the status
//! rewritten, the invocation run again. Query chaos found it through a stale
//! index row left behind.

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{ensure, Result},
    bytes::Bytes,
    common::fake_sdk::{FakeSdk, Step},
    serde_json::Value,
    std::time::Duration,
};

/// **A create resent with the same id does not create again**:
///
/// 1. One node, one range; an `Invoke` command with id X to the engine, runs to completion.
/// 2. The same command, same id X, once more — as the router resends after `Stopped`.
///
/// Must see: X still `completed`, `createdAt` and journal unchanged, the SDK
/// called only once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resent_ingress_create_does_not_recreate() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("ok")])
        .spawn()
        .await?;
    let node = common::start_led(common::solo_config(1)).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(60), "resend the create", async {
        common::register(&url, &sdk.url).await?;
        let id = cairn_store::make_invocation_id(cairn_store::PartitionKey(0x42), [7; 16]);
        let hex = cairn_store::id_hex(&id);
        let target = || cairn_engine::Target {
            tenant: cairn_store::TenantId::default(),
            scope: None,
            service: "Echo".into(),
            handler: "run".into(),
            object_key: String::new(),
        };
        let h = node
            .partitions
            .get(cairn_store::PartitionId(0))
            .expect("range 0");
        h.invoke(id, target(), None, None, Bytes::from_static(b"{}"))
            .await?;
        common::wait_status(&url, &hex, "completed").await?;
        let get = |p: String| {
            let url = url.clone();
            async move {
                Ok::<Value, anyhow::Error>(reqwest::get(format!("{url}{p}")).await?.json().await?)
            }
        };
        let before = get(format!("/invocations/{hex}")).await?;
        let journal = get(format!("/invocations/{hex}/journal")).await?;
        let calls = sdk.starts_len();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let _ = h
            .invoke(id, target(), None, None, Bytes::from_static(b"{}"))
            .await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let after = get(format!("/invocations/{hex}")).await?;
        ensure!(after["status"] == "completed", "{after}");
        ensure!(
            after["createdAt"] == before["createdAt"],
            "created again: {before} → {after}"
        );
        ensure!(
            get(format!("/invocations/{hex}/journal")).await? == journal,
            "journal rewritten"
        );
        ensure!(sdk.starts_len() == calls, "SDK called again");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}
