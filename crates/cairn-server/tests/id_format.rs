// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! An invocation id carries the **partition key**, not the partition number.
//!
//! The id is what users store in their own databases, so its format is a contract.
//! The old format (2 bytes of partition number + 14 bytes) welded the partition
//! number into that contract; the new format (8 bytes of partition key + 16 bytes)
//! lets the partition count change later without changing any id.

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

/// The returned id is 48 hex characters, and resolves back through
/// `/invocations/{id}` on every partition of a four-partition node.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ids_are_48_hex_and_route_by_table() -> Result<()> {
    let sdk = FakeSdk::builder()
        .object("Counter", "get", vec![Step::output("ok")])
        .service("Echo", "run", vec![Step::output("ok")])
        .spawn()
        .await?;
    let node = common::start_single_process(4).await?;
    let url = node.url();

    let r = common::within(TEST_DEADLINE, "24-byte id routed by the table", async {
        common::register(&url, &sdk.url).await?;
        let mut ids = Vec::new();
        for i in 0..8 {
            let key = format!("k-{i}");
            let (code, body) = common::invoke(&url, "Counter", "get", Some(&key), "x").await?;
            ensure!(code == 200, "{code} {body}");
            ids.push(body["id"].as_str().unwrap_or_default().to_string());
        }
        for _ in 0..4 {
            let (code, body) = common::invoke(&url, "Echo", "run", None, "x").await?;
            ensure!(code == 200, "{code} {body}");
            ids.push(body["id"].as_str().unwrap_or_default().to_string());
        }
        for id in &ids {
            ensure!(
                id.len() == 48 && id.chars().all(|c| c.is_ascii_hexdigit()),
                "id {id:?} is not 48 hex characters"
            );
            let (code, st) = common::invocation(&url, id).await?;
            ensure!(
                code == 200 && st["status"] == "completed",
                "/invocations/{id}: {code} {st}"
            );
        }
        // Same object key ⇒ same partition key (first 8 bytes) — a precondition of
        // `EXCLUSIVE` and of a table split by key range.
        let (_, a) = common::invoke(&url, "Counter", "get", Some("same"), "x").await?;
        let (_, b) = common::invoke(&url, "Counter", "get", Some("same"), "x").await?;
        let (a, b) = (
            a["id"].as_str().unwrap_or(""),
            b["id"].as_str().unwrap_or(""),
        );
        ensure!(
            a.get(..16) == b.get(..16),
            "same object key but different partition key: {a} {b}"
        );
        ensure!(a != b, "two different invocations share an id");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// A cross-partition message carries the **partition and table version** the sender
/// used to route it; the receiver recomputes from the id with its own table and
/// refuses on a mismatch (`409`), instead of writing a message that is not its own.
///
/// Today every node shares one table, so a mismatch only happens when the sender
/// routes wrongly; once the table can change (partition split/merge) this is the
/// fence between two versions of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn misrouted_inbound_rejected_409() -> Result<()> {
    use cairn_engine::{CompletionPayload, DeliverCompletion, InterPartition};
    let node = common::start_single_process(2).await?;
    // An id in partition 0 of a table split in two.
    let target = cairn_store::make_invocation_id(cairn_store::PartitionKey(0), [1; 16]);
    let msg = InterPartition::Completion(DeliverCompletion {
        target,
        completion_id: 1,
        result: CompletionPayload::Void,
    });
    let r = common::within(TEST_DEADLINE, "misrouted message", async {
        // The sender says "partition 1" — the receiver computes partition 0.
        let resp = common::post_internal(
            &node,
            "/internal/inbound?partition=1&table=1",
            msg.encode().to_vec(),
        )
        .await?;
        ensure!(
            resp.status() == reqwest::StatusCode::CONFLICT,
            "misrouted message accepted: {}",
            resp.status()
        );
        // Same for a mismatched table version.
        let resp = common::post_internal(
            &node,
            "/internal/inbound?partition=0&table=99",
            msg.encode().to_vec(),
        )
        .await?;
        ensure!(
            resp.status() == reqwest::StatusCode::CONFLICT,
            "message under another table version accepted: {}",
            resp.status()
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}
