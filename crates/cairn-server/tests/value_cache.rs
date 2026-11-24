// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The node-side segment cache.
//!
//! A single test in this file: the `cairnflow_value_*` counters belong to the
//! process (like `scan_fold.rs` in cairn-store), so other tests running in
//! parallel in the same binary (`cargo test`) would skew the readings.

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

mod common;

use {
    anyhow::{ensure, Result},
    bytes::Bytes,
    std::time::Duration,
};

const DEADLINE: Duration = Duration::from_secs(20);

fn json_blob(n: usize, c: u8) -> Bytes {
    let mut v = vec![c; n];
    v[0] = b'"';
    v[n - 1] = b'"';
    Bytes::from(v)
}

/// **A re-read does not GET the segment** (fake SDK on wire ≤ v5: the node
/// assembles).
///
/// 1. `send`: `ctx.set("messages", H)`, H = 600 KiB ⇒ the shell seals one
///    segment; the bytes just PUT go straight into the range's cache.
/// 2. `read` twice: a lazy `ctx.get` ⇒ the shell assembles before sending
///    down to the SDK, then returns a small output (a large output is
///    assembled by ingress — ingress has no engine and so no range cache;
///    the correctness of the assembled copy is the job of
///    `values.rs::chat_history_is_sealed_reused_and_read_back`).
///
/// Must see: both `read`s `200`; `cairnflow_value_cache_hits_total` ≥ 2,
/// `cairnflow_value_fetched_bytes_total` = 0 (no GET from the store);
/// `cairnflow_value_segments_sealed_total` = 1, `cairnflow_value_sealed_bytes_total`
/// = the segment size.
#[tokio::test(flavor = "multi_thread")]
async fn reads_after_seal_hit_the_range_cache() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    let h = json_blob(600 << 10, b'a');
    let sdk = FakeSdk::builder()
        .object(
            "Chat",
            "send",
            vec![
                Step::SetState {
                    key: "messages".into(),
                    value: h.clone(),
                },
                Step::output("ok"),
            ],
        )
        .object(
            "Chat",
            "read",
            vec![
                Step::GetState {
                    key: "messages".into(),
                },
                Step::output("read"),
            ],
        )
        .spawn()
        .await?;
    let bucket = common::bucket();
    let n = common::start_cluster_node(&bucket, "a", 1, Duration::from_secs(60)).await?;
    common::wait_probe(&[&n], DEADLINE, "a leads range 0", |p| {
        p.get(&0).is_some_and(|v| v == &vec![0])
    })
    .await?;
    let url = n.node.url();
    common::register(&url, &sdk.url).await?;
    let (code, v) = common::invoke(&url, "Chat", "send", Some("c1"), "{}").await?;
    ensure!(code == 200, "send: {code} {v}");
    for i in 0..2 {
        let (code, v) = common::invoke(&url, "Chat", "read", Some("c1"), "{}").await?;
        ensure!(code == 200, "read {i}: {code} {v}");
    }
    let hits = common::metric(&url, "cairnflow_value_cache_hits_total").await?;
    let fetched = common::metric(&url, "cairnflow_value_fetched_bytes_total").await?;
    let sealed = common::metric(&url, "cairnflow_value_segments_sealed_total").await?;
    let sealed_bytes = common::metric(&url, "cairnflow_value_sealed_bytes_total").await?;
    ensure!(hits >= 2, "both reads hit the cache: {hits}");
    ensure!(fetched == 0, "no segment GET: {fetched} bytes");
    ensure!(sealed == 1, "one sealed segment: {sealed}");
    // Sealing leaves `TAIL_RESERVE` (4 KiB) behind as the tail.
    ensure!(
        sealed_bytes == (h.len() - (4 << 10)) as u64,
        "sealed bytes: {sealed_bytes}"
    );
    n.node.shutdown().await;
    Ok(())
}
