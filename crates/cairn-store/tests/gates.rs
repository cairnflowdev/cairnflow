// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Measures the two store gates on a real store**, not on `InMemory`.
//!
//! The conditional-PUT gate (D12) was measured when it was introduced. The CAS-on-etag
//! gate came later, and nobody had measured it on SeaweedFS or MinIO — the earlier
//! measurement could not ask that question because the gate did not exist yet.
//!
//! This test is `#[ignore]` because it needs a real store running. Bring up the cluster,
//! then run:
//!
//! ```sh
//! scripts/dev-store/up.sh
//! CAIRN_TEST_S3_ENDPOINT=http://localhost:18333 \
//! CAIRN_TEST_S3_URI=s3://walbench/gate-test \
//! CAIRN_TEST_S3_KEY=dev-only-access-key CAIRN_TEST_S3_SECRET=dev-only-not-a-secret \
//! CAIRN_TEST_S3_REGION=us-east-1 \
//!   cargo test -p cairn-store --test gates -- --ignored --nocapture
//! ```
//!
//! It goes through **exactly the path the binary takes**: `StoreLocation::parse` with
//! credentials from environment variables, then the two public gate functions. A probe
//! written separately could be right while the real path is still broken — for example if
//! the URI parser dropped some option.

// Tests run on real time, network and processes; only `sim_cluster` must be deterministic
// (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterministic sources"
)]

use {
    anyhow::{bail, Result},
    cairn_store::{ObjectStores, StoreLocation},
};

/// Reads the store configuration from environment variables, and **sets** the variables
/// that `object_store`'s parser reads.
///
/// `Db::resolve_object_store` pushes every environment variable (lowercased) into
/// `parse_url_opts`, so `aws_endpoint`, `aws_access_key_id`, … are how it is configured.
/// Setting them here rather than making whoever runs the test remember ten exact names.
fn setup() -> Option<String> {
    let uri = std::env::var("CAIRN_TEST_S3_URI").ok()?;
    let set = |k: &str, v: Option<String>| {
        if let Some(v) = v {
            std::env::set_var(k, v);
        }
    };
    set("AWS_ENDPOINT", std::env::var("CAIRN_TEST_S3_ENDPOINT").ok());
    set("AWS_ACCESS_KEY_ID", std::env::var("CAIRN_TEST_S3_KEY").ok());
    set(
        "AWS_SECRET_ACCESS_KEY",
        std::env::var("CAIRN_TEST_S3_SECRET").ok(),
    );
    set("AWS_REGION", std::env::var("CAIRN_TEST_S3_REGION").ok());
    std::env::set_var("AWS_ALLOW_HTTP", "true");
    std::env::set_var("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "false");
    Some(uri)
}

#[tokio::test]
#[ignore = "needs a real store; see the comment at the top of the file"]
async fn conditional_put_gate_on_real_store() -> Result<()> {
    let Some(uri) = setup() else {
        bail!("CAIRN_TEST_S3_URI is missing");
    };
    let stores = ObjectStores::single(StoreLocation::parse(&uri)?);
    match stores.assert_conditional_put().await {
        Ok(()) => {
            println!("conditional PUT gate (D12): PASS — {uri}");
            Ok(())
        }
        Err(e) => {
            println!("conditional PUT gate (D12): FAIL — {uri}\n  {e}");
            Err(e.into())
        }
    }
}

#[tokio::test]
#[ignore = "needs a real store; see the comment at the top of the file"]
async fn cas_gate_on_real_store() -> Result<()> {
    let Some(uri) = setup() else {
        bail!("CAIRN_TEST_S3_URI is missing");
    };
    let stores = ObjectStores::single(StoreLocation::parse(&uri)?);
    match stores.assert_compare_and_swap().await {
        Ok(()) => {
            println!("CAS-on-etag gate:           PASS — {uri}");
            Ok(())
        }
        Err(e) => {
            println!("CAS-on-etag gate:           FAIL — {uri}\n  {e}");
            Err(e.into())
        }
    }
}

/// A real lease running on a real store: claim, renew over several rounds, the old owner
/// dies, someone else takes over.
///
/// This is what the two gates above only *predict*. A store may pass both probes and still
/// fail at the real pace — for example if the etag changed between two reads even though
/// the content did not, every observer would forever see "the old owner just renewed".
///
/// **But the converse does NOT hold, and that is the most important point here.**
/// Measured: this test is **green on Garage v1.0.1**, a store that fails both gates. The
/// reason is that it only takes the happy path — there is no real contention, so the CAS
/// condition is never exercised. On a store that swallows the condition, two nodes would
/// both believe they are leader, and **no functional test at this layer can see it**: to
/// see it one has to write deliberately with a stale etag, that is, do exactly what the
/// gate does.
///
/// So the order is: **the gate decides, functional tests only confirm**. The other way
/// round is how one concludes a store is usable when it is not.
#[tokio::test]
#[ignore = "needs a real store; see the comment at the top of the file"]
async fn lease_runs_on_real_store() -> Result<()> {
    use {
        cairn_store::{Claim, LeaseConfig, PartitionId, Session},
        std::{sync::Arc, time::Duration, time::Instant},
    };

    let Some(uri) = setup() else {
        bail!("CAIRN_TEST_S3_URI is missing");
    };
    let loc = StoreLocation::parse(&uri)?;
    // A prefix of its own for each run: the bucket is shared with other tests, and a
    // session left over from an earlier run would make this test wait for nothing.
    let prefix = slatedb::object_store::path::Path::from(format!(
        "{}/lease-{}",
        loc.prefix().as_ref(),
        std::process::id()
    ));

    let cfg = |node: &str| {
        LeaseConfig::new()
            .with_node_id(node)
            .with_ttl(Duration::from_secs(3))
            .with_renew_interval(Duration::from_millis(400))
    };
    let session = |node: &str| Session::establish(Arc::clone(loc.store()), &prefix, cfg(node));

    let t0 = Instant::now();
    let a = session("a").await?;
    let Claim::Won(grant_a, keeper_a) = a.claim(PartitionId(0)).await? else {
        bail!("A did not get the empty partition");
    };
    println!(
        "  A became leader after {:?}, term {}",
        t0.elapsed(),
        grant_a.term()
    );

    // Real renewals, several rounds. This is where `PutMode::Update` is exercised.
    tokio::time::sleep(Duration::from_secs(2)).await;
    if !keeper_a.is_valid() {
        bail!("A lost its session while renewing normally — does the store return unstable etags?");
    }
    println!("  A kept its session over ~5 renewal rounds");

    // **B watches a healthy A for LONGER than the TTL and must not get the partition.**
    //
    // A real bug once lived here, showing only on stores whose etag is a hash of the
    // content: a renewal rewriting identical content ⇒ unchanged etag ⇒ observers thought
    // the owner was dead. Measured on MinIO. Sessions judge alive or dead by `beat`.
    let b = session("b").await?;
    let watch = Instant::now();
    while watch.elapsed() < Duration::from_secs(9) {
        if let Claim::Won(..) = b.claim(PartitionId(0)).await? {
            bail!(
                "B GOT the partition of A while A was renewing normally, after {:?}",
                watch.elapsed()
            );
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    println!(
        "  B watched for {:?} (3× TTL) without getting it — correct",
        watch.elapsed()
    );
    if !keeper_a.is_valid() {
        bail!("A lost its session though nobody could take it");
    }

    // A dies, without cleaning up.
    a.crash_handle().crash();
    keeper_a.abort_without_revoking();
    let t1 = Instant::now();
    let (grant_b, keeper_b) = loop {
        if let Claim::Won(g, k) = b.claim(PartitionId(0)).await? {
            break (g, k);
        }
        if t1.elapsed() > Duration::from_secs(30) {
            bail!("B did not get it within 30s");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    println!(
        "  B got it after {:?} (TTL 3s), term {} → {}",
        t1.elapsed(),
        grant_a.term(),
        grant_b.term()
    );
    if grant_b.term() <= grant_a.term() {
        bail!("the term did not increase on the change of owner");
    }
    // A's last renewal may be up to one tick before its death.
    if t1.elapsed() < Duration::from_millis(2600) {
        bail!("B got it BEFORE A's session had been silent for a full TTL — the CAS condition was not honoured");
    }
    keeper_b.release().await;
    b.close().await;
    Ok(())
}
