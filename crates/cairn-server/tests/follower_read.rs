// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Follower reads over HTTP.
//!
//! > A read from a node that is not the leader gives a correct result, with bounded
//! > lag — and a follower's "not seen" must **not** become a 404.
//!
//! Two nodes in the same process, the same `InMemory` bucket, one partition.
//! A holds the lease; B does not win it and serves reads through a `DbReader`.

// Tests run on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

use {
    anyhow::{bail, Result},
    cairn_engine::Engine,
    cairn_server::{
        actor::{EngineHandle, Leadership, Readiness},
        follower::FollowerReads,
        ingress::{AppState, Partitions},
        registry::Registry,
        router,
        routing::RoutingTable,
    },
    cairn_store::{
        Claim, ClusterConfig, LeaseConfig, ObjectStores, PartitionId, PartitionWriter, Session,
        StoreLocation,
    },
    slatedb::object_store::{memory::InMemory, ObjectStore},
    std::{
        sync::Arc,
        time::{Duration, Instant},
    },
};

const PARTITION: PartitionId = PartitionId(0);
const TTL: Duration = Duration::from_millis(600);

struct Node {
    url: String,
    handle: EngineHandle,
}

/// A node with the full HTTP layer: engine competing for the lease + routing + follower reads.
async fn spawn_http_node(stores: &ObjectStores, node_id: &str) -> Result<Node> {
    spawn_http_node_with(
        stores,
        node_id,
        cairn_store::NodeWalConfig::default().frame_age_flush,
    )
    .await
}

/// Like [`spawn_http_node`], with flush age `T` (D-j) for the node's shared WAL.
async fn spawn_http_node_with(
    stores: &ObjectStores,
    node_id: &str,
    frame_age_flush: Duration,
) -> Result<Node> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let advertised = format!("http://{addr}");

    let registry = Registry::new();
    let engine_stores = stores.clone();
    let session = Session::establish(
        Arc::clone(stores.main().store()),
        stores.main().prefix(),
        LeaseConfig::new()
            .with_node_id(node_id)
            .with_address(advertised.clone())
            // One listener for both roles: the `leader` hint of a `503` is the ingress
            // port, and here it is the same address as above.
            .with_ingress(advertised.clone())
            .with_ttl(TTL)
            .with_renew_interval(TTL / 6),
    )
    .await?;
    let engine_session = session.clone();
    // Shared WAL, like every node (it is the only WAL): the follower reads the part
    // not yet in L0 through the tailer, following the owner record's `wal_segment`.
    let wal = cairn_store::NodeWal::start(
        stores,
        session
            .current()
            .ok_or_else(|| anyhow::anyhow!("session not open"))?,
        cairn_store::NodeWalConfig {
            window: Duration::from_millis(5),
            frame_age_flush,
            ..cairn_store::NodeWalConfig::default()
        },
    );
    let handle = EngineHandle::spawn(
        Box::new(move || {
            let stores = engine_stores.clone();
            let session = engine_session.clone();
            let wal = wal.clone();
            Box::pin(async move {
                let (grant, mut keeper) = loop {
                    if let Claim::Won(g, k) = session.claim(PARTITION).await? {
                        break (g, k);
                    }
                    tokio::time::sleep(TTL / 6).await;
                };
                let db =
                    cairn_store::acquire_shared(&stores, &wal, &mut keeper, PARTITION.0).await?;
                let writer = PartitionWriter::open(grant, db).await?;
                Ok(Leadership {
                    engine: Engine::open(writer).await?,
                    keeper: Some(keeper),
                })
            })
        }),
        registry.clone(),
        None,
        Readiness::Immediately,
    )?;

    let routing = RoutingTable::new();
    routing.clone().follow(
        session,
        Arc::clone(stores.main().store()),
        stores.main().prefix().clone(),
        ClusterConfig::single(),
        Duration::from_millis(100),
    );
    let followers = FollowerReads::new(stores.clone(), Duration::from_millis(50));

    let app = router(AppState::new_in_cluster(
        Partitions::single(handle.clone()),
        registry,
        ClusterConfig::single(),
        routing,
        None,
        Some(followers),
    ));
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(Node {
        url: advertised,
        handle,
    })
}

async fn wait_leader(node: &Node) -> Result<()> {
    let t0 = Instant::now();
    loop {
        match node.handle.status([0x11; 24]).await {
            Ok(_) => return Ok(()),
            Err(e) if e.is_fenced() => {}
            Err(e) => bail!("unexpected error while probing leadership: {e}"),
        }
        if t0.elapsed() > Duration::from_secs(10) {
            bail!("no leader after 10s");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// **B serves reads for A's invocation, and does not lie about what it cannot see.**
#[tokio::test]
async fn follower_serves_reads_and_does_not_turn_not_yet_seen_into_404() -> Result<()> {
    let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stores = ObjectStores::single(StoreLocation::from_store(raw, "", "memory://"));
    stores.assert_conditional_put().await?;
    stores.assert_compare_and_swap().await?;

    let a = spawn_http_node(&stores, "a").await?;
    wait_leader(&a).await?;
    let b = spawn_http_node(&stores, "b").await?;

    // A writes a real invocation (entry `Input` + status Running) through its own
    // API — the same trick as failover.rs: `invoke` writes entry 0 before looking up
    // the deployment, so the call's 404 does not affect the state already durable.
    // The first two bytes are the partition — `/invocations/{id}` routes by it.
    let marker: cairn_server::actor::InvocationId =
        cairn_store::make_invocation_id(cairn_store::PartitionKey(0), [0xB2; 16]);
    let _ = a
        .handle
        .invoke(
            marker,
            cairn_engine::Target {
                tenant: Default::default(),
                scope: None,
                service: "nonexistent".into(),
                handler: "h".into(),
                object_key: String::new(),
            },
            None,
            None,
            bytes::Bytes::from_static(b"hello"),
        )
        .await?;
    let hex: String = marker.iter().map(|x| format!("{x:02x}")).collect();

    let http = reqwest::Client::new();

    // (1) B — not the leader — can still return the status, within a bounded lag
    // (the reader polls every 50ms; give it 40 cycles).
    let deadline = Instant::now() + Duration::from_secs(4);
    let body: serde_json::Value = loop {
        let r = http
            .get(format!("{}/invocations/{hex}", b.url))
            .send()
            .await?;
        if r.status().is_success() {
            // The chaos history checker tells stale reads apart by this header.
            assert_eq!(
                r.headers()
                    .get("x-cairn-read")
                    .and_then(|v| v.to_str().ok()),
                Some("follower"),
                "a status read from a follower must carry x-cairn-read: follower"
            );
            break r.json().await?;
        }
        if Instant::now() > deadline {
            bail!(
                "B could not serve a follower read after 4s (last code: {})",
                r.status()
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    // The create commit (`running`) and the end commit (`failed` 404) follow each other:
    // on an atomic store the follower exposes objects contiguously, so it usually sees
    // the outcome at once; before that the last object was not exposed yet and B returned
    // `running`.
    assert!(
        matches!(body["status"].as_str(), Some("running" | "failed")),
        "wrong status read from follower: {body}"
    );

    // (2) The journal can be read from B too, and is identical to A's.
    let from_b: serde_json::Value = http
        .get(format!("{}/invocations/{hex}/journal", b.url))
        .send()
        .await?
        .json()
        .await?;
    let from_a = a.handle.journal(marker).await?;
    assert_eq!(
        from_b.as_array().map(Vec::len),
        Some(from_a.len()),
        "journal read from follower differs from the leader: {from_b}"
    );

    // (3) An id that does NOT exist: a follower that does not see it **must not return
    // 404** — the reader is slightly stale, "not seen" is no proof of non-existence. It
    // must be a 503 pointing to the leader, with a machine-readable address.
    let unknown: String = cairn_store::make_invocation_id(cairn_store::PartitionKey(0), [0xFF; 16])
        .iter()
        .map(|x| format!("{x:02x}"))
        .collect();
    let r = http
        .get(format!("{}/invocations/{unknown}", b.url))
        .send()
        .await?;
    assert_eq!(
        r.status().as_u16(),
        503,
        "follower returned {} for an id it has not seen — a 404 from stale data is a lie",
        r.status()
    );
    let err: serde_json::Value = r.json().await?;
    assert_eq!(
        err["leader"].as_str(),
        Some(a.url.as_str()),
        "a 503 must carry a machine-readable leader address: {err}"
    );

    // (4) B's `/cluster` declares the reader open and healthy.
    let cluster: serde_json::Value = http
        .get(format!("{}/cluster", b.url))
        .send()
        .await?
        .json()
        .await?;
    let readers = cluster["readers"].as_array().expect("readers field");
    assert_eq!(readers.len(), 1, "one open reader on B: {cluster}");
    assert_eq!(readers[0]["healthy"], true, "{cluster}");

    // (5) On leader A a non-existent id is still a real 404 — only the follower has
    // to refrain.
    let r = http
        .get(format!("{}/invocations/{unknown}", a.url))
        .send()
        .await?;
    assert_eq!(r.status().as_u16(), 404);

    // (6) On leader A, the status carries `x-cairn-read: leader`.
    let r = http
        .get(format!("{}/invocations/{hex}", a.url))
        .send()
        .await?;
    assert!(r.status().is_success(), "{}", r.status());
    assert_eq!(
        r.headers()
            .get("x-cairn-read")
            .and_then(|v| v.to_str().ok()),
        Some("leader")
    );

    a.handle.shutdown().await?;
    b.handle.shutdown().await?;
    Ok(())
}

/// **Timestamps read on the follower are the same as on the leader**: A creates an
/// invocation; B (not the leader) returns `createdAt`/`modifiedAt` equal to the ones A
/// returns — the same `create_ts` of the same row (G20).
#[tokio::test]
async fn follower_reports_the_same_times_as_the_leader() -> Result<()> {
    let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stores = ObjectStores::single(StoreLocation::from_store(raw, "", "memory://"));
    let a = spawn_http_node(&stores, "a").await?;
    wait_leader(&a).await?;
    let b = spawn_http_node(&stores, "b").await?;
    let marker: cairn_server::actor::InvocationId =
        cairn_store::make_invocation_id(cairn_store::PartitionKey(0), [0xC3; 16]);
    invoke_missing(&a.handle, marker).await?;
    let http = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(4);
    let (leader, follower) = loop {
        let (_, src_a, at_a) = read_invocation(&http, &a.url, &marker).await?;
        let (code, src_b, at_b) = read_invocation(&http, &b.url, &marker).await?;
        assert_eq!(src_a.as_deref(), Some("leader"));
        if code == 200 && at_b["status"] == at_a["status"] {
            assert_eq!(src_b.as_deref(), Some("follower"));
            break (at_a, at_b);
        }
        if Instant::now() > deadline {
            bail!("B has not caught up with A after 4 s: {at_a} / {at_b}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    for f in ["createdAt", "modifiedAt", "endedAt"] {
        assert!(leader[f].as_i64().is_some(), "leader lacks {f}: {leader}");
        assert_eq!(
            leader[f], follower[f],
            "{f}: leader {leader} / follower {follower}"
        );
    }
    a.handle.shutdown().await?;
    b.handle.shutdown().await?;
    Ok(())
}

/// **The journal also says where it was read from**: like `GET /invocations/{id}`, a
/// journal page carries `x-cairn-read` — `leader` on A, `follower` on B. Without it a
/// reader (the console, the history checker) cannot tell "the reader's stale journal"
/// from "a journal that lost entries".
///
/// 1. A leads, writes an invocation; B does not lead.
/// 2. Read the journal on B until B sees it (the reader caught up), then on A.
///
/// Must see: B returns `x-cairn-read: follower`, A returns `x-cairn-read: leader`.
#[tokio::test]
async fn journal_page_names_its_read_source() -> Result<()> {
    let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stores = ObjectStores::single(StoreLocation::from_store(raw, "", "memory://"));
    let a = spawn_http_node(&stores, "a").await?;
    wait_leader(&a).await?;
    let b = spawn_http_node(&stores, "b").await?;
    let marker: cairn_server::actor::InvocationId =
        cairn_store::make_invocation_id(cairn_store::PartitionKey(0), [0xD4; 16]);
    invoke_missing(&a.handle, marker).await?;
    let hex: String = marker.iter().map(|x| format!("{x:02x}")).collect();
    let http = reqwest::Client::new();
    let source = |r: &reqwest::Response| {
        r.headers()
            .get("x-cairn-read")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    };
    let deadline = Instant::now() + Duration::from_secs(4);
    loop {
        let r = http
            .get(format!("{}/invocations/{hex}/journal", b.url))
            .send()
            .await?;
        if r.status().is_success() {
            assert_eq!(
                source(&r).as_deref(),
                Some("follower"),
                "a journal read from a follower must carry x-cairn-read: follower"
            );
            break;
        }
        if Instant::now() > deadline {
            bail!("B serves no journal after 4 s (last code {})", r.status());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let r = http
        .get(format!("{}/invocations/{hex}/journal", a.url))
        .send()
        .await?;
    assert!(r.status().is_success(), "{}", r.status());
    assert_eq!(source(&r).as_deref(), Some("leader"));
    a.handle.shutdown().await?;
    b.handle.shutdown().await?;
    Ok(())
}

/// **A hanging reader open holds the follower's map lock**: `FollowerReads::slot`
/// held the `slots` lock (shared by the whole node) across
/// `open_reader_with(..).await`, and the open retries without a cap.
///
/// 1. Every GET/LIST under `ranges/1/` hangs (a store so slow it does not answer).
/// 2. A follower read into range 1 opens a reader — the open hangs.
/// 3. `/cluster` and `/metrics` read the readers' status (`statuses`).
///
/// Must see: `statuses` answers at once, and the read into range 1 returns `None`
/// (pointing to the leader) within a time cap. Before the fix both hung until the
/// store recovered: monitoring stalled exactly when the store had an incident.
#[tokio::test]
async fn follower_reader_open_hang_does_not_block_statuses() -> Result<()> {
    use cairn_store::testkit::{Action, FaultyStore, OpSet, PathMatch, Rule};
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let stores = ObjectStores::single(StoreLocation::from_store(
        faulty.clone(),
        "cairn",
        "memory://cairn",
    ));
    faulty.add_rule(Rule::new(
        OpSet::GET | OpSet::LIST,
        PathMatch::Contains("cairn/ranges/1/".into()),
        Action::Hang,
    ));
    let followers = FollowerReads::new(stores, Duration::from_millis(50));
    let opening = {
        let f = followers.clone();
        tokio::spawn(async move { f.status(PartitionId(1), &[0; 24]).await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    let statuses = tokio::time::timeout(Duration::from_secs(1), followers.statuses()).await;
    assert!(
        statuses.is_ok(),
        "statuses() hangs after a hanging reader open"
    );
    let got = tokio::time::timeout(Duration::from_secs(10), opening).await;
    assert!(
        matches!(got, Ok(Ok(None))),
        "a follower read into a range whose reader open hangs did not return None in 10 s"
    );
    Ok(())
}

/// **A follower does not serve a stale in-progress status while the leader's log is
/// quiet** — on a store that **may tear** objects (`--wal-atomic-put false`), where the
/// tailer keeps the contiguous-exposure gate. This is the safety net (the follower
/// yields to the leader); on an atomic store the tail is exposed at once
/// ([`follower_sees_an_idle_leaders_tail_without_redirecting`]).
///
/// 1. A leads the range; an invocation `x` on A writes the create commit (`running`,
///    object `k` of A's shared WAL log) then the end commit (`failed` 404 — no
///    deployment declares the service; object `k + 1`).
/// 2. A's log goes quiet: no object after `k + 1`, so no header
///    `acked_upto ≥ k + 1` reaches B's tailer — B exposes only up to `k`.
/// 3. The test asks B for the status of `x` throughout this window.
/// 4. A writes more (another invocation): object `k + 2` carries `acked_upto ≥ k + 1`.
///
/// Must see: after the follower's cap on an unexposed tail (1 s at a 50 ms poll), B
/// **no longer** returns `running` — a `503` with the leader (the client follows it to
/// A and sees `failed`) or the outcome itself — even though L0 has not been flushed
/// (default T 60 s). After step 4, B serves the outcome as a follower: the tail is
/// exposed, the follower still carries the reads. Before the fix, B returned the stale
/// `running` until the age-based flush of the frame plus one reader manifest reload —
/// measured at 60–80 s with default parameters.
#[tokio::test]
async fn follower_does_not_serve_a_stale_status_when_the_leader_is_idle() -> Result<()> {
    let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stores = ObjectStores::single(StoreLocation::from_store(raw, "", "memory://"))
        .with_wal_atomic_put(Some(false));
    stores.assert_conditional_put().await?;
    stores.assert_compare_and_swap().await?;

    let a = spawn_http_node(&stores, "a").await?;
    wait_leader(&a).await?;
    let b = spawn_http_node(&stores, "b").await?;
    let http = reqwest::Client::new();
    let read = |base: String, hex: String| {
        let http = http.clone();
        async move {
            let r = http.get(format!("{base}/invocations/{hex}")).send().await?;
            let code = r.status().as_u16();
            let source = r
                .headers()
                .get("x-cairn-read")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned);
            let body: serde_json::Value = r.json().await.unwrap_or_default();
            anyhow::Ok((code, source, body))
        }
    };
    let hex = |id: &cairn_server::actor::InvocationId| -> String {
        id.iter().map(|x| format!("{x:02x}")).collect()
    };
    let invoke = |marker: cairn_server::actor::InvocationId| {
        let a = a.handle.clone();
        async move {
            a.invoke(
                marker,
                cairn_engine::Target {
                    tenant: Default::default(),
                    scope: None,
                    service: "nonexistent".into(),
                    handler: "h".into(),
                    object_key: String::new(),
                },
                None,
                None,
                bytes::Bytes::from_static(b"i4"),
            )
            .await
        }
    };

    // B opens its reader first (one read of an unknown id): the later read is by a
    // reader already following A's log — exactly the node the client asks again.
    let warm = cairn_store::make_invocation_id(cairn_store::PartitionKey(0), [0xEE; 16]);
    let _ = read(b.url.clone(), hex(&warm)).await?;

    let x = cairn_store::make_invocation_id(cairn_store::PartitionKey(0), [0xC4; 16]);
    let _ = invoke(x).await?;

    // (1) A has ended `x`.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (code, source, body) = read(a.url.clone(), hex(&x)).await?;
        if code == 200 && body["status"] == "failed" {
            assert_eq!(source.as_deref(), Some("leader"));
            break;
        }
        if Instant::now() > deadline {
            bail!("A has not ended the invocation after 5 s: {body}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let ended = Instant::now();

    // (2) From 2.5 s after that until 5 s: B no longer returns the stale `running`.
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    let mut redirected = false;
    while ended.elapsed() < Duration::from_secs(5) {
        let (code, source, body) = read(b.url.clone(), hex(&x)).await?;
        match code {
            200 => assert_eq!(
                body["status"],
                "failed",
                "B returned {} ({source:?}) {:?} after A had ended it — stale in-progress status",
                body["status"],
                ended.elapsed()
            ),
            503 => {
                assert_eq!(
                    body["leader"].as_str(),
                    Some(a.url.as_str()),
                    "a 503 must point to the leader: {body}"
                );
                redirected = true;
            }
            other => bail!("B returned unexpected code {other}: {body}"),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        redirected,
        "B never yielded to the leader (A's tail not exposed, L0 not flushed)"
    );

    // (3) A writes more ⇒ the tail is exposed ⇒ B serves the outcome of `x` as a follower.
    let y = cairn_store::make_invocation_id(cairn_store::PartitionKey(0), [0xC5; 16]);
    let _ = invoke(y).await?;
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (code, source, body) = read(b.url.clone(), hex(&x)).await?;
        if code == 200 && body["status"] == "failed" {
            assert_eq!(source.as_deref(), Some("follower"));
            break;
        }
        if Instant::now() > deadline {
            bail!("B did not serve the outcome as follower after A wrote more: {code} {body}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    a.handle.shutdown().await?;
    b.handle.shutdown().await?;
    Ok(())
}

/// GET `/invocations/<id>` on `base`: code, source (`x-cairn-read`), body.
async fn read_invocation(
    http: &reqwest::Client,
    base: &str,
    id: &cairn_server::actor::InvocationId,
) -> Result<(u16, Option<String>, serde_json::Value)> {
    let hex: String = id.iter().map(|x| format!("{x:02x}")).collect();
    let r = http.get(format!("{base}/invocations/{hex}")).send().await?;
    let code = r.status().as_u16();
    let source = r
        .headers()
        .get("x-cairn-read")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let body: serde_json::Value = r.json().await.unwrap_or_default();
    Ok((code, source, body))
}

/// An invocation to a service nobody declares: a create commit (`running`) then an end
/// commit (`failed` 404) — two consecutive objects on the leader's log.
async fn invoke_missing(
    handle: &EngineHandle,
    marker: cairn_server::actor::InvocationId,
) -> Result<()> {
    let _ = handle
        .invoke(
            marker,
            cairn_engine::Target {
                tenant: Default::default(),
                scope: None,
                service: "nonexistent".into(),
                handler: "h".into(),
                object_key: String::new(),
            },
            None,
            None,
            bytes::Bytes::from_static(b"i4"),
        )
        .await?;
    Ok(())
}

/// **A quiet tail is exposed to the follower on an atomic store** — the root cause of
/// the stale in-progress read is removed instead of worked around by yielding to the
/// leader.
///
/// 1. A leads the range; B has opened a reader (following A's shared WAL log).
/// 2. Invocation `x` on A: the create commit (`running`, object `k`) then the end
///    commit (`failed`, object `k + 1`). A's log goes **quiet** after `k + 1`: no
///    header `acked_upto ≥ k + 1` reaches B's tailer.
/// 3. The test asks B for the status of `x` for 5 s after A ends it.
///
/// Must see: on `memory://` (`atomic_put`) B returns **`failed` as a follower**
/// within a few polls (50 ms) — without waiting for the age-based flush of the frame
/// (default T 60 s) —, and from 500 ms after that **never** a `503` yielding to the
/// leader nor a stale `running` (in the first 500 ms B may not see `x` yet: a `503` is
/// correct). Before this change the tailer exposed only `seq ≤ acked_upto`: B returned
/// `running`, then, after the cap on an unexposed tail (1 s), a `503` with the leader.
#[tokio::test]
async fn follower_sees_an_idle_leaders_tail_without_redirecting() -> Result<()> {
    let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stores = ObjectStores::single(StoreLocation::from_store(raw, "", "memory://"));
    assert!(stores.wal().atomic_put());
    stores.assert_conditional_put().await?;
    stores.assert_compare_and_swap().await?;

    let a = spawn_http_node(&stores, "a").await?;
    wait_leader(&a).await?;
    let b = spawn_http_node(&stores, "b").await?;
    let http = reqwest::Client::new();

    // (1) B opens its reader first (one read of an unknown id).
    let warm = cairn_store::make_invocation_id(cairn_store::PartitionKey(0), [0xEE; 16]);
    let _ = read_invocation(&http, &b.url, &warm).await?;

    // (2) A ends `x`.
    let x = cairn_store::make_invocation_id(cairn_store::PartitionKey(0), [0xC6; 16]);
    invoke_missing(&a.handle, x).await?;
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (code, source, body) = read_invocation(&http, &a.url, &x).await?;
        if code == 200 && body["status"] == "failed" {
            assert_eq!(source.as_deref(), Some("leader"));
            break;
        }
        if Instant::now() > deadline {
            bail!("A has not ended the invocation after 5 s: {body}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let ended = Instant::now();

    // (3) B: `failed` as a follower within a few polls, then unchanged until 5 s.
    let mut seen: Option<Duration> = None;
    while ended.elapsed() < Duration::from_secs(5) {
        let (code, source, body) = read_invocation(&http, &b.url, &x).await?;
        let at = ended.elapsed();
        match (code, body["status"].as_str()) {
            (200, Some("failed")) => {
                assert_eq!(source.as_deref(), Some("follower"), "{body}");
                seen.get_or_insert(at);
            }
            (200, Some(st)) => {
                assert!(
                    seen.is_none(),
                    "B returned {st} after it had seen `failed` ({at:?})"
                );
                assert!(
                    at < Duration::from_millis(500),
                    "B still returns {st} ({source:?}) {at:?} after A ended it — tail not exposed"
                );
            }
            // Has not seen `x` yet (not at the tailer yet) ⇒ yielding to the leader is
            // by the rules ("not seen" does not become 404) — only in the first few polls.
            (503, _) if seen.is_none() && at < Duration::from_millis(500) => {}
            (503, _) => bail!("B yielded to the leader ({at:?}) on an atomic store: {body}"),
            (other, _) => bail!("B returned unexpected code {other}: {body}"),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(seen.is_some(), "B never returned `failed` as a follower");

    a.handle.shutdown().await?;
    b.handle.shutdown().await?;
    Ok(())
}

/// **A promise on a node that does not lead: `503` with `leader`.**
///
/// 1. A holds the lease of the only range; B runs, and B's routing table knows A is
///    the leader.
/// 2. A client sends `resolve`/`reject`/a read of an object's promise to B.
///
/// Must see: `503` with `leader` = A's ingress port, like `/invoke` and
/// `/invocations/{id}`. Without it the client (the AI SDK adapter's transport — a
/// reviewer pressing a button) does not know where to retry: measured on a 3-node
/// cluster, every approval sent to a non-leading node failed.
#[tokio::test]
async fn promise_on_non_leader_returns_503_with_leader() -> Result<()> {
    let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stores = ObjectStores::single(StoreLocation::from_store(raw, "", "memory://"));
    let a = spawn_http_node(&stores, "a").await?;
    wait_leader(&a).await?;
    let b = spawn_http_node(&stores, "b").await?;
    let http = reqwest::Client::new();
    let base = format!("{}/promises/Chat/c1/approve-x", b.url);
    for (method, url) in [
        (reqwest::Method::POST, format!("{base}/resolve")),
        (reqwest::Method::POST, format!("{base}/reject")),
        (reqwest::Method::GET, base.clone()),
    ] {
        // B's routing table refreshes every 100 ms: give it 4 s to learn about A.
        let deadline = Instant::now() + Duration::from_secs(4);
        let err: serde_json::Value = loop {
            let mut req = http.request(method.clone(), &url);
            if url.ends_with("/resolve") {
                req = req.json(&serde_json::json!({ "approved": true }));
            } else if url.ends_with("/reject") {
                req = req.json(&serde_json::json!({ "message": "not approved" }));
            }
            let r = req.send().await?;
            let status = r.status().as_u16();
            let body: serde_json::Value = r.json().await.unwrap_or_default();
            if status == 503 && body["leader"].is_string() {
                break body;
            }
            if Instant::now() > deadline {
                bail!("{method} {url}: {status} without a leader after 4s: {body}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        assert_eq!(
            err["leader"].as_str(),
            Some(a.url.as_str()),
            "{method} {url}: {err}"
        );
    }
    Ok(())
}
