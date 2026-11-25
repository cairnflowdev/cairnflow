// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! A **non-HTTP** work source, Kafka-shaped, going through `ingress::Router` +
//! `turn::admit` — under `kill -9` and leader changes, nothing
//! lost, nothing duplicated.
//!
//! This is the seam for a Kafka ingress (behind a feature gate): the consumer does not
//! know HTTP, only [`Router::submit`] and its three answers — accepted
//! (advance the offset), not the leader (go where [`Router::not_leader`] points),
//! another error or no answer (resend **the same offset**). The idempotency key is
//! `(topic, source partition, offset)`, so a resend after an ambiguous outcome matches the
//! duplicate instead of producing a second invocation.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{anyhow, bail, ensure, Result},
    bytes::Bytes,
    cairn_engine::{InvocationStatus, Target},
    cairn_server::{
        actor::ActorError,
        ingress::{Call, Delivery, RouteError, Router},
    },
    cairn_store::{InvocationId, TenantId},
    common::fake_sdk::{FakeSdk, Step},
    std::{
        collections::{BTreeMap, BTreeSet},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
        time::{Duration, Instant},
    },
};

const TTL: Duration = Duration::from_secs(2);
/// The number of source partitions (one consumer each) and of records per partition.
const SOURCES: usize = 3;
const RECORDS: usize = 40;
/// The limit of one send: a node frozen midway answers nothing at all.
const SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// The nodes the consumers can still reach — a killed node leaves this set.
struct Live {
    routers: Mutex<Vec<Option<Router>>>,
    urls: Vec<String>,
}

impl Live {
    fn router(&self, at: usize) -> Option<Router> {
        self.routers.lock().unwrap()[at].clone()
    }

    fn kill(&self, at: usize) {
        self.routers.lock().unwrap()[at] = None;
    }

    /// The next live node after `at`.
    fn next(&self, at: usize) -> usize {
        let r = self.routers.lock().unwrap();
        (1..=r.len())
            .map(|d| (at + d) % r.len())
            .find(|&i| r[i].is_some())
            .unwrap_or(at)
    }

    /// The node whose ingress port is `addr` (the `leader` hint of `NotLeader`).
    fn by_ingress(&self, addr: &str) -> Option<usize> {
        self.urls
            .iter()
            .position(|u| u.trim_end_matches('/') == addr.trim_end_matches('/'))
    }
}

fn record(p: usize, offset: usize) -> Call {
    Call {
        target: Target {
            tenant: TenantId::default(),
            scope: None,
            service: "Sink".into(),
            handler: "put".into(),
            object_key: format!("p{p}-{offset}"),
        },
        limit_key: None,
        idempotency_key: Some(format!("orders/{p}/{offset}")),
        input: Bytes::from(format!("{{\"p\":{p},\"offset\":{offset}}}")),
        log_level: None,
        tags: Vec::new(),
    }
}

#[derive(Default)]
struct Tally {
    /// `NotLeader` answers that led to a node change.
    redirects: AtomicUsize,
    /// Resends of the same offset after another error or no answer.
    resends: AtomicUsize,
}

/// One consumer: sends records in offset order, advancing the offset only once the
/// target range has answered. Returns the id of each offset.
async fn consume(
    p: usize,
    live: Arc<Live>,
    progress: Arc<AtomicUsize>,
    tally: Arc<Tally>,
) -> Result<Vec<InvocationId>> {
    let mut at = p % live.urls.len();
    let mut ids = Vec::with_capacity(RECORDS);
    for offset in 0..RECORDS {
        let call = record(p, offset);
        let t0 = Instant::now();
        let id = loop {
            ensure!(
                t0.elapsed() < Duration::from_secs(60),
                "record {p}/{offset} could not be sent after 60 s"
            );
            let Some(router) = live.router(at) else {
                at = live.next(at);
                continue;
            };
            match tokio::time::timeout(SEND_TIMEOUT, router.submit(call.clone(), Delivery::Send))
                .await
            {
                Ok(Ok(accepted)) => break accepted.id,
                Ok(Err(RouteError::Range { id, error })) if error.is_fenced() => {
                    tally.redirects.fetch_add(1, Ordering::Relaxed);
                    let hint = router.not_leader(&id);
                    at = hint
                        .leader
                        .as_deref()
                        .and_then(|a| live.by_ingress(a))
                        .filter(|&i| live.router(i).is_some())
                        .unwrap_or_else(|| live.next(at));
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Ok(Err(RouteError::Range {
                    error: ActorError::Overloaded { retry_after_ms },
                    ..
                })) => {
                    // Keep the offset, deliver again after `retry_after` — like Kafka.
                    tokio::time::sleep(Duration::from_millis(retry_after_ms)).await;
                }
                Ok(Err(RouteError::Refused(r))) => bail!("record {p}/{offset} refused: {r:?}"),
                // Ambiguous (no answer, the range is changing, the engine stopped): resend the same
                // offset — to another node, since this node may have died.
                Ok(Err(_)) | Err(_) => {
                    tally.resends.fetch_add(1, Ordering::Relaxed);
                    at = live.next(at);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        };
        ids.push(id);
        progress.fetch_add(1, Ordering::SeqCst);
    }
    Ok(ids)
}

/// Interleaving:
///
/// 1. A three-node cluster `a`, `b`, `c` on one bucket, 4 ranges, TTL 2 s. Fake SDK:
///    `Sink/put` (an object, one key per record) returns `ok`.
/// 2. Three consumers (one per source partition), 40 records each,
///    attached to `a`, `b`, `c` in turn; sending through [`Router::submit`] (`Send`), idempotency
///    key `orders/<p>/<offset>`.
/// 3. Once 1/3 of the total is accepted: **`kill -9` `b`** — its store hangs every
///    request (the shape of `kill -9` as seen from the bucket), the node is dropped without shutdown;
///    its ranges change owner after the TTL. The send in flight to `b` gets no answer.
/// 4. Once 2/3 are accepted: **`c` shuts down gracefully** — its ranges are yielded to `a` at once.
/// 5. The consumers follow `NotLeader`/time-outs, resending the same offset.
///
/// Must see: every record has exactly one invocation (the SDK sees one id per key, and
/// it is the id derived from the key), every invocation is `completed`, the number of `Sink`
/// invocations in the cluster equals the number of records; and the run really did resend or
/// change node at least once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kafka_shaped_source_survives_kill_and_leader_change() -> Result<()> {
    let sdk = FakeSdk::builder()
        .object("Sink", "put", vec![Step::output("ok")])
        .spawn()
        .await?;
    let (_bucket, mut nodes) = common::cluster_nodes(&["a", "b", "c"], 4, TTL).await?;
    let r = common::within(Duration::from_secs(180), "Kafka-shaped source", async {
        common::register(&nodes[0].node.url(), &sdk.url).await?;
        {
            let refs: Vec<_> = nodes.iter().collect();
            // Every node leads at least one range: killing `b` and shutting down `c` must really
            // change leaders.
            common::wait_probe(
                &refs,
                Duration::from_secs(60),
                "one leader per range, at least one range per node",
                |m| {
                    m.len() == 4
                        && m.values().all(|v| v.len() == 1)
                        && (0..3).all(|i| m.values().any(|v| v[0] == i))
                },
            )
            .await?;
        }
        // The deployment registry reaches every node (synced through the bucket): a consumer asks
        // `derive` on the node it is attached to.
        let probe = record(0, 0);
        let t0 = Instant::now();
        while !nodes.iter().all(|n| n.node.router.derive(&probe).is_ok()) {
            ensure!(
                t0.elapsed() < Duration::from_secs(20),
                "the deployment registry has not reached every node"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let live = Arc::new(Live {
            routers: Mutex::new(nodes.iter().map(|n| Some(n.node.router.clone())).collect()),
            urls: nodes.iter().map(|n| n.node.url()).collect(),
        });
        let progress = Arc::new(AtomicUsize::new(0));
        let tally = Arc::new(Tally::default());
        let consumers: Vec<_> = (0..SOURCES)
            .map(|p| tokio::spawn(consume(p, live.clone(), progress.clone(), tally.clone())))
            .collect();

        let total = SOURCES * RECORDS;
        let wait_progress = |n: usize| {
            let progress = progress.clone();
            async move {
                let t0 = Instant::now();
                while progress.load(Ordering::SeqCst) < n {
                    ensure!(
                        t0.elapsed() < Duration::from_secs(60),
                        "consumer stuck at {} / {n}",
                        progress.load(Ordering::SeqCst)
                    );
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Ok::<_, anyhow::Error>(())
            }
        };

        wait_progress(total / 3).await?;
        common::freeze(&nodes[1].store);
        live.kill(1);
        let b = nodes.remove(1);
        drop(b.node);

        wait_progress(2 * total / 3).await?;
        live.kill(1); // `c` is now at index 1 of `nodes`, index 2 of `live`
        live.kill(2);
        let c = nodes.remove(1);
        c.node.shutdown().await;

        let mut expected: BTreeMap<String, InvocationId> = BTreeMap::new();
        for (p, h) in consumers.into_iter().enumerate() {
            let ids = h
                .await
                .map_err(|e| anyhow!("consumer {p} panicked: {e}"))??;
            for (offset, id) in ids.into_iter().enumerate() {
                expected.insert(format!("p{p}-{offset}"), id);
            }
        }
        ensure!(expected.len() == total);
        eprintln!(
            "router_source: {} node changes following NotLeader, {} resends",
            tally.redirects.load(Ordering::Relaxed),
            tally.resends.load(Ordering::Relaxed)
        );
        ensure!(
            tally.redirects.load(Ordering::Relaxed) + tally.resends.load(Ordering::Relaxed) > 0,
            "the kill and the leader change touched no send — the test checks nothing"
        );

        // Each record's id is the id derived from its key (whichever consumer, node
        // or send attempt).
        let a = nodes[0].node.router.clone();
        for p in 0..SOURCES {
            for offset in 0..RECORDS {
                let want = a.derive(&record(p, offset)).map_err(|r| anyhow!("{r:?}"))?;
                ensure!(
                    expected[&format!("p{p}-{offset}")] == want,
                    "record {p}/{offset}: the accepted id differs from the id derived from the key"
                );
            }
        }

        // Nothing lost: every invocation completes on `a`.
        for (key, id) in &expected {
            let id = *id;
            let t0 = Instant::now();
            loop {
                let status = a
                    .ask_range(cairn_store::partition_key_of(&id), |e| async move {
                        e.status(id).await
                    })
                    .await
                    .map(|(_, r)| r);
                match status {
                    Some(Ok(Some(InvocationStatus::Completed(_)))) => break,
                    other if t0.elapsed() > Duration::from_secs(60) => {
                        bail!("record {key} not completed after 60 s: {other:?}")
                    }
                    _ => tokio::time::sleep(Duration::from_millis(50)).await,
                }
            }
        }

        // Nothing duplicated: the SDK sees exactly one id per key, and the cluster has exactly
        // one invocation per record.
        let mut seen: BTreeMap<String, BTreeSet<Vec<u8>>> = BTreeMap::new();
        for s in sdk.starts_of("Sink", "put") {
            seen.entry(s.key.clone()).or_default().insert(s.id.to_vec());
        }
        ensure!(
            seen.len() == total,
            "SDK saw {} keys, must be {total}",
            seen.len()
        );
        for (key, ids) in &seen {
            ensure!(
                ids.len() == 1 && ids.contains(expected[key].as_slice()),
                "record {key}: SDK saw {} invocations",
                ids.len()
            );
        }
        let mut listed = 0;
        for (_, engine) in nodes[0].node.partitions.iter() {
            listed += engine
                .list_invocations(usize::MAX, None)
                .await
                .map_err(|e| anyhow!("list: {e}"))?
                .into_iter()
                .filter(|(_, _, t)| t.as_ref().is_some_and(|t| t.service == "Sink"))
                .count();
        }
        ensure!(
            listed == total,
            "the cluster has {listed} Sink invocations, must be {total}"
        );
        Ok(())
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// `Source::Restart { patch }`: `restart` can carry an
/// input patch for entry `n` ("fix it and re-run from a step"), but **before the
/// feature gate** a patch is refused with `400` — no restart, no
/// new invocation, the old one untouched.
///
/// Order:
/// 1. `Obj/hold` waits (`suspended`).
/// 2. `POST /invocations/{id}/restart` with body `{"patch": {"entry": 0, "input": …}}`.
/// 3. The same command without a body.
///
/// Must see: (2) ⇒ `400`, the old one still `suspended`, the cluster still has one invocation;
/// (3) ⇒ `200` with a new id (a restart without a patch still runs as before).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_with_patch_is_rejected_before_the_feature_gate() -> Result<()> {
    let sdk = FakeSdk::builder()
        .object("Obj", "hold", vec![Step::Suspend, Step::output("held")])
        .spawn()
        .await?;
    let node = common::start_single_process(1).await?;
    let url = node.url();
    let r = common::within(common::TEST_DEADLINE, "restart with a patch", async {
        common::register(&url, &sdk.url).await?;
        let (code, v) = common::send(&url, "Obj", "hold", Some("k"), "{}").await?;
        ensure!(code == 200 || code == 202, "{code} {v}");
        let id = v["id"].as_str().unwrap_or_default().to_string();
        common::wait_status(&url, &id, "suspended").await?;

        let client = reqwest::Client::new();
        let patched = client
            .post(format!("{url}/invocations/{id}/restart"))
            .json(&serde_json::json!({ "patch": { "entry": 0, "input": "{\"fixed\":true}" } }))
            .send()
            .await?;
        let status = patched.status().as_u16();
        let body = patched.text().await?;
        ensure!(
            status == 400,
            "restart with a patch must be 400, got {status}: {body}"
        );
        let (_, st) = common::invocation(&url, &id).await?;
        ensure!(st["status"] == "suspended", "the old one was touched: {st}");
        let list: serde_json::Value = client
            .get(format!("{url}/invocations"))
            .send()
            .await?
            .json()
            .await?;
        ensure!(
            list.as_array().map(Vec::len) == Some(1),
            "restart refused yet a new invocation exists: {list}"
        );

        let plain = client
            .post(format!("{url}/invocations/{id}/restart"))
            .send()
            .await?;
        let status = plain.status().as_u16();
        let body: serde_json::Value = plain.json().await?;
        ensure!(
            status == 200 && body["new"].is_string(),
            "restart without a patch: {status} {body}"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}
