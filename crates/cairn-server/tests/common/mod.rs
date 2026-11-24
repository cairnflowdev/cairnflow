// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Shared test infrastructure: real in-process nodes + a scripted fake SDK.
//!
//! Nodes are built with exactly the [`cairn_server::node::start`] the binary uses —
//! many partitions, real HTTP transport between them. Each test file's `mod common;`
//! takes what it needs; the rest is dead code in that file, so warnings are off here.

#![allow(dead_code)]

pub mod bench;
pub mod binary;
pub mod fake_sdk;
pub mod fixture;
#[cfg(feature = "testkit")]
pub mod net;
pub mod oidc;
pub mod rr_sdk;
#[cfg(feature = "testkit")]
pub mod tls_edge;

use {
    anyhow::{anyhow, Context, Result},
    cairn_server::node::{self, NodeConfig, NodeMode, RunningNode},
    cairn_store::{ObjectStores, StoreLocation},
    std::{future::Future, time::Duration},
};

/// The default deadline of a test using real nodes. A hang turns red fast instead of hanging CI.
pub const TEST_DEADLINE: Duration = Duration::from_secs(30);

/// A brand-new `memory://` store — every call is a separate bucket. With
/// `CAIRNFLOW_TEST_FAULTS` set it is wrapped in a `FaultyStore` carrying that fault
/// ([`install_env_faults`]).
pub fn memory_stores() -> ObjectStores {
    if env_faults().is_some() {
        let store = cairn_store::testkit::FaultyStore::wrap(std::sync::Arc::new(
            slatedb::object_store::memory::InMemory::new(),
        ));
        install_env_faults(&store);
        return ObjectStores::single(StoreLocation::from_store(store, "", "memory://"));
    }
    ObjectStores::single(StoreLocation::parse("memory://").expect("memory:// always parses"))
}

/// `CAIRNFLOW_TEST_FAULTS`, if set and non-empty.
fn env_faults() -> Option<String> {
    std::env::var("CAIRNFLOW_TEST_FAULTS")
        .ok()
        .filter(|v| !v.is_empty())
}

/// Faults injected per `CAIRNFLOW_TEST_FAULTS` into the store of every test node —
/// the "ambiguous once per op" row of `check.sh` (inspired by Neon's
/// `UnreliableWrapper`):
///
/// - `each-once`: for every pair (write or delete, path) of an object **cairn manages
///   itself** — the lease layer (`owners/`, `sessions/`, …) and the rest
///   (`cluster.conf`, `rules`, catalog, deployments, `auth/`, …) — the first attempt is
///   **ambiguous**: it reaches the store and is applied, then the caller gets the
///   answer of object_store's own retry (`Precondition` for a CAS, `AlreadyExists` for
///   a `Create`). Rule 7 says every write site must resolve it.
///
///   Does not touch SlateDB's WAL, manifest and SSTs or the shared WAL log: there
///   `AlreadyExists` **is** the fence (D12, NW-2) — a deliberate fence or fail-stop,
///   so every write would fail-stop.
/// - `each-once+nodewal`: like `each-once`, plus the `nodewal` layer — every PUT of
///   the shared WAL log (data, seal, floor, index) reaches the store the first time
///   and then gets the answer of object_store's own retry (case (b): a `Create` sees
///   `AlreadyExists` on cairn's very first attempt). On code from before ambiguous
///   log PUTs were resolved by reading back, every data PUT was a fail-stop, so its
///   `check.sh` row is off by default (`CHECK_EACH_ONCE_NODEWAL=1` turns it on).
pub fn install_env_faults(store: &cairn_store::testkit::FaultyStore) {
    use cairn_store::testkit::{Action, Ambiguity, OpSet, PathClass, PathMatch, Rule};
    let classes: &[PathClass] = match env_faults().as_deref() {
        None => return,
        Some("each-once") => &[PathClass::Lease, PathClass::Other],
        Some("each-once+nodewal") => &[PathClass::Lease, PathClass::Other, PathClass::NodeWal],
        Some(other) => {
            panic!("CAIRNFLOW_TEST_FAULTS={other:?} unknown — only each-once, each-once+nodewal")
        }
    };
    for &class in classes {
        store.add_rule(Rule::new(
            OpSet::PUT | OpSet::DELETE,
            PathMatch::Class(class),
            Action::EachOnce(Box::new(Action::Ambiguous {
                reply: Ambiguity::Retried,
                skip: 0,
                times: None,
            })),
        ));
    }
}

/// Configuration for tests: `memory://`, ports chosen by the OS, short background and
/// WAL flush ticks so that tests do not wait at production pace.
pub fn test_config(mode: NodeMode) -> NodeConfig {
    let mut cfg = NodeConfig::new(memory_stores(), mode);
    cfg.tick_interval = Some(Duration::from_millis(50));
    cfg.flush_interval = Some(Duration::from_millis(5));
    // `CAIRNFLOW_TEST_QUERY=process`: every node of the test runs the asker's `/query`
    // in a child process — rerun the whole suite in that mode.
    if std::env::var("CAIRNFLOW_TEST_QUERY").as_deref() == Ok("process") {
        cfg.query_worker = Some(query_worker());
    }
    cfg
}

/// The `/query` child process (the `cairn-query-worker` binary cargo builds for the
/// crate's tests), default memory cap.
pub fn query_worker() -> cairn_server::query::QueryWorkerConfig {
    cairn_server::query::QueryWorkerConfig {
        path: env!("CARGO_BIN_EXE_cairn-query-worker").into(),
        memory_bytes: cairn_server::query::DEFAULT_WORKER_MEMORY_MB << 20,
    }
}

/// A node with `partitions` partitions on `memory://` — a one-node cluster, like the
/// binary without `--cluster` — already leading every range.
pub async fn start_single_process(partitions: u16) -> Result<RunningNode> {
    start_led(test_config(NodeMode::single(partitions))).await
}

/// `POST` to `node`'s **internal port**, signed with its cluster secret — as another
/// node sends. The ingress port has no `/internal/*`.
pub async fn post_internal(
    node: &RunningNode,
    path_and_query: &str,
    body: Vec<u8>,
) -> Result<reqwest::Response> {
    let client = reqwest::Client::new();
    let req = client
        .post(format!("{}{path_and_query}", node.internal_url()))
        .body(body);
    Ok(node.cluster_secret.send(&client, req).await?)
}

/// Run `fut` with deadline `deadline`. Past the deadline ⇒ `Err` naming what was awaited.
pub async fn within<T>(
    deadline: Duration,
    what: &str,
    fut: impl Future<Output = Result<T>>,
) -> Result<T> {
    tokio::time::timeout(deadline, fut)
        .await
        .map_err(|_| anyhow!("past {deadline:?} and not done: {what}"))?
}

/// Register a deployment with the node over HTTP. Returns the response JSON.
pub async fn register(node_url: &str, sdk_url: &str) -> Result<serde_json::Value> {
    let resp = reqwest::Client::new()
        .post(format!("{node_url}/deployments"))
        .json(&serde_json::json!({ "uri": sdk_url }))
        .send()
        .await?;
    let status = resp.status();
    let body: serde_json::Value = resp
        .json()
        .await
        .context("registration response is not JSON")?;
    if !status.is_success() {
        return Err(anyhow!("deployment registration returned {status}: {body}"));
    }
    Ok(body)
}

/// Asks `GET /services` (default tenant) on every node in `node_urls` until each one
/// lists every name in `services`, within 40 s.
///
/// A registration (`POST /deployments`) lands in the registry of the node that served
/// it; the other nodes read it from the bucket after the registry-change notification
/// (`registry_sync`), so for a moment their ingress still answers `404 "no deployment
/// declares service … yet"`. A cluster test that registers on one node and then sends
/// to another waits here first. `/services` reads the same current-revision catalog as
/// ingress's `handler_kind`. The deadline is above the 30 s registry poll
/// (`DEFAULT_DEPLOY_REFRESH`), the net for a notification a loaded machine dropped.
pub async fn wait_services_known(node_urls: &[String], services: &[&str]) -> Result<()> {
    let http = reqwest::Client::new();
    within(
        Duration::from_secs(40),
        &format!("every node lists services {services:?}"),
        async {
            for url in node_urls {
                loop {
                    let r = http.get(format!("{url}/services")).send().await?;
                    let status = r.status();
                    let v: serde_json::Value = r.json().await.context("/services is not JSON")?;
                    if !status.is_success() {
                        return Err(anyhow!("GET {url}/services returned {status}: {v}"));
                    }
                    let known = |name: &str| {
                        v.as_array()
                            .is_some_and(|a| a.iter().any(|s| s["name"] == name))
                    };
                    if services.iter().all(|s| known(s)) {
                        break;
                    }
                    // A polling tick inside a bounded loop (AGENTS.md §6.3).
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
            Ok(())
        },
    )
    .await
}

/// `POST /invoke/{service}/{handler}[?key=..]`. Returns the HTTP code and the response JSON.
pub async fn invoke(
    node_url: &str,
    service: &str,
    handler: &str,
    key: Option<&str>,
    body: &str,
) -> Result<(u16, serde_json::Value)> {
    post_invoke(format!("{node_url}/invoke/{service}/{handler}"), key, body).await
}

/// `POST /invoke/{service}/{handler}/send` — answers as soon as the invocation is
/// durable, without waiting for it to run.
pub async fn send(
    node_url: &str,
    service: &str,
    handler: &str,
    key: Option<&str>,
    body: &str,
) -> Result<(u16, serde_json::Value)> {
    post_invoke(
        format!("{node_url}/invoke/{service}/{handler}/send"),
        key,
        body,
    )
    .await
}

async fn post_invoke(
    mut url: String,
    key: Option<&str>,
    body: &str,
) -> Result<(u16, serde_json::Value)> {
    if let Some(k) = key {
        url.push_str(&format!("?key={k}"));
    }
    let resp = reqwest::Client::new()
        .post(url)
        .body(body.to_string())
        .send()
        .await?;
    let status = resp.status().as_u16();
    let json = resp
        .json()
        .await
        .context("the invoke response is not valid JSON")?;
    Ok((status, json))
}

/// `GET /invocations/{id}`. Returns the HTTP code and the response JSON.
pub async fn invocation(node_url: &str, id: &str) -> Result<(u16, serde_json::Value)> {
    let resp = reqwest::get(format!("{node_url}/invocations/{id}")).await?;
    let status = resp.status().as_u16();
    let json = resp
        .json()
        .await
        .context("the status response is not valid JSON")?;
    Ok((status, json))
}

/// The partition holding invocation `id`, per `GET /invocations` — asks the API
/// rather than reading the id prefix, because the id layout is not a contract.
pub async fn partition_of(node_url: &str, id: &str) -> Result<u32> {
    let rows: Vec<serde_json::Value> = reqwest::get(format!("{node_url}/invocations?limit=1000"))
        .await?
        .json()
        .await
        .context("invocation list is not JSON")?;
    rows.iter()
        .find(|r| r["id"] == id)
        .and_then(|r| r["partition"].as_u64())
        .map(|p| p as u32)
        .ok_or_else(|| anyhow!("{id} not found in /invocations"))
}

/// The value of one sample on `/metrics` — `sample` is the verbatim text before the
/// value (name with labels, in the order the node prints them). Absent ⇒ `Err`.
pub async fn metric(node_url: &str, sample: &str) -> Result<u64> {
    let text = reqwest::get(format!("{node_url}/metrics"))
        .await?
        .text()
        .await?;
    text.lines()
        .find_map(|l| l.strip_prefix(sample)?.strip_prefix(' ')?.parse().ok())
        .ok_or_else(|| anyhow!("/metrics has no {sample}"))
}

/// Ask `/invocations/{id}` until the status is `want`.
pub async fn wait_status(node_url: &str, id: &str, want: &str) -> Result<serde_json::Value> {
    loop {
        let (code, st) = invocation(node_url, id).await?;
        if code == 200 && st["status"] == want {
            return Ok(st);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// An object key of `service` that lands in `partition`, searched with exactly the
/// hash the node uses.
pub fn key_on_partition(node: &RunningNode, service: &str, partition: u32, prefix: &str) -> String {
    (0..10_000)
        .map(|i| format!("{prefix}-{i}"))
        .find(|k| {
            node.cluster
                .partition_of(&cairn_store::TenantId::default(), service, k)
                .0
                == partition
        })
        .unwrap_or_else(|| panic!("no key of {service} lands in partition {partition}"))
}

// ───────────────────────────────────────────── a bucket verified on cleanup

/// The `InMemory` bucket of a cluster test, **running `cairn-ctl verify` on cleanup**
/// (inspired by Neon, which runs a scrubber after every test).
///
/// `Drop` — the test finished without panicking — runs a full
/// [`Ctl::verify`](cairn_server::ctl::Ctl::verify) (opening every Db with the
/// production loader) on the bucket, at the empty prefix of [`start_cluster_node`].
/// The test's nodes have already been dropped (declared after the bucket ⇒ dropped
/// first), so the bucket looks as after a `kill -9` at the point where the test stopped.
///
/// **Which findings are errors.** At an arbitrary `kill -9` point, some half-done
/// states are correct by design: a parent that has left the table before its owner
/// wrote the tombstone (SM-11 writes it on the owner's behalf when the owner dies), a
/// child of a rolled-back intent not yet cleaned up (SM-14), a range never opened —
/// verify marks them with `heals`. By default only findings that **no loop heals**
/// (`heals == None`: a lost manifest of an owned range, `external_dbs` pointing at
/// nothing, a broken table, a checkpoint nobody sees any more, a stuck intent) are
/// errors: they must hold at **every** kill point. `CAIRNFLOW_TEST_VERIFY=all` treats
/// every finding as an error (leak hunting), `=0` turns it off (when a test returns
/// `Err`, a panic from verify would hide the original error). Findings in
/// [`Bucket::allow`] ⇒ ignored; the rest ⇒ panic, with the list.
///
/// A test that **deliberately** leaves debris no loop heals (a fabricated broken
/// table, a deleted manifest, …) declares the allowed finding by check name and reason.
pub struct Bucket {
    mem: std::sync::Arc<slatedb::object_store::memory::InMemory>,
    allowed: std::sync::Mutex<Vec<(&'static str, &'static str)>>,
}

/// A new bucket for a cluster test. See [`Bucket`].
pub fn bucket() -> Bucket {
    Bucket {
        mem: std::sync::Arc::new(slatedb::object_store::memory::InMemory::new()),
        allowed: Default::default(),
    }
}

impl std::ops::Deref for Bucket {
    type Target = std::sync::Arc<slatedb::object_store::memory::InMemory>;

    fn deref(&self) -> &Self::Target {
        &self.mem
    }
}

impl Bucket {
    /// Finding `check` (`orphan_db`, `stuck_intent`, …) is allowed on cleanup — the
    /// test leaves it on purpose. `why` is mandatory: an exemption list without reasons
    /// is where real findings go to hide.
    pub fn allow(&self, check: &'static str, why: &'static str) {
        assert!(!why.trim().is_empty(), "allow({check}) needs a reason");
        self.allowed.lock().unwrap().push((check, why));
    }

    /// Run `verify` now (without waiting for cleanup) — findings not yet allowed.
    pub async fn verify(&self) -> Result<Vec<cairn_server::ctl::Finding>> {
        let allowed = self.allowed.lock().unwrap().clone();
        verify_bucket(self.mem.clone(), allowed).await
    }
}

/// Every object left on the bucket that cairnflow wrote but that has **no line** in
/// `OBJECT_POLICIES`: nobody wrote down its compatibility rule, so the first layout
/// change would not know what it breaks.
pub async fn unlisted_objects(
    mem: &std::sync::Arc<slatedb::object_store::memory::InMemory>,
) -> Result<Vec<String>> {
    use {futures::TryStreamExt, slatedb::object_store::ObjectStore};
    let all: Vec<_> = mem
        .list(None)
        .try_collect()
        .await
        .map_err(|e| anyhow!("bucket listing failed: {e}"))?;
    let mut out: Vec<String> = all
        .into_iter()
        .map(|m| m.location.as_ref().to_string())
        .filter(|rel| cairn_store::unlisted_object(rel))
        .collect();
    out.sort();
    Ok(out)
}

async fn verify_bucket(
    mem: std::sync::Arc<slatedb::object_store::memory::InMemory>,
    allowed: Vec<(&'static str, &'static str)>,
) -> Result<Vec<cairn_server::ctl::Finding>> {
    let ctl = cairn_server::ctl::Ctl::new(ObjectStores::single(StoreLocation::from_store(
        mem.clone(),
        "",
        "memory://",
    )));
    // `CAIRNFLOW_TEST_RESTORE_CHECKS=0` turns off the lost-cross-range-message check (j)
    // — to measure its cost, not to dodge a finding.
    let rep = ctl
        .verify(cairn_server::ctl::VerifyOptions {
            restore_checks: std::env::var("CAIRNFLOW_TEST_RESTORE_CHECKS").as_deref() != Ok("0"),
            // `sst_missing`: the nodes have stopped, so no GC runs concurrently — every
            // cluster test is a "no false positive" trial.
            deep: true,
            ..cairn_server::ctl::VerifyOptions::default()
        })
        .await
        .map_err(|e| anyhow!("verify failed: {e}"))?;
    let all = std::env::var("CAIRNFLOW_TEST_VERIFY").as_deref() == Ok("all");
    let unlisted = unlisted_objects(&mem).await?;
    Ok(rep
        .findings
        .into_iter()
        .chain(unlisted.into_iter().map(|rel| cairn_server::ctl::Finding {
            check: "unlisted_object",
            range: None,
            detail: format!("{rel}: no line in cairn_store::OBJECT_POLICIES"),
            heals: None,
        }))
        // A bucket that never had a cluster (the test stopped before the first node came up).
        .filter(|f| !(f.check == "table" && f.detail == "no cluster.conf"))
        .filter(|f| all || f.heals.is_none())
        .filter(|f| !allowed.iter().any(|(c, _)| *c == f.check))
        .collect())
}

impl Drop for Bucket {
    fn drop(&mut self) {
        if std::thread::panicking() || std::env::var("CAIRNFLOW_TEST_VERIFY").as_deref() == Ok("0")
        {
            return;
        }
        let (mem, allowed) = (self.mem.clone(), self.allowed.lock().unwrap().clone());
        // Drop runs inside the test's runtime: verify needs a runtime of its own, on
        // a thread of its own.
        let found = std::thread::spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("verify's runtime")
                .block_on(async {
                    tokio::time::timeout(Duration::from_secs(60), verify_bucket(mem, allowed))
                        .await
                        .unwrap_or_else(|_| Err(anyhow!("verify took over 60 s")))
                })
        })
        .join()
        .expect("verify thread panicked");
        match found {
            Ok(f) if f.is_empty() => {}
            Ok(f) => panic!(
                "verify on cleanup: {} findings not allowed (Bucket::allow):\n{}",
                f.len(),
                f.iter()
                    .map(|f| format!(
                        "  {} range {:?}: {} (healing loop: {})",
                        f.check,
                        f.range,
                        f.detail,
                        f.heals.unwrap_or("none")
                    ))
                    .collect::<Vec<_>>()
                    .join("\n")
            ),
            Err(e) => panic!("verify on cleanup failed: {e}"),
        }
    }
}

// ───────────────────────────────────────────────────── multi-node clusters

/// One node of a test cluster, with **its own** counting/fault-injecting store — all
/// nodes share one `InMemory` bucket underneath, but faults injected into this node
/// do not touch the others.
pub struct ClusterNode {
    pub node: RunningNode,
    pub store: std::sync::Arc<cairn_store::testkit::FaultyStore>,
}

/// The configuration of a node for "one-node" tests: a **one-node cluster** (also what
/// the binary runs without `--cluster`), shared WAL — the only WAL there is.
pub fn solo_config(partitions: u16) -> NodeConfig {
    let mut cfg = test_config(NodeMode {
        partitions,
        lease_ttl: Duration::from_secs(2),
        node_id: Some("node-solo".into()),
    });
    cfg.node_wal.window = Duration::from_millis(5);
    cfg
}

/// Build a node per `cfg`, then wait (up to 20 s) until every range in the table has
/// an engine leading on it — in cluster mode, `node::start` returns before the lease
/// is claimed.
pub async fn start_led(cfg: NodeConfig) -> Result<RunningNode> {
    let node = node::start(cfg)
        .await
        .map_err(|e| anyhow!("could not build the node: {e}"))?;
    let t0 = std::time::Instant::now();
    loop {
        let table = node.cluster.table();
        let mut all = true;
        for r in table.ranges() {
            let probe =
                cairn_store::make_invocation_id(cairn_store::PartitionKey(r.start), [0; 16]);
            let led = match node.partitions.get(r.id) {
                Some(h) => matches!(
                    tokio::time::timeout(Duration::from_millis(300), h.status(probe)).await,
                    Ok(Ok(_))
                ),
                None => false,
            };
            all &= led;
        }
        if all {
            return Ok(node);
        }
        if t0.elapsed() > Duration::from_secs(20) {
            return Err(anyhow!("node not leading every range after 20 s"));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Build a cluster node named `node_id` on bucket `shared`.
pub async fn start_cluster_node(
    shared: &std::sync::Arc<slatedb::object_store::memory::InMemory>,
    node_id: &str,
    partitions: u16,
    ttl: Duration,
) -> Result<ClusterNode> {
    start_cluster_node_tweaked(shared, node_id, partitions, ttl, |_| {}).await
}

/// Like [`start_cluster_node`], with the configuration further adjusted by `tweak`
/// before building (auth, ports, …).
pub async fn start_cluster_node_tweaked(
    shared: &std::sync::Arc<slatedb::object_store::memory::InMemory>,
    node_id: &str,
    partitions: u16,
    ttl: Duration,
    tweak: impl FnOnce(&mut NodeConfig),
) -> Result<ClusterNode> {
    let store = cairn_store::testkit::FaultyStore::wrap(shared.clone());
    install_env_faults(&store);
    let node = start_cluster_node_on(&store, node_id, partitions, ttl, tweak).await?;
    Ok(ClusterNode { node, store })
}

/// Like [`start_cluster_node_tweaked`], on an existing `store` — the test sets fault
/// rules **before** the node starts (the boot path). Does not add the
/// `CAIRNFLOW_TEST_FAULTS` faults.
pub async fn start_cluster_node_on(
    store: &std::sync::Arc<cairn_store::testkit::FaultyStore>,
    node_id: &str,
    partitions: u16,
    ttl: Duration,
    tweak: impl FnOnce(&mut NodeConfig),
) -> Result<RunningNode> {
    let stores = ObjectStores::single(StoreLocation::from_store(store.clone(), "", "memory://"));
    let mut cfg = NodeConfig::new(
        stores,
        NodeMode {
            partitions,
            lease_ttl: ttl,
            node_id: Some(node_id.to_string()),
        },
    );
    cfg.tick_interval = Some(Duration::from_millis(50));
    cfg.flush_interval = Some(Duration::from_millis(5));
    cfg.node_wal.window = Duration::from_millis(5);
    tweak(&mut cfg);
    node::start(cfg)
        .await
        .map_err(|e| anyhow!("could not build node {node_id}: {e}"))
}

/// The `tweak` for tests that **pin the `direct` protocol** (renewal by PUT, silence
/// rule LS-1): a node building a new bucket turns the quiet-lease gate with delegation
/// on by default, so a test checking the `direct` behaviour itself must turn it off
/// explicitly. Generic tests run under the new default.
pub fn direct(cfg: &mut NodeConfig) {
    cfg.quiet_lease = cairn_store::quiet_lease::Bootstrap::Off;
}

/// `ids.len()` nodes on the same new bucket — [`Bucket`]: verified on cleanup.
pub async fn cluster_nodes(
    ids: &[&str],
    partitions: u16,
    ttl: Duration,
) -> Result<(Bucket, Vec<ClusterNode>)> {
    cluster_nodes_tweaked(ids, partitions, ttl, |_| {}).await
}

/// Like [`cluster_nodes`], with each node's configuration further adjusted by `tweak`
/// (e.g. [`direct`]).
pub async fn cluster_nodes_tweaked(
    ids: &[&str],
    partitions: u16,
    ttl: Duration,
    tweak: impl Fn(&mut NodeConfig),
) -> Result<(Bucket, Vec<ClusterNode>)> {
    let shared = bucket();
    let mut nodes = Vec::new();
    for id in ids {
        nodes.push(start_cluster_node_tweaked(&shared, id, partitions, ttl, &tweak).await?);
    }
    Ok((shared, nodes))
}

/// Freeze a node: every request to its store hangs forever — the shape of a `kill -9`
/// seen from the bucket. Its HTTP still listens, but the engine can no longer prove any
/// leadership once the lease expires.
pub fn freeze(store: &cairn_store::testkit::FaultyStore) {
    use cairn_store::testkit::{Action, OpSet, PathMatch, Rule};
    store.add_rule(Rule::new(OpSet::ALL, PathMatch::Any, Action::Hang));
}

/// The truth about leadership, asked straight from the engines: partition → the nodes
/// (by index) answering `status` as leader within 300ms. Exactly one node per
/// partition is the healthy state.
pub async fn leaders_by_probe(
    nodes: &[&ClusterNode],
) -> std::collections::BTreeMap<u32, Vec<usize>> {
    let mut out: std::collections::BTreeMap<u32, Vec<usize>> = Default::default();
    for (i, n) in nodes.iter().enumerate() {
        for (p, h) in n.node.partitions.iter() {
            // Ask about an id **inside that engine's range**: the engine refuses
            // (`WrongRange`) every id outside its range (D23).
            let Some(range) = n.node.cluster.range(cairn_store::PartitionId(p)) else {
                continue;
            };
            let probe =
                cairn_store::make_invocation_id(cairn_store::PartitionKey(range.start), [0x11; 16]);
            let ok = matches!(
                tokio::time::timeout(Duration::from_millis(300), h.status(probe)).await,
                Ok(Ok(_))
            );
            let e = out.entry(p).or_default();
            if ok {
                e.push(i);
            }
        }
    }
    out
}

/// Every partition has exactly one leader ⇒ partition → node index.
pub fn single_leaders(
    probe: &std::collections::BTreeMap<u32, Vec<usize>>,
) -> Option<std::collections::BTreeMap<u32, usize>> {
    probe
        .iter()
        .map(|(p, v)| (v.len() == 1).then(|| (*p, v[0])))
        .collect()
}

/// Ask `probe` until `ok` holds or `deadline` passes.
pub async fn wait_probe(
    nodes: &[&ClusterNode],
    deadline: Duration,
    what: &str,
    ok: impl Fn(&std::collections::BTreeMap<u32, Vec<usize>>) -> bool,
) -> Result<std::collections::BTreeMap<u32, Vec<usize>>> {
    let t0 = std::time::Instant::now();
    loop {
        let p = leaders_by_probe(nodes).await;
        if ok(&p) {
            return Ok(p);
        }
        if t0.elapsed() > deadline {
            return Err(anyhow!(
                "past {deadline:?} and still not reached: {what} — last poll {p:?}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The number of partitions each node leads, by node index.
pub fn load_of(map: &std::collections::BTreeMap<u32, usize>, nodes: usize) -> Vec<usize> {
    let mut v = vec![0; nodes];
    for i in map.values() {
        v[*i] += 1;
    }
    v
}

/// Whether `bucket` has any object under `prefix`.
pub async fn memory_has_prefix(
    bucket: &std::sync::Arc<slatedb::object_store::memory::InMemory>,
    prefix: &str,
) -> Result<bool> {
    use {futures::TryStreamExt, slatedb::object_store::ObjectStore};
    let p = slatedb::object_store::path::Path::from(prefix);
    let first = bucket.list(Some(&p)).try_next().await?;
    Ok(first.is_some())
}
// ───────────────────────────────────────────────────── D-d (GC of retired Dbs)

/// Wall clock, ms — the same origin as the owner record's `retired_ms`.
pub fn wall_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Build a **retired range nobody references**: the Db of range `range` (opened then
/// closed, with a manifest) plus an owner record carrying a tombstone. `retired_ms` =
/// the record's `retired_ms=` line; `None` = a format-9 style record, without that
/// line yet. Uses an id beyond the table's `next_range_id`.
pub async fn fabricate_retired_range(
    stores: &ObjectStores,
    range: u32,
    retired_ms: Option<u64>,
) -> Result<()> {
    use slatedb::object_store::{path::Path, ObjectStoreExt, PutPayload};
    let w = cairn_store::PartitionWriter::open(
        cairn_store::LeadershipGrant::single_node(cairn_store::PartitionId(range)),
        stores.open_db(range).await?,
    )
    .await?;
    w.seal().await?;
    let mut owner =
        "cairnflow-owner/1\nterm=1\nsession=dead/1\nreleased=1\nretired=1\nwal_kind=slatedb\n"
            .to_string();
    if let Some(ms) = retired_ms {
        owner.push_str(&format!("retired_ms={ms}\n"));
    }
    let prefix = stores.main().prefix().as_ref().to_string();
    let path = if prefix.is_empty() {
        format!("owners/range-{range}")
    } else {
        format!("{prefix}/owners/range-{range}")
    };
    stores
        .main()
        .store()
        .put(&Path::from(path), PutPayload::from(owner.into_bytes()))
        .await?;
    Ok(())
}

/// An age old enough for D-d to delete with the default `min_age` (30 minutes).
pub const LONG_RETIRED_MS: u64 = 31 * 60 * 1000;

// ──────────────────────────────────────────────── coordinator

/// `control/coordinator` as a test can read it: session (`<node>/<inc>`), term,
/// whether released. Read leniently like the real reader: unknown lines are skipped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoordinatorSeen {
    pub session: String,
    pub term: u64,
    pub released: bool,
}

pub async fn coordinator_of(
    bucket: &std::sync::Arc<slatedb::object_store::memory::InMemory>,
) -> Option<CoordinatorSeen> {
    use slatedb::object_store::{path::Path, ObjectStoreExt};
    let raw = bucket
        .get(&Path::from("control/coordinator"))
        .await
        .ok()?
        .bytes()
        .await
        .ok()?;
    let text = String::from_utf8(raw.to_vec()).ok()?;
    let mut lines = text.lines();
    if lines.next()? != "cairnflow-coordinator/1" {
        return None;
    }
    let (mut session, mut term, mut released) = (None, None, false);
    for kv in lines.flat_map(str::split_whitespace) {
        if let Some(v) = kv.strip_prefix("session=") {
            session = Some(v.to_string());
        } else if let Some(v) = kv.strip_prefix("term=") {
            term = v.parse().ok();
        } else if kv == "released" {
            released = true;
        }
    }
    Some(CoordinatorSeen {
        session: session?,
        term: term?,
        released,
    })
}

// ───────────────────────── delegated renewal

/// The content of the session record of `n`'s current incarnation, read straight from
/// the bucket — `None` when the node has no incarnation or the record is absent.
pub async fn session_text(
    bucket: &slatedb::object_store::memory::InMemory,
    n: &ClusterNode,
) -> Option<String> {
    use slatedb::object_store::{path::Path, ObjectStoreExt};
    let id = n.node.session.current()?;
    let raw = bucket
        .get(&Path::from(format!("sessions/{}/{}", id.node, id.inc)))
        .await
        .ok()?
        .bytes()
        .await
        .ok()?;
    Some(String::from_utf8_lossy(&raw).into_owned())
}

/// `n`'s record on the bucket is delegating (`mode=delegated`, LS-14).
pub async fn is_delegated(
    bucket: &slatedb::object_store::memory::InMemory,
    n: &ClusterNode,
) -> bool {
    session_text(bucket, n)
        .await
        .is_some_and(|r| r.lines().any(|l| l == "mode=delegated"))
}

/// `n`'s record on the bucket is an unchallenged `solo` record (LS-16; the coordinator
/// when every other node delegates to it).
pub async fn is_solo(bucket: &slatedb::object_store::memory::InMemory, n: &ClusterNode) -> bool {
    session_text(bucket, n).await.is_some_and(|r| {
        r.lines().any(|l| l == "mode=solo") && !r.lines().any(|l| l.starts_with("challenged="))
    })
}

/// `n`'s record on the bucket is `direct` (no `mode=` line).
pub async fn is_direct(bucket: &slatedb::object_store::memory::InMemory, n: &ClusterNode) -> bool {
    session_text(bucket, n)
        .await
        .is_some_and(|r| !r.lines().any(|l| l.starts_with("mode=")))
}

/// Wait until every node other than `c` has a `mode=delegated` record.
pub async fn until_delegated(
    bucket: &slatedb::object_store::memory::InMemory,
    nodes: &[ClusterNode],
    c: usize,
    within: Duration,
) -> Result<()> {
    let t0 = std::time::Instant::now();
    loop {
        let mut all = true;
        for (i, n) in nodes.iter().enumerate() {
            all &= i == c || is_delegated(bucket, n).await;
        }
        if all {
            return Ok(());
        }
        if t0.elapsed() > within {
            let mut recs = Vec::new();
            for n in nodes {
                recs.push(session_text(bucket, n).await);
            }
            return Err(anyhow!("after {within:?} not all delegate: {recs:?}"));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A clock that jumps one hour on every monotonic read: `cairn-ctl`'s wait
/// ([`cairn_server::ctl::BOOT_WAIT`] of `enable-quiet-lease`) finishes at once. Tests
/// that want to drive time themselves use `ManualClock`.
#[derive(Debug, Default)]
pub struct JumpClock {
    reads: std::sync::atomic::AtomicU32,
}

impl cairn_store::clock::Clock for JumpClock {
    fn wall(&self) -> std::time::Duration {
        cairn_store::clock::SystemClock.wall()
    }

    fn mono(&self) -> tokio::time::Instant {
        let n = self
            .reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tokio::time::Instant::now() + std::time::Duration::from_secs(3600) * n
    }
}
