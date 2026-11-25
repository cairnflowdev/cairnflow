// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Format freeze** (D27; bumped to 10 once, deliberately).
//!
//! From `DATA_FORMAT` 10 on, every bucket change needs a **migration path**. Format 9
//! was broken on purpose exactly once (the user decided "break compatibility,
//! buckets included"), so the format-9 fixture bucket and the migration tests from
//! its old data (the original WAL, rule book `/1`) were removed with that bump. The
//! promise only means something if something catches a break of it: a format 10
//! fixture bucket, committed to the repo (`tests/fixtures/format10/`), that **every**
//! later binary must open and read correctly.
//!
//! The fixture bucket has everything a real user leaves behind: deployments + catalog
//! of two tenants, API keys (hashed), finished invocations (output, journal) and one
//! invocation suspended waiting for a signal (no timer — opening the bucket on any
//! day shows the same thing), virtual object state in two tenants. Retention is off:
//! nothing is cleaned up by the clock.
//!
//! Regenerate (only when **deliberately** changing the fixture bucket — that is, along
//! with a format bump that has a migration path):
//!
//! ```sh
//! CAIRNFLOW_REGENERATE_FIXTURES=1 \
//!   cargo test -p cairn-server --test format_fixture -- --ignored generate_format_10_fixture
//! ```
//!
//! Without that environment variable the generator does nothing: running the whole
//! file with `--ignored`/`--include-ignored` must not overwrite the fixture bucket.

// Tests run on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

mod common;

use {
    anyhow::{anyhow, ensure, Result},
    cairn_server::node::{self, NodeConfig, NodeMode},
    cairn_store::{ObjectStores, StoreLocation},
    common::fake_sdk::{FakeSdk, Step},
    reqwest::{Client, StatusCode},
    serde_json::Value,
    std::path::{Path, PathBuf},
};

const ADMIN_KEY: &str = common::fixture::ADMIN_KEY;

/// The generator writes only when the runner deliberately sets `CAIRNFLOW_REGENERATE_FIXTURES=1`.
fn regenerate() -> bool {
    let on = std::env::var("CAIRNFLOW_REGENERATE_FIXTURES").as_deref() == Ok("1");
    if !on {
        eprintln!("skipped: set CAIRNFLOW_REGENERATE_FIXTURES=1 to regenerate the fixture bucket");
    }
    on
}

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/format10")
}

fn config(dir: &Path) -> Result<NodeConfig> {
    let stores = ObjectStores::single(StoreLocation::parse(&format!("file://{}", dir.display()))?);
    let mut cfg = NodeConfig::new(stores, NodeMode::single(2));
    cfg.auth = cairn_server::auth::AuthConfig {
        required: true,
        admin_key: Some(ADMIN_KEY.into()),
        cluster_secret: Some("cf_fixture_cluster_secret_at_least_32b".into()),
        metrics_key: None,
        admin_key_sha256: None,
        provision_key_sha256: None,
    };
    cfg.retention_ms = None;
    cfg.tick_interval = Some(std::time::Duration::from_millis(50));
    Ok(cfg)
}

async fn call(
    http: &Client,
    key: &str,
    req: reqwest::RequestBuilder,
) -> Result<(StatusCode, Value)> {
    let r = req.bearer_auth(key).send().await?;
    let code = r.status();
    let text = r.text().await?;
    let _ = http;
    Ok((
        code,
        serde_json::from_str(&text).unwrap_or(Value::String(text)),
    ))
}

fn copy_dir(from: &Path, to: &Path) -> Result<()> {
    std::fs::create_dir_all(to)?;
    for e in std::fs::read_dir(from)? {
        let e = e?;
        let dst = to.join(e.file_name());
        if e.file_type()?.is_dir() {
            copy_dir(&e.path(), &dst)?;
        } else {
            std::fs::copy(e.path(), dst)?;
        }
    }
    Ok(())
}

/// `format_10_fixture_still_opens`: opens a copy of the fixture bucket and reads back
/// exactly what the generator wrote — including what only tenant `acme` sees.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn format_10_fixture_still_opens() -> Result<()> {
    let src = fixture_dir();
    ensure!(
        src.join("format").exists(),
        "fixture bucket missing at {}",
        src.display()
    );
    let tmp = std::env::temp_dir().join(format!("cairnflow-format10-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    copy_dir(&src, &tmp)?;
    let expected: Value = serde_json::from_slice(&std::fs::read(src.join("EXPECTED.json"))?)?;

    let node = node::start(config(&tmp)?)
        .await
        .map_err(|e| anyhow!("this binary cannot open the format 10 bucket: {e}"))?;
    let r = common::fixture::read_single(&node.url(), &expected).await;
    node.shutdown().await;
    let _ = std::fs::remove_dir_all(&tmp);
    r
}

/// **The fixture bucket's key registry across the key-roles gate**: the fixture
/// bucket has a `cluster-version` where the `auth/keys-v2` gate is not enabled yet,
/// registry `cairnflow-keys/1`.
///
/// 1. Open a copy with this binary.
/// 2. Create a `provision` key (and a `view` one); create an `admin` key.
/// 3. Read `auth/keys` back from disk.
///
/// Must see: (2) the new roles get `409` (`key_roles_disabled`) — an old binary may
/// still be in the cluster, and the bucket must not carry a registry it cannot read;
/// the `admin` key succeeds. (3) the header is still `cairnflow-keys/1`, the two keys
/// of the fixture bucket keep their lines intact, the new key is appended;
/// `cluster-version` unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn format_10_key_book_stays_v1_until_gate() -> Result<()> {
    let src = fixture_dir();
    let tmp = std::env::temp_dir().join(format!("cairnflow-format10-keys-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    copy_dir(&src, &tmp)?;
    let keys_before = std::fs::read_to_string(src.join("auth/keys"))?;
    let cv_before = std::fs::read_to_string(src.join("cluster-version"))?;
    let node = node::start(config(&tmp)?)
        .await
        .map_err(|e| anyhow!("this binary cannot open the format 10 bucket: {e}"))?;
    let r = async {
        let http = Client::new();
        let url = node.url();
        let post = |role: &'static str| {
            http.post(format!("{url}/admin/keys"))
                .json(&serde_json::json!({ "tenant": "acme", "role": role }))
        };
        for role in ["provision", "view"] {
            let (code, v) = call(&http, ADMIN_KEY, post(role)).await?;
            ensure!(
                code == StatusCode::CONFLICT && v["code"] == "key_roles_disabled",
                "{role}: {code} {v}"
            );
        }
        let (code, v) = call(&http, ADMIN_KEY, post("admin")).await?;
        ensure!(code == StatusCode::OK, "admin: {code} {v}");
        let keys = std::fs::read_to_string(tmp.join("auth/keys"))?;
        ensure!(keys.starts_with("cairnflow-keys/1\n"), "{keys}");
        for line in keys_before.lines() {
            ensure!(keys.lines().any(|l| l == line), "missing {line:?}: {keys}");
        }
        ensure!(
            keys.lines().count() == keys_before.lines().count() + 1,
            "{keys}"
        );
        ensure!(
            std::fs::read_to_string(tmp.join("cluster-version"))? == cv_before,
            "cluster-version changed"
        );
        Ok(())
    }
    .await;
    node.shutdown().await;
    let _ = std::fs::remove_dir_all(&tmp);
    r
}

/// The fixture bucket generator — run by hand, see the top of the file.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "generates the format 10 fixture bucket — run by hand when deliberately changing it"]
async fn generate_format_10_fixture() -> Result<()> {
    if !regenerate() {
        return Ok(());
    }
    let dir = fixture_dir();
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;
    let sdk = FakeSdk::builder()
        .object(
            "Cart",
            "add",
            vec![
                Step::SetState {
                    key: "items".into(),
                    // Keep as is: must match the state value in the frozen format-10
                    // fixture bucket (the on-bucket format is frozen).
                    value: bytes::Bytes::from_static(b"[\"sach\"]"),
                },
                // Keep as is: must match data in the frozen format-10 fixture
                // (the `output` of `EXPECTED.json` and the bucket's journal).
                Step::output("da-them"),
            ],
        )
        .object("Cart", "hold", vec![Step::Suspend, Step::output("freed")])
        .spawn()
        .await?;
    let node = node::start(config(&dir)?)
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let url = node.url();
    let http = Client::new();
    let mut invocations = Vec::new();
    let r = async {
        for tenant in ["acme", "default"] {
            let (code, k) = call(
                &http,
                ADMIN_KEY,
                http.post(format!("{url}/admin/keys"))
                    .json(&serde_json::json!({ "tenant": tenant, "role": "admin" })),
            )
            .await?;
            ensure!(code == StatusCode::OK, "key {tenant}: {code} {k}");
            let key = k["secret"].as_str().unwrap_or_default().to_string();
            let (code, d) = call(
                &http,
                &key,
                http.post(format!("{url}/deployments"))
                    .json(&serde_json::json!({ "uri": sdk.url })),
            )
            .await?;
            ensure!(code == StatusCode::OK, "register {tenant}: {code} {d}");
            let (code, done) = call(
                &http,
                &key,
                http.post(format!("{url}/invoke/Cart/add?key=k1")),
            )
            .await?;
            ensure!(code == StatusCode::OK, "add {tenant}: {code} {done}");
            let id = done["id"].as_str().unwrap_or_default().to_string();
            invocations.push((id, "completed", Some(done["output"].clone())));
            let (code, held) = call(
                &http,
                &key,
                http.post(format!("{url}/invoke/Cart/hold/send?key=k2")),
            )
            .await?;
            ensure!(code.is_success(), "hold {tenant}: {code} {held}");
            let id = held["id"].as_str().unwrap_or_default().to_string();
            for _ in 0..200 {
                let (_, st) = call(
                    &http,
                    ADMIN_KEY,
                    http.get(format!("{url}/invocations/{id}")),
                )
                .await?;
                if st["status"] == "suspended" {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            invocations.push((id, "suspended", None));
        }
        let mut out = Vec::new();
        for (id, status, output) in &invocations {
            let (_, j) = call(
                &http,
                ADMIN_KEY,
                http.get(format!("{url}/invocations/{id}/journal")),
            )
            .await?;
            let mut v = serde_json::json!({
                "id": id,
                "status": status,
                "journal": j.as_array().map(Vec::len).unwrap_or(0),
            });
            if let Some(o) = output {
                v["output"] = o.clone();
            }
            out.push(v);
        }
        Ok(serde_json::json!({ "dataFormat": cairn_store::DATA_FORMAT, "invocations": out }))
    }
    .await;
    node.shutdown().await;
    let expected = r?;
    std::fs::write(
        dir.join("EXPECTED.json"),
        serde_json::to_vec_pretty(&expected)?,
    )?;
    Ok(())
}

// ───────────────────────────────────────────── cluster fixture bucket
//
// The single-process fixture above has no `sessions/`, `owners/`, `nodewal/`,
// `rules`, `tenants`, `range-windows`, `archive/` or `metering/`. The **cluster**
// fixture bucket `tests/fixtures/format10-cluster/` has them all: two nodes with
// `WAL_KIND = shared`, a rule book, tenant settings (hash chain), one range's flush
// window, one finished split (the parent range retired, tombstone + Db intact since
// Db GC is off), one invocation archived into `archive/`, and `metering/` counts.
//
// The cluster bucket needs CAS on etags, so the generator runs on `InMemory` and
// then dumps each object to a file at the same path; the test loads the directory
// back into `InMemory`.
//
// **This bucket has the shape "no migration step has run yet"**: no
// `cluster-version` object, no `knows=` line in the session records, no `data=` in
// the owner records. The generator runs the current binary and then **strips** those
// lines/objects before dumping (see [`strip_migration_marks`]): opening the fixture
// bucket replays the whole migration registry on it (every step is idempotent, even
// on rows already in the new layout), and `format_10_cluster_fixture_migrates` can
// use a test registry of its own.
//
// ```sh
// CAIRNFLOW_REGENERATE_FIXTURES=1 \
//   cargo test -p cairn-server --test format_fixture -- --ignored generate_format_10_cluster_fixture
// ```

fn cluster_fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/format10-cluster")
}

const CLUSTER_NODES: [&str; 2] = ["node-alpha", "node-beta"];

async fn start_cluster_fixture_node(
    bucket: &std::sync::Arc<slatedb::object_store::memory::InMemory>,
    id: &str,
    migrations: &'static [cairn_store::migrate::Migration],
) -> Result<common::ClusterNode> {
    common::start_cluster_node_tweaked(bucket, id, 2, std::time::Duration::from_secs(2), |cfg| {
        cfg.auth = cairn_server::auth::AuthConfig {
            required: true,
            admin_key: Some(ADMIN_KEY.into()),
            // Not set: the cluster secret is generated and lives in the bucket
            // (`auth/cluster-secret`) — one more object in the fixture bucket.
            cluster_secret: None,
            metrics_key: None,
            admin_key_sha256: None,
            provision_key_sha256: None,
        };
        cfg.retention_ms = None;
        cfg.archive = true;
        cfg.metering_flush = std::time::Duration::from_millis(100);
        // Keys and deployments created on this node must reach the other one quickly.
        cfg.deploy_refresh = std::time::Duration::from_millis(200);
        cfg.migrations = migrations;
    })
    .await
}

/// Calls each node in turn until one answers something other than `503` (not the
/// leader / not ready yet); when `deadline` runs out returns the last answer.
async fn call_any(
    http: &Client,
    key: &str,
    urls: &[String],
    req: impl Fn(&str) -> reqwest::RequestBuilder,
) -> Result<(StatusCode, Value)> {
    let t0 = std::time::Instant::now();
    loop {
        let mut last = None;
        for u in urls {
            let (code, v) = call(http, key, req(u)).await?;
            if code != StatusCode::SERVICE_UNAVAILABLE && code != StatusCode::CONFLICT {
                return Ok((code, v));
            }
            last = Some((code, v));
        }
        if t0.elapsed() > std::time::Duration::from_secs(20) {
            return Ok(last.expect("at least one node"));
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// A `Cart` object key of tenant `tenant` that falls into range `range`.
fn cart_key_on(
    node: &cairn_server::node::RunningNode,
    tenant: &str,
    range: u32,
    prefix: &str,
) -> String {
    let t = cairn_store::TenantId::new(tenant).expect("valid tenant");
    (0..10_000)
        .map(|i| format!("{prefix}-{i}"))
        .find(|k| node.cluster.partition_of(&t, "Cart", k).0 == range)
        .unwrap_or_else(|| panic!("no Cart key of {tenant} falls into range {range}"))
}

/// Strips the migration framework's marks from `objects` (see the section head): the `cluster-version` object, the `knows=`
/// line of the session records, the `data=` line of the owner records. Other lines stay verbatim and in order.
fn strip_migration_marks(objects: &mut Vec<(String, Vec<u8>)>) {
    objects.retain(|(p, _)| p != "cluster-version");
    for (p, raw) in objects.iter_mut() {
        let drop_key = if p.starts_with("sessions/") {
            "knows="
        } else if p.starts_with("owners/") {
            "data="
        } else {
            continue;
        };
        let Ok(text) = std::str::from_utf8(raw) else {
            continue;
        };
        let kept: String = text
            .lines()
            .filter(|l| !l.starts_with(drop_key))
            .map(|l| format!("{l}\n"))
            .collect();
        *raw = kept.into_bytes();
    }
}

/// Every object of `bucket`: (path, content), ordered by path.
async fn dump_bucket(
    bucket: &std::sync::Arc<slatedb::object_store::memory::InMemory>,
) -> Result<Vec<(String, Vec<u8>)>> {
    use {
        futures::TryStreamExt,
        slatedb::object_store::{ObjectStore, ObjectStoreExt},
    };
    let listed: Vec<_> = bucket.list(None).try_collect().await?;
    let mut out = Vec::new();
    for m in listed {
        let raw = bucket.get(&m.location).await?.bytes().await?;
        out.push((m.location.to_string(), raw.to_vec()));
    }
    out.sort();
    Ok(out)
}

/// `format_10_cluster_fixture_still_opens`: loads the cluster fixture bucket, rebuilds
/// two nodes, and reads back exactly everything the generator wrote — invocations
/// (including the one moved to a child range by the split), rule book, tenants, range
/// window, archive, metering; the parent range is still a tombstone and its Db intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn format_10_cluster_fixture_still_opens() -> Result<()> {
    open_cluster_fixture(cairn_store::migrate::MIGRATIONS, |_, _| async { Ok(()) }).await
}

static FIXTURE_RANGE_RUNS: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

fn fixture_c1(
    _: u32,
    ctx: cairn_store::migrate::ClusterCtx,
) -> futures::future::BoxFuture<'static, cairn_store::Result<()>> {
    use slatedb::object_store::{path::Path as ObjPath, ObjectStoreExt, PutPayload};
    Box::pin(async move {
        ctx.store
            .put(&ObjPath::from("seen/fixture-c1"), PutPayload::from("1"))
            .await
            .map_err(|e| cairn_store::StoreError::Io(e.to_string()))?;
        Ok(())
    })
}

fn fixture_r2(
    _: u32,
    w: &mut cairn_store::PartitionWriter,
) -> futures::future::LocalBoxFuture<'_, cairn_store::Result<()>> {
    Box::pin(async move {
        let _ = w.get(b"\xfftest-r2").await?;
        FIXTURE_RANGE_RUNS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    })
}

const FIXTURE_LEDGER: &[cairn_store::migrate::Migration] = &[
    cairn_store::migrate::Migration {
        to: 1,
        name: "fixture-c1",
        run: cairn_store::migrate::Run::Cluster(fixture_c1),
    },
    cairn_store::migrate::Migration {
        to: 2,
        name: "fixture-r2",
        run: cairn_store::migrate::Run::Range(fixture_r2),
    },
];

/// **Migration from the cluster fixture bucket**: two nodes carrying a registry with
/// one cluster-level step and one range-level step open the format 10 cluster bucket
/// and migrate it — `cluster-version` goes to 2, each **live** range runs the
/// range-level step exactly once and carries `data=2`, a retired range is not opened —
/// then read back everything as in `format_10_cluster_fixture_still_opens`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn format_10_cluster_fixture_migrates() -> Result<()> {
    open_cluster_fixture(FIXTURE_LEDGER, |bucket, expected| async move {
        use slatedb::object_store::{path::Path as ObjPath, ObjectStoreExt};
        let read = |p: &'static str| {
            let bucket = bucket.clone();
            async move {
                Ok::<_, anyhow::Error>(String::from_utf8(
                    bucket.get(&ObjPath::from(p)).await?.bytes().await?.to_vec(),
                )?)
            }
        };
        ensure!(
            read("seen/fixture-c1").await? == "1",
            "cluster-level step did not run"
        );
        let cv = read("cluster-version").await?;
        ensure!(
            cv.lines().any(|l| l == "version=2") && !cv.contains("upgrading="),
            "{cv}"
        );
        let live: Vec<u64> = expected["ranges"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_u64)
            .collect();
        let t0 = std::time::Instant::now();
        loop {
            let mut datas = Vec::new();
            for r in &live {
                let text = String::from_utf8(
                    bucket
                        .get(&ObjPath::from(format!("owners/range-{r}")))
                        .await?
                        .bytes()
                        .await?
                        .to_vec(),
                )?;
                datas.push(
                    text.lines()
                        .find_map(|l| l.strip_prefix("data="))
                        .map(str::to_string),
                );
            }
            if datas.iter().all(|d| d.as_deref() == Some("2")) {
                break;
            }
            ensure!(
                t0.elapsed() < std::time::Duration::from_secs(20),
                "live range not at data=2 yet: {datas:?}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let runs = FIXTURE_RANGE_RUNS.load(std::sync::atomic::Ordering::SeqCst);
        ensure!(
            runs as usize == live.len(),
            "range-level step ran {runs} times for {} live ranges",
            live.len()
        );
        for r in expected["retired"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_u64)
        {
            let text = String::from_utf8(
                bucket
                    .get(&ObjPath::from(format!("owners/range-{r}")))
                    .await?
                    .bytes()
                    .await?
                    .to_vec(),
            )?;
            ensure!(
                !text.contains("data="),
                "retired range {r} was opened for migration:\n{text}"
            );
        }
        Ok(())
    })
    .await
}

/// Loads the cluster fixture bucket, builds two nodes carrying the `migrations`
/// registry, runs `extra` (the test's own checks), then checks everything the generator wrote.
async fn open_cluster_fixture<F, Fut>(
    migrations: &'static [cairn_store::migrate::Migration],
    extra: F,
) -> Result<()>
where
    F: FnOnce(std::sync::Arc<slatedb::object_store::memory::InMemory>, Value) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let src = cluster_fixture_dir();
    ensure!(
        src.join("format").exists(),
        "cluster fixture bucket missing at {}",
        src.display()
    );
    let expected: Value = serde_json::from_slice(&std::fs::read(src.join("EXPECTED.json"))?)?;
    let bucket = common::fixture::load(&src).await?;
    let mut nodes = Vec::new();
    for id in CLUSTER_NODES {
        nodes.push(
            start_cluster_fixture_node(&bucket, id, migrations)
                .await
                .map_err(|e| anyhow!("cannot open the format 10 cluster bucket: {e}"))?,
        );
    }
    let urls: Vec<String> = nodes.iter().map(|n| n.node.url()).collect();
    let http = Client::new();
    let r = common::within(
        std::time::Duration::from_secs(90),
        "read back the cluster fixture bucket",
        async {
            verify_cluster_fixture(&bucket, &nodes, &urls, &http, &expected).await?;
            extra(bucket.clone(), expected.clone()).await
        },
    )
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

async fn verify_cluster_fixture(
    bucket: &std::sync::Arc<slatedb::object_store::memory::InMemory>,
    nodes: &[common::ClusterNode],
    urls: &[String],
    http: &Client,
    expected: &Value,
) -> Result<()> {
    // ── Range table: exactly the ranges after the split.
    let ranges: Vec<u64> = nodes[0]
        .node
        .cluster
        .partitions()
        .iter()
        .map(|p| p.0 as u64)
        .collect();
    let want: Vec<u64> = expected["ranges"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_u64)
        .collect();
    ensure!(ranges == want, "range table {ranges:?} ≠ {want:?}");
    // ── Parent range: tombstone, and its Db still on the bucket.
    let store: std::sync::Arc<dyn slatedb::object_store::ObjectStore> = bucket.clone();
    for r in expected["retired"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_u64)
    {
        let rec = cairn_store::read_owner(
            &store,
            &slatedb::object_store::path::Path::from(""),
            cairn_store::PartitionId(r as u32),
        )
        .await?
        .ok_or_else(|| anyhow!("range {r}: owner record lost"))?;
        ensure!(rec.retired, "range {r} is no longer a tombstone: {rec:?}");
        ensure!(
            common::memory_has_prefix(bucket, &format!("ranges/{r}/manifest")).await?,
            "Db of retired range {r} disappeared"
        );
    }
    // ── Invocation.
    for inv in expected["invocations"].as_array().into_iter().flatten() {
        let id = inv["id"].as_str().unwrap_or_default();
        let (code, st) = call_any(http, ADMIN_KEY, urls, |u| {
            http.get(format!("{u}/invocations/{id}"))
        })
        .await?;
        ensure!(code == StatusCode::OK, "invocation {id}: {code} {st}");
        common::fixture::times_present(id, &st)?;
        ensure!(
            st["status"] == inv["status"],
            "invocation {id}: {st} ≠ {inv}"
        );
        if let Some(out) = inv.get("output") {
            ensure!(&st["output"] == out, "output of {id}: {st} ≠ {inv}");
        }
        let (code, j) = call_any(http, ADMIN_KEY, urls, |u| {
            http.get(format!("{u}/invocations/{id}/journal"))
        })
        .await?;
        ensure!(code == StatusCode::OK, "journal {id}: {code} {j}");
        ensure!(
            j.as_array().map(Vec::len) == inv["journal"].as_u64().map(|n| n as usize),
            "journal of {id} has another length: {j}"
        );
    }
    // ── Archive.
    let a = &expected["archived"];
    let (id, tenant, date) = (
        a["id"].as_str().unwrap_or_default(),
        a["tenant"].as_str().unwrap_or_default(),
        a["date"].as_str().unwrap_or_default(),
    );
    let (code, got) = call(
        http,
        ADMIN_KEY,
        http.get(format!(
            "{}/archive/{id}?tenant={tenant}&date={date}",
            urls[0]
        )),
    )
    .await?;
    ensure!(code == StatusCode::OK, "archive {id}: {code} {got}");
    ensure!(got["output"] == a["output"], "archive {id}: {got} ≠ {a}");
    // ── Metering.
    let m = &expected["metering"];
    let (code, got) = call(
        http,
        ADMIN_KEY,
        http.get(format!(
            "{}/metering?tenant={}&hour={}",
            urls[0],
            m["tenant"].as_str().unwrap_or_default(),
            m["hour"].as_str().unwrap_or_default()
        )),
    )
    .await?;
    ensure!(code == StatusCode::OK, "metering: {code} {got}");
    ensure!(
        got["steps"].as_u64() >= m["steps"].as_u64() && got["steps"].as_u64() > Some(0),
        "metering lost counts: {got} ≠ {m}"
    );
    // ── Rule book, tenants, range window.
    let (code, rules) = call(http, ADMIN_KEY, http.get(format!("{}/rules", urls[0]))).await?;
    ensure!(code == StatusCode::OK, "rules: {code} {rules}");
    ensure!(
        rules.to_string().contains("\"acme\""),
        "acme's rule lost: {rules}"
    );
    let (code, tenants) = call(
        http,
        ADMIN_KEY,
        http.get(format!("{}/admin/tenants", urls[0])),
    )
    .await?;
    ensure!(code == StatusCode::OK, "tenants: {code} {tenants}");
    ensure!(
        tenants.to_string().contains("acme"),
        "tenant settings of acme lost: {tenants}"
    );
    let (code, windows) = call(
        http,
        ADMIN_KEY,
        http.get(format!("{}/ranges/windows", urls[0])),
    )
    .await?;
    ensure!(code == StatusCode::OK, "windows: {code} {windows}");
    ensure!(
        windows == expected["windows"],
        "range window {windows} ≠ {}",
        expected["windows"]
    );
    Ok(())
}

/// The cluster fixture bucket generator — run by hand, see the section head.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "generates the format 10 cluster fixture bucket — run by hand when deliberately changing it"]
async fn generate_format_10_cluster_fixture() -> Result<()> {
    if !regenerate() {
        return Ok(());
    }
    let dir = cluster_fixture_dir();
    let bucket = std::sync::Arc::new(slatedb::object_store::memory::InMemory::new());
    let sdk = FakeSdk::builder()
        .object(
            "Cart",
            "add",
            vec![
                Step::SetState {
                    key: "items".into(),
                    // Keep as is: must match the state value in the frozen format-10
                    // fixture bucket (the on-bucket format is frozen).
                    value: bytes::Bytes::from_static(b"[\"sach\"]"),
                },
                // Keep as is: must match data in the frozen format-10 fixture
                // (the `output` of `EXPECTED.json` and the bucket's journal).
                Step::output("da-them"),
            ],
        )
        .object("Cart", "hold", vec![Step::Suspend, Step::output("freed")])
        .spawn()
        .await?;
    let mut nodes = Vec::new();
    for id in CLUSTER_NODES {
        nodes
            .push(start_cluster_fixture_node(&bucket, id, cairn_store::migrate::MIGRATIONS).await?);
    }
    let urls: Vec<String> = nodes.iter().map(|n| n.node.url()).collect();
    let http = Client::new();
    let r = common::within(std::time::Duration::from_secs(120), "generate cluster fixture", async {
        let refs: Vec<_> = nodes.iter().collect();
        // Both nodes lead: one range per node, so both have a shared WAL log and the
        // owner records point at both.
        common::wait_probe(&refs, std::time::Duration::from_secs(30), "one range per node", |p| {
            common::single_leaders(p).is_some_and(|m| common::load_of(&m, 2) == [1, 1])
        })
        .await?;
        // ── Tenants (hash chain for acme) and rule book.
        let (code, t) = call(
            &http,
            ADMIN_KEY,
            http.put(format!("{}/admin/tenants/acme", urls[0])).json(&serde_json::json!({ "hashChain": true })),
        )
        .await?;
        ensure!(code.is_success(), "tenant acme: {code} {t}");
        let (code, rule) = call(
            &http,
            ADMIN_KEY,
            http.post(format!("{}/rules", urls[0]))
                .json(&serde_json::json!({ "pattern": "acme", "concurrency": 50, "description": "fixture" })),
        )
        .await?;
        ensure!(code == StatusCode::OK, "rule: {code} {rule}");
        // ── Each tenant: key, deployment, finished + suspended invocations on both ranges.
        let mut invocations = Vec::new();
        let mut keys = std::collections::BTreeMap::new();
        for tenant in ["acme", "default"] {
            let (code, k) = call(
                &http,
                ADMIN_KEY,
                http.post(format!("{}/admin/keys", urls[0]))
                    .json(&serde_json::json!({ "tenant": tenant, "role": "admin" })),
            )
            .await?;
            ensure!(code == StatusCode::OK, "key {tenant}: {code} {k}");
            let key = k["secret"].as_str().unwrap_or_default().to_string();
            let (code, d) = call(
                &http,
                &key,
                http.post(format!("{}/deployments", urls[0])).json(&serde_json::json!({ "uri": sdk.url })),
            )
            .await?;
            ensure!(code == StatusCode::OK, "register {tenant}: {code} {d}");
            keys.insert(tenant, key);
        }
        // The other node sees the deployment after one registry refresh tick.
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        for tenant in ["acme", "default"] {
            let key = keys[tenant].clone();
            for range in [0u32, 1] {
                let k = cart_key_on(&nodes[0].node, tenant, range, "add");
                let (code, done) =
                    call_any(&http, &key, &urls, |u| http.post(format!("{u}/invoke/Cart/add?key={k}"))).await?;
                ensure!(code == StatusCode::OK, "add {tenant}/{range}: {code} {done}");
                let id = done["id"].as_str().unwrap_or_default().to_string();
                invocations.push((id, "completed", Some(done["output"].clone())));
            }
            let k = cart_key_on(&nodes[0].node, tenant, 0, "hold");
            let (code, held) =
                call_any(&http, &key, &urls, |u| http.post(format!("{u}/invoke/Cart/hold/send?key={k}"))).await?;
            ensure!(code.is_success(), "hold {tenant}: {code} {held}");
            let id = held["id"].as_str().unwrap_or_default().to_string();
            let mut suspended = false;
            for _ in 0..200 {
                let (_, st) = call_any(&http, ADMIN_KEY, &urls, |u| http.get(format!("{u}/invocations/{id}"))).await?;
                if st["status"] == "suspended" {
                    suspended = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            ensure!(suspended, "{id} not suspended");
            invocations.push((id, "suspended", None));
        }
        // ── Archive: purge by hand one finished invocation of acme.
        let k = cart_key_on(&nodes[0].node, "acme", 1, "purged");
        let (code, done) =
            call_any(&http, &keys["acme"], &urls, |u| http.post(format!("{u}/invoke/Cart/add?key={k}"))).await?;
        ensure!(code == StatusCode::OK, "add to purge: {code} {done}");
        let purged = done["id"].as_str().unwrap_or_default().to_string();
        let (code, p) =
            call_any(&http, ADMIN_KEY, &urls, |u| http.post(format!("{u}/invocations/{purged}/purge"))).await?;
        ensure!(code == StatusCode::OK, "purge: {code} {p}");
        let archived = dump_bucket(&bucket)
            .await?
            .into_iter()
            .map(|(p, _)| p)
            .find(|p| p.starts_with("archive/") && p.ends_with(&purged))
            .ok_or_else(|| anyhow!("no archive object for {purged}"))?;
        let date = archived.split('/').nth(2).unwrap_or_default().to_string();
        // ── Flush window of range 1 (the range that is not split).
        let (code, w) = call(
            &http,
            ADMIN_KEY,
            http.put(format!("{}/ranges/1/window", urls[0])).json(&serde_json::json!({ "ms": 300 })),
        )
        .await?;
        ensure!(code.is_success(), "range 1 window: {code} {w}");
        // ── Split range 0 on its leader: the parent range retires, two new children.
        let (code, view) = call_any(&http, ADMIN_KEY, &urls, |u| http.post(format!("{u}/ranges/0/split"))).await?;
        ensure!(code == StatusCode::OK, "split: {code} {view}");
        let refs: Vec<_> = nodes.iter().collect();
        common::wait_probe(&refs, std::time::Duration::from_secs(20), "one leader per child", |p| {
            common::single_leaders(p).is_some_and(|m| m.len() == 3)
        })
        .await?;
        // ── Metering: wait a few flush ticks.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let hour = {
            let (code, m) = call(&http, ADMIN_KEY, http.get(format!("{}/metering?tenant=acme", urls[0]))).await?;
            ensure!(code == StatusCode::OK && m["steps"].as_u64() > Some(0), "metering: {code} {m}");
            m
        };
        let (_, windows) = call(&http, ADMIN_KEY, http.get(format!("{}/ranges/windows", urls[0]))).await?;
        let mut out = Vec::new();
        for (id, status, output) in &invocations {
            let (_, j) = call_any(&http, ADMIN_KEY, &urls, |u| http.get(format!("{u}/invocations/{id}/journal"))).await?;
            let mut v = serde_json::json!({
                "id": id,
                "status": status,
                "journal": j.as_array().map(Vec::len).unwrap_or(0),
            });
            if let Some(o) = output {
                v["output"] = o.clone();
            }
            out.push(v);
        }
        let ranges: Vec<u32> = nodes[0].node.cluster.partitions().iter().map(|p| p.0).collect();
        Ok(serde_json::json!({
            "dataFormat": cairn_store::DATA_FORMAT,
            "invocations": out,
            "archived": { "id": purged, "tenant": "acme", "date": date, "output": done["output"] },
            "metering": { "tenant": "acme", "hour": hour["hour"], "steps": hour["steps"] },
            "windows": windows,
            "ranges": ranges,
            "retired": [0],
        }))
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    let expected = r?;
    let mut objects = dump_bucket(&bucket).await?;
    strip_migration_marks(&mut objects);
    let _ = std::fs::remove_dir_all(&dir);
    for (p, raw) in &objects {
        let path = dir.join(p);
        std::fs::create_dir_all(path.parent().expect("object has a parent directory"))?;
        std::fs::write(path, raw)?;
    }
    std::fs::write(
        dir.join("EXPECTED.json"),
        serde_json::to_vec_pretty(&expected)?,
    )?;
    Ok(())
}

/// The value of the `wal_name` field (vtable slot 40) and of `closed` (slot 42) of the
/// `ManifestV2` in a manifest object, decoded **by hand** by position — not through the
/// fork's generated code. The object is 2 bytes of format version (big-endian; 2 =
/// `ManifestV2`) then a flatbuffer.
fn manifest_slots(bytes: &[u8]) -> Result<(Option<String>, bool)> {
    const VT_WAL_NAME: usize = 40;
    const VT_CLOSED: usize = 42;
    ensure!(bytes.len() > 2, "empty manifest");
    let format = u16::from_be_bytes([bytes[0], bytes[1]]);
    ensure!(
        format == 2,
        "manifest format {format}, the test only decodes ManifestV2"
    );
    let buf = &bytes[2..];
    let slice = |at: usize, n: usize| {
        buf.get(at..at + n)
            .ok_or_else(|| anyhow!("read outside the flatbuffer"))
    };
    let u16_at =
        |at: usize| -> Result<usize> { Ok(u16::from_le_bytes(slice(at, 2)?.try_into()?) as usize) };
    let u32_at =
        |at: usize| -> Result<usize> { Ok(u32::from_le_bytes(slice(at, 4)?.try_into()?) as usize) };
    let table = u32_at(0)?;
    let back = i32::from_le_bytes(slice(table, 4)?.try_into()?);
    let vtable = usize::try_from(table as i64 - back as i64)?;
    let vt_len = u16_at(vtable)?;
    let field = |slot: usize| -> Result<Option<usize>> {
        if slot + 2 > vt_len {
            return Ok(None);
        }
        let off = u16_at(vtable + slot)?;
        Ok((off != 0).then_some(table + off))
    };
    let wal_name = match field(VT_WAL_NAME)? {
        None => None,
        Some(at) => {
            let s = at + u32_at(at)?;
            let n = u32_at(s)?;
            Some(String::from_utf8(slice(s + 4, n)?.to_vec())?)
        }
    };
    let closed = match field(VT_CLOSED)? {
        None => false,
        Some(at) => slice(at, 1)?[0] != 0,
    };
    Ok((wal_name, closed))
}

/// **The two fields the fork adds to the manifest keep their positions.** The fork
/// appends `wal_name` (id 18, slot 40) and `closed` (id 19, slot 42) at the end of
/// `ManifestV2`; upstream has not reserved those two positions. A rebase onto an
/// upstream that has inserted other fields there would shift the fork's fields —
/// every manifest on every bucket would then decode wrongly.
///
/// 1. Every manifest of the format 10 cluster fixture bucket (committed bytes, written
///    by an old binary): slot 40 decoded by hand must be the shared WAL name and equal
///    exactly what the fork's `Admin::read_manifest` reads.
/// 2. Close the Db of range 0 on the copy (`close_for_writes`), then decode by hand the
///    newest manifest: slot 42 must be `true`, matching `closed_for_writes()`, slot 40
///    unchanged.
///
/// Must see: the hand decoding and the fork agree in both. Red ⇒ a field position has
/// shifted — stop the rebase: the fork's fields must move with a manifest migration
/// before the rebase can land.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manifest_fields_keep_their_slots() -> Result<()> {
    use slatedb::{
        admin::Admin,
        object_store::{local::LocalFileSystem, path::Path as ObjPath, ObjectStore},
    };
    use std::sync::Arc;

    let src = cluster_fixture_dir();
    let tmp = std::env::temp_dir().join(format!("cairnflow-slots-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    copy_dir(&src, &tmp)?;
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&tmp)?);

    let ranges_dir = tmp.join("ranges");
    let mut seen = 0;
    let mut ranges = Vec::new();
    for range in std::fs::read_dir(&ranges_dir)? {
        let range = range?.file_name().to_string_lossy().into_owned();
        let admin = Admin::builder(ObjPath::from(format!("ranges/{range}")), store.clone()).build();
        let mdir = ranges_dir.join(&range).join("manifest");
        if !mdir.exists() {
            continue;
        }
        ranges.push(range.clone());
        for m in std::fs::read_dir(&mdir)? {
            let m = m?;
            let name = m.file_name().to_string_lossy().into_owned();
            let Some(id) = name.strip_suffix(".manifest") else {
                continue;
            };
            let id: u64 = id.parse()?;
            let (wal_name, closed) = manifest_slots(&std::fs::read(m.path())?)?;
            let fork = admin
                .read_manifest(Some(id))
                .await?
                .ok_or_else(|| anyhow!("fork cannot read manifest {range}/{id}"))?;
            ensure!(
                wal_name.as_deref() == Some(cairn_store::SHARED_WAL_NAME),
                "ranges/{range} manifest {id}: slot 40 decodes by hand to {wal_name:?}, the fixture wrote the shared WAL"
            );
            ensure!(
                fork.wal_name() == wal_name.as_deref() && fork.closed_for_writes() == closed,
                "ranges/{range} manifest {id}: fork reads ({:?}, {}), hand decoding gives ({wal_name:?}, {closed}) — field position shifted",
                fork.wal_name(),
                fork.closed_for_writes()
            );
            seen += 1;
        }
    }
    ensure!(seen > 0, "fixture bucket has no manifest");

    let range = ranges.first().ok_or_else(|| anyhow!("no range"))?;
    let admin = Admin::builder(ObjPath::from(format!("ranges/{range}")), store.clone()).build();
    admin.close_for_writes().await?;
    let latest = admin
        .read_manifest(None)
        .await?
        .ok_or_else(|| anyhow!("manifest lost after closing"))?;
    ensure!(
        latest.closed_for_writes(),
        "fork does not see the closed flag just written"
    );
    let newest = std::fs::read_dir(ranges_dir.join(range).join("manifest"))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "manifest"))
        .max()
        .ok_or_else(|| anyhow!("no manifest"))?;
    let (wal_name, closed) = manifest_slots(&std::fs::read(&newest)?)?;
    ensure!(
        closed,
        "slot 42 of the just-closed manifest decodes by hand to false — `closed` moved"
    );
    ensure!(
        wal_name.as_deref() == Some(cairn_store::SHARED_WAL_NAME),
        "slot 40 of the just-closed manifest decodes by hand to {wal_name:?}"
    );
    let _ = std::fs::remove_dir_all(&tmp);
    Ok(())
}
