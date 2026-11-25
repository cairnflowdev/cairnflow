// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Unit cost per feature: identity signing, backup / restore, SSO and console
//! session cookie checks.
//!
//! Every test here is a **measurement**, asserting no threshold: `#[ignore]`, numbers
//! printed to stderr, run by hand:
//!
//! ```sh
//! cargo test -p cairn-server --test cost_units <name> -- --ignored --nocapture
//! ```
//!
//! - `identity_signing_cost` — µs per identity JWT signature (the normal path, key
//!   already derived) and the first one of a `(tenant, generation)` (HKDF + building
//!   the Ed25519 key). `N` = `CAIRN_COST_N` (default 10 000).
//! - `backup_restore_cost` — source/target store requests of `Ctl::backup` and
//!   `Ctl::restore` by bucket size. Sizes (MB, comma-separated) =
//!   `CAIRN_COST_BACKUP_MB` (default `10,100`).
//! - `sso_and_cookie_cost` — store requests per full OIDC login, per login with a
//!   key, per cookie check. The idle background is measured over a separate window
//!   and subtracted by time.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{anyhow, ensure, Context, Result},
    cairn_server::{
        auth::AuthConfig,
        ctl::Ctl,
        node::{NodeConfig, NodeMode, RunningNode},
        ui::UiConfig,
    },
    cairn_store::{
        testkit::{FaultyStore, Op, PathClass, PutKind, StoreCounts},
        ObjectStores, StoreLocation,
    },
    common::fake_sdk::{FakeSdk, Step},
    futures::TryStreamExt,
    reqwest::{header, StatusCode},
    serde_json::{json, Value},
    slatedb::object_store::{memory::InMemory, ObjectStore},
    std::{
        collections::BTreeMap,
        hint::black_box,
        sync::Arc,
        time::{Duration, Instant},
    },
};

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn profile() -> &'static str {
    if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    }
}

/// Print a counter snapshot: every non-zero operation, split by path kind.
fn print_counts(label: &str, c: &StoreCounts, per: f64) {
    eprintln!("  {label}: {} requests in total", c.total());
    for op in Op::ALL {
        let n = c.op(op);
        if n == 0 {
            continue;
        }
        let by_class: Vec<String> = PathClass::ALL
            .iter()
            .filter(|&&cl| c.get(op, cl) > 0)
            .map(|&cl| format!("{cl:?}={}", c.get(op, cl)))
            .collect();
        eprintln!(
            "    {op:?}: {n} ({:.3}/unit) [{}]",
            n as f64 / per,
            by_class.join(", ")
        );
    }
    let pages: u64 = PathClass::ALL.iter().map(|&cl| c.list_pages(cl)).sum();
    let get_b: u64 = PathClass::ALL.iter().map(|&cl| c.get_bytes(cl)).sum();
    let put_b: u64 = PathClass::ALL.iter().map(|&cl| c.put_bytes(cl)).sum();
    let kinds: Vec<String> = PutKind::ALL
        .iter()
        .map(|&k| {
            let n: u64 = PathClass::ALL.iter().map(|&cl| c.put_kind(k, cl)).sum();
            format!("{k:?}={n}")
        })
        .collect();
    eprintln!(
        "    LIST pages {pages}; GET bytes {get_b}; PUT bytes {put_b}; PUT by mode [{}]",
        kinds.join(", ")
    );
}

// ───────────────────────────────────────────────────── 1. identity signing

/// µs per identity signature on the path of `invoker::Signing::apply`.
///
/// `Signing::apply` is `pub(crate)`; the nearest public part is
/// [`Identity::headers`] — exactly what `apply` calls, plus `Url::parse` and two
/// `RequestBuilder::header`s. Measures four paths, each `N` times on one thread
/// (wall time ≈ CPU):
///
/// 1. `headers` for one tenant, key cached (the normal path of every invocation);
/// 2. like `apply`: `invocation_jti` + `Url::parse` + `headers` + attaching the
///    headers to the `RequestBuilder`, minus the baseline of an unsigned
///    `client.post(url)`;
/// 3. `headers` with a new tenant every time (cache miss: HKDF-SHA256 + Ed25519 key
///    from the seed + signing) — the first one of a `(tenant, generation)`;
/// 4. `identity::verify` (the receiving side; the TS SDK does it itself with
///    WebCrypto — this number is only for comparison).
#[test]
#[ignore = "a cost measurement, run by hand"]
fn identity_signing_cost() -> Result<()> {
    use cairn_server::identity::{self, Identity, Master};
    let n = env_usize("CAIRN_COST_N", 10_000);
    let id = Identity::new(Some(Master::new(&[42u8; 32]).map_err(|e| anyhow!(e))?));
    let now_ms = 1_800_000_000_000u64;
    let url = "http://127.0.0.1:9080/invoke/Greeter/greet";
    let aud = "/invoke/Greeter/greet";
    let ids: Vec<_> = (0..n)
        .map(|i| {
            cairn_store::make_invocation_id(cairn_store::PartitionKey(i as u64), [i as u8; 16])
        })
        .collect();
    let jtis: Vec<String> = ids.iter().map(|x| identity::invocation_jti(x, 0)).collect();
    // Every JWT hashes the body (`bsh`) and carries the host: measure with a typical
    // 4 KiB RR body.
    let body = vec![7u8; 4096];
    let bound = identity::Bound {
        host: Some("127.0.0.1:9080"),
        body: Some(&body),
        nonce: Some([9; identity::NONCE_LEN]),
    };
    // Warm up: derive `acme`'s key.
    let token =
        identity::Identity::sign(&id, "acme", aud, &jtis[0], now_ms, bound).context("unsigned")?;

    // 1.
    let t = Instant::now();
    let mut len = 0usize;
    for jti in &jtis {
        let h = id.headers("acme", aud, jti, now_ms, bound);
        len += black_box(h)[1].1.len();
    }
    let cached = t.elapsed();

    // 2.
    let client = reqwest::Client::new();
    let t = Instant::now();
    for x in &ids {
        let _ = black_box(client.post(url));
        black_box(x);
    }
    let base = t.elapsed();
    let t = Instant::now();
    for x in &ids {
        let jti = identity::invocation_jti(x, 0);
        let mut req = client.post(url);
        let aud = reqwest::Url::parse(url)
            .map(|u| u.path().to_string())
            .unwrap_or_default();
        // Like `Signing::apply`: a fresh nonce per request.
        let bound = identity::Bound {
            nonce: Some(identity::fresh_nonce()),
            ..bound
        };
        for (k, v) in id.headers("acme", &aud, &jti, now_ms, bound) {
            req = req.header(k, v);
        }
        let _ = black_box(req);
    }
    let applied = t.elapsed();

    // 3.
    let tenants: Vec<String> = (0..n).map(|i| format!("tenant-{i}")).collect();
    let t = Instant::now();
    for (tenant, jti) in tenants.iter().zip(&jtis) {
        black_box(id.headers(tenant, aud, jti, now_ms, bound));
    }
    let derive = t.elapsed();

    // 4.
    let tenant = cairn_store::TenantId::new("acme").map_err(|e| anyhow!("{e}"))?;
    let kid = id
        .published(&tenant)
        .into_iter()
        .find(|k| k.active)
        .context("no published key")?
        .kid;
    let verify_n = n.min(2_000);
    let t = Instant::now();
    for _ in 0..verify_n {
        black_box(identity::verify(&token, &kid).map_err(|e| anyhow!(e))?);
    }
    let verify = t.elapsed();

    let us = |d: Duration, k: usize| d.as_secs_f64() * 1e6 / k as f64;
    eprintln!(
        "identity signing — profile {}, N={n}, JWT ~{} bytes",
        profile(),
        len / n
    );
    eprintln!(
        "  1. headers, key cached:              {:8.2} µs/op",
        us(cached, n)
    );
    eprintln!(
        "  2. like Signing::apply (minus {:.2}): {:8.2} µs/op",
        us(base, n),
        us(applied.saturating_sub(base), n)
    );
    eprintln!(
        "  3. first time (HKDF + key + sign):   {:8.2} µs/op ⇒ derivation ≈ {:.2} µs",
        us(derive, n),
        us(derive.saturating_sub(cached), n)
    );
    eprintln!(
        "  4. verify (receiving side, N={verify_n}): {:8.2} µs/op",
        us(verify, verify_n)
    );
    Ok(())
}

// ───────────────────────────────────────────────────── 2. backup / restore

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// The total (object count, bytes) under `prefix` of `store` (read directly, not counted).
async fn usage(store: &InMemory, prefix: Option<&str>) -> Result<(usize, u64)> {
    let p = prefix.map(slatedb::object_store::path::Path::from);
    let all: Vec<_> = store.list(p.as_ref()).try_collect().await?;
    Ok((all.len(), all.iter().map(|m| m.size).sum()))
}

/// Every commit has been flushed to L0 (as in `tests/backup.rs`).
async fn flushed(ctl: &Ctl) -> Result<bool> {
    let logs = ctl.wal_floors().await?;
    Ok(!logs.is_empty() && logs.iter().all(|l| l.pinned_by_range.is_none()))
}

/// Send invocations carrying a random payload (`payload` random bytes, sent as a hex
/// JSON string) until the bucket has ≥ `target` bytes. Returns the number of invocations.
async fn fill(bucket: &InMemory, url: &str, target: u64, payload: usize) -> Result<usize> {
    let mut sent = 0usize;
    loop {
        let (_, bytes) = usage(bucket, None).await?;
        if bytes >= target {
            return Ok(sent);
        }
        let mut batch = Vec::new();
        for _ in 0..32 {
            let mut raw = vec![0u8; payload];
            cairn_store::entropy::fill(&mut raw);
            let body = format!("\"{}\"", hex(&raw));
            let url = url.to_string();
            batch.push(async move { common::send(&url, "Svc", "run", None, &body).await });
        }
        for r in futures::future::join_all(batch).await {
            let (code, body) = r?;
            ensure!((200..300).contains(&code), "{code} {body}");
            sent += 1;
        }
    }
}

/// **Backup and restore by bucket size** (`ctl/backup.rs`).
///
/// For each size `S` MB:
/// 1. A 4-range cluster node on the source bucket (`retention_ms = None`, flushing
///    by frame age 100 ms); fake SDK `Svc/run` returns at once. Send invocations
///    carrying 32 KiB of random data (a 64 KB hex string) until the bucket is ≥ `S`
///    MB; wait until every range has flushed to L0.
/// 2. `Ctl::backup` on a `FaultyStore` wrapping the source bucket (a counting layer
///    only for ctl — the node has its own counting store) into a `FaultyStore`
///    wrapping a target `InMemory`, **while the node is still running** (as in real
///    operation).
/// 3. Stop the node. `Ctl::restore` from the target (a fresh counting wrapper) into
///    an empty bucket (counting wrapper).
///
/// Prints: objects, bytes, source/target requests by operation × path kind,
/// requests per object, time (debug profile: the time is only for relative comparison).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a cost measurement, run by hand"]
async fn backup_restore_cost() -> Result<()> {
    let sizes: Vec<u64> = std::env::var("CAIRN_COST_BACKUP_MB")
        .unwrap_or_else(|_| "10,100".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let payload = env_usize("CAIRN_COST_PAYLOAD", 32 * 1024);
    for mb in sizes {
        backup_once(mb, payload).await?;
    }
    Ok(())
}

async fn backup_once(mb: u64, payload: usize) -> Result<()> {
    let src = common::bucket();
    let sdk = FakeSdk::builder()
        .service("Svc", "run", vec![Step::output("\"ok\"")])
        .spawn()
        .await?;
    let tweak = |cfg: &mut NodeConfig| {
        cfg.node_wal.frame_age_flush = Duration::from_millis(100);
        cfg.retention_ms = None;
    };
    let a = common::start_cluster_node_tweaked(&src, "node-a", 4, Duration::from_secs(2), tweak)
        .await?;
    let url = a.node.url();
    let t_fill = Instant::now();
    let filled = async {
        common::register(&url, &sdk.url).await?;
        let sent = fill(&src, &url, mb * 1_000_000, payload).await?;
        let probe = Ctl::new(ObjectStores::single(StoreLocation::from_store(
            Arc::clone(&*src) as Arc<dyn ObjectStore>,
            "",
            "memory://",
        )));
        let t0 = Instant::now();
        while !flushed(&probe).await? {
            ensure!(
                t0.elapsed() < Duration::from_secs(120),
                "ranges not flushed to L0 after 120 s"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Ok(sent)
    }
    .await;
    let sent = match filled {
        Ok(s) => s,
        Err(e) => {
            a.node.shutdown().await;
            return Err(e);
        }
    };
    let fill_s = t_fill.elapsed().as_secs_f64();
    let (src_objects, src_bytes) = usage(&src, None).await?;

    // 2.
    let src_count = FaultyStore::wrap(Arc::clone(&*src) as Arc<dyn ObjectStore>);
    let dst_mem = Arc::new(InMemory::new());
    let dst_count = FaultyStore::wrap(Arc::clone(&dst_mem) as Arc<dyn ObjectStore>);
    let ctl = Ctl::new(ObjectStores::single(StoreLocation::from_store(
        src_count.clone(),
        "",
        "memory://",
    )));
    let target = StoreLocation::from_store(dst_count.clone(), "daily", "memory://r2/daily");
    let t = Instant::now();
    let rep = ctl
        .backup(
            &target,
            "cost@m4",
            "cairn-ctl backup --to memory://r2/daily",
        )
        .await;
    let backup_s = t.elapsed().as_secs_f64();
    a.node.shutdown().await;
    let rep = rep?;
    ensure!(rep.objects > 0, "{rep:?}");
    let src_c = src_count.counts();
    let dst_c = dst_count.counts();
    let (dst_objects, dst_bytes) = usage(&dst_mem, None).await?;

    // The kinds of source objects the copy took (by `object_kind` of the GET).
    let mut kinds: BTreeMap<&'static str, u64> = BTreeMap::new();
    for (_, op, _, kind, n) in src_count.counts_by_source_object() {
        if op == Op::Get {
            *kinds.entry(kind).or_default() += n;
        }
    }

    // 3.
    let restored = common::bucket();
    let into_count = FaultyStore::wrap(Arc::clone(&*restored) as Arc<dyn ObjectStore>);
    let from_count = FaultyStore::wrap(Arc::clone(&dst_mem) as Arc<dyn ObjectStore>);
    let into = Ctl::new(ObjectStores::single(StoreLocation::from_store(
        into_count.clone(),
        "",
        "memory://",
    )));
    let t = Instant::now();
    let rr = into
        .restore(
            &StoreLocation::from_store(from_count.clone(), "daily", "memory://r2/daily"),
            None,
            "cost@m4",
            "cairn-ctl restore",
        )
        .await?;
    let restore_s = t.elapsed().as_secs_f64();
    ensure!(rr.objects == rep.objects, "{rr:?} vs {rep:?}");

    let per = rep.objects as f64;
    eprintln!(
        "backup — target {mb} MB, profile {}, {sent} invocations × {payload} B random \
         (loaded in {fill_s:.1} s)",
        profile()
    );
    eprintln!(
        "  source bucket at backup time: {src_objects} objects, {src_bytes} bytes; copy: {} objects, \
         {} bytes ({} ranges); target has {dst_objects} objects, {dst_bytes} bytes; {backup_s:.2} s",
        rep.objects,
        rep.bytes,
        rep.ranges.len()
    );
    print_counts("backup, source store", &src_c, per);
    eprintln!("    source GETs by object kind: {kinds:?}");
    print_counts("backup, target store", &dst_c, per);
    eprintln!(
        "  restore: {} object, {} byte, {restore_s:.2} s",
        rr.objects, rr.bytes
    );
    print_counts("restore, backup store (reads)", &from_count.counts(), per);
    print_counts("restore, new store (writes)", &into_count.counts(), per);
    Ok(())
}

// ───────────────────────────────────────────────────── 3. SSO and cookies

const BOOT: &str = "cf_test_cost_units_bootstrap_key_0123456789";
const SECRET: &str = "cf_test_cluster_secret_cost_units_32b";
const HOST: &str = "127.0.0.1";
const CLIENT: &str = "env_acme";
const TENANT: &str = "acme";

async fn admin(node: &str, method: reqwest::Method, path: &str, body: Value) -> Result<Value> {
    let r = common::oidc::http()
        .request(method, format!("{node}{path}"))
        .bearer_auth(BOOT)
        .json(&body)
        .send()
        .await?;
    let code = r.status();
    let v = r.json().await.unwrap_or(Value::Null);
    ensure!(code.is_success(), "{path}: {code} {v}");
    Ok(v)
}

/// Counters by `(source, "operation/path kind", object kind)`.
type BySource = BTreeMap<(&'static str, String, &'static str), u64>;

fn by_source(store: &FaultyStore) -> BySource {
    store
        .counts_by_source_object()
        .into_iter()
        .map(|(s, o, c, k, n)| ((s, format!("{o:?}/{c:?}"), k), n))
        .collect()
}

fn diff(after: &BySource, before: &BySource) -> BySource {
    after
        .iter()
        .map(|(k, &n)| (k.clone(), n - before.get(k).copied().unwrap_or(0)))
        .filter(|(_, n)| *n > 0)
        .collect()
}

/// One measurement window: run `work` (returns the number of units), print requests
/// by source minus the idle background (`idle` counted over `idle_s` seconds, scaled
/// by the window's duration).
async fn window<F, Fut>(
    label: &str,
    store: &FaultyStore,
    idle: &BySource,
    idle_s: f64,
    work: F,
) -> Result<()>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<usize>>,
{
    let before = by_source(store);
    let c0 = store.counts();
    store.record_writes();
    let t = Instant::now();
    let units = work().await?;
    let secs = t.elapsed().as_secs_f64();
    let writes = store.take_writes();
    let got = diff(&by_source(store), &before);
    let total = store.counts().since(&c0);
    eprintln!(
        "  {label}: {units} units in {secs:.2} s; raw {} requests",
        total.total()
    );
    let mut keys: Vec<_> = got.keys().chain(idle.keys()).cloned().collect();
    keys.sort();
    keys.dedup();
    let mut net_total = 0.0;
    for k in keys {
        let raw = got.get(&k).copied().unwrap_or(0) as f64;
        let bg = idle.get(&k).copied().unwrap_or(0) as f64 * secs / idle_s;
        let net = raw - bg;
        if raw == 0.0 {
            continue;
        }
        net_total += net.max(0.0);
        eprintln!(
            "    {:<28} {:<20} {:<14} raw {raw:6} bg ~{bg:7.1} ⇒ {:7.3}/unit",
            k.0,
            k.1,
            k.2,
            net / units as f64
        );
    }
    eprintln!(
        "    ⇒ ~{:.3} requests/unit after subtracting the background",
        net_total / units as f64
    );
    // The write path, digits replaced by `#` to group by shape.
    let mut shapes: BTreeMap<String, u64> = BTreeMap::new();
    for (op, path) in writes {
        let shape: String = path
            .as_ref()
            .chars()
            .map(|c| if c.is_ascii_digit() { '#' } else { c })
            .collect();
        *shapes.entry(format!("{op:?} {shape}")).or_default() += 1;
    }
    for (shape, n) in shapes {
        eprintln!("    write: {shape} × {n}");
    }
    Ok(())
}

/// **Object store requests of SSO and the console session cookie.**
///
/// One node (1 range, auth on, console on) on a `FaultyStore` wrapping the test
/// bucket; OIDC configured with the fake issuer (`tests/common/oidc.rs`), one client
/// `127.0.0.1` → tenant `acme`; one tenant `admin` key. After 5 s of settling:
///
/// 1. Background: `IDLE_S` seconds doing nothing — one rate per `(source, op, kind)`.
/// 2. `N` full OIDC logins (`/ui/login/oidc` → issuer → callback), sequential (the
///    `SSO_START_BURST`/`EXCHANGE_BURST` cap = 20 ⇒ `N` ≤ 20).
/// 3. `N` `POST /ui/session`s with the bootstrap key, `N` with the tenant key.
/// 4. `M` `GET /auth/whoami`s with the SSO cookie `__Host-cairn_ui`, `M` with the key
///    cookie `cairn_ui`, `M` with Bearer (the control). `whoami` reads
///    `cluster-version` itself (`ingress/admin.rs`), so this is repeated on
///    `GET /admin/oidc`, which the tenant's `admin` role gets `403` for at the
///    permission layer — only the authentication part remains.
///
/// Each window prints the raw requests by source and what is left after subtracting
/// the background × the window's duration. An idle one-range node has periodic ticks
/// (renewal, cleanup), so a short window gives odd numbers ± the background tick:
/// read the "raw" column together with the "bg" column.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a cost measurement, run by hand"]
async fn sso_and_cookie_cost() -> Result<()> {
    let n = env_usize("CAIRN_COST_LOGINS", 20).min(20);
    let m = env_usize("CAIRN_COST_CHECKS", 500);
    let idle_s = env_usize("CAIRN_COST_IDLE_S", 20) as f64;
    let issuer = common::oidc::FakeIssuer::start().await?;
    let mem = common::bucket();
    let store = FaultyStore::wrap(Arc::clone(&*mem) as Arc<dyn ObjectStore>);
    let mut cfg = NodeConfig::new(
        ObjectStores::single(StoreLocation::from_store(store.clone(), "", "memory://")),
        NodeMode::single(1),
    );
    cfg.tick_interval = Some(Duration::from_millis(50));
    cfg.flush_interval = Some(Duration::from_millis(5));
    cfg.auth = AuthConfig {
        required: true,
        admin_key: Some(BOOT.into()),
        cluster_secret: Some(SECRET.into()),
        metrics_key: None,
        admin_key_sha256: None,
        provision_key_sha256: None,
    };
    cfg.ui = UiConfig::default();
    let node = common::start_led(cfg).await?;
    let r = sso_windows(&node, &issuer, &store, n, m, idle_s).await;
    node.shutdown().await;
    r
}

async fn sso_windows(
    node: &RunningNode,
    issuer: &common::oidc::FakeIssuer,
    store: &FaultyStore,
    n: usize,
    m: usize,
    idle_s: f64,
) -> Result<()> {
    use common::oidc::http;
    let url = node.url();
    admin(
        &url,
        reqwest::Method::PUT,
        "/admin/oidc",
        json!({"issuer": issuer.url, "displayName": "Cairnflow Cloud"}),
    )
    .await?;
    admin(
        &url,
        reqwest::Method::PUT,
        &format!("/admin/oidc/clients/{CLIENT}"),
        json!({"hostname": HOST, "tenant": TENANT, "maxRole": "admin",
               "portalUrl": "https://app.example.test/environments"}),
    )
    .await?;
    let key = admin(
        &url,
        reqwest::Method::POST,
        "/admin/keys",
        json!({"tenant": TENANT, "role": "admin"}),
    )
    .await?["secret"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    ensure!(!key.is_empty(), "could not create the tenant key");
    // One warm-up login (discovery/JWKS into the node's cache), then settle.
    let warm = common::oidc::login(&url, None).await?;
    ensure!(warm.cookie.is_some(), "warm-up login: {}", warm.landed);
    tokio::time::sleep(Duration::from_secs(5)).await;

    // 1.
    let b0 = by_source(store);
    let c0 = store.counts();
    tokio::time::sleep(Duration::from_secs_f64(idle_s)).await;
    let idle = diff(&by_source(store), &b0);
    let idle_c = store.counts().since(&c0);
    eprintln!(
        "SSO/cookie — profile {}, one-range node, N={n} logins, M={m} checks",
        profile()
    );
    eprintln!(
        "  idle background: {} requests in {idle_s} s ({:.2}/s)",
        idle_c.total(),
        idle_c.total() as f64 / idle_s
    );
    for (k, v) in &idle {
        eprintln!("    {:<28} {:<20} {:<14} {v}", k.0, k.1, k.2);
    }

    // 2.
    let mut sso_cookie = String::new();
    window("full OIDC login", store, &idle, idle_s, || async {
        for _ in 0..n {
            let l = common::oidc::login(&url, None).await?;
            sso_cookie = l.cookie.context("SSO login without a cookie")?;
        }
        Ok(n)
    })
    .await?;

    // 3.
    let key_login = |k: String| {
        let url = url.clone();
        async move {
            let r = reqwest::Client::new()
                .post(format!("{url}/ui/session"))
                .header(header::ORIGIN, &url)
                .json(&json!({ "key": k }))
                .send()
                .await?;
            ensure!(
                r.status() == StatusCode::OK,
                "login with a key: {}",
                r.status()
            );
            let set = r
                .headers()
                .get(header::SET_COOKIE)
                .and_then(|v| v.to_str().ok())
                .context("no Set-Cookie")?;
            Ok(set.split(';').next().unwrap_or_default().to_string())
        }
    };
    window(
        "login with the bootstrap key",
        store,
        &idle,
        idle_s,
        || async {
            for _ in 0..n {
                key_login(BOOT.to_string()).await?;
            }
            Ok(n)
        },
    )
    .await?;
    let mut key_cookie = String::new();
    window("tenant key login", store, &idle, idle_s, || async {
        for _ in 0..n {
            key_cookie = key_login(key.clone()).await?;
        }
        Ok(n)
    })
    .await?;

    // 4.
    let check = |cookie: Option<String>, bearer: Option<String>, path: &str, want: StatusCode| {
        let url = url.clone();
        let path = path.to_string();
        async move {
            let client = http();
            for _ in 0..m {
                let mut r = client.get(format!("{url}{path}"));
                if let Some(c) = &cookie {
                    r = r.header(header::COOKIE, c);
                }
                if let Some(b) = &bearer {
                    r = r.bearer_auth(b);
                }
                let r = r.send().await?;
                ensure!(r.status() == want, "{path}: {}", r.status());
            }
            Ok(m)
        }
    };
    window("SSO cookie check (whoami)", store, &idle, idle_s, || {
        check(
            Some(sso_cookie.clone()),
            None,
            "/auth/whoami",
            StatusCode::OK,
        )
    })
    .await?;
    window("key cookie check (whoami)", store, &idle, idle_s, || {
        check(
            Some(key_cookie.clone()),
            None,
            "/auth/whoami",
            StatusCode::OK,
        )
    })
    .await?;
    window("tenant key Bearer (whoami)", store, &idle, idle_s, || {
        check(None, Some(key.clone()), "/auth/whoami", StatusCode::OK)
    })
    .await?;
    // Authentication + the permission decision only: the tenant's `admin` role calls a
    // cluster route ⇒ `403` before the handler — none of the handler's own I/O as in
    // `whoami`.
    window(
        "SSO cookie check (403 /admin/oidc)",
        store,
        &idle,
        idle_s,
        || {
            check(
                Some(sso_cookie.clone()),
                None,
                "/admin/oidc",
                StatusCode::FORBIDDEN,
            )
        },
    )
    .await?;
    window(
        "key cookie check (403 /admin/oidc)",
        store,
        &idle,
        idle_s,
        || {
            check(
                Some(key_cookie.clone()),
                None,
                "/admin/oidc",
                StatusCode::FORBIDDEN,
            )
        },
    )
    .await?;
    window(
        "tenant key Bearer (403 /admin/oidc)",
        store,
        &idle,
        idle_s,
        || {
            check(
                None,
                Some(key.clone()),
                "/admin/oidc",
                StatusCode::FORBIDDEN,
            )
        },
    )
    .await?;
    Ok(())
}
