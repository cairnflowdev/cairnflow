// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The contract of the real S3 client (OS1,
//! `docs/reliability/irreversible.md` §0.3): object_store **retries a 5xx by
//! itself, conditional PUTs included**. If the first attempt was written but
//! returned 5xx, the retry meets that very write: `Create` returns
//! `AlreadyExists`, `Update(etag)` returns `Precondition` — while the object is
//! already ours. Rule 7 of AGENTS.md (`cas::put_resolving`: read back, compare
//! the content) exists because of exactly this behaviour; before these tests
//! there was only a simulation of it (`Ambiguity::Retried`), no test pinning the
//! real client.
//!
//! The fake S3 server (axum) only knows conditional PUT and GET (including
//! `If-None-Match`), and returns `503` for the `n`-th PUT to key `once<n>/…`
//! **after applying** it.
//!
//! G18: the client sends a GET's `If-None-Match` and surfaces `304` as
//! `NotModified`, `200` as the content — no HTTP caching layer in the client
//! answers a stale `304` in place of the store.

// The test runs on real time and a real network; only `sim_cluster` must be deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterminism"
)]

use {
    anyhow::{anyhow, ensure, Result},
    axum::{
        body::Bytes,
        extract::{Path as UrlPath, State},
        http::{HeaderMap, StatusCode},
        response::{IntoResponse, Response},
        routing::get,
        Router,
    },
    slatedb::object_store::{
        aws::AmazonS3Builder, path::Path, Error as OsError, ObjectStore, ObjectStoreExt, PutMode,
        PutOptions, UpdateVersion,
    },
    std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    },
};

#[derive(Default)]
struct Fake {
    /// key → (body, etag).
    objects: BTreeMap<String, (Bytes, String)>,
    /// key → number of PUTs received.
    puts: BTreeMap<String, u32>,
    next_etag: u64,
}

type Shared = Arc<Mutex<Fake>>;

/// `once<n>/…` ⇒ the `n`-th PUT to that key is applied, then returns `503`.
fn fails_on(key: &str) -> Option<u32> {
    key.strip_prefix("once")?.split('/').next()?.parse().ok()
}

async fn put(
    State(st): State<Shared>,
    UrlPath((_bucket, key)): UrlPath<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // `always/…` ⇒ every PUT returns `503` **before** applying it (OS6).
    if key.starts_with("always/") {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "<Error><Code>SlowDown</Code><Message>not written</Message></Error>",
        )
            .into_response();
    }
    let mut s = st.lock().unwrap();
    let n = {
        let c = s.puts.entry(key.clone()).or_default();
        *c += 1;
        *c
    };
    let header = |h: &str| headers.get(h).and_then(|v| v.to_str().ok());
    let current = s.objects.get(&key).map(|(_, e)| e.clone());
    let failed = |code: &str| {
        (
            StatusCode::PRECONDITION_FAILED,
            format!("<Error><Code>{code}</Code><Message>fake</Message></Error>"),
        )
            .into_response()
    };
    if header("if-none-match") == Some("*") && current.is_some() {
        return failed("PreconditionFailed");
    }
    if let Some(want) = header("if-match") {
        if current.as_deref() != Some(want) {
            return failed("PreconditionFailed");
        }
    }
    s.next_etag += 1;
    let etag = format!("\"e{}\"", s.next_etag);
    s.objects.insert(key.clone(), (body, etag.clone()));
    if fails_on(&key) == Some(n) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "<Error><Code>SlowDown</Code><Message>written, then failed</Message></Error>",
        )
            .into_response();
    }
    (StatusCode::OK, [("ETag", etag)]).into_response()
}

async fn get_object(
    State(st): State<Shared>,
    UrlPath((_bucket, key)): UrlPath<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let inm = headers.get("if-none-match").and_then(|v| v.to_str().ok());
    match st.lock().unwrap().objects.get(&key) {
        // `stale/…` ⇒ a caching layer answers `304` to every conditional GET (G18).
        Some(_) if inm.is_some() && key.starts_with("stale/") => {
            StatusCode::NOT_MODIFIED.into_response()
        }
        Some((_, etag)) if inm == Some(etag.as_str()) => {
            (StatusCode::NOT_MODIFIED, [("ETag", etag.clone())]).into_response()
        }
        Some((body, etag)) => (
            StatusCode::OK,
            [
                ("ETag", etag.clone()),
                ("Last-Modified", "Thu, 01 Jan 2026 00:00:00 GMT".into()),
            ],
            body.clone(),
        )
            .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            "<Error><Code>NoSuchKey</Code><Message>not found</Message></Error>",
        )
            .into_response(),
    }
}

/// Build the fake server and an S3 client configured like cairn's `s3_store`
/// (retry policy `s3_client_retry`: object_store's default, time limit
/// `S3_CLIENT_RETRY_TIMEOUT`).
async fn fake_s3() -> Result<(Arc<dyn ObjectStore>, Shared)> {
    fake_s3_with(slatedb::object_store::RetryConfig {
        retry_timeout: cairn_store::S3_CLIENT_RETRY_TIMEOUT,
        ..Default::default()
    })
    .await
}

async fn fake_s3_with(
    retry: slatedb::object_store::RetryConfig,
) -> Result<(Arc<dyn ObjectStore>, Shared)> {
    let st: Shared = Arc::default();
    let app = Router::new()
        .route("/{bucket}/{*key}", get(get_object).put(put))
        .with_state(st.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move { axum::serve(listener, app).await });
    let store = AmazonS3Builder::new()
        .with_endpoint(format!("http://{addr}"))
        .with_bucket_name("b")
        .with_region("us-east-1")
        .with_access_key_id("k")
        .with_secret_access_key("s")
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        // Like cairn-store's `config::s3_client_retry` (not exported).
        .with_retry(retry)
        .build()?;
    Ok((Arc::new(store), st))
}

fn body_of(st: &Shared, key: &str) -> Option<Bytes> {
    st.lock().unwrap().objects.get(key).map(|(b, _)| b.clone())
}

/// `Create` whose first attempt was written and then returned `503`: the client
/// retries by itself, the retry meets our own object and returns `AlreadyExists`.
/// The object on the store **is** our write.
#[tokio::test]
async fn create_that_landed_then_5xx_is_reported_as_already_exists() -> Result<()> {
    let (store, st) = fake_s3().await?;
    let key = "once1/owners/range-3";
    let got = store
        .put_opts(
            &Path::from(key),
            Bytes::from_static(b"ours").into(),
            PutOptions::from(PutMode::Create),
        )
        .await;
    ensure!(
        matches!(got, Err(OsError::AlreadyExists { .. })),
        "Create: must be AlreadyExists (the retry sees our own write), got {got:?}"
    );
    ensure!(
        body_of(&st, key).as_deref() == Some(&b"ours"[..]),
        "the object on the store must be our write"
    );
    ensure!(
        st.lock().unwrap().puts[key] == 2,
        "the client must retry exactly once"
    );
    Ok(())
}

/// `Update(etag)` whose first attempt was written and then returned `503`: the
/// retry carries the old etag, which that first attempt already changed ⇒
/// `Precondition`. A caller that reads this as "lost the CAS" drops a write that
/// won (rule 7).
#[tokio::test]
async fn update_that_landed_then_5xx_is_reported_as_precondition() -> Result<()> {
    let (store, st) = fake_s3().await?;
    let key = "once2/cluster.conf";
    let path = Path::from(key);
    let first = store
        .put_opts(
            &path,
            Bytes::from_static(b"v1").into(),
            PutOptions::from(PutMode::Create),
        )
        .await?;
    let got = store
        .put_opts(
            &path,
            Bytes::from_static(b"v2").into(),
            PutOptions::from(PutMode::Update(UpdateVersion {
                e_tag: first.e_tag.clone(),
                version: None,
            })),
        )
        .await;
    ensure!(
        matches!(got, Err(OsError::Precondition { .. })),
        "Update: must be Precondition (the etag was changed by our own write), got {got:?}"
    );
    ensure!(
        body_of(&st, key).as_deref() == Some(&b"v2"[..]),
        "the object on the store must be our write"
    );
    // Reading back (how `cas::put_resolving` resolves it) sees exactly our content.
    let read = store.get(&path).await?.bytes().await?;
    ensure!(&read[..] == b"v2", "read back: {read:?}");
    Ok(())
}

/// Control: without a `503` the real CAS answers correctly (win ⇒ `Ok`, loss ⇒
/// a precondition error) — the fake server does not produce two-way results by
/// itself.
#[tokio::test]
async fn without_5xx_conditional_puts_are_unambiguous() -> Result<()> {
    let (store, _st) = fake_s3().await?;
    let path = Path::from("plain/k");
    store
        .put_opts(
            &path,
            Bytes::from_static(b"a").into(),
            PutMode::Create.into(),
        )
        .await?;
    let again = store
        .put_opts(
            &path,
            Bytes::from_static(b"b").into(),
            PutMode::Create.into(),
        )
        .await;
    ensure!(
        matches!(again, Err(OsError::AlreadyExists { .. })),
        "second Create: {again:?}"
    );
    let stale = store
        .put_opts(
            &path,
            Bytes::from_static(b"c").into(),
            PutMode::Update(UpdateVersion {
                e_tag: Some("\"not-it\"".into()),
                version: None,
            })
            .into(),
        )
        .await;
    ensure!(
        matches!(stale, Err(OsError::Precondition { .. })),
        "Update with stale etag: {stale:?}"
    );
    store.get(&path).await.map_err(|e| anyhow!("GET: {e}"))?;
    Ok(())
}

/// OS6 (`docs/reliability/irreversible.md` §0.3): when retries of a `503` run
/// out, or the connection cannot be made, the client returns `Error::Generic` —
/// and `retry::object_store_verdict` calls that **transient**. The boot path
/// (probe gate, session open) relies on this to retry a 503 instead of
/// concluding "the store does not honour conditions"; a meaningful answer
/// (`412`, `409`, …) is a variant of its own
/// (`without_5xx_conditional_puts_are_unambiguous`).
#[tokio::test]
async fn exhausted_5xx_and_refused_connection_are_generic_and_transient() -> Result<()> {
    use cairn_store::retry::{object_store_verdict, Verdict};
    let quick = slatedb::object_store::RetryConfig {
        max_retries: 2,
        retry_timeout: std::time::Duration::from_secs(5),
        backoff: slatedb::object_store::BackoffConfig {
            init_backoff: std::time::Duration::from_millis(10),
            max_backoff: std::time::Duration::from_millis(20),
            base: 2.0,
        },
    };
    let (store, _st) = fake_s3_with(quick.clone()).await?;
    let got = store
        .put_opts(
            &Path::from("always/k"),
            Bytes::from_static(b"a").into(),
            PutMode::Create.into(),
        )
        .await;
    let Err(e) = got else {
        anyhow::bail!("503 forever, yet the PUT succeeded");
    };
    ensure!(
        matches!(e, OsError::Generic { .. }),
        "503 with retries exhausted: {e:?}"
    );
    ensure!(object_store_verdict(&e) == Verdict::Transient, "{e:?}");

    // A port nobody listens on: connection error.
    let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = closed.local_addr()?;
    drop(closed);
    let refused = AmazonS3Builder::new()
        .with_endpoint(format!("http://{addr}"))
        .with_bucket_name("b")
        .with_region("us-east-1")
        .with_access_key_id("k")
        .with_secret_access_key("s")
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .with_retry(quick)
        .build()?;
    let e = refused
        .get(&Path::from("k"))
        .await
        .err()
        .ok_or_else(|| anyhow!("GET to a closed port succeeded"))?;
    ensure!(matches!(e, OsError::Generic { .. }), "conn failed: {e:?}");
    ensure!(object_store_verdict(&e) == Verdict::Transient, "{e:?}");
    Ok(())
}

/// **G18, real client** (`docs/reliability/irreversible.md` §0.3): the S3
/// client's GET `If-None-Match` reaches the store and the answer comes back
/// intact — `304` ⇒ `NotModified`, `200` ⇒ the new content — so solo mode
/// (LS-16) and the read fence (LS-5) see exactly what the store says. The fake
/// server keeps the contract; `store_probe::conditional_get` (the probe of
/// `cairn-ctl cluster enable-quiet-lease`) runs 20 rounds through the client and
/// passes. Control: a `stale/…` key for which the server answers `304` to every
/// conditional GET (a proxy cache) ⇒ the probe refuses with the reason
/// "STALE 304".
#[tokio::test]
async fn conditional_get_reaches_the_store_and_a_stale_304_is_caught() -> Result<()> {
    use slatedb::object_store::GetOptions;
    let (store, _st) = fake_s3().await?;
    let path = Path::from("g18/x");
    let v1 = store
        .put_opts(
            &path,
            Bytes::from_static(b"v1").into(),
            PutMode::Create.into(),
        )
        .await?;
    let v2 = store
        .put_opts(
            &path,
            Bytes::from_static(b"v2").into(),
            PutMode::Update(UpdateVersion::from(v1.clone())).into(),
        )
        .await?;
    let cond = |e: &Option<String>| GetOptions {
        if_none_match: e.clone(),
        ..Default::default()
    };
    let old = store.get_opts(&path, cond(&v1.e_tag)).await?;
    let body = old.bytes().await?;
    ensure!(&body[..] == b"v2", "stale etag ⇒ {body:?}, must be v2");
    let new = store.get_opts(&path, cond(&v2.e_tag)).await;
    ensure!(
        matches!(new, Err(OsError::NotModified { .. })),
        "a new etag must give 304: {new:?}"
    );

    let prefix = Path::from("g18");
    let got = cairn_store::store_probe::conditional_get(&store, &store, &prefix, "n", 20).await?;
    got.map_err(|why| anyhow!("server keeps the contract, yet the probe failed: {why}"))?;

    let stale =
        cairn_store::store_probe::conditional_get(&store, &store, &Path::from("stale"), "n", 4)
            .await?;
    let why = stale
        .err()
        .ok_or_else(|| anyhow!("server answers a stale 304, yet the probe passed"))?;
    ensure!(why.contains("STALE 304"), "{why}");
    Ok(())
}
