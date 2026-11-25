// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `cairn-ctl --backup-env-prefix P`: the backup store uses its **own**
//! credentials/endpoint/region — variables `P` + `AWS_*` — instead of the same
//! `AWS_*` set as `--main-store`. Context: the source on one provider, the backup
//! on another, two key sets, two endpoints.
//!
//! Runs the `cairn-ctl` binary itself (the child process's environment built from
//! scratch, `env_clear`): the environment-variable read path, the flags and the exit
//! codes are what needs checking.
//!
//! The in-process fake S3 server (axum) accepts only **one** access key id (read from
//! `Authorization: AWS4-HMAC-SHA256 Credential=<id>/…`; the signature is not checked —
//! the real signature check is in the SeaweedFS test below) and records every request
//! with the id it used.

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
    axum::{
        body::Bytes,
        extract::State,
        http::{HeaderMap, Method, StatusCode, Uri},
        response::{IntoResponse, Response},
        Router,
    },
    futures::TryStreamExt,
    slatedb::object_store::{
        aws::AmazonS3Builder, local::LocalFileSystem, path::Path as ObjPath, ObjectStore,
        ObjectStoreExt, PutPayload,
    },
    std::{
        collections::BTreeMap,
        path::{Path, PathBuf},
        sync::{Arc, Mutex},
        time::Duration,
    },
};

const SRC_KEY: &str = "tigris-key-id";
const SRC_SECRET: &str = "tigris-secret-SENTINEL-1111";
const DST_KEY: &str = "r2-key-id";
const DST_SECRET: &str = "r2-secret-SENTINEL-2222";
const PREFIX: &str = "CAIRN_BACKUP_";

// ───────────────────────────────────────────── fake S3 server

#[derive(Default)]
struct FakeState {
    /// key → (body, etag).
    objects: BTreeMap<String, (Bytes, String)>,
    next_etag: u64,
    /// (method, path, access key id) of every request.
    log: Vec<(String, String, String)>,
}

struct Fake {
    bucket: String,
    key_id: String,
    st: Mutex<FakeState>,
}

struct FakeS3 {
    url: String,
    fake: Arc<Fake>,
}

impl FakeS3 {
    async fn spawn(bucket: &str, key_id: &str) -> Result<Self> {
        let fake = Arc::new(Fake {
            bucket: bucket.into(),
            key_id: key_id.into(),
            st: Mutex::default(),
        });
        let app = Router::new().fallback(handle).with_state(fake.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        tokio::spawn(async move { axum::serve(listener, app).await });
        Ok(Self { url, fake })
    }

    fn log(&self) -> Vec<(String, String, String)> {
        self.fake.st.lock().unwrap().log.clone()
    }

    fn keys(&self) -> Vec<String> {
        self.fake
            .st
            .lock()
            .unwrap()
            .objects
            .keys()
            .cloned()
            .collect()
    }
}

fn s3_error(status: StatusCode, code: &str) -> Response {
    (
        status,
        format!("<Error><Code>{code}</Code><Message>fake</Message></Error>"),
    )
        .into_response()
}

fn decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

async fn handle(
    State(f): State<Arc<Fake>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let akid = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|a| a.split("Credential=").nth(1))
        .and_then(|c| c.split('/').next())
        .unwrap_or_default()
        .to_string();
    let path = decode(uri.path());
    let mut st = f.st.lock().unwrap();
    st.log
        .push((method.to_string(), path.clone(), akid.clone()));
    if akid != f.key_id {
        return s3_error(StatusCode::FORBIDDEN, "InvalidAccessKeyId");
    }
    let rest = path.trim_start_matches('/');
    let (bucket, key) = rest.split_once('/').unwrap_or((rest, ""));
    if bucket != f.bucket {
        return s3_error(StatusCode::NOT_FOUND, "NoSuchBucket");
    }
    let header = |h: &str| headers.get(h).and_then(|v| v.to_str().ok());
    if header("range").is_some() || header("x-amz-copy-source").is_some() {
        return s3_error(StatusCode::NOT_IMPLEMENTED, "NotImplemented");
    }
    match (method.as_str(), key.is_empty()) {
        ("GET", true) => {
            let q: BTreeMap<String, String> = uri
                .query()
                .unwrap_or_default()
                .split('&')
                .filter(|s| !s.is_empty())
                .map(|kv| {
                    let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
                    (decode(k), decode(v))
                })
                .collect();
            let prefix = q.get("prefix").cloned().unwrap_or_default();
            let delim = q.get("delimiter").cloned().filter(|d| !d.is_empty());
            let mut contents = String::new();
            let mut common = std::collections::BTreeSet::new();
            let mut n = 0;
            for (k, (b, etag)) in st.objects.range(prefix.clone()..) {
                let Some(after) = k.strip_prefix(&prefix) else {
                    break;
                };
                if let Some(d) = &delim {
                    if let Some(at) = after.find(d.as_str()) {
                        common.insert(format!("{prefix}{}", &after[..at + d.len()]));
                        continue;
                    }
                }
                n += 1;
                contents.push_str(&format!(
                    "<Contents><Key>{}</Key><LastModified>2026-01-01T00:00:00.000Z</LastModified>\
                     <ETag>{}</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
                    xml_escape(k),
                    xml_escape(etag),
                    b.len()
                ));
            }
            let prefixes: String = common
                .iter()
                .map(|p| {
                    format!(
                        "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
                        xml_escape(p)
                    )
                })
                .collect();
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                 <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                 <Name>{}</Name><Prefix>{}</Prefix><KeyCount>{}</KeyCount><MaxKeys>1000</MaxKeys>\
                 <IsTruncated>false</IsTruncated>{contents}{prefixes}</ListBucketResult>",
                f.bucket,
                xml_escape(&prefix),
                n + common.len()
            );
            (StatusCode::OK, [("content-type", "application/xml")], xml).into_response()
        }
        ("GET" | "HEAD", false) => match st.objects.get(key) {
            Some((b, etag)) => {
                let len = b.len().to_string();
                let hdrs = [
                    ("ETag", etag.clone()),
                    ("Last-Modified", "Thu, 01 Jan 2026 00:00:00 GMT".to_string()),
                    ("Content-Length", len),
                ];
                if method == Method::HEAD {
                    (StatusCode::OK, hdrs).into_response()
                } else {
                    (StatusCode::OK, hdrs, b.clone()).into_response()
                }
            }
            None => s3_error(StatusCode::NOT_FOUND, "NoSuchKey"),
        },
        ("PUT", false) => {
            let current = st.objects.get(key).map(|(_, e)| e.clone());
            if header("if-none-match") == Some("*") && current.is_some() {
                return s3_error(StatusCode::PRECONDITION_FAILED, "PreconditionFailed");
            }
            if let Some(want) = header("if-match") {
                if current.as_deref() != Some(want) {
                    return s3_error(StatusCode::PRECONDITION_FAILED, "PreconditionFailed");
                }
            }
            st.next_etag += 1;
            let etag = format!("\"e{}\"", st.next_etag);
            st.objects.insert(key.to_string(), (body, etag.clone()));
            (StatusCode::OK, [("ETag", etag)]).into_response()
        }
        ("DELETE", false) => {
            st.objects.remove(key);
            StatusCode::NO_CONTENT.into_response()
        }
        _ => s3_error(StatusCode::NOT_IMPLEMENTED, "NotImplemented"),
    }
}

// ───────────────────────────────────────────── source cluster, binary

/// The test's own temporary directory (deleted on drop).
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Result<Self> {
        let p = std::env::temp_dir().join(format!(
            "cairn-e4-{tag}-{}-{}",
            std::process::id(),
            cairn_store::entropy::now_ms()
        ));
        std::fs::create_dir_all(&p)?;
        Ok(Self(p))
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A real cluster bucket (2 ranges, one node that has stopped), as `InMemory`.
async fn stopped_cluster() -> Result<common::Bucket> {
    let bucket = common::bucket();
    let n = common::start_cluster_node(&bucket, "node-a", 2, Duration::from_secs(2)).await?;
    n.node.shutdown().await;
    Ok(bucket)
}

/// Copy every object of `from` into `to`.
async fn copy_all(from: &dyn ObjectStore, to: &dyn ObjectStore, under: &str) -> Result<usize> {
    let metas: Vec<_> = from.list(None).try_collect().await?;
    for m in &metas {
        let body = from.get(&m.location).await?.bytes().await?;
        let dst = if under.is_empty() {
            m.location.clone()
        } else {
            ObjPath::from(format!("{under}/{}", m.location))
        };
        to.put(&dst, PutPayload::from(body)).await?;
    }
    Ok(metas.len())
}

/// (path, content) of every file under `dir` — compared before/after.
async fn snapshot(dir: &Path) -> Result<BTreeMap<String, Bytes>> {
    let fs = LocalFileSystem::new_with_prefix(dir)?;
    let mut out = BTreeMap::new();
    for m in fs.list(None).try_collect::<Vec<_>>().await? {
        let b = fs.get(&m.location).await?.bytes().await?;
        out.insert(m.location.to_string(), b);
    }
    Ok(out)
}

struct Run {
    code: i32,
    stdout: String,
    stderr: String,
}

/// Run `cairn-ctl` with **exactly** `env` (a cleared environment).
async fn ctl(env: &[(String, String)], args: &[&str]) -> Result<Run> {
    let env = env.to_vec();
    let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
    tokio::task::spawn_blocking(move || {
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_cairn-ctl"))
            .env_clear()
            .envs(env)
            .args(&args)
            .output()
            .context("run cairn-ctl")?;
        Ok(Run {
            code: out.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        })
    })
    .await?
}

fn env(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn no_secret(r: &Run, secrets: &[&str]) -> Result<()> {
    for s in secrets {
        ensure!(
            !r.stdout.contains(s) && !r.stderr.contains(s),
            "a secret leaked into cairn-ctl's output:\n{}\n{}",
            r.stdout,
            r.stderr
        );
    }
    Ok(())
}

/// **A backup target with its own credentials**, on fake S3 servers that each accept
/// only their own key.
///
/// Order:
/// 1. A real cluster (2 ranges) stopped, copied out to `file://` — the source.
/// 2. Two fake S3 servers: "Tigris" (bucket `cell`, accepts only `tigris-key-id`) —
///    exactly where `AWS_*` points, a decoy —, and "R2" (bucket `bk`, accepts only
///    `r2-key-id`) — where `CAIRN_BACKUP_AWS_*` points, region `auto`.
/// 3. `--backup-env-prefix CAIRN_BACKUP_` without
///    `CAIRN_BACKUP_AWS_SECRET_ACCESS_KEY` ⇒ code 2, the error names the variable; no
///    request goes anywhere; the source is unchanged.
/// 4. A wrong target key (an unknown `CAIRN_BACKUP_AWS_ACCESS_KEY_ID`) ⇒ code ≠ 0; R2
///    refuses, no object; Tigris gets no request; the source is unchanged.
/// 5. Correct ⇒ `backup` code 0; every request to R2 carries `r2-key-id`, Tigris
///    gets nothing; `backup list` sees a complete copy; `backup prune` (dry run) code 0.
/// 6. No flag (the old behaviour) ⇒ the target uses `AWS_*`: requests go to Tigris with
///    `tigris-key-id`, R2 gets nothing.
/// 7. `restore --from` R2 with the flag into an empty directory ⇒ code 0, `verify` clean.
/// 8. The flag on a command that is not a backup command ⇒ code 2.
///
/// Must see: as above, and no run prints a secret.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backup_destination_uses_only_its_own_credentials() -> Result<()> {
    // 1.
    let bucket = stopped_cluster().await?;
    let src = TempDir::new("src")?;
    let fs = LocalFileSystem::new_with_prefix(&src.0)?;
    let n = copy_all(&**bucket, &fs, "").await?;
    ensure!(n > 0, "empty source cluster");
    let before = snapshot(&src.0).await?;
    let src_dir = src.0.to_string_lossy().into_owned();
    // 2.
    let tigris = FakeS3::spawn("cell", SRC_KEY).await?;
    let r2 = FakeS3::spawn("bk", DST_KEY).await?;
    let ambient = [
        ("AWS_ENDPOINT", tigris.url.as_str()),
        ("AWS_ACCESS_KEY_ID", SRC_KEY),
        ("AWS_SECRET_ACCESS_KEY", SRC_SECRET),
        ("AWS_REGION", "iad"),
        ("AWS_ALLOW_HTTP", "true"),
        ("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "false"),
    ];
    let full = |dst_key: &str, with_secret: bool| {
        let mut e = env(&ambient);
        e.extend(env(&[
            ("CAIRN_BACKUP_AWS_ENDPOINT", r2.url.as_str()),
            ("CAIRN_BACKUP_AWS_ACCESS_KEY_ID", dst_key),
            ("CAIRN_BACKUP_AWS_REGION", "auto"),
            ("CAIRN_BACKUP_AWS_ALLOW_HTTP", "true"),
            ("CAIRN_BACKUP_AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "false"),
        ]));
        if with_secret {
            e.push((
                "CAIRN_BACKUP_AWS_SECRET_ACCESS_KEY".into(),
                DST_SECRET.into(),
            ));
        }
        e
    };
    let secrets = [SRC_SECRET, DST_SECRET];
    let base = ["--data-dir", src_dir.as_str(), "--who", "e4-test"];
    let with = |rest: &[&'static str]| -> Vec<&str> {
        let mut v = base.to_vec();
        v.extend(["--backup-env-prefix", PREFIX]);
        v.extend(rest);
        v
    };
    let backup = with(&["backup", "--to", "s3://bk/daily"]);

    // 3.
    let r = ctl(&full(DST_KEY, false), &backup).await?;
    no_secret(&r, &secrets)?;
    ensure!(
        r.code == 2,
        "a missing target secret must be code 2: {} {}",
        r.code,
        r.stderr
    );
    ensure!(
        r.stderr.contains("CAIRN_BACKUP_AWS_SECRET_ACCESS_KEY"),
        "the error must name the missing variable: {}",
        r.stderr
    );
    ensure!(
        tigris.log().is_empty() && r2.log().is_empty(),
        "configuration missing but requests sent: tigris {:?} r2 {:?}",
        tigris.log(),
        r2.log()
    );
    ensure!(snapshot(&src.0).await? == before, "source changed");

    // 4.
    let r = ctl(&full("wrong-key", true), &backup).await?;
    no_secret(&r, &secrets)?;
    ensure!(r.code != 0, "wrong target key but code 0: {}", r.stdout);
    ensure!(!r.stderr.trim().is_empty(), "empty error");
    ensure!(r2.keys().is_empty(), "target has objects: {:?}", r2.keys());
    ensure!(
        !r2.log().is_empty() && r2.log().iter().all(|(_, _, k)| k == "wrong-key"),
        "{:?}",
        r2.log()
    );
    ensure!(
        tigris.log().is_empty(),
        "target fell back to AWS_*: {:?}",
        tigris.log()
    );
    ensure!(
        snapshot(&src.0).await? == before,
        "wrong target key but the source changed"
    );

    // 5.
    let r2_seen = r2.log().len();
    let r = ctl(&full(DST_KEY, true), &backup).await?;
    no_secret(&r, &secrets)?;
    ensure!(r.code == 0, "backup: {} {} {}", r.code, r.stdout, r.stderr);
    let id = r
        .stdout
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| anyhow!("could not read the id: {}", r.stdout))?
        .to_string();
    ensure!(
        r2.keys().contains(&format!("daily/{id}/BACKUP")),
        "no backup manifest: {:?}",
        r2.keys()
    );
    ensure!(
        r2.log()[r2_seen..].iter().all(|(_, _, k)| k == DST_KEY),
        "a request to the target lacks the target key: {:?}",
        &r2.log()[r2_seen..]
    );
    ensure!(
        tigris.log().is_empty(),
        "Tigris got requests: {:?}",
        tigris.log()
    );
    let r = ctl(
        &full(DST_KEY, true),
        &with(&["backup", "list", "--to", "s3://bk/daily"]),
    )
    .await?;
    ensure!(
        r.code == 0 && r.stdout.contains(&id) && r.stdout.contains("complete"),
        "backup list: {} {} {}",
        r.code,
        r.stdout,
        r.stderr
    );
    let r = ctl(
        &full(DST_KEY, true),
        &with(&["backup", "prune", "--to", "s3://bk/daily"]),
    )
    .await?;
    ensure!(
        r.code == 0 && r.stdout.contains("DRY RUN"),
        "prune: {} {}",
        r.stdout,
        r.stderr
    );
    ensure!(
        r2.log().iter().skip(r2_seen).all(|(_, _, k)| k == DST_KEY) && tigris.log().is_empty(),
        "list/prune used the wrong credentials"
    );

    // 6.
    let r2_seen = r2.log().len();
    let mut plain = base.to_vec();
    plain.extend(["backup", "--to", "s3://cell/ctrl"]);
    let r = ctl(&full(DST_KEY, true), &plain).await?;
    ensure!(
        r.code == 0,
        "without the flag: {} {} {}",
        r.code,
        r.stdout,
        r.stderr
    );
    ensure!(
        !tigris.log().is_empty() && tigris.log().iter().all(|(_, _, k)| k == SRC_KEY),
        "without the flag the target uses AWS_*: {:?}",
        tigris.log()
    );
    ensure!(r2.log().len() == r2_seen, "no flag, yet R2 got requests");

    // 7.
    let dst = TempDir::new("restore")?;
    let dst_dir = dst.0.to_string_lossy().into_owned();
    let r = ctl(
        &full(DST_KEY, true),
        &[
            "--data-dir",
            &dst_dir,
            "--who",
            "e4-test",
            "--backup-env-prefix",
            PREFIX,
            "restore",
            "--from",
            "s3://bk/daily",
        ],
    )
    .await?;
    no_secret(&r, &secrets)?;
    ensure!(r.code == 0, "restore: {} {} {}", r.code, r.stdout, r.stderr);
    ensure!(r.stdout.contains(&id), "{}", r.stdout);
    let r = ctl(&env(&[]), &["--data-dir", &dst_dir, "verify"]).await?;
    ensure!(
        r.code == 0,
        "verify after restore: {} {} {}",
        r.code,
        r.stdout,
        r.stderr
    );

    // 8.
    let r = ctl(&full(DST_KEY, true), &with(&["table", "show"])).await?;
    ensure!(r.code == 2, "flag on table show: {} {}", r.code, r.stderr);
    Ok(())
}

// ───────────────────────────────────────────── real S3 (SeaweedFS)

fn need(var: &str) -> Result<String> {
    std::env::var(var).map_err(|_| anyhow!("missing {var}"))
}

/// `CAIRN_TEST_S3_*`, falling back to the ambient `AWS_*` (like `s3_contract_env` in `check.sh`).
fn need_or(var: &str, fallback: &str) -> Result<String> {
    need(var).or_else(|_| need(fallback))
}

/// **Two credential sets on real S3, real SigV4 signatures.** Runs in the "real S3"
/// row of `check.sh` (`CAIRN_TEST_S3_*`, SeaweedFS `localhost:18333`).
/// The target comes from `CAIRN_TEST_S3_BACKUP_{URI,ENDPOINT,KEY,SECRET,REGION}` —
/// absent, the same store as the source (one identity) with another prefix; point it
/// at a second SeaweedFS with another identity to get two providers, two key sets.
///
/// Order:
/// 1. A real cluster (2 ranges) stopped, copied up to `<CAIRN_TEST_S3_URI>/e4-<tag>/src`.
/// 2. A wrong target secret (right key id, broken signature) ⇒ code ≠ 0; the source
///    (paths, etags, sizes) unchanged; the target empty.
/// 3. Correct ⇒ `backup` code 0.
/// 4. Delete every source object; `restore --from` the target (same flags) into the
///    source URI itself ⇒ code 0; `verify` clean.
///
/// Must see: as above; both prefixes cleaned up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs real S3: CAIRN_TEST_S3_URI (+ CAIRN_TEST_S3_{ENDPOINT,KEY,SECRET,REGION})"]
async fn backup_across_two_credentials_on_real_s3() -> Result<()> {
    let base = need("CAIRN_TEST_S3_URI")?;
    let base = base.trim_end_matches('/');
    let region =
        need_or("CAIRN_TEST_S3_REGION", "AWS_REGION").unwrap_or_else(|_| "us-east-1".into());
    let endpoint = need_or("CAIRN_TEST_S3_ENDPOINT", "AWS_ENDPOINT")
        .unwrap_or_else(|_| format!("https://s3.{region}.amazonaws.com"));
    let key = need_or("CAIRN_TEST_S3_KEY", "AWS_ACCESS_KEY_ID")?;
    let secret = need_or("CAIRN_TEST_S3_SECRET", "AWS_SECRET_ACCESS_KEY")?;
    let tag = format!(
        "e4-{}-{}",
        std::process::id(),
        cairn_store::entropy::now_ms()
    );
    let src_uri = format!("{base}/{tag}/src");
    let b_endpoint = std::env::var("CAIRN_TEST_S3_BACKUP_ENDPOINT").unwrap_or(endpoint.clone());
    let b_key = std::env::var("CAIRN_TEST_S3_BACKUP_KEY").unwrap_or(key.clone());
    let b_secret = std::env::var("CAIRN_TEST_S3_BACKUP_SECRET").unwrap_or(secret.clone());
    let b_region = std::env::var("CAIRN_TEST_S3_BACKUP_REGION").unwrap_or(region.clone());
    let b_base = std::env::var("CAIRN_TEST_S3_BACKUP_URI").unwrap_or(base.to_string());
    let dst_uri = format!("{}/{tag}/bk", b_base.trim_end_matches('/'));

    let client = |uri: &str,
                  ep: &str,
                  k: &str,
                  s: &str,
                  r: &str|
     -> Result<(Arc<dyn ObjectStore>, String)> {
        let rest = uri
            .strip_prefix("s3://")
            .ok_or_else(|| anyhow!("not s3://: {uri}"))?;
        let (b, p) = rest.split_once('/').unwrap_or((rest, ""));
        let store = AmazonS3Builder::new()
            .with_bucket_name(b)
            .with_endpoint(ep)
            .with_access_key_id(k)
            .with_secret_access_key(s)
            .with_region(r)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .build()?;
        Ok((Arc::new(store), p.to_string()))
    };
    let (src_store, src_prefix) = client(&src_uri, &endpoint, &key, &secret, &region)?;
    let (dst_store, dst_prefix) = client(&dst_uri, &b_endpoint, &b_key, &b_secret, &b_region)?;
    let listing = |s: Arc<dyn ObjectStore>, p: String| async move {
        let metas: Vec<_> = s.list(Some(&ObjPath::from(p))).try_collect().await?;
        anyhow::Ok(
            metas
                .into_iter()
                .map(|m| (m.location.to_string(), m.e_tag, m.size))
                .collect::<Vec<_>>(),
        )
    };
    let wipe = |s: Arc<dyn ObjectStore>, p: String| async move {
        let metas: Vec<_> = s.list(Some(&ObjPath::from(p))).try_collect().await?;
        for m in metas {
            s.delete(&m.location).await?;
        }
        anyhow::Ok(())
    };

    let r = async {
        // 1.
        let bucket = stopped_cluster().await?;
        let n = copy_all(&**bucket, &*src_store, &src_prefix).await?;
        ensure!(n > 0, "empty source cluster");
        let before = listing(src_store.clone(), src_prefix.clone()).await?;
        let ambient = env(&[
            ("AWS_ENDPOINT", &endpoint),
            ("AWS_ACCESS_KEY_ID", &key),
            ("AWS_SECRET_ACCESS_KEY", &secret),
            ("AWS_REGION", &region),
            ("AWS_ALLOW_HTTP", "true"),
            ("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "false"),
        ]);
        let with_dst = |s: &str| {
            let mut e = ambient.clone();
            e.extend(env(&[
                ("CAIRN_BACKUP_AWS_ENDPOINT", &b_endpoint),
                ("CAIRN_BACKUP_AWS_ACCESS_KEY_ID", &b_key),
                ("CAIRN_BACKUP_AWS_SECRET_ACCESS_KEY", s),
                ("CAIRN_BACKUP_AWS_REGION", &b_region),
                ("CAIRN_BACKUP_AWS_ALLOW_HTTP", "true"),
                ("CAIRN_BACKUP_AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "false"),
            ]));
            e
        };
        let args = |main: &str, rest: &[&str]| -> Vec<String> {
            let mut v: Vec<String> = [
                "--main-store",
                main,
                "--who",
                "e4-test",
                "--backup-env-prefix",
                PREFIX,
            ]
            .iter()
            .map(|s| s.to_string())
            .collect();
            v.extend(rest.iter().map(|s| s.to_string()));
            v
        };
        let run = |e: Vec<(String, String)>, a: Vec<String>| async move {
            let a: Vec<&str> = a.iter().map(String::as_str).collect();
            ctl(&e, &a).await
        };
        let secrets = [secret.as_str(), b_secret.as_str()];
        // 2.
        let wrong = format!("{b_secret}x");
        let r = run(
            with_dst(&wrong),
            args(&src_uri, &["backup", "--to", &dst_uri]),
        )
        .await?;
        no_secret(&r, &secrets)?;
        ensure!(r.code != 0, "wrong target secret but code 0: {}", r.stdout);
        ensure!(
            listing(src_store.clone(), src_prefix.clone()).await? == before,
            "wrong target key but the source changed"
        );
        ensure!(
            listing(dst_store.clone(), dst_prefix.clone())
                .await?
                .is_empty(),
            "wrong target key but the target has objects"
        );
        // 3.
        let r = run(
            with_dst(&b_secret),
            args(&src_uri, &["backup", "--to", &dst_uri]),
        )
        .await?;
        no_secret(&r, &secrets)?;
        ensure!(r.code == 0, "backup: {} {} {}", r.code, r.stdout, r.stderr);
        // 4.
        wipe(src_store.clone(), src_prefix.clone()).await?;
        let r = run(
            with_dst(&b_secret),
            args(&src_uri, &["restore", "--from", &dst_uri]),
        )
        .await?;
        no_secret(&r, &secrets)?;
        ensure!(r.code == 0, "restore: {} {} {}", r.code, r.stdout, r.stderr);
        let r = ctl(&ambient, &["--main-store", &src_uri, "verify"]).await?;
        ensure!(
            r.code == 0,
            "verify after restore: {} {} {}",
            r.code,
            r.stdout,
            r.stderr
        );
        Ok(())
    }
    .await;
    let c1 = wipe(src_store.clone(), src_prefix.clone()).await;
    let c2 = wipe(dst_store.clone(), dst_prefix.clone()).await;
    r?;
    c1?;
    c2
}
