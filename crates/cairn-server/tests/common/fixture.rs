// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Format-10 sample buckets (`tests/fixtures/`) for tests outside `format_fixture.rs`
//! — the original WAL migration row also runs in `tests/failpoints.rs`.

use {
    anyhow::{ensure, Result},
    reqwest::{Client, StatusCode},
    serde_json::Value,
    std::path::{Path, PathBuf},
};

/// The admin key that the generator of both sample buckets writes into `auth/keys`.
pub const ADMIN_KEY: &str = "cf_fixture_bootstrap_key_do_not_use_0001";

/// The directory of sample bucket `name` (`format10`, `format10-cluster`).
pub fn dir(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

/// `EXPECTED.json` of the sample bucket at `dir`.
pub fn expected(dir: &Path) -> Result<Value> {
    Ok(serde_json::from_slice(&std::fs::read(
        dir.join("EXPECTED.json"),
    )?)?)
}

/// Load a sample bucket directory into a new `InMemory` (without `EXPECTED.json`).
pub async fn load(dir: &Path) -> Result<std::sync::Arc<slatedb::object_store::memory::InMemory>> {
    let bucket = std::sync::Arc::new(slatedb::object_store::memory::InMemory::new());
    fill(&bucket, dir).await?;
    Ok(bucket)
}

/// A sample bucket in a [`super::Bucket`] — `verify` runs on cleanup.
pub async fn bucket(dir: &Path) -> Result<super::Bucket> {
    let b = super::bucket();
    fill(&b, dir).await?;
    Ok(b)
}

async fn fill(bucket: &slatedb::object_store::memory::InMemory, dir: &Path) -> Result<()> {
    use slatedb::object_store::{path::Path as ObjPath, ObjectStoreExt, PutPayload};
    fn walk(root: &Path, dir: &Path, out: &mut Vec<(String, PathBuf)>) -> Result<()> {
        for e in std::fs::read_dir(dir)? {
            let e = e?;
            if e.file_type()?.is_dir() {
                walk(root, &e.path(), out)?;
            } else {
                let rel = e
                    .path()
                    .strip_prefix(root)?
                    .to_string_lossy()
                    .replace('\\', "/");
                out.push((rel, e.path()));
            }
        }
        Ok(())
    }
    let mut files = Vec::new();
    walk(dir, dir, &mut files)?;
    for (rel, path) in files {
        if rel == "EXPECTED.json" {
            continue;
        }
        bucket
            .put(&ObjPath::from(rel), PutPayload::from(std::fs::read(path)?))
            .await?;
    }
    Ok(())
}

/// Call `req` with key `key`; the response body is JSON if it can be read as such.
pub async fn call(key: &str, req: reqwest::RequestBuilder) -> Result<(StatusCode, Value)> {
    let r = req.bearer_auth(key).send().await?;
    let code = r.status();
    let text = r.text().await?;
    Ok((
        code,
        serde_json::from_str(&text).unwrap_or(Value::String(text)),
    ))
}

/// Read back through `url` exactly what the single-process sample bucket's generator
/// wrote (`EXPECTED.json`): `acme`'s deployment, and the status, output and journal
/// length of every invocation.
pub async fn read_single(url: &str, expected: &Value) -> Result<()> {
    let http = Client::new();
    let (code, deps) = call(
        ADMIN_KEY,
        http.get(format!("{url}/deployments?tenant=acme")),
    )
    .await?;
    ensure!(code == StatusCode::OK, "deployments: {code} {deps}");
    ensure!(
        deps.as_array().is_some_and(|a| !a.is_empty()),
        "acme's deployment lost: {deps}"
    );
    for inv in expected["invocations"].as_array().into_iter().flatten() {
        let id = inv["id"].as_str().unwrap_or_default();
        let (code, st) = call(ADMIN_KEY, http.get(format!("{url}/invocations/{id}"))).await?;
        ensure!(code == StatusCode::OK, "invocation {id}: {code} {st}");
        times_present(id, &st)?;
        ensure!(
            st["status"] == inv["status"],
            "invocation {id}: {st} ≠ {inv}"
        );
        if let Some(out) = inv.get("output") {
            ensure!(&st["output"] == out, "output of {id}: {st} ≠ {inv}");
        }
        let (code, j) = call(
            ADMIN_KEY,
            http.get(format!("{url}/invocations/{id}/journal")),
        )
        .await?;
        ensure!(code == StatusCode::OK, "journal {id}: {code} {j}");
        ensure!(
            j.as_array().map(Vec::len) == inv["journal"].as_u64().map(|n| n as usize),
            "journal of {id} has a different length: {j}"
        );
    }
    Ok(())
}

/// Auth of a node opening a sample bucket: the generator's admin key, a fixed cluster secret.
pub fn auth() -> cairn_server::auth::AuthConfig {
    cairn_server::auth::AuthConfig {
        required: true,
        admin_key: Some(ADMIN_KEY.into()),
        cluster_secret: Some("cf_fixture_cluster_secret_at_least_32b".into()),
        metrics_key: None,
        admin_key_sha256: None,
        provision_key_sha256: None,
    }
}

/// **Invocations created before timestamps existed have them too**: the timestamp
/// is the `create_ts` SlateDB has always recorded, so the format-10 sample bucket
/// returns `createdAt`/`modifiedAt` (and `endedAt` once finished) without any migration.
pub fn times_present(id: &str, st: &Value) -> Result<()> {
    let created = st["createdAt"]
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("sample invocation {id} lacks createdAt: {st}"))?;
    let modified = st["modifiedAt"]
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("invocation {id} lacks modifiedAt: {st}"))?;
    ensure!(
        created > 1_600_000_000_000 && modified >= created && modified <= super::wall_ms(),
        "invocation {id}: unexpected timestamps {st}"
    );
    let ended = matches!(st["status"].as_str(), Some("completed" | "failed"));
    ensure!(
        st.get("endedAt").and_then(Value::as_u64) == ended.then_some(modified),
        "invocation {id}: endedAt wrong {st}"
    );
    Ok(())
}
