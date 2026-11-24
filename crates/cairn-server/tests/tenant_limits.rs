// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Per-tenant limits for a shared cell.
//!
//! Each limit is a field of the `tenants` book, **absent = no limit**
//! (self-hosting does not change):
//!
//! | Field | Applied at | Hitting the limit |
//! |---|---|---|
//! | `retention-ms=` | engine, at the end (handler's `journalRetention` > tenant > `--retention-ms`) | the invocation is purged sooner |
//! | `max-payload=` | ingress (`/invoke`, `/send` body) and SDK frames (`Output`, `SetState`) — after sealing only the inline part counts (`SetState`/`GetEagerState` ≥ `seal_bytes` go to segments) | `413 payload_too_large`; the invocation `failed` with code 413 |
//! | `max-journal=` | engine, when appending an entry | the invocation `failed` with code 578 (`journal_too_long`) |
//! | `deploy-rate=N/S` | `POST /deployments` | `429 deploy_rate_limited` + `Retry-After` |
//!
//! An unknown value ⇒ the whole book cannot be read ⇒ the node **keeps the old
//! book** (the limits in force stay) and raises the gauge
//! `cairnflow_tenants_book_rejected` (fail safe). `GET /tenant` (Admin) returns
//! the tenant's own limits.

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterminism"
)]

mod common;

use {
    anyhow::{anyhow, ensure, Result},
    bytes::Bytes,
    cairn_server::{auth::AuthConfig, discovery::Contract},
    common::{
        fake_sdk::{FakeSdk, Step, HOLD_SIGNAL},
        within, ClusterNode, TEST_DEADLINE,
    },
    reqwest::{Client, StatusCode},
    serde_json::{json, Value},
    slatedb::object_store::{path::Path, ObjectStoreExt, PutPayload},
    std::time::Duration,
};

const ADMIN_KEY: &str = "cf_test_tenant_limits_bootstrap_key_0123";
const TTL: Duration = Duration::from_secs(2);

async fn authed(bucket: &common::Bucket, id: &str) -> Result<ClusterNode> {
    common::start_cluster_node_tweaked(bucket, id, 2, TTL, |cfg| {
        cfg.auth = AuthConfig {
            required: true,
            admin_key: Some(ADMIN_KEY.into()),
            cluster_secret: None,
            metrics_key: None,
            admin_key_sha256: None,
            provision_key_sha256: None,
        };
    })
    .await
}

/// A client bound to one tenant's key.
#[derive(Clone)]
struct As {
    url: String,
    key: String,
    http: Client,
}

impl As {
    fn admin(url: &str) -> Self {
        Self {
            url: url.into(),
            key: ADMIN_KEY.into(),
            http: Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
        }
    }

    /// A `role` key for `tenant`, created with the bootstrap key. A cluster node
    /// returns `503` until it takes its lease: bounded retries.
    async fn tenant(url: &str, tenant: &str, role: &str) -> Result<Self> {
        let admin = Self::admin(url);
        let t0 = std::time::Instant::now();
        loop {
            let (code, v) = admin
                .send(
                    admin
                        .http
                        .post(format!("{url}/admin/keys"))
                        .json(&json!({ "tenant": tenant, "role": role })),
                )
                .await?;
            if code == StatusCode::OK {
                return Ok(Self {
                    key: v["secret"].as_str().unwrap_or_default().to_string(),
                    ..admin
                });
            }
            ensure!(
                t0.elapsed() < TEST_DEADLINE,
                "create key {tenant}: {code} {v}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn send(&self, req: reqwest::RequestBuilder) -> Result<(StatusCode, Value)> {
        let r = req.bearer_auth(&self.key).send().await?;
        let code = r.status();
        let retry_after = r
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let text = r.text().await?;
        let mut v = serde_json::from_str(&text).unwrap_or(Value::String(text));
        if let (Some(ra), Value::Object(m)) = (retry_after, &mut v) {
            m.insert("_retryAfter".into(), Value::String(ra));
        }
        Ok((code, v))
    }

    async fn register(&self, sdk: &str) -> Result<(StatusCode, Value)> {
        self.send(
            self.http
                .post(format!("{}/deployments", self.url))
                .json(&json!({ "uri": sdk })),
        )
        .await
    }

    async fn invoke(
        &self,
        path: &str,
        body: impl Into<reqwest::Body>,
    ) -> Result<(StatusCode, Value)> {
        self.send(
            self.http
                .post(format!("{}/invoke/{path}", self.url))
                .body(body),
        )
        .await
    }

    async fn get(&self, path: &str) -> Result<(StatusCode, Value)> {
        self.send(self.http.get(format!("{}{path}", self.url)))
            .await
    }

    /// `PUT /admin/tenants/{tenant}` with this key.
    async fn put_tenant(&self, tenant: &str, body: Value) -> Result<(StatusCode, Value)> {
        self.send(
            self.http
                .put(format!("{}/admin/tenants/{tenant}", self.url))
                .json(&body),
        )
        .await
    }

    async fn metrics(&self) -> Result<String> {
        Ok(self
            .http
            .get(format!("{}/metrics", self.url))
            .bearer_auth(&self.key)
            .send()
            .await?
            .text()
            .await?)
    }
}

async fn set_limits(admin: &As, tenant: &str, body: Value) -> Result<()> {
    let (code, v) = admin.put_tenant(tenant, body).await?;
    ensure!(code == StatusCode::OK, "set limits {tenant}: {code} {v}");
    Ok(())
}

fn blob(n: usize) -> String {
    format!("\"{}\"", "x".repeat(n.saturating_sub(2)))
}

/// Wait until `who`'s `GET /invocations/{id}` shows an ended status.
async fn settled(who: &As, id: &str) -> Result<Value> {
    let t0 = std::time::Instant::now();
    loop {
        let (code, v) = who.get(&format!("/invocations/{id}")).await?;
        if code == StatusCode::OK && matches!(v["status"].as_str(), Some("completed" | "failed")) {
            return Ok(v);
        }
        ensure!(
            t0.elapsed() < TEST_DEADLINE,
            "{id} not ended yet: {code} {v}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn id_of(v: &Value) -> Result<String> {
    v["id"]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow!("no id: {v}"))
}

/// Self-hosting: no field in the book ⇒ no limit at all, `GET /tenant` returns
/// every limit as `null`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_fields_nothing_is_limited() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service(
            "Echo",
            "big",
            vec![Step::Output(Bytes::from(vec![b'7'; 64 * 1024]))],
        )
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "self-hosting unchanged", async {
        let acme = As::tenant(&url, "acme", "admin").await?;
        for _ in 0..5 {
            let (code, v) = acme.register(&sdk.url).await?;
            ensure!(code == StatusCode::OK, "register: {code} {v}");
        }
        let (code, v) = acme.invoke("Echo/big", blob(1024 * 1024)).await?;
        ensure!(code == StatusCode::OK, "1 MiB body: {code} {v}");
        let (code, v) = acme.get("/tenant").await?;
        ensure!(code == StatusCode::OK, "GET /tenant: {code} {v}");
        ensure!(v["tenant"] == "acme", "{v}");
        for k in ["retentionMs", "maxPayload", "maxJournal", "deployRate"] {
            ensure!(
                v["limits"][k].is_null(),
                "{k} must be null when the book has no field: {v}"
            );
        }
        Ok(())
    })
    .await
}

/// `max-payload=` at ingress: an `/invoke` or `/send` body over the limit ⇒
/// `413` `payload_too_large`; under the limit it passes; other tenants are not
/// affected.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_payload_at_ingress() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("Echo", "hi", vec![Step::output("ok")])
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "max-payload at ingress", async {
        let acme = As::tenant(&url, "acme", "admin").await?;
        let other = As::tenant(&url, "other", "admin").await?;
        acme.register(&sdk.url).await?;
        other.register(&sdk.url).await?;
        set_limits(&As::admin(&url), "acme", json!({ "maxPayload": 1024 })).await?;

        let (code, v) = acme.invoke("Echo/hi", blob(1000)).await?;
        ensure!(code == StatusCode::OK, "under the limit: {code} {v}");
        for path in ["Echo/hi", "Echo/hi/send"] {
            let (code, v) = acme.invoke(path, blob(2048)).await?;
            ensure!(
                code == StatusCode::PAYLOAD_TOO_LARGE && v["code"] == "payload_too_large",
                "{path} over the limit: {code} {v}"
            );
        }
        let (code, v) = other.invoke("Echo/hi", blob(2048)).await?;
        ensure!(code == StatusCode::OK, "other tenant: {code} {v}");
        Ok(())
    })
    .await
}

/// `max-payload=` on SDK frames: an `Output` or `SetState` over the limit ⇒ the
/// invocation `failed` with code 413, message `payload_too_large`; another
/// tenant running the same handler completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_payload_on_sdk_frames() -> Result<()> {
    let big = Bytes::from(vec![b'7'; 4096]);
    let sdk = FakeSdk::builder()
        .service("Big", "out", vec![Step::Output(big.clone())])
        .object(
            "BigState",
            "set",
            vec![
                Step::SetState {
                    key: "k".into(),
                    value: big.clone(),
                },
                Step::output("ok"),
            ],
        )
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "max-payload on SDK frames", async {
        let acme = As::tenant(&url, "acme", "admin").await?;
        let other = As::tenant(&url, "other", "admin").await?;
        acme.register(&sdk.url).await?;
        other.register(&sdk.url).await?;
        set_limits(&As::admin(&url), "acme", json!({ "maxPayload": 1024 })).await?;

        for path in ["Big/out/send", "BigState/set/send?key=a"] {
            let (code, v) = acme.invoke(path, "{}").await?;
            ensure!(
                code == StatusCode::ACCEPTED || code == StatusCode::OK,
                "{path}: {code} {v}"
            );
            let s = settled(&acme, &id_of(&v)?).await?;
            ensure!(
                s["status"] == "failed"
                    && s["code"] == 413
                    && s["message"]
                        .as_str()
                        .unwrap_or("")
                        .contains("payload_too_large"),
                "acme's {path}: {s}"
            );
            let (_, v) = other.invoke(path, "{}").await?;
            let s = settled(&other, &id_of(&v)?).await?;
            ensure!(s["status"] == "completed", "{path}, other tenant: {s}");
        }
        Ok(())
    })
    .await
}

/// `max-payload=` covers **every** SDK frame the runtime writes into the
/// journal, not only `Output`/`SetState`: a `ctx.run` result
/// (`ProposeRunCompletion`) and a `continueAsNew` new input over the limit ⇒
/// the invocation `failed` with code 413 `payload_too_large` (the message points
/// at `ctx.artifact`); another tenant runs to the end. A `ctx.run` result used
/// to slip through up to the 32 MiB RR body limit — blobs and conversation
/// history went into the journal and were resent on every REQUEST_RESPONSE turn.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_payload_covers_run_results() -> Result<()> {
    let big = Bytes::from(vec![b'7'; 4096]);
    let sdk = FakeSdk::builder()
        .service(
            "BigRun",
            "go",
            vec![Step::Run(big.clone()), Step::output("ok")],
        )
        .service(
            "BigNew",
            "go",
            vec![
                Step::ContinueAsNewOnce(
                    std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
                    big.clone(),
                ),
                Step::output("ok"),
            ],
        )
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "max-payload on ctx.run results", async {
        let acme = As::tenant(&url, "acme", "admin").await?;
        let other = As::tenant(&url, "other", "admin").await?;
        acme.register(&sdk.url).await?;
        other.register(&sdk.url).await?;
        set_limits(&As::admin(&url), "acme", json!({ "maxPayload": 1024 })).await?;
        for path in ["BigRun/go/send", "BigNew/go/send"] {
            let (code, v) = acme.invoke(path, "{}").await?;
            ensure!(
                code == StatusCode::ACCEPTED || code == StatusCode::OK,
                "{path}: {code} {v}"
            );
            let s = settled(&acme, &id_of(&v)?).await?;
            let msg = s["message"].as_str().unwrap_or("");
            ensure!(
                s["status"] == "failed"
                    && s["code"] == 413
                    && msg.contains("payload_too_large")
                    && msg.contains("ctx.artifact"),
                "acme's {path}: {s}"
            );
        }
        let (_, v) = other.invoke("BigRun/go/send", "{}").await?;
        let s = settled(&other, &id_of(&v)?).await?;
        ensure!(s["status"] == "completed", "other tenant: {s}");
        Ok(())
    })
    .await
}

/// Chat history size of the two large-history tests: bigger than the Beta's
/// 1 MiB limit, smaller than the 16 MiB frame limit.
const HISTORY: usize = 3 << 19;

/// **A history bigger than `max-payload=` used to break `send`.** The shape of
/// `chatObject`: the `send` turn writes the whole history into one state key
/// (`ctx.set("messages", …)`). A tenant with `max-payload=1 MiB` (Beta), a
/// 1.5 MiB history ⇒ before segmented values the invocation `failed` 413
/// `payload_too_large`.
///
/// Must see (the runtime seals values at the node, the limit only applies to
/// the inline part): the turn is `completed`, `sys_state` reports a `value_len`
/// of exactly the logical size.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn history_over_max_payload_does_not_fail_send() -> Result<()> {
    let sdk = FakeSdk::builder()
        .object(
            "Chat",
            "send",
            vec![
                Step::SetState {
                    key: "messages".into(),
                    value: Bytes::from(blob(HISTORY)),
                },
                Step::output("ok"),
            ],
        )
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "history > max-payload on send", async {
        let acme = As::tenant(&url, "acme", "admin").await?;
        acme.register(&sdk.url).await?;
        set_limits(&As::admin(&url), "acme", json!({ "maxPayload": 1 << 20 })).await?;
        let (code, v) = acme.invoke("Chat/send/send?key=c1", "{}").await?;
        ensure!(
            code == StatusCode::ACCEPTED || code == StatusCode::OK,
            "send: {code} {v}"
        );
        let s = settled(&acme, &id_of(&v)?).await?;
        ensure!(s["status"] == "completed", "send, 1.5 MiB history: {s}");
        let sql = json!({
            "sql": "SELECT value_len FROM sys_state \
                    WHERE service = 'Chat' AND key = 'c1' AND state_key = 'messages'"
        });
        let (code, v) = acme
            .send(acme.http.post(format!("{url}/query")).json(&sql))
            .await?;
        let len = v["rows"][0][0]
            .as_u64()
            .or_else(|| v["rows"][0][0].as_str().and_then(|s| s.parse().ok()));
        ensure!(
            code == StatusCode::OK && len == Some(HISTORY as u64),
            "sys_state after send: {code} {v}"
        );
        Ok(())
    })
    .await
}

/// **`messages` used to fail when the history > `max-payload=`.** The SHARED
/// `messages` handler of `chatObject` (page reload) returns the whole history as
/// output — a 1.5 MiB `Output` frame on a 1 MiB tenant ⇒ before segmented
/// outputs it `failed` 413, and a reloaded page did not see the conversation.
///
/// Must see (segmented output, assembled for the caller): the synchronous call
/// returns `200` with the whole history.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn history_over_max_payload_is_readable_by_messages() -> Result<()> {
    let history = blob(HISTORY);
    let sdk = FakeSdk::builder()
        .object(
            "Chat",
            "messages",
            vec![Step::Output(Bytes::from(history.clone()))],
        )
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "messages, history > max-payload", async {
        let acme = As::tenant(&url, "acme", "admin").await?;
        acme.register(&sdk.url).await?;
        set_limits(&As::admin(&url), "acme", json!({ "maxPayload": 1 << 20 })).await?;
        let (code, v) = acme.invoke("Chat/messages?key=c1", "{}").await?;
        ensure!(
            code == StatusCode::OK && v["output"].as_str().map(str::len) == Some(HISTORY),
            "messages: {code} {}",
            v.to_string().chars().take(300).collect::<String>()
        );
        Ok(())
    })
    .await
}

/// **A `ctx.run` result bigger than `max-payload=`.** The shape of
/// `cairnTools`: a tool returns 2 MiB on a 1 MiB tenant. Before segmented
/// values the 2 MiB `ProposeRunCompletion` frame exceeded the limit ⇒ the
/// invocation `failed` 413.
///
/// Must see (the shell seals `ProposeRunCompletion`, the journal keeps a ref,
/// the `RunCompletion` sent down to the SDK is assembled — a wire v6 SDK on a
/// node with no read path configured; an SDK before v6 receiving 2 MiB > the
/// limit gets 413): the next turn reads the whole result back and returns it as
/// output — `completed` with all 2 MiB.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_result_over_max_payload_is_sealed() -> Result<()> {
    const RESULT: usize = 2 << 20;
    let sdk = FakeSdk::builder()
        .wire(6)
        .object(
            "Tool",
            "fetch",
            vec![Step::Run(Bytes::from(blob(RESULT))), Step::EchoCallResult],
        )
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "2 MiB ctx.run on a 1 MiB tenant", async {
        let acme = As::tenant(&url, "acme", "admin").await?;
        acme.register(&sdk.url).await?;
        set_limits(&As::admin(&url), "acme", json!({ "maxPayload": 1 << 20 })).await?;
        let (code, v) = acme.invoke("Tool/fetch/send?key=t1", "{}").await?;
        ensure!(
            code == StatusCode::ACCEPTED || code == StatusCode::OK,
            "fetch/send: {code} {v}"
        );
        let s = settled(&acme, &id_of(&v)?).await?;
        ensure!(
            s["status"] == "completed" && s["output"].as_str().map(str::len) == Some(RESULT),
            "fetch: {}",
            s.to_string().chars().take(300).collect::<String>()
        );
        Ok(())
    })
    .await
}

/// A one-range node (every object in the same range, the same segment cache) with API keys.
async fn authed_one_range(bucket: &common::Bucket) -> Result<ClusterNode> {
    common::start_cluster_node_tweaked(bucket, "n1", 1, TTL, |cfg| {
        cfg.auth = AuthConfig {
            required: true,
            admin_key: Some(ADMIN_KEY.into()),
            cluster_secret: None,
            metrics_key: None,
            admin_key_sha256: None,
            provision_key_sha256: None,
        };
    })
    .await
}

/// An SDK with `Chat/seal`: `ctx.set("m", 600 KiB)` ⇒ the shell seals one segment.
async fn sealer() -> Result<FakeSdk> {
    FakeSdk::builder()
        .object(
            "Chat",
            "seal",
            vec![
                Step::SetState {
                    key: "m".into(),
                    value: Bytes::from(blob(600 << 10)),
                },
                Step::output("ok"),
            ],
        )
        .spawn()
        .await
}

/// A v6 SDK with `Chat/steal`: `SetState("m")` carrying `refs` (cells) ×3 plus a
/// 300 KiB tail (above the seal threshold) — enough for the merge rule to merge
/// all three refs.
async fn stealer(refs: common::fake_sdk::RefSlot) -> Result<FakeSdk> {
    FakeSdk::builder()
        .wire(6)
        .object(
            "Chat",
            "steal",
            vec![
                Step::SetStateRefs {
                    key: "m".into(),
                    refs,
                    tail: Bytes::from(vec![b'z'; 300 << 10]),
                },
                Step::output("ok"),
            ],
        )
        .spawn()
        .await
}

/// `(sha, len, gen)` of the segment of state `m` of `Chat/{key}`, through
/// `sys_value` with `who`'s key.
async fn segment_of(who: &As, key: &str) -> Result<([u8; 32], u64, u64)> {
    let sql = json!({
        "sql": format!("SELECT sha, gen, len FROM sys_value WHERE service = 'Chat' AND key = '{key}'")
    });
    let (code, v) = who
        .send(who.http.post(format!("{}/query", who.url)).json(&sql))
        .await?;
    ensure!(code == StatusCode::OK, "sys_value: {code} {v}");
    let row = &v["rows"][0];
    let hex = row[0]
        .as_str()
        .ok_or_else(|| anyhow!("sys_value row without a segment: {v}"))?;
    let mut sha = [0u8; 32];
    for (i, b) in sha.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16)?;
    }
    let gen = u64::from_str_radix(row[1].as_str().unwrap_or_default(), 16)?;
    let len = row[2].as_u64().ok_or_else(|| anyhow!("len: {v}"))?;
    Ok((sha, len, gen))
}

/// Number of `sys_state` rows of `Chat/{key}` that `who` sees.
async fn state_rows(who: &As, key: &str) -> Result<u64> {
    let sql = json!({
        "sql": format!("SELECT count(*) FROM sys_state WHERE service = 'Chat' AND key = '{key}'")
    });
    let (code, v) = who
        .send(who.http.post(format!("{}/query", who.url)).json(&sql))
        .await?;
    ensure!(code == StatusCode::OK, "sys_state: {code} {v}");
    v["rows"][0][0]
        .as_u64()
        .ok_or_else(|| anyhow!("count: {v}"))
}

/// Objects under the bucket's `values/{tenant}/`, with their sizes.
async fn value_objects_of(bucket: &common::Bucket, tenant: &str) -> Result<Vec<(String, u64)>> {
    use {futures::TryStreamExt, slatedb::object_store::ObjectStore as _};
    let listed: Vec<_> = bucket
        .list(Some(&Path::from(format!("values/{tenant}"))))
        .try_collect()
        .await?;
    Ok(listed
        .into_iter()
        .map(|m| (m.location.to_string(), m.size))
        .collect())
}

/// Wait a while, then check that `who`'s `steal` turn on `Chat/{key}` wrote
/// nothing: the invocation is not `completed`, no state row, and under
/// `values/{tenant}/` there is still exactly `before` (the shell did not PUT a
/// merged copy holding someone else's bytes).
async fn steal_refused(
    who: &As,
    bucket: &common::Bucket,
    tenant: &str,
    key: &str,
    id: &str,
    before: &[(String, u64)],
) -> Result<()> {
    tokio::time::sleep(Duration::from_secs(2)).await;
    let (code, v) = who.get(&format!("/invocations/{id}")).await?;
    ensure!(
        code == StatusCode::OK && v["status"] != "completed",
        "someone else's ref must be refused like a dangling ref: {code} {v}"
    );
    ensure!(
        state_rows(who, key).await? == 0,
        "Chat/{key} must not have state copied from someone else's segment"
    );
    let after = value_objects_of(bucket, tenant).await?;
    ensure!(
        after == before,
        "no new object under values/{tenant}/ (a merged copy holding someone else's bytes): {before:?} → {after:?}"
    );
    Ok(())
}

/// **Another tenant's ref cannot be read through a merge** (a security review
/// finding).
///
/// 1. Tenant `other` (`Chat/b1`) seals state `m` of 600 KiB ⇒ segment X, PUT
///    straight into the range's segment cache (one range, shared by every
///    tenant).
/// 2. The test reads X's `(sha, len, gen)` with `other`'s key (`sys_value`).
/// 3. Tenant `acme`, with a hand-built v6 SDK, sends `SetState("m")` with
///    `segments = [X, X, X]` plus a 300 KiB tail — the tail is above the
///    threshold so the shell seals, and the merge rule picks all three refs.
///
/// Old code: the merge GOT X by sha ⇒ a cache hit, copied `other`'s bytes into a
/// new segment under `acme`'s owner, and dropped ref X from the frame ⇒ the
/// engine saw no foreign ref and wrote the state. Must see: `acme`'s turn is
/// refused like a dangling ref (not `completed`, no state row), no object under
/// `values/acme/`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merge_cannot_read_another_tenants_segment() -> Result<()> {
    let refs = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let victim = sealer().await?;
    let thief = stealer(refs.clone()).await?;
    let bucket = common::bucket();
    let node = authed_one_range(&bucket).await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "merge another tenant's ref", async {
        let other = As::tenant(&url, "other", "admin").await?;
        let acme = As::tenant(&url, "acme", "admin").await?;
        other.register(&victim.url).await?;
        acme.register(&thief.url).await?;
        let (code, v) = other.invoke("Chat/seal?key=b1", "{}").await?;
        ensure!(code == StatusCode::OK, "other seal: {code} {v}");
        let x = segment_of(&other, "b1").await?;
        *refs.lock().unwrap() = vec![x, x, x];
        let before = value_objects_of(&bucket, "acme").await?;
        ensure!(before.is_empty(), "acme has no segment yet: {before:?}");
        let (code, v) = acme.invoke("Chat/steal/send?key=a1", "{}").await?;
        ensure!(
            code == StatusCode::ACCEPTED || code == StatusCode::OK,
            "acme steal: {code} {v}"
        );
        steal_refused(&acme, &bucket, "acme", "a1", &id_of(&v)?, &before).await
    })
    .await
}

/// **Same tenant, different owner variant** (a security review finding): like
/// [`merge_cannot_read_another_tenants_segment`], but X is a segment of
/// `Chat/o1` and the turn copying the ref is `Chat/o2`'s — the same tenant
/// `acme`, the same range, a different object. Must see: `o2`'s turn is refused
/// like a dangling ref, no state row, and under `values/acme/` there is still
/// only X.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merge_cannot_read_another_owners_segment() -> Result<()> {
    let refs = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let sdk = FakeSdk::builder()
        .wire(6)
        .object(
            "Chat",
            "seal",
            vec![
                Step::SetState {
                    key: "m".into(),
                    value: Bytes::from(blob(600 << 10)),
                },
                Step::output("ok"),
            ],
        )
        .object(
            "Chat",
            "steal",
            vec![
                Step::SetStateRefs {
                    key: "m".into(),
                    refs: refs.clone(),
                    tail: Bytes::from(vec![b'z'; 300 << 10]),
                },
                Step::output("ok"),
            ],
        )
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node = authed_one_range(&bucket).await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "merge another owner's ref", async {
        let acme = As::tenant(&url, "acme", "admin").await?;
        acme.register(&sdk.url).await?;
        let (code, v) = acme.invoke("Chat/seal?key=o1", "{}").await?;
        ensure!(code == StatusCode::OK, "o1 seal: {code} {v}");
        let x = segment_of(&acme, "o1").await?;
        *refs.lock().unwrap() = vec![x, x, x];
        let before = value_objects_of(&bucket, "acme").await?;
        ensure!(before.len() == 1, "only o1's segment: {before:?}");
        let (code, v) = acme.invoke("Chat/steal/send?key=o2", "{}").await?;
        ensure!(
            code == StatusCode::ACCEPTED || code == StatusCode::OK,
            "o2 steal: {code} {v}"
        );
        steal_refused(&acme, &bucket, "acme", "o2", &id_of(&v)?, &before).await
    })
    .await
}

/// **`max-value-bytes=` bounds the logical value.** A tenant with
/// `max-payload=1 MiB`, `max-value-bytes=1.5 MiB`. An SDK ≤ v5 sends the whole
/// value inline.
///
/// 1. `set` 2 MiB, `Output` 2 MiB, a `ctx.run` result of 2 MiB: over the
///    logical limit ⇒ the shell does not seal; the engine ends the invocation
///    `failed` with code 579, a message starting with `value_bytes_limit` (a
///    more specific limit than `payload_too_large`).
/// 2. `set` 1.25 MiB: under the logical limit, over `max-payload=` ⇒ sealed as
///    in the large-history tests, `completed`.
/// 3. `GET /tenant` returns `maxValueBytes`; an unknown value ⇒ `400`; `null`
///    removes the limit.
///
/// Must see: the first three turns `failed` 579 `value_bytes_limit`; the
/// 1.25 MiB turn `completed`; no `values/` object from the three blocked turns
/// (exactly one segment, from the 1.25 MiB turn).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_value_bytes_fails_large_values() -> Result<()> {
    const BIG: usize = 2 << 20;
    const OK: usize = 5 << 18;
    const LIMIT: u64 = 3 << 19;
    let sdk = FakeSdk::builder()
        .object(
            "V",
            "set",
            vec![
                Step::SetState {
                    key: "k".into(),
                    value: Bytes::from(blob(BIG)),
                },
                Step::output("ok"),
            ],
        )
        .service("V2", "out", vec![Step::Output(Bytes::from(blob(BIG)))])
        .service(
            "V3",
            "run",
            vec![Step::Run(Bytes::from(blob(BIG))), Step::output("ok")],
        )
        .object(
            "V4",
            "set",
            vec![
                Step::SetState {
                    key: "k".into(),
                    value: Bytes::from(blob(OK)),
                },
                Step::output("ok"),
            ],
        )
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "max-value-bytes=", async {
        let admin = As::admin(&url);
        let acme = As::tenant(&url, "acme", "admin").await?;
        acme.register(&sdk.url).await?;
        set_limits(
            &admin,
            "acme",
            json!({ "maxPayload": 1 << 20, "maxValueBytes": LIMIT }),
        )
        .await?;
        let (code, v) = acme.get("/tenant").await?;
        ensure!(
            code == StatusCode::OK && v["limits"]["maxValueBytes"] == LIMIT,
            "GET /tenant: {code} {v}"
        );
        for path in ["V/set/send?key=a", "V2/out/send", "V3/run/send"] {
            let (code, v) = acme.invoke(path, "{}").await?;
            ensure!(
                code == StatusCode::ACCEPTED || code == StatusCode::OK,
                "{path}: {code} {v}"
            );
            let s = settled(&acme, &id_of(&v)?).await?;
            ensure!(
                s["status"] == "failed"
                    && s["code"] == 579
                    && s["message"]
                        .as_str()
                        .unwrap_or("")
                        .starts_with("value_bytes_limit"),
                "{path}: {}",
                s.to_string().chars().take(300).collect::<String>()
            );
        }
        let (code, v) = acme.invoke("V4/set/send?key=b", "{}").await?;
        ensure!(
            code == StatusCode::ACCEPTED || code == StatusCode::OK,
            "V4: {code} {v}"
        );
        let s = settled(&acme, &id_of(&v)?).await?;
        ensure!(s["status"] == "completed", "1.25 MiB below limit: {s}");
        let objects: Vec<_> = {
            use {futures::TryStreamExt, slatedb::object_store::ObjectStore as _};
            bucket
                .list(Some(&Path::from("values")))
                .try_collect()
                .await?
        };
        ensure!(
            objects.len() == 1,
            "only the turn under the limit PUTs a segment: {} objects",
            objects.len()
        );
        for bad in [
            json!({ "maxValueBytes": 0 }),
            json!({ "maxValueBytes": "64m" }),
        ] {
            let (code, v) = admin.put_tenant("acme", bad.clone()).await?;
            ensure!(code == StatusCode::BAD_REQUEST, "{bad}: {code} {v}");
        }
        set_limits(&admin, "acme", json!({ "maxValueBytes": null })).await?;
        let (_, v) = acme.get("/tenant").await?;
        ensure!(v["limits"]["maxValueBytes"].is_null(), "{v}");
        Ok(())
    })
    .await
}

/// **A large output through `ctx.call` is COPYed to the caller.**
/// `Chat/messages` returns a 1.5 MiB history on a 1 MiB tenant; `Front/go` calls
/// it and returns exactly its result.
///
/// 1. The shell seals the `Output` of `messages` (owner = object `Chat[c1]`).
/// 2. The outbox `Reply` row carries the ref + the source owner; the shell
///    delivers: 1.5 MiB > the tenant's limit ⇒ COPY each segment to owner
///    `Front[f1]` (a new `gen`) before delivering; the caller's
///    `CallCompletion` carries the new ref.
/// 3. The caller's SDK (wire v6; the node has no read path configured ⇒ the
///    shell assembles) gets the assembled completion; its own output (1.5 MiB)
///    is sealed again under its own owner. An SDK before v6 receiving 1.5 MiB >
///    the limit ⇒ 413
///    (`old_sdk_gets_a_stable_error_when_the_assembled_value_exceeds_max_payload`).
///
/// Must see: `200` with the whole 1.5 MiB; the owner directory of `Front[f1]`
/// has **two** segments (the completion's COPY + its own output); after
/// `messages` is purged, `GET` of `go`'s output is still whole (no reference to
/// the source owner is left).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn call_output_over_max_payload_is_copied_to_the_caller() -> Result<()> {
    let history = blob(HISTORY);
    let sdk = FakeSdk::builder()
        .wire(6)
        .object(
            "Chat",
            "messages",
            vec![Step::Output(Bytes::from(history.clone()))],
        )
        .object(
            "Front",
            "go",
            vec![
                Step::Call {
                    service: "Chat".into(),
                    handler: "messages".into(),
                    key: Some("c1".into()),
                    idempotency_key: None,
                    spread: false,
                },
                Step::EchoCallResult,
            ],
        )
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "COPY a large output to the caller", async {
        let acme = As::tenant(&url, "acme", "admin").await?;
        acme.register(&sdk.url).await?;
        set_limits(&As::admin(&url), "acme", json!({ "maxPayload": 1 << 20 })).await?;
        let (code, v) = acme.invoke("Front/go/send?key=f1", "{}").await?;
        ensure!(
            code == StatusCode::ACCEPTED || code == StatusCode::OK,
            "go/send: {code} {v}"
        );
        let go = id_of(&v)?;
        let s = settled(&acme, &go).await?;
        ensure!(
            s["status"] == "completed" && s["output"].as_str().map(str::len) == Some(HISTORY),
            "go: {}",
            s.to_string().chars().take(300).collect::<String>()
        );
        // The caller's owner directory: the COPY + its own output.
        let tenant = cairn_store::TenantId::new("acme").map_err(|e| anyhow!("{e:?}"))?;
        let (pk, owner) = cairn_store::value::object_owner(&tenant, None, "Front", "f1")
            .ok_or_else(|| anyhow!("Front[f1] has an owner"))?;
        let store: std::sync::Arc<dyn slatedb::object_store::ObjectStore> = (*bucket).clone();
        let values = cairn_store::value::ValueStore::new(store, &Path::default());
        let mine = values.objects("acme", pk, &owner).await?;
        ensure!(mine.len() == 2, "owner Front[f1] has 2 segments: {mine:?}");
        // Purge the callee: the caller's output does not depend on it.
        let (code, list) = acme
            .get("/invocations?service=Chat&status=completed")
            .await?;
        ensure!(code == StatusCode::OK, "{code} {list}");
        let callee = list
            .as_array()
            .and_then(|a| a.first())
            .and_then(|r| r["id"].as_str())
            .ok_or_else(|| anyhow!("messages not found: {list}"))?
            .to_string();
        let (code, v) = acme
            .send(acme.http.post(format!("{url}/invocations/{callee}/purge")))
            .await?;
        ensure!(code.is_success(), "purge messages: {code} {v}");
        let s = settled(&acme, &go).await?;
        ensure!(
            s["output"].as_str().map(str::len) == Some(HISTORY),
            "go after purging the source: {}",
            s.to_string().chars().take(300).collect::<String>()
        );
        Ok(())
    })
    .await
}

/// `max-journal=`: the `max`-th entry (counting `Input`) makes the invocation
/// `failed` with code 578 `journal_too_long`; another tenant without a limit
/// runs to the end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_journal_fails_invocation() -> Result<()> {
    let mut steps: Vec<Step> = (0..6)
        .map(|i| Step::SetState {
            key: format!("k{i}"),
            value: Bytes::from_static(b"1"),
        })
        .collect();
    steps.push(Step::output("ok"));
    let sdk = FakeSdk::builder()
        .object("Long", "run", steps)
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "max-journal", async {
        let acme = As::tenant(&url, "acme", "admin").await?;
        let other = As::tenant(&url, "other", "admin").await?;
        acme.register(&sdk.url).await?;
        other.register(&sdk.url).await?;
        set_limits(&As::admin(&url), "acme", json!({ "maxJournal": 4 })).await?;
        let (_, v) = acme.invoke("Long/run/send?key=a", "{}").await?;
        let s = settled(&acme, &id_of(&v)?).await?;
        ensure!(
            s["status"] == "failed"
                && s["code"] == 578
                && s["message"]
                    .as_str()
                    .unwrap_or("")
                    .contains("journal_too_long"),
            "acme: {s}"
        );
        let (code, journal) = acme
            .get(&format!("/invocations/{}/journal", id_of(&v)?))
            .await?;
        ensure!(code == StatusCode::OK, "{code} {journal}");
        let (_, v) = other.invoke("Long/run/send?key=a", "{}").await?;
        let s = settled(&other, &id_of(&v)?).await?;
        ensure!(s["status"] == "completed", "other tenant: {s}");
        Ok(())
    })
    .await
}

/// `max-journal-bytes=`: ten 1 KiB `SetState`s with a 4 KiB limit ⇒ the
/// invocation is **`paused`** with code 580 `journal_bytes_limit` (not
/// `failed`), the durable journal does not exceed the limit, the node still
/// answers `/health`, and another tenant (no limit) runs the same program to
/// `completed`. `GET /tenant` returns the limit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_journal_bytes_pauses_invocation() -> Result<()> {
    let mut steps: Vec<Step> = (0..10)
        .map(|i| Step::SetState {
            key: format!("k{i}"),
            value: Bytes::from(vec![b'v'; 1024]),
        })
        .collect();
    steps.push(Step::output("ok"));
    let sdk = FakeSdk::builder()
        .object("Long", "run", steps)
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "max-journal-bytes", async {
        let acme = As::tenant(&url, "acme", "admin").await?;
        let other = As::tenant(&url, "other", "admin").await?;
        acme.register(&sdk.url).await?;
        other.register(&sdk.url).await?;
        set_limits(&As::admin(&url), "acme", json!({ "maxJournalBytes": 4096 })).await?;
        let (code, t) = acme.get("/tenant").await?;
        ensure!(
            code == StatusCode::OK && t["limits"]["maxJournalBytes"] == 4096,
            "GET /tenant: {code} {t}"
        );
        let (_, v) = acme.invoke("Long/run/send?key=a", "{}").await?;
        let id = id_of(&v)?;
        let t0 = std::time::Instant::now();
        let s = loop {
            let (_, s) = acme.get(&format!("/invocations/{id}")).await?;
            if s["status"] != "running" && s["status"] != "suspended" && s["status"] != "pending" {
                break s;
            }
            ensure!(t0.elapsed() < TEST_DEADLINE, "acme not stopped yet: {s}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        ensure!(
            s["status"] == "paused"
                && s["code"] == 580
                && s["message"]
                    .as_str()
                    .unwrap_or("")
                    .starts_with("journal_bytes_limit"),
            "acme: {s}"
        );
        let (code, journal) = acme.get(&format!("/invocations/{id}/journal")).await?;
        ensure!(code == StatusCode::OK, "{code} {journal}");
        let stored: usize = journal
            .as_array()
            .map(|a| {
                a.iter()
                    .map(|e| 2 + e["payload"].as_str().unwrap_or("").len() / 2)
                    .sum()
            })
            .unwrap_or(0);
        ensure!(stored <= 4096, "durable journal {stored} B over limit");
        let (code, _) = acme.get("/health").await?;
        ensure!(code == StatusCode::OK, "the node must be alive: {code}");
        let (_, v) = other.invoke("Long/run/send?key=a", "{}").await?;
        let s = settled(&other, &id_of(&v)?).await?;
        ensure!(s["status"] == "completed", "other tenant: {s}");
        Ok(())
    })
    .await
}

/// **An external completion is not lost when hitting `max-journal-bytes=`** (a
/// security review finding). Order:
///
/// 1. The invocation writes ~3 KiB of state, then suspends waiting for signal
///    [`HOLD_SIGNAL`]; a 4 KiB limit.
/// 2. An external signal carrying ~2 KiB arrives (`POST …/signal` ⇒ `200`): its
///    entry takes the journal past the limit.
/// 3. The turn continues; the **invocation's own** next entry (`Output`) hits
///    the limit ⇒ the invocation is `paused` 580.
/// 4. The operator raises the limit, then `POST …/resume`.
///
/// Must see: `paused` 580, then after `resume` the invocation gets past the
/// signal wait and reaches `completed` — a signal acked with `200` must be in
/// the durable journal. The old cut dropped the signal's entry while still
/// returning `200` (and for a completion delivered through a queue, it deleted
/// the source row in the same commit); the `Pause` outcome of a delivery step
/// outside a turn was not recorded either — the invocation stayed `suspended`
/// forever and the signal was lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn journal_bytes_cut_keeps_external_signal() -> Result<()> {
    let mut steps: Vec<Step> = (0..3)
        .map(|i| Step::SetState {
            key: format!("k{i}"),
            value: Bytes::from(vec![b'v'; 1024]),
        })
        .collect();
    steps.push(Step::Suspend);
    steps.push(Step::output("ok"));
    let sdk = FakeSdk::builder()
        .object("Long", "run", steps)
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "signal across the journal cut", async {
        let acme = As::tenant(&url, "acme", "admin").await?;
        let admin = As::admin(&url);
        acme.register(&sdk.url).await?;
        set_limits(&admin, "acme", json!({ "maxJournalBytes": 4096 })).await?;
        let (_, v) = acme.invoke("Long/run/send?key=a", "{}").await?;
        let id = id_of(&v)?;
        let status = |want: &'static str| {
            let acme = acme.clone();
            let id = id.clone();
            async move {
                let t0 = std::time::Instant::now();
                loop {
                    let (_, s) = acme.get(&format!("/invocations/{id}")).await?;
                    if s["status"] == want {
                        return Ok::<_, anyhow::Error>(s);
                    }
                    ensure!(
                        t0.elapsed() < Duration::from_secs(15),
                        "waiting for {want}, now: {s}"
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        };
        status("suspended").await?;
        let (code, v) = acme
            .send(
                acme.http
                    .post(format!("{url}/invocations/{id}/signal"))
                    .json(&json!({ "signal": HOLD_SIGNAL, "value": "x".repeat(2048) })),
            )
            .await?;
        ensure!(code == StatusCode::OK, "signal: {code} {v}");
        let s = status("paused").await?;
        ensure!(s["code"] == 580, "stopped by the byte limit: {s}");
        set_limits(&admin, "acme", json!({ "maxJournalBytes": 1_048_576 })).await?;
        let (code, v) = acme
            .send(acme.http.post(format!("{url}/invocations/{id}/resume")))
            .await?;
        ensure!(code.is_success(), "resume: {code} {v}");
        let s = status("completed").await?;
        ensure!(s["status"] == "completed", "{s}");
        Ok(())
    })
    .await
}

/// Paging `GET /invocations/{id}/journal`: `?limit=N` returns the first N
/// entries with an `x-cairnflow-journal-next` header (the next entry index);
/// `?from=` reads on; the last page has no header. The pages joined give exactly
/// the whole journal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn journal_is_paginated() -> Result<()> {
    let mut steps: Vec<Step> = (0..12)
        .map(|i| Step::SetState {
            key: format!("k{i}"),
            value: Bytes::from_static(b"1"),
        })
        .collect();
    steps.push(Step::output("ok"));
    let sdk = FakeSdk::builder()
        .object("Long", "run", steps)
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "journal pages", async {
        let acme = As::tenant(&url, "acme", "admin").await?;
        acme.register(&sdk.url).await?;
        let (_, v) = acme.invoke("Long/run/send?key=a", "{}").await?;
        let id = id_of(&v)?;
        settled(&acme, &id).await?;
        let (_, whole) = acme.get(&format!("/invocations/{id}/journal")).await?;
        let whole = whole.as_array().cloned().unwrap_or_default();
        ensure!(whole.len() > 12, "whole journal: {}", whole.len());
        let mut pages = Vec::new();
        let mut from = Some(0u64);
        while let Some(f) = from {
            let r = acme
                .http
                .get(format!("{url}/invocations/{id}/journal?from={f}&limit=5"))
                .bearer_auth(&acme.key)
                .send()
                .await?;
            ensure!(r.status() == StatusCode::OK, "page {f}: {}", r.status());
            from = r
                .headers()
                .get("x-cairnflow-journal-next")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse().ok());
            let page: Vec<Value> = r.json().await?;
            ensure!(page.len() <= 5, "page {f} has {} entries", page.len());
            ensure!(
                from.is_none() || page.len() == 5,
                "a middle page must be full: {}",
                page.len()
            );
            pages.extend(page);
            ensure!(pages.len() <= whole.len(), "pages repeat");
        }
        ensure!(pages == whole, "joined pages differ from the whole journal");
        Ok(())
    })
    .await
}

/// `retention-ms=`: a tenant's completed invocations are purged by its limit;
/// a handler's `journalRetention` overrides the tenant; another tenant follows
/// `--retention-ms`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retention_per_tenant() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("R", "short", vec![Step::output("ok")])
        .service("R", "pinned", vec![Step::output("ok")])
        .contract(
            "R",
            "pinned",
            Contract {
                journal_retention: Some(24 * 60 * 60 * 1000),
                ..Default::default()
            },
        )
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "retention by tenant", async {
        let acme = As::tenant(&url, "acme", "admin").await?;
        let other = As::tenant(&url, "other", "admin").await?;
        acme.register(&sdk.url).await?;
        other.register(&sdk.url).await?;
        set_limits(&As::admin(&url), "acme", json!({ "retentionMs": 300 })).await?;
        // Pushed into the range's engine on the background tick.
        tokio::time::sleep(Duration::from_millis(500)).await;

        let (_, a) = acme.invoke("R/short", "{}").await?;
        let (_, pinned) = acme.invoke("R/pinned", "{}").await?;
        let (_, o) = other.invoke("R/short", "{}").await?;
        let (a, pinned, o) = (id_of(&a)?, id_of(&pinned)?, id_of(&o)?);
        let t0 = std::time::Instant::now();
        loop {
            let (code, _) = acme.get(&format!("/invocations/{a}")).await?;
            if code == StatusCode::NOT_FOUND {
                break;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(10),
                "acme not purged after 10 s: {code}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let (code, v) = acme.get(&format!("/invocations/{pinned}")).await?;
        ensure!(
            code == StatusCode::OK,
            "the handler's journalRetention must override the tenant: {code} {v}"
        );
        let (code, v) = other.get(&format!("/invocations/{o}")).await?;
        ensure!(
            code == StatusCode::OK,
            "other tenant follows --retention-ms: {code} {v}"
        );
        Ok(())
    })
    .await
}

/// `deploy-rate=N/S`: the `N+1`-th registration within `S` seconds ⇒ `429`
/// `deploy_rate_limited` with `Retry-After`; other tenants are not affected.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deploy_rate_limits_registration() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("Echo", "hi", vec![Step::output("ok")])
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "deploy-rate", async {
        let acme = As::tenant(&url, "acme", "admin").await?;
        let other = As::tenant(&url, "other", "admin").await?;
        set_limits(&As::admin(&url), "acme", json!({ "deployRate": "2/60" })).await?;
        for i in 0..2 {
            let (code, v) = acme.register(&sdk.url).await?;
            ensure!(code == StatusCode::OK, "attempt {i}: {code} {v}");
        }
        let (code, v) = acme.register(&sdk.url).await?;
        ensure!(
            code == StatusCode::TOO_MANY_REQUESTS && v["code"] == "deploy_rate_limited",
            "attempt 3: {code} {v}"
        );
        let ra: u64 = v["_retryAfter"].as_str().unwrap_or("0").parse()?;
        ensure!((1..=60).contains(&ra), "Retry-After {ra}: {v}");
        for i in 0..3 {
            let (code, v) = other.register(&sdk.url).await?;
            ensure!(code == StatusCode::OK, "other tenant, try {i}: {code} {v}");
        }
        Ok(())
    })
    .await
}

/// Unknown values: `PUT` refuses with `400`; a broken `tenants` object on the
/// bucket ⇒ the node keeps the old book (the limits in force still apply) and
/// the gauge `cairnflow_tenants_book_rejected` = 1; once the book heals ⇒ the
/// gauge goes back to 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn weird_values_keep_the_old_book() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("Echo", "hi", vec![Step::output("ok")])
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "unknown values", async {
        let admin = As::admin(&url);
        let acme = As::tenant(&url, "acme", "admin").await?;
        acme.register(&sdk.url).await?;
        for bad in [
            json!({ "maxPayload": 0 }),
            json!({ "maxJournal": 0 }),
            json!({ "deployRate": "abc" }),
            json!({ "deployRate": "0/60" }),
            json!({ "maxPayload": "1kb" }),
        ] {
            let (code, v) = admin.put_tenant("acme", bad.clone()).await?;
            ensure!(code.is_client_error(), "{bad} must be refused: {code} {v}");
        }
        set_limits(&admin, "acme", json!({ "maxPayload": 1024 })).await?;

        // Someone writes a mistyped value into the bucket by hand.
        let raw = "cairnflow-tenants/1\nacme max-payload=1kb\n";
        bucket
            .put(
                &Path::from("tenants"),
                PutPayload::from(raw.as_bytes().to_vec()),
            )
            .await?;
        let (code, v) = admin.get("/admin/tenants").await?;
        ensure!(
            code.is_server_error(),
            "reading a broken book must report an error: {code} {v}"
        );
        let m = admin.metrics().await?;
        ensure!(
            m.lines().any(|l| l == "cairnflow_tenants_book_rejected 1"),
            "broken-book gauge: {}",
            m.lines()
                .filter(|l| l.contains("tenants_book"))
                .collect::<Vec<_>>()
                .join("\n")
        );
        let (code, v) = acme.invoke("Echo/hi", blob(2048)).await?;
        ensure!(
            code == StatusCode::PAYLOAD_TOO_LARGE,
            "the old limit must still hold: {code} {v}"
        );

        // Fix the book: the gauge goes back to 0, the new limit applies.
        let raw = "cairnflow-tenants/1\nacme max-payload=4096\n";
        bucket
            .put(
                &Path::from("tenants"),
                PutPayload::from(raw.as_bytes().to_vec()),
            )
            .await?;
        let (code, v) = admin.get("/admin/tenants").await?;
        ensure!(code == StatusCode::OK, "{code} {v}");
        let m = admin.metrics().await?;
        ensure!(
            m.lines().any(|l| l == "cairnflow_tenants_book_rejected 0"),
            "gauge not back to 0"
        );
        let (code, v) = acme.invoke("Echo/hi", blob(2048)).await?;
        ensure!(code == StatusCode::OK, "new limit: {code} {v}");
        Ok(())
    })
    .await
}

/// `GET /tenant`: an Admin sees the limits of its own tenant (nobody else's);
/// the `invoke` role gets `403`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_tenant_returns_own_limits() -> Result<()> {
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "GET /tenant", async {
        let admin = As::admin(&url);
        set_limits(
            &admin,
            "acme",
            json!({ "retentionMs": 60000, "maxPayload": 1024, "maxJournal": 100, "deployRate": "5/60" }),
        )
        .await?;
        let acme = As::tenant(&url, "acme", "admin").await?;
        let other = As::tenant(&url, "other", "admin").await?;
        let caller = As::tenant(&url, "acme", "invoke").await?;
        let (code, v) = acme.get("/tenant").await?;
        ensure!(code == StatusCode::OK, "{code} {v}");
        ensure!(
            v == json!({
                "tenant": "acme",
                "state": "active",
                "limits": {
                    "retentionMs": 60000, "maxPayload": 1024, "maxJournal": 100, "deployRate": "5/60",
                    "maxLive": null, "maxDeployments": null, "maxSchedules": null, "maxTimers": null,
                    "maxStreams": null, "maxStreamRam": null, "maxStateKeys": null,
                    "maxArtifactBytes": null, "maxJournalBytes": null, "maxValueBytes": null
                },
                "usage": { "live": 0, "artifactBytes": 0 }
            }),
            "{v}"
        );
        let (_, v) = other.get("/tenant").await?;
        ensure!(v["tenant"] == "other" && v["limits"]["maxPayload"].is_null(), "{v}");
        let (code, v) = caller.get("/tenant").await?;
        ensure!(code == StatusCode::FORBIDDEN, "invoke role: {code} {v}");
        Ok(())
    })
    .await
}

/// After `kill -9`: the limits are in the book on the bucket, a new node reads
/// it again at startup — `413` still applies on the replacement node.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn limits_survive_kill9() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("Echo", "hi", vec![Step::output("ok")])
        .spawn()
        .await?;
    let bucket = common::bucket();
    let n1 = authed(&bucket, "n1").await?;
    let url1 = n1.node.url();
    let acme_key = within(TEST_DEADLINE, "before kill -9", async {
        let acme = As::tenant(&url1, "acme", "admin").await?;
        acme.register(&sdk.url).await?;
        set_limits(&As::admin(&url1), "acme", json!({ "maxPayload": 1024 })).await?;
        Ok(acme.key)
    })
    .await?;
    common::freeze(&n1.store);
    let n2 = authed(&bucket, "n2").await?;
    let url2 = n2.node.url();
    within(TEST_DEADLINE, "after kill -9", async {
        let acme = As {
            key: acme_key,
            ..As::admin(&url2)
        };
        // The replacement node reads the book at startup: limits apply at ingress at
        // once, even before it takes the range's lease.
        let (code, v) = acme.invoke("Echo/hi", blob(2048)).await?;
        ensure!(
            code == StatusCode::PAYLOAD_TOO_LARGE,
            "replacement node: {code} {v}"
        );
        // Under the limit it runs, once the dead node's lease expires.
        let t0 = std::time::Instant::now();
        loop {
            let (code, v) = acme.invoke("Echo/hi", blob(512)).await?;
            if code == StatusCode::OK {
                break;
            }
            ensure!(
                code == StatusCode::SERVICE_UNAVAILABLE && t0.elapsed() < TEST_DEADLINE,
                "under the limit: {code} {v}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(())
    })
    .await?;
    drop(n1);
    Ok(())
}

// ───────────────────────── per-tenant resource limits

/// Like [`authed`], with a 200 ms usage counting beat (product: 5 s).
async fn counted(bucket: &common::Bucket, id: &str) -> Result<ClusterNode> {
    common::start_cluster_node_tweaked(bucket, id, 2, TTL, |cfg| {
        cfg.auth = AuthConfig {
            required: true,
            admin_key: Some(ADMIN_KEY.into()),
            cluster_secret: None,
            metrics_key: None,
            admin_key_sha256: None,
            provision_key_sha256: None,
        };
        cfg.tenant_usage_every = Duration::from_millis(200);
    })
    .await
}

/// Wait until new work of `who` is refused because of `max-live=` (`429
/// live_invocations_limit`, no `id`) — a soft limit, counted on a beat. Probe
/// with `Echo/hi` (completes at once): a probe that slips through leaves no live
/// invocation.
async fn until_live_limited(who: &As) -> Result<()> {
    let t0 = std::time::Instant::now();
    loop {
        let (code, v) = who.invoke("Echo/hi", "\"x\"").await?;
        if code == StatusCode::TOO_MANY_REQUESTS {
            ensure!(
                v["code"] == "live_invocations_limit" && v.get("id").is_none(),
                "{v}"
            );
            return Ok(());
        }
        ensure!(
            code.is_success() || code == StatusCode::SERVICE_UNAVAILABLE,
            "{code} {v}"
        );
        ensure!(
            t0.elapsed() < TEST_DEADLINE,
            "max-live does not block: {code} {v}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn napper() -> common::fake_sdk::FakeSdkBuilder {
    FakeSdk::builder()
        .service(
            "Nap",
            "once",
            vec![Step::Sleep(3_600_000), Step::output("woke")],
        )
        .service("Echo", "hi", vec![Step::output("ok")])
}

/// **`max-live=`** (live invocations from `QueryCounts`, a soft limit): a tenant
/// with 2 sleeping invocations and a limit of 2 ⇒ after one counting beat, new
/// work gets `429 live_invocations_limit`; other tenants do not; `GET /tenant`
/// returns the limit and the count; cancelling one invocation ⇒ work is accepted
/// again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_live_refuses_new_work() -> Result<()> {
    let sdk = napper().spawn().await?;
    let bucket = common::bucket();
    let node = counted(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "max-live", async {
        let acme = As::tenant(&url, "acme", "admin").await?;
        let other = As::tenant(&url, "other", "admin").await?;
        acme.register(&sdk.url).await?;
        other.register(&sdk.url).await?;
        set_limits(&As::admin(&url), "acme", json!({ "maxLive": 2 })).await?;
        let mut ids = Vec::new();
        for _ in 0..2 {
            let (code, v) = acme.invoke("Nap/once/send", "\"x\"").await?;
            ensure!(code.is_success(), "{code} {v}");
            ids.push(id_of(&v)?);
        }
        until_live_limited(&acme).await?;
        let (code, v) = other.invoke("Echo/hi", "\"x\"").await?;
        ensure!(code == StatusCode::OK, "other tenant: {code} {v}");
        let (code, v) = acme.get("/tenant").await?;
        ensure!(
            code == StatusCode::OK
                && v["limits"]["maxLive"] == 2
                && v["usage"]["live"].as_u64() >= Some(2),
            "{v}"
        );
        // Cancel one (hard): 1 live left ⇒ after one beat, work is accepted again.
        let (code, v) = acme
            .send(
                acme.http
                    .post(format!("{url}/invocations/{}/cancel", ids[0])),
            )
            .await?;
        ensure!(code.is_success(), "cancel: {code} {v}");
        let t0 = std::time::Instant::now();
        loop {
            let (code, v) = acme.invoke("Echo/hi", "\"x\"").await?;
            if code == StatusCode::OK {
                break;
            }
            ensure!(
                code == StatusCode::TOO_MANY_REQUESTS && t0.elapsed() < TEST_DEADLINE,
                "after the cancel: {code} {v}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(())
    })
    .await
}

/// **Counting after `kill -9`**: a node dies with 2 sleeping invocations; the
/// replacement node rebuilds the counter from `ByCreated` when it becomes leader
/// ⇒ the limit still blocks.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_live_counts_after_kill9() -> Result<()> {
    let sdk = napper().spawn().await?;
    let bucket = common::bucket();
    let n1 = counted(&bucket, "n1").await?;
    let url1 = n1.node.url();
    let acme_key = within(TEST_DEADLINE, "before kill -9", async {
        let acme = As::tenant(&url1, "acme", "admin").await?;
        acme.register(&sdk.url).await?;
        set_limits(&As::admin(&url1), "acme", json!({ "maxLive": 2 })).await?;
        for _ in 0..2 {
            let (code, v) = acme.invoke("Nap/once/send", "\"x\"").await?;
            ensure!(code.is_success(), "{code} {v}");
        }
        until_live_limited(&acme).await?;
        Ok(acme.key)
    })
    .await?;
    common::freeze(&n1.store);
    let n2 = counted(&bucket, "n2").await?;
    let url2 = n2.node.url();
    within(TEST_DEADLINE, "after kill -9", async {
        let acme = As {
            key: acme_key,
            ..As::admin(&url2)
        };
        until_live_limited(&acme).await
    })
    .await?;
    drop(n1);
    Ok(())
}

/// **Split and merge midway**: the tenant's live invocations are spread over
/// ranges; split and then merge the range holding them ⇒ the counters of the
/// child/merged ranges are rebuilt, and the limit still blocks after each
/// (while rebuilding it lets work through — a soft limit).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_live_counts_across_split_and_merge() -> Result<()> {
    let sdk = napper().spawn().await?;
    let bucket = common::bucket();
    let node = counted(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "split/merge", async {
        let admin = As::admin(&url);
        let acme = As::tenant(&url, "acme", "admin").await?;
        acme.register(&sdk.url).await?;
        set_limits(&admin, "acme", json!({ "maxLive": 3 })).await?;
        for _ in 0..3 {
            let (code, v) = acme.invoke("Nap/once/send", "\"x\"").await?;
            ensure!(code.is_success(), "{code} {v}");
        }
        until_live_limited(&acme).await?;
        let (code, v) = admin
            .send(admin.http.post(format!("{url}/ranges/0/split")))
            .await?;
        ensure!(code == StatusCode::OK, "split: {code} {v}");
        // The table after the split: the two children carry new ids (≥ 2 when the cluster opens with 0, 1).
        let kids: Vec<u64> = v["ranges"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|r| r["range"].as_u64())
                    .filter(|r| *r >= 2)
                    .collect()
            })
            .unwrap_or_default();
        ensure!(kids.len() == 2, "{v}");
        until_live_limited(&acme).await?;
        let (left, right) = (kids[0].min(kids[1]), kids[0].max(kids[1]));
        let (code, v) = admin
            .send(
                admin
                    .http
                    .post(format!("{url}/ranges/merge?left={left}&right={right}")),
            )
            .await?;
        ensure!(code == StatusCode::OK, "merge: {code} {v}");
        until_live_limited(&acme).await
    })
    .await
}

/// **`max-deployments=`**: the tenant's third deployment (limit 2) ⇒ `409
/// deployments_limit` — before discovery; other tenants are not affected;
/// removing an old one (no longer the current revision) ⇒ registration works.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_deployments_at_registration() -> Result<()> {
    let a = napper().spawn().await?;
    let b = FakeSdk::builder()
        .service("Other", "hi", vec![Step::output("ok")])
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "max-deployments", async {
        let acme = As::tenant(&url, "acme", "admin").await?;
        let other = As::tenant(&url, "other", "admin").await?;
        set_limits(&As::admin(&url), "acme", json!({ "maxDeployments": 2 })).await?;
        let (code, v) = acme.register(&a.url).await?;
        ensure!(code == StatusCode::OK, "{code} {v}");
        let first = v["id"].as_str().unwrap_or_default().to_string();
        // The second copy of the same endpoint becomes the current revision; the first can be removed.
        let (code, v) = acme.register(&a.url).await?;
        ensure!(code == StatusCode::OK, "{code} {v}");
        let (code, v) = acme.register(&b.url).await?;
        ensure!(
            code == StatusCode::CONFLICT && v["code"] == "deployments_limit",
            "second deployment: {code} {v}"
        );
        for sdk in [&a.url, &b.url] {
            let (code, v) = other.register(sdk).await?;
            ensure!(code == StatusCode::OK, "other tenant: {code} {v}");
        }
        let (code, v) = acme
            .send(acme.http.delete(format!("{url}/deployments/{first}")))
            .await?;
        ensure!(code.is_success(), "remove: {code} {v}");
        let (code, v) = acme.register(&b.url).await?;
        ensure!(code == StatusCode::OK, "after removing: {code} {v}");
        Ok(())
    })
    .await
}

/// **`max-schedules=`**: the tenant's second new schedule ⇒ `409
/// schedules_limit`; `PUT` on an existing schedule (an edit) passes; deleting
/// one ⇒ creating works.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_schedules_at_put() -> Result<()> {
    let sdk = napper().spawn().await?;
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "max-schedules", async {
        let acme = As::tenant(&url, "acme", "admin").await?;
        acme.register(&sdk.url).await?;
        set_limits(&As::admin(&url), "acme", json!({ "maxSchedules": 1 })).await?;
        let put = |name: &str, every: u64| {
            acme.send(acme.http.put(format!("{url}/schedules/{name}")).json(
                &json!({ "everyMs": every, "target": { "service": "Echo", "handler": "hi" } }),
            ))
        };
        let (code, v) = put("a", 60_000).await?;
        ensure!(code == StatusCode::OK, "{code} {v}");
        let (code, v) = put("a", 120_000).await?;
        ensure!(code == StatusCode::OK, "edit a schedule: {code} {v}");
        let (code, v) = put("b", 60_000).await?;
        ensure!(
            code == StatusCode::CONFLICT && v["code"] == "schedules_limit",
            "second schedule: {code} {v}"
        );
        let (code, v) = acme
            .send(acme.http.delete(format!("{url}/schedules/a")))
            .await?;
        ensure!(code.is_success(), "delete: {code} {v}");
        let (code, v) = put("b", 60_000).await?;
        ensure!(code == StatusCode::OK, "after deleting: {code} {v}");
        Ok(())
    })
    .await
}

/// `PUT /admin/tenants/{t}` accepts the new limits, refuses unknown values
/// (`400`), and `GET /tenant` reflects them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resource_limits_round_trip_the_api() -> Result<()> {
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "resource limit API", async {
        let admin = As::admin(&url);
        let acme = As::tenant(&url, "acme", "admin").await?;
        let all = json!({
            "maxLive": 100, "maxDeployments": 3, "maxSchedules": 5, "maxTimers": 8,
            "maxStreams": 4, "maxStateKeys": 50, "maxArtifactBytes": 1048576
        });
        set_limits(&admin, "acme", all.clone()).await?;
        let (code, v) = acme.get("/tenant").await?;
        ensure!(code == StatusCode::OK, "{code} {v}");
        for (k, want) in all.as_object().unwrap() {
            ensure!(v["limits"][k] == *want, "{k}: {v}");
        }
        for bad in [
            json!({ "maxLive": 0 }),
            json!({ "maxTimers": "x" }),
            json!({ "maxArtifactBytes": -1 }),
        ] {
            let (code, v) = admin.put_tenant("acme", bad.clone()).await?;
            ensure!(code == StatusCode::BAD_REQUEST, "{bad}: {code} {v}");
        }
        set_limits(&admin, "acme", json!({ "maxLive": null })).await?;
        let (_, v) = acme.get("/tenant").await?;
        ensure!(
            v["limits"]["maxLive"].is_null() && v["limits"]["maxTimers"] == 8,
            "{v}"
        );
        Ok(())
    })
    .await
}

// ───────────── `max-artifact-bytes=` from the `Artifacts` rows

/// Size of the artifact `Art/put` sends. The content is the same every time:
/// each invocation is its own `(id, sha)` row, so each run adds exactly
/// `ART_SIZE` bytes.
const ART_SIZE: usize = 1000;

/// A wire v4 `REQUEST_RESPONSE` fake SDK: `Art/put` returns one `ArtifactPut` of
/// [`ART_SIZE`] bytes, then `Output` + `End`.
async fn artifact_sdk() -> Result<String> {
    use {
        axum::{
            routing::{get, post},
            Router,
        },
        bytes::BytesMut,
        cairn_protocol::{encode_message, pb, Flags, MessageType},
        cairn_server::{
            discovery::{
                EndpointManifest, HandlerManifest, ProtocolMode, ServiceManifest, ServiceType,
                DISCOVERY_V1,
            },
            invoker::INVOCATION_V1,
        },
        http::header,
    };
    async fn discover() -> ([(header::HeaderName, &'static str); 1], String) {
        let m = EndpointManifest {
            protocol_mode: ProtocolMode::RequestResponse,
            min_protocol_version: 1,
            max_protocol_version: 4,
            services: vec![ServiceManifest {
                name: "Art".into(),
                ty: ServiceType::Service,
                handlers: vec![HandlerManifest {
                    name: "put".into(),
                    ty: None,
                    contract: Default::default(),
                }],
                contract: Default::default(),
            }],
        };
        (
            [(header::CONTENT_TYPE, DISCOVERY_V1)],
            serde_json::to_string(&m).unwrap(),
        )
    }
    async fn invoke() -> ([(header::HeaderName, &'static str); 1], Bytes) {
        use sha2::Digest;
        let body = Bytes::from(vec![b'a'; ART_SIZE]);
        let sha: [u8; 32] = sha2::Sha256::digest(&body).into();
        let mut b = BytesMut::new();
        b.extend_from_slice(&encode_message(
            MessageType::ARTIFACT_PUT,
            Flags::NONE,
            &pb::ArtifactPutMessage {
                sha256: Bytes::copy_from_slice(&sha),
                content: body,
                content_type: "text/plain".into(),
            },
        ));
        b.extend_from_slice(&encode_message(
            MessageType::OUTPUT,
            Flags::NONE,
            &pb::OutputCommandMessage {
                result: Some(pb::output_command_message::Result::Value(
                    cairn_protocol::value::inline(Bytes::from_static(b"1")),
                )),
                name: "output".into(),
            },
        ));
        b.extend_from_slice(&encode_message(
            MessageType::END,
            Flags::NONE,
            &pb::EndMessage {},
        ));
        ([(header::CONTENT_TYPE, INVOCATION_V1)], b.freeze())
    }
    let app = Router::new()
        .route("/discover", get(discover))
        .route("/invoke/{service}/{handler}", post(invoke));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(url)
}

/// Like [`counted`], plus **the artifact sweep turned off**: any LIST of
/// `artifacts/` left can only come from the usage counting loop, and the objects
/// of purged runs stay on the bucket (bucket bytes ≠ live bytes).
async fn art_node(bucket: &common::Bucket, id: &str) -> Result<ClusterNode> {
    common::start_cluster_node_tweaked(bucket, id, 2, TTL, |cfg| {
        cfg.auth = AuthConfig {
            required: true,
            admin_key: Some(ADMIN_KEY.into()),
            cluster_secret: None,
            metrics_key: None,
            admin_key_sha256: None,
            provision_key_sha256: None,
        };
        cfg.tenant_usage_every = Duration::from_millis(200);
        cfg.artifact_sweep = None;
    })
    .await
}

/// One `Art/put` of `who`, waiting until it ends: `(id, GET /invocations/{id})`.
async fn put_one(who: &As) -> Result<(String, Value)> {
    let t0 = std::time::Instant::now();
    let id = loop {
        let (code, v) = who.invoke("Art/put/send", "null").await?;
        if code.is_success() {
            break id_of(&v)?;
        }
        ensure!(
            code == StatusCode::SERVICE_UNAVAILABLE && t0.elapsed() < TEST_DEADLINE,
            "Art/put: {code} {v}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let s = settled(who, &id).await?;
    Ok((id, s))
}

/// The invocation ended with 579 `artifacts_limit`.
fn artifacts_limited(s: &Value) -> bool {
    s["status"] == "failed"
        && s["code"] == 579
        && s["message"]
            .as_str()
            .unwrap_or("")
            .contains("artifacts_limit")
}

/// Wait until `usage.artifactBytes` of `GET /tenant` equals `want` (a soft
/// limit, counted on a beat; a range being rebuilt is missing from the total).
async fn until_artifact_bytes(who: &As, want: u64) -> Result<()> {
    let t0 = std::time::Instant::now();
    loop {
        let (code, v) = who.get("/tenant").await?;
        if code == StatusCode::OK && v["usage"]["artifactBytes"].as_u64() == Some(want) {
            return Ok(());
        }
        ensure!(
            t0.elapsed() < TEST_DEADLINE,
            "artifactBytes not at {want}: {code} {v}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Number of LISTs under `artifacts/` the node's store has seen (every source).
fn artifact_lists(node: &ClusterNode) -> u64 {
    node.store
        .counts_by_source_object()
        .into_iter()
        .filter(|(_, op, _, kind, _)| *op == cairn_store::testkit::Op::List && *kind == "artifacts")
        .map(|(.., n)| n)
        .sum()
}

/// **`max-artifact-bytes=` does not LIST the bucket**: live artifact bytes are
/// counted from the `Artifacts` rows in the range's RAM, gathered through the
/// query layer — no more LIST of `artifacts/<tenant>/` on every beat on every
/// node.
///
/// 1. `acme` with a limit of 1 500 bytes; two `Art/put`s (1 000 bytes each)
///    complete.
/// 2. `usage.artifactBytes` reaches 2 000; the third `Art/put` ⇒ `failed` 579
///    `artifacts_limit`; another tenant (no limit) still completes.
/// 3. A 2 s measuring window (10 counting beats of 200 ms), the artifact sweep
///    off.
///
/// Must see: the limit blocks as before, and **0** LISTs under `artifacts/` in
/// the window (before the fix ≈ 10: one LIST per beat per tenant with a limit).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_artifact_bytes_lists_nothing() -> Result<()> {
    let sdk = artifact_sdk().await?;
    let bucket = common::bucket();
    let node = art_node(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "max-artifact-bytes", async {
        let acme = As::tenant(&url, "acme", "admin").await?;
        let other = As::tenant(&url, "other", "admin").await?;
        acme.register(&sdk).await?;
        other.register(&sdk).await?;
        set_limits(
            &As::admin(&url),
            "acme",
            json!({ "maxArtifactBytes": 1500 }),
        )
        .await?;
        for _ in 0..2 {
            let (_, s) = put_one(&acme).await?;
            ensure!(s["status"] == "completed", "under the limit: {s}");
        }
        until_artifact_bytes(&acme, 2 * ART_SIZE as u64).await?;
        let (_, s) = put_one(&acme).await?;
        ensure!(artifacts_limited(&s), "hits the limit: {s}");
        let (_, s) = put_one(&other).await?;
        ensure!(s["status"] == "completed", "other tenant: {s}");
        let before = artifact_lists(&node);
        // A measuring window, not "probably done": count the requests over 10 beats.
        tokio::time::sleep(Duration::from_secs(2)).await;
        let lists = artifact_lists(&node) - before;
        ensure!(lists == 0, "{lists} LIST artifacts/ in 2 s");
        until_artifact_bytes(&acme, 2 * ART_SIZE as u64).await
    })
    .await
}

/// **Purge subtracts correctly**: the bytes of a purged run no longer count,
/// even while the objects are still on the bucket (the trash holds them in
/// place / the sweep has not got there — here the sweep is off).
///
/// 1. `acme` with a limit of 1 500; two `Art/put`s ⇒ 2 000, the third gets 579.
/// 2. `POST /invocations/{id}/purge` one completed run.
///
/// Must see: `usage.artifactBytes` back to 1 000 and the next `Art/put`
/// completes (the bucket still has 2 000 bytes — before the fix the limit still
/// blocked).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn purged_artifacts_stop_counting() -> Result<()> {
    let sdk = artifact_sdk().await?;
    let bucket = common::bucket();
    let node = art_node(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "purge subtracts correctly", async {
        let acme = As::tenant(&url, "acme", "admin").await?;
        acme.register(&sdk).await?;
        set_limits(
            &As::admin(&url),
            "acme",
            json!({ "maxArtifactBytes": 1500 }),
        )
        .await?;
        let mut ids = Vec::new();
        for _ in 0..2 {
            let (id, s) = put_one(&acme).await?;
            ensure!(s["status"] == "completed", "{s}");
            ids.push(id);
        }
        until_artifact_bytes(&acme, 2 * ART_SIZE as u64).await?;
        let (_, s) = put_one(&acme).await?;
        ensure!(artifacts_limited(&s), "hits the limit: {s}");
        let (code, v) = acme
            .send(
                acme.http
                    .post(format!("{url}/invocations/{}/purge", ids[0])),
            )
            .await?;
        ensure!(code.is_success(), "purge: {code} {v}");
        until_artifact_bytes(&acme, ART_SIZE as u64).await?;
        let (_, s) = put_one(&acme).await?;
        ensure!(s["status"] == "completed", "after purge: {s}");
        until_artifact_bytes(&acme, 2 * ART_SIZE as u64).await
    })
    .await
}

/// **A `restart` copying artifacts adds up correctly** (AP-10): the new run
/// carries a copy of every artifact of the old run — its rows count like every
/// live row.
///
/// 1. `acme` with a limit of 2 500; two `Art/put`s ⇒ 2 000.
/// 2. `POST /invocations/{x}/restart` ⇒ a new id, X's artifacts copied over.
///
/// Must see: `usage.artifactBytes` = 3 000 (the new run reruns `Art/put` with
/// the same sha: the row already exists, it is not added again), and the next
/// `Art/put` gets 579.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_copies_count_once() -> Result<()> {
    let sdk = artifact_sdk().await?;
    let bucket = common::bucket();
    let node = art_node(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "restart copies artifacts", async {
        let acme = As::tenant(&url, "acme", "admin").await?;
        acme.register(&sdk).await?;
        set_limits(
            &As::admin(&url),
            "acme",
            json!({ "maxArtifactBytes": 2500 }),
        )
        .await?;
        let mut ids = Vec::new();
        for _ in 0..2 {
            let (id, s) = put_one(&acme).await?;
            ensure!(s["status"] == "completed", "{s}");
            ids.push(id);
        }
        until_artifact_bytes(&acme, 2 * ART_SIZE as u64).await?;
        let (code, v) = acme
            .send(
                acme.http
                    .post(format!("{url}/invocations/{}/restart", ids[0])),
            )
            .await?;
        ensure!(code.is_success(), "restart: {code} {v}");
        let new = v["new"].as_str().unwrap_or_default().to_string();
        ensure!(!new.is_empty(), "{v}");
        settled(&acme, &new).await?;
        until_artifact_bytes(&acme, 3 * ART_SIZE as u64).await?;
        let (_, s) = put_one(&acme).await?;
        ensure!(artifacts_limited(&s), "after restart: {s}");
        Ok(())
    })
    .await
}

/// **Counting after `kill -9`**: a node dies when the tenant has already hit
/// its limit; the replacement node rebuilds the counter from the `Artifacts`
/// table when it becomes leader.
///
/// Must see: on the replacement node `usage.artifactBytes` = 2 000 (exactly the
/// old total) and `Art/put` gets 579.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn artifact_bytes_after_kill9() -> Result<()> {
    let sdk = artifact_sdk().await?;
    let bucket = common::bucket();
    let n1 = art_node(&bucket, "n1").await?;
    let url1 = n1.node.url();
    let acme_key = within(TEST_DEADLINE, "before kill -9", async {
        let acme = As::tenant(&url1, "acme", "admin").await?;
        acme.register(&sdk).await?;
        set_limits(
            &As::admin(&url1),
            "acme",
            json!({ "maxArtifactBytes": 1500 }),
        )
        .await?;
        for _ in 0..2 {
            let (_, s) = put_one(&acme).await?;
            ensure!(s["status"] == "completed", "{s}");
        }
        until_artifact_bytes(&acme, 2 * ART_SIZE as u64).await?;
        Ok(acme.key)
    })
    .await?;
    common::freeze(&n1.store);
    let n2 = art_node(&bucket, "n2").await?;
    let url2 = n2.node.url();
    // Wait for n2 to lead **every** range and for their counters to be rebuilt
    // before trusting the count: while n2 takes the ranges one by one, a range
    // still on n1 (store frozen, RAM still answering remote scans) still counts
    // in the total ⇒ `usage` = 2 000 early; then when n2 takes the last ones,
    // the counter of a range being rebuilt is missing from the total (the soft
    // limit lets work through, `tenant_usage.rs`) ⇒ the snapshot drops for one
    // beat and a PUT slips through (flaky ~1/8 under Ready).
    common::wait_probe(&[&n2], TEST_DEADLINE, "n2 leads every range", |p| {
        (0..2).all(|r| p.get(&r).is_some_and(|v| v == &vec![0]))
    })
    .await?;
    within(TEST_DEADLINE, "after kill -9", async {
        let acme = As {
            key: acme_key,
            ..As::admin(&url2)
        };
        let sql = json!({ "sql": "SELECT bytes FROM sys_artifact_bytes" });
        loop {
            let (code, v) = acme
                .send(acme.http.post(format!("{url2}/query")).json(&sql))
                .await?;
            let bytes = v["rows"][0][0]
                .as_u64()
                .or_else(|| v["rows"][0][0].as_str().and_then(|s| s.parse().ok()));
            if code == StatusCode::OK && v["partial"] == false && bytes == Some(2 * ART_SIZE as u64)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        // A 200 ms counting beat: a counting round in flight while a counter is
        // still being rebuilt may record a short count right after the line above —
        // read `usage` over more than one beat.
        until_artifact_bytes(&acme, 2 * ART_SIZE as u64).await?;
        tokio::time::sleep(Duration::from_millis(300)).await;
        until_artifact_bytes(&acme, 2 * ART_SIZE as u64).await?;
        let (_, s) = put_one(&acme).await?;
        ensure!(artifacts_limited(&s), "replacement node: {s}");
        Ok(())
    })
    .await?;
    drop(n1);
    Ok(())
}

/// **Split and merge midway**: the tenant's artifacts are spread over two
/// ranges; split and then merge range 0 ⇒ the counters of the child/merged
/// ranges are rebuilt from the `Artifacts` table.
///
/// Must see: after the split and after the merge, `usage.artifactBytes` is back
/// to exactly 3 000 (the parent range's rows are not counted twice) and
/// `Art/put` gets 579.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn artifact_bytes_across_split_and_merge() -> Result<()> {
    let sdk = artifact_sdk().await?;
    let bucket = common::bucket();
    let node = art_node(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "split/merge", async {
        let admin = As::admin(&url);
        let acme = As::tenant(&url, "acme", "admin").await?;
        acme.register(&sdk).await?;
        set_limits(&admin, "acme", json!({ "maxArtifactBytes": 2500 })).await?;
        for _ in 0..3 {
            let (_, s) = put_one(&acme).await?;
            ensure!(s["status"] == "completed", "{s}");
        }
        until_artifact_bytes(&acme, 3 * ART_SIZE as u64).await?;
        let (code, v) = admin
            .send(admin.http.post(format!("{url}/ranges/0/split")))
            .await?;
        ensure!(code == StatusCode::OK, "split: {code} {v}");
        let kids: Vec<u64> = v["ranges"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|r| r["range"].as_u64())
                    .filter(|r| *r >= 2)
                    .collect()
            })
            .unwrap_or_default();
        ensure!(kids.len() == 2, "{v}");
        until_artifact_bytes(&acme, 3 * ART_SIZE as u64).await?;
        let (_, s) = put_one(&acme).await?;
        ensure!(artifacts_limited(&s), "after the split: {s}");
        let (left, right) = (kids[0].min(kids[1]), kids[0].max(kids[1]));
        let (code, v) = admin
            .send(
                admin
                    .http
                    .post(format!("{url}/ranges/merge?left={left}&right={right}")),
            )
            .await?;
        ensure!(code == StatusCode::OK, "merge: {code} {v}");
        until_artifact_bytes(&acme, 3 * ART_SIZE as u64).await?;
        let (_, s) = put_one(&acme).await?;
        ensure!(artifacts_limited(&s), "after the merge: {s}");
        Ok(())
    })
    .await
}

/// **An SDK ≤ v5 receives values inline: assembled up to the payload limit,
/// over the limit is an error with a stable code.**
///
/// A fake SDK with wire ≤ v5, tenant `acme`:
/// 1. No limit yet: `V/seed[a]` writes `k` = 1.5 MiB (the shell seals);
///    `V/wait[a]` (SHARED — does not hold the object's lock) reads it lazily
///    (the shell assembles 1.5 MiB — no limit yet, so it works), then suspends
///    waiting for a signal.
/// 2. Set `maxPayload` = 1 MiB.
/// 3. `V/read[a]`: a lazy read mid-turn ⇒ the assembled value is 1.5 MiB > the
///    limit.
/// 4. Signal `wait`: the new turn replays the journaled completion in the
///    **turn-opening batch** ⇒ also 1.5 MiB > the limit.
///
/// Must see: `read` and `wait` both `failed` 413, with a message starting with
/// `payload_too_large` (the same code as the upward limit) — not `completed`
/// with a frame larger than the limit, and no endless retries.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn old_sdk_gets_a_stable_error_when_the_assembled_value_exceeds_max_payload() -> Result<()> {
    const BIG: usize = 3 << 19;
    let sdk = FakeSdk::builder()
        .object(
            "V",
            "seed",
            vec![
                Step::SetState {
                    key: "k".into(),
                    value: Bytes::from(blob(BIG)),
                },
                Step::output("ok"),
            ],
        )
        .object(
            "V",
            "read",
            vec![Step::GetState { key: "k".into() }, Step::EchoCallResult],
        )
        .shared_object(
            "V",
            "wait",
            vec![
                Step::GetState { key: "k".into() },
                Step::Suspend,
                Step::EchoCallResult,
            ],
        )
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "SDK ≤ v5, over the limit", async {
        let admin = As::admin(&url);
        let acme = As::tenant(&url, "acme", "admin").await?;
        acme.register(&sdk.url).await?;
        // 1.
        let (code, v) = acme.invoke("V/seed/send?key=a", "{}").await?;
        ensure!(code.is_success(), "seed: {code} {v}");
        let s = settled(&acme, &id_of(&v)?).await?;
        ensure!(s["status"] == "completed", "seed: {s}");
        let (code, v) = acme.invoke("V/wait/send?key=a", "{}").await?;
        ensure!(code.is_success(), "wait: {code} {v}");
        let wait = id_of(&v)?;
        let t0 = std::time::Instant::now();
        loop {
            let (_, s) = acme.get(&format!("/invocations/{wait}")).await?;
            if s["status"] == "suspended" {
                break;
            }
            ensure!(t0.elapsed() < TEST_DEADLINE, "wait not suspended yet: {s}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // 2.
        set_limits(&admin, "acme", json!({ "maxPayload": 1 << 20 })).await?;
        let refused = |s: &Value, what: &str| {
            ensure!(
                s["status"] == "failed"
                    && s["code"] == 413
                    && s["message"]
                        .as_str()
                        .unwrap_or("")
                        .starts_with("payload_too_large"),
                "{what}: {}",
                s.to_string().chars().take(300).collect::<String>()
            );
            Ok(())
        };
        // 3.
        let (code, v) = acme.invoke("V/read/send?key=a", "{}").await?;
        ensure!(code.is_success(), "read: {code} {v}");
        refused(&settled(&acme, &id_of(&v)?).await?, "read mid-turn")?;
        // 4.
        let (code, v) = acme
            .send(
                acme.http
                    .post(format!("{url}/invocations/{wait}/signal"))
                    .json(&json!({ "signal": common::fake_sdk::HOLD_SIGNAL })),
            )
            .await?;
        ensure!(code == StatusCode::OK, "signal: {code} {v}");
        refused(&settled(&acme, &wait).await?, "wait at turn open")?;
        Ok(())
    })
    .await
}

/// **`max-schedules=` counts correctly when the range has more than 1 000
/// schedules of other tenants.**
///
/// 1. One range. `bravo` has 1 001 schedules; `acme` (limit 2) has two paused
///    schedules — due `u64::MAX`, at the end of the `(due, id)` index.
/// 2. `acme` creates a third schedule.
///
/// Before the fix the count listed the whole cluster on every `PUT` with a limit
/// of 1 000 rows of **every tenant** per range ⇒ `acme`'s schedules fell
/// outside the limit, the count came out as 0, and the third schedule slipped
/// through.
///
/// Must see: `409 schedules_limit` (within ≤ one counting beat of the node).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_schedules_counts_past_other_tenants_rows() -> Result<()> {
    let sdk = napper().spawn().await?;
    let bucket = common::bucket();
    let node = authed_one_range(&bucket).await?;
    let url = node.node.url();
    within(
        Duration::from_secs(300),
        "max-schedules with many tenants",
        async {
            let acme = As::tenant(&url, "acme", "admin").await?;
            let bravo = As::tenant(&url, "bravo", "admin").await?;
            acme.register(&sdk.url).await?;
            bravo.register(&sdk.url).await?;
            set_limits(&As::admin(&url), "acme", json!({ "maxSchedules": 2 })).await?;
            let put = |who: &As, name: String, paused: bool| {
                let who = who.clone();
                let url = url.clone();
                async move {
                    who.send(
                        who.http
                            .put(format!("{url}/schedules/{name}"))
                            .json(&json!({
                                "everyMs": 60_000, "paused": paused,
                                "target": { "service": "Echo", "handler": "hi" }
                            })),
                    )
                    .await
                }
            };
            for i in 0..1_001 {
                let (code, v) = put(&bravo, format!("b-{i:04}"), false).await?;
                ensure!(code == StatusCode::OK, "bravo {i}: {code} {v}");
            }
            for name in ["a1", "a2"] {
                let (code, v) = put(&acme, name.into(), true).await?;
                ensure!(code == StatusCode::OK, "acme {name}: {code} {v}");
            }
            // A soft limit: give the node one counting beat (5 s by default) before requiring it.
            let t0 = std::time::Instant::now();
            loop {
                let (code, v) = put(&acme, "a3".into(), true).await?;
                if code == StatusCode::CONFLICT && v["code"] == "schedules_limit" {
                    break;
                }
                ensure!(code == StatusCode::OK, "acme a3: {code} {v}");
                let (code, v) = acme
                    .send(acme.http.delete(format!("{url}/schedules/a3")))
                    .await?;
                ensure!(code.is_success(), "delete a3: {code} {v}");
                ensure!(
                    t0.elapsed() < Duration::from_secs(15),
                    "acme's third schedule still slips through after 15 s (limit 2)"
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Ok(())
        },
    )
    .await
}
