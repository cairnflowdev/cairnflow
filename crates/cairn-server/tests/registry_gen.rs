// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The registry generation and authentication when a registry-change notice is lost.
//!
//! The `/internal/registry/changed` notice is best-effort; its net is the registry
//! generation (`control/registry-gen`) and a full 10-minute reconciliation. The tests here
//! block the notice with the network harness (`--features testkit`, `required-features`)
//! then break exactly the generation bump, and require the change to still reach peers within a
//! few poll ticks rather than at the reconciliation.
//!
//! Each race test opens with a doc comment describing the interleaving it reproduces
//! (see AGENTS.md).

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{anyhow, ensure, Result},
    cairn_server::netfilter::NetFilter,
    cairn_store::testkit::{Action, Ambiguity, OpSet, PathMatch, Rule},
    common::{
        net::{Net, PathFilter},
        Bucket, ClusterNode,
    },
    std::{sync::Arc, time::Duration},
};

/// The registry refresh cadence of the tests here (to be quick).
const TICK: Duration = Duration::from_millis(200);

/// `ids.len()` nodes on a fresh bucket that is verified on cleanup ([`common::bucket`]),
/// sharing one [`Net`]; each node refreshes the registry every [`TICK`], further configured with
/// `tweak`.
async fn cluster(
    ids: &[&str],
    tweak: impl Fn(&mut cairn_server::node::NodeConfig),
) -> Result<(Bucket, Arc<Net>, Vec<ClusterNode>)> {
    let shared = common::bucket();
    let net = Net::new();
    let mut nodes = Vec::new();
    for id in ids {
        let filter: Arc<dyn NetFilter> = net.clone();
        let n = common::start_cluster_node_tweaked(&shared, id, 2, Duration::from_secs(2), |cfg| {
            cfg.net_filter = Some(filter);
            cfg.deploy_refresh = TICK;
            tweak(cfg);
        })
        .await?;
        net.register(id, &n.node);
        nodes.push(n);
    }
    Ok((shared, net, nodes))
}

/// The number of deployments `GET /deployments` on `url` lists.
async fn deployments(url: &str) -> Result<usize> {
    let listed: serde_json::Value = reqwest::get(format!("{url}/deployments"))
        .await?
        .json()
        .await?;
    listed
        .as_array()
        .map(Vec::len)
        .ok_or_else(|| anyhow!("{url}/deployments is not an array: {listed}"))
}

/// Waits until `url` lists all `n` deployments, at most `within`.
async fn wait_deployments(url: &str, n: usize, within: Duration) -> Result<()> {
    let t0 = std::time::Instant::now();
    loop {
        let got = deployments(url).await?;
        if got >= n {
            return Ok(());
        }
        ensure!(
            t0.elapsed() < within,
            "{url} lists only {got}/{n} deployments after {within:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Registers a new deployment on `url` (a separate fake SDK each time ⇒ a separate
/// id).
async fn register_new(url: &str, service: &str) -> Result<common::fake_sdk::FakeSdk> {
    let sdk = common::fake_sdk::FakeSdk::builder()
        .object(service, "run", vec![common::fake_sdk::Step::output("x")])
        .spawn()
        .await?;
    common::register(url, &sdk.url).await?;
    Ok(sdk)
}

/// A failed registry-generation bump does not leave the change stuck until the
/// reconciliation.
///
/// Formerly a failed `control/registry-gen` bump was "an error, so be it": the notice
/// still went out, and the full 10-minute reconciliation caught the rest. If the notice was
/// lost too, peers waited the full 10 minutes.
///
/// 1. Two nodes `a`, `b`, registry refresh every 200 ms, default reconciliation (10 minutes).
///    The `/internal/registry/changed` notice to `b` is dropped.
/// 2. Register deployment X on `a`; `b` sees X through the registry generation (the object exists from here).
/// 3. `a`'s store fails the next three PUTs to `control/registry-gen` (`Fail`;
///    the second time: an ambiguous one — written but returning an error), then heals.
/// 4. Register deployment Y on `a`: the main write succeeds, the bump right after it fails.
///
/// Must see: `b` lists Y within 5 s (`a`'s background retry wins once the
/// store heals; `b` sees the generation change at its next poll tick).
#[tokio::test(flavor = "multi_thread")]
async fn failed_generation_bump_is_retried_until_it_lands() -> Result<()> {
    let (_bucket, net, nodes) = cluster(&["a", "b"], |_| {}).await?;
    let (a, b) = (&nodes[0], &nodes[1]);
    net.add(PathFilter::new("/internal/registry/changed").to("b"));
    let mut sdks = vec![register_new(&a.node.url(), "X").await?];
    wait_deployments(&b.node.url(), 1, Duration::from_secs(5)).await?;

    let faults = [
        Action::Fail { times: Some(3) },
        Action::Ambiguous {
            reply: Ambiguity::Error,
            skip: 0,
            times: Some(1),
        },
    ];
    let mut r = Ok(());
    for (i, action) in faults.into_iter().enumerate() {
        a.store.add_rule(Rule::new(
            OpSet::PUT,
            PathMatch::Contains("control/registry-gen".into()),
            action.clone(),
        ));
        sdks.push(register_new(&a.node.url(), &format!("Y{i}")).await?);
        r = wait_deployments(&b.node.url(), 2 + i, Duration::from_secs(5))
            .await
            .map_err(|e| anyhow!("{action:?} on the bump: {e}"));
        if r.is_err() {
            break;
        }
    }
    drop(sdks);
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

const ADMIN_KEY: &str = "cf_test_registry_gen_bootstrap_key_0123";

fn authed(cfg: &mut cairn_server::node::NodeConfig) {
    cfg.auth = cairn_server::auth::AuthConfig {
        required: true,
        admin_key: Some(ADMIN_KEY.into()),
        cluster_secret: None,
        metrics_key: None,
        admin_key_sha256: None,
        provision_key_sha256: None,
    };
}

/// Creates an `invoke` key of tenant `default` on `url`; returns `(id, secret)`.
async fn create_key(url: &str) -> Result<(String, String)> {
    let r = reqwest::Client::new()
        .post(format!("{url}/admin/keys"))
        .bearer_auth(ADMIN_KEY)
        .json(&serde_json::json!({ "tenant": "default", "role": "invoke" }))
        .send()
        .await?;
    ensure!(r.status().is_success(), "create key: {}", r.status());
    let v: serde_json::Value = r.json().await?;
    Ok((
        v["id"].as_str().unwrap_or_default().to_string(),
        v["secret"].as_str().unwrap_or_default().to_string(),
    ))
}

/// The code `url` returns for secret `secret` on a call path (no such service ⇒ `404`
/// when the key is valid, `401` when not).
async fn status_with(url: &str, secret: &str) -> Result<u16> {
    Ok(reqwest::Client::new()
        .post(format!("{url}/invoke/NoSuchService/run"))
        .bearer_auth(secret)
        .send()
        .await?
        .status()
        .as_u16())
}

/// Waits until `url` returns `want` for `secret`, at most `within`.
async fn wait_status(url: &str, secret: &str, want: u16, within: Duration) -> Result<()> {
    let t0 = std::time::Instant::now();
    loop {
        let got = status_with(url, secret).await?;
        if got == want {
            return Ok(());
        }
        ensure!(
            t0.elapsed() < within,
            "{url} still returns {got} (want {want}) after {within:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Key revocation takes effect synchronously.
///
/// Formerly `DELETE /admin/keys/{id}` returned `204` as soon as the receiving node had
/// written the bucket; peers stopped accepting the key when the notice (best-effort, no ack) nudged
/// their refresh loop — or after a tick, or after reconciliation if the registry generation
/// failed too. The operator did not know when the key really stopped working.
///
/// 1. Two nodes `a`, `b`, auth on, registry refresh every 200 ms. Create key K1 on `a`;
///    wait for `b` to accept K1.
/// 2. Revoke K1 on `a`, network healthy.
/// 3. Create key K2 on `a`; wait for `b` to accept K2. Block every
///    `/internal/registry/…` notice to `b`. Revoke K2 on `a`.
///
/// Must see: (2) returns `204`, and **at** the very first question after that `b` refuses
/// K1 (`401`) — `204` means every live session has applied it; (3) returns `202` with
/// `pending` = `["b"]` (b did not ack within the limit), and `b` still refuses K2 within
/// a few ticks (through the registry generation).
#[tokio::test(flavor = "multi_thread")]
async fn key_revocation_returns_only_after_every_live_node_applied_it() -> Result<()> {
    let (_bucket, net, nodes) = cluster(&["a", "b"], authed).await?;
    let (a, b) = (nodes[0].node.url(), nodes[1].node.url());
    let http = reqwest::Client::new();
    let r = async {
        let (k1, s1) = create_key(&a).await?;
        wait_status(&b, &s1, 404, Duration::from_secs(5)).await?;
        let resp = http
            .delete(format!("{a}/admin/keys/{k1}"))
            .bearer_auth(ADMIN_KEY)
            .send()
            .await?;
        ensure!(
            resp.status() == 204,
            "revoke K1, network healthy: {}",
            resp.status()
        );
        let first = status_with(&b, &s1).await?;
        ensure!(
            first == 401,
            "b accepts K1 ({first}) after the revocation returned 204"
        );

        let (k2, s2) = create_key(&a).await?;
        wait_status(&b, &s2, 404, Duration::from_secs(5)).await?;
        net.add(PathFilter::new("/internal/registry/").to("b"));
        let resp = http
            .delete(format!("{a}/admin/keys/{k2}"))
            .bearer_auth(ADMIN_KEY)
            .send()
            .await?;
        let code = resp.status().as_u16();
        let body: serde_json::Value = resp.json().await.unwrap_or_default();
        ensure!(
            code == 202 && body["pending"] == serde_json::json!(["b"]),
            "revoke K2 while b does not ack: {code} {body}"
        );
        wait_status(&b, &s2, 401, Duration::from_secs(3)).await
    }
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// A node that cannot confirm the registry is fresh closes (fail-closed).
///
/// Formerly a node cut off from the bucket and from its peers kept accepting keys by its
/// in-RAM registry — even keys revoked on another node — for any length of time.
///
/// 1. Two nodes `a`, `b`, auth on, registry refresh every 200 ms. Create key K on `a`;
///    `b` accepts K.
/// 2. Cut `b` off: every GET of `control/registry-gen` and `auth/keys` by `b` fails, and
///    `b` is cut off from every peer (it does not hear `view`, does not get notices).
/// 3. Heal.
/// 4. A lone node `s`: its own GETs of `control/registry-gen` and `auth/keys`
///    fail, then heal. (A failing generation GET alone does not close: the full round re-reading
///    `auth/keys` successfully is also a confirmation.)
///
/// Must see: after (2), within 3 s `b` returns `503` for K (over 2·tick since the last
/// confirmation); the bootstrap key (not in the registry) still passes. After (3) `b` opens
/// again (`404` for K) within 3 s. (4): the same on a lone node.
#[tokio::test(flavor = "multi_thread")]
async fn node_that_cannot_confirm_a_fresh_registry_fails_closed() -> Result<()> {
    let cut = |store: &cairn_store::testkit::FaultyStore| {
        ["control/registry-gen", "auth/keys"].map(|p| {
            store.add_rule(Rule::new(
                OpSet::GET,
                PathMatch::Contains(p.into()),
                Action::Fail { times: None },
            ))
        })
    };
    let bootstrap_passes = |url: String| async move {
        let r = reqwest::Client::new()
            .get(format!("{url}/cluster"))
            .bearer_auth(ADMIN_KEY)
            .send()
            .await?;
        ensure!(
            r.status() != 503,
            "the bootstrap key is closed on {url}: {}",
            r.status()
        );
        Ok::<_, anyhow::Error>(())
    };
    let (_bucket, net, nodes) = cluster(&["a", "b"], authed).await?;
    let (a, b) = (&nodes[0], &nodes[1]);
    let r = async {
        let (_, k) = create_key(&a.node.url()).await?;
        wait_status(&b.node.url(), &k, 404, Duration::from_secs(5)).await?;
        let rules = cut(&b.store);
        let isolated = net.add(common::net::isolate("b"));
        wait_status(&b.node.url(), &k, 503, Duration::from_secs(3))
            .await
            .map_err(|e| anyhow!("b cut off from bucket and peers: {e}"))?;
        bootstrap_passes(b.node.url()).await?;
        for id in rules {
            b.store.remove_rule(id);
        }
        net.remove(isolated);
        wait_status(&b.node.url(), &k, 404, Duration::from_secs(3))
            .await
            .map_err(|e| anyhow!("b healed: {e}"))
    }
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r?;

    let (_bucket, _net, nodes) = cluster(&["s"], authed).await?;
    let s = &nodes[0];
    let r = async {
        let (_, k) = create_key(&s.node.url()).await?;
        wait_status(&s.node.url(), &k, 404, Duration::from_secs(5)).await?;
        let rules = cut(&s.store);
        wait_status(&s.node.url(), &k, 503, Duration::from_secs(3))
            .await
            .map_err(|e| anyhow!("lone node, generation GET failing: {e}"))?;
        bootstrap_passes(s.node.url()).await?;
        for id in rules {
            s.store.remove_rule(id);
        }
        wait_status(&s.node.url(), &k, 404, Duration::from_secs(3))
            .await
            .map_err(|e| anyhow!("lone node healed: {e}"))
    }
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// **A node cut off from the bucket does not accept a key whose revocation was acked.**
///
/// The revoking node answers `2xx` once every live peer has applied the revocation, and
/// stops waiting for a peer whose session record has ended on the bucket. A record ended
/// by an observer does not stop that peer's process: it keeps serving requests from its
/// in-RAM registry until its copy is older than 2 refresh ticks.
///
/// 1. Two nodes `a`, `b`, TTL 2 s, registry refresh every 20 s (so the age limit, 40 s,
///    is far above the TTL). Create key K on `a`; `b` accepts K.
/// 2. Cut `b` off: every store operation of `b` fails (its session can no longer be
///    renewed) and `b` is cut off from every peer — but `b` still answers clients.
/// 3. Revoke K on `a`: `b` cannot apply it; `a` keeps asking until `b`'s record turns
///    `expired`, then answers `204`.
///
/// Must see: `a` answers `204`, and from then on `b` never accepts K (no `404`): its grant
/// lapsed before the record could turn `expired`, so it no longer counts its copy fresh.
#[tokio::test(flavor = "multi_thread")]
async fn bucket_cut_node_refuses_a_revoked_key_once_the_revocation_is_acked() -> Result<()> {
    let (_bucket, net, nodes) = cluster(&["a", "b"], |cfg| {
        authed(cfg);
        cfg.deploy_refresh = Duration::from_secs(20);
    })
    .await?;
    let (a, b) = (&nodes[0], &nodes[1]);
    let r = async {
        let (id, k) = create_key(&a.node.url()).await?;
        wait_status(&b.node.url(), &k, 404, Duration::from_secs(5)).await?;
        let cut = b.store.add_rule(Rule::new(
            OpSet::ALL,
            PathMatch::Any,
            Action::Fail { times: None },
        ));
        let isolated = net.add(common::net::isolate("b"));
        let resp = reqwest::Client::new()
            .delete(format!("{}/admin/keys/{id}", a.node.url()))
            .bearer_auth(ADMIN_KEY)
            .send()
            .await?;
        let code = resp.status().as_u16();
        let body: serde_json::Value = resp.json().await.unwrap_or_default();
        ensure!(
            code == 204,
            "revoke K while b is cut off: {code} {body} (b's record should have turned expired within the confirm bound)"
        );
        let got = status_with(&b.node.url(), &k).await?;
        ensure!(
            got != 404,
            "b accepts K ({got}) after the revocation returned 204"
        );
        b.store.remove_rule(cut);
        net.remove(isolated);
        Ok(())
    }
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// Whether `node` leads at least one range (a status probe answers locally).
async fn leads_any(node: &cairn_server::node::RunningNode) -> bool {
    for r in node.cluster.table().ranges() {
        let probe = cairn_store::make_invocation_id(cairn_store::PartitionKey(r.start), [0; 16]);
        if let Some(h) = node.partitions.get(r.id) {
            if matches!(
                tokio::time::timeout(Duration::from_millis(300), h.status(probe)).await,
                Ok(Ok(_))
            ) {
                return true;
            }
        }
    }
    false
}

/// Makes `b` lose its session and open a new incarnation in-process: its session-record
/// operations fail and it is cut off from its peers until its incarnation changes, then
/// both heal. `during` runs while `b` is cut off. Returns once the new incarnation is live.
async fn reincarnate<F, Fut>(net: &Arc<Net>, b: &ClusterNode, during: F) -> Result<()>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let s1 = b.node.session.current();
    let cut = b.store.add_rule(Rule::new(
        OpSet::ALL,
        PathMatch::Contains("sessions/".into()),
        Action::Fail { times: None },
    ));
    let isolated = net.add(common::net::isolate("b"));
    during().await?;
    let t0 = std::time::Instant::now();
    while b.node.session.current() == s1 {
        ensure!(
            t0.elapsed() < Duration::from_secs(30),
            "b kept its session while cut off"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    b.store.remove_rule(cut);
    net.remove(isolated);
    let t0 = std::time::Instant::now();
    loop {
        let now = b.node.session.current();
        if now.is_some() && now != s1 {
            return Ok(());
        }
        ensure!(
            t0.elapsed() < Duration::from_secs(30),
            "b did not open a new incarnation"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Waits until `node` leads a range, at most `within`.
async fn wait_leads(node: &cairn_server::node::RunningNode, within: Duration) -> Result<()> {
    let t0 = std::time::Instant::now();
    while !leads_any(node).await {
        ensure!(
            t0.elapsed() < within,
            "{} leads no range after {within:?}",
            node.url()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Ok(())
}

/// **A catalog pointer to a deployment that cannot be found does not keep a
/// re-incarnated node from taking ranges.**
///
/// A new incarnation takes ranges only after a full refresh round. A round whose
/// deployment registry stays "incomplete" for a reason a retry cannot fix (a pointer to a
/// deployment object that is gone or unreadable) must not count as a failed read, or one
/// tenant's bad object would keep every re-incarnated node closed until restart.
///
/// 1. Two nodes `a`, `b`, refresh every 20 s; `b` leads a range.
/// 2. `b` is cut off (session operations failing, isolated from `a`). Meanwhile register
///    deployment X on `a`, then delete X's object from the bucket: `a`'s catalog now points
///    at a deployment that does not exist, and `b` has not read that catalog yet.
/// 3. `b`'s incarnation changes; it heals.
///
/// Must see: `b` leads a range again within 30 s.
#[tokio::test(flavor = "multi_thread")]
async fn dangling_catalog_pointer_does_not_keep_a_reincarnated_node_out() -> Result<()> {
    use {
        futures::TryStreamExt,
        slatedb::object_store::{ObjectStore, ObjectStoreExt},
    };
    let (bucket, net, nodes) = cluster(&["a", "b"], |cfg| {
        cfg.deploy_refresh = Duration::from_secs(20);
    })
    .await?;
    let (a, b) = (&nodes[0], &nodes[1]);
    let r = async {
        wait_leads(&b.node, Duration::from_secs(20)).await?;
        let mut sdk = None;
        reincarnate(&net, b, || async {
            sdk = Some(register_new(&a.node.url(), "Dangling").await?);
            let store: Arc<dyn ObjectStore> = bucket.clone();
            let listed: Vec<_> = store.list(None).try_collect().await?;
            let mut gone = 0;
            for m in listed {
                if m.location.as_ref().contains("deployments/") {
                    store.delete(&m.location).await?;
                    gone += 1;
                }
            }
            ensure!(gone > 0, "no deployment object to delete");
            Ok(())
        })
        .await?;
        wait_leads(&b.node, Duration::from_secs(30)).await
    }
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    bucket.allow(
        "deployments",
        "the test deletes a deployment object a catalog points at",
    );
    r
}

/// **A node that re-incarnated in-process takes no range before it has fully re-read the
/// registry.**
///
/// A synchronous registry write (tenant state, deployment `DELETE`) stops waiting for a
/// peer whose session record has ended. That peer may open a new incarnation in the
/// same process — without the boot sequence that reads the registry before serving — and
/// its range engines would admit work and pin deployments from its old copy.
///
/// 1. Two nodes `a`, `b`, TTL 2 s, refresh every 20 s; `b` leads a range.
/// 2. `b`'s reads of `tenants` start failing (and keep failing).
/// 3. `b` loses its session: its session-record operations fail and it is cut off from
///    `a`, until its incarnation has changed; then both heal.
///
/// Must see: for 8 s after `b`'s new incarnation opens, `b` leads no range (it cannot
/// complete a full refresh round); once `tenants` reads heal, `b` leads a range again.
#[tokio::test(flavor = "multi_thread")]
async fn reincarnated_node_takes_no_range_before_a_full_registry_round() -> Result<()> {
    let (_bucket, net, nodes) = cluster(&["a", "b"], |cfg| {
        authed(cfg);
        cfg.deploy_refresh = Duration::from_secs(20);
    })
    .await?;
    let b = &nodes[1];
    let r = async {
        let t0 = std::time::Instant::now();
        while !leads_any(&b.node).await {
            ensure!(
                t0.elapsed() < Duration::from_secs(20),
                "b never led a range"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let s1 = b.node.session.current();
        let tenants = b.store.add_rule(Rule::new(
            OpSet::GET,
            PathMatch::Contains("tenants".into()),
            Action::Fail { times: None },
        ));
        let cut = b.store.add_rule(Rule::new(
            OpSet::ALL,
            PathMatch::Contains("sessions/".into()),
            Action::Fail { times: None },
        ));
        let isolated = net.add(common::net::isolate("b"));
        let t0 = std::time::Instant::now();
        while b.node.session.current() == s1 {
            ensure!(
                t0.elapsed() < Duration::from_secs(30),
                "b kept its session while cut off"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        b.store.remove_rule(cut);
        net.remove(isolated);
        let t0 = std::time::Instant::now();
        loop {
            let now = b.node.session.current();
            if now.is_some() && now != s1 {
                break;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(30),
                "b did not open a new incarnation"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let t0 = std::time::Instant::now();
        while t0.elapsed() < Duration::from_secs(8) {
            ensure!(
                !leads_any(&b.node).await,
                "b leads a range under its new incarnation without having re-read tenants"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        b.store.remove_rule(tenants);
        let t0 = std::time::Instant::now();
        while !leads_any(&b.node).await {
            ensure!(
                t0.elapsed() < Duration::from_secs(40),
                "b took no range after tenants healed"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Ok(())
    }
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// **An unreadable key registry closes**: this is why
/// a `/2` registry must wait for the `auth/keys-v2` gate. A binary that only knows `/1` meeting a newer
/// registry does not hand out guessed roles to any key — it stops confirming freshness and returns `503`
/// for every stored key, including through session cookies; the bootstrap key (not in
/// the registry) still passes.
///
/// 1. Node `s`, auth on. Create an `invoke` key K, log in to the console with K.
/// 2. Overwrite `auth/keys` with a `cairnflow-keys/3` registry (a header newer than anything
///    this binary understands), then bump the registry generation like every registry writer.
/// 3. Write the old registry back, bump the generation.
///
/// Must see: after (2), within 3 s K returns `503` through Bearer and through the cookie; the
/// bootstrap key is not `503`. After (3), K opens again (`404`) within 3 s.
#[tokio::test(flavor = "multi_thread")]
async fn unreadable_key_book_fails_closed() -> Result<()> {
    use slatedb::object_store::{path::Path, ObjectStore, ObjectStoreExt, PutPayload};
    let (_bucket, _net, nodes) = cluster(&["s"], authed).await?;
    let s = &nodes[0];
    let url = s.node.url();
    let store: Arc<dyn ObjectStore> = s.store.clone();
    let bump = || {
        let store = store.clone();
        async move {
            let gen = cairn_server::registry_gen::RegistryGen::new(store, &Path::from(""), None);
            let open = gen.begin().await.map_err(|e| anyhow!("{e}"))?;
            gen.seal(open).await.map_err(|e| anyhow!("{e}"))
        }
    };
    let cookie_status = |cookie: String| {
        let url = url.clone();
        async move {
            Ok::<_, anyhow::Error>(
                reqwest::Client::new()
                    .get(format!("{url}/auth/whoami"))
                    .header(reqwest::header::COOKIE, cookie)
                    .send()
                    .await?
                    .status()
                    .as_u16(),
            )
        }
    };
    let r = async {
        let (_, k) = create_key(&url).await?;
        wait_status(&url, &k, 404, Duration::from_secs(5)).await?;
        let login = reqwest::Client::new()
            .post(format!("{url}/ui/session"))
            .header(reqwest::header::ORIGIN, url.clone())
            .json(&serde_json::json!({ "key": k }))
            .send()
            .await?;
        ensure!(login.status() == 200, "login: {}", login.status());
        let cookie = login
            .headers()
            .get(reqwest::header::SET_COOKIE)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split(';').next())
            .map(str::to_string)
            .ok_or_else(|| anyhow!("no Set-Cookie"))?;
        ensure!(cookie_status(cookie.clone()).await? == 200);

        let path = Path::from("auth/keys");
        let original = store.get(&path).await?.bytes().await?;
        store
            .put(&path, PutPayload::from("cairnflow-keys/3\nstrange\n"))
            .await?;
        bump().await?;
        wait_status(&url, &k, 503, Duration::from_secs(3))
            .await
            .map_err(|e| anyhow!("registry /3: {e}"))?;
        ensure!(
            cookie_status(cookie.clone()).await? == 503,
            "K's cookie still passes while the registry is unreadable"
        );
        let boot = reqwest::Client::new()
            .get(format!("{url}/cluster"))
            .bearer_auth(ADMIN_KEY)
            .send()
            .await?
            .status();
        ensure!(boot != 503, "the bootstrap key is closed: {boot}");

        store.put(&path, PutPayload::from(original)).await?;
        bump().await?;
        wait_status(&url, &k, 404, Duration::from_secs(3))
            .await
            .map_err(|e| anyhow!("registry healed: {e}"))
    }
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// `GET /admin/keys/{id}/revocation` on `url` with the bootstrap key: `(code, body)`.
async fn revocation(url: &str, id: &str) -> Result<(u16, serde_json::Value)> {
    let r = reqwest::Client::new()
        .get(format!("{url}/admin/keys/{id}/revocation"))
        .bearer_auth(ADMIN_KEY)
        .send()
        .await?;
    let code = r.status().as_u16();
    Ok((code, r.json().await.unwrap_or_default()))
}

/// Confirming a revocation again after `202 {pending}` (for a control plane).
///
/// Formerly a caller receiving `202 {pending: ["b"]}` had no way to ask again:
/// a repeated `DELETE` returned `404` (the key was already out of the registry), so a control plane
/// could only guess by the clock (`2 × --deploy-refresh-ms` + a margin).
///
/// 1. Two nodes `a`, `b` with the same cluster secret, auth on, registry refresh every 200 ms.
///    Create key K on `a`; wait for `b` to accept K.
/// 2. Block every `/internal/registry/…` notice to `b`. Revoke K on `a` ⇒ `202`
///    `pending = ["b"]`.
/// 3. Still blocked: ask `a` ⇒ `revoked`, **not yet** `confirmed`, `pending = ["b"]`.
///    Ask `b` (a load balancer sends the caller to another node): `b` re-reads the registry itself,
///    asks `a` (the `b → a` path is healthy) ⇒ `confirmed`.
/// 4. Heal the network. Ask `a` ⇒ `confirmed`, `pending` empty; `b` refuses K (`401`).
/// 5. Key K2 still in the registry ⇒ `revoked: false`. An id not in the registry (including an
///    id that never existed) ⇒ `revoked: true`: the question is "is this id still accepted on
///    any node", not "did this id ever exist".
/// 6. `a`'s LIST of `sessions/` fails ⇒ asking `a` does not give `confirmed` (`listed:
///    false`), and `DELETE` K2 on `a` returns `202`, not `204`: without listing the
///    sessions there is no knowing every node has applied it — the coordinator's `view`
///    or an existing table cannot replace the LIST (a node that just joined may be missing).
/// 7. `a`'s GET of `auth/keys` fails ⇒ `503`: without re-reading the registry there is no
///    answer at all.
///
/// Must see: exactly the codes and bodies above; never `confirmed: true` while
/// a live session has not acked or while the LIST failed.
#[tokio::test(flavor = "multi_thread")]
async fn revocation_can_be_confirmed_after_pending() -> Result<()> {
    let (_bucket, net, nodes) = cluster(&["a", "b"], authed).await?;
    let (a, b) = (nodes[0].node.url(), nodes[1].node.url());
    let http = reqwest::Client::new();
    let r = async {
        let (k, s) = create_key(&a).await?;
        wait_status(&b, &s, 404, Duration::from_secs(5)).await?;
        let blocked = net.add(PathFilter::new("/internal/registry/").to("b"));
        let resp = http
            .delete(format!("{a}/admin/keys/{k}"))
            .bearer_auth(ADMIN_KEY)
            .send()
            .await?;
        let code = resp.status().as_u16();
        let body: serde_json::Value = resp.json().await.unwrap_or_default();
        ensure!(
            code == 202 && body["pending"] == serde_json::json!(["b"]),
            "revoke K while b does not ack: {code} {body}"
        );

        let (code, v) = revocation(&a, &k).await?;
        ensure!(
            code == 200
                && v["revoked"] == true
                && v["confirmed"] == false
                && v["pending"] == serde_json::json!(["b"]),
            "ask a while b is still blocked: {code} {v}"
        );
        let (code, v) = revocation(&b, &k).await?;
        ensure!(
            code == 200 && v["revoked"] == true && v["confirmed"] == true,
            "ask b (re-reads the registry itself, a acks): {code} {v}"
        );

        net.remove(blocked);
        let (code, v) = revocation(&a, &k).await?;
        ensure!(
            code == 200
                && v["confirmed"] == true
                && v["pending"] == serde_json::json!([])
                && v["listed"] == true,
            "ask a after healing: {code} {v}"
        );
        let got = status_with(&b, &s).await?;
        ensure!(got == 401, "b accepts K ({got}) after confirmed");

        let (k2, s2) = create_key(&a).await?;
        wait_status(&b, &s2, 404, Duration::from_secs(5)).await?;
        let (code, v) = revocation(&a, &k2).await?;
        ensure!(
            code == 200 && v["revoked"] == false && v["confirmed"] == false,
            "K2 still in the registry: {code} {v}"
        );
        let (code, v) = revocation(&a, "k_never_existed").await?;
        ensure!(
            code == 200 && v["revoked"] == true && v["confirmed"] == true,
            "id not in the registry: {code} {v}"
        );

        let no_list = nodes[0].store.add_rule(Rule::new(
            OpSet::LIST,
            PathMatch::Contains("sessions".into()),
            Action::Fail { times: None },
        ));
        let (code, v) = revocation(&a, &k).await?;
        ensure!(
            code == 200 && v["confirmed"] == false && v["listed"] == false,
            "session LIST failing: {code} {v}"
        );
        let resp = http
            .delete(format!("{a}/admin/keys/{k2}"))
            .bearer_auth(ADMIN_KEY)
            .send()
            .await?;
        let code = resp.status().as_u16();
        let body: serde_json::Value = resp.json().await.unwrap_or_default();
        ensure!(
            code == 202 && body["listed"] == false,
            "DELETE K2 while the session LIST fails: {code} {body}"
        );
        nodes[0].store.remove_rule(no_list);

        let no_keys = nodes[0].store.add_rule(Rule::new(
            OpSet::GET,
            PathMatch::Contains("auth/keys".into()),
            Action::Fail { times: None },
        ));
        let (code, v) = revocation(&a, &k).await?;
        ensure!(code == 503, "GET auth/keys failing: {code} {v}");
        nodes[0].store.remove_rule(no_keys);
        Ok(())
    }
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// The console session cookie of key `secret`, logged in on `url`.
async fn console_cookie(url: &str, secret: &str) -> Result<String> {
    let login = reqwest::Client::new()
        .post(format!("{url}/ui/session"))
        .header(reqwest::header::ORIGIN, url)
        .json(&serde_json::json!({ "key": secret }))
        .send()
        .await?;
    ensure!(login.status() == 200, "login: {}", login.status());
    login
        .headers()
        .get(reqwest::header::SET_COOKIE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .map(str::to_string)
        .ok_or_else(|| anyhow!("no Set-Cookie"))
}

/// The code of `GET /auth/whoami` on `url` with `cookie`.
async fn whoami_with(url: &str, cookie: &str) -> Result<u16> {
    Ok(reqwest::Client::new()
        .get(format!("{url}/auth/whoami"))
        .header(reqwest::header::COOKIE, cookie)
        .send()
        .await?
        .status()
        .as_u16())
}

/// **An unreadable logged-out-session book does not close the keys;
/// a healthy book does not close cookie sessions by itself** (security review).
/// `auth/revoked-sessions` is written by the logouts of every tenant; formerly an error
/// reading it made `Keys::refresh` fail ⇒ past the freshness limit **every key** got `503`. The
/// session book's own freshness must follow the registry generation like the key registry —
/// otherwise the refresh loop skips reading it (the generation does not change) and
/// cookie sessions close by themselves after two ticks although the book is healthy.
///
/// 1. Node `s`, tick [`TICK`]. Key K, console cookie C of K.
/// 2. Wait 5 ticks, changing nothing.
/// 3. Overwrite `auth/revoked-sessions` with junk, bump the generation.
/// 4. Write back a valid empty book, bump the generation.
///
/// Must see: after (2) C is still `200`. After (3) K through Bearer is never `503` for
/// 5 ticks, C is closed (`503`, like the old key registry) within 3 s. After (4) C opens again within
/// 3 s.
#[tokio::test(flavor = "multi_thread")]
async fn unreadable_session_book_does_not_stale_bearer_keys() -> Result<()> {
    use slatedb::object_store::{path::Path, ObjectStore, ObjectStoreExt, PutPayload};
    let (_bucket, _net, nodes) = cluster(&["s"], authed).await?;
    let s = &nodes[0];
    let url = s.node.url();
    let store: Arc<dyn ObjectStore> = s.store.clone();
    let bump = || {
        let store = store.clone();
        async move {
            let gen = cairn_server::registry_gen::RegistryGen::new(store, &Path::from(""), None);
            let open = gen.begin().await.map_err(|e| anyhow!("{e}"))?;
            gen.seal(open).await.map_err(|e| anyhow!("{e}"))
        }
    };
    let r = async {
        let (_, k) = create_key(&url).await?;
        wait_status(&url, &k, 404, Duration::from_secs(5)).await?;
        let cookie = console_cookie(&url, &k).await?;
        tokio::time::sleep(TICK * 5).await;
        ensure!(
            whoami_with(&url, &cookie).await? == 200,
            "the session book is healthy yet the cookie session closed by itself after a few ticks"
        );
        let path = Path::from("auth/revoked-sessions");
        store.put(&path, PutPayload::from("junk\n")).await?;
        bump().await?;
        let until = tokio::time::Instant::now() + Duration::from_secs(3);
        let mut closed = false;
        while tokio::time::Instant::now() < until {
            ensure!(
                status_with(&url, &k).await? != 503,
                "a broken session book makes the Bearer key 503"
            );
            closed |= whoami_with(&url, &cookie).await? == 503;
            tokio::time::sleep(TICK / 2).await;
        }
        ensure!(closed, "unreadable session book yet cookie still open");
        store
            .put(
                &path,
                PutPayload::from(cairn_server::auth::oidc::encode_revoked_sessions(
                    &Default::default(),
                )),
            )
            .await?;
        bump().await?;
        let until = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            if whoami_with(&url, &cookie).await? == 200 {
                break Ok(());
            }
            ensure!(
                tokio::time::Instant::now() < until,
                "the session book healed yet the cookie stays closed"
            );
            tokio::time::sleep(TICK / 2).await;
        }
    }
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// **A logout reaches peers before the full reconciliation** (security review). Writing
/// `auth/revoked-sessions` did not go through the registry-generation layer: peers re-read the book only at
/// the full reconciliation (10 minutes) — a captured cookie still opened the API on another node
/// although the logout had returned `204`.
///
/// 1. Nodes `a`, `b`, tick [`TICK`]. Key K; cookies C and D (two logins)
///    on `a`; C passes on `b`.
/// 2. `DELETE <base>/session` with C on `a`.
///
/// Must see: (2) `204`; C gets `401` on `b` within 3 s; D is still `200` on `b` (another session
/// alive — C's `401` is not because `b`'s session book is considered stale).
#[tokio::test(flavor = "multi_thread")]
async fn logout_reaches_peers_before_reconcile() -> Result<()> {
    let (_bucket, _net, nodes) = cluster(&["a", "b"], authed).await?;
    let (a, b) = (nodes[0].node.url(), nodes[1].node.url());
    let r = async {
        let (_, k) = create_key(&a).await?;
        wait_status(&b, &k, 404, Duration::from_secs(5)).await?;
        let cookie = console_cookie(&a, &k).await?;
        let other = console_cookie(&a, &k).await?;
        ensure!(
            whoami_with(&b, &cookie).await? == 200,
            "cookie on b before logout"
        );
        let out = reqwest::Client::new()
            .delete(format!("{a}/ui/session"))
            .header(reqwest::header::ORIGIN, a.clone())
            .header(reqwest::header::COOKIE, cookie.clone())
            .send()
            .await?
            .status();
        ensure!(out == 204, "logout: {out}");
        let until = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            if whoami_with(&b, &cookie).await? == 401 {
                ensure!(
                    whoami_with(&b, &other).await? == 200,
                    "another session of the same key closed on b"
                );
                break Ok(());
            }
            ensure!(
                tokio::time::Instant::now() < until,
                "a peer still accepts a logged-out cookie after 3 s"
            );
            tokio::time::sleep(TICK / 2).await;
        }
    }
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// **Repeated logouts on a full book do not bump the registry
/// generation** (security review). The early filter only looked at the **tenant** limit on the RAM copy; with a
/// cluster-wide full book, the same cookie logged out repeatedly still reached `RegistrySync::write` every time —
/// two CASes of `control/registry-gen`, and every node ran a full refresh round.
///
/// 1. Node `s`. The session book has 20 tenants × 500 unexpired sessions (the whole book's limit
///    today), bump the generation so the node re-reads it.
/// 2. Key K of `default`, cookie C.
/// 3. 50 `DELETE <base>/session` with C.
///
/// Must see: the PUTs to `control/registry-gen` during (3) ≤ 2 (one generation open and seal
/// for the revocation that takes effect, if any).
#[tokio::test(flavor = "multi_thread")]
async fn logout_replay_on_a_full_book_does_not_bump_the_generation() -> Result<()> {
    use slatedb::object_store::{path::Path, ObjectStore, ObjectStoreExt, PutPayload};
    let (_bucket, _net, nodes) = cluster(&["s"], authed).await?;
    let s = &nodes[0];
    let url = s.node.url();
    let store: Arc<dyn ObjectStore> = s.store.clone();
    let r = async {
        let mut book = cairn_server::auth::oidc::SessionBook::new();
        for t in 0..20 {
            for i in 0..500 {
                book.insert(
                    format!("{t:04x}{i:028x}"),
                    cairn_server::auth::oidc::RevokedSession {
                        exp_s: 4_000_000_000,
                        tenant: format!("t{t:02}"),
                    },
                );
            }
        }
        store
            .put(
                &Path::from("auth/revoked-sessions"),
                PutPayload::from(cairn_server::auth::oidc::encode_revoked_sessions(&book)),
            )
            .await?;
        let gen =
            cairn_server::registry_gen::RegistryGen::new(store.clone(), &Path::from(""), None);
        gen.bump().await.map_err(|e| anyhow!("{e}"))?;
        let (_, k) = create_key(&url).await?;
        wait_status(&url, &k, 404, Duration::from_secs(5)).await?;
        let cookie = console_cookie(&url, &k).await?;
        tokio::time::sleep(TICK * 3).await;
        s.store.record_writes();
        for _ in 0..50 {
            reqwest::Client::new()
                .delete(format!("{url}/ui/session"))
                .header(reqwest::header::ORIGIN, url.clone())
                .header(reqwest::header::COOKIE, cookie.clone())
                .send()
                .await?;
        }
        let bumps = s
            .store
            .take_writes()
            .iter()
            .filter(|(_, p)| p.as_ref().ends_with("control/registry-gen"))
            .count();
        ensure!(
            bumps <= 2,
            "50 logouts of the same cookie wrote the registry generation {bumps} times"
        );
        Ok(())
    }
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}
