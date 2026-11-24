// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! HTTP for the operator's observability views.
//!
//! - `GET /stats/handlers?window=&tenant=[&scope=cluster]` — per-handler summary.
//! - `GET /invocations?scope=cluster` — the cluster-wide part (the local part is in
//!   `ingress/invocations.rs`).
//! - `POST /admin/capture`, `GET /admin/capture`, `GET /admin/capture/{tenant}/{id}`
//!   — capture the next runs.
//! - `GET /admin/quarantine`, `DELETE /admin/quarantine/{tenant}/{service}/{handler}`
//!   — the quarantine list of poison handlers.
//!
//! The cluster-wide version of every view goes through **one** fan-out function,
//! [`fan_out`] (inspired by TiDB's cluster tables: write a view for one node once, the
//! cluster-wide version is a fan-out plus a node column). A node that does not answer
//! is named in `missing` and the response carries `partial: true` — its part is never
//! guessed.

use {
    super::{acting_tenant, ApiError, ApiResult, AppState, InvocationView},
    crate::{
        capture::{CaptureError, CaptureSpec},
        stats::HandlerRow,
    },
    axum::{
        extract::{Path, Query, State},
        http::{header, StatusCode},
        response::{IntoResponse, Response},
        Json,
    },
    cairn_store::entropy::HashMap,
    serde::{de::DeserializeOwned, Deserialize, Serialize},
    std::time::Duration,
};

/// Cap on waiting for one node in a fan-out.
const FAN_OUT_TIMEOUT: Duration = Duration::from_secs(5);

fn wall_ms() -> u64 {
    cairn_store::entropy::now_ms()
}

// ─────────────────────────────────────────────────────────── fan-out

/// The answers of the **other** nodes in one fan-out.
pub(super) struct Fanned<T> {
    /// `(node, its part)`.
    pub parts: Vec<(String, T)>,
    /// Nodes in the routing table that could not answer (no address, no cluster
    /// secret, network error, 5 s elapsed, error status, broken JSON).
    pub missing: Vec<String>,
}

impl AppState {
    pub(super) fn me(&self) -> String {
        self.router
            .node_id
            .clone()
            .unwrap_or_else(|| "local".into())
    }
}

/// Asks `GET path?query` on the **internal port** of every other node leading at least
/// one range per the routing table, in parallel, HMAC-signed with the cluster secret,
/// capped at 5 s per node. The pattern of `cluster_pinned`, gathered in one place for
/// every cluster-wide view. Never called on the hot path.
pub(super) async fn fan_out<T: DeserializeOwned>(
    st: &AppState,
    path: &str,
    query: &[(&str, String)],
) -> Fanned<T> {
    let me = st.router.node_id.as_deref();
    let mut peers: std::collections::BTreeMap<String, Option<String>> = Default::default();
    for (_, leader) in st.router.routing.snapshot() {
        if Some(leader.node_id.as_str()) == me {
            continue;
        }
        let slot = peers.entry(leader.node_id).or_default();
        if slot.is_none() {
            *slot = leader.address;
        }
    }
    let mut out = Fanned {
        parts: Vec::new(),
        missing: Vec::new(),
    };
    let Some(secret) = st.cluster_secret.clone() else {
        out.missing = peers.into_keys().collect();
        return out;
    };
    let asks = peers.into_iter().map(|(node, addr)| {
        let (client, secret) = (st.client.clone(), secret.clone());
        async move {
            let Some(addr) = addr else {
                return (node, None);
            };
            let base = format!("{}{path}", addr.trim_end_matches('/'));
            // No parameters ⇒ no stray `?`: the signed path must equal the received path.
            let url = if query.is_empty() {
                reqwest::Url::parse(&base)
            } else {
                reqwest::Url::parse_with_params(&base, query)
            };
            let Ok(url) = url else {
                return (node, None);
            };
            let req = client.get(url).timeout(FAN_OUT_TIMEOUT);
            let part = match secret.send(&client, req).await {
                Ok(resp) if resp.status().is_success() => resp.json::<T>().await.ok(),
                Ok(resp) => {
                    tracing::debug!(node, status = %resp.status(), path, "fan-out: node returned an error");
                    None
                }
                Err(e) => {
                    tracing::debug!(node, error = %e, path, "fan-out: cannot ask node");
                    None
                }
            };
            (node, part)
        }
    });
    for (node, part) in futures::future::join_all(asks).await {
        match part {
            Some(p) => out.parts.push((node, p)),
            None => out.missing.push(node),
        }
    }
    out
}

pub(super) fn parse_scope(q: &HashMap<String, String>) -> ApiResult<bool> {
    match q.get("scope").map(String::as_str) {
        None | Some("local") => Ok(false),
        Some("cluster") => Ok(true),
        Some(other) => Err(ApiError(
            StatusCode::BAD_REQUEST,
            format!("scope {other:?} does not exist; choose local or cluster"),
        )),
    }
}

// ─────────────────────────────────────────── summary theo handler (6.2)

/// One node's part of `/stats/handlers`.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct NodeStats {
    node: String,
    from_ms: u64,
    to_ms: u64,
    windows: u64,
    handlers: Vec<HandlerRow>,
}

/// `?window=` → a span: `5m`, `15m`, `1h`, or a bare number of minutes. From 1 minute
/// to 1 hour (12 windows in RAM); default 5 minutes.
fn parse_window(q: &HashMap<String, String>) -> ApiResult<Duration> {
    let Some(raw) = q.get("window") else {
        return Ok(Duration::from_millis(crate::stats::WINDOW_MS));
    };
    let bad = || {
        ApiError(
            StatusCode::BAD_REQUEST,
            format!("window {raw:?} is invalid: use 5m, 15m, 1h (1 minute to 1 hour)"),
        )
    };
    let (n, unit) = match raw.strip_suffix('h') {
        Some(n) => (n, 60),
        None => (raw.strip_suffix('m').unwrap_or(raw), 1),
    };
    let minutes = n.parse::<u64>().map_err(|_| bad())? * unit;
    if !(1..=60).contains(&minutes) {
        return Err(bad());
    }
    Ok(Duration::from_secs(minutes * 60))
}

/// The tenants a stats read may see. A tenant's key: only that tenant (`?tenant=` ⇒
/// `403`, like every admin route). Cluster-admin: `?tenant=` if given, otherwise
/// **every** tenant — the operator asks "which handler is slow" across the cluster,
/// not on the `default` tenant.
fn stats_tenant(
    who: &crate::auth::Identity,
    q: &HashMap<String, String>,
) -> ApiResult<Option<cairn_store::TenantId>> {
    if q.contains_key("tenant") || !who.all_tenants() {
        return acting_tenant(who, q).map(Some);
    }
    Ok(None)
}

fn local_stats(st: &AppState, span: Duration, tenant: Option<&str>) -> ApiResult<NodeStats> {
    let stats = st.handler_stats.as_ref().ok_or_else(|| {
        ApiError(
            StatusCode::NOT_IMPLEMENTED,
            "this node has no per-handler summary".into(),
        )
    })?;
    let v = stats.view(span, tenant);
    let node = st.me();
    // Quarantine: `observe` and every other action show on the identity's line.
    let quarantined = stats.quarantine().snapshot();
    let now = wall_ms();
    Ok(NodeStats {
        handlers: v
            .rows
            .into_iter()
            .map(|r| HandlerRow {
                node: Some(node.clone()),
                quarantine: quarantined
                    .find(&r.tenant, &r.service, &r.handler, &r.deployment, now)
                    .map(|e| e.action.as_str().to_string()),
                ..r
            })
            .collect(),
        node,
        from_ms: v.from_ms,
        to_ms: v.to_ms,
        windows: v.windows,
    })
}

/// `GET /stats/handlers?window=5m&tenant=&scope=local|cluster` (`admin` role) — summary
/// by `(tenant, service, handler, deployment)`, each line carrying `node`. Cluster-wide:
/// each node's lines, not summed.
pub(super) async fn stats_handlers(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<serde_json::Value>> {
    let cluster = parse_scope(&q)?;
    let span = parse_window(&q)?;
    let tenant = stats_tenant(&who, &q)?;
    let local = local_stats(&st, span, tenant.as_ref().map(|t| t.as_str()))?;
    let mut nodes = vec![local.node.clone()];
    let mut rows = local.handlers;
    let (mut from, mut windows) = (local.from_ms, local.windows);
    let mut missing = Vec::new();
    if cluster {
        let mut query = vec![("spanMs", (span.as_millis() as u64).to_string())];
        if let Some(t) = &tenant {
            query.push(("tenant", t.to_string()));
        }
        let fanned = fan_out::<NodeStats>(&st, "/internal/stats/handlers", &query).await;
        for (node, part) in fanned.parts {
            nodes.push(node.clone());
            from = from.min(part.from_ms);
            windows = windows.max(part.windows);
            rows.extend(part.handlers.into_iter().map(|r| HandlerRow {
                node: Some(node.clone()),
                ..r
            }));
        }
        missing = fanned.missing;
        rows.sort_by_key(|r| std::cmp::Reverse(r.run_ms));
    }
    Ok(Json(serde_json::json!({
        "scope": if cluster { "cluster" } else { "local" },
        "windowMs": span.as_millis() as u64,
        "fromMs": from,
        "toMs": local.to_ms,
        "windows": windows,
        "nodes": nodes,
        "partial": !missing.is_empty(),
        "missing": missing,
        "handlers": rows,
    })))
}

/// `GET /internal/stats/handlers?spanMs=&tenant=` — this node's part. Internal: the
/// asking node has already checked the tenant.
pub(super) async fn internal_stats(
    State(st): State<AppState>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<NodeStats>> {
    let span = q
        .get("spanMs")
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_millis(crate::stats::WINDOW_MS));
    Ok(Json(local_stats(
        &st,
        span,
        q.get("tenant").map(String::as_str),
    )?))
}

// ──────────────────────────────────── durable statistics

/// `GET /stats/history?days=N&tenant=` (`admin` role) — the hourly series of the
/// per-handler summary, the latest `days` days (default 1, cap 30), merging the
/// `stats/` objects of every node: `{days, rows: [{hourMs, tenant, service,
/// handler, deployment, turns, completed, failed, paused, failedAttempts,
/// suspensions, steps, runMs, cu, p50Ms, p99Ms, maxMs}]}`. Tenant per the key.
pub(super) async fn stats_history(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<serde_json::Value>> {
    let tenant = acting_tenant(&who, &q)?;
    let days = match q.get("days") {
        Some(v) => v
            .parse::<u64>()
            .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "days is not a number".into()))?
            .clamp(1, crate::stats_history::MAX_DAYS),
        None => 1,
    };
    let (store, prefix) = st.metering.as_ref().ok_or_else(|| {
        ApiError(
            StatusCode::NOT_IMPLEMENTED,
            "this node has no metering bucket".into(),
        )
    })?;
    // Cache of closed days. A tenant being/already purged: its numbers are being
    // deleted from the bucket — skip the cache, read directly.
    let now = cairn_store::entropy::now_ms();
    let purging = st.tenant_settings().is_some_and(|s| {
        matches!(
            s.flags(&tenant).state(),
            cairn_engine::TenantState::Deleting | cairn_engine::TenantState::Purged
        )
    });
    let rows = if purging {
        st.history_cache.forget(tenant.as_str());
        crate::stats_history::read(store, prefix, tenant.as_str(), days, now).await
    } else {
        crate::stats_history::read_cached(
            &st.history_cache,
            store,
            prefix,
            tenant.as_str(),
            days,
            now,
        )
        .await
    }
    .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    Ok(Json(serde_json::json!({ "days": days, "rows": rows })))
}

// ──────────────────────────────── queryable slow runs

fn local_slow_runs(st: &AppState, tenant: Option<&str>) -> Vec<crate::slowlog::SlowRun> {
    let me = st.me();
    st.handler_stats
        .as_ref()
        .map(|h| h.slow_log().recent(tenant))
        .unwrap_or_default()
        .into_iter()
        .map(|r| crate::slowlog::SlowRun {
            node: Some(me.clone()),
            ..r
        })
        .collect()
}

/// `GET /stats/slow-runs?tenant=&scope=local|cluster` (`admin` role) — the latest slow
/// runs still in the node's ring (at most `slowlog::RING_MAX` per node), newest first;
/// cluster-wide: every node, re-sorted by time. Tenant like `/stats/handlers`.
pub(super) async fn stats_slow_runs(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<serde_json::Value>> {
    let cluster = parse_scope(&q)?;
    let tenant = stats_tenant(&who, &q)?;
    let mut runs = local_slow_runs(&st, tenant.as_ref().map(|t| t.as_str()));
    let mut missing = Vec::new();
    if cluster {
        let query: Vec<(&str, String)> = tenant.iter().map(|t| ("tenant", t.to_string())).collect();
        let fanned =
            fan_out::<Vec<crate::slowlog::SlowRun>>(&st, "/internal/stats/slow-runs", &query).await;
        for (node, part) in fanned.parts {
            runs.extend(part.into_iter().map(|r| crate::slowlog::SlowRun {
                node: Some(node.clone()),
                ..r
            }));
        }
        missing = fanned.missing;
        runs.sort_by_key(|r| std::cmp::Reverse(r.at_ms));
    }
    Ok(Json(serde_json::json!({
        "runs": runs,
        "partial": !missing.is_empty(),
        "missing": missing,
    })))
}

/// `GET /internal/stats/slow-runs?tenant=` — this node's part.
pub(super) async fn internal_slow_runs(
    State(st): State<AppState>,
    Query(q): Query<HashMap<String, String>>,
) -> Json<Vec<crate::slowlog::SlowRun>> {
    Json(local_slow_runs(&st, q.get("tenant").map(String::as_str)))
}

// ─────────────────────────────────────────── cluster-wide /invocations

/// One node's part of `/invocations?scope=cluster`.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct NodeInvocations {
    pub ranges: Vec<u32>,
    pub invocations: Vec<InvocationView>,
    /// The bounded scan: lines dropped for a missing mark, and ranges that stopped at
    /// the cap. Absent on older nodes (lenient read).
    #[serde(default)]
    pub untimed: usize,
    #[serde(default)]
    pub capped: bool,
}

/// `/invocations?scope=cluster`: this node's part plus every other node's part.
/// `partial` when a node is missing **or** a range had nobody answering.
pub(super) async fn cluster_invocations(
    st: &AppState,
    tenant: &cairn_store::TenantId,
    limit: usize,
    filter: &cairn_engine::ListFilter,
) -> ApiResult<Json<serde_json::Value>> {
    let me = st.me();
    let mut query = vec![("tenant", tenant.to_string()), ("limit", limit.to_string())];
    for (k, v) in [
        ("status", &filter.status),
        ("service", &filter.service),
        ("handler", &filter.handler),
    ] {
        if let Some(v) = v {
            query.push((k, v.clone()));
        }
    }
    if let Some(p) = &filter.id_prefix {
        query.push(("trace_id", p.iter().map(|b| format!("{b:02x}")).collect()));
    }
    for (k, v) in [
        ("created_after", filter.created_after),
        ("created_before", filter.created_before),
        ("ended_after", filter.ended_after),
        ("ended_before", filter.ended_before),
    ] {
        if let Some(v) = v {
            query.push((k, v.to_string()));
        }
    }
    if filter.created_desc {
        query.push(("order", "created_desc".into()));
    }
    // Our part and the fan-out at the same time: sequentially, the answer time is the
    // sum of the two, and the asker (the harness drain phase, the operator) waits for
    // both.
    //
    // Our part has **the same cap** as the other nodes' parts (every I/O await on such
    // a path is bounded): a range with a stuck engine (a long background tick, an L0
    // flush) must not hold the whole request. Cap reached ⇒ like a node not answering
    // the fan-out: we go into `missing`, our ranges into `missingRanges`.
    let (local, fanned) = futures::join!(
        cairn_store::retry::deadline(
            &cairn_store::retry::ops::INGRESS_LIST_LOCAL,
            FAN_OUT_TIMEOUT,
            super::local_invocations(st, tenant, limit, filter),
        ),
        fan_out::<NodeInvocations>(st, "/internal/invocations", &query),
    );
    let mut fanned = fanned;
    let mut nodes = Vec::new();
    let local = match local {
        Ok(local) => {
            nodes.push(me.clone());
            local?
        }
        Err(_) => {
            fanned.missing.push(me.clone());
            NodeInvocations {
                ranges: Vec::new(),
                invocations: Vec::new(),
                untimed: 0,
                capped: false,
            }
        }
    };
    let mut answered: std::collections::BTreeSet<u32> = local.ranges.iter().copied().collect();
    let (mut untimed, mut capped) = (local.untimed, local.capped);
    let mut rows: Vec<InvocationView> = local
        .invocations
        .into_iter()
        .map(|r| InvocationView {
            node: Some(me.clone()),
            ..r
        })
        .collect();
    for (node, part) in fanned.parts {
        nodes.push(node.clone());
        answered.extend(part.ranges);
        untimed += part.untimed;
        capped |= part.capped;
        rows.extend(part.invocations.into_iter().map(|r| InvocationView {
            node: Some(node.clone()),
            ..r
        }));
    }
    // Two nodes may both return a range during the leader handover window.
    let mut seen = std::collections::BTreeSet::new();
    rows.retain(|r| seen.insert(r.id.clone()));
    if filter.created_desc {
        super::invocations::sort_created_desc(&mut rows);
    }
    let missing_ranges: Vec<u32> = st
        .router
        .cluster
        .partitions()
        .iter()
        .map(|p| p.0)
        .filter(|p| !answered.contains(p))
        .collect();
    Ok(Json(serde_json::json!({
        "scope": "cluster",
        "nodes": nodes,
        "partial": !fanned.missing.is_empty() || !missing_ranges.is_empty() || capped,
        "missing": fanned.missing,
        "missingRanges": missing_ranges,
        // The real `limit` applied per range (clamped to `LIST_LIMIT_MAX`).
        "limit": limit,
        "invocations": rows,
        "untimed": untimed,
    })))
}

/// `GET /internal/invocations?tenant=&limit=&status=` — this node's part.
pub(super) async fn internal_invocations(
    State(st): State<AppState>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<NodeInvocations>> {
    let tenant =
        cairn_store::TenantId::new(q.get("tenant").map(String::as_str).unwrap_or_default())
            .map_err(|e| ApiError(StatusCode::BAD_REQUEST, e.to_string()))?;
    let (limit, filter) = super::invocations::list_query(&q)?;
    Ok(Json(
        super::local_invocations(&st, &tenant, limit, &filter).await?,
    ))
}

// ─────────────────────────────────────────────── capturing the next runs

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CaptureRequest {
    /// Default: the key's tenant.
    tenant: Option<String>,
    service: String,
    handler: String,
    deployment: Option<String>,
    #[serde(default = "one")]
    count: u32,
    /// Seconds. Default one hour.
    #[serde(default = "an_hour")]
    ttl: u64,
}

fn one() -> u32 {
    1
}

fn an_hour() -> u64 {
    3_600
}

fn capture_error(e: CaptureError) -> ApiError {
    match e {
        CaptureError::Invalid(m) => ApiError(StatusCode::BAD_REQUEST, m),
        e @ CaptureError::NewerFormat(_) => ApiError(StatusCode::CONFLICT, e.to_string()),
        CaptureError::Store(m) => ApiError(StatusCode::INTERNAL_SERVER_ERROR, m),
    }
}

fn capture_store(
    st: &AppState,
) -> ApiResult<(&crate::capture::Captures, &crate::capture::CaptureStore)> {
    let caps = st.handler_stats.as_ref().map(|s| s.captures());
    match caps.and_then(|c| c.store().map(|s| (c, s))) {
        Some(x) => Ok(x),
        None => Err(ApiError(
            StatusCode::NOT_IMPLEMENTED,
            "this node has no store for snapshots".into(),
        )),
    }
}

/// `POST /admin/capture {tenant, service, handler, deployment?, count, ttl}`
/// (cluster-admin) — the **next** matching invocations (at most `count`, within `ttl`
/// seconds) leave a bundle `captures/<tenant>/<id>` when they finish or when they are
/// `paused`.
pub(super) async fn create_capture(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Json(req): Json<CaptureRequest>,
) -> ApiResult<Json<CaptureSpec>> {
    let (caps, store) = capture_store(&st)?;
    let tenant = req.tenant.unwrap_or_else(|| who.tenant.to_string());
    let spec = store
        .create(
            &tenant,
            &req.service,
            &req.handler,
            req.deployment.as_deref(),
            req.count,
            Duration::from_secs(req.ttl),
        )
        .await
        .map_err(capture_error)?;
    // The node taking the command captures at once; other nodes after one refresh tick.
    caps.insert(spec.clone());
    Ok(Json(spec))
}

/// `GET /admin/capture?tenant=` — live requests and the ids of existing bundles.
pub(super) async fn list_captures(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<serde_json::Value>> {
    let tenant = acting_tenant(&who, &q)?;
    let (_, store) = capture_store(&st)?;
    let specs = store.specs(tenant.as_str()).await.map_err(capture_error)?;
    let bundles = store
        .bundles(tenant.as_str())
        .await
        .map_err(capture_error)?;
    Ok(Json(serde_json::json!({
        "tenant": tenant.as_str(),
        "captures": specs,
        "bundles": bundles,
    })))
}

/// `GET /admin/capture/{tenant}/{id}` — the raw bytes of one bundle, for
/// `cairn replay --bundle`.
pub(super) async fn get_capture_bundle(
    State(st): State<AppState>,
    Path((tenant, id)): Path<(String, String)>,
) -> ApiResult<Response> {
    let tenant = cairn_store::TenantId::new(&tenant)
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, e.to_string()))?;
    // The id is a segment of an object path: hex only, exactly the id length.
    if super::unhex(&id).is_none() {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "invalid invocation id".into(),
        ));
    }
    let (_, store) = capture_store(&st)?;
    match store
        .bundle(tenant.as_str(), &id)
        .await
        .map_err(capture_error)?
    {
        Some(bytes) => {
            Ok(([(header::CONTENT_TYPE, "application/octet-stream")], bytes).into_response())
        }
        None => Err(ApiError(StatusCode::NOT_FOUND, "no such bundle".into())),
    }
}

// ─────────────────────────────────────────── poison handler quarantine

fn quarantine_of(st: &AppState) -> ApiResult<&crate::quarantine::Quarantine> {
    st.handler_stats
        .as_ref()
        .map(|s| s.quarantine())
        .ok_or_else(|| {
            ApiError(
                StatusCode::NOT_IMPLEMENTED,
                "this node has no quarantine list".into(),
            )
        })
}

fn quarantine_error(e: crate::quarantine::QuarantineError) -> ApiError {
    match e {
        crate::quarantine::QuarantineError::NewerFormat(_) => {
            ApiError(StatusCode::CONFLICT, e.to_string())
        }
        crate::quarantine::QuarantineError::Store(_) => {
            ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        }
    }
}

/// `GET /admin/quarantine?tenant=` — the tenant's quarantine list, **read fresh** from
/// the bucket (every node sees the same list), with the mode and the effective rule.
pub(super) async fn list_quarantine(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<serde_json::Value>> {
    let tenant = acting_tenant(&who, &q)?;
    let quarantine = quarantine_of(&st)?;
    let entries = match quarantine.store() {
        Some(store) => store
            .entries(tenant.as_str())
            .await
            .map_err(quarantine_error)?,
        None => quarantine.snapshot().tenant(tenant.as_str()).to_vec(),
    };
    let (mode, rule) = effective_policy(&st, tenant.as_str());
    Ok(Json(serde_json::json!({
        "tenant": tenant.as_str(),
        "mode": mode.as_str(),
        "rule": rule.encode(),
        "entries": entries,
    })))
}

/// The mode and effective rule of `tenant`: the tenant's flags override the node default.
fn effective_policy(
    st: &AppState,
    tenant: &str,
) -> (crate::quarantine::Mode, crate::quarantine::Rule) {
    let node = st
        .handler_stats
        .as_ref()
        .map(|s| s.quarantine().config())
        .unwrap_or_default();
    let (mode, rule) = st
        .tenant_settings
        .as_ref()
        .map(|t| t.quarantine(tenant))
        .unwrap_or_default();
    (mode.unwrap_or(node.mode), rule.unwrap_or(node.rule))
}

/// `DELETE /admin/quarantine/{tenant}/{service}/{handler}` — manually removes every
/// entry of that handler (every deployment). The identity is left `cleared` (one rule
/// window + one summary window) so the detector does not put it back in on the same
/// old numbers.
/// Invocations already held stay `paused`: `resume` them one by one.
pub(super) async fn delete_quarantine(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path((tenant, service, handler)): Path<(String, String, String)>,
) -> ApiResult<Json<serde_json::Value>> {
    let tenant = cairn_store::TenantId::new(&tenant)
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, e.to_string()))?;
    if !who.all_tenants() && who.tenant != tenant {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "you can only remove entries from your own tenant's quarantine list".into(),
        ));
    }
    let quarantine = quarantine_of(&st)?;
    let (_, rule) = effective_policy(&st, tenant.as_str());
    let removed = quarantine
        .clear(tenant.as_str(), &service, &handler, rule.window)
        .await
        .map_err(quarantine_error)?;
    if removed > 0 {
        tracing::warn!(tenant = %tenant, service, handler, removed, "removed from the quarantine list by hand");
    }
    Ok(Json(serde_json::json!({
        "tenant": tenant.as_str(),
        "service": service,
        "handler": handler,
        "removed": removed,
    })))
}
