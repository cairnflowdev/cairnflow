// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Operational read endpoints for the query layer: nodes, the shared WAL, the scrubber,
//! the trash, the price table. Every route here is `cluster-admin` (the default arm of
//! `auth::required_role`), unless noted otherwise.
//!
//! `scope=cluster` merges the parts of every node leading at least one range (through
//! the internal port, `observe::fan_out`); a node leading no range is not in the
//! routing table and so is not asked — it cannot go into `missing`, it is simply absent.

use {
    super::{observe, ApiError, ApiResult, AppState},
    axum::{
        extract::{Query, State},
        http::StatusCode,
        Json,
    },
    cairn_store::entropy::HashMap,
    serde::{Deserialize, Serialize},
};

fn unavailable(what: &str) -> ApiError {
    ApiError(
        StatusCode::NOT_IMPLEMENTED,
        format!("this node has no {what}"),
    )
}

/// Merges this node's part with the other nodes' parts (when `cluster`).
async fn gather<T: Serialize + serde::de::DeserializeOwned>(
    st: &AppState,
    cluster: bool,
    internal: &str,
    mine: Vec<T>,
) -> serde_json::Value {
    let mut rows = mine;
    let mut missing = Vec::new();
    if cluster {
        let fanned = observe::fan_out::<Vec<T>>(st, internal, &[]).await;
        for (_, part) in fanned.parts {
            rows.extend(part);
        }
        missing = fanned.missing;
    }
    serde_json::json!({
        "rows": rows,
        "partial": !missing.is_empty(),
        "missing": missing,
    })
}

// ─────────────────────────────────────────────────────────────── node

/// A node, as it sees itself.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct NodeRow {
    node: String,
    /// Ranges this node leads per its routing table.
    leading: Vec<u32>,
    /// The published slowness score (`health::Published::slow`), `None` = not slow.
    slow: Option<u32>,
    p99_put_ms: Option<u64>,
    draining: bool,
    /// SST disk cache — absent when off or on an old node.
    #[serde(default)]
    disk_cache_bytes: Option<u64>,
    #[serde(default)]
    disk_cache_hit_ratio: Option<f64>,
}

fn local_node(st: &AppState) -> NodeRow {
    let me = st.me();
    let leading = st
        .router
        .routing
        .snapshot()
        .into_iter()
        .filter(|(_, l)| l.node_id == me)
        .map(|(p, _)| p)
        .collect();
    let health = cairn_store::health::meter_of(&me).map(|m| m.published());
    let disk = st
        .gc
        .as_ref()
        .and_then(|g| g.stores.disk_cache())
        .map(|c| c.snapshot());
    NodeRow {
        disk_cache_bytes: disk.map(|d| d.bytes),
        disk_cache_hit_ratio: disk.and_then(|d| d.hit_ratio()),
        leading,
        slow: health.as_ref().and_then(|h| h.slow),
        p99_put_ms: health.as_ref().and_then(|h| h.p99_put_ms),
        draining: health.is_some_and(|h| h.draining),
        node: me,
    }
}

/// `GET /cluster/nodes?scope=local|cluster` — nodes, the ranges they lead, published
/// PUT health.
pub(super) async fn nodes(
    State(st): State<AppState>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<serde_json::Value>> {
    let cluster = observe::parse_scope(&q)?;
    let mine = vec![local_node(&st)];
    Ok(Json(
        gather(&st, cluster, "/internal/cluster/nodes", mine).await,
    ))
}

pub(super) async fn internal_nodes(State(st): State<AppState>) -> Json<Vec<NodeRow>> {
    Json(vec![local_node(&st)])
}

// ─────────────────────────────────────────────────────────── Shared WAL

/// A shared WAL log that an incarnation of this node is cleaning.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct NodeWalRow {
    node: String,
    log: String,
    floor: u64,
    pinned_by_range: Option<u32>,
    pinned_wal_id: Option<u64>,
    pinned_for_ms: u64,
}

fn local_nodewal(st: &AppState) -> Vec<NodeWalRow> {
    cairn_store::nodewal::floor_reports(st.router.node_id.as_deref())
        .into_iter()
        .flat_map(|r| {
            let node = r.node.clone();
            r.logs.into_iter().map(move |l| NodeWalRow {
                node: node.clone(),
                log: l.log.to_string(),
                floor: l.floor,
                pinned_by_range: l.pinned_by.map(|(r, _)| r),
                pinned_wal_id: l.pinned_by.map(|(_, w)| w),
                pinned_for_ms: l.pinned_for.as_millis() as u64,
            })
        })
        .collect()
}

/// `GET /admin/nodewal?scope=local|cluster` — the floor of each live shared WAL log and
/// the range pinning it. A node not running the shared WAL: no rows.
pub(super) async fn nodewal(
    State(st): State<AppState>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<serde_json::Value>> {
    let cluster = observe::parse_scope(&q)?;
    let mine = local_nodewal(&st);
    Ok(Json(
        gather(&st, cluster, "/internal/admin/nodewal", mine).await,
    ))
}

pub(super) async fn internal_nodewal(State(st): State<AppState>) -> Json<Vec<NodeWalRow>> {
    Json(local_nodewal(&st))
}

// ──────────────────────────────────────────────────────────── scrubber

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ScrubRow {
    node: String,
    #[serde(flatten)]
    round: crate::scrub::LastRound,
}

fn local_scrub(st: &AppState) -> Vec<ScrubRow> {
    st.scrub_last
        .as_ref()
        .and_then(|l| l.get())
        .map(|round| ScrubRow {
            node: st.me(),
            round,
        })
        .into_iter()
        .collect()
}

/// `GET /admin/scrub?scope=local|cluster` — the latest round of the background scrubber
/// (only the node that is the D-d runner has rounds), with findings still standing.
pub(super) async fn scrub(
    State(st): State<AppState>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<serde_json::Value>> {
    let cluster = observe::parse_scope(&q)?;
    let mine = local_scrub(&st);
    Ok(Json(
        gather(&st, cluster, "/internal/admin/scrub", mine).await,
    ))
}

pub(super) async fn internal_scrub(State(st): State<AppState>) -> Json<Vec<ScrubRow>> {
    Json(local_scrub(&st))
}

// ─────────────────────────────────────────────────────────── trash

/// `GET /admin/trash` — entries in the trash (like `cairn-ctl trash list`): reads the
/// bucket, not RAM.
pub(super) async fn trash(State(st): State<AppState>) -> ApiResult<Json<serde_json::Value>> {
    let gc = st
        .gc
        .as_ref()
        .ok_or_else(|| unavailable("bucket read access"))?;
    let main = gc.stores.main();
    let bin = cairn_store::trash::Trash::new(std::sync::Arc::clone(main.store()), main.prefix());
    let entries = bin
        .entries()
        .await
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let rows: Vec<serde_json::Value> = entries
        .into_iter()
        .map(|e| {
            serde_json::json!({
                "entry": e.id.as_str(),
                "createdMs": e.id.created_ms(),
                "subject": e.subject.as_ref().map(|s| s.to_string()),
                "inPlace": e.subject.as_ref().is_some_and(|s| s.in_place()),
                "reason": e.reason,
                "objects": e.objects,
                "bytes": e.bytes,
                "unreadable": e.unreadable,
            })
        })
        .collect();
    Ok(Json(serde_json::json!({ "entries": rows })))
}

// ──────────────────────────────────────────────────────────── price table

/// `GET /metering/pricing` — the token price table per model (the `pricing` object):
/// `{version, by, models: [{model, input, output}]}`, micro-USD per 1 000 000 tokens.
/// No price table yet ⇒ `models` empty, `version` 0.
pub(super) async fn pricing(State(st): State<AppState>) -> ApiResult<Json<serde_json::Value>> {
    let (store, prefix) = st
        .metering
        .as_ref()
        .ok_or_else(|| unavailable("metering bucket"))?;
    let got = crate::pricing::read(store, prefix)
        .await
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    let p = got.map(|(p, _)| p).unwrap_or_default();
    let models: Vec<serde_json::Value> = p
        .models
        .iter()
        .map(|(m, price)| serde_json::json!({ "model": m, "input": price.input, "output": price.output }))
        .collect();
    Ok(Json(serde_json::json!({
        "version": p.version,
        "by": p.by,
        "models": models,
    })))
}
