// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! HTTP for auto split.
//!
//! - `GET /ranges/stats[?scope=cluster][&detail=1]` — load per range: the node returns
//!   the numbers of the ranges **it is leading**; `scope=cluster` also asks every other
//!   node through the internal port ([`super::observe::fan_out`]).
//! - `GET /ranges/suggestions[?scope=cluster]` — split suggestions: an array
//!   `[{range, reason: load|size|hot_owner, at, balance, blockedBy, …}]`, plus merge
//!   suggestions: `{range, reason: merge, mergeWith, sizeBytes, coldSecs, …}`, built
//!   from this node's facts or — with `scope=cluster` — every node's
//!   (`GET /internal/ranges/merge-facts`).

use {
    super::{observe, ApiError, ApiResult, AppState},
    crate::autosplit::{MergeFacts, RangeLoadView, Suggestion},
    axum::{
        extract::{Query, State},
        http::StatusCode,
        Json,
    },
    cairn_store::entropy::HashMap,
    serde::{Deserialize, Serialize},
};

fn auto_split(st: &AppState) -> ApiResult<&crate::autosplit::AutoSplit> {
    st.auto_split.as_ref().ok_or_else(|| {
        ApiError(
            StatusCode::NOT_IMPLEMENTED,
            "this node has no per-range load statistics".into(),
        )
    })
}

fn detail(q: &HashMap<String, String>) -> bool {
    matches!(q.get("detail").map(String::as_str), Some("1" | "true"))
}

/// One node's part of `/ranges/stats`.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct NodeRangeStats {
    pub node: String,
    pub ranges: Vec<RangeLoadView>,
}

fn local_stats(st: &AppState, detail: bool) -> ApiResult<NodeRangeStats> {
    let node = st.me();
    let ranges = auto_split(st)?
        .stats(detail)
        .into_iter()
        .map(|r| RangeLoadView {
            node: Some(node.clone()),
            ..r
        })
        .collect();
    Ok(NodeRangeStats { node, ranges })
}

/// `GET /ranges/stats`.
pub(super) async fn range_stats(
    State(st): State<AppState>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<serde_json::Value>> {
    let cluster = observe::parse_scope(&q)?;
    let detail = detail(&q);
    let local = local_stats(&st, detail)?;
    let mut nodes = vec![local.node];
    let mut ranges = local.ranges;
    let mut missing = Vec::new();
    if cluster {
        let query = if detail {
            vec![("detail", "1".to_string())]
        } else {
            Vec::new()
        };
        let fanned =
            observe::fan_out::<NodeRangeStats>(&st, "/internal/ranges/stats", &query).await;
        for (node, part) in fanned.parts {
            nodes.push(node);
            ranges.extend(part.ranges);
        }
        missing = fanned.missing;
    }
    ranges.sort_by_key(|r| r.range);
    Ok(Json(serde_json::json!({
        "scope": if cluster { "cluster" } else { "local" },
        "nodes": nodes,
        "partial": !missing.is_empty(),
        "missing": missing,
        "ranges": ranges,
    })))
}

/// `GET /internal/ranges/stats` — this node's part, for fan-out.
pub(super) async fn internal_range_stats(
    State(st): State<AppState>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<NodeRangeStats>> {
    Ok(Json(local_stats(&st, detail(&q))?))
}

fn local_suggestions(st: &AppState) -> ApiResult<Vec<Suggestion>> {
    let node = st.me();
    Ok(auto_split(st)?
        .suggestions()
        .into_iter()
        .map(|s| Suggestion {
            node: Some(node.clone()),
            ..s
        })
        .collect())
}

fn local_merge_facts(st: &AppState) -> ApiResult<Vec<MergeFacts>> {
    let node = st.me();
    Ok(auto_split(st)?
        .merge_facts()
        .into_iter()
        .map(|f| MergeFacts {
            node: Some(node.clone()),
            ..f
        })
        .collect())
}

/// `GET /ranges/suggestions` — this node's suggestions (the ranges it leads), or the
/// whole cluster's with `scope=cluster`. A node that does not answer has its part
/// missing (logged); the array carries no `partial` flag, to keep the documented shape.
/// Merge suggestions are only paired here, once, on this node's table: a range that no
/// node contributed facts for (node not answering, old binary) goes into no pair.
pub(super) async fn range_suggestions(
    State(st): State<AppState>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<Vec<Suggestion>>> {
    let cluster = observe::parse_scope(&q)?;
    let mut rows = local_suggestions(&st)?;
    let mut facts = local_merge_facts(&st)?;
    if cluster {
        let (fanned, fanned_facts) = tokio::join!(
            observe::fan_out::<Vec<Suggestion>>(&st, "/internal/ranges/suggestions", &[]),
            observe::fan_out::<Vec<MergeFacts>>(&st, "/internal/ranges/merge-facts", &[]),
        );
        for (_, part) in fanned.parts {
            rows.extend(part);
        }
        for (_, part) in fanned_facts.parts {
            facts.extend(part);
        }
        if !fanned.missing.is_empty() || !fanned_facts.missing.is_empty() {
            tracing::debug!(
                missing = ?fanned.missing,
                missing_facts = ?fanned_facts.missing,
                "/ranges/suggestions: node did not answer"
            );
        }
    }
    rows.extend(auto_split(&st)?.merge_rows(&facts));
    rows.sort_by_key(|r| r.range);
    Ok(Json(rows))
}

/// `GET /internal/ranges/merge-facts` — facts for merge suggestions of the ranges this
/// node leads, for fan-out.
pub(super) async fn internal_merge_facts(
    State(st): State<AppState>,
) -> ApiResult<Json<Vec<MergeFacts>>> {
    Ok(Json(local_merge_facts(&st)?))
}

/// `GET /internal/ranges/suggestions` — this node's part, for fan-out.
pub(super) async fn internal_range_suggestions(
    State(st): State<AppState>,
) -> ApiResult<Json<Vec<Suggestion>>> {
    Ok(Json(local_suggestions(&st)?))
}
