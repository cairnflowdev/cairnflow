// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `POST /invocations/batch/{op}`: the console's bulk operations.
//!
//! `{ids: [...]}` of at most [`BATCH_MAX`] ids, running **exactly the single-id
//! handler** for each id (`cancel`, `purge`, `resume`, `restart`) — same role, same
//! tenant check, same error codes — at most [`BATCH_PARALLEL`] ids at a time, keeping
//! order. Returns a result per id: `{id, ok, status, result | error}`. One bad id does
//! not fail the whole batch; an id of another tenant is a `404` of that id alone, as
//! when called singly.
//!
//! A request asking for forwarding (`x-cairn-forward: leader`): ids for which this node
//! returns `503` "not the leader" are grouped by leader, one sub-batch per leader run
//! there (one hop, like [`super::forward`]); the results are merged back in order. A
//! leader that cannot be reached ⇒ the ids keep their `503`; sent but no response ⇒
//! `504` per id (ambiguous: may have run).
//!
//! The parallelism cap is deliberate: each command is a commit on the owning range, and
//! a batch of 1 000 ids landing on a few ranges must not take all of their turns.

use super::*;

/// Maximum ids per batch — the client pages.
pub const BATCH_MAX: usize = 1_000;
/// Ids run at the same time within one batch.
pub const BATCH_PARALLEL: usize = 5;

#[derive(Deserialize)]
pub(super) struct BatchRequest {
    ids: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub(super) struct BatchItem {
    /// The id's range when the single call returns `503` "not the leader" — not on the wire.
    #[serde(skip)]
    partition: Option<cairn_store::PartitionId>,
    id: String,
    ok: bool,
    /// The HTTP status the single call for the same id would return.
    status: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Clone, Copy)]
enum Op {
    Cancel,
    Purge,
    Resume,
    Restart,
}

impl Op {
    fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "cancel" => Self::Cancel,
            "purge" => Self::Purge,
            "resume" => Self::Resume,
            "restart" => Self::Restart,
            _ => return None,
        })
    }
}

fn item<T: Serialize>(id: String, r: ApiResult<Json<T>>) -> BatchItem {
    match r {
        Ok(Json(v)) => BatchItem {
            partition: None,
            id,
            ok: true,
            status: 200,
            result: serde_json::to_value(v).ok(),
            error: None,
        },
        Err(e) => BatchItem {
            partition: e.partition(),
            id,
            ok: false,
            status: e.status.as_u16(),
            result: None,
            error: Some(e.public_message()),
        },
    }
}

/// Query (`mode=signal` of cancel, `from`/`deployment` of restart) applied to every id
/// of the batch.
#[allow(clippy::too_many_arguments)]
pub(super) async fn batch(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    meta: super::Meta,
    Path(op): Path<String>,
    Query(q): Query<HashMap<String, String>>,
    uri: axum::http::Uri,
    headers: axum::http::HeaderMap,
    forwarded: Option<axum::Extension<forward::Forwarded>>,
    Json(req): Json<BatchRequest>,
) -> ApiResult<Json<serde_json::Value>> {
    use futures::StreamExt;
    let kind = Op::parse(&op).ok_or_else(|| {
        ApiError(
            StatusCode::NOT_FOUND,
            format!("no batch operation {op:?} (cancel, purge, resume, restart)"),
        )
    })?;
    if req.ids.is_empty() || req.ids.len() > BATCH_MAX {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            format!(
                "ids must have 1..={BATCH_MAX} elements, got {}",
                req.ids.len()
            ),
        ));
    }
    let mut results: Vec<BatchItem> = futures::stream::iter(req.ids)
        .map(|id| {
            let (st, who, q, m) = (st.clone(), who.clone(), q.clone(), meta.clone());
            async move {
                // One audit record per id, in the same commit as the operation on
                // that id.
                let (s, w, p) = (State(st), axum::Extension(who), Path(id.clone()));
                match kind {
                    Op::Cancel => item(id, cancel(s, w, m, p, Query(q)).await),
                    Op::Purge => item(id, purge(s, w, m, p).await),
                    Op::Resume => item(id, resume(s, w, m, p).await),
                    Op::Restart => item(
                        id,
                        restart_invocation(s, w, m, p, Query(q), Bytes::new()).await,
                    ),
                }
            }
        })
        .buffered(BATCH_PARALLEL)
        .collect()
        .await;
    let mut ext = axum::http::Extensions::new();
    if let Some(axum::Extension(f)) = forwarded {
        ext.insert(f);
    }
    if forward::wanted(&headers, &ext) {
        forward_remote(&st, &uri, &headers, &mut results).await;
    }
    Ok(Json(serde_json::json!({ "op": op, "results": results })))
}

/// Groups `503` ids by leader and runs sub-batches there — see the module head.
async fn forward_remote(
    st: &AppState,
    uri: &axum::http::Uri,
    headers: &axum::http::HeaderMap,
    results: &mut [BatchItem],
) {
    let mut by_leader: std::collections::BTreeMap<String, Vec<usize>> = Default::default();
    for (i, r) in results.iter().enumerate() {
        if r.status != StatusCode::SERVICE_UNAVAILABLE.as_u16() {
            continue;
        }
        if let Some(addr) = r.partition.and_then(|p| forward::leader_address(st, p)) {
            by_leader.entry(addr).or_default().push(i);
        }
    }
    for (addr, idx) in by_leader {
        let ids: Vec<&str> = idx.iter().map(|&i| results[i].id.as_str()).collect();
        let body = serde_json::to_vec(&serde_json::json!({ "ids": ids })).unwrap_or_default();
        let out = forward::send(
            st,
            &addr,
            &axum::http::Method::POST,
            uri,
            headers,
            body.into(),
            // Sub-batch: the client IP does not travel (only single-request forwarding carries it).
            None,
        )
        .await;
        match out {
            forward::Outcome::Refused => {}
            forward::Outcome::Ambiguous(why) => {
                for &i in &idx {
                    let r = &mut results[i];
                    r.status = StatusCode::GATEWAY_TIMEOUT.as_u16();
                    r.error = Some(format!(
                        "forward to leader got no response ({why}) — it may have run"
                    ));
                }
            }
            forward::Outcome::Relayed(resp) => {
                let status = resp.status();
                let got = axum::body::to_bytes(resp.into_body(), 64 << 20).await;
                let parsed = got.ok().and_then(|b| {
                    #[derive(Deserialize)]
                    struct Out {
                        results: Vec<BatchItem>,
                    }
                    serde_json::from_slice::<Out>(&b).ok()
                });
                match parsed {
                    Some(out) if status.is_success() => {
                        let mut by_id: HashMap<String, BatchItem> =
                            out.results.into_iter().map(|r| (r.id.clone(), r)).collect();
                        for &i in &idx {
                            if let Some(r) = by_id.remove(&results[i].id) {
                                results[i] = r;
                            }
                        }
                    }
                    // The leader refused the whole batch (role, body): its words for every id.
                    _ => {
                        for &i in &idx {
                            let r = &mut results[i];
                            r.status = status.as_u16();
                            r.error =
                                Some(format!("leader returned {status} for the forwarded batch"));
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// **Batch error items do not carry the raw text of a `500` error.** The
    /// `opaque_internal` mask only runs in `into_response`; a batch item reading
    /// `e.message` directly ⇒ a store error (bucket, endpoint, the range's object path)
    /// leaks into the body of `POST /invocations/batch/{op}`.
    ///
    /// Must see: a `500` item carries the masked body (a log lookup code), not the
    /// original text; `4xx` errors keep their text.
    #[test]
    fn batch_item_hides_internal_error_text() {
        let detail = "cannot read s3://cell-bucket/demo/range-3/wal/0001.sst: endpoint 10.0.0.7";
        let it = item::<()>(
            "id".into(),
            Err(ApiError(StatusCode::INTERNAL_SERVER_ERROR, detail.into())),
        );
        let shown = it.error.unwrap_or_default();
        assert!(
            !shown.contains("s3://") && !shown.contains("10.0.0.7"),
            "batch item leaks store error text: {shown}"
        );
        assert!(shown.contains("details in the node log"), "{shown}");
        let it = item::<()>(
            "id".into(),
            Err(ApiError(StatusCode::CONFLICT, "already finished".into())),
        );
        assert_eq!(it.error.as_deref(), Some("already finished"));
    }
}
