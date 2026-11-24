// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Cluster-wide view, split/merge, per-range flush windows.

use super::*;

// ─────────────────────────────────────────────────────────── cluster

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionView {
    pub partition: u32,
    pub leader: Option<String>,
    pub address: Option<String>,
    pub term: Option<u64>,
}

/// A follower reader open on this node.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReaderView {
    pub partition: u32,
    /// How far the reader has replayed the WAL — the real "freshness" mark.
    pub durable_seq: u64,
    /// How far the manifest covers. `durable < manifest` for a long time = wedged.
    pub manifest_seq: u64,
    pub healthy: bool,
}

/// One row of the range table, with the leader per the routing table.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RangeView {
    pub range: u32,
    /// Start and end (inclusive) of the key span, 16 hex digits — like
    /// `cluster.conf`, so it can be compared by eye with the bucket.
    pub start: String,
    pub end: String,
    pub gen: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub intent: Option<crate::transport::IntentWire>,
    pub leader: Option<String>,
    /// The leader's **ingress** port — clients send requests here. The internal port
    /// is not exposed on the customer API.
    pub address: Option<String>,
    pub term: Option<u64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterView {
    /// Version of the range table this node is using.
    pub version: u64,
    /// The next id the table will assign — ids are never reused (D22).
    pub next_range_id: u32,
    /// The current range table, in key order.
    pub ranges: Vec<RangeView>,
    /// Number of ranges. Keeps the old name for old clients.
    pub partition_count: u16,
    /// Range → leader, old shape (`partition` = range id). Kept for old clients;
    /// `ranges` has everything here.
    pub partitions: Vec<PartitionView>,
    /// Readers open **on this node** — the only field of `/cluster` that depends on
    /// the node asked, because it describes local resources rather than the cluster.
    pub readers: Vec<ReaderView>,
}

/// Who holds which partition.
///
/// Read from [`RoutingTable`], i.e. from the lease objects in the bucket themselves. So
/// the answer **is the same on every node** — including a node that leads nothing, and
/// including when asked in the middle of a leader handover. That is what a diagnostic
/// endpoint needs: if it could only report what this node knows about itself, during an
/// incident we would have to ask node by node, exactly when we should not have to.
pub(super) async fn cluster_view(State(st): State<AppState>) -> Json<ClusterView> {
    Json(cluster_view_of(&st).await)
}

pub(super) async fn cluster_view_of(st: &AppState) -> ClusterView {
    // One table snapshot for the whole answer: a table changing midway must not make
    // `ranges` and `partitions` tell two different stories.
    let ranges_now = st.router.cluster.table();
    let table = st.router.routing.snapshot();
    let ranges = ranges_now
        .ranges()
        .iter()
        .map(|r| {
            let leader = table.get(&r.id.0);
            RangeView {
                range: r.id.0,
                start: format!("{:016x}", r.start),
                end: format!("{:016x}", r.end),
                gen: r.gen,
                intent: r.intent.as_ref().map(crate::transport::IntentWire::of),
                leader: leader.map(|l| l.node_id.clone()),
                address: leader.and_then(|l| l.ingress.clone()),
                term: leader.map(|l| l.term),
            }
        })
        .collect();
    let partitions = ranges_now
        .partitions()
        .map(|p| match table.get(&p.0) {
            Some(l) => PartitionView {
                partition: p.0,
                leader: Some(l.node_id.clone()),
                address: l.ingress.clone(),
                term: Some(l.term),
            },
            None => PartitionView {
                partition: p.0,
                leader: None,
                address: None,
                term: None,
            },
        })
        .collect();
    let readers = match &st.followers {
        Some(f) => f
            .statuses()
            .await
            .into_iter()
            .map(|(partition, s)| ReaderView {
                partition,
                durable_seq: s.freshness.durable_seq,
                manifest_seq: s.freshness.manifest_seq,
                healthy: s.healthy,
            })
            .collect(),
        None => Vec::new(),
    };
    ClusterView {
        version: ranges_now.version(),
        next_range_id: ranges_now.next_range_id(),
        ranges,
        partition_count: ranges_now.partition_count(),
        partitions,
        readers,
    }
}

// ─────────────────────────────────────────────────────────── split/merge

impl ApiError {
    fn from_range_op(e: crate::split::RangeOpError) -> Self {
        use crate::split::RangeOpError as E;
        match e {
            E::NotFound(m) => ApiError(StatusCode::NOT_FOUND, m),
            E::BadRequest(m) => ApiError(StatusCode::BAD_REQUEST, m),
            E::NotLeader { range, leader } => {
                let mut e = ApiError(
                    StatusCode::SERVICE_UNAVAILABLE,
                    match &leader {
                        Some(addr) => {
                            format!(
                                "this node does not lead range {range}; send the command to {addr}"
                            )
                        }
                        None => format!(
                            "this node does not lead range {range}, and the leader is not known yet"
                        ),
                    },
                );
                e.leader = leader;
                e.at_partition(cairn_store::PartitionId(range))
            }
            E::Conflict(m) => ApiError(StatusCode::CONFLICT, m),
            E::Busy(m) => ApiError(StatusCode::SERVICE_UNAVAILABLE, m),
            E::Failed(m) => ApiError(StatusCode::INTERNAL_SERVER_ERROR, m),
        }
    }
}

pub(super) fn range_ops(st: &AppState) -> ApiResult<&crate::split::RangeOps> {
    st.range_ops.as_ref().ok_or_else(|| {
        ApiError(
            StatusCode::NOT_IMPLEMENTED,
            "this node has no split/merge coordinator".into(),
        )
    })
}

#[derive(Deserialize)]
pub(super) struct SplitQuery {
    /// The first key of the right child range, 16 hex digits like `cluster.conf`.
    /// Absent ⇒ the middle of the key span.
    at: Option<String>,
}

/// `POST /ranges/{id}/split[?at=<hex16>]` — splits the range on the node leading it
/// (another node returns `503` with the leader address). Returns `/cluster` after the
/// commit.
///
/// The operation runs in the node's task: the handler only waits, so a client
/// disconnecting midway cancels nothing — ask again at `GET /ranges/ops`.
pub(super) async fn split_range(
    State(st): State<AppState>,
    Path(id): Path<u32>,
    Query(q): Query<SplitQuery>,
) -> ApiResult<Json<RangeOpView>> {
    let at = match q.at.as_deref() {
        None => None,
        Some(s) => Some(
            (s.len() == 16)
                .then(|| u64::from_str_radix(s, 16).ok())
                .flatten()
                .ok_or_else(|| {
                    ApiError(
                        StatusCode::BAD_REQUEST,
                        format!("at={s:?} is not 16 hex digits"),
                    )
                })?,
        ),
    };
    let ticket = range_ops(&st)?.start(crate::split::OpKind::Split {
        range: cairn_store::PartitionId(id),
        at,
    });
    let op_id = ticket.id.clone();
    let (_, report) = ticket.wait().await.map_err(ApiError::from_range_op)?;
    Ok(Json(RangeOpView {
        op_id,
        view: cluster_view_of(&st).await,
        report,
    }))
}

/// The split/merge response: `/cluster` after the commit, plus per-phase measurements
/// (`report`).
#[derive(Serialize)]
pub struct RangeOpView {
    /// Id in the node's operation log (`GET /ranges/ops/{id}`).
    #[serde(rename = "opId")]
    op_id: String,
    #[serde(flatten)]
    view: ClusterView,
    report: crate::split::OpReport,
}

#[derive(Deserialize)]
pub(super) struct MergeQuery {
    left: u32,
    right: u32,
}

/// `POST /ranges/merge?left=&right=` — merges two adjacent ranges (D-e: this node
/// leads one side, the other is gathered in). Returns `/cluster` after the commit.
pub(super) async fn merge_ranges(
    State(st): State<AppState>,
    Query(q): Query<MergeQuery>,
) -> ApiResult<Json<RangeOpView>> {
    let ticket = range_ops(&st)?.start(crate::split::OpKind::Merge {
        left: cairn_store::PartitionId(q.left),
        right: cairn_store::PartitionId(q.right),
    });
    let op_id = ticket.id.clone();
    let (_, report) = ticket.wait().await.map_err(ApiError::from_range_op)?;
    Ok(Json(RangeOpView {
        op_id,
        view: cluster_view_of(&st).await,
        report,
    }))
}

/// `GET /ranges/ops` — the split/merge operation log of **this node**: running, and up
/// to [`crate::split::OpBook::KEEP_DONE`] most recently finished. RAM only: when the
/// node restarts the log is empty, and an unfinished intent on the table is rolled back
/// by whoever claims the source range or by the intent reconcile loop (SM-3, SM-15).
/// `scope=cluster`: the logs of every node (the same request run on each node through
/// [`super::forward::to_peers`]), merged by id — `partial`/`missing` when a node does
/// not answer.
pub(super) async fn list_range_ops(
    State(st): State<AppState>,
    Query(q): Query<HashMap<String, String>>,
    uri: axum::http::Uri,
    headers: axum::http::HeaderMap,
    forwarded: Option<axum::Extension<super::forward::Forwarded>>,
) -> ApiResult<Json<serde_json::Value>> {
    let cluster = observe::parse_scope(&q)? && forwarded.is_none();
    let ops = range_ops(&st)?;
    let mut out = serde_json::json!({
        "node": ops.node_id,
        "ops": ops.book.list(),
    });
    if cluster {
        let (parts, missing) =
            super::forward::to_peers(&st, &axum::http::Method::GET, &uri, &headers, Bytes::new())
                .await;
        if let Some(all) = out["ops"].as_array_mut() {
            for (_, v) in parts {
                if let Some(theirs) = v["ops"].as_array() {
                    all.extend(theirs.iter().cloned());
                }
            }
            all.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
        }
        out["partial"] = (!missing.is_empty()).into();
        out["missing"] = missing.into();
    }
    Ok(Json(out))
}

/// `GET /ranges/ops/{id}` — one entry of the operation log. The id is prefixed with the
/// node that took the operation: ask that node.
pub(super) async fn get_range_op(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<crate::split::OpView>> {
    let ops = range_ops(&st)?;
    ops.book.get(&id).map(Json).ok_or_else(|| {
        ApiError(
            StatusCode::NOT_FOUND,
            format!(
                "no operation {id} in the log of node {} (the log is in memory only, keeps the last {} finished operations)",
                ops.node_id,
                crate::split::OpBook::KEEP_DONE
            ),
        )
    })
}

/// `GET /internal/ranges/{id}/coordinator` — whether this node's split/merge coordinator
/// is holding range `id` (`{"held": bool}`). The intent reconcile loop of the D-d runner
/// asks before rolling an intent back (SM-15).
pub(super) async fn range_coordinator(
    State(st): State<AppState>,
    Path(id): Path<u32>,
) -> ApiResult<Json<serde_json::Value>> {
    let ops = range_ops(&st)?;
    Ok(Json(
        serde_json::json!({ "held": ops.coordinates(cairn_store::PartitionId(id)) }),
    ))
}

// ─────────────────────────────────────── per-range flush windows (D-h)

#[derive(Deserialize)]
pub(super) struct WindowBody {
    /// Milliseconds; `null` = remove, follow the node.
    ms: Option<u64>,
}

/// `PUT /ranges/{id}/window {"ms": n|null}` — a range's own flush window: trades
/// commit latency for PUT count (plan tier). The shared WAL applies it after one
/// registry refresh tick (`--deploy-refresh-ms`) on every node; the native WAL at the
/// next Db open.
pub(super) async fn set_window(
    State(st): State<AppState>,
    Path(id): Path<u32>,
    Json(body): Json<WindowBody>,
) -> ApiResult<Json<serde_json::Value>> {
    let Some(w) = &st.windows else {
        return Err(ApiError(
            StatusCode::NOT_IMPLEMENTED,
            "node has no window registry".into(),
        ));
    };
    let window = body.ms.map(Duration::from_millis);
    if let Some(d) = window {
        if d.is_zero() || d > crate::windows::MAX_WINDOW {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                format!(
                    "ms must be in 1..={}",
                    crate::windows::MAX_WINDOW.as_millis()
                ),
            ));
        }
    }
    if !st.router.cluster.contains(cairn_store::PartitionId(id)) {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            format!("no range {id} in the table"),
        ));
    }
    w.set(id, window)
        .await
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    Ok(Json(serde_json::json!({ "range": id, "ms": body.ms })))
}

/// `GET /ranges/windows` — `{range: ms}` of every range with its own window.
pub(super) async fn list_windows(
    State(st): State<AppState>,
    Query(q): Query<cairn_store::entropy::HashMap<String, String>>,
) -> ApiResult<Json<serde_json::Value>> {
    let Some(w) = &st.windows else {
        return Err(ApiError(
            StatusCode::NOT_IMPLEMENTED,
            "node has no window registry".into(),
        ));
    };
    let m: serde_json::Map<String, serde_json::Value> = w
        .read()
        .await
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e))?
        .into_iter()
        .map(|(r, d)| (r.to_string(), serde_json::json!(d.as_millis() as u64)))
        .collect();
    // `?format=rows` (`sys_range_window`): `{rows: [{range, windowMs}]}` instead of the
    // object keyed by range — the old shape is unchanged.
    if q.get("format").is_some_and(|f| f == "rows") {
        let rows: Vec<serde_json::Value> = m
            .iter()
            .map(|(r, ms)| serde_json::json!({ "range": r.parse::<u32>().ok(), "windowMs": ms }))
            .collect();
        return Ok(Json(serde_json::json!({ "rows": rows })));
    }
    Ok(Json(serde_json::Value::Object(m)))
}
