// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The internal port `/internal/*` (node calling node): inbound, resign, session changed.

use super::*;

// ────────────────────────────────────────────── inter-partition (internal)

/// Receives a message from another partition.
///
/// `200` is a promise that the message **is durable** in the destination partition's
/// state — the sender deletes its outbox row only when it sees this status (INV-5 at
/// the boundary between two partitions). So this function must not return `200` early
/// for any reason.
///
/// It **does not process** the message **while the sender waits** (IR-7): processing
/// synchronously would deadlock two engine threads on a cyclic call chain A→B→A. The
/// destination range's drainer may read the message before it is durable (it is poked
/// as soon as the receiving commit is submitted), but `200` does not wait for the
/// drainer — only for the receiving commit to be durable (LF-4).
pub(super) async fn inbound(
    State(st): State<AppState>,
    Query(route): Query<InboundRoute>,
    body: Bytes,
) -> ApiResult<StatusCode> {
    let msg = InterPartition::decode(body)
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, format!("malformed message: {e}")))?;
    cairn_store::probe::mark("inbound.in", 1);
    let (engine, entry, table) = owning_engine(&st, &route, std::slice::from_ref(&msg)).await?;
    // The check runs again **inside the actor**: between the table lookup here and the
    // message reaching the engine, the range may have started retiring.
    engine.inbound(msg).await.map_err(|e| match e {
        crate::actor::ActorError::WrongRange => {
            ApiError::wrong_range(format!("range {} is retiring", entry.id.0), &table)
        }
        e => ApiError::from_actor(e, StatusCode::INTERNAL_SERVER_ERROR),
    })?;
    cairn_store::probe::mark("inbound.out", 1);
    Ok(StatusCode::OK)
}

/// Receives a **batch** of messages for one range: the body is
/// [`InterPartition::encode_batch`].
///
/// **All or nothing.** The D23 check runs once for the whole batch — table version, the
/// range the sender claims, intent — and **every** message must belong to exactly that
/// range; one stray message is `409 wrong_range` for the whole batch (the sender learns
/// the table and re-splits the batch). Then one `Command::InboundBatch` → one commit →
/// `200` once that commit is durable. An error anywhere ⇒ no message is written, the
/// sender keeps every row (IR-2).
///
/// **This route never returns `404`/`405`.** The sender reads `404`/`405` here as "old
/// binary, no batch route yet" and falls back to sending one message at a time; a
/// `404` from the handler itself (if any) is turned into `503` so it is not misread.
pub(super) async fn inbound_batch(
    State(st): State<AppState>,
    Query(route): Query<InboundRoute>,
    body: Bytes,
) -> ApiResult<StatusCode> {
    receive_batch(&st, &route, body).await.map_err(|e| {
        if matches!(
            e.status,
            StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED
        ) {
            e.with_status(StatusCode::SERVICE_UNAVAILABLE)
        } else {
            e
        }
    })
}

async fn receive_batch(st: &AppState, route: &InboundRoute, body: Bytes) -> ApiResult<StatusCode> {
    let msgs = InterPartition::decode_batch(body)
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, format!("malformed batch: {e}")))?;
    if msgs.len() > crate::transport::MAX_BATCH_MESSAGES {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            format!(
                "batch of {} messages exceeds the limit {}",
                msgs.len(),
                crate::transport::MAX_BATCH_MESSAGES
            ),
        ));
    }
    if msgs.is_empty() {
        return Ok(StatusCode::OK);
    }
    cairn_store::probe::mark("inbound.in", msgs.len() as u64);
    let n = msgs.len() as u64;
    let (engine, entry, table) = owning_engine(st, route, &msgs).await?;
    engine.inbound_batch(msgs).await.map_err(|e| match e {
        crate::actor::ActorError::WrongRange => {
            ApiError::wrong_range(format!("range {} is retiring", entry.id.0), &table)
        }
        e => ApiError::from_actor(e, StatusCode::INTERNAL_SERVER_ERROR),
    })?;
    cairn_store::probe::mark("inbound.out", n);
    Ok(StatusCode::OK)
}

/// **Do the messages belong to us** (D23) — shared by single messages and batches. The
/// sender says which range it routed by, with which table version; we recompute from
/// the id of **each** message with our own table. Returns that range's engine, the
/// table entry and the table used for the verdict (for the actor's `409`).
///
/// The sender holds a **newer** table ⇒ ours is stale: reload before judging, do not
/// refuse a correct message just because we are one tick behind.
async fn owning_engine(
    st: &AppState,
    route: &InboundRoute,
    msgs: &[InterPartition],
) -> ApiResult<(
    crate::actor::EngineHandle,
    cairn_store::RangeEntry,
    std::sync::Arc<cairn_store::PartitionTable>,
)> {
    if let Some(v) = route.table {
        if v > st.router.cluster.version() {
            if let Err(e) = st.router.cluster.reload().await {
                // Not knowing what the new table says ⇒ no verdict possible; the sender
                // retries later (its outbox row is intact).
                return Err(ApiError(
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!(
                        "sender uses range table version {v}, this node could not reload it: {e}"
                    ),
                ));
            }
        }
    }
    let table = st.router.cluster.table();
    let range_of =
        |m: &InterPartition| *table.range_of_key(cairn_store::partition_key_of(m.destination_id()));
    let entry = range_of(msgs.first().expect("never called with an empty batch"));
    // Every refusal below is `409 wrong_range` **with our table**: the sender applies
    // it (if newer than its own), recomputes the destination, and resends within the
    // turn — no one else to ask, no loop on the same version.
    let wrong = |why: String| ApiError::wrong_range(why, &table);
    if let Some(v) = route.table {
        if v != table.version() {
            return Err(wrong(format!(
                "sender routes by range table version {v}, this node uses {}",
                table.version()
            )));
        }
    }
    if let Some(p) = route.range.or(route.partition) {
        if p != entry.id.0 {
            return Err(wrong(format!(
                "message belongs to range {}, sender routed it to {p}",
                entry.id.0
            )));
        }
    }
    // Batch: every message in the same range as the first, otherwise the whole batch is misrouted.
    if let Some(stray) = msgs.iter().map(range_of).find(|r| r.id != entry.id) {
        return Err(wrong(format!(
            "batch mixes a message of range {} into a batch for range {}",
            stray.id.0, entry.id.0
        )));
    }
    if matches!(
        entry.intent,
        Some(cairn_store::RangeIntent::Splitting { .. })
    ) {
        return Err(wrong(format!("range {} is splitting", entry.id.0)));
    }
    let engine = st.router.partitions.get(entry.id).ok_or_else(|| {
        // The table says the range exists, but the node has not finished building its
        // engine (just applied a new table) — transient; the sender retries.
        ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "this node is not running the engine of range {} yet",
                entry.id.0
            ),
        )
    })?;
    Ok((engine, entry, table))
}

/// The routing the sender used — see [`inbound`]. Every field optional: missing means
/// not checked (compatible with hand-written senders, e.g. tests).
/// `partition` is the old name of `range`.
#[derive(Deserialize)]
pub(super) struct InboundRoute {
    range: Option<u32>,
    partition: Option<u32>,
    table: Option<u64>,
}

/// `POST /internal/ranges/{id}/resign` — hands this range over (asked for by the merge
/// coordinator on another node, D-e). Answers once sealing is done.
pub(super) async fn resign_range(
    State(st): State<AppState>,
    Path(id): Path<u32>,
) -> ApiResult<StatusCode> {
    // The requester has written an intent onto the table; reload **before** handing
    // over, so this node's balancer sees the intent and does not fight for the range
    // just handed over.
    if let Err(e) = st.router.cluster.reload().await {
        tracing::debug!(range = id, error = %e, "cannot reload the table before yielding");
    }
    let engine = st
        .router
        .partitions
        .get(cairn_store::PartitionId(id))
        .ok_or_else(|| {
            ApiError(
                StatusCode::NOT_FOUND,
                format!("this node does not run range {id}"),
            )
        })?;
    engine
        .resign()
        .await
        .map_err(|e| ApiError::from_actor(e, StatusCode::INTERNAL_SERVER_ERROR))?;
    Ok(StatusCode::OK)
}

/// `POST /internal/ranges/{id}/tenant-purge`: one batch deleting rows of a tenant being
/// purged on range `id`, which this node leads. The permission (`TenantPurgeGrant`) is
/// built **here**, reading a fresh bucket, outside the engine (D16) — the requester only
/// names the tenant and the cursor. Answers once the batch is durable.
pub(super) async fn tenant_purge_rows(
    State(st): State<AppState>,
    Path(id): Path<u32>,
    Json(req): Json<crate::tenant_purge::RowsRequest>,
) -> ApiResult<Json<crate::tenant_purge::RowsReply>> {
    let purger = st.tenant_purge().ok_or_else(|| {
        ApiError(
            StatusCode::NOT_IMPLEMENTED,
            "this node has no tenant purge loop".into(),
        )
    })?;
    purger
        .serve_lot(cairn_store::PartitionId(id), req)
        .await
        .map(Json)
        .map_err(|e| ApiError(StatusCode::SERVICE_UNAVAILABLE, e))
}

/// `POST /internal/ranges/{id}/tenant-purge-check`: one batch **checking** rows of a
/// tenant being purged on range `id` — deletes nothing, stops at the first row. The
/// permission is built here like the delete batch.
pub(super) async fn tenant_purge_check(
    State(st): State<AppState>,
    Path(id): Path<u32>,
    Json(req): Json<crate::tenant_purge::RowsRequest>,
) -> ApiResult<Json<crate::tenant_purge::RowsReply>> {
    let purger = st.tenant_purge().ok_or_else(|| {
        ApiError(
            StatusCode::NOT_IMPLEMENTED,
            "this node has no tenant purge loop".into(),
        )
    })?;
    purger
        .serve_check(cairn_store::PartitionId(id), req)
        .await
        .map(Json)
        .map_err(|e| ApiError(StatusCode::SERVICE_UNAVAILABLE, e))
}

/// `POST /internal/session/changed` — a peer just changed its session (graceful
/// shutdown): observe the session layer again at once instead of waiting for the next
/// round, to take over its ranges early. Carries no data: we read the bucket ourselves,
/// so a fake nudge costs only one observation round — and that round has the floor
/// `MIN_REFRESH_GAP`.
pub(super) async fn session_changed(State(st): State<AppState>) -> StatusCode {
    st.router.routing.request_refresh();
    // A peer shutting down gracefully may be the coordinator that just gave up the role.
    if let Some(c) = &st.control {
        c.poke();
    }
    StatusCode::NO_CONTENT
}

/// `POST /internal/heartbeat` — a node's heartbeat to the coordinator
/// ([`crate::control`]). We do not hold the role, or the batch fence does not pass ⇒
/// `409`: the sender reads `control/coordinator` again.
pub(super) async fn heartbeat(
    State(st): State<AppState>,
    axum::Json(hb): axum::Json<crate::control::Heartbeat>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    match &st.control {
        Some(c) => match c.on_heartbeat(hb).await {
            Some(reply) => axum::Json(reply).into_response(),
            None => StatusCode::CONFLICT.into_response(),
        },
        None => StatusCode::NOT_IMPLEMENTED.into_response(),
    }
}

/// `POST /internal/owner/changed` — a peer just claimed a range whose old owner record
/// pointed at our session ([`crate::owner_notice`]): the owner record watcher GETs
/// exactly that record at once. The notice is not trusted: the grant is revoked only
/// when the bucket read shows the record is no longer ours.
pub(super) async fn owner_changed(
    State(st): State<AppState>,
    axum::Json(n): axum::Json<crate::owner_notice::OwnerChanged>,
) -> StatusCode {
    match &st.range_ops {
        Some(ops) => {
            tracing::debug!(range = n.range, term = n.term, "range owner change notice");
            ops.session.owner_changed(cairn_store::PartitionId(n.range));
            StatusCode::NO_CONTENT
        }
        None => StatusCode::NOT_IMPLEMENTED,
    }
}

/// `POST /internal/registry/apply` — a peer just wrote a registry change that must take
/// effect synchronously (key revocation; tenant state): re-read `auth/keys` **and**
/// `tenants` **before** answering — once re-read, every range's state source has moved
/// to a new generation, so the next command on this node sees the new state. `204` =
/// both applied (the sender counts it as an ack); either failing or over the cap ⇒
/// `503` (the sender puts us in `pending`). Also nudges the registry refresh loop like
/// `/internal/registry/changed`.
pub(super) async fn registry_apply(
    State(st): State<AppState>,
    Query(q): Query<HashMap<String, String>>,
) -> StatusCode {
    if let Some(s) = &st.registry_sync {
        s.poke_local();
    }
    let t0 = tokio::time::Instant::now();
    let tenants = async {
        cairn_store::failpoint!("ingress::registry_apply::tenants", |_| Err(
            "failpoint: tenants not re-read".to_string()
        ));
        match &st.tenant_settings {
            Some(t) => t.refresh().await,
            None => Ok(()),
        }
    };
    // The deployment registry is re-read too: `DELETE /deployments/{id}` requires every
    // node to drop the old revision pointer **before** the last attachment count — a
    // node with a stale RAM registry does not attach new invocations to an id being
    // removed. Only when the request asks for it (`?deployments`): read every catalog
    // in full (not by the refresh loop's etag) — key revocations do not pay that price.
    let deployments = async {
        let (true, Some(store)) = (q.contains_key("deployments"), &st.deployments) else {
            return Ok(());
        };
        let mut etags = Default::default();
        let r = crate::node::refresh_registry(store, &st.registry, &mut etags).await;
        if r.changed {
            st.router.partitions.deployments_changed().await;
        }
        if r.incomplete {
            Err("deployment registry not fully re-read".to_string())
        } else {
            Ok(())
        }
    };
    let both = async {
        let (keys, tenants, deployments) =
            tokio::join!(st.auth.keys.refresh(), tenants, deployments);
        keys.map_err(|e| e.to_string())?;
        tenants?;
        deployments
    };
    match tokio::time::timeout(Duration::from_secs(2), both).await {
        Ok(Ok(())) => {
            if let Some(s) = &st.registry_sync {
                s.confirm(t0);
            }
            StatusCode::NO_CONTENT
        }
        Ok(Err(e)) => {
            tracing::debug!(error = %e, "cannot apply auth/keys from a notice that needs an ack");
            StatusCode::SERVICE_UNAVAILABLE
        }
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

/// `POST /internal/registry/changed` — a peer just wrote a cluster-level registry
/// (deployment, key, tenant, snapshot, range window): refresh the registries now
/// instead of waiting for the poll tick. Carries no data: we read the bucket
/// ourselves, so a fake nudge costs only one refresh round, with the floor
/// `registry_sync::MIN_REFRESH_GAP`.
pub(super) async fn registry_changed(State(st): State<AppState>) -> StatusCode {
    if let Some(s) = &st.registry_sync {
        s.poke_local();
    }
    StatusCode::NO_CONTENT
}
