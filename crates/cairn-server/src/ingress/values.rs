// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `GET /values/v1/{token}` — the `proxy` read path for segmented values.
//!
//! No API key needed: the token **is** the permission to read exactly one segment,
//! issued by a cluster node to the tenant's deployment in `Start.segment_urls`/
//! `SegmentUrls`, short-lived ([`crate::value_urls::Token`]). The mac covers tenant,
//! range, owner, segment and expiry, with a key derived from the cluster secret — any
//! node of the cluster can open it, and a token of this tenant opens nothing of another
//! tenant.
//!
//! - Every token error (decoding, mac, expiry) ⇒ `403` with the same body.
//! - Object missing ⇒ `404`; wrong sha or length ⇒ `500` (never send corrupt bytes).
//! - Never log the token or the path at any level.

use super::*;

/// Body of every token error.
const BAD_TOKEN: &str = "value URL is invalid or expired";

/// Concurrent segment reads of one tenant on the node. Each read loads a whole segment
/// (≤ 16 MiB) into RAM: without a cap, N connections with the same token are N ×
/// segment. Over ⇒ `429` with `Retry-After`.
pub(crate) const VALUE_PROXY_PER_TENANT: usize = 8;

pub(super) async fn get_segment(
    State(st): State<AppState>,
    axum::extract::Path(token): axum::extract::Path<String>,
) -> Result<axum::response::Response, ApiError> {
    let (Some(secret), Some(values)) = (&st.cluster_secret, st.value_store()) else {
        return Err(ApiError(
            StatusCode::NOT_IMPLEMENTED,
            "node serves no value URLs".into(),
        ));
    };
    let key = crate::value_urls::ProxyKey::from_cluster(secret);
    let seg = crate::value_urls::Token::open(&key, &token, crate::gc::wall_ms())
        .map_err(|_| ApiError(StatusCode::FORBIDDEN, BAD_TOKEN.into()))?;
    // The tenant in the token has passed the mac. Deleting/deleted ⇒ like a bad token: a
    // token issued before that no longer opens data (as in the key layer of `auth`).
    let tenant = cairn_store::TenantId::new(&seg.tenant)
        .map_err(|_| ApiError(StatusCode::FORBIDDEN, BAD_TOKEN.into()))?;
    if st
        .auth()
        .tenant_states
        .as_ref()
        .is_some_and(|s| s.snapshot().of(&tenant).state.refuses_access())
    {
        return Err(ApiError(StatusCode::FORBIDDEN, BAD_TOKEN.into()));
    }
    let Some(_slot) =
        super::streams::TenantSlot::take_in(&st.value_open, &tenant, VALUE_PROXY_PER_TENANT)
    else {
        let mut resp = ApiError(
            StatusCode::TOO_MANY_REQUESTS,
            format!(
                "{VALUE_PROXY_PER_TENANT} segment reads of this tenant are already running on this node — retry later"
            ),
        )
        .into_response();
        resp.headers_mut().insert(
            axum::http::header::RETRY_AFTER,
            axum::http::HeaderValue::from_static("1"),
        );
        return Ok(resp);
    };
    // Store client text (bucket, object path) does not go into the body of this
    // keyless route, nor into the log (the path carries owner/sha — the rule at the
    // file head).
    //
    // Cache hit ⇒ no rehash (the cache only holds bytes that matched the sha); miss ⇒
    // hash off the worker threads.
    let got = values.get_verified(&seg).await.map_err(|_| {
        ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "cannot read the segment right now — retry later".into(),
        )
    })?;
    let bytes = match got {
        cairn_store::value::Verified::Bytes(b) => b,
        cairn_store::value::Verified::Missing => {
            return Err(ApiError(StatusCode::NOT_FOUND, "segment not found".into()))
        }
        cairn_store::value::Verified::Mismatch => {
            return Err(ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                "segment does not match its sha256".into(),
            ))
        }
    };
    let max_age = st.value_urls.ttl().as_secs();
    // Segment bytes the node sends out through the `proxy` path (egress).
    crate::metrics::agent::add(
        &crate::metrics::agent::VALUE_PROXY_BYTES,
        bytes.len() as u64,
    );
    // The slot is held until the body has been sent: the body is a one-chunk stream
    // carrying the slot; hyper only asks for the next chunk (and the stream is only
    // dropped — releasing the slot) once the write buffer has drained, or when the
    // connection closes. A slow-reading client does not hold segment copies beyond the
    // cap.
    let len = bytes.len();
    let body = futures::stream::unfold((Some(bytes), _slot), |(bytes, slot)| async move {
        // No next chunk ⇒ the state (and the slot) is dropped here.
        let b = bytes?;
        Some((Ok::<_, std::convert::Infallible>(b), (None, slot)))
    });
    Ok((
        [
            (
                axum::http::header::CONTENT_TYPE,
                "application/octet-stream".to_string(),
            ),
            (
                axum::http::header::CACHE_CONTROL,
                format!("private, max-age={max_age}, immutable"),
            ),
            (axum::http::header::CONTENT_LENGTH, len.to_string()),
        ],
        axum::body::Body::from_stream(body),
    )
        .into_response())
}
