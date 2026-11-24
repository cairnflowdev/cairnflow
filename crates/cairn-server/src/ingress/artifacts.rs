// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `GET /invocations/{id}/artifacts` and `…/artifacts/{sha}`: reading an invocation's
//! artifacts, **only on the leader** (AP-13), checking the tenant first (AP-7).
//!
//! The Db's `Artifacts` row is the truth: an object with no row (an orphan, or from a
//! purged earlier life with the same id) cannot be read through here even if it is
//! still in the bucket — the route looks up the row to learn the epoch, then GETs the
//! object.

use super::*;

/// The artifact store on the node's main store — the same store as metering.
fn artifact_store(st: &AppState) -> Result<cairn_store::artifact::ArtifactStore, ApiError> {
    let Some((store, prefix)) = &st.metering else {
        return Err(ApiError(
            StatusCode::NOT_IMPLEMENTED,
            "node has no store for artifacts".into(),
        ));
    };
    Ok(cairn_store::artifact::ArtifactStore::new(
        Arc::clone(store),
        prefix,
    ))
}

/// The invocation's tenant (permission already checked) and its artifact rows, read on
/// the leader.
async fn rows(
    st: &AppState,
    who: &crate::auth::Identity,
    id: &str,
) -> Result<(InvocationId, String, crate::actor::ArtifactLookup), ApiError> {
    let raw = unhex(id).ok_or_else(|| {
        ApiError(
            StatusCode::BAD_REQUEST,
            "id is not 48 hex characters".into(),
        )
    })?;
    let access = super::streams::leader_access(st, who, raw).await?;
    let tenant = access
        .target
        .as_ref()
        .map(|t| t.tenant.as_str().to_string())
        .unwrap_or_default();
    let (_, rows) = st
        .router
        .ask_range(cairn_store::partition_key_of(&raw), |e| async move {
            e.artifacts(raw).await
        })
        .await
        .ok_or_else(range_settling)?;
    let rows = rows
        .map_err(|e| ApiError::from_actor_routed(e, StatusCode::INTERNAL_SERVER_ERROR, st, &raw))?;
    Ok((raw, tenant, rows))
}

/// `{artifacts: [{sha256, size, contentType}]}` — corrupt rows are left out of the
/// list.
pub(super) async fn list(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path(id): Path<String>,
) -> ApiResult<Json<serde_json::Value>> {
    let (_, _, found) = rows(&st, &who, &id).await?;
    let items: Vec<_> = found
        .rows
        .iter()
        .filter_map(|(sha, row)| {
            row.as_ref().map(|r| {
                serde_json::json!({
                    "sha256": cairn_store::artifact::sha_hex(sha),
                    "size": r.size,
                    "contentType": r.content_type,
                })
            })
        })
        .collect();
    Ok(Json(serde_json::json!({ "artifacts": items })))
}

/// The bytes of artifact `sha`, `content-type` as at PUT time.
pub(super) async fn get_one(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path((id, sha)): Path<(String, String)>,
) -> Result<axum::response::Response, ApiError> {
    let want = cairn_store::artifact::sha_from_hex(&sha.to_ascii_lowercase()).ok_or_else(|| {
        ApiError(
            StatusCode::BAD_REQUEST,
            "sha must be 64 hex characters".into(),
        )
    })?;
    let (raw, tenant, found) = rows(&st, &who, &id).await?;
    let not_found = || ApiError(StatusCode::NOT_FOUND, "no such artifact".into());
    let row = found
        .rows
        .into_iter()
        .find(|(s, _)| *s == want)
        .and_then(|(_, r)| r)
        .ok_or_else(not_found)?;
    let owner = cairn_store::artifact::Owner {
        tenant,
        id: raw,
        epoch: row.epoch,
    };
    // The range's cache first; the row was checked above, so the cache never returns an
    // artifact of a purged earlier life.
    let bytes = match found.cache.as_ref().and_then(|c| c.get(&want)) {
        Some(b) => b,
        None => {
            // A store read error (usually transient: the S3 client has run out of
            // retries) ⇒ `503 store_unavailable`, the client retries. Detail (object
            // path, S3 client text) goes only to the log — the masking rule.
            let b = artifact_store(&st)?
                .get(&owner, &want)
                .await
                .map_err(|e| super::error::store_unavailable("artifact read", &e))?
                .ok_or_else(not_found)?;
            crate::metrics::agent::bump(&crate::metrics::agent::ARTIFACT_GETS);
            if let Some(c) = &found.cache {
                c.insert(want, b.clone());
            }
            b
        }
    };
    let ct = if row.content_type.is_empty() {
        "application/octet-stream".to_string()
    } else {
        row.content_type
    };
    // **Inert** content (security review): the content type is set freely by the
    // handler, and the API accepts the console's session cookie — an HTML/SVG artifact
    // opened as a page would run script on this origin with the viewer's session.
    // `sandbox` gives the document an empty origin and no script; `nosniff` keeps the
    // declared type; apart from a few harmless view-in-place types, the browser downloads.
    let disposition = if inline_safe(&ct) {
        "inline"
    } else {
        "attachment"
    };
    Ok((
        [
            (axum::http::header::CONTENT_TYPE, ct),
            (
                axum::http::header::CACHE_CONTROL,
                "private, max-age=31536000, immutable".to_string(),
            ),
            (
                axum::http::header::X_CONTENT_TYPE_OPTIONS,
                "nosniff".to_string(),
            ),
            (
                axum::http::header::CONTENT_SECURITY_POLICY,
                "sandbox; default-src 'none'".to_string(),
            ),
            (
                axum::http::header::CONTENT_DISPOSITION,
                disposition.to_string(),
            ),
        ],
        bytes,
    )
        .into_response())
}

/// Types that can be viewed in place without running anything: raster images, plain
/// text, JSON. HTML, SVG, every `*xml`, PDF and unknown types ⇒ download.
fn inline_safe(content_type: &str) -> bool {
    let essence = content_type
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    matches!(
        essence.as_str(),
        "image/png"
            | "image/jpeg"
            | "image/gif"
            | "image/webp"
            | "image/avif"
            | "text/plain"
            | "application/json"
    )
}

#[cfg(test)]
mod tests {
    use super::inline_safe;

    #[test]
    fn only_inert_types_render_inline() {
        for ok in ["image/png", "text/plain; charset=utf-8", "Application/JSON"] {
            assert!(inline_safe(ok), "{ok}");
        }
        for bad in [
            "text/html",
            "image/svg+xml",
            "application/xhtml+xml",
            "text/xml",
            "application/pdf",
            "application/octet-stream",
            "",
            "text/plain+html",
        ] {
            assert!(!inline_safe(bad), "{bad}");
        }
    }
}
