// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `GET /audit`: a tenant's audit records over a time span — reads the objects under
//! `audit/tenant/<tenant>/<day>/`.

use super::*;

/// The default span when there is no `from`.
const DEFAULT_SPAN_MS: u64 = 24 * 3_600_000;
/// Cap on the number of days per read.
const MAX_DAYS: u64 = 31;
/// Cap on the objects read per request (one GET per object).
const MAX_OBJECTS: usize = 1_000;

/// `GET /audit?from=<ms>&to=<ms>&actor=<id>&action=<name>&tenant=<t>` — the key's
/// tenant; another `?tenant=` only for `Cluster`/`Provision`. Records in increasing
/// time order, each with `kind` (`row`, `intent`, `done`, `denied`, `noop`) and `name`
/// (object name). `truncated` = hit the read cap, narrow the span.
pub(super) async fn get_audit(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<serde_json::Value>> {
    let sink = st.audit_sink().ok_or_else(|| {
        ApiError(
            StatusCode::NOT_IMPLEMENTED,
            "this node does not record audit".into(),
        )
    })?;
    let tenant = match q.get("tenant") {
        Some(t) => {
            let t = cairn_store::TenantId::new(t)
                .map_err(|e| ApiError(StatusCode::BAD_REQUEST, e.to_string()))?;
            if t != who.tenant && !who.may_pick_tenant() {
                return Err(ApiError(
                    StatusCode::FORBIDDEN,
                    "another tenant's audit needs a provision or cluster-admin key".into(),
                ));
            }
            t
        }
        None => who.tenant.clone(),
    };
    let num = |k: &str| -> ApiResult<Option<u64>> {
        q.get(k)
            .map(|v| {
                v.parse::<u64>()
                    .map_err(|_| ApiError(StatusCode::BAD_REQUEST, format!("{k} must be ms")))
            })
            .transpose()
    };
    let to = num("to")?.unwrap_or_else(cairn_store::entropy::now_ms);
    let from = num("from")?.unwrap_or(to.saturating_sub(DEFAULT_SPAN_MS));
    if from > to {
        return Err(ApiError(StatusCode::BAD_REQUEST, "from > to".into()));
    }
    let (d0, d1) = (from / 86_400_000, to / 86_400_000);
    if d1 - d0 + 1 > MAX_DAYS {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            format!("window is at most {MAX_DAYS} days"),
        ));
    }
    // The tenant's read bucket: empty ⇒ `429` before any GET; the objects actually read
    // are charged to the bucket after the read loop (up to [`MAX_OBJECTS`]).
    let budget = st.query();
    if budget.is_some_and(|svc| !svc.spend_store_reads(tenant.as_str(), 0)) {
        return Err(super::archive::read_budget_spent());
    }
    let (actor, action) = (q.get("actor"), q.get("action"));
    let mut records = Vec::new();
    let mut read = 0usize;
    let mut truncated = false;
    for d in d0..=d1 {
        let day = cairn_store::audit::day_of(d * 86_400_000);
        let left = MAX_OBJECTS - read;
        let objects =
            cairn_store::audit::list_day(sink.store(), sink.prefix(), tenant.as_str(), &day, left)
                .await
                .map_err(store_error)?;
        read += objects.len();
        for (path, bytes) in objects {
            let Ok(mut v) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
                continue;
            };
            let ts = v["ts"].as_u64().unwrap_or(0);
            if ts < from || ts > to {
                continue;
            }
            if actor.is_some_and(|a| v["actor"]["id"].as_str() != Some(a.as_str())) {
                continue;
            }
            if action.is_some_and(|a| v["action"].as_str() != Some(a.as_str())) {
                continue;
            }
            let name = path.filename().unwrap_or_default().to_string();
            let kind = if name.starts_with("r-") {
                "row"
            } else if name.ends_with(".intent") {
                "intent"
            } else if name.ends_with(".done") {
                "done"
            } else if name.ends_with(".denied") {
                "denied"
            } else if name.ends_with(".noop") {
                "noop"
            } else {
                "other"
            };
            v["name"] = name.into();
            v["kind"] = kind.into();
            // Fixed keys of every record (null when not applicable) — a stable shape
            // for the console and `sys_audit`.
            for k in ["status", "count", "target", "ip", "userAgent", "requestId"] {
                if v.get(k).is_none() {
                    v[k] = serde_json::Value::Null;
                }
            }
            records.push(v);
        }
        if read >= MAX_OBJECTS {
            truncated = true;
            break;
        }
    }
    if let Some(svc) = budget {
        svc.spend_store_reads(tenant.as_str(), read as u64);
    }
    records.sort_by(|a, b| {
        (a["ts"].as_u64(), a["name"].as_str()).cmp(&(b["ts"].as_u64(), b["name"].as_str()))
    });
    Ok(Json(serde_json::json!({
        "tenant": tenant.as_str(),
        "from": from,
        "to": to,
        "records": records,
        "truncated": truncated,
    })))
}
