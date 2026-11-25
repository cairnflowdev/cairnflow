// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Journal view and archive.

use super::*;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JournalEntryView {
    pub index: u32,
    /// Message type code, as `0x0402`.
    pub ty: String,
    /// Human-readable name of the message type (`GetState`, `Sleep`, ...). Absent for
    /// unknown codes or v2 codes — the client shows `ty` when there is no name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<&'static str>,
    /// Payload as hex — enough to compare two runs bit-exactly.
    pub payload: String,
    /// `?decode=1`: the decoded header — the same JSON as the
    /// `sys_journal.entry_lite` column.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entry: Option<serde_json::Value>,
    /// `?decode=1`: the user body `{encoding, size, truncated, text|base64}`
    /// — the same JSON as `sys_journal.body`; absent when the entry has no body.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<serde_json::Value>,
}

/// Header of `GET /invocations/{id}/journal`: the next entry index when the page has
/// not reached the end of the journal. Absent = last page.
pub const JOURNAL_NEXT_HEADER: &str = "x-cairnflow-journal-next";
/// Entry cap of one page (`?limit=`; the default is also the cap).
pub const JOURNAL_PAGE_MAX_ENTRIES: usize = 10_000;
/// Payload byte cap of one page (after the first entry): one read request does not
/// load a whole large journal into the node's RAM.
pub const JOURNAL_PAGE_MAX_BYTES: usize = 4 << 20;

pub(super) async fn journal(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path(id): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<axum::response::Response> {
    let decode = matches!(q.get("decode").map(String::as_str), Some("1" | "true"));
    let raw = unhex(&id).ok_or_else(|| {
        ApiError(
            StatusCode::BAD_REQUEST,
            "id is not 48 hex characters".into(),
        )
    })?;
    let from = match q.get("from") {
        None => 0,
        Some(v) => v.parse::<u32>().map_err(|_| {
            ApiError(
                StatusCode::BAD_REQUEST,
                "from must be an entry index (a number ≥ 0)".into(),
            )
        })?,
    };
    let limit = match q.get("limit") {
        None => JOURNAL_PAGE_MAX_ENTRIES,
        Some(v) => match v.parse::<usize>() {
            Ok(n) if (1..=JOURNAL_PAGE_MAX_ENTRIES).contains(&n) => n,
            _ => {
                return Err(ApiError(
                    StatusCode::BAD_REQUEST,
                    format!("limit must be a number from 1 to {JOURNAL_PAGE_MAX_ENTRIES}"),
                ))
            }
        },
    };
    ensure_visible(&st, &who, &raw).await?;
    let (_, entries) = st
        .router
        .ask_range(cairn_store::partition_key_of(&raw), |e| async move {
            e.journal_page(raw, from, limit, JOURNAL_PAGE_MAX_BYTES)
                .await
        })
        .await
        .ok_or_else(range_settling)?;
    // Read source ([`READ_SOURCE_HEADER`]), like `status`: a follower's page is the
    // durable journal, possibly older than the page the leader just returned.
    let ((entries, next), source) = match entries {
        Ok(page) => (page, "leader"),
        // Like `status`: the follower serves when it has data, defers to the leader otherwise.
        Err(e) if e.is_fenced() => match follower_journal_page(&st, &raw, from, limit).await {
            Some(page) => (page, "follower"),
            None => {
                return Err(ApiError::from_actor_routed(
                    e,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &st,
                    &raw,
                ))
            }
        },
        Err(e) => {
            return Err(ApiError::from_actor_routed(
                e,
                StatusCode::INTERNAL_SERVER_ERROR,
                &st,
                &raw,
            ))
        }
    };
    let mut decoded = if decode {
        cairn_query::journal_json::decode_journal(
            &entries
                .iter()
                .map(|(_, ty, p)| (*ty, Bytes::copy_from_slice(p)))
                .collect::<Vec<_>>(),
            cairn_query::keyed::VALUE_MAX,
        )
        .into_iter()
        .map(Some)
        .collect()
    } else {
        Vec::new()
    };
    decoded.resize_with(entries.len(), || None);
    let body = Json(
        entries
            .into_iter()
            .zip(decoded)
            .map(|((index, ty, payload), d)| JournalEntryView {
                index,
                ty: format!("0x{ty:04X}"),
                name: cairn_protocol::MessageType(ty).name(),
                payload: payload.iter().map(|b| format!("{b:02x}")).collect(),
                entry: d.as_ref().map(|d| d.lite.clone()),
                body: d.and_then(|d| d.body),
            })
            .collect::<Vec<_>>(),
    );
    let mut res = body.into_response();
    res.headers_mut().insert(
        super::invocations::READ_SOURCE_HEADER,
        axum::http::HeaderValue::from_static(source),
    );
    if let Some(next) = next {
        res.headers_mut()
            .insert(JOURNAL_NEXT_HEADER, axum::http::HeaderValue::from(next));
    }
    Ok(res)
}

// ─────────────────────────────────────────────────── archive (6.1, 6.2)

/// `GET /archive/{id}[?date=yyyy-mm-dd][&days=N][&verify=true]` — an invocation that
/// **has been purged**, read from the acting tenant's archive object (admin).
///
/// Lookup by day: the object lives at `archive/<tenant>/<purge due day>/<id>`, and that
/// day cannot be derived from the id — so `?date=` goes straight there, otherwise we
/// walk back from today at most `days` days (default 31, cap 31), one GET per day.
/// This is an audit path, not a hot path; no secondary index has to be written at purge.
///
/// Shape = `/invocations/{id}` + `/journal`: `{id, status, output?,
/// failure?, journal}`, plus `purgedAtMs`, `chain` and — with `verify=true` —
/// `verified` (`true`/`false`; `null` when the tenant had no chain enabled when the
/// invocation was created). An archive that cannot be decoded ⇒ `verified: false` with
/// `error`.
/// `GET /archive?date=yyyy-mm-dd&limit=` (`sys_archive`) — the tenant's purged
/// invocations of one UTC day: `{date, entries: [{id, run, bytes, writtenAtMs}]}`, by
/// id, at most `limit` (default and cap 10 000). Detail (journal, status, hash chain):
/// `GET /archive/{id}`.
pub(super) async fn list_archive(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<serde_json::Value>> {
    let tenant = acting_tenant(&who, &q)?;
    let store = st.archive.as_ref().ok_or_else(|| {
        ApiError(
            StatusCode::NOT_IMPLEMENTED,
            "archiving is disabled on this node (--no-archive)".into(),
        )
    })?;
    let date = q.get("date").filter(|d| is_day(d)).ok_or_else(bad_date)?;
    let limit = match q.get("limit") {
        Some(v) => v
            .parse::<usize>()
            .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "limit is not a number".into()))?
            .min(10_000),
        None => 10_000,
    };
    let rows = store
        .list_day(&tenant, date, limit)
        .await
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let entries: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|(id, run, bytes, at)| {
            serde_json::json!({ "id": id, "run": run, "bytes": bytes, "writtenAtMs": at })
        })
        .collect();
    Ok(Json(
        serde_json::json!({ "date": date, "entries": entries }),
    ))
}

/// `yyyy-mm-dd`: ten bytes, digits everywhere except two dashes at positions 4 and 7.
fn is_day(d: &str) -> bool {
    d.len() == 10
        && d.bytes().enumerate().all(|(i, b)| {
            if i == 4 || i == 7 {
                b == b'-'
            } else {
                b.is_ascii_digit()
            }
        })
}

fn bad_date() -> ApiError {
    ApiError(StatusCode::BAD_REQUEST, "date must be yyyy-mm-dd".into())
}

/// Cap on the days `GET /archive/{id}` walks back when there is no `?date=`. Each day is
/// at least one store GET.
const MAX_FIND_DAYS: u32 = 31;

/// `429`: the tenant's read bucket is empty — shared by the REST read paths that go
/// straight to objects (`/archive`, `/audit`).
pub(super) fn read_budget_spent() -> ApiError {
    ApiError(
        StatusCode::TOO_MANY_REQUESTS,
        "query_budget: the tenant has used up its read units, retry later".into(),
    )
}

pub(super) async fn get_archive(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path(id): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<serde_json::Value>> {
    let raw = unhex(&id).ok_or_else(|| {
        ApiError(
            StatusCode::BAD_REQUEST,
            "id is not 48 hex characters".into(),
        )
    })?;
    let tenant = acting_tenant(&who, &q)?;
    let store = st.archive.as_ref().ok_or_else(|| {
        ApiError(
            StatusCode::NOT_IMPLEMENTED,
            "archiving is disabled on this node (--no-archive)".into(),
        )
    })?;
    // Each day walked back is at least one sequential store GET: a cap of 31 days, and
    // the expected GET count is charged to the tenant's read bucket **before** reading
    // — an unknown id is no longer 366 free GETs for a read-only key.
    let days: u32 = match q.get("days") {
        Some(v) => v
            .parse::<u32>()
            .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "days is not a number".into()))?
            .min(MAX_FIND_DAYS),
        None => MAX_FIND_DAYS,
    };
    let verify = q.get("verify").is_some_and(|v| v == "true" || v == "1");
    // `?date=` goes into the object path: check its shape before building it, like
    // `GET /archive?date=`.
    if q.get("date").is_some_and(|d| !is_day(d)) {
        return Err(bad_date());
    }
    let gets = if q.contains_key("date") {
        1
    } else {
        days.max(1)
    };
    if st
        .query()
        .is_some_and(|svc| !svc.spend_store_reads(tenant.as_str(), u64::from(gets)))
    {
        return Err(read_budget_spent());
    }
    let bytes = store
        .find(
            &tenant,
            &raw,
            q.get("date").map(String::as_str),
            now_ms(),
            days,
        )
        .await
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| {
            ApiError(
                StatusCode::NOT_FOUND,
                "no archive for this invocation".into(),
            )
        })?;
    let rec = match cairn_store::archive::ArchiveRecord::decode(&bytes) {
        Ok(r) if r.id == raw && r.tenant == tenant => r,
        Ok(_) if verify => {
            return Ok(Json(serde_json::json!({
                "id": id, "verified": false, "error": "archive carries an id/tenant different from the path",
            })))
        }
        Ok(_) => {
            return Err(ApiError(
                StatusCode::UNPROCESSABLE_ENTITY,
                "archive carries an id/tenant different from the path".into(),
            ))
        }
        Err(e) if verify => {
            return Ok(Json(serde_json::json!({
                "id": id, "verified": false, "error": e.to_string(),
            })))
        }
        Err(e) => return Err(ApiError(StatusCode::UNPROCESSABLE_ENTITY, e.to_string())),
    };
    let mut out = serde_json::json!({ "id": id, "purgedAtMs": rec.purged_at_ms });
    match InvocationStatus::decode(rec.status.clone()) {
        Some(InvocationStatus::Completed(cairn_engine::InvocationOutput::Inline(v))) => {
            out["status"] = "completed".into();
            out["output"] = String::from_utf8_lossy(&v).into_owned().into();
        }
        // Segmented output: the archive carries the ref like the journal does — report
        // the length, never read the tail as the whole value.
        Some(InvocationStatus::Completed(cairn_engine::InvocationOutput::Segmented(o))) => {
            out["status"] = "completed".into();
            out["outputSegmented"] = true.into();
            out["outputLen"] = o.len().into();
        }
        Some(InvocationStatus::Failed { code, message }) => {
            out["status"] = "failed".into();
            out["failure"] = format!("{code}: {message}").into();
        }
        Some(other) => out["status"] = status_str(&other).into(),
        None => out["status"] = "unknown".into(),
    }
    let mut journal = Vec::with_capacity(rec.journal.len());
    for (index, entry) in &rec.journal {
        let view = match cairn_runtime::JournalEntry::decode(entry.clone()) {
            Ok(e) => JournalEntryView {
                index: *index,
                ty: format!("0x{:04X}", e.ty.raw()),
                name: e.ty.name(),
                payload: e.payload.iter().map(|b| format!("{b:02x}")).collect(),
                entry: None,
                body: None,
            },
            Err(_) => JournalEntryView {
                index: *index,
                ty: "?".into(),
                name: None,
                payload: entry.iter().map(|b| format!("{b:02x}")).collect(),
                entry: None,
                body: None,
            },
        };
        journal.push(view);
    }
    out["journal"] = serde_json::to_value(journal).unwrap_or_default();
    out["chain"] = match &rec.chain {
        Some((len, h)) => serde_json::json!({
            "len": len,
            "hash": h.iter().map(|b| format!("{b:02x}")).collect::<String>(),
        }),
        None => serde_json::Value::Null,
    };
    if verify {
        out["verified"] = rec.verify().map_or(serde_json::Value::Null, Into::into);
    }
    Ok(Json(out))
}
