// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The door for external systems: signals, promises, awakeables.

use super::*;

// ───────────────────────────────────── door for external systems (human-in-the-loop)

/// The body of `POST /invocations/{id}/signal`.
///
/// `signal` is a number (built-in signal) or a string (named signal) — exactly the two
/// identity forms `SignalId` has.
#[derive(Deserialize)]
pub(super) struct SignalBody {
    signal: serde_json::Value,
    /// The payload delivered with it. Absent = `Void`, entirely different from "empty string".
    #[serde(default)]
    value: Option<serde_json::Value>,
}

#[derive(Serialize)]
pub(super) struct SignalResponse {
    id: String,
    status: String,
}

/// `POST /invocations/{id}/signal` — wakes up a waiting invocation.
///
/// This is the "send a signal" half of the human-in-the-loop door. Dedupe lives in the
/// state machine (a signal identity already seen makes a later delivery a no-op), so
/// **webhook retries are safe** — no separate idempotency key needed.
pub(super) async fn send_signal(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    meta: super::Meta,
    Path(id): Path<String>,
    Json(body): Json<SignalBody>,
) -> ApiResult<Json<SignalResponse>> {
    let raw = unhex(&id).ok_or_else(|| {
        ApiError(
            StatusCode::BAD_REQUEST,
            "id is not 48 hex characters".into(),
        )
    })?;
    ensure_visible(&st, &who, &raw).await?;

    let signal = match &body.signal {
        serde_json::Value::Number(n) => {
            let i = n
                .as_u64()
                .filter(|v| *v <= u32::MAX as u64)
                .ok_or_else(|| {
                    ApiError(
                        StatusCode::BAD_REQUEST,
                        "numeric signal must be a u32".into(),
                    )
                })?;
            cairn_engine::SignalId::Idx(i as u32)
        }
        serde_json::Value::String(s) if !s.is_empty() => cairn_engine::SignalId::Name(s.clone()),
        _ => {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                "signal must be a number (built-in) or a non-empty string (named)".into(),
            ))
        }
    };
    let value = match &body.value {
        None => cairn_engine::CompletionPayload::Void,
        Some(v) => cairn_engine::CompletionPayload::Value(Bytes::from(
            serde_json::to_vec(v).expect("a parsed JSON value re-encodes"),
        )),
    };

    // Through `ask_range`: if it arrives exactly while the range splits/merges, wait for
    // the table to change, then send again to the new engine — signals by id are
    // idempotent, so sending again is safe.
    let (_, status) = st
        .router
        .ask_range(cairn_store::partition_key_of(&raw), |e| {
            let (signal, value) = (signal.clone(), value.clone());
            let name = match &signal {
                cairn_engine::SignalId::Idx(i) => serde_json::json!(i),
                cairn_engine::SignalId::Name(n) => serde_json::json!(n),
            };
            let note = super::audit_note(
                &who,
                meta.as_deref(),
                "invocation.signal",
                serde_json::json!({ "invocation": hex(&raw), "signal": name }),
            );
            async move { e.audited(note).external_signal(raw, signal, value).await }
        })
        .await
        .ok_or_else(range_settling)?;
    let status = status
        .map_err(|e| ApiError::from_actor_routed(e, StatusCode::INTERNAL_SERVER_ERROR, &st, &raw))?
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "no such invocation".into()))?;
    Ok(Json(SignalResponse {
        id,
        status: status_str(&status).into(),
    }))
}

#[derive(Deserialize)]
pub(super) struct RejectBody {
    #[serde(default = "reject_default_code")]
    code: u32,
    message: String,
}

pub(super) fn reject_default_code() -> u32 {
    500
}

#[derive(Serialize)]
pub(super) struct PromiseResponse {
    service: String,
    key: String,
    name: String,
    /// `false` means the promise already had a result — this time is a no-op.
    completed: bool,
    /// The number of waiting invocations that had the result queued.
    ///
    /// `0` is **valid and common**: the external system answered before the workflow
    /// reached the wait point. Promises are durable, so whoever arrives later reads it
    /// at once.
    #[serde(skip_serializing_if = "Option::is_none")]
    woken: Option<usize>,
}

/// The engine of the partition holding promise `(service, key)`.
///
/// A promise belongs to a **virtual object**, so it lives in the same partition as that
/// object — the same routing function as `ctx.call`, not a separate rule.
pub(super) fn promise_key(
    tenant: &cairn_store::TenantId,
    scope: Option<&Scope>,
    service: &str,
    key: &str,
) -> ApiResult<PartitionKey> {
    // D-a: a promise of an object with no key and no scope belongs to no range —
    // refuse, do not route to a made-up key.
    partition_key_for_target(tenant, scope, service, key, None).ok_or_else(|| {
        ApiError(
            StatusCode::BAD_REQUEST,
            format!("{service}: promise needs an object key or a scope"),
        )
    })
}

/// No engine for the promise's key — only happens with a broken table.
pub(super) fn no_promise_engine(st: &AppState, key: PartitionKey) -> ApiError {
    ApiError(
        StatusCode::INTERNAL_SERVER_ERROR,
        format!(
            "partition {} does not belong to this cluster",
            st.router.cluster.partition_of_key(key).0
        ),
    )
}

pub(super) async fn complete_promise(
    st: AppState,
    tenant: cairn_store::TenantId,
    scope: Option<Scope>,
    service: String,
    key: String,
    name: String,
    result: cairn_engine::ReplyResult,
) -> ApiResult<Json<PromiseResponse>> {
    let pk = promise_key(&tenant, scope.as_ref(), &service, &key)?;
    let (_, outcome) = st
        .router
        .ask_range(pk, |e| {
            let (tenant, scope) = (tenant.clone(), scope.clone());
            let (service, key) = (service.clone(), key.clone());
            let (name, result) = (name.clone(), result.clone());
            async move {
                e.complete_promise(tenant, scope, service, key, name, result)
                    .await
            }
        })
        .await
        .ok_or_else(|| no_promise_engine(&st, pk))?;
    let outcome = outcome
        .map_err(|e| ApiError::from_actor_keyed(e, StatusCode::INTERNAL_SERVER_ERROR, &st, pk))?;
    Ok(Json(match outcome {
        cairn_engine::PromiseOutcome::Completed { woken } => PromiseResponse {
            service,
            key,
            name,
            completed: true,
            woken: Some(woken),
        },
        cairn_engine::PromiseOutcome::AlreadyCompleted(_) => PromiseResponse {
            service,
            key,
            name,
            completed: false,
            woken: None,
        },
    }))
}

// ─────────────────────────────────────────────────── awakeable (5.4)

/// The first signal index the SDK assigns to awakeables: 1–16 are reserved for built-in
/// signals (`CANCEL` = 1, `BuiltInSignal` reserves 2–15) — the same convention as
/// Restate's service protocol.
pub const FIRST_AWAKEABLE_SIGNAL: u32 = 17;

/// `awk_` + invocation id (48 hex) + signal index (8 hex).
///
/// No new table: an awakeable **is** a signal by index to exactly the invocation that
/// created it — the signal path is already durable and idempotent. The tenant is not in
/// the id (the SDK does not know its tenant); the receiving side checks it against
/// entry 0 of the invocation ([`ensure_visible`]), so an id of another tenant — even a
/// correct guess — gets `404`.
pub(super) fn parse_awakeable(s: &str) -> Option<(InvocationId, u32)> {
    let hex = s.strip_prefix("awk_")?;
    if hex.len() != 56 {
        return None;
    }
    let id = unhex(&hex[..48])?;
    let idx = u32::from_str_radix(&hex[48..], 16).ok()?;
    (idx >= FIRST_AWAKEABLE_SIGNAL).then_some((id, idx))
}

pub(super) async fn complete_awakeable(
    st: AppState,
    who: crate::auth::Identity,
    id: String,
    value: cairn_engine::CompletionPayload,
) -> ApiResult<Json<SignalResponse>> {
    let (raw, idx) = parse_awakeable(&id).ok_or_else(|| {
        ApiError(
            StatusCode::BAD_REQUEST,
            format!("invalid awakeable id: {id:?}"),
        )
    })?;
    ensure_visible(&st, &who, &raw).await?;
    let (_, status) = st
        .router
        .ask_range(cairn_store::partition_key_of(&raw), |e| {
            let value = value.clone();
            async move {
                e.external_signal(raw, cairn_engine::SignalId::Idx(idx), value)
                    .await
            }
        })
        .await
        .ok_or_else(range_settling)?;
    let status = status
        .map_err(|e| ApiError::from_actor_routed(e, StatusCode::INTERNAL_SERVER_ERROR, &st, &raw))?
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "no such invocation".into()))?;
    Ok(Json(SignalResponse {
        id,
        status: status_str(&status).into(),
    }))
}

/// `POST /awakeables/{id}/resolve` — the body is a JSON value (empty = `null`).
pub(super) async fn resolve_awakeable(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path(id): Path<String>,
    body: Bytes,
) -> ApiResult<Json<SignalResponse>> {
    let value = if body.is_empty() {
        cairn_engine::CompletionPayload::Void
    } else {
        serde_json::from_slice::<serde_json::Value>(&body)
            .map_err(|e| ApiError(StatusCode::BAD_REQUEST, format!("body is not JSON: {e}")))?;
        cairn_engine::CompletionPayload::Value(body)
    };
    complete_awakeable(st, who, id, value).await
}

/// `POST /awakeables/{id}/reject` `{code?, message}` — the waiter gets a terminal error.
pub(super) async fn reject_awakeable(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path(id): Path<String>,
    Json(body): Json<RejectBody>,
) -> ApiResult<Json<SignalResponse>> {
    complete_awakeable(
        st,
        who,
        id,
        cairn_engine::CompletionPayload::Failure {
            code: body.code,
            message: body.message,
        },
    )
    .await
}

/// `POST /promises/{service}/{key}/{name}/resolve` — the body is the value, raw bytes.
///
/// The door for external systems (an approval form, a Slack webhook, a provider
/// callback) to wake a workflow sitting in `await ctx.promise(name).get()`. Before it
/// existed, every user had to write their own `SHARED` handler just to call
/// `resolve()` — it works, but it is exactly a piece of boilerplate everyone has to
/// rewrite, which means it belongs in the runtime.
///
/// **Calling again is a no-op, not an error.** A promise completes exactly once; the
/// second time returns `completed: false`. Webhook retries are therefore safe.
pub(super) async fn resolve_promise(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path((service, key, name)): Path<(String, String, String)>,
    body: Bytes,
) -> ApiResult<Json<PromiseResponse>> {
    complete_promise(
        st,
        who.tenant,
        None,
        service,
        key,
        name,
        cairn_engine::ReplyResult::Value(body),
    )
    .await
}

/// `POST /scope/{scope}/promises/...` — a promise of a **scoped** virtual object. The
/// same human-in-the-loop door, a different instance.
pub(super) async fn scoped_resolve_promise(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path((scope, service, key, name)): Path<(String, String, String, String)>,
    body: Bytes,
) -> ApiResult<Json<PromiseResponse>> {
    let scope = parse_scope(&scope)?;
    complete_promise(
        st,
        who.tenant,
        Some(scope),
        service,
        key,
        name,
        cairn_engine::ReplyResult::Value(body),
    )
    .await
}

/// `POST /promises/{service}/{key}/{name}/reject` — the waiter gets a terminal error.
pub(super) async fn reject_promise(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path((service, key, name)): Path<(String, String, String)>,
    Json(body): Json<RejectBody>,
) -> ApiResult<Json<PromiseResponse>> {
    complete_promise(
        st,
        who.tenant,
        None,
        service,
        key,
        name,
        cairn_engine::ReplyResult::Failure {
            code: body.code,
            message: body.message,
        },
    )
    .await
}

pub(super) async fn scoped_reject_promise(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path((scope, service, key, name)): Path<(String, String, String, String)>,
    Json(body): Json<RejectBody>,
) -> ApiResult<Json<PromiseResponse>> {
    let scope = parse_scope(&scope)?;
    complete_promise(
        st,
        who.tenant,
        Some(scope),
        service,
        key,
        name,
        cairn_engine::ReplyResult::Failure {
            code: body.code,
            message: body.message,
        },
    )
    .await
}

#[derive(Serialize)]
pub(super) struct PromiseView {
    service: String,
    key: String,
    name: String,
    completed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    failure: Option<String>,
}

/// `GET /promises/{service}/{key}/{name}` — whether this promise has a result yet.
///
/// There so the operator can answer "is the workflow stuck because nobody approved, or
/// because the approval was sent and never arrived" — two entirely different incidents.
pub(super) async fn peek_promise(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path((service, key, name)): Path<(String, String, String)>,
) -> ApiResult<Json<PromiseView>> {
    peek_promise_inner(st, who.tenant, None, service, key, name).await
}

pub(super) async fn scoped_peek_promise(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path((scope, service, key, name)): Path<(String, String, String, String)>,
) -> ApiResult<Json<PromiseView>> {
    let scope = parse_scope(&scope)?;
    peek_promise_inner(st, who.tenant, Some(scope), service, key, name).await
}

pub(super) async fn peek_promise_inner(
    st: AppState,
    tenant: cairn_store::TenantId,
    scope: Option<Scope>,
    service: String,
    key: String,
    name: String,
) -> ApiResult<Json<PromiseView>> {
    let pk = promise_key(&tenant, scope.as_ref(), &service, &key)?;
    let (_, found) = st
        .router
        .ask_range(pk, |e| {
            let (tenant, scope) = (tenant.clone(), scope.clone());
            let (service, key, name) = (service.clone(), key.clone(), name.clone());
            async move { e.peek_promise(tenant, scope, service, key, name).await }
        })
        .await
        .ok_or_else(|| no_promise_engine(&st, pk))?;
    let found = found
        .map_err(|e| ApiError::from_actor_keyed(e, StatusCode::INTERNAL_SERVER_ERROR, &st, pk))?;
    Ok(Json(match found {
        None => PromiseView {
            service,
            key,
            name,
            completed: false,
            value: None,
            failure: None,
        },
        Some(cairn_engine::ReplyResult::Value(v)) => PromiseView {
            service,
            key,
            name,
            completed: true,
            value: Some(String::from_utf8_lossy(&v).into_owned()),
            failure: None,
        },
        // Promise values are always inline: no path writes a segmented record; never
        // read the tail as the whole value.
        Some(cairn_engine::ReplyResult::Segmented(_)) => PromiseView {
            service,
            key,
            name,
            completed: true,
            value: None,
            failure: None,
        },
        Some(cairn_engine::ReplyResult::Failure { code, message }) => PromiseView {
            service,
            key,
            name,
            completed: true,
            value: None,
            failure: Some(format!("{code}: {message}")),
        },
    }))
}
