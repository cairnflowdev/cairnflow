// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Invocation introspection and control: status/journal, resume, trial replay,
//! migrate/restart, cancel/purge/list.

use super::*;

// ─────────────────────────────────────────────────────────── introspection

#[derive(Serialize)]
pub struct StatusResponse {
    pub id: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    /// Code and message of `failed`/`paused` — why it stopped (e.g. `577` INFRA_STUCK).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// The error of the latest failed attempt — when the invocation is not finished or
    /// is `paused`, and read from the leader.
    #[serde(rename = "lastFailure", skip_serializing_if = "Option::is_none")]
    pub last_failure: Option<LastFailureView>,
    /// Timestamps: the `create_ts` of entry 0, of the status row, and — once finished —
    /// that mark itself. Leader and follower read the same marks.
    #[serde(rename = "createdAt", skip_serializing_if = "Option::is_none")]
    pub created_at: Option<u64>,
    #[serde(rename = "modifiedAt", skip_serializing_if = "Option::is_none")]
    pub modified_at: Option<u64>,
    #[serde(rename = "endedAt", skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<u64>,
    /// User tags — from the leader only.
    #[serde(skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub tags: std::collections::BTreeMap<String, String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LastFailureView {
    pub code: u32,
    pub message: String,
    pub attempt: u32,
    pub at_ms: u64,
    /// When the next attempt is due — only while backing off (`running` with a scheduled
    /// retry); absent while an attempt runs, and when `suspended`/`paused`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_attempt_at_ms: Option<u64>,
}

/// Reads the status (with timestamps) through the follower path, if the node has one configured.
pub(super) async fn follower_status(
    st: &AppState,
    id: &InvocationId,
) -> Option<(cairn_engine::InvocationStatus, cairn_engine::Times)> {
    st.followers
        .as_ref()?
        .status_times(st.router.cluster.partition_of_id(id), id)
        .await
}

/// Reads the journal through the follower path, if the node has one configured.
pub(super) async fn follower_journal(
    st: &AppState,
    id: &InvocationId,
) -> Option<Vec<cairn_engine::JournalRow>> {
    st.followers
        .as_ref()?
        .journal(st.router.cluster.partition_of_id(id), id)
        .await
}

/// Like [`follower_journal`], one page.
pub(super) async fn follower_journal_page(
    st: &AppState,
    id: &InvocationId,
    from: u32,
    limit: usize,
) -> Option<(Vec<cairn_engine::JournalRow>, Option<u32>)> {
    st.followers
        .as_ref()?
        .journal_page(
            st.router.cluster.partition_of_id(id),
            id,
            from,
            limit,
            super::archive::JOURNAL_PAGE_MAX_BYTES,
        )
        .await
}

/// Like [`follower_journal`], for callers that only read the first entry (tenant): does
/// not defer to the leader for an unexposed tail — the first entry does not change after
/// the creating commit.
pub(super) async fn follower_journal_head(
    st: &AppState,
    id: &InvocationId,
) -> Option<Vec<cairn_engine::JournalRow>> {
    st.followers
        .as_ref()?
        .journal_head(st.router.cluster.partition_of_id(id), id)
        .await
}

/// Header of `GET /invocations/{id}`: where the status was read from — `leader` (the
/// engine holding the lease, the newest) or `follower` (a reader on a node not leading:
/// durable status only, possibly **older** than an earlier leader read). The chaos
/// history checker relies on it to tell "stale read" apart from "a durable result
/// vanished".
pub const READ_SOURCE_HEADER: &str = "x-cairn-read";

pub(super) async fn status(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path(id): Path<String>,
) -> ApiResult<([(&'static str, &'static str); 1], Json<StatusResponse>)> {
    let raw = unhex(&id).ok_or_else(|| {
        ApiError(
            StatusCode::BAD_REQUEST,
            "id is not 48 hex characters".into(),
        )
    })?;
    // Target (tenant), status and latest attempt error in **one** turn on the leader —
    // one answer, one read fence (before: three commands, three fence GETs).
    // `cluster-admin` sees every tenant, so it needs no target.
    let check_tenant = !who.all_tenants();
    let (_, view) = st
        .router
        .ask_range(cairn_store::partition_key_of(&raw), |e| async move {
            e.view(raw, check_tenant).await
        })
        .await
        .ok_or_else(range_settling)?;
    let not_found = || ApiError(StatusCode::NOT_FOUND, "no such invocation".into());
    let (s, source, last_failure, next_attempt_ms, times, tags) = match view {
        Ok(v) => {
            if check_tenant && v.target.as_ref().map(|t| &t.tenant) != Some(&who.tenant) {
                return Err(not_found());
            }
            (
                v.status.ok_or_else(not_found)?,
                "leader",
                v.last_failure,
                v.next_attempt_ms,
                v.times,
                v.tags,
            )
        }
        // Not the leader ⇒ try the follower read path. It only serves **positive**
        // results: the reader is slightly stale, so its "not found" is not a 404 — in
        // that case fall back to 503 with the leader address as before. The tenant is
        // also read from the follower (`ensure_visible` falls back to the journal there).
        Err(e) if e.is_fenced() => {
            ensure_visible(&st, &who, &raw).await?;
            match follower_status(&st, &raw).await {
                Some((s, times)) => (s, "follower", None, None, times, Vec::new()),
                None => {
                    return Err(ApiError::from_actor_routed(
                        e,
                        StatusCode::INTERNAL_SERVER_ERROR,
                        &st,
                        &raw,
                    ))
                }
            }
        }
        Err(e) => {
            return Err(ApiError::from_actor_routed(
                e,
                StatusCode::INTERNAL_SERVER_ERROR,
                &st,
                &raw,
            ))
        }
    };

    let (status, output) = match &s {
        InvocationStatus::Completed(v) => (
            "completed",
            Some(super::invoke::output_text(&st, v.clone()).await?),
        ),
        other => (status_str(other), None),
    };
    let (code, message) = match &s {
        InvocationStatus::Failed { code, message } | InvocationStatus::Paused { code, message } => {
            (Some(*code), Some(message.clone()))
        }
        _ => (None, None),
    };
    // Why the latest attempt failed — useful while still running (waiting to retry) and
    // when `paused` (out of attempts). Leader only (read in the same turn in
    // `InvocationView`): the follower path returns the status without it. While backing off
    // it also says when the scheduled retry is due.
    let last_failure = last_failure.map(|f| LastFailureView {
        code: f.code,
        message: f.message,
        attempt: f.attempt,
        at_ms: f.at_ms,
        next_attempt_at_ms: next_attempt_ms,
    });
    Ok((
        [(READ_SOURCE_HEADER, source)],
        Json(StatusResponse {
            id,
            status: status.into(),
            output,
            code,
            message,
            last_failure,
            created_at: times.created_ms,
            modified_at: times.modified_ms,
            ended_at: times.ended_ms(&s),
            tags: tags.into_iter().collect(),
        }),
    ))
}

/// One mapping table, one place: the `?status=` filter runs inside the engine, so the
/// status names must be those of [`InvocationStatus`], not of the HTTP layer.
pub(super) fn status_str(s: &InvocationStatus) -> &'static str {
    s.name()
}

// ─────────────────────────────────────────────────────────── admin: cancel + list

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelResponse {
    pub id: String,
    /// `false` = already stopped before, this command changed nothing.
    pub cancelled: bool,
    /// The status *after* the command.
    pub status: String,
}

#[derive(Serialize)]
pub(super) struct ResumeResponse {
    id: String,
    resumed: bool,
    status: String,
}

/// `POST /invocations/{id}/resume` — takes an invocation out of `paused`.
///
/// `FailureAction::Pause` describes itself as "waiting for manual intervention", but
/// before this endpoint **there was no way to intervene**: once in `paused`, only
/// `cancel` or `purge` remained — the promise "keep it to inspect, then continue" only
/// kept its first half.
///
/// Continues right in the request, not left to the background tick: the operator has
/// just fixed what made it hang and is watching the screen.
///
/// `JOURNAL_MISMATCH` **always** leads here: deploy the fix, then call this endpoint to
/// continue. Calling it before the fix makes the rerun reproduce exactly the old error
/// and the invocation goes `paused` again — the returned `status` says exactly that.
pub(super) async fn resume(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    meta: super::Meta,
    Path(id): Path<String>,
) -> ApiResult<Json<ResumeResponse>> {
    use cairn_engine::ResumeOutcome;
    let raw = unhex(&id).ok_or_else(|| {
        ApiError(
            StatusCode::BAD_REQUEST,
            "id is not 48 hex characters".into(),
        )
    })?;
    ensure_visible(&st, &who, &raw).await?;
    let (engine, outcome) = st
        .router
        .ask_range(cairn_store::partition_key_of(&raw), |e| {
            let note = super::audit_note(
                &who,
                meta.as_deref(),
                "invocation.resume",
                json_target(&raw),
            );
            async move { e.audited(note).resume(raw).await }
        })
        .await
        .ok_or_else(range_settling)?;
    let outcome = outcome.map_err(|e| {
        ApiError::from_actor_routed(e, StatusCode::INTERNAL_SERVER_ERROR, &st, &raw)
    })?;
    match outcome {
        ResumeOutcome::NotFound => {
            Err(ApiError(StatusCode::NOT_FOUND, "no such invocation".into()))
        }
        ResumeOutcome::NotPaused(s) => Err(ApiError(
            StatusCode::CONFLICT,
            format!("invocation is {}, not paused", status_str(&s)),
        )),
        ResumeOutcome::Resumed => {
            // The status **after** continuing, not a token "running": it may already be
            // finished, suspended again, or paused again on the same old error.
            let after = engine
                .status(raw)
                .await
                .ok()
                .flatten()
                .map(|s| status_str(&s).to_string())
                .unwrap_or_else(|| "unknown".into());
            Ok(Json(ResumeResponse {
                id,
                resumed: true,
                status: after,
            }))
        }
    }
}

// ─────────────────────────────────────────────── trial replay

/// An invocation's journal — from the leader, or the follower when it has the data.
pub(super) async fn read_journal(
    st: &AppState,
    raw: InvocationId,
) -> ApiResult<Vec<cairn_engine::JournalRow>> {
    let (_, entries) = st
        .router
        .ask_range(cairn_store::partition_key_of(&raw), |e| async move {
            e.journal(raw).await
        })
        .await
        .ok_or_else(range_settling)?;
    match entries {
        Ok(rows) => Ok(rows),
        Err(e) if e.is_fenced() => match follower_journal(st, &raw).await {
            Some(rows) => Ok(rows),
            None => Err(ApiError::from_actor_routed(
                e,
                StatusCode::INTERNAL_SERVER_ERROR,
                st,
                &raw,
            )),
        },
        Err(e) => Err(ApiError::from_actor_routed(
            e,
            StatusCode::INTERNAL_SERVER_ERROR,
            st,
            &raw,
        )),
    }
}

pub(super) fn replay_error(e: crate::replay::ReplayError) -> ApiError {
    use crate::replay::ReplayError as E;
    match e {
        E::WireTooOld(..) | E::Segmented => ApiError(StatusCode::CONFLICT, e.to_string()),
        E::BadJournal(_) => ApiError(StatusCode::UNPROCESSABLE_ENTITY, e.to_string()),
        E::Sdk(_) | E::Unexpected(_) => ApiError(StatusCode::BAD_GATEWAY, e.to_string()),
        E::EgressDenied(d) => super::egress_denied(d),
    }
}

/// Replays an invocation on `deployment` (default: the deployment it is attached to).
pub(super) async fn replay_one(
    st: &AppState,
    raw: InvocationId,
    deployment: Option<&str>,
) -> ApiResult<crate::replay::ReplayReport> {
    let rows = read_journal(st, raw).await?;
    if rows.is_empty() {
        return Err(ApiError(StatusCode::NOT_FOUND, "no such invocation".into()));
    }
    let pinned = rows
        .first()
        .map(|(_, ty, p)| {
            cairn_runtime::JournalEntry::new(
                cairn_protocol::MessageType::from(*ty),
                Bytes::copy_from_slice(p),
            )
        })
        .and_then(|e| cairn_engine::input_head(&e))
        .and_then(|(_, d)| d);
    let want = deployment.map(str::to_string).or(pinned).ok_or_else(|| {
        ApiError(
            StatusCode::BAD_REQUEST,
            "invocation is not attached to a deployment — specify ?deployment=".into(),
        )
    })?;
    let catalog = st.registry.snapshot();
    let d = catalog
        .get(&want)
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, format!("no deployment {want:?}")))?;
    crate::replay::replay(
        &st.tenant_http,
        Some(&st.identity),
        &st.value_urls,
        d,
        raw,
        rows,
        crate::turn::params_with_retry(&raw, 0),
    )
    .await
    .map_err(replay_error)
}

/// `POST /invocations/{id}/replay?deployment=<id>` — a **read-only** trial replay:
/// commits nothing, delivers nothing, runs no closure of the new code.
/// Returns `{matches, upto, mismatchAt?, expected?, got?, message?}`.
pub(super) async fn replay_invocation(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path(id): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<crate::replay::ReplayReport>> {
    let raw = unhex(&id).ok_or_else(|| {
        ApiError(
            StatusCode::BAD_REQUEST,
            "id is not 48 hex characters".into(),
        )
    })?;
    ensure_visible(&st, &who, &raw).await?;
    if let Some(d) = q.get("deployment") {
        let tenant = invocation_tenant_or(&st, &who, &raw).await?;
        deployment_of_tenant(&st.registry.snapshot(), d, &tenant)?;
    }
    Ok(Json(
        replay_one(&st, raw, q.get("deployment").map(String::as_str)).await?,
    ))
}

/// `POST /invocations/{id}/migrate?deployment=<id>[&force=true]` — moves an
/// **unfinished** invocation to another deployment, deliberately.
///
/// An invocation is attached to its deployment for life; this is the only path that
/// changes that. By default a trial replay on the target must **match** first —
/// a mismatch is `409` with the report, no move. `force=true` skips the trial
/// (a deployment that does not speak v3 cannot do a trial replay).
pub(super) async fn migrate_invocation(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    meta: super::Meta,
    Path(id): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<serde_json::Value>> {
    let raw = unhex(&id).ok_or_else(|| {
        ApiError(
            StatusCode::BAD_REQUEST,
            "id is not 48 hex characters".into(),
        )
    })?;
    ensure_visible(&st, &who, &raw).await?;
    let to = q
        .get("deployment")
        .cloned()
        .ok_or_else(|| ApiError(StatusCode::BAD_REQUEST, "missing ?deployment=".into()))?;
    // The target deployment must belong to **the same tenant** as the invocation: moving
    // to another tenant's code would run this tenant's journal on someone else's code.
    let tenant = invocation_tenant_or(&st, &who, &raw).await?;
    deployment_of_tenant(&st.registry.snapshot(), &to, &tenant)?;
    super::deployments::ensure_not_retiring(&st, &tenant, &to).await?;
    let force = q.get("force").is_some_and(|v| v == "true" || v == "1");
    let report = if force {
        None
    } else {
        let r = replay_one(&st, raw, Some(&to)).await?;
        if !r.matches {
            return Err(ApiError(
                StatusCode::CONFLICT,
                format!(
                    "trial replay on {to} diverged — not moving: {}",
                    serde_json::to_string(&r).unwrap_or_default()
                ),
            ));
        }
        Some(r)
    };
    let (_, moved) = st
        .router
        .ask_range(cairn_store::partition_key_of(&raw), |e| {
            let to = to.clone();
            let note = super::audit_note(
                &who,
                meta.as_deref(),
                "invocation.migrate",
                serde_json::json!({ "invocation": hex(&raw), "deployment": to }),
            );
            async move { e.audited(note).migrate(raw, to).await }
        })
        .await
        .ok_or_else(range_settling)?;
    let moved = moved.map_err(|e| {
        ApiError::from_actor_routed(e, StatusCode::INTERNAL_SERVER_ERROR, &st, &raw)
    })?;
    if !moved {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "no such invocation, or it has finished".into(),
        ));
    }
    Ok(Json(
        serde_json::json!({ "id": id, "migrated": true, "replay": report }),
    ))
}

/// `POST /invocations/{id}/restart?from=<n>&deployment=<id>` — `restart-as-new`: a
/// **new** invocation whose journal is the first `n` entries of the old one (default 1:
/// only `Input`, rerun from the start), on `deployment` (default: the one the old
/// invocation is attached to). `ctx.run` results in the prefix are reused, not rerun. An
/// unfinished old invocation finishes with the pointer `{"continuedAsNew": "<new id>"}`,
/// and its waiters move to the new one.
///
/// Optional body `{"patch": {"entry": n, "input": …}}` — an input patch for entry `n`
/// ("fix, then rerun from a step"). **Before the feature gate** a present patch ⇒ `400`
/// (`turn::admit::Source::Restart`). Other bodies (empty, not JSON, no `patch`) are
/// ignored as before.
pub(super) async fn restart_invocation(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    meta: super::Meta,
    Path(id): Path<String>,
    Query(q): Query<HashMap<String, String>>,
    body: Bytes,
) -> ApiResult<Json<serde_json::Value>> {
    use cairn_engine::RestartOutcome as R;
    let raw = unhex(&id).ok_or_else(|| {
        ApiError(
            StatusCode::BAD_REQUEST,
            "id is not 48 hex characters".into(),
        )
    })?;
    ensure_visible(&st, &who, &raw).await?;
    let keep: u32 = match q.get("from") {
        Some(v) => v
            .parse()
            .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "from is not a number".into()))?,
        None => 1,
    };
    let deployment = q.get("deployment").cloned();
    if let Some(d) = &deployment {
        let tenant = invocation_tenant_or(&st, &who, &raw).await?;
        deployment_of_tenant(&st.registry.snapshot(), d, &tenant)?;
        super::deployments::ensure_not_retiring(&st, &tenant, d).await?;
    }
    let patch = restart_patch(&body)?;
    let (_, outcome) = st
        .router
        .ask_range(cairn_store::partition_key_of(&raw), |e| {
            let (deployment, patch) = (deployment.clone(), patch.clone());
            let note = super::audit_note(
                &who,
                meta.as_deref(),
                "invocation.restart",
                serde_json::json!({ "invocation": hex(&raw), "from": keep, "deployment": deployment }),
            );
            async move {
                e.audited(note)
                    .restart_patched(raw, keep, deployment, patch)
                    .await
            }
        })
        .await
        .ok_or_else(range_settling)?;
    match outcome
        .map_err(|e| ApiError::from_actor_routed(e, StatusCode::INTERNAL_SERVER_ERROR, &st, &raw))?
    {
        R::Restarted { new } => Ok(Json(serde_json::json!({ "id": id, "new": hex(&new) }))),
        R::NotFound => Err(ApiError(StatusCode::NOT_FOUND, "no such invocation".into())),
        R::BadPrefix { len } => Err(ApiError(
            StatusCode::BAD_REQUEST,
            format!("from must be in 1..={len} (entry 0 is Input, it must be kept)"),
        )),
        R::WorkflowRun => Err(ApiError(
            StatusCode::CONFLICT,
            "a workflow run cannot be restarted — its id is a function of (scope, service, key)"
                .into(),
        )),
    }
}

/// The patch of a `restart` body, if any. A string `input` ⇒ the string's bytes; another
/// JSON value ⇒ its JSON. A malformed `patch` ⇒ `400`.
fn restart_patch(body: &[u8]) -> ApiResult<Option<crate::turn::admit::Patch>> {
    let Ok(serde_json::Value::Object(o)) = serde_json::from_slice::<serde_json::Value>(body) else {
        return Ok(None);
    };
    let Some(p) = o.get("patch") else {
        return Ok(None);
    };
    let bad = |m: &str| ApiError(StatusCode::BAD_REQUEST, format!("patch: {m}"));
    let entry = match p.get("entry") {
        None => 0,
        Some(v) => v
            .as_u64()
            .and_then(|n| u32::try_from(n).ok())
            .ok_or_else(|| bad("entry must be a non-negative integer"))?,
    };
    let input = match p.get("input") {
        Some(serde_json::Value::String(s)) => Bytes::from(s.clone()),
        Some(v) => Bytes::from(v.to_string()),
        None => return Err(bad("missing input")),
    };
    Ok(Some(crate::turn::admit::Patch { entry, input }))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplayCheck {
    pub deployment: String,
    pub service: String,
    /// The number of invocations replayed.
    pub checked: usize,
    pub matched: usize,
    /// Invocations that mismatched, or replays that failed (with the reason).
    pub failures: Vec<serde_json::Value>,
    /// Ranges replayed: this node's, plus every other node's with `scope=cluster`.
    pub ranges: Vec<u32>,
    /// `scope=cluster`: nodes that did not answer — `partial` when there are any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub missing: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partial: Option<bool>,
}

/// `POST /deployments/{id}/replay-check?service=&sample=[&scope=cluster]` — trial-replays
/// a sample of **unfinished** invocations of `service` on deployment `id`. By default
/// only the ranges this node leads; `scope=cluster` runs the same request on every other
/// node ([`super::forward::to_peers`]) and adds up the reports — `sample` is a cap **per
/// node**.
///
/// A primitive, not a policy: the report comes back, and blocking or allowing a revision
/// move is up to the caller.
#[allow(clippy::too_many_arguments)]
pub(super) async fn replay_check(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path(deployment): Path<String>,
    Query(q): Query<HashMap<String, String>>,
    uri: axum::http::Uri,
    headers: axum::http::HeaderMap,
    forwarded: Option<axum::Extension<super::forward::Forwarded>>,
) -> ApiResult<Json<ReplayCheck>> {
    let cluster = observe::parse_scope(&q)? && forwarded.is_none();
    let tenant = acting_tenant(&who, &q)?;
    let service = q
        .get("service")
        .cloned()
        .ok_or_else(|| ApiError(StatusCode::BAD_REQUEST, "missing ?service=".into()))?;
    let sample: usize = match q.get("sample") {
        Some(v) => v
            .parse()
            .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "sample is not a number".into()))?,
        None => 20,
    }
    .min(LIST_LIMIT_MAX);
    deployment_of_tenant(&st.registry.snapshot(), &deployment, &tenant)?;
    let mut ids = Vec::new();
    let mut ranges = Vec::new();
    // The service is filtered inside the engine, bounded (security review): no scanning
    // the whole range with two GETs per row before cutting to `sample`.
    let filter = cairn_engine::ListFilter {
        service: Some(service.clone()),
        tenant: Some(tenant.clone()),
        ..Default::default()
    };
    for (p, engine) in st.router.partitions.iter() {
        let rows = match engine
            .list_invocations_where(LIST_LIMIT_MAX, filter.clone())
            .await
            .map(|answer| {
                answer
                    .rows
                    .into_iter()
                    .map(|r| (r.id, r.status, r.target))
                    .collect::<Vec<_>>()
            }) {
            Ok(rows) => rows,
            Err(e) if e.is_fenced() => continue,
            Err(crate::actor::ActorError::Stopped) => continue,
            Err(e) => return Err(ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
        };
        ranges.push(p);
        for (id, s, target) in rows {
            if !s.is_terminal()
                && target
                    .as_ref()
                    .is_some_and(|t| t.service == service && t.tenant == tenant)
            {
                ids.push(id);
            }
        }
    }
    ids.truncate(sample);
    let mut out = ReplayCheck {
        deployment: deployment.clone(),
        service,
        checked: 0,
        matched: 0,
        failures: Vec::new(),
        ranges,
        missing: None,
        partial: None,
    };
    for id in ids {
        out.checked += 1;
        match replay_one(&st, id, Some(&deployment)).await {
            Ok(r) if r.matches => out.matched += 1,
            Ok(r) => out
                .failures
                .push(serde_json::to_value(&r).unwrap_or_default()),
            Err(e) => out.failures.push(serde_json::json!({
                "id": hex(&id),
                "error": e.public_message(),
                "status": e.status.as_u16(),
            })),
        }
    }
    if cluster {
        let (parts, missing) =
            super::forward::to_peers(&st, &axum::http::Method::POST, &uri, &headers, Bytes::new())
                .await;
        for (_, v) in parts {
            out.checked += v["checked"].as_u64().unwrap_or(0) as usize;
            out.matched += v["matched"].as_u64().unwrap_or(0) as usize;
            if let Some(f) = v["failures"].as_array() {
                out.failures.extend(f.iter().cloned());
            }
            if let Some(r) = v["ranges"].as_array() {
                out.ranges
                    .extend(r.iter().filter_map(|x| x.as_u64()).map(|x| x as u32));
            }
        }
        out.ranges.sort_unstable();
        out.ranges.dedup();
        out.partial = Some(!missing.is_empty());
        out.missing = Some(missing);
    }
    Ok(Json(out))
}

pub(super) async fn cancel(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    meta: super::Meta,
    Path(id): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<CancelResponse>> {
    use cairn_engine::CancelOutcome;
    let raw = unhex(&id).ok_or_else(|| {
        ApiError(
            StatusCode::BAD_REQUEST,
            "id is not 48 hex characters".into(),
        )
    })?;
    ensure_visible(&st, &who, &raw).await?;
    let pk = cairn_store::partition_key_of(&raw);

    // `?mode=signal`: COOPERATIVE cancellation — delivers the CANCEL signal, the SDK
    // cleans up and finishes with its own error. Needs a live SDK; with a dead SDK the
    // invocation keeps running and the hard level (default, 572) is the remaining path —
    // which is why the hard level stays the default of an admin API.
    if q.get("mode").map(String::as_str) == Some("signal") {
        let (_, status) = st
            .router
            .ask_range(pk, |e| {
                let note = super::audit_note(
                    &who,
                    meta.as_deref(),
                    "invocation.cancel_signal",
                    json_target(&raw),
                );
                async move { e.audited(note).cancel_signal(raw).await }
            })
            .await
            .ok_or_else(range_settling)?;
        let status = status.map_err(|e| {
            ApiError::from_actor_routed(e, StatusCode::INTERNAL_SERVER_ERROR, &st, &raw)
        })?;
        let Some(status) = status else {
            return Err(ApiError(StatusCode::NOT_FOUND, "no such invocation".into()));
        };
        crate::metrics::Metrics::bump(&st.metrics.cancelled);
        return Ok(Json(CancelResponse {
            id,
            cancelled: status.is_terminal(),
            status: status_str(&status).into(),
        }));
    }

    let (_, outcome) = st
        .router
        .ask_range(pk, |e| {
            let note = super::audit_note(
                &who,
                meta.as_deref(),
                "invocation.cancel",
                json_target(&raw),
            );
            async move { e.audited(note).cancel(raw).await }
        })
        .await
        .ok_or_else(range_settling)?;
    let outcome = outcome.map_err(|e| {
        ApiError::from_actor_routed(e, StatusCode::INTERNAL_SERVER_ERROR, &st, &raw)
    })?;
    match outcome {
        CancelOutcome::NotFound => {
            Err(ApiError(StatusCode::NOT_FOUND, "no such invocation".into()))
        }
        CancelOutcome::AlreadyFinished(s) => Ok(Json(CancelResponse {
            id,
            cancelled: false,
            status: status_str(&s).into(),
        })),
        CancelOutcome::Cancelled => {
            crate::metrics::Metrics::bump(&st.metrics.cancelled);
            Ok(Json(CancelResponse {
                id,
                cancelled: true,
                status: "failed".into(),
            }))
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PurgeResponse {
    pub id: String,
    pub purged: bool,
}

/// Purges a finished invocation at once — its journal, status and idempotency key. The
/// automatic path is retention (`--retention`); this path spares the operator the wait.
pub(super) async fn purge(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    meta: super::Meta,
    Path(id): Path<String>,
) -> ApiResult<Json<PurgeResponse>> {
    use cairn_engine::PurgeOutcome;
    let raw = unhex(&id).ok_or_else(|| {
        ApiError(
            StatusCode::BAD_REQUEST,
            "id is not 48 hex characters".into(),
        )
    })?;
    ensure_visible(&st, &who, &raw).await?;
    let (_, outcome) = st
        .router
        .ask_range(cairn_store::partition_key_of(&raw), |e| {
            let note =
                super::audit_note(&who, meta.as_deref(), "invocation.purge", json_target(&raw));
            async move { e.audited(note).purge(raw).await }
        })
        .await
        .ok_or_else(range_settling)?;
    let outcome = outcome.map_err(|e| {
        ApiError::from_actor_routed(e, StatusCode::INTERNAL_SERVER_ERROR, &st, &raw)
    })?;
    match outcome {
        PurgeOutcome::NotFound => Err(ApiError(StatusCode::NOT_FOUND, "no such invocation".into())),
        PurgeOutcome::StillRunning => Err(ApiError(
            StatusCode::CONFLICT,
            "invocation has not fully stopped — cancel it before purging".into(),
        )),
        PurgeOutcome::Purged => Ok(Json(PurgeResponse { id, purged: true })),
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InvocationView {
    pub id: String,
    pub partition: u32,
    pub status: String,
    /// The routing target, read from entry 0. Absent when the journal could not be read yet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handler: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// The deployment the invocation is attached to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployment: Option<String>,
    /// Consecutive failed attempts of the pending retry; 0 = none.
    #[serde(default)]
    pub attempt: u32,
    /// The node that returned this row — only with `?scope=cluster`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    /// Timestamps — like [`StatusResponse`]. Absent on rows from older nodes (the fan-out
    /// reads leniently).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<u64>,
}

/// Lists the invocations of **the partitions this node leads** — an array.
///
/// `?scope=cluster`: adds every other node's part through the internal port, each row
/// carrying `node`; the response is an object
/// `{invocations, partial, missing, missingRanges, nodes}` — `partial` when a node did
/// not answer or a range had no one covering it. Without scope: local scope, telling the
/// truth about its scope.
///
/// `?status=` filters by status, `?limit=` bounds the cost (default 100 per partition —
/// each row is a GET of entry 0, plus two GETs for the deployment and the attempt).
/// The cap on `?limit=` of `GET /invocations` per range (security review): each row is a
/// GET of entry 0 plus two more GETs, and `?scope=cluster` multiplies that over every
/// node. Larger values are clamped, not an error — the cluster-wide response states the
/// real `limit`.
pub const LIST_LIMIT_MAX: usize = 1000;

/// The (clamped) `limit` and filter of `GET /invocations` — shared by the public door and
/// `/internal/invocations` (the node receiving the fan-out clamps again).
pub(super) fn list_query(
    q: &HashMap<String, String>,
) -> Result<(usize, cairn_engine::ListFilter), ApiError> {
    let bad = |m: String| ApiError(StatusCode::BAD_REQUEST, m);
    let limit: usize = match q.get("limit") {
        Some(v) => v.parse().map_err(|_| bad("limit is not a number".into()))?,
        None => 100,
    };
    let status = q.get("status").cloned();
    // A mistyped status ⇒ `400`, **not** an empty list. Empty looks exactly like "nothing
    // is broken", and whoever reads it is usually looking for what is broken.
    if let Some(f) = status.as_deref() {
        if !InvocationStatus::NAMES.contains(&f) {
            return Err(bad(format!(
                "status {f:?} does not exist; choose one of {}",
                InvocationStatus::NAMES.join(" · ")
            )));
        }
    }
    // The OTLP `trace_id` is the first 16 bytes of the id (`otel.rs`): filter by prefix.
    let id_prefix = match q.get("trace_id") {
        Some(t) => {
            let ok = t.len() == 32 && t.bytes().all(|b| b.is_ascii_hexdigit());
            if !ok {
                return Err(bad("trace_id must be 32 hex characters".into()));
            }
            Some(
                (0..16)
                    .map(|i| u8::from_str_radix(&t[2 * i..2 * i + 2], 16).unwrap_or(0))
                    .collect(),
            )
        }
        None => None,
    };
    // Timestamps: Unix ms, a non-negative integer; wrong ⇒ `400`.
    let ms = |name: &str| -> Result<Option<u64>, ApiError> {
        q.get(name)
            .map(|v| {
                v.parse::<u64>().map_err(|_| {
                    bad(format!(
                        "{name} must be a ms timestamp (non-negative integer)"
                    ))
                })
            })
            .transpose()
    };
    let (created_after, created_before) = (ms("created_after")?, ms("created_before")?);
    let (ended_after, ended_before) = (ms("ended_after")?, ms("ended_before")?);
    let by_time = [created_after, created_before, ended_after, ended_before]
        .iter()
        .any(Option::is_some);
    let created_desc = match q.get("order").map(String::as_str) {
        Some("created_desc") => true,
        None => by_time,
        Some(o) => {
            return Err(bad(format!(
                "order {o:?} does not exist; choose created_desc"
            )))
        }
    };
    Ok((
        limit.min(LIST_LIMIT_MAX),
        cairn_engine::ListFilter {
            status,
            id_prefix,
            service: q.get("service").cloned(),
            handler: q.get("handler").cloned(),
            created_after,
            created_before,
            ended_after,
            ended_before,
            created_desc,
            scan_max: None,
            tenant: None,
        },
    ))
}

/// Sorts rows by `createdAt` descending (rows without a mark last, keeping their old
/// order) — merging the parts of several ranges or nodes when `order=created_desc`.
pub(super) fn sort_created_desc(rows: &mut [InvocationView]) {
    rows.sort_by_key(|r| std::cmp::Reverse(r.created_at.unwrap_or(0)));
}

pub(super) async fn list_invocations(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    headers: axum::http::HeaderMap,
    ext: axum::http::Extensions,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Response> {
    // With `cursor` (even empty): the list on the query layer, paged by key. Without: the
    // old list, old clients unchanged.
    if q.contains_key("cursor") {
        let tenant = acting_tenant(&who, &q)?;
        let auth = crate::query::views::Credentials::of_request(&headers, &st.auth, &ext);
        return Ok(crate::query::list::page(&st, tenant.as_str().to_string(), auth, &q).await);
    }
    // Only invocations of the acting tenant. `limit` applies inside the engine,
    // **before** this filter — a small tenant on a busy range may get fewer than `limit`
    // rows while more exist; raise `limit` to see more. Every other filter (status,
    // service, handler, `trace_id`) applies inside the engine, before `limit`.
    let tenant = acting_tenant(&who, &q)?;
    let (limit, filter) = list_query(&q)?;
    if q.get("scope").map(String::as_str) == Some("cluster") {
        return Ok(observe::cluster_invocations(&st, &tenant, limit, &filter)
            .await?
            .into_response());
    }
    if let Some(other) = q.get("scope").filter(|s| s.as_str() != "local") {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            format!("scope {other:?} does not exist; choose local or cluster"),
        ));
    }
    let out = local_invocations(&st, &tenant, limit, &filter).await?;
    let mut resp = Json(serde_json::to_value(out.invocations).unwrap_or_default()).into_response();
    // The local array has no room for the two flags of the bounded scan: headers.
    let h = resp.headers_mut();
    if filter.by_created() || filter.by_ended() {
        h.insert("x-cairn-untimed", out.untimed.into());
    }
    // A range not answering within the cap is "missing" too — even when no filter is
    // bounded.
    if filter.capped() || out.capped {
        let v = if out.capped { "true" } else { "false" };
        h.insert("x-cairn-partial", axum::http::HeaderValue::from_static(v));
    }
    Ok(resp)
}

/// The number of ranges [`local_invocations`] asks at once.
const LIST_RANGES_IN_FLIGHT: usize = 32;

/// Cap on waiting for one range to answer the list: below the 5 s cap of the local part
/// in the cluster-wide version (`observe::FAN_OUT_TIMEOUT`), so one stuck range only
/// leaves that range missing, not the whole node.
const RANGE_LIST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(4);

/// The invocations of `tenant` on the ranges this node leads, with the list of ranges
/// that answered — one node's part of the cluster-wide version.
pub(super) async fn local_invocations(
    st: &AppState,
    tenant: &cairn_store::TenantId,
    limit: usize,
    filter: &cairn_engine::ListFilter,
) -> ApiResult<observe::NodeInvocations> {
    let mut out = observe::NodeInvocations {
        ranges: Vec::new(),
        invocations: Vec::new(),
        untimed: 0,
        capped: false,
    };
    // Ask the ranges **in parallel, bounded** ([`LIST_RANGES_IN_FLIGHT`]), keeping range
    // order. Asked one by one, the node's part costs the sum of every range's time (each
    // range a read fence and a scan): in a soak run a node holding ~193 ranges answered
    // past the 5 s fan-out cap and was always in `missing`. Each engine answers on its own
    // thread, and concurrent read fences fold into one GET.
    use futures::StreamExt as _;
    // The tenant is filtered inside the engine, before `limit` — like every other filter.
    let filter = &cairn_engine::ListFilter {
        tenant: Some(tenant.clone()),
        ..filter.clone()
    };
    let mut answers =
        futures::stream::iter(st.router.partitions.iter().map(|(p, engine)| async move {
            // `limit` applies **after** the filter, inside the engine — see
            // `Engine::list_invocations`. A cap per range (every I/O await is bounded): the
            // scan is bounded at 2 s **inside** the engine, but waiting for the engine's
            // turn is not — one range with a stuck engine (a long background tick, an L0
            // flush) would hold the whole request.
            (
                p,
                cairn_store::retry::deadline(
                    &cairn_store::retry::ops::INGRESS_LIST_LOCAL,
                    RANGE_LIST_TIMEOUT,
                    engine.list_invocations_where(limit, filter.clone()),
                )
                .await,
            )
        }))
        .buffered(LIST_RANGES_IN_FLIGHT);
    while let Some((p, answer)) = answers.next().await {
        // Cap reached: the range did not answer — drop it from `ranges` (the cluster-wide
        // version puts it in `missingRanges`) and report the list as partial
        // (`x-cairn-partial`).
        let Ok(answer) = answer else {
            out.capped = true;
            continue;
        };
        let answer = match answer {
            Ok(a) => a,
            // This partition is led by another node — out of this node's scope.
            Err(e) if e.is_fenced() => continue,
            // The range's engine has just exited (retired after a split/merge, a dead job
            // waiting to be rebuilt) while the handle is still in the set: right now the
            // node does not serve that range, like the arm above — not an error of the
            // whole list.
            Err(crate::actor::ActorError::Stopped) => continue,
            Err(e) => return Err(ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
        };
        out.ranges.push(p);
        out.untimed += answer.untimed;
        out.capped |= answer.capped;
        for r in answer.rows {
            let target = r.target;
            if target.as_ref().is_none_or(|t| t.tenant != *tenant) {
                continue;
            }
            out.invocations.push(InvocationView {
                id: hex(&r.id),
                partition: p,
                status: status_str(&r.status).into(),
                service: target.as_ref().map(|t| t.service.clone()),
                handler: target.as_ref().map(|t| t.handler.clone()),
                key: target
                    .as_ref()
                    .filter(|t| !t.object_key.is_empty())
                    .map(|t| t.object_key.clone()),
                deployment: r.deployment,
                attempt: r.attempt,
                node: None,
                created_at: r.times.created_ms,
                modified_at: r.times.modified_ms,
                ended_at: r.times.ended_ms(&r.status),
            });
        }
    }
    if filter.created_desc {
        sort_created_desc(&mut out.invocations);
    }
    Ok(out)
}

/// The target of an invocation's audit record.
pub(super) fn json_target(id: &InvocationId) -> serde_json::Value {
    serde_json::json!({ "invocation": hex(id) })
}
