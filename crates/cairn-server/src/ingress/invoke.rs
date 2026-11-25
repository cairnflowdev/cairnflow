// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Calling services and workflows: `invoke*`, `workflow_*`, `wait_for_settled`.

use super::*;

// ─────────────────────────────────────────────────────────── invoke

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum InvokeResponse {
    Completed {
        id: String,
        output: String,
    },
    /// Waiting on a timer. The client asks `/invocations/{id}` again.
    Waiting {
        id: String,
    },
    /// The slot wait cap ran out. `429`, with `Retry-After` **and** the id.
    ///
    /// Differs from `Waiting` in that it says *why*: the invocation has been accepted,
    /// is durable, is in the queue and has been rescheduled — it just has no budget to
    /// run yet. The client still looks up `/invocations/{id}` as usual.
    ///
    /// Not `503`: in cairn `503` already means "not the leader", and clients handle the
    /// two cases differently (one changes node, the other waits).
    Throttled {
        id: String,
        retry_after_ms: u64,
    },
    Failed {
        id: String,
        code: u32,
        message: String,
    },
    Paused {
        id: String,
        code: u32,
        message: String,
    },
    /// `409`: this workflow key already has a run. `id` is the run **holding** it — the
    /// client continues with `/workflow/{service}/{key}/attach`.
    AlreadyInvoked {
        id: String,
    },
}

/// The polling grid tick while the range's waiter book is being fed — a safety net, not
/// the main path (see [`wait_for_settled`]).
const FED_GRID: Duration = Duration::from_secs(1);

/// Waits for the invocation to stop for good, at most `st.invoke_wait`.
///
/// **Woken by the notifier, with the polling grid as a safety net.** The waiter
/// registers in the range's waiter book ([`crate::actor::Waiters`]); a commit taking the
/// invocation to `completed`/`failed`/`paused` notifies exactly that id at submit time,
/// and the next question lands right then. The final answer is still only produced in
/// `on_settled` → `finish` (durable ticket + read fence), so the notification carries no
/// state — it only makes the next question come sooner (LF-7). The book is not *pending
/// work* (INV-3): each waiter is an HTTP request already living in the ingress's RAM;
/// losing the book (leader change, range split, range on another node) only falls back
/// to the polling grid.
///
/// **A sparse grid when the notifier is present.** While the range's book is being fed —
/// the range's leader on this node has attached the notify hook
/// ([`crate::actor::Waiters::fed`]) — the grid only asks every [`FED_GRID`]: every
/// submitted outcome notifies, so the grid only catches failures of the notifier itself.
/// The waiter is registered **before** the first question (`watch` from
/// [`super::router::Accepted`] or the attach's status question), so the gap between the
/// first answer and the wait loop is notified too. When it stops serving, the leader
/// wakes every waiter (`Feeding::drop`) — they ask at once, see the range changed, and
/// follow the dense grid. A book not being fed (a range nobody leads yet) ⇒ the old
/// 5 → 50 ms grid. The wait cap and the question at the deadline are unchanged.
///
/// Waiting also stops at `Paused`, not only at completion: `Paused` is "needs a person",
/// and waiting longer is only waiting for something that will not come by itself.
///
/// An error while asking (losing leadership midway is a real case) ⇒ stop waiting and
/// return what we have. The invocation still exists, the client still has the id,
/// `/invocations/{id}` still points the way to the new leader.
pub(super) async fn wait_for_settled(
    st: &AppState,
    engine: &EngineHandle,
    id: &InvocationId,
    first: InvokeOutcome,
    watch: Option<crate::actor::Watch>,
    budget: Option<Duration>,
) -> InvokeOutcome {
    use crate::metrics::floor::{self, Answer, Wakeup};
    let Some(budget) = budget else {
        return first;
    };
    // `Throttled` does **not** keep waiting: the turn has been dropped and rescheduled,
    // so waiting here means holding an HTTP connection until the rescheduled run fires.
    // Returning `429` at once is the honest answer, and the client has `Retry-After` to
    // know how long to wait.
    if !matches!(first, InvokeOutcome::Waiting | InvokeOutcome::Retry { .. }) {
        return first;
    }
    let deadline = tokio::time::Instant::now() + budget;
    // The polling interval grows: most cross-partition calls finish within a few dozen
    // milliseconds, so asking densely at first pays off; for an invocation sleeping a
    // long time, asking densely only costs. The sparse grid floor is for tests only
    // (`NodeConfig::invoke_poll_floor`); the dense grid has no floor.
    let grid_floor = st.invoke_poll_floor.unwrap_or(Duration::ZERO);
    let mut every = Duration::from_millis(5);
    let mut engine = engine.clone();
    // Register **before** the first wait — if the caller registered before the first
    // question, use its registration; arm the notify future before every question — a
    // notification landing between the question and the wait is not lost.
    let (mut watch, mut armed) = match watch {
        Some(w) => w.into_parts(),
        None => engine.waiters().watch_armed(*id).into_parts(),
    };
    let mut ask_now = false;
    loop {
        if !ask_now {
            let step = if engine.waiters().fed() {
                FED_GRID.max(grid_floor)
            } else {
                every
            };
            let grid = tokio::time::sleep_until((tokio::time::Instant::now() + step).min(deadline));
            let by = tokio::select! {
                biased;
                _ = armed.as_mut() => Wakeup::Notify,
                () = grid => Wakeup::Grid,
            };
            floor::wakeup(by);
        }
        ask_now = false;
        armed = watch.arm();
        cairn_store::probe::mark("invoke.poll", every.as_millis() as u64);
        // Ask by **table**, not by the engine that accepted the call: the range may
        // split/merge while we wait — the old engine quiesces then retires, and the
        // invocation continues on the new range (`ask_range` waits for the table to
        // change, then asks again).
        //
        // Only ask **whether it is finished**: a "not yet" never goes outside, so it does
        // not pay a read fence GET per tick; a "finished" goes through the fence like
        // every read answer.
        let id = *id;
        let status = match st
            .router
            .ask_range(cairn_store::partition_key_of(&id), |e| async move {
                e.settled(id).await
            })
            .await
        {
            Some((e, r)) => {
                if !e.same(&engine) {
                    // The range changed (split/merge) while waiting: register in the new
                    // book, then ask once at once — an outcome commit landing in the gap
                    // between the two books is seen by that question.
                    watch = e.waiters().watch(id);
                    armed = watch.arm();
                    ask_now = true;
                }
                engine = e;
                r
            }
            None => engine.settled(id).await,
        };
        floor::poll(match &status {
            Ok(Some(
                InvocationStatus::Completed(_)
                | InvocationStatus::Failed { .. }
                | InvocationStatus::Paused { .. },
            )) => Answer::Final,
            Ok(_) => Answer::Pending,
            Err(_) => Answer::Error,
        });
        match status {
            Ok(Some(InvocationStatus::Completed(v))) => return InvokeOutcome::Completed(v),
            Ok(Some(InvocationStatus::Failed { code, message })) => {
                return InvokeOutcome::Failed { code, message }
            }
            Ok(Some(InvocationStatus::Paused { code, message })) => {
                return InvokeOutcome::Paused { code, message }
            }
            Ok(_) => {}
            // A range nobody leads yet (just created, being claimed): the invocation is
            // durable, keep asking until the cap. A leader elsewhere ⇒ return `202`.
            Err(crate::actor::ActorError::NotLeader)
                if st
                    .router
                    .leader_pending(st.router.cluster.partition_of_id(&id)) => {}
            Err(_) => return first,
        }
        if tokio::time::Instant::now() >= deadline {
            return first;
        }
        if !ask_now {
            every = (every * 2).min(Duration::from_millis(50));
        }
    }
}

/// The run id of a workflow ([`router::workflow_id`]); an empty key ⇒ `400`.
pub(super) fn workflow_id(
    tenant: &cairn_store::TenantId,
    scope: Option<&Scope>,
    service: &str,
    workflow_key: &str,
) -> ApiResult<InvocationId> {
    derive_workflow_id(tenant, scope, service, workflow_key).map_err(ApiError::refused)
}

/// `GET /workflow/{service}/{key}/id` — the run id of workflow
/// `(tenant, scope, service, key)`: **computed only**, reads nothing, does not say
/// whether the workflow has run (ask `GET /invocations/{id}` or
/// `sys_invocation WHERE id = …` — one point read). The same function as
/// `attach`/`output`, so the console and the CLI do not copy the tenant-aware hash. An
/// empty key ⇒ `400`; `cluster-admin` picks the tenant with `?tenant=`.
pub(super) async fn workflow_id_of(
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path((service, key)): Path<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<serde_json::Value>> {
    workflow_id_json(&who, None, &service, &key, &q)
}

pub(super) async fn scoped_workflow_id_of(
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path((scope, service, key)): Path<(String, String, String)>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<serde_json::Value>> {
    let scope = parse_scope(&scope)?;
    workflow_id_json(&who, Some(&scope), &service, &key, &q)
}

fn workflow_id_json(
    who: &crate::auth::Identity,
    scope: Option<&Scope>,
    service: &str,
    key: &str,
    q: &HashMap<String, String>,
) -> ApiResult<Json<serde_json::Value>> {
    let tenant = super::acting_tenant(who, q)?;
    let id = workflow_id(&tenant, scope, service, key)?;
    Ok(Json(serde_json::json!({
        "id": super::hex(&id),
        "tenant": tenant.as_str(),
        "service": service,
        "key": key,
    })))
}

/// `POST /workflow/{service}/{key}/attach` — waits for this workflow to stop for good.
///
/// This is the path forward after a `409 already_invoked`, and the way a **second**
/// client gets the result of a workflow it did not start. The same wait cap and the same
/// response shape as `/invoke` (see [`wait_for_settled`]) — a client that already reads
/// `/invoke` has nothing new to learn.
pub(super) async fn workflow_attach(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path((service, key)): Path<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Response> {
    let budget = attach_budget(&st, &q)?;
    workflow_watch(st, who.tenant, None, service, key, Some(budget)).await
}

pub(super) async fn scoped_workflow_attach(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path((scope, service, key)): Path<(String, String, String)>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Response> {
    let scope = parse_scope(&scope)?;
    let budget = attach_budget(&st, &q)?;
    workflow_watch(st, who.tenant, Some(scope), service, key, Some(budget)).await
}

/// The wait cap of an attach: `?wait_ms=` clamped to `--attach-max-wait-ms`; absent ⇒
/// `--invoke-wait-ms` as before. `0` ⇒ no wait. Not a number ⇒ `400`.
pub(super) fn attach_budget(
    st: &AppState,
    q: &HashMap<String, String>,
) -> ApiResult<Option<Duration>> {
    match q.get("wait_ms") {
        None => Ok(st.invoke_wait),
        Some(v) => {
            let ms: u64 = v.parse().map_err(|_| {
                ApiError(
                    StatusCode::BAD_REQUEST,
                    "wait_ms must be a non-negative number of milliseconds".into(),
                )
            })?;
            let d = Duration::from_millis(ms).min(st.attach_max_wait);
            Ok((!d.is_zero()).then_some(d))
        }
    }
}

/// `POST /invocations/{id}/attach[?wait_ms=]` — waits for an invocation **by id** to stop
/// for good (CG-2): the same body and the same in-RAM waiters as [`workflow_attach`], for
/// every invocation, not just workflows. Someone who just got `202` from `/invoke`
/// continues here with the very key that called `invoke`.
///
/// Another tenant or missing ⇒ `404` (the same sentence, existence not revealed). Target
/// and status are read in **one** turn on the leader (`view`); not the leader ⇒ `503`
/// with `leader` like every path by id.
pub(super) async fn attach_by_id(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path(id): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Response> {
    let raw = unhex(&id).ok_or_else(|| {
        ApiError(
            StatusCode::BAD_REQUEST,
            "id is not 48 hex characters".into(),
        )
    })?;
    let budget = attach_budget(&st, &q)?;
    // An attach that waits is a long-lived connection: hold the tenant's slot until the
    // answer.
    let _slot = match budget.map(|_| super::streams::TenantSlot::take(&st, &who.tenant)) {
        Some(None) => return Ok(super::streams::too_many_open_streams(&st)),
        slot => slot.flatten(),
    };
    let check_tenant = !who.all_tenants();
    let (engine, view) = st
        .router
        .ask_range(cairn_store::partition_key_of(&raw), |e| async move {
            // When waiting, register **before** the question (see [`wait_for_settled`]).
            let watch = budget.map(|_| e.waiters().watch_armed(raw));
            e.view(raw, check_tenant).await.map(|v| (v, watch))
        })
        .await
        .ok_or_else(range_settling)?;
    let (view, watch) =
        view.map_err(|e| ApiError::from_actor_routed(e, StatusCode::BAD_GATEWAY, &st, &raw))?;
    let not_found = || ApiError(StatusCode::NOT_FOUND, "no such invocation".into());
    if check_tenant && view.target.as_ref().map(|t| &t.tenant) != Some(&who.tenant) {
        return Err(not_found());
    }
    let status = view.status.ok_or_else(not_found)?;
    Ok(settled_response(&st, &engine, raw, status, watch, budget).await)
}

/// The answer of every attach: the current outcome, waiting up to `budget` more if it has
/// not stopped for good. `200` finished, `500` failed, `409` paused, `202` still running
/// — with the `x-cairn-invocation-id` header.
pub(super) async fn settled_response(
    st: &AppState,
    engine: &EngineHandle,
    id: InvocationId,
    status: InvocationStatus,
    watch: Option<crate::actor::Watch>,
    budget: Option<Duration>,
) -> Response {
    let outcome = match status {
        InvocationStatus::Completed(v) => InvokeOutcome::Completed(v),
        InvocationStatus::Failed { code, message } => InvokeOutcome::Failed { code, message },
        InvocationStatus::Paused { code, message } => InvokeOutcome::Paused { code, message },
        _ => InvokeOutcome::Waiting,
    };
    let outcome = wait_for_settled(st, engine, &id, outcome, watch, budget).await;
    let hex_id = hex(&id);
    let (code, body) = match outcome {
        InvokeOutcome::Completed(out) => match output_text(st, out).await {
            Ok(output) => (
                StatusCode::OK,
                InvokeResponse::Completed {
                    id: hex_id.clone(),
                    output,
                },
            ),
            Err(e) => return with_id_header(e.into_response(), &hex_id),
        },
        InvokeOutcome::Failed { code, message } => (
            StatusCode::INTERNAL_SERVER_ERROR,
            InvokeResponse::Failed {
                id: hex_id.clone(),
                code,
                message,
            },
        ),
        InvokeOutcome::Paused { code, message } => (
            StatusCode::CONFLICT,
            InvokeResponse::Paused {
                id: hex_id.clone(),
                code,
                message,
            },
        ),
        // Still running — even after the wait cap ran out. For `output` this is the
        // immediate answer, for `attach` it means "ask again later".
        _ => (
            StatusCode::ACCEPTED,
            InvokeResponse::Waiting { id: hex_id.clone() },
        ),
    };
    with_id_header((code, Json(body)).into_response(), &hex_id)
}

/// The output returned to the client: inline as today; segmented ⇒ assembled **outside**
/// the engine (GET each segment, check the sha) up to [`cairn_engine::MAX_ASSEMBLE`];
/// beyond ⇒ `413 value_too_large`. The JSON body is unchanged (`"output": "<string>"`).
/// The number of concurrent segmented-output assemblies of one tenant on the node. Each
/// allocates a buffer the size of the output (≤ [`cairn_engine::MAX_ASSEMBLE`]) and GETs
/// every segment not in the cache.
pub(super) const ASSEMBLE_PER_TENANT: usize = 4;

pub(super) async fn output_text(
    st: &AppState,
    out: cairn_engine::InvocationOutput,
) -> Result<String, ApiError> {
    let o = match out {
        cairn_engine::InvocationOutput::Inline(b) => {
            return Ok(String::from_utf8_lossy(&b).into_owned())
        }
        cairn_engine::InvocationOutput::Segmented(o) => o,
    };
    // The per-tenant concurrent assembly cap of the output (the segment's owner) — over
    // ⇒ `429` + `Retry-After`, no buffer allocated.
    let tenant = o
        .refs()
        .first()
        .and_then(|r| cairn_store::TenantId::new(&o.segment(r).tenant).ok());
    let _slot = match &tenant {
        Some(t) => Some(
            super::streams::TenantSlot::take_in(&st.assemble_open, t, ASSEMBLE_PER_TENANT)
                .ok_or_else(|| {
                    ApiError::coded(
                        StatusCode::TOO_MANY_REQUESTS,
                        Code::ValueAssemblyBusy,
                        format!(
                            "{ASSEMBLE_PER_TENANT} large outputs of this tenant are being assembled on this node — retry later"
                        ),
                    )
                    .retry_after_ms(1_000)
                })?,
        ),
        None => None,
    };
    // The node's cached store: reading the same output again does not GET the segments again.
    let Some(values) = st.value_store() else {
        return Err(store_unavailable("output", &"node has no main store"));
    };
    match crate::value_output::assemble(&values, &o, cairn_engine::MAX_ASSEMBLE).await {
        Ok(b) => Ok(String::from_utf8_lossy(&b).into_owned()),
        Err(crate::value_output::OutputError::TooLarge { len, max }) => Err(ApiError::coded(
            StatusCode::PAYLOAD_TOO_LARGE,
            Code::ValueTooLarge,
            format!("value_too_large: output of {len} bytes exceeds the limit of {max} bytes"),
        )),
        Err(e) => Err(store_unavailable("output", &e)),
    }
}

/// The `x-cairn-invocation-id` header: the client reads the id without parsing the body
/// — including an error body.
pub(super) fn with_id_header(mut resp: Response, hex_id: &str) -> Response {
    if let Ok(v) = axum::http::HeaderValue::from_str(hex_id) {
        resp.headers_mut().insert("x-cairn-invocation-id", v);
    }
    resp
}

/// `GET /workflow/{service}/{key}/output` — the result **if it exists**, no waiting.
pub(super) async fn workflow_output(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path((service, key)): Path<(String, String)>,
) -> ApiResult<Response> {
    workflow_watch(st, who.tenant, None, service, key, None).await
}

pub(super) async fn scoped_workflow_output(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path((scope, service, key)): Path<(String, String, String)>,
) -> ApiResult<Response> {
    let scope = parse_scope(&scope)?;
    workflow_watch(st, who.tenant, Some(scope), service, key, None).await
}

/// The shared body of `attach` and `output`.
///
/// **Never run ⇒ `404`, not `202`.** The two differ in what the client must do next:
/// `202` means "ask again", `404` means "nobody started this workflow, call `run`".
/// Merging them would leave a client waiting forever for something that never comes.
pub(super) async fn workflow_watch(
    st: AppState,
    tenant: cairn_store::TenantId,
    scope: Option<Scope>,
    service: String,
    key: String,
    // `None` = `output` (no waiting); `Some(cap)` = attach.
    wait: Option<Option<Duration>>,
) -> ApiResult<Response> {
    if key.is_empty() {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "workflow key is empty".into(),
        ));
    }
    let budget = wait.flatten();
    // An attach that waits is a long-lived connection.
    let _slot = match budget.map(|_| super::streams::TenantSlot::take(&st, &tenant)) {
        Some(None) => return Ok(super::streams::too_many_open_streams(&st)),
        slot => slot.flatten(),
    };
    let id = workflow_id(&tenant, scope.as_ref(), &service, &key)?;
    let (engine, status) = st
        .router
        .ask_range(cairn_store::partition_key_of(&id), |e| async move {
            // When waiting, register **before** the status question (see
            // [`wait_for_settled`]).
            let watch = budget.map(|_| e.waiters().watch_armed(id));
            e.status(id).await.map(|s| (s, watch))
        })
        .await
        .ok_or_else(|| {
            ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!(
                    "partition {} does not belong to this cluster",
                    st.router.cluster.partition_of_id(&id).0
                ),
            )
        })?;
    let (status, watch) =
        status.map_err(|e| ApiError::from_actor_routed(e, StatusCode::BAD_GATEWAY, &st, &id))?;
    let Some(status) = status else {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            format!("workflow {service}/{key} has never been called"),
        ));
    };
    Ok(settled_response(&st, &engine, id, status, watch, budget).await)
}

/// `POST /invoke/{service}/{handler}` — **waits for the result**.
///
/// This door used to return right after the first turn, and the response shape
/// therefore depended on **where the callee hashed to**: a `ctx.call` within the same
/// partition finished within the caller's turn ⇒ `200` with the result; across
/// partitions it could not (the receiver writes the message and acks at once, to avoid
/// deadlocking two engine threads on a cyclic call chain) ⇒ `202`. The same code, the
/// same request, two shapes — a one-partition dev setup saw `200`, an eight-partition
/// production saw `202`. The green-in-dev, broken-in-prod kind of failure.
///
/// Now there is one rule, on every cluster: **wait until the invocation stops for good,
/// at most `--invoke-wait-ms`**; past the cap, `202` with the id. `202` can still happen
/// — it has to, because a handler sleeping for an hour must not hold an HTTP connection
/// for an hour — but it no longer depends on topology.
///
/// Waiting here is cheap **thanks to separating SDK calls from the engine thread**:
/// status queries no longer queue behind a running turn. Before that, this wait loop
/// would have deadlocked itself.
pub(super) async fn invoke(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path((service, handler)): Path<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> ApiResult<Response> {
    invoke_inner(
        st, who.tenant, None, service, handler, q, headers, body, true,
    )
    .await
}

/// `POST /scope/{scope}/invoke/{service}/{handler}`.
///
/// The same semantics as [`invoke`], with exactly one difference: everything this
/// invocation touches **lives inside the scope** — state, promises, idempotency keys, and
/// concurrency counters. `Cart/a` here is a different instance from `Cart/a` on the
/// unscoped path, and two tenants sharing an object key do not see each other.
pub(super) async fn scoped_invoke(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path((scope, service, handler)): Path<(String, String, String)>,
    Query(q): Query<HashMap<String, String>>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> ApiResult<Response> {
    let scope = parse_scope(&scope)?;
    invoke_inner(
        st,
        who.tenant,
        Some(scope),
        service,
        handler,
        q,
        headers,
        body,
        true,
    )
    .await
}

pub(super) async fn scoped_invoke_send(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path((scope, service, handler)): Path<(String, String, String)>,
    Query(q): Query<HashMap<String, String>>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> ApiResult<Response> {
    let scope = parse_scope(&scope)?;
    invoke_inner(
        st,
        who.tenant,
        Some(scope),
        service,
        handler,
        q,
        headers,
        body,
        false,
    )
    .await
}

pub(super) fn parse_scope(raw: &str) -> ApiResult<Scope> {
    Scope::new(raw).map_err(|e: IdentError| ApiError(StatusCode::BAD_REQUEST, e.to_string()))
}

/// The limit key from `?limit-key=` or `x-cairn-limit-key`.
///
/// **A limit key without a scope ⇒ `400`.** Not fussiness: without a scope there is no
/// counter to hang it on, so a limit key on the unscoped path is a parameter *with no
/// effect at all* — and silently ignoring it would let the operator believe they just
/// set a cap.
pub(super) fn parse_limit_key(
    scope: Option<&Scope>,
    q: &HashMap<String, String>,
    headers: &axum::http::HeaderMap,
) -> ApiResult<Option<LimitKey>> {
    let raw = q.get("limit-key").cloned().or_else(|| {
        headers
            .get("x-cairn-limit-key")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    });
    let Some(raw) = raw else {
        return Ok(None);
    };
    if scope.is_none() {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "a limit key must come with a scope — use /scope/{scope}/invoke/... (without a scope there is no counter to limit)"
                .into(),
        ));
    }
    Ok(Some(LimitKey::parse(&raw).map_err(|e: IdentError| {
        ApiError(StatusCode::BAD_REQUEST, e.to_string())
    })?))
}

/// `POST /invoke/{service}/{handler}/send` — fire and forget, no waiting.
///
/// Answers as soon as the invocation **is durable**, not when it has run: the id handed
/// to the client can always be looked up at `/invocations/{id}`, and a slow handler does
/// not hold the request of someone who only wants to light the fuse. Idempotency still
/// applies — the same `Idempotency-Key` is still one invocation.
pub(super) async fn invoke_send(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path((service, handler)): Path<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> ApiResult<Response> {
    invoke_inner(
        st, who.tenant, None, service, handler, q, headers, body, false,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn invoke_inner(
    st: AppState,
    tenant: cairn_store::TenantId,
    scope: Option<Scope>,
    service: String,
    handler: String,
    q: HashMap<String, String>,
    headers: axum::http::HeaderMap,
    body: Bytes,
    wait: bool,
) -> ApiResult<Response> {
    crate::metrics::Metrics::bump(&st.metrics.invoke_requests);
    cairn_store::probe::mark("invoke.in", u64::from(wait));
    // The tenant's `max-payload=`: before anything else.
    if let Some(max) = st
        .tenant_settings
        .as_ref()
        .and_then(|t| t.flags(&tenant).limits.max_payload)
        .filter(|max| body.len() as u64 > *max)
    {
        crate::metrics::tenant::bump(&crate::metrics::tenant::PAYLOAD_REJECTED);
        return Err(ApiError::coded(
            StatusCode::PAYLOAD_TOO_LARGE,
            super::error::Code::PayloadTooLarge,
            format!(
                "body of {} bytes exceeds the {max} byte limit of tenant {tenant}",
                body.len()
            ),
        ));
    }
    // Whether the service/handler is in the tenant's registry — before any header error
    // (this door's error order predates the router split).
    st.router
        .handler_kind(tenant.as_str(), &service, &handler)
        .map_err(ApiError::refused)?;
    // Idempotency at the HTTP door: the `Idempotency-Key` header. A client retrying with
    // the same key gets the same invocation — without it, a retry after a timeout is a
    // side effect running twice, and no layer below can save it because each request
    // already carries a brand-new id.
    let idempotency_key = match headers.get("idempotency-key") {
        None => None,
        Some(v) => Some(
            v.to_str()
                .map_err(|_| {
                    ApiError(
                        StatusCode::BAD_REQUEST,
                        "Idempotency-Key must be valid UTF-8".into(),
                    )
                })?
                .to_owned(),
        ),
    };

    let limit_key = parse_limit_key(scope.as_ref(), &q, &headers)?;
    // The minimum `ctx.log` level for this invocation: goes into the header of the same
    // name on entry 0, the SDK overrides `HandlerOptions.logLevel`. An unknown value ⇒
    // `400` before creating anything — a level silently misread means logs vanish with
    // nobody knowing why.
    let log_level = match headers.get("cairnflow-log-level") {
        None => None,
        Some(v) => match v.to_str().map(str::to_ascii_lowercase) {
            Ok(l) if matches!(l.as_str(), "debug" | "info" | "warn" | "error") => Some(l),
            _ => {
                return Err(ApiError(
                    StatusCode::BAD_REQUEST,
                    "cairnflow-log-level must be debug, info, warn or error".into(),
                ))
            }
        },
    };
    let tags = parse_tags(&headers)?;
    let call = Call {
        target: Target {
            tenant,
            scope,
            service,
            handler,
            object_key: q.get("key").cloned().unwrap_or_default(),
        },
        limit_key,
        idempotency_key,
        input: body,
        log_level,
        tags,
    };
    // Generating the id, asking the right range, answering NotLeader: [`Router`] — this
    // door only parses and serializes.
    let delivery = if wait { Delivery::Wait } else { Delivery::Send };
    let Accepted {
        id,
        engine,
        outcome,
        watch,
    } = st
        .router
        .submit(call, delivery)
        .await
        .map_err(|e| ApiError::from_route(e, StatusCode::BAD_GATEWAY, &st))?;

    cairn_store::probe::mark("invoke.submitted", 0);
    let outcome = if wait {
        wait_for_settled(&st, &engine, &id, outcome, watch, st.invoke_wait).await
    } else {
        outcome
    };

    let hex = hex(&id);
    let m = &st.metrics;
    // `Retry-After` only exists on the `429` arm, and it must be a real header — clients
    // and load balancers already know how to read it; making them parse JSON to know how
    // long to wait would invent a second contract for something that already has a
    // standard.
    let mut retry_after: Option<u64> = None;
    let (code, body) = match outcome {
        InvokeOutcome::Completed(out) => {
            crate::metrics::Metrics::bump(&m.invoke_completed);
            let output = match output_text(&st, out).await {
                Ok(o) => o,
                Err(e) => return Ok(with_id_header(e.into_response(), &hex)),
            };
            (
                StatusCode::OK,
                InvokeResponse::Completed { id: hex, output },
            )
        }
        // Waiting and retry-scheduled look the same from the client: not done, ask later.
        InvokeOutcome::Waiting | InvokeOutcome::Retry { .. } => {
            crate::metrics::Metrics::bump(&m.invoke_waiting);
            (StatusCode::ACCEPTED, InvokeResponse::Waiting { id: hex })
        }
        InvokeOutcome::Failed { code, message } => {
            crate::metrics::Metrics::bump(&m.invoke_failed);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                InvokeResponse::Failed {
                    id: hex,
                    code,
                    message,
                },
            )
        }
        InvokeOutcome::Throttled { retry_after_ms } => {
            crate::metrics::Metrics::bump(&m.invoke_throttled);
            retry_after = Some(retry_after_ms);
            (
                StatusCode::TOO_MANY_REQUESTS,
                InvokeResponse::Throttled {
                    id: hex,
                    retry_after_ms,
                },
            )
        }
        InvokeOutcome::Paused { code, message } => {
            crate::metrics::Metrics::bump(&m.invoke_paused);
            (
                StatusCode::CONFLICT,
                InvokeResponse::Paused {
                    id: hex,
                    code,
                    message,
                },
            )
        }
        InvokeOutcome::AlreadyInvoked => (
            StatusCode::CONFLICT,
            InvokeResponse::AlreadyInvoked { id: hex },
        ),
    };
    cairn_store::probe::mark("invoke.out", u64::from(code.as_u16()));
    let id_header = match &body {
        InvokeResponse::Completed { id, .. }
        | InvokeResponse::Waiting { id }
        | InvokeResponse::Throttled { id, .. }
        | InvokeResponse::Failed { id, .. }
        | InvokeResponse::Paused { id, .. }
        | InvokeResponse::AlreadyInvoked { id } => id.clone(),
    };
    let mut resp = with_id_header((code, Json(body)).into_response(), &id_header);
    if let Some(v) = retry_after.and_then(retry_after_header) {
        resp.headers_mut()
            .insert(axum::http::header::RETRY_AFTER, v);
    }
    Ok(resp)
}

/// User tags of a call: one tag per `cairnflow-tag: k=v` header, at most
/// [`cairn_store::query_index::MAX_TAGS`]; keys ≤ 32 bytes `[a-z0-9_.-]`, not starting
/// with `$` (reserved for the system), values ≤ 64 bytes, no repeated key. Wrong ⇒ `400`
/// before creating anything: tags are immutable, and a silently misread tag is a filter
/// that never matches.
pub(super) fn parse_tags(headers: &axum::http::HeaderMap) -> ApiResult<Vec<(String, String)>> {
    use cairn_store::query_index::{check_tag, MAX_TAGS};
    let bad = |m: String| ApiError(StatusCode::BAD_REQUEST, m);
    let mut tags: Vec<(String, String)> = Vec::new();
    for v in headers.get_all("cairnflow-tag") {
        let s = v
            .to_str()
            .map_err(|_| bad("cairnflow-tag must be valid UTF-8".into()))?;
        let (k, val) = s
            .split_once('=')
            .ok_or_else(|| bad(format!("cairnflow-tag {s:?} must have the form k=v")))?;
        let (k, val) = (k.trim(), val.trim());
        check_tag(k, val).map_err(bad)?;
        if tags.iter().any(|(x, _)| x == k) {
            return Err(bad(format!("tag {k:?} repeated")));
        }
        tags.push((k.to_string(), val.to_string()));
    }
    if tags.len() > MAX_TAGS {
        return Err(bad(format!("{} tags, the limit is {MAX_TAGS}", tags.len())));
    }
    Ok(tags)
}
