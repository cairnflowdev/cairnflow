// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The server-side schedule API: `/schedules/{name}` and the `/scope/{scope}/…`
//! variant.
//!
//! What needs no Db is checked here (name, spec, input, overlap by key); the rest —
//! the `schedules/table` gate, the handler being in the current revision, same hash
//! ⇒ no-op — happens in the engine of the range holding the schedule's pk
//! ([`crate::schedules`]).

use {
    super::*,
    crate::schedules::{PutSchedule, ScheduleAnswer, ScheduleOp, ScheduleView},
    cairn_engine::schedule::{LastKind, Overlap, SpecRow, MAX_INPUT},
};

/// Cap on `jitterMs`: one day — no schedule needs a longer delay, and a huge number once
/// moved the firing before the mark.
const MAX_JITTER_MS: u64 = 86_400_000;

/// Cap on the length of a schedule's `target.key`: the key goes into the state/
/// idempotency keys of the destination range; too long and the firing would hit
/// `KeyTooLong` in `admit` instead of the `PUT` being refused.
const MAX_TARGET_KEY: usize = 4_096;

/// Index rows per listing/counting chunk: one engine command reads up to this many
/// index rows and as many schedule rows, then releases the engine.
const PAGE: usize = 256;

/// Cap on the schedules **of the asking tenant** per range in one listing.
const LIST_LIMIT: usize = 1_000;

/// How long `trigger` waits at most for the run to show up on the destination range (a
/// destination on another range: the `Fire` row is durable on the schedule's range, the
/// drainer still has to carry it over).
const TRIGGER_DELIVERY_WAIT: Duration = Duration::from_secs(10);

/// Waits, bounded, for the `run` invocation to show up on its range. `true` = seen.
///
/// Without it, the handle `trigger` returns for a destination on another range, attached
/// at once, would hit `404` in the gap before the drainer delivers `StartCall`. A
/// destination on the same range: the first question already sees it (the run is created
/// in the same commit as the fire command).
async fn delivered(st: &AppState, run: InvocationId) -> bool {
    let deadline = tokio::time::Instant::now() + TRIGGER_DELIVERY_WAIT;
    let mut gap = Duration::from_millis(10);
    loop {
        let seen = st
            .router
            .ask_range(cairn_store::partition_key_of(&run), |e| async move {
                e.view(run, false).await
            })
            .await
            .and_then(|(_, v)| v.ok())
            .is_some_and(|v| v.status.is_some());
        if seen {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        cairn_store::pace::poll_gap(gap).await;
        gap = (gap * 2).min(Duration::from_millis(200));
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TargetBody {
    service: String,
    handler: String,
    key: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ScheduleBody {
    cron: Option<String>,
    time_zone: Option<String>,
    every_ms: Option<u64>,
    target: TargetBody,
    input: Option<serde_json::Value>,
    limit_key: Option<String>,
    overlap: Option<String>,
    #[serde(default)]
    paused: bool,
    #[serde(default)]
    jitter_ms: u64,
}

fn bad(message: impl Into<String>) -> ApiError {
    ApiError(StatusCode::BAD_REQUEST, message.into())
}

/// Schedule name: 1–128 characters `[A-Za-z0-9._-]`.
fn check_name(name: &str) -> ApiResult<()> {
    let ok = (1..=128).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if ok {
        Ok(())
    } else {
        Err(bad("schedule name must be 1–128 characters [A-Za-z0-9._-]"))
    }
}

/// `PUT` body ⇒ a checked definition (except the parts needing the deployment registry and the Db).
fn parse_body(
    st: &AppState,
    tenant: &cairn_store::TenantId,
    scope: Option<Scope>,
    name: String,
    body: &[u8],
) -> ApiResult<PutSchedule> {
    let b: ScheduleBody =
        serde_json::from_slice(body).map_err(|e| bad(format!("invalid schedule body: {e}")))?;
    let spec = match (b.cron, b.every_ms) {
        (Some(expr), None) => SpecRow::Cron {
            expr,
            time_zone: b.time_zone.unwrap_or_else(|| "UTC".into()),
        },
        (None, Some(ms)) if b.time_zone.is_none() => SpecRow::Every(ms),
        (None, Some(_)) => return Err(bad("timeZone only goes with cron")),
        _ => return Err(bad("need exactly one of cron / everyMs")),
    };
    let min = st.schedule_min_period.as_millis() as u64;
    let parsed = spec.parse(min).map_err(|e| bad(e.to_string()))?;
    parsed
        .validate(cairn_store::entropy::now_ms() as i64)
        .map_err(|e| bad(e.to_string()))?;
    let input = match &b.input {
        None => Bytes::new(),
        Some(v) => Bytes::from(serde_json::to_vec(v).map_err(|e| bad(e.to_string()))?),
    };
    if input.len() > MAX_INPUT {
        return Err(bad(format!("input exceeds {MAX_INPUT} bytes")));
    }
    if let Some(k) = &b.limit_key {
        LimitKey::parse(k).map_err(|e: IdentError| bad(e.to_string()))?;
    }
    if b.jitter_ms > MAX_JITTER_MS {
        return Err(bad(format!(
            "jitterMs is at most {MAX_JITTER_MS} (24 hours)"
        )));
    }
    let key = b.target.key.unwrap_or_default();
    if key.len() > MAX_TARGET_KEY {
        return Err(bad(format!(
            "target.key is at most {MAX_TARGET_KEY} bytes, got {}",
            key.len()
        )));
    }
    // A key or a scope ⇒ the run is on the destination's range, a different range: the
    // previous run cannot be read in the same Db, so only `allow` (a VO already queues
    // by key).
    let keyed = !key.is_empty() || scope.is_some();
    let overlap = match (b.overlap.as_deref(), keyed) {
        (None, true) | (Some("allow"), _) => Overlap::Allow,
        (None, false) | (Some("skip"), false) => Overlap::Skip,
        (Some("skip"), true) => {
            return Err(bad(
                "overlap skip is only for targets without a key or scope — this target only allows allow",
            ))
        }
        (Some(o), _) => return Err(bad(format!("overlap {o:?} does not exist; choose skip or allow"))),
    };
    Ok(PutSchedule {
        tenant: tenant.clone(),
        scope,
        name,
        spec,
        service: b.target.service,
        handler: b.target.handler,
        key,
        limit_key: b.limit_key,
        input,
        overlap,
        paused: b.paused,
        jitter_ms: b.jitter_ms,
    })
}

/// The JSON of a schedule. `upcoming`: the next 10 marks (only `GET` of a single schedule).
fn render(v: &ScheduleView, upcoming: bool) -> serde_json::Value {
    let r = &v.row;
    let mut target = serde_json::json!({ "service": r.service, "handler": r.handler });
    if !r.key.is_empty() {
        target["key"] = r.key.clone().into();
    }
    let paused = v.due == u64::MAX;
    let mut out = serde_json::json!({
        "name": r.name,
        "id": hex(&v.id),
        "target": target,
        "overlap": match r.overlap { Overlap::Skip => "skip", Overlap::Allow => "allow" },
        "paused": r.paused,
        "jitterMs": r.jitter_ms,
        "nextAt": if paused { serde_json::Value::Null } else { r.next_at.into() },
        "fired": r.fired,
        "missed": r.missed,
        "skipped": r.skipped,
        "createdAt": r.created_ms,
        "updatedAt": r.updated_ms,
    });
    if !paused {
        out["fireAt"] = v.due.into();
    }
    if let Some(s) = &r.scope {
        out["scope"] = s.clone().into();
    }
    match &r.spec {
        SpecRow::Cron { expr, time_zone } => {
            out["cron"] = expr.clone().into();
            out["timeZone"] = time_zone.clone().into();
        }
        SpecRow::Every(ms) => out["everyMs"] = (*ms).into(),
    }
    if !r.input.is_empty() {
        out["input"] = serde_json::from_slice(&r.input).unwrap_or(serde_json::Value::Null);
    }
    if let Some(k) = &r.limit_key {
        out["limitKey"] = k.clone().into();
    }
    out["last"] = r
        .last
        .iter()
        .enumerate()
        .map(|(i, l)| {
            let mut x = serde_json::json!({
                "at": l.at,
                "id": hex(&l.id),
                "outcome": match l.kind { LastKind::Fired => "fired", LastKind::Skipped => "skipped" },
            });
            if let Some(Some(s)) = v.last_status.get(i) {
                x["status"] = (*s).into();
            }
            x
        })
        .collect::<Vec<_>>()
        .into();
    if upcoming {
        // From the next mark (inclusive); when paused, from the current time.
        let after = if paused {
            cairn_store::entropy::now_ms() as i64
        } else {
            r.next_at as i64 - 1
        };
        if let Ok(spec) = r.spec.parse(0) {
            out["upcoming"] = spec.upcoming(after, 10).into();
        }
    }
    out
}

/// Sends `op` to the range holding the pk of schedule `(tenant, scope, name)`.
async fn ask(
    st: &AppState,
    tenant: &cairn_store::TenantId,
    scope: Option<&Scope>,
    name: &str,
    op: ScheduleOp,
    note: Option<crate::audit::AuditNote>,
) -> ApiResult<ScheduleAnswer> {
    let pk = cairn_store::partition_key_for_schedule(tenant, scope, name);
    let (_, r) = st
        .router
        .ask_range(pk, |e| {
            let (op, note) = (op.clone(), note.clone());
            async move { e.audited(note).schedule(op).await }
        })
        .await
        .ok_or_else(range_settling)?;
    let answer = r.map_err(|e| ApiError::from_actor_keyed(e, StatusCode::BAD_GATEWAY, st, pk))?;
    if let ScheduleAnswer::Refused {
        status,
        code,
        message,
    } = answer
    {
        let status = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_REQUEST);
        return Err(match code {
            Some(crate::schedules::SCHEDULES_DISABLED) => {
                ApiError::coded(status, Code::SchedulesDisabled, message)
            }
            _ => ApiError(status, message),
        });
    }
    Ok(answer)
}

fn not_found(name: &str) -> ApiError {
    ApiError(StatusCode::NOT_FOUND, format!("no schedule {name:?}"))
}

fn scoped(raw: Option<String>) -> ApiResult<Option<Scope>> {
    raw.map(|s| super::invoke::parse_scope(&s)).transpose()
}

async fn put_inner(
    st: AppState,
    tenant: cairn_store::TenantId,
    scope: Option<Scope>,
    name: String,
    body: Bytes,
    note: Option<crate::audit::AuditNote>,
) -> ApiResult<Json<serde_json::Value>> {
    check_name(&name)?;
    let def = parse_body(&st, &tenant, scope.clone(), name.clone(), &body)?;
    // `max-schedules=`: a **new** schedule over the cap ⇒ `409`. Counted over the
    // cluster-wide list (every range, every scope); editing an existing schedule does not
    // count. A list missing ranges (`partial`) counts what it can — a soft cap, like
    // `max-live=`.
    if let Some(max) = st
        .tenant_settings
        .as_ref()
        .and_then(|t| t.limits_of(&tenant).max_schedules)
    {
        // Count from `TenantUsage` (the node's counting loop) plus the schedules this node
        // created since that count — no cluster-wide listing on every `PUT`. Editing an
        // existing schedule does not count: one point read of the schedule row on its range.
        let id = cairn_store::schedule_id(&tenant, scope.as_ref(), &name);
        let exists = matches!(
            ask(
                &st,
                &tenant,
                scope.as_ref(),
                &name,
                ScheduleOp::Get(id),
                None
            )
            .await?,
            ScheduleAnswer::View(Some(_))
        );
        let used = st.schedules_in_use(&tenant);
        if !exists && used >= u64::from(max) {
            return Err(ApiError::coded(
                StatusCode::CONFLICT,
                super::error::Code::SchedulesLimit,
                format!(
                    "tenant {tenant} already has {used} schedules (max-schedules limit {max}) — delete an old schedule and create again"
                ),
            ));
        }
    }
    match ask(
        &st,
        &tenant,
        scope.as_ref(),
        &name,
        ScheduleOp::Put(Box::new(def)),
        note,
    )
    .await?
    {
        ScheduleAnswer::Put {
            view,
            created,
            changed,
        } => {
            if created {
                st.note_schedule_created(&tenant, 1);
            }
            let mut out = render(&view, false);
            out["created"] = created.into();
            out["changed"] = changed.into();
            Ok(Json(out))
        }
        other => Err(unexpected(other)),
    }
}

fn unexpected(a: ScheduleAnswer) -> ApiError {
    ApiError(
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("unexpected schedule answer: {a:?}"),
    )
}

/// Commands by schedule id: `GET`, `DELETE`, `pause`, `resume`, `trigger`.
#[derive(Clone, Copy)]
enum ById {
    Get,
    Delete,
    Pause,
    Resume,
    Trigger,
}

async fn by_id(
    st: AppState,
    tenant: cairn_store::TenantId,
    scope: Option<Scope>,
    name: String,
    what: ById,
    headers: &axum::http::HeaderMap,
    note: impl FnOnce(&'static str) -> Option<crate::audit::AuditNote>,
) -> ApiResult<Json<serde_json::Value>> {
    check_name(&name)?;
    let id = cairn_store::schedule_id(&tenant, scope.as_ref(), &name);
    let op = match what {
        ById::Get => ScheduleOp::Get(id),
        ById::Delete => ScheduleOp::Delete(id),
        ById::Pause => ScheduleOp::Pause(id),
        ById::Resume => ScheduleOp::Resume(id),
        ById::Trigger => {
            // Request id: `Idempotency-Key`, random when absent (each send is one
            // firing).
            let request = match headers.get("idempotency-key") {
                Some(v) => v
                    .to_str()
                    .map_err(|_| bad("Idempotency-Key must be valid UTF-8"))?
                    .to_owned(),
                None => {
                    let mut b = [0u8; 16];
                    cairn_store::entropy::fill(&mut b);
                    b.iter().map(|x| format!("{x:02x}")).collect()
                }
            };
            ScheduleOp::Trigger { id, request }
        }
    };
    let note = match what {
        ById::Get => None,
        ById::Delete => note("schedule.delete"),
        ById::Pause => note("schedule.pause"),
        ById::Resume => note("schedule.resume"),
        ById::Trigger => note("schedule.trigger"),
    };
    match ask(&st, &tenant, scope.as_ref(), &name, op, note).await? {
        ScheduleAnswer::View(Some(v)) => Ok(Json(render(&v, matches!(what, ById::Get)))),
        ScheduleAnswer::View(None) | ScheduleAnswer::Deleted(false) => Err(not_found(&name)),
        ScheduleAnswer::Deleted(true) => {
            st.note_schedule_created(&tenant, -1);
            Ok(Json(serde_json::json!({ "name": name, "deleted": true })))
        }
        ScheduleAnswer::Triggered(None) => Err(not_found(&name)),
        ScheduleAnswer::Triggered(Some(Some((run, created)))) => {
            // Cap reached before arrival: the firing is durable (the `Fire` row), the run
            // will exist — `delivered: false` so the client knows an attach may hit `404`
            // for a while.
            let delivered = delivered(&st, run).await;
            Ok(Json(
                serde_json::json!({ "id": hex(&run), "created": created, "delivered": delivered }),
            ))
        }
        // L0 brake: nothing created, sending again with the same key after `Retry-After` is safe.
        ScheduleAnswer::Triggered(Some(None)) => Err(ApiError(
            StatusCode::TOO_MANY_REQUESTS,
            "range is overloaded — not fired, retry later".into(),
        )
        .retry_after_ms(1_000)),
        other => Err(unexpected(other)),
    }
}

/// The target of a schedule's audit record.
fn schedule_target(scope: Option<&str>, name: &str) -> serde_json::Value {
    serde_json::json!({ "schedule": name, "scope": scope })
}

pub(super) async fn put_schedule(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    meta: super::Meta,
    Path(name): Path<String>,
    body: Bytes,
) -> ApiResult<Json<serde_json::Value>> {
    let note = super::audit_note(
        &who,
        meta.as_deref(),
        "schedule.put",
        schedule_target(None, &name),
    );
    put_inner(st, who.tenant, None, name, body, note).await
}

pub(super) async fn scoped_put_schedule(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    meta: super::Meta,
    Path((scope, name)): Path<(String, String)>,
    body: Bytes,
) -> ApiResult<Json<serde_json::Value>> {
    let note = super::audit_note(
        &who,
        meta.as_deref(),
        "schedule.put",
        schedule_target(Some(&scope), &name),
    );
    put_inner(st, who.tenant, scoped(Some(scope))?, name, body, note).await
}

macro_rules! by_id_routes {
    ($($plain:ident, $scoped:ident => $what:expr;)*) => {$(
        pub(super) async fn $plain(
            State(st): State<AppState>,
            axum::Extension(who): axum::Extension<crate::auth::Identity>,
            meta: super::Meta,
            Path(name): Path<String>,
            headers: axum::http::HeaderMap,
        ) -> ApiResult<Json<serde_json::Value>> {
            let target = schedule_target(None, &name);
            let note = |action| super::audit_note(&who, meta.as_deref(), action, target.clone());
            by_id(st.clone(), who.tenant.clone(), None, name, $what, &headers, note).await
        }

        pub(super) async fn $scoped(
            State(st): State<AppState>,
            axum::Extension(who): axum::Extension<crate::auth::Identity>,
            meta: super::Meta,
            Path((scope, name)): Path<(String, String)>,
            headers: axum::http::HeaderMap,
        ) -> ApiResult<Json<serde_json::Value>> {
            let target = schedule_target(Some(&scope), &name);
            let note = |action| super::audit_note(&who, meta.as_deref(), action, target.clone());
            by_id(st.clone(), who.tenant.clone(), scoped(Some(scope))?, name, $what, &headers, note)
                .await
        }
    )*};
}

by_id_routes! {
    get_schedule, scoped_get_schedule => ById::Get;
    delete_schedule, scoped_delete_schedule => ById::Delete;
    pause_schedule, scoped_pause_schedule => ById::Pause;
    resume_schedule, scoped_resume_schedule => ById::Resume;
    trigger_schedule, scoped_trigger_schedule => ById::Trigger;
}

/// One node's part of `GET /schedules`.
#[derive(Serialize, Deserialize)]
pub(super) struct NodeSchedules {
    ranges: Vec<u32>,
    schedules: Vec<serde_json::Value>,
    /// Ranges that returned the full [`LIST_LIMIT`] (every tenant) — that range's list
    /// may be missing schedules of the asking tenant. Absent on old binaries.
    #[serde(default)]
    truncated: Vec<u32>,
}

/// Cap on index chunks per range per `GET /schedules` request (security review): the
/// `ScheduleDue` index is ordered by due time, not by tenant, so a tenant with few
/// schedules used to scan **the whole** index of every tenant on every request. Over the
/// cap ⇒ the range goes into `truncatedRanges`, `partial: true` — as before, but the
/// tenant filter still runs under the engine, so the cap is not eaten by other tenants'
/// schedules.
const LIST_PAGES_PER_RANGE: usize = 8;

/// Read units charged to the asking tenant's bucket **before** each chunk: up to
/// [`PAGE`] index rows plus as many point reads of schedule rows.
const PAGE_READ_UNITS: u64 = 2 * PAGE as u64;

/// The schedules of `tenant` (and `scope`, if given) on the ranges this node holds —
/// at most [`LIST_PAGES_PER_RANGE`] chunks per range, each chunk charging
/// [`PAGE_READ_UNITS`] to `tenant`'s read bucket on this node; bucket empty or cap
/// reached ⇒ the range goes into `truncated`.
async fn local_schedules(
    st: &AppState,
    tenant: &cairn_store::TenantId,
    scope: Option<&Scope>,
) -> ApiResult<NodeSchedules> {
    let mut out = NodeSchedules {
        ranges: Vec::new(),
        schedules: Vec::new(),
        truncated: Vec::new(),
    };
    // Row form: `default` is written as `None`.
    let want_tenant = (!tenant.is_default()).then(|| tenant.as_str().to_string());
    'ranges: for (p, engine) in st.router.partitions.iter() {
        // Chunked with a cursor, filtering tenant/scope under the engine: other tenants'
        // schedules do not eat the asking tenant's cap; one engine command per chunk —
        // the engine is released between two chunks.
        let mut after = None;
        let mut mine = Vec::new();
        let mut pages = 0;
        loop {
            if pages == LIST_PAGES_PER_RANGE
                || st
                    .query()
                    .is_some_and(|svc| !svc.spend_read_units(tenant.as_str(), PAGE_READ_UNITS))
            {
                out.truncated.push(p);
                break;
            }
            pages += 1;
            let (rows, next) = match engine
                .schedule(ScheduleOp::Page {
                    after,
                    limit: PAGE,
                    tenant: want_tenant.clone(),
                    scope: scope.map(|s| s.as_str().to_string()),
                })
                .await
            {
                Ok(ScheduleAnswer::Page { rows, next }) => (rows, next),
                Ok(other) => return Err(unexpected(other)),
                Err(e) if e.is_fenced() => continue 'ranges,
                Err(crate::actor::ActorError::Stopped) => continue 'ranges,
                Err(e) => return Err(ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
            };
            mine.extend(rows);
            if mine.len() > LIST_LIMIT {
                mine.truncate(LIST_LIMIT);
                out.truncated.push(p);
                break;
            }
            match next {
                Some(n) => after = Some(n),
                None => break,
            }
        }
        out.ranges.push(p);
        out.schedules.extend(mine.iter().map(|v| render(v, false)));
    }
    Ok(out)
}

/// One node's part of the per-tenant schedule count.
#[derive(Default, Serialize, Deserialize)]
pub(crate) struct NodeScheduleCounts {
    ranges: Vec<u32>,
    /// Tenant (row form, `""` = `default`) → schedule count.
    counts: std::collections::BTreeMap<String, u64>,
}

/// Counts schedules per tenant on the ranges this node holds, in chunks.
async fn local_schedule_counts(st: &AppState) -> ApiResult<NodeScheduleCounts> {
    let mut out = NodeScheduleCounts::default();
    'ranges: for (p, engine) in st.router.partitions.iter() {
        let mut after = None;
        let mut counts = std::collections::BTreeMap::new();
        loop {
            let (by_tenant, next) = match engine
                .schedule(ScheduleOp::Counts { after, limit: PAGE })
                .await
            {
                Ok(ScheduleAnswer::Counts { by_tenant, next }) => (by_tenant, next),
                Ok(other) => return Err(unexpected(other)),
                Err(e) if e.is_fenced() => continue 'ranges,
                Err(crate::actor::ActorError::Stopped) => continue 'ranges,
                Err(e) => return Err(ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
            };
            for (t, n) in by_tenant {
                *counts.entry(t.unwrap_or_default()).or_insert(0) += n;
            }
            match next {
                Some(n) => after = Some(n),
                None => break,
            }
        }
        out.ranges.push(p);
        for (t, n) in counts {
            *out.counts.entry(t).or_insert(0) += n;
        }
    }
    Ok(out)
}

/// `GET /internal/schedules/counts` — this node's part of the schedule count.
pub(super) async fn internal_schedule_counts(
    State(st): State<AppState>,
) -> ApiResult<Json<NodeScheduleCounts>> {
    Ok(Json(local_schedule_counts(&st).await?))
}

/// Schedules per tenant across the cluster: this node's part plus every other node's
/// part. A range or node not answering ⇒ missing from the total (the soft cap lets it pass).
pub(crate) async fn schedule_counts(
    st: &AppState,
) -> Result<std::collections::BTreeMap<cairn_store::TenantId, u64>, String> {
    let local = local_schedule_counts(st)
        .await
        .map_err(|e| e.public_message())?;
    let fanned =
        observe::fan_out::<NodeScheduleCounts>(st, "/internal/schedules/counts", &[]).await;
    let mut out = std::collections::BTreeMap::new();
    let mut seen: std::collections::BTreeSet<u32> = Default::default();
    for part in std::iter::once(local).chain(fanned.parts.into_iter().map(|(_, p)| p)) {
        // Two nodes may both return a range in the leader change window: take the first.
        let fresh = part.ranges.iter().any(|r| seen.insert(*r));
        if !fresh {
            continue;
        }
        for (t, n) in part.counts {
            let t = if t.is_empty() {
                cairn_store::TenantId::default()
            } else {
                match cairn_store::TenantId::new(&t) {
                    Ok(t) => t,
                    Err(_) => continue,
                }
            };
            *out.entry(t).or_insert(0) += n;
        }
    }
    Ok(out)
}

/// The asking tenant: the key's, or `?tenant=` — another tenant only for `cluster-admin`.
fn list_tenant(
    who: &crate::auth::Identity,
    q: &HashMap<String, String>,
) -> ApiResult<cairn_store::TenantId> {
    match q.get("tenant") {
        None => Ok(who.tenant.clone()),
        Some(t) => {
            let t = cairn_store::TenantId::new(t).map_err(|e| bad(e.to_string()))?;
            if t != who.tenant && !who.all_tenants() {
                return Err(ApiError(
                    StatusCode::FORBIDDEN,
                    "another tenant's schedules need a cluster-admin key".into(),
                ));
            }
            Ok(t)
        }
    }
}

async fn list_inner(
    st: AppState,
    tenant: cairn_store::TenantId,
    scope: Option<Scope>,
) -> ApiResult<Json<serde_json::Value>> {
    // The tenant's read bucket is empty ⇒ `429` before reading anything (like `/archive`).
    if st
        .query()
        .is_some_and(|svc| !svc.spend_read_units(tenant.as_str(), 0))
    {
        return Err(super::archive::read_budget_spent());
    }
    let mut query = vec![("tenant", tenant.to_string())];
    if let Some(s) = &scope {
        query.push(("scope", s.as_str().to_string()));
    }
    let (local, fanned) = futures::join!(
        local_schedules(&st, &tenant, scope.as_ref()),
        observe::fan_out::<NodeSchedules>(&st, "/internal/schedules", &query),
    );
    let local = local?;
    let mut answered: std::collections::BTreeSet<u32> = local.ranges.into_iter().collect();
    let mut truncated: std::collections::BTreeSet<u32> = local.truncated.into_iter().collect();
    let mut rows = local.schedules;
    for (_, part) in fanned.parts {
        answered.extend(part.ranges);
        truncated.extend(part.truncated);
        rows.extend(part.schedules);
    }
    // Two nodes may both return a range during the leader handover window.
    let mut seen = std::collections::BTreeSet::new();
    rows.retain(|r| seen.insert(r["id"].as_str().unwrap_or_default().to_string()));
    rows.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    let missing_ranges: Vec<u32> = st
        .router
        .cluster
        .partitions()
        .iter()
        .map(|p| p.0)
        .filter(|p| !answered.contains(p))
        .collect();
    Ok(Json(serde_json::json!({
        "schedules": rows,
        "partial": !fanned.missing.is_empty()
            || !missing_ranges.is_empty()
            || !truncated.is_empty(),
        "missing": fanned.missing,
        "missingRanges": missing_ranges,
        "truncatedRanges": truncated.into_iter().collect::<Vec<_>>(),
    })))
}

pub(super) async fn list_schedules(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<serde_json::Value>> {
    let tenant = list_tenant(&who, &q)?;
    list_inner(st, tenant, None).await
}

pub(super) async fn scoped_list_schedules(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path(scope): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<serde_json::Value>> {
    let tenant = list_tenant(&who, &q)?;
    list_inner(st, tenant, scoped(Some(scope))?).await
}

/// `GET /internal/schedules?tenant=&scope=` — this node's part.
pub(super) async fn internal_schedules(
    State(st): State<AppState>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<NodeSchedules>> {
    let tenant =
        cairn_store::TenantId::new(q.get("tenant").map(String::as_str).unwrap_or_default())
            .map_err(|e| bad(e.to_string()))?;
    let scope = scoped(q.get("scope").cloned())?;
    Ok(Json(local_schedules(&st, &tenant, scope.as_ref()).await?))
}
