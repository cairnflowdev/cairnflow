// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Admin: API keys, GC fences, cluster-version, failpoints, tenant settings, `/admin/tick`.

use super::*;

// ─────────────────────────────────────────────────────────── API keys

/// An API key as `/admin/keys` returns it — **without** the secret, except at creation.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyView {
    pub id: String,
    pub tenant: String,
    pub role: &'static str,
    pub created_at_ms: u64,
    /// Only present in the response of `POST /admin/keys`. Not stored anywhere — lose it
    /// and you create a new key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret: Option<String>,
}

impl KeyView {
    fn of(k: &crate::auth::KeyRecord, secret: Option<String>) -> Self {
        Self {
            id: k.id.clone(),
            tenant: k.tenant.to_string(),
            role: k.role.as_str(),
            created_at_ms: k.created_ms,
            secret,
        }
    }
}

#[derive(Deserialize)]
pub struct CreateKeyRequest {
    pub tenant: String,
    pub role: String,
}

fn key_error(e: crate::auth::KeyError) -> ApiError {
    match e {
        crate::auth::KeyError::RolesDisabled(_) => key_roles_disabled(e.to_string()),
        crate::auth::KeyError::Store(e) => ApiError(StatusCode::INTERNAL_SERVER_ERROR, e),
        crate::auth::KeyError::Contended => keys_busy(),
    }
}

/// `POST /admin/keys {tenant, role}` (`Provision` permission) — creates a key, returns
/// the secret **once**.
///
/// Escalation guard ([`crate::auth::Role::may_grant`]): a non-`Cluster` key only grants
/// tenant roles (`invoke`, `view`, `admin`) ⇒ `403` for `provision`/`cluster-admin`.
/// `view`/`provision` roles before the `auth/keys-v2` gate ⇒ `409 key_roles_disabled`,
/// the registry unchanged.
pub(super) async fn create_key(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Json(req): Json<CreateKeyRequest>,
) -> ApiResult<Json<KeyView>> {
    let tenant = cairn_store::TenantId::new(&req.tenant)
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, e.to_string()))?;
    let role = crate::auth::Role::parse(&req.role).ok_or_else(|| {
        ApiError(
            StatusCode::BAD_REQUEST,
            format!(
                "role '{}' does not exist — only invoke | view | admin | provision | cluster-admin",
                req.role
            ),
        )
    })?;
    if !who.role.may_grant(role) {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            format!(
                "a `{}` key cannot grant a `{}` key — only invoke | view | admin",
                who.role.as_str(),
                role.as_str()
            ),
        ));
    }
    // A tenant being deleted or deleted: a new key would open nothing, and a `purged`
    // tombstone does not hand its id out again.
    if let Some(settings) = &st.tenant_settings {
        // Cannot re-read (broken registry, I/O): use the copy we hold, like every other
        // state read path.
        if let Err(e) = settings.refresh().await {
            tracing::warn!(error = %e, "create key: cannot re-read the tenants registry");
        }
        let state = settings.flags(&tenant).state();
        if state.refuses_access() {
            let code = if state == cairn_engine::TenantState::Purged {
                super::error::Code::TenantPurged
            } else {
                super::error::Code::TenantDeleting
            };
            return Err(ApiError::coded(
                StatusCode::CONFLICT,
                code,
                format!("tenant {tenant} is {} — not issuing keys", state.as_str()),
            ));
        }
    }
    let (record, secret) = st.auth.keys.create(tenant, role).await.map_err(key_error)?;
    Ok(Json(KeyView::of(&record, Some(secret))))
}

/// `GET /admin/keys` (cluster-admin) — every stored key, no secrets.
pub(super) async fn list_keys(State(st): State<AppState>) -> ApiResult<Json<Vec<KeyView>>> {
    if let Err(e) = st.auth.keys.refresh().await {
        tracing::debug!(error = %e, "cannot re-read auth/keys — returning the in-memory copy");
    }
    Ok(Json(
        st.auth
            .keys
            .list()
            .iter()
            .map(|k| KeyView::of(k, None))
            .collect(),
    ))
}

/// `DELETE /admin/keys/{id}` (`Provision` permission). This node stops accepting it at
/// once. The [`crate::registry_sync::on_change`] layer wraps the handler: `204` only
/// when every live session has confirmed it applied the change, otherwise
/// `202 {pending}`.
///
/// Revocation follows the same rule as granting ([`crate::auth::Role::may_grant`]): a
/// non-`Cluster` key only revokes tenant role keys ⇒ `403` for `provision`/
/// `cluster-admin` keys, the registry unchanged. The check runs on the registry just
/// read inside the CAS loop.
pub(super) async fn delete_key(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path(id): Path<String>,
) -> ApiResult<StatusCode> {
    use crate::auth::Deleted;
    match st
        .auth
        .keys
        .delete_if(&id, |r| who.role.may_grant(r))
        .await
        .map_err(key_error)?
    {
        Deleted::Deleted => Ok(StatusCode::NO_CONTENT),
        Deleted::Absent => Err(ApiError(StatusCode::NOT_FOUND, format!("no API key {id}"))),
        Deleted::Refused(r) => Err(ApiError(
            StatusCode::FORBIDDEN,
            format!(
                "a `{}` key cannot revoke a `{}` key",
                who.role.as_str(),
                r.as_str()
            ),
        )),
    }
}

/// `GET /admin/keys/{id}/revocation` (cluster-admin) — whether key `id` is still
/// accepted on any node, answered **for certain** or stating clearly that it is not yet
/// certain. For a caller that just got `202 {pending}` (or a `DELETE` with an ambiguous
/// outcome — resolved by reading back) to ask again, on any node of the cluster:
///
/// 1. re-read `auth/keys` from the bucket (failure ⇒ `503`, no answer) and apply it to
///    this node; `id` still present ⇒ `revoked: false` — the revocation was not written,
///    call `DELETE` again;
/// 2. gone ⇒ require every live session in one LIST of `sessions/` **after** that read
///    to re-read the registry ([`crate::registry_sync::RegistrySync::confirm_peers`]).
///    Each ack is a registry read after step 1, so it sees `id` gone.
///
/// `confirmed: true` only when the LIST succeeded and no session is `pending`. An id that
/// never existed is also `revoked: true`: the question is "is this id still accepted".
/// Writes nothing, does not bump the registry generation (does not go through the
/// `on_change` layer).
///
/// A node without a live session on the bucket (its session `expired`/`closed` while the
/// process keeps running) is not in the LIST: it still reads the registry every
/// `--deploy-refresh-ms`, and closes requests authenticated by registry keys when it
/// cannot confirm a registry fresher than 2 ticks.
pub(super) async fn key_revocation(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<serde_json::Value>> {
    let t0 = tokio::time::Instant::now();
    let present =
        match tokio::time::timeout(Duration::from_secs(2), st.auth.keys.reload_contains(&id)).await
        {
            Ok(Ok(p)) => p,
            Ok(Err(e)) => {
                return Err(ApiError(
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("cannot re-read auth/keys: {e} — outcome unknown, retry later"),
                ))
            }
            Err(_) => {
                return Err(ApiError(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "re-reading auth/keys exceeded the 2 s limit — outcome unknown, retry later"
                        .into(),
                ))
            }
        };
    if let Some(s) = &st.registry_sync {
        s.confirm(t0);
    }
    if present {
        return Ok(Json(serde_json::json!({
            "id": id,
            "revoked": false,
            "confirmed": false,
            "pending": [],
            "listed": true,
        })));
    }
    let acks = match &st.registry_sync {
        Some(s) => s.confirm_peers().await,
        None => crate::registry_sync::Acks {
            pending: Vec::new(),
            listed: true,
        },
    };
    Ok(Json(serde_json::json!({
        "id": id,
        "revoked": true,
        "confirmed": acks.all(),
        "pending": acks.pending,
        "listed": acks.listed,
    })))
}

// ─────────────────────────────────────────────── GC fences

pub(super) fn gc_admin(st: &AppState) -> ApiResult<&crate::gc::GcAdmin> {
    st.gc.as_ref().ok_or_else(|| {
        ApiError(
            StatusCode::NOT_IMPLEMENTED,
            "node runs without a cluster object store".into(),
        )
    })
}

impl From<crate::gc::GcAdminError> for ApiError {
    fn from(e: crate::gc::GcAdminError) -> Self {
        match e {
            crate::gc::GcAdminError::Bad(m) => ApiError(StatusCode::BAD_REQUEST, m),
            crate::gc::GcAdminError::Store(e) => {
                ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
            }
        }
    }
}

/// `GET /admin/gc/plan` (cluster-admin) — what the D-d round **would** delete if it ran
/// now, and which ranges are blocked by whom (`barrier`, `age`, `reference`), deleting
/// nothing. Runs on any node, even while the D-d round is off: the operator previews
/// before turning it on.
pub(super) async fn gc_plan(State(st): State<AppState>) -> ApiResult<Json<crate::gc::GcPlan>> {
    gc_admin(&st)?
        .plan()
        .await
        .map(Json)
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

/// `GET /auth/whoami` (every key) — who the calling key is, how it authenticated, the
/// node and the version: the console reads it when it opens to learn the tenant, role
/// and session expiry. Reveals no secret. `clusterVersion` is `null` when the node
/// cannot read it (hand-built router, bucket unreachable) — not an error of this
/// question.
///
/// An SSO session adds `user {sub, email, name, client}`. `sso` and `portalUrl` are about
/// **the request's hostname**: whether SSO login is available, and the "switch
/// environment" link of its OIDC client (also when logged in with a key).
pub(super) async fn whoami(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    via: Option<axum::Extension<crate::auth::Via>>,
    req: axum::extract::Request,
) -> Json<serde_json::Value> {
    let via = via.map(|v| v.0).unwrap_or(crate::auth::Via::NoAuth);
    let host_client = st
        .auth
        .request_host(req.headers(), req.extensions())
        .and_then(|h| st.auth.keys.oidc().login_client(&h))
        .map(|(_, c)| c);
    let cluster_version = match &st.gc {
        Some(gc) => cairn_store::migrate::read_cluster_version(
            gc.stores.main().store(),
            gc.stores.main().prefix(),
        )
        .await
        .ok()
        .map(|cv| cv.map(|(cv, _)| cv).unwrap_or_default().version),
        None => None,
    };
    Json(serde_json::json!({
        "tenant": who.tenant.as_str(),
        "role": who.role.as_str(),
        // The permission set: the console hides actions by this, it does not infer it
        // from the role name.
        "capabilities": who.role.caps().names(),
        "keyId": who.key_id,
        // Where the key lives: `bootstrap` (the env's cluster-admin), `env` (another key
        // from the env: `provision`, a metrics token), `registry` (a key in the registry);
        // `null` for SSO sessions and `--no-auth`. `bootstrap` is `true` only for the real
        // bootstrap key — the console reads this field, and the control plane's
        // `provision` key is not cluster-admin.
        "kind": key_kind(&who, via),
        "bootstrap": key_kind(&who, via) == Some("bootstrap"),
        "noAuth": via == crate::auth::Via::NoAuth,
        "via": via.as_str(),
        "node": st.router.node_id,
        "version": env!("CARGO_PKG_VERSION"),
        "clusterVersion": cluster_version,
        "envLabel": st.env_label.as_deref(),
        "sessionExpiresAt": via.expires_at_s().map(|s| s * 1000),
        "user": who.user.as_ref().map(|u| serde_json::json!({
            "sub": u.sub,
            "email": u.email,
            "name": u.name,
            "client": u.client,
        })),
        "sso": st.auth.required && host_client.is_some(),
        "portalUrl": host_client.and_then(|c| c.portal_url),
    }))
}

/// The source of the calling key (`whoami.kind`).
fn key_kind(who: &crate::auth::Identity, via: crate::auth::Via) -> Option<&'static str> {
    if via == crate::auth::Via::NoAuth || who.user.is_some() {
        None
    } else if who.key_id.is_some() {
        Some("registry")
    } else if who.role == crate::auth::Role::ClusterAdmin {
        Some("bootstrap")
    } else {
        Some("env")
    }
}

/// `GET /admin/cluster-version` (cluster-admin) — the cluster version, the migration
/// runner lock if any, this binary's migration list, and the number of steps the binary
/// of each live session knows (`knows=`).
///
/// `requiresKnows` = `max(version, minBinary)`: a binary knowing fewer steps than that
/// can no longer start on this cluster. Rolling back to an old binary is only possible
/// while that number is still ≤ its `knows` — that is, **before** `version` is bumped.
pub(super) async fn cluster_version(
    State(st): State<AppState>,
) -> ApiResult<Json<serde_json::Value>> {
    let stores = &gc_admin(&st)?.stores;
    let (store, prefix) = (stores.main().store(), stores.main().prefix());
    let err =
        |e: cairn_store::StoreError| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
    let cv = cairn_store::migrate::read_cluster_version(store, prefix)
        .await
        .map_err(err)?
        .map(|(cv, _)| cv)
        .unwrap_or_default();
    let nodes = cairn_store::migrate::live_binaries(store, prefix)
        .await
        .map_err(err)?;
    let ledger: Vec<serde_json::Value> = cairn_store::migrate::MIGRATIONS
        .iter()
        .map(|m| {
            serde_json::json!({
                "to": m.to,
                "name": m.name,
                "tier": match m.tier() {
                    cairn_store::migrate::Tier::Cluster => "cluster",
                    cairn_store::migrate::Tier::RangeDb => "range-db",
                    cairn_store::migrate::Tier::Range => "range",
                    cairn_store::migrate::Tier::Gate => "gate",
                },
            })
        })
        .collect();
    Ok(Json(serde_json::json!({
        "version": cv.version,
        "minBinary": cv.min_binary,
        "requiresKnows": cv.version.max(cv.min_binary),
        "callPrefix": cv.call_prefix_on(),
        "callPlacement": cv.call_placement_on(),
        "streams": cv.streams_on(),
        "schedules": cv.schedules_on(),
        "keyRoles": cv.key_roles_on(),
        "tenantState": cv.tenant_state_on(),
        "audit": cv.audit_on(),
        "tenantPurge": cv.tenant_purge_on(),
        "values": cv.values_on(),
        "upgrading": cv.upgrading.as_ref().map(|u| serde_json::json!({
            "from": u.from,
            "to": u.to,
            "by": u.by.to_string(),
        })),
        "binary": { "knows": cairn_store::migrate::KNOWS, "ledger": ledger },
        "nodes": nodes
            .iter()
            .map(|(id, knows, age)| serde_json::json!({ "node": id.node, "inc": id.inc, "knows": knows, "ageMs": age }))
            .collect::<Vec<_>>(),
    })))
}

/// `GET /admin/gc/barriers` (cluster-admin) — every fence in the bucket, including
/// expired fences (waiting for the D-d runner to delete them) and unreadable fences
/// (blocking every range).
pub(super) async fn list_gc_barriers(
    State(st): State<AppState>,
) -> ApiResult<Json<serde_json::Value>> {
    let barriers = gc_admin(&st)?
        .list_barriers()
        .await
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(
        serde_json::json!({ "barriers": barriers, "nowMs": crate::gc::wall_ms() }),
    ))
}

/// Cap on the reports `GET /admin/ops-reports` returns — `cairn-ctl` keeps
/// [`cairn_store::ops_report::KEEP_COUNT`], so the cap is only hit when cleanup fails.
const OPS_REPORTS_LIST_CAP: usize = 1000;

/// `GET /admin/ops-reports` (`Provision` permission) — the `cairn-ctl …
/// --report-to-bucket` reports in the bucket, newest first: id, the store's
/// `last_modified` mark, size. The content is read one by one at `/{id}`.
pub(super) async fn list_ops_reports(
    State(st): State<AppState>,
) -> ApiResult<Json<serde_json::Value>> {
    let stores = &gc_admin(&st)?.stores;
    let all = cairn_store::ops_report::list(stores.main().store(), stores.main().prefix())
        .await
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let reports: Vec<serde_json::Value> = all
        .iter()
        .take(OPS_REPORTS_LIST_CAP)
        .map(|m| {
            serde_json::json!({
                "id": m.id,
                "lastModifiedMs": m.last_modified_ms,
                "sizeBytes": m.size,
            })
        })
        .collect();
    Ok(Json(serde_json::json!({
        "reports": reports,
        "truncated": all.len() > OPS_REPORTS_LIST_CAP,
    })))
}

/// `GET /admin/ops-reports/{id}` (`Provision` permission) — one report: `{id, header,
/// report}`, `report` being the JSON `cairn-ctl` wrote, verbatim (lenient read — unknown
/// keys pass through). Missing ⇒ `404` (the command has not finished, or not run);
/// malformed id ⇒ `400`; an unreadable object ⇒ `500` with the reason, no guessing.
pub(super) async fn get_ops_report(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<serde_json::Value>> {
    cairn_store::ops_report::check_id(&id).map_err(|e| ApiError(StatusCode::BAD_REQUEST, e))?;
    let stores = &gc_admin(&st)?.stores;
    let broken = |why: String| {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("report ops-reports/{id} is unreadable: {why}"),
        )
    };
    let (header, body) =
        cairn_store::ops_report::read(stores.main().store(), stores.main().prefix(), &id)
            .await
            .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .ok_or_else(|| {
                ApiError(
                    StatusCode::NOT_FOUND,
                    format!("no report ops-reports/{id} yet"),
                )
            })?
            .map_err(broken)?;
    let report: serde_json::Value =
        serde_json::from_slice(&body).map_err(|e| broken(format!("not JSON: {e}")))?;
    Ok(Json(serde_json::json!({
        "id": id,
        "header": header,
        "report": report,
    })))
}

/// `GET /admin/gc/barriers/{id}` (cluster-admin).
pub(super) async fn get_gc_barrier(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<crate::gc::BarrierView>> {
    gc_admin(&st)?
        .get_barrier(&id)
        .await?
        .map(Json)
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, format!("no fence {id:?}")))
}

/// `PUT /admin/gc/barriers/{id} {holder, protects, ttlMs, reason}` (cluster-admin) —
/// sets or extends a fence; `ttlMs: 0` removes it. The TTL is mandatory and capped: no
/// infinite fences. See [`crate::gc::GcAdmin::put_barrier`].
pub(super) async fn put_gc_barrier(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<crate::gc::BarrierRequest>,
) -> ApiResult<Json<serde_json::Value>> {
    Ok(Json(gc_admin(&st)?.put_barrier(&id, req).await?))
}

// ─────────────────────────────────────────────────────────── failpoint

#[cfg(feature = "failpoints")]
#[derive(Deserialize)]
pub struct FailpointRequest {
    /// A name in [`cairn_store::failpoint::NAMES`].
    pub name: String,
    /// `fail::cfg` syntax: `return(503)`, `1*return(x)`, `sleep(200)`,
    /// `50%return->off`, `panic`, … `off` or empty ⇒ remove the point.
    pub action: String,
}

/// `POST /admin/failpoints {name, action}` (cluster-admin, only when built with
/// `--features failpoints`) — turns a failpoint on/off **on this node**. The chaos
/// harness uses it to inject faults into a real process.
///
/// An unknown name ⇒ `400`: a typo must not silently become "turned on a point nobody
/// calls". `pause` is not accepted — an engine thread paused forever in a real process
/// cannot be released by anyone over HTTP; use `sleep(ms)`.
#[cfg(feature = "failpoints")]
pub(super) async fn set_failpoint(
    Json(req): Json<FailpointRequest>,
) -> ApiResult<Json<serde_json::Value>> {
    if !cairn_store::failpoint::NAMES.contains(&req.name.as_str()) {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            format!(
                "unknown failpoint '{}' — see GET /admin/failpoints",
                req.name
            ),
        ));
    }
    let action = req.action.trim();
    if action.is_empty() || action == "off" {
        cairn_store::__fail::remove(&req.name);
    } else {
        // `[p%][cnt*]task[(arg)]`: drop the frequency and count, look at `task`.
        let task = |a: &str| {
            a.trim()
                .trim_start_matches(|c: char| c.is_ascii_digit() || matches!(c, '%' | '*' | '.'))
                .split('(')
                .next()
                .unwrap_or_default()
                .to_string()
        };
        if action.split("->").any(|a| task(a) == "pause") {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                "`pause` is not accepted over HTTP — use sleep(ms)".into(),
            ));
        }
        cairn_store::__fail::cfg(&req.name, action)
            .map_err(|e| ApiError(StatusCode::BAD_REQUEST, format!("malformed action: {e}")))?;
    }
    tracing::warn!(failpoint = %req.name, action = %action, "failpoint changed");
    Ok(Json(
        serde_json::json!({ "name": req.name, "action": action }),
    ))
}

/// `GET /admin/failpoints` (cluster-admin) — the points enabled on this node and every
/// settable name.
#[cfg(feature = "failpoints")]
pub(super) async fn list_failpoints() -> Json<serde_json::Value> {
    let mut active: Vec<(String, String)> = cairn_store::__fail::list()
        .into_iter()
        .filter(|(_, a)| !a.is_empty())
        .collect();
    active.sort();
    Json(serde_json::json!({
        "active": active
            .into_iter()
            .map(|(name, action)| serde_json::json!({ "name": name, "action": action }))
            .collect::<Vec<_>>(),
        "names": cairn_store::failpoint::NAMES,
    }))
}

/// A cap field of [`TenantSettingsBody`]: **present** ⇒ `Some`, including `null`
/// (= remove the cap). A plain `Option<Value>` reads `null` as `None` — "keep as is" —
/// so `null` could not remove anything (a bug of an earlier version).
fn present<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<serde_json::Value>, D::Error> {
    serde_json::Value::deserialize(d).map(Some)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TenantSettingsBody {
    /// Absent = keep as is.
    #[serde(default)]
    pub hash_chain: Option<bool>,
    /// The tenant's own slow log threshold, ms. `0` = back to the node default; absent =
    /// keep as is.
    #[serde(default)]
    pub slow_ms: Option<u64>,
    /// The tenant's own poison handler quarantine mode: `off|observe|cooldown|hold`;
    /// `""` or `"default"` = back to the node default; absent = keep as is.
    #[serde(default)]
    pub quarantine: Option<String>,
    /// The tenant's own quarantine rule `attempts/invocations/seconds`, e.g. `20/5/300`;
    /// `""` or `"default"` = back to the node default; absent = keep as is.
    #[serde(default)]
    pub quarantine_rule: Option<String>,
    /// Per-tenant caps: absent = keep as is, `null` = remove the cap.
    /// `retentionMs`: ms, `0` = keep forever; overrides `--retention-ms`, loses to the
    /// handler's `journalRetention`.
    #[serde(default, deserialize_with = "present")]
    pub retention_ms: Option<serde_json::Value>,
    /// Maximum bytes of a `/invoke`/`/send` body and of an `Output`/`SetState` frame, ≥ 1.
    #[serde(default, deserialize_with = "present")]
    pub max_payload: Option<serde_json::Value>,
    /// Maximum journal entries per invocation (including `Input`), ≥ 1.
    #[serde(default, deserialize_with = "present")]
    pub max_journal: Option<serde_json::Value>,
    /// `"N/S"`: at most N `POST /deployments` in S seconds.
    #[serde(default, deserialize_with = "present")]
    pub deploy_rate: Option<serde_json::Value>,
    /// Resource caps, each a number ≥ 1; absent = keep, `null` = remove.
    /// Live invocations of the tenant (soft cap).
    #[serde(default, deserialize_with = "present")]
    pub max_live: Option<serde_json::Value>,
    /// The tenant's deployment count.
    #[serde(default, deserialize_with = "present")]
    pub max_deployments: Option<serde_json::Value>,
    /// The tenant's schedule count.
    #[serde(default, deserialize_with = "present")]
    pub max_schedules: Option<serde_json::Value>,
    /// Pending timers per invocation.
    #[serde(default, deserialize_with = "present")]
    pub max_timers: Option<serde_json::Value>,
    /// User streams per invocation.
    #[serde(default, deserialize_with = "present")]
    pub max_streams: Option<serde_json::Value>,
    /// State keys per object.
    #[serde(default, deserialize_with = "present")]
    pub max_state_keys: Option<serde_json::Value>,
    /// Stream RAM bytes of the tenant on each node (security review).
    #[serde(default, deserialize_with = "present")]
    pub max_stream_ram: Option<serde_json::Value>,
    /// Total artifact bytes of the tenant.
    #[serde(default, deserialize_with = "present")]
    pub max_artifact_bytes: Option<serde_json::Value>,
    /// Total journal bytes per invocation: over ⇒ `paused` 580.
    #[serde(default, deserialize_with = "present")]
    pub max_journal_bytes: Option<serde_json::Value>,
    /// Maximum logical bytes of one value: over ⇒ `failed` 579 `value_bytes_limit`.
    #[serde(default, deserialize_with = "present")]
    pub max_value_bytes: Option<serde_json::Value>,
}

/// One cap field of [`TenantSettingsBody`]: absent ⇒ `None` (keep), `null` ⇒
/// `Some(None)` (remove), a value ⇒ `Some(Some(_))`; an invalid value ⇒ `400` — the
/// registry never accepts a mistyped cap through the API (fail closed).
fn limit_field<T>(
    name: &str,
    v: &Option<serde_json::Value>,
    parse: impl Fn(&serde_json::Value) -> Option<T>,
    expect: &str,
) -> Result<Option<Option<T>>, ApiError> {
    match v {
        None => Ok(None),
        Some(serde_json::Value::Null) => Ok(Some(None)),
        Some(x) => parse(x).map(|t| Some(Some(t))).ok_or_else(|| {
            ApiError(
                StatusCode::BAD_REQUEST,
                format!("{name} {x} is invalid: {expect}"),
            )
        }),
    }
}

/// `PUT /admin/tenants/{tenant} {"hashChain"?: bool, "slowMs"?: n,
/// "quarantine"?: s, "quarantineRule"?: s}` (cluster-admin) — turns the tamper-evident
/// hash chain on/off for a tenant, sets its own slow log threshold, and the poison
/// handler quarantine mode and rule. The hash chain applies to invocations **created
/// after** this command; existing invocations stay as they are. The threshold applies at
/// once on this node, on other nodes after one refresh tick.
pub(super) async fn put_tenant_settings(
    State(st): State<AppState>,
    Path(tenant): Path<String>,
    Json(body): Json<TenantSettingsBody>,
) -> ApiResult<Json<serde_json::Value>> {
    let tenant = cairn_store::TenantId::new(&tenant)
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, e.to_string()))?;
    let settings = st.tenant_settings.as_ref().ok_or_else(|| {
        ApiError(
            StatusCode::NOT_IMPLEMENTED,
            "this node has no tenant settings store".into(),
        )
    })?;
    if body.hash_chain.is_none()
        && body.slow_ms.is_none()
        && body.quarantine.is_none()
        && body.quarantine_rule.is_none()
        && body.retention_ms.is_none()
        && body.max_payload.is_none()
        && body.max_journal.is_none()
        && body.deploy_rate.is_none()
        && body.max_live.is_none()
        && body.max_deployments.is_none()
        && body.max_schedules.is_none()
        && body.max_timers.is_none()
        && body.max_streams.is_none()
        && body.max_state_keys.is_none()
        && body.max_stream_ram.is_none()
        && body.max_artifact_bytes.is_none()
        && body.max_journal_bytes.is_none()
        && body.max_value_bytes.is_none()
    {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "nothing to change: send hashChain, slowMs, quarantine, quarantineRule, retentionMs, maxPayload, maxJournal, deployRate, maxLive, maxDeployments, maxSchedules, maxTimers, maxStreams, maxStreamRam, maxStateKeys, maxArtifactBytes, maxJournalBytes and/or maxValueBytes"
                .into(),
        ));
    }
    use crate::tenants::{parse_max_journal, parse_max_payload, parse_retention, DeployRate};
    let num = |x: &serde_json::Value| x.as_u64().map(|n| n.to_string());
    let retention = limit_field(
        "retentionMs",
        &body.retention_ms,
        |x| num(x).and_then(|v| parse_retention(&v)),
        "a number of ms, 0 = keep forever",
    )?;
    let max_payload = limit_field(
        "maxPayload",
        &body.max_payload,
        |x| num(x).and_then(|v| parse_max_payload(&v)),
        "a number of bytes ≥ 1",
    )?;
    let max_journal = limit_field(
        "maxJournal",
        &body.max_journal,
        |x| num(x).and_then(|v| parse_max_journal(&v)),
        "a number of entries ≥ 1",
    )?;
    use crate::tenants::{parse_count_u32, parse_count_u64};
    let u64_field = |name: &str, v: &Option<serde_json::Value>| {
        limit_field(
            name,
            v,
            |x| num(x).and_then(|v| parse_count_u64(&v)),
            "a number ≥ 1",
        )
    };
    let u32_field = |name: &str, v: &Option<serde_json::Value>| {
        limit_field(
            name,
            v,
            |x| num(x).and_then(|v| parse_count_u32(&v)),
            "a number ≥ 1",
        )
    };
    let max_live = u64_field("maxLive", &body.max_live)?;
    let max_deployments = u32_field("maxDeployments", &body.max_deployments)?;
    let max_schedules = u32_field("maxSchedules", &body.max_schedules)?;
    let max_timers = u32_field("maxTimers", &body.max_timers)?;
    let max_streams = u32_field("maxStreams", &body.max_streams)?;
    let max_state_keys = u32_field("maxStateKeys", &body.max_state_keys)?;
    let max_stream_ram = u64_field("maxStreamRam", &body.max_stream_ram)?;
    let max_artifact_bytes = u64_field("maxArtifactBytes", &body.max_artifact_bytes)?;
    let max_journal_bytes = u64_field("maxJournalBytes", &body.max_journal_bytes)?;
    let max_value_bytes = u64_field("maxValueBytes", &body.max_value_bytes)?;
    let deploy_rate = limit_field(
        "deployRate",
        &body.deploy_rate,
        |x| x.as_str().and_then(DeployRate::parse),
        "\"N/S\", N times per S seconds, both ≥ 1",
    )?;
    let reset = |v: &str| v.is_empty() || v == "default";
    let mode = match body.quarantine.as_deref() {
        None => None,
        Some(v) if reset(v) => Some(None),
        Some(v) => Some(Some(crate::quarantine::Mode::parse(v).ok_or_else(
            || {
                ApiError(
                    StatusCode::BAD_REQUEST,
                    format!("quarantine {v:?} is invalid: off, observe, cooldown, hold or default"),
                )
            },
        )?)),
    };
    let rule = match body.quarantine_rule.as_deref() {
        None => None,
        Some(v) if reset(v) => Some(None),
        Some(v) => Some(Some(crate::quarantine::Rule::parse(v).ok_or_else(|| {
            ApiError(
                StatusCode::BAD_REQUEST,
                format!("quarantineRule {v:?} is invalid: attempts/invocations/seconds, e.g. 20/5/300"),
            )
        })?)),
    };
    settings
        .update(&tenant, |f| {
            if let Some(on) = body.hash_chain {
                f.hash_chain = on;
            }
            if let Some(ms) = body.slow_ms {
                f.slow_ms = (ms > 0).then_some(ms);
            }
            if let Some(m) = mode {
                f.quarantine = m;
            }
            if let Some(r) = rule {
                f.quarantine_rule = r;
            }
            if let Some(v) = retention {
                f.limits.retention_ms = v;
            }
            if let Some(v) = max_payload {
                f.limits.max_payload = v;
            }
            if let Some(v) = max_journal {
                f.limits.max_journal = v;
            }
            if let Some(v) = deploy_rate {
                f.deploy_rate = v;
            }
            let l = &mut f.limits;
            if let Some(v) = max_live {
                l.max_live = v;
            }
            if let Some(v) = max_deployments {
                l.max_deployments = v;
            }
            if let Some(v) = max_schedules {
                l.max_schedules = v;
            }
            if let Some(v) = max_timers {
                l.max_timers = v;
            }
            if let Some(v) = max_streams {
                l.max_streams = v;
            }
            if let Some(v) = max_state_keys {
                l.max_state_keys = v;
            }
            if let Some(v) = max_stream_ram {
                l.max_stream_ram = v;
            }
            if let Some(v) = max_artifact_bytes {
                l.max_artifact_bytes = v;
            }
            if let Some(v) = max_journal_bytes {
                l.max_journal_bytes = v;
            }
            if let Some(v) = max_value_bytes {
                l.max_value_bytes = v;
            }
        })
        .await
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    Ok(Json(serde_json::json!({
        "tenant": tenant.to_string(),
        "hashChain": settings.hash_chain(&tenant),
        "slowMs": settings.slow_overrides().get(tenant.as_str()),
        "quarantine": settings.quarantine(tenant.as_str()).0.map(|m| m.as_str()),
        "quarantineRule": settings.quarantine(tenant.as_str()).1.map(|r| r.encode()),
        "limits": limits_json(&settings.flags(&tenant)),
    })))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantStateBody {
    /// `active` | `limited` | `suspended` | `deleting`.
    pub state: String,
}

/// `PUT /admin/tenants/{tenant}/state {"state": "active"|"limited"|"suspended"}`
/// (`Provision`).
///
/// - `limited`: the tenant's **new** work is refused (`429 tenant_limited`), schedules
///   do not fire; work already running runs to its outcome.
/// - `suspended`: like `limited` (`403 tenant_suspended`), plus the scheduler grants no
///   slot — the engine does not call out to the tenant's endpoint; data and positions
///   are kept, and back at `active` it continues within one scheduled tick.
///
/// Returns **`204`** only when every live session has re-read the registry (the
/// `registry_sync::on_change` layer, like key revocation), `202 {pending}` otherwise —
/// the caller reads `202` as "not yet in effect everywhere" and sends the same command
/// again (idempotent). Before the `tenants/state` gate ⇒ `409 tenant_state_disabled`: an
/// old binary ignores `state=`, so the state cannot be promised.
///
/// `deleting`: phase A of the purge — every key, cookie and SSO session of the tenant ⇒
/// `403 tenant_deleting`, `deleting-ms=` recorded; the runner's purge loop really
/// deletes after 7 days. Before the `tenants/purge` gate ⇒ `409 tenant_purge_disabled`;
/// the `default` tenant ⇒ `400`. It can be reversed (`suspended`, `limited`, `active`)
/// until the registry has `purging-ms=`, after that ⇒ `409 tenant_purging`; a `purged`
/// tombstone ⇒ `409 tenant_purged` for every target.
pub(super) async fn put_tenant_state(
    State(st): State<AppState>,
    Path(tenant): Path<String>,
    Json(body): Json<TenantStateBody>,
) -> ApiResult<StatusCode> {
    use cairn_engine::TenantState as S;
    let tenant = cairn_store::TenantId::new(&tenant)
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, e.to_string()))?;
    let state = match body.state.as_str() {
        "active" => S::Active,
        "limited" => S::Limited,
        "suspended" => S::Suspended,
        "deleting" => S::Deleting,
        v => {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                format!("state {v:?} is invalid: active, limited, suspended or deleting"),
            ))
        }
    };
    if state == S::Deleting && tenant == cairn_store::TenantId::default() {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "tenant default cannot be deleted".into(),
        ));
    }
    let settings = st.tenant_settings.as_ref().ok_or_else(|| {
        ApiError(
            StatusCode::NOT_IMPLEMENTED,
            "this node has no tenant settings store".into(),
        )
    })?;
    if !settings
        .state_enabled()
        .await
        .map_err(|e| ApiError(StatusCode::SERVICE_UNAVAILABLE, e))?
    {
        return Err(ApiError::coded(
            StatusCode::CONFLICT,
            super::error::Code::TenantStateDisabled,
            "gate tenants/state is not enabled — old binaries ignore tenant state; run `cairn-ctl cluster enable-tenant-state --apply` once every node is up"
                .into(),
        ));
    }
    if state == S::Deleting
        && !settings
            .purge_enabled()
            .await
            .map_err(|e| ApiError(StatusCode::SERVICE_UNAVAILABLE, e))?
    {
        return Err(ApiError::coded(
            StatusCode::CONFLICT,
            super::error::Code::TenantPurgeDisabled,
            "gate tenants/purge is not enabled — old binaries read deleting as suspended (reads not blocked, rows not deleted); run `cairn-ctl cluster enable-tenant-purge --apply` once every node is up"
                .into(),
        ));
    }
    // The `deleting-ms` mark carries this node's clock: a node slow by Δ shortens the
    // reversal window by Δ. Check the clock against the store like the gate of the
    // delete loop; skew over the cap or not measurable ⇒ `503` (when in doubt, refuse).
    if state == S::Deleting {
        if let Some(p) = st.tenant_purge() {
            let limit = crate::gc::clock_skew_limit(p.keep);
            match crate::gc::measure_clock_skew(&p.stores).await {
                Ok(skew) if skew <= limit => {}
                Ok(skew) => {
                    return Err(ApiError(
                        StatusCode::SERVICE_UNAVAILABLE,
                        format!(
                            "this node's clock is {} ms off the store clock (limit {} ms) — a deleting mark stamped with it would shift the restore window; fix the node clock or send the request to another node",
                            skew.as_millis(),
                            limit.as_millis()
                        ),
                    ))
                }
                Err(e) => {
                    return Err(ApiError(
                        StatusCode::SERVICE_UNAVAILABLE,
                        format!(
                            "cannot check this node's clock against the store ({e}) — the deleting mark is not written; retry"
                        ),
                    ))
                }
            }
        }
    }
    let now_ms = cairn_store::entropy::now_ms();
    let refused = settings
        .try_update(&tenant, |f| {
            f.change_state(&tenant, state, now_ms).map(|_| ())
        })
        .await
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    if let Err(why) = refused {
        use crate::tenants::StateRefusal as R;
        return Err(match why {
            R::Default => ApiError(
                StatusCode::BAD_REQUEST,
                "tenant default cannot be deleted".into(),
            ),
            R::Purging => ApiError::coded(
                StatusCode::CONFLICT,
                super::error::Code::TenantPurging,
                format!(
                    "tenant {tenant} is past its purging-ms mark — its data is being deleted for real, this cannot be undone"
                ),
            ),
            R::Purged => ApiError::coded(
                StatusCode::CONFLICT,
                super::error::Code::TenantPurged,
                format!("tenant {tenant} was purged — the id is not reused"),
            ),
        });
    }
    tracing::info!(tenant = %tenant, state = state.as_str(), "tenant state changed");
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /admin/tenants/{tenant}/purge` (`Provision`) — purge progress, read fresh from
/// the bucket: the state and the registry's three marks, the prefixes being held in
/// place, the objects left under each prefix (cap 1000), and the latest round if this
/// node is the runner. No run data. Bounded on the server
/// ([`crate::tenant_purge::STATUS_BOUND`]): classifying the trash has its own cap, the
/// part not yet classified is reported through `heldComplete`/`heldUnchecked`; over the
/// cap for the whole visit ⇒ `503`.
pub(super) async fn get_tenant_purge(
    State(st): State<AppState>,
    Path(tenant): Path<String>,
) -> ApiResult<Json<serde_json::Value>> {
    let tenant = cairn_store::TenantId::new(&tenant)
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, e.to_string()))?;
    let purger = st.tenant_purge().ok_or_else(|| {
        ApiError(
            StatusCode::NOT_IMPLEMENTED,
            "this node has no tenant purge loop".into(),
        )
    })?;
    let bound = crate::tenant_purge::STATUS_BOUND;
    let v = cairn_store::retry::deadline(
        &cairn_store::retry::ops::TENANT_PURGE_STATUS,
        bound,
        purger.status(&tenant),
    )
    .await
    .map_err(|_| {
        ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "tenant purge status did not finish within {} s — retry",
                bound.as_secs()
            ),
        )
    })?
    .map_err(|e| ApiError(StatusCode::SERVICE_UNAVAILABLE, e.to_string()))?;
    Ok(Json(v))
}

/// Per-tenant caps in API form (`GET /tenant`, `/admin/tenants`): `null` = no cap.
pub(super) fn limits_json(f: &crate::tenants::TenantFlags) -> serde_json::Value {
    serde_json::json!({
        "retentionMs": f.limits.retention_ms,
        "maxPayload": f.limits.max_payload,
        "maxJournal": f.limits.max_journal,
        "deployRate": f.deploy_rate.map(|r| r.encode()),
        "maxLive": f.limits.max_live,
        "maxDeployments": f.limits.max_deployments,
        "maxSchedules": f.limits.max_schedules,
        "maxTimers": f.limits.max_timers,
        "maxStreams": f.limits.max_streams,
        "maxStateKeys": f.limits.max_state_keys,
        "maxStreamRam": f.limits.max_stream_ram,
        "maxArtifactBytes": f.limits.max_artifact_bytes,
        "maxJournalBytes": f.limits.max_journal_bytes,
        "maxValueBytes": f.limits.max_value_bytes,
    })
}

/// `GET /tenant` (Admin) — the limits of the key's **own** tenant. Only cluster-admin can
/// change them (`PUT /admin/tenants/{t}`, through the control plane's reconciler).
pub(super) async fn get_tenant(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
) -> ApiResult<Json<serde_json::Value>> {
    // The **effective** caps: an absent field takes the node default (SaaS profile).
    let flags = st
        .tenant_settings
        .as_ref()
        .map(|s| {
            let mut f = s.flags(&who.tenant);
            f.limits = s.limits_of(&who.tenant);
            f
        })
        .unwrap_or_default();
    // The background loop's counts — only present when the tenant has a cap using them;
    // up to one counting tick late.
    let usage = st
        .tenant_settings
        .as_ref()
        .map(|s| s.limits().snapshot().usage(&who.tenant))
        .unwrap_or_default();
    Ok(Json(serde_json::json!({
        "tenant": who.tenant.to_string(),
        "state": flags.state().as_str(),
        "limits": limits_json(&flags),
        "usage": { "live": usage.live, "artifactBytes": usage.artifact_bytes },
    })))
}

/// `GET /admin/tenants` (cluster-admin, `provision`) — tenants with the hash chain on.
///
/// Quarantine rules (`quarantineRule`, `quarantine.*.rule`) are only returned to keys
/// that see every tenant (`Cluster`): a rule may carry the customer's service/handler
/// names, and `/rules` hides patterns from `provision` too.
pub(super) async fn list_tenant_settings(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
) -> ApiResult<Json<serde_json::Value>> {
    let show_rules = who.all_tenants();
    let settings = st.tenant_settings.as_ref().ok_or_else(|| {
        ApiError(
            StatusCode::NOT_IMPLEMENTED,
            "this node has no tenant settings store".into(),
        )
    })?;
    settings
        .refresh()
        .await
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    let mut chained: Vec<String> = settings
        .chained()
        .snapshot()
        .into_iter()
        .map(|t| t.to_string())
        .collect();
    chained.sort();
    let quarantine: serde_json::Map<String, serde_json::Value> = settings
        .quarantine_overrides()
        .into_iter()
        .map(|(t, m, r)| {
            (
                t,
                serde_json::json!({
                    "mode": m.map(|m| m.as_str()),
                    "rule": r.filter(|_| show_rules).map(|r| r.encode()),
                }),
            )
        })
        .collect();
    // One row per tenant with its own settings (`sys_tenant`) — the same data as the
    // three tables above, in table form.
    let slow = settings.slow_overrides();
    let mut names: std::collections::BTreeSet<String> = chained.iter().cloned().collect();
    names.extend(quarantine.keys().cloned());
    if let Some(m) = serde_json::to_value(&slow)
        .ok()
        .and_then(|v| v.as_object().cloned())
    {
        names.extend(m.keys().cloned());
    }
    let slow_json = serde_json::to_value(&slow).unwrap_or_default();
    let tenants: Vec<serde_json::Value> = names
        .into_iter()
        .map(|t| {
            serde_json::json!({
                "tenant": t,
                "hashChain": chained.contains(&t),
                "slowMs": slow_json.get(&t),
                "quarantineMode": quarantine.get(&t).and_then(|q| q.get("mode")),
                "quarantineRule": quarantine.get(&t).and_then(|q| q.get("rule")),
            })
        })
        .collect();
    let limits: serde_json::Map<String, serde_json::Value> = settings
        .limited()
        .into_iter()
        .map(|(t, f)| (t.to_string(), limits_json(&f)))
        .collect();
    Ok(Json(serde_json::json!({
        "hashChain": chained,
        "slowMs": slow,
        "quarantine": quarantine,
        "tenants": tenants,
        "limits": limits,
        "states": settings.states(),
    })))
}

// ─────────────────────────────────────────────────────────── timer

/// Lets due timers fire.
///
/// Exposed as an endpoint rather than run in the background: tests need to control time,
/// and a `sleep(1s)` loop would make every test with `ctx.sleep` really wait. The
/// background loop calls this same function.
pub(super) async fn tick(
    State(st): State<AppState>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<TickResponse>> {
    let now_ms = match q.get("now_ms") {
        Some(v) => v
            .parse()
            .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "now_ms is not a number".into()))?,
        None => cairn_store::entropy::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0),
    };
    if st.registry.is_empty() {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "no deployment registered yet".into(),
        ));
    }

    // A tick for **every** range of the table this node leads. A range with a leader on
    // another node returns `NotLeader`, and that is not an error — that node ticks it.
    //
    // **A changing table must not swallow the tick** (split/merge): a quiesced parent
    // range returns `NotLeader`, a child without an engine yet or not yet leader — the
    // tick would then "finish" with `advanced: 0` while nobody fires the due timers. So:
    // wait (bounded) until the table has no intent and every range in it has a leader
    // either here or on another node; cap reached ⇒ `503`.
    let deadline = tokio::time::Instant::now() + TICK_SETTLE;
    let mut changes = st.router.cluster.subscribe();
    // Summed across rounds: a range ticked in an earlier round (its timers already fired)
    // does nothing when ticked again in a later round.
    let mut advanced = 0;
    loop {
        let table = st.router.cluster.table();
        let mut this_pass = 0;
        let mut settled = table.ranges().iter().all(|r| r.intent.is_none());
        if settled {
            for r in table.ranges() {
                let Some(engine) = st.router.partitions.get(r.id) else {
                    settled = false;
                    break;
                };
                if engine.is_retiring() {
                    settled = false;
                    break;
                }
                match engine.tick(now_ms).await {
                    Ok(n) => {
                        advanced += n;
                        this_pass += n;
                    }
                    // A range nobody leads yet (just created, being claimed) — wait;
                    // a leader on another node — that node's business.
                    Err(e) if e.is_fenced() => {
                        if st.router.leader_pending(r.id) {
                            settled = false;
                        }
                    }
                    Err(
                        crate::actor::ActorError::WrongRange | crate::actor::ActorError::Stopped,
                    ) => {
                        settled = false;
                    }
                    Err(e) => return Err(ApiError(StatusCode::BAD_GATEWAY, e.to_string())),
                }
            }
        }
        if settled && st.router.cluster.version() == table.version() {
            // The tick **hands** timer-firing turns to the scheduler and returns; a range
            // that starts splitting right after quiesces and cancels turns still waiting
            // for a slot — the timer rows stay durable, but only fire again on the
            // background tick by **real** time: a tick with `now_ms` in the future
            // (tests, operators fast-forwarding time) would lose its effect. So when a
            // tick fired something, watch the table a little longer: changed ⇒ tick again.
            if this_pass == 0 || !table_changes_within(&st, table.version(), TICK_GUARD).await {
                break;
            }
            continue;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "range table is changing (split/merge, range without a leader) — the tick did not complete, retry"
                    .into(),
            ));
        }
        st.router.cluster.request_reload();
        let _ = tokio::time::timeout(Duration::from_millis(50), changes.changed()).await;
    }
    crate::metrics::Metrics::bump(&st.metrics.ticks);
    crate::metrics::Metrics::add(&st.metrics.tick_advanced, advanced as u64);
    Ok(Json(TickResponse { advanced }))
}

/// Cap on waiting for the range table to settle in `/admin/tick` — see the comment in [`tick`].
pub(super) const TICK_SETTLE: Duration = Duration::from_secs(10);

/// After a tick that fired something: watch the range table this much longer — see [`tick`].
pub(super) const TICK_GUARD: Duration = Duration::from_millis(300);

/// Whether the range table changes away from `version` within `within`.
pub(super) async fn table_changes_within(st: &AppState, version: u64, within: Duration) -> bool {
    let mut changes = st.router.cluster.subscribe();
    let until = tokio::time::Instant::now() + within;
    while st.router.cluster.version() == version {
        match tokio::time::timeout_at(until, changes.changed()).await {
            Ok(Ok(())) => {}
            // Time is up, or nobody updates the table any more.
            _ => return false,
        }
    }
    true
}

#[derive(Serialize)]
pub struct TickResponse {
    pub advanced: usize,
}
