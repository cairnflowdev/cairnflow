// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Deployments (registration, revisions, removal — with the cluster-wide pinned count) and
//! services.

use super::*;

// ─────────────────────────────────────────────────────────── deployments

#[derive(Deserialize)]
pub struct RegisterRequest {
    pub uri: String,
    /// `declared` or `auto`; absent ⇒ `--deploy-transport`, and with
    /// `--deploy-https-request-response` (SaaS profile) an `https://` endpoint declaring
    /// BIDI becomes `REQUEST_RESPONSE`. When present it is exactly what the tenant chose —
    /// `declared` keeps BIDI for platforms that speak h2 all the way to the app.
    #[serde(default)]
    pub transport: Option<String>,
}

/// camelCase to match the manifest — this API has no spec of its own, so it follows the
/// conventions of what has been specified instead of inventing a second convention.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterResponse {
    /// The immutable id of the deployment just created. Registering the same endpoint
    /// again is a new id.
    pub id: String,
    pub tenant: String,
    pub created_at_ms: u64,
    pub endpoint: String,
    pub wire_version: u32,
    /// The transport settled on. The operator needs to see it: if `BIDI_STREAM` is
    /// declared in the manifest and h2c cannot pass the proxy, every invocation of this
    /// deployment fails at the transport, and the first question is "which mode is it
    /// running in".
    pub protocol_mode: discovery::ProtocolMode,
    pub services: Vec<String>,
    /// The services for which this deployment **is the current revision**. An old
    /// deployment still runs the invocations attached to it but takes no new ones.
    pub current_for: Vec<String>,
    /// The number of unfinished invocations still attached to this deployment, across
    /// **the whole cluster**. Only on `GET /deployments/{id}`; `None` there = not every
    /// range could be asked (a range without a leader to answer).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pinned_invocations: Option<u64>,
    /// The identity scheme the SDK declared it verifies — absent when the SDK does not
    /// verify requests.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_identity: Option<String>,
    /// The negotiated `REQUEST_RESPONSE` body compression — `gzip`, or absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rr_compression: Option<String>,
    /// How the transport mode was chosen at registration — absent on deployments
    /// registered before this field existed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transport: Option<discovery::Transport>,
    /// The manifest captured at registration: services, handlers with their **effective**
    /// kinds, the retry/timeout contract. Only on `GET /deployments/{id}` — to compare a
    /// version not yet promoted to revision with the current one before promoting it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest: Option<Vec<ManifestService>>,
}

/// One service of the manifest, with the same conventions as [`ServiceView`].
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManifestService {
    pub name: String,
    pub ty: &'static str,
    pub handlers: Vec<ManifestHandler>,
    /// The service's shared contract — a handler's declaration overrides it.
    #[serde(flatten)]
    pub contract: crate::discovery::Contract,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManifestHandler {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ty: Option<&'static str>,
    /// The handler's **own** contract (not yet merged with the service's).
    #[serde(flatten)]
    pub contract: crate::discovery::Contract,
}

fn service_ty(t: crate::discovery::ServiceType) -> &'static str {
    use crate::discovery::ServiceType;
    match t {
        ServiceType::Service => "service",
        ServiceType::VirtualObject => "virtualObject",
        ServiceType::Workflow => "workflow",
    }
}

fn handler_ty(t: crate::discovery::HandlerType) -> &'static str {
    use crate::discovery::HandlerType;
    match t {
        HandlerType::Exclusive => "exclusive",
        HandlerType::Shared => "shared",
        HandlerType::Workflow => "workflow",
    }
}

fn manifest_of(d: &discovery::Deployment) -> Vec<ManifestService> {
    d.services
        .iter()
        .map(|s| ManifestService {
            name: s.name.clone(),
            ty: service_ty(s.ty),
            handlers: s
                .handlers
                .iter()
                .map(|h| ManifestHandler {
                    name: h.name.clone(),
                    ty: h.effective_ty(s.ty).map(handler_ty),
                    contract: h.contract.clone(),
                })
                .collect(),
            contract: s.contract.clone(),
        })
        .collect()
}

impl RegisterResponse {
    fn of(d: &discovery::Deployment, catalog: &crate::registry::Catalog) -> Self {
        Self {
            id: d.id.clone(),
            tenant: d.tenant.clone(),
            created_at_ms: d.created_at_ms,
            endpoint: d.endpoint.clone(),
            wire_version: d.wire_version,
            protocol_mode: d.protocol_mode,
            services: d.services.iter().map(|s| s.name.clone()).collect(),
            current_for: d
                .services
                .iter()
                .filter(|s| catalog.current_id(&d.tenant, &s.name) == Some(d.id.as_str()))
                .map(|s| s.name.clone())
                .collect(),
            pinned_invocations: None,
            request_identity: d.request_identity.clone(),
            rr_compression: d.rr_compression.clone(),
            transport: d.transport.clone(),
            manifest: None,
        }
    }
}

/// Unfinished invocations per deployment, on the ranges **this node** leads, with the
/// list of ranges that answered.
#[derive(Default, Serialize, Deserialize)]
pub struct PinnedUsage {
    pub ranges: Vec<u32>,
    pub counts: std::collections::BTreeMap<String, u64>,
}

pub(super) async fn local_pinned(st: &AppState) -> Result<PinnedUsage, ApiError> {
    let mut out = PinnedUsage::default();
    for (p, engine) in st.router.partitions.iter() {
        crate::metrics::agent::add(&crate::metrics::agent::PINNED_SCANS, 1);
        match engine.pinned_counts().await {
            Ok(counts) => {
                out.ranges.push(p);
                for (d, n) in counts {
                    *out.counts.entry(d).or_insert(0) += n as u64;
                }
            }
            // This range is led by another node — that node answers its part.
            Err(e) if e.is_fenced() => continue,
            Err(e) => return Err(ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
        }
    }
    Ok(out)
}

/// `GET /internal/deployments/usage` — this node's part of the cluster-wide count.
/// Internal: only other nodes call it.
pub(super) async fn internal_pinned(State(st): State<AppState>) -> ApiResult<Json<PinnedUsage>> {
    Ok(Json(local_pinned(&st).await?))
}

/// The node's latest attachment count, with the time it **started**. The count is
/// O(unfinished invocations of every tenant) under the engine of every range; a key
/// repeating `GET /deployments/{id}` — or a `DELETE` that is refused with `409` every
/// time — must not stall the whole cluster's engines each time. An async lock:
/// concurrent requests queue behind **one** count.
#[derive(Default)]
pub(crate) struct PinnedCache(
    tokio::sync::Mutex<
        Option<(
            tokio::time::Instant,
            std::collections::BTreeMap<String, u64>,
        )>,
    >,
);

/// Like [`cluster_pinned`], shared by concurrent requests: reuses a full-range count
/// that **started after** the request arrived — the answer is never older than the
/// request itself (a read right after an invocation finishes sees the new number), while
/// N concurrent requests cost at most two counts (the one running when they arrived, and
/// one more after it for the whole group). A count missing ranges ⇒ `None`, not
/// remembered.
///
/// `DELETE` uses it too: a count that started after the `DELETE` arrived is exactly as
/// fresh as one the request would have started itself, and what happens between the
/// count and the removal is the job of the "being removed" mark, not of the count.
async fn cluster_pinned_shared(
    st: &AppState,
) -> Result<Option<std::collections::BTreeMap<String, u64>>, ApiError> {
    let arrived = tokio::time::Instant::now();
    let mut slot = st.pinned_cache.0.lock().await;
    if let Some((started, counts)) = slot.as_ref() {
        if *started >= arrived {
            return Ok(Some(counts.clone()));
        }
    }
    let started = tokio::time::Instant::now();
    let counts = cluster_pinned(st).await?;
    *slot = counts.clone().map(|c| (started, c));
    Ok(counts)
}

/// Unfinished invocations per deployment on **every** range of the table: this node's
/// part plus the part of each node leading another range. `None` when a range had
/// nobody answering — removing a deployment based on a count with gaps is removing it
/// from under invocations we cannot see.
pub(super) async fn cluster_pinned(
    st: &AppState,
) -> Result<Option<std::collections::BTreeMap<String, u64>>, ApiError> {
    let mut total = local_pinned(st).await?;
    let mut answered: std::collections::BTreeSet<u32> = total.ranges.iter().copied().collect();
    // Without the cluster secret no peer can be asked: the count has gaps ⇒ `None`
    // (through the range check below).
    let fanned = observe::fan_out::<PinnedUsage>(st, "/internal/deployments/usage", &[]).await;
    for (_, part) in fanned.parts {
        // A range changing owner between two questions may be counted twice — an
        // overcount only makes removal wrongly refuse, never wrongly allow.
        answered.extend(part.ranges);
        for (d, n) in part.counts {
            *total.counts.entry(d).or_insert(0) += n;
        }
    }
    let all = st
        .router
        .cluster
        .partitions()
        .iter()
        .all(|p| answered.contains(&p.0));
    Ok(all.then_some(total.counts))
}

/// `POST /deployments` — discovers, then registers a **new** deployment.
///
/// Two steps on the bucket, in this order: create `deployments/<tenant>/<id>` (`Create`,
/// immutable), then move the pointer of every service in the manifest with **one** CAS
/// on the tenant's catalog. Dying between the two steps leaves a deployment nobody
/// points to — harmless, removable; it never leaves a pointer to something that does
/// not exist yet.
pub(super) async fn register(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Query(q): Query<HashMap<String, String>>,
    Json(req): Json<RegisterRequest>,
) -> ApiResult<Json<RegisterResponse>> {
    let tenant = acting_tenant(&who, &q)?;
    // `?activate=false` ("safe deploy"): create the deployment but promote **no**
    // revision — the operator compares manifests, tries a replay, then promotes service
    // by service with `PUT /services/{s}/revision` carrying `expect`.
    let activate = match q.get("activate").map(String::as_str) {
        None | Some("true") => true,
        Some("false") => false,
        Some(v) => {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                format!("activate must be true or false, not {v:?}"),
            ))
        }
    };
    // A `suspended` tenant: the engine no longer calls out to its endpoint — discovery
    // is also a request out to that endpoint. `limited` can still register (it is not
    // new work).
    if let Some(settings) = &st.tenant_settings {
        let state = settings.flags(&tenant).state();
        if state.refuses_turns() {
            return Err(super::error::tenant_refused(
                cairn_engine::TenantRefusal::State(state),
            ));
        }
        // `max-deployments=`: each registration is a new record; counted from this
        // node's registry (other nodes see it after one refresh tick — a soft cap across
        // nodes). Before discovery: at the cap, do not call out.
        if let Some(max) = settings.limits_of(&tenant).max_deployments {
            let have = st
                .registry
                .snapshot()
                .deployments()
                .iter()
                .filter(|d| d.tenant == tenant.as_str())
                .count();
            if have >= max as usize {
                return Err(ApiError::coded(
                    StatusCode::CONFLICT,
                    super::error::Code::DeploymentsLimit,
                    format!(
                        "tenant {tenant} already has {have} deployments (max-deployments limit {max}) — remove an old one (DELETE /deployments/{{id}}) and register again"
                    ),
                ));
            }
        }
    }
    // The tenant's `deploy-rate=`: each registration is a GET out to a third-party host
    // — block before discovery, failed attempts count too.
    if let Some(settings) = &st.tenant_settings {
        if let Err(wait_ms) = settings.admit_deploy(&tenant, now_ms()) {
            crate::metrics::tenant::bump(&crate::metrics::tenant::DEPLOY_RATE_LIMITED);
            // `overloaded` carries `Retry-After` and `retry_after_ms`; `code` overrides the code.
            let wait = u32::try_from(wait_ms).unwrap_or(u32::MAX).max(1);
            return Err(ApiError::coded(
                StatusCode::TOO_MANY_REQUESTS,
                super::error::Code::DeployRateLimited,
                format!("tenant {tenant} exceeded the deployment registration rate limit"),
            )
            .retry_after_ms(wait));
        }
    }
    // The request states `transport` ⇒ a deliberate tenant choice, the SaaS profile's
    // `https://` rule does not override it.
    let explicit = req.transport.is_some();
    let policy = match req.transport.as_deref() {
        None => st.deploy_transport,
        Some(v) => discovery::TransportPolicy::parse(v).ok_or_else(|| {
            ApiError(
                StatusCode::BAD_REQUEST,
                format!("transport must be declared or auto, not {v:?}"),
            )
        })?,
    };
    let mut deployment = discovery::discover(
        &st.tenant_http,
        &req.uri,
        Some((&st.identity, tenant.as_str())),
    )
    .await
    // No overlapping version ⇒ refused **at registration**. That is deliberate: the
    // operator knows at deploy time, not when a customer's first invocation fails.
    .map_err(|e| match e {
        discovery::DiscoveryError::EgressDenied(d) => super::egress_denied(d),
        e => ApiError(StatusCode::BAD_REQUEST, e.to_string()),
    })?;
    // SaaS profile: an SDK that does not verify the cell's signature can be called by
    // another tenant through a known URL — refuse before writing anything.
    if st.deploy_require_identity
        && deployment.request_identity.as_deref() != Some(crate::identity::SCHEME_V1)
    {
        return Err(ApiError::coded(
            StatusCode::BAD_REQUEST,
            super::error::Code::IdentityRequired,
            format!(
                "endpoint {} does not declare request identity verification (requestIdentity: v1) — this node only accepts SDKs with identityKeys (GET /identity)",
                req.uri
            ),
        ));
    }
    // RR body compression: the operator can turn it off for new deployments.
    if st.deploy_no_rr_compression {
        deployment.rr_compression = None;
    }
    // `auto`: BIDI only when HTTP/2 goes all the way to the SDK. SaaS profile
    // (`--deploy-https-request-response`): `https://` declaring BIDI with a request that
    // does not state `transport` ⇒ `REQUEST_RESPONSE` — runs behind any TLS edge. Only
    // existing values are used (`declared` + the reason in `bidiProbe`), so old binaries
    // can read it.
    if let Some(t) = deployment.transport.as_mut() {
        t.policy = policy;
        if policy == discovery::TransportPolicy::Declared
            && !explicit
            && st.deploy_https_request_response
            && deployment.protocol_mode == discovery::ProtocolMode::BidiStream
            && discovery::is_https(&deployment.endpoint)
        {
            t.bidi_probe = Some(discovery::BIDI_SKIPPED_HTTPS.into());
            deployment.protocol_mode = discovery::ProtocolMode::RequestResponse;
        }
        if policy == discovery::TransportPolicy::Auto
            && deployment.protocol_mode == discovery::ProtocolMode::BidiStream
        {
            match discovery::probe_bidi(
                &st.tenant_h2,
                &deployment.endpoint,
                Some((&st.identity, tenant.as_str())),
            )
            .await
            {
                Ok(()) => t.bidi_probe = Some("ok".into()),
                Err(why) => {
                    t.bidi_probe = Some(format!("failed: {why}"));
                    deployment.protocol_mode = discovery::ProtocolMode::RequestResponse;
                }
            }
        }
    }
    // SaaS profile: a bare `REQUEST_RESPONSE` body is the expensive case — every turn
    // resends the journal, and the cell pays the egress. Checked after the transport
    // mode is settled (https:// and `auto` may turn BIDI into RR), before writing
    // anything. BIDI does not replay every turn: not checked.
    if st.deploy_require_compression
        && deployment.protocol_mode == discovery::ProtocolMode::RequestResponse
        && deployment.rr_compression.as_deref() != Some(discovery::RR_GZIP)
    {
        return Err(ApiError::coded(
            StatusCode::BAD_REQUEST,
            super::error::Code::CompressionRequired,
            format!(
                "endpoint {} runs REQUEST_RESPONSE but the SDK does not declare body compression (requestCompression: gzip) — this node only accepts SDKs with compression (TypeScript SDK: compression is on by default)",
                req.uri
            ),
        ));
    }
    let now = now_ms();
    deployment.id = discovery::new_deployment_id(now);
    deployment.created_at_ms = now;
    deployment.tenant = tenant.as_str().to_string();

    // **Not** through the actor, and not into any partition's state: deployments are
    // cluster configuration. See `cairn_store::DeploymentStore`.
    //
    // Direct consequence: registering needs no write authority, so a follower node can
    // do it, and every partition sees it — including partitions led by other nodes.
    match &st.deployments {
        Some(store) => {
            let bytes = serde_json::to_vec(&deployment)
                .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            store
                .create(&tenant, &deployment.id, &bytes)
                .await
                .map_err(store_error)?;
            st.registry.insert(deployment.clone());
            if activate {
                let id = deployment.id.clone();
                let services: Vec<String> =
                    deployment.services.iter().map(|s| s.name.clone()).collect();
                let doc = store
                    .update_catalog(&tenant, &mut |c: &mut cairn_store::CatalogDoc| {
                        for svc in &services {
                            c.services.insert(svc.clone(), id.clone());
                        }
                        Ok(())
                    })
                    .await
                    .map_err(store_error)?;
                st.registry
                    .set_revisions_written(tenant.as_str(), doc.version, &doc.services);
            }
        }
        None if activate => {
            st.registry.upsert(deployment.clone());
        }
        None => {
            st.registry.insert(deployment.clone());
        }
    }
    let resp = RegisterResponse::of(&deployment, &st.registry.snapshot());

    // Wake every partition **we lead**: the deployment that just arrived may be what
    // their unfinished invocations are waiting for to continue. Partitions held by other
    // nodes see it on their node's refresh loop.
    st.router.partitions.deployments_changed().await;
    Ok(Json(resp))
}

pub(super) async fn list_deployments(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<Vec<RegisterResponse>>> {
    let tenant = acting_tenant(&who, &q)?;
    let catalog = st.registry.snapshot();
    // `?pinned=true` (the `sys_deployment.pinned` column): the number of invocations
    // attached to each deployment, one cluster-wide count for the whole list — like
    // `GET /deployments/{id}`; a count missing ranges ⇒ absent.
    let pinned = match q.get("pinned").map(String::as_str) {
        Some("true") => cluster_pinned_shared(&st).await?,
        _ => None,
    };
    Ok(Json(
        catalog
            .deployments()
            .into_iter()
            .filter(|d| d.tenant == tenant.as_str())
            .map(|d| {
                let mut view = RegisterResponse::of(d, &catalog);
                if let Some(c) = &pinned {
                    view.pinned_invocations = Some(c.get(&d.id).copied().unwrap_or(0));
                }
                view
            })
            .collect(),
    ))
}

pub(super) async fn get_deployment(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path(id): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<RegisterResponse>> {
    let tenant = acting_tenant(&who, &q)?;
    let catalog = st.registry.snapshot();
    let d = deployment_of_tenant(&catalog, &id, &tenant)?;
    let mut view = RegisterResponse::of(d, &catalog);
    view.manifest = Some(manifest_of(d));
    view.pinned_invocations = cluster_pinned_shared(&st)
        .await?
        .map(|c| c.get(&id).copied().unwrap_or(0));
    Ok(Json(view))
}

/// `DELETE /deployments/{id}` — removes a deployment **nobody uses any more**.
///
/// Refused (`409`) while it is still the current revision of a service: removing it
/// would leave that service uncallable, and that must be a deliberate pointer move, not
/// a side effect of a cleanup.
/// The part of `DELETE` after the mark: a CAS checking the catalog, counting the
/// attachments **again**, deleting. The recount catches invocations attached to `id`
/// between the first count and setting the mark (`migrate`/`restart` ahead of the mark,
/// another node's stale RAM registry).
async fn remove_marked(
    st: &AppState,
    store: &cairn_store::DeploymentStore,
    tenant: &cairn_store::TenantId,
    id: &str,
) -> ApiResult<()> {
    // Checked on the catalog **in the bucket**, under CAS: the RAM catalog may lag one
    // refresh round behind a registration on another node.
    let id2 = id.to_string();
    store
        .update_catalog(tenant, &mut |c: &mut cairn_store::CatalogDoc| {
            let using: Vec<&String> = c
                .services
                .iter()
                .filter(|(_, v)| **v == id2)
                .map(|(k, _)| k)
                .collect();
            if using.is_empty() {
                Ok(())
            } else {
                Err(format!(
                    "deployment {id2} is still the current revision of {using:?}"
                ))
            }
        })
        .await
        .map_err(store_error)?;
    cairn_store::failpoint!("ingress::delete_deployment::before_remove");
    // Every other node re-reads the deployment registry **before** the final count:
    // after the ack, `/invoke` on any node creates invocations per the new pointer — the
    // count below catches every invocation attached to `id` created before. Missing acks
    // ⇒ do not remove (deletion only when proven).
    if let Some(sync) = &st.registry_sync {
        let acks = sync
            .confirm_peers_at("/internal/registry/apply?deployments")
            .await;
        if !acks.all() {
            return Err(ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                format!(
                    "could not confirm that every node dropped the old revision pointers (waiting on {:?}) — retry",
                    acks.pending
                ),
            ));
        }
    }
    match cluster_pinned(st).await? {
        None => {
            return Err(ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "cannot ask every range — attached invocations not counted yet, retry later".into(),
            ))
        }
        Some(c) if c.get(id).copied().unwrap_or(0) > 0 => {
            return Err(ApiError(
                StatusCode::CONFLICT,
                format!(
                    "deployment {id} still has {} unfinished invocations attached to it",
                    c[id]
                ),
            ))
        }
        Some(_) => {}
    }
    store.remove(tenant, id).await.map_err(store_error)
}

/// `409` when deployment `id` is being removed or is gone from the bucket — the target
/// of `set_revision`/`migrate`/`restart`, read **after** their write.
pub(super) async fn ensure_not_retiring(
    st: &AppState,
    tenant: &cairn_store::TenantId,
    id: &str,
) -> ApiResult<()> {
    let Some(store) = &st.deployments else {
        return Ok(());
    };
    let gone = store.is_retiring(tenant, id).await.map_err(store_error)?
        || store.get(tenant, id).await.map_err(store_error)?.is_none();
    if gone {
        return Err(ApiError(
            StatusCode::CONFLICT,
            format!("deployment {id} is being removed — choose another deployment"),
        ));
    }
    Ok(())
}

pub(super) async fn delete_deployment(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path(id): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<StatusCode> {
    cairn_store::check_deployment_id(&id).map_err(store_error)?;
    let tenant = acting_tenant(&who, &q)?;
    let catalog = st.registry.snapshot();
    let d = deployment_of_tenant(&catalog, &id, &tenant)?;
    // Invocations still attached ⇒ do not remove: they can only run on exactly this
    // deployment.
    match cluster_pinned_shared(&st).await? {
        None => {
            return Err(ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "cannot ask every range — attached invocations not counted yet, retry later".into(),
            ))
        }
        Some(c) if c.get(&id).copied().unwrap_or(0) > 0 => {
            return Err(ApiError(
                StatusCode::CONFLICT,
                format!(
                    "deployment {id} still has {} unfinished invocations attached to it",
                    c[&id]
                ),
            ))
        }
        Some(_) => {}
    }
    if let Some(store) = &st.deployments {
        // Two-phase removal: the "being removed" mark **before** the CAS checking the
        // catalog. `set_revision`/`migrate`/`restart` to this id read the mark after their
        // write: their CAS before the CAS here ⇒ we see the pointer and return `409`;
        // after ⇒ they see the mark and back out.
        let mut nonce = [0u8; 8];
        cairn_store::entropy::fill(&mut nonce);
        let note = format!(
            "{}\nnode={}\nat_ms={}\nnonce={:016x}\n",
            cairn_store::DEPLOYMENT_RETIRING_HEADER,
            st.router.node_id.as_deref().unwrap_or("-"),
            cairn_store::entropy::now_ms(),
            u64::from_be_bytes(nonce)
        );
        store
            .mark_retiring(&tenant, &id, note.as_bytes())
            .await
            .map_err(store_error)?;
        let removed = remove_marked(&st, store, &tenant, &id).await;
        // Every outcome removes the mark. Failure ⇒ the mark stays: it only blocks moving
        // pointers to this id, and the next `DELETE` removes it.
        if let Err(e) = store.clear_retiring(&tenant, &id).await {
            tracing::warn!(deployment = %id, error = %e, "could not clear the retiring mark of a deployment");
        }
        removed?;
    } else if d
        .services
        .iter()
        .any(|s| catalog.current_id(&d.tenant, &s.name) == Some(id.as_str()))
    {
        return Err(ApiError(
            StatusCode::CONFLICT,
            format!("deployment {id} is still the current revision"),
        ));
    }
    st.registry.remove(&id);
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
pub struct RevisionRequest {
    /// The target deployment. Must declare this service.
    pub deployment: String,
    /// Only move while the pointer **currently** points at this id — CAS at the API
    /// level: of two people rolling back at once exactly one wins, the other gets `409`.
    #[serde(default)]
    pub expect: Option<String>,
}

/// `PUT /services/{service}/revision` — moves a service's revision pointer to an
/// existing deployment (a rollback, or pinning a specific version).
pub(super) async fn set_revision(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path(service): Path<String>,
    Query(q): Query<HashMap<String, String>>,
    Json(req): Json<RevisionRequest>,
) -> ApiResult<Json<serde_json::Value>> {
    cairn_store::check_deployment_id(&req.deployment).map_err(store_error)?;
    let tenant = acting_tenant(&who, &q)?;
    let catalog = st.registry.snapshot();
    let Some(d) = catalog.get(&req.deployment) else {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            format!("no deployment {:?}", req.deployment),
        ));
    };
    if d.tenant != tenant.as_str() {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            format!("no deployment {:?}", req.deployment),
        ));
    }
    if d.service(&service).is_none() {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            format!(
                "deployment {} does not declare service {service:?}",
                req.deployment
            ),
        ));
    }
    // The pointer before our move (the winning CAS) — to move it back when the target
    // turns out to be being removed.
    let before = std::sync::Mutex::new(None::<String>);
    let edit = |c: &mut cairn_store::CatalogDoc| {
        let now = c.services.get(&service).cloned();
        *before.lock().unwrap_or_else(|e| e.into_inner()) = now.clone();
        if let Some(expect) = &req.expect {
            if now.as_ref() != Some(expect) {
                return Err(format!(
                    "pointer of {service:?} is at {now:?}, not {expect:?}"
                ));
            }
        }
        c.services.insert(service.clone(), req.deployment.clone());
        Ok(())
    };
    match &st.deployments {
        Some(store) => {
            let mut edit = edit;
            let doc = store
                .update_catalog(&tenant, &mut edit)
                .await
                .map_err(store_error)?;
            // Read the mark **after** the CAS: `DELETE` sets its mark before its CAS.
            if let Err(refused) = ensure_not_retiring(&st, &tenant, &req.deployment).await {
                let back = before.lock().unwrap_or_else(|e| e.into_inner()).clone();
                let doc = store
                    .update_catalog(&tenant, &mut |c: &mut cairn_store::CatalogDoc| {
                        if c.services.get(&service) == Some(&req.deployment) {
                            match &back {
                                Some(b) => {
                                    c.services.insert(service.clone(), b.clone());
                                }
                                None => {
                                    c.services.remove(&service);
                                }
                            }
                        }
                        Ok(())
                    })
                    .await
                    .map_err(store_error)?;
                st.registry
                    .set_revisions_written(tenant.as_str(), doc.version, &doc.services);
                return Err(refused);
            }
            st.registry
                .set_revisions_written(tenant.as_str(), doc.version, &doc.services);
        }
        None => {
            let mut doc = cairn_store::CatalogDoc::default();
            for (t, svc, id) in catalog.revisions() {
                if t == tenant.as_str() {
                    doc.services.insert(svc.to_string(), id.to_string());
                }
            }
            edit(&mut doc).map_err(|r| ApiError(StatusCode::CONFLICT, r))?;
            st.registry.set_revisions(tenant.as_str(), &doc.services);
        }
    }
    st.router.partitions.deployments_changed().await;
    Ok(Json(serde_json::json!({
        "service": service,
        "deployment": req.deployment,
    })))
}

// ─────────────────────────────────────────────────────────── services

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HandlerView {
    pub name: String,
    /// `exclusive` · `shared` — only meaningful for virtual objects and workflows.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ty: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceView {
    pub name: String,
    /// `service` · `virtualObject` · `workflow`.
    pub ty: String,
    /// The endpoint of the current revision.
    pub deployment: String,
    /// The id of the current revision.
    pub deployment_id: String,
    pub handlers: Vec<HandlerView>,
}

/// Services from the *caller's* point of view: what can I call, on which handler.
///
/// `/deployments` answers the operator's question ("which endpoints are registered");
/// this endpoint answers the client author's question — so it is flat by service, not
/// by deployment.
pub(super) async fn list_services(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<Vec<ServiceView>>> {
    let tenant = acting_tenant(&who, &q)?;
    let mut out = Vec::new();
    // By the tenant's **current revision**: a service has exactly one row, however many
    // old deployments still declare it.
    let catalog = st.registry.snapshot();
    for (t, svc_name, id) in catalog.revisions() {
        if t != tenant.as_str() {
            continue;
        }
        let Some(d) = catalog.get(id) else { continue };
        for s in d.services.iter().filter(|s| s.name == svc_name) {
            out.push(ServiceView {
                name: s.name.clone(),
                ty: service_ty(s.ty).into(),
                deployment: d.endpoint.clone(),
                deployment_id: d.id.clone(),
                handlers: s
                    .handlers
                    .iter()
                    // The **effective** kind, not the kind the SDK declared: a workflow
                    // handler that does not declare `ty` is `SHARED`, and the client
                    // author needs to know that here instead of looking up the spec.
                    .map(|h| HandlerView {
                        name: h.name.clone(),
                        ty: h.effective_ty(s.ty).map(|t| handler_ty(t).into()),
                    })
                    .collect(),
            });
        }
    }
    Ok(Json(out))
}
