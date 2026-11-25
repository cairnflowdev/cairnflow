// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Ingress HTTP + admin API.
//!
//! The core jobs: registering deployments, calling services, viewing status. Every
//! partition here is reached through the router; this module owns the HTTP surface.

use cairn_store::entropy::HashMap;
use {
    crate::routing::RoutingTable,
    crate::{
        actor::{EngineHandle, InvokeOutcome},
        discovery,
        registry::Registry,
    },
    axum::{
        extract::{Path, Query, State},
        http::StatusCode,
        response::{IntoResponse, Response},
        routing::{get, post},
        Json, Router as HttpRouter,
    },
    bytes::Bytes,
    cairn_engine::InterPartition,
    cairn_engine::{InvocationStatus, Target},
    cairn_store::{
        partition_key_for_target, ClusterConfig, IdentError, InvocationId, Limit, LimitKey,
        PartitionKey, RuleStore, Scope, STAR_WARNING,
    },
    serde::{Deserialize, Serialize},
    std::{sync::Arc, time::Duration},
};

// Observability for operators: per-handler summaries, cluster-wide views, capturing the
// next runs. A separate file so admin routes can change here without colliding with
// the rest of this file.
mod observe;
mod ops;
mod range_load;
mod router;
pub use router::{
    workflow_id as derive_workflow_id, Accepted, Call, Delivery, NotLeader, Refused, RouteError,
    Router,
};

pub use crate::partitions::Partitions;

mod error;
pub use error::*;
mod internal;
use internal::*;
mod cluster;
pub use cluster::*;
mod admin;
pub use admin::*;
mod deployments;
pub use deployments::*;
mod invoke;
pub use invoke::*;
mod invocations;
pub use invocations::*;
// The console's bulk operations.
mod batch;
mod external;
pub use external::*;
mod metrics;
use metrics::*;
mod flow_admin;
pub use flow_admin::*;
mod archive;
mod audit;
pub use archive::*;
mod streams;
// Forwarding to the leader.
pub mod forward;
// Invocation artifacts.
mod artifacts;
mod values;
// Server-side schedules.
mod schedules;
pub(crate) use schedules::schedule_counts;
// The tenant's public request-identity keys.
mod identity;
// The cell's OIDC relying party configuration.
mod oidc;

#[derive(Clone)]
pub struct AppState {
    /// Routing, id generation, NotLeader answers — independent of HTTP.
    router: Router,
    /// The client of internal paths (forwarding to the leader, asking sibling nodes) —
    /// [`crate::invoker::internal_http`]; never carries a tenant's request.
    client: reqwest::Client,
    /// The client of requests on behalf of a tenant (discovery, trial replay) — through
    /// the egress policy.
    tenant_http: crate::egress::TenantHttp,
    /// The h2 client of requests on behalf of a tenant — only to probe `BIDI_STREAM` at
    /// registration; BIDI turns use the client of `invoker::Clients`.
    tenant_h2: crate::egress::TenantHttp,
    /// The policy for choosing the transport mode (`--deploy-transport`).
    deploy_transport: crate::discovery::TransportPolicy,
    /// `https://` + an SDK declaring BIDI ⇒ `REQUEST_RESPONSE` when the request does not
    /// state `transport` (`--deploy-https-request-response`).
    deploy_https_request_response: bool,
    /// `--deploy-no-rr-compression`.
    deploy_no_rr_compression: bool,
    /// The request identity signer — discovery, trial replay, `GET /identity`.
    identity: crate::identity::Identity,
    /// `POST /deployments` requires the SDK to declare `requestIdentity`.
    deploy_require_identity: bool,
    /// `POST /deployments` refuses `REQUEST_RESPONSE` without declared compression (SaaS profile).
    deploy_require_compression: bool,
    registry: Registry,
    /// The cluster-level deployment store. `None` ⇒ not configured (old tests).
    deployments: Option<cairn_store::DeploymentStore>,
    /// The cluster-level rule book. `None` ⇒ the node runs without a cluster object
    /// store, so no caps can be set — `/rules` returns `501` instead of silently doing
    /// nothing.
    rules: Option<RuleStore>,
    /// The read path for partitions this node does not lead. `None` ⇒ a single node —
    /// the leader is always here, nobody to read on its behalf.
    followers: Option<crate::follower::FollowerReads>,
    metrics: Arc<crate::metrics::Metrics>,
    /// How long `/invoke` waits at most for the invocation to stop for good. `None` = no wait.
    invoke_wait: Option<Duration>,
    /// The cap of `?wait_ms=` on attach.
    attach_max_wait: Duration,
    /// The floor of `everyMs` in `PUT /schedules`.
    schedule_min_period: Duration,
    /// The floor of `/invoke`'s polling grid tick — tests only, see
    /// `NodeConfig::invoke_poll_floor`.
    invoke_poll_floor: Option<Duration>,
    /// The node's split/merge coordinator. `None` ⇒ `501`.
    range_ops: Option<crate::split::RangeOps>,
    /// The per-range flush window registry. `None` ⇒ `/ranges/{id}/window` returns `501`.
    windows: Option<crate::windows::RangeWindows>,
    /// The read cache of `/stats/history`, one per node.
    history_cache: crate::stats_history::HistoryCache,
    /// The main store, for reading metering numbers. `None` ⇒ `/metering` returns `501`.
    metering: Option<(
        Arc<dyn slatedb::object_store::ObjectStore>,
        slatedb::object_store::path::Path,
    )>,
    /// The ingress port's API keys. Off by default — hand-built routers.
    auth: crate::auth::Auth,
    /// Stream configuration and the node's count of open SSE connections — an atomic,
    /// only touched when a connection opens/closes.
    streams: crate::streams::StreamConfig,
    sse_open: Arc<std::sync::atomic::AtomicUsize>,
    /// The long-lived connections (SSE, waiting attaches) open per tenant on the node —
    /// capped by `StreamConfig::tenant_max`.
    tenant_open: Arc<streams::TenantOpen>,
    /// In-flight segment reads through `GET /values/v1/{token}` per tenant — capped by
    /// [`values::VALUE_PROXY_PER_TENANT`].
    value_open: Arc<streams::TenantOpen>,
    /// Running segmented-output assemblies per tenant — capped by
    /// [`invoke::ASSEMBLE_PER_TENANT`].
    assemble_open: Arc<streams::TenantOpen>,
    /// Schedules this node created (+) / deleted (−) per tenant **since the latest
    /// `TenantUsage` count** — `max-schedules=` does not wait for the next counting
    /// tick. The counting loop clears this book when it applies new numbers.
    schedule_pending: Arc<std::sync::Mutex<std::collections::BTreeMap<cairn_store::TenantId, i64>>>,
    /// The attachment count reused for `GET /deployments`.
    pinned_cache: Arc<deployments::PinnedCache>,
    /// The cluster secret: the internal port only accepts requests signed with it, and
    /// the node signs with it when asking peers. `None` ⇒ the internal port refuses
    /// everything.
    cluster_secret: Option<crate::auth::ClusterSecret>,
    /// The node's segment read URL issuer: trial replay for v6 SDKs, the expiry of
    /// `GET /values/v1/{token}`.
    value_urls: Arc<crate::value_urls::ValueUrls>,
    /// The segment store for the ingress read paths — assembling segmented output,
    /// `GET /values/v1/{token}` — with a cache sharing the node's `value_cache` cap.
    /// `None` ⇒ an uncached store built from `metering`.
    value_reader: Option<cairn_store::value::ValueStore>,
    /// Kho archive (6.1) — `GET /archive/{id}`. `None` ⇒ `501`.
    archive: Option<cairn_store::archive::ArchiveStore>,
    /// Per-tenant settings. `None` ⇒ `/admin/tenants` returns `501`.
    tenant_settings: Option<crate::tenants::TenantSettings>,
    /// The data plane's audit sink. `None` ⇒ no records written to objects, `GET /audit`
    /// returns `501` (hand-built routers).
    audit: Option<crate::audit::Sink>,
    /// The node's per-handler summary + slow log + capture. `None` ⇒ `/stats/handlers`,
    /// `/admin/capture` return `501`.
    handler_stats: Option<crate::stats::HandlerStats>,
    /// The D-d fences and plan. `None` ⇒ `/admin/gc/*` returns `501`.
    gc: Option<crate::gc::GcAdmin>,
    /// Tenant purge. `None` ⇒ the internal route and `GET /admin/tenants/{t}/purge`
    /// return `501`.
    tenant_purge: Option<Arc<crate::tenant_purge::Purger>>,
    /// The node's auto split. `None` ⇒ `/ranges/stats`, `/ranges/suggestions` return
    /// `501`.
    auto_split: Option<crate::autosplit::AutoSplit>,
    /// Cluster-level registry change notices. `None` ⇒ nobody is told; peers learn the
    /// change on their poll tick.
    registry_sync: Option<crate::registry_sync::RegistrySync>,
    /// The node's control loop — a peer's graceful shutdown notice nudges it to re-read
    /// `control/coordinator`. `None` ⇒ no nudge.
    control: Option<crate::control::Control>,
    /// The scrubber's latest round (`GET /admin/scrub`).
    scrub_last: Option<crate::scrub::ScrubLast>,
    /// The node's query layer. `None` ⇒ `/query` returns `501`.
    query: Option<crate::query::QueryService>,
    /// The web console: the outermost layer of [`router`]. Empty by default — hand-built
    /// routers do not serve the UI.
    ui: crate::ui::Ui,
    /// `--ui-env-label` — `whoami.envLabel`.
    env_label: Option<Arc<str>>,
    /// The node's own ingress router, set after it is built: `/internal/forward` runs
    /// forwarded requests on it.
    ingress: crate::query::views::RouterSlot,
}

impl AppState {
    /// The node's query layer, if enabled.
    pub fn query(&self) -> Option<&crate::query::QueryService> {
        self.query.as_ref()
    }

    /// The port's auth — the `/query` view tables copy the effective `Host` of the
    /// original request ([`crate::query::views::Credentials::of_request`]).
    pub fn auth(&self) -> &crate::auth::Auth {
        &self.auth
    }

    /// What remote scans need: the routing table, this node's name, the client and the
    /// cluster secret to sign with.
    pub(crate) fn query_peers(&self) -> crate::query::http::Peers {
        crate::query::http::Peers {
            leaders: self.router.routing.snapshot(),
            me: self.router.node_id.clone(),
            client: self.client.clone(),
            secret: self.cluster_secret.clone(),
        }
    }

    pub fn with_query(mut self, q: Option<crate::query::QueryService>) -> Self {
        self.query = q;
        self
    }

    pub fn new(engine: EngineHandle, registry: Registry) -> Self {
        Self::new_in_cluster(
            Partitions::single(engine),
            registry,
            ClusterConfig::single(),
            RoutingTable::new(),
            None,
            None,
        )
    }

    pub fn new_in_cluster(
        partitions: Partitions,
        registry: Registry,
        cluster: ClusterConfig,
        routing: RoutingTable,
        deployments: Option<cairn_store::DeploymentStore>,
        followers: Option<crate::follower::FollowerReads>,
    ) -> Self {
        Self {
            router: Router {
                partitions,
                registry: registry.clone(),
                cluster,
                routing,
                node_id: None,
            },
            client: crate::invoker::internal_http(),
            // Discovery and trial replay call down to the SDK: the same idle connection
            // lifetime as turns (`invoker::SDK_POOL_IDLE`), the same egress policy.
            tenant_http: crate::egress::TenantHttp::h1(Default::default()),
            tenant_h2: crate::egress::TenantHttp::h2c(Default::default()),
            deploy_transport: Default::default(),
            deploy_https_request_response: false,
            deploy_no_rr_compression: false,
            identity: Default::default(),
            deploy_require_identity: false,
            deploy_require_compression: false,
            registry,
            deployments,
            rules: None,
            followers,
            metrics: Arc::new(crate::metrics::Metrics::default()),
            invoke_wait: None,
            attach_max_wait: crate::node::DEFAULT_ATTACH_MAX_WAIT,
            schedule_min_period: crate::node::DEFAULT_SCHEDULE_MIN_PERIOD,
            invoke_poll_floor: None,
            range_ops: None,
            history_cache: Default::default(),
            metering: None,
            windows: None,
            auth: crate::auth::Auth::disabled(),
            streams: crate::streams::StreamConfig::default(),
            sse_open: Arc::default(),
            tenant_open: Arc::default(),
            value_open: Arc::default(),
            assemble_open: Arc::default(),
            pinned_cache: Arc::default(),
            schedule_pending: Arc::default(),
            cluster_secret: None,
            value_urls: Default::default(),
            value_reader: None,
            archive: None,
            tenant_settings: None,
            audit: None,
            handler_stats: None,
            gc: None,
            tenant_purge: None,
            auto_split: None,
            registry_sync: None,
            control: None,
            scrub_last: None,
            query: None,
            ui: crate::ui::Ui::default(),
            env_label: None,
            ingress: Default::default(),
        }
    }

    /// Where the node's ingress router goes — set by `serve_http`.
    pub fn ingress_slot(&self) -> &crate::query::views::RouterSlot {
        &self.ingress
    }

    /// The egress policy of requests on behalf of a tenant.
    pub fn with_egress(mut self, policy: std::sync::Arc<crate::egress::EgressPolicy>) -> Self {
        self.tenant_h2 = crate::egress::TenantHttp::h2c(std::sync::Arc::clone(&policy));
        self.tenant_http = crate::egress::TenantHttp::h1(policy);
        self
    }

    /// The policy for choosing the transport mode at registration.
    pub fn with_deploy_transport(mut self, p: crate::discovery::TransportPolicy) -> Self {
        self.deploy_transport = p;
        self
    }

    /// `https://` + an SDK declaring BIDI ⇒ `REQUEST_RESPONSE`.
    /// New deployments do not record `rrCompression`.
    pub fn with_deploy_no_rr_compression(mut self, on: bool) -> Self {
        self.deploy_no_rr_compression = on;
        self
    }

    pub fn with_deploy_https_request_response(mut self, on: bool) -> Self {
        self.deploy_https_request_response = on;
        self
    }

    /// The node's request identity signer.
    pub fn with_identity(mut self, identity: crate::identity::Identity) -> Self {
        self.identity = identity;
        self
    }

    /// Refuses deployments that do not verify request identity.
    /// `POST /deployments` refuses `REQUEST_RESPONSE` endpoints whose SDK does not declare
    /// `requestCompression: gzip` (`--deploy-require-compression`).
    pub fn with_deploy_require_compression(mut self, on: bool) -> Self {
        self.deploy_require_compression = on;
        self
    }

    pub fn with_deploy_require_identity(mut self, on: bool) -> Self {
        self.deploy_require_identity = on;
        self
    }

    /// Attaches the web console.
    pub fn with_ui(mut self, ui: crate::ui::Ui) -> Self {
        self.ui = ui;
        self
    }

    /// The console's environment label.
    pub fn with_env_label(mut self, label: Option<String>) -> Self {
        self.env_label = label.map(Arc::from);
        self
    }

    /// Attaches the cluster-level registry change signal.
    pub fn with_control(mut self, c: Option<crate::control::Control>) -> Self {
        self.control = c;
        self
    }

    pub fn with_scrub_last(mut self, s: Option<crate::scrub::ScrubLast>) -> Self {
        self.scrub_last = s;
        self
    }

    pub fn with_registry_sync(mut self, s: Option<crate::registry_sync::RegistrySync>) -> Self {
        self.registry_sync = s;
        self
    }

    /// Attaches the node's auto split.
    pub fn with_auto_split(mut self, a: Option<crate::autosplit::AutoSplit>) -> Self {
        self.auto_split = a;
        self
    }

    /// Attaches the per-handler summary / slow log / capture.
    pub fn with_handler_stats(mut self, s: Option<crate::stats::HandlerStats>) -> Self {
        self.handler_stats = s;
        self
    }

    /// Attaches the D-d fence/plan API.
    pub fn with_gc(mut self, gc: Option<crate::gc::GcAdmin>) -> Self {
        self.gc = gc;
        self
    }

    /// Attaches the tenant purge loop.
    pub fn with_tenant_purge(mut self, p: Option<Arc<crate::tenant_purge::Purger>>) -> Self {
        self.tenant_purge = p;
        self
    }

    pub(crate) fn tenant_purge(&self) -> Option<&Arc<crate::tenant_purge::Purger>> {
        self.tenant_purge.as_ref()
    }

    /// Attaches the archive store.
    pub fn with_archive(mut self, archive: Option<cairn_store::archive::ArchiveStore>) -> Self {
        self.archive = archive;
        self
    }

    /// The node's per-tenant settings (the usage counting loop).
    pub(crate) fn tenant_settings(&self) -> Option<&crate::tenants::TenantSettings> {
        self.tenant_settings.as_ref()
    }

    /// Attaches per-tenant settings.
    pub fn with_tenant_settings(mut self, s: Option<crate::tenants::TenantSettings>) -> Self {
        self.tenant_settings = s;
        self
    }

    /// The node's stream configuration (SSE cap, keepalive, maximum lifetime).
    pub fn with_streams(mut self, cfg: crate::streams::StreamConfig) -> Self {
        self.streams = cfg;
        self
    }

    /// Attaches the ingress port's auth.
    pub fn with_auth(mut self, auth: crate::auth::Auth) -> Self {
        self.auth = auth;
        self
    }

    /// Attaches the node's issuer of segment read URLs (SDK wire v6).
    pub fn with_value_urls(mut self, urls: Arc<crate::value_urls::ValueUrls>) -> Self {
        self.value_urls = urls;
        self
    }

    /// Attaches the internal port's cluster secret.
    pub fn with_cluster_secret(mut self, secret: crate::auth::ClusterSecret) -> Self {
        self.cluster_secret = Some(secret);
        self
    }

    /// Attaches the node's split/merge coordinator.
    pub fn with_range_ops(mut self, ops: Option<crate::split::RangeOps>) -> Self {
        self.range_ops = ops;
        self
    }

    /// The per-range flush window registry.
    pub fn with_windows(mut self, w: crate::windows::RangeWindows) -> Self {
        self.windows = Some(w);
        self
    }

    /// The data plane's audit sink.
    pub fn with_audit(mut self, sink: Option<crate::audit::Sink>) -> Self {
        self.audit = sink;
        self
    }

    pub(crate) fn audit_sink(&self) -> Option<&crate::audit::Sink> {
        self.audit.as_ref()
    }

    /// The `/stats/history` cache the node flushes every `flush`.
    pub fn with_history_flush(mut self, flush: std::time::Duration) -> Self {
        self.history_cache = crate::stats_history::HistoryCache::new(flush);
        self
    }

    /// The main store for reading per-tenant metering numbers.
    pub fn with_metering(
        mut self,
        store: Arc<dyn slatedb::object_store::ObjectStore>,
        prefix: slatedb::object_store::path::Path,
    ) -> Self {
        self.metering = Some((store, prefix));
        self
    }

    /// The segment cache of the ingress read path, on the store of
    /// [`Self::with_metering`] — call **after** it. Shares the `budget` cap with the
    /// ranges' segment caches.
    pub fn with_value_cache(mut self, budget: cairn_store::artifact::CacheBudget) -> Self {
        self.value_reader = self.metering.as_ref().map(|(store, prefix)| {
            cairn_store::value::ValueStore::new(Arc::clone(store), prefix).with_cache(budget)
        });
        self
    }

    /// The segment store for the ingress read path: cached if [`Self::with_value_cache`]
    /// was called, otherwise built on the main store. `None` = the node has no main
    /// store.
    pub(super) fn value_store(&self) -> Option<cairn_store::value::ValueStore> {
        self.value_reader.clone().or_else(|| {
            self.metering.as_ref().map(|(store, prefix)| {
                cairn_store::value::ValueStore::new(Arc::clone(store), prefix)
            })
        })
    }

    /// The schedule count of `tenant` for the `max-schedules=` cap: the `TenantUsage`
    /// count plus the schedules this node created/deleted since that count.
    pub(crate) fn schedules_in_use(&self, tenant: &cairn_store::TenantId) -> u64 {
        let counted = self
            .tenant_settings
            .as_ref()
            .map(|t| t.limits().snapshot().usage(tenant).schedules)
            .unwrap_or(0);
        let pending = self
            .schedule_pending
            .lock()
            .map(|m| m.get(tenant).copied().unwrap_or(0))
            .unwrap_or(0);
        counted.saturating_add_signed(pending)
    }

    /// Records a schedule this node just created (`+1`) or deleted (`-1`) for `tenant`.
    pub(crate) fn note_schedule_created(&self, tenant: &cairn_store::TenantId, delta: i64) {
        if let Ok(mut m) = self.schedule_pending.lock() {
            *m.entry(tenant.clone()).or_insert(0) += delta;
        }
    }

    /// The `TenantUsage` counting loop just applied new numbers: the book of schedules
    /// created/deleted since the previous count is already included in them.
    pub(crate) fn reset_schedule_pending(&self) {
        if let Ok(mut m) = self.schedule_pending.lock() {
            m.clear();
        }
    }

    /// This node's id in the routing table.
    pub fn with_node_id(mut self, node_id: Option<String>) -> Self {
        self.router.node_id = node_id;
        self
    }

    /// Sets the wait cap of `/invoke`. See [`invoke`].
    pub fn with_invoke_wait(mut self, wait: Option<Duration>) -> Self {
        self.invoke_wait = wait;
        self
    }

    /// Sets the floor of `everyMs` for schedules.
    pub fn with_schedule_min_period(mut self, min: Duration) -> Self {
        self.schedule_min_period = min;
        self
    }

    /// Sets the cap of `?wait_ms=` on attach.
    pub fn with_attach_max_wait(mut self, max: Duration) -> Self {
        self.attach_max_wait = max;
        self
    }

    /// Sets the floor of `/invoke`'s polling grid tick — tests only.
    pub fn with_invoke_poll_floor(mut self, floor: Option<Duration>) -> Self {
        self.invoke_poll_floor = floor;
        self
    }

    /// Attaches the cluster rule book store.
    pub fn with_rules(mut self, rules: Option<RuleStore>) -> Self {
        self.rules = rules;
        self
    }

    /// The node's routing path — usable without HTTP.
    pub fn router(&self) -> &Router {
        &self.router
    }

    pub fn cluster(&self) -> &ClusterConfig {
        &self.router.cluster
    }
}

// ─────────────────────────────────────────────────────────── tenant (2.3)

/// The tenant an admin request acts in: the API key's tenant — **never** from a
/// self-declared path or header. A key with the `Cluster` permission may administer
/// another tenant with `?tenant=`, and so may a `Provision` key (the control plane) on
/// the routes its permission gate lets through — provisioning routes only (identity,
/// metering); other roles sending `?tenant=` ⇒ `403` (silently ignoring it would let the
/// person typing it believe they are looking at the other tenant).
fn acting_tenant(
    who: &crate::auth::Identity,
    q: &HashMap<String, String>,
) -> ApiResult<cairn_store::TenantId> {
    match q.get("tenant") {
        None => Ok(who.tenant.clone()),
        Some(t) if who.may_pick_tenant() => cairn_store::TenantId::new(t)
            .map_err(|e| ApiError(StatusCode::BAD_REQUEST, e.to_string())),
        Some(_) => Err(ApiError(
            StatusCode::FORBIDDEN,
            "only a cluster-admin key (or provision, on provisioning routes) may choose a tenant with ?tenant="
                .into(),
        )),
    }
}

/// `404` if invocation `id` does not belong to the caller's tenant — **the same** answer
/// as "no such invocation": saying "it exists but is not yours" confirms to someone
/// guessing ids that they guessed right. A `cluster-admin` key sees every tenant.
///
/// The tenant is read from entry 0 (one GET on the leader), or from the journal on a
/// follower when this node does not lead that range.
/// The audit note of a command by `action` on `target`.
/// Tenant: the key's, unless the key sees every tenant (cluster-admin) — then the
/// actor reads the target entity's tenant under the engine.
pub(super) fn audit_note(
    who: &crate::auth::Identity,
    meta: Option<&crate::audit::RequestMeta>,
    action: &str,
    target: serde_json::Value,
) -> Option<crate::audit::AuditNote> {
    let tenant = (!who.all_tenants()).then_some(&who.tenant);
    Some(crate::audit::AuditNote::new(
        meta, who, tenant, action, target,
    ))
}

/// The optional extractor of [`crate::audit::RequestMeta`].
pub(super) type Meta = Option<axum::Extension<crate::audit::RequestMeta>>;

async fn ensure_visible(
    st: &AppState,
    who: &crate::auth::Identity,
    id: &InvocationId,
) -> ApiResult<()> {
    if who.all_tenants() {
        return Ok(());
    }
    let raw = *id;
    let (_, head) = st
        .router
        .ask_range(cairn_store::partition_key_of(&raw), |e| async move {
            e.head(raw).await
        })
        .await
        .ok_or_else(range_settling)?;
    let tenant = match head {
        Ok(h) => h.map(|t| t.tenant),
        Err(e) if e.is_fenced() => match follower_journal_head(st, &raw).await {
            Some(rows) => rows
                .first()
                .map(|(_, ty, p)| {
                    cairn_runtime::JournalEntry::new(
                        cairn_protocol::MessageType::from(*ty),
                        Bytes::copy_from_slice(p),
                    )
                })
                .and_then(|e| cairn_engine::input_head(&e))
                .map(|(t, _)| t.tenant),
            None => {
                return Err(ApiError::from_actor_routed(
                    e,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    st,
                    &raw,
                ))
            }
        },
        Err(e) => {
            return Err(ApiError::from_actor_routed(
                e,
                StatusCode::INTERNAL_SERVER_ERROR,
                st,
                &raw,
            ))
        }
    };
    match tenant {
        Some(t) if t == who.tenant => Ok(()),
        _ => Err(ApiError(StatusCode::NOT_FOUND, "no such invocation".into())),
    }
}

/// The tenant of invocation `id`: the caller's tenant (already through
/// [`ensure_visible`]) — except for `cluster-admin`, who sees every tenant, so it must be
/// actually read.
async fn invocation_tenant_or(
    st: &AppState,
    who: &crate::auth::Identity,
    id: &InvocationId,
) -> ApiResult<cairn_store::TenantId> {
    if !who.all_tenants() {
        return Ok(who.tenant.clone());
    }
    let raw = *id;
    let (_, head) = st
        .router
        .ask_range(cairn_store::partition_key_of(&raw), |e| async move {
            e.head(raw).await
        })
        .await
        .ok_or_else(range_settling)?;
    head.map_err(|e| ApiError::from_actor_routed(e, StatusCode::INTERNAL_SERVER_ERROR, st, &raw))?
        .map(|t| t.tenant)
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "no such invocation".into()))
}

/// Whether deployment `id` belongs to `tenant` — `404` as if it did not exist otherwise.
fn deployment_of_tenant<'a>(
    catalog: &'a crate::registry::Catalog,
    id: &str,
    tenant: &cairn_store::TenantId,
) -> ApiResult<&'a crate::discovery::Deployment> {
    catalog
        .get(id)
        .filter(|d| d.tenant == tenant.as_str())
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, format!("no deployment {id:?}")))
}

/// The router of the **ingress port**: the customer API + admin, behind the API key
/// layer ([`crate::auth::authenticate`]). Does not serve `/internal/*` — that is
/// [`internal_router`], on another listener.
pub fn router(state: AppState) -> HttpRouter {
    let auth = state.auth.clone();
    let ui = state.ui.clone();
    let ui_auth = state.auth.clone();
    let router = HttpRouter::new()
        .route("/health", get(|| async { "ok" }))
        // The `proxy` read path for segmented values: the token is the permission, no
        // API key.
        .route("/values/v1/{token}", get(values::get_segment))
        // The calling key's identity — the console asks when it opens.
        .route("/auth/whoami", get(whoami))
        .route("/identity", get(identity::published))
        .route("/tenant", get(get_tenant))
        .route("/audit", get(audit::get_audit))
        .route("/deployments", post(register).get(list_deployments))
        .route(
            "/deployments/{id}",
            get(get_deployment).delete(delete_deployment),
        )
        .route(
            "/services/{service}/revision",
            axum::routing::put(set_revision),
        )
        .route("/services", get(list_services))
        .route("/invoke/{service}/{handler}", post(invoke))
        .route("/invoke/{service}/{handler}/send", post(invoke_send))
        // **Scoped** paths. Not an alias: the scope decides the partition, decides the
        // keys of the four state tables, and is the only thing that can turn on a
        // concurrency cap. The unscoped paths above stay as they are and are **subject
        // to no limit** besides the partition's turn cap.
        .route(
            "/scope/{scope}/invoke/{service}/{handler}",
            post(scoped_invoke),
        )
        .route(
            "/scope/{scope}/invoke/{service}/{handler}/send",
            post(scoped_invoke_send),
        )
        // Workflows: the run id is deterministic from `(scope, service, key)`, so these
        // two doors need no id — they compute it. See [`workflow_id`].
        .route("/workflow/{service}/{key}/attach", post(workflow_attach))
        .route("/workflow/{service}/{key}/output", get(workflow_output))
        // The run id, computed only — the console finds a workflow by key.
        .route("/workflow/{service}/{key}/id", get(workflow_id_of))
        .route(
            "/scope/{scope}/workflow/{service}/{key}/attach",
            post(scoped_workflow_attach),
        )
        .route(
            "/scope/{scope}/workflow/{service}/{key}/output",
            get(scoped_workflow_output),
        )
        .route(
            "/scope/{scope}/workflow/{service}/{key}/id",
            get(scoped_workflow_id_of),
        )
        .route(
            "/scope/{scope}/promises/{service}/{key}/{name}/resolve",
            post(scoped_resolve_promise),
        )
        .route(
            "/scope/{scope}/promises/{service}/{key}/{name}/reject",
            post(scoped_reject_promise),
        )
        .route(
            "/scope/{scope}/promises/{service}/{key}/{name}",
            get(scoped_peek_promise),
        )
        // Four flow-control endpoints, one per question an operator asks of the
        // scheduler: the rules, the limits per counter, the queues, where things are
        // blocked.
        .route(
            "/rules",
            get(list_rules).post(put_rule).delete(delete_tenant_rule),
        )
        .route("/rules/{*pattern}", get(get_rule).delete(delete_rule))
        .route("/limits", get(list_limits))
        .route("/vqueues", get(list_vqueues))
        .route("/scheduler", get(scheduler_status))
        .route("/invocations", get(list_invocations))
        .route("/query", post(crate::query::http::post_query))
        .route("/invocations/{id}", get(status))
        .route("/invocations/{id}/journal", get(journal))
        .route("/invocations/{id}/streams/{name}", get(streams::stream_sse))
        .route("/invocations/{id}/logs", get(streams::logs))
        .route("/invocations/{id}/spans", get(streams::spans))
        .route("/invocations/{id}/artifacts", get(artifacts::list))
        .route("/invocations/{id}/artifacts/{sha}", get(artifacts::get_one))
        .route("/invocations/{id}/cancel", post(cancel))
        .route("/invocations/{id}/purge", post(purge))
        .route("/invocations/{id}/signal", post(send_signal))
        .route("/invocations/{id}/attach", post(attach_by_id))
        .route("/invocations/{id}/resume", post(resume))
        .route("/invocations/{id}/replay", post(replay_invocation))
        .route("/invocations/{id}/migrate", post(migrate_invocation))
        .route("/invocations/{id}/restart", post(restart_invocation))
        .route("/invocations/batch/{op}", post(batch::batch))
        .route("/deployments/{id}/replay-check", post(replay_check))
        // Server-side schedules.
        .route("/schedules", get(schedules::list_schedules))
        .route(
            "/schedules/{name}",
            get(schedules::get_schedule)
                .put(schedules::put_schedule)
                .delete(schedules::delete_schedule),
        )
        .route("/schedules/{name}/pause", post(schedules::pause_schedule))
        .route("/schedules/{name}/resume", post(schedules::resume_schedule))
        .route(
            "/schedules/{name}/trigger",
            post(schedules::trigger_schedule),
        )
        .route(
            "/scope/{scope}/schedules",
            get(schedules::scoped_list_schedules),
        )
        .route(
            "/scope/{scope}/schedules/{name}",
            get(schedules::scoped_get_schedule)
                .put(schedules::scoped_put_schedule)
                .delete(schedules::scoped_delete_schedule),
        )
        .route(
            "/scope/{scope}/schedules/{name}/pause",
            post(schedules::scoped_pause_schedule),
        )
        .route(
            "/scope/{scope}/schedules/{name}/resume",
            post(schedules::scoped_resume_schedule),
        )
        .route(
            "/scope/{scope}/schedules/{name}/trigger",
            post(schedules::scoped_trigger_schedule),
        )
        .route("/metering", get(metering_view))
        .route("/metering/hours", get(metering_hours))
        .route("/ranges/windows", get(list_windows))
        .route("/ranges/stats", get(range_load::range_stats))
        .route("/ranges/suggestions", get(range_load::range_suggestions))
        .route("/ranges/{id}/window", axum::routing::put(set_window))
        .route("/awakeables/{id}/resolve", post(resolve_awakeable))
        .route("/awakeables/{id}/reject", post(reject_awakeable))
        .route(
            "/promises/{service}/{key}/{name}/resolve",
            post(resolve_promise),
        )
        .route(
            "/promises/{service}/{key}/{name}/reject",
            post(reject_promise),
        )
        .route("/promises/{service}/{key}/{name}", get(peek_promise))
        .route("/cluster", get(cluster_view))
        .route("/metrics", get(metrics_text))
        .route("/ranges/{id}/split", post(split_range))
        .route("/ranges/merge", post(merge_ranges))
        .route("/ranges/ops", get(list_range_ops))
        .route("/ranges/ops/{id}", get(get_range_op))
        .route("/admin/tick", post(tick))
        .route("/admin/keys", post(create_key).get(list_keys))
        .route("/admin/tenants", get(list_tenant_settings))
        .route(
            "/admin/tenants/{tenant}",
            axum::routing::put(put_tenant_settings),
        )
        .route(
            "/admin/tenants/{tenant}/state",
            axum::routing::put(put_tenant_state),
        )
        .route("/admin/tenants/{tenant}/purge", get(get_tenant_purge))
        .route("/archive", get(archive::list_archive))
        .route("/archive/{id}", get(get_archive))
        .route("/admin/keys/{id}", axum::routing::delete(delete_key))
        .route("/admin/keys/{id}/revocation", get(key_revocation))
        .route("/admin/oidc", get(oidc::get_oidc).put(oidc::put_oidc))
        .route(
            "/admin/oidc/clients/{id}",
            axum::routing::put(oidc::put_client).delete(oidc::delete_client),
        )
        .route("/admin/oidc/revoked", post(oidc::post_revoked));
    // Observability — a separate block, see `observe`.
    let router = router
        .route("/stats/handlers", get(observe::stats_handlers))
        .route("/stats/slow-runs", get(observe::stats_slow_runs))
        .route("/stats/history", get(observe::stats_history))
        .route("/cluster/nodes", get(ops::nodes))
        .route("/admin/nodewal", get(ops::nodewal))
        .route("/admin/scrub", get(ops::scrub))
        .route("/admin/trash", get(ops::trash))
        .route("/metering/pricing", get(ops::pricing))
        .route(
            "/admin/capture",
            post(observe::create_capture).get(observe::list_captures),
        )
        .route(
            "/admin/capture/{tenant}/{id}",
            get(observe::get_capture_bundle),
        )
        .route("/admin/quarantine", get(observe::list_quarantine))
        .route(
            "/admin/quarantine/{tenant}/{service}/{handler}",
            axum::routing::delete(observe::delete_quarantine),
        );
    // GC fences and plan — a separate block.
    let router = router
        .route("/admin/cluster-version", get(cluster_version))
        // Reports of `cairn-ctl --report-to-bucket`.
        .route("/admin/ops-reports", get(list_ops_reports))
        .route("/admin/ops-reports/{id}", get(get_ops_report))
        .route("/admin/gc/plan", get(gc_plan))
        .route("/admin/gc/barriers", get(list_gc_barriers))
        .route(
            "/admin/gc/barriers/{id}",
            get(get_gc_barrier).put(put_gc_barrier),
        );
    // Failpoints: the route only exists in builds with the feature — a release binary
    // returns 404 as for any unknown path.
    #[cfg(feature = "failpoints")]
    let router = router.route(
        "/admin/failpoints",
        post(set_failpoint).get(list_failpoints),
    );
    let sync = state.registry_sync.clone();
    let forward_state = state.clone();
    let audit_sink = (state.audit.clone(), auth.keys.clone());
    let console_audit = state.audit.clone();
    let meta_auth = auth.clone();
    let router = router
        .with_state(state)
        // A successful cluster-level registry write ⇒ nudge the registry refresh loop and
        // tell peers. Sits **inside** the key layer: refused requests never get here.
        .layer(axum::middleware::from_fn_with_state(
            sync,
            crate::registry_sync::on_change,
        ))
        // Audit of actions on objects: **inside** the key layer (needs an identity),
        // **outside** the registry change layer (the ACK's `202 pending` goes into the
        // outcome).
        .layer(axum::middleware::from_fn_with_state(
            audit_sink,
            crate::audit::object_layer,
        ))
        // Forwarding to the leader **inside** the key layer (security review): this layer
        // buffers the body (≤ 2 MiB) to run the request twice, so placing it outside the
        // key layer would read unauthenticated bodies on the public listener. A request
        // that has not passed auth here has nothing to forward (`401`/`403` carry no
        // "not the leader" marker); the leader authenticates again from the original
        // headers.
        .layer(axum::middleware::from_fn_with_state(
            forward_state,
            forward::layer,
        ))
        // `layer`, not `route_layer`: unknown paths require a key too — an
        // unauthenticated caller cannot tell real routes from missing ones.
        .layer(axum::middleware::from_fn_with_state(
            auth,
            crate::auth::authenticate,
        ))
        // IP (behind a trusted proxy), user agent, request id for the audit.
        .layer(axum::middleware::from_fn_with_state(
            meta_auth,
            crate::audit::capture,
        ))
        // Outermost: count every response of the public API by status class — including
        // the `401`/`429` of inner layers — and measure ack latency.
        .layer(axum::middleware::from_fn(crate::metrics::http::observe));
    // The web console **outside** the key layer: static files under the base path are
    // open to everyone; other requests go on into auth as before — see `crate::ui`.
    ui.wrap(router, ui_auth, console_audit)
}

/// The router of the **internal port**: `/internal/*` only, requests signed with the
/// cluster secret only ([`crate::auth::require_cluster_signature`]). This listener's
/// address is what the node advertises to peers; clients have no need to reach it.
pub fn internal_router(state: AppState) -> HttpRouter {
    let secret = state.cluster_secret.clone();
    HttpRouter::new()
        .route("/internal/inbound", post(inbound))
        .route("/internal/forward", post(forward::internal_forward))
        .route("/internal/inbound/batch", post(inbound_batch))
        .route("/internal/deployments/usage", get(internal_pinned))
        .route("/internal/ranges/{id}/resign", post(resign_range))
        .route(
            "/internal/ranges/{id}/tenant-purge",
            post(tenant_purge_rows),
        )
        .route(
            "/internal/ranges/{id}/tenant-purge-check",
            post(tenant_purge_check),
        )
        .route("/internal/ranges/{id}/coordinator", get(range_coordinator))
        .route("/internal/session/changed", post(session_changed))
        .route("/internal/heartbeat", post(heartbeat))
        .route("/internal/owner/changed", post(owner_changed))
        .route("/internal/registry/changed", post(registry_changed))
        .route("/internal/registry/apply", post(registry_apply))
        .route("/internal/stats/handlers", get(observe::internal_stats))
        .route(
            "/internal/stats/slow-runs",
            get(observe::internal_slow_runs),
        )
        .route("/internal/cluster/nodes", get(ops::internal_nodes))
        .route("/internal/admin/nodewal", get(ops::internal_nodewal))
        .route("/internal/admin/scrub", get(ops::internal_scrub))
        .route("/internal/invocations", get(observe::internal_invocations))
        .route("/internal/schedules", get(schedules::internal_schedules))
        .route(
            "/internal/schedules/counts",
            get(schedules::internal_schedule_counts),
        )
        .route(
            "/internal/query/scan",
            post(crate::query::http::internal_scan),
        )
        .route(
            "/internal/ranges/stats",
            get(range_load::internal_range_stats),
        )
        .route(
            "/internal/ranges/suggestions",
            get(range_load::internal_range_suggestions),
        )
        .route(
            "/internal/ranges/merge-facts",
            get(range_load::internal_merge_facts),
        )
        .with_state(state)
        // The handler body cap = the cap the signature layer has already read: without
        // this line the `Bytes` extractor cuts at axum's default 2 MiB, a larger
        // inter-range message gets `413` forever and blocks the head of the
        // destination's outbox queue. A forwarded request rebuilt in `internal_forward`
        // does not carry this extension — the ingress keeps its 2 MiB cap.
        .layer(axum::extract::DefaultBodyLimit::max(
            crate::auth::MAX_INTERNAL_BODY,
        ))
        .layer(axum::middleware::from_fn_with_state(
            secret,
            crate::auth::require_cluster_signature,
        ))
}

// ─────────────────────────────────────────────────────────── helpers

/// The table says the key belongs to a range the node has no engine for yet (a range
/// just created by a split/merge, the `follow_table` loop not done building it):
/// **transient** — `503`, not "unknown id".
fn range_settling() -> ApiError {
    ApiError(
        StatusCode::SERVICE_UNAVAILABLE,
        "the range holding this id is changing (split/merge) — retry later".into(),
    )
}

pub fn hex(id: &InvocationId) -> String {
    cairn_store::id_hex(id)
}

pub fn unhex(s: &str) -> Option<InvocationId> {
    cairn_store::id_from_hex(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_round_trip() {
        let id = [0xABu8; cairn_store::ID_LEN];
        assert_eq!(hex(&id).len(), 2 * cairn_store::ID_LEN);
        assert_eq!(unhex(&hex(&id)), Some(id));
        assert_eq!(unhex("too-short"), None);
        assert_eq!(unhex(&"z".repeat(2 * cairn_store::ID_LEN)), None);
    }

    /// D-a: a workflow without a key has no owner — 400, not an id under a made-up pk
    /// (an earlier version fell back to `PartitionKey(0)`, or the scope's own pk).
    #[test]
    fn workflow_without_key_is_rejected() {
        let bob = Scope::new("bob").unwrap();
        for scope in [None, Some(&bob)] {
            match workflow_id(&cairn_store::TenantId::default(), scope, "Signup", "") {
                Err(e) => assert_eq!(e.status, StatusCode::BAD_REQUEST, "{scope:?}"),
                Ok(_) => panic!("empty workflow key still produced an id ({scope:?})"),
            }
        }
        let Ok(id) = workflow_id(&cairn_store::TenantId::default(), None, "Signup", "u1") else {
            panic!("a workflow with a key must produce an id");
        };
        assert_eq!(
            Some(cairn_store::partition_key_of(&id)),
            partition_key_for_target(
                &cairn_store::TenantId::default(),
                None,
                "Signup",
                "u1",
                None
            )
        );
    }
}
