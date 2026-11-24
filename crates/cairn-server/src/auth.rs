// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Auth.
//!
//! Two doors, two mechanisms, because there are two kinds of callers:
//!
//! - **The ingress port** (the customer API + admin): bearer API keys. Each key is tied
//!   to one tenant and one role — a permission set; the [`authenticate`] middleware looks
//!   the key up, compares its permission set with the route's ([`required_caps`]), then
//!   puts the [`Identity`] into the request's extensions — the tenant every handler
//!   reads. Keys are stored **hashed** (SHA-256) in the bucket's `auth/keys` object
//!   ([`Keys`]); the secret is revealed only once, at creation.
//! - **The internal port** (`/internal/*`, called only by other nodes): every request is
//!   HMAC-SHA256 signed with the **cluster secret** ([`ClusterSecret`]) — the
//!   [`require_cluster_signature`] middleware. Nobody outside the cluster can send fake
//!   inter-range messages, ask a range to be handed over, or count deployments on a
//!   node's behalf.
//!
//! mTLS is left for later: HMAC stops fake messages right away without bringing in a PKI.

use {
    axum::{
        extract::{Request, State},
        http::{HeaderMap, Method, StatusCode},
        middleware::Next,
        response::{IntoResponse, Response},
        Json,
    },
    cairn_store::TenantId,
    sha2::{Digest, Sha256},
    slatedb::object_store::{
        path::Path, Error as OsError, ObjectStore, ObjectStoreExt, PutMode, UpdateVersion,
    },
    std::sync::{Arc, Mutex, RwLock},
};

// Web console sessions: a cookie signed with a key derived from the cluster secret,
// `kid` checked again in the key registry on every request.
mod session;
pub use session::*;
// Identifying people through OIDC: the `auth/oidc` + `auth/revoked` registry, the SSO
// session cookie v2, the relying party.
pub mod oidc;

// ───────────────────────────────────────────────────────────── permissions and roles

/// A permission. A route declares the **permission set** that accepts it
/// ([`required_caps`]); a key carries a role, and a role is a fixed permission set
/// ([`Role::caps`]). A key can call a route ⇔ the two sets intersect ([`permits`]).
///
/// It replaces the earlier nested role chain `invoke ⊂ admin ⊂ cluster-admin`: the `view`
/// role reads everything `admin` reads without calling or changing anything, and the
/// control plane's `provision` role reads no run data — neither fits on a total order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Capability {
    /// Reading the tenant's data: invocation lists, journals, deployments, services,
    /// archive, queries, statistics, metering, limits, identity.
    Read,
    /// Calling: invoke, workflows, promises, awakeables, signals, cooperative cancel,
    /// schedules; reading the result of **one** invocation by id (whoever just got `202`).
    Invoke,
    /// Changing the tenant: registering/removing deployments, changing revisions, hard
    /// cancel, purge, resume, replay, migrate, restart, bulk commands.
    Operate,
    /// The control plane's work: API keys, tenant limits, per-tenant rate caps, request
    /// identity and metering of any tenant, the cluster version. Does **not** read run
    /// data (invocations, journals, state, `sys_*`, archive, artifacts, streams).
    Provision,
    /// The cluster's work: pattern rules, ranges, split/merge, GC, failpoints, capture,
    /// quarantine, metrics, and seeing across every tenant.
    Cluster,
}

impl Capability {
    pub const ALL: [Capability; 5] = [
        Self::Read,
        Self::Invoke,
        Self::Operate,
        Self::Provision,
        Self::Cluster,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Invoke => "invoke",
            Self::Operate => "operate",
            Self::Provision => "provision",
            Self::Cluster => "cluster",
        }
    }

    const fn bit(self) -> u8 {
        1 << self as u8
    }
}

/// A set of [`Capability`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct Caps(u8);

impl Caps {
    pub const NONE: Caps = Caps(0);

    pub const fn of(caps: &[Capability]) -> Caps {
        let mut bits = 0;
        let mut i = 0;
        while i < caps.len() {
            bits |= caps[i].bit();
            i += 1;
        }
        Caps(bits)
    }

    pub const fn has(self, c: Capability) -> bool {
        self.0 & c.bit() != 0
    }

    /// The two sets share at least one permission.
    pub const fn meets(self, other: Caps) -> bool {
        self.0 & other.0 != 0
    }

    /// Every permission of `other` is in `self`.
    pub const fn covers(self, other: Caps) -> bool {
        self.0 & other.0 == other.0
    }

    pub fn iter(self) -> impl Iterator<Item = Capability> {
        Capability::ALL.into_iter().filter(move |c| self.has(*c))
    }

    /// The permission names in the order of [`Capability::ALL`] — `whoami` returns this so
    /// the console can hide actions.
    pub fn names(self) -> Vec<&'static str> {
        self.iter().map(Capability::as_str).collect()
    }
}

use Capability::{Cluster, Invoke, Operate, Provision, Read};

/// The tenant permissions (read, call, change) — what a `provision` key never has.
pub const TENANT_CAPS: Caps = Caps::of(&[Read, Invoke, Operate]);

/// A key's role: a fixed permission set ([`Self::caps`]).
///
/// | Role | Permissions | Where it exists |
/// |---|---|---|
/// | `invoke` | Invoke | registry `/1`, `/2` |
/// | `view` | Read | registry `/2` (after the `auth/keys-v2` gate) |
/// | `admin` | Read, Invoke, Operate | registry `/1`, `/2` |
/// | `provision` | Provision | registry `/2` (after the `auth/keys-v2` gate), hash `CAIRNFLOW_PROVISION_KEY_SHA256` |
/// | `cluster-admin` | all five | registry `/1`, `/2`, the bootstrap key (`CAIRNFLOW_ADMIN_KEY` or the hash `CAIRNFLOW_ADMIN_KEY_SHA256`) |
/// | `metrics` | no permission — only `GET /metrics` ([`permits`]) | `CAIRNFLOW_METRICS_KEY`, never in the registry |
///
/// No `Ord`: there is no permission order left to compare with `>=`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Only `GET /metrics` — for Prometheus, so the scraper does not have to hold a
    /// cluster-admin key. Not in the `auth/keys` registry (an unknown role would make old
    /// binaries fail to read the whole registry): only through `CAIRNFLOW_METRICS_KEY`.
    Metrics,
    /// Calls services, workflows, promises; reads the status of one invocation by id.
    Invoke,
    /// Read-only for **one tenant**: everything `admin` reads, without calling or
    /// changing anything.
    View,
    /// Everything of **one tenant**: deployments, services, invocation lists, journals,
    /// cancel/purge/resume/signal.
    Admin,
    /// The control plane's key: keys, per-tenant limits and rate caps, identity,
    /// metering, the cluster version — reads no tenant's run data.
    Provision,
    /// Everything of **the cluster**: flow control rules, split/merge, tick, `/cluster`,
    /// metrics, key management.
    ClusterAdmin,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Metrics => "metrics",
            Self::Invoke => "invoke",
            Self::View => "view",
            Self::Admin => "admin",
            Self::Provision => "provision",
            Self::ClusterAdmin => "cluster-admin",
        }
    }

    /// The role of a key **in the registry** (`auth/keys`, `POST /admin/keys`).
    /// `metrics` is not here: it is never written to the registry.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "invoke" => Some(Self::Invoke),
            "view" => Some(Self::View),
            "admin" => Some(Self::Admin),
            "provision" => Some(Self::Provision),
            "cluster-admin" => Some(Self::ClusterAdmin),
            _ => None,
        }
    }

    /// Roles that can be written into the `cairnflow-keys/1` registry — what binaries
    /// before the permission sets can read. Other roles only exist after the
    /// `auth/keys-v2` gate.
    pub fn in_v1_book(self) -> bool {
        matches!(self, Self::Invoke | Self::Admin | Self::ClusterAdmin)
    }

    pub const fn caps(self) -> Caps {
        match self {
            Self::Metrics => Caps::NONE,
            Self::Invoke => Caps::of(&[Invoke]),
            Self::View => Caps::of(&[Read]),
            Self::Admin => TENANT_CAPS,
            Self::Provision => Caps::of(&[Provision]),
            Self::ClusterAdmin => Caps::of(&Capability::ALL),
        }
    }

    /// Whether a key of role `self` may **grant** (create) or **revoke** a key of role
    /// `target` — the escalation guard:
    ///
    /// - has `Cluster` ⇒ every role;
    /// - has `Provision` (without `Cluster`) ⇒ only tenant roles (`invoke`, `view`,
    ///   `admin`). Not `cluster-admin` (more permissions than itself), not `provision`: a
    ///   leaked `provision` key cannot mint more peer keys to hang on after it is revoked,
    ///   and cannot revoke the operators' keys;
    /// - otherwise ⇒ nothing (the key routes already block it before the handler).
    pub fn may_grant(self, target: Role) -> bool {
        let mine = self.caps();
        if mine.has(Cluster) {
            return target != Self::Metrics;
        }
        mine.has(Provision) && TENANT_CAPS.covers(target.caps()) && target.caps() != Caps::NONE
    }
}

/// Who is calling — the middleware puts it into the extensions of every request that
/// passed auth.
///
/// Handlers read `tenant` from here (never from the path). A `--no-auth` cluster gives
/// every request [`Identity::unauthenticated`]: tenant `default`, full permissions.
#[derive(Clone, Debug)]
pub struct Identity {
    pub tenant: TenantId,
    pub role: Role,
    /// The key id used; `None` = the bootstrap key, auth off, or an SSO session.
    pub key_id: Option<String>,
    /// The user of an SSO session; `None` = an API key.
    pub user: Option<Arc<oidc::SsoUser>>,
}

impl Identity {
    pub fn unauthenticated() -> Self {
        Self {
            tenant: TenantId::default(),
            role: Role::ClusterAdmin,
            key_id: None,
            user: None,
        }
    }

    /// A key living in an environment variable (bootstrap, bootstrap `provision`, metrics
    /// token) — not a registry key, not an SSO session.
    pub fn is_env_key(&self) -> bool {
        self.key_id.is_none() && self.user.is_none()
    }

    pub fn can(&self, c: Capability) -> bool {
        self.role.caps().has(c)
    }

    /// Sees **every** tenant (the `Cluster` permission): reads other tenants'
    /// invocations, streams, queries. A `provision` key does not: it can only pick a
    /// tenant on provisioning routes ([`Self::may_pick_tenant`]).
    pub fn all_tenants(&self) -> bool {
        self.can(Cluster)
    }

    /// May pick a tenant with `?tenant=` on routes it has passed the permission gate for:
    /// `Cluster`, or `Provision` — routes open to `Provision` are only provisioning routes
    /// (identity, metering), no run data route
    /// (`tests/tenant_isolation.rs::provision_and_view_keys_over_the_matrix`).
    pub fn may_pick_tenant(&self) -> bool {
        self.can(Cluster) || self.can(Provision)
    }
}

/// What a route declares at the ingress port.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Need {
    /// Open, no key needed (only `GET /health`).
    Open,
    /// The key has at least one permission in this set.
    Any(Caps),
}

impl Need {
    const fn any(caps: &[Capability]) -> Need {
        Need::Any(Caps::of(caps))
    }

    /// The permission names, for the `403` message.
    fn describe(self) -> String {
        match self {
            Need::Open => "—".into(),
            Need::Any(c) => c.names().join(" | "),
        }
    }
}

/// Every registry key (not `metrics`): `whoami`.
const ANY_KEY: Need = Need::Any(Caps::of(&Capability::ALL));
const READ: Need = Need::any(&[Read]);
/// Reading **one** thing of the caller: the `invoke` key that just called, or a `view` key.
const READ_OR_INVOKE: Need = Need::any(&[Read, Invoke]);
const INVOKE: Need = Need::any(&[Invoke]);
const OPERATE: Need = Need::any(&[Operate]);
/// Reading one's own tenant, or any tenant with `?tenant=` for the control plane.
const READ_OR_PROVISION: Need = Need::any(&[Read, Provision]);
const PROVISION: Need = Need::any(&[Provision]);
const CLUSTER: Need = Need::any(&[Cluster]);

/// The permissions `method path` **declares** at the ingress port; `None` = no route
/// declares it — [`required_caps`] closes it down to `Cluster`.
///
/// One table in one place, by **path prefix**, rather than spread over every route: the
/// reader sees the whole policy on one screen. Every route of the router must have a
/// line here (`tests/tenant_isolation.rs::every_route_declares_capability`).
pub fn declared_caps(method: &Method, path: &str) -> Option<Need> {
    let seg: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    // `HEAD` is `GET` without a body (axum answers it with the `GET` route).
    let get = method == Method::GET || method == Method::HEAD;
    // Metrics: exactly `GET /metrics` verbatim, not through `/scope/*` (the `metrics`
    // role opens exactly this route — [`permits`]).
    if seg.as_slice() == ["metrics"] && get {
        return Some(CLUSTER);
    }
    // `/scope/{scope}/…` carries exactly the policy of its tail.
    let tail: &[&str] = match seg.as_slice() {
        ["scope", _, rest @ ..] => rest,
        all => all,
    };
    Some(match tail {
        ["health"] if get => Need::Open,
        // A segment read token: the permission lives in the token (a mac of the cluster
        // secret, short-lived), not in the API key.
        ["values", "v1", _] if get => Need::Open,
        // The key's own identity: every key can ask who it is.
        ["auth", "whoami"] if get => ANY_KEY,
        // Awakeables: an external system's webhook completes them with the tenant's
        // `invoke` key — the tenant is checked on the receiving side against entry 0 of
        // the invocation. `GET` (workflow results, promises) is a read: `view` may too.
        ["invoke", ..] | ["workflow", ..] | ["promises", ..] | ["awakeables", ..] => {
            if get {
                READ_OR_INVOKE
            } else {
                INVOKE
            }
        }
        // Reading the status of **one** invocation by id: whoever just got `202` must be
        // able to ask for the result with the very key that called. Not the journal — it
        // is inner content, the administrator's business.
        ["invocations", _] if get => READ_OR_INVOKE,
        // Attach by id: whoever just got `202` waits for the result with the very key
        // that called — like `/workflow/…/attach`.
        ["invocations", _, "attach"] if method == Method::POST => INVOKE,
        // Signals by name: an `invoke` key can already resolve awakeables — a signal by
        // index to the invocation itself — so sending a signal by name is its business
        // too. The tenant is checked in the route.
        ["invocations", _, "signal"] if method == Method::POST => INVOKE,
        // Server-side schedules: the work of an app within its own tenant — the app calls
        // `PUT` at startup. The tenant is the key's tenant; another tenant's schedules
        // (`?tenant=`) are checked in the route.
        ["schedules", ..] => {
            if get {
                READ_OR_INVOKE
            } else {
                INVOKE
            }
        }
        // User streams: what a client holding an `invoke` key needs to see — tokens,
        // progress. `$log`/`$trace` (and every `$…` name) are inner content like the
        // journal: `Read` only.
        ["invocations", _, "streams", name]
            if get && !name.starts_with('$') && !name.starts_with("%24") =>
        {
            READ_OR_INVOKE
        }
        // Artifacts: a handler's outputs — images, reports — that a client holding an
        // `invoke` key fetches; the tenant is checked in the route.
        ["invocations", _, "artifacts"] | ["invocations", _, "artifacts", _] if get => {
            READ_OR_INVOKE
        }
        // The tenant's public request identity keys: whoever registers deployments needs
        // them to turn on verification in the SDK; the control plane reads any tenant's
        // (`?tenant=`).
        ["identity"] if get => READ_OR_PROVISION,
        // Metering: a tenant sees its own; the control plane reads any tenant's
        // (`?tenant=`) — counts, not content. The price table belongs to the cluster.
        ["metering"] | ["metering", "hours"] if get => READ_OR_PROVISION,
        // The audit log: a tenant reads its own; the control plane reads any tenant's
        // (`?tenant=`) — who did what, no run data.
        ["audit"] if get => READ_OR_PROVISION,
        // The archive holds the tenant's purged invocations — read like the journal.
        ["invocations", ..]
        | ["deployments", ..]
        | ["services", ..]
        | ["archive", ..]
        // Per-handler summaries: a tenant key sees exactly its tenant — filtered in the
        // handler, like `/invocations`.
        | ["stats", ..]
        // The tenant's own limits; changing them is the business of `/admin/tenants`.
        | ["tenant"]
            if get =>
        {
            READ
        }
        // The query layer: `POST` but read-only. A tenant key sees exactly its tenant (by
        // the structure of the scan), `Cluster` every tenant.
        ["query"] if method == Method::POST => READ,
        // Every write command on the tenant's invocations, deployments, services: hard
        // cancel (cooperative cancel is lowered in [`required_caps_query`]), purge, resume,
        // replay, migrate, restart, bulk; registering, removing, `replay-check` (sends the
        // journal to the SDK); changing revisions.
        ["invocations", ..] | ["deployments", ..] | ["services", ..] => OPERATE,
        // The control plane's work: keys (create, list, revoke, re-check revocation),
        // tenant limits, the cluster version. The key handlers block escalation
        // ([`Role::may_grant`]).
        ["admin", "keys"] if get || method == Method::POST => PROVISION,
        ["admin", "keys", _] if method == Method::DELETE => PROVISION,
        ["admin", "keys", _, "revocation"] if get => PROVISION,
        // The OIDC relying party: the issuer, clients by hostname → tenant, SSO session
        // revocation.
        ["admin", "oidc"] if get || method == Method::PUT => PROVISION,
        ["admin", "oidc", "clients", _] if method == Method::PUT || method == Method::DELETE => {
            PROVISION
        }
        ["admin", "oidc", "revoked"] if method == Method::POST => PROVISION,
        ["admin", "tenants"] if get => PROVISION,
        ["admin", "tenants", _] if method == Method::PUT => PROVISION,
        // Tenant state: suspending/unsuspending is the control plane's business (spend
        // caps, abuse), not the tenant's.
        ["admin", "tenants", _, "state"] if method == Method::PUT => PROVISION,
        // Purge progress: phase, marks, hold entries, remaining object counts — no run
        // data.
        ["admin", "tenants", _, "purge"] if get => PROVISION,
        ["admin", "cluster-version"] if get => PROVISION,
        // Reports of `cairn-ctl --report-to-bucket`: backup results, gates, versions — no
        // secrets, no run data.
        ["admin", "ops-reports"] if get => PROVISION,
        ["admin", "ops-reports", _] if get => PROVISION,
        // Per-tenant rate caps (the `tenant` lines of the rule book): read, set, remove.
        // The handler cuts the rest for keys without `Cluster`: they do not see pattern
        // rules (patterns carry the customer's service names) and cannot set them.
        ["rules"] if get || method == Method::POST || method == Method::DELETE => {
            Need::any(&[Provision, Cluster])
        }
        // Pattern rules, queues, the scheduler seen across every tenant; split, tick,
        // `/cluster`, and every remaining `/admin/*` are the cluster's business.
        ["rules", ..]
        | ["limits"]
        | ["vqueues"]
        | ["scheduler"]
        | ["ranges", ..]
        | ["cluster", ..]
        | ["metering", "pricing"]
        | ["admin", ..] => CLUSTER,
        _ => return None,
    })
}

/// The minimum permissions for `method path` at the ingress port: [`declared_caps`], and
/// a **new** route nobody remembered to classify falls back to `Cluster` — closed by
/// default, not open by default.
pub fn required_caps(method: &Method, path: &str) -> Need {
    declared_caps(method, path).unwrap_or(CLUSTER)
}

/// Whether a key of role `role` can call `method path` (a route needing `need`): the
/// role's permission set intersects the route's. [`Role::Metrics`] has no permission:
/// exactly `GET /metrics` verbatim — no `HEAD`, no `/scope/*/metrics`, no other route,
/// including new routes not yet classified.
pub fn permits(role: Role, need: Need, method: &Method, path: &str) -> bool {
    match (role, need) {
        (Role::Metrics, _) => method == Method::GET && path == "/metrics",
        (_, Need::Open) => true,
        (r, Need::Any(c)) => r.caps().meets(c),
    }
}

/// Like [`required_caps`], with a path **including the query** (`path?query`):
/// cooperative cancel (`cancel?mode=signal`) is `Invoke` — the same route as hard
/// cancel, differing only in the parameter. Only `mode=signal` exactly lowers the
/// permission; every other value (including absent) keeps hard cancel's `Operate`.
pub fn required_caps_query(method: &Method, path_and_query: &str) -> Need {
    let (path, query) = match path_and_query.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (path_and_query, None),
    };
    // Lower the permission only when **every** `mode` is `signal`, decoded as the handler
    // decodes it (`Query<HashMap>`: the last value wins) — a `mode=signal` pair placed
    // before `mode=hard` must not open hard cancel to an `invoke` key.
    let cooperative = query.is_some_and(|q| {
        let mut modes = form_urlencoded::parse(q.as_bytes()).filter(|(k, _)| k == "mode");
        let first = modes.next();
        first.is_some_and(|(_, v)| v == "signal") && modes.all(|(_, v)| v == "signal")
    });
    if cooperative && method == Method::POST {
        let seg: Vec<&str> = path.trim_start_matches('/').split('/').collect();
        let tail: &[&str] = match seg.as_slice() {
            ["scope", _, rest @ ..] => rest,
            all => all,
        };
        // `batch/cancel` keeps `Operate` — `batch` is not an id.
        if matches!(tail, ["invocations", id, "cancel"] if *id != "batch") {
            return INVOKE;
        }
    }
    required_caps(method, path)
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    let v = headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?;
    let (scheme, token) = v.split_once(' ')?;
    scheme.eq_ignore_ascii_case("bearer").then(|| token.trim())
}

pub(crate) fn deny(status: StatusCode, msg: String) -> Response {
    let mut r = (status, Json(serde_json::json!({ "error": msg }))).into_response();
    if status == StatusCode::UNAUTHORIZED {
        r.headers_mut().insert(
            axum::http::header::WWW_AUTHENTICATE,
            axum::http::HeaderValue::from_static("Bearer"),
        );
    }
    r
}

/// The auth state of the ingress port.
#[derive(Clone)]
pub struct Auth {
    /// `false` = `--no-auth`: every request is [`Identity::unauthenticated`].
    pub required: bool,
    pub keys: Keys,
    /// The freshness of the key registry. `None` ⇒ never closes (hand-built routers, old
    /// tests).
    pub registry: Option<crate::registry_sync::RegistrySync>,
    /// Web console sessions. `None` ⇒ no cookies accepted (hand-built routers, no cluster
    /// secret yet).
    pub sessions: Option<Sessions>,
    /// The OIDC relying party. `None` ⇒ no SSO login (hand-built routers); existing SSO
    /// cookies can still be checked with `sessions`.
    pub oidc: Option<oidc::Rp>,
    /// Permission denials, aggregated for the audit. `None` ⇒ not recorded (hand-built
    /// routers).
    pub denials: Option<Arc<crate::audit::Denials>>,
    /// Tenant state: keys, cookies and SSO sessions of a `deleting`/`purged` tenant ⇒
    /// `403 tenant_deleting`. `None` ⇒ not blocked (hand-built routers).
    pub tenant_states: Option<cairn_engine::TenantLimitsSource>,
    /// The per-key request rate cap. `None` ⇒ no cap.
    pub key_rate: Option<Arc<KeyRate>>,
    /// Wrong Bearer tokens per client IP: a burst of [`BEARER_FAIL_BURST`], then one per
    /// [`BEARER_FAIL_EVERY`]; out of allowance ⇒ **wrong** attempts from that IP get `429`
    /// until it recovers. A correct key is never capped.
    pub bearer_failures: Arc<crate::rate::KeyedLimiter<std::net::IpAddr>>,
}

/// The burst of wrong Bearer tokens per IP before a wrong attempt gets `429`. Only wrong
/// attempts are capped: a correct key always passes, so the cap does not hurt customers
/// behind a NAT — it only slows down probing.
pub const BEARER_FAIL_BURST: u32 = 100;
/// The recovery interval of the wrong-Bearer allowance per IP.
pub const BEARER_FAIL_EVERY: std::time::Duration = std::time::Duration::from_secs(1);
/// Cap on the number of IPs tracked at once for [`Auth::bearer_failures`].
const BEARER_FAIL_TRACKED: usize = 10_000;

/// A new table of wrong Bearer tokens per IP.
pub fn bearer_failure_limiter() -> Arc<crate::rate::KeyedLimiter<std::net::IpAddr>> {
    Arc::new(crate::rate::KeyedLimiter::new(
        BEARER_FAIL_BURST,
        BEARER_FAIL_EVERY,
        BEARER_FAIL_TRACKED,
    ))
}

fn bearer_failures_capped(wait: std::time::Duration) -> Response {
    let mut r = (
        StatusCode::TOO_MANY_REQUESTS,
        Json(serde_json::json!({
            "error": "too many requests with an invalid API key from this address — retry after the Retry-After header",
            "code": "auth_failures_limited",
        })),
    )
        .into_response();
    if let Ok(v) = axum::http::HeaderValue::from_str(&wait.as_secs().max(1).to_string()) {
        r.headers_mut().insert(axum::http::header::RETRY_AFTER, v);
    }
    r
}

/// GCRA `N/S` per API key (or SSO person) at the ingress: a burst of at most `N`, then
/// one request every `S/N` seconds. Keys living in environment variables (bootstrap,
/// metrics token) do not count — they are the operators and the control plane's
/// reconciler. Counted on the node receiving the request.
pub struct KeyRate {
    rate: crate::tenants::DeployRate,
    /// `key → theoretical arrival time (ms)`. Not on the turn's hot path.
    tat: Mutex<std::collections::BTreeMap<String, u64>>,
}

/// The number of keys kept in the GCRA table before sweeping out keys that have fully recovered.
const KEY_RATE_SWEEP_AT: usize = 100_000;

impl KeyRate {
    pub fn new(rate: crate::tenants::DeployRate) -> Self {
        Self {
            rate,
            tat: Mutex::default(),
        }
    }

    /// Lets a request of `who` at `now_ms` pass — or `Err(ms to wait)`.
    pub fn admit(&self, who: &Identity, now_ms: u64) -> Result<(), u64> {
        let key = match (&who.key_id, &who.user) {
            (Some(k), _) => k.clone(),
            // By (client, sub): the same person in two environments has two budgets
            // (security review).
            (None, Some(u)) => format!("sso:{}:{}", u.client, u.sub),
            (None, None) => return Ok(()),
        };
        let window = u64::from(self.rate.per_s) * 1000;
        let interval = (window / u64::from(self.rate.count)).max(1);
        let tolerance = window.saturating_sub(interval);
        let Ok(mut tat) = self.tat.lock() else {
            return Ok(());
        };
        if tat.len() >= KEY_RATE_SWEEP_AT {
            tat.retain(|_, t| *t > now_ms);
        }
        // A valid TAT is never beyond `now + window`. The system clock going back ⇒ an
        // old TAT lies far in the future: clamp **and store it** — clamping only for the
        // comparison would leave the stored TAT far ahead next time, and the key blocked
        // forever (security review).
        let stored = tat.get(&key).copied().unwrap_or(0);
        let t = stored.min(now_ms.saturating_add(window)).max(now_ms);
        if t.saturating_sub(tolerance) > now_ms {
            if t < stored {
                tat.insert(key, t);
            }
            return Err(t - tolerance - now_ms);
        }
        tat.insert(key, t + interval);
        Ok(())
    }
}

fn key_rate_limited(wait_ms: u64) -> Response {
    let mut r = (
        StatusCode::TOO_MANY_REQUESTS,
        Json(serde_json::json!({
            "error": "too many requests with this API key — retry after the Retry-After header",
            "code": "key_rate_limited",
        })),
    )
        .into_response();
    if let Ok(v) = axum::http::HeaderValue::from_str(&wait_ms.div_ceil(1000).max(1).to_string()) {
        r.headers_mut().insert(axum::http::header::RETRY_AFTER, v);
    }
    r
}

impl Auth {
    /// The request's console session by cookie: the SSO cookie before the key cookie —
    /// the two kinds never open the same request together, whichever verifies first is
    /// used. Does not check the origin (the caller does).
    pub(crate) fn cookie_session(
        &self,
        headers: &axum::http::HeaderMap,
        ext: &axum::http::Extensions,
    ) -> Option<(Identity, Via)> {
        let sso = self.sessions.as_ref().and_then(|s| {
            let book = self.keys.oidc();
            if book.with(|cfg, _| cfg.clients.is_empty()) {
                return None;
            }
            let host = self.request_host(headers, ext);
            cookies_named(headers, oidc::SSO_COOKIE)
                .find_map(|c| oidc::verify_sso(s, c, book, host.as_deref(), now_s()))
        });
        sso.map(|(who, exp)| (who, Via::Sso { expires_at_s: exp }))
            .or_else(|| {
                self.sessions.as_ref().and_then(|s| {
                    // The name follows the request's `Secure`: when `Secure`, the plain
                    // `cairn_ui` copy (settable from a sibling origin) opens no session.
                    let fwd = s.trusted().forwarded_in(headers, ext);
                    let mut found = session_cookies(headers, s.cookie_name(fwd.as_ref()))
                        .filter_map(|c| s.verify(c, &self.keys));
                    let (who, exp) = found.next()?;
                    // Two copies that verify but name different identities ⇒ pick neither.
                    if found.any(|(o, _)| {
                        (&o.tenant, o.role, &o.key_id) != (&who.tenant, who.role, &who.key_id)
                    }) {
                        return None;
                    }
                    Some((who, Via::Cookie { expires_at_s: exp }))
                })
            })
    }

    /// Whether the request's identity (`headers`, `ext` captured at open) is **still**
    /// valid right now — for long-lived connections rechecking periodically: the key not
    /// revoked, the session not logged out, the registry still fresh, the tenant not
    /// being deleted.
    pub(crate) fn still_valid(
        &self,
        headers: &axum::http::HeaderMap,
        ext: &axum::http::Extensions,
    ) -> bool {
        if !self.required {
            return true;
        }
        let (who, cookie) = match bearer(headers) {
            Some(t) => (self.keys.authenticate(t), false),
            None => (self.cookie_session(headers, ext).map(|(w, _)| w), true),
        };
        let Some(who) = who else {
            return false;
        };
        if self.stale_via(&who, cookie) {
            return false;
        }
        !self.tenant_states.as_ref().is_some_and(|s| {
            !who.may_pick_tenant() && s.snapshot().of(&who.tenant).state.refuses_access()
        })
    }

    /// Auth off, no key store — for hand-built routers in tests.
    pub fn disabled() -> Self {
        Self {
            required: false,
            keys: Keys::new(None, None),
            registry: None,
            sessions: None,
            oidc: None,
            denials: None,
            tenant_states: None,
            key_rate: None,
            bearer_failures: bearer_failure_limiter(),
        }
    }

    /// A registry key — or an SSO session, relying on `auth/oidc` and `auth/revoked` read
    /// on the same tick — for which the node cannot confirm the registry is still fresh:
    /// it may have been revoked on another node. The bootstrap key is not in the
    /// registry: it is never stale.
    pub fn stale(&self, who: &Identity) -> bool {
        !who.is_env_key() && self.registry.as_ref().is_some_and(|r| !r.fresh())
    }

    /// Like [`Self::stale`], plus the logged-out session registry's own freshness when the
    /// identity comes from a cookie (security review): a session registry failing to read
    /// beyond the cap closes cookie sessions (`503`), not Bearer.
    pub fn stale_via(&self, who: &Identity, cookie: bool) -> bool {
        self.stale(who) || (cookie && self.keys.oidc().sessions_stale())
    }

    /// The request's hostname (lower case, no port): a trusted proxy's
    /// `X-Forwarded-Host`, otherwise `Host`.
    pub fn request_host(
        &self,
        headers: &HeaderMap,
        ext: &axum::http::Extensions,
    ) -> Option<String> {
        let fwd = self
            .sessions
            .as_ref()
            .and_then(|s| s.trusted().forwarded_in(headers, ext));
        request_host(headers, fwd.as_ref())
            .as_deref()
            .and_then(oidc::normalize_host)
    }
}

/// A trusted proxy's statement about `req` (`--trust-proxy`).
fn forwarded(auth: &Auth, req: &Request) -> Option<Forwarded> {
    auth.sessions
        .as_ref()
        .and_then(|s| s.trusted().forwarded_in(req.headers(), req.extensions()))
}

/// The request's `Host` verbatim (with the port): a trusted proxy's if any.
pub fn request_host(headers: &HeaderMap, fwd: Option<&Forwarded>) -> Option<String> {
    fwd.and_then(|f| f.host.clone()).or_else(|| {
        headers
            .get(axum::http::header::HOST)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    })
}

/// `403 tenant_deleting` — the same body shape as the ingress errors (`code`).
fn tenant_deleting(tenant: &TenantId) -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(serde_json::json!({
            "error": format!("tenant {tenant} is being deleted — its API keys and sessions can no longer be used"),
            "code": "tenant_deleting",
        })),
    )
        .into_response()
}

fn stale_keys() -> Response {
    crate::registry_gen::STALE_REJECTED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    deny(
        StatusCode::SERVICE_UNAVAILABLE,
        "node could not confirm that its key registry is fresh (lost the bucket and the coordinator?) — try another node or retry later"
            .into(),
    )
}

/// The ingress port's middleware. Wraps **the whole** router (including 404): an unknown
/// path must not answer an unauthenticated caller differently from a real path.
pub async fn authenticate(State(auth): State<Auth>, mut req: Request, next: Next) -> Response {
    let need = match req.uri().path_and_query() {
        Some(pq) => required_caps_query(req.method(), pq.as_str()),
        None => required_caps(req.method(), req.uri().path()),
    };
    // Bearer first; with no header, the console's session cookie. A **wrong** Bearer does
    // not fall back to the cookie: whoever sends a key wants exactly that key.
    // The client IP of a request carrying Bearer: behind a trusted proxy, the address in
    // `X-Forwarded-For`. Unknown (a forwarded request has no peer) ⇒ no cap.
    let bearer_ip = (auth.required && bearer(req.headers()).is_some())
        .then(|| {
            let peer = peer_ip(req.extensions());
            match &auth.sessions {
                Some(s) => s.trusted().client_ip(req.headers(), peer),
                None => peer,
            }
        })
        .flatten();
    let now = tokio::time::Instant::now();
    let (who, via) = if !auth.required {
        (Some(Identity::unauthenticated()), Via::NoAuth)
    } else if let Some(t) = bearer(req.headers()) {
        let who = auth.keys.authenticate(t);
        // Only **wrong** attempts cost the IP's allowance: a correct key is never capped
        // — someone behind the same NAT/proxy cannot lock out customers with a valid key.
        // A wrong attempt within the allowance ⇒ `401` as before; over the allowance ⇒
        // `429` (still counted).
        if who.is_none() {
            if let Some(ip) = &bearer_ip {
                // IPv6 grouped by /64 like the login limiter (security review): one
                // machine has a whole /64 to rotate addresses in.
                if let Err(wait) = auth
                    .bearer_failures
                    .take(&crate::rate::client_key(*ip), now)
                {
                    crate::metrics::auth_failures::note(
                        crate::metrics::auth_failures::Kind::Bearer,
                    );
                    return bearer_failures_capped(wait);
                }
            }
        }
        (who, Via::Bearer)
    } else {
        match auth.cookie_session(req.headers(), req.extensions()) {
            Some((who, via)) => (Some(who), via),
            None => (None, Via::Bearer),
        }
    };
    // Closed when unsure: a registry key for which the node has not been able to confirm
    // the registry is still fresh for over `2·deploy_refresh` — that key may have been
    // revoked on another node. The bootstrap key is not in the registry: not closed.
    if need != Need::Open
        && who
            .as_ref()
            .is_some_and(|w| auth.stale_via(w, via.is_cookie()))
    {
        return stale_keys();
    }
    // CSRF protection: cookies go with every request the browser sends to this host,
    // including from pages on another port — unsafe methods authenticated by cookie must
    // prove the same origin. Bearer is not checked. Behind a trusted proxy
    // (`--trust-proxy`) the host and scheme come from `X-Forwarded-*`.
    if via.is_cookie()
        && !is_safe(req.method())
        && !same_origin(req.headers(), forwarded(&auth, &req).as_ref())
    {
        return deny(
            StatusCode::FORBIDDEN,
            "requests with a session cookie must come from the console itself (cross-origin Origin / Sec-Fetch-Site) — tools use `Authorization: Bearer`"
                .into(),
        );
    }
    // The per-key rate cap **before** the permission check: a request getting `403` also
    // costs budget — otherwise a key lacking permissions could fire unknown paths without
    // a cap, each path one `Denials` aggregation key (security review). Unauthenticated
    // (`who = None`) does not get here: `401` still comes first.
    if let (true, Some(i), Some(rate)) = (need != Need::Open, &who, &auth.key_rate) {
        if let Err(wait) = rate.admit(i, cairn_store::entropy::now_ms()) {
            crate::metrics::auth_failures::KEY_RATE_LIMITED
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return key_rate_limited(wait);
        }
    }
    if need != Need::Open {
        match &who {
            None => {
                // Counted; a wrong Bearer also charges the per-IP allowance above.
                use crate::metrics::auth_failures::{note, Kind};
                note(if bearer(req.headers()).is_some() {
                    Kind::Bearer
                } else if req.headers().contains_key(axum::http::header::COOKIE) {
                    Kind::Cookie
                } else {
                    Kind::Missing
                });
                return deny(
                    StatusCode::UNAUTHORIZED,
                    "a valid API key is required: header `Authorization: Bearer <key>`".into(),
                );
            }
            Some(i) if !permits(i.role, need, req.method(), req.uri().path()) => {
                // Aggregated by `(tenant, person, action)` per minute.
                if let Some(d) = &auth.denials {
                    d.note(i, req.method().as_str(), req.uri().path());
                }
                return deny(
                    StatusCode::FORBIDDEN,
                    format!(
                        "API key with role `{}` lacks the capability for {} {} (needs one of: {})",
                        i.role.as_str(),
                        req.method(),
                        req.uri().path(),
                        need.describe()
                    ),
                );
            }
            Some(_) => {}
        }
    }
    // A tenant being deleted or deleted: every key, cookie and SSO session **of** it is
    // refused — including read routes. Keys that can pick a tenant (`cluster-admin`,
    // `provision`) are the operators and are not blocked: they need to read the state
    // and reverse it within 7 days.
    if let (true, Some(i), Some(states)) = (need != Need::Open, &who, &auth.tenant_states) {
        if !i.may_pick_tenant() && states.snapshot().of(&i.tenant).state.refuses_access() {
            return tenant_deleting(&i.tenant);
        }
    }
    if let Some(i) = who {
        req.extensions_mut().insert(i);
        req.extensions_mut().insert(via);
    }
    next.run(req).await
}

// ───────────────────────────────────────────────────────── cluster secret

/// The header carrying the timestamp (Unix seconds) of an internal request.
pub const TIMESTAMP_HEADER: &str = "x-cairn-timestamp";
/// The header carrying the hex signature of an internal request.
pub const SIGNATURE_HEADER: &str = "x-cairn-signature";
/// The maximum clock skew between signer and receiver. It is also the replay window of a
/// captured request: the signature carries no nonce, and the receiver does not remember
/// requests it has seen. Most internal paths are idempotent (inter-range messages with
/// deterministic keys, range handover, counting) — **not all**: `/internal/forward`
/// carries customer requests to the leader (replaying a `POST /invoke` without an
/// idempotency key is one more invocation), and the operational commands
/// (`tenant-purge`, `resign`, `admin/*`) take effect each time. The internal channel must
/// therefore be private (a private network or TLS); a leaked cluster secret is enough to
/// forge any request within this window.
pub const MAX_SKEW_S: u64 = 60;
/// The minimum length (bytes) of a cluster secret set with `CAIRNFLOW_CLUSTER_SECRET`:
/// shorter ⇒ the node refuses to start.
pub const MIN_ENV_SECRET: usize = 32;
/// The header carrying the `hex(sha256(body))` the signature covers (security review):
/// the receiver checks the signature over this hash **before** reading the body, then
/// compares it with the hash of the real body.
pub const DIGEST_HEADER: &str = "x-cairn-content-sha256";
/// The body cap of an internal request — inter-range messages carry call parameters.
/// Also the `DefaultBodyLimit` of `ingress::internal_router`: without it the handler's
/// `Bytes` extractor cuts at 2 MiB even though this layer has read this far.
pub(crate) const MAX_INTERNAL_BODY: usize = 256 * 1024 * 1024;
/// The cap on the body buffered **before** the signature is checked: a request without
/// [`DIGEST_HEADER`] (an older sender during a rolling upgrade) must read the whole body
/// before the signature can be checked.
pub const MAX_PRE_AUTH_BODY: usize = 4 * 1024 * 1024;

/// HMAC-SHA256 (RFC 2104) on top of `sha2` — ten lines, not worth a crate.
fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut k = [0u8; 64];
    if key.len() > 64 {
        k[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0u8; 64];
    let mut opad = [0u8; 64];
    for i in 0..64 {
        ipad[i] = k[i] ^ 0x36;
        opad[i] = k[i] ^ 0x5c;
    }
    let inner = Sha256::new()
        .chain_update(ipad)
        .chain_update(msg)
        .finalize();
    Sha256::new()
        .chain_update(opad)
        .chain_update(inner)
        .finalize()
        .into()
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

/// Compares two byte strings in time independent of where they first differ.
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

pub(crate) fn now_s() -> u64 {
    cairn_store::entropy::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn random<const N: usize>() -> [u8; N] {
    let mut b = [0u8; N];
    cairn_store::entropy::fill(&mut b);
    b
}

/// The secret shared by every node of the cluster.
///
/// Every node-to-node path goes through [`Self::send`] (the inter-range transport,
/// cluster-wide metric gathering, asking for a range handover during a merge, telling
/// peers at shutdown), so this is the only place where the test harness plugs in the
/// network filter ([`crate::netfilter`], feature `testkit`). Release builds do not have
/// that field.
#[derive(Clone)]
pub struct ClusterSecret {
    key: Arc<[u8]>,
    #[cfg(feature = "testkit")]
    net: Option<crate::netfilter::NetHook>,
}

impl std::fmt::Debug for ClusterSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ClusterSecret(…)")
    }
}

impl ClusterSecret {
    pub fn new(bytes: impl AsRef<[u8]>) -> Self {
        Self {
            key: Arc::from(bytes.as_ref()),
            #[cfg(feature = "testkit")]
            net: None,
        }
    }

    /// Makes every [`Self::send`] of node `node` go through `filter` (only with the
    /// `testkit` feature) — see [`crate::netfilter`].
    #[cfg(feature = "testkit")]
    pub fn with_net_filter(
        mut self,
        node: &str,
        filter: Arc<dyn crate::netfilter::NetFilter>,
    ) -> Self {
        self.net = Some(crate::netfilter::NetHook::new(node, filter));
        self
    }

    /// A 32-byte key derived from the cluster secret for a purpose `info` (HKDF-SHA256)
    /// — every node with the same secret gets the same key; this key opens nothing else.
    pub fn derive(&self, info: &[u8]) -> [u8; 32] {
        let prk = ring::hkdf::Salt::new(ring::hkdf::HKDF_SHA256, &[]).extract(&self.key);
        let info = [info];
        let okm = prk
            .expand(&info, ring::hkdf::HKDF_SHA256)
            .expect("32 bytes fit the HKDF-SHA256 output limit");
        let mut out = [0u8; 32];
        okm.fill(&mut out).expect("length matches HKDF_SHA256");
        out
    }

    /// 32 random bytes — for tests building routers by hand.
    pub fn random() -> Self {
        Self::new(random::<32>())
    }

    /// The cluster's secret: `from_env` if present (the `CAIRNFLOW_CLUSTER_SECRET`
    /// variable, at least [`MIN_ENV_SECRET`] bytes),
    /// otherwise the `auth/cluster-secret` object in the bucket — the first node creates it
    /// with `PutMode::Create` (32 random bytes), every later node reads it back.
    ///
    /// The bucket is enough: whoever can read the bucket can already read everything the
    /// cluster has; what must be stopped is someone who can only reach the nodes'
    /// **network**. Operators who want the secret outside the bucket set the environment
    /// variable on every node.
    pub async fn resolve(
        store: &Arc<dyn ObjectStore>,
        prefix: &Path,
        from_env: Option<&str>,
    ) -> Result<Self, String> {
        if let Some(s) = from_env.filter(|s| !s.is_empty()) {
            // A secret the operator sets must be long enough — a configuration error, no
            // retry (`boot::text_verdict`).
            if s.len() < MIN_ENV_SECRET {
                return Err(format!(
                    "cluster secret CAIRNFLOW_CLUSTER_SECRET is too short ({} bytes, needs ≥ {MIN_ENV_SECRET}) — generate it with `openssl rand -hex 32`",
                    s.len()
                ));
            }
            return Ok(Self::new(s.as_bytes()));
        }
        let path = object_path(prefix, "auth/cluster-secret");
        let fresh = random::<32>();
        // Ambiguity resolved by reading back: 32 random bytes — a match is ours. Before
        // that, a write that landed but reported an error made the node die at startup.
        match cairn_store::cas::put_resolving(store, &path, fresh.to_vec().into(), PutMode::Create)
            .await
        {
            Ok(cairn_store::cas::Cas::Written(_)) => return Ok(Self::new(fresh)),
            Ok(cairn_store::cas::Cas::Lost) => {}
            Err(e) => return Err(format!("cannot create cluster secret {path}: {e}")),
        }
        let got = store
            .get(&path)
            .await
            .map_err(|e| format!("cannot read cluster secret {path}: {e}"))?
            .bytes()
            .await
            .map_err(|e| format!("cannot read cluster secret {path}: {e}"))?;
        if got.len() < 16 {
            return Err(format!(
                "cluster secret {path} is too short ({} bytes)",
                got.len()
            ));
        }
        Ok(Self::new(&got))
    }

    /// The signature of `method \n path?query \n ts \n hex(sha256(body))`.
    pub fn signature(&self, method: &str, path_and_query: &str, ts: u64, body: &[u8]) -> String {
        self.signature_of_digest(method, path_and_query, ts, &hex(&Sha256::digest(body)))
    }

    /// Like [`Self::signature`], with `hex(sha256(body))` given.
    fn signature_of_digest(
        &self,
        method: &str,
        path_and_query: &str,
        ts: u64,
        digest_hex: &str,
    ) -> String {
        let msg = format!("{method}\n{path_and_query}\n{ts}\n{digest_hex}");
        hex(&hmac_sha256(&self.key, msg.as_bytes()))
    }

    /// Signs a built request (sets three headers: the timestamp, the signature, and the
    /// body hash [`DIGEST_HEADER`] the signature covers). A streaming body cannot be
    /// signed — every internal request has its body in RAM.
    pub fn sign(&self, mut req: reqwest::Request) -> reqwest::Request {
        let ts = now_s();
        let url = req.url();
        let path = match url.query() {
            Some(q) => format!("{}?{q}", url.path()),
            None => url.path().to_string(),
        };
        let body = req.body().and_then(|b| b.as_bytes()).unwrap_or_default();
        let digest = hex(&Sha256::digest(body));
        let sig = self.signature_of_digest(req.method().as_str(), &path, ts, &digest);
        let h = req.headers_mut();
        h.insert(TIMESTAMP_HEADER, ts.into());
        if let Ok(v) = sig.parse() {
            h.insert(SIGNATURE_HEADER, v);
        }
        if let Ok(v) = digest.parse() {
            h.insert(DIGEST_HEADER, v);
        }
        req
    }

    /// Builds, signs, sends.
    pub async fn send(
        &self,
        client: &reqwest::Client,
        req: reqwest::RequestBuilder,
    ) -> reqwest::Result<reqwest::Response> {
        let req = self.sign(req.build()?);
        // The harness's network filter: applied **after** signing, so a message held and
        // then released carries exactly the signature and timestamp of the send — held
        // beyond `MAX_SKEW_S`, the receiver refuses it, like a really old message.
        #[cfg(feature = "testkit")]
        if let Some(net) = &self.net {
            return net.send(client, req).await;
        }
        crate::egress::execute(client, req).await
    }

    /// Checks a received request. `Err` = the reason, for the log.
    pub fn verify(
        &self,
        method: &str,
        path_and_query: &str,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<(), &'static str> {
        self.verify_digest(method, path_and_query, headers, &hex(&Sha256::digest(body)))
    }

    /// Checks the signature over a given `hex(sha256(body))` — no body needed yet. The
    /// caller still has to compare the real body's hash with `digest_hex`.
    fn verify_digest(
        &self,
        method: &str,
        path_and_query: &str,
        headers: &HeaderMap,
        digest_hex: &str,
    ) -> Result<(), &'static str> {
        let ts: u64 = headers
            .get(TIMESTAMP_HEADER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .ok_or("missing timestamp")?;
        let sig = headers
            .get(SIGNATURE_HEADER)
            .and_then(|v| v.to_str().ok())
            .and_then(unhex)
            .ok_or("missing signature")?;
        if now_s().abs_diff(ts) > MAX_SKEW_S {
            return Err("timestamp skew exceeds the limit");
        }
        let want = unhex(&self.signature_of_digest(method, path_and_query, ts, digest_hex))
            .unwrap_or_default();
        if !same(&sig, &want) {
            return Err("bad signature");
        }
        Ok(())
    }
}

/// The internal port's middleware: only requests correctly signed with the cluster
/// secret pass. `None` (a hand-built router with no secret attached) ⇒ refuse
/// everything — closed by default.
///
/// No buffering of strangers' bodies (security review): a request carrying
/// [`DIGEST_HEADER`] has the signature over the declared hash checked **before** any body
/// byte is read, and only then is the body read (up to [`MAX_INTERNAL_BODY`]) and its
/// hash compared. A request without it (an older sender during a rolling upgrade) is
/// buffered only up to [`MAX_PRE_AUTH_BODY`] before the check.
pub async fn require_cluster_signature(
    State(secret): State<Option<ClusterSecret>>,
    req: Request,
    next: Next,
) -> Response {
    let Some(secret) = secret else {
        return deny(
            StatusCode::UNAUTHORIZED,
            "node has no cluster secret yet".into(),
        );
    };
    let (parts, body) = req.into_parts();
    let path = parts
        .uri
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or_else(|| parts.uri.path());
    let refuse = |why: &'static str| {
        tracing::warn!(path = %parts.uri.path(), reason = why, "refusing internal request that is not correctly signed");
        deny(StatusCode::UNAUTHORIZED, format!("internal request: {why}"))
    };
    let body = match parts.headers.get(DIGEST_HEADER) {
        Some(v) => {
            let Some(claimed) = v.to_str().ok().and_then(unhex).filter(|d| d.len() == 32) else {
                return refuse("malformed body hash header");
            };
            if let Err(why) =
                secret.verify_digest(parts.method.as_str(), path, &parts.headers, &hex(&claimed))
            {
                return refuse(why);
            }
            let body = match axum::body::to_bytes(body, MAX_INTERNAL_BODY).await {
                Ok(b) => b,
                Err(e) => return deny(StatusCode::BAD_REQUEST, format!("cannot read body: {e}")),
            };
            if !same(&Sha256::digest(&body), &claimed) {
                return refuse("body does not match the signed hash");
            }
            body
        }
        None => {
            let body = match axum::body::to_bytes(body, MAX_PRE_AUTH_BODY).await {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!(path = %parts.uri.path(), "refusing internal request without a body hash, body over the pre-authentication limit");
                    return deny(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        format!(
                            "internal request does not carry {DIGEST_HEADER}: body over {MAX_PRE_AUTH_BODY} bytes or read failed ({e})"
                        ),
                    );
                }
            };
            if let Err(why) = secret.verify(parts.method.as_str(), path, &parts.headers, &body) {
                return refuse(why);
            }
            body
        }
    };
    next.run(Request::from_parts(parts, axum::body::Body::from(body)))
        .await
}

// ───────────────────────────────────────────────────────────── keys

fn object_path(prefix: &Path, name: &str) -> Path {
    let p = prefix.as_ref();
    if p.is_empty() {
        Path::from(name)
    } else {
        Path::from(format!("{p}/{name}"))
    }
}

/// An API key, **without** the secret — only its hash.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyRecord {
    pub id: String,
    pub hash: [u8; 32],
    pub tenant: TenantId,
    pub role: Role,
    pub created_ms: u64,
}

impl KeyRecord {
    /// The first 8 hex characters of the hash — so operators can match it in reports
    /// without printing the whole hash.
    pub fn hash_prefix(&self) -> String {
        hex(&self.hash[..4])
    }
}

/// The registry of the first three roles (`invoke`, `admin`, `cluster-admin`) — what
/// binaries before the permission sets can read.
const KEYS_HEADER_V1: &str = "cairnflow-keys/1";
/// The registry adding the `view` and `provision` roles. Only written when the
/// `auth/keys-v2` gate is on ([`cairn_store::migrate::KEY_ROLES`]).
const KEYS_HEADER_V2: &str = "cairnflow-keys/2";

/// `auth/keys`: the header, then one line per key `id hash role created_ms tenant` —
/// text like the rule book and the catalog, readable by eye in the bucket. `/1` and `/2`
/// share the line layout; they differ in the roles allowed.
///
/// `v2 = false` (the gate not yet on) while the registry has roles outside `/1` ⇒ the
/// write is refused, never lowering a role or dropping a line: an old binary reading
/// `/1` must not see a registry different from the real one.
fn encode_keys(keys: &[KeyRecord], v2: bool) -> Result<Vec<u8>, KeyError> {
    if !v2 {
        if let Some(k) = keys.iter().find(|k| !k.role.in_v1_book()) {
            return Err(KeyError::RolesDisabled(k.role));
        }
    }
    let mut out = format!("{}\n", if v2 { KEYS_HEADER_V2 } else { KEYS_HEADER_V1 });
    for k in keys {
        out.push_str(&format!(
            "{} {} {} {} {}\n",
            k.id,
            hex(&k.hash),
            k.role.as_str(),
            k.created_ms,
            k.tenant
        ));
    }
    Ok(out.into_bytes())
}

/// Strict decoding: `/1` only the first three roles (a `/1` writer never writes other
/// roles — if present the registry is broken), `/2` the five storable roles; an unknown
/// role or a broken line ⇒ the whole registry is broken (a misread key grants the wrong
/// permissions). A header newer than `/2` ⇒ an error asking to upgrade the binary.
fn decode_keys(raw: &[u8]) -> Result<Vec<KeyRecord>, String> {
    let text = std::str::from_utf8(raw).map_err(|e| format!("auth/keys is not UTF-8: {e}"))?;
    let mut lines = text.lines();
    let header = lines.next().unwrap_or_default();
    let v1 = match header {
        KEYS_HEADER_V1 => true,
        KEYS_HEADER_V2 => false,
        _ => {
            if let Some(e) = cairn_store::newer_header("auth/keys", KEYS_HEADER_V2, header) {
                return Err(e.to_string());
            }
            return Err(format!(
                "auth/keys is missing header {KEYS_HEADER_V1} or {KEYS_HEADER_V2}"
            ));
        }
    };
    let mut out = Vec::new();
    for line in lines.filter(|l| !l.trim().is_empty()) {
        let bad = || format!("auth/keys: malformed line {line:?}");
        let f: Vec<&str> = line.split(' ').collect();
        let [id, hash, role, created, tenant] = f.as_slice() else {
            return Err(bad());
        };
        let hash: [u8; 32] = unhex(hash)
            .and_then(|h| h.try_into().ok())
            .ok_or_else(bad)?;
        let role = Role::parse(role)
            .filter(|r| !v1 || r.in_v1_book())
            .ok_or_else(bad)?;
        out.push(KeyRecord {
            id: id.to_string(),
            hash,
            role,
            created_ms: created.parse().map_err(|_| bad())?,
            tenant: TenantId::new(tenant).map_err(|_| bad())?,
        });
    }
    Ok(out)
}

/// Errors of the key registry writer.
#[derive(Debug)]
pub enum KeyError {
    /// This role needs the `auth/keys-v2` gate (`cairn-ctl cluster enable-key-roles`)
    /// — the API returns `409 key_roles_disabled`.
    RolesDisabled(Role),
    /// The bucket read/write failed, or the registry is broken.
    Store(String),
    /// The CAS loop on the registry ran out of attempts because others keep writing
    /// (concurrent create/revoke commands): **nothing written**, sending again is safe —
    /// the API returns `503 keys_busy` with `Retry-After`.
    Contended,
}

impl std::fmt::Display for KeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RolesDisabled(r) => write!(
                f,
                "role `{}` needs the auth/keys-v2 gate — `cairn-ctl cluster enable-key-roles --apply` after every node runs a binary that knows step {}",
                r.as_str(),
                cairn_store::migrate::KEY_ROLES
            ),
            Self::Store(e) => f.write_str(e),
            Self::Contended => f.write_str(
                "the API key registry kept changing under concurrent writes — nothing was written; retry",
            ),
        }
    }
}

impl From<String> for KeyError {
    fn from(e: String) -> Self {
        Self::Store(e)
    }
}

/// The result of [`Keys::delete_if`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Deleted {
    Deleted,
    /// That key is not in the registry just read.
    Absent,
    /// Present, but the caller may not revoke this role ([`Role::may_grant`]); the
    /// registry is unchanged.
    Refused(Role),
}

/// The cluster's API key store: the `auth/keys` object + a copy in RAM to look up on
/// every request.
///
/// Re-read periodically (like the deployment registry) so keys created/deleted on other
/// nodes take effect here; created/deleted on this node they take effect at once. The
/// bootstrap key (`CAIRNFLOW_ADMIN_KEY`) is never written down: it is the cluster-admin
/// of the `default` tenant, living in the node's environment variable.
#[derive(Clone)]
pub struct Keys(Arc<KeysInner>);

struct KeysInner {
    /// (store, the `auth/keys` path, the cluster prefix — for `cluster-version`).
    store: Option<(Arc<dyn ObjectStore>, Path, Path)>,
    bootstrap: Option<[u8; 32]>,
    /// The hash of the metrics token (`CAIRNFLOW_METRICS_KEY`) — like the bootstrap key:
    /// lives in an environment variable, written down nowhere.
    metrics: Option<[u8; 32]>,
    /// The hash of the bootstrap `provision` key (`CAIRNFLOW_PROVISION_KEY_SHA256`) — like
    /// the bootstrap key: lives in an environment variable, not in the registry, cannot be
    /// revoked through the API (rotating = set a new hash and restart).
    provision: Option<[u8; 32]>,
    cache: RwLock<Vec<KeyRecord>>,
    /// `auth/oidc` + `auth/revoked`: the same refresh tick, the same ACK path, the same
    /// freshness rule as the key registry.
    oidc: oidc::OidcBook,
    /// The apply order of `cache`: a read takes its ticket before the GET, a write of the
    /// node itself takes its ticket after the CAS; a copy with an older ticket than the
    /// one applied is dropped. It replaces the old `local_writes` fence — that fence only
    /// blocked the node's **own** writes, not an old read of the refresh loop overwriting
    /// the copy an apply request just read (a peer's key revocation came back to life
    /// until the next round).
    order: crate::registry_sync::ReadOrder,
}

impl Keys {
    /// `store` = (object store, cluster prefix); `None` = RAM only (tests).
    pub fn new(store: Option<(Arc<dyn ObjectStore>, &Path)>, bootstrap: Option<&str>) -> Self {
        let oidc = oidc::OidcBook::new(store.as_ref().map(|(s, p)| (Arc::clone(s), *p)));
        Self(Arc::new(KeysInner {
            store: store.map(|(s, p)| (s, object_path(p, "auth/keys"), p.clone())),
            bootstrap: bootstrap
                .filter(|k| !k.is_empty())
                .map(|k| Sha256::digest(k.as_bytes()).into()),
            metrics: None,
            provision: None,
            oidc,
            cache: RwLock::new(Vec::new()),
            order: crate::registry_sync::ReadOrder::default(),
        }))
    }

    /// Attaches the metrics token (role [`Role::Metrics`]) — at construction, before
    /// sharing `Clone`s. Already checked in [`check_metrics_key`].
    pub fn with_metrics_key(mut self, key: Option<&str>) -> Self {
        let inner = Arc::get_mut(&mut self.0)
            .unwrap_or_else(|| panic!("Keys::with_metrics_key called after Clone was shared"));
        inner.metrics = key
            .filter(|k| !k.is_empty())
            .map(|k| Sha256::digest(k.as_bytes()).into());
        self
    }

    /// Attaches the bootstrap keys given **as hashes** ([`EnvKeys`]) — at construction,
    /// before sharing `Clone`s. The cluster-admin hash takes the place of the plain
    /// bootstrap key (already checked in [`EnvKeys::resolve`]: not both set).
    pub fn with_env_hashes(mut self, env: &EnvKeys) -> Self {
        let inner = Arc::get_mut(&mut self.0)
            .unwrap_or_else(|| panic!("Keys::with_env_hashes called after Clone was shared"));
        if let Some(h) = env.admin_sha256 {
            inner.bootstrap = Some(h);
        }
        inner.provision = env.provision_sha256;
        self
    }

    /// No key can call the API — not even the bootstrap key (plain or hashed) or the
    /// bootstrap `provision` key. The binary refuses to start with auth on in this state:
    /// nobody could call anything, and that error should show when the command is typed,
    /// not at the first request. The metrics token does not count: it cannot create any
    /// key.
    pub fn is_empty(&self) -> bool {
        self.0.bootstrap.is_none()
            && self.0.provision.is_none()
            && self.0.cache.read().map(|c| c.is_empty()).unwrap_or(true)
    }

    async fn load(&self) -> Result<(Vec<KeyRecord>, Option<UpdateVersion>), String> {
        let Some((store, path, _)) = &self.0.store else {
            let cached = self.0.cache.read().map(|c| c.clone()).unwrap_or_default();
            return Ok((cached, None));
        };
        match store.get(path).await {
            Ok(got) => {
                let version = UpdateVersion {
                    e_tag: got.meta.e_tag.clone(),
                    version: got.meta.version.clone(),
                };
                let raw = got.bytes().await.map_err(|e| e.to_string())?;
                Ok((decode_keys(&raw)?, Some(version)))
            }
            Err(OsError::NotFound { .. }) => Ok((Vec::new(), None)),
            Err(e) => Err(format!("cannot read {path}: {e}")),
        }
    }

    /// Re-reads `auth/keys` into RAM — unless this node has just written the cache after
    /// a bucket write while this read was in flight: the copy read may be older, and the
    /// next refresh reads again.
    ///
    /// Also re-reads `auth/oidc` + `auth/revoked` ([`oidc::OidcBook::refresh`]): a
    /// successful refresh means all three objects are fresh — the caller confirms
    /// freshness and answers ACKs based on that. `auth/revoked-sessions` is re-read too,
    /// but its errors do not make the refresh fail: it has its own freshness and only
    /// closes cookie sessions.
    pub async fn refresh(&self) -> Result<(), String> {
        let ticket = self.0.order.ticket();
        let (keys, _) = self.load().await?;
        cairn_store::failpoint!("auth::keys::refresh::loaded");
        self.apply_read(ticket, keys);
        self.0.oidc.refresh().await
    }

    /// Writes `keys` into the cache if its ticket is newer than the copy being applied.
    fn apply_read(&self, ticket: u64, keys: Vec<KeyRecord>) {
        self.0.order.apply_if_newer(ticket, || {
            if let Ok(mut c) = self.0.cache.write() {
                *c = keys;
            }
        });
    }

    /// The cluster's OIDC registry.
    pub fn oidc(&self) -> &oidc::OidcBook {
        &self.0.oidc
    }

    /// Like [`Self::refresh`], and answers whether the registry **just read from the
    /// bucket** has key `id`. Not answered from the cache: the cache may hold a copy newer
    /// than the read (a write of the node itself landing in between), but never an older
    /// one — key ids are never reissued, so "the read does not see it" is enough for "the
    /// registry no longer has it".
    pub async fn reload_contains(&self, id: &str) -> Result<bool, String> {
        let ticket = self.0.order.ticket();
        let (keys, _) = self.load().await?;
        let present = keys.iter().any(|k| k.id == id);
        self.apply_read(ticket, keys);
        self.0.oidc.refresh().await?;
        Ok(present)
    }

    /// Whether the `auth/keys-v2` gate is on
    /// ([`cairn_store::migrate::ClusterVersion::key_roles_on`]) — reads `cluster-version`
    /// on every question: only the registry write path (admin work, rare) asks. No bucket
    /// (RAM-only registry, tests) ⇒ on: no old binary reads this registry.
    pub async fn roles_enabled(&self) -> Result<bool, String> {
        let Some((store, _, prefix)) = &self.0.store else {
            return Ok(true);
        };
        cairn_store::migrate::read_cluster_version(store, prefix)
            .await
            .map(|cv| cv.is_some_and(|(cv, _)| cv.key_roles_on()))
            .map_err(|e| format!("cannot read cluster-version: {e}"))
    }

    /// Read-modify-write with a CAS, like the rule book: when two operators create keys at
    /// the same time, both keys remain. `edit` returns `(result, changed)`: not changed ⇒
    /// nothing is written.
    ///
    /// The header written follows the `auth/keys-v2` gate, read right before each
    /// attempt: off ⇒ `/1` (old binaries may still run), and a registry carrying roles
    /// outside `/1` is not written ([`KeyError::RolesDisabled`]); on ⇒ `/2`.
    async fn update<T>(
        &self,
        mut edit: impl FnMut(&mut Vec<KeyRecord>) -> (T, bool),
    ) -> Result<T, KeyError> {
        for _ in 0..8 {
            let (mut keys, version) = self.load().await?;
            let (out, changed) = edit(&mut keys);
            if !changed {
                return Ok(out);
            }
            if let Some((store, path, _)) = &self.0.store {
                let body = encode_keys(&keys, self.roles_enabled().await?)?;
                let mode = match version {
                    Some(v) => PutMode::Update(v),
                    None => PutMode::Create,
                };
                // Ambiguity resolved by reading back: a new key carries a random id and
                // hash, so identical content is our own write — earlier, object_store's
                // retry pushed the record again (two entries with the same id) and the
                // delete reported 404. A store that cannot CAS (`LocalFileSystem`,
                // `--data-dir`): overwrite — the same rule as the rule book, where a
                // missing CAS already prevents forming a cluster.
                match cairn_store::cas::put_resolving_or_overwrite(store, path, body.into(), mode)
                    .await
                {
                    Ok(cairn_store::cas::Cas::Written(_)) => {}
                    Ok(cairn_store::cas::Cas::Lost) => continue,
                    Err(e) => return Err(KeyError::Store(format!("cannot write {path}: {e}"))),
                }
            }
            // The ticket **after** the write won (see `ReadOrder`).
            let ticket = self.0.order.ticket();
            self.apply_read(ticket, keys);
            return Ok(out);
        }
        Err(KeyError::Contended)
    }

    /// Creates a new key. Returns the record and the **secret** — the only time the
    /// secret leaves the node; afterwards only its hash remains. A role outside the `/1`
    /// registry while the `auth/keys-v2` gate is off ⇒ [`KeyError::RolesDisabled`],
    /// nothing written.
    pub async fn create(
        &self,
        tenant: TenantId,
        role: Role,
    ) -> Result<(KeyRecord, String), KeyError> {
        if role == Role::Metrics {
            return Err(KeyError::Store(
                "the metrics role does not go into the key registry".into(),
            ));
        }
        if !role.in_v1_book() && !self.roles_enabled().await? {
            return Err(KeyError::RolesDisabled(role));
        }
        let secret = format!("cf_{}", hex(&random::<24>()));
        let record = KeyRecord {
            id: format!("k_{}", hex(&random::<8>())),
            hash: Sha256::digest(secret.as_bytes()).into(),
            tenant,
            role,
            created_ms: cairn_store::entropy::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        };
        let r = record.clone();
        self.update(move |keys| {
            keys.push(r.clone());
            ((), true)
        })
        .await?;
        Ok((record, secret))
    }

    /// Deletes key `id`. `false` = no such key.
    pub async fn delete(&self, id: &str) -> Result<bool, KeyError> {
        Ok(self.delete_if(id, |_| true).await? == Deleted::Deleted)
    }

    /// Deletes key `id` if `allow(its role)` — the check runs on the registry **just
    /// read** inside the CAS loop, not on the cache (a key created on another node that
    /// the cache has not seen yet is still judged by its right role).
    pub async fn delete_if(
        &self,
        id: &str,
        allow: impl Fn(Role) -> bool,
    ) -> Result<Deleted, KeyError> {
        self.update(|keys| match keys.iter().position(|k| k.id == id) {
            None => (Deleted::Absent, false),
            Some(i) if !allow(keys[i].role) => (Deleted::Refused(keys[i].role), false),
            Some(i) => {
                keys.remove(i);
                (Deleted::Deleted, true)
            }
        })
        .await
    }

    /// Every stored key (not the bootstrap key), in creation order.
    pub fn list(&self) -> Vec<KeyRecord> {
        self.0.cache.read().map(|c| c.clone()).unwrap_or_default()
    }

    /// The `kid` of the bootstrap key: `bootstrap:<first 8 bytes of the hash>` — changing
    /// the environment variable changes the `kid`, so its old sessions die.
    fn bootstrap_kid(&self) -> Option<String> {
        self.0
            .bootstrap
            .map(|b| format!("bootstrap:{}", hex(&b[..8])))
    }

    /// Like [`Self::authenticate`], with the `kid` to sign the session: the key id in the
    /// registry, or `bootstrap:…`.
    pub fn login(&self, token: &str) -> Option<(Identity, String)> {
        let who = self.authenticate(token)?;
        // The metrics token does not open the console: it has no `key_id` either, and the
        // branch below treats a missing `key_id` as the bootstrap key — a cookie signed
        // with the bootstrap `kid` is a cluster-admin cookie. Nor does a `provision` key:
        // the control plane calls the API with Bearer, the console has no screen for it,
        // and a cookie of it would only be one more place to leak.
        if matches!(who.role, Role::Metrics | Role::Provision) {
            return None;
        }
        let kid = match &who.key_id {
            Some(id) => id.clone(),
            None => self.bootstrap_kid()?,
        };
        Some((who, kid))
    }

    /// The **current** identity of key `kid` — `None` when the key has been revoked (or
    /// the bootstrap key has changed).
    pub fn identify(&self, kid: &str) -> Option<Identity> {
        if kid.starts_with("bootstrap:") {
            return (self.bootstrap_kid().as_deref() == Some(kid)).then(|| Identity {
                tenant: TenantId::default(),
                role: Role::ClusterAdmin,
                key_id: None,
                user: None,
            });
        }
        let cache = self.0.cache.read().ok()?;
        cache.iter().find(|k| k.id == kid).map(|k| Identity {
            tenant: k.tenant.clone(),
            role: k.role,
            key_id: Some(k.id.clone()),
            user: None,
        })
    }

    /// The identity of a secret, if it is a valid key.
    pub fn authenticate(&self, token: &str) -> Option<Identity> {
        let hash: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        if self.0.bootstrap.is_some_and(|b| same(&b, &hash)) {
            return Some(Identity {
                tenant: TenantId::default(),
                role: Role::ClusterAdmin,
                key_id: None,
                user: None,
            });
        }
        if self.0.metrics.is_some_and(|m| same(&m, &hash)) {
            return Some(Identity {
                tenant: TenantId::default(),
                role: Role::Metrics,
                key_id: None,
                user: None,
            });
        }
        // The bootstrap `provision` key: no `key_id`, like every key living in an
        // environment variable — never "stale" by the registry ([`Auth::stale`]), and
        // [`Self::login`] refuses this role before reaching the bootstrap `kid` branch.
        if self.0.provision.is_some_and(|p| same(&p, &hash)) {
            return Some(Identity {
                tenant: TenantId::default(),
                role: Role::Provision,
                key_id: None,
                user: None,
            });
        }
        let cache = self.0.cache.read().ok()?;
        cache
            .iter()
            .find(|k| same(&k.hash, &hash))
            .map(|k| Identity {
                tenant: k.tenant.clone(),
                role: k.role,
                key_id: Some(k.id.clone()),
                user: None,
            })
    }
}

/// A node's auth configuration ([`crate::node::NodeConfig::auth`]).
///
/// Default = **off** — for tests building nodes in process. The binary defaults to
/// **on** and is only turned off with `--no-auth`.
#[derive(Clone, Default)]
pub struct AuthConfig {
    /// `true` ⇒ every route except `/health` requires an API key.
    pub required: bool,
    /// The bootstrap key (cluster-admin, tenant `default`), never stored.
    pub admin_key: Option<String>,
    /// The cluster secret; `None` ⇒ read/create the `auth/cluster-secret` object in the bucket.
    pub cluster_secret: Option<String>,
    /// The read-only metrics token (`CAIRNFLOW_METRICS_KEY`): role [`Role::Metrics`],
    /// exactly `GET /metrics`. `None` ⇒ none.
    pub metrics_key: Option<String>,
    /// `CAIRNFLOW_ADMIN_KEY_SHA256`: 64 hex of `sha256(cluster-admin key)` — a bootstrap
    /// key the node only knows by hash; the plain key lives in a break-glass store. Not
    /// set together with [`Self::admin_key`].
    pub admin_key_sha256: Option<String>,
    /// `CAIRNFLOW_PROVISION_KEY_SHA256`: 64 hex of `sha256(provision key)` — the control
    /// plane's key, role `provision`, tenant `default`, not in the registry, no console
    /// login.
    pub provision_key_sha256: Option<String>,
}

/// The hashes of the bootstrap keys set through environment variables, checked. Built
/// with [`EnvKeys::resolve`] at startup: a broken or duplicate value ⇒ the node does
/// not start.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EnvKeys {
    /// The cluster-admin key hash from `CAIRNFLOW_ADMIN_KEY_SHA256`.
    pub admin_sha256: Option<[u8; 32]>,
    /// The `provision` key hash from `CAIRNFLOW_PROVISION_KEY_SHA256`.
    pub provision_sha256: Option<[u8; 32]>,
}

/// The environment variable carrying the cluster-admin key hash.
pub const ADMIN_KEY_SHA256_ENV: &str = "CAIRNFLOW_ADMIN_KEY_SHA256";
/// The environment variable carrying the `provision` key hash.
pub const PROVISION_KEY_SHA256_ENV: &str = "CAIRNFLOW_PROVISION_KEY_SHA256";

/// `sha256` as 64 hex digits (upper or lower case; whitespace at both ends is ignored —
/// secret tools loading from a file often add a newline). Anything other than 64 hex
/// digits ⇒ an error naming the variable, never the value.
pub fn parse_sha256_hex(var: &str, v: &str) -> Result<[u8; 32], String> {
    let t = v.trim();
    let bytes = (t.len() == 64 && t.bytes().all(|b| b.is_ascii_hexdigit()))
        .then(|| unhex(&t.to_ascii_lowercase()))
        .flatten()
        .ok_or_else(|| {
            format!(
                "{var} must be the 64-digit sha256 hex of the API key (got {} characters) — compute it with `printf %s \"$KEY\" | sha256sum`; set the hash, not the plaintext",
                t.len()
            )
        })?;
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

impl EnvKeys {
    /// Checks and resolves the bootstrap keys of `auth` (plus the metrics token):
    ///
    /// - hashes are 64 hex, otherwise an error;
    /// - not both `CAIRNFLOW_ADMIN_KEY` and `CAIRNFLOW_ADMIN_KEY_SHA256` (two bootstrap
    ///   cluster-admin keys — one of them would silently win);
    /// - the four keys (cluster-admin, `provision`, metrics) must have pairwise distinct
    ///   hashes: a duplicate would make [`Keys::authenticate`] return the stronger role
    ///   for exactly the secret the operator thinks is narrow.
    pub fn resolve(auth: &AuthConfig) -> Result<EnvKeys, String> {
        let nonempty = |v: &Option<String>| v.clone().filter(|s| !s.trim().is_empty());
        let admin_plain = auth.admin_key.as_deref().filter(|k| !k.is_empty());
        let metrics_plain = auth.metrics_key.as_deref().filter(|k| !k.is_empty());
        if let Some(m) = metrics_plain {
            check_metrics_key(m, admin_plain)?;
        }
        let admin_sha256 = nonempty(&auth.admin_key_sha256)
            .map(|v| parse_sha256_hex(ADMIN_KEY_SHA256_ENV, &v))
            .transpose()?;
        let provision_sha256 = nonempty(&auth.provision_key_sha256)
            .map(|v| parse_sha256_hex(PROVISION_KEY_SHA256_ENV, &v))
            .transpose()?;
        // The plain bootstrap key is something people type by hand: the same floor as
        // the cluster secret, the metrics token and the identity master. The message
        // does not name the key.
        if let Some(k) = admin_plain {
            if k.len() < MIN_ENV_SECRET {
                return Err(format!(
                    "bootstrap cluster-admin API key CAIRNFLOW_ADMIN_KEY is too short ({} bytes, needs ≥ {MIN_ENV_SECRET}) — generate one with `printf 'cf_%s' \"$(openssl rand -hex 24)\"`, or set {ADMIN_KEY_SHA256_ENV} to its sha256 instead",
                    k.len()
                ));
            }
        }
        if admin_plain.is_some() && admin_sha256.is_some() {
            return Err(format!(
                "both CAIRNFLOW_ADMIN_KEY and {ADMIN_KEY_SHA256_ENV} are set — pick one: plaintext (dev, self-run cluster) or hash (plaintext only in the break-glass vault)"
            ));
        }
        let digest = |k: &str| -> [u8; 32] { Sha256::digest(k.as_bytes()).into() };
        let admin = admin_sha256.or_else(|| admin_plain.map(digest));
        let metrics = metrics_plain.map(digest);
        let named = [
            ("bootstrap cluster-admin API key", admin),
            (PROVISION_KEY_SHA256_ENV, provision_sha256),
            ("CAIRNFLOW_METRICS_KEY", metrics),
        ];
        for (i, (a, ha)) in named.iter().enumerate() {
            for (b, hb) in &named[i + 1..] {
                if let (Some(x), Some(y)) = (ha, hb) {
                    if same(x, y) {
                        return Err(format!(
                            "{a} and {b} are the same API key — each role needs its own secret"
                        ));
                    }
                }
            }
        }
        Ok(EnvKeys {
            admin_sha256,
            provision_sha256,
        })
    }
}

/// Checks the metrics token at startup: ≥ [`MIN_ENV_SECRET`] bytes (Prometheus sends it
/// on every scrape, and it opens metrics across every tenant), and different from the
/// bootstrap key — a duplicate would make [`Keys::authenticate`] return cluster-admin for
/// exactly the secret the operator thinks is read-only.
pub fn check_metrics_key(metrics: &str, admin: Option<&str>) -> Result<(), String> {
    if metrics.len() < MIN_ENV_SECRET {
        return Err(format!(
            "metrics token CAIRNFLOW_METRICS_KEY is too short ({} bytes, needs ≥ {MIN_ENV_SECRET}) — generate it with `openssl rand -hex 32`",
            metrics.len()
        ));
    }
    if admin == Some(metrics) {
        return Err(
            "CAIRNFLOW_METRICS_KEY equals CAIRNFLOW_ADMIN_KEY — the metrics token must be its own secret"
                .into(),
        );
    }
    Ok(())
}

impl std::fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthConfig")
            .field("required", &self.required)
            .field("admin_key", &self.admin_key.as_ref().map(|_| "…"))
            .field("cluster_secret", &self.cluster_secret.as_ref().map(|_| "…"))
            .field("metrics_key", &self.metrics_key.as_ref().map(|_| "…"))
            .field(
                "admin_key_sha256",
                &self.admin_key_sha256.as_ref().map(|_| "…"),
            )
            .field(
                "provision_key_sha256",
                &self.provision_key_sha256.as_ref().map(|_| "…"),
            )
            .finish()
    }
}

#[cfg(test)]
mod key_rate_tests {
    use super::*;

    fn key(id: &str) -> Identity {
        Identity {
            tenant: TenantId::new("acme").unwrap(),
            role: Role::Admin,
            key_id: Some(id.into()),
            user: None,
        }
    }

    /// **The system clock going back does not block an active key** (security review).
    /// Order: `10/1` (a burst of 10, one request every 100 ms); the key uses up the burst
    /// at `t`; the clock goes back 1 hour.
    ///
    /// Must see: after going back, the key waits no more than one window (1 s) — not the
    /// whole hour the clock went back.
    #[test]
    fn clock_stepping_back_waits_at_most_a_window() {
        let rate = KeyRate::new(crate::tenants::DeployRate::parse("10/1").unwrap());
        let k = key("k_a");
        let t = 1_800_000_000_000u64;
        for _ in 0..10 {
            rate.admit(&k, t).unwrap();
        }
        assert!(rate.admit(&k, t).is_err(), "burst used up");
        let back = t - 3_600_000;
        match rate.admit(&k, back) {
            Ok(()) => {}
            Err(wait) => assert!(wait <= 1_000, "{wait} ms wait after 1 h clock step"),
        }
        let later = back + 1_000;
        assert!(rate.admit(&k, later).is_ok(), "one window after going back");
    }

    fn sso(client: &str, sub: &str) -> Identity {
        Identity {
            tenant: TenantId::new(client).unwrap(),
            role: Role::Admin,
            key_id: None,
            user: Some(Arc::new(oidc::SsoUser {
                sub: sub.into(),
                email: None,
                name: None,
                client: client.into(),
            })),
        }
    }

    /// An SSO person's budget is per **(client, sub)** (security review, an accepted risk
    /// with a suggested fix): the same `sub` in two environments (two tenants) has two
    /// budgets — one environment firing all of it does not block that person in the
    /// other.
    #[test]
    fn sso_budget_is_per_client() {
        let rate = KeyRate::new(crate::tenants::DeployRate::parse("1/60").unwrap());
        let t = 1_800_000_000_000u64;
        rate.admit(&sso("env-a", "u1"), t).unwrap();
        assert!(
            rate.admit(&sso("env-a", "u1"), t).is_err(),
            "budget used up in env-a"
        );
        assert!(
            rate.admit(&sso("env-b", "u1"), t).is_ok(),
            "the same person in env-b has a separate budget"
        );
    }
}

#[cfg(test)]
mod tests {
    /// **Apply order**: two re-reads running in parallel — the refresh loop and an apply
    /// request — finish in any order.
    ///
    /// Interleaving:
    /// 1. Node B reads the registry (copy 1). Peer A shares the bucket.
    /// 2. B's next first GET reads, then stalls 300 ms before returning
    ///    (`DelayResponse`): B refreshes in the background — copy 1.
    /// 3. A writes copy 2.
    /// 4. B refreshes again (the apply request): copy 2, applied.
    /// 5. Read (2) returns.
    ///
    /// Must see: B stays at copy 2 — a copy read before the write does not overwrite the
    /// newer one.
    #[tokio::test]
    async fn stale_refresh_never_resurrects_a_revoked_key() {
        use cairn_store::testkit::{Action, FaultyStore, OpSet, PathMatch, Rule};
        let mem = Arc::new(slatedb::object_store::memory::InMemory::new());
        let faulty = FaultyStore::wrap(mem.clone());
        let peer = Keys::new(Some((mem.clone(), &Path::from("p"))), None);
        let me = Keys::new(Some((faulty.clone(), &Path::from("p"))), None);
        let (k, _) = peer
            .create(TenantId::new("acme").unwrap(), Role::Admin)
            .await
            .unwrap();
        me.refresh().await.unwrap();
        let has = |keys: &Keys| keys.list().iter().any(|r| r.id == k.id);
        assert!(has(&me));
        faulty.add_rule(Rule::new(
            OpSet::GET,
            PathMatch::Contains("p/auth/keys".into()),
            Action::EachOnce(Box::new(Action::DelayResponse(
                std::time::Duration::from_millis(300),
            ))),
        ));
        let slow = tokio::spawn({
            let me = me.clone();
            async move { me.refresh().await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(peer.delete(&k.id).await.unwrap());
        me.refresh().await.unwrap();
        assert!(!has(&me), "apply sees the revocation");
        slow.await.unwrap().unwrap();
        assert!(!has(&me), "a pre-revocation read revived the key");
    }

    use super::*;

    /// The `metrics` role: exactly `GET /metrics`, nothing else — no permission in its
    /// set, so no other route opens.
    #[test]
    fn metrics_role_permits_only_get_metrics() {
        let m = Role::Metrics;
        let need = |method: &Method, path: &str| required_caps(method, path);
        assert!(permits(
            m,
            need(&Method::GET, "/metrics"),
            &Method::GET,
            "/metrics"
        ));
        for (method, path) in [
            (Method::HEAD, "/metrics"),
            (Method::POST, "/metrics"),
            (Method::GET, "/metrics/"),
            (Method::GET, "/scope/x/metrics"),
            (Method::GET, "/cluster"),
            (Method::GET, "/auth/whoami"),
            (Method::POST, "/invoke/S/h"),
            (Method::GET, "/invocations/x"),
            (Method::POST, "/admin/keys"),
            (Method::GET, "/new-unclassified-route"),
        ] {
            assert!(
                !permits(m, need(&method, path), &method, path),
                "{method} {path}"
            );
        }
        assert_eq!(m.caps(), Caps::NONE);
        for r in [
            Role::Invoke,
            Role::View,
            Role::Admin,
            Role::Provision,
            Role::ClusterAdmin,
        ] {
            assert_eq!(
                permits(r, need(&Method::GET, "/metrics"), &Method::GET, "/metrics"),
                r == Role::ClusterAdmin,
                "{r:?}"
            );
        }
        assert_eq!(
            Role::parse("metrics"),
            None,
            "the metrics role does not go into the key registry"
        );
    }

    /// **The permission set of each role**: the three old roles keep exactly the
    /// permissions of the earlier nested chain (`invoke ⊂ admin ⊂ cluster-admin`); `view`
    /// only reads; `provision` has no tenant permission.
    #[test]
    fn role_capability_sets() {
        let names = |r: Role| r.caps().names();
        assert_eq!(names(Role::Invoke), ["invoke"]);
        assert_eq!(names(Role::View), ["read"]);
        assert_eq!(names(Role::Admin), ["read", "invoke", "operate"]);
        assert_eq!(names(Role::Provision), ["provision"]);
        assert_eq!(
            names(Role::ClusterAdmin),
            ["read", "invoke", "operate", "provision", "cluster"]
        );
        assert!(!Role::Provision.caps().meets(TENANT_CAPS));
        for r in ["invoke", "view", "admin", "provision", "cluster-admin"] {
            assert_eq!(Role::parse(r).map(Role::as_str), Some(r));
        }
        assert_eq!(
            ["invoke", "admin", "cluster-admin", "view", "provision"]
                .map(|r| Role::parse(r).is_some_and(Role::in_v1_book)),
            [true, true, true, false, false]
        );
    }

    /// **The escalation guard when granting keys**: `provision` only grants/revokes
    /// tenant roles — not `cluster-admin` (above itself), not `provision` (a peer: a leaked
    /// key cannot mint new keys to hang on); `cluster-admin` grants every role; tenant
    /// roles grant nothing.
    #[test]
    fn grant_rules_block_escalation() {
        use Role::*;
        let all = [Invoke, View, Admin, Provision, ClusterAdmin];
        for t in all {
            assert!(ClusterAdmin.may_grant(t), "{t:?}");
            assert_eq!(
                Provision.may_grant(t),
                matches!(t, Invoke | View | Admin),
                "{t:?}"
            );
            for r in [Metrics, Invoke, View, Admin] {
                assert!(!r.may_grant(t), "{r:?} → {t:?}");
            }
        }
        assert!(!ClusterAdmin.may_grant(Metrics));
    }

    /// The metrics token authenticates as the `metrics` role, cannot log in to the
    /// console, and does not mix with the bootstrap key.
    #[test]
    fn metrics_key_is_its_own_identity_and_cannot_log_in() {
        let mk = "m".repeat(MIN_ENV_SECRET);
        let keys = Keys::new(None, Some("boot")).with_metrics_key(Some(&mk));
        let who = keys.authenticate(&mk).expect("valid metrics token");
        assert_eq!(who.role, Role::Metrics);
        assert!(who.key_id.is_none());
        assert!(
            keys.login(&mk).is_none(),
            "the metrics token's cookie is a bootstrap cookie"
        );
        assert_eq!(
            keys.authenticate("boot").map(|w| w.role),
            Some(Role::ClusterAdmin)
        );
        assert!(keys.authenticate("wrong").is_none());
        assert!(check_metrics_key(&mk, Some("boot")).is_ok());
        assert!(check_metrics_key("tiny", None).is_err());
        assert!(check_metrics_key(&mk, Some(&mk)).is_err());
    }

    fn sha_hex(k: &str) -> String {
        hex(&Sha256::digest(k.as_bytes()))
    }

    fn cfg(admin: Option<&str>, admin_sha: Option<&str>, prov_sha: Option<&str>) -> AuthConfig {
        AuthConfig {
            required: true,
            admin_key: admin.map(str::to_string),
            admin_key_sha256: admin_sha.map(str::to_string),
            provision_key_sha256: prov_sha.map(str::to_string),
            ..AuthConfig::default()
        }
    }

    /// **A short plain bootstrap key is refused at startup.** Before,
    /// `CAIRNFLOW_ADMIN_KEY=admin123` started normally (only empty was filtered), while
    /// the metrics token, the cluster secret and the identity master all require ≥ 32
    /// bytes. Must see: < 32 bytes ⇒ an `Err` naming the variable and how to generate a
    /// key; exactly 32 bytes and the hash form are accepted.
    #[test]
    fn short_bootstrap_key_is_refused() {
        let err = EnvKeys::resolve(&cfg(Some("admin123"), None, None))
            .expect_err("an 8-byte key must be refused");
        assert!(
            err.contains("CAIRNFLOW_ADMIN_KEY") && err.contains("openssl rand"),
            "{err}"
        );
        assert!(
            !err.contains("admin123"),
            "the message must not name the key: {err}"
        );
        EnvKeys::resolve(&cfg(Some(&"k".repeat(32)), None, None)).expect("32 bytes accepted");
        EnvKeys::resolve(&cfg(None, Some(&sha_hex("admin123")), None))
            .expect("the hash form is not measured by the original key's length");
    }

    /// Bootstrap keys by hash only: the `provision` hash authenticates as the `provision`
    /// role of tenant `default`, cannot log in to the console, counts as "has a key"
    /// ([`Keys::is_empty`]); the cluster-admin hash takes exactly the place of the plain
    /// bootstrap key (the same `kid`, the same console session).
    #[test]
    fn env_hashed_keys_authenticate_without_plaintext() {
        let (admin, prov) = (
            "cf_test_break_glass_admin",
            "cf_test_control_plane_provision",
        );
        let env = EnvKeys::resolve(&cfg(None, Some(&sha_hex(admin)), Some(&sha_hex(prov))))
            .expect("two valid hashes");
        let keys = Keys::new(None, None).with_env_hashes(&env);
        assert!(!keys.is_empty());
        let p = keys.authenticate(prov).expect("provision key");
        assert_eq!(
            (p.role, p.tenant.as_str(), p.key_id.as_deref()),
            (Role::Provision, "default", None)
        );
        assert!(keys.login(prov).is_none(), "provision key opened console");
        let a = keys.authenticate(admin).expect("cluster-admin key");
        assert_eq!(a.role, Role::ClusterAdmin);
        let (_, kid) = keys
            .login(admin)
            .expect("break-glass can log in to the console");
        let plain = Keys::new(None, Some(admin));
        assert_eq!(
            plain.login(admin).map(|(_, k)| k),
            Some(kid.clone()),
            "the hash and the plain form of the same key must have the same kid"
        );
        assert_eq!(
            keys.identify(&kid).map(|w| w.role),
            Some(Role::ClusterAdmin)
        );
        // The plain form never matches the hash string itself.
        assert!(keys.authenticate(&sha_hex(prov)).is_none());
        assert!(keys.authenticate("wrong").is_none());

        // Only the provision key: still counts as having a key — a cell runs without any
        // cluster-admin key.
        let only = EnvKeys::resolve(&cfg(None, None, Some(&sha_hex(prov)))).unwrap();
        let keys = Keys::new(None, None).with_env_hashes(&only);
        assert!(!keys.is_empty());
        assert!(keys.authenticate(admin).is_none());
        // Nothing at all ⇒ empty; the metrics token alone does not count.
        let mk = "m".repeat(MIN_ENV_SECRET);
        assert!(Keys::new(None, None).with_metrics_key(Some(&mk)).is_empty());
    }

    /// A broken or duplicate value ⇒ an error (the node does not start), the error names
    /// the variable but not the value.
    #[test]
    fn env_hashed_keys_reject_bad_or_shared_values() {
        let prov = sha_hex("cf_test_control_plane_provision");
        for bad in [
            "abc",
            &prov[..63],
            &format!("{prov}0"),
            &format!("{}g", &prov[..63]),
            "cf_0123456789abcdef0123456789abcdef0123456789abcdef",
        ] {
            let e = EnvKeys::resolve(&cfg(None, None, Some(bad))).expect_err(bad);
            assert!(e.contains(PROVISION_KEY_SHA256_ENV), "{e}");
            assert!(!e.contains(bad), "the error leaks the value: {e}");
            let e = EnvKeys::resolve(&cfg(None, Some(bad), None)).expect_err(bad);
            assert!(e.contains(ADMIN_KEY_SHA256_ENV), "{e}");
        }
        // Upper case, and the newline a secret tool adds when loading from a file: accepted.
        let upper = format!("{}\n", prov.to_ascii_uppercase());
        assert_eq!(
            EnvKeys::resolve(&cfg(None, None, Some(&upper)))
                .unwrap()
                .provision_sha256,
            EnvKeys::resolve(&cfg(None, None, Some(&prov)))
                .unwrap()
                .provision_sha256
        );
        // Both the plain and the hashed cluster-admin key.
        // The plain bootstrap key is ≥ 32 bytes — otherwise the length error comes first.
        let boot = "cf_test_unit_bootstrap_key_000000000000";
        let e = EnvKeys::resolve(&cfg(Some(boot), Some(&sha_hex(boot)), None)).unwrap_err();
        assert!(e.contains("pick one"), "{e}");
        // provision equal to cluster-admin (plain or hashed).
        let same_as_admin = sha_hex(boot);
        assert!(EnvKeys::resolve(&cfg(Some(boot), None, Some(&same_as_admin))).is_err());
        assert!(EnvKeys::resolve(&cfg(None, Some(&same_as_admin), Some(&same_as_admin))).is_err());
        // provision or the cluster-admin hash equal to the metrics token.
        let mk = "m".repeat(MIN_ENV_SECRET);
        let mut c = cfg(None, None, Some(&sha_hex(&mk)));
        c.metrics_key = Some(mk.clone());
        assert!(EnvKeys::resolve(&c)
            .unwrap_err()
            .contains("CAIRNFLOW_METRICS_KEY"));
        let mut c = cfg(None, Some(&sha_hex(&mk)), None);
        c.metrics_key = Some(mk.clone());
        assert!(EnvKeys::resolve(&c).is_err());
        // All different ⇒ fine.
        let mut c = cfg(None, Some(&sha_hex("boot")), Some(&prov));
        c.metrics_key = Some(mk);
        assert!(EnvKeys::resolve(&c).is_ok());
        // Empty/whitespace ≡ absent.
        assert_eq!(
            EnvKeys::resolve(&cfg(None, Some("  "), Some(""))).unwrap(),
            EnvKeys::default()
        );
    }

    /// This header is a header of the policy table.
    #[test]
    fn header_is_in_policy() {
        assert_eq!(
            cairn_store::object_policy("auth/keys").map(|p| p.header),
            Some(KEYS_HEADER_V2)
        );
    }

    /// **A slow refresh removes a just-created key from the cache** (seen in a full check
    /// run: `auth.rs::invoke_key_cannot_call_admin` got 401 for an admin key just
    /// created).
    ///
    /// Interleaving:
    /// 1. `auth/keys` has one key. `refresh` GETs it; the answer comes back late.
    /// 2. In the gap, `create` writes a new key to the bucket and into the cache.
    /// 3. `refresh` gets the old answer and writes it into the cache.
    ///
    /// Must see: the secret just created still authenticates. Before the fix, the cache
    /// went back to the old copy until the next refresh.
    #[tokio::test]
    async fn slow_refresh_does_not_drop_a_key_created_meanwhile() {
        use cairn_store::testkit::{Action, FaultyStore, Op, OpSet, PathClass, PathMatch, Rule};
        let faulty = FaultyStore::wrap(Arc::new(slatedb::object_store::memory::InMemory::new()));
        let store: Arc<dyn ObjectStore> = faulty.clone();
        let keys = Keys::new(Some((store.clone(), &Path::from("p"))), None);
        keys.create(TenantId::default(), Role::Invoke)
            .await
            .expect("first key");
        // (1)
        let gets = |f: &FaultyStore| f.counts().get(Op::Get, PathClass::Other);
        let before = gets(&faulty);
        let slow = faulty.add_rule(Rule::new(
            OpSet::GET,
            PathMatch::Contains("auth/keys".into()),
            Action::DelayResponse(std::time::Duration::from_millis(300)),
        ));
        let refresh = {
            let keys = keys.clone();
            tokio::spawn(async move { keys.refresh().await })
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while gets(&faulty) == before {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("refresh did not GET");
        faulty.remove_rule(slow);
        // (2)
        let (_, secret) = keys
            .create(TenantId::default(), Role::Admin)
            .await
            .expect("create key");
        assert!(
            keys.authenticate(&secret).is_some(),
            "a key just created must authenticate"
        );
        // (3)
        refresh.await.expect("join").expect("refresh");
        assert!(
            keys.authenticate(&secret).is_some(),
            "a slow refresh removed the key just created from the cache"
        );
    }

    /// Ambiguous outcomes: the `auth/keys` CAS and the cluster secret's `Create` were
    /// written while the answer said otherwise. Must see: creating a key yields **one**
    /// record (before the fix: the retry pushed the record again — two entries with the
    /// same id), deleting reports `true` (not 404 for a key just deleted), and the node
    /// gets the cluster secret instead of dying at startup.
    #[tokio::test]
    async fn ambiguous_writes_are_resolved_by_reread() {
        use cairn_store::testkit::{Ambiguity, FaultyStore, Rule};
        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let faulty =
                FaultyStore::wrap(Arc::new(slatedb::object_store::memory::InMemory::new()));
            let store: Arc<dyn ObjectStore> = faulty.clone();
            let keys = Keys::new(Some((store.clone(), &Path::from("p"))), None);
            faulty.add_rule(Rule::ambiguous_put("auth/keys", reply));
            let (rec, _) = keys
                .create(TenantId::new("acme").unwrap(), Role::Admin)
                .await
                .unwrap_or_else(|e| panic!("{reply:?}: key written but reported an error: {e}"));
            keys.refresh().await.unwrap();
            assert_eq!(keys.list().len(), 1, "{reply:?}: {:?}", keys.list());
            faulty.add_rule(Rule::ambiguous_put("auth/keys", reply));
            assert!(
                keys.delete(&rec.id)
                    .await
                    .unwrap_or_else(|e| panic!("{reply:?}: deleted but errored: {e}")),
                "{reply:?}: key just deleted reported as absent"
            );

            faulty.add_rule(Rule::ambiguous_put("auth/cluster-secret", reply));
            let a = ClusterSecret::resolve(&store, &Path::from("p"), None)
                .await
                .unwrap_or_else(|e| panic!("{reply:?}: secret written but reported an error: {e}"));
            let b = ClusterSecret::resolve(&store, &Path::from("p"), None)
                .await
                .unwrap();
            assert_eq!(
                a.signature("POST", "/x", 1, b""),
                b.signature("POST", "/x", 1, b""),
                "{reply:?}: a later read returned a different secret"
            );
        }
    }

    /// A secret from the environment < 32 bytes is a permanent error (startup does not
    /// retry), ≥ 32 bytes is accepted verbatim; not set ⇒ the bucket.
    #[tokio::test]
    async fn env_secret_must_be_at_least_32_bytes() {
        let store: Arc<dyn ObjectStore> = Arc::new(slatedb::object_store::memory::InMemory::new());
        let short = "x".repeat(MIN_ENV_SECRET - 1);
        let e = ClusterSecret::resolve(&store, &Path::from("p"), Some(&short))
            .await
            .expect_err("31 bytes must be refused");
        assert!(e.contains("CAIRNFLOW_CLUSTER_SECRET"), "{e}");
        assert!(
            matches!(
                crate::boot::text_verdict(&e),
                cairn_store::retry::Verdict::Permanent
            ),
            "a configuration error must not be retried: {e}"
        );
        let ok = "y".repeat(MIN_ENV_SECRET);
        let s = ClusterSecret::resolve(&store, &Path::from("p"), Some(&ok))
            .await
            .expect("32 byte");
        assert_eq!(
            s.signature("POST", "/x", 1, b""),
            ClusterSecret::new(ok.as_bytes()).signature("POST", "/x", 1, b"")
        );
    }

    /// The test vector of RFC 4231, section 4.2.
    #[test]
    fn hmac_matches_rfc_4231() {
        let got = hmac_sha256(&[0x0b; 20], b"Hi There");
        assert_eq!(
            hex(&got),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
    }

    #[test]
    fn valid_signature_passes_invalid_is_rejected() {
        let s = ClusterSecret::new(b"secret");
        let mut h = HeaderMap::new();
        let ts = now_s();
        h.insert(TIMESTAMP_HEADER, ts.into());
        h.insert(
            SIGNATURE_HEADER,
            s.signature("POST", "/internal/inbound?x=1", ts, b"b")
                .parse()
                .unwrap(),
        );
        assert!(s.verify("POST", "/internal/inbound?x=1", &h, b"b").is_ok());
        assert!(s.verify("POST", "/internal/inbound?x=2", &h, b"b").is_err());
        assert!(s.verify("POST", "/internal/inbound?x=1", &h, b"c").is_err());
        assert!(ClusterSecret::new(b"other")
            .verify("POST", "/internal/inbound?x=1", &h, b"b")
            .is_err());
    }

    /// An internal router with one route behind [`require_cluster_signature`]; the
    /// handler returns the number of body bytes it received (reading `Body` directly,
    /// not through the `Bytes` extractor's default 2 MB cap).
    fn internal_echo(secret: ClusterSecret) -> axum::Router {
        axum::Router::new()
            .route(
                "/internal/x",
                axum::routing::post(|req: Request| async move {
                    axum::body::to_bytes(req.into_body(), usize::MAX)
                        .await
                        .map(|b| b.len().to_string())
                        .unwrap_or_default()
                }),
            )
            .layer(axum::middleware::from_fn_with_state(
                Some(secret),
                require_cluster_signature,
            ))
    }

    /// A body of `total` bytes (64 KiB chunks) counting the bytes the server pulled.
    fn counted_body(total: usize) -> (axum::body::Body, Arc<std::sync::atomic::AtomicUsize>) {
        use std::sync::atomic::{AtomicUsize, Ordering};
        const CHUNK: usize = 64 * 1024;
        let pulled = Arc::new(AtomicUsize::new(0));
        let chunk = bytes::Bytes::from(vec![b'x'; CHUNK]);
        let seen = pulled.clone();
        let stream = futures::stream::iter((0..total / CHUNK).map(move |_| {
            seen.fetch_add(CHUNK, Ordering::Relaxed);
            Ok::<_, std::io::Error>(chunk.clone())
        }));
        (axum::body::Body::from_stream(stream), pulled)
    }

    async fn call(router: axum::Router, req: Request) -> (StatusCode, String) {
        let res = tower_service::Service::call(&mut router.clone(), req)
            .await
            .expect("router does not fail");
        let status = res.status();
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .expect("read the response body");
        (status, String::from_utf8_lossy(&body).into_owned())
    }

    /// Security review: the internal port buffered up to 256 MB of body **before**
    /// checking the signature, so anyone reaching the port (no cluster secret needed)
    /// could make each connection eat up to 256 MB of RAM.
    ///
    /// Order:
    /// 1. A stranger sends `POST /internal/x` with a fresh timestamp, a made-up signature,
    ///    a 32 MiB body — with and without the body hash header.
    /// 2. The server answers.
    ///
    /// Must see: `401`/`413` with the server pulling **no more** than the pre-auth cap
    /// (4 MiB + one chunk) — with the hash header it pulls no byte at all.
    #[tokio::test]
    async fn body_is_not_buffered_before_signature_check() {
        use std::sync::atomic::Ordering;
        let router = internal_echo(ClusterSecret::new([7u8; 32]));
        let total = 32 * 1024 * 1024;
        for with_digest in [true, false] {
            let (body, pulled) = counted_body(total);
            let mut req = Request::post("/internal/x").body(body).unwrap();
            let h = req.headers_mut();
            h.insert(TIMESTAMP_HEADER, now_s().into());
            h.insert(SIGNATURE_HEADER, hex(&[0u8; 32]).parse().unwrap());
            if with_digest {
                h.insert(DIGEST_HEADER, hex(&[1u8; 32]).parse().unwrap());
            }
            let (status, _) = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                call(router.clone(), req),
            )
            .await
            .expect("server hung");
            assert!(
                status == StatusCode::UNAUTHORIZED || status == StatusCode::PAYLOAD_TOO_LARGE,
                "digest={with_digest}: {status}"
            );
            let pulled = pulled.load(Ordering::Relaxed);
            let cap = if with_digest {
                0
            } else {
                MAX_PRE_AUTH_BODY + 64 * 1024
            };
            assert!(
                pulled <= cap,
                "digest={with_digest}: the server pulled {pulled} bytes before the signature was checked (cap {cap})"
            );
        }
    }

    /// A sender inside the cluster ([`ClusterSecret::sign`]) carries the body hash header:
    /// a body larger than the pre-auth cap still reaches the handler; a body swapped after
    /// signing (the header still right) is blocked after hashing.
    #[tokio::test]
    async fn signed_large_body_passes_and_swapped_body_is_rejected() {
        let secret = ClusterSecret::new([9u8; 32]);
        let router = internal_echo(secret.clone());
        let big = vec![b'y'; 2 * MAX_PRE_AUTH_BODY];
        let signed = secret.sign(
            reqwest::Client::new()
                .post("http://node/internal/x")
                .body(big.clone())
                .build()
                .unwrap(),
        );
        assert!(
            signed.headers().contains_key(DIGEST_HEADER),
            "the sender must carry the body hash header"
        );
        let build = |body: Vec<u8>| {
            let mut req = Request::post("/internal/x")
                .body(axum::body::Body::from(body))
                .unwrap();
            for (k, v) in signed.headers() {
                req.headers_mut().insert(k.clone(), v.clone());
            }
            req
        };
        let (status, got) = call(router.clone(), build(big.clone())).await;
        assert_eq!(status, StatusCode::OK, "{got}");
        assert_eq!(got, big.len().to_string());
        let mut swapped = big;
        swapped[0] = b'z';
        let (status, got) = call(router, build(swapped)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{got}");
    }

    /// The permission set a route declares, a shorthand for the table below.
    fn caps(m: Method, p: &str) -> Vec<&'static str> {
        match required_caps(&m, p) {
            Need::Open => vec![],
            Need::Any(c) => c.names(),
        }
    }

    #[test]
    fn role_policy() {
        let r = caps;
        assert_eq!(required_caps(&Method::GET, "/health"), Need::Open);
        assert_eq!(r(Method::POST, "/invoke/S/h"), ["invoke"]);
        assert_eq!(r(Method::GET, "/tenant"), ["read"]);
        assert_eq!(r(Method::GET, "/audit"), ["read", "provision"]);
        assert_eq!(r(Method::POST, "/scope/a/invoke/S/h/send"), ["invoke"]);
        assert_eq!(r(Method::GET, "/invocations/abc"), ["read", "invoke"]);
        assert_eq!(r(Method::GET, "/invocations/abc/journal"), ["read"]);
        assert_eq!(
            r(Method::GET, "/invocations/abc/streams/ui"),
            ["read", "invoke"]
        );
        assert_eq!(r(Method::GET, "/invocations/abc/streams/$log"), ["read"]);
        assert_eq!(
            r(Method::GET, "/invocations/abc/streams/%24trace"),
            ["read"]
        );
        // `$log`/`$trace` read through their two own paths are Read too.
        assert_eq!(r(Method::GET, "/invocations/abc/logs"), ["read"]);
        assert_eq!(r(Method::GET, "/invocations/abc/spans"), ["read"]);
        assert_eq!(
            r(Method::GET, "/invocations/abc/artifacts"),
            ["read", "invoke"]
        );
        assert_eq!(
            r(Method::GET, "/invocations/abc/artifacts/00"),
            ["read", "invoke"]
        );
        assert_eq!(r(Method::POST, "/invocations/abc/cancel"), ["operate"]);
        assert_eq!(r(Method::POST, "/awakeables/awk_x/resolve"), ["invoke"]);
        assert_eq!(r(Method::GET, "/workflow/W/k/output"), ["read", "invoke"]);
        assert_eq!(r(Method::POST, "/workflow/W/k/attach"), ["invoke"]);
        assert_eq!(r(Method::GET, "/invocations"), ["read"]);
        assert_eq!(r(Method::HEAD, "/invocations"), ["read"]);
        assert_eq!(r(Method::POST, "/deployments"), ["operate"]);
        assert_eq!(r(Method::GET, "/deployments"), ["read"]);
        assert_eq!(r(Method::DELETE, "/deployments/d"), ["operate"]);
        assert_eq!(r(Method::POST, "/deployments/d/replay-check"), ["operate"]);
        assert_eq!(r(Method::PUT, "/services/S/revision"), ["operate"]);
        assert_eq!(r(Method::POST, "/invocations/batch/cancel"), ["operate"]);
        assert_eq!(r(Method::GET, "/stats/handlers"), ["read"]);
        assert_eq!(r(Method::POST, "/query"), ["read"]);
        assert_eq!(r(Method::GET, "/identity"), ["read", "provision"]);
        assert_eq!(r(Method::GET, "/metering"), ["read", "provision"]);
        assert_eq!(r(Method::GET, "/metering/hours"), ["read", "provision"]);
        assert_eq!(
            r(Method::GET, "/auth/whoami"),
            ["read", "invoke", "operate", "provision", "cluster"]
        );
        // The control plane.
        for (m, p) in [
            (Method::POST, "/admin/keys"),
            (Method::GET, "/admin/keys"),
            (Method::DELETE, "/admin/keys/k_1"),
            (Method::GET, "/admin/keys/k_1/revocation"),
            (Method::GET, "/admin/tenants"),
            (Method::PUT, "/admin/tenants/acme"),
            (Method::PUT, "/admin/tenants/acme/state"),
            (Method::GET, "/admin/tenants/acme/purge"),
            (Method::GET, "/admin/cluster-version"),
            (Method::GET, "/admin/ops-reports"),
            (Method::GET, "/admin/ops-reports/op-1"),
        ] {
            assert_eq!(r(m.clone(), p), ["provision"], "{m} {p}");
        }
        for m in [Method::GET, Method::POST, Method::DELETE] {
            assert_eq!(r(m.clone(), "/rules"), ["provision", "cluster"], "{m}");
        }
        // The cluster.
        for (m, p) in [
            (Method::GET, "/rules/acme"),
            (Method::DELETE, "/rules/acme"),
            (Method::POST, "/admin/capture"),
            (Method::GET, "/cluster"),
            (Method::GET, "/metrics"),
            (Method::GET, "/metering/pricing"),
            (Method::POST, "/admin/failpoints"),
            (Method::GET, "/admin/failpoints"),
            (Method::PUT, "/admin/gc/barriers/x"),
            (Method::GET, "/admin/gc/plan"),
            (Method::PATCH, "/admin/keys"),
            (Method::DELETE, "/admin/tenants/acme"),
            (Method::GET, "/admin/tenants/acme/state"),
            (Method::GET, "/new-unclassified-route"),
            (Method::POST, "/health"),
            (Method::POST, "/tenant"),
        ] {
            assert_eq!(r(m.clone(), p), ["cluster"], "{m} {p}");
        }
        assert!(declared_caps(&Method::GET, "/new-unclassified-route").is_none());
    }

    /// Signals and cooperative cancel are `Invoke`; hard cancel keeps `Operate` — even
    /// with an unknown `mode`.
    #[test]
    fn signal_and_cooperative_cancel_are_invoke() {
        let r = |p: &str| required_caps_query(&Method::POST, p);
        assert_eq!(r("/invocations/abc/signal"), INVOKE);
        assert_eq!(r("/invocations/abc/cancel?mode=signal"), INVOKE);
        assert_eq!(r("/scope/s/invocations/abc/cancel?x=1&mode=signal"), INVOKE);
        assert_eq!(r("/invocations/abc/cancel"), OPERATE);
        assert_eq!(r("/invocations/abc/cancel?mode=hard"), OPERATE);
        assert_eq!(r("/invocations/abc/cancel?mode=signalx"), OPERATE);
        // Lowering is for `invoke` only: `view` cannot cancel, not even cooperatively.
        assert!(!permits(
            Role::View,
            r("/invocations/abc/cancel?mode=signal"),
            &Method::POST,
            "/invocations/abc/cancel"
        ));
    }

    /// A repeated `mode` must not lower the permission: the `cancel` handler reads
    /// `Query<HashMap>` (the last value wins), so `?mode=signal&mode=hard` used to pass the
    /// auth layer with the `invoke` role and then hard-cancel. The permission is lowered
    /// only when **every** `mode` (decoded as the handler decodes it) is `signal`.
    /// `batch/cancel` is always `Operate` (bulk work belongs to operators), even with
    /// `mode=signal`.
    #[test]
    fn repeated_mode_does_not_lower_role() {
        let r = |p: &str| required_caps_query(&Method::POST, p);
        assert_eq!(r("/invocations/abc/cancel?mode=signal&mode=hard"), OPERATE);
        assert_eq!(r("/invocations/abc/cancel?mode=hard&mode=signal"), OPERATE);
        assert_eq!(r("/invocations/abc/cancel?mode=signal&mode=signal"), INVOKE);
        assert_eq!(r("/invocations/abc/cancel?mode=sign%61l"), INVOKE);
        assert_eq!(r("/invocations/batch/cancel?mode=signal"), OPERATE);
        assert_eq!(r("/scope/s/invocations/batch/cancel?mode=signal"), OPERATE);
    }

    /// Hourly metering like `/metering`; the price table still belongs to the cluster.
    #[test]
    fn metering_hours_is_read_or_provision() {
        assert_eq!(caps(Method::GET, "/metering/hours"), ["read", "provision"]);
        assert_eq!(caps(Method::GET, "/metering/pricing"), ["cluster"]);
    }

    /// Attach by id: an `invoke` key that just got `202` can wait for the result with that
    /// very key, like `/workflow/…/attach`.
    #[test]
    fn attach_by_id_is_invoke() {
        assert_eq!(caps(Method::POST, "/invocations/abc/attach"), ["invoke"]);
        assert_eq!(
            caps(Method::POST, "/scope/s/invocations/abc/attach"),
            ["invoke"]
        );
    }

    /// Server-side schedules: every command is `invoke`, including the scoped variant;
    /// reading is allowed for `view` too.
    #[test]
    fn schedules_are_invoke() {
        for (m, p) in [
            (Method::PUT, "/schedules/nightly"),
            (Method::DELETE, "/schedules/nightly"),
            (Method::POST, "/schedules/nightly/trigger"),
            (Method::POST, "/scope/s/schedules/nightly/pause"),
        ] {
            assert_eq!(caps(m.clone(), p), ["invoke"], "{m} {p}");
        }
        assert_eq!(caps(Method::GET, "/schedules"), ["read", "invoke"]);
    }

    /// The registry writer: gate off ⇒ registry `/1`, new roles `RolesDisabled` and the
    /// registry unchanged; gate on (`key_roles=on` **and** `min_binary ≥ 8`) ⇒ registry
    /// `/2` keeping the old keys. Only `key_roles=on` with a low `min_binary` (an old
    /// `cairn-ctl` lowered it) does not count as on.
    #[tokio::test]
    async fn writer_keeps_v1_until_gate_then_writes_v2() {
        let store: Arc<dyn ObjectStore> = Arc::new(slatedb::object_store::memory::InMemory::new());
        let p = Path::from("p");
        let keys = Keys::new(Some((store.clone(), &p)), None);
        let raw = |store: Arc<dyn ObjectStore>| async move {
            String::from_utf8(
                store
                    .get(&Path::from("p/auth/keys"))
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap()
                    .to_vec(),
            )
            .unwrap()
        };
        let set_cv = |text: &'static str| {
            let store = store.clone();
            async move {
                store
                    .put(&Path::from("p/cluster-version"), text.into())
                    .await
                    .unwrap();
            }
        };
        let (admin, _) = keys
            .create(TenantId::new("acme").unwrap(), Role::Admin)
            .await
            .unwrap();
        assert!(raw(store.clone()).await.starts_with("cairnflow-keys/1\n"));
        for cv in [
            None,
            Some("cairnflow-cluster-version/1\nversion=2\nmin_binary=7\nkey_roles=on\n"),
            Some("cairnflow-cluster-version/1\nversion=2\nmin_binary=8\n"),
        ] {
            if let Some(cv) = cv {
                set_cv(cv).await;
            }
            let before = raw(store.clone()).await;
            for role in [Role::Provision, Role::View] {
                match keys.create(TenantId::default(), role).await {
                    Err(KeyError::RolesDisabled(r)) => assert_eq!(r, role),
                    other => panic!("{cv:?} {role:?}: {other:?}"),
                }
            }
            assert_eq!(raw(store.clone()).await, before, "{cv:?}");
        }
        set_cv("cairnflow-cluster-version/1\nversion=2\nmin_binary=8\nkey_roles=on\n").await;
        let (prov, secret) = keys
            .create(TenantId::default(), Role::Provision)
            .await
            .unwrap();
        let text = raw(store.clone()).await;
        assert!(text.starts_with("cairnflow-keys/2\n"), "{text}");
        assert_eq!(decode_keys(text.as_bytes()).unwrap(), vec![admin, prov]);
        assert_eq!(
            keys.authenticate(&secret).map(|w| w.role),
            Some(Role::Provision)
        );
        assert!(keys.login(&secret).is_none(), "provision opened console");
    }

    /// Ambiguous outcomes for the `/2` registry write (a new conditional write): the
    /// `auth/keys` CAS carrying a `provision` key was written while the answer said
    /// otherwise. Must see: one record, registry `/2`; revoking with `delete_if` reports
    /// `Deleted`; a refused revocation ([`Deleted::Refused`]) writes nothing even when
    /// every write is ambiguous.
    #[tokio::test]
    async fn ambiguous_v2_writes_are_resolved_by_reread() {
        use cairn_store::testkit::{Ambiguity, FaultyStore, Op, OpSet, PathClass, Rule};
        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let faulty =
                FaultyStore::wrap(Arc::new(slatedb::object_store::memory::InMemory::new()));
            let store: Arc<dyn ObjectStore> = faulty.clone();
            store
                .put(
                    &Path::from("p/cluster-version"),
                    "cairnflow-cluster-version/1\nversion=2\nmin_binary=8\nkey_roles=on\n".into(),
                )
                .await
                .unwrap();
            let keys = Keys::new(Some((store.clone(), &Path::from("p"))), None);
            let (ca, _) = keys
                .create(TenantId::default(), Role::ClusterAdmin)
                .await
                .unwrap();
            faulty.add_rule(Rule::ambiguous_put("auth/keys", reply));
            let (rec, _) = keys
                .create(TenantId::default(), Role::Provision)
                .await
                .unwrap_or_else(|e| panic!("{reply:?}: key written but reported an error: {e}"));
            keys.refresh().await.unwrap();
            assert_eq!(keys.list().len(), 2, "{reply:?}: {:?}", keys.list());
            let puts = |f: &FaultyStore| f.counts().get(Op::Put, PathClass::Other);
            let before = puts(&faulty);
            faulty.add_rule(Rule::new(
                OpSet::PUT,
                cairn_store::testkit::PathMatch::Contains("auth/keys".into()),
                cairn_store::testkit::Action::Fail { times: None },
            ));
            assert_eq!(
                keys.delete_if(&ca.id, |r| Role::Provision.may_grant(r))
                    .await
                    .unwrap(),
                Deleted::Refused(Role::ClusterAdmin)
            );
            assert_eq!(puts(&faulty), before, "{reply:?}: refused but still PUT");
            faulty.clear_rules();
            faulty.add_rule(Rule::ambiguous_put("auth/keys", reply));
            assert_eq!(
                keys.delete_if(&rec.id, |r| Role::ClusterAdmin.may_grant(r))
                    .await
                    .unwrap_or_else(|e| panic!("{reply:?}: deleted but errored: {e}")),
                Deleted::Deleted
            );
            keys.refresh().await.unwrap();
            assert_eq!(keys.list(), vec![ca], "{reply:?}");
        }
    }

    /// **Key registries `/1`, `/2`, `/3`**: the decoder reads `/1` (the three old roles —
    /// a new role in `/1` means corruption: a `/1` writer never writes them) and `/2` (the
    /// five storable roles); a role that cannot be stored (`metrics`) or an unknown role
    /// breaks the whole object; `/3` is the registry of a newer binary — the error names
    /// the header and asks to upgrade the binary.
    #[test]
    fn keys_book_decodes_v1_v2_and_refuses_v3() {
        let book = |header: &str, role: &str| {
            format!("{header}\nk_1 {} {role} 42 acme\n", "07".repeat(32)).into_bytes()
        };
        for role in ["invoke", "admin", "cluster-admin"] {
            for header in ["cairnflow-keys/1", "cairnflow-keys/2"] {
                let got = decode_keys(&book(header, role)).unwrap_or_else(|e| panic!("{e}"));
                assert_eq!(got.len(), 1);
                assert_eq!(got[0].role.as_str(), role, "{header}");
                assert_eq!(got[0].tenant.as_str(), "acme");
            }
        }
        for role in ["provision", "view"] {
            assert!(
                decode_keys(&book("cairnflow-keys/1", role)).is_err(),
                "/1 never carries role {role}"
            );
            let got = decode_keys(&book("cairnflow-keys/2", role))
                .unwrap_or_else(|e| panic!("/2 {role}: {e}"));
            assert_eq!(got[0].role.as_str(), role);
        }
        for role in ["metrics", "root", ""] {
            assert!(
                decode_keys(&book("cairnflow-keys/2", role)).is_err(),
                "/2 role {role:?} must break the whole registry"
            );
        }
        let e = decode_keys(&book("cairnflow-keys/3", "admin")).expect_err("/3 is newer");
        assert!(
            e.contains("cairnflow-keys/3") && e.contains("upgrade this binary"),
            "{e}"
        );
    }

    #[test]
    fn key_encode_decode() {
        let k = KeyRecord {
            id: "k_1".into(),
            hash: [7; 32],
            tenant: TenantId::new("acme").unwrap(),
            role: Role::Admin,
            created_ms: 42,
        };
        assert_eq!(
            decode_keys(&encode_keys(std::slice::from_ref(&k), false).unwrap()).unwrap(),
            vec![k]
        );
    }
}
