// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Discovery and version negotiation.
//!
//! **Why this module existed from the first commit.** A known high risk:
//! *"protocol v1 turns out wrong somewhere"*. Restate's service protocol went
//! through V3 and V4 and then redid the journal in V5. The negotiation
//! mechanism must exist **before** there are users, because adding it later is
//! already too late — by then no SDK out there knows how to negotiate.
//!
//! Two **independent** axes, do not merge them:
//!
//! | Axis | Mechanism | What breaks if it fails |
//! |---|---|---|
//! | Discovery | Content-type when calling `/discover` | The manifest cannot be read |
//! | Wire | `minProtocolVersion`/`maxProtocolVersion` in the manifest | The manifest reads, but the two sides cannot talk |

use serde::{Deserialize, Serialize};

/// Content-type of the v1 manifest.
pub const DISCOVERY_V1: &str = "application/vnd.cairnflow.endpointmanifest.v1+json";

/// The wire versions this runtime supports.
///
/// **v2 = flow control.** `CallCommandMessage` and `OneWayCallCommandMessage`
/// carry `scope` and `limit_key` in addition (fields 7/8 and 8/9). Both fields
/// are `optional`, so in pure protobuf terms an old SDK and a new runtime can
/// talk — but **the other direction is silently wrong**: a new SDK sending a
/// scope to a v1 runtime has the field ignored as unknown, and the call runs
/// *with no cap at all*. The user sees an invoice, not an error.
///
/// So it is a real negotiation axis: the runtime declares v2 here, the SDK
/// declares v2 in its manifest, and the agreed version is **sent back to the
/// SDK in the content-type of `/invoke`**
/// ([`crate::invoker::invocation_content_type`]) so the SDK refuses
/// `ctx.scoped(...)` when the other side only speaks v1.
pub const WIRE_MIN: u32 = 1;
pub const WIRE_MAX: u32 = 6;

/// The first wire version with `Keepalive` and the `replay_only` flag (one bump
/// for both). Negotiated like v2: older SDKs (≤ v2) still run, just without
/// keepalive; the runtime never sends v3 fields to a deployment agreed on v2.
pub const WIRE_KEEPALIVE: u32 = 3;

/// The first wire version with the `StreamAppend` (`0x000B`) and `ArtifactPut`
/// (`0x000C`) side channels. The SDK only sends them once v4 is agreed; for a
/// deployment agreed below v4, the turn layer lets such a frame reach the state
/// machine, which answers 571 as for an unknown code.
pub const WIRE_STREAMS: u32 = 4;

/// The first wire version with incremental replay for `REQUEST_RESPONSE`
/// (`Start.replay_from`/`prefix_digest`, a `409` on a miss). The runtime only
/// sends deltas to a deployment agreed on v5 **and** declaring `replayCache`.
pub const WIRE_REPLAY_DELTA: u32 = 5;

/// The first wire version with segmented values sent down to the SDK: `Value`
/// carries a ref + tail, read URLs in `Start.segment_urls` and the
/// `SegmentUrls` frame (`0x000D`); `SetState` from the SDK may carry a reused
/// ref. The runtime only sends refs to a deployment agreed on v6 when the read
/// path is not `inline`; below v6, the shell assembles values as before.
pub const WIRE_SEGMENTED: u32 = 6;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EndpointManifest {
    pub protocol_mode: ProtocolMode,
    pub min_protocol_version: u32,
    pub max_protocol_version: u32,
    pub services: Vec<ServiceManifest>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProtocolMode {
    BidiStream,
    RequestResponse,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ServiceManifest {
    pub name: String,
    pub ty: ServiceType,
    pub handlers: Vec<HandlerManifest>,
    /// The contract shared by every handler — a handler's own declaration overrides it.
    #[serde(flatten, default)]
    pub contract: Contract,
}

/// How to give up after `maxAttempts` attempts. The names match Restate's
/// service protocol manifest.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OnMaxAttempts {
    /// Keep the invocation `paused`, waiting for `resume`.
    Pause,
    /// End the invocation with an error.
    Kill,
}

/// The contract of a service or handler.
///
/// **The field names match the endpoint manifest of Restate's service
/// protocol** (`inactivityTimeout`, `retryPolicyInitialInterval`, …) — it
/// costs nothing and keeps the compatibility path open. Every field is an
/// *addition* to the discovery JSON: an old SDK declaring nothing gets the
/// node's defaults as before, so no wire bump. Durations are in ms. The values
/// are captured into the deployment at registration: changing the policy means
/// registering a new deployment, and running invocations keep their policy.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Contract {
    /// Limit on SDK silence within a turn — overrides `--inactivity-timeout-ms`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inactivity_timeout: Option<u64>,
    /// How long to keep finished invocations — overrides `--retention-ms`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub journal_retention: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_policy_initial_interval: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_policy_max_interval: Option<u64>,
    /// `serde_json::Number` rather than `f64`: the manifest must be comparable
    /// with `Eq` (the registry refresh loop relies on it to know what changed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_policy_exponentiation_factor: Option<serde_json::Number>,
    /// Total number of attempts, **including the first**. Absent = unlimited.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_policy_max_attempts: Option<u32>,
    /// Default `PAUSE`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_policy_on_max_attempts: Option<OnMaxAttempts>,
    /// `"runtime"` ⇒ the runtime writes one span per turn and per `ctx.run`
    /// into `$trace`. Absent or `"off"` ⇒ it does not (the
    /// `cairnflow.run`/`cairnflow.turn` spans then only go to OTLP).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace: Option<String>,
    /// Flush durable streams not yet carried by any commit at this interval
    /// (ms), timed by the node's clock. Absent = only ride along existing
    /// commits and flush when full (the default, 0 fixed PUTs).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_flush_ms: Option<u64>,
}

impl Contract {
    /// `self` overrides `base` field by field.
    pub fn over(&self, base: &Contract) -> Contract {
        Contract {
            inactivity_timeout: self.inactivity_timeout.or(base.inactivity_timeout),
            journal_retention: self.journal_retention.or(base.journal_retention),
            retry_policy_initial_interval: self
                .retry_policy_initial_interval
                .or(base.retry_policy_initial_interval),
            retry_policy_max_interval: self
                .retry_policy_max_interval
                .or(base.retry_policy_max_interval),
            retry_policy_exponentiation_factor: self
                .retry_policy_exponentiation_factor
                .clone()
                .or_else(|| base.retry_policy_exponentiation_factor.clone()),
            retry_policy_max_attempts: self
                .retry_policy_max_attempts
                .or(base.retry_policy_max_attempts),
            retry_policy_on_max_attempts: self
                .retry_policy_on_max_attempts
                .or(base.retry_policy_on_max_attempts),
            trace: self.trace.clone().or_else(|| base.trace.clone()),
            log_flush_ms: self.log_flush_ms.or(base.log_flush_ms),
        }
    }

    /// The runtime writes spans into `$trace` (`trace: "runtime"`).
    pub fn runtime_trace(&self) -> bool {
        self.trace.as_deref() == Some("runtime")
    }

    /// Whether any retry policy is declared — if not, the node's default backoff.
    pub fn has_retry_policy(&self) -> bool {
        self.retry_policy_initial_interval.is_some()
            || self.retry_policy_max_interval.is_some()
            || self.retry_policy_exponentiation_factor.is_some()
    }

    /// How long to wait before attempt `attempt + 1`, after `attempt`
    /// consecutive failures (`attempt ≥ 1`). `None` = use the default backoff.
    pub fn retry_delay_ms(&self, attempt: u32) -> Option<u64> {
        if !self.has_retry_policy() {
            return None;
        }
        let initial = self.retry_policy_initial_interval.unwrap_or(100) as f64;
        let factor = self
            .retry_policy_exponentiation_factor
            .as_ref()
            .and_then(serde_json::Number::as_f64)
            .filter(|f| f.is_finite() && *f >= 1.0)
            .unwrap_or(2.0);
        let max = self.retry_policy_max_interval.unwrap_or(60_000) as f64;
        let d = initial * factor.powi(attempt.saturating_sub(1).min(64) as i32);
        Some(d.min(max).max(0.0) as u64)
    }

    pub fn on_max_attempts(&self) -> OnMaxAttempts {
        self.retry_policy_on_max_attempts
            .unwrap_or(OnMaxAttempts::Pause)
    }

    /// Checks values at registration: wrong ⇒ refuse at once, not at runtime.
    fn check(&self, owner: &str) -> Result<(), DiscoveryError> {
        let bad = |why: &str| Err(DiscoveryError::BadContract(format!("{owner}: {why}")));
        if self.retry_policy_max_attempts == Some(0) {
            return bad("retryPolicyMaxAttempts must be ≥ 1 (including the first attempt)");
        }
        if let Some(f) = self.retry_policy_exponentiation_factor.as_ref() {
            if !f.as_f64().is_some_and(|f| f.is_finite() && f >= 1.0) {
                return bad("retryPolicyExponentiationFactor must be a number ≥ 1");
            }
        }
        if self.inactivity_timeout == Some(0) {
            return bad("inactivityTimeout must be > 0");
        }
        if self
            .inactivity_timeout
            .is_some_and(|ms| ms > INACTIVITY_TIMEOUT_MAX_MS)
        {
            return bad("inactivityTimeout must be ≤ 3600000 (one hour)");
        }
        if self.log_flush_ms == Some(0) {
            return bad("logFlushMs must be > 0");
        }
        if let Some(t) = self.trace.as_deref() {
            if t != "runtime" && t != "off" {
                return bad("trace must be \"runtime\" or \"off\"");
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ServiceType {
    Service,
    VirtualObject,
    /// A multi-step process, **one run per workflow id**.
    ///
    /// It differs from a Virtual Object in exactly one place, and that place
    /// changes everything else: the identity of the run *is*
    /// `(scope, service, key)`, so it needs no lock for exclusivity — a second
    /// call computes the same id and is stopped at the status lookup. `SHARED`
    /// handlers run alongside it to complete promises and read state.
    Workflow,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct HandlerManifest {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ty: Option<HandlerType>,
    /// The handler's own contract — overrides the service's.
    #[serde(flatten, default)]
    pub contract: Contract,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum HandlerType {
    Exclusive,
    Shared,
    /// The main run of a workflow. Only valid in a `WORKFLOW` service, and
    /// **exactly one** handler per service — see [`DiscoveryError::WorkflowRunCount`].
    Workflow,
}

impl ServiceType {
    /// What does a handler that declares no `ty` default to?
    ///
    /// `(Workflow, None) → SHARED`: inside a workflow, a plain handler is the
    /// door through which the outside world knocks on the run — by default it
    /// is *not* the main run.
    ///
    /// `(VirtualObject, None) → EXCLUSIVE`, as in Restate's service protocol.
    /// This used to be "untyped" and did **not** take the VO lock: following
    /// the SDK's first example (which declares no `handlerTypes`), two turns
    /// writing state of the same object ran in parallel and lost updates. The
    /// safe default is the lock; whoever wants parallel reads declares
    /// `SHARED`.
    fn default_handler_type(self) -> Option<HandlerType> {
        match self {
            Self::Service => None,
            Self::VirtualObject => Some(HandlerType::Exclusive),
            Self::Workflow => Some(HandlerType::Shared),
        }
    }

    /// Whether a handler of this kind is allowed in a service of that kind.
    fn accepts(self, handler: Option<HandlerType>) -> bool {
        matches!(
            (self, handler),
            (Self::Service, None)
                | (
                    Self::VirtualObject,
                    None | Some(HandlerType::Exclusive | HandlerType::Shared)
                )
                | (
                    Self::Workflow,
                    None | Some(HandlerType::Shared | HandlerType::Workflow)
                )
        )
    }
}

impl HandlerManifest {
    /// The handler's actual kind, with the service's default applied.
    pub fn effective_ty(&self, service: ServiceType) -> Option<HandlerType> {
        self.ty.or_else(|| service.default_handler_type())
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DiscoveryError {
    #[error(
        "no common wire version: SDK supports {sdk_min}..={sdk_max}, runtime supports {rt_min}..={rt_max}"
    )]
    NoCommonWireVersion {
        sdk_min: u32,
        sdk_max: u32,
        rt_min: u32,
        rt_max: u32,
    },

    #[error("SDK declares a reversed version range: {min}..={max}")]
    InvertedRange { min: u32, max: u32 },

    #[error("SDK returned an unrecognized content-type: {0:?}")]
    UnsupportedDiscoveryVersion(String),

    #[error("cannot read manifest: {0}")]
    Malformed(String),

    /// The egress policy refused the endpoint — `400 egress_denied`, no byte
    /// left the node.
    #[error("{0}")]
    EgressDenied(crate::egress::Denied),

    #[error("/discover body exceeds the {} MiB limit", DISCOVERY_BODY_MAX >> 20)]
    TooLarge,

    #[error("deployment has no services")]
    NoServices,

    /// The manifest exceeds the cap on the number of services, handlers or the
    /// name length.
    #[error("manifest is too large: {0}")]
    TooManyEntries(String),

    #[error("service {service:?} declares duplicate handler name {handler:?}")]
    DuplicateHandler { service: String, handler: String },

    #[error("deployment declares duplicate service name: {0:?}")]
    DuplicateService(String),

    #[error("invalid service name: {0:?} — must not be empty, `.`/`..`, or contain control characters or `/` `\\` `?` `#` `%`")]
    BadServiceName(String),

    #[error("service {service:?}: invalid handler name: {handler:?} — must not be empty, `.`/`..`, or contain control characters or `/` `\\` `?` `#` `%`")]
    BadHandlerName { service: String, handler: String },

    #[error("invalid handler contract: {0}")]
    BadContract(String),

    #[error("service {service:?} of type {service_ty:?} does not accept handler {handler:?} of type {handler_ty:?}")]
    BadHandlerType {
        service: String,
        service_ty: ServiceType,
        handler: String,
        handler_ty: Option<HandlerType>,
    },

    #[error(
        "workflow {service:?} declares {found} WORKFLOW handlers — must be exactly one (the run id is a function of (scope, service, key) and does not include the handler name, so two WORKFLOW handlers would share one id)"
    )]
    WorkflowRunCount { service: String, found: usize },
}

/// Chooses the wire version: **the largest value within both ranges**.
///
/// No overlap ⇒ refuse **at registration**, not at invoke. That is
/// deliberate: the operator finds out on deploy, rather than when a customer's
/// first invocation fails.
pub fn negotiate_wire_version(sdk_min: u32, sdk_max: u32) -> Result<u32, DiscoveryError> {
    if sdk_min > sdk_max {
        return Err(DiscoveryError::InvertedRange {
            min: sdk_min,
            max: sdk_max,
        });
    }
    let lo = sdk_min.max(WIRE_MIN);
    let hi = sdk_max.min(WIRE_MAX);
    if lo > hi {
        return Err(DiscoveryError::NoCommonWireVersion {
            sdk_min,
            sdk_max,
            rt_min: WIRE_MIN,
            rt_max: WIRE_MAX,
        });
    }
    Ok(hi)
}

/// Checks the content-type the SDK returns from `/discover`.
///
/// Compares the media type part, ignoring parameters (`; charset=utf-8`) —
/// proxies and frameworks like to add those, and refusing because of them
/// would be a wrong refusal.
pub fn check_discovery_content_type(header: &str) -> Result<(), DiscoveryError> {
    let media = header.split(';').next().unwrap_or("").trim();
    if media.eq_ignore_ascii_case(DISCOVERY_V1) {
        Ok(())
    } else {
        Err(DiscoveryError::UnsupportedDiscoveryVersion(
            header.to_string(),
        ))
    }
}

/// The result of registering a deployment. `Serialize`/`Deserialize` for
/// storage.
///
/// **Immutable once it has an id**: the discovered manifest is captured here
/// and never changes. Registering the same endpoint again is a *new*
/// deployment, with a new id; an invocation is bound to the id at creation,
/// so changing code behind an unfinished invocation must be done on purpose
/// (`resume` after moving it, or a trial replay), not as a side effect of a
/// registration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Deployment {
    /// `dp_…`, issued at registration ([`new_deployment_id`]). Empty = not
    /// registered yet (the result of [`accept`] before an id is issued).
    #[serde(default)]
    pub id: String,
    /// The tenant owning the deployment — service names belong to the tenant.
    #[serde(default = "default_tenant")]
    pub tenant: String,
    /// Registration time, Unix ms — for readers to sort by, not used for routing.
    #[serde(default)]
    pub created_at_ms: u64,
    pub endpoint: String,
    pub wire_version: u32,
    pub protocol_mode: ProtocolMode,
    pub services: Vec<ServiceManifest>,
    /// The identity scheme the SDK declared it verifies at `/discover`
    /// (`requestIdentity`) — `v1`, or `None` when the SDK does not verify
    /// requests. The SaaS profile refuses `None` deployments.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_identity: Option<String>,
    /// `REQUEST_RESPONSE` body compression the SDK declared at `/discover`
    /// (`requestCompression`, modelled on the `lambdaCompression` field of
    /// Restate's service protocol). Only [`RR_GZIP`]; absent (the SDK declares
    /// none, an unknown value, a deployment registered before this field
    /// existed, or a node with `--no-rr-compression`) ⇒ uncompressed bodies as
    /// before. Older binaries ignore the unknown field ⇒ no compression — still
    /// correct.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rr_compression: Option<String>,
    /// The SDK declared `replayCache: true` at `/discover`: it keeps the journal
    /// of running invocations and accepts deltas on wire v5. Absent (an older
    /// SDK, a deployment registered before this field existed) ⇒ full replay
    /// as before; older binaries ignore the unknown field ⇒ full replay — still
    /// correct.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub replay_cache: bool,
    /// How the transport mode was chosen at registration. Absent on
    /// deployments registered before this field existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport: Option<Transport>,
}

/// The policy choosing the transport mode at registration.
///
/// - `declared` (default — the earlier behaviour): the mode is the one the SDK
///   declares.
/// - `auto`: an SDK declaring `BIDI_STREAM` gets `/discover` probed once
///   through the h2 client ([`probe_bidi`]) — `BIDI_STREAM` only works when
///   HTTP/2 gets through to the SDK; a failed probe (a proxy speaking only h1,
///   an edge buffering request bodies) ⇒ `REQUEST_RESPONSE`. An SDK declaring
///   `REQUEST_RESPONSE` keeps it: it has its reasons (Lambda, the Fetch API —
///   `sdk/typescript/src/fetch.ts`).
///
/// The default has not switched to `auto` yet: that waits for a measurement
/// of `REQUEST_RESPONSE` replay bytes on a real store.
///
/// No variants are added: the enum has no `#[serde(other)]`, so an older
/// binary meeting an unknown value cannot read the deployment. The SaaS
/// profile's rule `https://` ⇒ `REQUEST_RESPONSE` is recorded with existing
/// values — `declared` + mode `REQUEST_RESPONSE` + a reason in `bidiProbe`
/// ([`BIDI_SKIPPED_HTTPS`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TransportPolicy {
    #[default]
    Declared,
    Auto,
}

impl TransportPolicy {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "declared" => Some(Self::Declared),
            "auto" => Some(Self::Auto),
            _ => None,
        }
    }
}

/// The transport mode chosen and why — recorded in the deployment (JSON read leniently).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transport {
    pub policy: TransportPolicy,
    /// The mode the SDK declares in its manifest.
    pub declared: ProtocolMode,
    /// HTTP version of the `/discover` call (`HTTP/1.1`, `HTTP/2` — the
    /// tenant's h1 client negotiates ALPN over TLS).
    pub discovered_http: String,
    /// `auto` + BIDI declared: `ok`, or `failed: <reason>`. BIDI declared but
    /// `--deploy-https-request-response` settled on `REQUEST_RESPONSE`:
    /// [`BIDI_SKIPPED_HTTPS`]. Absent when there was no probe and no mode
    /// change.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bidi_probe: Option<String>,
}

/// The `bidiProbe` of an `https://` deployment declaring `BIDI_STREAM` that
/// `--deploy-https-request-response` (the SaaS profile) registered as
/// `REQUEST_RESPONSE` (a hosted cell accepts only `REQUEST_RESPONSE` over
/// `https://`). The `skipped:` prefix is stable; the rest is text for human
/// readers.
///
/// Why no probe: the TLS edge of most platforms (Heroku, Railway, Render,
/// Vercel, Netlify) speaks h1 to the app and buffers request bodies, so every
/// BIDI turn hangs until `inactivityTimeout`. The `auto` probe is a GET with
/// no body — it gets through even body-buffering edges — so it is not enough
/// to go on.
pub const BIDI_SKIPPED_HTTPS: &str = "skipped: https:// endpoint registered as REQUEST_RESPONSE (--deploy-https-request-response, profile saas) — TLS edges usually speak HTTP/1 to the app and buffer the request body, so BIDI_STREAM hangs there; if the platform speaks HTTP/2 all the way to the app, register with transport: declared to keep BIDI_STREAM";

/// Whether `endpoint` (already through discovery) is `https://`.
pub fn is_https(endpoint: &str) -> bool {
    reqwest::Url::parse(endpoint).is_ok_and(|u| u.scheme() == "https")
}

/// The HTTP version name as readers know it.
fn http_name(v: reqwest::Version) -> &'static str {
    match v {
        reqwest::Version::HTTP_09 => "HTTP/0.9",
        reqwest::Version::HTTP_10 => "HTTP/1.0",
        reqwest::Version::HTTP_11 => "HTTP/1.1",
        reqwest::Version::HTTP_2 => "HTTP/2",
        reqwest::Version::HTTP_3 => "HTTP/3",
        _ => "HTTP/?",
    }
}

/// Limit on the BIDI probe ([`probe_bidi`]).
pub const BIDI_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Probes whether HTTP/2 gets through to the SDK: one `GET {endpoint}/discover`
/// through the `BIDI_STREAM` client (h2c prior-knowledge, h2 over TLS) — the
/// very client every BIDI turn will use. `Ok` ⇔ a `2xx` response over HTTP/2
/// with the discovery content-type. Signs the identity like discovery does
/// (an SDK with `identityKeys` refuses unsigned requests).
pub async fn probe_bidi(
    client: &crate::egress::TenantHttp,
    endpoint: &str,
    signing: Option<(&crate::identity::Identity, &str)>,
) -> Result<(), String> {
    let url = format!("{}/discover", endpoint.trim_end_matches('/'));
    let mut req = client
        .client()
        .get(&url)
        .timeout(BIDI_PROBE_TIMEOUT)
        .header(reqwest::header::ACCEPT, DISCOVERY_V1);
    if let Some((identity, tenant)) = signing {
        req = crate::invoker::Signing {
            identity,
            tenant,
            jti: crate::identity::DISCOVER_JTI,
            now_ms: cairn_store::entropy::now_ms(),
        }
        .apply(&url, None, req);
    }
    let resp = crate::egress::send_for_tenant(client, req)
        .await
        .map_err(|e| match e.denied() {
            Some(d) => d.to_string(),
            None => format!("h2 cannot reach the SDK ({})", short_error(&e)),
        })?;
    if resp.version() != reqwest::Version::HTTP_2 {
        return Err(format!("SDK answered over {}", http_name(resp.version())));
    }
    if !resp.status().is_success() {
        return Err(format!("SDK returned {} over h2", resp.status()));
    }
    let ct = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    check_discovery_content_type(&ct).map_err(|e| e.to_string())
}

/// A short transport error for `bidiProbe`: the kind of error, never echoing
/// anything from the SDK's side.
fn short_error(e: &crate::egress::TenantError) -> &'static str {
    match e {
        crate::egress::TenantError::Http(e) if e.is_timeout() => "timed out",
        crate::egress::TenantError::Http(e) if e.is_connect() => "cannot open connection",
        _ => "protocol error",
    }
}

fn default_tenant() -> String {
    cairn_store::TenantId::DEFAULT.to_string()
}

/// A new deployment id: `dp_` + the ms timestamp (hex, 12 characters — sorts by
/// time when read by eye) + 64 random bits. No shared counter needed: the
/// object is written with `PutMode::Create`, so two equal ids (probability
/// ~2⁻⁶⁴ within the same ms) make the second write fail instead of
/// overwriting.
pub fn new_deployment_id(now_ms: u64) -> String {
    let mut b = [0u8; 8];
    cairn_store::entropy::fill(&mut b);
    format!("dp_{now_ms:012x}{:016x}", u64::from_be_bytes(b))
}

impl Deployment {
    pub fn handler(&self, service: &str, handler: &str) -> Option<&HandlerManifest> {
        self.services
            .iter()
            .find(|s| s.name == service)?
            .handlers
            .iter()
            .find(|h| h.name == handler)
    }

    pub fn service(&self, service: &str) -> Option<&ServiceManifest> {
        self.services.iter().find(|s| s.name == service)
    }

    /// The effective contract of `(service, handler)`: the handler overrides
    /// the service. None ⇒ an empty contract (the node's defaults).
    pub fn contract(&self, service: &str, handler: &str) -> Contract {
        let Some(svc) = self.service(service) else {
            return Contract::default();
        };
        match svc.handlers.iter().find(|h| h.name == handler) {
            Some(h) => h.contract.over(&svc.contract),
            None => svc.contract.clone(),
        }
    }

    /// The contract of a `(service, handler)` — what gets stamped into entry 0
    /// when an invocation is created. See [`cairn_engine::InvocationKind`].
    ///
    /// A service not in the registry ⇒ `Plain`. Callers must check existence
    /// first (every creation path does), so returning the default here is
    /// right: guessing a contract for a service we know nothing about would be
    /// worse.
    pub fn kind(&self, service: &str, handler: &str) -> cairn_engine::InvocationKind {
        use cairn_engine::InvocationKind as K;
        let Some(svc) = self.service(service) else {
            return K::Plain;
        };
        if svc.ty != ServiceType::Workflow {
            return K::Plain;
        }
        match self
            .handler(service, handler)
            .and_then(|h| h.effective_ty(svc.ty))
        {
            Some(HandlerType::Workflow) => K::WorkflowRun,
            _ => K::WorkflowShared,
        }
    }
}

/// A service/handler name usable as **one** path segment of the SDK call URL
/// (`/invoke/{service}/{handler}`), and as an unambiguous key in the catalog:
/// not empty, not `.`/`..` (normalised away), no control characters, no
/// characters a URL treats as structure — `/`, `\` (like `/` for http(s)),
/// `?`, `#`, `%`.
fn segment_name(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && !name
            .chars()
            .any(|c| c.is_control() || matches!(c, '/' | '\\' | '?' | '#' | '%'))
}

/// Cap on the number of services in a deployment. Before it, the only cap was
/// the 1 MiB of `/discover` bytes ⇒ ~75 000 handlers; the deployment registry
/// keeps the whole manifest on **every** node.
pub const MAX_SERVICES: usize = 256;
/// Cap on the number of handlers in a service.
pub const MAX_HANDLERS_PER_SERVICE: usize = 1_024;
/// Cap on the length (bytes) of service and handler names: names go into state
/// keys, the catalog and the SDK call URL.
pub const MAX_NAME_BYTES: usize = 256;

/// Checks the manifest, then settles the deployment.
pub fn accept(endpoint: &str, manifest: EndpointManifest) -> Result<Deployment, DiscoveryError> {
    let wire_version =
        negotiate_wire_version(manifest.min_protocol_version, manifest.max_protocol_version)?;
    if manifest.services.is_empty() {
        return Err(DiscoveryError::NoServices);
    }
    // Caps before every per-name check: the duplicate check below uses a hash
    // set, no longer O(n²) on a tokio thread.
    if manifest.services.len() > MAX_SERVICES {
        return Err(DiscoveryError::TooManyEntries(format!(
            "{} services, the limit is {MAX_SERVICES}",
            manifest.services.len()
        )));
    }
    for s in &manifest.services {
        if s.name.len() > MAX_NAME_BYTES {
            return Err(DiscoveryError::TooManyEntries(format!(
                "a service name is {} bytes, the limit is {MAX_NAME_BYTES}",
                s.name.len()
            )));
        }
        if s.handlers.len() > MAX_HANDLERS_PER_SERVICE {
            return Err(DiscoveryError::TooManyEntries(format!(
                "service {:?} declares {} handlers, the limit is {MAX_HANDLERS_PER_SERVICE}",
                s.name,
                s.handlers.len()
            )));
        }
        if let Some(h) = s.handlers.iter().find(|h| h.name.len() > MAX_NAME_BYTES) {
            return Err(DiscoveryError::TooManyEntries(format!(
                "service {:?}: a handler name is {} bytes, the limit is {MAX_NAME_BYTES}",
                s.name,
                h.name.len()
            )));
        }
    }
    let mut service_names = std::collections::BTreeSet::new();
    // Duplicate names make routing ambiguous, and that ambiguity would surface
    // as "the invocation ran the wrong handler" — very hard to trace. Stop it
    // here.
    for s in &manifest.services {
        // The service name goes into the catalog (a `service\t<name>\t<id>`
        // line) and into state keys — a control character or an empty name
        // here is an ambiguous key.
        if !segment_name(&s.name) {
            return Err(DiscoveryError::BadServiceName(s.name.clone()));
        }
        s.contract.check(&s.name)?;
        for h in &s.handlers {
            h.contract.check(&format!("{}/{}", s.name, h.name))?;
        }
        if !service_names.insert(s.name.as_str()) {
            return Err(DiscoveryError::DuplicateService(s.name.clone()));
        }
        let mut handler_names = std::collections::BTreeSet::new();
        for h in &s.handlers {
            // The handler name goes into the SDK call URL
            // (`/invoke/{service}/{handler}`): a `/` there is a different path.
            if !segment_name(&h.name) {
                return Err(DiscoveryError::BadHandlerName {
                    service: s.name.clone(),
                    handler: h.name.clone(),
                });
            }
            if !handler_names.insert(h.name.as_str()) {
                return Err(DiscoveryError::DuplicateHandler {
                    service: s.name.clone(),
                    handler: h.name.clone(),
                });
            }
            // A wrong (service kind, handler kind) pair only shows at runtime —
            // an `EXCLUSIVE` in an unkeyed service locks nothing, a `WORKFLOW`
            // in a virtual object gets no deterministic id. Both run and are
            // silently wrong, so stop them at registration.
            if !s.ty.accepts(h.ty) {
                return Err(DiscoveryError::BadHandlerType {
                    service: s.name.clone(),
                    service_ty: s.ty,
                    handler: h.name.clone(),
                    handler_ty: h.ty,
                });
            }
        }
        // **Exactly one** `WORKFLOW` handler per workflow: the run's id does
        // not carry the handler name (`workflow_invocation_id`), so two
        // `WORKFLOW` handlers would be two entrances sharing one identity —
        // calling the second would be treated as a duplicate of the first.
        // With no such handler the service has no run to be a workflow of.
        if s.ty == ServiceType::Workflow {
            let runs = s
                .handlers
                .iter()
                .filter(|h| h.effective_ty(s.ty) == Some(HandlerType::Workflow))
                .count();
            if runs != 1 {
                return Err(DiscoveryError::WorkflowRunCount {
                    service: s.name.clone(),
                    found: runs,
                });
            }
        }
    }
    Ok(Deployment {
        id: String::new(),
        tenant: default_tenant(),
        created_at_ms: 0,
        endpoint: endpoint.trim_end_matches('/').to_string(),
        wire_version,
        protocol_mode: manifest.protocol_mode,
        services: manifest.services,
        request_identity: None,
        rr_compression: None,
        replay_cache: false,
        transport: None,
    })
}

/// The only value of `requestCompression` the runtime understands.
pub const RR_GZIP: &str = "gzip";

/// `requestIdentity` and `requestCompression` of the manifest: read
/// separately, not added to [`EndpointManifest`] — older binaries ignore
/// unknown fields like any other.
#[derive(Deserialize)]
struct ManifestIdentity {
    #[serde(default, rename = "requestIdentity")]
    request_identity: Option<String>,
    #[serde(default, rename = "requestCompression")]
    request_compression: Option<String>,
    /// `replayCache` — only meaningful on wire ≥ v5.
    #[serde(default, rename = "replayCache")]
    replay_cache: Option<bool>,
}

/// Cap on the `/discover` body: a real manifest is a few KB; every
/// registration is a GET to a host the tenant chooses, so we do not read
/// unbounded bytes from it into RAM.
pub const DISCOVERY_BODY_MAX: usize = 1 << 20;

/// Upper limit of the `inactivityTimeout` declared in a contract: one hour —
/// wider than any real handler standing still mid-turn, narrow enough that an
/// SDK cannot hold a turn slot and a response-reading stream forever.
pub const INACTIVITY_TIMEOUT_MAX_MS: u64 = 3_600_000;

/// Limit on the whole `/discover` request (connect + headers + body).
pub const DISCOVERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Calls `GET {endpoint}/discover` and settles the deployment. Goes through the
/// tenant client (egress policy, no redirects, no proxy); `signing` = (signer,
/// registering tenant) attaches request identity (`jti`
/// [`crate::identity::DISCOVER_JTI`]).
pub async fn discover(
    client: &crate::egress::TenantHttp,
    endpoint: &str,
    signing: Option<(&crate::identity::Identity, &str)>,
) -> Result<Deployment, DiscoveryError> {
    let base = endpoint.trim_end_matches('/');
    let url = format!("{base}/discover");
    let mut req = client
        .client()
        .get(&url)
        .timeout(DISCOVERY_TIMEOUT)
        // List EVERY discovery version the runtime supports; the SDK picks one.
        .header(reqwest::header::ACCEPT, DISCOVERY_V1);
    if let Some((identity, tenant)) = signing {
        req = crate::invoker::Signing {
            identity,
            tenant,
            jti: crate::identity::DISCOVER_JTI,
            now_ms: cairn_store::entropy::now_ms(),
        }
        .apply(&url, None, req);
    }
    let mut resp = crate::egress::send_for_tenant(client, req)
        .await
        .map_err(|e| match e.denied() {
            Some(d) => DiscoveryError::EgressDenied(d.clone()),
            None => DiscoveryError::Malformed(e.to_string()),
        })?;
    // No redirects (tenant client): `3xx` is an error, `Location` is not echoed.
    if resp.status().is_redirection() {
        return Err(DiscoveryError::Malformed(format!(
            "/discover returned {} — the runtime does not follow redirects",
            resp.status()
        )));
    }

    let version = resp.version();
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    check_discovery_content_type(&content_type)?;

    if resp
        .content_length()
        .is_some_and(|n| n > DISCOVERY_BODY_MAX as u64)
    {
        return Err(DiscoveryError::TooLarge);
    }
    let mut body = Vec::new();
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| DiscoveryError::Malformed(e.to_string()))?
    {
        if body.len() + chunk.len() > DISCOVERY_BODY_MAX {
            return Err(DiscoveryError::TooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    let manifest: EndpointManifest =
        serde_json::from_slice(&body).map_err(|e| DiscoveryError::Malformed(e.to_string()))?;
    let extras = serde_json::from_slice::<ManifestIdentity>(&body).ok();
    let mut d = accept(base, manifest)?;
    d.request_identity = extras.as_ref().and_then(|m| m.request_identity.clone());
    d.replay_cache = d.wire_version >= WIRE_REPLAY_DELTA
        && extras
            .as_ref()
            .and_then(|m| m.replay_cache)
            .unwrap_or(false);
    d.rr_compression = extras
        .and_then(|m| m.request_compression)
        .filter(|c| c == RR_GZIP);
    d.transport = Some(Transport {
        policy: TransportPolicy::Declared,
        declared: d.protocol_mode,
        discovered_http: http_name(version).to_string(),
        bidi_probe: None,
    });
    Ok(d)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest(min: u32, max: u32) -> EndpointManifest {
        EndpointManifest {
            protocol_mode: ProtocolMode::RequestResponse,
            min_protocol_version: min,
            max_protocol_version: max,
            services: vec![ServiceManifest {
                name: "OrderService".into(),
                ty: ServiceType::VirtualObject,
                handlers: vec![HandlerManifest {
                    name: "submit".into(),
                    ty: Some(HandlerType::Exclusive),
                    contract: Default::default(),
                }],
                contract: Default::default(),
            }],
        }
    }

    /// `inactivityTimeout` has an upper limit: an SDK declaring an unbounded
    /// wait would hold a turn slot and a response-reading stream forever.
    /// Registration must refuse values above [`INACTIVITY_TIMEOUT_MAX_MS`];
    /// exactly at the limit is accepted.
    #[test]
    fn inactivity_timeout_has_an_upper_bound() {
        let mut m = manifest(1, WIRE_MAX);
        m.services[0].handlers[0].contract.inactivity_timeout = Some(INACTIVITY_TIMEOUT_MAX_MS);
        accept("http://sdk", m.clone()).expect("at the limit is accepted");
        m.services[0].handlers[0].contract.inactivity_timeout = Some(INACTIVITY_TIMEOUT_MAX_MS + 1);
        let err = accept("http://sdk", m.clone()).expect_err("over the limit");
        assert!(matches!(err, DiscoveryError::BadContract(_)), "{err}");
        m.services[0].handlers[0].contract.inactivity_timeout = None;
        m.services[0].contract.inactivity_timeout = Some(u64::MAX);
        assert!(
            accept("http://sdk", m).is_err(),
            "the limit applies at service level too"
        );
    }

    /// **The manifest has caps on the number of services, handlers and the
    /// name length.** Before them the only cap was 1 MiB of bytes:
    /// `{"name":"aN"}` × ~75 000 handlers got through, the O(n²) duplicate
    /// check blocked a worker for seconds per registration, and the deployment
    /// registry of **every** node kept the whole manifest.
    ///
    /// Must see: 50 000 handlers, 1 000 services, 10 KiB names are all refused
    /// at registration; a manifest exactly at the caps is still accepted.
    #[test]
    #[allow(
        clippy::disallowed_methods,
        clippy::disallowed_types,
        reason = "measures the real time of a synchronous check, outside deterministic simulation"
    )]
    fn manifest_size_is_bounded() {
        let handler = |i: usize| HandlerManifest {
            name: format!("h{i}"),
            ty: Some(HandlerType::Exclusive),
            contract: Default::default(),
        };
        let mut many_handlers = manifest(1, WIRE_MAX);
        many_handlers.services[0].handlers = (0..50_000).map(handler).collect();
        let started = std::time::Instant::now();
        assert!(
            accept("http://sdk", many_handlers).is_err(),
            "50 000 handler"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_millis(500),
            "refusing 50 000 handlers took {:?}",
            started.elapsed()
        );
        let mut many_services = manifest(1, WIRE_MAX);
        let one = many_services.services[0].clone();
        many_services.services = (0..1_000)
            .map(|i| ServiceManifest {
                name: format!("S{i}"),
                ..one.clone()
            })
            .collect();
        assert!(
            accept("http://sdk", many_services).is_err(),
            "1 000 service"
        );
        let mut long = manifest(1, WIRE_MAX);
        long.services[0].name = "s".repeat(10 << 10);
        assert!(accept("http://sdk", long).is_err(), "10 KiB service name");
        let mut long = manifest(1, WIRE_MAX);
        long.services[0].handlers[0].name = "h".repeat(10 << 10);
        assert!(accept("http://sdk", long).is_err(), "10 KiB handler name");
        let mut at_cap = manifest(1, WIRE_MAX);
        at_cap.services[0].handlers = (0..MAX_HANDLERS_PER_SERVICE).map(handler).collect();
        at_cap.services[0].name = "s".repeat(MAX_NAME_BYTES);
        accept("http://sdk", at_cap).expect("at the caps is accepted");
    }

    /// The handler name goes into the SDK call URL (`/invoke/{service}/{handler}`)
    /// like the service name: a `/` or a control character there is a
    /// different path. Registration must refuse it, as for service names.
    #[test]
    fn handler_name_with_slash_or_control_is_rejected() {
        for bad in ["a/b", "../x", "", "x\ny"] {
            let mut m = manifest(1, WIRE_MAX);
            m.services[0].handlers[0].name = bad.into();
            let err = accept("http://sdk", m).expect_err(bad);
            assert!(
                err.to_string().contains(&format!("{bad:?}")),
                "{bad:?}: {err}"
            );
        }
        let ok = manifest(1, WIRE_MAX);
        assert!(accept("http://sdk", ok).is_ok());
    }

    /// Characters a URL treats as structure (`\\` like `/` for http(s), `?`,
    /// `#`, `%`) and `.`/`..` segments (normalised away) also change the SDK
    /// call path — refused in handler names and service names alike.
    #[test]
    fn url_structural_names_are_rejected() {
        for bad in ["a\\b", "a?b", "a#b", "a%2Fb", "..", "."] {
            let mut m = manifest(1, WIRE_MAX);
            m.services[0].handlers[0].name = bad.into();
            assert!(accept("http://sdk", m).is_err(), "handler {bad:?}");
            let mut m = manifest(1, WIRE_MAX);
            m.services[0].name = bad.into();
            assert!(accept("http://sdk", m).is_err(), "service {bad:?}");
        }
        for ok in ["submit", "Order_Service", "v2.run", "a-b"] {
            let mut m = manifest(1, WIRE_MAX);
            m.services[0].handlers[0].name = ok.into();
            assert!(accept("http://sdk", m).is_ok(), "handler {ok:?}");
        }
    }

    #[test]
    fn picks_highest_version_in_both_ranges() {
        assert_eq!(negotiate_wire_version(1, 1).unwrap(), 1);
        // SDK newer than the runtime: choose the runtime's maximum.
        assert_eq!(negotiate_wire_version(1, 7).unwrap(), WIRE_MAX);
    }

    /// An SDK that **speaks only v1** can still register, and settles on v1.
    ///
    /// That is the condition for v2 (flow control) to be an addition rather
    /// than a forced upgrade: old deployments keep running, their
    /// `ctx.scoped` just does not exist — and the runtime says v1 back in the
    /// content-type so a new SDK running on an old deployment knows to refuse.
    #[test]
    fn sdk_speaking_only_v1_still_registers() {
        assert_eq!(negotiate_wire_version(1, 1).unwrap(), 1);
    }

    #[test]
    fn disjoint_ranges_rejected_at_registration() {
        // The SDK only speaks versions above the runtime's maximum.
        let err = negotiate_wire_version(WIRE_MAX + 1, WIRE_MAX + 4).unwrap_err();
        assert!(matches!(err, DiscoveryError::NoCommonWireVersion { .. }));
        // And an SDK that is too old.
        assert!(negotiate_wire_version(0, 0).is_err());
    }

    #[test]
    fn inverted_range_rejected() {
        assert_eq!(
            negotiate_wire_version(5, 2).unwrap_err(),
            DiscoveryError::InvertedRange { min: 5, max: 2 }
        );
    }

    #[test]
    fn content_type_ignores_parameters() {
        // Proxies and frameworks like to add a charset; refusing because of it is a wrong refusal.
        assert!(check_discovery_content_type(DISCOVERY_V1).is_ok());
        assert!(check_discovery_content_type(&format!("{DISCOVERY_V1}; charset=utf-8")).is_ok());
        assert!(check_discovery_content_type("application/json").is_err());
        assert!(check_discovery_content_type("").is_err());
    }

    #[test]
    fn manifest_round_trips_through_json_with_camel_case_names() {
        // The field names are a contract with the SDK — changing them breaks it.
        let json = serde_json::to_string(&manifest(1, 1)).unwrap();
        assert!(json.contains("\"minProtocolVersion\""), "{json}");
        assert!(
            json.contains("\"protocolMode\":\"REQUEST_RESPONSE\""),
            "{json}"
        );
        assert!(json.contains("\"ty\":\"VIRTUAL_OBJECT\""), "{json}");
        let back: EndpointManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(back, manifest(1, 1));
    }

    #[test]
    fn duplicate_names_rejected_at_registration() {
        let mut m = manifest(1, 1);
        m.services[0].handlers.push(HandlerManifest {
            name: "submit".into(),
            ty: None,
            contract: Default::default(),
        });
        assert!(matches!(
            accept("http://x", m).unwrap_err(),
            DiscoveryError::DuplicateHandler { .. }
        ));

        let mut m = manifest(1, 1);
        let dup = m.services[0].clone();
        m.services.push(dup);
        assert!(matches!(
            accept("http://x", m).unwrap_err(),
            DiscoveryError::DuplicateService(_)
        ));
    }

    fn workflow_manifest(handlers: Vec<HandlerManifest>) -> EndpointManifest {
        EndpointManifest {
            protocol_mode: ProtocolMode::RequestResponse,
            min_protocol_version: 1,
            max_protocol_version: 1,
            services: vec![ServiceManifest {
                name: "Signup".into(),
                ty: ServiceType::Workflow,
                handlers,
                contract: Default::default(),
            }],
        }
    }

    fn h(name: &str, ty: Option<HandlerType>) -> HandlerManifest {
        HandlerManifest {
            name: name.into(),
            ty,
            contract: Default::default(),
        }
    }

    /// **Exactly one** `WORKFLOW` handler — not zero, not two.
    ///
    /// Not pedantry: the run's id is `hash(scope, service, key)` and does not
    /// carry the handler name, so two `WORKFLOW` handlers share one identity.
    /// That bug only shows at runtime, as "calling handler B returns 409
    /// because handler A already ran" — so it must die at registration.
    #[test]
    fn workflow_must_have_exactly_one_run_handler() {
        let ok = workflow_manifest(vec![
            h("run", Some(HandlerType::Workflow)),
            // No `ty` declared in a workflow ⇒ SHARED, not the run.
            h("click", None),
        ]);
        let d = accept("http://x", ok).unwrap();
        assert_eq!(
            d.kind("Signup", "run"),
            cairn_engine::InvocationKind::WorkflowRun
        );
        assert_eq!(
            d.kind("Signup", "click"),
            cairn_engine::InvocationKind::WorkflowShared
        );

        for handlers in [
            vec![h("click", None)],
            vec![
                h("run", Some(HandlerType::Workflow)),
                h("run2", Some(HandlerType::Workflow)),
            ],
        ] {
            assert!(matches!(
                accept("http://x", workflow_manifest(handlers)).unwrap_err(),
                DiscoveryError::WorkflowRunCount { .. }
            ));
        }
    }

    /// A wrong (service kind, handler kind) pair runs but is silently wrong:
    /// `EXCLUSIVE` in a workflow locks nothing, `WORKFLOW` outside a workflow
    /// gets no deterministic id. Stop it at registration.
    #[test]
    fn invalid_service_handler_pair_rejected() {
        let m = workflow_manifest(vec![
            h("run", Some(HandlerType::Workflow)),
            h("cart", Some(HandlerType::Exclusive)),
        ]);
        assert!(matches!(
            accept("http://x", m).unwrap_err(),
            DiscoveryError::BadHandlerType { .. }
        ));

        let mut m = manifest(1, 1);
        m.services[0].handlers[0].ty = Some(HandlerType::Workflow);
        assert!(matches!(
            accept("http://x", m).unwrap_err(),
            DiscoveryError::BadHandlerType { .. }
        ));

        let mut m = manifest(1, 1);
        m.services[0].ty = ServiceType::Service;
        m.services[0].handlers[0].ty = Some(HandlerType::Shared);
        assert!(matches!(
            accept("http://x", m).unwrap_err(),
            DiscoveryError::BadHandlerType { .. }
        ));
    }

    /// Plain services do **not** change meaning because workflows exist: a
    /// handler declaring no `ty` is still "untyped", not SHARED. Virtual
    /// objects: none declared ⇒ EXCLUSIVE.
    #[test]
    fn defaults_of_the_two_old_kinds_unchanged() {
        let d = accept("http://x", manifest(1, 1)).unwrap();
        assert_eq!(
            d.kind("OrderService", "submit"),
            cairn_engine::InvocationKind::Plain
        );
        assert_eq!(
            d.handler("OrderService", "submit")
                .unwrap()
                .effective_ty(ServiceType::VirtualObject),
            Some(HandlerType::Exclusive)
        );
        assert_eq!(ServiceType::Service.default_handler_type(), None);
        assert_eq!(
            ServiceType::VirtualObject.default_handler_type(),
            Some(HandlerType::Exclusive)
        );
    }

    #[test]
    fn deployment_strips_trailing_slash() {
        let d = accept("http://sdk:9080/", manifest(1, 1)).unwrap();
        assert_eq!(d.endpoint, "http://sdk:9080");
        assert!(d.handler("OrderService", "submit").is_some());
        assert!(d.handler("OrderService", "missing").is_none());
    }
}
