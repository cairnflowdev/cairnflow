// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! API errors: `ApiError`, the JSON error body, `store_error`.

use super::*;

// ─────────────────────────────────────────────────────────── errors

/// An API error. `leader` is the **machine-readable leader address**, present only on
/// `503 NotLeader`: the text in `error` is for people, and a client must not be forced
/// to parse a human sentence to learn where to go. With `leader` the client retries at
/// exactly that address; without it, it backs off and asks again.
pub(super) struct ApiError {
    pub(super) status: StatusCode,
    /// The text — **private to this module**: it is read out only through
    /// [`ApiError::public_message`] (a `500` through the [`opaque_internal`] mask), so a
    /// batch item or a report does not leak store text. The compiler is the source rule.
    message: String,
    pub(super) leader: Option<String>,
    /// `409 wrong_range` (D23): this node's range table, for the sender to learn from.
    /// `Box`: rare (internal port only), and inline it would take most of the size of
    /// `ApiError` — the `Err` arm of every handler (clippy `result_large_err`).
    pub(super) wrong_range: Option<Box<crate::transport::TableWire>>,
    /// The invocation this error is about, if the error happened after it got an identity.
    ///
    /// Not decoration. `InvokeResponse` carries `id` on **every** outcome, including
    /// `Failed` — but a lower-layer error (the SDK cannot be reached, write authority lost
    /// midway) leaves through the `ApiError` path, and that path has no `id`. The caller
    /// gets a sentence and nothing to look up: the invocation still exists, is still
    /// durable, will still be rescheduled — and they do not know its name.
    pub(super) id: Option<String>,
    /// `Some(ms)` ⇒ `429 range_overloaded`: `code` in the body, a `Retry-After` header
    /// (seconds, rounded up, at least 1) and `retry_after_ms`. `NonZeroU32` so the field
    /// fits in the padding next to `status` — `ApiError` is the `Err` arm of every
    /// handler, and growing it means clippy `result_large_err` on each one.
    pub(super) overloaded: Option<std::num::NonZeroU32>,
    /// A machine-readable code other than `range_overloaded`: one byte, for the same
    /// reason as `overloaded`.
    pub(super) code: Option<Code>,
    /// `503` "not the leader" of this range: the forwarding layer reads it through
    /// [`super::forward::NotLeaderAt`] to know where to send the request.
    /// Stores `range + 1` in a `NonZeroU32` so the field fits in the padding, for the same
    /// reason as `overloaded` — read/written through [`ApiError::partition`] and
    /// [`ApiError::at_partition`].
    pub(super) not_leader_of: Option<std::num::NonZeroU32>,
}

impl ApiError {
    /// The text put into a response body (batch item, replay report, …): like the body of
    /// this error itself — a `500` through the mask, a log lookup code instead of detail.
    pub(super) fn public_message(&self) -> String {
        if self.status == StatusCode::INTERNAL_SERVER_ERROR {
            opaque_internal(&self.message)
        } else {
            self.message.clone()
        }
    }

    /// The same error with another status (keeps the text and every other field).
    pub(super) fn with_status(mut self, status: StatusCode) -> Self {
        self.status = status;
        self
    }

    /// `429` with `Retry-After`/`retry_after_ms` = `ms` (like `range_overloaded`).
    pub(super) fn retry_after_ms(mut self, ms: u32) -> Self {
        self.overloaded = std::num::NonZeroU32::new(ms.max(1));
        self
    }

    /// The range this "not the leader" error is about.
    pub(super) fn partition(&self) -> Option<cairn_store::PartitionId> {
        self.not_leader_of
            .map(|n| cairn_store::PartitionId(n.get() - 1))
    }

    /// Attaches the range to a "not the leader" error.
    pub(super) fn at_partition(mut self, p: cairn_store::PartitionId) -> Self {
        self.not_leader_of = p.0.checked_add(1).and_then(std::num::NonZeroU32::new);
        self
    }
}

/// Machine-readable codes of [`ApiError`] other than `range_overloaded`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Code {
    /// `410`: the stream is gone ([`STREAM_EXPIRED`]).
    StreamExpired,
    /// `429`: the node is out of SSE slots.
    SseLimit,
    /// `429`: the tenant is out of long-lived connection slots on the node.
    TooManyOpenStreams,
    /// `409`: the `schedules/table` gate is not enabled (CG-13).
    SchedulesDisabled,
    /// `400`: the egress policy refuses the destination.
    EgressDenied,
    /// `413`: the body exceeds the tenant's `max-payload=`.
    PayloadTooLarge,
    /// `429`: `POST /deployments` exceeds the tenant's `deploy-rate=`.
    DeployRateLimited,
    /// `400`: the SDK does not declare `requestIdentity` and the node requires it.
    IdentityRequired,
    /// `400`: a `REQUEST_RESPONSE` endpoint does not declare `requestCompression` and
    /// the node requires it (`--deploy-require-compression`, SaaS profile).
    CompressionRequired,
    /// `409`: the `view`/`provision` roles need the `auth/keys-v2` gate.
    KeyRolesDisabled,
    /// `429`: a `limited` tenant takes no new work.
    TenantLimited,
    /// `403`: the tenant is `suspended`.
    TenantSuspended,
    /// `409`: changing the tenant state needs the `tenants/state` gate.
    TenantStateDisabled,
    /// `403`: the tenant is being deleted or has been deleted.
    TenantDeleting,
    /// `409`: `deleting` needs the `tenants/purge` gate.
    TenantPurgeDisabled,
    /// `409`: past the `purging-ms=` mark — cannot be reversed.
    TenantPurging,
    /// `409`: the tenant is already a `purged` tombstone.
    TenantPurged,
    /// `429`: the tenant has reached `max-live=`.
    LiveInvocationsLimit,
    /// `409`: the tenant has reached `max-deployments=`.
    DeploymentsLimit,
    /// `409`: the tenant has reached `max-schedules=`.
    SchedulesLimit,
    /// `503`: the cell's store failed transiently — detail only goes to the log, send
    /// again later.
    StoreUnavailable,
    /// `413`: a segmented output larger than the ingress assembly cap.
    ValueTooLarge,
    /// `503`: the CAS loop on the key registry ran out of attempts because of concurrent
    /// key create/revoke commands — nothing was written, send again after
    /// `Retry-After`.
    KeysBusy,
    /// `429`: the tenant already has enough concurrent segmented-output assemblies on the
    /// node.
    ValueAssemblyBusy,
}

impl Code {
    fn as_str(self) -> &'static str {
        match self {
            Code::StreamExpired => STREAM_EXPIRED,
            Code::SseLimit => "sse_limit",
            Code::TooManyOpenStreams => "too_many_open_streams",
            Code::SchedulesDisabled => crate::schedules::SCHEDULES_DISABLED,
            Code::EgressDenied => crate::egress::EGRESS_DENIED,
            Code::PayloadTooLarge => cairn_engine::PAYLOAD_TOO_LARGE_CODE,
            Code::DeployRateLimited => "deploy_rate_limited",
            Code::IdentityRequired => "identity_required",
            Code::CompressionRequired => "compression_required",
            Code::KeyRolesDisabled => KEY_ROLES_DISABLED,
            Code::TenantLimited => TENANT_LIMITED,
            Code::TenantSuspended => TENANT_SUSPENDED,
            Code::TenantStateDisabled => TENANT_STATE_DISABLED,
            Code::TenantDeleting => TENANT_DELETING,
            Code::TenantPurgeDisabled => "tenant_purge_disabled",
            Code::TenantPurging => "tenant_purging",
            Code::TenantPurged => "tenant_purged",
            Code::LiveInvocationsLimit => "live_invocations_limit",
            Code::DeploymentsLimit => "deployments_limit",
            Code::SchedulesLimit => "schedules_limit",
            Code::StoreUnavailable => STORE_UNAVAILABLE,
            Code::ValueTooLarge => "value_too_large",
            Code::KeysBusy => KEYS_BUSY,
            Code::ValueAssemblyBusy => "value_assembly_busy",
        }
    }
}

#[allow(non_snake_case)]
pub(super) fn ApiError(status: StatusCode, message: String) -> ApiError {
    ApiError {
        status,
        message,
        leader: None,
        wrong_range: None,
        id: None,
        overloaded: None,
        code: None,
        not_leader_of: None,
    }
}

/// `400 egress_denied`: says why, without repeating anything from the destination side,
/// the address a name resolved to or the blocked range ([`crate::egress::Denied`]).
pub(super) fn egress_denied(d: crate::egress::Denied) -> ApiError {
    let mut e = ApiError(StatusCode::BAD_REQUEST, d.to_string());
    e.code = Some(Code::EgressDenied);
    e
}

/// `code` of the `409` when creating a `view`/`provision` role key before the
/// `auth/keys-v2` gate.
pub const KEY_ROLES_DISABLED: &str = "key_roles_disabled";

/// `409 key_roles_disabled`.
pub(super) fn key_roles_disabled(message: String) -> ApiError {
    let mut e = ApiError(StatusCode::CONFLICT, message);
    e.code = Some(Code::KeyRolesDisabled);
    e
}

/// `code` of the `429` when the tenant is `limited`: **nothing was created**, no `id`.
pub const TENANT_LIMITED: &str = "tenant_limited";
/// `code` of the `403` when the tenant is `suspended`.
pub const TENANT_SUSPENDED: &str = "tenant_suspended";
/// `code` of the `409` when changing the tenant state before the `tenants/state` gate.
pub const TENANT_STATE_DISABLED: &str = "tenant_state_disabled";
/// `code` of the `403` when the tenant is `deleting`/`purged`.
pub const TENANT_DELETING: &str = "tenant_deleting";

/// `429 tenant_limited` / `403 tenant_suspended`, `429 live_invocations_limit`.
pub(super) fn tenant_refused(why: cairn_engine::TenantRefusal) -> ApiError {
    let state = match why {
        cairn_engine::TenantRefusal::State(s) => s,
        cairn_engine::TenantRefusal::Live { limit } => {
            let mut e = ApiError(
                StatusCode::TOO_MANY_REQUESTS,
                format!(
                    "tenant already has {limit} live invocations (max-live limit) — nothing created; send again when less work is running"
                ),
            );
            e.code = Some(Code::LiveInvocationsLimit);
            return e;
        }
    };
    let (status, code) = if state == cairn_engine::TenantState::Limited {
        (StatusCode::TOO_MANY_REQUESTS, Code::TenantLimited)
    } else if state.refuses_access() {
        (StatusCode::FORBIDDEN, Code::TenantDeleting)
    } else {
        (StatusCode::FORBIDDEN, Code::TenantSuspended)
    };
    let mut e = ApiError(
        status,
        format!(
            "tenant is {} — not accepting new work; nothing created",
            state.as_str()
        ),
    );
    e.code = Some(code);
    e
}

/// `code` of the `429` when the range is overloaded because of L0 — the contract with
/// the client: **no invocation was created**, the body has no `id`, sending again after
/// `Retry-After` is safe.
pub const RANGE_OVERLOADED: &str = "range_overloaded";

/// The `Retry-After` header from milliseconds: **seconds**, rounded up, at least 1 — a 0
/// turns a client that honours the header into a busy loop.
pub(super) fn retry_after_header(ms: u64) -> Option<axum::http::HeaderValue> {
    axum::http::HeaderValue::from_str(&ms.div_ceil(1000).max(1).to_string()).ok()
}

/// `code` of the `503` when the cell's store fails transiently.
pub const STORE_UNAVAILABLE: &str = "store_unavailable";

/// `code` of the `503` when the key registry keeps changing underneath.
pub const KEYS_BUSY: &str = "keys_busy";

/// Send the key command again after this long on `503 keys_busy`.
const KEYS_BUSY_RETRY_MS: u64 = 1_000;

/// `503 keys_busy` with `Retry-After`: the CAS loop on the key registry ran out of
/// attempts — concurrent key create/revoke commands. Nothing was written; the body
/// carries no object path.
pub(super) fn keys_busy() -> ApiError {
    ApiError::coded(
        StatusCode::SERVICE_UNAVAILABLE,
        Code::KeysBusy,
        crate::auth::KeyError::Contended.to_string(),
    )
}

/// A transient store error on a tenant path ⇒ `503 store_unavailable`, with no detail in
/// the body (bucket, host, object path, S3 client text — the cell's infrastructure,
/// which the masking rule keeps from tenants). Detail goes to the log at `warn` with
/// `what` (which path).
pub(super) fn store_unavailable(what: &str, detail: &dyn std::fmt::Display) -> ApiError {
    tracing::warn!(what, detail = %detail, "store error returned as 503 — details only in the log");
    ApiError::coded(
        StatusCode::SERVICE_UNAVAILABLE,
        Code::StoreUnavailable,
        "the store is temporarily unavailable — retry later".into(),
    )
}

/// The body of every `500`: a store error carries the bucket name, host, object path —
/// the cell's infrastructure, not the tenant's business. Detail goes to the log (at
/// `error`) with a random code; the body has only that code for the operator to look up.
pub(crate) fn opaque_internal(detail: &str) -> String {
    let mut b = [0u8; 6];
    cairn_store::entropy::fill(&mut b);
    let code: String = b.iter().map(|x| format!("{x:02x}")).collect();
    tracing::error!(code = %code, detail = detail, "internal error returned as 500 — details only in the log");
    format!("internal node error — details in the node log, code {code}")
}

impl IntoResponse for ApiError {
    fn into_response(mut self) -> Response {
        let self_code = self.code;
        if self.status == StatusCode::INTERNAL_SERVER_ERROR {
            self.message = opaque_internal(&self.message);
        }
        if let Some(table) = self.wrong_range.take().map(|t| *t) {
            return (
                self.status,
                Json(crate::transport::WrongRangeBody {
                    error: self.message,
                    code: crate::transport::WRONG_RANGE.into(),
                    table,
                }),
            )
                .into_response();
        }
        let not_leader_at = self
            .partition()
            .map(|partition| super::forward::NotLeaderAt {
                partition,
                id: self.id.clone(),
            });
        let mut resp = (
            self.status,
            Json(ErrorBody {
                error: self.message,
                leader: self.leader,
                id: self.id,
                code: self
                    .code
                    .map(Code::as_str)
                    .or(self.overloaded.map(|_| RANGE_OVERLOADED)),
                retry_after_ms: self.overloaded.map(|ms| u64::from(ms.get())),
            }),
        )
            .into_response();
        if let Some(at) = not_leader_at {
            resp.extensions_mut().insert(at);
        }
        let retry_after_ms = self
            .overloaded
            .map(|ms| u64::from(ms.get()))
            .or((self_code == Some(Code::KeysBusy)).then_some(KEYS_BUSY_RETRY_MS));
        if let Some(v) = retry_after_ms.and_then(retry_after_header) {
            resp.headers_mut()
                .insert(axum::http::header::RETRY_AFTER, v);
        }
        resp
    }
}

/// `code` of the `410` when the stream is gone: a transient stream after sealing +
/// `STREAM_LINGER`, a durable stream after purge.
pub const STREAM_EXPIRED: &str = "stream_expired";

impl ApiError {
    /// An error carrying the machine-readable code `code`.
    pub(super) fn coded(status: StatusCode, code: Code, message: String) -> Self {
        Self {
            code: Some(code),
            ..ApiError(status, message)
        }
    }

    /// `409 wrong_range` with this node's range table (D23).
    pub(super) fn wrong_range(message: String, table: &cairn_store::PartitionTable) -> Self {
        Self {
            wrong_range: Some(Box::new(table.into())),
            ..ApiError(StatusCode::CONFLICT, message)
        }
    }

    /// An error from the engine layer, classified.
    ///
    /// The only thing that must be told apart is **lost write authority**: principle N3
    /// says the leader handover window shows outside as a *retryable* error, and it must
    /// be part of the API contract rather than hidden. Folding it into `500` tells the
    /// client that something is broken, when it should tell it to *retry on another node*.
    ///
    /// `503`, not `421 Misdirected Request`: `503` is what every load balancer and every
    /// HTTP library already understands as "retry", and `421` is not. `fallback` is the
    /// status for every other error, different per call path.
    ///
    /// The condition is `is_fenced()`, **not** `NotLeader` alone. Two different errors
    /// lead to the same place, and the boundary between them is only timing: the
    /// **first** command to hit the fence gets `Fenced` (layer 2 just detected it), the
    /// commands after it get `NotLeader` (the node has finished stepping down). To the
    /// caller both say one thing: *retry on another node*. Recognising only one of them
    /// means exactly one request — the unlucky one that went first — comes back as `500`,
    /// and that is the hardest request to reproduce when hunting for it.
    pub(super) fn from_actor(e: crate::actor::ActorError, fallback: StatusCode) -> Self {
        if let crate::actor::ActorError::Overloaded { retry_after_ms } = e {
            // `429` like `Throttled`, but **without an id**: when the slot wait cap runs
            // out the invocation is already durable and queued; here nothing exists yet.
            let ms = u32::try_from(retry_after_ms).unwrap_or(u32::MAX).max(1);
            return Self {
                overloaded: std::num::NonZeroU32::new(ms),
                ..ApiError(StatusCode::TOO_MANY_REQUESTS, e.to_string())
            };
        }
        if let crate::actor::ActorError::Rejected(why) = e {
            return ApiError(StatusCode::BAD_REQUEST, why);
        }
        if let crate::actor::ActorError::TenantRefused(s) = e {
            return tenant_refused(s);
        }
        if matches!(e, crate::actor::ActorError::WrongRange) {
            // The table was reloaded and retried once ([`AppState::ask_range`]) and the
            // range is still changing — transient, retry.
            return ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "the range holding this key is changing (split/merge/retire) — retry later".into(),
            );
        }
        if e.is_fenced() {
            return ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "this node is not the leader of the partition — retry on another node".into(),
            );
        }
        // A store error: its text carries the bucket, host, object path — the cell's
        // infrastructure. Transient ⇒ `503 store_unavailable`, send again; otherwise ⇒
        // `500` (body masked in `into_response`). Errors caused by the command itself
        // (key too long, bad id) keep their text and `fallback`.
        use {
            crate::actor::ActorError as A, cairn_engine::EngineError, cairn_store::StoreError as S,
        };
        match &e {
            A::Spawn(_)
            | A::SealFailed(_)
            | A::Engine(EngineError::Store(
                S::Io(_)
                | S::LeaseIo(_)
                | S::Closed
                | S::Slate(_)
                | S::LeaseUnavailable { .. }
                | S::RangeRetired { .. },
            )) => store_unavailable("engine", &e),
            A::Stopped => ApiError(StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
            A::Engine(EngineError::Store(
                S::KeyTooLong { .. }
                | S::Ident(_)
                | S::BadDeploymentId { .. }
                | S::DeploymentExists { .. }
                | S::CatalogRejected { .. },
            )) => ApiError(fallback, e.to_string()),
            A::Engine(EngineError::Store(_)) => {
                ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
            }
            _ => ApiError(fallback, e.to_string()),
        }
    }

    /// Like [`Self::from_actor`], but **with the leader address** when known.
    ///
    /// Without it the client only knows "not here" and has to probe node by node — while
    /// the routing table already knows the answer. Still a `503` and not a redirect:
    /// redirecting a `POST` is something HTTP libraries handle differently everywhere,
    /// while "retry here" is unambiguous.
    pub(super) fn from_actor_routed(
        e: crate::actor::ActorError,
        fallback: StatusCode,
        st: &AppState,
        id: &InvocationId,
    ) -> Self {
        // Every exit of this function **carries the id**: the caller already holds it,
        // and the invocation is durable — it still exists and will still run, so hiding
        // its name makes the caller lose track of work that is still running.
        let with_id = |mut e: ApiError| {
            e.id = Some(hex(id));
            e
        };
        // Except exactly one error: `Overloaded` means **nothing was created** — the id
        // here would be the id of an invocation that does not exist.
        if matches!(
            e,
            crate::actor::ActorError::Overloaded { .. }
                | crate::actor::ActorError::TenantRefused(_)
        ) {
            return Self::from_actor(e, fallback);
        }
        if !e.is_fenced() {
            return with_id(Self::from_actor(e, fallback));
        }
        // Which range, where its leader is — and ask for a routing table refresh when it
        // may be pointing the wrong way: [`Router::not_leader`].
        with_id(Self::not_leader(st.router.not_leader(id)))
    }

    /// Like [`Self::from_actor_routed`], for a call routed by key with no id yet (an
    /// object's promise): `503` with `leader` when known.
    pub(super) fn from_actor_keyed(
        e: crate::actor::ActorError,
        fallback: StatusCode,
        st: &AppState,
        key: cairn_store::PartitionKey,
    ) -> Self {
        if !e.is_fenced() {
            return Self::from_actor(e, fallback);
        }
        Self::not_leader(st.router.not_leader_of_key(key))
    }

    /// `503` "not the leader", with the leader's ingress port when the table knows it.
    fn not_leader(n: NotLeader) -> Self {
        let NotLeader {
            partition: p,
            leader,
        } = n;
        let at = p;
        let e = match leader {
            Some(addr) => Self {
                status: StatusCode::SERVICE_UNAVAILABLE,
                message: format!(
                    "this node is not the leader of partition {}; the leader is at {addr}",
                    p.0
                ),
                leader: Some(addr),
                ..ApiError(StatusCode::SERVICE_UNAVAILABLE, String::new())
            },
            None => ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                format!(
                    "this node is not the leader of partition {}, and the leader is not known yet",
                    p.0
                ),
            ),
        };
        e.at_partition(at)
    }

    /// A refusal from [`Router::derive`] — before there is an id.
    pub(super) fn refused(r: Refused) -> Self {
        match r {
            Refused::NotFound(m) => ApiError(StatusCode::NOT_FOUND, m),
            Refused::BadRequest(m) => ApiError(StatusCode::BAD_REQUEST, m),
        }
    }

    /// An error from [`Router::submit`].
    pub(super) fn from_route(e: RouteError, fallback: StatusCode, st: &AppState) -> Self {
        match e {
            RouteError::Refused(r) => Self::refused(r),
            RouteError::NoEngine { partition, .. } => ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("partition {} does not belong to this cluster", partition.0),
            ),
            RouteError::Range { id, error } => Self::from_actor_routed(error, fallback, st, &id),
        }
    }
}

#[derive(Serialize)]
pub(super) struct ErrorBody {
    error: String,
    /// The leader address to retry at — only on `503 NotLeader` when the routing table
    /// knows the answer. Machines read this field, people read `error`.
    #[serde(skip_serializing_if = "Option::is_none")]
    leader: Option<String>,
    /// The invocation this error is about — present when it had an identity when the
    /// error happened.
    ///
    /// The same field name as `InvokeResponse.id`, on purpose: the client does not write
    /// two read paths for the same question ("which turn was the one just now").
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    /// A machine-readable code — `range_overloaded` (429, no `id`).
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_after_ms: Option<u64>,
}

pub(super) type ApiResult<T> = std::result::Result<T, ApiError>;

pub(super) fn store_error(e: cairn_store::StoreError) -> ApiError {
    use cairn_store::StoreError as E;
    match e {
        E::CatalogRejected { reason } => ApiError(StatusCode::CONFLICT, reason),
        E::BadDeploymentId { .. } => ApiError(StatusCode::BAD_REQUEST, e.to_string()),
        other => ApiError(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The detail of a typical store error: bucket name, host, object path.
    const LEAK: &str = "Generic S3 error: error GET https://customer-bucket.s3.tigris.dev/cell-1/ranges/3/manifest/00000000000000000042.manifest";

    async fn body_of(r: Response) -> String {
        let b = axum::body::to_bytes(r.into_body(), usize::MAX)
            .await
            .unwrap();
        String::from_utf8_lossy(&b).into_owned()
    }

    /// The body of a `500` (security review item "raw store error in 500") does not
    /// contain the bucket name, host or object path — detail goes only to the log.
    /// Errors other than `500` keep their text (`409`, `400` are the caller's own words).
    #[tokio::test]
    async fn internal_error_body_does_not_leak_store_details() {
        for e in [
            store_error(cairn_store::StoreError::Corrupt(LEAK.into())),
            ApiError(StatusCode::INTERNAL_SERVER_ERROR, LEAK.into()),
        ] {
            let r = e.into_response();
            assert_eq!(r.status(), StatusCode::INTERNAL_SERVER_ERROR);
            let body = body_of(r).await;
            for part in ["customer-bucket", "tigris", "ranges/3", "manifest"] {
                assert!(!body.contains(part), "500 body leaks {part:?}: {body}");
            }
            assert!(body.contains("\"error\""), "{body}");
        }
        let r = crate::query::http::err(StatusCode::INTERNAL_SERVER_ERROR, LEAK);
        let body = body_of(r).await;
        assert!(
            !body.contains("customer-bucket"),
            "/query 500 body leaks the bucket: {body}"
        );
        let r = ApiError(StatusCode::CONFLICT, "kept as is".into()).into_response();
        assert!(body_of(r).await.contains("kept as is"));
    }

    /// A store error through the actor (`from_actor`, the path of `/invoke`, `/send`,
    /// state reads, schedules…) ⇒ `503 store_unavailable` — transient, send again — the
    /// body carries no bucket, host or object path; a non-transient store error ⇒ a masked
    /// `500`. Before: `fallback` (usually `502`) with the error verbatim.
    #[tokio::test]
    async fn actor_store_errors_are_retryable_and_opaque() {
        use {crate::actor::ActorError, cairn_engine::EngineError, cairn_store::StoreError as S};
        let store = |e: S| ActorError::Engine(EngineError::Store(e));
        for e in [
            store(S::Io(LEAK.into())),
            store(S::LeaseIo(LEAK.into())),
            ActorError::Spawn(LEAK.into()),
            ActorError::SealFailed(LEAK.into()),
        ] {
            let what = format!("{e:?}");
            let r = ApiError::from_actor(e, StatusCode::BAD_GATEWAY).into_response();
            assert_eq!(r.status(), StatusCode::SERVICE_UNAVAILABLE, "{what}");
            let body = body_of(r).await;
            assert!(body.contains("\"store_unavailable\""), "{what}: {body}");
            for part in ["customer-bucket", "tigris", "ranges/3", "manifest"] {
                assert!(!body.contains(part), "{what}: body leaks {part:?}: {body}");
            }
        }
        let r = ApiError::from_actor(store(S::Corrupt(LEAK.into())), StatusCode::BAD_GATEWAY)
            .into_response();
        assert_eq!(r.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(!body_of(r).await.contains("customer-bucket"));
        // The caller's own errors keep their text.
        let r = ApiError::from_actor(
            store(S::KeyTooLong {
                what: "object key",
                len: 9000,
                max: 4096,
            }),
            StatusCode::BAD_REQUEST,
        )
        .into_response();
        assert_eq!(r.status(), StatusCode::BAD_REQUEST);
        assert!(body_of(r).await.contains("9000"));
    }
}
