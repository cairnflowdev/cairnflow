// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The data-plane audit log — the server's part.
//!
//! The layout on the bucket, object names and how each record is written
//! exactly once live in [`cairn_store::audit`]. Here:
//!
//! - [`RequestMeta`]: IP (after `--trust-proxy`), user agent, request id — the
//!   [`capture`] layer attaches it to every ingress request.
//! - [`AuditNote`]: who did what to what — ingress builds it, an actor command
//!   carries it (`Command::Audited`) to exactly where the engine commits the
//!   action; the `Table::Audit` row goes **in that same commit**
//!   (`cairn_engine::Engine::audit_next`).
//! - [`Sink::intent`]: actions on bucket objects (deployments, keys,
//!   `tenants`, rules, console sessions) — the intent first, the outcome after.
//! - [`Denials`]: requests refused for lack of permission, **merged** by
//!   `(tenant, actor, action)` per minute, with a cap — not one PUT per
//!   request. It also merges **no-op** outcomes of in-range actions
//!   (`not_found`, `noop`): the pending row of a command that changed nothing
//!   is dropped, and the command is counted under the key `(tenant, actor,
//!   action, outcome)` — the `….noop` object carries `count`.
//! - [`export`]: the loop exporting a range's `Table::Audit` rows.
//! - [`CLASSES`]: every ingress write route is classified (audited in the
//!   range, on an object, or exempt with a reason) —
//!   `tests/audit.rs::every_mutation_is_audited`.

use {
    crate::auth::Identity,
    bytes::Bytes,
    cairn_store::TenantId,
    serde_json::{json, Value},
    slatedb::object_store::{path::Path, ObjectStore},
    std::{
        collections::BTreeMap,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    },
};

/// Version of the JSON record.
pub const RECORD_VERSION: u32 = 1;

/// Metadata of the originating request.
#[derive(Clone, Debug, Default)]
pub struct RequestMeta {
    pub ip: Option<String>,
    pub user_agent: Option<String>,
    pub request_id: String,
    /// The node's no-op outcome merger — audit notes built from this request
    /// carry it to the actor.
    pub noops: Option<Arc<Denials>>,
}

/// The ingress layer: attaches [`RequestMeta`]. The IP is the client's behind
/// a trusted proxy (`--trust-proxy`), otherwise the TCP peer's. The client's
/// `x-request-id` is kept (cut to 64 characters); absent, a new one is
/// generated.
pub async fn capture(
    axum::extract::State(auth): axum::extract::State<crate::auth::Auth>,
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let meta = meta_of(&auth, &req);
    req.extensions_mut().insert(meta);
    next.run(req).await
}

/// The [`RequestMeta`] of a request — for [`capture`] and for console routes
/// outside that layer (login/logout, `crate::ui`).
pub fn meta_of(auth: &crate::auth::Auth, req: &axum::extract::Request) -> RequestMeta {
    let peer = crate::auth::peer_ip(req.extensions());
    let ip = auth
        .sessions
        .as_ref()
        .and_then(|s| s.trusted().client_ip(req.headers(), peer))
        .or(peer)
        .map(|ip| ip.to_string());
    let headers = req.headers();
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.chars().take(256).collect::<String>())
    };
    RequestMeta {
        noops: auth.denials.clone(),
        ip,
        user_agent: header("user-agent"),
        request_id: header("x-request-id")
            .map(|v| v.chars().take(64).collect())
            .unwrap_or_else(new_request_id),
    }
}

fn new_request_id() -> String {
    let mut b = [0u8; 12];
    cairn_store::entropy::fill(&mut b);
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// The actor: `{type, id}` — the user of an SSO session (`id` = the issuer's
/// `sub`, plus `email` when present and the session's client), an API key
/// (the key's `id`; bootstrap/env keys are `bootstrap:<role>`), or
/// unauthenticated (`--no-auth`).
pub fn actor_of(who: &Identity) -> Value {
    if let Some(u) = &who.user {
        let mut a = json!({
            "type": "user",
            "id": u.sub,
            "sub": u.sub,
            "role": who.role.as_str(),
            "client": u.client,
        });
        if let Some(e) = &u.email {
            a["email"] = e.as_str().into();
        }
        return a;
    }
    match &who.key_id {
        Some(id) => json!({ "type": "key", "id": id, "role": who.role.as_str() }),
        None => json!({
            "type": "key",
            "id": format!("bootstrap:{}", who.role.as_str()),
            "role": who.role.as_str(),
        }),
    }
}

/// Who did what to what, no outcome yet.
#[derive(Clone, Debug)]
pub struct AuditNote {
    /// Tenant of the target entity. `None` = the actor sees every tenant
    /// (cluster-admin) and ingress has not read the target: the actor reads it
    /// under the engine (`turn::Ctx::audit_begin`).
    pub tenant: Option<String>,
    pub ts_ms: u64,
    base: Value,
    /// The no-op outcome merger; `None` ⇒ a no-op writes a row like any other
    /// outcome (a hand-built router).
    noops: Option<Arc<Denials>>,
}

impl AuditNote {
    pub fn new(
        meta: Option<&RequestMeta>,
        who: &Identity,
        tenant: Option<&TenantId>,
        action: &str,
        target: Value,
    ) -> Self {
        let ts_ms = cairn_store::entropy::now_ms();
        let m = meta.cloned().unwrap_or_default();
        Self {
            noops: m.noops.clone(),
            tenant: tenant.map(|t| t.as_str().to_string()),
            ts_ms,
            base: json!({
                "v": RECORD_VERSION,
                "ts": ts_ms,
                "actor": actor_of(who),
                "action": action,
                "target": target,
                "ip": m.ip,
                "userAgent": m.user_agent,
                "requestId": m.request_id,
            }),
        }
    }

    pub fn action(&self) -> &str {
        self.base["action"].as_str().unwrap_or_default()
    }

    /// Whether the note has a no-op outcome merger; if not, the caller writes
    /// a row like any other outcome.
    pub fn aggregates_noops(&self) -> bool {
        self.noops.is_some()
    }

    /// Merges the no-op outcome `outcome` of tenant `tenant`.
    pub fn aggregate_noop(&self, tenant: &str, outcome: &str) {
        if let Some(d) = &self.noops {
            d.note_outcome(tenant, self.base["actor"].clone(), self.action(), outcome);
        }
    }

    /// Tenant of the record: known, or `default`.
    pub fn tenant_or_default(&self) -> &str {
        self.tenant.as_deref().unwrap_or(TenantId::DEFAULT)
    }

    /// The JSON record of tenant `tenant` with `outcome` (and extra fields).
    pub fn record_for(&self, tenant: &str, outcome: &str, extra: Option<(&str, Value)>) -> Vec<u8> {
        let mut v = self.base.clone();
        v["tenant"] = tenant.into();
        v["outcome"] = outcome.into();
        if let Some((k, x)) = extra {
            v[k] = x;
        }
        serde_json::to_vec(&v).unwrap_or_default()
    }

    /// Like [`Self::record_for`], with the note's known tenant.
    pub fn record(&self, outcome: &str, extra: Option<(&str, Value)>) -> Vec<u8> {
        let mut v = self.base.clone();
        v["tenant"] = self.tenant_or_default().into();
        v["outcome"] = outcome.into();
        if let Some((k, x)) = extra {
            v[k] = x;
        }
        serde_json::to_vec(&v).unwrap_or_default()
    }
}

/// Where the node writes audit objects: the main store, the prefix, the node
/// identity for intent names.
#[derive(Clone)]
pub struct Sink {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    node: String,
    boot: u64,
    seq: Arc<AtomicU64>,
}

impl Sink {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: Path, node: &str) -> Self {
        let mut b = [0u8; 8];
        cairn_store::entropy::fill(&mut b);
        Self {
            store,
            prefix,
            // The node name may contain `/` (ids generated per process): it
            // must not become a path segment.
            node: node.replace(['/', '.'], "_"),
            boot: u64::from_be_bytes(b),
            seq: Arc::default(),
        }
    }

    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    pub fn prefix(&self) -> &Path {
        &self.prefix
    }

    /// Writes the intent of an action on an object **before** doing it.
    /// Failure ⇒ `Err`: the caller does not perform the action (there is never
    /// an action without an intent).
    pub async fn intent(&self, note: AuditNote) -> Result<Intent, String> {
        let n = self.seq.fetch_add(1, Ordering::Relaxed);
        let name = format!("{}-{}-{:016x}-{n}", note.ts_ms, self.node, self.boot);
        let path = cairn_store::audit::object_path(
            &self.prefix,
            note.tenant_or_default(),
            note.ts_ms,
            &format!("{name}.intent"),
        );
        cairn_store::audit::put_new(
            &self.store,
            &path,
            Bytes::from(note.record("pending", None)),
        )
        .await
        .map_err(|e| format!("cannot write audit intent: {e}"))?;
        Ok(Intent {
            sink: self.clone(),
            note,
            name,
        })
    }
}

/// Attempts of one outcome PUT ([`Intent::done`]).
const OUTCOME_RETRY: cairn_store::retry::Policy =
    cairn_store::retry::Policy::new(Duration::from_millis(200), Duration::from_secs(2), 4)
        .with_total(Duration::from_secs(10));

/// An intent that has been written; [`Self::done`] writes the outcome.
pub struct Intent {
    sink: Sink,
    note: AuditNote,
    name: String,
}

impl Intent {
    /// Writes the outcome (`ok`, `error`, …) with the HTTP status. A failure is
    /// only logged: the action has happened, and an intent without an outcome
    /// is what `verify` cross-checks.
    pub async fn done(self, outcome: &str, status: u16) {
        let path = cairn_store::audit::object_path(
            &self.sink.prefix,
            self.note.tenant_or_default(),
            self.note.ts_ms,
            &format!("{}.done", self.name),
        );
        let body = Bytes::from(self.note.record(outcome, Some(("status", status.into()))));
        // A few bounded attempts (`retry::run`; an ambiguous PUT is resolved by reading
        // back inside `write_once`). Still failing ⇒ the intent stays without an outcome
        // until the reconcile round ([`reconcile_intents`]) records `unknown`.
        let written = cairn_store::retry::run(
            &cairn_store::retry::ops::AUDIT_OUTCOME,
            &OUTCOME_RETRY,
            cairn_store::retry::Never,
            || cairn_store::audit::write_once(&self.sink.store, &path, body.clone()),
        )
        .await;
        match written {
            Ok(cairn_store::audit::Written::Done) => {}
            Ok(cairn_store::audit::Written::Conflict) => {
                tracing::warn!(
                    "audit: an outcome with other content is already there — kept as is"
                );
            }
            Err(e) => {
                tracing::warn!(error = %e, "audit: cannot write the outcome — intent left behind for the reconcile round");
            }
        }
    }
}

// ─────────────────────────────────────────── reconciling intents

/// An intent without an outcome older than this is reconciled: no audited request runs
/// this long, so its outcome will never come (the node died between intent and outcome,
/// or the outcome PUT failed).
pub const RECONCILE_AFTER_MS: u64 = 3_600_000;

/// What one [`reconcile_intents`] pass did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Reconciled {
    /// Intents without an outcome past [`RECONCILE_AFTER_MS`] that now have one.
    pub written: usize,
}

/// Cap on audit objects one [`reconcile_intents`] pass lists (like `verify`'s cap).
const RECONCILE_MAX_OBJECTS: usize = 200_000;

/// Gives every intent under `audit/tenant/` older than [`RECONCILE_AFTER_MS`] at `now_ms`
/// and without an outcome a `….done` outcome `unknown`: the intent's record with
/// `outcome = "unknown"` and `reconciledMs`. Written with `Create`, ambiguity resolved by
/// reading back ([`cairn_store::audit::write_once`]); an outcome already there with other
/// content (the request's own, or another runner's pass) is left as is. Idempotent: run by
/// the D-d runner; two runners overlapping when the role moves write at most one outcome.
pub async fn reconcile_intents(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    now_ms: u64,
) -> cairn_store::Result<Reconciled> {
    use {
        cairn_store::audit::Written, futures::TryStreamExt, slatedb::object_store::ObjectStoreExt,
    };
    let root = if prefix.as_ref().is_empty() {
        Path::from(cairn_store::audit::DIR)
    } else {
        Path::from(format!("{}/{}", prefix.as_ref(), cairn_store::audit::DIR))
    };
    let mut intents: Vec<(Path, String)> = Vec::new();
    let mut outcomes = std::collections::BTreeSet::new();
    let mut listed = store.list(Some(&root));
    let mut seen = 0usize;
    while let Some(m) = listed
        .try_next()
        .await
        .map_err(|e| cairn_store::StoreError::Io(e.to_string()))?
    {
        seen += 1;
        if seen > RECONCILE_MAX_OBJECTS {
            break;
        }
        let loc = m.location.as_ref().to_string();
        if let Some(base) = loc.strip_suffix(".intent") {
            intents.push((m.location.clone(), base.to_string()));
        } else if let Some(base) = loc.strip_suffix(".done") {
            outcomes.insert(base.to_string());
        }
    }
    let mut out = Reconciled::default();
    for (path, base) in intents {
        if outcomes.contains(&base) {
            continue;
        }
        // `<dir>/<ts>-<node>-<boot>-<seq>`: the intent's own timestamp.
        let ts = base
            .rsplit('/')
            .next()
            .and_then(|n| n.split('-').next())
            .and_then(|t| t.parse::<u64>().ok());
        let Some(ts) = ts else { continue };
        if ts.saturating_add(RECONCILE_AFTER_MS) > now_ms {
            continue;
        }
        let raw = match store.get(&path).await {
            Ok(g) => g
                .bytes()
                .await
                .map_err(|e| cairn_store::StoreError::Io(e.to_string()))?,
            // Gone meanwhile (the tenant's purge): nothing to reconcile.
            Err(slatedb::object_store::Error::NotFound { .. }) => continue,
            Err(e) => return Err(cairn_store::StoreError::Io(e.to_string())),
        };
        let mut v = serde_json::from_slice::<Value>(&raw).unwrap_or_else(|_| json!({ "ts": ts }));
        if !v.is_object() {
            v = json!({ "ts": ts });
        }
        v["outcome"] = "unknown".into();
        v["reconciledMs"] = now_ms.into();
        let done = Path::from(format!("{base}.done"));
        match cairn_store::audit::write_once(store, &done, Bytes::from(v.to_string())).await? {
            Written::Done => out.written += 1,
            Written::Conflict => {}
        }
    }
    Ok(out)
}

// ─────────────────────────────────────────── permission denials

/// One minute's denials, merged by `(tenant, actor, action)` — and no-op
/// outcomes, merged by `(tenant, actor, action, outcome)`.
#[derive(Default)]
pub struct Denials {
    inner: Mutex<BTreeMap<DenialKey, Denied>>,
}

/// `(tenant, actor, action, outcome)`.
type DenialKey = (String, String, String, String);

impl std::fmt::Debug for Denials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Denials")
    }
}

/// Outcome of a permission denial.
const DENIED: &str = "denied";

struct Denied {
    actor: Value,
    count: u64,
    first_ms: u64,
}

/// Interval for writing merged denials.
pub const DENIALS_EVERY: Duration = Duration::from_secs(60);

/// Cap on the number of merge keys per minute **per tenant**: past it, counts
/// go to the key `(tenant, "*", "*", outcome)` with actor `{"id": "*"}` (a
/// shared cap would let a noisy tenant push other tenants into the overflow
/// key).
pub const MAX_DENIAL_KEYS: usize = 1000;

/// Cap on the total number of merge keys per minute (memory): past it, every
/// new key goes to its tenant's overflow key.
pub const MAX_DENIAL_KEYS_TOTAL: usize = 100_000;

/// Actor of the overflow key.
const ANY: &str = "*";

impl Denials {
    /// A request refused with `403` (authenticated, missing permission).
    pub fn note(&self, who: &Identity, method: &str, path: &str) {
        let action = format!("{method} {}", route_shape(path));
        self.note_outcome(who.tenant.as_str(), actor_of(who), &action, DENIED);
    }

    /// A command of `actor` on `tenant` with outcome `outcome` (`denied`, or a
    /// no-op of an in-range action).
    pub fn note_outcome(&self, tenant: &str, actor: Value, action: &str, outcome: &str) {
        let id = actor["id"].as_str().unwrap_or_default().to_string();
        let Ok(mut m) = self.inner.lock() else {
            return;
        };
        let mut key = (
            tenant.to_string(),
            id,
            action.to_string(),
            outcome.to_string(),
        );
        let mut actor = actor;
        if !m.contains_key(&key) {
            let lo = (
                tenant.to_string(),
                String::new(),
                String::new(),
                String::new(),
            );
            let of_tenant = m
                .range(lo..)
                .take_while(|(k, _)| k.0 == tenant)
                .take(MAX_DENIAL_KEYS)
                .count();
            if of_tenant >= MAX_DENIAL_KEYS || m.len() >= MAX_DENIAL_KEYS_TOTAL {
                key.1 = ANY.into();
                key.2 = ANY.into();
                actor = json!({ "id": ANY });
            }
        }
        m.entry(key)
            .or_insert_with(|| Denied {
                actor,
                count: 0,
                first_ms: cairn_store::entropy::now_ms(),
            })
            .count += 1;
    }

    /// Merge keys currently held, with actor and count (tests).
    #[cfg(test)]
    fn snapshot(&self) -> Vec<(DenialKey, Value, u64)> {
        self.inner
            .lock()
            .map(|m| {
                m.iter()
                    .map(|(k, d)| (k.clone(), d.actor.clone(), d.count))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Writes every merged key as an object (one `denied` record per key).
    pub async fn flush(&self, sink: &Sink) {
        let taken = match self.inner.lock() {
            Ok(mut m) => std::mem::take(&mut *m),
            Err(_) => return,
        };
        for ((tenant, _, action, outcome), d) in taken {
            let Ok(t) = TenantId::new(&tenant) else {
                continue;
            };
            let ts = cairn_store::entropy::now_ms();
            let n = sink.seq.fetch_add(1, Ordering::Relaxed);
            let kind = if outcome == DENIED { "denied" } else { "noop" };
            let name = format!("{ts}-{}-{:016x}-{n}.{kind}", sink.node, sink.boot);
            let body = json!({
                "v": RECORD_VERSION,
                "ts": ts,
                "tenant": t.as_str(),
                "actor": d.actor,
                "action": action,
                "outcome": outcome,
                "count": d.count,
                "firstMs": d.first_ms,
            });
            let path = cairn_store::audit::object_path(&sink.prefix, t.as_str(), ts, &name);
            let bytes = Bytes::from(serde_json::to_vec(&body).unwrap_or_default());
            if let Err(e) = cairn_store::audit::put_new(&sink.store, &path, bytes).await {
                tracing::warn!(error = %e, "audit: cannot write merged denials");
            }
        }
    }

    /// The loop writing denials every `every`.
    pub async fn run(self: Arc<Self>, sink: Sink, every: Duration) {
        loop {
            cairn_store::pace::beat(every).await;
            self.flush(&sink).await;
        }
    }
}

/// The route shape of a path (ids, names → `{}`) — merge keys do not grow with ids.
fn route_shape(path: &str) -> String {
    path.split('/')
        .map(|seg| {
            let id_like = seg.len() >= 16 && seg.chars().all(|c| c.is_ascii_hexdigit());
            if id_like || seg.starts_with("k_") || seg.starts_with("dp_") {
                "{}"
            } else {
                seg
            }
        })
        .collect::<Vec<_>>()
        .join("/")
}

// ─────────────────────────────────────────── exporting a range's rows

/// Rows per export round.
pub const EXPORT_BATCH: usize = 64;

/// One round exporting a range's `Table::Audit` rows: read the rows under the
/// engine, **release the engine** to write the objects (D16), then take the
/// engine again to delete the written rows — one commit. The object name is a
/// function of the row key ⇒ dying in any gap, the next leader writes exactly
/// that object again (reading back a match ⇒ treated as written) before
/// deleting. Returns the number of rows exported.
pub async fn export(engine: &mut crate::lease::Lease) -> Result<usize, crate::actor::ActorError> {
    if !engine.audit_dirty() {
        return Ok(0);
    }
    let Some(store) = engine.audit_store().cloned() else {
        return Ok(0);
    };
    let rows = engine.audit_rows(EXPORT_BATCH).await?;
    if rows.is_empty() {
        engine.set_audit_dirty(false);
        return Ok(0);
    }
    cairn_store::failpoint!("audit::before_export", err);
    let written: Vec<Vec<u8>> = engine
        .released(async move |_: &crate::lease::Released| {
            let mut done = Vec::new();
            for (key, value) in rows {
                match store.write_row(&key, &value).await {
                    Ok(Some((_, cairn_store::audit::Written::Done))) => done.push(key),
                    Ok(Some((path, cairn_store::audit::Written::Conflict))) => {
                        crate::metrics::audit::bump(&crate::metrics::audit::CONFLICTS);
                        tracing::error!(path = %path, "audit: object already has other content — keeping the row");
                    }
                    Ok(None) => tracing::warn!("audit: corrupt row, left in place (verify reports it)"),
                    Err(e) => {
                        tracing::debug!(error = %e, "audit: not written yet, next round retries");
                        break;
                    }
                }
            }
            done
        })
        .await;
    // The gap between writing the objects and deleting the rows: dying here,
    // the next leader writes the same objects again (matching) and deletes.
    cairn_store::failpoint!("audit::after_export", err);
    engine.audit_remove(&written).await?;
    crate::metrics::audit::add(&crate::metrics::audit::EXPORTED, written.len() as u64);
    Ok(written.len())
}

// ─────────────────────────────────────────── classifying write routes

/// How an ingress write route is audited.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    /// An in-range action: a `Table::Audit` row in the same commit.
    InRange(&'static str),
    /// An action on a bucket object: intent + outcome.
    Object(&'static str),
    /// Not audited, with a reason.
    Exempt(&'static str),
}

/// Every write route (not `GET`) of `ingress::router` — `(method, path, class)`.
/// A new write route must have a line here (`tests/audit.rs::every_mutation_is_audited`).
pub const CLASSES: &[(&str, &str, Class)] = &[
    ("POST", "/deployments", Class::Object("deployment.register")),
    (
        "DELETE",
        "/deployments/{id}",
        Class::Object("deployment.remove"),
    ),
    (
        "PUT",
        "/services/{service}/revision",
        Class::Object("service.revision"),
    ),
    (
        "POST",
        "/invoke/{service}/{handler}",
        Class::Exempt("data plane: an application call"),
    ),
    (
        "POST",
        "/invoke/{service}/{handler}/send",
        Class::Exempt("data plane: an application call"),
    ),
    (
        "POST",
        "/scope/{scope}/invoke/{service}/{handler}",
        Class::Exempt("data plane: an application call"),
    ),
    (
        "POST",
        "/scope/{scope}/invoke/{service}/{handler}/send",
        Class::Exempt("data plane: an application call"),
    ),
    (
        "POST",
        "/workflow/{service}/{key}/attach",
        Class::Exempt("only reads a result"),
    ),
    (
        "POST",
        "/scope/{scope}/workflow/{service}/{key}/attach",
        Class::Exempt("only reads a result"),
    ),
    (
        "POST",
        "/scope/{scope}/promises/{service}/{key}/{name}/resolve",
        Class::Exempt("data plane: completes an application promise"),
    ),
    (
        "POST",
        "/scope/{scope}/promises/{service}/{key}/{name}/reject",
        Class::Exempt("data plane: completes an application promise"),
    ),
    ("POST", "/rules", Class::Object("rules.set")),
    ("DELETE", "/rules", Class::Object("rules.remove")),
    ("DELETE", "/rules/{*pattern}", Class::Object("rules.remove")),
    ("POST", "/query", Class::Exempt("read only")),
    (
        "POST",
        "/invocations/{id}/cancel",
        Class::InRange("invocation.cancel"),
    ),
    (
        "POST",
        "/invocations/{id}/purge",
        Class::InRange("invocation.purge"),
    ),
    (
        "POST",
        "/invocations/{id}/signal",
        Class::InRange("invocation.signal"),
    ),
    (
        "POST",
        "/invocations/{id}/attach",
        Class::Exempt("only reads a result"),
    ),
    (
        "POST",
        "/invocations/{id}/resume",
        Class::InRange("invocation.resume"),
    ),
    (
        "POST",
        "/invocations/{id}/replay",
        Class::Exempt("read only: replays the journal to the SDK, durable state unchanged"),
    ),
    (
        "POST",
        "/invocations/{id}/migrate",
        Class::InRange("invocation.migrate"),
    ),
    (
        "POST",
        "/invocations/{id}/restart",
        Class::InRange("invocation.restart"),
    ),
    (
        "POST",
        "/invocations/batch/{op}",
        Class::InRange("invocation.batch"),
    ),
    (
        "POST",
        "/deployments/{id}/replay-check",
        Class::Exempt("read only: sends the journal to the SDK, changes nothing"),
    ),
    ("PUT", "/schedules/{name}", Class::InRange("schedule.put")),
    (
        "DELETE",
        "/schedules/{name}",
        Class::InRange("schedule.delete"),
    ),
    (
        "POST",
        "/schedules/{name}/pause",
        Class::InRange("schedule.pause"),
    ),
    (
        "POST",
        "/schedules/{name}/resume",
        Class::InRange("schedule.resume"),
    ),
    (
        "POST",
        "/schedules/{name}/trigger",
        Class::InRange("schedule.trigger"),
    ),
    (
        "PUT",
        "/scope/{scope}/schedules/{name}",
        Class::InRange("schedule.put"),
    ),
    (
        "DELETE",
        "/scope/{scope}/schedules/{name}",
        Class::InRange("schedule.delete"),
    ),
    (
        "POST",
        "/scope/{scope}/schedules/{name}/pause",
        Class::InRange("schedule.pause"),
    ),
    (
        "POST",
        "/scope/{scope}/schedules/{name}/resume",
        Class::InRange("schedule.resume"),
    ),
    (
        "POST",
        "/scope/{scope}/schedules/{name}/trigger",
        Class::InRange("schedule.trigger"),
    ),
    (
        "PUT",
        "/ranges/{id}/window",
        Class::Exempt("cluster work (cluster-admin), belongs to no tenant"),
    ),
    (
        "POST",
        "/awakeables/{id}/resolve",
        Class::Exempt("data plane: completes an application awakeable"),
    ),
    (
        "POST",
        "/awakeables/{id}/reject",
        Class::Exempt("data plane: completes an application awakeable"),
    ),
    (
        "POST",
        "/promises/{service}/{key}/{name}/resolve",
        Class::Exempt("data plane: completes an application promise"),
    ),
    (
        "POST",
        "/promises/{service}/{key}/{name}/reject",
        Class::Exempt("data plane: completes an application promise"),
    ),
    (
        "POST",
        "/ranges/{id}/split",
        Class::Exempt("cluster work (cluster-admin)"),
    ),
    (
        "POST",
        "/ranges/merge",
        Class::Exempt("cluster work (cluster-admin)"),
    ),
    (
        "POST",
        "/admin/tick",
        Class::Exempt("cluster work: a manual background tick (tests, operations)"),
    ),
    ("POST", "/admin/keys", Class::Object("key.create")),
    (
        "PUT",
        "/admin/tenants/{tenant}",
        Class::Object("tenant.settings"),
    ),
    (
        "PUT",
        "/admin/tenants/{tenant}/state",
        Class::Object("tenant.state"),
    ),
    ("DELETE", "/admin/keys/{id}", Class::Object("key.revoke")),
    // OIDC relying party: `auth/oidc`, `auth/revoked`.
    ("PUT", "/admin/oidc", Class::Object("oidc.config")),
    (
        "PUT",
        "/admin/oidc/clients/{id}",
        Class::Object("oidc.client.put"),
    ),
    (
        "DELETE",
        "/admin/oidc/clients/{id}",
        Class::Object("oidc.client.remove"),
    ),
    ("POST", "/admin/oidc/revoked", Class::Object("oidc.revoke")),
    ("POST", "/admin/capture", Class::Object("capture.request")),
    (
        "DELETE",
        "/admin/quarantine/{tenant}/{service}/{handler}",
        Class::Object("quarantine.clear"),
    ),
    (
        "PUT",
        "/admin/gc/barriers/{id}",
        Class::Exempt("cluster work (cluster-admin)"),
    ),
    ("POST", "/admin/failpoints", Class::Exempt("test builds")),
];

/// The route of the [`CLASSES`] table matching `(method, path)`: `{x}` matches
/// one segment, `{*x}` matches the rest (at least one segment).
pub fn match_route(method: &str, path: &str) -> Option<(&'static str, Class)> {
    let segs: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    CLASSES.iter().find_map(|(m, route, c)| {
        if *m != method {
            return None;
        }
        let pat: Vec<&str> = route.trim_start_matches('/').split('/').collect();
        let mut i = 0;
        for (j, p) in pat.iter().enumerate() {
            if p.starts_with("{*") {
                return (segs.len() > j).then_some((*route, *c));
            }
            let s = segs.get(i)?;
            if !(p.starts_with('{') || p == s) || s.is_empty() {
                return None;
            }
            i += 1;
        }
        (i == segs.len()).then_some((*route, *c))
    })
}

/// Cap on the body buffered to read the target tenant (`POST /admin/keys`,
/// `POST /admin/capture`): the body is a small JSON.
const MAX_PEEK_BODY: usize = 64 * 1024;

/// The ingress layer for actions **on objects** (class [`Class::Object`]):
/// writes the intent **before** the handler — cannot write ⇒ `503`, the
/// handler does not run — then the outcome from the response status. Sits
/// inside the key layer (it needs the identity) and outside the
/// registry-change notification layer (whose `202 pending` ACK status goes into
/// the outcome).
///
/// Tenant of the record: the `{tenant}` segment of the path, the `tenant` of
/// the JSON body (`POST /admin/keys`, `/admin/capture`,
/// `PUT /admin/oidc/clients/{id}`), the tenant of the key in the registry
/// (key revocation), the tenant of the OIDC client in the registry
/// (`DELETE /admin/oidc/clients/{id}`, `client` of
/// `POST /admin/oidc/revoked`), `?tenant=` (deployments, revisions) — **only
/// when** the caller may pick a tenant (`Identity::may_pick_tenant`);
/// otherwise the key's tenant. The intent is written before the handler, so
/// the target tenant comes from a request nobody has authorised yet: a key of
/// another tenant sending `?tenant=acme` gets `403` from the handler, and that
/// attempt must land in its own audit, not in acme's.
pub async fn object_layer(
    axum::extract::State((sink, keys)): axum::extract::State<(Option<Sink>, crate::auth::Keys)>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let Some(sink) = sink else {
        return next.run(req).await;
    };
    let method = req.method().as_str().to_string();
    let path = req.uri().path().to_string();
    let Some((route, Class::Object(action))) = match_route(&method, &path) else {
        return next.run(req).await;
    };
    let Some(who) = req.extensions().get::<Identity>().cloned() else {
        return next.run(req).await;
    };
    let meta = req.extensions().get::<RequestMeta>().cloned();
    let segs: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    let pats: Vec<&str> = route.trim_start_matches('/').split('/').collect();
    let mut target = serde_json::Map::new();
    let mut tenant: Option<String> = None;
    for (p, s) in pats.iter().zip(&segs) {
        if let Some(name) = p.strip_prefix('{').and_then(|p| p.strip_suffix('}')) {
            let name = name.trim_start_matches('*');
            if name == "tenant" {
                tenant = Some((*s).to_string());
            } else {
                target.insert(name.to_string(), (*s).into());
            }
        }
    }
    // Key revocation: the tenant of the key in the registry (before it disappears).
    if tenant.is_none() && route == "/admin/keys/{id}" {
        if let Some(id) = target.get("id").and_then(|v| v.as_str()) {
            tenant = keys
                .list()
                .into_iter()
                .find(|k| k.id == id)
                .map(|k| k.tenant.as_str().to_string());
        }
    }
    // Removing an OIDC client: the tenant of the client in the registry (before it disappears).
    if tenant.is_none() && route == "/admin/oidc/clients/{id}" && method == "DELETE" {
        if let Some(id) = target.get("id").and_then(|v| v.as_str()) {
            tenant = client_tenant(&keys, id);
        }
    }
    if tenant.is_none() {
        tenant = req.uri().query().and_then(|q| {
            q.split('&')
                .filter_map(|kv| kv.split_once('='))
                .find(|(k, _)| *k == "tenant")
                .map(|(_, v)| v.to_string())
        });
    }
    // A small JSON body carrying the target tenant: buffer, read, hand it back verbatim.
    let req = if tenant.is_none()
        && matches!(
            (method.as_str(), route),
            (
                "POST",
                "/admin/keys" | "/admin/capture" | "/admin/oidc/revoked"
            ) | ("PUT", "/admin/oidc/clients/{id}")
        ) {
        let (parts, body) = req.into_parts();
        let bytes = match axum::body::to_bytes(body, MAX_PEEK_BODY).await {
            Ok(b) => b,
            Err(_) => {
                return (axum::http::StatusCode::PAYLOAD_TOO_LARGE, "body too big").into_response()
            }
        };
        if let Ok(v) = serde_json::from_slice::<Value>(&bytes) {
            tenant = v["tenant"].as_str().map(str::to_string);
            if let Some(r) = v["role"].as_str() {
                target.insert("role".into(), r.into());
            }
            // Revoking an SSO session: the tenant of the client in the registry; the target is `(client, sub)`.
            if route == "/admin/oidc/revoked" {
                for k in ["client", "sub"] {
                    if let Some(x) = v[k].as_str() {
                        target.insert(k.into(), x.chars().take(256).collect::<String>().into());
                    }
                }
                tenant = v["client"].as_str().and_then(|c| client_tenant(&keys, c));
            }
        }
        axum::extract::Request::from_parts(parts, axum::body::Body::from(bytes))
    } else {
        req
    };
    let tenant = tenant
        .and_then(|t| TenantId::new(&t).ok())
        .filter(|t| *t == who.tenant || who.may_pick_tenant())
        .unwrap_or_else(|| who.tenant.clone());
    let note = AuditNote::new(
        meta.as_ref(),
        &who,
        Some(&tenant),
        action,
        Value::Object(target),
    );
    let intent = match sink.intent(note).await {
        Ok(i) => i,
        Err(e) => {
            tracing::warn!(error = %e, "audit: cannot write the intent — refusing the action");
            return (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                axum::Json(json!({ "error": format!("{e} — nothing was done, try again later") })),
            )
                .into_response();
        }
    };
    let resp = next.run(req).await;
    let code = resp.status().as_u16();
    let outcome = match code {
        200..=201 | 204 => "ok",
        202 => "pending",
        _ => "error",
    };
    intent.done(outcome, code).await;
    resp
}

/// The tenant of OIDC client `id` in the node's registry (`auth/oidc`).
fn client_tenant(keys: &crate::auth::Keys, id: &str) -> Option<String> {
    keys.oidc()
        .config()
        .client(id)
        .map(|c| c.tenant.as_str().to_string())
}

/// The intent of a console action outside `ingress::router` (login/logout).
/// `sink` absent (a hand-built router) ⇒ `Ok(None)`, nothing written.
pub async fn console_intent(
    sink: Option<&Sink>,
    meta: &RequestMeta,
    who: &Identity,
    action: &str,
    target: Value,
) -> Result<Option<Intent>, String> {
    let Some(sink) = sink else {
        return Ok(None);
    };
    let note = AuditNote::new(Some(meta), who, Some(&who.tenant), action, target);
    sink.intent(note).await.map(Some)
}

/// The class of `(method, route)` — `None` = not classified yet.
pub fn class_of(method: &str, route: &str) -> Option<Class> {
    CLASSES
        .iter()
        .find(|(m, p, _)| *m == method && *p == route)
        .map(|(_, _, c)| *c)
}

#[cfg(test)]
mod denials_tests {
    use super::*;

    /// **Overflowing the merge table does not misattribute another tenant's
    /// actor.** Order: `noisy`'s key `k_noisy` causes `MAX_DENIAL_KEYS + 5`
    /// distinct merge keys (unknown paths); then `quiet`'s key `k_quiet` is
    /// refused once.
    ///
    /// Must see: `quiet`'s record carries exactly `k_quiet` (overflow is
    /// counted per tenant); `noisy`'s overflow key records the actor
    /// `{"id": "*"}`, not whoever came first.
    #[test]
    fn overflow_is_per_tenant_and_anonymous() {
        let d = Denials::default();
        for i in 0..MAX_DENIAL_KEYS + 5 {
            d.note_outcome(
                "noisy",
                json!({ "id": format!("k_noisy{}", i % 7) }),
                &format!("GET /x{i}"),
                DENIED,
            );
        }
        d.note_outcome("quiet", json!({ "id": "k_quiet" }), "GET /tenant", DENIED);
        let snap = d.snapshot();
        let quiet: Vec<_> = snap.iter().filter(|(k, _, _)| k.0 == "quiet").collect();
        assert_eq!(quiet.len(), 1, "{quiet:?}");
        assert_eq!(quiet[0].1["id"], "k_quiet", "{quiet:?}");
        assert_eq!(quiet[0].0 .1, "k_quiet", "{quiet:?}");
        let over: Vec<_> = snap
            .iter()
            .filter(|(k, _, _)| k.0 == "noisy" && k.2 == "*")
            .collect();
        assert_eq!(over.len(), 1, "one overflow key for noisy: {over:?}");
        assert_eq!(over[0].1["id"], "*", "{over:?}");
        assert_eq!(over[0].2, 5, "{over:?}");
    }
}

#[cfg(test)]
mod reconcile_tests {
    use {
        super::*,
        slatedb::object_store::{memory::InMemory, ObjectStoreExt},
    };

    async fn names(store: &Arc<dyn ObjectStore>) -> Vec<String> {
        use futures::TryStreamExt;
        let mut v: Vec<String> = store
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.location.to_string())
            .collect();
        v.sort();
        v
    }

    /// **An intent left without an outcome fails `verify` (k) after 24 h for good.** The
    /// outcome PUT failed (logged only) or the node died between intent and outcome; nothing
    /// ever wrote the outcome, so the 24 h soak's final verify reports every such intent of
    /// its first hours.
    ///
    /// 1. Three intents of tenant `acme`: two hours old without an outcome, two hours old
    ///    with its outcome, five minutes old without an outcome.
    /// 2. One reconcile pass.
    ///
    /// Must see: the old orphan gets `….done` with outcome `unknown` carrying the intent's
    /// fields; the other two are untouched; a second pass writes nothing.
    #[tokio::test]
    async fn stale_intent_gets_an_unknown_outcome() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = Path::from("p");
        let now = 1_793_536_986_452_u64;
        let old = now - 2 * 3_600_000;
        let put = |name: String, ts: u64, body: Value| {
            let store = Arc::clone(&store);
            let path = cairn_store::audit::object_path(&prefix, "acme", ts, &name);
            async move {
                cairn_store::audit::put_new(&store, &path, Bytes::from(body.to_string()))
                    .await
                    .unwrap();
            }
        };
        let rec =
            |ts: u64, outcome: &str| json!({"ts": ts, "action": "keys.create", "outcome": outcome});
        let orphan = format!("{old}-n1-{:016x}-0", 7);
        let answered = format!("{old}-n1-{:016x}-1", 7);
        let young = format!("{}-n1-{:016x}-2", now - 300_000, 7);
        put(format!("{orphan}.intent"), old, rec(old, "pending")).await;
        put(format!("{answered}.intent"), old, rec(old, "pending")).await;
        put(format!("{answered}.done"), old, rec(old, "ok")).await;
        put(
            format!("{young}.intent"),
            now - 300_000,
            rec(now - 300_000, "pending"),
        )
        .await;
        let before = names(&store).await;

        let r = reconcile_intents(&store, &prefix, now).await.unwrap();
        assert_eq!(r.written, 1, "{r:?}");
        let path = cairn_store::audit::object_path(&prefix, "acme", old, &format!("{orphan}.done"));
        let got: Value =
            serde_json::from_slice(&store.get(&path).await.unwrap().bytes().await.unwrap())
                .unwrap();
        assert_eq!(got["outcome"], "unknown", "{got}");
        assert_eq!(got["action"], "keys.create", "{got}");
        assert_eq!(got["ts"], old, "{got}");
        assert_eq!(got["reconciledMs"], now, "{got}");
        let after = names(&store).await;
        assert_eq!(after.len(), before.len() + 1, "{after:?}");
        assert_eq!(
            reconcile_intents(&store, &prefix, now)
                .await
                .unwrap()
                .written,
            0
        );
    }
}
