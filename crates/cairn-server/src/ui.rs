// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The web console on the ingress port.
//!
//! The static files live in the `cairn-ui` crate (feature `ui`, on by default).
//! Here there is only configuration, the base-path check against the node's
//! routes, and the axum layer that mounts the UI on the
//! router.
//!
//! **The UI stands outside the auth layer**, rather than being an open branch
//! of [`crate::auth::required_role`]: the base path is configurable
//! (`--ui-base-path`), while the role table is a pure function of the path. The
//! UI layer answers only `GET`/`HEAD` under the base path (and `GET /` with
//! `Accept: text/html` ⇒ `302`), plus `POST`/`DELETE <base>/session` (log
//! in/out) and three SSO login routes: `GET <base>/login/options`, `GET
//! <base>/login/oidc`, `GET <base>/login/oidc/callback`. Every other request
//! goes on into the auth layer as before. Static files expose no data, only
//! the version.
//!
//! The SSO routes live **under the base path**, not at the root: their first
//! segment is the base path, which [`check_base_path`] keeps distinct from
//! every API route ([`RESERVED`]).

use axum::Router as HttpRouter;

/// Default UI base path.
pub const DEFAULT_BASE_PATH: &str = "/ui";

/// First segment of every route on the ingress port (and on the internal port).
/// The UI base path must not collide with one: the UI layer stands before auth,
/// so a base `/invocations` would swallow the browser's `GET /invocations/…`.
/// `tests/ui_serving.rs` checks this list against the router.
pub const RESERVED: &[&str] = &[
    "admin",
    "archive",
    "audit",
    "auth",
    "awakeables",
    "cluster",
    "deployments",
    "health",
    "identity",
    "internal",
    "invocations",
    "invoke",
    "limits",
    "metering",
    "metrics",
    "promises",
    "query",
    "ranges",
    "rules",
    "schedules",
    "scheduler",
    "scope",
    "services",
    "stats",
    "tenant",
    "values",
    "vqueues",
    "workflow",
];

/// A given UI file set: (`relative name`, bytes).
pub type UiFiles = Vec<(String, Vec<u8>)>;

/// A node's UI configuration.
#[derive(Clone, Debug)]
pub struct UiConfig {
    /// `false` = `--no-ui`: nothing is served, requests fall through to the API
    /// as if there were no UI.
    pub enabled: bool,
    /// `--ui-base-path`, default [`DEFAULT_BASE_PATH`].
    pub base_path: String,
    /// File set replacing the embedded build (`relative name`, bytes) — tests
    /// only, to check assets, compression and caching without `ui/dist`.
    /// `None` = the embedded build.
    pub files: Option<std::sync::Arc<UiFiles>>,
    /// The session cookie carries `Secure` (`--ui-cookie-secure on|off`).
    /// `None` = by `host`: on when the node listens on a non-loopback address
    /// (cairn has no TLS yet — behind a TLS proxy it is on, plain HTTP access
    /// from afar needs it turned off by hand).
    pub cookie_secure: Option<bool>,
    /// `--trust-proxy <cidr>`: peers in these ranges are trusted for
    /// `X-Forwarded-Host`/`-Proto`/`-For` — the `Origin` check, the `Secure`
    /// flag, the IP of login limits. Empty = trust nobody.
    pub trust_proxy: crate::auth::TrustedProxies,
    /// Lifetime of one session (default [`crate::auth::SESSION_TTL`]; tests lower it).
    pub session_ttl: std::time::Duration,
    /// `--ui-env-label`: the console environment label shown in the top bar
    /// (`prod`, `test-eu`, …), via `GET /auth/whoami`. Already checked with
    /// [`check_env_label`]. `None` = no badge.
    pub env_label: Option<String>,
}

/// Length cap of `--ui-env-label`.
pub const ENV_LABEL_MAX: usize = 24;

/// A valid environment label: 1..=[`ENV_LABEL_MAX`] printable ASCII
/// characters, no leading/trailing whitespace — it goes into the top bar of
/// every page, it is not a place for a sentence.
pub fn check_env_label(v: &str) -> Result<(), String> {
    if v.is_empty() || v.len() > ENV_LABEL_MAX {
        return Err(format!(
            "environment label must have 1..={ENV_LABEL_MAX} characters"
        ));
    }
    if v.trim() != v || !v.bytes().all(|b| (0x20..0x7f).contains(&b)) {
        return Err(
            "environment label accepts only printable ASCII, no leading/trailing whitespace".into(),
        );
    }
    Ok(())
}

impl Default for UiConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            base_path: DEFAULT_BASE_PATH.into(),
            files: None,
            cookie_secure: None,
            trust_proxy: crate::auth::TrustedProxies::default(),
            session_ttl: crate::auth::SESSION_TTL,
            env_label: None,
        }
    }
}

/// `host` is a loopback address (`127.0.0.0/8`, `::1`, `localhost`).
pub fn is_loopback(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .trim_matches(['[', ']'])
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// `true` when the binary has the real UI (feature `ui` and `ui/dist` built at compile time).
pub fn built() -> bool {
    #[cfg(feature = "ui")]
    {
        cairn_ui::built()
    }
    #[cfg(not(feature = "ui"))]
    {
        false
    }
}

/// Checks the base path: syntax `^(/[A-Za-z0-9._~-]+)+$` (no `.`/`..`
/// segment) and a first segment that matches no route of the node ([`RESERVED`]).
pub fn check_base_path(s: &str) -> Result<(), String> {
    let syntax = s.len() > 1
        && s.starts_with('/')
        && s[1..].split('/').all(|seg| {
            !seg.is_empty()
                && seg != "."
                && seg != ".."
                && seg
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._~-".contains(&b))
        });
    if !syntax {
        return Err(format!(
            "--ui-base-path {s:?} is invalid — use /a or /a/b, each segment only A-Z a-z 0-9 . _ ~ -"
        ));
    }
    let first = s[1..].split('/').next().unwrap_or("");
    if RESERVED.contains(&first) {
        return Err(format!(
            "--ui-base-path {s:?} collides with the API route /{first} — pick another name (default {DEFAULT_BASE_PATH})"
        ));
    }
    Ok(())
}

/// A node's built UI — empty when disabled or when built without the feature.
#[derive(Clone, Default)]
pub struct Ui {
    #[cfg(feature = "ui")]
    inner: Option<std::sync::Arc<cairn_ui::Ui>>,
}

impl Ui {
    /// Builds the UI from `cfg`. A bad base path is an error **even** when the
    /// UI is disabled: a wrong configuration must not lie dormant until the day
    /// it is turned on.
    pub fn build(cfg: &UiConfig) -> Result<Self, String> {
        check_base_path(&cfg.base_path)?;
        if let Some(l) = &cfg.env_label {
            check_env_label(l)?;
        }
        #[cfg(feature = "ui")]
        {
            if !cfg.enabled {
                return Ok(Self::default());
            }
            let version = env!("CARGO_PKG_VERSION");
            let ui = match &cfg.files {
                Some(files) => cairn_ui::Ui::from_files(
                    &cfg.base_path,
                    version,
                    files.iter().map(|(n, b)| (n.as_str(), b.as_slice())),
                ),
                None => cairn_ui::Ui::new(&cfg.base_path, version),
            }?;
            Ok(Self {
                inner: Some(std::sync::Arc::new(ui)),
            })
        }
        #[cfg(not(feature = "ui"))]
        Ok(Self::default())
    }

    /// `true` when the node serves the UI.
    pub fn enabled(&self) -> bool {
        #[cfg(feature = "ui")]
        {
            self.inner.is_some()
        }
        #[cfg(not(feature = "ui"))]
        {
            false
        }
    }

    /// Wraps `router` with the UI layer — call it **after** the auth layer so
    /// the UI runs before it. `auth` is the port's auth: login checks keys with
    /// its own registry. `audit`: where logins/logouts are recorded; `None` ⇒
    /// not recorded (hand-built router).
    #[cfg_attr(not(feature = "ui"), allow(unused_variables))]
    pub fn wrap(
        &self,
        router: HttpRouter,
        auth: crate::auth::Auth,
        audit: Option<crate::audit::Sink>,
    ) -> HttpRouter {
        #[cfg(feature = "ui")]
        if let Some(ui) = &self.inner {
            let base = ui.base_path().to_string();
            let gate = Gate {
                session: format!("{base}/session"),
                login_options: format!("{base}/login/options"),
                oidc_start: format!("{base}/login/oidc"),
                oidc_callback: format!("{base}/login/oidc/callback"),
                base,
                ui: ui.clone(),
                auth,
                audit,
                limits: std::sync::Arc::new(session::LoginLimits::new()),
            };
            return router.layer(axum::middleware::from_fn_with_state(gate, serve));
        }
        router
    }
}

#[cfg(feature = "ui")]
#[derive(Clone)]
struct Gate {
    ui: std::sync::Arc<cairn_ui::Ui>,
    auth: crate::auth::Auth,
    /// Audit sink for logins/logouts.
    audit: Option<crate::audit::Sink>,
    /// `<base>/session`.
    session: String,
    /// The UI base path.
    base: String,
    /// `<base>/login/options`, `<base>/login/oidc`, `<base>/login/oidc/callback`.
    login_options: String,
    oidc_start: String,
    oidc_callback: String,
    /// The node's failed-login limits.
    limits: std::sync::Arc<session::LoginLimits>,
}

#[cfg(feature = "ui")]
async fn serve(
    axum::extract::State(g): axum::extract::State<Gate>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::http::Method;
    if req.uri().path() == g.session {
        if req.method() == Method::POST {
            return session::login(&g.auth, g.audit.as_ref(), &g.limits, req).await;
        }
        if req.method() == Method::DELETE {
            return session::logout(&g.auth, g.audit.as_ref(), &g.limits, req).await;
        }
    }
    if req.method() == Method::GET {
        let path = req.uri().path();
        if path == g.login_options {
            return session::login_options(&g.auth, &req);
        }
        if path == g.oidc_start {
            return session::oidc_start(&g.auth, &g.limits, &g.base, req).await;
        }
        if path == g.oidc_callback {
            return session::oidc_callback(&g.auth, g.audit.as_ref(), &g.limits, &g.base, req)
                .await;
        }
    }
    match g.ui.respond(req.method(), req.uri().path(), req.headers()) {
        Some(r) => r,
        None => next.run(req).await,
    }
}

/// Console login / logout.
#[cfg(feature = "ui")]
mod session {
    use {
        crate::auth::{
            cookies_named, deny, oidc, peer_ip, request_host, same_origin, Auth, Forwarded,
        },
        axum::{
            http::{header, HeaderValue, StatusCode},
            response::{IntoResponse, Response},
            Json,
        },
    };

    /// Body cap of `POST <base>/session` — one `cf_…` key.
    const MAX_BODY: usize = 16 * 1024;

    /// Failed logins per IP: `IP_BURST` at once, then one every `IP_EVERY`.
    /// Someone mistyping a few times never hits it.
    pub(super) const IP_BURST: u32 = 10;
    pub(super) const IP_EVERY: std::time::Duration = std::time::Duration::from_secs(6);
    /// Failed logins per key prefix ([`key_prefix`]) — blocks guessing one key
    /// from many IPs.
    pub(super) const PREFIX_BURST: u32 = 5;
    pub(super) const PREFIX_EVERY: std::time::Duration = std::time::Duration::from_secs(12);
    /// Cap on IPs/prefixes tracked at once ([`crate::rate::KeyedLimiter`]).
    const MAX_TRACKED: usize = 10_000;
    /// `GET <base>/login/oidc` per client: each one is a flow cookie and one
    /// read of the issuer's discovery document.
    pub(super) const SSO_START_BURST: u32 = 20;
    pub(super) const SSO_START_EVERY: std::time::Duration = std::time::Duration::from_secs(3);
    /// Callbacks per client.
    pub(super) const SSO_CALLBACK_BURST: u32 = 10;
    pub(super) const SSO_CALLBACK_EVERY: std::time::Duration = std::time::Duration::from_secs(6);
    /// Calls to the issuer's token endpoint **per OIDC client**: the cap stops a
    /// flood from many IPs at one tenant — every valid callback is an outbound
    /// request — without touching other tenants.
    pub(super) const EXCHANGE_BURST: u32 = 20;
    pub(super) const EXCHANGE_EVERY: std::time::Duration = std::time::Duration::from_secs(1);
    /// Tickets per IP for the "clean" lane of a client's `exchange` cap, refilled one
    /// a minute. A ticket is taken only for a callback with a fresh, valid flow, right
    /// before the exchange; a successful login gives it back, a failed exchange keeps
    /// it. Junk codes from an IP then get 3 exchanges and one a minute on the lane real
    /// users log in through, and draw on the client's other lane after that; 3 lets a
    /// few users behind one NAT log in at the same time on the clean lane.
    pub(super) const SSO_TRUST_BURST: u32 = 3;
    pub(super) const SSO_TRUST_EVERY: std::time::Duration = std::time::Duration::from_secs(60);
    /// The client's other lane (IPs out of tickets): half the clean lane, so the
    /// client's two lanes together reach the issuer at most 1.5 times as often as
    /// one lane did.
    pub(super) const SUSPECT_EXCHANGE_BURST: u32 = 10;
    pub(super) const SUSPECT_EXCHANGE_EVERY: std::time::Duration =
        std::time::Duration::from_secs(2);
    /// The node-wide `exchange` cap: only left to stop the total of **many**
    /// clients flooded at once (protecting the issuer and the node), 10 times
    /// the per-client cap.
    pub(super) const NODE_EXCHANGE_BURST: u32 = 200;
    pub(super) const NODE_EXCHANGE_EVERY: std::time::Duration =
        std::time::Duration::from_millis(100);
    /// Cap on logouts **that write**, per IP: burst 30, then one every 2 s.
    /// A logout that writes nothing (expired cookie, already revoked) costs no slot.
    pub(super) const LOGOUT_IP_BURST: u32 = 30;
    pub(super) const LOGOUT_IP_EVERY: std::time::Duration = std::time::Duration::from_secs(2);
    /// Cap on logouts that write, per tenant on the node: burst 60, then one per
    /// second — one tenant cannot keep the cluster registry generation changing
    /// continuously.
    pub(super) const LOGOUT_TENANT_BURST: u32 = 60;
    pub(super) const LOGOUT_TENANT_EVERY: std::time::Duration = std::time::Duration::from_secs(1);
    /// Number of OIDC clients tracked at once by the per-client cap.
    const MAX_CLIENTS: usize = 10_000;
    /// Cap on used `state`s remembered (each entry lives until the flow cookie
    /// expires, 600 s). Full ⇒ new flows are refused, old entries are not forgotten.
    const MAX_USED_STATES: usize = 100_000;
    /// Key prefix length: `cf_` + 8 characters.
    const PREFIX_LEN: usize = 11;

    /// A node's login limits: GCRA per IP and per key prefix. Only **failed**
    /// attempts count; when out of slots, `429` before the key is checked — even
    /// for a correct key, so a guesser gets no test at all.
    pub(super) struct LoginLimits {
        by_ip: crate::rate::KeyedLimiter<std::net::IpAddr>,
        by_prefix: crate::rate::KeyedLimiter<String>,
        sso_start: crate::rate::KeyedLimiter<std::net::IpAddr>,
        sso_callback: crate::rate::KeyedLimiter<std::net::IpAddr>,
        exchange: crate::rate::KeyedLimiter<()>,
        /// `exchange` cap per OIDC client (`client_id`), for IPs holding a ticket
        /// ([`SSO_TRUST_BURST`]).
        exchange_by_client: crate::rate::KeyedLimiter<String>,
        /// The smaller cap per client for IPs out of tickets
        /// ([`SUSPECT_EXCHANGE_BURST`]).
        exchange_suspect_by_client: crate::rate::KeyedLimiter<String>,
        /// The clean lane's tickets per IP ([`SSO_TRUST_BURST`]).
        sso_trust: crate::rate::KeyedLimiter<std::net::IpAddr>,
        /// Cap on logouts **that write**, per IP and per tenant: each one is a
        /// write of the cluster-level session registry and a bump of the
        /// registry generation.
        logout_by_ip: crate::rate::KeyedLimiter<std::net::IpAddr>,
        logout_by_tenant: crate::rate::KeyedLimiter<String>,
        /// `state` of SSO flows that passed the callback ⇒ the flow cookie's
        /// expiry. The flow cookie is signed and stateless, so it can be
        /// replayed until it expires; this set makes it single-use **on this
        /// node**. Another node receiving the same cookie lets it through at
        /// most once more — the `exchange` cap bounds the total.
        used: std::sync::Mutex<std::collections::BTreeMap<String, u64>>,
    }

    /// Outcome of [`LoginLimits::claim_state`].
    enum Claim {
        Fresh,
        Replayed,
        Full,
    }

    impl LoginLimits {
        pub(super) fn new() -> Self {
            Self {
                by_ip: crate::rate::KeyedLimiter::new(IP_BURST, IP_EVERY, MAX_TRACKED),
                by_prefix: crate::rate::KeyedLimiter::new(PREFIX_BURST, PREFIX_EVERY, MAX_TRACKED),
                sso_start: crate::rate::KeyedLimiter::new(
                    SSO_START_BURST,
                    SSO_START_EVERY,
                    MAX_TRACKED,
                ),
                sso_callback: crate::rate::KeyedLimiter::new(
                    SSO_CALLBACK_BURST,
                    SSO_CALLBACK_EVERY,
                    MAX_TRACKED,
                ),
                exchange: crate::rate::KeyedLimiter::new(
                    NODE_EXCHANGE_BURST,
                    NODE_EXCHANGE_EVERY,
                    1,
                ),
                exchange_by_client: crate::rate::KeyedLimiter::new(
                    EXCHANGE_BURST,
                    EXCHANGE_EVERY,
                    MAX_CLIENTS,
                ),
                exchange_suspect_by_client: crate::rate::KeyedLimiter::new(
                    SUSPECT_EXCHANGE_BURST,
                    SUSPECT_EXCHANGE_EVERY,
                    MAX_CLIENTS,
                ),
                sso_trust: crate::rate::KeyedLimiter::new(
                    SSO_TRUST_BURST,
                    SSO_TRUST_EVERY,
                    MAX_TRACKED,
                ),
                logout_by_ip: crate::rate::KeyedLimiter::new(
                    LOGOUT_IP_BURST,
                    LOGOUT_IP_EVERY,
                    MAX_TRACKED,
                ),
                logout_by_tenant: crate::rate::KeyedLimiter::new(
                    LOGOUT_TENANT_BURST,
                    LOGOUT_TENANT_EVERY,
                    MAX_TRACKED,
                ),
                used: std::sync::Mutex::default(),
            }
        }

        /// Marks `state` used until `exp` (seconds). When full, sweep expired
        /// entries first; still full ⇒ `Full`.
        fn claim_state(&self, state: &str, exp: u64, now_s: u64) -> Claim {
            let mut used = self.used.lock().unwrap_or_else(|p| p.into_inner());
            if used.contains_key(state) {
                return Claim::Replayed;
            }
            if used.len() >= MAX_USED_STATES {
                used.retain(|_, e| *e > now_s);
                if used.len() >= MAX_USED_STATES {
                    return Claim::Full;
                }
            }
            used.insert(state.to_owned(), exp);
            Claim::Fresh
        }

        /// Returns `state` when the attempt was refused **before** `exchange`
        /// by the node cap — the user retries that same callback after `Retry-After`.
        fn unclaim_state(&self, state: &str) {
            self.used
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(state);
        }

        fn check(
            &self,
            ip: &std::net::IpAddr,
            prefix: &str,
            now: tokio::time::Instant,
        ) -> Result<(), std::time::Duration> {
            let a = self.by_ip.check(ip, now);
            let b = self.by_prefix.check(&prefix.to_owned(), now);
            match (a, b) {
                (Ok(()), Ok(())) => Ok(()),
                (Err(x), Err(y)) => Err(x.max(y)),
                (Err(x), _) | (_, Err(x)) => Err(x),
            }
        }

        fn failed(&self, ip: &std::net::IpAddr, prefix: &str, now: tokio::time::Instant) {
            self.by_ip.charge(ip, now);
            self.by_prefix.charge(&prefix.to_owned(), now);
        }
    }

    /// The prefix of the sender's key — the first [`PREFIX_LEN`] characters
    /// (the whole key if shorter), on a character boundary.
    fn key_prefix(key: &str) -> String {
        key.chars().take(PREFIX_LEN).collect()
    }

    /// The trusted proxy's claims and the request's client IP. No peer (router
    /// called in process) ⇒ IP `0.0.0.0` — one shared bucket.
    fn seen(auth: &Auth, req: &axum::extract::Request) -> (Option<Forwarded>, std::net::IpAddr) {
        let peer = peer_ip(req.extensions());
        let trusted = auth.sessions.as_ref().map(|s| s.trusted());
        let fwd = trusted.and_then(|t| t.forwarded_in(req.headers(), req.extensions()));
        let ip = trusted
            .and_then(|t| t.client_ip(req.headers(), peer))
            .or(peer)
            .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));
        // The client's key in every per-IP limiter: IPv6 grouped by /64.
        (fwd, crate::rate::client_key(ip))
    }

    fn too_many(wait: std::time::Duration) -> Response {
        too_many_because(wait, "too many failed logins")
    }

    /// `429` with `Retry-After` (seconds, rounded up, ≥ 1).
    fn too_many_because(wait: std::time::Duration, why: &str) -> Response {
        let secs = wait.as_secs() + u64::from(wait.subsec_nanos() > 0);
        let secs = secs.max(1);
        let mut r = deny(
            StatusCode::TOO_MANY_REQUESTS,
            format!("{why} — retry in {secs} s"),
        );
        if let Ok(v) = HeaderValue::from_str(&secs.to_string()) {
            r.headers_mut().insert(header::RETRY_AFTER, v);
        }
        no_store(r)
    }

    fn cross_origin() -> Response {
        deny(
            StatusCode::FORBIDDEN,
            "login/logout must come from the console itself (cross-origin Origin / Sec-Fetch-Site)"
                .into(),
        )
    }

    fn no_store(mut r: Response) -> Response {
        r.headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        r
    }

    /// `POST <base>/session` `{key}`: checks the key like a Bearer, returns the
    /// identity and `Set-Cookie`. Login CSRF (someone else logging the victim's
    /// browser into their own key) is blocked by the same origin check.
    pub(super) async fn login(
        auth: &Auth,
        audit: Option<&crate::audit::Sink>,
        limits: &LoginLimits,
        req: axum::extract::Request,
    ) -> Response {
        let (fwd, ip) = seen(auth, &req);
        let meta = crate::audit::meta_of(auth, &req);
        if !same_origin(req.headers(), fwd.as_ref()) {
            return cross_origin();
        }
        if !auth.required {
            return no_store(Json(serde_json::json!({ "noAuth": true })).into_response());
        }
        let Some(sessions) = &auth.sessions else {
            return deny(
                StatusCode::SERVICE_UNAVAILABLE,
                "node has no cluster secret yet — cannot sign sessions".into(),
            );
        };
        let body = match axum::body::to_bytes(req.into_body(), MAX_BODY).await {
            Ok(b) => b,
            Err(e) => return deny(StatusCode::BAD_REQUEST, format!("cannot read body: {e}")),
        };
        #[derive(serde::Deserialize)]
        struct Login {
            key: String,
        }
        let Ok(Login { key }) = serde_json::from_slice::<Login>(&body) else {
            return deny(
                StatusCode::BAD_REQUEST,
                "body must be {\"key\": \"cf_…\"}".into(),
            );
        };
        let key = key.trim();
        let prefix = key_prefix(key);
        let now = tokio::time::Instant::now();
        if let Err(wait) = limits.check(&ip, &prefix, now) {
            return too_many(wait);
        }
        let Some((who, kid)) = auth.keys.login(key) else {
            limits.failed(&ip, &prefix, now);
            crate::metrics::auth_failures::note(crate::metrics::auth_failures::Kind::Login);
            return deny(StatusCode::UNAUTHORIZED, "invalid API key".into());
        };
        if auth.stale(&who) {
            return deny(
                StatusCode::SERVICE_UNAVAILABLE,
                "node could not confirm that its key registry is fresh — try another node or retry later".into(),
            );
        }
        // The intent before the session is granted: if it cannot be written ⇒
        // no login.
        let intent = match crate::audit::console_intent(
            audit,
            &meta,
            &who,
            "session.login",
            serde_json::json!({ "via": "key" }),
        )
        .await
        {
            Ok(i) => i,
            Err(e) => return audit_down(&e),
        };
        let (value, exp) = sessions.mint(&kid, &who);
        let mut r = Json(serde_json::json!({
            "tenant": who.tenant.as_str(),
            "role": who.role.as_str(),
            "capabilities": who.role.caps().names(),
            "keyId": who.key_id,
            "bootstrap": who.key_id.is_none(),
            "sessionExpiresAt": exp * 1000,
        }))
        .into_response();
        if let Ok(v) = HeaderValue::from_str(&sessions.set_cookie(&value, fwd.as_ref())) {
            r.headers_mut().insert(header::SET_COOKIE, v);
        }
        if let Some(i) = intent {
            i.done("ok", r.status().as_u16()).await;
        }
        no_store(r)
    }

    /// The login's audit intent could not be written ⇒ `503`, no session granted.
    fn audit_down(e: &str) -> Response {
        tracing::warn!(error = %e, "audit: cannot write the login intent — refusing");
        deny(
            StatusCode::SERVICE_UNAVAILABLE,
            "cannot write audit — not logged in, retry later".into(),
        )
    }

    /// `DELETE <base>/session`: clears the cookie and revokes this session on the
    /// server — its session id goes into `auth/revoked-sessions`
    /// (`docs/self-hosting.md` §8); other sessions of the same key live on.
    ///
    /// Audit: a still-valid cookie ⇒ that person's `session.logout` pair.
    /// Unlike login, a failed write does **not** block: logout only reduces
    /// rights, and the user must always be able to leave the session.
    pub(super) async fn logout(
        auth: &Auth,
        audit: Option<&crate::audit::Sink>,
        limits: &LoginLimits,
        req: axum::extract::Request,
    ) -> Response {
        let (fwd, ip) = seen(auth, &req);
        if !same_origin(req.headers(), fwd.as_ref()) {
            return cross_origin();
        }
        let intent = match auth.cookie_session(req.headers(), req.extensions()) {
            Some((who, _)) if auth.required => {
                let target = match who.user.as_deref() {
                    Some(u) => serde_json::json!({ "via": "sso", "client": u.client }),
                    None => serde_json::json!({ "via": "key" }),
                };
                let meta = crate::audit::meta_of(auth, &req);
                crate::audit::console_intent(audit, &meta, &who, "session.logout", target)
                    .await
                    .unwrap_or_else(|e| {
                        tracing::warn!(error = %e, "audit: cannot write the logout intent — logging out anyway");
                        None
                    })
            }
            _ => None,
        };
        // Revoke on the server side every session the request carries: a cookie
        // captured before the logout no longer opens the API. A failed write ⇒
        // the cookie is still cleared in the browser (the user can always leave
        // the session) and `503` is returned so the console says plainly that
        // the session was not revoked.
        //
        // Only a correctly signed cookie, **not yet expired**, not yet in the
        // registry (RAM copy) reaches the registry — a repeated logout, or one
        // with an old cookie, costs no I/O. A tenant already at the cap on
        // unexpired logged-out sessions ⇒ `429`, no I/O.
        let mut revoke_failed = false;
        let mut book_full = false;
        let mut limited = None;
        if let (Some(s), true) = (&auth.sessions, auth.required) {
            let now = crate::auth::now_s();
            let book = auth.keys.oidc();
            let mut ids: Vec<(String, u64, String)> =
                crate::auth::session_cookies(req.headers(), s.cookie_name(fwd.as_ref()))
                    .filter_map(|c| s.session_id(c))
                    .collect();
            ids.extend(
                cookies_named(req.headers(), oidc::SSO_COOKIE)
                    .filter_map(|c| oidc::sso_session_id(s, c, now)),
            );
            ids.retain(|(sid, ..)| !book.session_listed(sid) && !book.revoke_refused(sid, now));
            ids.dedup_by(|a, b| a.0 == b.0);
            ids.retain(|(_, _, tenant)| {
                let under =
                    book.revoked_sessions_of(tenant, now) < oidc::MAX_REVOKED_SESSIONS_PER_TENANT;
                book_full |= !under;
                under
            });
            // Only logouts **that write** cost a slot, per IP then per tenant.
            if !ids.is_empty() {
                let t0 = tokio::time::Instant::now();
                let by_tenant = |t: &String| limits.logout_by_tenant.take(t, t0);
                if let Err(wait) = limits
                    .logout_by_ip
                    .take(&ip, t0)
                    .and_then(|()| ids.iter().try_for_each(|(_, _, t)| by_tenant(t)))
                {
                    limited = Some(wait);
                    ids.clear();
                }
            }
            // Write through the registry-generation layer like every
            // cluster-level registry write: peers re-read at once instead of at
            // the full reconciliation (10 minutes), and every node's refresh
            // loop is confirmed to have a fresh session registry by generation.
            let revoke_all = async {
                let (mut written, mut full, mut failed) = (false, false, false);
                for (sid, exp, tenant) in &ids {
                    match book.revoke_session(sid, tenant, *exp, now).await {
                        Ok(()) => written = true,
                        Err(oidc::RevokeError::Full) => full = true,
                        Err(e) => {
                            tracing::warn!(error = %e, "could not revoke the session on logout");
                            failed = true;
                        }
                    }
                }
                (written, full, failed)
            };
            if !ids.is_empty() {
                let outcome = match &auth.registry {
                    Some(sync) => sync
                        .write(revoke_all, |(written, ..)| {
                            if *written {
                                crate::registry_sync::After::Publish
                            } else {
                                crate::registry_sync::After::Quiet
                            }
                        })
                        .await
                        .map_err(|e| {
                            tracing::warn!(error = %e, "could not open a registry generation for the logout");
                        }),
                    None => Ok(revoke_all.await),
                };
                match outcome {
                    Ok((_, full, failed)) => {
                        book_full |= full;
                        revoke_failed |= failed;
                    }
                    Err(()) => revoke_failed = true,
                }
            }
        }
        let mut r = if let Some(wait) = limited {
            let mut r = deny(
                StatusCode::TOO_MANY_REQUESTS,
                "signed out in this browser, but too many sign-outs came from this client or tenant — the session stays valid on the server until its cookie expires unless you retry later".into(),
            );
            if let Ok(v) = HeaderValue::from_str(&wait.as_secs().max(1).to_string()) {
                r.headers_mut().insert(header::RETRY_AFTER, v);
            }
            r
        } else if revoke_failed {
            deny(
                StatusCode::SERVICE_UNAVAILABLE,
                "signed out in this browser, but the session could not be revoked on the server — retry".into(),
            )
        } else if book_full {
            let mut r = deny(
                StatusCode::TOO_MANY_REQUESTS,
                "signed out in this browser, but your tenant has too many signed-out sessions that have not expired yet — the session stays valid on the server until its cookie expires; revoke the API key to end it now".into(),
            );
            r.headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("60"));
            r
        } else {
            StatusCode::NO_CONTENT.into_response()
        };
        if let Some(s) = &auth.sessions {
            for c in s.clear_cookies(fwd.as_ref()) {
                if let Ok(v) = HeaderValue::from_str(&c) {
                    r.headers_mut().append(header::SET_COOKIE, v);
                }
            }
        }
        if let Some(i) = intent {
            i.done("ok", r.status().as_u16()).await;
        }
        no_store(r)
    }

    // ───────────────────────────────────────── SSO login

    /// The OIDC client of the request's hostname, when SSO is on for it:
    /// (issuer, client, verbatim `Host` with port, proxy claims).
    fn sso_client(
        auth: &Auth,
        req: &axum::extract::Request,
    ) -> Option<(String, oidc::OidcClient, String, Option<Forwarded>)> {
        if !auth.required {
            return None;
        }
        let (fwd, _) = seen(auth, req);
        let raw = request_host(req.headers(), fwd.as_ref())?.to_ascii_lowercase();
        // `Host` goes into the redirect URL: only the characters of a host[:port].
        if !raw
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b".-:[]".contains(&b))
        {
            return None;
        }
        let host = oidc::normalize_host(&raw)?;
        let (issuer, client) = auth.keys.oidc().login_client(&host)?;
        Some((issuer, client, raw, fwd))
    }

    /// `GET <base>/login/options` (open): the login page asks whether to show
    /// an SSO button. Exposes only "this hostname has SSO", the IdP name on the
    /// button and the portal link — no tenant, no issuer.
    pub(super) fn login_options(auth: &Auth, req: &axum::extract::Request) -> Response {
        let sso = sso_client(auth, req);
        // Under the read lock, without copying the whole configuration: this route is open.
        let name = sso
            .as_ref()
            .and_then(|_| auth.keys.oidc().with(|c, _| c.display_name.clone()));
        no_store(
            Json(serde_json::json!({
                "sso": sso.is_some(),
                "displayName": name,
                "portalUrl": sso.and_then(|(_, c, _, _)| c.portal_url),
            }))
            .into_response(),
        )
    }

    fn query(req: &axum::extract::Request) -> Vec<(String, String)> {
        form_urlencoded::parse(req.uri().query().unwrap_or_default().as_bytes())
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect()
    }

    fn param<'a>(q: &'a [(String, String)], k: &str) -> Option<&'a str> {
        q.iter().find(|(key, _)| key == k).map(|(_, v)| v.as_str())
    }

    /// `303` to `to`, with the `Set-Cookie`s.
    fn redirect(to: &str, cookies: &[String]) -> Response {
        let mut r = StatusCode::SEE_OTHER.into_response();
        if let Ok(v) = HeaderValue::from_str(to) {
            r.headers_mut().insert(header::LOCATION, v);
        }
        for c in cookies {
            if let Ok(v) = HeaderValue::from_str(c) {
                r.headers_mut().append(header::SET_COOKIE, v);
            }
        }
        no_store(r)
    }

    /// Failed SSO login: back to the login page with a stable code
    /// (`sso_error`) — the browser is navigating, not calling an API. The
    /// temporary cookie is cleared.
    fn sso_error(base: &str, code: &str) -> Response {
        redirect(
            &format!("{base}/login?sso_error={code}"),
            &[oidc::clear_flow_cookie()],
        )
    }

    /// `GET <base>/login/oidc?return_to=…`: sinh `state`, `nonce`,
    /// `code_verifier` into a temporary cookie signed with the cluster secret,
    /// then redirects to the issuer. `return_to` only accepts a same-origin
    /// relative path.
    pub(super) async fn oidc_start(
        auth: &Auth,
        limits: &LoginLimits,
        base: &str,
        req: axum::extract::Request,
    ) -> Response {
        let (_, ip) = seen(auth, &req);
        if let Err(wait) = limits.sso_start.take(&ip, tokio::time::Instant::now()) {
            return too_many_because(wait, "too many SSO login starts");
        }
        let (Some(sessions), Some(rp)) = (&auth.sessions, &auth.oidc) else {
            return sso_error(base, "not_configured");
        };
        let Some((issuer, client, raw_host, fwd)) = sso_client(auth, &req) else {
            return sso_error(base, "not_configured");
        };
        let q = query(&req);
        let return_to = oidc::safe_return_to(param(&q, "return_to"), &format!("{base}/"));
        let provider = match rp.provider(&issuer).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error = %e, "cannot fetch the OIDC issuer's discovery");
                return sso_error(base, "issuer_unavailable");
            }
        };
        let scheme = if sessions.secure_for(fwd.as_ref()) {
            "https"
        } else {
            "http"
        };
        let flow = oidc::Flow::new(
            &client.client_id,
            format!("{scheme}://{raw_host}{base}/login/oidc/callback"),
            return_to,
            crate::auth::now_s(),
        );
        let to = oidc::Rp::authorize_url(&provider, &client.client_id, &flow);
        redirect(to.as_str(), &[oidc::set_flow_cookie(&flow.seal(sessions))])
    }

    /// `GET <base>/login/oidc/callback?code&state`: `state` matches the
    /// temporary cookie (against login CSRF), the code is exchanged for tokens,
    /// `id_token` is checked, the v2 session cookie is set, back to `return_to`.
    pub(super) async fn oidc_callback(
        auth: &Auth,
        audit: Option<&crate::audit::Sink>,
        limits: &LoginLimits,
        base: &str,
        req: axum::extract::Request,
    ) -> Response {
        // The per-client cap counts only **failed** callbacks (like key login):
        // real users behind a shared NAT who log in successfully do not eat
        // each other's slots. An `exchange` flood from one client is still
        // blocked — every callback needs a new `state`, and a `state` only
        // comes from `/login/oidc` (which has its own per-client cap) and is
        // single-use.
        let (_, ip) = seen(auth, &req);
        let now = tokio::time::Instant::now();
        if let Err(wait) = limits.sso_callback.check(&ip, now) {
            return too_many_because(wait, "too many failed SSO login callbacks");
        }
        let mut ticket = false;
        let r = callback_flow(auth, audit, limits, base, req, ip, &mut ticket).await;
        let failed = r
            .headers()
            .get(header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|l| l.starts_with(&format!("{base}/login?sso_error=")));
        if failed {
            limits.sso_callback.charge(&ip, now);
        } else if ticket {
            // Logged in: the ticket goes back ([`SSO_TRUST_BURST`]).
            limits.sso_trust.refund(&ip, tokio::time::Instant::now());
        }
        r
    }

    async fn callback_flow(
        auth: &Auth,
        audit: Option<&crate::audit::Sink>,
        limits: &LoginLimits,
        base: &str,
        req: axum::extract::Request,
        ip: std::net::IpAddr,
        ticket: &mut bool,
    ) -> Response {
        let (Some(sessions), Some(rp)) = (&auth.sessions, &auth.oidc) else {
            return sso_error(base, "not_configured");
        };
        let now = crate::auth::now_s();
        let q = query(&req);
        let flow = param(&q, "state").and_then(|state| {
            cookies_named(req.headers(), oidc::FLOW_COOKIE)
                .filter_map(|c| oidc::Flow::open(sessions, c, now))
                .find(|f| f.matches_state(state))
        });
        let Some(flow) = flow else {
            return sso_error(base, "invalid_state");
        };
        if let Some(e) = param(&q, "error") {
            // The IdP's OAuth error code (`access_denied`…): lowercase letters and `_` only.
            let code = if e == "access_denied" {
                "access_denied"
            } else {
                "idp_error"
            };
            return sso_error(base, code);
        }
        let Some(code) = param(&q, "code").filter(|c| !c.is_empty()) else {
            return sso_error(base, "invalid_state");
        };
        let Some((issuer, client, _, _)) = sso_client(auth, &req) else {
            return sso_error(base, "not_configured");
        };
        if client.client_id != flow.client {
            return sso_error(base, "invalid_state");
        }
        // RFC 9207: if the issuer sends `iss` with the callback, it must be our
        // issuer (against issuer mix-up). Absent is fine — not required.
        if param(&q, "iss").is_some_and(|iss| iss != issuer) {
            return sso_error(base, "token_rejected");
        }
        // The registry is not confirmed fresh ⇒ the client may have been removed on another node.
        if auth.registry.as_ref().is_some_and(|r| !r.fresh()) {
            return sso_error(base, "unavailable");
        }
        let provider = match rp.provider(&issuer).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error = %e, "cannot fetch the OIDC issuer's discovery");
                return sso_error(base, "issuer_unavailable");
            }
        };
        // Single-use flow: a `state` that already passed the callback on this
        // node ⇒ refused **before** calling the issuer. Then the node's
        // `exchange` cap; when out of slots, return the `state` so the user can
        // retry the callback.
        match limits.claim_state(&flow.state, flow.exp, now) {
            Claim::Fresh => {}
            Claim::Replayed => return sso_error(base, "invalid_state"),
            Claim::Full => {
                return too_many_because(
                    SSO_START_EVERY,
                    "node is holding too many SSO login flows",
                )
            }
        }
        // The client's cap first (flooding one tenant only costs that tenant's
        // slots), then the node cap (many clients flooded at once). The client's cap
        // has two lanes: an IP with a ticket ([`SSO_TRUST_BURST`]) uses the clean
        // one, so junk codes from a few IPs cannot empty the lane real users log in
        // through. Only a fresh, valid flow gets this far, so tickets cannot be
        // burnt without one. Refused here ⇒ nothing was exchanged: every slot goes back.
        let at = tokio::time::Instant::now();
        *ticket = limits.sso_trust.take(&ip, at).is_ok();
        let lane = if *ticket {
            &limits.exchange_by_client
        } else {
            &limits.exchange_suspect_by_client
        };
        let give_back = |ticket: &mut bool| {
            limits.unclaim_state(&flow.state);
            if std::mem::take(ticket) {
                limits.sso_trust.refund(&ip, at);
            }
        };
        if let Err(wait) = lane.take(&client.client_id, at) {
            give_back(ticket);
            return too_many_because(wait, "this SSO client is exchanging too many codes");
        }
        if let Err(wait) = limits.exchange.take(&(), at) {
            give_back(ticket);
            lane.refund(&client.client_id, at);
            return too_many_because(wait, "node is exchanging too many SSO codes");
        }
        let token = match rp.exchange(&provider, &client.client_id, code, &flow).await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(error = %e, "exchanging the OIDC code for a token failed");
                return sso_error(base, "token_rejected");
            }
        };
        let keys = rp.keys_for(&provider, &token).await;
        let verified = match oidc::verify_id_token(
            &token,
            &keys,
            &oidc::Expect {
                issuer: &issuer,
                client_id: &client.client_id,
                nonce: &flow.nonce,
                tenant: &client.tenant,
                max_role: client.max_role,
                now_s: crate::auth::now_s(),
            },
        ) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(reason = e.code(), client = %client.client_id, "id_token rejected");
                return sso_error(base, "token_rejected");
            }
        };
        let now = crate::auth::now_s();
        let book = auth.keys.oidc();
        if book
            .revoked_before(&client.client_id, &verified.sub)
            .is_some_and(|b| now.saturating_mul(1000) < b)
        {
            return sso_error(base, "revoked");
        }
        let ttl_s = book.with(|c, _| c.session_ttl_ms) / 1000;
        // The intent before the session is granted: if it cannot be written ⇒
        // no login.
        let who = crate::auth::Identity {
            tenant: client.tenant.clone(),
            role: verified.role,
            key_id: None,
            user: Some(std::sync::Arc::new(oidc::SsoUser {
                sub: verified.sub.clone(),
                email: verified.email.clone(),
                name: verified.name.clone(),
                client: client.client_id.clone(),
            })),
        };
        let meta = crate::audit::meta_of(auth, &req);
        let intent = match crate::audit::console_intent(
            audit,
            &meta,
            &who,
            "session.login",
            serde_json::json!({ "via": "sso", "client": client.client_id }),
        )
        .await
        {
            Ok(i) => i,
            Err(e) => {
                tracing::warn!(error = %e, "audit: cannot write the SSO login intent — refusing");
                return sso_error(base, "unavailable");
            }
        };
        let value = oidc::mint_sso(
            sessions,
            &oidc::SsoClaims {
                v: 2,
                src: "oidc".into(),
                iss: issuer,
                sub: verified.sub,
                email: verified.email,
                name: verified.name,
                tenant: client.tenant.as_str().to_string(),
                role: verified.role.as_str().to_string(),
                client: client.client_id,
                iat: now,
                exp: now + ttl_s,
                sid: oidc::new_session_id(),
            },
        );
        let r = redirect(
            &flow.return_to,
            &[
                oidc::set_sso_cookie(&value, ttl_s),
                oidc::clear_flow_cookie(),
            ],
        );
        if let Some(i) = intent {
            i.done("ok", r.status().as_u16()).await;
        }
        r
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_path_rejects_api_routes_and_bad_syntax() {
        assert!(check_base_path("/ui").is_ok());
        assert!(check_base_path("/console/v1").is_ok());
        for bad in [
            "/invocations",
            "/admin/ui",
            "/query",
            "/",
            "ui",
            "/ui/",
            "/a/../b",
        ] {
            assert!(check_base_path(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn loopback_hosts() {
        for h in [
            "127.0.0.1",
            "127.1.2.3",
            "::1",
            "[::1]",
            "localhost",
            "LOCALHOST",
        ] {
            assert!(is_loopback(h), "{h}");
        }
        for h in ["0.0.0.0", "10.0.0.1", "example.com", "::"] {
            assert!(!is_loopback(h), "{h}");
        }
    }
}
