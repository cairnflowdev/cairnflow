// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Identifying people through OIDC: the cell is a relying party.
//!
//! - **Configuration** lives in the `auth/oidc` object (`cairnflow-oidc/1`, strict
//!   read): one issuer, a session TTL, and a list of clients — each client one hostname,
//!   one tenant, one role cap (`max_role`). No object ⇒ OIDC off, behaviour as before
//!   OIDC existed. Revocations live in `auth/revoked` (`cairnflow-revoked/1`, strict
//!   read): one line `client<TAB>sub<TAB>before_ms` each. Both objects are re-read on the
//!   same tick as the key registry ([`super::Keys::refresh`]), so they share the
//!   synchronous ACK path (`/internal/registry/apply`) and the same rule of closing when
//!   the registry is not fresh.
//! - **The tenant comes from the configuration, not from a claim**: a request to
//!   hostname H ⇒ H's client ⇒ the client's tenant. The `cairn_tenant` claim, if any, is
//!   only a confirmation. The role comes from `cairn_role`, capped by `max_role`
//!   ([`cap_role`]) — at login **and** at every cookie check.
//! - **Session cookie v2** (`__Host-cairn_ui`): JSON signed with its own key derived from
//!   the cluster secret. Every request checks that the client is still in `auth/oidc`,
//!   the request's hostname is the client's hostname, the tenant has not changed, the
//!   issuer has not changed, the session has not been revoked, and caps the role again
//!   by the current `max_role`. It never carries permissions beyond the tenant
//!   (`cluster-admin`, `provision`) even with a valid signature: anyone with the cluster
//!   secret can sign a cookie.
//! - **Time** is `cairn_store::entropy::now` (seconds), passed in as a parameter — no
//!   JWT library reads the clock by itself here.
//! - **Network**: discovery, JWKS and token go through [`crate::egress`] with the node's
//!   policy (the issuer is set by whoever has the `Provision` permission — an arbitrary
//!   URL). The last good copy is kept in RAM: a dead issuer only blocks new logins;
//!   existing sessions and API keys keep working.

use {
    super::{random, same, Identity, Role, Sessions, TENANT_CAPS},
    base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine},
    cairn_store::TenantId,
    serde::{Deserialize, Serialize},
    sha2::{Digest, Sha256},
    slatedb::object_store::{
        path::Path, Error as OsError, ObjectStore, ObjectStoreExt, PutMode, UpdateVersion,
    },
    std::{
        sync::{Arc, Mutex, RwLock},
        time::Duration,
    },
};

// ───────────────────────────────────────────────────────────── constants

/// Header of `auth/oidc`.
pub const OIDC_HEADER: &str = "cairnflow-oidc/1";
/// Header of `auth/revoked`.
pub const REVOKED_HEADER: &str = "cairnflow-revoked/1";
/// The SSO session cookie (v2). The `__Host-` prefix forces `Secure`, `Path=/`, no
/// `Domain` — a sibling environment on the parent domain cannot set it.
pub const SSO_COOKIE: &str = "__Host-cairn_ui";
/// The temporary cookie of the login flow: `state`, `nonce`, `code_verifier`.
/// `SameSite=Lax` is required — `Strict` is not sent when an IdP on another site
/// redirects back to the callback.
pub const FLOW_COOKIE: &str = "__Host-cairn_oidc";
/// The lifetime of the temporary cookie (seconds): the user has this long to log in at the IdP.
pub const FLOW_TTL_S: u64 = 600;
/// The default SSO session TTL (1 h).
pub const DEFAULT_SESSION_TTL_MS: u64 = 60 * 60 * 1000;
/// Floor and cap of the SSO session TTL. The cap is also the pruning mark of
/// `auth/revoked`: a line whose `before_ms + cap + skew` has passed has no live session
/// left to block, however the TTL has changed since.
pub const MIN_SESSION_TTL_MS: u64 = 60 * 1000;
pub const MAX_SESSION_TTL_MS: u64 = 24 * 60 * 60 * 1000;
/// Allowed clock skew (ms): the `exp`/`iat` of an `id_token`, and the revocation mark is
/// computed as the cell's time plus this — a node running fast cannot save a session
/// just minted.
pub const CLOCK_SKEW_MS: u64 = super::MAX_SKEW_S * 1000;
/// The furthest revocation mark accepted, counted from the cell's time.
pub const MAX_REVOKE_AHEAD_MS: u64 = MAX_SESSION_TTL_MS;
/// Cap on requests to the issuer (discovery, JWKS, token).
pub const ISSUER_TIMEOUT: Duration = Duration::from_secs(10);
/// Cap on the body of one issuer response.
const ISSUER_MAX_BODY: usize = 256 * 1024;
/// Discovery + JWKS older than this are fetched again at the next login (keeping the old
/// copy if the refetch fails).
const PROVIDER_MAX_AGE: Duration = Duration::from_secs(60 * 60);
/// The floor between two JWKS refetches because of an unknown `kid`, and between two
/// failed discovery attempts — a storm of requests does not become a storm of requests
/// to the issuer.
const REFETCH_FLOOR: Duration = Duration::from_secs(30);
const RETRY_FLOOR: Duration = Duration::from_secs(5);
/// Cap on the length of the user's display fields (cookie ≤ 4 KB).
const MAX_SUB: usize = 255;
const MAX_EMAIL: usize = 254;
const MAX_NAME: usize = 128;
const MAX_URL: usize = 2048;

// ───────────────────────────────────────────────────────────── configuration

/// One OIDC client — one environment (control plane: `client_id` = the environment id).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OidcClient {
    pub client_id: String,
    /// The hostname (lower case, no port) whose requests belong to this client.
    pub hostname: String,
    pub tenant: TenantId,
    /// The highest role a session of this client can have — always a tenant role.
    pub max_role: Role,
    /// The console's "switch environment" link.
    pub portal_url: Option<String>,
}

/// `auth/oidc`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OidcConfig {
    /// `None` ⇒ no issuer set yet: SSO login is not possible.
    pub issuer: Option<String>,
    pub session_ttl_ms: u64,
    /// The IdP's name on the login button ("Sign in with …"); `None` ⇒ "SSO".
    pub display_name: Option<String>,
    pub clients: Vec<OidcClient>,
}

impl Default for OidcConfig {
    fn default() -> Self {
        Self {
            issuer: None,
            session_ttl_ms: DEFAULT_SESSION_TTL_MS,
            display_name: None,
            clients: Vec::new(),
        }
    }
}

impl OidcConfig {
    pub fn client(&self, id: &str) -> Option<&OidcClient> {
        self.clients.iter().find(|c| c.client_id == id)
    }

    pub fn client_for_host(&self, host: &str) -> Option<&OidcClient> {
        self.clients.iter().find(|c| c.hostname == host)
    }
}

/// One line of `auth/revoked`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Revocation {
    pub client: String,
    pub sub: String,
    /// Sessions with an `iat` (ms) before this mark are refused.
    pub before_ms: u64,
}

/// The tenant roles (`invoke`, `view`, `admin`) — the only roles an OIDC client can be
/// granted.
pub fn is_tenant_role(r: Role) -> bool {
    r.caps() != super::Caps::NONE && TENANT_CAPS.covers(r.caps())
}

/// The role `claimed` capped by `max`: intersect the two permission sets, then find the
/// tenant role with exactly that set. `cluster-admin` capped by `admin` is `admin`;
/// `provision` (no tenant permissions) ⇒ `None`; `view ∩ invoke` is empty ⇒ `None`.
/// `max` not a tenant role ⇒ `None`.
pub fn cap_role(claimed: Role, max: Role) -> Option<Role> {
    if !is_tenant_role(max) {
        return None;
    }
    let mine = claimed.caps();
    [Role::Admin, Role::View, Role::Invoke]
        .into_iter()
        .find(|r| max.caps().covers(r.caps()) && mine.covers(r.caps()))
}

pub fn check_client_id(s: &str) -> Result<(), String> {
    if s.is_empty()
        || s.len() > 128
        || !s
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._~-".contains(&b))
    {
        return Err(format!(
            "clientId {s:?}: 1..=128 characters from A-Z a-z 0-9 . _ ~ -"
        ));
    }
    Ok(())
}

/// A client's hostname: lower case, no port — a DNS name, IPv4, or IPv6 in square
/// brackets.
pub fn check_hostname(s: &str) -> Result<(), String> {
    let ok = !s.is_empty()
        && s.len() <= 253
        && normalize_host(s).as_deref() == Some(s)
        && if let Some(v6) = s.strip_prefix('[') {
            v6.strip_suffix(']')
                .is_some_and(|a| a.parse::<std::net::Ipv6Addr>().is_ok())
        } else {
            s.bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
        };
    if !ok {
        return Err(format!(
            "hostname {s:?}: lowercase domain name (or IP), no port, no scheme"
        ));
    }
    Ok(())
}

/// The hostname of a `Host` value: lower case, port dropped. `None` when the value does
/// not look like a host.
pub fn normalize_host(host: &str) -> Option<String> {
    let host = host.trim().to_ascii_lowercase();
    let name = if host.starts_with('[') {
        let end = host.find(']')?;
        let rest = &host[end + 1..];
        if !(rest.is_empty() || rest.strip_prefix(':').is_some_and(all_digits)) {
            return None;
        }
        host[..=end].to_string()
    } else {
        match host.rsplit_once(':') {
            Some((h, port)) if all_digits(port) => h.to_string(),
            Some(_) => return None,
            None => host,
        }
    };
    let name = name.trim_end_matches('.').to_string();
    (!name.is_empty() && !name.contains(['/', '@', ' ', '\\'])).then_some(name)
}

fn all_digits(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

/// An absolute `http(s)` URL without userinfo or fragment — issuer, portal link.
fn check_http_url(what: &str, s: &str, query_ok: bool) -> Result<url::Url, String> {
    let bad = |why: &str| Err(format!("{what} {s:?}: {why}"));
    if s.len() > MAX_URL {
        return bad("too long");
    }
    // `Url::parse` silently drops TAB/newline, while the **original** string is what gets
    // stored: a `\n` slipping through would write an extra line into `auth/oidc`
    // (security review). Printable ASCII only, no whitespace — a valid URL encodes
    // everything else as `%xx`.
    if !s.bytes().all(|b| b.is_ascii_graphic()) {
        return bad("only printable ASCII, no whitespace (encode as %xx)");
    }
    let Ok(u) = url::Url::parse(s) else {
        return bad("not an absolute URL");
    };
    if !matches!(u.scheme(), "http" | "https") {
        return bad("only http:// or https://");
    }
    if u.host_str().is_none_or(str::is_empty) {
        return bad("missing host");
    }
    if !u.username().is_empty() || u.password().is_some() {
        return bad("must not carry a user/password");
    }
    if u.fragment().is_some() || (!query_ok && u.query().is_some()) {
        return bad("must not carry a query or fragment");
    }
    Ok(u)
}

/// Issuer: an absolute `http(s)` URL, no query/fragment. Compared **verbatim** with the
/// discovery's `issuer` and the `iss` claim.
pub fn check_issuer(s: &str) -> Result<(), String> {
    check_http_url("issuer", s, false).map(|_| ())
}

/// Portal link: absolute `http(s)` — the console shows it as a link, so any other scheme
/// (`javascript:`) is XSS.
pub fn check_portal_url(s: &str) -> Result<(), String> {
    check_http_url("portalUrl", s, true).map(|_| ())
}

/// The IdP name on the login button: 1..=40 characters, no control characters, no
/// leading/trailing whitespace (React shows it as text — no HTML passes through).
pub fn check_display_name(s: &str) -> Result<(), String> {
    let n = s.chars().count();
    if n == 0 || n > 40 || s.trim() != s || s.chars().any(char::is_control) {
        return Err("displayName: 1..=40 characters, no control characters".into());
    }
    Ok(())
}

pub fn check_session_ttl_ms(ms: u64) -> Result<(), String> {
    if !(MIN_SESSION_TTL_MS..=MAX_SESSION_TTL_MS).contains(&ms) {
        return Err(format!(
            "sessionTtlMs must be within {MIN_SESSION_TTL_MS}..={MAX_SESSION_TTL_MS}"
        ));
    }
    Ok(())
}

/// A person's `sub`: 1..=255 bytes, no control characters (the lines of `auth/revoked`
/// are split by TAB).
pub fn check_sub(s: &str) -> Result<(), String> {
    if s.is_empty() || s.len() > MAX_SUB || s.chars().any(char::is_control) {
        return Err("sub: 1..=255 bytes, no control characters".into());
    }
    Ok(())
}

/// A client's role cap.
pub fn check_max_role(s: &str) -> Result<Role, String> {
    Role::parse(s)
        .filter(|r| is_tenant_role(*r))
        .ok_or_else(|| format!("maxRole {s:?}: only `admin`, `view` or `invoke`"))
}

/// Checks a whole client (the syntax of every field).
pub fn check_client(c: &OidcClient) -> Result<(), String> {
    check_client_id(&c.client_id)?;
    check_hostname(&c.hostname)?;
    if !is_tenant_role(c.max_role) {
        return Err(format!(
            "maxRole {:?} is not a tenant role",
            c.max_role.as_str()
        ));
    }
    if let Some(p) = &c.portal_url {
        check_portal_url(p)?;
    }
    Ok(())
}

/// `auth/oidc`:
///
/// ```text
/// cairnflow-oidc/1
/// issuer <url>
/// session_ttl_ms <n>
/// display_name <name, x-www-form-urlencoded>
/// client <client_id> <hostname> <tenant> <max_role> <portal_url | ->
/// ```
///
/// No field contains whitespace (checked at the API), so fields are split by spaces.
pub fn encode_config(c: &OidcConfig) -> Vec<u8> {
    let mut out = format!("{OIDC_HEADER}\n");
    if let Some(i) = &c.issuer {
        out.push_str(&format!("issuer {i}\n"));
    }
    out.push_str(&format!("session_ttl_ms {}\n", c.session_ttl_ms));
    if let Some(n) = &c.display_name {
        let enc: String = form_urlencoded::byte_serialize(n.as_bytes()).collect();
        out.push_str(&format!("display_name {enc}\n"));
    }
    for cl in &c.clients {
        out.push_str(&format!(
            "client {} {} {} {} {}\n",
            cl.client_id,
            cl.hostname,
            cl.tenant,
            cl.max_role.as_str(),
            cl.portal_url.as_deref().unwrap_or("-")
        ));
    }
    out.into_bytes()
}

/// Strict decoding: an unknown line, a broken field, clients with a duplicate id or
/// hostname ⇒ the whole object is broken (a misread client is a wrong tenant).
pub fn decode_config(raw: &[u8]) -> Result<OidcConfig, String> {
    let text = std::str::from_utf8(raw).map_err(|e| format!("auth/oidc is not UTF-8: {e}"))?;
    let mut lines = text.lines();
    let header = lines.next().unwrap_or_default();
    if header != OIDC_HEADER {
        if let Some(e) = cairn_store::newer_header("auth/oidc", OIDC_HEADER, header) {
            return Err(e.to_string());
        }
        return Err(format!("auth/oidc is missing header {OIDC_HEADER}"));
    }
    let mut cfg = OidcConfig::default();
    let mut ttl_seen = false;
    for line in lines.filter(|l| !l.trim().is_empty()) {
        let bad = |why: String| format!("auth/oidc: line {line:?}: {why}");
        let f: Vec<&str> = line.split(' ').collect();
        match f.as_slice() {
            ["issuer", url] if cfg.issuer.is_none() => {
                check_issuer(url).map_err(bad)?;
                cfg.issuer = Some((*url).to_string());
            }
            ["session_ttl_ms", n] if !ttl_seen => {
                let ms: u64 = n.parse().map_err(|_| bad("not a number".into()))?;
                check_session_ttl_ms(ms).map_err(bad)?;
                cfg.session_ttl_ms = ms;
                ttl_seen = true;
            }
            ["display_name", enc] if cfg.display_name.is_none() => {
                let name = form_urlencoded::parse(format!("n={enc}").as_bytes())
                    .next()
                    .map(|(_, v)| v.into_owned())
                    .unwrap_or_default();
                check_display_name(&name).map_err(bad)?;
                cfg.display_name = Some(name);
            }
            ["client", id, host, tenant, role, portal] => {
                let c = OidcClient {
                    client_id: (*id).to_string(),
                    hostname: (*host).to_string(),
                    tenant: TenantId::new(tenant).map_err(|e| bad(e.to_string()))?,
                    max_role: check_max_role(role).map_err(bad)?,
                    portal_url: (*portal != "-").then(|| (*portal).to_string()),
                };
                check_client(&c).map_err(bad)?;
                if cfg.client(id).is_some() || cfg.client_for_host(host).is_some() {
                    return Err(bad("duplicate client id or hostname".into()));
                }
                cfg.clients.push(c);
            }
            _ => return Err(bad("unknown line".into())),
        }
    }
    Ok(cfg)
}

/// `auth/revoked`: the header, then one line `client<TAB>sub<TAB>before_ms` each.
pub fn encode_revoked(r: &[Revocation]) -> Vec<u8> {
    let mut out = format!("{REVOKED_HEADER}\n");
    for x in r {
        out.push_str(&format!("{}\t{}\t{}\n", x.client, x.sub, x.before_ms));
    }
    out.into_bytes()
}

pub fn decode_revoked(raw: &[u8]) -> Result<Vec<Revocation>, String> {
    let text = std::str::from_utf8(raw).map_err(|e| format!("auth/revoked is not UTF-8: {e}"))?;
    let mut lines = text.lines();
    let header = lines.next().unwrap_or_default();
    if header != REVOKED_HEADER {
        if let Some(e) = cairn_store::newer_header("auth/revoked", REVOKED_HEADER, header) {
            return Err(e.to_string());
        }
        return Err(format!("auth/revoked is missing header {REVOKED_HEADER}"));
    }
    let mut out = Vec::new();
    for line in lines.filter(|l| !l.is_empty()) {
        let bad = || format!("auth/revoked: malformed line {line:?}");
        let f: Vec<&str> = line.split('\t').collect();
        let [client, sub, before] = f.as_slice() else {
            return Err(bad());
        };
        check_client_id(client).map_err(|_| bad())?;
        check_sub(sub).map_err(|_| bad())?;
        out.push(Revocation {
            client: (*client).to_string(),
            sub: (*sub).to_string(),
            before_ms: before.parse().map_err(|_| bad())?,
        });
    }
    Ok(out)
}

/// Adds a revocation `r` (with the mark the caller gives) by `(client, sub)`:
///
/// - a line that already has a mark ≥ `r.before_ms` ⇒ unchanged — repeated sends with
///   the same mark (the control plane retries until it gets an ACK) do not slide the
///   mark, so a session the user logged into again after the first attempt is not
///   killed by a later attempt;
/// - none yet, or lower ⇒ store `max(r.before_ms, now_ms + CLOCK_SKEW_MS)`: a slow
///   caller clock cannot save a session just minted on the cell.
///
/// Prunes lines with no live session left to block ([`MAX_SESSION_TTL_MS`]). Returns
/// `true` when the registry changed.
pub fn merge_revocation(book: &mut Vec<Revocation>, r: Revocation, now_ms: u64) -> bool {
    let floor = now_ms + CLOCK_SKEW_MS;
    let before = book.len();
    book.retain(|x| x.before_ms + MAX_SESSION_TTL_MS + CLOCK_SKEW_MS >= now_ms);
    let mut changed = book.len() != before;
    match book
        .iter_mut()
        .find(|x| x.client == r.client && x.sub == r.sub)
    {
        Some(x) if x.before_ms >= r.before_ms => {}
        Some(x) => {
            x.before_ms = r.before_ms.max(floor);
            changed = true;
        }
        None => {
            book.push(Revocation {
                before_ms: r.before_ms.max(floor),
                ..r
            });
            changed = true;
        }
    }
    changed
}

// ───────────────────────────────────────────────────────────── registry in the bucket

/// The cluster's `auth/oidc` + `auth/revoked`: objects in the bucket + a copy in RAM to
/// check every request. Refreshed on the same tick as the key registry
/// ([`super::Keys::refresh`]).
#[derive(Clone)]
pub struct OidcBook(Arc<BookInner>);

struct BookInner {
    /// (store, `auth/oidc`, `auth/revoked`). `None` = RAM only (tests).
    store: Option<(Arc<dyn ObjectStore>, Path, Path)>,
    /// `auth/revoked-sessions`: logged-out session ids → the cookie's expiry mark and
    /// tenant. `None` = RAM only.
    sessions_path: Option<Path>,
    sessions: RwLock<SessionBook>,
    sessions_order: crate::registry_sync::ReadOrder,
    /// The last time the session registry in RAM was confirmed to match the bucket (a
    /// successful read or write) — its own freshness, so a read error of this registry
    /// does not spoil the freshness of the key registry. `None` = never.
    sessions_seen: Mutex<Option<tokio::time::Instant>>,
    /// The age cap of the session registry: over the cap ⇒ every cookie session is
    /// treated as revoked (closed when unsure, like keys — but only cookie sessions).
    /// `None` = no cap (tests, RAM-only registry).
    sessions_limit: Mutex<Option<Duration>>,
    /// Closed because the session registry is stale — to log once per transition.
    sessions_closed: std::sync::atomic::AtomicBool,
    /// The latest read of the session registry failed: the refresh loop runs a full
    /// round on the next tick and does not confirm the session registry by generation.
    sessions_failed: std::sync::atomic::AtomicBool,
    /// One session registry write at a time on the node: concurrent logouts on the same
    /// node do not fight each other over the CAS.
    sessions_write: tokio::sync::Mutex<()>,
    /// `sid`s refused because the tenant is at its cap → cookie expiry, capped at
    /// [`MAX_REVOKE_REFUSED`] entries.
    refused: Mutex<std::collections::BTreeMap<String, u64>>,
    cache: RwLock<(OidcConfig, Vec<Revocation>)>,
    /// Like `Keys::order`: applied in read order — an old read of the refresh loop
    /// arriving late does not overwrite the copy that an apply request (removing a
    /// client, revoking a session) just read.
    order: crate::registry_sync::ReadOrder,
}

/// Which object of the registry.
#[derive(Clone, Copy)]
enum Which {
    Config,
    Revoked,
}

impl OidcBook {
    pub fn new(store: Option<(Arc<dyn ObjectStore>, &Path)>) -> Self {
        let sessions_path = store
            .as_ref()
            .map(|(_, p)| super::object_path(p, REVOKED_SESSIONS_OBJECT));
        Self(Arc::new(BookInner {
            sessions_path,
            sessions: RwLock::new(Default::default()),
            sessions_order: crate::registry_sync::ReadOrder::default(),
            sessions_seen: Mutex::new(None),
            sessions_limit: Mutex::new(None),
            sessions_closed: std::sync::atomic::AtomicBool::new(false),
            sessions_failed: std::sync::atomic::AtomicBool::new(false),
            sessions_write: tokio::sync::Mutex::new(()),
            refused: Mutex::new(Default::default()),
            store: store.map(|(s, p)| {
                (
                    s,
                    super::object_path(p, "auth/oidc"),
                    super::object_path(p, "auth/revoked"),
                )
            }),
            cache: RwLock::new(Default::default()),
            order: crate::registry_sync::ReadOrder::default(),
        }))
    }

    /// The current configuration in RAM.
    pub fn config(&self) -> OidcConfig {
        self.0.cache.read().map(|c| c.0.clone()).unwrap_or_default()
    }

    /// Runs `f` on the current configuration and revocations **under the read lock**,
    /// without copying: the per-request path uses it, not [`Self::config`]/
    /// [`Self::revoked`] (which copy the whole `Vec`).
    pub fn with<R>(&self, f: impl FnOnce(&OidcConfig, &[Revocation]) -> R) -> R {
        match self.0.cache.read() {
            Ok(c) => f(&c.0, &c.1),
            Err(p) => {
                let c = p.into_inner();
                f(&c.0, &c.1)
            }
        }
    }

    /// The current revocations in RAM.
    pub fn revoked(&self) -> Vec<Revocation> {
        self.0.cache.read().map(|c| c.1.clone()).unwrap_or_default()
    }

    /// The client of hostname `host` when OIDC is on (has an issuer), with the issuer.
    pub fn login_client(&self, host: &str) -> Option<(String, OidcClient)> {
        let c = self.0.cache.read().ok()?;
        let issuer = c.0.issuer.clone()?;
        c.0.client_for_host(host).cloned().map(|cl| (issuer, cl))
    }

    /// The revocation mark of `(client, sub)` (ms), if any.
    pub fn revoked_before(&self, client: &str, sub: &str) -> Option<u64> {
        let c = self.0.cache.read().ok()?;
        c.1.iter()
            .find(|r| r.client == client && r.sub == sub)
            .map(|r| r.before_ms)
    }

    fn path(&self, which: Which) -> Option<(&Arc<dyn ObjectStore>, &Path)> {
        self.0.store.as_ref().map(|(s, c, r)| {
            (
                s,
                match which {
                    Which::Config => c,
                    Which::Revoked => r,
                },
            )
        })
    }

    async fn load_raw(
        &self,
        which: Which,
    ) -> Result<Option<(bytes::Bytes, UpdateVersion)>, String> {
        let Some((store, path)) = self.path(which) else {
            return Ok(None);
        };
        match store.get(path).await {
            Ok(got) => {
                let v = UpdateVersion {
                    e_tag: got.meta.e_tag.clone(),
                    version: got.meta.version.clone(),
                };
                let raw = got.bytes().await.map_err(|e| e.to_string())?;
                Ok(Some((raw, v)))
            }
            Err(OsError::NotFound { .. }) => Ok(None),
            Err(e) => Err(format!("cannot read {path}: {e}")),
        }
    }

    async fn load(&self) -> Result<(OidcConfig, Vec<Revocation>), String> {
        if self.0.store.is_none() {
            return Ok(self.0.cache.read().map(|c| c.clone()).unwrap_or_default());
        }
        let config = match self.load_raw(Which::Config).await? {
            Some((raw, _)) => decode_config(&raw)?,
            None => OidcConfig::default(),
        };
        let revoked = match self.load_raw(Which::Revoked).await? {
            Some((raw, _)) => decode_revoked(&raw)?,
            None => Vec::new(),
        };
        Ok((config, revoked))
    }

    /// Re-reads all three objects (configuration, per-person revocations, per-session
    /// revocations). A broken `auth/oidc` or `auth/revoked` (read error, corrupt object)
    /// ⇒ `Err`, the cache keeps the last good copy — and the caller does not confirm the
    /// registry as fresh, so after the cap the sessions relying on it are closed like
    /// keys.
    ///
    /// A broken `auth/revoked-sessions` does **not**: that object is written by every
    /// tenant's logouts and must not make every key `503`. The last good copy is kept,
    /// and its own freshness ([`Self::sessions_stale`]) closes only cookie sessions after
    /// the cap.
    pub async fn refresh(&self) -> Result<(), String> {
        let ticket = self.0.order.ticket();
        let fresh = self.load().await?;
        self.apply_read(ticket, fresh);
        self.refresh_sessions().await;
        Ok(())
    }

    /// Re-reads the logged-out session registry; an error is only logged.
    async fn refresh_sessions(&self) {
        let ticket = self.0.sessions_order.ticket();
        let at = tokio::time::Instant::now();
        match self.load_sessions().await {
            Ok(loaded) => {
                self.apply_sessions(ticket, loaded.map(|(s, _)| s).unwrap_or_default());
                self.sessions_confirm(at);
                self.0
                    .sessions_failed
                    .store(false, std::sync::atomic::Ordering::Relaxed);
            }
            Err(e) => {
                self.0
                    .sessions_failed
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                tracing::debug!(error = %e, "cannot re-read {REVOKED_SESSIONS_OBJECT} — keeping the last good copy")
            }
        }
    }

    /// The latest read of the session registry failed — the refresh loop forces a full
    /// round on the next tick, and does not confirm the session registry by generation.
    pub fn sessions_read_failed(&self) -> bool {
        self.0
            .sessions_failed
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The session registry in RAM matches the bucket at `at` because the registry
    /// generation has not changed since the latest full read (the refresh loop) — every
    /// session registry write goes through `RegistrySync::write` (logout). A failed
    /// latest read ⇒ not confirmed.
    pub fn confirm_sessions_unchanged(&self, at: tokio::time::Instant) {
        if !self.sessions_read_failed() {
            self.sessions_confirm(at);
        }
    }

    /// Sets the age cap of the session registry — the node sets the same cap as the key
    /// registry's freshness. Like `RegistryGen::set_fresh_limit`: the read at boot counts
    /// as fresh at the time it is set (building engines can take longer than the cap);
    /// if it was never read, it stays closed until the first read.
    pub fn set_sessions_fresh_limit(&self, limit: Duration) {
        if let Ok(mut l) = self.0.sessions_limit.lock() {
            *l = Some(limit);
        }
        if let Ok(mut seen) = self.0.sessions_seen.lock() {
            if seen.is_some() {
                *seen = Some(tokio::time::Instant::now());
            }
        }
    }

    fn sessions_confirm(&self, at: tokio::time::Instant) {
        if let Ok(mut seen) = self.0.sessions_seen.lock() {
            if seen.is_none_or(|s| at > s) {
                *seen = Some(at);
            }
        }
    }

    /// The session registry in RAM is over its age cap: a cookie session may have been
    /// revoked on another node ⇒ the caller closes requests authenticated by cookie
    /// (`503` like a stale key registry — `Auth::stale_via`). No cap ⇒ never.
    pub fn sessions_stale(&self) -> bool {
        let Some(limit) = self.0.sessions_limit.lock().ok().and_then(|l| *l) else {
            return false;
        };
        let age = self
            .0
            .sessions_seen
            .lock()
            .ok()
            .and_then(|s| *s)
            .map(|s| s.elapsed());
        let stale = age.is_none_or(|a| a >= limit);
        if self
            .0
            .sessions_closed
            .swap(stale, std::sync::atomic::Ordering::Relaxed)
            != stale
        {
            if stale {
                tracing::warn!(
                    limit_ms = limit.as_millis() as u64,
                    "cannot confirm {REVOKED_SESSIONS_OBJECT} is current — refusing console session cookies (API keys keep working) until it is read again"
                );
            } else {
                tracing::warn!("{REVOKED_SESSIONS_OBJECT} confirmed current again — console session cookies accepted");
            }
        }
        stale
    }

    /// Session `sid` has logged out — per the copy in RAM. The registry's freshness is not
    /// considered here but in [`Self::sessions_stale`].
    pub fn session_revoked(&self, sid: &str) -> bool {
        self.session_listed(sid)
    }

    /// Whether `sid` is in the RAM copy of the registry (freshness not considered).
    pub fn session_listed(&self, sid: &str) -> bool {
        self.0
            .sessions
            .read()
            .map(|s| s.contains_key(sid))
            .unwrap_or(true)
    }

    /// Records session `sid` of `tenant` (its cookie expiring at `exp_s`) in
    /// `auth/revoked-sessions` with a CAS. Expired cookie lines are pruned at every write
    /// — the registry cleans itself. Ambiguity is resolved by reading back: the new
    /// content carries this session's random `sid`, so it is distinctive enough.
    ///
    /// A session already expired or already in the registry ⇒ done, no write; each tenant
    /// keeps at most [`MAX_REVOKED_SESSIONS_PER_TENANT`] unexpired sessions (no shared
    /// cap) — over ⇒ [`RevokeError::Full`], no write, and the `sid` is remembered so a
    /// repeated logout does no I/O ([`Self::revoke_refused`]). One write at a time on the
    /// node; losing the CAS (another node) ⇒ back off and retry, at most
    /// [`REVOKE_ATTEMPTS`] times within [`REVOKE_BUDGET`].
    pub async fn revoke_session(
        &self,
        sid: &str,
        tenant: &str,
        exp_s: u64,
        now_s: u64,
    ) -> Result<(), RevokeError> {
        if sid.is_empty() || !sid.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(RevokeError::Failed("malformed session id".into()));
        }
        if expired(exp_s, now_s) {
            return Ok(());
        }
        let _one = self.0.sessions_write.lock().await;
        let deadline = tokio::time::Instant::now() + REVOKE_BUDGET;
        let mut backoff =
            cairn_store::pace::Backoff::new(Duration::from_millis(10), Duration::from_millis(250));
        for attempt in 0..REVOKE_ATTEMPTS {
            if attempt > 0 {
                if tokio::time::Instant::now() >= deadline {
                    break;
                }
                backoff.wait().await;
            }
            let ticket = self.0.sessions_order.ticket();
            let at = tokio::time::Instant::now();
            let loaded = self.load_sessions().await.map_err(RevokeError::Failed)?;
            let (mut book, version) = match loaded {
                Some((b, v)) => (b, Some(v)),
                None => (Default::default(), None),
            };
            if book.contains_key(sid) {
                self.apply_sessions(ticket, book);
                self.sessions_confirm(at);
                return Ok(());
            }
            book.retain(|_, r| !expired(r.exp_s, now_s));
            // Only a per-tenant cap: no shared cap that a few tenants could fill to break
            // other tenants' logouts. The registry size ≤ the tenants with console
            // sessions × the tenant cap, and lines fall off with the cookie expiry.
            let of_tenant = book.values().filter(|r| r.tenant == tenant).count();
            if of_tenant >= MAX_REVOKED_SESSIONS_PER_TENANT {
                self.remember_refused(sid, exp_s, now_s);
                return Err(RevokeError::Full);
            }
            book.insert(
                sid.to_string(),
                RevokedSession {
                    exp_s,
                    tenant: tenant.to_string(),
                },
            );
            let Some((store, path)) = self
                .0
                .store
                .as_ref()
                .map(|(s, ..)| s)
                .zip(self.0.sessions_path.as_ref())
            else {
                self.apply_sessions(self.0.sessions_order.ticket(), book);
                return Ok(());
            };
            let mode = match version {
                Some(v) => PutMode::Update(v),
                None => PutMode::Create,
            };
            match cairn_store::cas::put_resolving_or_overwrite(
                store,
                path,
                encode_revoked_sessions(&book).into(),
                mode,
            )
            .await
            {
                Ok(cairn_store::cas::Cas::Written(_)) => {
                    self.apply_sessions(self.0.sessions_order.ticket(), book);
                    self.sessions_confirm(at);
                    return Ok(());
                }
                Ok(cairn_store::cas::Cas::Lost) => continue,
                Err(e) => return Err(RevokeError::Failed(format!("cannot write {path}: {e}"))),
            }
        }
        Err(RevokeError::Failed(format!(
            "{REVOKED_SESSIONS_OBJECT} kept changing underneath — retry"
        )))
    }

    /// `sid` was just refused because the tenant is at its cap and the cookie has not
    /// expired — a repeated logout with the same cookie does no I/O and does not bump the
    /// registry generation.
    pub fn revoke_refused(&self, sid: &str, now_s: u64) -> bool {
        self.0
            .refused
            .lock()
            .map(|r| r.get(sid).is_some_and(|exp| !expired(*exp, now_s)))
            .unwrap_or(false)
    }

    fn remember_refused(&self, sid: &str, exp_s: u64, now_s: u64) {
        if let Ok(mut r) = self.0.refused.lock() {
            if r.len() >= MAX_REVOKE_REFUSED {
                r.retain(|_, exp| !expired(*exp, now_s));
            }
            if r.len() < MAX_REVOKE_REFUSED {
                r.insert(sid.to_string(), exp_s);
            }
        }
    }

    /// The number of unexpired sessions of `tenant` in the RAM copy — so logout refuses
    /// early, with no I/O, when the tenant is already at its cap.
    pub fn revoked_sessions_of(&self, tenant: &str, now_s: u64) -> usize {
        self.0
            .sessions
            .read()
            .map(|s| {
                s.values()
                    .filter(|r| r.tenant == tenant && !expired(r.exp_s, now_s))
                    .count()
            })
            .unwrap_or(0)
    }

    async fn load_sessions(&self) -> Result<Option<(SessionBook, UpdateVersion)>, String> {
        let (Some((store, ..)), Some(path)) =
            (self.0.store.as_ref(), self.0.sessions_path.as_ref())
        else {
            let book = self
                .0
                .sessions
                .read()
                .map(|s| s.clone())
                .unwrap_or_default();
            return Ok(Some((
                book,
                UpdateVersion {
                    e_tag: None,
                    version: None,
                },
            )));
        };
        match store.get(path).await {
            Ok(got) => {
                let v = UpdateVersion {
                    e_tag: got.meta.e_tag.clone(),
                    version: got.meta.version.clone(),
                };
                let raw = got.bytes().await.map_err(|e| e.to_string())?;
                Ok(Some((decode_revoked_sessions(&raw)?, v)))
            }
            Err(OsError::NotFound { .. }) => Ok(None),
            Err(e) => Err(format!("cannot read {path}: {e}")),
        }
    }

    fn apply_sessions(&self, ticket: u64, book: SessionBook) {
        self.0.sessions_order.apply_if_newer(ticket, || {
            if let Ok(mut s) = self.0.sessions.write() {
                *s = book;
            }
        });
    }

    /// Writes `book` into the cache if its ticket is newer than the copy being applied.
    fn apply_read(&self, ticket: u64, book: (OidcConfig, Vec<Revocation>)) {
        self.0.order.apply_if_newer(ticket, || {
            if let Ok(mut c) = self.0.cache.write() {
                *c = book;
            }
        });
    }

    /// Reads the bucket directly (for `GET /admin/oidc`: the control plane reconciles
    /// against the real registry, not the cache).
    pub async fn read(&self) -> Result<(OidcConfig, Vec<Revocation>), String> {
        self.load().await
    }

    /// Read-modify-write of `auth/oidc` with a CAS, like the key registry. `edit` returns
    /// `Ok((result, changed))`; `Err` ⇒ no write, that error is returned.
    pub async fn update_config<T, E: From<String>>(
        &self,
        mut edit: impl FnMut(&mut OidcConfig) -> Result<(T, bool), E>,
    ) -> Result<T, E> {
        self.update(Which::Config, |c, _| edit(c)).await
    }

    /// Writes one revocation ([`merge_revocation`]).
    pub async fn revoke(&self, r: Revocation, now_ms: u64) -> Result<(), String> {
        self.update(Which::Revoked, |_, book| {
            Ok::<_, String>(((), merge_revocation(book, r.clone(), now_ms)))
        })
        .await
    }

    async fn update<T, E: From<String>>(
        &self,
        which: Which,
        mut edit: impl FnMut(&mut OidcConfig, &mut Vec<Revocation>) -> Result<(T, bool), E>,
    ) -> Result<T, E> {
        for _ in 0..8 {
            let (mut config, mut revoked) = self.load().await?;
            let version = match self.load_raw(which).await? {
                Some((raw, v)) => {
                    // The copy just read on its own may be newer than `load`'s — resolve again.
                    match which {
                        Which::Config => config = decode_config(&raw)?,
                        Which::Revoked => revoked = decode_revoked(&raw)?,
                    }
                    Some(v)
                }
                None => None,
            };
            let (out, changed) = edit(&mut config, &mut revoked)?;
            if !changed {
                return Ok(out);
            }
            if let Some((store, path)) = self.path(which) {
                let body = match which {
                    Which::Config => encode_config(&config),
                    Which::Revoked => encode_revoked(&revoked),
                };
                let mode = match version {
                    Some(v) => PutMode::Update(v),
                    None => PutMode::Create,
                };
                // Ambiguity resolved by reading back: identical content is our own
                // write. Two writers of an identical registry get the same result — the
                // write is idempotent by content. A store without CAS (`--data-dir`) ⇒
                // overwrite, like the key registry.
                match cairn_store::cas::put_resolving_or_overwrite(store, path, body.into(), mode)
                    .await
                {
                    Ok(cairn_store::cas::Cas::Written(_)) => {}
                    Ok(cairn_store::cas::Cas::Lost) => continue,
                    Err(e) => return Err(format!("cannot write {path}: {e}").into()),
                }
            }
            // The ticket **after** the write won (see `crate::registry_sync::ReadOrder`).
            let ticket = self.0.order.ticket();
            self.apply_read(ticket, (config, revoked));
            return Ok(out);
        }
        Err(String::from("auth/oidc kept changing underneath — retry").into())
    }
}

// ───────────────────────────────────────────────────────────── cookie v2

/// The user of an SSO session — an extension next to [`Identity`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SsoUser {
    pub sub: String,
    pub email: Option<String>,
    pub name: Option<String>,
    /// The session's client (environment).
    pub client: String,
}

/// The content of cookie v2 — adds `iss` so that changing the issuer kills every old
/// session (`sub` is only unique within one issuer).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SsoClaims {
    pub v: u8,
    pub src: String,
    pub iss: String,
    pub sub: String,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    pub tenant: String,
    pub role: String,
    pub client: String,
    pub iat: u64,
    pub exp: u64,
    /// A random session id: logout revokes exactly this session. Absent (a cookie minted
    /// before session ids existed) ⇒ refused.
    #[serde(default)]
    pub sid: String,
}

/// The object name of the per-session revocation registry.
pub const REVOKED_SESSIONS_OBJECT: &str = "auth/revoked-sessions";
/// Header of `auth/revoked-sessions`.
pub const REVOKED_SESSIONS_HEADER: &str = "cairnflow-revoked-sessions/1";

/// One line of `auth/revoked-sessions`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RevokedSession {
    /// The cookie's expiry mark (seconds).
    pub exp_s: u64,
    /// The session's tenant (the string form of `TenantId`) — for the per-tenant cap.
    /// Absent (lines written before the tenant column existed) = `""`.
    pub tenant: String,
}

/// The logged-out session registry: `sid` → line.
pub type SessionBook = std::collections::BTreeMap<String, RevokedSession>;

/// Cap on the **unexpired** logged-out sessions of one tenant in the registry. Logging
/// in correctly costs no slot: without a cap one key could mint and then log out any
/// number of sessions, and the registry — an object read on every full read round of
/// every node — would grow without bound. Lines fall off on their own when the cookie
/// expires (≤ 24 h). There is no shared cap: a shared cap would let a few tenants fill
/// it and break every other tenant's logout; the registry size ≤ tenants × 200 × ~70
/// bytes.
pub const MAX_REVOKED_SESSIONS_PER_TENANT: usize = 200;
/// Cap on the refused `sid`s remembered in the node's RAM.
const MAX_REVOKE_REFUSED: usize = 10_000;
/// Maximum attempts to write the session registry when losing the CAS.
pub const REVOKE_ATTEMPTS: usize = 32;
/// Time cap of one revocation when losing the CAS repeatedly.
pub const REVOKE_BUDGET: Duration = Duration::from_secs(5);

/// Why a session revocation could not be written.
#[derive(Debug)]
pub enum RevokeError {
    /// The registry is at the tenant's cap or the cluster's — nothing written.
    Full,
    /// Read/write failed, or the CAS was lost beyond the cap.
    Failed(String),
}

impl std::fmt::Display for RevokeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RevokeError::Full => f.write_str("too many signed-out sessions are still unexpired"),
            RevokeError::Failed(e) => f.write_str(e),
        }
    }
}

/// Whether a cookie expiring at `exp_s` is dead at `now_s` (plus clock skew).
fn expired(exp_s: u64, now_s: u64) -> bool {
    exp_s.saturating_add(super::MAX_SKEW_S) < now_s
}

/// `header\n`, then one line per session `sid\texp_s\ttenant`, sorted by `sid`
/// (deterministic).
pub fn encode_revoked_sessions(book: &SessionBook) -> Vec<u8> {
    let mut out = format!("{REVOKED_SESSIONS_HEADER}\n");
    for (sid, r) in book {
        out.push_str(&format!("{sid}\t{}\t{}\n", r.exp_s, r.tenant));
    }
    out.into_bytes()
}

/// The inverse of [`encode_revoked_sessions`] — **lenient read**: unknown lines are
/// skipped (a later binary adding fields does not make this binary close every
/// session), a missing third field ⇒ tenant `""`; a header of another family ⇒ error.
pub fn decode_revoked_sessions(raw: &[u8]) -> Result<SessionBook, String> {
    let text =
        std::str::from_utf8(raw).map_err(|_| "auth/revoked-sessions is not UTF-8".to_string())?;
    let mut lines = text.lines();
    if lines.next() != Some(REVOKED_SESSIONS_HEADER) {
        return Err("auth/revoked-sessions has an unknown header".into());
    }
    Ok(lines
        .filter_map(|l| {
            let mut f = l.split('\t');
            let sid = f.next()?;
            let exp_s = f.next()?.trim().parse::<u64>().ok()?;
            let tenant = f.next().unwrap_or_default().trim().to_string();
            Some((sid.to_string(), RevokedSession { exp_s, tenant }))
        })
        .collect())
}

/// A new session id: 16 random bytes, hex.
pub fn new_session_id() -> String {
    let mut b = [0u8; 16];
    cairn_store::entropy::fill(&mut b);
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// `(sid, exp, tenant)` of a correctly signed v2 cookie that **has not expired** at
/// `now_s` — for logout. Expired ⇒ `None`: nothing left to revoke, and recording it
/// only bloats the registry.
pub fn sso_session_id(s: &Sessions, value: &str, now_s: u64) -> Option<(String, u64, String)> {
    let payload = s.open(super::Seal::Sso, value)?;
    let c: SsoClaims = serde_json::from_slice(&payload).ok()?;
    (!c.sid.is_empty() && c.exp > now_s).then_some((c.sid, c.exp, c.tenant))
}

/// A new v2 cookie. Returns the value and the expiry mark (seconds).
pub fn mint_sso(s: &Sessions, c: &SsoClaims) -> String {
    let payload = serde_json::to_vec(c).expect("claims always encode");
    s.seal(super::Seal::Sso, &payload)
}

/// The identity of a v2 cookie at `now_s`, given the current registry and the request's
/// hostname. Every condition is in the module head doc.
pub fn verify_sso(
    s: &Sessions,
    value: &str,
    book: &OidcBook,
    host: Option<&str>,
    now_s: u64,
) -> Option<(Identity, u64)> {
    let payload = s.open(super::Seal::Sso, value)?;
    let c: SsoClaims = serde_json::from_slice(&payload).ok()?;
    if book.session_revoked(&c.sid) {
        return None;
    }
    book.with(|cfg, revoked| check_sso(&c, cfg, revoked, host, now_s))
        .map(|who| (who, c.exp))
}

/// The pure part of [`verify_sso`] — unit tests call it directly.
pub fn check_sso(
    c: &SsoClaims,
    cfg: &OidcConfig,
    revoked: &[Revocation],
    host: Option<&str>,
    now_s: u64,
) -> Option<Identity> {
    if c.v != 2 || c.src != "oidc" || c.exp <= now_s || c.iat > now_s + super::MAX_SKEW_S {
        return None;
    }
    if c.sid.is_empty() {
        return None;
    }
    if cfg.issuer.as_deref() != Some(c.iss.as_str()) {
        return None;
    }
    let client = cfg.client(&c.client)?;
    // A correctly signed cookie sent to another hostname ⇒ no: the request's tenant is
    // the hostname's tenant.
    if host != Some(client.hostname.as_str()) {
        return None;
    }
    if client.tenant.as_str() != c.tenant {
        return None;
    }
    // Never permissions beyond the tenant, even with a valid signature.
    let role = Role::parse(&c.role).filter(|r| is_tenant_role(*r))?;
    let role = cap_role(role, client.max_role)?;
    let iat_ms = c.iat.saturating_mul(1000);
    if revoked
        .iter()
        .any(|r| r.client == c.client && r.sub == c.sub && iat_ms < r.before_ms)
    {
        return None;
    }
    Some(Identity {
        tenant: client.tenant.clone(),
        role,
        key_id: None,
        user: Some(Arc::new(SsoUser {
            sub: c.sub.clone(),
            email: c.email.clone(),
            name: c.name.clone(),
            client: c.client.clone(),
        })),
    })
}

/// The `Set-Cookie` of an SSO session: always `Secure` (the `__Host-` prefix requires
/// it), `Strict` — only the callback needs `Lax`, and the callback reads the temporary
/// cookie, not this one.
pub fn set_sso_cookie(value: &str, ttl_s: u64) -> String {
    format!("{SSO_COOKIE}={value}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age={ttl_s}")
}

pub fn clear_sso_cookie() -> String {
    format!("{SSO_COOKIE}=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0")
}

// ───────────────────────────────────────────────────────────── login flow

/// The content of the login flow's temporary cookie.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Flow {
    pub v: u8,
    pub client: String,
    pub state: String,
    pub nonce: String,
    pub verifier: String,
    pub redirect_uri: String,
    pub return_to: String,
    pub exp: u64,
}

impl Flow {
    /// A new flow for `client`: random `state`, `nonce` and `code_verifier` (32 bytes
    /// each).
    pub fn new(client: &str, redirect_uri: String, return_to: String, now_s: u64) -> Self {
        let r = |_| URL_SAFE_NO_PAD.encode(random::<32>());
        Self {
            v: 1,
            client: client.to_string(),
            state: r(0),
            nonce: r(1),
            verifier: r(2),
            redirect_uri,
            return_to,
            exp: now_s + FLOW_TTL_S,
        }
    }

    /// The S256 `code_challenge` of `verifier` (RFC 7636).
    pub fn challenge(&self) -> String {
        URL_SAFE_NO_PAD.encode(Sha256::digest(self.verifier.as_bytes()))
    }

    pub fn seal(&self, s: &Sessions) -> String {
        s.seal(
            super::Seal::Flow,
            &serde_json::to_vec(self).expect("flow always encodes"),
        )
    }

    /// The flow of a cookie value: the signature is right, not expired.
    pub fn open(s: &Sessions, value: &str, now_s: u64) -> Option<Self> {
        let f: Flow = serde_json::from_slice(&s.open(super::Seal::Flow, value)?).ok()?;
        (f.v == 1 && f.exp > now_s).then_some(f)
    }

    /// Whether the callback's `state` matches the flow (constant-time comparison).
    pub fn matches_state(&self, state: &str) -> bool {
        same(self.state.as_bytes(), state.as_bytes())
    }
}

pub fn set_flow_cookie(value: &str) -> String {
    format!("{FLOW_COOKIE}={value}; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age={FLOW_TTL_S}")
}

pub fn clear_flow_cookie() -> String {
    format!("{FLOW_COOKIE}=; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=0")
}

/// `return_to` only accepts same-origin relative paths (against open redirects):
/// starting with `/`, not `//` or `/\` (browsers read those as a URL of another origin),
/// no control characters. Otherwise ⇒ `fallback`.
pub fn safe_return_to(v: Option<&str>, fallback: &str) -> String {
    match v {
        Some(p)
            if p.starts_with('/')
                && !p.starts_with("//")
                && !p.starts_with("/\\")
                && p.len() <= MAX_URL
                && !p.chars().any(|c| c.is_control() || c == '\\') =>
        {
            p.to_string()
        }
        _ => fallback.to_string(),
    }
}

// ───────────────────────────────────────────────────────────── id_token

/// Why an `id_token` was refused — a stable code for logs and tests, never repeating the
/// token's content.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TokenError {
    Malformed,
    Algorithm,
    UnknownKey,
    Signature,
    Issuer,
    Audience,
    Expired,
    IssuedInFuture,
    Nonce,
    Subject,
    Tenant,
    Role,
}

impl TokenError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Malformed => "malformed",
            Self::Algorithm => "algorithm",
            Self::UnknownKey => "unknown_key",
            Self::Signature => "signature",
            Self::Issuer => "issuer",
            Self::Audience => "audience",
            Self::Expired => "expired",
            Self::IssuedInFuture => "issued_in_future",
            Self::Nonce => "nonce",
            Self::Subject => "subject",
            Self::Tenant => "tenant",
            Self::Role => "role",
        }
    }
}

/// One key of a JWKS (RFC 7517) — only the fields needed to check a signature.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Jwk {
    pub kty: String,
    #[serde(default)]
    pub kid: Option<String>,
    #[serde(default)]
    pub alg: Option<String>,
    #[serde(default, rename = "use")]
    pub use_: Option<String>,
    #[serde(default)]
    pub n: Option<String>,
    #[serde(default)]
    pub e: Option<String>,
    #[serde(default)]
    pub crv: Option<String>,
    #[serde(default)]
    pub x: Option<String>,
    #[serde(default)]
    pub y: Option<String>,
}

#[derive(Deserialize)]
struct JwkSet {
    keys: Vec<serde_json::Value>,
}

/// The keys of a JWKS. An unreadable key (unknown kind) is skipped, without spoiling the
/// whole set.
pub fn parse_jwks(raw: &[u8]) -> Result<Vec<Jwk>, String> {
    let set: JwkSet = serde_json::from_slice(raw).map_err(|e| format!("malformed JWKS: {e}"))?;
    Ok(set
        .keys
        .into_iter()
        .filter_map(|k| serde_json::from_value::<Jwk>(k).ok())
        .filter(|k| k.use_.as_deref().is_none_or(|u| u == "sig"))
        .collect())
}

/// The accepted algorithms. Never `none` or `HS*` (the symmetric key is the
/// `client_secret` — a public client has none).
/// A key's signature check `(message, signature) -> valid?`.
type SigCheck = Box<dyn Fn(&[u8], &[u8]) -> bool>;

fn verifier_for(alg: &str, k: &Jwk) -> Option<SigCheck> {
    use ring::signature as sig;
    if k.alg.as_deref().is_some_and(|a| a != alg) {
        return None;
    }
    let b = |s: &Option<String>| s.as_deref().and_then(|v| URL_SAFE_NO_PAD.decode(v).ok());
    match (alg, k.kty.as_str()) {
        ("RS256" | "PS256", "RSA") => {
            let (n, e) = (b(&k.n)?, b(&k.e)?);
            let params: &'static sig::RsaParameters = if alg == "RS256" {
                &sig::RSA_PKCS1_2048_8192_SHA256
            } else {
                &sig::RSA_PSS_2048_8192_SHA256
            };
            Some(Box::new(move |msg, s| {
                sig::RsaPublicKeyComponents { n: &n, e: &e }
                    .verify(params, msg, s)
                    .is_ok()
            }))
        }
        ("ES256", "EC") if k.crv.as_deref() == Some("P-256") => {
            let (x, y) = (b(&k.x)?, b(&k.y)?);
            if x.len() != 32 || y.len() != 32 {
                return None;
            }
            let mut point = vec![4u8];
            point.extend_from_slice(&x);
            point.extend_from_slice(&y);
            Some(Box::new(move |msg, s| {
                sig::UnparsedPublicKey::new(&sig::ECDSA_P256_SHA256_FIXED, &point)
                    .verify(msg, s)
                    .is_ok()
            }))
        }
        ("EdDSA", "OKP") if k.crv.as_deref() == Some("Ed25519") => {
            let x = b(&k.x)?;
            Some(Box::new(move |msg, s| {
                sig::UnparsedPublicKey::new(&sig::ED25519, &x)
                    .verify(msg, s)
                    .is_ok()
            }))
        }
        _ => None,
    }
}

/// The header of a JWS: `alg`, `kid`.
pub fn jws_header(token: &str) -> Result<(String, Option<String>), TokenError> {
    let h = token.split('.').next().ok_or(TokenError::Malformed)?;
    let v: serde_json::Value = serde_json::from_slice(
        &URL_SAFE_NO_PAD
            .decode(h)
            .map_err(|_| TokenError::Malformed)?,
    )
    .map_err(|_| TokenError::Malformed)?;
    let alg = v["alg"].as_str().ok_or(TokenError::Malformed)?.to_string();
    Ok((alg, v["kid"].as_str().map(str::to_string)))
}

/// What an `id_token` must satisfy.
pub struct Expect<'a> {
    pub issuer: &'a str,
    pub client_id: &'a str,
    pub nonce: &'a str,
    pub tenant: &'a TenantId,
    pub max_role: Role,
    pub now_s: u64,
}

/// The result of a valid `id_token`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Verified {
    pub sub: String,
    pub email: Option<String>,
    pub name: Option<String>,
    /// The role capped by `max_role`.
    pub role: Role,
}
/// Checks an `id_token` (OIDC Core, section 3.1.3.7) against the key set `keys`: the
/// signature, `iss`, `aud`/`azp`, `exp`/`iat` against `now_s` with a [`CLOCK_SKEW_MS`]
/// skew, `nonce`, `sub`, `cairn_tenant` (if present) equal to the client's tenant, and
/// `cairn_role` capped by `max_role`.
/// `cairn_role` capped by `max_role`.
pub fn verify_id_token(token: &str, keys: &[Jwk], x: &Expect) -> Result<Verified, TokenError> {
    let parts: Vec<&str> = token.split('.').collect();
    let [h, p, s] = parts.as_slice() else {
        return Err(TokenError::Malformed);
    };
    let (alg, kid) = jws_header(token)?;
    if !matches!(alg.as_str(), "RS256" | "PS256" | "ES256" | "EdDSA") {
        return Err(TokenError::Algorithm);
    }
    let sig = URL_SAFE_NO_PAD
        .decode(s)
        .map_err(|_| TokenError::Malformed)?;
    let signed = format!("{h}.{p}");
    // With a `kid` ⇒ exactly that key; without ⇒ try every key of a matching algorithm.
    let candidates: Vec<&Jwk> = keys
        .iter()
        .filter(|k| kid.is_none() || k.kid == kid)
        .collect();
    let verifiers: Vec<_> = candidates
        .iter()
        .filter_map(|k| verifier_for(&alg, k))
        .collect();
    if verifiers.is_empty() {
        return Err(TokenError::UnknownKey);
    }
    if !verifiers.iter().any(|v| v(signed.as_bytes(), &sig)) {
        return Err(TokenError::Signature);
    }
    let c: serde_json::Value = serde_json::from_slice(
        &URL_SAFE_NO_PAD
            .decode(p)
            .map_err(|_| TokenError::Malformed)?,
    )
    .map_err(|_| TokenError::Malformed)?;
    if c["iss"].as_str() != Some(x.issuer) {
        return Err(TokenError::Issuer);
    }
    let aud_ok = match &c["aud"] {
        serde_json::Value::String(a) => a == x.client_id,
        serde_json::Value::Array(a) => {
            a.iter().any(|v| v.as_str() == Some(x.client_id))
                && (a.len() == 1 || c["azp"].as_str() == Some(x.client_id))
        }
        _ => false,
    };
    if !aud_ok || c["azp"].as_str().is_some_and(|azp| azp != x.client_id) {
        return Err(TokenError::Audience);
    }
    let num = |k: &str| c[k].as_u64().or_else(|| c[k].as_f64().map(|f| f as u64));
    let skew = CLOCK_SKEW_MS / 1000;
    let exp = num("exp").ok_or(TokenError::Malformed)?;
    if exp + skew <= x.now_s {
        return Err(TokenError::Expired);
    }
    let iat = num("iat").ok_or(TokenError::Malformed)?;
    if iat > x.now_s + skew {
        return Err(TokenError::IssuedInFuture);
    }
    match c["nonce"].as_str() {
        Some(n) if same(n.as_bytes(), x.nonce.as_bytes()) => {}
        _ => return Err(TokenError::Nonce),
    }
    let sub = c["sub"].as_str().ok_or(TokenError::Subject)?;
    check_sub(sub).map_err(|_| TokenError::Subject)?;
    if let Some(t) = c.get("cairn_tenant").filter(|v| !v.is_null()) {
        if t.as_str() != Some(x.tenant.as_str()) {
            return Err(TokenError::Tenant);
        }
    }
    let role = c["cairn_role"]
        .as_str()
        .and_then(Role::parse)
        .and_then(|r| cap_role(r, x.max_role))
        .ok_or(TokenError::Role)?;
    let text = |k: &str, max: usize| {
        c[k].as_str()
            .filter(|v| !v.is_empty() && !v.chars().any(char::is_control))
            .map(|v| v.chars().take(max).collect::<String>())
    };
    Ok(Verified {
        sub: sub.to_string(),
        email: text("email", MAX_EMAIL).filter(|e| e.len() <= MAX_EMAIL),
        name: text("name", MAX_NAME),
        role,
    })
}

// ───────────────────────────────────────────────────────────── relying party

/// The issuer's metadata (discovery) and its key set.
#[derive(Clone, Debug)]
pub struct Provider {
    pub issuer: String,
    pub authorization_endpoint: url::Url,
    pub token_endpoint: url::Url,
    pub jwks_uri: url::Url,
    pub keys: Vec<Jwk>,
    fetched: tokio::time::Instant,
}

/// Errors on the issuer side (network, broken discovery, the token endpoint refusing).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IssuerError(pub String);

impl std::fmt::Display for IssuerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A node's relying party: a client with the egress policy, the last good discovery +
/// JWKS in RAM.
#[derive(Clone)]
pub struct Rp(Arc<RpInner>);

struct RpInner {
    http: crate::egress::TenantHttp,
    provider: RwLock<Option<Provider>>,
    /// The latest JWKS refetch because of an unknown `kid`, and the latest failed discovery.
    last_refetch: Mutex<Option<tokio::time::Instant>>,
    last_failure: Mutex<Option<tokio::time::Instant>>,
}

impl std::fmt::Debug for Rp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Rp").finish_non_exhaustive()
    }
}

impl Rp {
    pub fn new(policy: Arc<crate::egress::EgressPolicy>) -> Self {
        Self(Arc::new(RpInner {
            http: crate::egress::TenantHttp::h1(policy),
            provider: RwLock::new(None),
            last_refetch: Mutex::new(None),
            last_failure: Mutex::new(None),
        }))
    }

    async fn get(&self, url: &url::Url) -> Result<bytes::Bytes, IssuerError> {
        let req = self
            .0
            .http
            .client()
            .get(url.clone())
            .header(reqwest::header::ACCEPT, "application/json")
            .timeout(ISSUER_TIMEOUT);
        let resp = crate::egress::send_for_tenant(&self.0.http, req)
            .await
            .map_err(|e| IssuerError(format!("GET {url}: {e}")))?;
        if !resp.status().is_success() {
            return Err(IssuerError(format!("GET {url}: {}", resp.status())));
        }
        read_capped(resp).await
    }

    async fn fetch(&self, issuer: &str) -> Result<Provider, IssuerError> {
        let disc = url::Url::parse(&format!(
            "{}/.well-known/openid-configuration",
            issuer.trim_end_matches('/')
        ))
        .map_err(|e| IssuerError(format!("issuer {issuer}: {e}")))?;
        let meta: serde_json::Value = serde_json::from_slice(&self.get(&disc).await?)
            .map_err(|e| IssuerError(format!("malformed discovery: {e}")))?;
        // OIDC Discovery, section 4.3: the document's `issuer` must be exactly the
        // configured issuer — otherwise the document belongs to another issuer.
        if meta["issuer"].as_str() != Some(issuer) {
            return Err(IssuerError(format!(
                "discovery says issuer {:?}, configured is {issuer:?}",
                meta["issuer"]
            )));
        }
        let endpoint = |k: &str| {
            meta[k]
                .as_str()
                .and_then(|u| check_http_url(k, u, true).ok())
                .ok_or_else(|| IssuerError(format!("discovery is missing a valid {k}")))
        };
        let jwks_uri = endpoint("jwks_uri")?;
        let keys = parse_jwks(&self.get(&jwks_uri).await?).map_err(IssuerError)?;
        Ok(Provider {
            issuer: issuer.to_string(),
            authorization_endpoint: endpoint("authorization_endpoint")?,
            token_endpoint: endpoint("token_endpoint")?,
            jwks_uri,
            keys,
            fetched: tokio::time::Instant::now(),
        })
    }

    /// The discovery + JWKS of `issuer`: the RAM copy while fresh; refetched when older
    /// than [`PROVIDER_MAX_AGE`] — on failure the old copy is used. No copy yet and the
    /// fetch fails ⇒ `Err`; the next attempt comes at least [`RETRY_FLOOR`] later.
    pub async fn provider(&self, issuer: &str) -> Result<Provider, IssuerError> {
        let cached = self
            .0
            .provider
            .read()
            .ok()
            .and_then(|p| p.clone())
            .filter(|p| p.issuer == issuer);
        if let Some(p) = &cached {
            if p.fetched.elapsed() < PROVIDER_MAX_AGE {
                return Ok(p.clone());
            }
        }
        let now = tokio::time::Instant::now();
        let recently_failed = self
            .0
            .last_failure
            .lock()
            .ok()
            .and_then(|l| *l)
            .is_some_and(|t| now.duration_since(t) < RETRY_FLOOR);
        if recently_failed {
            return cached.ok_or_else(|| IssuerError("issuer just failed — retry later".into()));
        }
        match self.fetch(issuer).await {
            Ok(p) => {
                if let Ok(mut slot) = self.0.provider.write() {
                    *slot = Some(p.clone());
                }
                Ok(p)
            }
            Err(e) => {
                if let Ok(mut l) = self.0.last_failure.lock() {
                    *l = Some(now);
                }
                match cached {
                    Some(p) => {
                        tracing::warn!(error = %e, "cannot refresh discovery/JWKS — using the last good copy");
                        Ok(p)
                    }
                    None => Err(e),
                }
            }
        }
    }

    /// The key set to check `token` with: `p`'s, or — when the token's `kid` is not in it
    /// (the issuer rotated keys) — a refetched JWKS, with the floor [`REFETCH_FLOOR`].
    pub async fn keys_for(&self, p: &Provider, token: &str) -> Vec<Jwk> {
        let kid = jws_header(token).ok().and_then(|(_, k)| k);
        if kid.is_none() || p.keys.iter().any(|k| k.kid == kid) {
            return p.keys.clone();
        }
        let now = tokio::time::Instant::now();
        {
            let Ok(mut last) = self.0.last_refetch.lock() else {
                return p.keys.clone();
            };
            if last.is_some_and(|t| now.duration_since(t) < REFETCH_FLOOR) {
                return p.keys.clone();
            }
            *last = Some(now);
        }
        match self
            .get(&p.jwks_uri)
            .await
            .and_then(|raw| parse_jwks(&raw).map_err(IssuerError))
        {
            Ok(keys) => {
                if let Ok(mut slot) = self.0.provider.write() {
                    if let Some(cur) = slot.as_mut().filter(|c| c.issuer == p.issuer) {
                        cur.keys = keys.clone();
                    }
                }
                keys
            }
            Err(e) => {
                tracing::debug!(error = %e, "cannot refresh JWKS");
                p.keys.clone()
            }
        }
    }

    /// Exchanges `code` for an `id_token` (public client + PKCE).
    pub async fn exchange(
        &self,
        p: &Provider,
        client_id: &str,
        code: &str,
        flow: &Flow,
    ) -> Result<String, IssuerError> {
        let req = self
            .0
            .http
            .client()
            .post(p.token_endpoint.clone())
            .header(reqwest::header::ACCEPT, "application/json")
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/x-www-form-urlencoded",
            )
            .body(
                form_urlencoded::Serializer::new(String::new())
                    .append_pair("grant_type", "authorization_code")
                    .append_pair("code", code)
                    .append_pair("redirect_uri", &flow.redirect_uri)
                    .append_pair("client_id", client_id)
                    .append_pair("code_verifier", &flow.verifier)
                    .finish(),
            )
            .timeout(ISSUER_TIMEOUT);
        let resp = crate::egress::send_for_tenant(&self.0.http, req)
            .await
            .map_err(|e| IssuerError(format!("token endpoint: {e}")))?;
        let status = resp.status();
        let body = read_capped(resp).await?;
        if !status.is_success() {
            // OAuth errors (`invalid_grant`…): only the code, no other content.
            let code = serde_json::from_slice::<serde_json::Value>(&body)
                .ok()
                .and_then(|v| v["error"].as_str().map(str::to_string))
                .unwrap_or_default();
            return Err(IssuerError(format!("token endpoint {status} {code}")));
        }
        let v: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|e| IssuerError(format!("malformed token response: {e}")))?;
        v["id_token"]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| IssuerError("token response has no id_token".into()))
    }

    /// The authorize URL of flow `flow`.
    pub fn authorize_url(p: &Provider, client_id: &str, flow: &Flow) -> url::Url {
        let mut u = p.authorization_endpoint.clone();
        u.query_pairs_mut()
            .append_pair("response_type", "code")
            .append_pair("client_id", client_id)
            .append_pair("redirect_uri", &flow.redirect_uri)
            .append_pair("scope", "openid email profile")
            .append_pair("state", &flow.state)
            .append_pair("nonce", &flow.nonce)
            .append_pair("code_challenge", &flow.challenge())
            .append_pair("code_challenge_method", "S256");
        u
    }
}

/// The response body, at most [`ISSUER_MAX_BODY`] bytes.
async fn read_capped(mut resp: reqwest::Response) -> Result<bytes::Bytes, IssuerError> {
    let mut out = bytes::BytesMut::new();
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| IssuerError(format!("reading issuer response: {e}")))?
    {
        if out.len() + chunk.len() > ISSUER_MAX_BODY {
            return Err(IssuerError("issuer response too large".into()));
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out.freeze())
}

#[cfg(test)]
mod tests;
