// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Web console sessions: trading an API key for a **stateless** `HttpOnly` cookie.
//!
//! - The cookie **does not contain the key**: `hex(json{v, kid, tenant, role, iat, exp}) .
//!   hex(hmac)`, the HMAC keyed with the derived key `HMAC(cluster secret,
//!   "cairn-ui-session/v1")` — any node in the cluster can verify it, there is no
//!   session registry to synchronise.
//! - **Every request looks `kid` up again** in the key registry ([`Keys::identify`]),
//!   the same registry as Bearer: revoking the key ⇒ the session dies on the same tick.
//!   Tenant and role come from the current record, not from the cookie. The `kid` of the
//!   bootstrap key is `bootstrap:<first 8 bytes of sha256(key)>` — changing
//!   `CAIRNFLOW_ADMIN_KEY` kills every old session of it.
//! - A fixed expiry ([`SESSION_TTL`]), no sliding renewal.
//! - When authenticating by cookie, unsafe methods must prove the same origin
//!   ([`same_origin`]): `SameSite=Strict` does not stop another port on the same
//!   `localhost`.
//! - Behind a TLS proxy, `X-Forwarded-Host`/`-Proto`/`-For` are read only when the peer
//!   is inside `--trust-proxy <cidr>` ([`TrustedProxies`]).

use {
    super::{hex, hmac_sha256, now_s, same, unhex, ClusterSecret, Identity, Keys, Role},
    axum::http::{header, Extensions, HeaderMap, Method},
    serde::{Deserialize, Serialize},
    std::{net::IpAddr, time::Duration},
};

/// The name of the key session cookie when **not** `Secure` (loopback, `http://`).
pub const SESSION_COOKIE: &str = "cairn_ui";
/// The name of the key session cookie when `Secure`: the `__Host-` prefix makes the
/// browser accept it only from the host itself, `Secure`, `Path=/`, no `Domain` — a
/// sibling origin cannot set its own copy. A different name from the SSO cookie
/// (`__Host-cairn_ui`): the two kinds never collide.
pub const SESSION_COOKIE_SECURE: &str = "__Host-cairn_key";
/// The lifetime of a session.
pub const SESSION_TTL: Duration = Duration::from_secs(12 * 60 * 60);
/// The label deriving the session signing key from the cluster secret. Changing the label = every
/// old session dies.
const LABEL: &[u8] = b"cairn-ui-session/v1";
/// The label of the SSO session signing key (cookie v2) — a separate key, so that a
/// signature of one cookie kind is never a valid signature of the other.
const LABEL_SSO: &[u8] = b"cairn-ui-session/v2-oidc";
/// The label of the key signing the temporary cookies of the OIDC login flow (`state`,
/// `nonce`, `code_verifier`).
const LABEL_FLOW: &[u8] = b"cairn-oidc-flow/v1";
/// Accepted clock skew for an `iat` in the future (a session signed on another node).
const IAT_SKEW_S: u64 = 60;

/// What the request authenticated with — an extension next to [`Identity`], for `whoami`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Via {
    Bearer,
    /// A session cookie, expiring at `expires_at_s` (Unix seconds).
    Cookie {
        expires_at_s: u64,
    },
    /// An SSO session cookie (`__Host-cairn_ui`), expiring at `expires_at_s`.
    Sso {
        expires_at_s: u64,
    },
    /// `--no-auth`.
    NoAuth,
}

impl Via {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Bearer => "bearer",
            Self::Cookie { .. } => "cookie",
            Self::Sso { .. } => "sso",
            Self::NoAuth => "none",
        }
    }

    /// Authenticated by cookie (API key or SSO): unsafe methods must prove the same
    /// origin.
    pub fn is_cookie(self) -> bool {
        matches!(self, Self::Cookie { .. } | Self::Sso { .. })
    }

    /// The expiry of the cookie session, Unix seconds.
    pub fn expires_at_s(self) -> Option<u64> {
        match self {
            Self::Cookie { expires_at_s } | Self::Sso { expires_at_s } => Some(expires_at_s),
            _ => None,
        }
    }
}

/// The kind of data [`Sessions::seal`] signs — one derived key per kind.
#[derive(Clone, Copy, Debug)]
pub(super) enum Seal {
    /// The SSO session cookie v2.
    Sso,
    /// The temporary cookie of the OIDC login flow.
    Flow,
}

#[derive(Serialize, Deserialize)]
struct Claims {
    v: u8,
    kid: String,
    tenant: String,
    role: String,
    iat: u64,
    exp: u64,
    /// A random session id. Absent ⇒ refused.
    #[serde(default)]
    sid: String,
}

/// A node's session signer/verifier.
#[derive(Clone)]
pub struct Sessions {
    key: [u8; 32],
    sso_key: [u8; 32],
    flow_key: [u8; 32],
    ttl: Duration,
    secure: bool,
    trusted: TrustedProxies,
}

impl std::fmt::Debug for Sessions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sessions")
            .field("ttl", &self.ttl)
            .field("secure", &self.secure)
            .field("trusted", &self.trusted)
            .finish()
    }
}

impl Sessions {
    /// `secure` ⇒ cookie mang `Secure` (sau proxy TLS, `--ui-cookie-secure`).
    pub fn new(secret: &ClusterSecret, ttl: Duration, secure: bool) -> Self {
        Self {
            key: hmac_sha256(&secret.key, LABEL),
            sso_key: hmac_sha256(&secret.key, LABEL_SSO),
            flow_key: hmac_sha256(&secret.key, LABEL_FLOW),
            ttl,
            secure,
            trusted: TrustedProxies::default(),
        }
    }

    /// Trusted proxies (`--trust-proxy`).
    pub fn with_trusted_proxies(mut self, trusted: TrustedProxies) -> Self {
        self.trusted = trusted;
        self
    }

    pub fn trusted(&self) -> &TrustedProxies {
        &self.trusted
    }

    /// Whether the cookie carries `Secure`: the configuration, or a trusted proxy saying
    /// the browser came over `https` — never lowers a configuration flag that is on.
    pub fn secure_for(&self, fwd: Option<&Forwarded>) -> bool {
        self.secure || fwd.is_some_and(Forwarded::https)
    }

    fn sign(&self, payload: &[u8]) -> [u8; 32] {
        hmac_sha256(&self.key, payload)
    }

    fn seal_key(&self, which: Seal) -> &[u8; 32] {
        match which {
            Seal::Sso => &self.sso_key,
            Seal::Flow => &self.flow_key,
        }
    }

    /// `hex(payload).hex(hmac)` with the key of `which` — the same shape as cookie v1.
    pub(super) fn seal(&self, which: Seal, payload: &[u8]) -> String {
        format!(
            "{}.{}",
            hex(payload),
            hex(&hmac_sha256(self.seal_key(which), payload))
        )
    }

    /// The inverse of [`Self::seal`]: the payload when the signature is right.
    pub(super) fn open(&self, which: Seal, value: &str) -> Option<Vec<u8>> {
        let (payload, mac) = value.split_once('.')?;
        let payload = unhex(payload)?;
        let mac = unhex(mac)?;
        same(&mac, &hmac_sha256(self.seal_key(which), &payload)).then_some(payload)
    }

    /// A new cookie for `who` (key `kid`). Returns the value and the expiry mark (seconds).
    pub fn mint(&self, kid: &str, who: &Identity) -> (String, u64) {
        let iat = now_s();
        let exp = iat + self.ttl.as_secs();
        let claims = Claims {
            v: 1,
            kid: kid.to_string(),
            tenant: who.tenant.as_str().to_string(),
            role: who.role.as_str().to_string(),
            iat,
            exp,
            sid: super::oidc::new_session_id(),
        };
        let payload = serde_json::to_vec(&claims).expect("claims always encode");
        (
            format!("{}.{}", hex(&payload), hex(&self.sign(&payload))),
            exp,
        )
    }

    /// The identity of a cookie value: the signature is right, not expired, and `kid` is
    /// still in the key registry **right now**.
    pub fn verify(&self, value: &str, keys: &Keys) -> Option<(Identity, u64)> {
        let (payload, mac) = value.split_once('.')?;
        let payload = unhex(payload)?;
        let mac = unhex(mac)?;
        if !same(&mac, &self.sign(&payload)) {
            return None;
        }
        let c: Claims = serde_json::from_slice(&payload).ok()?;
        let now = now_s();
        if c.v != 1 || c.exp <= now || c.iat > now + IAT_SKEW_S {
            return None;
        }
        // Per-session revocation: no id ⇒ cannot be revoked ⇒ does not open.
        if c.sid.is_empty() || keys.oidc().session_revoked(&c.sid) {
            return None;
        }
        // A `provision` key (and the metrics token) can never log in ([`Keys::login`]);
        // anyone with the cluster secret can sign a cookie carrying the `kid` of a
        // `provision` key — the signature must not be the only thing stopping it.
        keys.identify(&c.kid)
            .filter(|who| !matches!(who.role, Role::Provision | Role::Metrics))
            .map(|who| (who, c.exp))
    }

    /// `(sid, exp, tenant)` of a correctly signed key session cookie that **has not
    /// expired** — for logout. Expired ⇒ `None`: it no longer opens anything, and
    /// recording it in the registry only bloats the registry.
    pub fn session_id(&self, value: &str) -> Option<(String, u64, String)> {
        let (payload, mac) = value.split_once('.')?;
        let payload = unhex(payload)?;
        if !same(&unhex(mac)?, &self.sign(&payload)) {
            return None;
        }
        let c: Claims = serde_json::from_slice(&payload).ok()?;
        (!c.sid.is_empty() && c.exp > now_s()).then_some((c.sid, c.exp, c.tenant))
    }

    /// The key session cookie name for a request with proxy statement `fwd`: `Secure` ⇒
    /// [`SESSION_COOKIE_SECURE`], otherwise [`SESSION_COOKIE`].
    pub fn cookie_name(&self, fwd: Option<&Forwarded>) -> &'static str {
        if self.secure_for(fwd) {
            SESSION_COOKIE_SECURE
        } else {
            SESSION_COOKIE
        }
    }

    /// The `Set-Cookie` of a new session. `fwd` = the trusted proxy's statement.
    pub fn set_cookie(&self, value: &str, fwd: Option<&Forwarded>) -> String {
        format!(
            "{}={value}; HttpOnly; SameSite=Strict; Path=/; Max-Age={}{}",
            self.cookie_name(fwd),
            self.ttl.as_secs(),
            if self.secure_for(fwd) { "; Secure" } else { "" }
        )
    }

    /// The `Set-Cookie` clearing the session (logout) — the SSO v2 cookie too: logging
    /// out is logging out, whether the session came from a key or from SSO.
    pub fn clear_cookies(&self, fwd: Option<&Forwarded>) -> [String; 2] {
        [self.clear_cookie(fwd), super::oidc::clear_sso_cookie()]
    }

    /// The `Set-Cookie` clearing the API key session.
    pub fn clear_cookie(&self, fwd: Option<&Forwarded>) -> String {
        format!(
            "{}=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0{}",
            self.cookie_name(fwd),
            if self.secure_for(fwd) { "; Secure" } else { "" }
        )
    }
}

/// Every value of the request's key session cookie named `name`
/// ([`Sessions::cookie_name`]). A browser may send several copies with the same name
/// (cookies set from a sibling subdomain): the caller refuses when the copies that
/// verify name two different identities, rather than picking the first.
pub fn session_cookies<'a>(headers: &'a HeaderMap, name: &'a str) -> impl Iterator<Item = &'a str> {
    cookies_named(headers, name)
}

/// Every non-empty value of the cookie named `name` (verbatim).
pub fn cookies_named<'a>(headers: &'a HeaderMap, name: &'a str) -> impl Iterator<Item = &'a str> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .filter_map(move |kv| {
            let (k, v) = kv.trim().split_once('=')?;
            (k == name && !v.is_empty()).then_some(v)
        })
}

/// Methods that change nothing (no need to prove the same origin).
pub fn is_safe(method: &Method) -> bool {
    matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS)
}

/// Whether the request comes from the node's own origin (CSRF protection for requests
/// authenticated by cookie — the same approach as the Temporal and Hatchet web UIs).
///
/// If there is `Sec-Fetch-Site`, it decides: only `same-origin`. Without it (old
/// browsers, tools) `Origin` must equal `Host`. Neither ⇒ no.
/// `fwd` (a trusted proxy, [`TrustedProxies::forwarded`]): the host is taken from
/// `X-Forwarded-Host` instead of `Host`, and with `X-Forwarded-Proto` the scheme of
/// `Origin` must match too.
pub fn same_origin(headers: &HeaderMap, fwd: Option<&Forwarded>) -> bool {
    let get = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    if let Some(site) = get("sec-fetch-site") {
        return site.eq_ignore_ascii_case("same-origin");
    }
    let host = fwd.and_then(|f| f.host.as_deref()).or_else(|| get("host"));
    let (Some(origin), Some(host)) = (get("origin"), host) else {
        return false;
    };
    let proto = fwd.and_then(|f| f.proto.as_deref());
    origin.split_once("://").is_some_and(|(scheme, authority)| {
        authority.eq_ignore_ascii_case(host) && proto.is_none_or(|p| scheme.eq_ignore_ascii_case(p))
    })
}

// ─────────────────────────────────────────── trusted proxies

/// The IP of the TCP peer — the ingress port's `ConnectInfo` extension. `None` when the
/// router is called in process (`/query` views, forwarding).
pub fn peer_ip(ext: &Extensions) -> Option<IpAddr> {
    ext.get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
        .map(|c| c.0.ip())
}

/// A trusted proxy's statement about the original request.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Forwarded {
    /// `X-Forwarded-Host` (the last value — from the proxy nearest the node).
    pub host: Option<String>,
    /// `X-Forwarded-Proto`, lower case.
    pub proto: Option<String>,
}

/// The proxy statement that **the node receiving** a forwarded request trusted
/// (security review) — an extension `ingress::forward::decode` attaches from the signed
/// envelope; a client cannot set extensions.
#[derive(Clone, Debug)]
pub struct RelayedForwarded(pub Forwarded);

impl Forwarded {
    pub(super) fn https(&self) -> bool {
        self.proto.as_deref() == Some("https")
    }
}

/// Trusted proxy address ranges (`--trust-proxy <cidr>[,<cidr>…]`). Empty (the
/// default) ⇒ no `X-Forwarded-*` header is trusted: anyone can send them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TrustedProxies(Vec<(IpAddr, u8)>);

impl TrustedProxies {
    /// `a.b.c.d/n`, `x::y/n` or a single address (both `/32`, `/128`), comma-separated.
    pub fn parse(s: &str) -> Result<Self, String> {
        let mut out = Vec::new();
        for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            let (ip, bits) = match part.split_once('/') {
                Some((ip, b)) => (ip, Some(b)),
                None => (part, None),
            };
            let ip: IpAddr = ip
                .parse()
                .map_err(|_| format!("proxy range {part:?}: malformed IP address"))?;
            let ip = ip.to_canonical();
            let max = if ip.is_ipv4() { 32 } else { 128 };
            let bits = match bits {
                None => max,
                Some(b) => b.parse::<u8>().ok().filter(|b| *b <= max).ok_or_else(|| {
                    format!("proxy range {part:?}: prefix length must be 0..={max}")
                })?,
            };
            out.push((ip, bits));
        }
        if out.is_empty() {
            return Err("at least one proxy range is required, e.g. 10.0.0.0/8".into());
        }
        Ok(Self(out))
    }

    /// Adds the ranges of `other` (a repeated flag).
    pub fn extend(&mut self, other: Self) {
        self.0.extend(other.0);
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether `ip` is inside a trusted range.
    pub fn contains(&self, ip: IpAddr) -> bool {
        let ip = ip.to_canonical();
        self.0.iter().any(|&(net, bits)| match (net, ip) {
            (IpAddr::V4(n), IpAddr::V4(a)) => same_prefix(&n.octets(), &a.octets(), bits),
            (IpAddr::V6(n), IpAddr::V6(a)) => same_prefix(&n.octets(), &a.octets(), bits),
            _ => false,
        })
    }

    /// `X-Forwarded-Host`/`-Proto` — only when `peer` is a trusted proxy.
    /// The proxy statement of a request: [`RelayedForwarded`] if the request was rebuilt
    /// from a forwarding envelope, otherwise [`Self::forwarded`] by peer.
    pub fn forwarded_in(&self, headers: &HeaderMap, ext: &Extensions) -> Option<Forwarded> {
        if let Some(r) = ext.get::<RelayedForwarded>() {
            return Some(r.0.clone());
        }
        self.forwarded(headers, peer_ip(ext))
    }

    pub fn forwarded(&self, headers: &HeaderMap, peer: Option<IpAddr>) -> Option<Forwarded> {
        if !peer.is_some_and(|p| self.contains(p)) {
            return None;
        }
        Some(Forwarded {
            host: last_value(headers, "x-forwarded-host").map(str::to_string),
            proto: last_value(headers, "x-forwarded-proto").map(str::to_ascii_lowercase),
        })
    }

    /// The client IP: `peer`, or — when `peer` is a trusted proxy — the rightmost address
    /// in `X-Forwarded-For` not inside a trusted range (the left part is written by the
    /// client itself, not trusted). A broken value ⇒ stop at the last trusted proxy.
    pub fn client_ip(&self, headers: &HeaderMap, peer: Option<IpAddr>) -> Option<IpAddr> {
        let mut ip = peer?;
        if !self.contains(ip) {
            return Some(ip);
        }
        let hops: Vec<&str> = headers
            .get_all("x-forwarded-for")
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(','))
            .map(str::trim)
            .collect();
        for hop in hops.into_iter().rev() {
            let Ok(next) = hop.parse::<IpAddr>() else {
                break;
            };
            ip = next.to_canonical();
            if !self.contains(ip) {
                break;
            }
        }
        Some(ip)
    }
}

/// The last value (after the last comma, of the last header) of `name`.
fn last_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get_all(name)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(str::trim)
        .rfind(|v| !v.is_empty())
}

fn same_prefix(a: &[u8], b: &[u8], bits: u8) -> bool {
    let full = usize::from(bits / 8);
    let rest = bits % 8;
    if a[..full] != b[..full] {
        return false;
    }
    rest == 0 || {
        let mask = 0xffu8 << (8 - rest);
        a[full] & mask == b[full] & mask
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::auth::Role, cairn_store::TenantId};

    fn ids(keys: &Keys, token: &str) -> (Identity, String) {
        keys.login(token).expect("valid key")
    }

    #[tokio::test]
    async fn mint_verify_tamper_revoke() {
        let keys = Keys::new(None, Some("boot"));
        let (rec, secret) = keys
            .create(TenantId::new("acme").unwrap(), Role::Admin)
            .await
            .unwrap();
        let s = Sessions::new(&ClusterSecret::new("s1"), SESSION_TTL, false);
        let (who, kid) = ids(&keys, &secret);
        assert_eq!(kid, rec.id);
        let (cookie, exp) = s.mint(&kid, &who);
        assert!(!cookie.contains(&secret), "cookie carries the key");
        let (back, e) = s.verify(&cookie, &keys).expect("session just signed");
        assert_eq!(
            (back.tenant.as_str(), back.role, e),
            ("acme", Role::Admin, exp)
        );
        assert_eq!(back.key_id.as_deref(), Some(rec.id.as_str()));

        // Altering any byte ⇒ broken.
        for i in 0..cookie.len() {
            let mut b = cookie.clone().into_bytes();
            b[i] = if b[i] == b'0' { b'1' } else { b'0' };
            let t = String::from_utf8(b).unwrap();
            if t != cookie {
                assert!(s.verify(&t, &keys).is_none(), "byte {i} flip passes");
            }
        }
        // Another cluster secret ⇒ broken; the same secret (another node) ⇒ passes.
        let other = Sessions::new(&ClusterSecret::new("s2"), SESSION_TTL, false);
        assert!(other.verify(&cookie, &keys).is_none());
        let twin = Sessions::new(&ClusterSecret::new("s1"), SESSION_TTL, false);
        assert!(twin.verify(&cookie, &keys).is_some());
        // Revoked ⇒ the session dies.
        assert!(keys.delete(&rec.id).await.unwrap());
        assert!(s.verify(&cookie, &keys).is_none(), "key already revoked");
    }

    #[test]
    fn expired_and_bootstrap_rotation() {
        let keys = Keys::new(None, Some("boot-a"));
        let (who, kid) = ids(&keys, "boot-a");
        assert!(
            kid.starts_with("bootstrap:") && kid.len() == "bootstrap:".len() + 16,
            "{kid}"
        );
        let s = Sessions::new(&ClusterSecret::new("s"), SESSION_TTL, false);
        let (cookie, _) = s.mint(&kid, &who);
        assert!(s.verify(&cookie, &keys).is_some());
        // Changing the bootstrap key ⇒ old sessions die.
        let rotated = Keys::new(None, Some("boot-b"));
        assert!(s.verify(&cookie, &rotated).is_none());
        // TTL 0 ⇒ expired at once.
        let dead = Sessions::new(&ClusterSecret::new("s"), Duration::ZERO, false);
        let (c, _) = dead.mint(&kid, &who);
        assert!(dead.verify(&c, &keys).is_none());
    }

    /// **An expired cookie does not trigger revocation (security review).** Logout
    /// revokes every cookie the request carries; an expired cookie no longer opens
    /// anything, and recording it only bloats the registry (and each time is a GET + PUT
    /// of the whole registry). Must see: `session_id` of an expired cookie ⇒ `None`;
    /// not expired ⇒ `Some`.
    #[test]
    fn expired_cookie_has_no_session_to_revoke() {
        let keys = Keys::new(None, Some("boot-a"));
        let (who, kid) = ids(&keys, "boot-a");
        let live = Sessions::new(&ClusterSecret::new("s"), SESSION_TTL, false);
        let (c, _) = live.mint(&kid, &who);
        assert!(live.session_id(&c).is_some(), "cookie not expired");
        let dead = Sessions::new(&ClusterSecret::new("s"), Duration::ZERO, false);
        let (c, _) = dead.mint(&kid, &who);
        assert!(
            dead.session_id(&c).is_none(),
            "an expired cookie still yields a session id to record"
        );
    }

    /// **A key session cookie without a session id is refused.** Per-session revocation
    /// needs the id; a cookie minted before session ids existed (no `sid`) cannot be
    /// revoked and so no longer opens a session — the user logs in again once. Must
    /// see: a correctly signed payload, not expired, `kid` still in the registry, but no
    /// `sid` ⇒ `None`.
    #[test]
    fn cookie_without_session_id_is_refused() {
        let keys = Keys::new(None, Some("boot-key-for-the-session-unit-test-0001"));
        let (who, kid) = ids(&keys, "boot-key-for-the-session-unit-test-0001");
        let s = Sessions::new(&ClusterSecret::new("s"), SESSION_TTL, false);
        let now = now_s();
        let payload = serde_json::json!({
            "v": 1, "kid": kid, "tenant": who.tenant.as_str(), "role": who.role.as_str(),
            "iat": now, "exp": now + 3600,
        })
        .to_string();
        let cookie = format!(
            "{}.{}",
            hex(payload.as_bytes()),
            hex(&s.sign(payload.as_bytes()))
        );
        assert!(
            s.verify(&cookie, &keys).is_none(),
            "a cookie without sid still opens a session"
        );
    }

    #[test]
    fn cookie_attributes() {
        let s = Sessions::new(&ClusterSecret::new("s"), SESSION_TTL, true);
        let c = s.set_cookie("v", None);
        // `Secure` ⇒ the `__Host-` name.
        assert!(c.starts_with("__Host-cairn_key=v;"), "{c}");
        for part in [
            "HttpOnly",
            "SameSite=Strict",
            "Path=/",
            "Max-Age=43200",
            "Secure",
        ] {
            assert!(c.contains(part), "{c}");
        }
        let plain = Sessions::new(&ClusterSecret::new("s"), SESSION_TTL, false);
        assert!(plain.set_cookie("v", None).starts_with("cairn_ui=v;"));
        assert!(!plain.set_cookie("v", None).contains("Secure"));
        assert!(plain.clear_cookie(None).contains("Max-Age=0"));
    }

    #[test]
    fn origin_checks() {
        let h = |pairs: &[(&'static str, &'static str)]| {
            let mut m = HeaderMap::new();
            for (k, v) in pairs {
                m.append(*k, v.parse().unwrap());
            }
            m
        };
        assert!(same_origin(&h(&[("sec-fetch-site", "same-origin")]), None));
        assert!(!same_origin(
            &h(&[
                ("sec-fetch-site", "same-site"),
                ("origin", "http://a:1"),
                ("host", "a:1")
            ]),
            None
        ));
        assert!(!same_origin(&h(&[("sec-fetch-site", "cross-site")]), None));
        assert!(same_origin(
            &h(&[
                ("origin", "http://127.0.0.1:8080"),
                ("host", "127.0.0.1:8080")
            ]),
            None
        ));
        assert!(!same_origin(
            &h(&[
                ("origin", "http://127.0.0.1:9999"),
                ("host", "127.0.0.1:8080")
            ]),
            None
        ));
        assert!(!same_origin(&h(&[("origin", "null"), ("host", "a")]), None));
        assert!(!same_origin(&h(&[("host", "a")]), None));
        let mut two = h(&[("cookie", "x=1; cairn_ui=AAA"), ("cookie", "cairn_ui=BBB")]);
        two.append("cookie", "cairn_ui=".parse().unwrap());
        assert_eq!(
            session_cookies(&two, SESSION_COOKIE).collect::<Vec<_>>(),
            ["AAA", "BBB"]
        );
    }

    /// Trusted ranges, `X-Forwarded-*` only from a trusted peer, the last value wins,
    /// `Origin` must match the scheme too, the client IP is the rightmost address not
    /// inside a trusted range.
    #[test]
    fn trusted_proxies() {
        assert!(TrustedProxies::parse("").is_err());
        assert!(TrustedProxies::parse("10.0.0.0/33").is_err());
        assert!(TrustedProxies::parse("nope").is_err());
        let t = TrustedProxies::parse("10.0.0.0/8, fd00::/8,127.0.0.1").unwrap();
        let ip = |s: &str| s.parse::<IpAddr>().unwrap();
        assert!(t.contains(ip("10.1.2.3")));
        assert!(t.contains(ip("::ffff:10.1.2.3")), "v4-mapped v6");
        assert!(t.contains(ip("fd12::1")));
        assert!(t.contains(ip("127.0.0.1")));
        assert!(!t.contains(ip("127.0.0.2")));
        assert!(!t.contains(ip("11.0.0.1")));
        assert!(TrustedProxies::parse("0.0.0.0/0")
            .unwrap()
            .contains(ip("1.2.3.4")));

        let mut h = HeaderMap::new();
        h.append("x-forwarded-host", "evil.test, real.test".parse().unwrap());
        h.append("x-forwarded-proto", "HTTPS".parse().unwrap());
        h.append(
            "x-forwarded-for",
            "6.6.6.6, 1.2.3.4, 10.0.0.9".parse().unwrap(),
        );
        h.append("origin", "https://real.test".parse().unwrap());
        h.append("host", "10.0.0.5:8080".parse().unwrap());
        assert_eq!(t.forwarded(&h, Some(ip("11.0.0.1"))), None, "unknown peer");
        assert_eq!(t.forwarded(&h, None), None, "no peer");
        let f = t.forwarded(&h, Some(ip("10.0.0.2"))).unwrap();
        assert_eq!(f.host.as_deref(), Some("real.test"));
        assert_eq!(f.proto.as_deref(), Some("https"));
        assert!(same_origin(&h, Some(&f)));
        assert!(!same_origin(&h, None), "untrusted proxy: use Host");
        let http = Forwarded {
            proto: Some("http".into()),
            ..f.clone()
        };
        assert!(!same_origin(&h, Some(&http)), "wrong scheme");
        assert_eq!(t.client_ip(&h, Some(ip("10.0.0.2"))), Some(ip("1.2.3.4")));
        assert_eq!(t.client_ip(&h, Some(ip("8.8.8.8"))), Some(ip("8.8.8.8")));
        let s = Sessions::new(&ClusterSecret::new("s"), SESSION_TTL, false);
        assert!(s.set_cookie("v", Some(&f)).contains("; Secure"));
        assert!(!s.set_cookie("v", Some(&http)).contains("; Secure"));
    }
}
