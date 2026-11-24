// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Every HTTP request a node sends out (to another node, to an SDK) goes
//! through here.
//!
//! By default this is exactly `client.execute(req)`. With the `testkit`
//! feature, deterministic simulation installs an in-process network
//! ([`crate::netfilter::LocalNet`]) for its thread: a request to a registered
//! address is handed straight to the receiver's `Router`, through no socket —
//! a real loopback socket has kernel latency, and tokio's paused clock
//! advances by itself while waiting on it, so the order of events would change
//! between runs.
//!
//! # Egress with a policy
//!
//! Requests the engine makes **on behalf of a tenant** — discovery,
//! `REQUEST_RESPONSE` and `BIDI_STREAM` turns, trial replays — go through
//! [`TenantHttp`], not through the internal-path client
//! ([`crate::invoker::internal_http`]: forwarding to the leader, asking sibling
//! nodes — those paths *must* reach internal IPs). The tenant client:
//!
//! - `.no_proxy()`: reqwest reads `HTTP(S)_PROXY` by default, and then the
//!   resolver only sees the proxy's name and every filter below looks at the
//!   wrong destination;
//! - `redirect::Policy::none()`: a `302` to `169.254.169.254` is a new request
//!   that no URL check sees;
//! - a connect timeout ([`TENANT_CONNECT_TIMEOUT`]); discovery also has a limit
//!   on the whole request ([`crate::discovery::DISCOVERY_TIMEOUT`]) and a body
//!   size cap.
//!
//! Checked in **three places**, because each one sees only part of the
//! picture:
//!
//! 1. **URL** — [`execute_for_tenant`], *before* the sim's `LocalNet::route`
//!    branch: the scheme (`--egress-require-https`) and a host that is an IP
//!    literal (the resolver is never called for `http://169.254.169.254/`,
//!    `http://[::ffff:a9fe:a9fe]/`, or `http://2852039166/` — the `url` crate
//!    has already normalised decimal/hex forms into IPv4). The only layer that
//!    runs under `sim_cluster`.
//! 2. **DNS** — the client's resolver ([`PolicyResolver`]) filters every IP a
//!    domain name resolves to. The client connects to **exactly** the
//!    addresses the resolver returns, so there is no second resolution for DNS
//!    rebinding to slip in between the check and the connect.
//! 3. **The real peer** — a connector layer ([`PeerGuardLayer`]) reads the far
//!    address of the socket just opened (`HttpInfo::remote_addr`) and drops the
//!    connection before any HTTP byte is sent. The last safety net for any
//!    path around the two layers above (a later reqwest resolving again by
//!    itself, happy eyeballs). Only testable with real sockets
//!    (`tests/egress_policy.rs`), absent in the sim.
//!
//! Layers 2 and 3 are installed only when the policy has IP rules
//! ([`EgressPolicy::filters_ips`]): self-hosting by default runs exactly as
//! before.

use {
    ipnet::IpNet,
    std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
        time::Duration,
    },
};

/// Sends `req` with `client` (or over the sim's in-process network).
#[allow(
    clippy::disallowed_methods,
    reason = "the single seam every outgoing request goes through"
)]
pub(crate) async fn execute(
    client: &reqwest::Client,
    req: reqwest::Request,
) -> reqwest::Result<reqwest::Response> {
    #[cfg(feature = "testkit")]
    let req = match crate::netfilter::LocalNet::route(req) {
        Ok(local) => return local.await,
        Err(req) => req,
    };
    client.execute(req).await
}

/// Builds `req`, then [`execute`].
pub(crate) async fn send(
    client: &reqwest::Client,
    req: reqwest::RequestBuilder,
) -> reqwest::Result<reqwest::Response> {
    execute(client, req.build()?).await
}

// ───────────────────────────────────────────────────────── the policy

/// Machine-readable code of the `400` error when the egress policy refuses a destination.
pub const EGRESS_DENIED: &str = "egress_denied";

/// Time limit for opening a TCP connection (and the TLS handshake) to a tenant's endpoint.
pub const TENANT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// The ranges `--egress-deny-private` blocks: loopback, private networks,
/// link-local (cloud metadata), CGNAT, special-purpose ranges, multicast,
/// reserved — and every IPv6 form embedding an IPv4 (mapped, compatible,
/// NAT64, 6to4), because their far end can be any of the IPv4s above.
const PRIVATE: &[&str] = &[
    "0.0.0.0/8",
    "10.0.0.0/8",
    "100.64.0.0/10",
    "127.0.0.0/8",
    // Azure's platform address (WireServer): public range, answers only inside a VM.
    "168.63.129.16/32",
    "169.254.0.0/16",
    "172.16.0.0/12",
    "192.0.0.0/24",
    "192.168.0.0/16",
    "198.18.0.0/15",
    "224.0.0.0/4",
    "240.0.0.0/4",
    // `::/128`, `::1` and IPv4-compatible (`::a.b.c.d`, deprecated but still parsed).
    "::/96",
    "::ffff:0:0/96",
    "64:ff9b::/96",
    "64:ff9b:1::/48",
    "100::/64",
    "2002::/16",
    "fc00::/7",
    "fe80::/10",
    "fec0::/10",
    "ff00::/8",
];

/// Why the policy refuses a destination. The message says **why**, never echoing
/// anything from the destination's side, the address a name resolved to, or the
/// blocked range: it reaches the tenant (`400 egress_denied`, an invocation's last
/// failure), who could otherwise map internal DNS and the operator's deny list.
/// The address and range go to the node's `debug` log.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("egress_denied: {0}")]
pub struct Denied(pub String);

/// The node's egress policy (`--egress-*` flags). The default lets everything
/// through: self-hosting behaviour as before the policy existed.
#[derive(Clone, Default)]
pub struct EgressPolicy {
    /// `--egress-deny-private`: block [`PRIVATE`].
    pub deny_private: bool,
    /// `--egress-require-https`: accept only `https://`.
    pub require_https: bool,
    /// `--egress-allow-cidr` (repeatable): deliberate internal networks, wins
    /// over [`PRIVATE`].
    pub allow: Vec<IpNet>,
    /// `--egress-deny-cidr` (repeatable): the cell's own ingress/ALB addresses.
    /// Wins over everything, `allow` included, and applies even when
    /// `deny_private` is off.
    pub deny: Vec<IpNet>,
    /// The tenant client's domain-name resolver. `None` = the system's
    /// `getaddrinfo`. Only tests set it (a fake resolver for DNS rebinding,
    /// `tests/egress_policy.rs`).
    pub resolver: Option<Resolver>,
    /// TLS roots **replacing** the system root store for the tenant client — so
    /// tests can build a fake `https://` edge (`tests/common/tls_edge.rs`).
    /// Empty = the system store (`rustls-platform-verifier`). Only `testkit`
    /// builds have this field; a `testkit` binary reads it from
    /// [`TLS_ROOTS_ENV`].
    #[cfg(feature = "testkit")]
    pub tls_roots: Vec<reqwest::Certificate>,
}

/// Environment variable a `testkit` binary reads into
/// [`EgressPolicy::tls_roots`]: the path of a PEM file. Release builds have no
/// such path.
#[cfg(feature = "testkit")]
pub const TLS_ROOTS_ENV: &str = "CAIRNFLOW_TESTKIT_TLS_ROOTS";

impl std::fmt::Debug for EgressPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EgressPolicy")
            .field("deny_private", &self.deny_private)
            .field("require_https", &self.require_https)
            .field("allow", &self.allow)
            .field("deny", &self.deny)
            .field("resolver", &self.resolver.as_ref().map(|_| "<custom>"))
            .finish_non_exhaustive()
    }
}

impl EgressPolicy {
    /// Whether the policy has any IP-based rule — if not, the tenant client
    /// installs no resolver/connector of its own.
    pub fn filters_ips(&self) -> bool {
        self.deny_private || !self.deny.is_empty()
    }

    /// Whether there is any rule at all (`require_https` included).
    pub fn is_permissive(&self) -> bool {
        !self.filters_ips() && !self.require_https
    }

    /// Whether `ip` may be connected to.
    pub fn check_ip(&self, ip: IpAddr) -> Result<(), Denied> {
        // An IPv4 embedded in IPv6 is also compared with `deny`/`allow` in its
        // IPv4 form: `--egress-deny-cidr 10.1.2.3/32` must also block
        // `::ffff:10.1.2.3`.
        let forms = [Some(ip), embedded_v4(ip).map(IpAddr::V4)];
        let forms = forms.iter().flatten();
        if let Some(net) = forms
            .clone()
            .find_map(|a| self.deny.iter().find(|n| n.contains(a)))
        {
            tracing::debug!(%ip, %net, "egress: address in --egress-deny-cidr");
            return Err(Denied(
                "the destination address is blocked by --egress-deny-cidr".into(),
            ));
        }
        if !self.deny_private {
            return Ok(());
        }
        if self.allow.iter().any(|n| n.contains(&ip)) {
            return Ok(());
        }
        if let Some(net) = private_nets().find(|n| n.contains(&ip)) {
            tracing::debug!(%ip, %net, "egress: internal address");
            return Err(Denied(
                "the destination is an internal address (--egress-deny-private)".into(),
            ));
        }
        Ok(())
    }

    /// Layer 1: the scheme and literal host of `url`.
    pub fn check_url(&self, url: &reqwest::Url) -> Result<(), Denied> {
        match url.scheme() {
            "https" => {}
            "http" if !self.require_https => {}
            s => {
                return Err(Denied(format!(
                    "scheme {s:?} is not allowed{}",
                    if self.require_https {
                        " (--egress-require-https: https only)"
                    } else {
                        ""
                    }
                )))
            }
        }
        match url.host() {
            Some(url::Host::Ipv4(a)) => self.check_ip(IpAddr::V4(a)),
            Some(url::Host::Ipv6(a)) => self.check_ip(IpAddr::V6(a)),
            Some(url::Host::Domain(d)) => {
                // A name every resolver maps to loopback: block it at once, no resolution needed.
                let d = d.trim_end_matches('.').to_ascii_lowercase();
                if self.deny_private && (d == "localhost" || d.ends_with(".localhost")) {
                    return Err(Denied(format!("{d} is loopback")));
                }
                Ok(())
            }
            None => Err(Denied("URL has no host".into())),
        }
    }
}

fn private_nets() -> impl Iterator<Item = IpNet> + Clone {
    PRIVATE
        .iter()
        .map(|s| s.parse::<IpNet>().expect("constant range is valid"))
}

/// The IPv4 embedded in `ip`: mapped (`::ffff:a.b.c.d`), compatible
/// (`::a.b.c.d`), NAT64 (`64:ff9b::/96`), 6to4 (`2002:aabb:ccdd::/48`).
fn embedded_v4(ip: IpAddr) -> Option<Ipv4Addr> {
    let IpAddr::V6(v6) = ip else { return None };
    if let Some(v4) = v6.to_ipv4() {
        return Some(v4);
    }
    let s = v6.segments();
    let low = |a: u16, b: u16| Ipv4Addr::from(((a as u32) << 16) | b as u32);
    if s[..6] == [0x64, 0xff9b, 0, 0, 0, 0] {
        return Some(low(s[6], s[7]));
    }
    if s[0] == 0x2002 {
        return Some(low(s[1], s[2]));
    }
    None
}

/// Error of a request on behalf of a tenant: refused by the policy (before any
/// byte left the node), or an ordinary HTTP error.
#[derive(Debug, thiserror::Error)]
pub enum TenantError {
    #[error(transparent)]
    Denied(#[from] Denied),
    #[error(transparent)]
    Http(#[from] reqwest::Error),
}

impl TenantError {
    /// An HTTP error whose root is a refusal by layer 2 or 3 is also `Denied`.
    pub fn denied(&self) -> Option<&Denied> {
        match self {
            TenantError::Denied(d) => Some(d),
            TenantError::Http(e) => denied_in(e),
        }
    }
}

/// `Denied` is in the `source()` chain of `e` (refused by the resolver/connector).
pub fn denied_in<'a>(e: &'a (dyn std::error::Error + 'static)) -> Option<&'a Denied> {
    let mut cur: Option<&'a (dyn std::error::Error + 'static)> = Some(e);
    while let Some(err) = cur {
        if let Some(d) = err.downcast_ref::<Denied>() {
            return Some(d);
        }
        cur = err.source();
    }
    None
}

// ───────────────────────────────────────────────────────── client tenant

/// An HTTP client for requests on behalf of tenants, with its policy.
/// `Clone` is cheap.
#[derive(Clone, Debug)]
pub struct TenantHttp {
    client: reqwest::Client,
    policy: Arc<EgressPolicy>,
}

/// The tenant client's domain-name resolver. The default is the system's
/// `getaddrinfo` (`tokio::net::lookup_host`); tests substitute a fake resolver
/// (rebinding).
pub type Resolver = Arc<dyn reqwest::dns::Resolve>;

impl TenantHttp {
    /// The h1 client (discovery, `REQUEST_RESPONSE`, trial replays).
    pub fn h1(policy: Arc<EgressPolicy>) -> Self {
        let r = policy.resolver.clone();
        Self::build(policy, false, r, true)
    }

    /// Client h2c prior-knowledge (`BIDI_STREAM`).
    pub fn h2c(policy: Arc<EgressPolicy>) -> Self {
        let r = policy.resolver.clone();
        Self::build(policy, true, r, true)
    }

    /// Like [`Self::h1`], with a given resolver. `filter_dns = false` only to
    /// test layer 3 on its own (the resolver hands a blocked IP straight to the
    /// connector).
    pub fn h1_with_resolver(
        policy: Arc<EgressPolicy>,
        resolver: Resolver,
        filter_dns: bool,
    ) -> Self {
        Self::build(policy, false, Some(resolver), filter_dns)
    }

    fn build(
        policy: Arc<EgressPolicy>,
        h2c: bool,
        resolver: Option<Resolver>,
        filter_dns: bool,
    ) -> Self {
        let mut b = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(TENANT_CONNECT_TIMEOUT);
        b = if h2c {
            // No total timeout: a bidi turn lives exactly as long as the
            // handler runs (the no-progress limit is the handler's
            // `inactivityTimeout`, in `turn/`).
            b.http2_prior_knowledge()
        } else {
            b.pool_idle_timeout(crate::invoker::SDK_POOL_IDLE)
        };
        if policy.filters_ips() || resolver.is_some() {
            let inner = resolver.unwrap_or_else(|| Arc::new(SystemResolver));
            b = b.dns_resolver(Arc::new(PolicyResolver {
                inner,
                policy: filter_dns.then(|| Arc::clone(&policy)),
            }));
        }
        if policy.filters_ips() {
            b = b.connector_layer(PeerGuardLayer(Arc::clone(&policy)));
        }
        #[cfg(feature = "testkit")]
        if !policy.tls_roots.is_empty() {
            b = b.tls_certs_only(policy.tls_roots.iter().cloned());
        }
        Self {
            client: b
                .build()
                .expect("a client without custom TLS has nothing to fail"),
            policy,
        }
    }

    pub fn client(&self) -> &reqwest::Client {
        &self.client
    }

    pub fn policy(&self) -> &EgressPolicy {
        &self.policy
    }
}

/// Sends a request on behalf of a tenant: layer 1
/// ([`EgressPolicy::check_url`]), then [`execute`] — checked **before** the
/// `LocalNet::route` branch so this layer runs under the sim.
pub(crate) async fn execute_for_tenant(
    http: &TenantHttp,
    req: reqwest::Request,
) -> Result<reqwest::Response, TenantError> {
    http.policy.check_url(req.url())?;
    Ok(execute(&http.client, req).await?)
}

/// Builds `req`, then [`execute_for_tenant`].
pub(crate) async fn send_for_tenant(
    http: &TenantHttp,
    req: reqwest::RequestBuilder,
) -> Result<reqwest::Response, TenantError> {
    execute_for_tenant(http, req.build()?).await
}

/// The system's `getaddrinfo`, like reqwest's default resolver.
struct SystemResolver;

impl reqwest::dns::Resolve for SystemResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_string();
        Box::pin(async move {
            let addrs: Vec<SocketAddr> =
                tokio::net::lookup_host((host.as_str(), 0)).await?.collect();
            Ok(Box::new(addrs.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

/// Layer 2: filters every IP the inner resolver returns. No IP left ⇒ a
/// [`Denied`] error (the client connects to nothing).
struct PolicyResolver {
    inner: Resolver,
    /// `None` ⇒ no filtering (only for testing layer 3).
    policy: Option<Arc<EgressPolicy>>,
}

impl reqwest::dns::Resolve for PolicyResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_string();
        let (got, policy) = (self.inner.resolve(name), self.policy.clone());
        Box::pin(async move {
            let addrs: Vec<SocketAddr> = got.await?.collect();
            let Some(policy) = policy else {
                return Ok(Box::new(addrs.into_iter()) as reqwest::dns::Addrs);
            };
            let mut denied = None;
            let kept: Vec<SocketAddr> = addrs
                .into_iter()
                .filter(|a| match policy.check_ip(a.ip()) {
                    Ok(()) => true,
                    Err(d) => {
                        denied.get_or_insert(d);
                        false
                    }
                })
                .collect();
            if kept.is_empty() {
                let why = denied.map_or_else(|| "no address".to_string(), |d| d.0);
                return Err(Box::new(Denied(format!("{host}: {why}")))
                    as Box<dyn std::error::Error + Send + Sync>);
            }
            Ok(Box::new(kept.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Layer 3: a connector layer checking the real peer address of the connection just opened.
#[derive(Clone)]
pub struct PeerGuardLayer(Arc<EgressPolicy>);

impl<S> tower_layer::Layer<S> for PeerGuardLayer {
    type Service = PeerGuard<S>;

    fn layer(&self, inner: S) -> Self::Service {
        PeerGuard {
            inner,
            policy: Arc::clone(&self.0),
        }
    }
}

#[derive(Clone)]
pub struct PeerGuard<S> {
    inner: S,
    policy: Arc<EgressPolicy>,
}

impl<S, R> tower_service::Service<R> for PeerGuard<S>
where
    S: tower_service::Service<R, Error = BoxError>,
    S::Response: hyper_util::client::legacy::connect::Connection,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = BoxError;
    type Future = Pin<Box<dyn std::future::Future<Output = Result<S::Response, BoxError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: R) -> Self::Future {
        let (fut, policy) = (self.inner.call(req), Arc::clone(&self.policy));
        Box::pin(async move {
            use hyper_util::client::legacy::connect::Connection as _;
            let conn = fut.await?;
            let mut ext = axum::http::Extensions::new();
            conn.connected().get_extras(&mut ext);
            let Some(info) = ext.get::<hyper_util::client::legacy::connect::HttpInfo>() else {
                // Unknown far end (not TCP): cannot prove it is allowed ⇒ do
                // not use it.
                return Err(Box::new(Denied("cannot read peer address".into())) as BoxError);
            };
            policy
                .check_ip(info.remote_addr().ip())
                .map_err(|d| Box::new(d) as BoxError)?;
            Ok(conn)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deny_private() -> EgressPolicy {
        EgressPolicy {
            deny_private: true,
            ..Default::default()
        }
    }

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    /// `168.63.129.16` is Azure's platform address (WireServer, instance metadata
    /// fallback): a public-range IP that only answers from inside a VM, like
    /// `169.254.169.254`. Its neighbours are ordinary public addresses.
    #[test]
    fn azure_platform_address_is_denied() {
        let p = deny_private();
        assert!(p.check_ip(ip("168.63.129.16")).is_err());
        assert!(p.check_ip(ip("::ffff:168.63.129.16")).is_err());
        assert!(p.check_ip(ip("168.63.129.15")).is_ok());
        assert!(p.check_ip(ip("168.63.129.17")).is_ok());
    }

    /// The refusal reaches the tenant (`400 egress_denied`, an invocation's last
    /// failure), so it names neither the address a name resolved to nor the
    /// blocked range: a tenant could otherwise register `x.internal.example`
    /// names one by one and map the cell's internal DNS and the operator's
    /// `--egress-deny-cidr` list from the error bodies.
    #[test]
    fn denial_names_neither_the_address_nor_the_range() {
        let p = EgressPolicy {
            deny_private: true,
            deny: vec!["203.0.113.0/24".parse().unwrap()],
            ..Default::default()
        };
        for (addr, hidden) in [
            ("10.1.2.3", ["10.1.2.3", "10.0.0.0"]),
            ("::ffff:10.1.2.3", ["10.1.2.3", "10.0.0.0"]),
            ("169.254.169.254", ["169.254.169.254", "169.254.0.0"]),
            ("203.0.113.9", ["203.0.113.9", "203.0.113.0"]),
        ] {
            let d = p.check_ip(ip(addr)).unwrap_err().to_string();
            assert!(d.starts_with("egress_denied: "), "{d}");
            for h in hidden {
                assert!(!d.contains(h), "{addr}: the refusal names {h}: {d}");
            }
        }
    }

    /// Every blocked range, at both its first and its last address.
    #[test]
    fn every_listed_range_is_denied() {
        let p = deny_private();
        for a in [
            "0.0.0.0",
            "0.255.255.255",
            "10.0.0.1",
            "10.255.255.255",
            "100.64.0.1",
            "100.127.255.255",
            "127.0.0.1",
            "127.255.255.254",
            "169.254.169.254",
            "172.16.0.1",
            "172.31.255.255",
            "192.0.0.8",
            "192.168.1.1",
            "198.18.0.1",
            "198.19.255.255",
            "224.0.0.1",
            "239.255.255.255",
            "240.0.0.1",
            "255.255.255.255",
            "::",
            "::1",
            "::127.0.0.1",
            "::ffff:169.254.169.254",
            "::ffff:8.8.8.8",
            "64:ff9b::a9fe:a9fe",
            "64:ff9b:1::1",
            "100::1",
            "2002:a9fe:a9fe::1",
            "fc00::1",
            "fdff::1",
            "fe80::1",
            "febf::1",
            "fec0::1",
            "ff02::1",
        ] {
            assert!(p.check_ip(ip(a)).is_err(), "{a} must be blocked");
        }
        for a in [
            "8.8.8.8",
            "1.1.1.1",
            "100.63.255.255",
            "100.128.0.0",
            "172.15.255.255",
            "172.32.0.0",
            "192.0.1.1",
            "198.17.255.255",
            "2001:4860:4860::8888",
            "2606:4700::1111",
        ] {
            assert!(p.check_ip(ip(a)).is_ok(), "{a} must pass");
        }
    }

    #[test]
    fn deny_cidr_wins_over_allow_and_applies_without_deny_private() {
        let p = EgressPolicy {
            deny: vec!["10.1.2.3/32".parse().unwrap()],
            allow: vec!["10.0.0.0/8".parse().unwrap()],
            deny_private: true,
            ..Default::default()
        };
        assert!(p.check_ip(ip("10.1.2.3")).is_err());
        assert!(p.check_ip(ip("::ffff:10.1.2.3")).is_err());
        assert!(p.check_ip(ip("10.9.9.9")).is_ok());
        let only_deny = EgressPolicy {
            deny: vec!["203.0.113.0/24".parse().unwrap()],
            ..Default::default()
        };
        assert!(only_deny.filters_ips());
        assert!(only_deny.check_ip(ip("203.0.113.7")).is_err());
        assert!(only_deny.check_ip(ip("64:ff9b::cb00:7107")).is_err());
        assert!(only_deny.check_ip(ip("127.0.0.1")).is_ok());
    }

    /// IP literals in every form `url` accepts are normalised before comparing.
    #[test]
    fn literal_hosts_in_every_spelling_are_checked() {
        let p = deny_private();
        for u in [
            "http://169.254.169.254/latest/meta-data",
            "http://2852039166/",
            "http://0xa9fea9fe/",
            "http://0251.0376.0251.0376/",
            "http://169.254.43518/",
            "http://[::ffff:a9fe:a9fe]/",
            "http://[::ffff:169.254.169.254]/",
            "http://127.1/",
            "http://localhost:9080/discover",
            "http://LOCALHOST./",
            "http://api.localhost/",
            "http://[::1]:8080/",
        ] {
            let url = reqwest::Url::parse(u).unwrap();
            assert!(p.check_url(&url).is_err(), "{u} must be blocked");
        }
        assert!(p
            .check_url(&reqwest::Url::parse("https://sdk.example.com/").unwrap())
            .is_ok());
    }

    #[test]
    fn require_https_refuses_plain_http() {
        let p = EgressPolicy {
            require_https: true,
            ..Default::default()
        };
        assert!(p
            .check_url(&reqwest::Url::parse("http://sdk.example.com/").unwrap())
            .is_err());
        assert!(p
            .check_url(&reqwest::Url::parse("https://sdk.example.com/").unwrap())
            .is_ok());
        // Default: http is still allowed (self-hosting, e2e calls localhost).
        assert!(EgressPolicy::default()
            .check_url(&reqwest::Url::parse("http://localhost:9080/").unwrap())
            .is_ok());
    }
}
