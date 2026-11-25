// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Forwarding to the leader.
//!
//! Every path by id or by key asks the **local** engine (`Router::ask_range`) and
//! returns `503 {error, leader}` when the range is on another node. The SDK client
//! follows `leader` on its own; a browser does not (CSP `connect-src 'self'`, cookies
//! per origin) — the console on a cluster behind a load balancer would fail on every
//! range another node leads. This layer lets a request that **asks** for forwarding
//! (`x-cairn-forward: leader`) go on to the leader instead of getting `503`:
//!
//! 1. Run the request on this node as usual (body buffered, ≤ [`BODY_MAX`]).
//! 2. The response carries [`NotLeaderAt`] (attached by the "not the leader"
//!    `ApiError`) and the routing table knows another leader with an internal port ⇒
//!    send the request verbatim (method, path, original headers including `Host`,
//!    `Origin`, `Cookie`, `Authorization`, body) in the body of a `POST
//!    /internal/forward`, signed with the cluster secret, **once**, no retry.
//! 3. The leader decodes it, attaches [`Forwarded`] (an in-process extension — a client
//!    cannot set it), then runs the request on **its own ingress router**:
//!    authenticating again from the original headers like a normal request, checking
//!    the origin against the original `Host`. `Forwarded` ⇒ no further hop (one hop
//!    only): if the leader no longer leads either, it returns its own `503` as is.
//! 4. The leader's response (including an SSE body, copied as a stream) comes back
//!    intact, with `x-cairn-forwarded-by: <node>`.
//!
//! Outcomes when the leader cannot be reached ([`Outcome`]): not sent (connection
//! refused, the leader is an old binary without the route, signature refused) ⇒ return
//! the original `503` as is — nothing has run. Sent with no response (broken midway,
//! over the cap waiting for the head) ⇒ `504` with `id` when known: the operation **may**
//! have run (an ambiguous outcome); operations by id are all idempotent, the client
//! asks for the status again.
//!
//! No engine is held during the network call (D16): this layer sits outside the
//! handler, after the handler has answered.

use {
    super::AppState,
    axum::{
        body::{Body, Bytes},
        extract::{Request, State},
        http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri},
        middleware::Next,
        response::{IntoResponse, Response},
        Json,
    },
    cairn_store::PartitionId,
    std::{
        sync::atomic::{AtomicU64, Ordering},
        time::Duration,
    },
};

/// The header a client sets to ask for forwarding. A value other than `leader` ⇒ as if absent.
pub const FORWARD_HEADER: &str = "x-cairn-forward";
/// A header **inside the signed envelope** of `/internal/forward` carrying the client IP
/// the receiving node resolved (after its `--trust-proxy`). The leader sets it as the
/// peer of the rebuilt request (audit, per-IP Bearer cap). A copy sent by the client
/// itself is dropped at encoding; an old binary does not send it ⇒ no peer, as before.
const CLIENT_HEADER: &str = "x-cairn-forwarded-client";
/// A header **inside the signed envelope** carrying the `X-Forwarded-Proto` the
/// receiving node trusted (a proxy in its `--trust-proxy`). The leader builds the proxy
/// statement of the rebuilt request from it ([`crate::auth::RelayedForwarded`]): the
/// session cookie name (`__Host-cairn_key` or `cairn_ui`) and `Secure` exactly as the
/// receiving node chose them. A copy sent by the client itself is dropped at encoding.
const PROTO_HEADER: &str = "x-cairn-forwarded-proto";

/// The header the leader attaches to a response that went through forwarding: which node
/// answered. Absent on a `/internal/forward` response ⇒ the leader did not run the request.
pub const FORWARDED_BY_HEADER: &str = "x-cairn-forwarded-by";

/// Cap on the body buffered to run twice — equal to axum's `DefaultBodyLimit`, the cap
/// every ingress extractor is under: a larger body is a `413` anyway.
const BODY_MAX: usize = 2 * 1024 * 1024;

/// Extra wait on top of the ingress's own longest wait cap (`/invoke` waiting for the
/// invocation to stop, attach waiting for the result) before the response head is
/// considered lost.
const HEAD_SLACK: Duration = Duration::from_secs(30);

/// The `503` "not the leader" response of this range — `ApiError` attaches it when it knows the
/// range.
#[derive(Clone, Debug)]
pub(super) struct NotLeaderAt {
    pub partition: PartitionId,
    /// The invocation the error is about, so an ambiguous `504` still carries it.
    pub id: Option<String>,
}

/// This request has already gone through one forwarding hop — no further hop. Only set
/// in process ([`internal_forward`]); no header can turn it on.
#[derive(Clone, Copy, Debug)]
pub(super) struct Forwarded;

/// Counts per outcome for `cairnflow_forward_total{result}`.
pub static RELAYED: AtomicU64 = AtomicU64::new(0);
pub static REFUSED: AtomicU64 = AtomicU64::new(0);
pub static AMBIGUOUS: AtomicU64 = AtomicU64::new(0);

/// Whether the request asks for forwarding and has not gone through any hop yet.
pub(super) fn wanted(headers: &HeaderMap, ext: &axum::http::Extensions) -> bool {
    ext.get::<Forwarded>().is_none()
        && headers
            .get(FORWARD_HEADER)
            .is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"leader"))
}

/// The ingress router layer — see the module head.
pub(super) async fn layer(State(st): State<AppState>, req: Request, next: Next) -> Response {
    if st.cluster_secret.is_none() || !wanted(req.headers(), req.extensions()) {
        return next.run(req).await;
    }
    let (parts, body) = req.into_parts();
    let body = match axum::body::to_bytes(body, BODY_MAX).await {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(serde_json::json!({ "error": format!("cannot read body: {e}") })),
            )
                .into_response()
        }
    };
    let (method, uri, headers) = (
        parts.method.clone(),
        parts.uri.clone(),
        parts.headers.clone(),
    );
    // The client IP as this node sees it: the peer, or the address in a trusted proxy's
    // `X-Forwarded-For`.
    let (client, relayed) = {
        let peer = crate::auth::peer_ip(&parts.extensions);
        match &st.auth().sessions {
            Some(s) => (
                s.trusted().client_ip(&parts.headers, peer),
                // A trusted proxy's statement: the effective host and the proto.
                s.trusted().forwarded(&parts.headers, peer),
            ),
            None => (peer, None),
        }
    };
    let local = next
        .run(Request::from_parts(parts, Body::from(body.clone())))
        .await;
    let Some(at) = local.extensions().get::<NotLeaderAt>().cloned() else {
        return local;
    };
    let Some(addr) = leader_address(&st, at.partition) else {
        return local;
    };
    match send_from(
        &st,
        &addr,
        &method,
        &uri,
        &headers,
        body,
        client,
        relayed.as_ref(),
    )
    .await
    {
        Outcome::Relayed(r) => r,
        Outcome::Refused => local,
        Outcome::Ambiguous(why) => ambiguous(why, at.id),
    }
}

/// The internal port of range `p`'s leader, when the table knows a leader **other** than this node.
pub(super) fn leader_address(st: &AppState, p: PartitionId) -> Option<String> {
    let l = st.router.routing.leader_of(p)?;
    if st.router.node_id.as_deref() == Some(l.node_id.as_str()) {
        return None;
    }
    l.address
}

/// The outcome of one send.
pub(super) enum Outcome {
    /// The leader ran the request; this is its response.
    Relayed(Response),
    /// Nothing ran on the leader — answer with this node's original response.
    Refused,
    /// Sent, outcome unknown.
    Ambiguous(String),
}

pub(super) fn ambiguous(why: String, id: Option<String>) -> Response {
    let mut body = serde_json::json!({
        "error": format!(
            "forward to leader got no response ({why}) — the operation may have run; check its state again before resending"
        ),
    });
    if let Some(id) = id {
        body["id"] = id.into();
    }
    (StatusCode::GATEWAY_TIMEOUT, Json(body)).into_response()
}

/// Sends a request to `/internal/forward` of `addr` (the leader's internal port).
pub(super) async fn send(
    st: &AppState,
    addr: &str,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: Bytes,
    client: Option<std::net::IpAddr>,
) -> Outcome {
    send_from(st, addr, method, uri, headers, body, client, None).await
}

/// Like [`send`], with the request's trusted proxy statement (effective host; proto).
#[allow(clippy::too_many_arguments)]
async fn send_from(
    st: &AppState,
    addr: &str,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: Bytes,
    client: Option<std::net::IpAddr>,
    relayed: Option<&crate::auth::Forwarded>,
) -> Outcome {
    let Some(secret) = st.cluster_secret.as_ref() else {
        return Outcome::Refused;
    };
    let url = format!("{}/internal/forward", addr.trim_end_matches('/'));
    let wire = encode_with_host(method, uri, headers, &body, client, relayed);
    let rb = st.client.post(url).body(wire);
    let bound = head_bound(st);
    let got = cairn_store::retry::deadline(
        &cairn_store::retry::ops::INGRESS_FORWARD,
        bound,
        secret.send(&st.client, rb),
    )
    .await;
    let resp = match got {
        Err(e) => {
            AMBIGUOUS.fetch_add(1, Ordering::Relaxed);
            return Outcome::Ambiguous(e.to_string());
        }
        // Connection failed: the request never left this node.
        Ok(Err(e)) if e.is_connect() || e.is_builder() => {
            REFUSED.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(error = %e, addr, "cannot connect to the leader to forward");
            return Outcome::Refused;
        }
        Ok(Err(e)) => {
            AMBIGUOUS.fetch_add(1, Ordering::Relaxed);
            return Outcome::Ambiguous(e.to_string());
        }
        Ok(Ok(r)) => r,
    };
    // No marker ⇒ the leader did not run the request (an old binary without the route,
    // signature refused, its router not ready yet).
    let Some(by) = resp.headers().get(FORWARDED_BY_HEADER).cloned() else {
        REFUSED.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(status = %resp.status(), addr, "leader did not accept the forward");
        return Outcome::Refused;
    };
    RELAYED.fetch_add(1, Ordering::Relaxed);
    let mut out = Response::builder().status(resp.status());
    if let Some(h) = out.headers_mut() {
        for (k, v) in resp.headers() {
            if !hop_by_hop(k) {
                h.append(k.clone(), v.clone());
            }
        }
        h.insert(FORWARDED_BY_HEADER, by);
    }
    // The body is copied in chunks: the leader's SSE response passes through as a stream.
    Outcome::Relayed(
        out.body(Body::from_stream(resp.bytes_stream()))
            .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response()),
    )
}

/// Runs **the same request** on every other node leading at least one range
/// (`scope=cluster` of views that only see the part of the node running them — the
/// operation log, trial replay). Each node authenticates again from the original
/// headers like normal forwarding, runs with [`Forwarded`] (one hop, no further
/// spreading), and `scope` is removed from the query. In parallel. Returns `(node,
/// JSON)` of the nodes answering `2xx`, and the names of nodes with no part.
pub(super) async fn to_peers(
    st: &AppState,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: Bytes,
) -> (Vec<(String, serde_json::Value)>, Vec<String>) {
    let me = st.router.node_id.as_deref();
    let mut peers: std::collections::BTreeMap<String, Option<String>> = Default::default();
    for (_, l) in st.router.routing.snapshot() {
        if Some(l.node_id.as_str()) == me {
            continue;
        }
        let slot = peers.entry(l.node_id).or_default();
        if slot.is_none() {
            *slot = l.address;
        }
    }
    let uri = without_scope(uri);
    let asks = peers.into_iter().map(|(node, addr)| {
        let (uri, body) = (uri.clone(), body.clone());
        async move {
            let Some(addr) = addr else {
                return (node, None);
            };
            let got = match send(st, &addr, method, &uri, headers, body, None).await {
                Outcome::Relayed(r) if r.status().is_success() => {
                    axum::body::to_bytes(r.into_body(), 64 << 20)
                        .await
                        .ok()
                        .and_then(|b| serde_json::from_slice(&b).ok())
                }
                _ => None,
            };
            (node, got)
        }
    });
    let mut parts = Vec::new();
    let mut missing = Vec::new();
    for (node, got) in futures::future::join_all(asks).await {
        match got {
            Some(v) => parts.push((node, v)),
            None => missing.push(node),
        }
    }
    (parts, missing)
}

/// `uri` without every `scope=…` pair of the query — the part run on a peer is its own part.
fn without_scope(uri: &Uri) -> Uri {
    let Some(q) = uri.query() else {
        return uri.clone();
    };
    let kept: Vec<&str> = q
        .split('&')
        .filter(|kv| !kv.is_empty() && kv.split('=').next() != Some("scope"))
        .collect();
    let pq = if kept.is_empty() {
        uri.path().to_string()
    } else {
        format!("{}?{}", uri.path(), kept.join("&"))
    };
    pq.parse().unwrap_or_else(|_| uri.clone())
}

/// Cap on waiting for the leader's response head: the ingress's longest wait cap plus
/// [`HEAD_SLACK`]. The body (SSE) has no cap — the client closing cancels it.
fn head_bound(st: &AppState) -> Duration {
    st.invoke_wait
        .unwrap_or_default()
        .max(st.attach_max_wait)
        .saturating_add(HEAD_SLACK)
}

fn hop_by_hop(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-connection"
            | "transfer-encoding"
            | "upgrade"
            | "te"
            | "trailer"
            | "content-length"
    )
}

/// `POST /internal/forward` — the leader side. Only accepts requests signed with the
/// cluster secret (internal port). A response **without** [`FORWARDED_BY_HEADER`] ⇒
/// the request did not run.
pub(super) async fn internal_forward(State(st): State<AppState>, body: Bytes) -> Response {
    let Some(mut req) = decode(&body) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "malformed forward body" })),
        )
            .into_response();
    };
    let Some(router) = st.ingress.get() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "ingress router not ready yet" })),
        )
            .into_response();
    };
    req.extensions_mut().insert(Forwarded);
    let mut resp = {
        use tower_service::Service;
        let mut router = router;
        match router.call(req).await {
            Ok(r) => r,
            Err(never) => match never {},
        }
    };
    let by = st.router.node_id.as_deref().unwrap_or("-");
    if let Ok(v) = HeaderValue::from_str(by) {
        resp.headers_mut().insert(FORWARDED_BY_HEADER, v);
    }
    resp
}

// ─────────────────────────────────────────────────────────── wire

/// `lp16(method) ‖ lp32(path?query) ‖ u16 header count ‖ (lp16(name) ‖
/// lp32(value))* ‖ body` — big-endian. Hop-by-hop headers (`connection`, …) and
/// `x-cairn-forward` do not travel.
/// Encodes the request with the client IP the receiving node resolved: the client's
/// [`CLIENT_HEADER`] is dropped, the node's own (if any) added.
#[cfg(test)]
fn encode_from(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: &[u8],
    client: Option<std::net::IpAddr>,
) -> Vec<u8> {
    encode_with_host(method, uri, headers, body, client, None)
}

/// Like [`encode_from`], with the trusted proxy statement the receiving node resolved.
/// The effective host (`X-Forwarded-Host`) becomes the `Host` of the request rebuilt on
/// the leader — the leader does not trust that request's `X-Forwarded-*` (the peer is
/// the client), so without it an SSO session behind a proxy that rewrites `Host` would
/// be refused; `X-Forwarded-Host` does not travel. The proto travels in
/// [`PROTO_HEADER`].
fn encode_with_host(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: &[u8],
    client: Option<std::net::IpAddr>,
    relayed: Option<&crate::auth::Forwarded>,
) -> Vec<u8> {
    let host = relayed.and_then(|f| f.host.as_deref());
    let own_proto = relayed
        .and_then(|f| f.proto.as_deref())
        .and_then(|p| HeaderValue::from_str(p).ok())
        .map(|v| (HeaderName::from_static(PROTO_HEADER), v));
    let ip = client.map(|ip| ip.to_string());
    let pq = uri.path_and_query().map(|p| p.as_str()).unwrap_or("/");
    let own = ip
        .as_deref()
        .and_then(|v| HeaderValue::from_str(v).ok())
        .map(|v| (HeaderName::from_static(CLIENT_HEADER), v));
    let own_host = host
        .and_then(|h| HeaderValue::from_str(h).ok())
        .map(|v| (axum::http::header::HOST, v));
    let replace_host = own_host.is_some();
    let kept: Vec<(&HeaderName, &HeaderValue)> = headers
        .iter()
        .filter(|(k, _)| {
            !hop_by_hop(k)
                && k.as_str() != FORWARD_HEADER
                && k.as_str() != CLIENT_HEADER
                && k.as_str() != PROTO_HEADER
                && !(replace_host
                    && (*k == axum::http::header::HOST || k.as_str() == "x-forwarded-host"))
        })
        .chain(own.as_ref().map(|(k, v)| (k, v)))
        .chain(own_host.as_ref().map(|(k, v)| (k, v)))
        .chain(own_proto.as_ref().map(|(k, v)| (k, v)))
        .collect();
    let mut b = Vec::with_capacity(body.len() + pq.len() + 64 * kept.len() + 16);
    let m = method.as_str().as_bytes();
    b.extend_from_slice(&(m.len() as u16).to_be_bytes());
    b.extend_from_slice(m);
    b.extend_from_slice(&(pq.len() as u32).to_be_bytes());
    b.extend_from_slice(pq.as_bytes());
    b.extend_from_slice(&(kept.len().min(u16::MAX as usize) as u16).to_be_bytes());
    for (k, v) in kept.into_iter().take(u16::MAX as usize) {
        b.extend_from_slice(&(k.as_str().len() as u16).to_be_bytes());
        b.extend_from_slice(k.as_str().as_bytes());
        b.extend_from_slice(&(v.as_bytes().len() as u32).to_be_bytes());
        b.extend_from_slice(v.as_bytes());
    }
    b.extend_from_slice(body);
    b
}

fn decode(raw: &[u8]) -> Option<Request> {
    let mut c = Cursor(raw);
    let method = Method::from_bytes(c.lp16()?).ok()?;
    let uri: Uri = std::str::from_utf8(c.lp32()?).ok()?.parse().ok()?;
    // Path only: the request runs on the leader's own router, and goes nowhere else.
    if uri.scheme().is_some() || uri.authority().is_some() {
        return None;
    }
    let n = c.u16()?;
    let mut headers = HeaderMap::new();
    for _ in 0..n {
        let k = HeaderName::from_bytes(c.lp16()?).ok()?;
        let v = HeaderValue::from_bytes(c.lp32()?).ok()?;
        headers.append(k, v);
    }
    // The proto the receiving node trusted becomes the rebuilt request's proxy statement.
    let proto = headers
        .remove(PROTO_HEADER)
        .and_then(|v| v.to_str().ok().map(str::to_ascii_lowercase));
    // The receiving node's client IP becomes the rebuilt request's peer.
    let client = headers.remove(CLIENT_HEADER).and_then(|v| {
        v.to_str()
            .ok()
            .and_then(|s| s.parse::<std::net::IpAddr>().ok())
    });
    let mut req = Request::new(Body::from(Bytes::copy_from_slice(c.0)));
    *req.method_mut() = method;
    *req.uri_mut() = uri;
    *req.headers_mut() = headers;
    if let Some(ip) = client {
        req.extensions_mut()
            .insert(axum::extract::ConnectInfo(std::net::SocketAddr::new(ip, 0)));
    }
    if let Some(proto) = proto {
        req.extensions_mut()
            .insert(crate::auth::RelayedForwarded(crate::auth::Forwarded {
                host: None,
                proto: Some(proto),
            }));
    }
    Some(req)
}

struct Cursor<'a>(&'a [u8]);

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        if self.0.len() < n {
            return None;
        }
        let (h, t) = self.0.split_at(n);
        self.0 = t;
        Some(h)
    }
    fn u16(&mut self) -> Option<u16> {
        Some(u16::from_be_bytes(self.take(2)?.try_into().ok()?))
    }
    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_be_bytes(self.take(4)?.try_into().ok()?))
    }
    fn lp16(&mut self) -> Option<&'a [u8]> {
        let n = self.u16()? as usize;
        self.take(n)
    }
    fn lp32(&mut self) -> Option<&'a [u8]> {
        let n = self.u32()? as usize;
        self.take(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_round_trip() {
        let mut h = HeaderMap::new();
        h.insert("host", HeaderValue::from_static("console.local:8080"));
        h.insert(
            "origin",
            HeaderValue::from_static("http://console.local:8080"),
        );
        h.append("cookie", HeaderValue::from_static("a=1"));
        h.append("cookie", HeaderValue::from_static("cairn_ui=x.y"));
        h.insert("connection", HeaderValue::from_static("keep-alive"));
        h.insert(FORWARD_HEADER, HeaderValue::from_static("leader"));
        let uri: Uri = "/invocations/ab/cancel?mode=signal".parse().unwrap();
        let wire = encode_from(&Method::POST, &uri, &h, b"{\"x\":1}", None);
        let req = decode(&wire).expect("decodes");
        assert_eq!(req.method(), Method::POST);
        assert_eq!(req.uri(), &uri);
        assert_eq!(req.headers().get("host").unwrap(), "console.local:8080");
        assert_eq!(req.headers().get_all("cookie").iter().count(), 2);
        assert!(req.headers().get("connection").is_none());
        assert!(req.headers().get(FORWARD_HEADER).is_none());
    }

    #[test]
    fn broken_wire_or_absolute_uri_rejected() {
        assert!(decode(&[]).is_none());
        assert!(decode(&[0, 4, b'P', b'O']).is_none());
        let abs: Uri = "http://evil.example/x".parse().unwrap();
        let wire = encode_from(&Method::GET, &abs, &HeaderMap::new(), b"", None);
        // `encode` only takes path?query — build by hand a body carrying an absolute URI.
        assert!(decode(&wire).is_some());
        let mut raw = Vec::new();
        raw.extend_from_slice(&3u16.to_be_bytes());
        raw.extend_from_slice(b"GET");
        let s = b"http://evil.example/x";
        raw.extend_from_slice(&(s.len() as u32).to_be_bytes());
        raw.extend_from_slice(s);
        raw.extend_from_slice(&0u16.to_be_bytes());
        assert!(decode(&raw).is_none());
    }

    #[test]
    fn strips_scope_from_query() {
        let u = |s: &str| s.parse::<Uri>().unwrap();
        assert_eq!(
            without_scope(&u("/ranges/ops?scope=cluster")),
            u("/ranges/ops")
        );
        assert_eq!(
            without_scope(&u(
                "/deployments/dp_a/replay-check?service=S&scope=cluster&sample=5"
            )),
            u("/deployments/dp_a/replay-check?service=S&sample=5")
        );
        assert_eq!(without_scope(&u("/x")), u("/x"));
    }

    #[test]
    fn asks_only_by_header_and_one_hop() {
        let mut h = HeaderMap::new();
        let mut ext = axum::http::Extensions::new();
        assert!(!wanted(&h, &ext));
        h.insert(FORWARD_HEADER, HeaderValue::from_static("other"));
        assert!(!wanted(&h, &ext));
        h.insert(FORWARD_HEADER, HeaderValue::from_static("Leader"));
        assert!(wanted(&h, &ext));
        ext.insert(Forwarded);
        assert!(!wanted(&h, &ext));
    }
}

#[cfg(test)]
mod client_ip_tests {
    use super::*;

    /// **A forwarded request carries the client IP to the leader.** `decode` builds a
    /// `Request` without `ConnectInfo` ⇒ the audit on the leader records `ip: null`, and
    /// the per-IP Bearer cap and `--trust-proxy` do not apply — exactly for the commands
    /// that go through forwarding.
    ///
    /// Must see: (1) the receiving node's envelope (`encode_from` with a client IP)
    /// decodes into a request carrying exactly that IP as its peer; (2) a header of the
    /// same name sent by the client itself does not pass (the IP cannot be faked at the
    /// leader); (3) an envelope without an IP (old binary) decodes into a request with no
    /// peer, as today.
    #[test]
    fn forwarded_envelope_carries_the_client_ip() {
        use std::net::{IpAddr, Ipv4Addr};
        let peer_of = |req: &Request| {
            req.extensions()
                .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
                .map(|c| c.0.ip())
        };
        let uri: Uri = "/invocations/x/cancel".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-cairn-forwarded-client",
            HeaderValue::from_static("6.6.6.6"),
        );
        let client = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 9));
        let raw = encode_from(&Method::POST, &uri, &headers, b"", Some(client));
        let req = decode(&raw).expect("envelope decodes");
        assert_eq!(peer_of(&req), Some(client), "the leader sees the client IP");
        let raw = encode_from(&Method::POST, &uri, &headers, b"", None);
        let req = decode(&raw).expect("envelope decodes");
        assert_eq!(peer_of(&req), None, "a client-sent header became the peer");
    }

    /// **Host part** — behind a proxy that rewrites `Host`, the receiving node learns the
    /// effective host through a trusted proxy's `X-Forwarded-Host`; the leader (its peer
    /// is the client, not the proxy) only sees the raw `Host` ⇒ a forwarded SSO session
    /// is refused (the hostname does not belong to the client). Must see: the envelope
    /// carries the effective host as the `Host` of the rebuilt request;
    /// `X-Forwarded-Host` does not pass.
    #[test]
    fn forwarded_envelope_carries_the_effective_host() {
        let uri: Uri = "/auth/whoami".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::HOST,
            HeaderValue::from_static("cairn-1:8080"),
        );
        headers.insert(
            "x-forwarded-host",
            HeaderValue::from_static("app.example.test"),
        );
        let raw = encode_with_host(
            &Method::GET,
            &uri,
            &headers,
            b"",
            None,
            Some(&crate::auth::Forwarded {
                host: Some("app.example.test".into()),
                proto: None,
            }),
        );
        let req = decode(&raw).expect("envelope decodes");
        assert_eq!(
            req.headers()
                .get(axum::http::header::HOST)
                .and_then(|v| v.to_str().ok()),
            Some("app.example.test")
        );
        assert!(req.headers().get("x-forwarded-host").is_none());
    }

    /// **The leader picks the session cookie name like the receiving node (security
    /// review).** The receiving node (its peer is a trusted proxy, `X-Forwarded-Proto:
    /// https`) reads `__Host-cairn_key`; the leader rebuilds the request with the client
    /// IP as its peer (not the proxy) ⇒ no proxy statement ⇒ `Secure` per configuration.
    /// `--ui-cookie-secure off` ⇒ the leader would read plain `cairn_ui` — a cookie
    /// someone else can set from a sibling origin.
    ///
    /// Must see: the envelope carries the trusted proto ⇒ the leader sees `https` and
    /// picks `__Host-cairn_key`; a proto header sent by the client itself does not
    /// become a proxy statement.
    #[test]
    fn forwarded_envelope_carries_the_trusted_proto() {
        use crate::auth::{ClusterSecret, Forwarded, Sessions, TrustedProxies};
        let s = Sessions::new(&ClusterSecret::new("s"), crate::auth::SESSION_TTL, false);
        let none = TrustedProxies::default();
        let uri: Uri = "/auth/whoami".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(PROTO_HEADER, HeaderValue::from_static("https"));
        // The receiving node's envelope trusts no proxy: the header the client sent itself is
        // dropped.
        let raw = encode_with_host(&Method::GET, &uri, &headers, b"", None, None);
        let req = decode(&raw).expect("envelope decodes");
        let fwd = none.forwarded_in(req.headers(), req.extensions());
        assert_eq!(fwd, None, "a client-sent proto became a proxy statement");
        assert_eq!(s.cookie_name(fwd.as_ref()), crate::auth::SESSION_COOKIE);
        // The receiving node trusts the proxy, proto https.
        let relayed = Forwarded {
            host: None,
            proto: Some("https".into()),
        };
        let raw = encode_with_host(
            &Method::GET,
            &uri,
            &HeaderMap::new(),
            b"",
            None,
            Some(&relayed),
        );
        let req = decode(&raw).expect("envelope decodes");
        let fwd = none.forwarded_in(req.headers(), req.extensions());
        assert_eq!(fwd.as_ref().and_then(|f| f.proto.as_deref()), Some("https"));
        assert_eq!(
            s.cookie_name(fwd.as_ref()),
            crate::auth::SESSION_COOKIE_SECURE,
            "the leader picks a different cookie name from the receiving node"
        );
    }
}
