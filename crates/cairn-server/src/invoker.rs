// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Calling down into the SDK — both modes.
//!
//! **Why `REQUEST_RESPONSE` was done first.** It is the mode with narrower
//! semantics and needs no full-duplex HTTP/2: the runtime sends the whole
//! journal, the SDK returns a whole response. Everything hard about
//! `BIDI_STREAM` — both directions running in parallel, `REQUESTED_ACK`,
//! `ctx.run` not ending the turn — is an *addition*, not a *change*. Doing the
//! narrow one first means the wide one needs no rewrite, and [`call`] below
//! stays intact line for line.
//!
//! **`BIDI_STREAM` is HTTP/2 h2c, not HTTP/1.1.** Two directions on one h1
//! connection (the client sending the request body while reading the
//! response) are not reliable: hyper's client does not guarantee full duplex
//! on h1. So bidi requires HTTP/2, as Restate's service protocol also
//! specifies. The consequence: **two** clients (see [`Clients`]) — discovery
//! reads the manifest *before* knowing the mode, so it must run on h1 as
//! before.
//!
//! The protocol describes the transport but **names no content-type for the
//! `/invoke` endpoint**. We choose a value and record it here.

use {
    crate::{
        lease::Released,
        metrics::sdk::{Dir::*, Mode::*},
    },
    bytes::{Bytes, BytesMut},
    cairn_protocol::{Frame, FrameDecoder, Message, ProtocolError},
    std::{
        pin::Pin,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
        task::{Context, Poll},
    },
};

/// Content-type of the message stream on the `/invoke` endpoint.
pub const INVOCATION_V1: &str = "application/vnd.cairnflow.invocation.v1";
/// v2: adds `scope`/`limit_key` for flow control. See
/// [`crate::discovery::WIRE_MAX`].
pub const INVOCATION_V2: &str = "application/vnd.cairnflow.invocation.v2";
/// v3: `Keepalive` and `Start.keepalive_interval_ms` / `replay_only`.
pub const INVOCATION_V3: &str = "application/vnd.cairnflow.invocation.v3";
/// v4: the `StreamAppend` and `ArtifactPut` side channels.
pub const INVOCATION_V4: &str = "application/vnd.cairnflow.invocation.v4";
/// v5: incremental replay for `REQUEST_RESPONSE`.
pub const INVOCATION_V5: &str = "application/vnd.cairnflow.invocation.v5";
/// v6: segmented values sent down to the SDK (ref + read URL), `SetState`
/// reusing refs.
pub const INVOCATION_V6: &str = "application/vnd.cairnflow.invocation.v6";

/// The content-type for the wire version agreed at discovery.
///
/// **This is where the agreed version is told back to the SDK.** Without it the
/// SDK has no way to know whether the runtime at the other end understands
/// `scope` — and a `ctx.scoped` silently ignored is a cap that does not exist
/// while the user believes it does.
pub fn invocation_content_type(wire_version: u32) -> &'static str {
    match wire_version {
        0 | 1 => INVOCATION_V1,
        2 => INVOCATION_V2,
        3 => INVOCATION_V3,
        4 => INVOCATION_V4,
        5 => INVOCATION_V5,
        _ => INVOCATION_V6,
    }
}

/// Whether the content-type the SDK returns is a version we understand.
///
/// Accepts **both**: a valid v1 SDK returns v1 even when we sent v2 down, and
/// refusing it would refuse a correctly running deployment.
fn known_invocation_type(header: &str) -> bool {
    let media = header.split(';').next().unwrap_or("").trim();
    [
        INVOCATION_V1,
        INVOCATION_V2,
        INVOCATION_V3,
        INVOCATION_V4,
        INVOCATION_V5,
        INVOCATION_V6,
    ]
    .iter()
    .any(|v| media.eq_ignore_ascii_case(v))
}

/// Two HTTP clients, and why it must be two rather than one.
///
/// `http2_prior_knowledge` makes **every** request of a client go h2c from the
/// first byte, with no negotiation. That is exactly what `BIDI_STREAM` needs
/// and exactly what `/discover` cannot tolerate: the manifest is read before
/// we know which mode the deployment speaks, so discovery must stay on h1. One
/// client cannot do both jobs, hence two.
///
/// Both are **tenant** clients ([`crate::egress::TenantHttp`]): every call
/// down into the SDK is a request on behalf of a tenant, going through the
/// egress policy.
#[derive(Clone, Debug)]
pub struct Clients {
    /// h1 — `REQUEST_RESPONSE`.
    pub http: crate::egress::TenantHttp,
    /// h2c prior-knowledge — `BIDI_STREAM`.
    pub bidi: crate::egress::TenantHttp,
    /// The request-identity signer. No master ⇒ no signing.
    pub identity: crate::identity::Identity,
    /// The issuer of segment read URLs for SDK v6. Default `inline`: the shell
    /// assembles values as before.
    pub values: std::sync::Arc<crate::value_urls::ValueUrls>,
}

impl Default for Clients {
    fn default() -> Self {
        Self::new(Default::default(), Default::default())
    }
}

/// The identity a request down to the SDK carries: signed on behalf of
/// `tenant`, a given `jti`, the caller's time `now_ms` (the engine's
/// injectable clock). `aud` is the path of the request URL itself —
/// [`call`]/[`open_bidi`] compute it.
#[derive(Clone, Copy, Debug)]
pub struct Signing<'a> {
    pub identity: &'a crate::identity::Identity,
    pub tenant: &'a str,
    pub jti: &'a str,
    pub now_ms: u64,
}

impl Signing<'_> {
    /// Attaches identity headers to `req` (nothing when the node does not
    /// sign). `body`: the body **before** compression (BIDI: the body opening
    /// the turn) — claim `bsh`; `None` for requests without a body (discovery).
    pub(crate) fn apply(
        &self,
        url: &str,
        body: Option<&[u8]>,
        mut req: reqwest::RequestBuilder,
    ) -> reqwest::RequestBuilder {
        if !self.identity.signs() {
            return req;
        }
        let parsed = reqwest::Url::parse(url).ok();
        let aud = parsed
            .as_ref()
            .map(|u| u.path().to_string())
            .unwrap_or_default();
        // Exactly the `Host` header string reqwest sends: the host, plus
        // `:port` when the URL names a port other than the scheme's default.
        let host = parsed.as_ref().and_then(|u| {
            u.host_str().map(|h| match u.port() {
                Some(p) => format!("{h}:{p}"),
                None => h.to_string(),
            })
        });
        // A fresh nonce per request: two requests of the same turn in the same
        // second (two quick resumes of a paused BIDI invocation, a resend) must
        // not carry the same JWT — a verifying SDK opens one stream per JWT.
        let bound = crate::identity::Bound {
            host: host.as_deref(),
            body,
            nonce: Some(crate::identity::fresh_nonce()),
        };
        for (k, v) in self
            .identity
            .headers(self.tenant, &aud, self.jti, self.now_ms, bound)
        {
            req = req.header(k, v);
        }
        req
    }
}

/// An h1 connection to the SDK idle for longer than this is dropped by the
/// client, not reused.
///
/// It must be **shorter** than the SDK server's idle keep-alive timeout: if
/// the server closes first, a request sent at that very moment on the old
/// connection fails at the transport layer ("error sending request") before
/// the SDK runs anything — the turn fails, the retry carries `attempt` 1 (the
/// handler's first run does not see 0 as the `ctx.attempt` contract promises)
/// and a synchronous `/invoke` returns 502 for a healthy SDK. `node:http`
/// closes after 5 s (the default `keepAliveTimeout`) — the shortest among
/// common servers; reqwest's default is 90 s.
pub const SDK_POOL_IDLE: std::time::Duration = std::time::Duration::from_secs(4);

/// The h1 client of the cluster's **internal path**: forwarding to the leader
/// (`ingress/forward.rs`), asking sibling nodes (`ingress/observe.rs`). This
/// path *must* reach internal IPs, so it does not go through the tenants'
/// egress policy — and never carries a request on behalf of a tenant. Keeps
/// idle connections for at most [`SDK_POOL_IDLE`].
pub fn internal_http() -> reqwest::Client {
    reqwest::Client::builder()
        .pool_idle_timeout(SDK_POOL_IDLE)
        .build()
        .expect("an h1 client without TLS has nothing to fail")
}

impl Clients {
    /// The two tenant clients per `policy`.
    ///
    /// The h2c client has no total timeout: a bidi turn lives exactly as long
    /// as the handler runs, and handlers may run for a long time. A turn ends
    /// on Suspension or when one side closes the stream — not by the clock.
    pub fn with_value_urls(mut self, urls: std::sync::Arc<crate::value_urls::ValueUrls>) -> Self {
        self.values = urls;
        self
    }

    pub fn new(
        policy: std::sync::Arc<crate::egress::EgressPolicy>,
        identity: crate::identity::Identity,
    ) -> Self {
        Self {
            http: crate::egress::TenantHttp::h1(std::sync::Arc::clone(&policy)),
            bidi: crate::egress::TenantHttp::h2c(policy),
            identity,
            values: Default::default(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InvokeError {
    #[error("SDK call failed: {0}")]
    Transport(String),

    /// The egress policy refused the destination — no byte left the node. The
    /// turn fails like a transport error (rescheduled with backoff): the policy
    /// or DNS may change, and a durable invocation is not abandoned because of
    /// it.
    #[error("{0}")]
    EgressDenied(crate::egress::Denied),

    #[error("SDK returned HTTP {status}: {body}")]
    Status { status: u16, body: String },

    #[error("SDK returned the wrong content-type: {0:?}")]
    WrongContentType(String),

    #[error("corrupt message stream from the SDK: {0}")]
    Protocol(#[from] ProtocolError),

    #[error("message stream from the SDK cut off: {0} bytes left that do not form a frame")]
    Truncated(usize),

    /// The SDK's response body exceeds the cap — the turn fails like a
    /// transport error, rescheduled with backoff; the node does not gather the
    /// body into RAM.
    #[error("SDK response body exceeds the limit of {0} bytes")]
    TooLarge(usize),

    /// The SDK's response body holds more frames than [`RESPONSE_FRAMES_MAX`] — the
    /// turn fails like [`InvokeError::TooLarge`].
    #[error("SDK response body exceeds the limit of {0} frames")]
    TooManyFrames(usize),

    /// The SDK reports a replay cache **miss** (`409` + `cairn-replay: miss`,
    /// wire v5) for a delta turn. The turn layer resends at once with a full
    /// replay; a miss on a full turn does not become this variant but
    /// [`InvokeError::Status`] — an SDK error.
    ///
    /// `held` (header `cairn-replay-held: <k>:<hex h_k>`): the part of the
    /// journal the SDK still holds — **a claim**, the turn layer only uses it
    /// after checking `h_k` against the current journal. Absent or malformed ⇒
    /// `None`.
    #[error("SDK returned a replay cache miss (409 cairn-replay: miss)")]
    ReplayMiss {
        held: Option<(u32, cairn_runtime::replay::Digest)>,
    },

    /// A `BIDI_STREAM` stream opened with no response headers within
    /// [`BIDI_HEADERS_TIMEOUT`]. The SDK returns headers as soon as it receives
    /// the stream (`serve.ts` `openBidi`), so silence that long is almost
    /// certainly an edge buffering request bodies (TLS h2 outside, h1 to the
    /// app). The turn fails like a transport error, rescheduled with backoff;
    /// the runtime does **not** switch modes by itself.
    #[error(
        "{BIDI_NO_RESPONSE_HEADERS}: SDK sent no response headers within {}s after opening a \
         BIDI_STREAM stream to {url} — the edge between the runtime and the SDK may speak only \
         HTTP/1 or buffer request bodies; re-register the deployment with transport: auto, or run \
         the SDK as REQUEST_RESPONSE",
        BIDI_HEADERS_TIMEOUT.as_secs()
    )]
    NoResponseHeaders { url: String },
}

/// Stable code at the start of the message of [`InvokeError::NoResponseHeaders`].
pub const BIDI_NO_RESPONSE_HEADERS: &str = "bidi_no_response_headers";

/// Limit on waiting for response headers when opening a `BIDI_STREAM` turn —
/// separate from `inactivityTimeout` (60 s by default, up to 1 h): an SDK
/// following the protocol returns headers before reading any byte of the
/// body, so a few seconds is plenty; a body-buffering edge never returns them,
/// and the on-call engineer needs to see the error code early.
pub const BIDI_HEADERS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

/// Cap on the SDK's `REQUEST_RESPONSE` response body. The per-frame cap (16 MiB,
/// `FrameDecoder`) does not stop a body of **many** valid frames; recovery
/// reruns that turn, so without a cap a malicious SDK could push the node into
/// a loop of OOM crashes.
pub const RESPONSE_BODY_MAX: usize = 32 << 20;

/// Cap on the number of frames in one `REQUEST_RESPONSE` response body. The byte cap
/// alone does not bound memory: an empty frame is 8 bytes on the wire and several times
/// that once decoded, so a body of empty frames multiplied [`RESPONSE_BODY_MAX`]. A
/// turn sends a handful of frames per command; a quarter of a million is far above any
/// journal a `REQUEST_RESPONSE` turn can replay.
pub const RESPONSE_FRAMES_MAX: usize = 1 << 18;

/// The leading part of an error (non-2xx) body that is read — the message keeps only 500 characters.
pub const ERROR_BODY_MAX: usize = 4 << 10;

/// Cap on the total payload one [`BidiSession::buffered`] call returns — the
/// rest is read next time.
pub const BUFFERED_MAX: usize = 32 << 20;

/// Joins messages into the byte stream sent down to the SDK.
pub fn encode_stream(msgs: &[Message]) -> Bytes {
    let mut out = BytesMut::new();
    for m in msgs {
        // The runtime never sets REQUESTED_ACK: that flag is the SDK asking the runtime.
        out.extend_from_slice(&m.encode(cairn_protocol::Flags::NONE));
    }
    out.freeze()
}

/// Splits the byte stream the SDK returns into frames.
///
/// Leftover bytes that do not form a frame are an error, not "wait for more":
/// in `REQUEST_RESPONSE` the response body has ended, so there is nothing left
/// to wait for.
pub fn decode_stream(body: Bytes) -> Result<Vec<Frame>, InvokeError> {
    let dec = FrameDecoder::new();
    // Takes over the buffer when nobody else holds it, instead of copying the body.
    let mut buf = BytesMut::from(body);
    let mut out = Vec::new();
    while let Some(f) = dec.decode(&mut buf)? {
        if out.len() == RESPONSE_FRAMES_MAX {
            return Err(InvokeError::TooManyFrames(RESPONSE_FRAMES_MAX));
        }
        out.push(f);
    }
    if !buf.is_empty() {
        return Err(InvokeError::Truncated(buf.len()));
    }
    Ok(out)
}

/// An error of [`crate::egress::send_for_tenant`] as an [`InvokeError`];
/// `what` says which path failed.
fn tenant_error(
    e: crate::egress::TenantError,
    what: impl FnOnce(&reqwest::Error) -> String,
) -> InvokeError {
    if let Some(d) = e.denied() {
        return InvokeError::EgressDenied(d.clone());
    }
    match e {
        crate::egress::TenantError::Http(e) => InvokeError::Transport(what(&e)),
        crate::egress::TenantError::Denied(d) => InvokeError::EgressDenied(d),
    }
}

/// Response header of an RR delta turn: the number of leading journal entries the SDK holds after the turn.
pub const REPLAY_CACHE_HEADER: &str = "cairn-replay-cache";
/// Response header of a `409` miss: the value [`REPLAY_MISS`].
pub const REPLAY_HEADER: &str = "cairn-replay";
pub const REPLAY_MISS: &str = "miss";
/// Header accompanying a `409` miss: `<k>:<64 hex of h_k>` — the part the SDK still holds.
pub const REPLAY_HELD_HEADER: &str = "cairn-replay-held";

/// `<k>:<64 hex>` ⇒ `(k, h_k)`; any other form ⇒ `None`.
pub fn parse_held(v: &str) -> Option<(u32, cairn_runtime::replay::Digest)> {
    let (k, h) = v.trim().split_once(':')?;
    let k: u32 = k.trim().parse().ok()?;
    let h = h.trim();
    if h.len() != 64 {
        return None;
    }
    let mut d = [0u8; 32];
    for (i, b) in d.iter_mut().enumerate() {
        *b = u8::from_str_radix(h.get(2 * i..2 * i + 2)?, 16).ok()?;
    }
    Some((k, d))
}
/// Affinity response header (`fly=<machine id>`).
pub const AFFINITY_HEADER: &str = "cairn-affinity";

/// The affinity an RR turn carries to the SDK: the `Cookie` and
/// `fly-force-instance-id` that the **previous** response of this very
/// invocation returned.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Affinity {
    /// `name=value; …` — filtered, capped ([`crate::turn::replay`]).
    pub cookie: Option<String>,
    /// A Fly.io machine id (alphanumeric, ≤ 64 characters).
    pub fly: Option<String>,
}

/// The response of an RR turn: frames, plus the v5 part in the headers.
#[derive(Debug, Default)]
pub struct RrReply {
    pub frames: Vec<Frame>,
    /// `cairn-replay-cache` (wire v5): `None` = absent, `0` or unreadable.
    pub cache: Option<u32>,
    /// The affinity the SDK wants on the next turn (`Set-Cookie`, `cairn-affinity`).
    pub affinity: Affinity,
}

/// `POST {endpoint}/invoke/{service}/{handler}` — the turn layer's
/// `REQUEST_RESPONSE` turn: like [`call_detached`] plus outgoing affinity and
/// the v5 part of the response.
#[allow(clippy::too_many_arguments)]
pub async fn call(
    client: &crate::egress::TenantHttp,
    endpoint: &str,
    service: &str,
    handler: &str,
    wire_version: u32,
    body: Bytes,
    signing: Option<Signing<'_>>,
    metered: Option<&str>,
    gzip: bool,
    affinity: &Affinity,
    _released: &Released,
) -> Result<RrReply, InvokeError> {
    call_rr(
        client,
        endpoint,
        service,
        handler,
        wire_version,
        body,
        signing,
        metered,
        gzip,
        affinity,
    )
    .await
}

/// Like [`call`], for callers **holding no engine** (trial replay at ingress)
/// — the `Released` ticket of `call` is there to remind code inside the engine
/// to release it.
///
/// `metered`: the tenant is charged the on-the-wire bytes in metering; `None`
/// = not charged (an operator's trial replay).
///
/// `gzip`: the deployment declares `rrCompression: gzip` (modelled on the
/// `lambdaCompression` field of Restate's service protocol) — offer
/// `accept-encoding: gzip`, compress bodies from [`RR_COMPRESS_MIN`] bytes,
/// accept gzip responses (decompressed with a cap).
#[allow(clippy::too_many_arguments)]
pub async fn call_detached(
    client: &crate::egress::TenantHttp,
    endpoint: &str,
    service: &str,
    handler: &str,
    wire_version: u32,
    body: Bytes,
    signing: Option<Signing<'_>>,
    metered: Option<&str>,
    gzip: bool,
) -> Result<Vec<Frame>, InvokeError> {
    let r = call_rr(
        client,
        endpoint,
        service,
        handler,
        wire_version,
        body,
        signing,
        metered,
        gzip,
        &Affinity::default(),
    )
    .await?;
    Ok(r.frames)
}

#[allow(clippy::too_many_arguments)]
async fn call_rr(
    client: &crate::egress::TenantHttp,
    endpoint: &str,
    service: &str,
    handler: &str,
    wire_version: u32,
    body: Bytes,
    signing: Option<Signing<'_>>,
    metered: Option<&str>,
    gzip: bool,
    affinity: &Affinity,
) -> Result<RrReply, InvokeError> {
    let url = format!(
        "{}/invoke/{service}/{handler}",
        endpoint.trim_end_matches('/')
    );
    let ct = invocation_content_type(wire_version);
    let mut req = client
        .client()
        .post(&url)
        .header(reqwest::header::CONTENT_TYPE, ct)
        .header(reqwest::header::ACCEPT, ct);
    // The signature covers the body **before** compression: the SDK hashes after decompressing.
    if let Some(s) = signing {
        req = s.apply(&url, Some(body.as_ref()), req);
    }
    let body = if gzip {
        req = req.header(reqwest::header::ACCEPT_ENCODING, GZIP);
        match compress_off_engine(body).await {
            Ok(z) => {
                req = req.header(reqwest::header::CONTENT_ENCODING, GZIP);
                z
            }
            Err(body) => body,
        }
    } else {
        body
    };
    let sent = body.len();
    let mut req = req.body(body);
    // Affinity: only what the previous response of **this very** invocation
    // returned, to the same deployment — `turn::replay` keeps and filters it.
    if let Some(c) = &affinity.cookie {
        req = req.header(reqwest::header::COOKIE, c);
    }
    if let Some(f) = &affinity.fly {
        req = req.header("fly-force-instance-id", f);
    }
    let resp = crate::egress::send_for_tenant(client, req)
        .await
        .map_err(|e| tenant_error(e, |e| e.to_string()))?;
    // Response headers arrived ⇒ the request body has gone out. Transport
    // errors are not counted: how much was partly sent cannot be known.
    crate::metrics::sdk::add_bytes(Rr, ToSdk, sent);
    if let Some(t) = metered {
        crate::metering::sdk_bytes(t, sent as u64, 0);
    }

    let status = resp.status();
    cairn_store::probe::mark("sdk.open_done", u64::from(status.as_u16()));
    if status == reqwest::StatusCode::CONFLICT
        && resp
            .headers()
            .get(REPLAY_HEADER)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.trim().eq_ignore_ascii_case(REPLAY_MISS))
    {
        let held = resp
            .headers()
            .get(REPLAY_HELD_HEADER)
            .and_then(|v| v.to_str().ok())
            .and_then(parse_held);
        // A miss body is empty per the protocol; read it (capped) so the connection can be reused.
        let _ = error_body(resp).await;
        return Err(InvokeError::ReplayMiss { held });
    }
    if !status.is_success() {
        return Err(InvokeError::Status {
            status: status.as_u16(),
            body: error_body(resp).await,
        });
    }

    let ct = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    if !known_invocation_type(&ct) {
        return Err(InvokeError::WrongContentType(ct));
    }

    let encoding = resp
        .headers()
        .get(reqwest::header::CONTENT_ENCODING)
        .map(|v| v.to_str().unwrap_or("?").trim().to_ascii_lowercase());
    let gzipped = match encoding.as_deref() {
        None | Some("identity") => false,
        Some(GZIP) if gzip => true,
        Some(e) => {
            return Err(InvokeError::Transport(format!(
                "SDK returned content-encoding {e:?} that the runtime did not offer (gzip is \
                 offered only when the deployment declares requestCompression)"
            )))
        }
    };
    let cache = resp
        .headers()
        .get(REPLAY_CACHE_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u32>().ok())
        .filter(|k| *k > 0);
    let affinity = Affinity {
        cookie: crate::turn::replay::cookie_of(
            resp.headers()
                .get_all(reqwest::header::SET_COOKIE)
                .iter()
                .filter_map(|v| v.to_str().ok()),
        ),
        fly: resp
            .headers()
            .get(AFFINITY_HEADER)
            .and_then(|v| v.to_str().ok())
            .and_then(crate::turn::replay::fly_of),
    };
    let body = read_capped(resp, RESPONSE_BODY_MAX).await?;
    crate::metrics::sdk::add_bytes(Rr, FromSdk, body.len());
    if let Some(t) = metered {
        crate::metering::sdk_bytes(t, 0, body.len() as u64);
    }
    let body = if gzipped {
        gunzip_off_engine(body, RESPONSE_BODY_MAX).await?
    } else {
        body
    };
    Ok(RrReply {
        frames: decode_stream(body)?,
        cache,
        affinity,
    })
}

/// The `accept-encoding`/`content-encoding` value for RR body compression.
const GZIP: &str = "gzip";

/// RR request bodies smaller than this go uncompressed. A threshold like 4 MiB
/// would suit Lambda's payload cap; the goal here is egress bytes, and replay
/// bytes come from bodies of 10 KiB – 5 MiB.
pub const RR_COMPRESS_MIN: usize = 8 << 10;

/// Bodies from this many bytes are compressed/decompressed on the runtime's
/// blocking threads (`spawn_blocking`), not on the calling thread.
///
/// A `REQUEST_RESPONSE` turn runs on the engine thread — a `current_thread`
/// runtime **shared by many ranges** ([`crate::engine_pool`]). The engine is
/// released while calling the SDK (D16), but compression CPU still occupies
/// the thread: an 8 MiB journal at level 1 takes around ten ms, and every
/// range on the same thread stands waiting. Below the threshold it runs in
/// place: one thread switch costs more than the compression.
pub const RR_OFFLOAD_MIN: usize = 64 << 10;

/// The maximum expansion ratio of deflate (~1032:1, including multiple gzip
/// members — each member costs an extra header): an upper bound on the bytes a
/// gzip body decompresses to, to choose where to decompress **before**
/// decompressing. The real cap is still the `max` of [`gunzip_capped`].
const DEFLATE_MAX_RATIO: usize = 1032;

/// [`compress`] of `body`, on a blocking thread when the body is at least
/// [`RR_OFFLOAD_MIN`] bytes. `Ok(gzip)` or `Err(body)` (send uncompressed — as
/// when [`compress`] returns `None`).
async fn compress_off_engine(body: Bytes) -> Result<Bytes, Bytes> {
    if body.len() < RR_OFFLOAD_MIN {
        return compress(&body).ok_or(body);
    }
    let input = body.clone();
    match tokio::task::spawn_blocking(move || compress(&input)).await {
        Ok(Some(z)) => Ok(z),
        Ok(None) => Err(body),
        // A panic in compression keeps the semantics of the in-place call.
        Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
        // The runtime is shutting down: send uncompressed — the turn is about to be dropped with the runtime.
        Err(_) => Err(body),
    }
}

/// [`gunzip_capped`] of `body`, on a blocking thread when the decompressed size
/// **may** reach [`RR_OFFLOAD_MIN`] (bounded by [`DEFLATE_MAX_RATIO`], not
/// trusting the ISIZE field the SDK writes): a small compressed response can
/// still decompress up to `max`.
async fn gunzip_off_engine(body: Bytes, max: usize) -> Result<Bytes, InvokeError> {
    if body.len().saturating_mul(DEFLATE_MAX_RATIO).min(max) < RR_OFFLOAD_MIN {
        return gunzip_capped(&body, max);
    }
    match tokio::task::spawn_blocking(move || gunzip_capped(&body, max)).await {
        Ok(r) => r,
        Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
        Err(e) => Err(InvokeError::Transport(format!(
            "gzip body from the SDK was not decoded: {e}"
        ))),
    }
}

/// Gzip level: fastest. An RR body is the whole journal, recompressed every
/// turn — CPU quadratic in the number of steps; level 1 gives a ratio close to
/// level 6 on JSON for far less CPU.
pub const RR_GZIP_LEVEL: u32 = 1;

/// Gzip of `body` when worth it: from [`RR_COMPRESS_MIN`] bytes and smaller
/// than the uncompressed body (an incompressible payload ⇒ `None`, sent
/// uncompressed). A **new** compressor every time — no compression state
/// crosses requests or tenants.
///
/// **No cross-frame compression** (against the BREACH class, modelled on
/// HPACK's context separation): input chosen from outside and the results of
/// `ctx.run`/state sit in different frames; every frame boundary is a full
/// flush — `miniz_oxide` clears its hash table and window there, so a later
/// frame cannot reference bytes of an earlier one. Still **one** standard gzip
/// member: the SDK decompresses as before. A body that cannot be split into
/// frames (does not happen: the runtime builds it itself) ⇒ `None`, sent
/// uncompressed.
pub fn compress(body: &[u8]) -> Option<Bytes> {
    if body.len() < RR_COMPRESS_MIN {
        return None;
    }
    // The fence of the test "compression does not stall the engine thread".
    cairn_store::failpoint!("invoker::rr_gzip");
    let frames = frame_spans(body)?;
    let mut c = flate2::Compress::new(flate2::Compression::new(RR_GZIP_LEVEL), false);
    let mut out = Vec::with_capacity(body.len() / 2 + 64);
    // Gzip header (RFC 1952): magic, deflate, no flags, mtime 0, OS "unknown".
    out.extend_from_slice(&[0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 0xff]);
    let last = frames.len() - 1;
    for (i, frame) in frames.into_iter().enumerate() {
        let flush = if i == last {
            flate2::FlushCompress::Finish
        } else {
            flate2::FlushCompress::Full
        };
        let mut input = frame;
        loop {
            if out.capacity() - out.len() < 64 {
                out.reserve(out.capacity().max(4096));
            }
            let seen = c.total_in();
            let status = c.compress_vec(input, &mut out, flush).ok()?;
            input = &input[(c.total_in() - seen) as usize..];
            let done = match flush {
                flate2::FlushCompress::Finish => status == flate2::Status::StreamEnd,
                // As in zlib: the flush is done when the call leaves room in the output.
                _ => input.is_empty() && out.len() < out.capacity(),
            };
            if done {
                break;
            }
        }
    }
    let mut crc = flate2::Crc::new();
    crc.update(body);
    out.extend_from_slice(&crc.sum().to_le_bytes());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    (out.len() < body.len()).then(|| Bytes::from(out))
}

/// Splits `body` into frames (header of [`cairn_protocol::HEADER_LEN`] bytes,
/// the length in the low 32 bits). Leftover bytes or a truncated frame ⇒
/// `None`.
fn frame_spans(body: &[u8]) -> Option<Vec<&[u8]>> {
    let mut spans = Vec::new();
    let mut rest = body;
    while !rest.is_empty() {
        let head: [u8; cairn_protocol::HEADER_LEN] =
            rest.get(..cairn_protocol::HEADER_LEN)?.try_into().ok()?;
        let len = u64::from_be_bytes(head) as u32 as usize;
        let end = cairn_protocol::HEADER_LEN.checked_add(len)?;
        spans.push(rest.get(..end)?);
        rest = &rest[end..];
    }
    (!spans.is_empty()).then_some(spans)
}

/// Decompresses gzip (every member) with a cap on bytes **after**
/// decompression — a small compressed response decompressing without bound (a
/// "zip bomb") fails like an uncompressed body over the cap.
pub fn gunzip_capped(body: &[u8], max: usize) -> Result<Bytes, InvokeError> {
    use std::io::Read as _;
    // The fence of the test "decompression does not stall the engine thread".
    cairn_store::failpoint!("invoker::rr_gunzip");
    let mut out = Vec::new();
    flate2::read::MultiGzDecoder::new(body)
        .take(max as u64 + 1)
        .read_to_end(&mut out)
        .map_err(|e| InvokeError::Transport(format!("corrupt gzip body from the SDK: {e}")))?;
    if out.len() > max {
        return Err(InvokeError::TooLarge(max));
    }
    Ok(Bytes::from(out))
}

/// The response body, no more than `max` bytes (shaped like
/// `discovery::discover`): a declared `Content-Length` over it ⇒ refused before
/// reading; a streamed body over it ⇒ stop reading at the chunk that crosses
/// and drop the connection.
async fn read_capped(mut resp: reqwest::Response, max: usize) -> Result<Bytes, InvokeError> {
    if resp.content_length().is_some_and(|n| n > max as u64) {
        return Err(InvokeError::TooLarge(max));
    }
    let mut body = BytesMut::new();
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| InvokeError::Transport(e.to_string()))?
    {
        if body.len() + chunk.len() > max {
            return Err(InvokeError::TooLarge(max));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

/// The leading part (≤ [`ERROR_BODY_MAX`] bytes, ≤ 500 characters) of an error
/// response body — only for the message; the rest is not read, the connection
/// is dropped.
async fn error_body(mut resp: reqwest::Response) -> String {
    let mut body = Vec::new();
    while body.len() < ERROR_BODY_MAX {
        match resp.chunk().await {
            Ok(Some(c)) => body.extend_from_slice(&c[..c.len().min(ERROR_BODY_MAX - body.len())]),
            _ => break,
        }
    }
    String::from_utf8_lossy(&body).chars().take(500).collect()
}

// ───────────────────────────────────────────────────────────── BIDI_STREAM

/// Number of byte batches queued for sending down to the SDK before `send`
/// blocks.
///
/// Bounded because backpressure must **reach** where the data is produced:
/// unbounded, a slow-reading SDK would only make the runtime's buffer grow
/// until RAM runs out, instead of slowing the engine down — and slowing the
/// engine down is exactly what we want.
const BIDI_QUEUE: usize = 16;

/// The open request body to the SDK: a byte queue the turn pushes into.
///
/// A hand-written `Stream` rather than `futures_util::stream::unfold` — it is
/// a single `poll_recv`, and a whole crate for that much is expensive.
struct RequestBody(tokio::sync::mpsc::Receiver<Bytes>, Option<Arc<Tally>>);

/// Bytes on the wire of **one** BIDI stream with a tenant's endpoint: added
/// atomically per batch, charged to metering once when the last holder (the
/// request body, the session, the draining task) drops it — no lock per frame.
struct Tally {
    tenant: String,
    out: AtomicU64,
    received: AtomicU64,
}

impl Tally {
    fn new(tenant: Option<&str>) -> Option<Arc<Tally>> {
        tenant.map(|t| {
            Arc::new(Tally {
                tenant: t.to_string(),
                out: AtomicU64::new(0),
                received: AtomicU64::new(0),
            })
        })
    }
}

fn tally_in(t: &Option<Arc<Tally>>, n: usize) {
    if let Some(t) = t {
        t.received.fetch_add(n as u64, Ordering::Relaxed);
    }
}

impl Drop for Tally {
    fn drop(&mut self) {
        crate::metering::sdk_bytes(
            &self.tenant,
            self.out.load(Ordering::Relaxed),
            self.received.load(Ordering::Relaxed),
        );
    }
}

impl futures_core::Stream for RequestBody {
    type Item = std::result::Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Counted when hyper takes the batch — including the `Start` opening the turn.
        let this = &mut *self;
        this.0.poll_recv(cx).map(|o| {
            o.map(|b| {
                crate::metrics::sdk::add_bytes(Bidi, ToSdk, b.len());
                if let Some(t) = &this.1 {
                    t.out.fetch_add(b.len() as u64, Ordering::Relaxed);
                }
                Ok(b)
            })
        })
    }
}

/// Limit on waiting for the SDK to close its half of the stream after the
/// runtime lets go of a bidi turn.
///
/// Letting go of a stream while the SDK still has its half open is
/// `RST_STREAM(CANCEL)`, and the SDK's h2 server counts RSTs: nghttp2
/// (`node:http2`) gives each connection 1 000 RSTs, refilled at 33/s (against
/// rapid reset, CVE-2023-44487); over the cap it **kills the whole
/// connection**, failing every turn open on it. Every turn waiting for the
/// outside world ends exactly that way, so above 33 such turns/s one
/// connection is certain to hit the cap — sooner or later. So letting go =
/// sending END_STREAM on the request body and waiting for the SDK to close its
/// half (the SDK closes as soon as it sees the request body end — the
/// `woken === null` branch of `runBidi`), RST only past this limit. Short: a
/// healthy SDK closes after one round trip; a stuck SDK's stream occupies a
/// place in its `SETTINGS_MAX_CONCURRENT_STREAMS` until the limit.
pub const BIDI_CLOSE_GRACE: std::time::Duration = std::time::Duration::from_secs(2);

/// A turn open on an h2c connection.
///
/// Two **independent** directions: [`Self::send`] pushes into the open request
/// body, [`Self::next_frame`] reads from the open response body. Nothing pairs
/// them into request-response, and that is exactly what `REQUEST_RESPONSE`
/// cannot do.
///
/// Letting go of the session = closing the stream. That is how a turn ends
/// when the SDK is waiting for something only the outside world can answer.
/// Closed **cleanly**, no RST — see [`BIDI_CLOSE_GRACE`].
pub struct BidiSession {
    /// `None` only after `drop` has closed the runtime's half.
    to_sdk: Option<tokio::sync::mpsc::Sender<Bytes>>,
    from_sdk: Pin<Box<dyn futures_core::Stream<Item = reqwest::Result<Bytes>> + Send>>,
    buf: BytesMut,
    dec: FrameDecoder,
    /// The stream has ended (seen in [`Self::buffered`]) — do not poll a
    /// finished stream again.
    ended: bool,
    tally: Option<Arc<Tally>>,
}

impl BidiSession {
    /// Pushes messages down to the SDK. An empty list is a no-op, not an empty write.
    ///
    /// `_released`: only callable while the engine is released — see
    /// [`crate::lease::Released`].
    pub async fn send(&self, msgs: &[Message], _released: &Released) -> Result<(), InvokeError> {
        if msgs.is_empty() {
            return Ok(());
        }
        let closed = || InvokeError::Transport("SDK closed the receiving side".into());
        let to_sdk = self.to_sdk.as_ref().ok_or_else(closed)?;
        cairn_store::probe::mark("sdk.send", msgs.len() as u64);
        to_sdk.send(encode_stream(msgs)).await.map_err(|_| closed())
    }

    /// The next frame from the SDK. `Ok(None)` = the SDK closed the stream cleanly.
    ///
    /// Leftover bytes at close are an error, **unlike** leftover bytes before
    /// close: here the stream has ended so there is nothing left to wait for,
    /// exactly the reasoning of [`decode_stream`].
    pub async fn next_frame(&mut self, _released: &Released) -> Result<Option<Frame>, InvokeError> {
        cairn_store::probe::mark("sdk.read", 0);
        loop {
            if let Some(f) = self.dec.decode(&mut self.buf)? {
                cairn_store::probe::mark("sdk.frame", u64::from(f.ty.0));
                return Ok(Some(f));
            }
            if self.ended {
                return if self.buf.is_empty() {
                    Ok(None)
                } else {
                    Err(InvokeError::Truncated(self.buf.len()))
                };
            }
            match std::future::poll_fn(|cx| self.from_sdk.as_mut().poll_next(cx)).await {
                Some(Ok(chunk)) => {
                    crate::metrics::sdk::add_bytes(Bidi, FromSdk, chunk.len());
                    tally_in(&self.tally, chunk.len());
                    self.buf.extend_from_slice(&chunk);
                }
                Some(Err(e)) => {
                    // The stream has failed: nothing left for `drop` to wait for.
                    self.ended = true;
                    return Err(InvokeError::Transport(e.to_string()));
                }
                None => {
                    self.ended = true;
                    return if self.buf.is_empty() {
                        Ok(None)
                    } else {
                        Err(InvokeError::Truncated(self.buf.len()))
                    };
                }
            }
        }
    }

    /// Every frame that **has arrived** but is unread — waiting for nothing.
    ///
    /// The SDK sends `[Run, ProposeRunCompletion, Suspension]` in one write;
    /// reading them one at a time and waiting for durability after each pays
    /// three WAL flushes for something one flush covers. This function drains
    /// what is already in the buffer and what the stream can return *at once*
    /// (polled once with a no-op waker), stopping where it would have to wait.
    /// A stream ending midway is left for `next_frame` to report.
    ///
    /// A total cap of [`BUFFERED_MAX`] payload bytes per call: if the SDK
    /// pushes faster than the runtime reads, the call stops at the cap and the
    /// rest stays in `buf`/the stream for next time — no frame is lost.
    pub fn buffered(&mut self) -> Result<Vec<Frame>, InvokeError> {
        let mut out = Vec::new();
        let mut size = 0usize;
        let mut cx = Context::from_waker(std::task::Waker::noop());
        loop {
            while let Some(f) = self.dec.decode(&mut self.buf)? {
                size += f.payload.len();
                out.push(f);
                if size >= BUFFERED_MAX {
                    return Ok(out);
                }
            }
            match self.from_sdk.as_mut().poll_next(&mut cx) {
                Poll::Ready(Some(Ok(chunk))) => {
                    crate::metrics::sdk::add_bytes(Bidi, FromSdk, chunk.len());
                    tally_in(&self.tally, chunk.len());
                    self.buf.extend_from_slice(&chunk);
                }
                Poll::Ready(Some(Err(e))) => {
                    self.ended = true;
                    return Err(InvokeError::Transport(e.to_string()));
                }
                Poll::Ready(None) => {
                    self.ended = true;
                    return Ok(out);
                }
                Poll::Pending => return Ok(out),
            }
        }
    }
}

impl Drop for BidiSession {
    /// Clean close: END_STREAM on the request body, then a detached task drains
    /// the response body until the SDK closes its half (limit
    /// [`BIDI_CLOSE_GRACE`]). Only past the limit is the unfinished response
    /// dropped — h2 sends `RST_STREAM(CANCEL)` as before.
    ///
    /// Holds no engine: the task does not touch the engine, and the turn has
    /// ended. Bytes the SDK sends after the runtime lets go are dropped —
    /// just as when the stream is RST.
    fn drop(&mut self) {
        // Dropping the queue ⇒ `RequestBody` returns `None` ⇒ hyper sends END_STREAM.
        drop(self.to_sdk.take());
        if self.ended {
            return;
        }
        // Outside a tokio runtime (does not happen on the turn path) there is
        // nowhere to run the waiting task: drop directly, RST as before.
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let mut from_sdk = std::mem::replace(
            &mut self.from_sdk,
            Box::pin(futures::stream::empty::<reqwest::Result<Bytes>>()),
        );
        let tally = self.tally.clone();
        rt.spawn(async move {
            let drained = cairn_store::retry::deadline(
                &cairn_store::retry::ops::SDK_BIDI_CLOSE,
                BIDI_CLOSE_GRACE,
                async {
                    // `Err` = the SDK sent RST or the connection broke: the
                    // stream is closed, done. Dropped bytes still went over the
                    // wire: count them as read.
                    while let Some(Ok(chunk)) =
                        std::future::poll_fn(|cx| from_sdk.as_mut().poll_next(cx)).await
                    {
                        crate::metrics::sdk::add_bytes(Bidi, FromSdk, chunk.len());
                        tally_in(&tally, chunk.len());
                    }
                },
            )
            .await;
            if drained.is_err() {
                tracing::debug!(
                    "SDK did not close the bidi stream within {BIDI_CLOSE_GRACE:?} after the runtime \
                     let go — RST_STREAM"
                );
            }
        });
    }
}

/// Opens a `BIDI_STREAM` turn: `POST {endpoint}/invoke/{service}/{handler}`
/// over h2c, with the request body left open.
///
/// `opening` is **queued before** `send()` is awaited, and that order is
/// mandatory: `send()` only returns when the response headers arrive, and the
/// SDK has no reason to return headers before it has read `Start`. Pushing
/// afterwards means both sides wait for each other.
#[allow(clippy::too_many_arguments)]
pub async fn open_bidi(
    client: &crate::egress::TenantHttp,
    endpoint: &str,
    service: &str,
    handler: &str,
    wire_version: u32,
    opening: Bytes,
    signing: Option<Signing<'_>>,
    metered: Option<&str>,
    _released: &Released,
) -> Result<BidiSession, InvokeError> {
    let url = format!(
        "{}/invoke/{service}/{handler}",
        endpoint.trim_end_matches('/')
    );
    let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(BIDI_QUEUE);
    // The signature covers the body opening the turn — keep a copy (Bytes: cheap) before sending.
    let signed_opening = opening.clone();
    tx.try_send(opening).expect("a fresh queue has room");
    let tally = Tally::new(metered);
    cairn_store::probe::mark("sdk.open", 0);

    let ct = invocation_content_type(wire_version);
    let mut req = client
        .client()
        .post(&url)
        .header(reqwest::header::CONTENT_TYPE, ct)
        .header(reqwest::header::ACCEPT, ct)
        .body(reqwest::Body::wrap_stream(RequestBody(rx, tally.clone())));
    if let Some(s) = signing {
        req = s.apply(&url, Some(signed_opening.as_ref()), req);
    }
    // A separate limit for the response headers: a body-buffering edge holds
    // the turn until `inactivityTimeout` with no error saying why.
    let resp = tokio::time::timeout(
        BIDI_HEADERS_TIMEOUT,
        crate::egress::send_for_tenant(client, req),
    )
    .await
    .map_err(|_| InvokeError::NoResponseHeaders { url: url.clone() })?
    // Say **which path** failed. The most common failure here is
    // configuration, not an incident: the deployment declares `BIDI_STREAM`
    // but an h1-only proxy sits between the runtime and the SDK, or an old SDK
    // does not serve h2c yet. reqwest's bare error then only says "connection
    // closed", and the on-call engineer spends half an hour guessing it is a
    // transport issue.
    .map_err(|e| tenant_error(e, |e| bidi_open_failed(&url, e)))?;

    let status = resp.status();
    if !status.is_success() {
        return Err(InvokeError::Status {
            status: status.as_u16(),
            body: error_body(resp).await,
        });
    }
    let ct = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    if !known_invocation_type(&ct) {
        return Err(InvokeError::WrongContentType(ct));
    }

    Ok(BidiSession {
        to_sdk: Some(tx),
        from_sdk: Box::pin(resp.bytes_stream()),
        buf: BytesMut::new(),
        dec: FrameDecoder::new(),
        ended: false,
        tally,
    })
}

/// The message when opening a BIDI stream fails at the transport: `http://` is
/// h2c prior-knowledge, `https://` is h2 negotiated through ALPN — saying
/// "cleartext" for an `https://` URI sends the on-call engineer the wrong way.
fn bidi_open_failed(url: &str, e: &reqwest::Error) -> String {
    let need = if url.starts_with("https://") {
        "HTTP/2 all the way to the SDK (ALPN h2 at the TLS edge, and the edge must not downgrade \
         to HTTP/1 or buffer request bodies) — otherwise re-register with transport: auto, or run \
         the SDK as REQUEST_RESPONSE"
    } else {
        "HTTP/2 cleartext (h2c)"
    };
    format!(
        "opening a BIDI_STREAM stream to {url} failed: {e} — the deployment declares BIDI_STREAM, so \
         the invoke path must speak {need}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_protocol::{pb, MessageType};

    /// RR body compression: below the threshold or incompressible ⇒
    /// uncompressed; a compress/decompress round trip keeps the bytes;
    /// decompression has a cap after decompression.
    #[test]
    fn rr_gzip_round_trip_threshold_and_cap() {
        assert!(compress(&vec![b'x'; RR_COMPRESS_MIN - 1]).is_none());
        let json = cairn_protocol::encode_frame(
            MessageType(0x0401),
            cairn_protocol::Flags::NONE,
            &br#"{"step":1,"result":"ok"}"#.repeat(2_000),
        );
        let z = compress(&json).expect("JSON compresses");
        assert!(
            z.len() * 10 < json.len(),
            "{} against {}",
            z.len(),
            json.len()
        );
        assert_eq!(gunzip_capped(&z, json.len()).unwrap().as_ref(), &json[..]);
        assert!(matches!(
            gunzip_capped(&z, json.len() - 1),
            Err(InvokeError::TooLarge(_))
        ));
        // Pseudo-random bytes (xorshift) do not compress ⇒ sent uncompressed.
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        let noise: Vec<u8> = (0..64 << 10)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect();
        let noise =
            cairn_protocol::encode_frame(MessageType(0x0401), cairn_protocol::Flags::NONE, &noise);
        assert!(compress(&noise).is_none());
        // A body that cannot be split into frames ⇒ sent uncompressed.
        assert!(compress(&[b'x'; RR_COMPRESS_MIN]).is_none());
        assert!(matches!(
            gunzip_capped(b"not gzip", 1 << 20),
            Err(InvokeError::Transport(_))
        ));
    }

    /// The off-thread path gives the same bytes as the in-place path, on both
    /// sides of the threshold; off-thread decompression keeps the cap after
    /// decompression.
    #[tokio::test(flavor = "current_thread")]
    async fn rr_gzip_off_the_engine_thread_matches_inline() {
        for n in [RR_COMPRESS_MIN * 2, RR_OFFLOAD_MIN * 4] {
            let body = cairn_protocol::encode_frame(
                MessageType(0x0401),
                cairn_protocol::Flags::NONE,
                &br#"{"step":1,"result":"ok"}"#.repeat(n / 24),
            );
            let z = compress_off_engine(body.clone())
                .await
                .expect("JSON compresses");
            assert_eq!(Some(&z), compress(&body).as_ref());
            assert_eq!(
                gunzip_off_engine(z.clone(), body.len()).await.unwrap(),
                body
            );
            assert!(matches!(
                gunzip_off_engine(z, body.len() - 1).await,
                Err(InvokeError::TooLarge(_))
            ));
        }
        let plain = Bytes::from(vec![b'x'; RR_OFFLOAD_MIN]);
        assert_eq!(compress_off_engine(plain.clone()).await, Err(plain));
    }

    /// Pseudo-random bytes (xorshift), incompressible.
    fn noise(n: usize, seed: u64) -> Vec<u8> {
        let mut x = seed;
        (0..n)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect()
    }

    /// **No cross-frame compression** (against the BREACH class, modelled on
    /// HPACK's context separation): an RR body is the journal — input chosen
    /// from outside and the results of `ctx.run`/state (possibly secrets) sit
    /// in different frames. Compressed in one shared window, a later frame
    /// could reference an earlier one, and the TLS ciphertext length would
    /// reveal that a guessed string matches a secret.
    ///
    /// 1. Frame A and frame B contain **the same** 6 KiB of random bytes.
    /// 2. Compress the whole body.
    /// 3. With a shared window: B becomes a reference to A, the compressed body
    ///    ≈ ½ the uncompressed body.
    ///
    /// Must see: no cross reference — the body is incompressible (sent
    /// uncompressed) or compresses to ≥ 2 × 6 KiB. Two JSON frames still
    /// compress frame by frame and decompress to the right bytes.
    #[test]
    fn rr_gzip_never_compresses_across_frames() {
        let secret = noise(6 << 10, 0x9E37_79B9_7F4A_7C15);
        let frame = |ty: u16, payload: &[u8]| {
            cairn_protocol::encode_frame(MessageType(ty), cairn_protocol::Flags::NONE, payload)
        };
        let body = [frame(0x0400, &secret), frame(0x0401, &secret)].concat();
        if let Some(z) = compress(&body) {
            assert!(
                z.len() >= 2 * secret.len(),
                "a later frame was compressed in a window shared with an earlier one: {} compressed bytes vs {} secret bytes",
                z.len(),
                secret.len()
            );
        }
        let json = br#"{"step":1,"result":"ok"}"#.repeat(400);
        let body = [frame(0x0400, &json), frame(0x0401, &json)].concat();
        let z = compress(&body).expect("two JSON frames compress");
        assert!(
            z.len() * 5 < body.len(),
            "{} against {}",
            z.len(),
            body.len()
        );
        assert_eq!(gunzip_capped(&z, body.len()).unwrap().as_ref(), &body[..]);
    }

    /// The message of a failed stream open names the right protocol for the
    /// scheme: `https://` never says "cleartext".
    #[tokio::test]
    async fn bidi_open_failure_names_the_right_transport() {
        let client = reqwest::Client::new();
        let e = crate::egress::send(&client, client.get("http://127.0.0.1:1/"))
            .await
            .expect_err("port 1 does not listen");
        let tls = bidi_open_failed("https://sdk.example/invoke/S/h", &e);
        assert!(!tls.contains("cleartext"), "{tls}");
        assert!(
            tls.contains("transport: auto") && tls.contains("REQUEST_RESPONSE"),
            "{tls}"
        );
        let plain = bidi_open_failed("http://sdk:9080/invoke/S/h", &e);
        assert!(plain.contains("h2c"), "{plain}");
    }

    /// `cairn-replay-held`: only the exact form `<k>:<64 hex>`; anything else
    /// ⇒ `None` (the turn layer sends everything).
    #[test]
    fn held_header_parses_only_k_colon_64_hex() {
        let h = "ab".repeat(32);
        assert_eq!(parse_held(&format!("3:{h}")), Some((3, [0xab; 32])));
        assert_eq!(parse_held(&format!(" 7 : {h} ")), Some((7, [0xab; 32])));
        for bad in [
            String::new(),
            "3".into(),
            format!("x:{h}"),
            format!("3:{}", &h[..62]),
            format!("3:{h}00"),
            format!("3:{}zz", &h[..62]),
            format!("-1:{h}"),
        ] {
            assert_eq!(parse_held(&bad), None, "{bad:?}");
        }
    }

    #[test]
    fn message_stream_round_trip() {
        let msgs = vec![
            Message::Start(pb::StartMessage {
                known_entries: 3,
                ..Default::default()
            }),
            Message::End(pb::EndMessage {}),
        ];
        let frames = decode_stream(encode_stream(&msgs)).unwrap();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].ty, MessageType::START);
        assert_eq!(frames[1].ty, MessageType::END);
    }

    #[test]
    fn trailing_bytes_are_an_error_not_a_wait() {
        let mut wire = BytesMut::from(&encode_stream(&[Message::End(pb::EndMessage {})])[..]);
        wire.extend_from_slice(&[0x00, 0x00, 0x00]); // half a header
        assert!(matches!(
            decode_stream(wire.freeze()),
            Err(InvokeError::Truncated(3))
        ));
    }

    // ───────────────────────────── response body cap

    /// An h1 server faking a `REQUEST_RESPONSE` SDK: every request returns
    /// `status` with a body streamed in `chunk`-sized pieces, `total` bytes in
    /// all (`None` = never ends, one chunk every 2 ms). Returns the URL and a
    /// counter of bytes the server has sent.
    async fn body_server(
        status: u16,
        chunk: Bytes,
        total: Option<usize>,
    ) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        use std::sync::{atomic::Ordering, Arc};
        let sent = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let counter = Arc::clone(&sent);
        let app = axum::Router::new().fallback(move || {
            let (chunk, counter) = (chunk.clone(), Arc::clone(&counter));
            async move {
                let body = futures::stream::unfold(0usize, move |done| {
                    let (chunk, counter) = (chunk.clone(), Arc::clone(&counter));
                    async move {
                        match total {
                            Some(t) if done >= t => return None,
                            None => tokio::time::sleep(std::time::Duration::from_millis(2)).await,
                            _ => {}
                        }
                        let n = chunk.len();
                        counter.fetch_add(n, Ordering::Relaxed);
                        Some((Ok::<_, std::io::Error>(chunk), done + n))
                    }
                });
                axum::response::Response::builder()
                    .status(status)
                    .header("content-type", INVOCATION_V1)
                    .body(axum::body::Body::from_stream(body))
                    .unwrap()
            }
        });
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (url, sent)
    }

    /// The body cap counts bytes, and eight zero bytes are a valid empty frame: 32 MiB
    /// of zeros (about 33 KiB of gzip on the wire) decoded into 4 194 304 frames, each
    /// costing the node far more memory than its 8 bytes. Must see: the decoder refuses
    /// a body with more frames than any turn sends, and still decodes 10 000 of them.
    #[test]
    fn rr_body_of_empty_frames_is_refused() {
        let zeros = Bytes::from(vec![0u8; RESPONSE_BODY_MAX]);
        assert!(
            matches!(decode_stream(zeros), Err(InvokeError::TooManyFrames(n)) if n == RESPONSE_FRAMES_MAX),
            "4 194 304 empty frames must not be gathered into memory"
        );
        let some = decode_stream(Bytes::from(vec![0u8; 8 * 10_000])).expect("10 000 frames");
        assert_eq!(some.len(), 10_000);
    }

    /// A malicious SDK returns 64 MiB of **valid** frames (each frame one 64 KiB
    /// `Output`, under the 16 MiB per-frame cap): the old cap only applied per
    /// frame, so the runtime gathered the whole body into RAM. Must see:
    /// `TooLarge`, reading no more than [`RESPONSE_BODY_MAX`] (plus one chunk).
    #[tokio::test]
    async fn rr_body_over_cap_is_too_large() {
        let frame = encode_stream(&[Message::Output(pb::OutputCommandMessage {
            result: Some(pb::output_command_message::Result::Value(
                cairn_protocol::value::inline(Bytes::from(vec![b'x'; 64 << 10])),
            )),
            name: String::new(),
        })]);
        let (url, _) = body_server(200, frame, Some(64 << 20)).await;
        let clients = Clients::default();
        let r = call_detached(
            &clients.http,
            &url,
            "S",
            "h",
            1,
            Bytes::new(),
            None,
            None,
            false,
        )
        .await;
        assert!(
            matches!(r, Err(InvokeError::TooLarge(n)) if n == RESPONSE_BODY_MAX),
            "a 64 MiB body must be stopped at the cap: {:?}",
            r.map(|f| f.len())
        );
    }

    /// The SDK returns `500` with a body that never ends: the old runtime read
    /// the whole error body (`resp.text()`) — unbounded RAM, and the turn hung
    /// until the SDK stopped. Must see: a `Status` error comes back at once,
    /// the body ≤ 500 characters, the server has sent under 1 MiB when the
    /// runtime lets go.
    #[tokio::test]
    async fn error_body_is_read_only_a_prefix() {
        let (url, sent) = body_server(500, Bytes::from(vec![b'e'; 16 << 10]), None).await;
        let clients = Clients::default();
        let r = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            call_detached(
                &clients.http,
                &url,
                "S",
                "h",
                1,
                Bytes::new(),
                None,
                None,
                false,
            ),
        )
        .await
        .expect("an endless error body must not hold the turn");
        let at = sent.load(std::sync::atomic::Ordering::Relaxed);
        match r {
            Err(InvokeError::Status { status: 500, body }) => {
                assert!(body.chars().count() <= 500, "error len {}", body.len())
            }
            other => panic!("must be Status 500: {:?}", other.map(|f| f.len())),
        }
        assert!(at < 1 << 20, "runtime read {at} bytes of the error body");
    }

    /// `buffered()` gathers every frame **that has arrived** in one call: an
    /// SDK pushing faster than the runtime reads would make that call's `Vec`
    /// grow without bound. Must see: one call returns no more than
    /// [`BUFFERED_MAX`] payload bytes (plus one frame); the rest is still
    /// readable next time, no frame lost.
    #[tokio::test]
    async fn buffered_has_a_total_cap() {
        let payload = 64usize << 10;
        let frame = encode_stream(&[Message::Output(pb::OutputCommandMessage {
            result: Some(pb::output_command_message::Result::Value(
                cairn_protocol::value::inline(Bytes::from(vec![b'x'; payload])),
            )),
            name: String::new(),
        })]);
        let n = (64usize << 20) / frame.len();
        let chunks: Vec<reqwest::Result<Bytes>> = (0..n).map(|_| Ok(frame.clone())).collect();
        let mut session = BidiSession {
            to_sdk: None,
            from_sdk: Box::pin(futures::StreamExt::chain(
                futures::stream::iter(chunks),
                futures::stream::pending(),
            )),
            buf: BytesMut::new(),
            dec: FrameDecoder::new(),
            ended: false,
            tally: None,
        };
        let first = session.buffered().unwrap();
        let size: usize = first.iter().map(|f| f.payload.len()).sum();
        assert!(
            size <= BUFFERED_MAX + frame.len(),
            "one buffered() call returned {size} bytes"
        );
        let mut total = first.len();
        loop {
            let more = session.buffered().unwrap();
            if more.is_empty() {
                break;
            }
            total += more.len();
        }
        assert_eq!(total, n, "no frame lost");
    }

    /// How a bidi stream ended, as seen from an h2 server faking the SDK.
    #[derive(Debug, PartialEq)]
    enum Close {
        /// The runtime sent END_STREAM on the request body, the SDK closed its
        /// half, nobody sent RST_STREAM.
        Clean,
        /// The runtime sent RST_STREAM with this reason.
        Reset(Option<h2::Reason>),
    }

    /// A one-stream h2c server faking the SDK: returns headers, writes one
    /// frame, then waits for the runtime to close. `end_on_request_end` = close
    /// its own half when the request body ends (like the TypeScript SDK's
    /// `runBidi` in the `woken === null` branch); `false` = a stuck SDK that
    /// never closes.
    async fn fake_sdk(end_on_request_end: bool) -> (String, tokio::sync::oneshot::Receiver<Close>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut conn = h2::server::handshake(socket).await.unwrap();
            let (req, mut respond) = conn.accept().await.unwrap().unwrap();
            // The connection must keep being polled for the frame to go out.
            tokio::spawn(async move { while conn.accept().await.is_some() {} });
            let resp = http::Response::builder()
                .status(200)
                .header("content-type", INVOCATION_V3)
                .body(())
                .unwrap();
            let mut send = respond.send_response(resp, false).unwrap();
            send.send_data(encode_stream(&[Message::End(pb::EndMessage {})]), false)
                .unwrap();
            let mut body = req.into_body();
            let outcome = loop {
                match body.data().await {
                    Some(Ok(chunk)) => {
                        let _ = body.flow_control().release_capacity(chunk.len());
                    }
                    Some(Err(e)) => break Some(Close::Reset(e.reason())),
                    None => break None,
                }
            };
            let outcome = match outcome {
                Some(o) => o,
                // The request body ended cleanly. An RST arriving **after**
                // END_STREAM is still an RST: nghttp2 counts it towards the
                // rapid-reset cap all the same.
                None => {
                    let wait = if end_on_request_end {
                        std::time::Duration::from_millis(500)
                    } else {
                        std::time::Duration::from_secs(30)
                    };
                    match tokio::time::timeout(wait, std::future::poll_fn(|cx| send.poll_reset(cx)))
                        .await
                    {
                        Ok(Ok(reason)) => Close::Reset(Some(reason)),
                        Ok(Err(e)) => Close::Reset(e.reason()),
                        Err(_) if end_on_request_end => {
                            send.send_data(Bytes::new(), true).unwrap();
                            Close::Clean
                        }
                        Err(_) => panic!("runtime did not close a stuck SDK stream in {wait:?}"),
                    }
                }
            };
            let _ = tx.send(outcome);
        });
        (url, rx)
    }

    /// Lets go of a bidi turn while the SDK still has its half open — the path
    /// of every turn waiting for the outside world (`drive_bidi` returns
    /// `Step::Done` after `Suspension`).
    async fn open_and_drop(url: &str) {
        let clients = Clients::default();
        let released = crate::lease::Released::for_test();
        let opening = encode_stream(&[Message::Start(pb::StartMessage::default())]);
        let mut session = open_bidi(
            &clients.bidi,
            url,
            "S",
            "h",
            3,
            opening,
            None,
            None,
            &released,
        )
        .await
        .unwrap();
        let f = session.next_frame(&released).await.unwrap().unwrap();
        assert_eq!(f.ty, MessageType::END);
        drop(session);
    }

    /// Letting go of a bidi stream is a **clean close**, not RST_STREAM.
    ///
    /// The interleaving reproduced (xcall at concurrency 64 over BIDI_STREAM,
    /// ~430 inv/s):
    ///
    /// 1. The SDK writes `Suspension` and leaves the stream open, waiting for
    ///    the runtime to push what it waits for.
    /// 2. The runtime sees that this must come from outside, ends the turn and
    ///    lets go of the `BidiSession` — h2 sends `RST_STREAM(CANCEL)` because
    ///    the SDK's half is still open.
    /// 3. nghttp2 (the h2 server of `node:http2`) has a rapid-reset cap
    ///    (CVE-2023-44487): 1 000 RSTs received, refilled at 33/s per
    ///    connection. Over the cap it **kills the whole connection** ("Protocol
    ///    error"): every turn open on it fails with "error decoding response
    ///    body", a turn opening a new stream fails with "error sending
    ///    request".
    ///
    /// Must see: the runtime sends END_STREAM on the request body, waits for
    /// the SDK to close its half, no RST at all.
    #[tokio::test]
    async fn dropping_bidi_stream_closes_cleanly_without_rst() {
        let (url, outcome) = fake_sdk(true).await;
        open_and_drop(&url).await;
        let got = tokio::time::timeout(std::time::Duration::from_secs(10), outcome)
            .await
            .expect("the fake server reported no result")
            .unwrap();
        assert_eq!(got, Close::Clean, "runtime sent RST, not a clean close");
    }

    /// A stuck SDK that does not close its half still gets its stream ended by
    /// the runtime — bounded, not holding the stream (and the place in the
    /// SDK's `SETTINGS_MAX_CONCURRENT_STREAMS`) forever.
    #[tokio::test]
    async fn stuck_sdk_not_closing_still_gets_stream_cut() {
        let (url, outcome) = fake_sdk(false).await;
        open_and_drop(&url).await;
        let got = tokio::time::timeout(std::time::Duration::from_secs(20), outcome)
            .await
            .expect("the runtime held a stuck SDK's stream for more than 20 s")
            .unwrap();
        assert_eq!(got, Close::Reset(Some(h2::Reason::CANCEL)));
    }

    #[test]
    fn empty_stream_is_valid() {
        // The SDK sent nothing at all — not a framing error; the layer above
        // decides whether that is a violation.
        assert!(decode_stream(Bytes::new()).unwrap().is_empty());
    }

    /// Every wire version the runtime speaks (1..=`WIRE_MAX`) has its **own**
    /// content-type `invocation.v<n>` that the runtime also accepts back: the
    /// SDK reads the agreed version from it (SDK v6 only sends reused refs
    /// when it sees v6).
    #[test]
    fn every_wire_version_has_its_own_content_type() {
        for v in 1..=crate::discovery::WIRE_MAX {
            let ct = invocation_content_type(v);
            assert!(ct.ends_with(&format!(".v{v}")), "wire v{v} ⇒ {ct}");
            assert!(known_invocation_type(ct), "{ct}");
        }
    }
}
