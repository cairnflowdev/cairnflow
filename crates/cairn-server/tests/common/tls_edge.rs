// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! A fake TLS edge in front of the fake SDK: **terminates TLS, speaks h2 to the
//! runtime, h1 to the app, and buffers the whole request body** before forwarding —
//! exactly the shape of the PaaS edges (Heroku, Railway, Render, Vercel, Netlify)
//! that `BIDI_STREAM` cannot get through.
//!
//! - ALPN offers only `h2`: the tenant's h1 client (discovery, `REQUEST_RESPONSE`)
//!   negotiates h2 over TLS as with a real edge; the BIDI client is h2 too.
//! - Each h2 stream is read up to `END_STREAM` before **one** h1 request is sent to
//!   the app. A `BIDI_STREAM` turn leaves its request body open (the runtime waits
//!   for a response before sending more), so it is never forwarded and never gets
//!   response headers — the hang to reproduce.
//! - `identity` ≠ `None` ⇒ inject `requestIdentity` into the `/discover` manifest
//!   (the SaaS profile requires the SDK to declare verification; the fake SDK does
//!   not), and inject `requestCompression: "gzip"` (the SaaS profile requires
//!   compressed RR bodies): the runtime's gzip bodies are decoded here before reaching
//!   the fake SDK — the fake SDK knows no gzip, the real TS SDK decodes them itself.
//!   Responses go out uncompressed (the runtime accepts both).
//!
//! Certificates: a CA and a leaf for `localhost`/`127.0.0.1` in `tests/fixtures/tls/`
//! (valid until 2126; the CA's key is not kept). The runtime trusts the CA through
//! [`cairn_server::egress::EgressPolicy::tls_roots`] (`testkit` build).

use {
    anyhow::{Context, Result},
    bytes::{Bytes, BytesMut},
    std::{
        path::PathBuf,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    },
    tokio::task::JoinHandle,
    tokio_rustls::rustls::{
        self,
        pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer},
    },
};

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("tests/fixtures/tls/{name}"))
}

/// The PEM path of the CA the edge uses — handed to the runtime as a trust root.
pub fn ca_path() -> PathBuf {
    fixture("ca.pem")
}

/// The edge's CA as a root for the tenant client.
pub fn ca_roots() -> Vec<reqwest::Certificate> {
    let pem = std::fs::read(ca_path()).expect("fixture tls/ca.pem");
    reqwest::Certificate::from_pem_bundle(&pem).expect("valid ca.pem")
}

/// A running edge. Drop ⇒ stops accepting connections.
pub struct TlsEdge {
    /// `https://127.0.0.1:<port>`.
    pub url: String,
    /// The number of streams forwarded to the app (whole body read).
    pub forwarded: Arc<AtomicUsize>,
    /// The number of streams opened to the edge (including streams whose body never ends).
    pub opened: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl Drop for TlsEdge {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl TlsEdge {
    /// Build the edge in front of `upstream` (the fake SDK's `http://…`).
    pub async fn spawn(upstream: &str, identity: Option<&'static str>) -> Result<Self> {
        let certs = CertificateDer::pem_file_iter(fixture("edge.pem"))
            .context("tls/edge.pem")?
            .collect::<Result<Vec<_>, _>>()?;
        let key = PrivateKeyDer::from_pem_file(fixture("edge.key")).context("tls/edge.key")?;
        let mut cfg = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
        cfg.alpn_protocols = vec![b"h2".to_vec()];
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(cfg));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("https://{}", listener.local_addr()?);
        let upstream = upstream.trim_end_matches('/').to_string();
        let forwarded = Arc::new(AtomicUsize::new(0));
        let opened = Arc::new(AtomicUsize::new(0));
        let (fwd, open) = (Arc::clone(&forwarded), Arc::clone(&opened));
        let app = reqwest::Client::builder().http1_only().build()?;
        let task = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let (acceptor, upstream, app) = (acceptor.clone(), upstream.clone(), app.clone());
                let (fwd, open) = (Arc::clone(&fwd), Arc::clone(&open));
                tokio::spawn(async move {
                    let Ok(tls) = acceptor.accept(socket).await else {
                        return;
                    };
                    let Ok(mut conn) = h2::server::handshake(tls).await else {
                        return;
                    };
                    while let Some(Ok((req, respond))) = conn.accept().await {
                        open.fetch_add(1, Ordering::SeqCst);
                        let (upstream, app, fwd) =
                            (upstream.clone(), app.clone(), Arc::clone(&fwd));
                        tokio::spawn(async move {
                            let _ = relay(req, respond, &upstream, &app, identity, &fwd).await;
                        });
                    }
                });
            }
        });
        Ok(Self {
            url,
            forwarded,
            opened,
            task,
        })
    }
}

/// Buffer an h2 stream's whole body, send one h1 request to the app, return the response.
async fn relay(
    req: http::Request<h2::RecvStream>,
    mut respond: h2::server::SendResponse<Bytes>,
    upstream: &str,
    app: &reqwest::Client,
    identity: Option<&'static str>,
    forwarded: &AtomicUsize,
) -> Result<()> {
    let (head, mut body) = req.into_parts();
    let mut buf = BytesMut::new();
    // Buffer up to `END_STREAM` — an open body waits here forever.
    while let Some(chunk) = body.data().await {
        let chunk = chunk?;
        let _ = body.flow_control().release_capacity(chunk.len());
        buf.extend_from_slice(&chunk);
    }
    forwarded.fetch_add(1, Ordering::SeqCst);
    let gzipped = head
        .headers
        .get(http::header::CONTENT_ENCODING)
        .is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"gzip"));
    let body = if gzipped {
        let mut plain = Vec::new();
        std::io::Read::read_to_end(&mut flate2::read::GzDecoder::new(&buf[..]), &mut plain)?;
        Bytes::from(plain)
    } else {
        buf.freeze()
    };
    let path = head.uri.path_and_query().map_or("/", |p| p.as_str());
    let mut out = app.request(head.method.clone(), format!("{upstream}{path}"));
    for (k, v) in &head.headers {
        if !matches!(
            k.as_str(),
            "host"
                | "content-length"
                | "te"
                | "connection"
                | "content-encoding"
                | "accept-encoding"
        ) {
            out = out.header(k, v);
        }
    }
    let r = out.body(body).send().await?;
    let status = r.status();
    let ct = r.headers().get(http::header::CONTENT_TYPE).cloned();
    let mut bytes = r.bytes().await?;
    if let (Some(v), "/discover") = (identity, head.uri.path()) {
        let mut m: serde_json::Value = serde_json::from_slice(&bytes)?;
        m["requestIdentity"] = v.into();
        m["requestCompression"] = "gzip".into();
        bytes = Bytes::from(serde_json::to_vec(&m)?);
    }
    let mut resp = http::Response::builder().status(status.as_u16());
    if let Some(ct) = ct {
        resp = resp.header(http::header::CONTENT_TYPE, ct);
    }
    let mut send = respond.send_response(resp.body(())?, false)?;
    send.send_data(bytes, true)?;
    Ok(())
}
