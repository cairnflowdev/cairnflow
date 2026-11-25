// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The HTTP accept loop of both ports.
//!
//! Replaces `axum::serve`, which attaches no timer (no header read limit: a
//! client keeps a connection forever with half a header) and, through
//! `auto::Builder`, also answers h2c — one HTTP/2 connection opens many
//! streams and so bypasses every HTTP/1 limit. Here:
//!
//! - **HTTP/1 only** (`http1::Builder`): an HTTP/2 preface is a malformed
//!   request;
//! - **header read limit** (`header_read_timeout`): hyper's HTTP/1 has no
//!   separate idle timeout — the same timer also runs while a keep-alive
//!   connection waits for the next request, so the limit both stops slowloris
//!   and acts as the idle timeout (it must be larger than the idle timeout of
//!   the proxy in front, `docs/self-hosting.md`);
//! - **request body silence limit** ([`IdleBody`], same value as the header
//!   limit): hyper's header limit stops at the header — a complete header
//!   followed by silence leaves a path that reads the body **before**
//!   authentication (`/ui/session`, the forwarding layer, the internal port)
//!   waiting forever. A body silent past the limit between two chunks ⇒ a body
//!   read error, which the handler answers;
//! - **`ConnectInfo<SocketAddr>`** attached to every request, as
//!   `into_make_service_with_connect_info` does — without it `peer_ip` returns
//!   `None`, every per-IP limiter falls into one bucket and `--trust-proxy`
//!   has no effect;
//! - **graceful shutdown** (`graceful`): stop accepting, tell every connection
//!   to close after its running request, wait with the limit [`DRAIN`].
//!
//! No cap on the number of connections: behind a platform proxy every
//! connection comes from a few addresses, so one shared cap is one shared
//! bucket. Long-lived connections are capped **per tenant** in ingress
//! (`TenantSlot`); the internal port is uncapped (heartbeats and
//! `registry/apply` must not queue behind `/internal/forward`).
//!
//! Deterministic simulation does not go through here: the router lives in
//! `netfilter::LocalNet`.

use {
    axum::{extract::ConnectInfo, Router},
    hyper::{body::Incoming, server::conn::http1},
    hyper_util::{
        rt::{TokioIo, TokioTimer},
        server::graceful::GracefulShutdown,
    },
    std::time::Duration,
    tokio::{net::TcpListener, sync::watch},
};

/// Limit on waiting for open connections to close after a shutdown. SSE and
/// attach may stay open up to 600 s/60 s; past the limit they are dropped —
/// clients reconnect to another node.
pub(super) const DRAIN: Duration = Duration::from_secs(30);

/// Serves `app` on `listener` until `stop` becomes `true` (or the sender is
/// dropped), then drains with a limit.
pub(super) async fn serve(
    listener: TcpListener,
    app: Router,
    header_timeout: Duration,
    mut stop: watch::Receiver<bool>,
) -> std::io::Result<()> {
    let mut builder = http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        .header_read_timeout(header_timeout);
    let graceful = GracefulShutdown::new();
    loop {
        let accepted = tokio::select! {
            r = listener.accept() => r,
            _ = stop.wait_for(|v| *v) => break,
        };
        let (tcp, peer) = match accepted {
            Ok(c) => c,
            Err(e) => {
                accept_error(e).await;
                continue;
            }
        };
        let app = app.clone();
        let svc = hyper::service::service_fn(move |mut req: hyper::Request<Incoming>| {
            req.extensions_mut().insert(ConnectInfo(peer));
            let mut app = app.clone();
            async move {
                let req = req.map(|b| axum::body::Body::new(IdleBody::new(b, header_timeout)));
                tower_service::Service::call(&mut app, req).await
            }
        });
        let conn = graceful.watch(builder.serve_connection(TokioIo::new(tcp), svc));
        tokio::spawn(async move {
            // An error on one connection (client hung up, header limit hit,
            // malformed request) is that connection's business.
            if let Err(e) = conn.await {
                tracing::trace!(peer = %peer, error = %e, "HTTP connection closed");
            }
        });
    }
    drop(listener);
    if tokio::time::timeout(DRAIN, graceful.shutdown())
        .await
        .is_err()
    {
        tracing::warn!(
            drain_s = DRAIN.as_secs(),
            "HTTP connections still open after the drain limit — dropping"
        );
    }
    Ok(())
}

/// An `accept` error. An error of the connection itself (the client left mid
/// handshake) ⇒ accept again at once; any other error (out of file
/// descriptors) ⇒ pause 1 s to let the system breathe — as `axum::serve`
/// does, so the loop does not spin and eat a whole core.
async fn accept_error(e: std::io::Error) {
    use std::io::ErrorKind::{ConnectionAborted, ConnectionRefused, ConnectionReset};
    if matches!(
        e.kind(),
        ConnectionRefused | ConnectionAborted | ConnectionReset
    ) {
        return;
    }
    tracing::error!(error = %e, "accept failed — pausing 1 s");
    cairn_store::pace::retry_pause(Duration::from_secs(1)).await;
}

/// A request body with a silence limit: no chunk (nor end of body) within
/// `idle` since the last successful read ⇒ error. Plus a total limit of
/// `2 × idle` from the request's arrival: a trickling body (1 byte every half
/// limit) cannot hold a request forever. Default 150 s for bodies ≤ 2 MiB (the
/// extractor's cap) — enough for links ≥ 14 KB/s.
struct IdleBody {
    inner: Incoming,
    idle: Duration,
    deadline: std::pin::Pin<Box<tokio::time::Sleep>>,
    /// Total limit: `2 × idle` from the request's arrival.
    total: std::pin::Pin<Box<tokio::time::Sleep>>,
}

impl IdleBody {
    fn new(inner: Incoming, idle: Duration) -> Self {
        Self {
            inner,
            idle,
            deadline: Box::pin(tokio::time::sleep(idle)),
            total: Box::pin(tokio::time::sleep(idle * 2)),
        }
    }
}

/// Error of [`IdleBody`]: the body was silent past the limit, or flowed past the total limit.
#[derive(Debug)]
enum BodyIdle {
    Silent(Duration),
    Slow(Duration),
}

impl std::fmt::Display for BodyIdle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Silent(d) => write!(f, "request body silent for more than {d:?}"),
            Self::Slow(d) => write!(f, "request body took longer than {d:?}"),
        }
    }
}

impl std::error::Error for BodyIdle {}

impl hyper::body::Body for IdleBody {
    type Data = hyper::body::Bytes;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        use std::task::Poll;
        let this = &mut *self;
        if !this.inner.is_end_stream()
            && std::future::Future::poll(this.total.as_mut(), cx).is_ready()
        {
            return Poll::Ready(Some(Err(Box::new(BodyIdle::Slow(this.idle * 2)))));
        }
        match std::pin::Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(frame) => {
                let next = tokio::time::Instant::now() + this.idle;
                this.deadline.as_mut().reset(next);
                Poll::Ready(frame.map(|f| f.map_err(Into::into)))
            }
            Poll::Pending => match std::future::Future::poll(this.deadline.as_mut(), cx) {
                Poll::Ready(()) => Poll::Ready(Some(Err(Box::new(BodyIdle::Silent(this.idle))))),
                Poll::Pending => Poll::Pending,
            },
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner.size_hint()
    }
}
