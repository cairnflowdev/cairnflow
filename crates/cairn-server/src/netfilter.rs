// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Network filter between nodes for the in-process test harness (inspired by
//! TiKV's transport simulator for its raftstore tests).
//!
//! **Only with the `testkit` feature.** With the feature off, this module, the
//! `NodeConfig::net_filter` field and the hook in
//! [`crate::auth::ClusterSecret::send`] do not exist — the release binary
//! carries none of it.
//!
//! One hook in `ClusterSecret::send` covers all four node-to-node paths:
//! inter-range messages (`Transport::send_once`), cluster-wide metric gathering
//! (`fan_out`, `cluster_pinned`), asking for a range yield during a merge
//! (`/internal/ranges/{id}/resign`) and notifying peers on shutdown
//! (`/internal/session/changed`). Followers read the bucket directly, not over
//! HTTP, so they are not filtered; cutting *node ↔ bucket* is `FaultyStore`'s
//! job.
//!
//! For each request, [`NetFilter::verdict`] receives the sending node's name
//! (the `node_id` of the node the filter is plugged into), the destination
//! address (`host:port` of the receiver's internal port) and `path?query`, and
//! returns a [`Verdict`]:
//!
//! - `Pass` — send as usual.
//! - `Drop` — do not send; the sender gets a network error **at once** (a real
//!   `reqwest::Error`, of the timeout kind). It does not wait out the request
//!   timeout, so tests do not pay 10 s for every blocked message.
//! - `Delay(d)` — send after `d`. The message **always arrives**, even when the
//!   sender has already hit the request timeout and gone — the shape of a late
//!   packet.
//! - `Hold` — park the request in [`NetFilter::hold`] as a [`Held`]; the test
//!   lets it go with [`Held::release`]. The sender waits until it is released,
//!   or until the request timeout (then it gets a timeout error, while the
//!   message stays there waiting to be released — a stale message arriving
//!   after the sender has retried).

use {
    std::{sync::Arc, sync::OnceLock, time::Duration},
    tokio::sync::oneshot,
};

/// The fate of one node-to-node request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    Pass,
    Drop,
    Delay(Duration),
    Hold,
}

/// The network filter. One instance is usually shared by every node of the
/// test cluster; `from` says which node is sending.
pub trait NetFilter: Send + Sync + 'static {
    /// `from` = `node_id` of the sending node; `to` = destination `host:port`;
    /// `path` = `path?query`. Called on the send path — must be cheap and must
    /// not block.
    fn verdict(&self, from: &str, to: &str, path: &str) -> Verdict;

    /// Receives a request that got `Hold`. Dropping it without `release` ⇒ the
    /// sender gets a network error (as with `Drop`).
    fn hold(&self, held: Held);
}

/// A held request — see [`Verdict::Hold`].
pub struct Held {
    from: String,
    to: String,
    path: String,
    client: reqwest::Client,
    req: reqwest::Request,
    reply: oneshot::Sender<reqwest::Result<reqwest::Response>>,
}

impl std::fmt::Debug for Held {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Held")
            .field("from", &self.from)
            .field("to", &self.to)
            .field("path", &self.path)
            .field("sender_waiting", &self.sender_waiting())
            .finish()
    }
}

impl Held {
    pub fn from(&self) -> &str {
        &self.from
    }

    pub fn to(&self) -> &str {
        &self.to
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    /// The request body, if it is a body built in memory (inter-range messages
    /// always are) — so the test knows **which message** is held, not only to
    /// which range.
    pub fn body(&self) -> Option<&[u8]> {
        self.req.body().and_then(|b| b.as_bytes())
    }

    /// Whether the sender is still waiting for the result — `false` once it hit
    /// the request timeout (or was cancelled): releasing then delivers a stale
    /// message nobody waits for.
    pub fn sender_waiting(&self) -> bool {
        !self.reply.is_closed()
    }

    /// Sends the request **now**, with the signature from send time. If the
    /// sender is still waiting it gets exactly this response; returns the HTTP
    /// status the receiver returned (or the network error) so the test can
    /// check the message's fate.
    pub async fn release(self) -> Result<u16, String> {
        let res = crate::egress::execute(&self.client, self.req).await;
        let status = match &res {
            Ok(r) => Ok(r.status().as_u16()),
            Err(e) => Err(e.to_string()),
        };
        let _ = self.reply.send(res);
        status
    }

    /// Sends the request now, but **the ack is lost**: the receiver processes
    /// it, the sender gets a network error (at once, without waiting for the
    /// receiver) — the shape of "message arrived, ack never came back"; the
    /// sender will resend a duplicate. Returns the HTTP status the receiver
    /// returned.
    pub async fn release_lose_reply(self) -> Result<u16, String> {
        let Held {
            client, req, reply, ..
        } = self;
        drop(reply);
        crate::egress::execute(&client, req)
            .await
            .map(|r| r.status().as_u16())
            .map_err(|e| e.to_string())
    }
}

/// The filter attached to a node's cluster secret: node name + the shared filter.
#[derive(Clone)]
pub(crate) struct NetHook {
    node: Arc<str>,
    filter: Arc<dyn NetFilter>,
}

impl NetHook {
    pub(crate) fn new(node: &str, filter: Arc<dyn NetFilter>) -> Self {
        Self {
            node: node.into(),
            filter,
        }
    }

    /// Replaces `client.execute(req)` in [`crate::auth::ClusterSecret::send`].
    pub(crate) async fn send(
        &self,
        client: &reqwest::Client,
        req: reqwest::Request,
    ) -> reqwest::Result<reqwest::Response> {
        let url = req.url();
        let to = match (url.host_str(), url.port_or_known_default()) {
            (Some(h), Some(p)) => format!("{h}:{p}"),
            (Some(h), None) => h.to_string(),
            _ => String::new(),
        };
        let path = match url.query() {
            Some(q) => format!("{}?{q}", url.path()),
            None => url.path().to_string(),
        };
        let wait = req.timeout().copied();
        let (tx, rx) = oneshot::channel();
        match self.filter.verdict(&self.node, &to, &path) {
            Verdict::Pass => return crate::egress::execute(client, req).await,
            Verdict::Drop => return Err(lost().await),
            Verdict::Delay(d) => {
                let client = client.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(d).await;
                    let _ = tx.send(crate::egress::execute(&client, req).await);
                });
            }
            Verdict::Hold => self.filter.hold(Held {
                from: self.node.to_string(),
                to,
                path,
                client: client.clone(),
                req,
                reply: tx,
            }),
        }
        let got = match wait {
            Some(t) => tokio::time::timeout(t, rx).await.ok(),
            None => Some(rx.await),
        };
        match got {
            Some(Ok(res)) => res,
            // Request timeout hit, or the message was held and then dropped.
            _ => Err(lost().await),
        }
    }
}

/// A real `reqwest::Error` of the timeout kind: a request to a listener that
/// never calls `accept` (the connection sits in the kernel backlog, nobody
/// answers), with a 1 ms timeout. A `reqwest::Error` cannot be built by hand,
/// and the sender classifies network errors by exactly that type.
#[allow(
    clippy::disallowed_methods,
    reason = "builds a real `reqwest::Error` against a mute listener, not a node request"
)]
async fn lost() -> reqwest::Error {
    if LocalNet::installed() {
        return LocalNet::lost().await;
    }
    static BLACKHOLE: OnceLock<Option<(std::net::TcpListener, String)>> = OnceLock::new();
    let url = BLACKHOLE
        .get_or_init(|| {
            let l = std::net::TcpListener::bind("127.0.0.1:0").ok()?;
            let url = format!("http://{}/netfilter-drop", l.local_addr().ok()?);
            Some((l, url))
        })
        .as_ref()
        .map(|(_, u)| u.clone())
        // Cannot bind ⇒ port 0: the connection fails at once, also a real network error.
        .unwrap_or_else(|| "http://127.0.0.1:0/netfilter-drop".into());
    match reqwest::Client::new()
        .get(url)
        .timeout(Duration::from_millis(1))
        .send()
        .await
    {
        Err(e) => e,
        Ok(_) => unreachable!("the netfilter listener never answers"),
    }
}

// ───────────────────────────────────────────────────────── in-process network

/// In-process network for deterministic simulation.
///
/// Installed for the **current thread** with [`LocalNet::install`] (the whole
/// sim cluster lives on one thread). Once installed: every node, once built,
/// registers the `Router` of its two ports under their real addresses
/// ([`LocalNet::serve`], called from `node::start`), and so does the fake SDK;
/// every outgoing request through `egress` to a registered address is handed
/// straight to that `Router` in its own task — no socket, no kernel, so there
/// is no real latency for tokio's paused clock to skip over by itself. The
/// real listeners stay open, nobody just calls them.
///
/// It keeps the shape of a real network where it matters: the handler runs in
/// its own task (a sender that gives up on a timeout still has its message
/// processed), and the request `timeout` counts on the runtime's clock. A
/// network error is a `reqwest::Error` built without I/O ([`LocalNet::lost`]).
pub struct LocalNet {
    routes: std::cell::RefCell<std::collections::BTreeMap<String, axum::Router>>,
    /// Next fake port ([`LocalNet::allocate_pair`]).
    next_port: std::cell::Cell<u16>,
}

/// First fake port that [`LocalNet::allocate_pair`] hands out.
const FIRST_PORT: u16 = 41_000;

thread_local! {
    static LOCAL_NET: std::cell::RefCell<Option<std::rc::Rc<LocalNet>>> =
        const { std::cell::RefCell::new(None) };
}

/// See [`LocalNet::install`]. Drop ⇒ removes the network from the thread.
#[must_use = "dropping it at once removes the network at once"]
pub struct LocalNetGuard(());

impl Drop for LocalNetGuard {
    fn drop(&mut self) {
        LOCAL_NET.with(|n| n.borrow_mut().take());
    }
}

impl LocalNet {
    /// Installs an empty network for the current thread.
    pub fn install() -> LocalNetGuard {
        LOCAL_NET.with(|n| {
            *n.borrow_mut() = Some(std::rc::Rc::new(LocalNet {
                routes: Default::default(),
                next_port: std::cell::Cell::new(FIRST_PORT),
            }))
        });
        LocalNetGuard(())
    }

    fn current() -> Option<std::rc::Rc<LocalNet>> {
        LOCAL_NET.with(|n| n.borrow().clone())
    }

    /// Whether this thread has an in-process network.
    pub fn installed() -> bool {
        Self::current().is_some()
    }

    /// Two addresses (ingress, internal) for a node just built, from the
    /// network's counter — the same build order ⇒ the same addresses on every
    /// run. Ports chosen by the OS differ between runs, and peers are sorted by
    /// advertised address (`Membership::peer_addresses`): the order of
    /// node-to-node messages would change from run to run. `None` when there is
    /// no network.
    pub fn allocate_pair() -> Option<(std::net::SocketAddr, std::net::SocketAddr)> {
        let net = Self::current()?;
        let p = net.next_port.get();
        net.next_port.set(p + 2);
        let at = |port| std::net::SocketAddr::from(([127, 0, 0, 1], port));
        Some((at(p), at(p + 1)))
    }

    /// Registers `router` under `addr` (`host:port`). Does nothing when no
    /// network is installed.
    pub fn serve(addr: &str, router: axum::Router) {
        if let Some(net) = Self::current() {
            net.routes.borrow_mut().insert(addr.to_string(), router);
        }
    }

    /// Removes `addr` — requests to it are network errors from now on (the node died).
    pub fn unserve(addr: &str) {
        if let Some(net) = Self::current() {
            net.routes.borrow_mut().remove(addr);
        }
    }

    /// A real `reqwest::Error` without touching any socket: for an unsupported
    /// scheme reqwest returns an error at once, before any I/O.
    #[allow(
        clippy::disallowed_methods,
        reason = "builds a real `reqwest::Error` without touching any socket"
    )]
    pub async fn lost() -> reqwest::Error {
        match reqwest::Client::new()
            .get("cairn-sim-lost://netfilter-drop/")
            .send()
            .await
        {
            Err(e) => e,
            Ok(_) => unreachable!("an unknown scheme never has a response"),
        }
    }

    /// Sends `req` over the in-process network — for the sim itself (a client
    /// outside the cluster). Unregistered address or no network installed ⇒
    /// network error, never a socket.
    pub async fn call(req: reqwest::Request) -> reqwest::Result<reqwest::Response> {
        match Self::route(req) {
            Ok(local) => local.await,
            Err(_) => Err(Self::lost().await),
        }
    }

    /// `Ok(response future)` if `req` goes to a registered address; otherwise
    /// returns `req` to be sent for real. The large `Err` is deliberate: the
    /// request is returned by value, with no extra allocation on the real send
    /// path.
    #[allow(clippy::type_complexity, clippy::result_large_err)]
    pub(crate) fn route(
        req: reqwest::Request,
    ) -> Result<
        std::pin::Pin<
            Box<dyn std::future::Future<Output = reqwest::Result<reqwest::Response>> + Send>,
        >,
        reqwest::Request,
    > {
        let Some(net) = Self::current() else {
            return Err(req);
        };
        let addr = match (req.url().host_str(), req.url().port_or_known_default()) {
            (Some(h), Some(p)) => format!("{h}:{p}"),
            _ => return Err(req),
        };
        let router = net.routes.borrow().get(&addr).cloned();
        let Some(mut router) = router else {
            // Address not registered while a network is installed: the node
            // died, or a destination outside the sim cluster — network error,
            // never a real socket.
            return Ok(Box::pin(async { Err(Self::lost().await) }));
        };
        Ok(Box::pin(async move {
            let wait = req.timeout().copied();
            let req: http::Request<reqwest::Body> = match req.try_into() {
                Ok(r) => r,
                Err(e) => return Err(e),
            };
            let req = req.map(axum::body::Body::new);
            let handled = tokio::spawn(async move {
                use tower_service::Service;
                std::future::poll_fn(|cx| {
                    <axum::Router as Service<http::Request<axum::body::Body>>>::poll_ready(
                        &mut router,
                        cx,
                    )
                })
                .await
                .ok()?;
                router.call(req).await.ok()
            });
            let got = match wait {
                Some(t) => tokio::time::timeout(t, handled).await.ok(),
                None => Some(handled.await),
            };
            match got {
                Some(Ok(Some(resp))) => Ok(reqwest::Response::from(
                    resp.map(|b| reqwest::Body::wrap_stream(b.into_data_stream())),
                )),
                _ => Err(Self::lost().await),
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use {super::*, std::time::Duration};

    /// A request to a registered address goes straight into the `Router`; an
    /// unknown address is a network error at once, no socket; `timeout` counts
    /// on the runtime's (paused) clock, and the handler still runs to the end
    /// after the sender gives up.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn localnet_delivers_straight_to_router() {
        let _net = LocalNet::install();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let done_tx = std::sync::Mutex::new(Some(done_tx));
        let done_tx = Arc::new(done_tx);
        let app = axum::Router::new()
            .route(
                "/hi",
                axum::routing::post(|body: String| async move { format!("hi {body}") }),
            )
            .route(
                "/slow",
                axum::routing::get(move || {
                    let done_tx = Arc::clone(&done_tx);
                    async move {
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        if let Some(tx) = done_tx.lock().unwrap().take() {
                            let _ = tx.send(());
                        }
                        "slow"
                    }
                }),
            );
        LocalNet::serve("10.9.9.9:80", app);
        let client = reqwest::Client::new();

        let req = client
            .post("http://10.9.9.9:80/hi")
            .body("sim")
            .build()
            .unwrap();
        let resp = crate::egress::execute(&client, req).await.unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(resp.text().await.unwrap(), "hi sim");

        let req = client.get("http://10.9.9.8:80/hi").build().unwrap();
        assert!(crate::egress::execute(&client, req).await.is_err());

        let t0 = tokio::time::Instant::now();
        let req = client
            .get("http://10.9.9.9:80/slow")
            .timeout(Duration::from_secs(1))
            .build()
            .unwrap();
        assert!(crate::egress::execute(&client, req).await.is_err());
        assert_eq!(t0.elapsed(), Duration::from_secs(1));
        // The message arrived: the handler runs to the end even though the sender gave up.
        done_rx.await.unwrap();
        assert_eq!(t0.elapsed(), Duration::from_secs(5));

        LocalNet::unserve("10.9.9.9:80");
        let req = client
            .post("http://10.9.9.9:80/hi")
            .body("x")
            .build()
            .unwrap();
        assert!(crate::egress::execute(&client, req).await.is_err());
    }
}
