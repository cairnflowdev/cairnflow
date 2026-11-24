// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `RunningNode`: the running node, background tasks, shutdown.

use super::*;

/// Closes the session when it goes out of scope — unless [`Self::disarm`]ed.
/// For the error paths of [`start`] after the session has been opened.
pub(super) struct CloseOnDrop(pub(super) Option<Session>);

impl CloseOnDrop {
    pub(super) fn disarm(mut self) {
        self.0 = None;
    }
}

impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        if let Some(s) = self.0.take() {
            tokio::spawn(async move { s.close().await });
        }
    }
}

/// Background tasks of a node. Drop ⇒ cancel them all, so no task outlives
/// the node that spawned it — even when `start` fails midway or a test panics
/// before it gets to `shutdown`.
#[derive(Default)]
pub(super) struct Tasks(pub(super) Vec<JoinHandle<()>>);

impl Tasks {
    pub(super) fn push(&mut self, t: JoinHandle<()>) {
        self.0.push(t);
    }

    async fn stop(&mut self) {
        for t in self.0.drain(..) {
            t.abort();
            let _ = t.await;
        }
    }
}

impl Drop for Tasks {
    fn drop(&mut self) {
        for t in &self.0 {
            t.abort();
        }
    }
}

/// A running node. See [`RunningNode::shutdown`].
pub struct RunningNode {
    /// The **real** HTTP address of the ingress port (the port actually
    /// assigned, not the one requested).
    pub addr: SocketAddr,
    /// Real address of the internal port — what is advertised to peers.
    pub internal_addr: SocketAddr,
    /// Cluster secret — requests to the internal port must be signed with it.
    pub cluster_secret: crate::auth::ClusterSecret,
    /// Node identity — the identity on leases.
    pub node_id: String,
    /// Engines of every range, by range id — a shared set that follows the table.
    pub partitions: Partitions,
    pub routing: RoutingTable,
    pub cluster: ClusterConfig,
    /// The node's session (D20).
    pub session: Session,
    /// The node's split/merge coordinator.
    pub ranges: crate::split::RangeOps,
    /// The node's routing path, not over HTTP.
    pub router: crate::ingress::Router,
    /// The quarantine list this node currently applies.
    pub quarantine: crate::quarantine::Quarantine,
    /// The node's balancing policy — the release log for tests (LS-10).
    pub balancer: crate::balance::Balancer,
    /// Per-range load registry (writer, L0 throttle) — for tests that build
    /// their own background loop on the same node (`lineage::Lineage`).
    pub load_book: crate::autosplit::LoadBook,
    pub(super) tasks: Tasks,
    /// The metering flusher — a graceful shutdown flushes one last time.
    pub(super) metering: crate::metering::Flusher,
    pub(super) history: crate::stats_history::History,
    pub(super) server: Option<JoinHandle<std::io::Result<()>>>,
    pub(super) stop_server: Option<oneshot::Sender<()>>,
    /// Exclusive lock on the local store directory (`file://`), held for the
    /// node's whole life. Declared **last**: dropped after everything else of
    /// the node.
    pub(super) _local_lock: Option<cairn_store::local::LocalLock>,
}

/// Wait limit for each shutdown step. Shutdown is cleanup, not a place
/// allowed to hang: a stuck engine must not turn `shutdown` into a hung test.
pub(super) const SHUTDOWN_STEP: Duration = Duration::from_secs(5);

/// Drop without [`RunningNode::shutdown`] (a test panicking midway): the
/// background tasks have cancelled themselves through `Tasks`, and the server
/// is cancelled here — it holds handles to every engine, so cancelling it
/// closes the command channels and the engine threads end on their own.
impl Drop for RunningNode {
    fn drop(&mut self) {
        if let Some(server) = self.server.take() {
            server.abort();
        }
        // Like `kill -9`: stop renewing, write nothing. Others mark the
        // session after one TTL of silence.
        self.session.abort();
    }
}

impl RunningNode {
    /// `http://host:port` of the node.
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// `http://host:port` of the internal port.
    pub fn internal_url(&self) -> String {
        format!("http://{}", self.internal_addr)
    }

    /// Waits for the HTTP server to stop — for the binary this is "run
    /// forever". The server's I/O errors come out here.
    pub async fn wait(&mut self) -> Result<(), Error> {
        if let Some(server) = self.server.as_mut() {
            let r = server.await;
            self.server = None;
            r??;
        }
        Ok(())
    }

    /// Stops the node: stop accepting new connections, stop every background
    /// task, shut the engines down (seal the writer, release the lease), close
    /// the session, notify peers, then wait for the HTTP server to close.
    ///
    /// The order is deliberate: shut the engines down **before** waiting for
    /// the server, because in-flight requests (an `/invoke` waiting for its
    /// result) only finish when the engine answers — even if with an error.
    ///
    /// This is the `SIGTERM` path: the goal is for peers to take all our ranges
    /// in < 2 s instead of waiting for the leases to expire.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.stop_server.take() {
            let _ = tx.send(());
        }
        self.tasks.stop().await;
        // Shut every range down **in parallel**: each seal waits for a WAL
        // flush to the object store (tens to hundreds of milliseconds), and
        // sequentially the last range would wait for the sum of all before it
        // — a hundred ranges is several seconds without an owner.
        let mut stopping = tokio::task::JoinSet::new();
        for (p, h) in self.partitions.iter() {
            stopping
                .spawn(async move { (p, tokio::time::timeout(SHUTDOWN_STEP, h.shutdown()).await) });
        }
        let stop_engines = async {
            while let Some(done) = stopping.join_next().await {
                match done {
                    Ok((_, Ok(Ok(())))) => {}
                    Ok((p, Ok(Err(e)))) => tracing::debug!(partition = p, error = %e, "engine off"),
                    Ok((p, Err(_))) => {
                        tracing::warn!(partition = p, "engine did not answer the shutdown command")
                    }
                    Err(e) => tracing::warn!(error = %e, "engine shutdown task failed"),
                }
            }
        };
        // Who needs to be told when we leave — found **while** the engines are
        // sealing, so the shutdown is not one LIST round longer.
        let find_peers = async {
            tokio::time::timeout(
                PEER_NOTIFY_TIMEOUT,
                self.session.membership().discover_peers(),
            )
            .await
            .unwrap_or_default()
        };
        let ((), peers) = tokio::join!(stop_engines, find_peers);
        // The engines have stopped: no turn is counting any more. Flush one
        // last time — the periodic flush task was cancelled along with `tasks`,
        // and previously the count since the last flush (up to one interval,
        // 60 s by default) was lost on every graceful shutdown.
        if tokio::time::timeout(SHUTDOWN_STEP, self.metering.flush())
            .await
            .is_err()
        {
            tracing::warn!("final metering flush at shutdown over its limit");
        }
        if tokio::time::timeout(SHUTDOWN_STEP, self.history.flush())
            .await
            .is_err()
        {
            tracing::warn!("final durable stats flush at shutdown over its limit");
        }
        // The session closes **after** the engines: the engines yield their
        // partitions on shutdown, and the session must still be alive for
        // those yields to mean anything. That is why the session is not in
        // `tasks` — stopping it first would let its deadline pass while the
        // partitions shut down one by one.
        self.session.close().await;
        // `closed` is on the bucket now, but peers only see it at their next
        // observation round — up to TTL/5 later (2 s with the default TTL), and
        // meanwhile nobody takes our ranges. Tell peers directly to observe
        // again now. Only a hint: if it fails peers still see it next round.
        notify_peers(&peers, &self.cluster_secret).await;
        if let Some(mut server) = self.server.take() {
            if tokio::time::timeout(SHUTDOWN_STEP, &mut server)
                .await
                .is_err()
            {
                tracing::warn!("HTTP server did not close in time — cancelling");
                server.abort();
                let _ = server.await;
            }
        }
    }
}

/// Wait limit for finding peers, and for one hint sent to a peer, at shutdown
/// — a failed hint is still seen by the peer at its observation round, so it
/// is not worth making the shutdown longer.
const PEER_NOTIFY_TIMEOUT: Duration = Duration::from_millis(500);

/// Tells every peer "our session just changed — observe again now"
/// (`POST /internal/session/changed`), in parallel, waiting no longer than
/// [`PEER_NOTIFY_TIMEOUT`].
async fn notify_peers(peers: &[String], secret: &crate::auth::ClusterSecret) {
    if peers.is_empty() {
        return;
    }
    let client = reqwest::Client::new();
    let mut sends = tokio::task::JoinSet::new();
    for addr in peers {
        let req = client
            .post(format!(
                "{}/internal/session/changed",
                addr.trim_end_matches('/')
            ))
            .timeout(PEER_NOTIFY_TIMEOUT);
        let addr = addr.clone();
        let (client, secret) = (client.clone(), secret.clone());
        sends.spawn(async move {
            if let Err(e) = secret.send(&client, req).await {
                tracing::debug!(peer = %addr, error = %e, "could not notify peer at shutdown");
            }
        });
    }
    while sends.join_next().await.is_some() {}
}
