// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Notifying **cluster-level registry** changes between nodes.
//!
//! The cluster-level registries — deployments and catalogs, API keys, tenant settings,
//! capture requests, per-range flush windows, the quarantine list — live on the bucket,
//! and each node re-reads them in `RefreshLoop` (node/refresh.rs); the rule book is
//! re-read by each range's engine on a tick (`RULES_POLL_INTERVAL`, actor/serve.rs) and by
//! the generation of `RuleStore`. That poll tick (5 s) used to be the only propagation path:
//! a few GETs + a few LISTs per node per tick, ~13 000 requests an hour for an idle
//! 3-node cluster. Now a node that serves a successful registry write nudges its own
//! refresh round and notifies every peer through the internal port
//! (`POST /internal/registry/changed`, signed with the cluster secret); the poll tick is
//! raised to 30 s and only remains a net for missed notifications (a peer not yet in the
//! session table, dropped packets). The notification carries no data: the receiver reads
//! the bucket itself, so a forged notification only costs one refresh round — and that
//! round has a floor [`MIN_REFRESH_GAP`].
//!
//! The quarantine list (`quarantine/`) is written by the node itself (the detector, manual
//! removal), not through the [`on_change`] layer: `Quarantine` wraps its write in
//! [`RegistrySync::write`] ([`After::Publish`]), and the notification also nudges the
//! receiver's quarantine round — before that, that round polled every 5 s.
//!
//! Every registry write goes through [`RegistrySync::write`]: the registry generation
//! (`control/registry-gen`, [`crate::registry_gen`]) goes odd before the main write and
//! even after it, and only then notifies: a node's poll tick with nothing changed costs
//! only one `If-None-Match` GET instead of a full round, and a writer that dies midway
//! leaves an odd generation — a reader that sees odd runs a full round every tick.

use {
    axum::{
        extract::{Request, State},
        http::Method,
        middleware::Next,
        response::{IntoResponse, Response},
    },
    cairn_store::retry,
    std::{sync::Arc, time::Duration},
    tokio::sync::Notify,
};

/// Floor between two registry refresh rounds: a burst of registry writes (registering
/// many deployments at once) merges into one round.
pub const MIN_REFRESH_GAP: Duration = Duration::from_millis(100);

/// Per-peer wait limit when notifying a change: a hung peer holds the reply to the
/// registry write request at most this long.
const PEER_NOTIFY_TIMEOUT: Duration = Duration::from_millis(500);

/// Per-peer wait limit for confirming that a change that must be synchronous has been
/// applied (key revocation). Past it the peer goes into the answer's `pending`.
const PEER_ACK_TIMEOUT: Duration = Duration::from_secs(2);

/// Limit on the session LIST that finds peers before notifying; past it, notify by the existing table.
const PEER_DISCOVER_TIMEOUT: Duration = Duration::from_secs(2);

struct Inner {
    local: Notify,
    /// The quarantine round (`Quarantine::spawn`) waits on this — a separate `Notify`,
    /// because `notify_one` only wakes one waiter.
    quarantine: Notify,
    /// The tenant purge round (`node/setup.rs::spawn_tenant_purge`) waits on this:
    /// changing a tenant's state (`PUT …/state`) is a registry write, and phase A's hold
    /// entries must appear (and be removed when reversed) at once, not after the round's
    /// tick (up to 5 minutes) — a backup taken in that gap would carry orphan hold entries.
    tenant_purge: Notify,
    /// The node's rule book: a registry change notification bumps its generation, the engine asks again.
    rules: Option<cairn_store::RuleStore>,
    session: Option<cairn_store::Session>,
    secret: Option<crate::auth::ClusterSecret>,
    client: reqwest::Client,
    /// Registry generation and the full reconcile tick; absent (single process, old tests) ⇒
    /// a full round every tick, nothing bumped.
    gen: Option<(crate::registry_gen::RegistryGen, Duration)>,
}

/// A node's "cluster-level registry just changed" signal.
#[derive(Clone)]
pub struct RegistrySync(Arc<Inner>);

/// What to notify after a registry write ([`RegistrySync::write`]), decided by the result
/// of the main write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum After {
    /// The registry changed: nudge the node's own refresh round, notify every peer.
    Notify,
    /// The registry changed and the writer applied it to its RAM copy itself (quarantine:
    /// `Quarantine::replace_local`): notify peers only.
    Publish,
    /// Nothing changed, or failed (possibly a partial write): only seal the generation —
    /// any round that sees the generation change runs in full.
    Quiet,
}

impl RegistrySync {
    /// `session`/`secret` absent (single process, old tests) ⇒ only nudge the node's own
    /// round. `rules`: a registry change notification also tells the engine to re-ask the rule book.
    pub fn new(
        session: Option<cairn_store::Session>,
        secret: Option<crate::auth::ClusterSecret>,
        rules: Option<cairn_store::RuleStore>,
    ) -> Self {
        RegistrySync(Arc::new(Inner {
            local: Notify::new(),
            quarantine: Notify::new(),
            tenant_purge: Notify::new(),
            rules,
            session,
            secret,
            client: reqwest::Client::new(),
            gen: None,
        }))
    }

    /// Attach the registry generation (at construction, before sharing `Clone`s): writers bump
    /// it, re-read rounds ask it, a full reconcile every `reconcile`.
    pub fn with_gen(self, gen: crate::registry_gen::RegistryGen, reconcile: Duration) -> Self {
        let mut inner = Arc::try_unwrap(self.0)
            .unwrap_or_else(|_| panic!("RegistrySync::with_gen called after Clone was shared"));
        inner.gen = Some((gen, reconcile));
        RegistrySync(Arc::new(inner))
    }

    /// Every registry the node serves from was re-read after `at` — see
    /// `RegistryGen::confirm_full`. No generation ⇒ does nothing.
    pub fn confirm_full(&self, at: tokio::time::Instant) {
        if let Some((g, _)) = &self.0.gen {
            g.confirm_full(at);
        }
    }

    /// The key registry exactly as on the bucket at `at` — see `RegistryGen::confirm`.
    /// No generation ⇒ does nothing.
    pub fn confirm(&self, at: tokio::time::Instant) {
        if let Some((g, _)) = &self.0.gen {
            g.confirm(at);
        }
    }

    /// Whether the key registry still counts as fresh — see `RegistryGen::fresh`. No
    /// generation ⇒ always fresh.
    pub fn fresh(&self) -> bool {
        self.0.gen.as_ref().is_none_or(|(g, _)| g.fresh())
    }

    /// Generation tracker for one re-read round; `None` ⇒ a full round every tick.
    pub fn watch(&self) -> Option<crate::registry_gen::GenWatch> {
        self.0.gen.as_ref().map(|(g, every)| g.watch(*every))
    }

    /// The node's registry refresh round waits on this (alongside the poll tick).
    pub async fn notified(&self) {
        self.0.local.notified().await
    }

    /// The node's quarantine round waits on this (alongside the poll tick).
    pub async fn quarantine_notified(&self) {
        self.0.quarantine.notified().await
    }

    /// The node's tenant purge round waits on this (alongside the round's tick).
    pub async fn tenant_purge_notified(&self) {
        self.0.tenant_purge.notified().await
    }

    /// Nudge the node's own registry refresh round, quarantine round and tenant purge
    /// round, and tell the engine to re-ask the rule book.
    pub fn poke_local(&self) {
        self.0.local.notify_one();
        self.0.quarantine.notify_one();
        self.0.tenant_purge.notify_one();
        if let Some(r) = &self.0.rules {
            r.mark_changed();
        }
    }

    /// **Every** cluster-level registry write goes through here: open an odd
    /// generation ([`RegistryGen::begin`]), run the main write `write`, seal it
    /// even ([`RegistryGen::seal`]), then notify per `after(result)`. Cannot open the
    /// generation (a failing store after a few attempts) ⇒ `Err`, `write` does not run.
    /// No generation (single process, old tests) ⇒ just run `write` and notify.
    ///
    /// Notifies peers **before** returning — that is, before the registry write request
    /// is answered —, bounded ([`PEER_DISCOVER_TIMEOUT`] + [`PEER_NOTIFY_TIMEOUT`]).
    /// Not split off into a background task: a background task races the caller that
    /// just got its answer, and deterministic simulation (`sim_cluster`) sees its session
    /// LIST/GET and network messages slip in at non-deterministic points. (A failed seal
    /// is retried in the background — only under store errors.)
    ///
    /// Peers come from a session LIST + GET **at notification time**
    /// (`Membership::discover_peers`), not from the existing table: the table only LISTs
    /// every TTL, so a node that just joined may not be in it yet and would wait a whole
    /// poll tick. Registry writes are rare (operator commands), so the cost is small. A
    /// failed notification is let go: the registry generation (odd while writing, changed
    /// after the seal) is the net.
    ///
    /// [`RegistryGen::begin`]: crate::registry_gen::RegistryGen::begin
    /// [`RegistryGen::seal`]: crate::registry_gen::RegistryGen::seal
    pub async fn write<T>(
        &self,
        write: impl std::future::Future<Output = T>,
        after: impl FnOnce(&T) -> After,
    ) -> Result<T, slatedb::object_store::Error> {
        let open = match &self.0.gen {
            Some((g, _)) => Some((g, g.begin().await?)),
            None => None,
        };
        let out = write.await;
        // The writer "dies" in the gap between the main write and the seal: no seal,
        // no notification — the odd generation stands until the coordinator removes it.
        if cairn_store::failpoint::fires("registry::between_write_and_seal") {
            return Ok(out);
        }
        let after = after(&out);
        if let Some((g, open)) = open {
            if let Err(e) = g.seal(open).await {
                tracing::debug!(error = %e, "cannot seal the registry generation — retrying in the background");
            }
        }
        match after {
            After::Notify => {
                self.poke_local();
                self.notify_peers().await;
            }
            After::Publish => self.notify_peers().await,
            After::Quiet => {}
        }
        Ok(out)
    }

    /// Notify every peer to re-read the registry. See [`Self::write`].
    async fn notify_peers(&self) {
        let (Some(session), Some(secret)) = (&self.0.session, &self.0.secret) else {
            return;
        };
        let members = session.membership();
        let peers = tokio::time::timeout(PEER_DISCOVER_TIMEOUT, members.discover_peers())
            .await
            .unwrap_or_else(|_| members.peer_addresses());
        let client = &self.0.client;
        futures::future::join_all(peers.into_iter().map(|addr| async move {
            let req = client
                .post(format!(
                    "{}/internal/registry/changed",
                    addr.trim_end_matches('/')
                ))
                .timeout(PEER_NOTIFY_TIMEOUT);
            if let Err(e) = secret.send(client, req).await {
                tracing::debug!(peer = %addr, error = %e, "cannot notify peer of the cluster registry change");
            }
        }))
        .await;
    }
}

/// Apply order of **one** in-RAM copy of a cluster-level registry: `tenants`,
/// `auth/keys`, `auth/oidc` + `auth/revoked`.
///
/// Several re-reads of the same object run in parallel — the refresh round
/// (`RefreshLoop`), an apply request (`/internal/registry/apply`), the node's own
/// write — and arrive in any order. Applying in **arrival** order lets an old read that
/// arrives late overwrite a newer, acknowledged copy: a registry write makes the
/// generation odd, a peer's refresh round runs in full right then, its GET returns the
/// copy from before the write and arrives after the apply request's read — the `204`
/// has gone out, and the node falls back to the old copy until the next round.
///
/// Rule: every read takes a **ticket** ([`Self::ticket`]) **before** sending its GET;
/// the node's write takes a ticket **after** its CAS wins. Apply only when the ticket is
/// greater than the applied one ([`Self::apply_if_newer`]). The object store has strong
/// read-after-write, so a read that took its ticket after a write finished sees that
/// write or a newer one; the applied copy always belongs to the largest ticket that has
/// arrived — never older than a read that started after the write the `204` promised.
/// A dropped read does not break the caller's "registry fresh at `t0`": the applied copy
/// comes from a read that started later than it.
#[derive(Debug, Default)]
pub struct ReadOrder {
    issued: std::sync::atomic::AtomicU64,
    applied: std::sync::Mutex<u64>,
}

impl ReadOrder {
    /// Ticket for a read about to be sent (or a write that just won).
    pub fn ticket(&self) -> u64 {
        self.issued
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            .saturating_add(1)
    }

    /// Run `apply` if `ticket` is newer than every applied ticket — under the order's lock,
    /// so two applies do not interleave. `false` = this copy is older than the applied one: dropped.
    pub fn apply_if_newer(&self, ticket: u64, apply: impl FnOnce()) -> bool {
        let mut last = self.applied.lock().unwrap_or_else(|e| e.into_inner());
        if ticket <= *last {
            return false;
        }
        apply();
        *last = ticket;
        true
    }
}

/// Result of one round asking every node to apply the key registry ([`RegistrySync::confirm_peers`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Acks {
    /// Nodes of live sessions that have not confirmed applying (no answer, an error,
    /// no address, a binary that does not know the notification — `404`), by node name.
    pub pending: Vec<String>,
    /// `false` = could not LIST `sessions/` within the limit: the set of live sessions is
    /// not known in full, so it never counts as every node having applied.
    pub listed: bool,
}

impl Acks {
    /// Every live session has confirmed applying — for certain.
    pub fn all(&self) -> bool {
        self.listed && self.pending.is_empty()
    }
}

impl RegistrySync {
    /// Ask **every** other live session to apply at once the part of the registry that
    /// authentication decisions rest on (`POST /internal/registry/apply`: re-read `auth/keys`
    /// and `tenants` before answering), each peer bounded by [`PEER_ACK_TIMEOUT`]. Called
    /// **after** the write has been sealed, or after the caller has re-read the registry
    /// (`GET /admin/keys/{id}/revocation`). No peers
    /// (one node, `--data-dir`) ⇒ done at once.
    ///
    /// The set of sessions comes from a `sessions/` LIST **right now**
    /// ([`cairn_store::Membership::live_peers_now`]), not from `view` or the existing
    /// table: a node that just joined may be missing there, and the answer "every node has
    /// applied" must be certain. Each listed session's record is read too: a sparse
    /// observer's table counts a killed incarnation live for up to `SPARSE_AFTER_TTLS` TTLs
    /// after the coordinator wrote `expired`, and asking it would answer `202` for nothing.
    ///
    /// The set is fixed by the first successful LIST; a peer that does not confirm is asked
    /// again within the request (paced, [`confirm_policy`]: two lease TTLs plus one ack
    /// timeout) until it confirms or its record ends on the bucket — a missed apply (an
    /// injected GET fault) or an incarnation killed less than a TTL earlier no longer turns
    /// the answer into `202`. Only a `2xx` or an ended record drops a peer. A LIST that
    /// fails (or runs past [`PEER_DISCOVER_TIMEOUT`]) is retried within the same bound; never
    /// listed ⇒ `listed = false`, and the sessions in the table are asked once so that
    /// `pending` is useful.
    pub async fn confirm_peers(&self) -> Acks {
        self.confirm_peers_at("/internal/registry/apply").await
    }

    /// Like [`Self::confirm_peers`], with the request sent to `path_and_query` of the internal
    /// port (e.g. `/internal/registry/apply?deployments` — re-read the deployment
    /// registry as well).
    pub async fn confirm_peers_at(&self, path_and_query: &str) -> Acks {
        let (Some(session), Some(secret)) = (&self.0.session, &self.0.secret) else {
            return Acks {
                pending: Vec::new(),
                listed: true,
            };
        };
        let members = session.membership();
        let client = &self.0.client;
        // The set of peers to confirm, fixed by the first successful LIST: a session that
        // starts after it serves registry keys only once it has read the registry after
        // the write — at boot it reads `auth/keys` and `tenants` before serving
        // (`node/start.rs`), and an incarnation opened in-process counts its copy fresh only
        // after re-reading (`registry_gen::SessionGate`) — so it needs no confirmation.
        // `None` = not listed yet.
        let left: std::sync::Mutex<Option<Vec<Peer>>> = std::sync::Mutex::new(None);
        let stuck = self.0.gen.as_ref().map(|(g, _)| g.stuck_after());
        let policy = confirm_policy(members.ttl(), stuck);
        let (left_ref, members_ref) = (&left, &members);
        let round = move || async move {
            let (left, members) = (left_ref, members_ref);
            let peers = match take(left) {
                Some(peers) => peers,
                None => match tokio::time::timeout(PEER_DISCOVER_TIMEOUT, members.live_peers_now())
                    .await
                {
                    Ok(Ok(peers)) => peers,
                    Ok(Err(e)) => {
                        return Err(Unconfirmed(format!("cannot LIST sessions: {e}")));
                    }
                    Err(_) => {
                        return Err(Unconfirmed("session LIST over its limit".into()));
                    }
                },
            };
            let failed = ask(client, secret, path_and_query, peers).await;
            // A peer that did not confirm but whose record has ended on the bucket serves
            // no registry key any more: its grant lapsed (⅔ TTL on its own clock) before an
            // observer could write `expired` (a full TTL of silence), and a node counts its
            // registry copy fresh only under a valid grant of an incarnation that re-read it
            // (`registry_gen::SessionGate`). A killed incarnation inside its TTL when the
            // LIST ran is the common case.
            let ended =
                futures::future::join_all(failed.iter().map(|(id, _)| members.ended_on_bucket(id)))
                    .await;
            let still: Vec<Peer> = failed
                .into_iter()
                .zip(ended)
                .filter(|(_, ended)| !ended)
                .map(|(peer, _)| peer)
                .collect();
            let n = still.len();
            *left.lock().expect("mutex") = Some(still);
            if n == 0 {
                Ok(())
            } else {
                Err(Unconfirmed(format!("{n} peers have not confirmed")))
            }
        };
        let _ = retry::run(&retry::ops::REGISTRY_CONFIRM, &policy, retry::Never, round).await;
        match take(&left) {
            Some(still) => Acks {
                pending: names(still),
                listed: true,
            },
            // Never listed within the bound: ask the sessions of the existing table once so
            // that `pending` is useful; never "every node applied".
            None => {
                tracing::debug!("no session LIST within the bound before asking for acks");
                let failed = ask(client, secret, path_and_query, members.peers()).await;
                Acks {
                    pending: names(failed),
                    listed: false,
                }
            }
        }
    }
}

/// A peer to confirm: its session and internal address.
type Peer = (cairn_store::SessionId, Option<String>);

/// First rest between two rounds asking the unconfirmed peers again.
const CONFIRM_FIRST_PAUSE: Duration = Duration::from_millis(250);

/// Longest rest between two such rounds.
const CONFIRM_MAX_PAUSE: Duration = Duration::from_secs(2);

/// How long a registry write request keeps asking the peers that have not confirmed: two
/// lease TTLs (a killed incarnation's record turns `expired` within about one TTL after
/// its last renewal — 5.4–11.2 s with a 6 s TTL in a soak, the longer ones for a killed
/// coordinator) plus one ack timeout — capped at half the registry generation's "stuck
/// odd" threshold `stuck`: a deployment `DELETE` confirms inside its own write, while the
/// generation is odd, and peers remove the token of a writer standing longer than that
/// threshold. Past it the answer is `202` with what is left.
fn confirm_policy(ttl: Duration, stuck: Option<Duration>) -> retry::Policy {
    let bound = ttl * 2 + PEER_ACK_TIMEOUT;
    retry::Policy::for_duration(
        CONFIRM_FIRST_PAUSE,
        CONFIRM_MAX_PAUSE,
        stuck.map_or(bound, |s| bound.min(s / 2)),
    )
}

/// What a round of [`RegistrySync::confirm_peers_at`] left unconfirmed, for
/// [`retry::run`]: always transient (asking again changes nothing on the bucket).
struct Unconfirmed(String);

impl std::fmt::Display for Unconfirmed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl retry::Retryable for Unconfirmed {
    fn verdict(&self) -> retry::Verdict {
        retry::Verdict::Transient
    }
    fn timed_out(op: &'static str, after: Duration) -> Self {
        Unconfirmed(format!("{op}: over {after:?}"))
    }
}

fn take(left: &std::sync::Mutex<Option<Vec<Peer>>>) -> Option<Vec<Peer>> {
    left.lock().expect("mutex").clone()
}

fn names(peers: Vec<Peer>) -> Vec<String> {
    let mut pending: Vec<String> = peers.into_iter().map(|(id, _)| id.node).collect();
    pending.sort();
    pending.dedup();
    pending
}

/// Ask every peer in `peers` (in parallel, each bounded by [`PEER_ACK_TIMEOUT`]) to apply
/// the registry at once; returns the peers that did not confirm (no address, an error, a
/// non-`2xx` answer).
async fn ask(
    client: &reqwest::Client,
    secret: &crate::auth::ClusterSecret,
    path_and_query: &str,
    peers: Vec<Peer>,
) -> Vec<Peer> {
    let acks = futures::future::join_all(peers.into_iter().map(|(id, addr)| async move {
        let Some(url) = addr.as_deref() else {
            return ((id, addr), false);
        };
        let req = client
            .post(format!("{}{path_and_query}", url.trim_end_matches('/')))
            .timeout(PEER_ACK_TIMEOUT);
        let ok = match secret.send(client, req).await {
            Ok(r) => r.status().is_success(),
            Err(e) => {
                tracing::debug!(peer = %url, error = %e, "peer has not confirmed applying the registry");
                false
            }
        };
        ((id, addr), ok)
    }))
    .await;
    acks.into_iter()
        .filter(|(_, ok)| !ok)
        .map(|(peer, _)| peer)
        .collect()
}

/// Registry write requests whose answer must wait for every node to apply: key
/// revocation; tenant state changes; every OIDC configuration write — removing a
/// client, changing the issuer, revoking SSO sessions.
fn confirms(method: &Method, path: &str) -> bool {
    (method == Method::DELETE && path.starts_with("/admin/keys/"))
        || (method == Method::PUT
            && path.starts_with("/admin/tenants/")
            && path.ends_with("/state"))
        || (method != Method::GET && method != Method::HEAD && path.starts_with("/admin/oidc"))
}

/// A request writing a cluster-level registry that `RefreshLoop` re-reads.
fn writes_registry(method: &Method, path: &str) -> bool {
    if method == Method::GET || method == Method::HEAD {
        return false;
    }
    path == "/deployments"
        || (path.starts_with("/deployments/") && !path.ends_with("/replay-check"))
        || (path.starts_with("/services/") && path.ends_with("/revision"))
        || (path.starts_with("/ranges/") && path.ends_with("/window"))
        || path.starts_with("/admin/keys")
        || path.starts_with("/admin/oidc")
        || path.starts_with("/admin/tenants/")
        || path == "/admin/capture"
        || path == "/rules"
        || path.starts_with("/rules/")
}

/// Layer of the ingress router: cluster-level registry write requests go through
/// [`RegistrySync::write`] — odd generation before the handler, even after. `2xx` ⇒
/// notify ([`After::Notify`]). `4xx`/`5xx` ⇒ only seal ([`After::Quiet`]): the request may
/// have written part of its change (a deployment without its catalog yet) before failing,
/// and the poll tick only runs a full round when the generation changes. Cannot open the
/// generation ⇒ `503`, the handler does not run.
pub async fn on_change(
    State(sync): State<Option<RegistrySync>>,
    req: Request,
    next: Next,
) -> Response {
    let writes = writes_registry(req.method(), req.uri().path());
    let confirm = confirms(req.method(), req.uri().path());
    let Some(s) = sync.filter(|_| writes) else {
        return next.run(req).await;
    };
    let after = |resp: &Response| {
        if resp.status().is_success() {
            After::Notify
        } else {
            After::Quiet
        }
    };
    match s.write(next.run(req), after).await {
        // Key revocation: answer `2xx` only once every live session has applied; not enough
        // acks within the limit ⇒ `202` + `pending`.
        Ok(resp) if confirm && resp.status().is_success() => {
            let acks = s.confirm_peers().await;
            if acks.all() {
                resp
            } else {
                tracing::warn!(pending = ?acks.pending, listed = acks.listed, "key revocation written but not yet confirmed by every node");
                (
                    axum::http::StatusCode::ACCEPTED,
                    axum::Json(serde_json::json!({
                        "pending": acks.pending,
                        "listed": acks.listed,
                        "note": "written; the nodes in `pending` have not confirmed applying it (`listed: false`: could not LIST sessions, the set of nodes is not known in full) — they see it through the registry generation within one tick, or close authenticated requests when they cannot confirm a fresh registry. Ask again: GET /admin/keys/{id}/revocation (key) or resend the same PUT …/state (tenant state)",
                    })),
                )
                    .into_response()
            }
        }
        Ok(resp) => resp,
        Err(e) => {
            tracing::warn!(error = %e, "cannot open the registry generation — refusing the registry write request");
            (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                axum::Json(serde_json::json!({
                    "error": format!("cannot open the registry generation (control/registry-gen): {e} — nothing written, retry later")
                })),
            )
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    /// **The confirm bound stays under half the "stuck odd" threshold** (a deployment
    /// `DELETE` confirms while the generation is odd; past the threshold peers remove the
    /// token of a writer that is still alive).
    #[test]
    fn confirm_bound_stays_under_half_the_stuck_threshold() {
        let stuck = crate::registry_gen::DEFAULT_STUCK;
        for ttl_s in [1, 6, 10, 30, 59, 60, 120, 600] {
            let p = super::confirm_policy(Duration::from_secs(ttl_s), Some(stuck));
            let total = p.total.expect("a time bound");
            assert!(
                total <= stuck / 2,
                "TTL {ttl_s}s: {total:?} > {:?}",
                stuck / 2
            );
        }
        let p = super::confirm_policy(Duration::from_secs(6), Some(stuck));
        assert_eq!(
            p.total,
            Some(Duration::from_secs(14)),
            "the TTL bound when below the cap"
        );
    }

    use super::*;

    #[test]
    fn only_registry_writes_count() {
        for (m, p) in [
            (Method::POST, "/deployments"),
            (Method::DELETE, "/deployments/dp_1"),
            (Method::PUT, "/services/Job/revision"),
            (Method::PUT, "/ranges/3/window"),
            (Method::POST, "/admin/keys"),
            (Method::DELETE, "/admin/keys/k1"),
            (Method::PUT, "/admin/tenants/t1"),
            (Method::POST, "/admin/capture"),
            (Method::POST, "/rules"),
            (Method::DELETE, "/rules"),
            (Method::DELETE, "/rules/acme.*"),
        ] {
            assert!(writes_registry(&m, p), "{m} {p}");
        }
        assert!(confirms(&Method::DELETE, "/admin/keys/k1"));
        for (m, p) in [
            (Method::PUT, "/admin/oidc"),
            (Method::PUT, "/admin/oidc/clients/env1"),
            (Method::DELETE, "/admin/oidc/clients/env1"),
            (Method::POST, "/admin/oidc/revoked"),
        ] {
            assert!(writes_registry(&m, p) && confirms(&m, p), "{m} {p}");
        }
        assert!(!confirms(&Method::GET, "/admin/oidc"));
        assert!(!confirms(&Method::POST, "/admin/keys"));
        assert!(!confirms(&Method::PUT, "/admin/tenants/t1"));
        assert!(confirms(&Method::PUT, "/admin/tenants/t1/state"));
        assert!(writes_registry(&Method::PUT, "/admin/tenants/t1/state"));
        for (m, p) in [
            (Method::GET, "/deployments"),
            (Method::POST, "/deployments/dp_1/replay-check"),
            (Method::POST, "/invoke/Job/run"),
            (Method::POST, "/ranges/3/split"),
            (Method::GET, "/admin/keys"),
        ] {
            assert!(!writes_registry(&m, p), "{m} {p}");
        }
    }
}
