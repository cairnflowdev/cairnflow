// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Refreshing the deployment registry, quarantine and intents.

use super::*;

/// Default refresh interval of the cluster-level registries: **30 s** (it used
/// to be 5 s). Propagation goes through the writing node's notification
/// ([`crate::registry_sync`]); this interval only catches missed
/// notifications.
pub const DEFAULT_DEPLOY_REFRESH: Duration = Duration::from_secs(30);

/// Cap on the quarantine list refresh interval: **30 s**, the same as the
/// registry (it used to be 5 s). The quarantine list is written by the node
/// itself when it detects a poisonous handler, or removed by hand, not through
/// ingress's `on_change` layer — `Quarantine` notifies peers itself after every
/// write ([`crate::registry_sync::RegistrySync::notify_peers`]), so this
/// interval only catches missed notifications.
pub(super) const QUARANTINE_REFRESH_MAX: Duration = Duration::from_secs(30);

/// The loop refreshing the cluster-level things a node must follow.
///
/// The deployment registry is cluster-level, so every node must refresh it
/// periodically — when another node registers something, this node must also
/// know whom to call. This period is the propagation delay of a registration
/// to the other nodes **when the writing node's notification is missed** — the
/// notification ([`crate::registry_sync`]) kicks this loop at once.
///
/// Cost of a full round: three LISTs and three GETs. Deployments are immutable,
/// so only ids not yet seen are fetched; the catalog is fetched only when its
/// etag changed. A poll tick where the registry generation
/// (`control/registry-gen`, [`crate::registry_gen`]) did not change costs a
/// single `If-None-Match` GET; a full reconciliation runs every
/// `registry_reconcile` regardless of the generation.
pub(super) struct RefreshLoop {
    pub(super) deployments: DeploymentStore,
    pub(super) registry: Registry,
    pub(super) handles: Partitions,
    pub(super) every: Duration,
    /// The key store follows the same interval: a key created/deleted on
    /// another node takes effect here within one interval — one GET per
    /// interval per node.
    pub(super) keys: crate::auth::Keys,
    pub(super) settings: crate::tenants::TenantSettings,
    pub(super) captures: crate::capture::Captures,
    pub(super) windows: crate::windows::RangeWindows,
    pub(super) wals: NodeWals,
    pub(super) session: Session,
    pub(super) table: ClusterConfig,
    pub(super) etags: cairn_store::entropy::HashMap<cairn_store::TenantId, Option<String>>,
    /// A "registry changed" notification (from this node or a peer) kicks the loop at once.
    pub(super) sync: crate::registry_sync::RegistrySync,
}

impl RefreshLoop {
    pub(super) fn spawn(self) -> JoinHandle<()> {
        let Self {
            deployments,
            registry,
            handles,
            every,
            keys,
            settings,
            captures,
            windows,
            wals,
            session,
            table,
            etags,
            sync,
        } = self;
        let node = session.node_id().to_string();
        let body = async move {
            let mut etags = etags;
            // Registry generation: a poll tick where the generation did not
            // change skips the round — one GET instead of three LISTs and
            // three GETs.
            let mut gen = sync.watch();
            // First round, a notification, or an unfinished previous round ⇒ a
            // full round, without asking whether the generation changed (it is
            // still read **before** the round, to record it afterwards).
            let mut force = true;
            loop {
                cairn_store::failpoint!("node::refresh_loop::before_tick");
                // The freshness mark of the key registry: **before** the
                // generation read.
                let t0 = tokio::time::Instant::now();
                let reading = match gen.as_mut() {
                    None => Some(None),
                    Some(g) if force => Some(Some(g.force().await)),
                    Some(g) => g.check().await.map(Some),
                };
                if reading.is_none() {
                    // The generation at `t0` is still that of the last full round applied.
                    sync.confirm(t0);
                    // So is the logged-out session registry: every write to it
                    // goes through `RegistrySync::write`.
                    keys.oidc().confirm_sessions_unchanged(t0);
                }
                // Any part that fails to read ⇒ the next round is full; a failed
                // deployment registry ⇒ the next round also comes **early**.
                let mut failed = false;
                // Keys, tenants or deployments failed to re-read: not a full round for
                // the session gate (captures and range windows do not count, nor a
                // catalog pointer that cannot be resolved — a persistent condition of
                // those must not keep a new incarnation from ever taking a range).
                let mut core_failed = false;
                let mut registry_incomplete = false;
                if let Some(reading) = reading {
                    // Per-range windows changed on another node ⇒ apply them
                    // to our shared WAL at once.
                    match windows.refresh().await {
                        Ok(true) => {
                            if let Some(id) = session.current() {
                                if let Ok(wal) = wals.get(&id) {
                                    for p in table.partitions() {
                                        wal.set_range_window(p.0, windows.get(p.0));
                                    }
                                }
                            }
                        }
                        Ok(false) => {}
                        Err(e) => {
                            failed = true;
                            tracing::debug!(error = %e, "cannot re-read range-windows")
                        }
                    }
                    match keys.refresh().await {
                        // The key registry re-read after `t0` ⇒ fresh as of
                        // `t0`, regardless of the generation. A failed read of
                        // the session registry does not spoil the keys'
                        // freshness but forces the next round to be full.
                        Ok(()) => {
                            sync.confirm(t0);
                            failed |= keys.oidc().sessions_read_failed();
                        }
                        Err(e) => {
                            failed = true;
                            core_failed = true;
                            tracing::debug!(error = %e, "cannot re-read auth/keys");
                        }
                    }
                    if let Err(e) = settings.refresh().await {
                        failed = true;
                        core_failed = true;
                        tracing::debug!(error = %e, "cannot re-read tenants");
                    }
                    // Capture requests: one LIST of `capture-requests/`, a GET
                    // when the etag changed.
                    if let Err(e) = captures.refresh().await {
                        failed = true;
                        tracing::debug!(error = %e, "cannot re-read captures/");
                    }
                    let round = refresh_registry(&deployments, &registry, &mut etags).await;
                    if round.changed {
                        handles.deployments_changed().await;
                    }
                    registry_incomplete = round.incomplete;
                    failed |= round.incomplete;
                    core_failed |= round.read_failed;
                    if !core_failed {
                        // Keys, tenants and deployments were all re-read after `t0`:
                        // the only confirmation that reopens a new incarnation
                        // (`registry_gen::SessionGate`).
                        sync.confirm_full(t0);
                    }
                    if !failed {
                        if let (Some(g), Some(r)) = (gen.as_mut(), reading) {
                            g.applied(r);
                        }
                    }
                }
                // Close/open transitions get logged even when nobody calls.
                sync.fresh();
                // Floor between two rounds: a burst of registry writes merges into one round.
                cairn_store::pace::beat(crate::registry_sync::MIN_REFRESH_GAP.min(every)).await;
                // Unfinished round (store error) ⇒ next round early, without waiting a full poll tick.
                let next = if registry_incomplete {
                    REFRESH_RETRY.min(every)
                } else {
                    every
                };
                // `biased`: a registry-change notification before the grid tick.
                let poked = tokio::select! {
                    biased;
                    _ = sync.notified() => true,
                    _ = tokio::time::sleep(next.saturating_sub(crate::registry_sync::MIN_REFRESH_GAP)) => false,
                };
                force = poked || failed;
            }
        };
        // The failpoints `node::refresh_loop::*`, `tenants::refresh::*`,
        // `auth::keys::refresh::*` know the node.
        tokio::spawn(cairn_store::io_source::tagged("node.refresh", async move {
            cairn_store::failpoint::in_node(&node, body).await
        }))
    }
}

/// The **node's** rule-book poll loop: one GET per
/// [`crate::actor::RULES_POLL_INTERVAL`] for the whole node, pushed into the
/// shared copy of [`RuleStore`]; the ranges' engines apply the shared copy on
/// their background tick without I/O. Previously every range did its own GET
/// on becoming leader and on every tick — a cost multiplied by the number of
/// ranges.
///
/// A change notification ([`RuleStore::mark_changed`]: the writing node, or a
/// peer notifying through [`crate::registry_sync`]) kicks a re-read at once,
/// after the floor [`crate::registry_sync::MIN_REFRESH_GAP`] that merges a
/// burst of writes. A failed round ⇒ the next one after
/// [`crate::actor::RULES_RETRY`]. Every GET has a limit
/// ([`crate::actor::RULES_READ_BOUND`], `retry::ops::RULES_POLL`).
pub(super) fn spawn_rules_poller(rules: &RuleStore, node_id: &str, tasks: &mut Tasks) {
    let rules = rules.clone();
    let node = node_id.to_string();
    tasks.push(tokio::spawn(cairn_store::io_source::tagged(
        "rules",
        async move {
            cairn_store::failpoint::in_node(&node, async move {
                loop {
                    let ok = rules.poll_once(crate::actor::RULES_READ_BOUND).await;
                    cairn_store::pace::beat(crate::registry_sync::MIN_REFRESH_GAP).await;
                    let next = if ok {
                        crate::actor::RULES_POLL_INTERVAL
                    } else {
                        crate::actor::RULES_RETRY
                    };
                    // `biased`: a change notification before the grid tick.
                    tokio::select! {
                        biased;
                        () = rules.changed() => {}
                        () = tokio::time::sleep(next.saturating_sub(crate::registry_sync::MIN_REFRESH_GAP)) => {}
                    }
                }
            })
            .await
        },
    )));
}

/// Intent reconciliation (SM-15): the last safety net for an intent left on
/// the table while the source's leader is still alive. Same runner as the
/// tombstone-on-behalf loop (also a rollback path's job, also not behind the
/// D-d flag), its own interval (`reconcile_intents_every`): each round is a
/// single GET of `cluster.conf`, so it stays at 15 s while the
/// tombstone-on-behalf loop moves up to an hour.
pub(super) fn spawn_intent_reconciler(
    reconcile_intents_every: Option<Duration>,
    range_ops: &crate::split::RangeOps,
    cluster: &ClusterConfig,
    handles: &Partitions,
    node_id: &str,
    tasks: &mut Tasks,
) {
    if let Some(every) = reconcile_intents_every {
        let ops = range_ops.clone();
        let cluster = cluster.clone();
        let handles = handles.clone();
        let node = node_id.to_string();
        tasks.push(tokio::spawn(cairn_store::io_source::tagged(
            "split.intents",
            async move {
                cairn_store::failpoint::in_node(&node, async move {
                    let mut reconciler = crate::split::IntentReconciler::default();
                    loop {
                        cairn_store::pace::beat(every.max(Duration::from_millis(100))).await;
                        if !crate::gc::is_runner(&cluster, &handles).await {
                            continue;
                        }
                        if let Err(e) = reconciler.run_once(&ops).await {
                            tracing::warn!(error = %e, "intent reconciliation round failed, next tick retries");
                        }
                    }
                })
                .await
            },
        )));
    }
}

/// Result of one deployment-registry refresh round ([`refresh_registry`]).
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Refreshed {
    /// The registry really changed — tell the engines to reconcile.
    pub changed: bool,
    /// Some part could not be read (LIST, catalog or a deployment failed): the
    /// next round runs early ([`REFRESH_RETRY`]), without waiting a full poll
    /// tick.
    pub incomplete: bool,
    /// A **read** failed (a LIST, or a transient error reading a catalog or a deployment):
    /// the round did not see the bucket. Unlike `incomplete`, a catalog pointer to a
    /// deployment that is gone or unreadable does not set it — a retry cannot fix that,
    /// and it must not keep a new incarnation closed ([`crate::registry_gen::RegistryGen::confirm_full`]).
    pub read_failed: bool,
}

/// An unfinished registry refresh round is rerun after this long (or the poll
/// interval, if shorter) — a transient store error must not keep the node on
/// the old registry for a whole poll interval (30 s by default).
pub(crate) const REFRESH_RETRY: Duration = Duration::from_secs(1);

/// One round refreshing the deployment registry from the bucket.
///
/// Order: the catalog **first**, then the deployments. A registration writes
/// the deployment before the catalog; reading in the opposite order could see
/// a new pointer before the deployment it points at — so any pointer to an
/// unknown id GETs that id directly.
///
/// **A pointer is applied only once the deployment it points at has been
/// loaded** (found by the deterministic simulation, seed 29). Previously the
/// catalog was applied before the deployment GET: a failed GET left the
/// service pointing at an id not yet loaded, and ingress answered `404` "no
/// deployment declares this service" — an answer clients treat as permanent
/// — until the next poll tick. Now that service keeps its current revision
/// (absent if it has none, as before), the catalog's etag is not recorded (the
/// next round re-reads it), and the next round runs early.
pub(crate) async fn refresh_registry(
    store: &DeploymentStore,
    registry: &Registry,
    etags: &mut cairn_store::entropy::HashMap<cairn_store::TenantId, Option<String>>,
) -> Refreshed {
    let mut out = Refreshed::default();
    let Ok(catalogs) = store.list_catalogs().await else {
        out.incomplete = true;
        out.read_failed = true;
        return out;
    };
    let transient = |e: &cairn_store::StoreError| {
        cairn_store::retry::store_verdict(e) == cairn_store::retry::Verdict::Transient
    };
    let mut fresh = Vec::new();
    let mut wanted: Vec<(cairn_store::TenantId, String)> = Vec::new();
    for (tenant, etag) in catalogs {
        // No etag (the store does not return one) ⇒ always re-read.
        if etag.is_some() && etags.get(&tenant) == Some(&etag) {
            continue;
        }
        // Ticket **before** the GET: a read that returns late does not overwrite a newer copy.
        let ticket = registry.catalog_ticket();
        match store.load_catalog(&tenant).await {
            Ok(loaded) => {
                for id in loaded.doc.services.values() {
                    if !registry.contains(id) {
                        wanted.push((tenant.clone(), id.clone()));
                    }
                }
                fresh.push((
                    tenant,
                    etag,
                    ticket,
                    loaded.doc.version,
                    loaded.doc.services,
                ));
            }
            Err(e) => {
                out.incomplete = true;
                out.read_failed |= transient(&e);
                tracing::warn!(tenant = %tenant, error = %e, "reading the deployment catalog failed");
            }
        }
    }
    match store.list_ids().await {
        Ok(ids) => {
            for (tenant, id) in ids {
                if !registry.contains(&id) {
                    wanted.push((tenant, id));
                }
            }
        }
        Err(_) => {
            out.incomplete = true;
            out.read_failed = true;
        }
    }
    wanted.sort();
    wanted.dedup();
    for (tenant, id) in wanted {
        match store.get(&tenant, &id).await {
            Ok(Some(raw)) => match serde_json::from_slice::<crate::discovery::Deployment>(&raw) {
                Ok(d) => out.changed |= registry.insert(d),
                // Skip one object that fails to read instead of the whole
                // round: losing one deployment means losing one service,
                // skipping the whole round means losing them all.
                Err(e) => tracing::warn!(deployment = %id, error = %e, "deployment unreadable"),
            },
            Ok(None) => {}
            Err(e) => {
                out.incomplete = true;
                out.read_failed |= transient(&e);
                tracing::warn!(deployment = %id, error = %e, "reading deployment failed");
            }
        }
    }
    // Apply pointers **after** loading: a service whose new revision could not
    // be loaded keeps its current revision, and the catalog is re-read next
    // round.
    for (tenant, etag, ticket, version, services) in fresh {
        let mut apply = std::collections::BTreeMap::new();
        let mut complete = true;
        for (svc, id) in &services {
            if registry.contains(id) {
                apply.insert(svc.clone(), id.clone());
            } else {
                complete = false;
                if let Some(old) = registry.find_in(tenant.as_str(), svc) {
                    apply.insert(svc.clone(), old.id);
                }
            }
        }
        out.changed |= registry.set_revisions_read(tenant.as_str(), ticket, version, &apply);
        if complete {
            etags.insert(tenant, etag);
        } else {
            out.incomplete = true;
        }
    }
    out
}
