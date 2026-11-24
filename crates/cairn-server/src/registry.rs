// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The deployment registry.
//!
//! Shared between ingress (writes on registration) and the engine threads (read in the
//! background loop). This is the **in-RAM cache** of the deployment registry; the
//! durable copy lives in `cairn_store::DeploymentStore` on the bucket (cluster-level).
//! Losing the cache means reloading it in the refresh round — not registering again.
//!
//! # Two questions, two tables
//!
//! - **"Calling this service creates an invocation on which deployment?"** — the
//!   revision pointer `(tenant, service) → id`, a copy of the catalog on the bucket. Every
//!   node answers the same because they all read one object, independent of registration
//!   order or LIST order (previously the first element of a `Vec` won, and two nodes
//!   could call two different deployments for the same service).
//! - **"Where does this invocation run?"** — the id written in its entry 0 at creation
//!   ([`Catalog::get`]). The pointer is never asked again: moving the pointer to a new
//!   revision must not drag an in-flight invocation onto new code.

use cairn_store::entropy::HashMap;
use {
    crate::discovery::Deployment,
    std::sync::{Arc, RwLock},
};

/// Immutable snapshot of the registry — a turn holds it throughout, locking nothing.
#[derive(Clone, Debug, Default)]
pub struct Catalog {
    by_id: HashMap<String, Arc<Deployment>>,
    /// `(tenant, service) → id`.
    current: HashMap<(String, String), String>,
}

impl Catalog {
    /// Deployment by id — the path of an invocation already bound.
    pub fn get(&self, id: &str) -> Option<&Deployment> {
        self.by_id.get(id).map(|d| &**d)
    }

    /// Current revision of `(tenant, service)` — the path of an invocation about to be created.
    pub fn current(&self, tenant: &str, service: &str) -> Option<&Deployment> {
        let id = self
            .current
            .get(&(tenant.to_string(), service.to_string()))?;
        self.get(id)
    }

    /// Id of the current revision, even if its deployment has not been loaded on this node yet.
    pub fn current_id(&self, tenant: &str, service: &str) -> Option<&str> {
        self.current
            .get(&(tenant.to_string(), service.to_string()))
            .map(String::as_str)
    }

    /// Every known deployment, in registration order (then id) — deterministic.
    pub fn deployments(&self) -> Vec<&Deployment> {
        let mut all: Vec<&Deployment> = self.by_id.values().map(|d| &**d).collect();
        all.sort_by(|a, b| (a.created_at_ms, &a.id).cmp(&(b.created_at_ms, &b.id)));
        all
    }

    /// Every revision pointer `(tenant, service, id)`, sorted.
    pub fn revisions(&self) -> Vec<(&str, &str, &str)> {
        let mut out: Vec<(&str, &str, &str)> = self
            .current
            .iter()
            .map(|((t, s), id)| (t.as_str(), s.as_str(), id.as_str()))
            .collect();
        out.sort();
        out
    }

    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }
}

#[derive(Clone, Default)]
pub struct Registry {
    inner: Arc<RwLock<Arc<Catalog>>>,
    order: Arc<CatalogOrder>,
}

/// Order in which catalog pointers are applied, per tenant.
///
/// Two sources arrive in any order: the node's own **write** (a won CAS — carrying the
/// catalog's `version`) and a **read** by the refresh round or a notification (a ticket
/// taken **before** the GET, like `registry_sync::ReadOrder`). Rules:
///
/// - a write applies when its `version` is not lower than the applied copy's — two
///   CAS writes follow each other on the store, so the version is their real order;
/// - a read applies when its ticket is **newer** than the applied copy's ticket (the GET
///   started after that apply ⇒ the store's strong read-after-write shows that copy or
///   a newer one), or when its `version` is greater. The ticket branch is what follows a
///   **rebuilt** catalog (purge the tenant and create it again: the version goes back to
///   1) — comparing versions only, the node would refuse every new catalog until restart.
///
/// A write after the catalog was rebuilt (a version lower than the applied copy) is
/// dropped; the refresh round's next read (a newer ticket) applies it.
#[derive(Default)]
struct CatalogOrder {
    issued: std::sync::atomic::AtomicU64,
    /// `tenant → (version, ticket)` of the applied copy.
    applied: std::sync::Mutex<HashMap<String, (u64, u64)>>,
}

impl CatalogOrder {
    fn ticket(&self) -> u64 {
        self.issued
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            .saturating_add(1)
    }
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of everything — cheap (one `Arc`), so every engine command takes one.
    pub fn snapshot(&self) -> Arc<Catalog> {
        self.inner
            .read()
            .map(|c| Arc::clone(&c))
            .unwrap_or_default()
    }

    fn edit<R>(&self, f: impl FnOnce(&mut Catalog) -> R) -> Option<R> {
        let mut guard = self.inner.write().ok()?;
        let mut next = (**guard).clone();
        let r = f(&mut next);
        *guard = Arc::new(next);
        Some(r)
    }

    /// Add a deployment **and** move the pointer of every service it declares to it —
    /// exactly what a registration does. A deployment without an id (built by hand in
    /// tests) is given one.
    pub fn upsert(&self, mut deployment: Deployment) -> Deployment {
        if deployment.id.is_empty() {
            deployment.id = crate::discovery::new_deployment_id(now_ms());
        }
        let out = deployment.clone();
        self.edit(|c| {
            for s in &deployment.services {
                c.current.insert(
                    (deployment.tenant.clone(), s.name.clone()),
                    deployment.id.clone(),
                );
            }
            c.by_id.insert(deployment.id.clone(), Arc::new(deployment));
        });
        out
    }

    /// Add a deployment already on the bucket, moving **no** pointer. Returns
    /// `true` if it is new to this node.
    pub fn insert(&self, deployment: Deployment) -> bool {
        if self
            .inner
            .read()
            .is_ok_and(|c| c.by_id.contains_key(&deployment.id))
        {
            return false;
        }
        self.edit(|c| {
            c.by_id
                .insert(deployment.id.clone(), Arc::new(deployment))
                .is_none()
        })
        .unwrap_or(false)
    }

    pub fn contains(&self, id: &str) -> bool {
        self.inner.read().is_ok_and(|c| c.by_id.contains_key(id))
    }

    /// Replace all of a tenant's pointers with the catalog just read. Returns `true`
    /// if anything **really changed** — the refresh round runs every few seconds, and
    /// waking every partition each round would rerun `recover` forever.
    pub fn set_revisions(
        &self,
        tenant: &str,
        services: &std::collections::BTreeMap<String, String>,
    ) -> bool {
        let same = self.inner.read().is_ok_and(|c| {
            let mine = c.current.iter().filter(|((t, _), _)| t == tenant).count();
            mine == services.len()
                && services
                    .iter()
                    .all(|(svc, id)| c.current.get(&(tenant.to_string(), svc.clone())) == Some(id))
        });
        if same {
            return false;
        }
        self.edit(|c| {
            c.current.retain(|(t, _), _| t != tenant);
            for (svc, id) in services {
                c.current
                    .insert((tenant.to_string(), svc.clone()), id.clone());
            }
        });
        true
    }

    /// Ticket for a catalog **read** about to be sent — taken
    /// **before** the GET, like `registry_sync::ReadOrder`. See [`CatalogOrder`].
    pub fn catalog_ticket(&self) -> u64 {
        self.order.ticket()
    }

    /// Apply `tenant`'s pointers from a **write** by this node (a won CAS, catalog
    /// `version`) — dropped if a copy with a greater version has been applied
    /// ([`CatalogOrder`]). `true` if anything really changed.
    pub fn set_revisions_written(
        &self,
        tenant: &str,
        version: u64,
        services: &std::collections::BTreeMap<String, String>,
    ) -> bool {
        let mut applied = self.order.applied.lock().unwrap_or_else(|e| e.into_inner());
        if applied.get(tenant).is_some_and(|(v, _)| version < *v) {
            return false;
        }
        let ticket = self.order.ticket();
        applied.insert(tenant.to_string(), (version, ticket));
        self.set_revisions(tenant, services)
    }

    /// Apply `tenant`'s pointers from a catalog **read** that took ticket `ticket`
    /// before the GET — dropped if the applied copy is both newer than the ticket and of a
    /// greater version ([`CatalogOrder`]). `true` if anything really changed.
    pub fn set_revisions_read(
        &self,
        tenant: &str,
        ticket: u64,
        version: u64,
        services: &std::collections::BTreeMap<String, String>,
    ) -> bool {
        let mut applied = self.order.applied.lock().unwrap_or_else(|e| e.into_inner());
        let keep = match applied.get(tenant) {
            None => (version, ticket),
            Some(&(_, t)) if ticket > t => (version, ticket),
            Some(&(v, t)) if version > v => (version, t),
            Some(_) => return false,
        };
        applied.insert(tenant.to_string(), keep);
        self.set_revisions(tenant, services)
    }

    /// Drop a deployment from the cache (removed on the bucket).
    pub fn remove(&self, id: &str) -> bool {
        self.edit(|c| c.by_id.remove(id).is_some()).unwrap_or(false)
    }

    /// Current revision of `service` **in `tenant`**. There is no
    /// "default tenant" version of this function: a call site that forgets the tenant
    /// calls someone else's deployment.
    pub fn find_in(&self, tenant: &str, service: &str) -> Option<Deployment> {
        self.snapshot().current(tenant, service).cloned()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().map(|c| c.is_empty()).unwrap_or(true)
    }
}

fn now_ms() -> u64 {
    cairn_store::entropy::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::discovery::{ProtocolMode, ServiceManifest, ServiceType},
    };

    fn dep(endpoint: &str, services: &[&str]) -> Deployment {
        Deployment {
            id: String::new(),
            tenant: "default".into(),
            created_at_ms: 0,
            endpoint: endpoint.into(),
            wire_version: 2,
            protocol_mode: ProtocolMode::RequestResponse,
            request_identity: None,
            rr_compression: None,
            replay_cache: false,
            transport: None,
            services: services
                .iter()
                .map(|s| ServiceManifest {
                    name: (*s).into(),
                    ty: ServiceType::Service,
                    handlers: vec![],
                    contract: Default::default(),
                })
                .collect(),
        }
    }

    #[test]
    fn new_registration_moves_pointer_but_keeps_old_version() {
        let r = Registry::new();
        let a = r.upsert(dep("http://a", &["Cart"]));
        let b = r.upsert(dep("http://b", &["Cart"]));
        let snap = r.snapshot();
        assert_eq!(snap.current("default", "Cart").unwrap().id, b.id);
        // The old revision can still be called by id — invocations bound to it keep running.
        assert_eq!(snap.get(&a.id).unwrap().endpoint, "http://a");
        // The same endpoint registered again is a new deployment.
        let a2 = r.upsert(dep("http://a", &["Cart"]));
        assert_ne!(a2.id, a.id);
    }

    /// **Catalog pointers are applied in order, and follow a
    /// rebuilt catalog.**
    ///
    /// 1. Two writes of the node arrive in reverse order: v2 (`dp_2`) applied first, v1
    ///    (`dp_1`) later ⇒ the registry must keep `dp_2` (it used to fall back to `dp_1`).
    /// 2. A read that took its ticket **before** the v3 write returns the old v2 later ⇒ keep `dp_3`.
    /// 3. The catalog is deleted and rebuilt (tenant purge): a read that took its ticket
    ///    **after** every apply returns v1 (`dp_new`) ⇒ applied, even though the version is lower.
    #[test]
    fn catalog_applies_in_order_and_follows_recreation() {
        let r = Registry::new();
        let m = |id: &str| {
            let mut m = std::collections::BTreeMap::new();
            m.insert("Cart".to_string(), id.to_string());
            m
        };
        let at = |r: &Registry| {
            r.snapshot()
                .current_id("default", "Cart")
                .map(str::to_string)
        };
        r.set_revisions_written("default", 2, &m("dp_2"));
        r.set_revisions_written("default", 1, &m("dp_1"));
        assert_eq!(
            at(&r).as_deref(),
            Some("dp_2"),
            "late v1 write overwrote v2"
        );
        let early = r.catalog_ticket();
        r.set_revisions_written("default", 3, &m("dp_3"));
        r.set_revisions_read("default", early, 2, &m("dp_2"));
        assert_eq!(
            at(&r).as_deref(),
            Some("dp_3"),
            "old read overwrote a newer write"
        );
        let fresh = r.catalog_ticket();
        r.set_revisions_read("default", fresh, 1, &m("dp_new"));
        assert_eq!(
            at(&r).as_deref(),
            Some("dp_new"),
            "rebuilt catalog (version back to 1) was not applied"
        );
    }

    #[test]
    fn set_revisions_reports_change_only_on_real_change() {
        let r = Registry::new();
        let mut m = std::collections::BTreeMap::new();
        m.insert("Cart".to_string(), "dp_1".to_string());
        assert!(r.set_revisions("default", &m));
        assert!(!r.set_revisions("default", &m));
        m.insert("Cart".to_string(), "dp_2".to_string());
        assert!(r.set_revisions("default", &m));
        assert_eq!(r.snapshot().current_id("default", "Cart"), Some("dp_2"));
        assert!(r.set_revisions("default", &Default::default()));
        assert_eq!(r.snapshot().current_id("default", "Cart"), None);
    }
}
