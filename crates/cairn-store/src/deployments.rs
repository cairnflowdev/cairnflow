// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Deployments — **cluster configuration, not partition state**.
//!
//! In the single-partition phase they lived in the partition's `deployments` table, and that
//! was right while there was only one partition. With many partitions it is wrong in a silent
//! way: register on partition 3 and partition 7 does not see it, so partition 7 **does not
//! know whom to call**. Configuration belongs to no partition, so it lives next to
//! `cluster.conf`, and registering **needs no write permission** — a follower node can
//! register, and a deployment survives every leader change.
//!
//! # Immutable deployments + catalog
//!
//! Previously the registry was keyed by endpoint URL and overwritten blindly: two deployments
//! declaring the same service meant **the first element of an in-RAM `Vec` won**, and that
//! order differed between nodes (LIST order) — two nodes called two different deployments for
//! the same service. Now there are two kinds of object:
//!
//! ```text
//! deployments/<tenant>/<id>   one deployment: the discovered manifest, immutable,
//!                             written with `PutMode::Create` — never overwritten
//! catalog/<tenant>            the tenant's service → deployment pointers,
//!                             moved by CAS on the etag
//! ```
//!
//! Registering = creating a **new** deployment (even for the same endpoint), then moving the
//! pointer of every service it declares. An invocation is bound to a deployment id when it is
//! created, so moving a pointer does not touch running invocations.
//!
//! **One catalog per tenant, not one object per service.** One registration moves the pointer
//! of *every* service in the manifest; with one object per service, a registration that fails
//! halfway leaves the tenant with half its services on the new version and half on the old.
//! With one object, the move is a single CAS — atomic. Registrations are rare, so contention on
//! one object per tenant is not a significant cost.

use {
    crate::{
        error::{Result, StoreError},
        scope::TenantId,
    },
    slatedb::object_store::{
        path::Path, Error as OsError, ObjectStore, ObjectStoreExt, PutMode, UpdateVersion,
    },
    std::{collections::BTreeMap, sync::Arc},
};

/// Cluster-level deployment store. `Clone` freely — it is not a state write permission.
#[derive(Clone)]
pub struct DeploymentStore {
    store: Arc<dyn ObjectStore>,
    deployments: Path,
    catalogs: Path,
}

/// A tenant's service → deployment pointers.
///
/// `version` increases on every write — for logs, and so that readers can tell two versions
/// apart without comparing etags.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CatalogDoc {
    pub version: u64,
    /// `service → deployment id`. A `BTreeMap` so the encoding is deterministic.
    pub services: BTreeMap<String, String>,
}

pub(crate) const CATALOG_HEADER: &str = "cairnflow-catalog v1";

impl CatalogDoc {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = format!("{CATALOG_HEADER}\nversion={}\n", self.version);
        for (svc, id) in &self.services {
            out.push_str(&format!("service\t{svc}\t{id}\n"));
        }
        out.into_bytes()
    }

    pub fn decode(raw: &[u8]) -> Result<Self> {
        let bad = |reason: String| StoreError::BadCatalog { reason };
        let text = std::str::from_utf8(raw).map_err(|_| bad("not UTF-8".into()))?;
        let mut lines = text.lines();
        let header = lines.next().unwrap_or_default();
        if header != CATALOG_HEADER {
            if let Some(e) = crate::format::newer_header("catalog/<tenant>", CATALOG_HEADER, header)
            {
                return Err(e);
            }
            return Err(bad(format!("unknown header, expected {CATALOG_HEADER:?}")));
        }
        let version = lines
            .next()
            .and_then(|l| l.strip_prefix("version=")?.parse::<u64>().ok())
            .ok_or_else(|| bad("missing version".into()))?;
        let mut services = BTreeMap::new();
        for line in lines {
            let mut f = line.split('\t');
            match (f.next(), f.next(), f.next(), f.next()) {
                (Some("service"), Some(svc), Some(id), None) => {
                    services.insert(svc.to_string(), id.to_string());
                }
                _ => return Err(bad(format!("unknown line: {line:?}"))),
            }
        }
        Ok(Self { version, services })
    }
}

/// A freshly read catalog, with the object version for the next write's CAS.
pub struct LoadedCatalog {
    pub doc: CatalogDoc,
    version: Option<UpdateVersion>,
}

/// Error of a catalog edit function: a refusal with a reason (becomes 409 at ingress), as
/// opposed to an I/O error.
pub type CatalogEdit<'a> =
    dyn FnMut(&mut CatalogDoc) -> std::result::Result<(), String> + Send + 'a;

impl DeploymentStore {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: &Path) -> Self {
        let p = prefix.as_ref();
        let under = |name: &str| {
            Path::from(if p.is_empty() {
                name.to_string()
            } else {
                format!("{p}/{name}")
            })
        };
        Self {
            store,
            deployments: under("deployments"),
            catalogs: under("catalog"),
        }
    }

    /// Writes a **new** deployment. An existing id ⇒ [`StoreError::DeploymentExists`]:
    /// deployments are immutable, and an id is never reused for different content —
    /// invocations are bound to the id, not to the content.
    pub async fn create(&self, tenant: &TenantId, id: &str, manifest: &[u8]) -> Result<()> {
        check_id(id)?;
        let path = self.deployment_path(tenant, id);
        // An ambiguous outcome is resolved by reading back: the id is freshly allocated by the
        // registrant (`new_deployment_id`) and the manifest is carried at creation, so "the
        // object carries exactly our manifest" is our own write — not a 409 for a deployment
        // that was just created (before this, the catalog would never have been moved).
        match crate::cas::put_resolving(
            &self.store,
            &path,
            bytes::Bytes::copy_from_slice(manifest),
            PutMode::Create,
        )
        .await
        {
            Ok(crate::cas::Cas::Written(_)) => Ok(()),
            Ok(crate::cas::Cas::Lost) => Err(StoreError::DeploymentExists { id: id.to_string() }),
            Err(e) => Err(StoreError::LeaseIo(e.to_string())),
        }
    }

    /// Reads a deployment by id. `None` = absent (never existed, or removed).
    pub async fn get(&self, tenant: &TenantId, id: &str) -> Result<Option<bytes::Bytes>> {
        check_id(id)?;
        match self.store.get(&self.deployment_path(tenant, id)).await {
            Ok(g) => g
                .bytes()
                .await
                .map(Some)
                .map_err(|e| StoreError::LeaseIo(e.to_string())),
            Err(OsError::NotFound { .. }) => Ok(None),
            Err(e) => Err(StoreError::LeaseIo(e.to_string())),
        }
    }

    /// Removes a deployment. The caller must make sure itself that no invocation is still bound
    /// to it and no catalog pointer still points at it.
    pub async fn remove(&self, tenant: &TenantId, id: &str) -> Result<()> {
        check_id(id)?;
        match self.store.delete(&self.deployment_path(tenant, id)).await {
            Ok(()) | Err(OsError::NotFound { .. }) => Ok(()),
            Err(e) => Err(StoreError::LeaseIo(e.to_string())),
        }
    }

    /// Places the "retiring" marker of deployment `id`: `deployments/<tenant>/<id>.retiring`, a
    /// **new, leniently read** object — only its presence means anything, absent = nobody is
    /// retiring it (an old binary neither reads nor lists it: the name does not pass
    /// [`check_id`]). Overwrites (unconditionally): two people retiring the same id mean the
    /// same thing. An ambiguous outcome is resolved by reading back and comparing bytes —
    /// `note` carries node, timestamp and nonce, so it is distinctive enough.
    pub async fn mark_retiring(&self, tenant: &TenantId, id: &str, note: &[u8]) -> Result<()> {
        check_id(id)?;
        match crate::cas::put_resolving(
            &self.store,
            &self.retiring_path(tenant, id),
            bytes::Bytes::copy_from_slice(note),
            PutMode::Overwrite,
        )
        .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(StoreError::LeaseIo(e.to_string())),
        }
    }

    /// Whether the bucket has the "retiring" marker of `id` ([`Self::mark_retiring`]).
    pub async fn is_retiring(&self, tenant: &TenantId, id: &str) -> Result<bool> {
        check_id(id)?;
        match self.store.head(&self.retiring_path(tenant, id)).await {
            Ok(_) => Ok(true),
            Err(OsError::NotFound { .. }) => Ok(false),
            Err(e) => Err(StoreError::LeaseIo(e.to_string())),
        }
    }

    /// Removes the "retiring" marker — after the removal finished or gave up. The marker is not
    /// customer data; a leftover marker (the node died mid-removal) only blocks moving pointers
    /// to that exact id, and the next `DELETE` removes it.
    pub async fn clear_retiring(&self, tenant: &TenantId, id: &str) -> Result<()> {
        check_id(id)?;
        match self.store.delete(&self.retiring_path(tenant, id)).await {
            Ok(()) | Err(OsError::NotFound { .. }) => Ok(()),
            Err(e) => Err(StoreError::LeaseIo(e.to_string())),
        }
    }

    /// `(tenant, id)` of every deployment — names only, no content.
    ///
    /// Deployments are immutable, so the refresh loop only needs to GET the ids it has not seen:
    /// the cost of each round is one LIST, not one GET per deployment.
    ///
    /// LIST is avoided everywhere else in the system (object names can be derived), but the set
    /// of deployments cannot be derived from anywhere. In exchange it runs once every few
    /// seconds, not on the ack path.
    pub async fn list_ids(&self) -> Result<Vec<(TenantId, String)>> {
        let mut out = Vec::new();
        for (parts, _) in self.list_under(&self.deployments).await? {
            if let [tenant, id] = parts.as_slice() {
                if let (Ok(t), Ok(())) = (TenantId::new(tenant), check_id(id)) {
                    out.push((t, id.clone()));
                }
            }
        }
        Ok(out)
    }

    /// Tenants with a catalog, with the object's etag (if the store has one) — the refresh loop
    /// only GETs catalogs whose etag changed.
    pub async fn list_catalogs(&self) -> Result<Vec<(TenantId, Option<String>)>> {
        let mut out = Vec::new();
        for (parts, etag) in self.list_under(&self.catalogs).await? {
            if let [tenant] = parts.as_slice() {
                if let Ok(t) = TenantId::new(tenant) {
                    out.push((t, etag));
                }
            }
        }
        Ok(out)
    }

    /// Reads a tenant's catalog. No object yet ⇒ an empty catalog at version 0.
    pub async fn load_catalog(&self, tenant: &TenantId) -> Result<LoadedCatalog> {
        match self.store.get(&self.catalog_path(tenant)).await {
            Ok(got) => {
                let version = UpdateVersion {
                    e_tag: got.meta.e_tag.clone(),
                    version: got.meta.version.clone(),
                };
                let raw = got
                    .bytes()
                    .await
                    .map_err(|e| StoreError::LeaseIo(e.to_string()))?;
                Ok(LoadedCatalog {
                    doc: CatalogDoc::decode(&raw)?,
                    version: Some(version),
                })
            }
            Err(OsError::NotFound { .. }) => Ok(LoadedCatalog {
                doc: CatalogDoc::default(),
                version: None,
            }),
            Err(e) => Err(StoreError::LeaseIo(e.to_string())),
        }
    }

    /// Read-modify-write of the catalog with CAS. `edit` may run **several times** (once per
    /// failed CAS), so it must be a pure function of the catalog. `edit` returns `Err` ⇒ nothing
    /// is written, and the error goes up as [`StoreError::CatalogRejected`].
    ///
    /// Two nodes moving the same pointer at the same time: both read, both CAS, exactly one wins;
    /// the loser reads again and applies `edit` to the winner's version — so a conditional move
    /// ("only if it points at X") loses exactly as it should.
    pub async fn update_catalog(
        &self,
        tenant: &TenantId,
        edit: &mut CatalogEdit<'_>,
    ) -> Result<CatalogDoc> {
        const MAX_TRIES: usize = 8;
        let path = self.catalog_path(tenant);
        for _ in 0..MAX_TRIES {
            let loaded = self.load_catalog(tenant).await?;
            let mut doc = loaded.doc;
            edit(&mut doc).map_err(|reason| StoreError::CatalogRejected { reason })?;
            doc.version = doc.version.saturating_add(1);
            let mode = match loaded.version {
                Some(v) => PutMode::Update(v),
                None => PutMode::Create,
            };
            // `version` increases on every write ⇒ the content is distinctive enough to resolve
            // ambiguity by reading back: a conditional move that reached the store is not refused
            // by itself on the read-back. A store without CAS (`LocalFileSystem`) overwrites —
            // same reasoning as the rule book (`RuleStore::put_cas`).
            match crate::cas::put_resolving_or_overwrite(
                &self.store,
                &path,
                doc.encode().into(),
                mode,
            )
            .await
            {
                Ok(crate::cas::Cas::Written(_)) => return Ok(doc),
                Ok(crate::cas::Cas::Lost) => continue,
                Err(e) => return Err(StoreError::LeaseIo(e.to_string())),
            }
        }
        Err(StoreError::CatalogRejected {
            reason: format!(
                "CAS failed {MAX_TRIES} times in a row — is someone writing continuously?"
            ),
        })
    }

    fn deployment_path(&self, tenant: &TenantId, id: &str) -> Path {
        Path::from(format!("{}/{}/{id}", self.deployments, tenant.as_str()))
    }

    fn retiring_path(&self, tenant: &TenantId, id: &str) -> Path {
        Path::from(format!(
            "{}/{}/{id}{RETIRING_SUFFIX}",
            self.deployments,
            tenant.as_str()
        ))
    }

    fn catalog_path(&self, tenant: &TenantId) -> Path {
        Path::from(format!("{}/{}", self.catalogs, tenant.as_str()))
    }

    /// The path segments under `dir` of every object, with etags.
    async fn list_under(&self, dir: &Path) -> Result<Vec<(Vec<String>, Option<String>)>> {
        use futures::StreamExt;
        let mut stream = self.store.list(Some(dir));
        let mut out = Vec::new();
        let base: Vec<String> = dir.parts().map(|p| p.as_ref().to_string()).collect();
        while let Some(meta) = stream.next().await {
            let meta = meta.map_err(|e| StoreError::LeaseIo(e.to_string()))?;
            let parts: Vec<String> = meta
                .location
                .parts()
                .map(|p| p.as_ref().to_string())
                .collect();
            if parts.len() > base.len() && parts[..base.len()] == base[..] {
                out.push((parts[base.len()..].to_vec(), meta.e_tag.clone()));
            }
        }
        Ok(out)
    }
}

/// Name suffix of the "retiring" marker ([`DeploymentStore::mark_retiring`]).
pub const RETIRING_SUFFIX: &str = ".retiring";
/// Header of the "retiring" marker (`OBJECT_POLICIES`).
pub const RETIRING_HEADER: &str = "cairnflow-retiring v1";

/// A valid deployment id: `dp_` + digits/lowercase letters, ≤ 64 bytes — it is an object path
/// segment, and an id sent from outside (`DELETE /deployments/{id}`) must not climb out of the
/// directory.
pub fn check_id(id: &str) -> Result<()> {
    let ok = id.len() <= 64
        && id.strip_prefix("dp_").is_some_and(|rest| {
            !rest.is_empty()
                && rest
                    .bytes()
                    .all(|b| b.is_ascii_digit() || b.is_ascii_lowercase())
        });
    if ok {
        Ok(())
    } else {
        Err(StoreError::BadDeploymentId { id: id.to_string() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::memory::InMemory;

    fn store() -> DeploymentStore {
        DeploymentStore::new(Arc::new(InMemory::new()), &Path::from("prod"))
    }

    fn t(s: &str) -> TenantId {
        TenantId::new(s).unwrap()
    }

    #[tokio::test]
    async fn deployment_immutable_and_id_never_reused() {
        let d = store();
        d.create(&t("acme"), "dp_1", b"A").await.unwrap();
        assert!(matches!(
            d.create(&t("acme"), "dp_1", b"B").await,
            Err(StoreError::DeploymentExists { .. })
        ));
        assert_eq!(d.get(&t("acme"), "dp_1").await.unwrap().unwrap(), &b"A"[..]);
        // The same id in another tenant is a different object.
        d.create(&t("other"), "dp_1", b"C").await.unwrap();
        let mut ids = d.list_ids().await.unwrap();
        ids.sort();
        assert_eq!(
            ids,
            vec![(t("acme"), "dp_1".into()), (t("other"), "dp_1".into())]
        );
        d.remove(&t("acme"), "dp_1").await.unwrap();
        assert!(d.get(&t("acme"), "dp_1").await.unwrap().is_none());
        d.remove(&t("acme"), "dp_1").await.unwrap();
    }

    #[tokio::test]
    async fn strange_id_cannot_escape_directory() {
        let d = store();
        for evil in [
            "../../cluster.conf",
            "dp_../x",
            "dp_A",
            "dp_",
            "x",
            "dp_a/b",
        ] {
            assert!(
                matches!(
                    d.create(&t("acme"), evil, b"x").await,
                    Err(StoreError::BadDeploymentId { .. })
                ),
                "{evil:?} got through"
            );
        }
        assert!(TenantId::new("..").is_err());
        assert!(TenantId::new("a/b").is_err());
    }

    #[tokio::test]
    async fn catalog_changes_by_cas_and_encodes_deterministically() {
        let d = store();
        let acme = t("acme");
        assert_eq!(
            d.load_catalog(&acme).await.unwrap().doc,
            CatalogDoc::default()
        );
        let doc = d
            .update_catalog(&acme, &mut |c: &mut CatalogDoc| {
                c.services.insert("Cart".into(), "dp_1".into());
                c.services.insert("Auth".into(), "dp_1".into());
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(doc.version, 1);
        assert_eq!(CatalogDoc::decode(&doc.encode()).unwrap(), doc);
        let err = d
            .update_catalog(&acme, &mut |_: &mut CatalogDoc| Err("no".into()))
            .await;
        assert!(matches!(err, Err(StoreError::CatalogRejected { .. })));
        assert_eq!(d.load_catalog(&acme).await.unwrap().doc.version, 1);
        let cats = d.list_catalogs().await.unwrap();
        assert_eq!(cats.len(), 1);
        assert_eq!(cats[0].0, acme);
    }

    /// The `Create` of a deployment / the catalog CAS wrote, but the reply says otherwise. Must
    /// see: registering does **not** come out as `DeploymentExists` (a 409 for a deployment
    /// just created — the catalog would never be moved), and a conditional catalog move is not
    /// refused by itself on the read-back.
    #[tokio::test]
    async fn ambiguous_writes_are_resolved_by_reread() {
        use crate::testkit::{Ambiguity, FaultyStore, Rule};
        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
            let d = DeploymentStore::new(faulty.clone(), &Path::from("prod"));
            let acme = t("acme");
            faulty.add_rule(Rule::ambiguous_put("deployments/", reply));
            d.create(&acme, "dp_1", b"A")
                .await
                .unwrap_or_else(|e| panic!("{reply:?}: deployment created, yet an error: {e}"));
            assert_eq!(d.get(&acme, "dp_1").await.unwrap().unwrap(), &b"A"[..]);
            // An id already used for different content is still `DeploymentExists`.
            assert!(matches!(
                d.create(&acme, "dp_1", b"B").await,
                Err(StoreError::DeploymentExists { .. })
            ));

            for (from, to) in [(None, "dp_1"), (Some("dp_1"), "dp_2")] {
                faulty.add_rule(Rule::ambiguous_put("catalog/", reply));
                let doc = d
                    .update_catalog(&acme, &mut |c: &mut CatalogDoc| match c
                        .services
                        .get("Cart")
                        .map(String::as_str)
                    {
                        now if now == from => {
                            c.services.insert("Cart".into(), to.into());
                            Ok(())
                        }
                        other => Err(format!("points at {other:?}")),
                    })
                    .await
                    .unwrap_or_else(|e| panic!("{reply:?}: moving {from:?}→{to} was refused: {e}"));
                assert_eq!(doc.services["Cart"], to, "{reply:?}");
                assert_eq!(d.load_catalog(&acme).await.unwrap().doc, doc, "{reply:?}");
            }
        }
    }

    /// `revision_pointer_moves_by_cas`: two people move the same pointer **conditionally**
    /// ("only if it points at dp_1") at the same time ⇒ exactly one wins; the loser sees the
    /// winner's version and is refused, not overwritten.
    #[tokio::test]
    async fn revision_pointer_moves_by_cas() {
        let d = store();
        let acme = t("acme");
        d.update_catalog(&acme, &mut |c: &mut CatalogDoc| {
            c.services.insert("Cart".into(), "dp_1".into());
            Ok(())
        })
        .await
        .unwrap();
        let mv = |to: &'static str| {
            let d = d.clone();
            let acme = acme.clone();
            async move {
                d.update_catalog(&acme, &mut |c: &mut CatalogDoc| match c
                    .services
                    .get("Cart")
                    .map(String::as_str)
                {
                    Some("dp_1") => {
                        c.services.insert("Cart".into(), to.into());
                        Ok(())
                    }
                    other => Err(format!("points at {other:?}")),
                })
                .await
            }
        };
        let (a, b) = tokio::join!(mv("dp_2"), mv("dp_3"));
        assert!(a.is_ok() ^ b.is_ok(), "exactly one wins: {a:?} / {b:?}");
        let now = d.load_catalog(&acme).await.unwrap().doc;
        let winner = if a.is_ok() { "dp_2" } else { "dp_3" };
        assert_eq!(now.services["Cart"], winner);
    }
}
