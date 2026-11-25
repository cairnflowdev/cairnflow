// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Capture the next run — inspired by TiDB's `PLAN REPLAYER CAPTURE`: place a
//! "capture next time" request in advance, and the next matching run leaves a
//! bundle sufficient to reproduce it outside the cluster. The thing most
//! needed when a customer reports "handler X fails now and then".
//!
//! # Two kinds of object
//!
//! **Capture request** — `capture-requests/<tenant>`, text, written with CAS
//! on the etag. (Not `captures/<tenant>`: the bundles live under
//! `captures/<tenant>/`, and a filesystem-style store (`--data-dir`) cannot
//! hold a file and a directory with the same name.)
//!
//! ```text
//! cairnflow-captures/1
//! <cap-id> service=<s> handler=<h> deployment=<d|*> remaining=<n> expires_ms=<ms>
//! ```
//!
//! **Lenient read, absence means nothing** (as with `range-windows`): an
//! unknown `k=v` key on a line is kept verbatim on rewrite; an unreadable line
//! (missing fields) is kept verbatim and skipped. A header other than
//! `cairnflow-captures/1` belongs to a newer binary: it reads as empty, and is
//! **not overwritten** — better to capture nothing than delete someone else's
//! request. Expired lines or `remaining=0` are cleaned up on the next write.
//!
//! **Bundle** — `captures/<tenant>/<invocation-id-hex>`, written with
//! `PutMode::Create` (idempotent per id):
//!
//! ```text
//! cairnflow-capture/1\n
//! len u32 ‖ JSON {capture, capturedAtMs, status, deployment, contract}
//! ‖ the rest: one ArchiveRecord, exactly the archive.rs layout (journal + state)
//! ```
//!
//! Journal + state go through exactly the archive codec, so every tool that
//! reads archives can read that part; the deployment (endpoint, wire version,
//! manifest) and the handler's contract live in the JSON part — enough for
//! `cairn replay --bundle` to run [`crate::replay`] without a cluster.
//!
//! # Who captures, and when
//!
//! Every node re-reads the requests at the deployment-registry refresh
//! interval (one LIST of `capture-requests/`, a GET only when the etag
//! changed). When a run matching `(tenant, service, handler, deployment?)`
//! ends (completed/failed) or stops `paused`, its leader:
//!
//! 1. reads a journal snapshot under the engine lock and makes it durable
//!    (the rule "no outside effect before a durability ticket");
//! 2. **releases the engine**, then: a bundle for that id already exists ⇒
//!    stop; claims a slot by a CAS decrementing `remaining` (no slot left ⇒
//!    stop); PUTs the bundle.
//!
//! Claim first, write after: at most `count` bundles, even when several nodes
//! match at once. Dying between the two steps loses a slot — fewer bundles,
//! never more.

use cairn_store::entropy::HashMap;
use {
    crate::{
        discovery::{Contract, Deployment},
        stats::HandlerKey,
    },
    bytes::{Buf, Bytes},
    cairn_store::{
        archive::ArchiveRecord,
        cas::{put_resolving, put_resolving_or_overwrite, Cas},
        InvocationId,
    },
    serde::{Deserialize, Serialize},
    slatedb::object_store::{
        path::Path, Error as OsError, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload,
        UpdateVersion,
    },
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, OnceLock, RwLock,
        },
        time::Duration,
    },
};

const SPEC_HEADER: &str = "cairnflow-captures/1";
const BUNDLE_HEADER: &[u8] = b"cairnflow-capture/1\n";
/// Cap on a request's `count`.
pub const MAX_COUNT: u32 = 100;
/// Cap on a request's `ttl` — there are no unbounded requests.
pub const MAX_TTL: Duration = Duration::from_secs(7 * 86_400);
/// Cap on the number of live requests per tenant.
pub const MAX_SPECS: usize = 100;

fn now_ms() -> u64 {
    cairn_store::entropy::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// One capture request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CaptureSpec {
    pub id: String,
    pub tenant: String,
    pub service: String,
    pub handler: String,
    /// `None` = every deployment.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployment: Option<String>,
    pub remaining: u32,
    pub expires_ms: u64,
    /// Unknown `k=v` pairs — kept verbatim on rewrite.
    #[serde(skip)]
    extra: Vec<String>,
}

impl CaptureSpec {
    pub fn live(&self, now: u64) -> bool {
        self.remaining > 0 && self.expires_ms > now
    }

    pub fn matches(&self, key: &HandlerKey, now: u64) -> bool {
        self.live(now)
            && self.tenant == key.tenant
            && self.service == key.service
            && self.handler == key.handler
            && self
                .deployment
                .as_ref()
                .is_none_or(|d| *d == key.deployment)
    }

    fn line(&self) -> String {
        let mut l = format!(
            "{} service={} handler={} deployment={} remaining={} expires_ms={}",
            self.id,
            self.service,
            self.handler,
            self.deployment.as_deref().unwrap_or("*"),
            self.remaining,
            self.expires_ms
        );
        for x in &self.extra {
            l.push(' ');
            l.push_str(x);
        }
        l
    }

    fn parse(tenant: &str, line: &str) -> Option<Self> {
        let mut f = line.split(' ').filter(|x| !x.is_empty());
        let id = f.next()?.to_string();
        let (mut service, mut handler, mut deployment, mut remaining, mut expires) =
            (None, None, None, None, None);
        let mut extra = Vec::new();
        for kv in f {
            match kv.split_once('=') {
                Some(("service", v)) => service = Some(v.to_string()),
                Some(("handler", v)) => handler = Some(v.to_string()),
                Some(("deployment", v)) => deployment = Some((v != "*").then(|| v.to_string())),
                Some(("remaining", v)) => remaining = v.parse().ok(),
                Some(("expires_ms", v)) => expires = v.parse().ok(),
                _ => extra.push(kv.to_string()),
            }
        }
        Some(Self {
            id,
            tenant: tenant.to_string(),
            service: service?,
            handler: handler?,
            deployment: deployment?,
            remaining: remaining?,
            expires_ms: expires?,
            extra,
        })
    }
}

/// Contents of one `capture-requests/<tenant>` object: the readable requests,
/// and the unreadable lines (kept verbatim).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct SpecFile {
    specs: Vec<CaptureSpec>,
    unknown: Vec<String>,
}

/// `None` = unknown header (newer binary): do not read, do not overwrite.
fn decode(tenant: &str, raw: &[u8]) -> Option<SpecFile> {
    let text = std::str::from_utf8(raw).ok()?;
    let mut lines = text.lines();
    if lines.next() != Some(SPEC_HEADER) {
        return None;
    }
    let mut out = SpecFile::default();
    for l in lines.filter(|l| !l.trim().is_empty()) {
        match CaptureSpec::parse(tenant, l) {
            Some(s) => out.specs.push(s),
            None => out.unknown.push(l.to_string()),
        }
    }
    Some(out)
}

fn encode(f: &SpecFile) -> Vec<u8> {
    let mut out = format!("{SPEC_HEADER}\n");
    for s in &f.specs {
        out.push_str(&s.line());
        out.push('\n');
    }
    for l in &f.unknown {
        out.push_str(l);
        out.push('\n');
    }
    out.into_bytes()
}

/// Service/handler/deployment names must fit within one field of the line.
fn plain(s: &str) -> bool {
    !s.is_empty() && s.len() <= 256 && !s.chars().any(|c| c.is_whitespace() || c == '=' || c == '/')
}

#[derive(Debug, thiserror::Error)]
pub enum CaptureError {
    #[error("{0}")]
    Invalid(String),
    #[error("object capture-requests/{0} has an unknown header (newer binary?) — not overwriting")]
    NewerFormat(String),
    #[error("{0}")]
    Store(String),
}

// ─────────────────────────────────────────────────────── bundles

/// The descriptive part of a bundle.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BundleMeta {
    /// Id of the capture request that produced this bundle.
    pub capture: String,
    pub captured_at_ms: u64,
    /// `completed` | `failed` | `paused`.
    pub status: String,
    pub deployment: Deployment,
    pub contract: Contract,
}

/// One capture bundle: description + journal/state in the archive layout.
#[derive(Clone, Debug, PartialEq)]
pub struct Bundle {
    pub meta: BundleMeta,
    pub record: ArchiveRecord,
}

impl Bundle {
    pub fn encode(&self) -> Vec<u8> {
        let meta = serde_json::to_vec(&self.meta).unwrap_or_default();
        let mut out = Vec::with_capacity(BUNDLE_HEADER.len() + 4 + meta.len() + 256);
        out.extend_from_slice(BUNDLE_HEADER);
        out.extend_from_slice(&(meta.len() as u32).to_be_bytes());
        out.extend_from_slice(&meta);
        out.extend_from_slice(&self.record.encode());
        out
    }

    pub fn decode(raw: &[u8]) -> Result<Self, String> {
        let mut b = Bytes::copy_from_slice(raw);
        if !b.starts_with(BUNDLE_HEADER) {
            return Err("not a capture bundle (missing the cairnflow-capture/1 header)".into());
        }
        b.advance(BUNDLE_HEADER.len());
        if b.remaining() < 4 {
            return Err("truncated bundle".into());
        }
        let n = b.get_u32() as usize;
        if b.remaining() < n {
            return Err("truncated bundle".into());
        }
        let meta: BundleMeta = serde_json::from_slice(&b.split_to(n))
            .map_err(|e| format!("corrupt bundle metadata: {e}"))?;
        let record = ArchiveRecord::decode(&b).map_err(|e| e.to_string())?;
        Ok(Self { meta, record })
    }
}

// ─────────────────────────────────────────────────────── store

/// Path to `captures/` under the cluster prefix.
#[derive(Clone)]
pub struct CaptureStore {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
}

impl CaptureStore {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: &Path) -> Self {
        Self {
            store,
            prefix: prefix.clone(),
        }
    }

    fn rel(&self, rel: &str) -> Path {
        let p = self.prefix.as_ref();
        Path::from(if p.is_empty() {
            rel.to_string()
        } else {
            format!("{p}/{rel}")
        })
    }

    fn spec_path(&self, tenant: &str) -> Path {
        self.rel(&format!("capture-requests/{tenant}"))
    }

    fn bundle_path(&self, tenant: &str, id: &str) -> Path {
        self.rel(&format!("captures/{tenant}/{id}"))
    }

    async fn load(
        &self,
        tenant: &str,
    ) -> Result<(Option<SpecFile>, Option<UpdateVersion>), CaptureError> {
        let path = self.spec_path(tenant);
        match self.store.get(&path).await {
            Ok(got) => {
                let v = UpdateVersion {
                    e_tag: got.meta.e_tag.clone(),
                    version: got.meta.version.clone(),
                };
                let raw = got
                    .bytes()
                    .await
                    .map_err(|e| CaptureError::Store(e.to_string()))?;
                Ok((decode(tenant, &raw), Some(v)))
            }
            Err(OsError::NotFound { .. }) => Ok((Some(SpecFile::default()), None)),
            Err(e) => Err(CaptureError::Store(format!("cannot read {path}: {e}"))),
        }
    }

    /// Updates `capture-requests/<tenant>` with CAS (retrying when someone else
    /// just wrote). `f` returning `None` = no change, no write.
    ///
    /// `resolve` = resolve an ambiguous result by reading back (rule 7) — only
    /// when the content is unique enough: a new request carries a random id.
    /// **Not** for `claim`: two nodes decrementing the same slot from the same
    /// version produce identical content, so "reading back shows what we
    /// wrote" does not prove we claimed it — comparing content would let both
    /// capture (over the quota). Under ambiguity `claim` may decrement twice
    /// (capturing one fewer), never capture over the quota.
    async fn update<R>(
        &self,
        tenant: &str,
        resolve: bool,
        mut f: impl FnMut(&mut Vec<CaptureSpec>) -> Option<R>,
    ) -> Result<Option<R>, CaptureError> {
        let path = self.spec_path(tenant);
        for _ in 0..8 {
            let (file, version) = self.load(tenant).await?;
            let Some(mut file) = file else {
                return Err(CaptureError::NewerFormat(tenant.to_string()));
            };
            let Some(r) = f(&mut file.specs) else {
                return Ok(None);
            };
            let now = now_ms();
            file.specs.retain(|s| s.live(now));
            let mode = match version {
                Some(v) => PutMode::Update(v),
                None => PutMode::Create,
            };
            let body = bytes::Bytes::from(encode(&file));
            let res = if resolve {
                // Ambiguity resolved by reading back (rule 7). A store without
                // CAS (`--data-dir`): overwrite — as `tenants` does.
                put_resolving_or_overwrite(&self.store, &path, body, mode).await
            } else {
                plain_cas(&self.store, &path, body, mode).await
            };
            match res {
                Ok(Cas::Written(_)) => return Ok(Some(r)),
                Ok(Cas::Lost) => {}
                Err(e) => return Err(CaptureError::Store(format!("cannot write {path}: {e}"))),
            }
        }
        Err(CaptureError::Store(format!(
            "{path} kept changing underneath — retry"
        )))
    }

    /// Places a new capture request.
    pub async fn create(
        &self,
        tenant: &str,
        service: &str,
        handler: &str,
        deployment: Option<&str>,
        count: u32,
        ttl: Duration,
    ) -> Result<CaptureSpec, CaptureError> {
        if cairn_store::TenantId::new(tenant).is_err() {
            return Err(CaptureError::Invalid(format!(
                "tenant {tenant:?} is invalid"
            )));
        }
        for (what, v) in [("service", service), ("handler", handler)] {
            if !plain(v) {
                return Err(CaptureError::Invalid(format!("{what} {v:?} is invalid")));
            }
        }
        if deployment.is_some_and(|d| !plain(d) || d == "*") {
            return Err(CaptureError::Invalid("deployment is invalid".into()));
        }
        if count == 0 || count > MAX_COUNT {
            return Err(CaptureError::Invalid(format!(
                "count must be within 1..={MAX_COUNT}"
            )));
        }
        if ttl.is_zero() || ttl > MAX_TTL {
            return Err(CaptureError::Invalid(format!(
                "ttl must be within 1..={} seconds",
                MAX_TTL.as_secs()
            )));
        }
        let mut rnd = [0u8; 8];
        cairn_store::entropy::fill(&mut rnd);
        let spec = CaptureSpec {
            id: format!("cap_{:016x}", u64::from_be_bytes(rnd)),
            tenant: tenant.to_string(),
            service: service.to_string(),
            handler: handler.to_string(),
            deployment: deployment.map(str::to_string),
            remaining: count,
            expires_ms: now_ms() + ttl.as_millis() as u64,
            extra: Vec::new(),
        };
        let mut full = false;
        self.update(tenant, true, |specs| {
            let now = now_ms();
            if specs.iter().filter(|s| s.live(now)).count() >= MAX_SPECS {
                full = true;
                return None;
            }
            specs.push(spec.clone());
            Some(())
        })
        .await?;
        if full {
            return Err(CaptureError::Invalid(format!(
                "tenant already has {MAX_SPECS} live capture requests"
            )));
        }
        Ok(spec)
    }

    /// Claims a slot of request `spec`: decrements `remaining` with CAS.
    /// `false` = no slot left, expired, or the request is gone.
    pub async fn claim(&self, tenant: &str, spec: &str) -> Result<bool, CaptureError> {
        Ok(self
            .update(tenant, false, |specs| {
                let now = now_ms();
                let s = specs.iter_mut().find(|s| s.id == spec && s.live(now))?;
                s.remaining -= 1;
                Some(())
            })
            .await?
            .is_some())
    }

    /// Live requests of `tenant`.
    pub async fn specs(&self, tenant: &str) -> Result<Vec<CaptureSpec>, CaptureError> {
        let now = now_ms();
        Ok(self
            .load(tenant)
            .await?
            .0
            .map(|f| f.specs.into_iter().filter(|s| s.live(now)).collect())
            .unwrap_or_default())
    }

    /// Ids of `tenant`'s bundles.
    pub async fn bundles(&self, tenant: &str) -> Result<Vec<String>, CaptureError> {
        use futures::TryStreamExt;
        let dir = self.rel(&format!("captures/{tenant}/"));
        let listed: Vec<_> = self
            .store
            .list(Some(&dir))
            .try_collect()
            .await
            .map_err(|e| CaptureError::Store(e.to_string()))?;
        let mut ids: Vec<String> = listed
            .into_iter()
            .filter_map(|m| m.location.filename().map(str::to_string))
            .collect();
        ids.sort();
        Ok(ids)
    }

    /// Raw bytes of bundle `id`, `None` = not there.
    pub async fn bundle(&self, tenant: &str, id: &str) -> Result<Option<Bytes>, CaptureError> {
        let path = self.bundle_path(tenant, id);
        match self.store.get(&path).await {
            Ok(got) => got
                .bytes()
                .await
                .map(Some)
                .map_err(|e| CaptureError::Store(e.to_string())),
            Err(OsError::NotFound { .. }) => Ok(None),
            Err(e) => Err(CaptureError::Store(format!("cannot read {path}: {e}"))),
        }
    }

    /// Claims a slot then writes the bundle of invocation `id`. `false` = not
    /// written (a bundle for this id already exists, or no slot left).
    ///
    /// `artifacts` — `(sha, object path)` of the artifacts that still have rows
    /// — are copied (server-side copy) to
    /// `capture-artifacts/<tenant>/<id>/<sha>` **after** claiming the slot and
    /// **before** the bundle: the bundle can always be reproduced outside the
    /// cluster and never points at something the trash later deletes. A
    /// separate tree, so `bundles` does not mix in sha names.
    pub async fn write(
        &self,
        tenant: &str,
        spec: &str,
        id: &InvocationId,
        bundle: &Bundle,
        artifacts: &[([u8; 32], Path)],
    ) -> Result<bool, CaptureError> {
        let hex = cairn_store::id_hex(id);
        let path = self.bundle_path(tenant, &hex);
        match self.store.head(&path).await {
            Ok(_) => return Ok(false),
            Err(OsError::NotFound { .. }) => {}
            Err(e) => return Err(CaptureError::Store(format!("cannot read {path}: {e}"))),
        }
        if !self.claim(tenant, spec).await? {
            return Ok(false);
        }
        for (sha, from) in artifacts {
            let to = self.rel(&format!(
                "capture-artifacts/{tenant}/{hex}/{}",
                cairn_store::artifact::sha_hex(sha)
            ));
            self.store
                .copy(from, &to)
                .await
                .map_err(|e| CaptureError::Store(format!("cannot copy {from} → {to}: {e}")))?;
        }
        // The bundle carries the capture time and the request id: identical
        // content is our bundle (rule 7) — previously an object_store retry
        // reported "not written" and the node took the request as out of
        // slots.
        match put_resolving(&self.store, &path, bundle.encode().into(), PutMode::Create).await {
            Ok(Cas::Written(_)) => Ok(true),
            Ok(Cas::Lost) => Ok(false),
            Err(e) => Err(CaptureError::Store(format!("cannot write {path}: {e}"))),
        }
    }

    /// Every `capture-requests/<tenant>` object: `(tenant, etag)`.
    async fn spec_objects(&self) -> Result<Vec<(String, Option<String>)>, CaptureError> {
        let dir = self.rel("capture-requests");
        let listed = self
            .store
            .list_with_delimiter(Some(&dir))
            .await
            .map_err(|e| CaptureError::Store(e.to_string()))?;
        Ok(listed
            .objects
            .into_iter()
            .filter_map(|m| {
                m.location
                    .filename()
                    .map(|t| (t.to_string(), m.e_tag.clone()))
            })
            .collect())
    }
}

// ─────────────────────────────────────────────── the node's in-RAM registry

#[derive(Default)]
struct Registry {
    /// tenant → (etag when read, requests).
    by_tenant: HashMap<String, (Option<String>, Vec<CaptureSpec>)>,
}

#[derive(Default)]
struct Inner {
    store: OnceLock<CaptureStore>,
    reg: RwLock<Registry>,
    /// Whether any request is live — the hot path reads only this.
    any: AtomicBool,
    /// One refresh at a time.
    refreshing: tokio::sync::Mutex<()>,
}

/// The capture requests this node knows about. `Clone` is cheap.
#[derive(Clone, Default)]
pub struct Captures(Arc<Inner>);

impl Captures {
    /// Attaches the main store (once, when the node is built). Not attached ⇒ captures nothing.
    pub fn attach(&self, store: CaptureStore) {
        let _ = self.0.store.set(store);
    }

    pub fn store(&self) -> Option<&CaptureStore> {
        self.0.store.get()
    }

    fn recompute(&self, reg: &Registry) {
        let now = now_ms();
        let any = reg
            .by_tenant
            .values()
            .any(|(_, v)| v.iter().any(|s| s.live(now)));
        self.0.any.store(any, Ordering::Relaxed);
    }

    /// Re-reads from the bucket: one LIST, GET only for objects whose etag changed.
    pub async fn refresh(&self) -> Result<(), CaptureError> {
        let Some(store) = self.store() else {
            return Ok(());
        };
        let _one = self.0.refreshing.lock().await;
        let objects = store.spec_objects().await?;
        let mut fresh: HashMap<String, (Option<String>, Vec<CaptureSpec>)> = HashMap::default();
        for (tenant, etag) in objects {
            let cached = self
                .0
                .reg
                .read()
                .ok()
                .and_then(|r| r.by_tenant.get(&tenant).cloned())
                .filter(|(e, _)| etag.is_some() && *e == etag);
            let entry = match cached {
                Some(c) => c,
                None => (etag, store.specs(&tenant).await?),
            };
            fresh.insert(tenant, entry);
        }
        if let Ok(mut reg) = self.0.reg.write() {
            reg.by_tenant = fresh;
            self.recompute(&reg);
        }
        Ok(())
    }

    /// Records a request just placed on this node in the registry at once — without waiting for the refresh.
    pub fn insert(&self, spec: CaptureSpec) {
        if let Ok(mut reg) = self.0.reg.write() {
            let e = reg.by_tenant.entry(spec.tenant.clone()).or_default();
            // The old etag is no longer right: the next refresh must GET again.
            e.0 = None;
            e.1.push(spec);
            self.recompute(&reg);
        }
    }

    /// The first live request matching `key`, if any.
    pub fn matching(&self, key: &HandlerKey) -> Option<CaptureSpec> {
        if !self.0.any.load(Ordering::Relaxed) {
            return None;
        }
        let now = now_ms();
        self.0
            .reg
            .read()
            .ok()?
            .by_tenant
            .get(&key.tenant)?
            .1
            .iter()
            .find(|s| s.matches(key, now))
            .cloned()
    }

    /// One slot of `spec` was just used (on this node) — decrement it in the registry.
    pub fn used(&self, tenant: &str, spec: &str) {
        if let Ok(mut reg) = self.0.reg.write() {
            if let Some((etag, specs)) = reg.by_tenant.get_mut(tenant) {
                *etag = None;
                for s in specs.iter_mut().filter(|s| s.id == spec) {
                    s.remaining = s.remaining.saturating_sub(1);
                }
            }
            self.recompute(&reg);
        }
    }

    /// Like [`Self::used`], for a failed claim: the request ran out of slots
    /// elsewhere — drop it from the registry until the next refresh.
    pub fn exhausted(&self, tenant: &str, spec: &str) {
        if let Ok(mut reg) = self.0.reg.write() {
            if let Some((etag, specs)) = reg.by_tenant.get_mut(tenant) {
                *etag = None;
                specs.retain(|s| s.id != spec);
            }
            self.recompute(&reg);
        }
    }
}

/// CAS that does **not** resolve ambiguity: `Precondition`/`AlreadyExists` ⇒
/// `Lost`, other errors ⇒ error; a store without CAS ⇒ overwrite. For `claim` —
/// see [`CaptureStore::update`].
async fn plain_cas(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    body: bytes::Bytes,
    mode: PutMode,
) -> Result<Cas, OsError> {
    match store
        .put_opts(path, PutPayload::from(body.clone()), PutOptions::from(mode))
        .await
    {
        Ok(r) => Ok(Cas::Written(r.into())),
        Err(OsError::Precondition { .. } | OsError::AlreadyExists { .. }) => Ok(Cas::Lost),
        Err(OsError::NotImplemented { .. } | OsError::NotSupported { .. }) => store
            .put_opts(
                path,
                PutPayload::from(body),
                PutOptions::from(PutMode::Overwrite),
            )
            .await
            .map(|r| Cas::Written(r.into())),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn headers_are_in_policy() {
        // Every object on the bucket has a row in the policy table.
        assert_eq!(
            cairn_store::object_policy("capture-requests/<tenant>").map(|p| p.header),
            Some(super::SPEC_HEADER)
        );
        assert_eq!(
            cairn_store::object_policy("captures/<tenant>/<id>").map(|p| p.header.as_bytes()),
            Some(super::BUNDLE_HEADER.trim_ascii_end())
        );
    }

    use {super::*, slatedb::object_store::memory::InMemory};

    /// Rule 7: writing a capture request reached the store but the answer says
    /// otherwise. Must see: exactly **one** request (before the fix: the retry
    /// pushed a second copy into the list — `Retried` — or reported an error
    /// for a request that had already taken effect — `Error`).
    ///
    /// `claim` (decrementing `remaining`) does **not** resolve by comparing
    /// content: two nodes decrementing the same slot from the same record
    /// produce identical content, so "reading back shows what we wrote" does
    /// not prove we claimed it — under ambiguity it may decrement twice
    /// (capturing one fewer), never capture over the quota.
    #[tokio::test]
    async fn ambiguous_creates_are_resolved_by_reread() {
        use cairn_store::testkit::{Ambiguity, FaultyStore, Rule};
        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
            let s = CaptureStore::new(faulty.clone(), &Path::from(""));
            faulty.add_rule(Rule::ambiguous_put("capture-requests/acme", reply));
            let spec = s
                .create("acme", "Svc", "h", None, 2, Duration::from_secs(60))
                .await
                .unwrap_or_else(|e| panic!("{reply:?}: written but got error: {e}"));
            let specs = s.specs("acme").await.unwrap();
            assert_eq!(specs.len(), 1, "{reply:?}: {specs:?}");
            assert_eq!(specs[0].id, spec.id);
        }
    }

    fn key(d: &str) -> HandlerKey {
        HandlerKey {
            tenant: "acme".into(),
            service: "S".into(),
            handler: "h".into(),
            deployment: d.into(),
        }
    }

    #[test]
    fn unknown_lines_and_keys_kept_verbatim() {
        let raw = format!(
            "{SPEC_HEADER}\ncap_1 service=S handler=h deployment=* remaining=2 expires_ms=99 color=blue\njunk junk\n"
        );
        let f = decode("acme", raw.as_bytes()).unwrap();
        assert_eq!(f.specs.len(), 1);
        assert_eq!(f.specs[0].deployment, None);
        assert_eq!(f.unknown, vec!["junk junk".to_string()]);
        assert_eq!(String::from_utf8(encode(&f)).unwrap(), raw);
        assert_eq!(
            decode("acme", b"cairnflow-captures/2\nx"),
            None,
            "unknown header"
        );
    }

    #[test]
    fn matches_by_optional_deployment() {
        let mut s = CaptureSpec::parse(
            "acme",
            "c service=S handler=h deployment=dp_1 remaining=1 expires_ms=10",
        )
        .unwrap();
        assert!(s.matches(&key("dp_1"), 5));
        assert!(!s.matches(&key("dp_2"), 5));
        assert!(!s.matches(&key("dp_1"), 10), "expired");
        s.deployment = None;
        assert!(s.matches(&key("dp_2"), 5));
        s.remaining = 0;
        assert!(!s.matches(&key("dp_2"), 5));
    }

    #[tokio::test]
    async fn claiming_slots_never_exceeds_count() {
        let store = CaptureStore::new(Arc::new(InMemory::new()), &Path::from(""));
        let spec = store
            .create("acme", "S", "h", None, 2, Duration::from_secs(60))
            .await
            .unwrap();
        assert!(store.claim("acme", &spec.id).await.unwrap());
        assert!(store.claim("acme", &spec.id).await.unwrap());
        assert!(!store.claim("acme", &spec.id).await.unwrap());
        assert!(
            store.specs("acme").await.unwrap().is_empty(),
            "removed once used up"
        );
        assert!(store
            .create("acme", "S", "h", None, 0, Duration::from_secs(1))
            .await
            .is_err());
        assert!(store
            .create("acme", "a b", "h", None, 1, Duration::from_secs(1))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn in_memory_ledger_reloads_from_bucket() {
        let store = CaptureStore::new(Arc::new(InMemory::new()), &Path::from("p"));
        let caps = Captures::default();
        caps.attach(store.clone());
        caps.refresh().await.unwrap();
        assert!(caps.matching(&key("dp_1")).is_none());
        let spec = store
            .create("acme", "S", "h", None, 1, Duration::from_secs(60))
            .await
            .unwrap();
        caps.refresh().await.unwrap();
        assert_eq!(
            caps.matching(&key("dp_1")).map(|s| s.id),
            Some(spec.id.clone())
        );
        // Bundles live elsewhere and are not read as requests.
        store
            .store
            .put(
                &store.bundle_path("acme", "abc"),
                PutPayload::from_static(b"x"),
            )
            .await
            .unwrap();
        caps.refresh().await.unwrap();
        assert!(caps.matching(&key("dp_1")).is_some());
        assert_eq!(
            store.bundles("acme").await.unwrap(),
            vec!["abc".to_string()]
        );
        caps.used("acme", &spec.id);
        assert!(caps.matching(&key("dp_1")).is_none());
    }
}
