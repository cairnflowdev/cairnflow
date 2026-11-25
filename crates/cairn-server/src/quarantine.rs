// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Poison-handler quarantine — inspired by TiDB's runaway query management: per-group
//! rules, an identity that breaks a rule goes into a **watch list with a TTL**, the next
//! time it is handled **before it runs**, with tiers of actions and a limit on entries.
//!
//! The **identity** is `(tenant, service, handler, deployment)` — the same key as the
//! per-handler summary ([`crate::stats::HandlerKey`]). The deployment is part of the
//! identity because the usual fix is deploying a new revision, and the new revision must
//! get a clean run: the rule no longer matches it by itself.
//!
//! # Object `quarantine/<tenant>`
//!
//! ```text
//! cairnflow-quarantine/1
//! <service> <handler> <deployment|-> action=<a> since_ms=<ms> expires_ms=<ms> reason=<rest of the line>
//! ```
//!
//! **Read leniently, absent means empty** (rule 2 of AGENTS.md, precedent
//! `capture-requests/`): unknown `k=v` keys before `reason=` are kept verbatim when
//! written back; unreadable lines (missing fields, unknown action) are kept verbatim and
//! skipped. A header other than `cairnflow-quarantine/1` belongs to a newer binary: it
//! reads as empty and is **not overwritten**. Written by CAS on the etag; expired entries
//! are cleaned at the next write; limit of [`MAX_ENTRIES`] entries per tenant.
//!
//! # Actions (default `observe`)
//!
//! - `observe` — a `warn` log on detection, metrics, shown in `/stats/handlers`
//!   and `GET /admin/quarantine`. Does not touch turns at all.
//! - `cooldown` — the identity's concurrency limit on each range drops to 1 (one
//!   tier of `flow::ResourceManager::poll_acquire`).
//! - `hold` — **new** invocations of the identity go to `paused` with code 576
//!   ([`cairn_engine::QUARANTINED`]) within the creating commit; in-flight invocations
//!   carry on with their retries. Never cancelled or `failed`: `resume` after deploying
//!   the fix.
//! - `cleared` — the operator just removed it by hand: applies nothing, only stops the
//!   detector from putting the identity back right away (the numbers in the window are
//!   still the old ones) until it expires (= one rule window + one summary window, see
//!   [`Quarantine::clear`]).
//!
//! # Who does what
//!
//! - **Counting** belongs to the per-handler summary: [`crate::stats::HandlerStats::health`],
//!   merged per node every second, with a key limit (LRU). Nothing is counted again here.
//! - **Detection** ([`Quarantine::detect`]) runs in the node's background task every second,
//!   on the numbers of the ranges this node leads. If two nodes detect the same thing, the
//!   later writer sees the entry already there (CAS + a re-check inside the closure) —
//!   idempotent per identity.
//! - The node's **book** ([`Quarantine`]) re-reads the bucket when a peer reports a change
//!   and on the deployment registry refresh tick (one LIST of `quarantine/`, a GET when the
//!   etag changes) and publishes an immutable snapshot with a version number (atomic). A
//!   node that has written notifies its peers
//!   ([`crate::registry_sync::RegistrySync::notify_peers`]): a write on one node takes
//!   effect on the other nodes at once, the poll tick is only a net.
//! - Each range's **engine thread** keeps a copy of the snapshot ([`RangeView`]): the turn
//!   path only reads one atomic and one `Rc` — no shared lock (AGENTS.md §6).

use {
    crate::stats::{HandlerHealth, HandlerKey, HandlerStats},
    cairn_store::cas::{put_resolving_or_overwrite, Cas},
    cairn_store::entropy::{HashMap, HashSet},
    serde::{Deserialize, Serialize},
    slatedb::object_store::{
        path::Path, Error as OsError, ObjectStore, ObjectStoreExt, PutMode, UpdateVersion,
    },
    std::{
        cell::{Cell, RefCell},
        hash::{Hash, Hasher},
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, OnceLock, RwLock,
        },
        time::Duration,
    },
};

pub const HEADER: &str = "cairnflow-quarantine/1";
/// Limit on entries per tenant. When full the detector adds no more.
pub const MAX_ENTRIES: usize = 1000;
/// An empty deployment (an unbound invocation) is written as this mark on the line.
const NO_DEPLOYMENT: &str = "-";

/// Identities just added to the list (every action).
pub static DETECTED: AtomicU64 = AtomicU64::new(0);
/// New invocations put in `paused` 576 at creation because of `hold`.
pub static HELD: AtomicU64 = AtomicU64::new(0);
/// Turns blocked at the `cooldown` tier of `poll_acquire`.
pub static COOLED: AtomicU64 = AtomicU64::new(0);
/// Number of entries in effect in the node's book, per action (gauge).
pub static ENTRIES_OBSERVE: AtomicU64 = AtomicU64::new(0);
pub static ENTRIES_COOLDOWN: AtomicU64 = AtomicU64::new(0);
pub static ENTRIES_HOLD: AtomicU64 = AtomicU64::new(0);

fn now_ms() -> u64 {
    cairn_store::entropy::now_ms()
}

// ─────────────────────────────────────────────────────── actions, rules

/// The action of an entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Action {
    Observe,
    Cooldown,
    Hold,
    /// Removed by hand — see the module's top.
    Cleared,
}

impl Action {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Observe => "observe",
            Self::Cooldown => "cooldown",
            Self::Hold => "hold",
            Self::Cleared => "cleared",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "observe" => Self::Observe,
            "cooldown" => Self::Cooldown,
            "hold" => Self::Hold,
            "cleared" => Self::Cleared,
            _ => return None,
        })
    }
}

/// Quarantine mode of the node or of a tenant: the detector off entirely, or the action
/// attached to new entries.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Mode {
    Off,
    #[default]
    Observe,
    Cooldown,
    Hold,
}

impl Mode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Observe => "observe",
            Self::Cooldown => "cooldown",
            Self::Hold => "hold",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "off" => Self::Off,
            "observe" => Self::Observe,
            "cooldown" => Self::Cooldown,
            "hold" => Self::Hold,
            _ => return None,
        })
    }

    /// The action of new entries. `None` = no detection.
    pub fn action(self) -> Option<Action> {
        match self {
            Self::Off => None,
            Self::Observe => Some(Action::Observe),
            Self::Cooldown => Some(Action::Cooldown),
            Self::Hold => Some(Action::Hold),
        }
    }
}

/// The rule: within `window`, ≥ `failed_attempts` failed attempts over ≥
/// `failed_invocations` distinct invocations **and 0 successes**. "0 successes" prevents
/// false alarms when only part of the input is bad.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Rule {
    pub failed_attempts: u64,
    pub failed_invocations: usize,
    pub window: Duration,
}

impl Default for Rule {
    fn default() -> Self {
        Self {
            failed_attempts: 20,
            failed_invocations: 5,
            window: Duration::from_secs(300),
        }
    }
}

impl Rule {
    /// `20/5/300` = attempts / invocations / seconds.
    pub fn parse(s: &str) -> Option<Self> {
        let mut f = s.split('/');
        let r = Self {
            failed_attempts: f.next()?.parse().ok()?,
            failed_invocations: f.next()?.parse().ok()?,
            window: Duration::from_secs(f.next()?.parse().ok()?),
        };
        (f.next().is_none() && r.failed_attempts > 0 && !r.window.is_zero()).then_some(r)
    }

    pub fn encode(&self) -> String {
        format!(
            "{}/{}/{}",
            self.failed_attempts,
            self.failed_invocations,
            self.window.as_secs()
        )
    }

    pub fn violated(&self, h: &HandlerHealth) -> bool {
        h.completed == 0
            && h.failed_attempts >= self.failed_attempts
            && h.failed_invocations >= self.failed_invocations
    }
}

/// The node's quarantine settings; a tenant overrides parts of them with the `quarantine=`
/// and `quarantine-rule=` flags of the `tenants` object.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QuarantineConfig {
    pub mode: Mode,
    pub rule: Rule,
    /// How long a new entry lives (it removes itself when it expires).
    pub ttl: Duration,
}

impl Default for QuarantineConfig {
    fn default() -> Self {
        Self {
            mode: Mode::Observe,
            rule: Rule::default(),
            ttl: Duration::from_secs(3600),
        }
    }
}

// ─────────────────────────────────────────────────────── one entry

/// One entry of the watch list.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Entry {
    pub service: String,
    pub handler: String,
    /// Empty = the invocation is not bound to a deployment.
    pub deployment: String,
    pub action: Action,
    pub since_ms: u64,
    pub expires_ms: u64,
    pub reason: String,
    /// Unknown `k=v` pairs — kept verbatim when written back.
    #[serde(skip)]
    extra: Vec<String>,
}

impl Entry {
    pub fn new(
        key: &HandlerKey,
        action: Action,
        since_ms: u64,
        ttl: Duration,
        reason: String,
    ) -> Self {
        Self {
            service: key.service.clone(),
            handler: key.handler.clone(),
            deployment: key.deployment.clone(),
            action,
            since_ms,
            expires_ms: since_ms.saturating_add(ttl.as_millis() as u64),
            reason,
            extra: Vec::new(),
        }
    }

    pub fn live(&self, now: u64) -> bool {
        self.expires_ms > now
    }

    pub fn same_identity(&self, key: &HandlerKey) -> bool {
        self.service == key.service
            && self.handler == key.handler
            && self.deployment == key.deployment
    }

    fn line(&self) -> String {
        let mut l = format!(
            "{} {} {} action={} since_ms={} expires_ms={}",
            self.service,
            self.handler,
            if self.deployment.is_empty() {
                NO_DEPLOYMENT
            } else {
                &self.deployment
            },
            self.action.as_str(),
            self.since_ms,
            self.expires_ms,
        );
        for x in &self.extra {
            l.push(' ');
            l.push_str(x);
        }
        l.push_str(" reason=");
        // One line per entry: the reason must not carry line breaks.
        l.push_str(&self.reason.replace(['\n', '\r'], " "));
        l
    }

    fn parse(line: &str) -> Option<Self> {
        let (head, reason) = match line.split_once(" reason=") {
            Some((h, r)) => (h, r.to_string()),
            None => (line, String::new()),
        };
        let mut f = head.split(' ').filter(|x| !x.is_empty());
        let service = f.next()?.to_string();
        let handler = f.next()?.to_string();
        let deployment = match f.next()? {
            NO_DEPLOYMENT => String::new(),
            d => d.to_string(),
        };
        let (mut action, mut since, mut expires) = (None, None, None);
        let mut extra = Vec::new();
        for kv in f {
            match kv.split_once('=') {
                Some(("action", v)) => action = Some(Action::parse(v)?),
                Some(("since_ms", v)) => since = v.parse().ok(),
                Some(("expires_ms", v)) => expires = v.parse().ok(),
                _ => extra.push(kv.to_string()),
            }
        }
        Some(Self {
            service,
            handler,
            deployment,
            action: action?,
            since_ms: since?,
            expires_ms: expires?,
            reason,
            extra,
        })
    }
}

/// The content of one `quarantine/<tenant>` object: readable entries and unreadable lines
/// (kept verbatim).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ListFile {
    entries: Vec<Entry>,
    unknown: Vec<String>,
}

/// `None` = unknown header (a newer binary): not read, not overwritten.
fn decode(raw: &[u8]) -> Option<ListFile> {
    let text = std::str::from_utf8(raw).ok()?;
    let mut lines = text.lines();
    if lines.next() != Some(HEADER) {
        return None;
    }
    let mut out = ListFile::default();
    for l in lines.filter(|l| !l.trim().is_empty()) {
        match Entry::parse(l) {
            Some(e) => out.entries.push(e),
            None => out.unknown.push(l.to_string()),
        }
    }
    Some(out)
}

fn encode(f: &ListFile) -> Vec<u8> {
    let mut out = format!("{HEADER}\n");
    for e in &f.entries {
        out.push_str(&e.line());
        out.push('\n');
    }
    for l in &f.unknown {
        out.push_str(l);
        out.push('\n');
    }
    out.into_bytes()
}

#[derive(Debug, thiserror::Error)]
pub enum QuarantineError {
    #[error("object quarantine/{0} has an unknown header (a newer binary?) — not overwriting")]
    NewerFormat(String),
    #[error("{0}")]
    Store(String),
}

// ─────────────────────────────────────────────────────── store

/// The path to `quarantine/` under the cluster prefix.
#[derive(Clone)]
pub struct QuarantineStore {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
}

impl QuarantineStore {
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

    fn path(&self, tenant: &str) -> Path {
        self.rel(&format!("quarantine/{tenant}"))
    }

    async fn load(
        &self,
        tenant: &str,
    ) -> Result<(Option<ListFile>, Option<UpdateVersion>), QuarantineError> {
        let path = self.path(tenant);
        match self.store.get(&path).await {
            Ok(got) => {
                let v = UpdateVersion {
                    e_tag: got.meta.e_tag.clone(),
                    version: got.meta.version.clone(),
                };
                let raw = got
                    .bytes()
                    .await
                    .map_err(|e| QuarantineError::Store(e.to_string()))?;
                Ok((decode(&raw), Some(v)))
            }
            Err(OsError::NotFound { .. }) => Ok((Some(ListFile::default()), None)),
            Err(e) => Err(QuarantineError::Store(format!("cannot read {path}: {e}"))),
        }
    }

    /// Unexpired entries of `tenant` (a fresh read). Unknown header ⇒ empty.
    pub async fn entries(&self, tenant: &str) -> Result<Vec<Entry>, QuarantineError> {
        let now = now_ms();
        Ok(self
            .load(tenant)
            .await?
            .0
            .map(|f| f.entries.into_iter().filter(|e| e.live(now)).collect())
            .unwrap_or_default())
    }

    /// Change `quarantine/<tenant>` by CAS (retried when someone else just wrote).
    /// `f` receives the **unexpired** entries and returns `None` = nothing changed, nothing
    /// written. Returns the written list with `f`'s result.
    pub async fn update<R>(
        &self,
        tenant: &str,
        mut f: impl FnMut(&mut Vec<Entry>, u64) -> Option<R>,
    ) -> Result<Option<(R, Vec<Entry>)>, QuarantineError> {
        let path = self.path(tenant);
        for _ in 0..8 {
            let (file, version) = self.load(tenant).await?;
            let Some(mut file) = file else {
                return Err(QuarantineError::NewerFormat(tenant.to_string()));
            };
            let now = now_ms();
            file.entries.retain(|e| e.live(now));
            let Some(r) = f(&mut file.entries, now) else {
                return Ok(None);
            };
            file.entries.truncate(MAX_ENTRIES);
            let body = encode(&file);
            let mode = match version {
                Some(v) => PutMode::Update(v),
                None => PutMode::Create,
            };
            // Ambiguity is resolved by reading back (rule 7): entries carry the change's
            // `since_ms` mark. Previously the retry ran `f` on a copy already carrying our
            // change and `f` returned "nothing changed". A store without CAS
            // (`--data-dir`): overwrite — like `tenants`.
            match put_resolving_or_overwrite(&self.store, &path, body.into(), mode).await {
                Ok(Cas::Written(_)) => return Ok(Some((r, file.entries))),
                Ok(Cas::Lost) => {}
                Err(e) => {
                    return Err(QuarantineError::Store(format!(
                        "cannot write object {path}: {e}"
                    )))
                }
            }
        }
        Err(QuarantineError::Store(format!(
            "{path} keeps changing underfoot — retry"
        )))
    }

    /// Every `quarantine/<tenant>` object: `(tenant, etag)`.
    async fn objects(&self) -> Result<Vec<(String, Option<String>)>, QuarantineError> {
        let dir = self.rel("quarantine");
        let listed = self
            .store
            .list_with_delimiter(Some(&dir))
            .await
            .map_err(|e| QuarantineError::Store(e.to_string()))?;
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

// ─────────────────────────────────────────────────────── snapshot

fn hash_of(parts: &[&str]) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    for p in parts {
        p.hash(&mut h);
    }
    h.finish()
}

/// Immutable snapshot of every unexpired entry the node knows. Lookups do not allocate:
/// an index by the hash of `(tenant, service, handler)`, then string comparison.
#[derive(Debug, Default)]
pub struct Snapshot {
    /// tenant → entries.
    by_tenant: HashMap<String, Vec<Entry>>,
    /// hash of `(tenant, service, handler)` → `(tenant, index in by_tenant)`.
    index: HashMap<u64, Vec<(String, usize)>>,
    /// hashes of `(tenant, service)` with at least one `cooldown` entry.
    cooled_services: HashSet<u64>,
}

impl Snapshot {
    fn build(by_tenant: HashMap<String, Vec<Entry>>) -> Self {
        let mut index: HashMap<u64, Vec<(String, usize)>> = HashMap::default();
        let mut cooled_services = HashSet::default();
        for (t, entries) in &by_tenant {
            for (i, e) in entries.iter().enumerate() {
                index
                    .entry(hash_of(&[t, &e.service, &e.handler]))
                    .or_default()
                    .push((t.clone(), i));
                if e.action == Action::Cooldown {
                    cooled_services.insert(hash_of(&[t, &e.service]));
                }
            }
        }
        Self {
            by_tenant,
            index,
            cooled_services,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.by_tenant.is_empty()
    }

    /// The identity's unexpired entry, if any.
    pub fn find(
        &self,
        tenant: &str,
        service: &str,
        handler: &str,
        deployment: &str,
        now: u64,
    ) -> Option<&Entry> {
        if self.by_tenant.is_empty() {
            return None;
        }
        self.index
            .get(&hash_of(&[tenant, service, handler]))?
            .iter()
            .filter(|(t, _)| t == tenant)
            .filter_map(|(t, i)| self.by_tenant.get(t)?.get(*i))
            .find(|e| {
                e.service == service
                    && e.handler == handler
                    && e.deployment == deployment
                    && e.live(now)
            })
    }

    /// Whether `(tenant, service)` has any `cooldown` entry — one hash lookup,
    /// so the turn path only reads entry 0 when really needed.
    pub fn cools(&self, tenant: &str, service: &str) -> bool {
        !self.cooled_services.is_empty()
            && self.cooled_services.contains(&hash_of(&[tenant, service]))
    }

    pub fn tenant(&self, tenant: &str) -> &[Entry] {
        self.by_tenant.get(tenant).map_or(&[], Vec::as_slice)
    }

    pub fn tenants(&self) -> impl Iterator<Item = (&String, &Vec<Entry>)> {
        self.by_tenant.iter()
    }
}

// ─────────────────────────────────────────────── the node's in-RAM book

#[derive(Default)]
struct Inner {
    store: OnceLock<QuarantineStore>,
    /// The node's registry change notifier: after a write notify peers, on a peer's notice re-read.
    sync: OnceLock<crate::registry_sync::RegistrySync>,
    /// The node's settings (once, at node construction). Not set ⇒ defaults.
    config: OnceLock<QuarantineConfig>,
    snap: RwLock<Arc<Snapshot>>,
    /// tenant → etag at read time (`None` = must GET again).
    etags: RwLock<HashMap<String, Option<String>>>,
    /// Bumped each time the snapshot changes — engine threads compare this number (one atomic)
    /// instead of locking `snap` on every turn.
    version: AtomicU64,
    /// One refresh at a time.
    refreshing: tokio::sync::Mutex<()>,
}

/// The watch list this node knows. `Clone` is cheap.
#[derive(Clone, Default)]
pub struct Quarantine(Arc<Inner>);

impl std::fmt::Debug for Quarantine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Quarantine")
            .field("version", &self.0.version.load(Ordering::Relaxed))
            .finish()
    }
}

impl Quarantine {
    /// Attach the main store (once, at node construction). Not attached ⇒ in RAM only.
    pub fn attach(&self, store: QuarantineStore) {
        let _ = self.0.store.set(store);
    }

    pub fn store(&self) -> Option<&QuarantineStore> {
        self.0.store.get()
    }

    /// Attach the node's registry change notifier (once, at node construction). Not attached ⇒
    /// nobody is notified, poll only.
    pub fn attach_sync(&self, sync: crate::registry_sync::RegistrySync) {
        let _ = self.0.sync.set(sync);
    }

    /// A `quarantine/` write on this node goes through the two phases of the registry
    /// generation ([`crate::registry_sync::RegistrySync::write`])
    /// and tells peers to re-read at once when `wrote(result)`. Cannot open the generation ⇒
    /// an error, nothing written (the next detection tick retries).
    async fn write_registry<T>(
        &self,
        write: impl std::future::Future<Output = Result<T, QuarantineError>>,
        wrote: impl FnOnce(&T) -> bool,
    ) -> Result<T, QuarantineError> {
        use crate::registry_sync::After;
        match self.0.sync.get() {
            Some(s) => s
                .write(write, |r| match r {
                    Ok(v) if wrote(v) => After::Publish,
                    _ => After::Quiet,
                })
                .await
                .map_err(|e| QuarantineError::Store(format!("cannot open registry-gen: {e}")))?,
            None => write.await,
        }
    }

    /// Wait for a peer's notice; with no notifier attached, wait forever.
    async fn notified(&self) {
        match self.0.sync.get() {
            Some(s) => s.quarantine_notified().await,
            None => std::future::pending().await,
        }
    }

    /// Set the node's settings (once, at node construction).
    pub fn configure(&self, cfg: QuarantineConfig) {
        let _ = self.0.config.set(cfg);
    }

    /// The node's settings.
    pub fn config(&self) -> QuarantineConfig {
        self.0.config.get().copied().unwrap_or_default()
    }

    /// The current snapshot.
    pub fn snapshot(&self) -> Arc<Snapshot> {
        self.0
            .snap
            .read()
            .map(|s| Arc::clone(&s))
            .unwrap_or_default()
    }

    pub fn version(&self) -> u64 {
        self.0.version.load(Ordering::Acquire)
    }

    fn publish(&self, by_tenant: HashMap<String, Vec<Entry>>) {
        let now = now_ms();
        let by_tenant: HashMap<String, Vec<Entry>> = by_tenant
            .into_iter()
            .map(|(t, v)| (t, v.into_iter().filter(|e| e.live(now)).collect::<Vec<_>>()))
            .filter(|(_, v)| !v.is_empty())
            .collect();
        let count = |a: Action| {
            by_tenant
                .values()
                .flat_map(|v| v.iter())
                .filter(|e| e.action == a)
                .count() as u64
        };
        ENTRIES_OBSERVE.store(count(Action::Observe), Ordering::Relaxed);
        ENTRIES_COOLDOWN.store(count(Action::Cooldown), Ordering::Relaxed);
        ENTRIES_HOLD.store(count(Action::Hold), Ordering::Relaxed);
        let snap = Arc::new(Snapshot::build(by_tenant));
        if let Ok(mut s) = self.0.snap.write() {
            *s = snap;
        }
        self.0.version.fetch_add(1, Ordering::AcqRel);
    }

    /// Replace `tenant`'s entries with the `entries` just written on this node — without
    /// waiting for the refresh tick. The old etag is no longer right: the next refresh GETs again.
    pub fn replace_local(&self, tenant: &str, entries: Vec<Entry>) {
        let snap = self.snapshot();
        let mut all: HashMap<String, Vec<Entry>> = snap
            .tenants()
            .map(|(t, v)| (t.clone(), v.clone()))
            .collect();
        all.insert(tenant.to_string(), entries);
        if let Ok(mut e) = self.0.etags.write() {
            e.insert(tenant.to_string(), None);
        }
        self.publish(all);
    }

    /// Drop expired entries from the snapshot, no I/O — for ticks where the registry generation
    /// did not change (the re-read round was skipped). Lookups by identity already filter
    /// expiry themselves; this keeps the counts and `cools` right (hashed by service, no expiry).
    pub fn expire_local(&self) {
        let snap = self.snapshot();
        let now = now_ms();
        if snap.by_tenant.values().flatten().all(|e| e.live(now)) {
            return;
        }
        self.publish(
            snap.tenants()
                .map(|(t, v)| (t.clone(), v.clone()))
                .collect(),
        );
    }

    /// Re-read from the bucket: one LIST, a GET only for objects whose etag changed. Expired
    /// entries fall out of the snapshot here (and on every lookup).
    pub async fn refresh(&self) -> Result<(), QuarantineError> {
        let Some(store) = self.store() else {
            return Ok(());
        };
        let _one = self.0.refreshing.lock().await;
        let objects = store.objects().await?;
        let snap = self.snapshot();
        let known = self.0.etags.read().map(|e| e.clone()).unwrap_or_default();
        let mut fresh = HashMap::default();
        let mut etags = HashMap::default();
        let mut changed = objects.len() != snap.by_tenant.len();
        for (tenant, etag) in objects {
            let cached = known
                .get(&tenant)
                .filter(|e| etag.is_some() && **e == etag)
                .and_then(|_| snap.by_tenant.get(&tenant).cloned().or(Some(Vec::new())));
            let entries = match cached {
                Some(c) => c,
                None => {
                    changed = true;
                    store.entries(&tenant).await?
                }
            };
            etags.insert(tenant.clone(), etag);
            fresh.insert(tenant, entries);
        }
        // Entries expired since the previous read are a change too.
        let now = now_ms();
        changed |= snap.by_tenant.values().flatten().any(|e| !e.live(now));
        if let Ok(mut e) = self.0.etags.write() {
            *e = etags;
        }
        if changed {
            self.publish(fresh);
        }
        Ok(())
    }

    /// One detection tick: identities breaking their tenant's rule that do not have an
    /// entry yet (including `cleared`) go into the list with the tenant's action.
    /// `policy(tenant)` returns the tenant's effective mode and rule.
    ///
    /// Runs in the node's background task — network I/O is allowed here, never on the
    /// turn path.
    pub async fn detect(
        &self,
        stats: &HandlerStats,
        policy: impl Fn(&str) -> (Mode, Rule, Duration),
    ) -> Result<usize, QuarantineError> {
        let snap = self.snapshot();
        let now = now_ms();
        // One summary read per distinct window (usually just one).
        let mut by_window: HashMap<Duration, Vec<(HandlerKey, HandlerHealth)>> = HashMap::default();
        let mut found: HashMap<String, Vec<Entry>> = HashMap::default();
        let mut policies: HashMap<String, (Mode, Rule, Duration)> = HashMap::default();
        let mut tenants: Vec<String> = Vec::new();
        let rows = stats.health(Rule::default().window);
        for (key, _) in &rows {
            if !tenants.contains(&key.tenant) {
                tenants.push(key.tenant.clone());
            }
        }
        by_window.insert(Rule::default().window, rows);
        for t in &tenants {
            policies.insert(t.clone(), policy(t));
        }
        for (mode, rule, _) in policies.values() {
            if mode.action().is_some() && !by_window.contains_key(&rule.window) {
                by_window.insert(rule.window, stats.health(rule.window));
            }
        }
        for (tenant, (mode, rule, ttl)) in &policies {
            let Some(action) = mode.action() else {
                continue;
            };
            let Some(rows) = by_window.get(&rule.window) else {
                continue;
            };
            for (key, h) in rows.iter().filter(|(k, _)| &k.tenant == tenant) {
                if !rule.violated(h)
                    || snap
                        .find(tenant, &key.service, &key.handler, &key.deployment, now)
                        .is_some()
                {
                    continue;
                }
                let reason = format!(
                    "{} failed attempts over {}{} invocations, 0 successes within {} s (rule {})",
                    h.failed_attempts,
                    h.failed_invocations,
                    if h.failed_invocations_capped { "+" } else { "" },
                    rule.window.as_secs(),
                    rule.encode(),
                );
                found
                    .entry(tenant.clone())
                    .or_default()
                    .push(Entry::new(key, action, now, *ttl, reason));
            }
        }
        if found.is_empty() {
            return Ok(0);
        }
        let write = async {
            let mut added = 0;
            for (tenant, new) in found {
                let fresh = match self.store() {
                    Some(store) => {
                        store
                            .update(&tenant, |entries, _| {
                                let mut n = Vec::new();
                                for e in &new {
                                    let dup = entries.iter().any(|x| {
                                        x.service == e.service
                                            && x.handler == e.handler
                                            && x.deployment == e.deployment
                                    });
                                    if !dup && entries.len() < MAX_ENTRIES {
                                        entries.push(e.clone());
                                        n.push(e.clone());
                                    }
                                }
                                (!n.is_empty()).then_some(n)
                            })
                            .await?
                    }
                    None => {
                        let mut entries: Vec<Entry> = snap.tenant(&tenant).to_vec();
                        entries.extend(new.iter().cloned());
                        Some((new.clone(), entries))
                    }
                };
                let Some((written, entries)) = fresh else {
                    continue;
                };
                for e in &written {
                    DETECTED.fetch_add(1, Ordering::Relaxed);
                    added += 1;
                    tracing::warn!(
                        tenant = %tenant,
                        service = %e.service,
                        handler = %e.handler,
                        deployment = %e.deployment,
                        action = e.action.as_str(),
                        expires_ms = e.expires_ms,
                        reason = %e.reason,
                        "poison handler: added to the quarantine list"
                    );
                }
                self.replace_local(&tenant, entries);
            }
            Ok(added)
        };
        self.write_registry(write, |added| *added > 0).await
    }

    /// Remove by hand every entry of `(service, handler)` of `tenant` (every deployment)
    /// and leave `cleared` behind — so the detector does not put the identity back with
    /// the very same old numbers. `cleared` expires after `window` **plus one summary window**
    /// (5 minutes): `HandlerStats::health` merges every wall-clock-aligned window *touching*
    /// `[now − window, now]`, so numbers from before the removal can still be read until then.
    /// Returns the number of entries removed (not counting old `cleared` ones).
    pub async fn clear(
        &self,
        tenant: &str,
        service: &str,
        handler: &str,
        window: Duration,
    ) -> Result<usize, QuarantineError> {
        let hold_off = window.as_millis() as u64 + crate::stats::WINDOW_MS;
        let edit = |entries: &mut Vec<Entry>, now: u64| {
            let mut removed = 0;
            for e in entries
                .iter_mut()
                .filter(|e| e.service == service && e.handler == handler)
            {
                if e.action != Action::Cleared {
                    removed += 1;
                }
                e.action = Action::Cleared;
                e.since_ms = now;
                e.expires_ms = now.saturating_add(hold_off);
                e.reason = "removed by hand through DELETE /admin/quarantine".into();
            }
            (removed > 0).then_some(removed)
        };
        let write = async {
            let res = match self.store() {
                Some(store) => store.update(tenant, edit).await?,
                None => {
                    let mut entries = self.snapshot().tenant(tenant).to_vec();
                    edit(&mut entries, now_ms()).map(|r| (r, entries))
                }
            };
            Ok(res.map(|(removed, entries)| {
                self.replace_local(tenant, entries);
                removed
            }))
        };
        Ok(self
            .write_registry(write, Option::is_some)
            .await?
            .unwrap_or(0))
    }

    /// The node's background loop: detection every `every`, re-reading the bucket when a peer
    /// reports a change and every `refresh_every`. The task stops when the handle is cancelled.
    ///
    /// The `refresh_every` tick asks the registry generation first (one
    /// `If-None-Match` GET): unchanged and the reconcile not due ⇒ skip the LIST,
    /// only drop expired entries from the snapshot. A peer's notice, or a failed previous
    /// re-read, ⇒ a full re-read.
    pub fn spawn(
        &self,
        stats: HandlerStats,
        policy: impl Fn(&str) -> (Mode, Rule, Duration) + Send + Sync + 'static,
        every: Duration,
        refresh_every: Duration,
    ) -> tokio::task::JoinHandle<()> {
        let me = self.clone();
        tokio::spawn(cairn_store::io_source::tagged("quarantine", async move {
            let mut last_refresh = tokio::time::Instant::now();
            let mut gen = me.0.sync.get().and_then(|s| s.watch());
            // The previous re-read failed ⇒ the next one is full, without asking the generation.
            let mut failed = false;
            loop {
                // `biased`: a peer's notice before the detection tick. A notice arriving
                // during detection is kept by `Notify` for the next round.
                let poked = tokio::select! {
                    biased;
                    _ = me.notified() => true,
                    _ = cairn_store::pace::beat(every) => false,
                };
                if poked || last_refresh.elapsed() >= refresh_every {
                    last_refresh = tokio::time::Instant::now();
                    let reading = match gen.as_mut() {
                        None => Some(None),
                        Some(g) if poked || failed => Some(Some(g.force().await)),
                        Some(g) => g.check().await.map(Some),
                    };
                    match reading {
                        None => me.expire_local(),
                        Some(reading) => match me.refresh().await {
                            Ok(()) => {
                                failed = false;
                                if let (Some(g), Some(r)) = (gen.as_mut(), reading) {
                                    g.applied(r);
                                }
                            }
                            Err(e) => {
                                failed = true;
                                tracing::debug!(error = %e, "cannot re-read quarantine/");
                            }
                        },
                    }
                    // Floor between two re-reads: a burst of notices merges.
                    if poked {
                        cairn_store::pace::beat(crate::registry_sync::MIN_REFRESH_GAP).await;
                    }
                }
                if let Err(e) = me.detect(&stats, &policy).await {
                    tracing::warn!(error = %e, "poison-handler detection tick failed, retrying next tick");
                }
            }
        }))
    }
}

// ─────────────────────────────────────────────── a range's copy

/// A copy of the snapshot on a range's engine thread (one per leader term).
/// Each lookup compares the node's version (one atomic) and only copies again when it
/// changed — the node's `RwLock` is touched exactly once per list change.
pub struct RangeView {
    node: Quarantine,
    seen: Cell<u64>,
    snap: RefCell<Arc<Snapshot>>,
}

impl RangeView {
    pub fn new(node: &Quarantine) -> Self {
        Self {
            node: node.clone(),
            seen: Cell::new(node.version()),
            snap: RefCell::new(node.snapshot()),
        }
    }

    /// The node's current snapshot, copied again if the node just changed.
    pub fn current(&self) -> Arc<Snapshot> {
        let v = self.node.version();
        if v != self.seen.get() {
            *self.snap.borrow_mut() = self.node.snapshot();
            self.seen.set(v);
        }
        Arc::clone(&self.snap.borrow())
    }

    /// The identity's `hold` entry — its new invocations must go to `paused`.
    pub fn held(
        &self,
        tenant: &str,
        service: &str,
        handler: &str,
        deployment: &str,
    ) -> Option<Entry> {
        let snap = self.current();
        // The usual path: an empty list — no clock read, no hashing.
        if snap.is_empty() {
            return None;
        }
        snap.find(tenant, service, handler, deployment, now_ms())
            .filter(|e| e.action == Action::Hold)
            .cloned()
    }

    /// Whether `(tenant, service)` has any `cooldown` entry.
    pub fn cools(&self, tenant: &str, service: &str) -> bool {
        let snap = self.current();
        !snap.is_empty() && snap.cools(tenant, service)
    }

    /// Identities in `cooldown`.
    pub fn cooled(&self, tenant: &str, service: &str, handler: &str, deployment: &str) -> bool {
        self.current()
            .find(tenant, service, handler, deployment, now_ms())
            .is_some_and(|e| e.action == Action::Cooldown)
    }
}

#[cfg(test)]
mod tests {
    use {super::*, slatedb::object_store::PutPayload};

    fn key(h: &str, d: &str) -> HandlerKey {
        HandlerKey {
            tenant: "acme".into(),
            service: "Svc".into(),
            handler: h.into(),
            deployment: d.into(),
        }
    }

    /// This header is the header of the policy table.
    #[test]
    fn header_is_in_policy() {
        assert_eq!(
            cairn_store::object_policy("quarantine/<tenant>").map(|p| p.header),
            Some(HEADER)
        );
    }

    #[test]
    fn round_trips_and_keeps_unknown_lines_verbatim() {
        let e = Entry::new(
            &key("h", "dp_1"),
            Action::Hold,
            10,
            Duration::from_secs(1),
            "20 failed\nattempts".into(),
        );
        let f = ListFile {
            entries: vec![
                e.clone(),
                Entry::new(
                    &key("g", ""),
                    Action::Observe,
                    1,
                    Duration::from_secs(9),
                    String::new(),
                ),
            ],
            unknown: vec!["an unreadable line".into()],
        };
        let raw = encode(&f);
        let back = decode(&raw).unwrap();
        assert_eq!(back.entries[0].reason, "20 failed attempts");
        assert_eq!(back.entries[1].deployment, "");
        assert_eq!(back.unknown, f.unknown);
        // An unknown key before `reason=` and an unknown action.
        let raw = format!(
            "{HEADER}\nS h dp action=hold since_ms=1 expires_ms=2 color=green reason=x y\nS h dp action=nuke since_ms=1 expires_ms=2\n"
        );
        let f = decode(raw.as_bytes()).unwrap();
        assert_eq!(f.entries.len(), 1);
        assert_eq!(f.entries[0].reason, "x y");
        assert_eq!(
            f.unknown.len(),
            1,
            "unknown action: kept verbatim, not applied"
        );
        assert!(String::from_utf8(encode(&f))
            .unwrap()
            .contains("color=green reason=x y"));
        assert!(decode(b"cairnflow-quarantine/2\n").is_none());
    }

    #[test]
    fn rule_parsing_and_matching() {
        let r = Rule::parse("20/5/300").unwrap();
        assert_eq!(r, Rule::default());
        assert!(Rule::parse("0/5/300").is_none() && Rule::parse("1/2").is_none());
        let bad = HandlerHealth {
            failed_attempts: 25,
            failed_invocations: 5,
            ..Default::default()
        };
        assert!(r.violated(&bad));
        assert!(
            !r.violated(&HandlerHealth {
                completed: 1,
                ..bad
            }),
            "0 successes"
        );
        assert!(!r.violated(&HandlerHealth {
            failed_invocations: 4,
            ..bad
        }));
    }

    #[tokio::test]
    async fn detection_writes_cas_once_and_manual_clear_does_not_reenter() {
        let store: Arc<dyn ObjectStore> = Arc::new(slatedb::object_store::memory::InMemory::new());
        let stats = HandlerStats::new(10);
        let sh = stats.shard();
        let k = key("bad", "dp_1");
        for i in 0..30u8 {
            sh.outcome(&k, &[i % 6; 24], crate::stats::RunOutcome::FailedAttempt);
        }
        sh.outcome(
            &key("ok", "dp_1"),
            &[9; 24],
            crate::stats::RunOutcome::Completed,
        );
        stats.drain();
        // Two nodes see it at once: only one entry.
        let a = Quarantine::default();
        let b = Quarantine::default();
        for q in [&a, &b] {
            q.attach(QuarantineStore::new(Arc::clone(&store), &Path::from("")));
        }
        let policy = |_: &str| (Mode::Hold, Rule::default(), Duration::from_secs(60));
        assert_eq!(a.detect(&stats, policy).await.unwrap(), 1);
        assert_eq!(
            b.detect(&stats, policy).await.unwrap(),
            0,
            "the later writer sees the entry already there"
        );
        let list = a.store().unwrap().entries("acme").await.unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(
            (list[0].handler.as_str(), list[0].action),
            ("bad", Action::Hold)
        );

        let view = RangeView::new(&b);
        b.refresh().await.unwrap();
        assert!(view.held("acme", "Svc", "bad", "dp_1").is_some());
        assert!(
            view.held("acme", "Svc", "bad", "dp_2").is_none(),
            "a new deployment does not match"
        );
        assert!(view.held("acme", "Svc", "ok", "dp_1").is_none());

        assert_eq!(
            b.clear("acme", "Svc", "bad", Duration::from_secs(300))
                .await
                .unwrap(),
            1
        );
        assert!(view.held("acme", "Svc", "bad", "dp_1").is_none());
        assert_eq!(
            b.detect(&stats, policy).await.unwrap(),
            0,
            "cleared blocks re-detection"
        );
        a.refresh().await.unwrap();
        assert_eq!(a.snapshot().tenant("acme")[0].action, Action::Cleared);
    }

    /// Removed by hand in the middle of a summary window: `health(window)` still merges the
    /// whole 5-minute window containing the removal, so `cleared` must live `window` **plus one
    /// summary window** — otherwise the numbers from before the removal put the identity back.
    #[tokio::test]
    async fn cleared_outlives_summary_window_containing_clear() {
        let store: Arc<dyn ObjectStore> = Arc::new(slatedb::object_store::memory::InMemory::new());
        let q = Quarantine::default();
        q.attach(QuarantineStore::new(Arc::clone(&store), &Path::from("")));
        q.store()
            .unwrap()
            .update("acme", |entries, now| {
                entries.push(Entry::new(
                    &key("bad", "dp_1"),
                    Action::Hold,
                    now,
                    Duration::from_secs(60),
                    String::new(),
                ));
                Some(())
            })
            .await
            .unwrap();
        assert_eq!(
            q.clear("acme", "Svc", "bad", Duration::from_secs(60))
                .await
                .unwrap(),
            1
        );
        let e = &q.store().unwrap().entries("acme").await.unwrap()[0];
        assert_eq!(e.action, Action::Cleared);
        assert_eq!(e.expires_ms - e.since_ms, 60_000 + crate::stats::WINDOW_MS);
    }

    /// Rule 7: the `quarantine/<tenant>` CAS was written but the answer says
    /// otherwise. Must see: `update` returns the result of **the write that landed** — before the
    /// fix, the retry ran `f` on a copy already carrying our change, `f` saw the duplicate entry
    /// and returned "nothing changed" (`None`).
    #[tokio::test]
    async fn ambiguous_write_is_resolved_by_reread() {
        use cairn_store::testkit::{Ambiguity, FaultyStore, Rule};
        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let faulty =
                FaultyStore::wrap(Arc::new(slatedb::object_store::memory::InMemory::new()));
            let s = QuarantineStore::new(faulty.clone(), &Path::from(""));
            faulty.add_rule(Rule::ambiguous_put("quarantine/acme", reply));
            let added = s
                .update("acme", |entries, now| {
                    if entries.iter().any(|e| e.handler == "bad") {
                        return None;
                    }
                    entries.push(Entry::new(
                        &key("bad", "dp_1"),
                        Action::Hold,
                        now,
                        Duration::from_secs(60),
                        String::new(),
                    ));
                    Some(())
                })
                .await
                .unwrap_or_else(|e| panic!("{reply:?}: written but reported an error: {e}"));
            assert!(
                added.is_some(),
                "{reply:?}: entry written but reported nothing changed"
            );
            assert_eq!(s.entries("acme").await.unwrap().len(), 1, "{reply:?}");
        }
    }

    #[tokio::test]
    async fn newer_header_not_overwritten() {
        let store: Arc<dyn ObjectStore> = Arc::new(slatedb::object_store::memory::InMemory::new());
        store
            .put(
                &Path::from("quarantine/acme"),
                PutPayload::from("cairnflow-quarantine/2\nx\n"),
            )
            .await
            .unwrap();
        let s = QuarantineStore::new(Arc::clone(&store), &Path::from(""));
        assert!(s.entries("acme").await.unwrap().is_empty());
        let r = s.update("acme", |_, _| Some(())).await;
        assert!(matches!(r, Err(QuarantineError::NewerFormat(_))));
    }

    #[test]
    fn expired_does_not_match() {
        let mut e = Entry::new(
            &key("h", "d"),
            Action::Cooldown,
            0,
            Duration::from_millis(1),
            String::new(),
        );
        e.expires_ms = 1;
        let snap = Snapshot::build([("acme".to_string(), vec![e])].into_iter().collect());
        assert!(snap.find("acme", "Svc", "h", "d", 5).is_none());
        assert!(snap.cools("acme", "Svc"));
    }
}
