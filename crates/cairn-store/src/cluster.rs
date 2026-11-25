// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Partitions: how many there are, and what belongs to which.
//!
//! Both things here **cannot be fixed once there is data**, so they are enforced up front
//! instead of being left as conventions:
//!
//! 1. **The range table lives in the bucket, and every node follows the same table.** The
//!    table changes only by CAS ([`ClusterConfig::update`]), one version at a time; nodes
//!    reload it in the background. Changing the number of partitions through a *startup
//!    parameter* is still refused: the same object key must land on the same range, otherwise
//!    the old state stays behind in the old range and nobody looks for it again — data loss,
//!    and **no error reported**.
//!
//! 2. **An invocation's id carries its partition *key*** (the first 8 bytes), and
//!    [`PartitionTable`] — versioned, stored in `cluster.conf` — says which key belongs to
//!    which range. `/invocations/{id}` can be routed without asking anyone, and ranges can
//!    change by changing the table, **without** changing any id (range identity: D22).
//!
//! The id format is a contract with users — they store ids in their own databases. The old
//! ids (a 2-byte partition *number*) welded the partition count into that contract; the new
//! ids only promise "the first 8 bytes are a position in the key space".

use {
    crate::{
        error::{Result, StoreError},
        ids::{InvocationId, ID_LEN},
        partition_table::{
            partition_key_for_callee, partition_key_for_target, PartitionKey, PartitionTable,
            RangeEntry, RangeId,
        },
        scope::{Scope, TenantId},
    },
    slatedb::object_store::{
        path::Path, Error as OsError, ObjectStore, ObjectStoreExt, PutMode, UpdateVersion,
    },
    std::{sync::Arc, time::Duration},
    tokio::sync::{watch, Notify},
};

/// Cap on the number of concurrent ranges — `PartitionTable` counts with a `u16`.
pub const MAX_PARTITIONS: u16 = u16::MAX;

/// v3: the range table has `next_range_id`, `gen`, `intent` (D22); changed by CAS.
pub(crate) const HEADER: &str = "cairnflow-cluster/3";

/// Cluster configuration: the range table partition key → range, **changeable at runtime**.
///
/// One shared handle: every `clone` sees **one** current table (one `watch`), so transport,
/// ingress, routing and the balancer see a new table at the same time without anyone needing
/// to be told separately. A read is snapshotting an `Arc` — cheap, and a lookup never sees
/// half an old table and half a new one.
///
/// The in-RAM table only **moves forward**: [`Self::apply`] skips a table that is not newer,
/// so a slow read cannot pull back a table we have already seen.
#[derive(Clone)]
pub struct ClusterConfig {
    shared: Arc<Shared>,
}

struct Shared {
    table: watch::Sender<Arc<PartitionTable>>,
    /// `cluster.conf` on the bucket. `None` ⇒ the table lives only in RAM (one partition,
    /// tests) — nothing to reload or CAS onto.
    remote: Option<Remote>,
    /// "The table may be stale — reload now." The [`ClusterConfig::follow`] loop listens to it.
    reload: Notify,
}

struct Remote {
    store: Arc<dyn ObjectStore>,
    path: Path,
    /// The cluster's prefix — for table history ([`crate::cluster_history`]).
    prefix: Path,
}

/// Outcome of [`ClusterConfig::update`].
#[derive(Debug)]
pub enum TableUpdate {
    /// We won: the new table is on the bucket.
    Applied(Arc<PartitionTable>),
    /// Someone changed the table before us; this is the winner's table, read back (and applied
    /// to the in-RAM table).
    Conflict(Arc<PartitionTable>),
}

impl PartialEq for ClusterConfig {
    fn eq(&self, other: &Self) -> bool {
        *self.table() == *other.table()
    }
}

impl Eq for ClusterConfig {}

impl std::fmt::Debug for ClusterConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterConfig")
            .field("table", &self.table())
            .field(
                "remote",
                &self.shared.remote.as_ref().map(|r| r.path.to_string()),
            )
            .finish()
    }
}

impl ClusterConfig {
    fn new(table: PartitionTable, remote: Option<Remote>) -> Self {
        Self {
            shared: Arc::new(Shared {
                table: watch::Sender::new(Arc::new(table)),
                remote,
                reload: Notify::new(),
            }),
        }
    }

    /// A given table, in RAM only — for tests.
    pub fn from_table(table: PartitionTable) -> Self {
        Self::new(table, None)
    }

    /// Reads the configuration from the bucket, creating it if absent.
    ///
    /// `PutMode::Create` settles the race: when several nodes start for the first time
    /// together, one node wins and the others read back and **use the winning value**, even
    /// when their command-line parameters say otherwise.
    ///
    /// But it yields silently only when the cluster has *just* been created. If the bucket
    /// already has a table that **never changed** (version 1) and its number of ranges differs
    /// from `desired`, that is an operator configuration error and this function **refuses to
    /// start**. Once the table has changed (split/merge, version > 1) `desired` means nothing
    /// any more — it is only the number of ranges at cluster creation — and the bucket's table
    /// wins.
    ///
    /// **A table is only created on an empty bucket** (SM-21). A table that is absent while
    /// the prefix already has `ranges/`, `partition-*` or `owners/` means the object was deleted
    /// from outside: a new table would hand out again ids of Dbs still alive and leave holes
    /// where ranges had been split — [`StoreError::MissingClusterConfig`], nothing is created.
    /// A failed LIST ⇒ `Err` (ambiguous, so nothing is created).
    pub async fn open(store: &Arc<dyn ObjectStore>, prefix: &Path, desired: u16) -> Result<Self> {
        if desired == 0 {
            return Err(StoreError::BadClusterConfig {
                reason: "the number of partitions must be ≥ 1".into(),
            });
        }
        let path = config_path(prefix);
        let remote = || Remote {
            store: Arc::clone(store),
            path: path.clone(),
            prefix: prefix.clone(),
        };

        if let Some((found, _)) = read(store, &path).await? {
            if found.version() == 1 && found.partition_count() != desired {
                return Err(StoreError::BadClusterConfig {
                    reason: format!(
                        "the bucket already has a cluster of {} partitions, this node is configured with {} partitions.\n\
                         The number of partitions CANNOT change once there is data: the same object key would \
                         hash to a different partition, the old state would stay behind in the old partition and nobody \
                         would look for it again — data loss, with no error to notice.\n\
                         Change this node's parameter to {} to match the cluster.",
                        found.partition_count(),
                        desired,
                        found.partition_count()
                    ),
                });
            }
            return Ok(Self::new(found, Some(remote())));
        }

        // Positive proof that the bucket has never had a cluster. `cluster-version` does not
        // count: `migrate_cluster` runs before this step and may have just written it.
        if crate::format::has_cluster_data(store, prefix).await? {
            // A race while forming the cluster: another node created the table and claimed
            // ranges in the gap between the read above and this LIST. A table present on the
            // re-read is the winner's — follow it, like the losing `Create` branch below.
            return match read(store, &path).await? {
                Some((found, _)) => Ok(Self::follow_winner(found, desired, remote())),
                None => Err(StoreError::MissingClusterConfig {
                    path: path.to_string(),
                }),
            };
        }

        let table = PartitionTable::equal(desired);
        let body = bytes::Bytes::from(encode(&table));
        // An ambiguous write is resolved by reading back: every node creates the `equal`
        // version-1 table, so two identical tables are the same cluster — whoever wrote it.
        match crate::cas::put_resolving(store, &path, body.clone(), PutMode::Create).await {
            Ok(crate::cas::Cas::Written(_)) => {
                // History after the winning write. Two nodes that both "win" with the identical
                // `equal` table write the same bytes — the later one's `Create` reads back
                // exactly that.
                crate::cluster_history::after_won(store, prefix, table.version(), body).await;
                Ok(Self::new(table, Some(remote())))
            }
            // Lost the race. Read back and follow the winner — at cluster creation every value
            // is equally valid, the only thing that matters is **one** value.
            Ok(crate::cas::Cas::Lost) => {
                let (found, _) =
                    read(store, &path)
                        .await?
                        .ok_or_else(|| StoreError::BadClusterConfig {
                            reason: "the cluster configuration both exists and cannot be read"
                                .into(),
                        })?;
                Ok(Self::follow_winner(found, desired, remote()))
            }
            Err(e) => Err(StoreError::LeaseIo(e.to_string())),
        }
    }

    /// Follows the table another node just created while forming the cluster — at cluster
    /// creation every value is equally valid, the only thing that matters is **one** value.
    fn follow_winner(found: PartitionTable, desired: u16, remote: Remote) -> Self {
        if found.partition_count() != desired {
            tracing::warn!(
                cluster_count = found.partition_count(),
                node_count = desired,
                "another node created the cluster first; following the cluster's partition count"
            );
        }
        Self::new(found, Some(remote))
    }

    /// Reads `cluster.conf` **without creating** it — for offline tools (`cairn-ctl`):
    /// [`Self::open`] creates a new cluster when the bucket has no table, so pointing at the
    /// wrong prefix would leave an empty cluster there. `Ok(None)` = the bucket has no cluster
    /// at this prefix.
    ///
    /// The returned value can still CAS through [`Self::update`] — a tool's repair command goes
    /// through exactly the same next-version rules as a node.
    pub async fn read_only(store: &Arc<dyn ObjectStore>, prefix: &Path) -> Result<Option<Self>> {
        let path = config_path(prefix);
        Ok(read(store, &path).await?.map(|(table, _)| {
            Self::new(
                table,
                Some(Remote {
                    store: Arc::clone(store),
                    path,
                    prefix: prefix.clone(),
                }),
            )
        }))
    }

    /// `last_modified` of `cluster.conf` on the bucket (ms). Intents carry no timestamp, so this
    /// is a **lower bound** on the age of every intent in the table: if the table has not
    /// changed since then, an intent has been there at least that long.
    pub async fn stored_modified_ms(&self) -> Result<Option<u64>> {
        let Some(r) = &self.shared.remote else {
            return Ok(None);
        };
        match r.store.head(&r.path).await {
            Ok(m) => Ok(Some(m.last_modified.timestamp_millis().max(0) as u64)),
            Err(OsError::NotFound { .. }) => Ok(None),
            Err(e) => Err(StoreError::LeaseIo(e.to_string())),
        }
    }

    /// A one-partition cluster, without touching the object store. For tests and single-node
    /// deployments.
    pub fn single() -> Self {
        Self::from_table(PartitionTable::equal(1))
    }

    /// A cluster of `n` partitions splitting the key space evenly, without touching the object
    /// store — for tests and for a multi-partition node in one process.
    pub fn equal(n: u16) -> Self {
        Self::from_table(PartitionTable::equal(n))
    }

    /// The current table — a snapshot: a later table change does not change this snapshot.
    pub fn table(&self) -> Arc<PartitionTable> {
        self.shared.table.borrow().clone()
    }

    /// Listens for table changes. The first value is the current table.
    pub fn subscribe(&self) -> watch::Receiver<Arc<PartitionTable>> {
        self.shared.table.subscribe()
    }

    pub fn version(&self) -> u64 {
        self.shared.table.borrow().version()
    }

    /// Applies a table **newer** than the current one. `false` = skipped (not newer).
    pub fn apply(&self, table: Arc<PartitionTable>) -> bool {
        self.shared.table.send_if_modified(|cur| {
            if table.version() > cur.version() {
                tracing::info!(
                    from = cur.version(),
                    to = table.version(),
                    "applying new range table"
                );
                *cur = table;
                true
            } else {
                false
            }
        })
    }

    /// Re-reads `cluster.conf` and applies it if newer. `Ok(false)` = unchanged, or the table
    /// does not live on the bucket (one process).
    ///
    /// **A table absent from the bucket ⇒ `Err(MissingClusterConfig)`**: a cluster whose table
    /// lives on the bucket, with a GET finding it absent, means `cluster.conf` was deleted from
    /// outside. Previously that counted as "unchanged" ⇒ every "reload the table" proof gate
    /// (D-d, trash, orphan loop, tenant purge) carried on with the RAM table. Now the caller
    /// sees an error and skips the round (delete only when provable); open ranges keep serving.
    pub async fn reload(&self) -> Result<bool> {
        let Some(r) = &self.shared.remote else {
            return Ok(false);
        };
        match read(&r.store, &r.path).await? {
            Some((t, _)) => Ok(self.apply(Arc::new(t))),
            None => Err(StoreError::MissingClusterConfig {
                path: r.path.to_string(),
            }),
        }
    }

    /// The table **read from the bucket** (applied if newer, like [`Self::reload`]) — the source
    /// of truth for irreversible decisions, not the RAM table: the RAM table may come from a
    /// peer's `409` body (D23) without anyone reading the bucket. `Ok(None)` = the table does
    /// not live on the bucket (one process): the RAM table is the only source. A cluster whose
    /// table lives on the bucket, with a GET finding it absent ⇒ `Err(MissingClusterConfig)`,
    /// like [`Self::reload`].
    pub async fn read_stored(&self) -> Result<Option<Arc<PartitionTable>>> {
        let Some(r) = &self.shared.remote else {
            return Ok(None);
        };
        match read(&r.store, &r.path).await? {
            Some((t, _)) => {
                let t = Arc::new(t);
                self.apply(Arc::clone(&t));
                Ok(Some(t))
            }
            None => Err(StoreError::MissingClusterConfig {
                path: r.path.to_string(),
            }),
        }
    }

    /// Asks the [`Self::follow`] loop to reload now, without waiting for the tick.
    pub fn request_reload(&self) {
        self.shared.reload.notify_one();
    }

    /// Watches `cluster.conf` in the background: reloads every `every`, or as soon as someone
    /// calls [`Self::request_reload`]. One GET per tick **per node** — not per range.
    ///
    /// If the table does not live on the bucket, the task ends at once.
    pub fn follow(&self, every: Duration) -> tokio::task::JoinHandle<()> {
        self.follow_with(every, Arc::new(|| None))
    }

    /// Like [`Self::follow`], with a **version hint**: `hint()` = `Some(v)` is the table version
    /// carried by the coordinator's fresh `view`. A tick where `v` is not greater than the
    /// current table skips the GET — only the coordinator polls `cluster.conf` on the tick; a
    /// greater `v` reloads at once. A reload request ([`Self::request_reload`]) always GETs, and
    /// every [`crate::LIST_NET`] ticks there is one GET regardless of the hint (reconciliation).
    /// The RAM table only shows the way; irreversible decisions read the bucket
    /// ([`Self::read_stored`]).
    pub fn follow_with(
        &self,
        every: Duration,
        hint: Arc<dyn Fn() -> Option<u64> + Send + Sync>,
    ) -> tokio::task::JoinHandle<()> {
        let me = self.clone();
        tokio::spawn(crate::io_source::tagged("cluster.follow", async move {
            if me.shared.remote.is_none() {
                return;
            }
            let mut last_get = tokio::time::Instant::now();
            loop {
                // `biased`: both branches lead to the same reload — the order only matters for
                // the deterministic sim.
                let asked = tokio::select! {
                    biased;
                    _ = me.shared.reload.notified() => true,
                    _ = tokio::time::sleep(every) => false,
                };
                let reconcile = last_get.elapsed() >= crate::LIST_NET;
                if !asked && !reconcile && hint().is_some_and(|v| v <= me.table().version()) {
                    continue;
                }
                last_get = tokio::time::Instant::now();
                if let Err(e) = me.reload().await {
                    tracing::debug!(error = %e, "cannot reload the range table");
                }
            }
        }))
    }

    /// Changes the table on the bucket by **CAS**: writes only when the table on the bucket is
    /// at `expected_version`, and the write carries the read's etag (`PutMode::Update`) — two
    /// people editing from the same version means exactly one wins.
    ///
    /// `f` receives the current table and returns the next; unknown lines of the current table
    /// (`PartitionTable::extra`) are copied over to it. The next table must have version
    /// `expected_version + 1`, must not decrease `next_range_id`, and every **new** id in it
    /// must be one never allocated in the old table — the "ids are never reused" rule (D22) is
    /// enforced here, not by the caller.
    ///
    /// Losing (version mismatch, or a failed etag) ⇒ [`TableUpdate::Conflict`] carrying the
    /// winner's table, already applied to RAM — the caller recomputes from it if it wants. A CAS
    /// with an ambiguous outcome where the table on the bucket **is exactly** `next` ⇒
    /// `Applied` ([`crate::cas::put_resolving`]). `Err` means we do not know whether the write
    /// landed.
    pub async fn update(
        &self,
        expected_version: u64,
        f: impl FnOnce(&PartitionTable) -> Result<PartitionTable> + Send,
    ) -> Result<TableUpdate> {
        let r = self
            .shared
            .remote
            .as_ref()
            .ok_or_else(|| StoreError::BadClusterConfig {
                reason: "the range table does not live on the bucket — nothing to CAS onto".into(),
            })?;
        let gone = || StoreError::BadClusterConfig {
            reason: "cluster.conf vanished from the bucket".into(),
        };
        let (cur, version) = read(&r.store, &r.path).await?.ok_or_else(gone)?;
        let cur = Arc::new(cur);
        self.apply(Arc::clone(&cur));
        if cur.version() != expected_version {
            return Ok(TableUpdate::Conflict(cur));
        }
        // Unknown lines of the current table (added by a newer binary) carry over verbatim to
        // the next table — `f` builds the table from the ranges and does not know them.
        let next = f(&cur)?.with_extra(cur.extra());
        check_successor(&cur, &next)?;
        // The gap between the read and the CAS: if someone else changes the table here, our CAS
        // must fail.
        crate::failpoint!("cluster::update::before_cas", err);
        // An ambiguous outcome (object_store retried a 5xx whose first attempt had written, or an
        // error after the write) is resolved by reading back and comparing with `next`: treating
        // it as lost would leave the coordinator's own intent stuck on the table.
        let body = bytes::Bytes::from(encode(&next));
        match crate::cas::put_resolving(&r.store, &r.path, body.clone(), PutMode::Update(version))
            .await
        {
            Ok(crate::cas::Cas::Written(_)) => {
                let next = Arc::new(next);
                self.apply(Arc::clone(&next));
                // History **after** the winning CAS, not before: a copy written before the CAS
                // could be a table that never won. An error here does not break the table
                // change — log + metric.
                crate::cluster_history::after_won(&r.store, &r.prefix, next.version(), body).await;
                Ok(TableUpdate::Applied(next))
            }
            Ok(crate::cas::Cas::Lost) => {
                let (won, _) = read(&r.store, &r.path).await?.ok_or_else(gone)?;
                let won = Arc::new(won);
                self.apply(Arc::clone(&won));
                Ok(TableUpdate::Conflict(won))
            }
            Err(e) => Err(StoreError::LeaseIo(e.to_string())),
        }
    }

    pub fn partition_count(&self) -> u16 {
        self.table().partition_count()
    }

    /// Ids of every range in the current table, in key order.
    pub fn partitions(&self) -> Vec<RangeId> {
        self.table().partitions().collect()
    }

    pub fn contains(&self, p: RangeId) -> bool {
        self.table().contains(p)
    }

    /// The line of range `p` in the current table.
    pub fn range(&self, p: RangeId) -> Option<RangeEntry> {
        self.table().range(p).copied()
    }

    /// The range holding a partition key.
    pub fn partition_of_key(&self, key: PartitionKey) -> RangeId {
        self.table().partition_of_key(key)
    }

    /// The range holding an invocation — reads the key from the id's first 8 bytes and looks up
    /// the table. Asks no store: that is the whole reason the key is in the id.
    pub fn partition_of_id(&self, id: &InvocationId) -> RangeId {
        self.partition_of_key(partition_key_of(id))
    }

    /// The range of an object `(tenant, service, key)`.
    pub fn partition_of(&self, tenant: &TenantId, service: &str, object_key: &str) -> RangeId {
        self.partition_of_key(
            partition_key_for_target(tenant, None, service, object_key, None)
                .expect("a non-empty object key always yields a key"),
        )
    }

    /// The range of a scope of `tenant` — every target in the scope lands here (D-F1).
    pub fn partition_of_scope(&self, tenant: &TenantId, scope: &Scope) -> RangeId {
        self.partition_of_key(
            partition_key_for_target(tenant, Some(scope), "", "", None)
                .expect("a scope always yields a key"),
        )
    }
}

/// The rules of one table step: version +1, `next_range_id` does not decrease, new ids were
/// never allocated.
fn check_successor(cur: &PartitionTable, next: &PartitionTable) -> Result<()> {
    let bad = |reason: String| Err(StoreError::BadClusterConfig { reason });
    if next.version() != cur.version() + 1 {
        return bad(format!(
            "the next table must have version {}, got {}",
            cur.version() + 1,
            next.version()
        ));
    }
    if next.next_range_id() < cur.next_range_id() {
        return bad(format!(
            "next_range_id must not decrease: {} → {}",
            cur.next_range_id(),
            next.next_range_id()
        ));
    }
    // An id that an intent of the current table has allocated (allocated by the CAS writing the
    // intent, entering the table at the commit CAS) is a valid new id, even though it is already
    // below `next_range_id`.
    let promised = cur.promised();
    for r in next.ranges() {
        if !cur.contains(r.id) && r.id.0 < cur.next_range_id() && !promised.contains(&r.id) {
            return bad(format!(
                "range {} was already allocated — ids are never reused (D22)",
                r.id.0
            ));
        }
    }
    // A **new** intent only promises ids allocated by this very CAS. Without this rule, an id
    // already allocated but no longer in the table (a retired range, a child of a rolled-back
    // split) could come back through an intent — and "left the table, promised by nobody" would
    // stop being the permanent state that the tombstone-on-behalf loop (SM-11) relies on.
    for id in next.promised() {
        if !promised.contains(&id) && id.0 < cur.next_range_id() {
            return bad(format!(
                "intent promises range {} which was already allocated — ids are never reused (D22)",
                id.0
            ));
        }
    }
    Ok(())
}

fn encode(table: &PartitionTable) -> Vec<u8> {
    format!("{HEADER}\n{}", table.encode()).into_bytes()
}

fn decode(raw: &[u8]) -> Result<PartitionTable> {
    let bad = |reason: String| StoreError::BadClusterConfig { reason };
    let text = std::str::from_utf8(raw).map_err(|_| bad("not UTF-8".into()))?;
    let header = text.lines().next().unwrap_or_default();
    if header != HEADER {
        if let Some(e) = crate::format::newer_header("cluster.conf", HEADER, header) {
            return Err(e);
        }
        return Err(bad(format!(
            "header {header:?}, expected {HEADER:?} — a range table of a format before 9, with no \
             migration path; only its own old binary can read it"
        )));
    }
    PartitionTable::decode(text)
}

/// The table on the bucket with the object version to CAS onto. `Ok(None)` = absent.
async fn read(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
) -> Result<Option<(PartitionTable, UpdateVersion)>> {
    match store.get(path).await {
        Ok(g) => {
            let version = UpdateVersion {
                e_tag: g.meta.e_tag.clone(),
                version: g.meta.version.clone(),
            };
            let raw = g
                .bytes()
                .await
                .map_err(|e| StoreError::LeaseIo(e.to_string()))?;
            Ok(Some((decode(&raw)?, version)))
        }
        Err(OsError::NotFound { .. }) => Ok(None),
        Err(e) => Err(StoreError::LeaseIo(e.to_string())),
    }
}

/// Version of the table **on the bucket** at `prefix`; `Ok(None)` = no table yet.
pub(crate) async fn stored_version(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
) -> Result<Option<u64>> {
    Ok(read(store, &config_path(prefix))
        .await?
        .map(|(t, _)| t.version()))
}

/// The `cluster.conf` bytes of `table` (header + table) — for `cairn-ctl table restore` when it
/// rebuilds the committed table from a history copy.
pub fn encode_table(table: &PartitionTable) -> Vec<u8> {
    encode(table)
}

/// Decodes the bytes of a `cluster.conf` (or one of its history copies) — for `cairn-ctl table
/// restore`, without touching the bucket.
pub fn decode_table(raw: &[u8]) -> Result<PartitionTable> {
    decode(raw)
}

pub(crate) fn config_path(prefix: &Path) -> Path {
    let p = prefix.as_ref();
    if p.is_empty() {
        Path::from("cluster.conf")
    } else {
        Path::from(format!("{p}/cluster.conf"))
    }
}

/// The partition key sits in the first 8 bytes of the id (big-endian).
pub fn partition_key_of(id: &InvocationId) -> PartitionKey {
    PartitionKey(u64::from_be_bytes(
        id[..8].try_into().expect("an id is longer than 8 bytes"),
    ))
}

/// Joins a partition key with 16 bytes of identifier.
///
/// `unique` must be unique within the **key** — two invocations with different keys never get
/// the same id, even if their identifier parts collide.
pub fn make_invocation_id(key: PartitionKey, unique: [u8; ID_LEN - 8]) -> InvocationId {
    let mut id = [0u8; ID_LEN];
    id[..8].copy_from_slice(&key.0.to_be_bytes());
    id[8..].copy_from_slice(&unique);
    id
}

/// The first 16 bytes of SHA-256 over a domain-tagged, length-prefixed tuple.
fn unique_of(domain: &[u8], segs: &[&[u8]]) -> [u8; ID_LEN - 8] {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(domain);
    for seg in segs {
        h.update((seg.len() as u64).to_be_bytes());
        h.update(seg);
    }
    let digest = h.finalize();
    let mut unique = [0u8; ID_LEN - 8];
    unique.copy_from_slice(&digest[..ID_LEN - 8]);
    unique
}

/// The deterministic id of an invocation carrying an idempotency key.
///
/// Derived **from the key itself**, not from the caller: every caller with the same key — an
/// SDK on any partition, or an HTTP client at ingress — computes **the same** id by itself
/// without asking the target partition, so deduplication needs no lookup before the id is
/// known.
///
/// SHA-256 truncated to 128 bits rather than a cheap hash: idempotency keys are user-supplied
/// data, and a deliberate collision between two keys means one person receives the other's
/// result. The scope goes into the derivation because deduplication is scoped: key `k` of
/// `bob` and of `alice` are two different keys.
pub fn idempotent_invocation_id(
    key: PartitionKey,
    tenant: &TenantId,
    scope: Option<&Scope>,
    service: &str,
    handler: &str,
    object_key: &str,
    idempotency_key: &str,
) -> InvocationId {
    let scope_str = scope.map(|s| s.as_str()).unwrap_or("");
    make_invocation_id(
        key,
        tenant_unique(
            tenant,
            b"cairnflow-ik/2",
            b"cairnflow-ik-tenant/2",
            &[
                scope_str.as_bytes(),
                service.as_bytes(),
                handler.as_bytes(),
                object_key.as_bytes(),
                idempotency_key.as_bytes(),
            ],
        ),
    )
}

/// [`unique_of`] **within one tenant**: deduplication is per tenant — key `k` of two tenants is
/// two invocations. Tenant `default` keeps exactly the derivation from before tenants existed;
/// other tenants derive under their own tag, with the tenant as the first segment.
fn tenant_unique(
    tenant: &TenantId,
    domain: &[u8],
    tenant_domain: &[u8],
    segs: &[&[u8]],
) -> [u8; ID_LEN - 8] {
    if tenant.is_default() {
        return unique_of(domain, segs);
    }
    let mut all: Vec<&[u8]> = Vec::with_capacity(segs.len() + 1);
    all.push(tenant.as_str().as_bytes());
    all.extend_from_slice(segs);
    unique_of(tenant_domain, &all)
}

/// The deterministic id of run `run` of a workflow — the whole "once per workflow id"
/// mechanism: a second `run` call for the same `(scope, service, key)` computes exactly the
/// first run's id, and looking up `status(id)` is the duplicate check.
///
/// **No `handler` in the hash**: a workflow service may declare exactly one `WORKFLOW` handler
/// (`discovery::accept` refuses otherwise at registration).
pub fn workflow_invocation_id(
    key: PartitionKey,
    tenant: &TenantId,
    scope: Option<&Scope>,
    service: &str,
    workflow_key: &str,
) -> InvocationId {
    let scope_str = scope.map(|s| s.as_str()).unwrap_or("");
    make_invocation_id(
        key,
        tenant_unique(
            tenant,
            b"cairnflow-wf/2",
            b"cairnflow-wf-tenant/2",
            &[
                scope_str.as_bytes(),
                service.as_bytes(),
                workflow_key.as_bytes(),
            ],
        ),
    )
}

/// The id of a **schedule**: `schedule pk ‖ H16("cairnflow-sched/1", tenant, scope, name)`.
/// 24 bytes like an invocation id — it keys the `Schedules` row, the `Fire` outbox row
/// (`(schedule id, fire_seq)`) and is the sender of the `StartCall` message of a firing on
/// another range.
pub fn schedule_id(tenant: &TenantId, scope: Option<&Scope>, name: &str) -> InvocationId {
    let scope_str = scope.map(|s| s.as_str()).unwrap_or("");
    make_invocation_id(
        crate::partition_table::partition_key_for_schedule(tenant, scope, name),
        tenant_unique(
            tenant,
            b"cairnflow-sched/1",
            b"cairnflow-sched-tenant/1",
            &[scope_str.as_bytes(), name.as_bytes()],
        ),
    )
}

/// The id of an **unkeyed** run of schedule `schedule` at time `at`: `schedule pk ‖
/// H16("cairnflow-sched-run/1", schedule id, at)` — on the same range as the schedule (placement
/// rule like `callee_pk`), and deterministic: firing again at the same time after a crash
/// yields the same id, and looking up `status(id)` is the deduplication (CG-10).
pub fn schedule_run_id(schedule: &InvocationId, at_ms: u64) -> InvocationId {
    make_invocation_id(
        partition_key_of(schedule),
        unique_of(b"cairnflow-sched-run/1", &[schedule, &at_ms.to_be_bytes()]),
    )
}

/// The id of a manual firing (`POST /schedules/{name}/trigger`) by request id — resending the
/// same request yields the same id.
pub fn schedule_manual_run_id(schedule: &InvocationId, request: &str) -> InvocationId {
    make_invocation_id(
        partition_key_of(schedule),
        unique_of(
            b"cairnflow-sched-run/1",
            &[schedule, b"manual", request.as_bytes()],
        ),
    )
}

/// The deterministic id of the callee of a `ctx.call` (without an idempotency key):
/// `(caller, completion)` is unique, so the same outbox row processed again (after failover,
/// after a retry) yields **the same** id. Independent of the number of partitions — unlike the
/// old `Engine::callee_id`, which took the target partition as a parameter.
pub fn callee_invocation_id(
    key: PartitionKey,
    caller: &InvocationId,
    completion_id: u32,
) -> InvocationId {
    make_invocation_id(
        key,
        unique_of(
            b"cairnflow-callee/2",
            &[caller, &completion_id.to_be_bytes()],
        ),
    )
}

/// The callee's key: by the call's key tuple if there is one, otherwise by
/// `(caller, completion)` — deterministic in both cases.
pub fn callee_partition_key(
    tenant: &TenantId,
    scope: Option<&Scope>,
    service: &str,
    object_key: &str,
    idempotency_key: Option<&str>,
    caller: &InvocationId,
    completion_id: u32,
) -> PartitionKey {
    // No key ⇒ by `(caller, completion)`: the caller's id already belongs to its own tenant, so
    // that branch needs no tenant.
    partition_key_for_target(tenant, scope, service, object_key, idempotency_key)
        .unwrap_or_else(|| partition_key_for_callee(caller, completion_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::PartitionId;
    use slatedb::object_store::memory::InMemory;

    fn mem() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    /// The first `Create` of `cluster.conf` wrote, but the reply is an error. Must see: the node
    /// opens the cluster (the read-back finds exactly the table it meant to write) instead of
    /// dying at startup.
    #[tokio::test]
    async fn ambiguous_bootstrap_is_resolved_by_reread() {
        use crate::testkit::{Ambiguity, FaultyStore, Rule};
        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
            let store: Arc<dyn ObjectStore> = faulty.clone();
            faulty.add_rule(Rule::ambiguous_put("cluster.conf", reply));
            let c = ClusterConfig::open(&store, &Path::from("p"), 4)
                .await
                .unwrap_or_else(|e| panic!("{reply:?}: cluster.conf written, yet an error: {e}"));
            assert_eq!(c.partition_count(), 4);
        }
    }

    #[test]
    fn partition_recovered_from_id_via_table() {
        for n in [1u16, 2, 3, 8, 64] {
            let c = ClusterConfig::equal(n);
            for k in [0u64, 1, u64::MAX / 3, u64::MAX] {
                let id = make_invocation_id(PartitionKey(k), [7; ID_LEN - 8]);
                assert_eq!(partition_key_of(&id), PartitionKey(k));
                assert_eq!(c.partition_of_id(&id), c.partition_of_key(PartitionKey(k)));
            }
        }
    }

    /// Same object key ⇒ same partition, **always** — the condition for `EXCLUSIVE` to mean
    /// anything.
    #[test]
    fn same_object_key_same_partition() {
        let c = ClusterConfig::equal(64);
        let a = c.partition_of(&TenantId::default(), "cart", "user-42");
        for _ in 0..10 {
            assert_eq!(c.partition_of(&TenantId::default(), "cart", "user-42"), a);
        }
    }

    /// Same scope ⇒ same key ⇒ same partition, across every `N` (D-F1).
    #[test]
    fn same_scope_same_partition_for_every_n() {
        let scope = Scope::new("acme").unwrap();
        let key =
            partition_key_for_target(&TenantId::default(), Some(&scope), "", "", None).unwrap();
        for (svc, k, idem) in [
            ("cart", "", None),
            ("cart", "u-1", None),
            ("billing", "u-2", Some("k")),
        ] {
            assert_eq!(
                partition_key_for_target(&TenantId::default(), Some(&scope), svc, k, idem),
                Some(key)
            );
        }
        for n in [1u16, 2, 3, 8, 64, 999] {
            let c = ClusterConfig::equal(n);
            assert_eq!(
                c.partition_of_scope(&TenantId::default(), &scope),
                c.partition_of_key(key)
            );
        }
    }

    #[test]
    fn every_partition_is_used() {
        let c = ClusterConfig::equal(16);
        let seen: std::collections::BTreeSet<u32> = (0..4000)
            .map(|i| {
                c.partition_of(&TenantId::default(), "svc", &format!("key-{i}"))
                    .0
            })
            .collect();
        assert_eq!(
            seen.len(),
            16,
            "skewed distribution: only touched {} partitions",
            seen.len()
        );
    }

    #[test]
    fn idempotent_id_deterministic_per_key_tuple() {
        let k = PartitionKey(42);
        let a = idempotent_invocation_id(
            k,
            &TenantId::default(),
            None,
            "billing",
            "charge",
            "u-1",
            "k-42",
        );
        assert_eq!(
            a,
            idempotent_invocation_id(
                k,
                &TenantId::default(),
                None,
                "billing",
                "charge",
                "u-1",
                "k-42"
            )
        );
        assert_eq!(partition_key_of(&a), k);
        for other in [
            idempotent_invocation_id(
                k,
                &TenantId::default(),
                None,
                "billing2",
                "charge",
                "u-1",
                "k-42",
            ),
            idempotent_invocation_id(
                k,
                &TenantId::default(),
                None,
                "billing",
                "refund",
                "u-1",
                "k-42",
            ),
            idempotent_invocation_id(
                k,
                &TenantId::default(),
                None,
                "billing",
                "charge",
                "u-2",
                "k-42",
            ),
            idempotent_invocation_id(
                k,
                &TenantId::default(),
                None,
                "billing",
                "charge",
                "u-1",
                "k-43",
            ),
            idempotent_invocation_id(
                k,
                &TenantId::default(),
                Some(&Scope::new("bob").unwrap()),
                "billing",
                "charge",
                "u-1",
                "k-42",
            ),
        ] {
            assert_ne!(a, other);
        }
        // Length prefix: ("ab","c") differs from ("a","bc").
        assert_ne!(
            idempotent_invocation_id(k, &TenantId::default(), None, "ab", "c", "", "k"),
            idempotent_invocation_id(k, &TenantId::default(), None, "a", "bc", "", "k"),
        );
    }

    #[test]
    fn workflow_id_deterministic_per_key() {
        let k = PartitionKey(7);
        let a = workflow_invocation_id(k, &TenantId::default(), None, "Signup", "u-1");
        assert_eq!(
            a,
            workflow_invocation_id(k, &TenantId::default(), None, "Signup", "u-1")
        );
        assert_ne!(
            a,
            workflow_invocation_id(k, &TenantId::default(), None, "Signup", "u-2")
        );
        assert_ne!(
            workflow_invocation_id(
                k,
                &TenantId::default(),
                Some(&Scope::new("bob").unwrap()),
                "Signup",
                "u-1"
            ),
            workflow_invocation_id(
                k,
                &TenantId::default(),
                Some(&Scope::new("alice").unwrap()),
                "Signup",
                "u-1"
            ),
        );
    }

    /// The callee's id **does not depend on the number of partitions**: the same call (caller,
    /// completion) yields the same id under every table — processing an outbox row again after
    /// a table change does not spawn a second invocation.
    #[test]
    fn callee_id_independent_of_partition_count() {
        let caller = make_invocation_id(PartitionKey(9), [3; ID_LEN - 8]);
        let key = callee_partition_key(&TenantId::default(), None, "svc", "", None, &caller, 5);
        let id = callee_invocation_id(key, &caller, 5);
        for n in [1u16, 4, 64] {
            let c = ClusterConfig::equal(n);
            let again = callee_invocation_id(
                callee_partition_key(&TenantId::default(), None, "svc", "", None, &caller, 5),
                &caller,
                5,
            );
            assert_eq!(again, id);
            assert_eq!(c.partition_of_id(&again), c.partition_of_key(key));
        }
        assert_ne!(callee_invocation_id(key, &caller, 6), id);
    }

    /// **Gate off: ids byte-identical to the derivation before the call-prefix rule.**
    ///
    /// A v5 outbox row (written while the `calls/prefix` gate is off, or by an old binary) is
    /// drained with exactly this computation **forever** — even after the gate is turned on
    /// (IR-9). Pins the id of each key tier: whoever changes the old rule's derivation must
    /// change this test, and read this sentence. The unique part of the id
    /// (`cairnflow-callee/2`) is also the unique part of the prefix rule, so only the 8 pk bytes
    /// may differ between the two rules.
    #[test]
    fn callee_ids_with_the_gate_off_are_pinned() {
        let caller = make_invocation_id(PartitionKey(0x0102_0304_0506_0708), [9; ID_LEN - 8]);
        let tenant = TenantId::default();
        let scope = Scope::new("acme").unwrap();
        let ids = [
            (None, "", None),
            (None, "user-1", None),
            (Some(&scope), "", None),
            (None, "", Some("idem-1")),
        ]
        .map(|(scope, key, idem)| {
            let pk = callee_partition_key(&tenant, scope, "Leaf", key, idem, &caller, 1);
            let id = match idem {
                Some(i) => idempotent_invocation_id(pk, &tenant, scope, "Leaf", "echo", key, i),
                None => callee_invocation_id(pk, &caller, 1),
            };
            crate::id_hex(&id)
        });
        assert_eq!(ids, PINNED_GATE_OFF_IDS);
        // The prefix rule only changes the 8 pk bytes of the unkeyed tier.
        let prefixed = callee_invocation_id(
            crate::partition_key_for_callee_prefix(partition_key_of(&caller), &caller, 1),
            &caller,
            1,
        );
        assert_eq!(&crate::id_hex(&prefixed)[16..], &ids[0][16..]);
        assert_eq!(&crate::id_hex(&prefixed)[..8], "01020304");
    }
    const PINNED_GATE_OFF_IDS: [&str; 4] = [
        // unkeyed: pk = H("callee", caller, cid)
        "8c6a127e4a08b407162377fe0c73d4f812bf31a82f6754d8",
        // object key
        "916c958ee8e5e2dc162377fe0c73d4f812bf31a82f6754d8",
        // scope
        "8e678f3ea9ec1870162377fe0c73d4f812bf31a82f6754d8",
        // idempotency key
        "dd549f3c80e06d4b9b099ed581d5d6827ffc05652d63005a",
    ];

    #[tokio::test]
    async fn config_created_once_then_read_back() {
        let store = mem();
        let p = Path::default();
        let a = ClusterConfig::open(&store, &p, 8).await.unwrap();
        assert_eq!(a.partition_count(), 8);
        let b = ClusterConfig::open(&store, &p, 8).await.unwrap();
        assert_eq!(a, b);
    }

    /// A node configured inconsistently with the existing cluster ⇒ **refuses to start**.
    #[tokio::test]
    async fn mismatched_config_is_rejected() {
        let store = mem();
        let p = Path::default();
        ClusterConfig::open(&store, &p, 8).await.unwrap();
        let err = ClusterConfig::open(&store, &p, 16).await.unwrap_err();
        assert!(
            matches!(err, StoreError::BadClusterConfig { .. }),
            "{err:?}"
        );
        assert!(err.to_string().contains("to 8"), "{err}");
    }

    #[tokio::test]
    async fn corrupt_config_rejected_not_guessed() {
        for raw in [
            &b"junk"[..],
            b"cairnflow-cluster/1\npartition_count=4\n",
            b"cairnflow-cluster/2\n",
            // An intact v2 table (`partition_count`, ranges without gen) — the old format.
            b"cairnflow-cluster/2\ntable_version=1\npartition_count=1\n\
              range=0000000000000000-ffffffffffffffff:0\n",
            b"cairnflow-cluster/3\n",
        ] {
            assert!(decode(raw).is_err(), "must refuse: {raw:?}");
        }
    }

    /// Once the table has changed (version > 1), `--partitions` at startup means nothing: it is
    /// only the number of ranges at cluster creation, and the bucket's table wins.
    #[tokio::test]
    async fn changed_table_skips_startup_partition_count_check() {
        let store = mem();
        let p = Path::default();
        let a = ClusterConfig::open(&store, &p, 2).await.unwrap();
        let mut r: Vec<RangeEntry> = a.table().ranges().to_vec();
        // Split the last range in two: three ranges, ids 2 and 3 are new.
        let last = r.pop().unwrap();
        let mid = last.start + (last.end - last.start) / 2;
        r.push(RangeEntry::new(last.start, mid, PartitionId(2)));
        r.push(RangeEntry::new(mid + 1, last.end, PartitionId(3)));
        let applied = a
            .update(1, |t| PartitionTable::from_entries(t.version() + 1, 4, r))
            .await
            .unwrap();
        assert!(matches!(applied, TableUpdate::Applied(_)), "{applied:?}");
        let b = ClusterConfig::open(&store, &p, 2).await.unwrap();
        assert_eq!(b.partition_count(), 3);
        assert_eq!(
            b.partitions(),
            [PartitionId(0), PartitionId(2), PartitionId(3)]
        );
    }

    /// An already allocated id may not come back, and the version must increase by exactly one.
    #[tokio::test]
    async fn update_enforces_no_id_reuse() {
        let store = mem();
        let p = Path::default();
        let a = ClusterConfig::open(&store, &p, 2).await.unwrap();
        // The same ranges, the next version — valid, no new id.
        let same = a.table().ranges().to_vec();
        assert!(matches!(
            a.update(1, |t| PartitionTable::from_entries(
                2,
                t.next_range_id(),
                same
            ))
            .await
            .unwrap(),
            TableUpdate::Applied(_)
        ));
        // Replace range 1 with range 5 over the same key span (new id, valid); then try to
        // bring back id 1 — retired, still < next_range_id.
        let mut r: Vec<RangeEntry> = a.table().ranges().to_vec();
        r[1].id = PartitionId(5);
        a.update(2, |_| PartitionTable::from_entries(3, 6, r))
            .await
            .unwrap();
        let mut back: Vec<RangeEntry> = a.table().ranges().to_vec();
        back[1].id = PartitionId(1);
        let err = a
            .update(3, |t| {
                PartitionTable::from_entries(4, t.next_range_id(), back)
            })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("never reused"), "{err}");
        // A skipped version is refused.
        let r = a.table().ranges().to_vec();
        let err = a
            .update(3, |t| PartitionTable::from_entries(9, t.next_range_id(), r))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("version 4"), "{err}");
        // A mismatched expected version ⇒ Conflict, nothing written.
        let r = a.table().ranges().to_vec();
        let got = a
            .update(1, |t| PartitionTable::from_entries(2, t.next_range_id(), r))
            .await
            .unwrap();
        assert!(matches!(got, TableUpdate::Conflict(t) if t.version() == 3));
    }

    /// **D22, the other half:** a **new** intent may only promise ids allocated by that very
    /// CAS (`≥ next_range_id` of the current table). An already allocated id that is no longer
    /// in the table and promised by no intent — a retired range, or a child of a rolled-back
    /// split — never comes back, not even through an intent. The tombstone-on-behalf loop
    /// (SM-11) relies on this: it writes tombstones for such ranges without having to hold the
    /// table still until the owner-record CAS.
    #[tokio::test]
    async fn update_forbids_new_intent_promising_issued_id() {
        let store = mem();
        let p = Path::default();
        let a = ClusterConfig::open(&store, &p, 2).await.unwrap();
        let split_of_0 = |left: u32, right: u32| {
            move |t: &PartitionTable| {
                let r = t
                    .ranges()
                    .iter()
                    .map(|r| {
                        if r.id == PartitionId(0) {
                            RangeEntry {
                                intent: Some(crate::partition_table::RangeIntent::Splitting {
                                    at: r.end / 2,
                                    left: PartitionId(left),
                                    right: PartitionId(right),
                                }),
                                ..*r
                            }
                        } else {
                            *r
                        }
                    })
                    .collect();
                PartitionTable::from_entries(t.version() + 1, t.next_range_id().max(right + 1), r)
            }
        };
        // v2: the intent allocates 2, 3 (by the rules). v3: rollback — 2, 3 allocated, no range.
        a.update(1, split_of_0(2, 3)).await.unwrap();
        let clear: Vec<RangeEntry> = a
            .table()
            .ranges()
            .iter()
            .map(|r| RangeEntry { intent: None, ..*r })
            .collect();
        a.update(2, |t| {
            PartitionTable::from_entries(3, t.next_range_id(), clear)
        })
        .await
        .unwrap();
        // v4: a new intent promises 2, 3 again — must be refused.
        let err = a.update(3, split_of_0(2, 3)).await.unwrap_err();
        assert!(err.to_string().contains("never reused"), "{err}");
        // A new intent allocating new ids (4, 5) is still valid.
        a.update(3, split_of_0(4, 5)).await.unwrap();
        // An intent **already present** in the previous table passes through intact (the table
        // changed for another reason while the intent is in flight).
        let same = a.table().ranges().to_vec();
        a.update(4, |t| {
            PartitionTable::from_entries(5, t.next_range_id(), same)
        })
        .await
        .unwrap();
    }

    /// A newer binary adds a line `x=1` to `cluster.conf`; after this binary (whose struct does
    /// not know the key `x`) completes a split, that line must still be there verbatim. The old
    /// code re-encoded from the struct and silently deleted every unknown line — a new field
    /// could not survive a rolling upgrade.
    #[tokio::test]
    async fn update_keeps_unknown_lines() {
        let store = mem();
        let p = Path::default();
        let path = config_path(&p);
        let table = PartitionTable::equal(2);
        let newer = format!("{HEADER}\n{}x=1\nfuture_flag=on,off\n", table.encode());
        store.put(&path, newer.into_bytes().into()).await.unwrap();
        let c = ClusterConfig::open(&store, &p, 2).await.unwrap();
        // Split range 1 into 1→(2, 3) like the coordinator does: the new table is rebuilt from
        // the lines and carries nothing but the ranges.
        let mut r: Vec<RangeEntry> = c.table().ranges().to_vec();
        let (start, end) = (r[1].start, r[1].end);
        let mid = start + (end - start) / 2;
        r[1] = RangeEntry::new(start, mid, PartitionId(2));
        r.push(RangeEntry::new(mid + 1, end, PartitionId(3)));
        assert!(matches!(
            c.update(1, |_| PartitionTable::from_entries(2, 4, r))
                .await
                .unwrap(),
            TableUpdate::Applied(_)
        ));
        let raw = store.get(&path).await.unwrap().bytes().await.unwrap();
        let text = std::str::from_utf8(&raw).unwrap();
        assert!(
            text.contains("\nx=1\n"),
            "unknown line lost after split:\n{text}"
        );
        assert!(
            text.contains("\nfuture_flag=on,off\n"),
            "unknown line lost after split:\n{text}"
        );
        assert!(text.contains("table_version=2"), "{text}");
        // And reading back still yields exactly the new table.
        let again = ClusterConfig::open(&store, &p, 2).await.unwrap();
        assert_eq!(
            again.partitions(),
            [PartitionId(0), PartitionId(2), PartitionId(3)]
        );
    }

    /// Every clone sees **one** table; the table only moves forward.
    #[tokio::test]
    async fn every_clone_sees_one_table_that_only_moves_forward() {
        let store = mem();
        let p = Path::default();
        let a = ClusterConfig::open(&store, &p, 2).await.unwrap();
        let shared = a.clone();
        let mut rx = a.subscribe();
        let other = ClusterConfig::open(&store, &p, 2).await.unwrap();
        let r = other.table().ranges().to_vec();
        other
            .update(1, |t| PartitionTable::from_entries(2, t.next_range_id(), r))
            .await
            .unwrap();
        assert_eq!(a.version(), 1, "a has not reloaded");
        assert!(a.reload().await.unwrap());
        assert_eq!(shared.version(), 2);
        assert!(rx.has_changed().unwrap());
        assert_eq!(rx.borrow_and_update().version(), 2);
        // An older table cannot pull it back.
        assert!(!a.apply(Arc::new(PartitionTable::equal(2))));
        assert_eq!(a.version(), 2);
    }

    /// **The range table's CAS (D22).** Two nodes both read table version 1 and both write ⇒
    /// exactly one side wins; the loser does not overwrite but reads back and sees exactly the
    /// winner's table.
    ///
    /// Both must read **before** anyone gets to write — otherwise the later side only sees a
    /// version mismatch and the CAS on the etag is never exercised. `edit` waits until both
    /// have got there.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn table_update_is_cas() {
        let store = mem();
        let p = Path::default();
        let a = ClusterConfig::open(&store, &p, 4).await.unwrap();
        let b = ClusterConfig::open(&store, &p, 4).await.unwrap();
        let arrived = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        // Each side moves the boundary between the first and second range by a different
        // amount, so the two sides' tables can be told apart.
        let edit = |shift: u64| {
            let arrived = Arc::clone(&arrived);
            move |t: &PartitionTable| {
                arrived.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let t0 = tokio::time::Instant::now();
                while arrived.load(std::sync::atomic::Ordering::SeqCst) < 2
                    && t0.elapsed() < std::time::Duration::from_secs(5)
                {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                let mut r = t.ranges().to_vec();
                r[0].end -= shift;
                r[1].start -= shift;
                PartitionTable::from_entries(t.version() + 1, t.next_range_id(), r)
            }
        };
        let (ta, tb) = (a.clone(), b.clone());
        let (ea, eb) = (edit(1), edit(2));
        let ra = tokio::spawn(async move { ta.update(1, ea).await });
        let rb = tokio::spawn(async move { tb.update(1, eb).await });
        let (ra, rb) = (ra.await.unwrap().unwrap(), rb.await.unwrap().unwrap());
        let (winner, loser_cfg, seen_by_loser) = match (ra, rb) {
            (TableUpdate::Applied(w), TableUpdate::Conflict(l)) => (w, &a, l),
            (TableUpdate::Conflict(l), TableUpdate::Applied(w)) => (w, &b, l),
            other => panic!("exactly one side must win: {other:?}"),
        };
        let _ = loser_cfg;
        assert_eq!(winner.version(), 2);
        assert_eq!(
            *seen_by_loser, *winner,
            "the loser must see the winner's table"
        );
        assert_eq!(*a.table(), *winner);
        assert_eq!(*b.table(), *winner);
        // And the bucket holds exactly that table.
        let fresh = ClusterConfig::open(&store, &p, 4).await.unwrap();
        assert_eq!(*fresh.table(), *winner);
    }

    /// **An ambiguous outcome of the table CAS is resolved by reading back** (SM-2).
    /// object_store 0.14 retries a 5xx by itself, even for conditional PUTs: the first attempt
    /// wrote, the retry sees `Precondition`. Or the PUT reports an error after writing, and
    /// nobody retries.
    ///
    /// Interleaving:
    /// 1. The table is at version 1. `update(1, f)` sends the CAS of table `next` (version 2).
    /// 2. The store applies the write, then returns `Precondition` (`Retried`) or `Generic`
    ///    (`Error`) to the caller.
    ///
    /// Must see: `Applied(next)` — the table on the bucket **is** ours — and RAM has applied it.
    /// Before the fix: `Conflict(won)` with `won == next` (the coordinator believed it lost
    /// while its own intent was on the bucket), or `Err`.
    #[tokio::test]
    async fn ambiguous_cas_resolves_to_applied_when_our_table_landed() {
        use crate::testkit::{Action, Ambiguity, FaultyStore, OpSet, PathMatch, Rule};
        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let faulty = FaultyStore::wrap(mem());
            let store: Arc<dyn ObjectStore> = faulty.clone();
            let p = Path::default();
            let c = ClusterConfig::open(&store, &p, 2).await.unwrap();
            faulty.add_rule(Rule::new(
                OpSet::PUT,
                PathMatch::Contains("cluster.conf".into()),
                Action::Ambiguous {
                    reply,
                    skip: 0,
                    times: Some(1),
                },
            ));
            let got = c
                .update(1, |t| {
                    let mut r = t.ranges().to_vec();
                    r[0].end -= 7;
                    r[1].start -= 7;
                    PartitionTable::from_entries(t.version() + 1, t.next_range_id(), r)
                })
                .await;
            let on_bucket = ClusterConfig::open(&store, &p, 2).await.unwrap().table();
            assert_eq!(
                on_bucket.version(),
                2,
                "{reply:?}: the write must already be on the bucket"
            );
            match got {
                Ok(TableUpdate::Applied(t)) => {
                    assert_eq!(*t, *on_bucket, "{reply:?}");
                    assert_eq!(
                        *c.table(),
                        *on_bucket,
                        "{reply:?}: RAM has not applied our table"
                    );
                }
                other => {
                    panic!("{reply:?}: our table is on the bucket but update returned {other:?}")
                }
            }
        }
    }

    /// The other side: a CAS that **really** lost (someone else wrote first in the gap between
    /// our read and our CAS) is still a `Conflict` carrying the winner's table — the read-back
    /// sees a table different from the one we meant to write.
    #[tokio::test]
    async fn cas_that_really_lost_stays_conflict() {
        use crate::testkit::{Action, FaultyStore, Gate, OpSet, PathMatch, Rule};
        let faulty = FaultyStore::wrap(mem());
        let p = Path::default();
        let mine = ClusterConfig::open(&(faulty.clone() as Arc<dyn ObjectStore>), &p, 2)
            .await
            .unwrap();
        let other = ClusterConfig::open(faulty.inner(), &p, 2).await.unwrap();
        let shift = |by: u64| {
            move |t: &PartitionTable| {
                let mut r = t.ranges().to_vec();
                r[0].end -= by;
                r[1].start -= by;
                PartitionTable::from_entries(t.version() + 1, t.next_range_id(), r)
            }
        };
        let gate = Gate::closed();
        faulty.add_rule(Rule::new(
            OpSet::PUT,
            PathMatch::Contains("cluster.conf".into()),
            Action::Gate(gate.clone()),
        ));
        let m = mine.clone();
        let cas = tokio::spawn(async move { m.update(1, shift(3)).await });
        while gate.blocked() == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        let Ok(TableUpdate::Applied(won)) = other.update(1, shift(5)).await else {
            panic!("the other side must win version 2");
        };
        gate.open();
        match cas.await.unwrap() {
            Ok(TableUpdate::Conflict(w)) => assert_eq!(*w, *won),
            other => panic!("the CAS really lost but update returned {other:?}"),
        }
    }

    /// **A table is only created on an empty bucket** (SM-21). A prefix with range data
    /// (`ranges/`, the previous format's `partition-*`) or owner records but no `cluster.conf`
    /// ⇒ `MissingClusterConfig` pointing at the runbook, and no `cluster.conf` object is
    /// written.
    #[tokio::test]
    async fn table_is_not_created_over_cluster_data() {
        for prefix in ["", "p"] {
            for evidence in [
                "ranges/3/manifest/00000000000000000001.manifest",
                "owners/range-3",
                "partition-0/manifest/00000000000000000001.manifest",
            ] {
                let store = mem();
                let p = Path::from(prefix);
                let at = if prefix.is_empty() {
                    Path::from(evidence)
                } else {
                    Path::from(format!("{prefix}/{evidence}"))
                };
                store.put(&at, "x".into()).await.unwrap();
                let err = ClusterConfig::open(&store, &p, 2).await.unwrap_err();
                assert!(
                    matches!(err, StoreError::MissingClusterConfig { .. }),
                    "{prefix:?}/{evidence}: {err:?}"
                );
                assert!(
                    err.to_string()
                        .contains("docs/runbooks/cluster-conf-lost.md"),
                    "{err}"
                );
                assert!(
                    read(&store, &config_path(&p)).await.unwrap().is_none(),
                    "{prefix:?}/{evidence}: refused but still wrote cluster.conf"
                );
            }
        }
    }

    /// The other side: what a node writes **before** opening the table (the format marker,
    /// `migrate_cluster`'s `cluster-version`, sessions) is not evidence — a freshly formed bucket
    /// can still create the table.
    #[tokio::test]
    async fn boot_objects_are_not_cluster_data() {
        let store = mem();
        for o in [
            "format",
            "cluster-version",
            "sessions/node-a",
            "auth/cluster-secret",
        ] {
            store.put(&Path::from(o), "x".into()).await.unwrap();
        }
        let c = ClusterConfig::open(&store, &Path::default(), 2)
            .await
            .unwrap();
        assert_eq!(c.partition_count(), 2);
        assert!(read(&store, &config_path(&Path::default()))
            .await
            .unwrap()
            .is_some());
    }

    /// The race while forming the cluster is unchanged: node B reads the table as absent, then
    /// in the gap before its LIST node A creates the table (3 ranges) and claims ranges —
    /// `owners/`, `ranges/` appear. Must see: B reads back and follows A's table, no refusal.
    #[tokio::test]
    async fn bootstrap_race_follows_the_winner_that_already_has_ranges() {
        use crate::testkit::{Action, FaultyStore, Gate, OpSet, PathMatch, Rule};
        let faulty = FaultyStore::wrap(mem());
        let gate = Gate::closed();
        faulty.add_rule(Rule::new(
            OpSet::LIST,
            PathMatch::Any,
            Action::Gate(gate.clone()),
        ));
        let b = tokio::spawn({
            let store: Arc<dyn ObjectStore> = faulty.clone();
            async move { ClusterConfig::open(&store, &Path::default(), 2).await }
        });
        while gate.blocked() == 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let inner = faulty.inner();
        let a = ClusterConfig::open(inner, &Path::default(), 3)
            .await
            .unwrap();
        for o in [
            "owners/range-0",
            "ranges/0/manifest/00000000000000000001.manifest",
        ] {
            inner.put(&Path::from(o), "x".into()).await.unwrap();
        }
        gate.open();
        let b = b.await.unwrap().expect("B must follow A's table");
        assert_eq!(b, a);
        assert_eq!(b.partition_count(), 3);
    }

    /// A failed LIST ⇒ unknown whether the bucket is empty ⇒ `Err`, nothing created (delete
    /// only when provable: ambiguous means keep).
    #[tokio::test]
    async fn failed_list_never_creates_the_table() {
        use crate::testkit::{Action, FaultyStore, OpSet, PathMatch, Rule};
        let faulty = FaultyStore::wrap(mem());
        faulty.add_rule(Rule::new(
            OpSet::LIST,
            PathMatch::Any,
            Action::Fail { times: None },
        ));
        let store: Arc<dyn ObjectStore> = faulty.clone();
        let err = ClusterConfig::open(&store, &Path::default(), 2)
            .await
            .unwrap_err();
        assert!(matches!(err, StoreError::Io(_)), "{err:?}");
        assert!(read(faulty.inner(), &config_path(&Path::default()))
            .await
            .unwrap()
            .is_none());
    }

    // ── Table history `cluster-history/<version>`.

    /// The next step moves the boundary between the first and second range by `by` — version +1.
    fn shift(by: u64) -> impl FnOnce(&PartitionTable) -> Result<PartitionTable> + Send {
        move |t: &PartitionTable| {
            let mut r = t.ranges().to_vec();
            r[0].end -= by;
            r[1].start -= by;
            PartitionTable::from_entries(t.version() + 1, t.next_range_id(), r)
        }
    }

    /// The bytes of `<prefix>/cluster-history/<v>` on `store`, read directly.
    async fn history_at(
        store: &Arc<dyn ObjectStore>,
        prefix: &str,
        v: u64,
    ) -> Option<bytes::Bytes> {
        match store
            .get(&Path::from(format!("{prefix}/cluster-history/{v}")))
            .await
        {
            Ok(g) => Some(g.bytes().await.unwrap()),
            Err(OsError::NotFound { .. }) => None,
            Err(e) => panic!("{e}"),
        }
    }

    async fn conf_bytes(store: &Arc<dyn ObjectStore>, prefix: &Path) -> bytes::Bytes {
        store
            .get(&config_path(prefix))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    }

    /// Versions of every object under `<prefix>/cluster-history/`, ascending.
    async fn history_versions(store: &Arc<dyn ObjectStore>, prefix: &str) -> Vec<u64> {
        use futures::TryStreamExt;
        let listed: Vec<_> = store
            .list(Some(&Path::from(format!("{prefix}/cluster-history"))))
            .try_collect()
            .await
            .unwrap();
        let mut out: Vec<u64> = listed
            .iter()
            .map(|m| m.location.filename().unwrap().parse().unwrap())
            .collect();
        out.sort_unstable();
        out
    }

    /// **Every table that won has a history copy carrying exactly its bytes.** The first
    /// `Create` in `open` (version 1) and every winning CAS of `update` write
    /// `cluster-history/<version>` — exactly the bytes of `cluster.conf` at that version,
    /// **after** the CAS. A lost CAS writes nothing.
    #[tokio::test]
    async fn history_holds_the_bytes_of_every_won_table() {
        let store = mem();
        let p = Path::from("p");
        let c = ClusterConfig::open(&store, &p, 4).await.unwrap();
        assert_eq!(
            history_at(&store, "p", 1).await,
            Some(conf_bytes(&store, &p).await),
            "version 1 (cluster creation)"
        );
        for v in 1..4 {
            let got = c.update(v, shift(1)).await.unwrap();
            assert!(matches!(got, TableUpdate::Applied(_)), "{got:?}");
            assert_eq!(
                history_at(&store, "p", v + 1).await,
                Some(conf_bytes(&store, &p).await),
                "version {}",
                v + 1
            );
        }
        // A lost CAS (the expected version is stale) writes no history.
        let got = c.update(1, shift(1)).await.unwrap();
        assert!(matches!(got, TableUpdate::Conflict(_)), "{got:?}");
        assert_eq!(history_versions(&store, "p").await, [1, 2, 3, 4]);
    }

    /// **The 16 newest copies are kept; older ones go through the trash.** After 20 table
    /// changes (version 21) the bucket still has versions 6..=21; versions 1..=5 sit in the
    /// trash with the same bytes — restorable.
    #[tokio::test]
    async fn history_keeps_the_newest_16_and_moves_older_ones_to_the_trash() {
        use futures::TryStreamExt;
        let store = mem();
        let p = Path::from("p");
        let c = ClusterConfig::open(&store, &p, 2).await.unwrap();
        let mut saved = std::collections::BTreeMap::new();
        saved.insert(1, conf_bytes(&store, &p).await);
        for v in 1..=20 {
            let got = c.update(v, shift(1)).await.unwrap();
            assert!(matches!(got, TableUpdate::Applied(_)), "{got:?}");
            saved.insert(v + 1, conf_bytes(&store, &p).await);
        }
        assert_eq!(
            history_versions(&store, "p").await,
            (6..=21).collect::<Vec<u64>>()
        );
        let trashed: Vec<_> = store
            .list(Some(&Path::from("p/trash")))
            .try_collect()
            .await
            .unwrap();
        for v in 1..=5u64 {
            let suffix = format!("/obj/cluster-history/{v}");
            let at = trashed
                .iter()
                .find(|m| m.location.as_ref().ends_with(&suffix))
                .unwrap_or_else(|| panic!("version {v} is not in the trash: {trashed:?}"));
            let got = store
                .get(&at.location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            assert_eq!(got, saved[&v], "version {v}");
        }
    }

    /// The `Create` of a history copy wrote but the reply says otherwise (the retry's
    /// `AlreadyExists`, or an error after the write). Must see: the table change is still
    /// `Applied`, and the history copy is exactly the bytes of the new table — the name is the
    /// content, so reading back and comparing bytes resolves it.
    #[tokio::test]
    async fn ambiguous_history_write_is_resolved_by_reread() {
        use crate::testkit::{Action, Ambiguity, FaultyStore, OpSet, PathMatch, Rule};
        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let faulty = FaultyStore::wrap(mem());
            let store: Arc<dyn ObjectStore> = faulty.clone();
            let p = Path::from("p");
            let c = ClusterConfig::open(&store, &p, 2).await.unwrap();
            faulty.add_rule(Rule::new(
                OpSet::PUT,
                PathMatch::Contains("cluster-history".into()),
                Action::Ambiguous {
                    reply,
                    skip: 0,
                    times: Some(1),
                },
            ));
            let got = c.update(1, shift(7)).await.unwrap();
            assert!(matches!(got, TableUpdate::Applied(_)), "{reply:?}: {got:?}");
            assert_eq!(
                history_at(&store, "p", 2).await,
                Some(conf_bytes(&store, &p).await),
                "{reply:?}"
            );
        }
    }

    #[tokio::test]
    async fn zero_partitions_is_config_error() {
        let store = mem();
        assert!(ClusterConfig::open(&store, &Path::default(), 0)
            .await
            .is_err());
    }
}
