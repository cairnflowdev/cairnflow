// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Disk cache for SSTs**: one SlateDB [`CachedObjectStore`] **per node**, handed to every
//! Db, reader and compactor of a range ([`crate::ObjectStores::with_disk_cache`]).
//!
//! One disk budget, one evictor for the whole node — instead of letting each Db's `Settings`
//! build its own cache (16 GB each by default, multiplied by the number of ranges).
//!
//! The policy is the fork's (`cached_object_store/policy.rs`): only reads of tagged SSTs go
//! through the cache; WAL, manifests, untagged reads (all of cairn's own I/O: leases, owner
//! records, catalog…), compactor reads and GC go straight to the object store. Nothing that
//! decides durability or fencing goes through here. A corrupt part ⇒ a block checksum mismatch
//! ⇒ SlateDB re-reads once with `Refetch`; a disk I/O error ⇒ fall back to the object store.
//! The contract is pinned in `tests/library_contracts.rs` (`d_*`) and in
//! `docs/reliability/irreversible.md` §0.3.
//!
//! **Customer data on the node's disk**: the cache holds copies of SSTs. A purge cannot delete
//! the copies; they disappear when evicted or when the directory is deleted — see the disk
//! cache section of `docs/self-hosting.md`.

// SlateDB 0.18 marks `CachedObjectStore` deprecated (replaced by a mirror or
// `FoyerHybridCache`). Moving to `FoyerHybridCache` is a separate change; until then this
// whole module keeps using it.
#![allow(
    deprecated,
    reason = "CachedObjectStore is deprecated since SlateDB 0.18; removed when the cache moves to FoyerHybridCache"
)]

use {
    crate::{Result, StoreError},
    slatedb::{
        cached_object_store::CachedObjectStore,
        object_store::{path::Path, ObjectStore},
    },
    slatedb_common::metrics::{
        CounterFn, GaugeFn, HistogramFn, MetricsRecorder, NoopMetricsRecorder, UpDownCounterFn,
    },
    std::{
        path::PathBuf,
        sync::{
            atomic::{AtomicI64, AtomicU64, Ordering},
            Arc,
        },
        time::Duration,
    },
};

/// Default cache cap (`--disk-cache-bytes`): 16 GiB.
pub const DEFAULT_MAX_BYTES: u64 = 16 << 30;
/// Size of one part — the fork's default.
pub const PART_BYTES: usize = 4 << 20;
/// Cap on the bytes warmed **per range** when a range is taken over.
pub const WARM_MAX_BYTES: usize = 256 << 20;
/// Bound on building the cache at startup (every I/O wait gets a bound).
const OPEN_BOUND: Duration = Duration::from_secs(60);
/// Time bound on one warm-up (every I/O wait gets a bound).
pub const WARM_DEADLINE: Duration = Duration::from_secs(60);

/// Configuration (`--disk-cache-dir`, `--disk-cache-bytes`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiskCacheConfig {
    pub dir: PathBuf,
    pub max_bytes: u64,
}

impl DiskCacheConfig {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            max_bytes: DEFAULT_MAX_BYTES,
        }
    }
}

/// The node's disk cache. `Clone` is cheap (everything is shared through `Arc`).
#[derive(Clone)]
pub struct DiskCache {
    store: Arc<CachedObjectStore>,
    stats: Arc<DiskCacheStats>,
    config: DiskCacheConfig,
}

impl std::fmt::Debug for DiskCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskCache")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl DiskCache {
    /// Builds the cache on top of `upstream` (the main store, **after** every cairn wrapper:
    /// `LocalCas`, failpoints…) and starts the evictor.
    ///
    /// `cache_on_flush` and `cache_on_compaction` are on: freshly flushed / compacted SSTs go to
    /// disk at once, so there is no GET storm after each compaction.
    pub async fn open(config: DiskCacheConfig, upstream: Arc<dyn ObjectStore>) -> Result<Self> {
        std::fs::create_dir_all(&config.dir).map_err(|e| StoreError::BadStoreUri {
            uri: config.dir.display().to_string(),
            reason: format!("cannot create the disk cache directory: {e}"),
        })?;
        let stats = Arc::new(DiskCacheStats::default());
        let build = CachedObjectStore::builder(config.dir.clone(), upstream)
            .with_max_cache_size_bytes(Some(config.max_bytes as usize))
            .with_part_size_bytes(PART_BYTES)
            .with_cache_on_flush(true)
            .with_cache_on_compaction(true)
            .with_metrics_recorder(Arc::clone(&stats) as Arc<dyn MetricsRecorder>)
            .build();
        // Building only touches the local disk (scanning the directory for the evictor), but
        // the bound is still set here: a hung volume must not hang the whole startup.
        let store = crate::retry::deadline(&crate::retry::ops::OPEN_DB, OPEN_BOUND, build)
            .await
            .map_err(|_| StoreError::Io("building the disk cache exceeded its bound".into()))?
            .map_err(StoreError::from_slate)?;
        Ok(Self {
            store,
            stats,
            config,
        })
    }

    /// The store handed to Db/reader/compactor.
    pub fn store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.store) as Arc<dyn ObjectStore>
    }

    pub fn config(&self) -> &DiskCacheConfig {
        &self.config
    }

    pub fn snapshot(&self) -> DiskCacheSnapshot {
        self.stats.snapshot()
    }

    /// Loads `paths` (in priority order) into the cache up to `max_bytes`, bounded by
    /// [`WARM_DEADLINE`]. Per-file errors are ignored (the fork's best-effort); past the bound
    /// it stops — a warmer cache is an optimisation, not a correctness condition.
    pub async fn warm(&self, paths: Vec<Path>, max_bytes: usize) -> bool {
        let fut = self.store.load_files_to_cache(paths, max_bytes);
        matches!(
            crate::retry::deadline(&crate::retry::ops::DISK_CACHE_WARM, WARM_DEADLINE, fut).await,
            Ok(Ok(()))
        )
    }
}

/// The cache's measurements (from the fork's `cached_object_store/stats.rs`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DiskCacheSnapshot {
    /// Part reads that hit the cache.
    pub hits: u64,
    /// Part reads through the cache (hits + misses).
    pub accesses: u64,
    /// Bytes currently in the cache.
    pub bytes: u64,
    /// Entries (parts + heads) currently in the cache.
    pub keys: u64,
    /// Evicted entries.
    pub evicted_keys: u64,
    pub evicted_bytes: u64,
}

impl DiskCacheSnapshot {
    pub fn misses(&self) -> u64 {
        self.accesses.saturating_sub(self.hits)
    }

    /// Hit ratio; `None` while there has been no read.
    pub fn hit_ratio(&self) -> Option<f64> {
        (self.accesses > 0).then(|| self.hits as f64 / self.accesses as f64)
    }
}

/// Metric names of the fork — copies of the constants in
/// `slatedb::cached_object_store::stats` (that module is not public).
const PART_HIT_COUNT: &str = "slatedb.object_store_cache.part_hit_count";
const PART_ACCESS_COUNT: &str = "slatedb.object_store_cache.part_access_count";
const CACHE_KEYS: &str = "slatedb.object_store_cache.cache_keys";
const CACHE_BYTES: &str = "slatedb.object_store_cache.cache_bytes";
const EVICTED_KEYS: &str = "slatedb.object_store_cache.evicted_keys";
const EVICTED_BYTES: &str = "slatedb.object_store_cache.evicted_bytes";

#[derive(Default)]
struct Counter(AtomicU64);

impl CounterFn for Counter {
    fn increment(&self, value: u64) {
        self.0.fetch_add(value, Ordering::Relaxed);
    }
}

#[derive(Default)]
struct Gauge(AtomicI64);

impl GaugeFn for Gauge {
    fn set(&self, value: i64) {
        self.0.store(value, Ordering::Relaxed);
    }
}

/// The cache's recorder: keeps exactly its six numbers (atomic), no-op for any other.
#[derive(Default)]
struct DiskCacheStats {
    hits: Arc<Counter>,
    accesses: Arc<Counter>,
    keys: Arc<Gauge>,
    bytes: Arc<Gauge>,
    evicted_keys: Arc<Counter>,
    evicted_bytes: Arc<Counter>,
    noop: NoopMetricsRecorder,
}

impl DiskCacheStats {
    fn snapshot(&self) -> DiskCacheSnapshot {
        let g = |g: &Gauge| g.0.load(Ordering::Relaxed).max(0) as u64;
        DiskCacheSnapshot {
            hits: self.hits.0.load(Ordering::Relaxed),
            accesses: self.accesses.0.load(Ordering::Relaxed),
            bytes: g(&self.bytes),
            keys: g(&self.keys),
            evicted_keys: self.evicted_keys.0.load(Ordering::Relaxed),
            evicted_bytes: self.evicted_bytes.0.load(Ordering::Relaxed),
        }
    }
}

impl MetricsRecorder for DiskCacheStats {
    fn register_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn CounterFn> {
        match name {
            PART_HIT_COUNT => self.hits.clone(),
            PART_ACCESS_COUNT => self.accesses.clone(),
            EVICTED_KEYS => self.evicted_keys.clone(),
            EVICTED_BYTES => self.evicted_bytes.clone(),
            _ => self.noop.register_counter(name, description, labels),
        }
    }

    fn register_gauge(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn GaugeFn> {
        match name {
            CACHE_KEYS => self.keys.clone(),
            CACHE_BYTES => self.bytes.clone(),
            _ => self.noop.register_gauge(name, description, labels),
        }
    }

    fn register_up_down_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn UpDownCounterFn> {
        self.noop
            .register_up_down_counter(name, description, labels)
    }

    fn register_histogram(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
        boundaries: &[f64],
    ) -> Arc<dyn HistogramFn> {
        self.noop
            .register_histogram(name, description, labels, boundaries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The constant names here must match the fork's — a mismatch is a metric silently stuck at 0.
    #[test]
    fn names_match_slatedb() {
        let s = DiskCacheStats::default();
        s.register_counter(PART_ACCESS_COUNT, "", &[]).increment(4);
        s.register_counter(PART_HIT_COUNT, "", &[]).increment(3);
        s.register_gauge(CACHE_BYTES, "", &[]).set(1 << 20);
        s.register_gauge(CACHE_KEYS, "", &[]).set(2);
        s.register_counter(EVICTED_KEYS, "", &[]).increment(1);
        s.register_counter(EVICTED_BYTES, "", &[]).increment(10);
        let snap = s.snapshot();
        assert_eq!(
            snap,
            DiskCacheSnapshot {
                hits: 3,
                accesses: 4,
                bytes: 1 << 20,
                keys: 2,
                evicted_keys: 1,
                evicted_bytes: 10,
            }
        );
        assert_eq!(snap.misses(), 1);
        assert_eq!(snap.hit_ratio(), Some(0.75));
        // The real anchor: the fork's constants (read from source, as the module is not public).
        let src = include_str!("../../../vendor/slatedb/slatedb/src/cached_object_store/stats.rs");
        for n in [
            PART_HIT_COUNT,
            PART_ACCESS_COUNT,
            CACHE_KEYS,
            CACHE_BYTES,
            EVICTED_KEYS,
            EVICTED_BYTES,
        ] {
            let suffix = n.trim_start_matches("slatedb.object_store_cache.");
            assert!(
                src.contains(&format!("oscache_stat_name!(\"{suffix}\")")),
                "{n}"
            );
        }
    }
}
