# Runbook — the SST disk cache is full or broken

**Alert:** `CairnflowDiskCacheThrashing`. A node's disk cache evicts entries constantly while the hit
ratio is low: the cache is smaller than the set of SSTs being read.

**Applies when** the node runs with `--disk-cache-dir`. Without that flag the node has no disk cache
and prints none of the metrics below.

## Mechanism

Code: `crates/cairn-store/src/disk_cache.rs` (wraps the SlateDB fork's `CachedObjectStore`,
`vendor/slatedb/slatedb/src/cached_object_store/`). Contracts DC1–DC5 in
[`docs/reliability/irreversible.md`](../reliability/irreversible.md) §0.3.

- One cache per node, shared by every range the node leads or follows. The cap is
  `--disk-cache-bytes` (default 16 GiB); the fork's LRU evictor keeps it under the cap.
- **Only compacted SSTs** go into the cache. WAL, manifests, leases, owner records and every other
  cairn object go straight to the object store (DC1). The cache can only make things faster or
  slower, **never wrong**.
- A damaged part (a crash mid-write) ⇒ a block checksum mismatch ⇒ SlateDB reads it again from the
  object store once and overwrites it (DC2). A disk I/O error (full, permissions, broken) ⇒ the read
  falls back to the object store (DC4).
- When it takes a range, the node loads the range's L0 SSTs into the cache in the background (cap
  256 MiB per range, 60 s).

Metrics (only when the cache is on):

- `cairnflow_disk_cache_hits_total`, `cairnflow_disk_cache_misses_total`: part reads (4 MiB) that
  hit / miss. Hit ratio =
  `rate(cairnflow_disk_cache_hits_total[15m]) / (rate(cairnflow_disk_cache_hits_total[15m]) + rate(cairnflow_disk_cache_misses_total[15m]))`.
- `cairnflow_disk_cache_evictions_total`: entries evicted for hitting the cap.
- `cairnflow_disk_cache_bytes`: bytes currently in the cache.

The same numbers through `POST /query`: `SELECT node, disk_cache_bytes, disk_cache_hit_ratio FROM sys_node`.

## Diagnosis

1. **The cache is full and evicts constantly (the alert fires).** `cairnflow_disk_cache_bytes` is
   close to `--disk-cache-bytes`, `cairnflow_disk_cache_evictions_total` grows steadily, the hit
   ratio is low. The hot SST set of the node's ranges is larger than the cap.
2. **The disk is really full (filesystem).** A cache that cannot write raises no error, it just stops
   storing: `cairnflow_disk_cache_bytes` stays flat below the cap, `cairnflow_disk_cache_misses_total`
   grows, `cairnflow_disk_cache_hits_total` barely changes. Check `df` on the volume of
   `--disk-cache-dir`.
3. **A broken disk / damaged parts.** SlateDB `warn` logs about reading a damaged cache part or a
   checksum mismatch, then reading again. Reads stay correct; each damaged block costs one GET.

## Handling

- **Raise the cap** (`--disk-cache-bytes`) if the volume has room, or **add nodes** so each node
  leads fewer ranges. Restart the node to pick up the new flag; the cache on disk survives the
  restart (the evictor rescans the directory when it opens).
- **Full / broken disk:** stop the node, delete **the whole** `--disk-cache-dir` directory (safe
  whenever the node is stopped: the cache is not a source of truth), fix the volume, restart. Or drop
  the `--disk-cache-dir` flag to run without a cache.
- **Never** edit cache files by hand or copy them to another node.

## Customer data

The cache directory holds copies of SSTs: customers' journals, state and output. Purging an
invocation does **not** remove copies already in the cache; they go when evicted or when the
directory is deleted — with no time bound. Use an encrypted volume, and delete the directory when the
node leaves the cluster ([`docs/self-hosting.md` §7, Disk cache](../self-hosting.md#disk-cache)).
