# bench/query — measuring the query layer

Measured in-process, no cluster needed:

```sh
CAIRN_BENCH_PER_RANGE=10000  cargo test --release -p cairn-query --test bench -- --ignored --nocapture
CAIRN_BENCH_PER_RANGE=100000 cargo test --release -p cairn-query --test bench -- --ignored --nocapture
```

The code is in `crates/cairn-query/tests/bench.rs`: four `InMemory` ranges, N invocations per range
(1 % cancelled, 30 % `paused`, the rest `active`), 20 services. Each query runs 20 times and prints
the median and the read units (`ReadStats::units`). "Old list" is `Engine::list_invocations_where`
(tenant + `created_desc`, limit 50) on every range — the path of `GET /invocations` without `cursor`.

**Interference** (`interference`): `create` on range 0 for 2 s, alone and then together with a task
that keeps running the newest page on all four ranges. The store is `InMemory`, so this only
measures CPU/lock contention inside the process — it is **not** the real acceptance criterion (p99
of an echo turn on a real store ≤ +10 %), which has not been measured yet.

Query loop under chaos: `node harness/chaos/chaos.ts --query` (see the head of that file).

## Results 2026-10-28 (Ryzen 5 5600X, 12 threads, release build)

| query | 10 k/range: ms | read units | 100 k/range: ms | read units |
|---|---|---|---|---|
| newest page (50) | 0.90 | 200 | 1.10 | 200 |
| count per class (`sys_invocation_counts`) | 0.45 | 0 | 0.45 | 0 |
| rare class `failed` (50) | 1.35 | 400 | 11.19 | 4 000 |
| by service (50) | 0.62 | 50 | 0.75 | 50 |
| ordered by `transitioned_at` (50) | 1.63 | 450 | 2.38 | 450 |
| no index: filter that cannot be pushed down (50) | 255 | 80 000 | 4 555 | 800 000 |
| **old list `GET /invocations` (50)** | **220** | — | **997** | — |

The first page is ~245× (10 k) and ~900× (100 k) faster than the old list — the target is ≥ 5×. A
rare class ordered by `created_at` reads the whole class (the class index is ordered by
`transitioned_at`), so it scales with the size of the class. A query whose filter cannot be pushed
down reads every row — the default `read_budget` stops it (`query_budget_is_enforced`).

| `create` on range 0, 2 s | 10 k/range: count, median, p99 ms | 100 k/range: count, median, p99 ms |
|---|---|---|
| alone | 81 228, 0.023, 0.038 | 55 418, 0.032, 0.078 |
| with the newest page running continuously | 57 781, 0.031, 0.072 (1 376 queries) | 54 998, 0.034, 0.068 (1 143 queries) |

On the `InMemory` store (a turn takes ~25 µs, no I/O) CPU contention is visible at 10 k: median
+35 %, p99 almost doubled; at 100 k it is not visible (noise). Not measured on a real store yet.

## Disk cache, 2026-10-28

| measurement | with `--disk-cache-dir` | without |
|---|---|---|
| SST GETs to the store, open + first turn after a leader change (`tests/disk_cache.rs`) | 0 | 21 |
| p99 echo, SeaweedFS, concurrency 16, 60 s (two runs) | 29.00 / 29.98 ms | 29.59 / 29.94 ms |

```sh
LOAD_ARGS="--disk-cache-dir /tmp/dc" LOAD_STORE_URI=s3://walbench/dc-$RANDOM \
  LOAD_SCENARIOS=echo LOAD_SWEEP=0 LOAD_CONCURRENCY=16 LOAD_DURATION_S=60 \
  node harness/bench/load.ts
```

## Cost of the query layer: build, binary, split window (2026-10-28)

Compared with the build before the query layer: split window +7 % (limit 10 %), warm dev build
+48 % (limit 30 % — **exceeded**, left for review), stripped binary 39.6 → 151.2 MB.

### Four cache scenarios (`crates/cairn-server/tests/cache_bench.rs`, release)

```sh
CACHE_BENCH_CHAOS_S=600 cargo test --release -p cairn-server --test cache_bench -- --ignored --nocapture
```

| scenario | turns | SST GETs to the store | GET/turn | RAM cache hits | disk cache hits |
|---|---|---|---|---|---|
| **no disk cache** | | | | | |
| steady | 2000 | 2008 | 1.00 | 95.8 % | — |
| restart | 500 | 2050 | 4.10 | 88.7 % | — |
| split | 500 | 1362 | 2.72 | 93.2 % | — |
| leader change under chaos (600 s, 25 kills) | 277248 | 1417925 | 5.11 | 94.0 % | — |
| **with disk cache** | | | | | |
| steady | 2000 | 144 | 0.07 | 94.9 % | 100.0 % |
| restart | 500 | 0 | 0.00 | 87.0 % | 100.0 % |
| split | 500 | 40 | 0.08 | 92.9 % | 100.0 % |
| leader change under chaos (600 s, 25 kills) | 265856 | 39312 | 0.15 | 94.1 % | 99.9 % |
