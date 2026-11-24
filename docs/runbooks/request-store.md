# Runbook — request rate to the store (cost)

**Alert:** `CairnflowStoreRequestRateHigh`.

## Mechanism

Code: `crates/cairn-store/src/metered.rs` (`MeteredStore`, wrapping the node's `main` and `wal`
stores right below the SST disk cache).

- `cairnflow_store_requests_total{op}` counts requests that **actually reach the store**, by
  operation: `put`, `list`, `copy` are class A, `get`, `head` class B, `delete` is free at typical
  S3-compatible pricing. A `list` counts one call (not each page), a multipart upload counts its
  start — so this is a lower bound.
- An idle node with 8 ranges sends about 400 class A requests per hour. Under load, each commit is
  one shared WAL PUT (batching many turns), plus compaction SSTs and a session renewal PUT every 2 s.
- The alert fires when class A exceeds 20 requests/s averaged over an hour (≈ 72 000/hour, about
  $0.36/hour at a typical class A price of $5 per million) — a cost threshold, not an error.

## Diagnosis

```sh
# class A by node and operation (PromQL)
sum by (instance, op) (rate(cairnflow_store_requests_total{op=~"put|list|copy"}[1h]))
```

- `put` grows with the number of turns: compare with `cairnflow_invoke_requests_total` and
  `cairnflow_settle_total`. One tenant calling in bursts: `GET /admin/tenants/<t>/usage` gives units
  per tenant.
- High `list`: background rounds reading directories (GC, scrubber, artifact sweep). Their intervals
  are node flags (`--gc-every-s`, `--scrub-every-s`, `--scrub-requests-per-hour`…).
- High `put` with unchanged load: the number of ranges grew (auto split) — each range has its own
  compaction and timers.

## Handling

1. Real customer load: the cost is right; review the tenant's plan and CU caps.
2. One tenant driving cost: a rate cap (the tenant's `rule`, `--key-rate`), or `limited`.
3. Background rounds too frequent for the cluster's size: lengthen their interval.
