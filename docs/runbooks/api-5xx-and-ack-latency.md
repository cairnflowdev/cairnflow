# Runbook — API `5xx` rate and ack latency

**Alerts:** `CairnflowHttp5xxRate`, `CairnflowAckLatencyHigh`.

## Mechanism

Code: `crates/cairn-server/src/metrics.rs` (`http::observe`, the outermost layer of the public
router, `ingress/mod.rs`).

- `cairnflow_http_responses_total{class}` counts **every** response of the public API by status
  class (`2xx`, `4xx`, `5xx`…), including the `401`/`403`/`429` of the authentication and rate-limit
  layers. Console static files and the internal port (`/internal/*`) are not counted.
- `cairnflow_ack_seconds{route}` is the time from receipt to the `2xx` response of a command that
  accepts work:
  - `send`: `POST /invoke/{service}/{handler}/send` (and the scoped form) — answers once the
    invocation is **durable** (INV-5), so this is pure ack latency: the WAL flush interval plus the
    PUT to the store;
  - `invoke`: `POST /invoke/{service}/{handler}` — waits for the outcome up to a cap and then answers
    `202`, so it includes the handler's run time; not used for alerting;
  - `signal`, `batch`: a durable signal into an invocation, batched commands.
- `503` is the "retry" code: not the leader (the leader hand-over window, D23), the range is
  splitting/merging, and `store_unavailable` (a transient store error — details only in the node
  log, level `warn`, "store error returned as 503"). `500` is an unexpected error: the body only has
  a lookup code ("internal node error — details in the node log, code <code>"), the details are in
  the node log at level `error` with the same code.

## Diagnosis

```sh
# Status classes per node, last 10 minutes (PromQL)
sum by (instance, class) (rate(cairnflow_http_responses_total[10m]))
# p99 ack of /send
histogram_quantile(0.99, sum by (instance, le) (rate(cairnflow_ack_seconds_bucket{route="send"}[10m])))
```

- `5xx` while `cairnflow_partitions_with_leader` < `cairnflow_partitions`: the cluster is electing
  leaders (a node just died or restarted) — see [stuck-intent.md](./stuck-intent.md).
- `5xx` together with the log "store error returned as 503": the store is slow or failing —
  `cairnflow_object_put_seconds` and `cairnflow_retry_exhausted_total` by `op`; check the provider's
  status page.
- Slow acks together with a rising `cairnflow_commit_wait_seconds`: the shared WAL flush waits on
  PUTs — a slow store or a slow node ([slow-node-self-yield.md](./slow-node-self-yield.md)). Slow
  acks together with `cairnflow_range_l0_reject_ratio` > 0: a range is L0-overloaded
  ([range-l0-overload.md](./range-l0-overload.md)).
- Node log at level `error` "internal error returned as 500" with `code`: look up the exact code
  the client reported.

## Handling

1. Store failing: wait for the provider; a client resending a create command with the same
   idempotency key is safe.
2. Prolonged leader election: follow the runbook of the `CairnflowRangesWithoutLeader` alert.
3. `500` repeating at the same place: collect the logs around the code and open an issue — this
   is a cairn bug.
