# cairnflow operations runbooks

The operations pack has three parts:

- the Grafana dashboard [`deploy/grafana/cairnflow.json`](../../deploy/grafana/cairnflow.json);
- the Prometheus alert rules [`deploy/prometheus/alerts.yml`](../../deploy/prometheus/alerts.yml);
- the runbooks in this directory.

Every metric name, label, endpoint and command here comes from the code on `main`. The test
`crates/cairn-server/tests/ops_pack.rs` checks that every metric the dashboard, the alerts and the
runbooks mention exists on `/metrics` (compared with the real `metrics::render`). It also checks
that each rule has a `runbook_url` pointing at a file that exists, and that the rule name appears
in the table below. Things that have no metric yet are listed under
[Missing metrics](#missing-metrics), at the end of this page.

## Setup

`/metrics` accepts the **metrics token** (`CAIRNFLOW_METRICS_KEY`, role `metrics`: only
`GET /metrics`, every other route `403`) or a cluster-admin key. Give Prometheus the metrics
token: if the scrape configuration leaks, only metrics leak, not write access to the whole
cluster. The token is a secret of its own (≥ 32 bytes, different from `CAIRNFLOW_ADMIN_KEY`, the
same on every node, generated with `openssl rand -hex 32`); rotating it = changing the variable
and restarting the nodes, as for the bootstrap key. Each node is one target, and the `instance`
label tells nodes apart.

```yaml
# prometheus.yml
rule_files:
  - /etc/prometheus/cairnflow-alerts.yml   # = deploy/prometheus/alerts.yml
scrape_configs:
  - job_name: cairnflow
    metrics_path: /metrics
    authorization:
      type: Bearer
      credentials_file: /etc/prometheus/cairnflow-metrics-key   # = CAIRNFLOW_METRICS_KEY, mode 600
    static_configs:
      - targets: ["node-a:8080", "node-b:8080", "node-c:8080"]
```

Dashboard: Grafana → *Dashboards → New → Import*, paste `deploy/grafana/cairnflow.json`, then
pick the Prometheus datasource. The `instance` variable filters by node, the `range` variable
filters by range in the L0 row.

`runbook_url` in `alerts.yml` is a path in the repository; change it to the URL where you host
the docs.

Commands in the runbooks use two variables (the `cairn` CLI reads exactly these two as well):

```sh
export CAIRNFLOW_SERVER=http://<node>:8080
export CAIRNFLOW_TOKEN=<cluster-admin key>
```

`cairn-ctl` reads **the bucket directly** (`--main-store`, `--wal-store`, as a node does). It
needs credentials for the bucket, but no node has to be running.

## Alert → runbook

| Alert | Severity | Fires when | Runbook |
|---|---|---|---|
| `CairnflowNodeWalFloorStuck` | warning | `cairnflow_nodewal_floor_age_seconds` > 600 s | [shared-wal-floor-stalled.md](./shared-wal-floor-stalled.md) |
| `CairnflowNodeFailStops` | warning | `cairnflow_failstop_total` grows more than 2 times in 30 minutes with the same `cause` | [node-fail-stop.md](./node-fail-stop.md) |
| `CairnflowGcClockSkew` | warning | `cairnflow_gc_clock_blocked_total` grows within 1 hour (the GC runner's clock is off from the store by more than `min_age/4`) | [gc-and-barriers.md](./gc-and-barriers.md) |
| `CairnflowSessionUnexpirable` | warning | `cairnflow_session_rearm_total{outcome="failed"}` grows within 30 minutes | [session-cannot-mark-expired.md](./session-cannot-mark-expired.md) |
| `CairnflowClockRateUntrusted` | critical | `cairnflow_clock_rate_untrusted` > 0 (the node's monotonic clock runs faster or slower than the store by more than ±10% — the node has stopped leading) | [clock-rate-skew.md](./clock-rate-skew.md) |
| `CairnflowNodeSlowScoreMax` | warning | `cairnflow_slow_score` ≥ 100 for more than 1 minute | [slow-node-self-yield.md](./slow-node-self-yield.md) |
| `CairnflowNodeDrainingLong` | warning | `cairnflow_draining` = 1 for more than 20 minutes | [slow-node-self-yield.md](./slow-node-self-yield.md) |
| `CairnflowRangeL0Throttling` | warning | `cairnflow_range_l0_reject_ratio` > 0 (L0 ≥ `--l0-soft`) for more than 10 minutes | [range-l0-overload.md](./range-l0-overload.md) |
| `CairnflowRangeL0Stalling` | critical | `cairnflow_range_slatedb_l0_stalls_total` grows throughout 10 minutes | [range-l0-overload.md](./range-l0-overload.md) |
| `CairnflowRangesWithoutLeader` | critical | `cairnflow_partitions` − `cairnflow_partitions_with_leader` > 0 for more than 2 minutes | [stuck-intent.md](./stuck-intent.md) |
| `CairnflowAutoSplitFailed` | info | `cairnflow_auto_split_failed_total` grows within 30 minutes | [stuck-intent.md](./stuck-intent.md) |
| `CairnflowInfraStuck` | warning | `cairnflow_rescue_infra_stuck_total` grows within 15 minutes | [invocation-stuck-on-infra.md](./invocation-stuck-on-infra.md) |
| `CairnflowPoisonRows` | warning | `cairnflow_poison_rows_total` grows within 15 minutes | [invocation-stuck-on-infra.md](./invocation-stuck-on-infra.md) |
| `CairnflowSdkTimeouts` | warning | more than 5 turns per 15 minutes dropped by the SDK wait cap (`cairnflow_sdk_timeouts_total`), lasting 15 minutes | [turn-sdk-wait-cap.md](./turn-sdk-wait-cap.md) |
| `CairnflowRegistryStale` | critical | `cairnflow_registry_stale` = 1 for more than 2 minutes (the node refuses requests authenticated by a key in the registry) | [key-registry-stale.md](./key-registry-stale.md) |
| `CairnflowDiskCacheThrashing` | info | `cairnflow_disk_cache_evictions_total` grows and the disk cache hit ratio is < 50% throughout 30 minutes | [disk-cache.md](./disk-cache.md) |
| `CairnflowTenantsBookRejected` | warning | `cairnflow_tenants_book_rejected` = 1 for more than 2 minutes (the `tenants` object is broken, the node keeps the old registry) | [tenants-registry-broken.md](./tenants-registry-broken.md) |
| `CairnflowAuthFailuresSpike` | info | `cairnflow_auth_failures_total{kind!="missing"}` above 5/s throughout 10 minutes (count only — Bearer 401s are not blocked per IP) | [auth-failures.md](./auth-failures.md) |
| `CairnflowScrubFindings` | warning | `cairnflow_scrub_findings` > 0 for more than 15 minutes (a finding that survived two scrub rounds) | [state-scrubber.md](./state-scrubber.md) |
| `CairnflowScrubStale` | warning | `min(cairnflow_scrub_last_complete_age_seconds)` > 6 h for more than 15 minutes (no complete scrub round) | [state-scrubber.md](./state-scrubber.md) |
| `CairnflowHttp5xxRate` | warning | more than 5% of public API responses are `5xx` (`cairnflow_http_responses_total`) throughout 10 minutes, with ≥ 0.1 request/s | [api-5xx-and-ack-latency.md](./api-5xx-and-ack-latency.md) |
| `CairnflowAckLatencyHigh` | warning | p99 of `cairnflow_ack_seconds{route="send"}` > 2 s throughout 15 minutes | [api-5xx-and-ack-latency.md](./api-5xx-and-ack-latency.md) |
| `CairnflowStoreRequestRateHigh` | info | class A requests to the store (`cairnflow_store_requests_total{op=~"put\|list\|copy"}`) > 20/s averaged over an hour | [request-store.md](./request-store.md) |
| `CairnflowMemoryHigh` | warning | `cairnflow_process_resident_bytes` / `cairnflow_memory_limit_bytes` > 85% throughout 10 minutes | [host-memory-and-disk.md](./host-memory-and-disk.md) |
| `CairnflowDiskLow` | warning | `cairnflow_disk_free_bytes` / `cairnflow_disk_total_bytes` < 10% throughout 10 minutes | [host-memory-and-disk.md](./host-memory-and-disk.md) |

Three alerts we want **cannot be written yet**, because the code does not export a metric for
them. Until it does, poll an endpoint:

| Want to alert on | Poll with | Runbook |
|---|---|---|
| the cluster stuck in `upgrading` for more than N minutes | `GET /admin/cluster-version` (sample polling loop in the runbook) | [upgrade-and-rollback.md](./upgrade-and-rollback.md) |
| a hung split/merge intent | `cairn-ctl verify` (check (e), intent older than 100 s; exit code 1). Now also the scrubber's `CairnflowScrubFindings{check="stuck_intent"}` (up to two rounds late) | [stuck-intent.md](./stuck-intent.md) |
| retired-DB GC blocked for too long | `GET /admin/gc/plan` (`blocked[].by`). A DB that is **eligible** but D-d never deletes: `CairnflowScrubFindings{check="retired_unreferenced"}` (only when D-d is on) | [gc-and-barriers.md](./gc-and-barriers.md) |

## Runbooks

| Runbook | When |
|---|---|
| [slow-node-self-yield.md](./slow-node-self-yield.md) | a node has a high slow score, gives up leadership by itself, or drains for too long |
| [clock-rate-skew.md](./clock-rate-skew.md) | a node's clock runs fast/slow relative to the store; the node stops leading |
| [range-l0-overload.md](./range-l0-overload.md) | a range throttles with `429 range_overloaded`, L0 hits the wall |
| [shared-wal-floor-stalled.md](./shared-wal-floor-stalled.md) | the shared WAL's GC floor does not move |
| [node-fail-stop.md](./node-fail-stop.md) | a node drops its incarnation (fail-stop, lost session) repeatedly; read the `cause` label |
| [session-cannot-mark-expired.md](./session-cannot-mark-expired.md) | a session is stale but cannot be marked expired (torn object, stale read); nobody takes that node's ranges |
| [state-scrubber.md](./state-scrubber.md) | the state scrubber sees a leak or damage that survives two rounds |
| [disk-cache.md](./disk-cache.md) | the SST disk cache (`--disk-cache-dir`) is full, evicts constantly, or its disk is broken |
| [upgrade-and-rollback.md](./upgrade-and-rollback.md) | rolling binary upgrade, stuck `upgrading` lock, rolling back |
| [gc-and-barriers.md](./gc-and-barriers.md) | retired-DB GC blocked, setting/removing barriers, orphan DBs |
| [stuck-intent.md](./stuck-intent.md) | a half-done split/merge that nobody rolls back; `cairn-ctl intent clear` |
| [invocation-stuck-on-infra.md](./invocation-stuck-on-infra.md) | invocations `paused` with 577 (`INFRA_STUCK`), message rows that cannot be decoded |
| [restore-from-fork.md](./restore-from-fork.md) | investigating an incident or rehearsing a repair command on a `cairn-ctl fork` copy |
| [restore-from-backup.md](./restore-from-backup.md) | self-contained backup to another provider, rebuilding the cluster after losing the bucket; `cairn-ctl backup/restore` |
| [restore-from-trash.md](./restore-from-trash.md) | a DB, owner record or shared WAL log deleted by mistake; `cairn-ctl trash list/restore` |
| [cluster-conf-lost.md](./cluster-conf-lost.md) | a node refuses to start because the bucket has range data but lost `cluster.conf` (the range table); recover with `cairn-ctl table restore` (the `cluster-history/` history) or bucket versioning |
| [tenants-registry-broken.md](./tenants-registry-broken.md) | the `tenants` object is broken (mistyped limit value, broken line); nodes keep the old registry, `PUT /admin/tenants` returns 500 |
| [control-plane.md](./control-plane.md) | worst-case latency of the grids (owner watch, registry, reconciliation), forcing a refresh, 202 `pending`, turning the quiet-renewal gate and downgrade on/off |

## Missing metrics

The dashboard and alerts need the numbers below, but the code on `main` **does not export them
yet**. Names in the "Proposed" column are **proposals** only: they are not in the code, and must
not be used in the dashboard or `alerts.yml` until they are. The `ops_pack.rs` test skips this
section.

| Missing | Needed for | Proposed | Notes |
|---|---|---|---|
| Cluster version and the `upgrading` lock | alert "`upgrading` for more than N minutes", migration panel | gauges `cairnflow_cluster_version`, `cairnflow_cluster_upgrading` (0/1), `cairnflow_cluster_upgrading_seconds` (lock age by the clock of the node that sees it), `cairnflow_binary_knows` | `upgrading=` on `cluster-version` carries no start time, so the age must count from when a node first **sees** the lock. A node reads `cluster-version` at startup; it would need to re-read it periodically |
| Age of a split/merge intent on the range table | alert "hung intent" | gauge `cairnflow_range_intent_age_seconds{range,kind}` (`kind` = `splitting`/`merging`) | The table does not record when the intent was set; a node counts from when it sees the intent through a table reload. Reference threshold: `INTENT_STUCK_AFTER` = 100 s |
| Plan and result of the D-d GC round | GC panel, alert "GC blocked for too long" | gauges `cairnflow_gc_retired_eligible`, `cairnflow_gc_retired_blocked{by}` (`barrier`/`age`/`reference`/`upgrading`), `cairnflow_gc_retired_blocked_oldest_seconds`; counters `cairnflow_gc_rounds_total{outcome}` (`deleted`/`skipped`/`plan`), `cairnflow_gc_deleted_bytes_total`; gauge `cairnflow_gc_barriers{state}` (`active`/`expired`/`unreadable`) | Only the round runner (the leader of the range with the smallest id) has a plan; other nodes print no line, or print 0 with a clear label |
| Auto-split suggestions | panel "auto split (suggestions)" | gauge `cairnflow_auto_split_suggestions{reason,blocked_by}` | Today there is only `GET /ranges/suggestions`. `cairnflow_auto_split_total` and `cairnflow_auto_split_failed_total` (done/failed) exist already |
| Manual split/merge | telling table changes made by a person from those made by auto split | counters `cairnflow_range_splits_total{trigger,outcome}`, `cairnflow_range_merges_total{outcome}` | `cairnflow_range_table_version` changes with every table CAS (intent, commit, roll-back), so it cannot count splits |
| 429 `throttled` by reason | panel "429 by reason" separating the concurrency cap from the rate cap (`rate`) | counter with a **new name** `cairnflow_invoke_throttled_by_reason_total{reason}` (`concurrency`/`rate`) | `cairnflow_invoke_throttled_total` lumps both. Adding a label to the old name would create a `sum()` double-counting trap (`metrics::counter_per_partition`), hence a new name |
| End-to-end ack latency at ingress | panel "commit/ack" | histogram `cairnflow_invoke_seconds{outcome}` | Today there is only the commit part (`cairnflow_commit_wait_seconds`) and one run (`cairnflow_drive_seconds`) |
| The node's `--l0-soft` threshold | an L0 rule by view count instead of throttle level; an L0 rule when throttling is off | gauges `cairnflow_l0_soft`, `cairnflow_l0_cap` | The current rule uses `cairnflow_range_l0_reject_ratio` > 0, which does not depend on the threshold, but is silent with `--l0-soft 0` |
| Node identity on every series | joining `instance` with the node id of `/cluster`, `cairn-ctl owners` | gauge `cairnflow_node_info{node,inc,knows}` = 1 | Only `cairnflow_nodewal_floor_age_seconds` carries a `node` label |
| Self-yield events | counting yields and the ranges given up | counters `cairnflow_slow_shed_total`, `cairnflow_slow_shed_resigned_total` | Today there is only the gauge `cairnflow_draining` and a log line |
| Trash | alert "expired entry still needed" (tripwire `trash::purge::still_needed`), trash size panel | counters `cairnflow_trash_still_needed_total`, `cairnflow_trash_moved_objects_total{subject}`, `cairnflow_trash_purged_objects_total`; gauge `cairnflow_trash_bytes` | Today there is only the `error` log of the D-d runner and `cairn-ctl trash list` |
| Control plane | alerts "node cannot hear the coordinator", "registry generation behind for too long", session mode panel | gauges `cairnflow_coordinator_term`, `cairnflow_coordinating` (0/1), `cairnflow_view_age_seconds`, `cairnflow_registry_gen_applied`, `cairnflow_session_mode{mode}` (`direct`/`solo`/`delegated`); counters `cairnflow_owner_watch_hints_total{source}` (`notice`/`view`/`grid`), `cairnflow_owner_watch_revoked_total` | Today there is only `cairn-ctl cluster status` (reads the bucket) and `GET /cluster`. Runbook: [control-plane.md](./control-plane.md) |

To add a metric, add it to `metrics::render`, then move the matching row from this table to the
dashboard or `alerts.yml`. The `ops_pack.rs` test picks up the new name.
