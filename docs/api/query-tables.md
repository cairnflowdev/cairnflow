# Query tables (`POST /query`)

*Generated from `cairn_query::catalog::markdown` — do not edit by hand; `CAIRNFLOW_REGENERATE_QUERY_DOCS=1 cargo test -p cairn-query --test catalog`.*

Adding columns is fine, changing/removing one breaks the API.

## `sys_invocation`

Invocations, by the declared index. Declared order `[created_at DESC, id ASC]`.

| Column | Type | Meaning |
|---|---|---|
| `id` | text | id invocation (48 hex) |
| `tenant` | text | tenant |
| `service` | text | service |
| `handler` | text | handler |
| `key` | text | object key (empty for unkeyed services) |
| `kind` | text | `call`, `send`, `workflow`, … |
| `class` | text | `active`, `paused`, `completed`, `failed` |
| `status` | text | detailed status (`running`, `suspended`, … — one read when projected) |
| `root` | bool | root (not a child call) |
| `created_at` | timestamp | creation time |
| `transitioned_at` | timestamp | time the current class was entered (`active`: creation time) |
| `ended_at` | timestamp | end time |
| `duration_ms` | int | `ended_at − created_at` |
| `scope` | text | scope (metadata — one read when projected) |
| `parent_id` | text | caller id (metadata) |
| `schedule` | text | schedule that fired it (metadata) |
| `idempotency_key` | text | idempotency key (metadata) |
| `deployment` | text | deployment at creation (metadata) |
| `restarted_from` | text | previous run of restart-as-new (metadata) |
| `tags` | text | user tags, JSON (metadata) |
| `range` | uint | range that holds it |
| `failure_code` | int | error code of class `failed`/`paused` (572 cancelled, 575 out of attempts, 576 quarantined, 577 stuck on infrastructure); one point read per row when projected |
| `failure_message` | text | error message that goes with `failure_code` |
| `attempt` | int | attempts counted by the pending retry (`Retries`); one point read per row when projected |
| `retry_at` | timestamp | due time of the pending retry (or watchdog) |
| `last_failure_code` | int | code of the latest failed attempt (`LastFailure`); one point read per row when projected |
| `last_failure_message` | text | message of the latest failed attempt (cut at 1 KiB) |
| `last_failure_at` | timestamp | time the latest failed attempt ended |
| `purge_at` | timestamp | purge (retention) due time of a finished invocation; one point read per row when projected |
| `hash_chain_len` | int | number of entries in the tamper-evident hash chain (tenant with `hashChain` on); one point read per row when projected |
| `pinned_deployment` | text | currently pinned deployment (after `migrate`; `deployment` is the one at creation) — unfinished invocations only; one point read per row when projected |
| `root_id` | text | root of the call tree (NULL at the root) — `WHERE root_id = …` is one `ByAttr[$root]` seek per range: the whole tree of one root |
| `journal_len` | int | number of journal entries (console question 32: `WHERE journal_len >= N`) — one reverse scan for the last entry per row when projected; filtering is a residual after the scan path of another filter |
| `limit_key` | text | limit key at creation (`?limit-key=`, metadata; NULL for invocations created before this column existed) — together with `scope` it is the identity of its queue in `sys_vqueue_entry` |

## `sys_invocation_counts`

RAM counters by group; does not read the store.

| Column | Type | Meaning |
|---|---|---|
| `tenant` | text | tenant |
| `service` | text | service |
| `handler` | text | handler |
| `class` | text | class |
| `root` | bool | root |
| `count` | uint | invocation count (RAM, exact as of the commits in the range) |

## `sys_artifact_bytes`

Live artifact bytes per tenant (RAM counters, does not read the store); source of the `maxArtifactBytes` cap. A range rebuilding its counters ⇒ warning `indexing`.

| Column | Type | Meaning |
|---|---|---|
| `tenant` | text | tenant |
| `bytes` | uint | live artifact bytes (committed `Artifacts` rows; RAM, exact as of the commits in the range) |

## `sys_journal`

Journal of one invocation, in write order.

**Required key:** `id`

| Column | Type | Meaning |
|---|---|---|
| `id` | text | invocation id |
| `index` | int | entry index |
| `ty` | text | message type code, as `0x0402` |
| `name` | text | message type name (`Sleep`, `Call`, …) |
| `payload` | text | payload as hex, cut at 64 KiB |
| `payload_len` | int | payload size, bytes |
| `entry_lite` | text | decoded head, JSON: `kind`, `name`, target (`service`/`handler`/`key`), `invocationId`, `wakeUpAtMs`, `promise`, `stateKey`, `signal`, `failure`, `awaits`/`completes`, `completed` (commands awaiting a result: `false` = still waiting), `hasBody` |
| `body` | text | decoded user body, JSON `{encoding: json|utf8|base64, size, truncated, text|base64}`, cut at 64 KiB; NULL when the entry has no body |

## `sys_state`

State of one virtual object.

**Required key:** `service` + `key`

| Column | Type | Meaning |
|---|---|---|
| `tenant` | text | tenant |
| `scope` | text | scope (absent = no scope) |
| `service` | text | service |
| `key` | text | object key |
| `state_key` | text | state key (UTF-8, hex otherwise) |
| `value` | text | value (UTF-8, hex otherwise), cut at 64 KiB; `NULL` for a segmented value (the bytes live in segments) |
| `value_len` | int | value size (logical), bytes |

## `sys_value`

Segments of the segmented values of one object: one row per (holder, segment) of the owner object — a segment with several holders has several rows. Bytes/segments of the owner: `count(DISTINCT sha || gen)`, `sum(len)` over the rows `holder_kind = 'state'`.

**Required key:** `service` + `key`

| Column | Type | Meaning |
|---|---|---|
| `tenant` | text | tenant |
| `scope` | text | scope (absent = no scope) |
| `service` | text | service |
| `key` | text | object key |
| `holder_kind` | text | `state` (the current value of a key), `invocation` (journal), `promise` |
| `holder` | text | state key (UTF-8, hex otherwise), invocation id (48 hex) or promise name |
| `sha` | text | sha256 of the segment (64 hex) |
| `gen` | text | sealing generation (16 hex) |
| `len` | int | segment size, bytes |

## `sys_promise`

**Completed** durable promises of one object (workflow). Pending promises: the waiter's `sys_journal`.

**Required key:** `service` + `key`

| Column | Type | Meaning |
|---|---|---|
| `tenant` | text | tenant |
| `scope` | text | scope |
| `service` | text | service |
| `key` | text | object key |
| `name` | text | promise name |
| `ok` | bool | completed with a value (not a failure) |
| `value` | text | value (UTF-8, hex otherwise), cut at 64 KiB |
| `failure_code` | int | error code |
| `failure_message` | text | error message |
| `completed_by` | text | id of the invocation that completed it |

## `sys_artifact`

Artifacts of one invocation (content: REST `/invocations/{id}/artifacts/{sha}`).

**Required key:** `id`

| Column | Type | Meaning |
|---|---|---|
| `id` | text | invocation id |
| `sha256` | text | content hash, hex |
| `size` | int | size, bytes (absent when the row cannot be read) |
| `content_type` | text | content type |
| `epoch` | int | writer epoch |

## `sys_log`

Handler log (stream `$log`) of one invocation, by offset.

**Required key:** `id`

| Column | Type | Meaning |
|---|---|---|
| `id` | text | invocation id |
| `offset` | int | record offset |
| `step` | int | step |
| `attempt` | int | attempt |
| `kind` | text | `data`, `gap` (dropped by the cap), `superseded` |
| `body` | text | body (UTF-8, hex otherwise) |

## `sys_vqueue_entry`

Entries of one virtual queue in order (`Index::VQueue`): who holds the slot/lock (`running`), who waits (`inbox`, by `seq`). The queue of an object: `service` + `key`; of an unkeyed service: `service`.

**Required key:** `service`

| Column | Type | Meaning |
|---|---|---|
| `tenant` | text | tenant |
| `scope` | text | scope |
| `limit_key` | text | limit key |
| `service` | text | service |
| `key` | text | object key (the object's exclusive queue) |
| `stage` | text | `inbox`, `running`, `suspended`, `paused` |
| `seq` | int | order in the queue (of the range holding the entry) |
| `id` | text | invocation id |
| `range` | int | range holding the entry |

## `sys_delayed`

Delayed one-way calls not yet due (`Index::Delayed`), by due time. No key needed: the index holds only pending calls (the query's read cap bounds the size).

**Required key:** 

| Column | Type | Meaning |
|---|---|---|
| `caller_id` | text | id of the sending invocation |
| `completion_id` | int | completion of the send command |
| `due_at_ms` | int | time it will be sent, ms |
| `tenant` | text | tenant of the sender |
| `service` | text | target service |
| `handler` | text | target handler |
| `key` | text | target object key |
| `range` | int | range holding the call |

## `sys_timer`

Pending timers (`ctx.sleep`) of one invocation — "what is it waiting for" in time.

**Required key:** `id`

| Column | Type | Meaning |
|---|---|---|
| `id` | text | invocation id |
| `completion_id` | int | completion the timer completes |
| `due_at_ms` | int | due time, ms |

## `sys_stream`

Durable streams of one invocation (`ctx.stream`, `$log`, `$trace`), one row per stream.

**Required key:** `id`

| Column | Type | Meaning |
|---|---|---|
| `id` | text | invocation id |
| `name` | text | stream name |
| `chunks` | int | chunk count |
| `records` | int | record count |
| `total_bytes` | int | payload bytes received (cumulative) |
| `dropped` | int | records dropped by the cap |
| `last_offset` | int | offset of the last record |

## `sys_stream_record`

Records of one stream of one invocation, by offset.

**Required key:** `id` + `name`

| Column | Type | Meaning |
|---|---|---|
| `id` | text | invocation id |
| `name` | text | stream name |
| `offset` | int | offset |
| `step` | int | step |
| `attempt` | int | attempt |
| `kind` | text | `data`, `gap`, `superseded` |
| `body` | text | body (UTF-8, hex otherwise), cut at 64 KiB |

## `sys_promise_waiter`

Who is waiting for the **not yet** completed promises of one object (workflow).

**Required key:** `service` + `key`

| Column | Type | Meaning |
|---|---|---|
| `tenant` | text | tenant |
| `scope` | text | scope |
| `service` | text | service |
| `key` | text | object key |
| `name` | text | promise name |
| `waiter_id` | text | id of the waiting invocation |
| `completion_id` | int | completion of the waiter |

## `sys_outbox`

Undelivered calls / replies / watches / cancels (`Outbox`). Filtered by `id` (sender): rows that invocation produced; unfiltered: the whole range in production order, cluster-admin only. Replies of a finished invocation are keyed by the receiver — visible only unfiltered.

**Required key:** `id` or 

| Column | Type | Meaning |
|---|---|---|
| `range` | int | range holding the row |
| `seq` | int | production order |
| `id` | text | invocation in the key (the caller; the receiver for `reply`) |
| `completion_id` | int | completion in the key |
| `kind` | text | `call`, `reply`, `attach`, `peek_output`, `cancel`, `fire`, `signal` (`completion_id` is the send entry), `unknown` |
| `target` | text | target invocation (`attach`, `peek_output`, `cancel`) |
| `service` | text | target service (`call`, `fire`) |
| `handler` | text | target handler (`call`, `fire`) |
| `key` | text | target object key (`call`, `fire`) |

## `sys_inbound`

Cross-range messages received, not yet applied (`Inbound`), in receive order. cluster-admin only.

**Required key:** 

| Column | Type | Meaning |
|---|---|---|
| `range` | int | receiving range |
| `seq` | int | receive order |
| `kind` | text | `start_call`, `completion`, `attach`, `peek_output`, `signal`, `cancel` |
| `id` | text | invocation in the key (the callee for `start_call`, otherwise the sender / waiter) |
| `completion_id` | int | completion in the key |
| `bytes` | int | message size |

## `sys_attachment`

Extra waiters of one invocation (attach by id / workflow, duplicate calls): who is waiting for the result.

**Required key:** `id`

| Column | Type | Meaning |
|---|---|---|
| `id` | text | the awaited invocation |
| `caller_id` | text | the waiting invocation |
| `completion_id` | int | completion of the waiter that receives the result |

## `sys_span`

Spans of the `$trace` of one invocation, one row per span (`start` ∪ `end`, like `GET /invocations/{id}/spans` but flat — the tree is built with `parent_id`). Durable chunks only: records still in the leader's RAM are not seen.

**Required key:** `id`

| Column | Type | Meaning |
|---|---|---|
| `id` | text | invocation id |
| `span_id` | text | span id |
| `parent_id` | text | parent span |
| `name` | text | name |
| `start_ts` | text | `ts` of `start` (JSON) |
| `end_ts` | text | `ts` of `end` (JSON) |
| `status` | text | status at `end` |
| `unfinished` | bool | has `start`, no `end` |
| `start_lost` | bool | has `end`, no `start` |
| `attrs` | text | merged attributes (JSON) |
| `input_tokens` | int | `attrs.input_tokens` at `end` |
| `output_tokens` | int | `attrs.output_tokens` at `end` |
| `step` | int | step |
| `attempt` | int | attempt |

## `sys_object`

Objects (virtual objects, workflows) of one service — the `ObjectKeys` hint index: one row per object that ever had an invocation, **checked** on read (the object still has state or an invocation; hidden otherwise). Order by backlog: `JOIN sys_vqueue`.

**Required key:** `service`

| Column | Type | Meaning |
|---|---|---|
| `tenant` | text | tenant |
| `scope` | text | scope |
| `service` | text | service |
| `key` | text | object key |
| `has_state` | bool | still has state |
| `invocations` | int | invocations still held (counted up to 16; 16 = at least 16) |
| `range` | int | owner range |

## `sys_deployment`

Registered deployments (`GET /deployments?pinned=true`).

| Column | Type | Meaning |
|---|---|---|
| `id` | text | deployment id |
| `tenant` | text | tenant |
| `created_at_ms` | int | time registered, ms |
| `endpoint` | text | SDK URI |
| `wire_version` | int | wire version |
| `protocol_mode` | text | `BIDI_STREAM` / `REQUEST_RESPONSE` |
| `services` | text | its services (JSON) |
| `current_for` | text | services for which it is the current revision (JSON) |
| `pinned` | int | invocations still pinned (counted cluster-wide; NULL when a range has not answered) — 0 ⇒ removable |

## `sys_service`

Current services (`GET /services`).

| Column | Type | Meaning |
|---|---|---|
| `name` | text | service name |
| `ty` | text | kind (`service`, `virtual_object`, `workflow`) |
| `deployment` | text | current deployment URI |
| `deployment_id` | text | id of the current deployment |

## `sys_handler`

Handlers of every current service (`GET /services`, `handlers` flattened).

| Column | Type | Meaning |
|---|---|---|
| `service` | text | service |
| `name` | text | handler name |
| `ty` | text | handler kind (`exclusive`, `shared`, `workflow`) |
| `deployment_id` | text | id of the current deployment |

## `sys_vqueue`

Virtual queues of the ranges this node leads (`GET /vqueues`).

| Column | Type | Meaning |
|---|---|---|
| `range` | int | range |
| `queue` | text | queue name |
| `scope` | text | scope |
| `limit_key` | text | limit key |
| `service` | text | service |
| `object_key` | text | object key |
| `exclusive` | bool | exclusive (virtual object) |
| `waiting` | int | waiting |
| `granted` | int | slots granted (cumulative) |
| `blocked_times` | int | times blocked |
| `blocked_on` | text | currently blocked by |
| `lock_holder` | text | id holding the lock |

## `sys_scheduler`

Turn scheduler of each range this node leads (`GET /scheduler`).

| Column | Type | Meaning |
|---|---|---|
| `range` | int | range |
| `eligible` | int | queues eligible to run |
| `queues` | int | queues |
| `waiting` | int | waiting invocations |
| `turns_in_use` | int | turn slots in use |
| `max_turns` | int | turn slot cap |
| `locks_held` | int | locks held |
| `rules_version` | int | rule book version |
| `granted` | int | granted (cumulative) |
| `blocked` | text | blocks by reason (JSON) |
| `tenants` | text | rate per tenant (JSON) |

## `sys_limit`

Concurrency-cap counters of the ranges this node leads (`GET /limits`).

| Column | Type | Meaning |
|---|---|---|
| `range` | int | range |
| `counter` | text | counter |
| `level` | int | level |
| `used` | int | in use |
| `limit` | int | cap |
| `available` | int | available |
| `rule` | text | rule setting the cap |

## `sys_range`

The cluster's range table and the leader of each range (`GET /cluster`).

| Column | Type | Meaning |
|---|---|---|
| `range` | int | range id |
| `start` | text | start key (hex) |
| `end` | text | end key (hex) |
| `gen` | int | generation |
| `intent` | text | unfinished split/merge (JSON) |
| `leader` | text | leader node |
| `address` | text | internal port of the leader |
| `term` | int | term |

## `sys_key`

API keys (without secrets) (`GET /admin/keys`, cluster-admin).

| Column | Type | Meaning |
|---|---|---|
| `id` | text | key id |
| `tenant` | text | tenant |
| `role` | text | role |
| `created_at_ms` | int | time created, ms |

## `sys_handler_stats`

Per-handler summary, cluster-wide, default window (`GET /stats/handlers?scope=cluster`).

| Column | Type | Meaning |
|---|---|---|
| `node` | text | node (absent in the merged row) |
| `tenant` | text | tenant |
| `service` | text | service |
| `handler` | text | handler |
| `deployment` | text | deployment |
| `turns` | int | turns |
| `completed` | int | completed |
| `failed` | int | failures |
| `paused` | int | paused awaiting a human |
| `failed_attempts` | int | failed attempts |
| `suspensions` | int | suspensions |
| `steps` | int | steps |
| `run_ms` | int | run time, ms |
| `cu` | int | compute units |
| `p50_ms` | float | p50 turn, ms |
| `p99_ms` | float | p99 turn, ms |
| `max_ms` | float | longest turn, ms |
| `quarantine` | text | quarantined because of |

## `sys_metering`

The tenant's metering for the current hour (`GET /metering`).

| Column | Type | Meaning |
|---|---|---|
| `tenant` | text | tenant |
| `hour` | text | hour, `yyyy-mm-ddThh` UTC |
| `invocations` | int | invocation |
| `steps` | int | steps |
| `run_ms` | int | run time, ms |
| `input_tokens` | int | input tokens |
| `output_tokens` | int | output tokens |
| `cost_micros` | int | cost, millionths |
| `sdk_bytes_out` | int | wire bytes sent to the SDK endpoint (after compression) |
| `sdk_bytes_in` | int | wire bytes received from the SDK endpoint |
| `units` | int | billing units: each durable command ⌈payload / 64 KiB⌉ (at least 1), +2 per invocation |

## `sys_rule`

The cluster's rule book (`GET /rules`, cluster-admin).

| Column | Type | Meaning |
|---|---|---|
| `pattern` | text | pattern |
| `concurrency` | text | concurrency cap |
| `rate` | text | rate cap |
| `burst` | int | burst |
| `description` | text | description |
| `disabled` | bool | disabled |
| `modified_ms` | int | time modified, ms |
| `level` | int | level |

## `sys_schedule`

Server-side schedules of the tenant (`GET /schedules`, ranges this node leads).

| Column | Type | Meaning |
|---|---|---|
| `name` | text | schedule name |
| `id` | text | schedule id |
| `service` | text | target service |
| `handler` | text | target handler |
| `overlap` | text | `skip` / `allow` |
| `paused` | bool | paused |
| `next_at` | int | next fire, ms |
| `fired` | int | fired |
| `missed` | int | missed |
| `skipped` | int | skipped (overlap) |
| `created_at` | int | time created, ms |
| `updated_at` | int | time modified, ms |

## `sys_range_op`

Split/merge operations this node coordinates (`GET /ranges/ops`, cluster-admin).

| Column | Type | Meaning |
|---|---|---|
| `id` | text | operation id |
| `kind` | text | `split` / `merge` |
| `ranges` | text | source ranges (JSON) |
| `at` | text | split point (hex) |
| `state` | text | state (JSON) |
| `elapsed_ms` | float | running for, ms |
| `error` | text | error (JSON) |

## `sys_range_suggestion`

Suggestions to split hot/large ranges and merge cold adjacent ranges, cluster-wide (`GET /ranges/suggestions`, cluster-admin).

| Column | Type | Meaning |
|---|---|---|
| `range` | int | range |
| `reason` | text | reason (JSON) |
| `at` | text | suggested split point (hex) |
| `balance` | float | balance of the two halves |
| `blocked_by` | text | why it has not split by itself |
| `owner` | text | node holding the range |
| `turn_share` | float | share of the node's turn time |
| `hot_secs` | int | continuously hot, seconds |
| `size_bytes` | int | size, bytes |
| `since_ms` | int | suggested since, ms |
| `node` | text | node making the suggestion |
| `merge_with` | int | merge suggestion: right range |
| `cold_secs` | int | merge suggestion: continuously cold, seconds |

## `sys_quarantine`

Quarantined handlers of the tenant (`GET /admin/quarantine`).

| Column | Type | Meaning |
|---|---|---|
| `service` | text | service |
| `handler` | text | handler |
| `deployment` | text | deployment |
| `action` | text | action (JSON) |
| `since_ms` | int | since, ms |
| `expires_ms` | int | expires, ms |
| `reason` | text | reason |

## `sys_capture`

Live requests to capture the tenant's next run (`GET /admin/capture`); captured bundles: REST.

| Column | Type | Meaning |
|---|---|---|
| `id` | text | request id |
| `tenant` | text | tenant |
| `service` | text | service |
| `handler` | text | handler |
| `deployment` | text | deployment |
| `remaining` | int | captures remaining |
| `expires_ms` | int | expires, ms |

## `sys_cluster_version`

Cluster-tier migration ledger (`GET /admin/cluster-version`, cluster-admin), one row per step.

| Column | Type | Meaning |
|---|---|---|
| `to` | int | version after the step |
| `name` | text | step name |
| `tier` | text | tier |

## `sys_slow_run`

Latest slow runs, cluster-wide (`GET /stats/slow-runs?scope=cluster`; a ring of 1 000 rows per node, capped at 10 rows/s).

| Column | Type | Meaning |
|---|---|---|
| `at_ms` | int | time recorded, ms |
| `node` | text | node |
| `tenant` | text | tenant |
| `service` | text | service |
| `handler` | text | handler |
| `deployment` | text | deployment |
| `invocation` | text | invocation id |
| `outcome` | text | `completed`, `failed`, `paused`, `retry`, `suspended`, `error` |
| `total_ms` | int | total time of the run, ms |
| `threshold_ms` | int | the tenant's threshold, ms |
| `turns` | int | SDK turns |
| `steps` | int | durable steps |
| `run_ms` | int | run time, ms |
| `sdk_ms` | int | waiting for the SDK, ms |
| `durable_waits` | int | number of durability waits |
| `durable_wait_ms` | int | waiting for durability, ms |
| `engine_wait_ms` | int | waiting for the engine, ms |
| `permit_wait_ms` | int | waiting for a flow-control permit, ms |

## `sys_node`

Nodes leading at least one range, as each sees itself (`GET /cluster/nodes?scope=cluster`). Each node's `knows`: `sys_cluster_version`.

| Column | Type | Meaning |
|---|---|---|
| `node` | text | node |
| `leading` | text | ranges it leads (JSON) |
| `slow` | int | published slowness score; NULL = not slow |
| `p99_put_ms` | int | p99 PUT of the latest round, ms |
| `draining` | bool | draining ranges away |
| `disk_cache_bytes` | int | bytes in the SST disk cache; NULL = off |
| `disk_cache_hit_ratio` | float | disk cache partial hit ratio since start; NULL = off or nothing read yet |

## `sys_nodewal`

Live shared WAL logs: floor and the pinning ranges (`GET /admin/nodewal?scope=cluster`).

| Column | Type | Meaning |
|---|---|---|
| `node` | text | node |
| `log` | text | log |
| `floor` | int | lowest data object still on the bucket |
| `pinned_by` | int | first pinning range |
| `pinned_wal_id` | int | wal id of the held frame |
| `pinned_for_ms` | int | floor stuck for, ms |

## `sys_scrub`

Open findings of the latest round of the background scrubber (`GET /admin/scrub?scope=cluster`; only the runner node has rounds). Clean round ⇒ no rows.

| Column | Type | Meaning |
|---|---|---|
| `node` | text | node that ran the round |
| `at_ms` | int | time the round finished, ms |
| `complete` | bool | complete round (skipped no more than 2 %) |
| `check` | text | check (`verify`) |
| `range` | int | range |
| `detail` | text | detail |
| `heals` | text | background loop that heals it; NULL = a real leak or corruption |

## `sys_trash`

Trash entries (`GET /admin/trash`, like `cairn-ctl trash list`).

| Column | Type | Meaning |
|---|---|---|
| `entry` | text | entry id |
| `created_ms` | int | time it was trashed, ms |
| `subject` | text | what was trashed (`range-db 3`, `wal-log …`) |
| `in_place` | bool | held in place (artifact) |
| `reason` | text | reason |
| `objects` | int | number of objects under `obj/` |
| `bytes` | int | total bytes |
| `unreadable` | text | error reading the entry |

## `sys_pricing`

Token price list by model (`GET /metering/pricing`), micro-USD per 1 000 000 tokens.

| Column | Type | Meaning |
|---|---|---|
| `model` | text | model |
| `input` | int | input token price |
| `output` | int | output token price |

## `sys_tenant`

Per-tenant settings (`GET /admin/tenants`): hash chain, slow log threshold, quarantine.

| Column | Type | Meaning |
|---|---|---|
| `tenant` | text | tenant |
| `hash_chain` | bool | hash chain enabled |
| `slow_ms` | int | own slow log threshold, ms |
| `quarantine_mode` | text | own quarantine mode |
| `quarantine_rule` | text | own quarantine rule |

## `sys_archive`

Purged invocations of the tenant in one UTC day (`GET /archive?date=…`; filtering by `date = 'yyyy-mm-dd'` is required). Journal, status, hash chain: `GET /archive/{id}`.

| Column | Type | Meaning |
|---|---|---|
| `date` | text | UTC day |
| `id` | text | invocation id |
| `run` | int | run within the day (0 = first) |
| `bytes` | int | object size |
| `written_at_ms` | int | time the archive was written, ms |

## `sys_range_load`

Load per range, cluster-wide (`GET /ranges/stats?scope=cluster`) — the input of auto split.

| Column | Type | Meaning |
|---|---|---|
| `range` | int | range |
| `node` | text | leader node |
| `windows` | int | number of measurement windows |
| `ops_per_sec` | float | commands per second |
| `turn_ms_per_sec` | float | turn ms per second |
| `bytes_per_sec` | float | bytes written per second |
| `turn_share` | float | share of the node's turn time |
| `hot_streak` | int | consecutive hot windows |
| `size_bytes` | int | Db size, latest measurement |

## `sys_range_window`

Per-range flush windows (`GET /ranges/windows?format=rows`, object `range-windows`).

| Column | Type | Meaning |
|---|---|---|
| `range` | int | range |
| `window_ms` | int | flush window, ms |

## `sys_handler_history`

Per-handler summary **by hour**, last 30 days (`GET /stats/history?days=30`; one `stats/` object per day per node) — charts from 24 h to 30 days.

| Column | Type | Meaning |
|---|---|---|
| `hour_ms` | int | start of the hour, ms |
| `tenant` | text | tenant |
| `service` | text | service |
| `handler` | text | handler |
| `deployment` | text | deployment |
| `turns` | int | SDK turns |
| `completed` | int | completed invocations |
| `failed` | int | failed invocations |
| `paused` | int | suspended invocations |
| `failed_attempts` | int | failed attempts |
| `suspensions` | int | suspensions |
| `steps` | int | durable steps |
| `run_ms` | int | run time, ms |
| `cu` | int | CU |
| `p50_ms` | float | p50 turn duration, ms (bucket upper bound) |
| `p99_ms` | float | p99 turn duration, ms (bucket upper bound) |
| `max_ms` | float | longest turn, ms |

## `sys_audit`

Data-plane audit log of the tenant (`GET /audit`, last 24 hours): every action by a person — rows in a range, intents/outcomes on objects, merged denials and no-op outcomes.

| Column | Type | Meaning |
|---|---|---|
| `ts` | int | time of the action, ms |
| `tenant` | text | tenant |
| `kind` | text | `row` (in a range), `intent`/`done` (on objects), `denied`/`noop` (merged) |
| `action` | text | action (`invocation.cancel`, `deployment.register`, …) |
| `outcome` | text | `ok`, `noop`, `not_found`, `pending`, `error`, `denied`, `unknown` (reconciled: the outcome never came) |
| `actor_type` | text | actor type |
| `actor_id` | text | actor id (key id) |
| `actor_role` | text | role of the key |
| `target` | text | target (JSON) |
| `status` | int | HTTP code (outcome on objects) |
| `count` | int | number of occurrences (merged `denied`/`noop` records) |
| `ip` | text | client IP |
| `user_agent` | text | user agent |
| `request_id` | text | request id |
| `name` | text | object name |

## Keyspace coverage

| Keyspace table | Query |
|---|---|
| `InvocationStatus` | `sys_invocation` |
| `InvocationMeta` | `sys_invocation` |
| `Journal` | `sys_journal` |
| `State` | `sys_state` |
| `Idempotency` | `sys_invocation` |
| `Promises` | `sys_promise` |
| `VQueueIndex` | `sys_vqueue`, `sys_vqueue_entry` |
| `Streams` | `sys_log`, `sys_stream`, `sys_stream_record` |
| `Artifacts` | `sys_artifact`, `sys_artifact_bytes` |
| `Schedules` | `sys_schedule` |
| `Audit` | `sys_audit` |
| `Timers` | `sys_timer` |
| `Outbox` | `sys_outbox` |
| `SignalOutbox` | `sys_outbox` |
| `Inbound` | `sys_inbound` |
| `Attachments` | `sys_attachment` |
| `PromiseWaiters` | `sys_promise_waiter` |
| `Retries` | `sys_invocation` |
| `LastFailure` | `sys_invocation` |
| `Purges` | `sys_invocation` |
| `Delayed` | `sys_delayed` |
| `Pins` | `sys_invocation`, `sys_deployment` |
| `Chain` | `sys_invocation` |
| `SegmentedState` | `sys_state` |
| `ValueRefs` | `sys_value` |

## Feature coverage matrix

Every cairn feature → how to see it through `POST /query`. **REST** = an action or raw bytes, already served by an endpoint; **—** = deliberately not queryable.

| # | Feature | Table / column |
|---|---|---|
| F1 | Calls: `/invoke`, `/send`, scope, workflow, delayed | `sys_invocation`, `sys_delayed` — columns `kind`, `scope`, `tenant` |
| F2 | Idempotency key | `sys_invocation` — column `idempotency_key` |
| F3 | Status, class, timestamps | `sys_invocation` — `status`, `class`, `created_at`, `transitioned_at`, `ended_at`, `duration_ms` |
| F4 | Retries, backoff, latest error | `sys_invocation` — `attempt`, `retry_at`, `last_failure_*` |
| F5 | Pause (575/576/577), resume | `sys_invocation` — class `paused`, `failure_code`, `failure_message`; resume: REST |
| F6 | Hard / cooperative cancel, propagated down the child tree | `sys_invocation` — `failure_code = 572`; children: `parent_id` |
| F7 | `restart-as-new` | `sys_invocation` — `restarted_from` |
| F8 | `migrate` to another deployment | `sys_invocation` — `pinned_deployment` (current), `deployment` (at creation) |
| F9 | Trial replay, replay-check | — REST: read-only action, the result is not stored |
| F10 | Retention, purge | `sys_invocation` — `purge_at`; purge: REST |
| F11 | Archive | `sys_archive` — by day; details: REST `/archive/{id}` |
| F12 | Tamper-evident hash chain | `sys_invocation`, `sys_tenant` — `hash_chain_len`; check: REST `/archive/{id}?verify=true` |
| F13 | Attach (wait for the result by id / workflow) | `sys_attachment` |
| F14 | Journal | `sys_journal` |
| F15 | `ctx.sleep`, timer | `sys_timer` |
| F16 | Awakeable | `sys_journal` — uncompleted entries; resolve/reject: REST |
| F17 | Signal (wait / send) | `sys_journal`, `sys_outbox` — undelivered: `kind = 'signal'` |
| F18 | Workflow promises, waiters | `sys_promise`, `sys_promise_waiter` |
| F19 | Combinators, future tree | `sys_journal` |
| F20 | Undelivered calls / replies | `sys_outbox` |
| F21 | Unapplied inbound messages | `sys_inbound` |
| F22 | Delayed sends | `sys_delayed` |
| F23 | State | `sys_state`, `sys_value` — bytes/segments of segmented values: `sys_value` |
| F24 | `EXCLUSIVE` lock, the object's waiting queue | `sys_vqueue_entry` |
| F25 | Object list of a service | `sys_object` — `ObjectKeys` hint index, checked on read |
| F26 | Workflow: `run` turn, `SHARED` handlers, output | `sys_invocation`, `sys_promise` — output: REST |
| F27 | Scope, limit key, rule book | `sys_rule`, `sys_limit` |
| F28 | Virtual queues, scheduler, tenant fairness, throttle | `sys_vqueue`, `sys_scheduler` |
| F29 | Poison-handler quarantine | `sys_quarantine` |
| F30 | Deployments, revisions, pinned invocation count | `sys_deployment` — `pinned` |
| F31 | Services, handlers, handler options | `sys_service`, `sys_handler` |
| F32 | Schedules | `sys_schedule`, `sys_invocation` — runs: column `schedule` |
| F33 | Durable streams (`ctx.stream`) | `sys_stream`, `sys_stream_record` — ephemeral streams (RAM only): REST SSE |
| F34 | Log (`$log`) | `sys_log` |
| F35 | Span, token (`$trace`) | `sys_span` — tree: REST `/spans` |
| F36 | Artifact | `sys_artifact`, `sys_artifact_bytes` — byte: REST |
| F37 | Metering, CU, tokens, cost | `sys_metering`, `sys_pricing` |
| F38 | Per-handler statistics | `sys_handler_stats`, `sys_handler_history` — hourly, 30 days: `sys_handler_history` |
| F39 | Slow log | `sys_slow_run` |
| F40 | Capture of the next run | `sys_capture` — bundle bytes: REST |
| F41 | Metric Prometheus, OTel | — already served by `/metrics`, the OTLP exporter |
| F42 | Tenant settings | `sys_tenant` |
| F43 | API keys | `sys_key` — no secrets |
| F44 | Ranges, load, L0, throttle, flush windows | `sys_range`, `sys_range_load`, `sys_range_window` |
| F45 | Split/merge operations, split suggestions | `sys_range_op`, `sys_range_suggestion` |
| F46 | Nodes, slow nodes | `sys_node`, `sys_cluster_version` — which step each binary knows: `sys_cluster_version` |
| F47 | Version, gates | `sys_cluster_version` |
| F48 | Shared WAL | `sys_nodewal` |
| F49 | Trash (GC) | `sys_trash` |
| F50 | Scrubber / `verify` | `sys_scrub` |
| F51 | Data-plane audit log | `sys_audit` — last 24 hours; other ranges: `GET /audit?from&to` |
