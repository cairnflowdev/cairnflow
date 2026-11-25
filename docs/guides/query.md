# Guide: the query layer (declared indexes + DataFusion)

*This is a map, and a hypothesis: it drifts faster than the code. Check in the code before
trusting it. Search by function name; the guide deliberately records no line numbers.*

The query layer has two halves:

- **Layer A — indexes**, written in the same commit as the state transition. An immutable
  metadata row per invocation (`Table::InvocationMeta` 0x1B) and four indexes in the derived
  region (`ByCreated`, `ByService`, `ByTransition`, `ByAttr`, codes 0x0D–0x10). Each index is **one
  descriptor row** in `cairn_store::query_index::QUERY_INDEXES`; the write path, background
  build, `verify` and the path chooser all derive from that table. An in-RAM counter
  `(tenant, service, handler, class, root)` per range.
- **Layer B — SQL**, the `cairn-query` crate (DataFusion `=55.2.0`): `sys_invocation` chooses its
  path from the descriptors + counters, `sys_invocation_counts` reads RAM. Reads go through the
  range's read handle (`RangeReadHandle`: a DB clone, a snapshot at level `Remote`), **not
  through the engine**. HTTP door: `POST /query` (`cairn-server::query`).

## 1. Invariants

- **Q-1. Every index is derived from (metadata, state) through the descriptor table, at every
  moment.** After each commit, the set of index rows of a `readable` descriptor equals exactly
  `rows(descriptor, id, meta, Placement::new(class, t, meta))` over every live invocation. The only
  write path is `Mutations::query_event`; the engine calls it only in `Engine::stage_status` (the
  eight state-write sites declare the old class), `purge`, and `create` when the id **already
  exists** (overwriting create: removes the old placement in the same commit).
  Tests: `cairn-engine/tests/query_meta.rs::class_indexes_follow_transitions`,
  `::purge_leaves_no_query_rows`, `::create_over_existing_id_replaces_query_rows`,
  `cairn-server/tests/ctl.rs::verify_flags_query_index_drift`; every cluster test (`verify` at
  teardown, check (i)).
- **Q-2. `running ↔ suspended` touches no index and reads nothing.** A wake-up only takes
  `Woken`, which can only be built from a state read as `suspended`.
  Tests: `query_meta.rs::running_suspended_writes_no_index`, `::wake_only_suspended`.
- **Q-3. A class's `t` has one source.** `active` ⇒ the metadata's `created_ms`;
  `paused`/`completed`/`failed` ⇒ the timestamp in the state payload (format 10), kept as is while
  the class does not change. `Placement::new` is the only function deriving it — live writes and
  the background build both call it.
  Tests: `query_meta.rs::created_at_is_stable_across_transitions`,
  `cairn-engine/tests/derived_rebuild.rs::derived_region_rebuilds_to_same_bytes`.
- **Q-4. The path chooser only uses the descriptors that are `readable` in that range.** The mark
  `0x01 ‖ 0xFC` carries name + version; missing/different ⇒ `building`; that range takes the
  fallback + an `indexing` warning. The fallback walks the range's hot region in chunks and charges
  each chunk to the read budget before reading the next, so it never holds the hot region in memory.
  Tests: `query_meta.rs::adding_an_index_builds_in_background`,
  `::dropped_index_rows_are_swept`, `::query_index_rebuilds_after_split`,
  `cairn-query/tests/sql.rs::fallback_scan_stops_at_the_read_budget`.
- **Q-5. The background build only puts, one chunk per engine hold; the mark is written last.**
  Dying midway ⇒ the build starts over.
  Tests: `query_meta.rs::query_index_rebuild_races_live_writes`,
  `::query_build_resumes_after_death_mid_chunk`.
- **Q-6. The counters are exact per commit within a range.** Deltas apply after the batch is
  submitted; during a build, only deltas of invocations whose `ByCreated` key is ≤ the cursor apply.
  Tests: `query_meta.rs::counts_match_scan`, `cairn-query/tests/sql.rs::counts_read_nothing`.
  On a node with **on-demand** counters (`Engine::with_counts_on_demand`): the `ByCreated` scan runs
  only after the first reader of the term (`QueryCounts::snapshot` before completion marks
  `want`); until then readers get `None` (`partial` + `indexing`). Nobody asks ⇒ no scan.
  Test: `cairn-server/tests/failpoints.rs::query_counts_are_built_on_demand`.
- **Q-7. `Exact` means re-applied at the leaf.** Every form `filter::supported` accepts has a
  matching condition in `Row::admits`; any other form is `Unsupported`.
  Test: `sql.rs::query_matches_oracle` (200 random queries × an exhaustive oracle).
- **Q-8. The declared order is the real order.** The leaf declares `[created_at DESC, id ASC]`
  (or another axis/direction through `try_pushdown_sort`) and always sorts that way before
  returning; within the same timestamp, ascending id bytes = ascending hex.
  Tests: `sql.rs::scan_order_matches_declared`, `::plan_shape_list_page`,
  `cairn-store` `query_index::tests::same_time_orders_by_id_hex`.
- **Q-9. The tenant is structural.** Every stream starts with the asker's tenant prefix; point
  reads and counters filter by tenant.
  Tests: `sql.rs::tenant_is_structural`, `cairn-server/tests/query_api.rs::query_roles`.
- **Q-10. Reads see only durable rows and do not wait for the engine.**
  Tests: `cairn-store/tests/range_read.rs::query_sees_only_durable_rows`,
  `cairn-server/tests/failpoints.rs::query_does_not_wait_for_engine`.
- **Q-11. Lockdown and limits.** No DDL/DML/statements, no table functions, no files, no scalar
  array-generating functions, limits on memory/reads/rows/time, 4 concurrent queries per node.
  Results are read **block by block**: the row limit stops the query at row `max_rows + 1` instead
  of collecting everything and then counting; results hold their space in the query's pool; each
  block checks the deadline and yields the thread. Tests: `sql.rs::sql_surface_is_locked`,
  `::memory_cap_is_enforced`, `::scalar_range_is_locked`, `::rows_cap_stops_before_materialising`,
  `::result_counts_against_the_pool`, `query_api.rs::query_budget_is_enforced`.

- **Q-12. Ranges on other nodes run exactly the same leaf code, without duplicates and without
  silently missing anything.** The coordinating node reads the ranges open on it (only handles
  matching the id **and** the bounds of the current table — the parent of a split does not read
  together with its children), sends the rest to the leading node through `POST
  /internal/query/scan` (`remote::serve` runs `local_rows` like a local leaf), merges and cuts
  `fetch`. A node not answering ⇒ `partial` + the range names; a descriptor of a different version
  on a remote node ⇒ a metadata scan + `indexing`.
  Cancelling a query (deadline, request dropped) releases every snapshot; the handle of a leader
  that stepped down never returns an old snapshot as a complete answer.
  Tests: `cairn-query/tests/sql.rs::query_spans_nodes_in_process`, `::remote_down_is_partial`,
  `::mixed_index_versions_report_indexing`, `::query_cancel_releases_snapshot`,
  `cairn-server/tests/query_cluster.rs::query_spans_nodes`, `::query_during_split`,
  `::query_on_deposed_leader`.

- **Q-13. Keyed tables never scan a whole range.** `sys_journal`, `sys_state`, `sys_promise`,
  `sys_artifact`, `sys_log` require `=` on a key set ([`keyed::Spec::keys`]); missing ⇒ an error of
  the asker at planning time. Tables keyed by `id` read the metadata to check the tenant; tables
  keyed by object take the tenant from the key. An empty key set (`sys_delayed`, `sys_inbound`,
  `sys_outbox` without `id`) scans an ordered index with a read limit; `sys_inbound` and
  `sys_outbox` without `id` are cluster-admin only, refused before asking any range. A query error
  (`Sql`, `Budget`) in one range is an error of the whole query, not a `partial`.
  Tests: `sql.rs::bulk_table_without_key_is_refused`, `::journal_matches_engine` (local, remote,
  `JOIN`, tenant), `::state_reads_one_object`.

- **Q-14. View tables are REST, not copies.** RAM/bucket tables (`cairn_query::view::VIEWS`) run
  the `GET` of the corresponding REST endpoint on the node's router with the asker's key; a REST
  4xx is the asker's error. The router keeps only a `Weak` inside the query service (no `Arc`
  cycle). `params` columns (`sys_archive.date`) require `=` and only accept `[A-Za-z0-9_.:-]`
  before entering the path.
  Tests: `query_api.rs::views_match_rest`, `::views_follow_rest_roles`,
  `::ops_tables_are_cluster_admin`, `cairn-query` `view::tests::nested_rows_take_parent_columns`.

- **Q-16. Hint rows are not deleted on the write path.** The `Applies::PerObject` descriptor
  (`ObjectKeys`) carries no id; `event_ops` never deletes it. The reader (`sys_object`) checks that
  the object still has state or invocations before returning it; the sweep loop
  (`hint_sweep_step`, a background loop 60 s after the background build, not on the path to
  leadership) deletes rows with nothing left — reading and deleting under the same `&mut Engine`,
  keeping on any ambiguity. Chunks of 4 rows per engine hold, a 250 ms pause between chunks, the
  range's cursor **on this node** survives across terms (`actor::HintSweep`), one full round and
  then a 1 h pause (an unpaced sweep slowed the whole cluster down under chaos).
  Additional test: `failpoints.rs::hint_sweep_is_paced_and_resumes_across_terms`.
  Tests: `query_index::tests::object_keys_is_a_hint_never_deleted`,
  `query_meta.rs::object_hints_survive_purge_and_are_swept`,
  `sql.rs::sys_object_lists_live_objects`.

- **Q-17. Lineage follows the call without breaking the wire.** The callee receives `parent_id`
  (the waiter, or the one-way sender), `root_id` (the caller's root) and the SDK's tags through
  the tail block of the `Call` row/`StartCall` message; a message without a tail block = exactly
  the old bytes.
  Tests: `lineage.rs::call_lineage_reaches_the_callee`, `outbox::tests::call_lineage_is_a_tail`,
  `interpartition::tests::lineage_is_a_tail`, `sdk/typescript/test/wire.test.ts` (field 14).

- **Q-18. `sys_handler_history` only caches bucket days that no longer change.** The flusher only
  writes days `≥ yesterday` by its own clock, even when the flush tick is stuck for long; the
  reader treats a day starting at `s` as closed when `s + 2 days + HISTORY_SKEW_MS ≤ now`, and only
  then keeps it longer than 30 s (at most 1 h). A `deleting`/`purged` tenant ⇒ the cache is dropped,
  reads go straight to the bucket.
  Tests: `stats_history::tests::{closed_days_are_never_rewritten, cache_lists_open_days_only,
  forget_drops_closed_days}`, `tenant_purge.rs::deleting_tenant_is_purged_after_keep`.

- **Q-19. `sys_artifact_bytes` = Σ `size` of the counted `Artifacts` rows, exact per commit within
  a range.** A row is counted when it decodes **and** its id has metadata; the tenant is the
  metadata's. Every path that changes "counted or not" emits a key delta per artifact row, computed
  from the state read in the same engine hold as the commit: `Engine::stage_artifact_put` (the
  SDK's PUT, `restart`'s copy — `+size − old size`, only with metadata), `restart_in` (the new
  invocation: `+size` of the copied rows), `purge` and `purge_tenant_rows` (`−size`). Rebuilt by a
  chunked scan of the range's hot region (`artifact_bytes_step`, only when some tenant has
  `maxArtifactBytes`); only deltas of keys `≤` the cursor apply; not finished ⇒ `indexing`, and a
  range missing from the total lets the soft limit pass. Reads no store.
  Tests: `query_meta.rs::artifact_bytes_match_scan`,
  `range_model.rs::turns_and_recovery_on_model_with_random_crashes` (compared after each step),
  `cairn-engine/tests/tenant_purge.rs::tenant_purge_drops_artifact_bytes`,
  `cairn-server/tests/tenant_limits.rs::{max_artifact_bytes_lists_nothing,
  purged_artifacts_stop_counting, restart_copies_count_once, artifact_bytes_after_kill9,
  artifact_bytes_across_split_and_merge}`, `cairn-store` `artifact_bytes::tests::deltas_follow_the_cursor`.

- **Q-15. Coverage is checked.** Every `keyspace::Table` has a query table or an exclusion reason
  (`catalog::TABLE_COVERAGE`); every ingress `GET` route has a table or a reason
  (`QUERY_ENDPOINTS`); `docs/api/query-tables.md` is generated from the declarations.
  Tests: `cairn-query/tests/catalog.rs::{every_table_is_queryable_or_excluded,
  query_tables_doc_is_current, every_invocation_column_is_documented}`,
  `query_api.rs::every_read_endpoint_has_a_query_table`.

- **Q-20. A query's shape is bounded before DataFusion; REST read paths into objects charge the
  tenant's bucket.** `cairn_query::check_sql` runs before every `/query` permit and at the start of
  `run_with_params`, in two layers:
  (1) **on tokens, before parsing**: text ≤ `MAX_SQL_BYTES` 64 KiB, operators ≤
  `MAX_SQL_OPERATORS` 500, joined relations (`JOIN`/`APPLY`/commas at the `FROM` level) ≤
  `MAX_SQL_RELATIONS` 64, blocks (`SELECT`/`VALUES`/`TABLE`/set operations) ≤ `MAX_SQL_BLOCKS` 64 —
  this stops a deep left-leaning tree before sqlparser builds it (building, walking and `Drop` are
  all recursive: it aborted at 2 000 operators on a 2 MiB thread, and an `INTERSECT` of 3 448 arms
  took 76 s); (2) **on the AST** (`sql_check.rs`): parsed with exactly the session's
  `DFParserBuilder`/dialect/`recursion_limit`, the AST is walked against an **allow list** (an
  unknown structure ⇒ refused), CTEs are inlined as DataFusion does (definition keys through
  `IdentNormalizer::normalize`, lookup keys through
  `object_name_to_table_reference(…).to_string()` — every way of writing a name resolves exactly as
  the planner does), then the limits apply **after inlining**: blocks ≤ 64, relations ≤ 64,
  expression nodes ≤ `MAX_SQL_EXPRESSIONS` 20 000, the width of each schema ≤ `MAX_SQL_WIDTH` 256
  columns (a base table counts 48), total cells (the accumulated width of each join + `VALUES`
  cells + projected columns) ≤ `MAX_SQL_CELLS` 20 000, the grouping sets of `GROUP BY` (`CUBE` 2^n,
  `ROLLUP` n+1, `GROUPING SETS` n, multiplied together) ≤ `MAX_SQL_GROUPING_SETS` 256 — counted
  **before** DataFusion expands them (it allocates 2^n before its own limit of 4 096). The referee:
  `adversarial_queries_plan_fast` (queries generated from the grammar; every query passing
  `check_sql` must plan in ≤ 1 s in release, RSS ≤ 2 GiB). The query runtime runs 64 MiB threads
  (`QUERY_STACK`).
  `GET /archive/{id}` (≤ 31 days, one GET per day) and `GET /audit` (objects read) charge
  `STORE_GET_UNITS` per GET to the tenant's bucket; exhausted ⇒ `429` before reading — `sys_audit`
  goes through `/audit`, so it uses the same bucket. `GET /schedules` (and `sys_schedule`) charges
  `2 × 256` units per `ScheduleDue` index chunk before reading, at most 8 chunks per range per
  request; over the limit or the bucket running out midway ⇒ the range goes into
  `truncatedRanges`, `partial: true`.
  Tests: `cairn-query/tests/sql.rs::{long_operator_chain_is_refused, deep_relation_tree_is_refused,
  cte_fanout_is_refused, cte_inlining_is_counted, shapes_the_token_counter_missed_are_refused,
  adversarial_queries_plan_fast}` (child process), `schedules.rs::{schedule_list_stops_at_the_page_cap,
  schedule_list_spends_the_read_budget}`,
  `query_api.rs::oversized_sql_is_400`, `archive.rs::missing_id_costs_bounded_store_reads`.
  Since query isolation, the node only runs layer (1) before the permit; layer (2) runs with the
  query (the query runtime or the child process, Q-21) — the ingress thread does not recursively
  parse the asker's query. `SELECT * REPLACE (…)` (expressions not passing through the count) and
  `BY NAME` set operations (width beyond the left arm) are refused.
  Additional test: `sql.rs::replace_and_by_name_are_refused`.
- **Q-21. Queries written by the asker run in a child process, not in the node.** The shape check
  (Q-20) is a first filter, not a boundary: DataFusion's planning, optimisation, constant folding
  and scalar functions allocate outside the pool and run synchronous CPU that the deadline does
  not cut. So `POST /query` (mode `process`, the default when the `cairn-query-worker` binary is
  present, mandatory with `--profile saas`) hands the query to a child: the child runs
  `run_with_params` in full, treating the node as a remote node for every range (`ScanRequest`
  over a stdin/stdout pipe), and has no range handles, cluster secret, environment variables or
  asker identity. The node (`query/isolate.rs`) treats the child's messages as **untrusted**: it
  sets the tenant, range and read budget of every scan itself (ignoring the fields the child
  sends), counts read units into the tenant's bucket itself, only calls declared view-table paths
  (`view::is_view_path`) with the asker's `Credentials`, frames are length-prefixed (requests
  ≤ 1 MiB, answers ≤ 17 MiB, checked before allocating — the child refuses answers > 16 MiB),
  answers are written to the HTTP body as slices of the frame; the read budget is **reserved** for
  every scan in flight (scans without room queue), with a limit of 128 MiB of scan answers per
  query; answers are checked against the request (rows ≤ the limit, warnings ≤ 4 × the number of
  ranges + 8, within the query's ranges); the child is spawned with its own session,
  `RLIMIT_AS/FSIZE/NOFILE/CORE`, `PDEATHSIG` compared against the node pid captured before `fork`
  (the node may be PID 1), on Linux also non-dumpable and seccomp after initialisation; 1 024
  requests per query, 16 being served, the `MAX_PRED_ENTRIES` filter; any violation ⇒ the child is
  killed, `422`. The child's path chooser uses the class counts of the local ranges that the node
  sends along (`invocation::class_counts`), so the child picks the same path and reads the same
  number of units as an in-process query (`sql.rs::worker_ctx_reads_like_the_coordinator`). The
  child counts allocations (its own allocator, only in the child binary); exceeding
  `--query-worker-memory-mb` (512; `--profile saas` 256) ⇒ exit code 86 ⇒ `422` naming the limit;
  query deadline + 1 s exceeded ⇒ the node kills the child ⇒ `422`; a child dying for any reason
  ⇒ `422` naming the signal. Each child runs one query and is then killed; a pool of 4 warm
  children (`MAX_CONCURRENT`), dead children are replaced. Fixed-shape queries built by the server
  (`GET /invocations`, tenant usage counting) and the deterministic simulation (`query_threads = 0`)
  run in process. `NodeConfig::query_worker` `None` (the library default) = in process;
  `CAIRNFLOW_TEST_QUERY=process` runs cluster tests in child mode.
  Tests: `cairn-server/tests/query_isolation.rs::{adversarial_queries_do_not_kill_the_node,
  worker_death_is_reported_and_replaced, isolation_flag_is_honoured}` (real binary),
  `query/isolate.rs::tests` (hostile children over an in-memory pipe), `query_api.rs` (the whole
  suite in child mode), `view.rs::only_view_paths_are_accepted`, `worker::tests::sandbox_blocks_escapes`
  (Linux), `query_isolation.rs::{node_as_pid_1_answers_queries, workers_are_sandboxed}` (Linux),
  `profile_saas.rs::{refuses_to_start_without_its_conditions, running_node_has_every_safe_default}`,
  `main.rs::{query_isolation_flags, saas_profile_caps_query_workers_at_256_mib}`,
  `profile_saas.rs::saas_node_environ_is_not_readable_by_its_user` (Linux: `--profile saas` makes
  the node non-dumpable).

## 2. Files to read, in order

1. `crates/cairn-store/src/query_index.rs` — metadata, `Class`, `Placement`, the descriptor
   table, `event_ops`, `decode_row`, `scan_prefix`/`time_bounds`, marks, `QueryCounts`;
   `artifact_bytes.rs` — `ArtifactBytes` (Q-19).
2. `crates/cairn-engine/src/engine/meta.rs` — `Was`, `stage_status`, `Woken`, `meta_of`.
3. `crates/cairn-engine/src/engine/query_build.rs` — descriptor state, `query_build_step`,
   counters, `read_handle`; `engine/artifact_bytes.rs` — the write and build paths of
   `ArtifactBytes`.
4. `crates/cairn-store/src/range_read.rs` — the read contract.
5. `crates/cairn-query/src/{filter,invocation,counts,keyed,remote,session}.rs`.
6. `crates/cairn-query/src/worker.rs` (wire, allocator, child side) and
   `crates/cairn-server/src/query/isolate.rs` (child pool, parent side) — Q-21.
7. `crates/cairn-server/src/query/{ranges,http}.rs`, `actor/leadership.rs` (registering/removing
   handles), `actor/background.rs::query_build_loop`, `ctl.rs::query_index_findings`.

## 3. Impact matrix

| Change | Run |
|---|---|
| Add an index | one `QUERY_INDEXES` row (a new code in `QUERY_CODES`, never reused) + `version = 1`; the `streams()` branch of `invocation.rs` if the path chooser uses it; `query_meta.rs`, `sql.rs`, `ctl` |
| Change a descriptor's layout | bump `version`; `query_meta.rs::dropped_index_rows_are_swept` |
| Add a UI question | a test in `sql.rs` asserting the path and the read cost |
| A new state-write site | goes through `stage_status`, declares `Was`; `query_meta.rs` + the `turn/` row of AGENTS §2 |
| A new write/delete path of `Artifacts` rows, or a new metadata delete | emits `ArtifactBytes` deltas (table in the header doc of `engine/artifact_bytes.rs`); `query_meta.rs::artifact_bytes_match_scan`, `range_model.rs` (Q-19) |
| Add a keyed table | a `Spec` + a read function in `keyed.rs`, added to `keyed::TABLES` (registration and remote scans follow automatically); a row in `bulk_table_without_key_is_refused` |
| Add a view table | a `ViewSpec` in `view.rs` + `VIEWS`; keys that only exist when non-empty go into `OPTIONAL` of `views_match_rest`; a new path parameter needs a sample value in `views_match_rest`; a new endpoint needs a `QUERY_ENDPOINTS` row |
| Add a `sys_invocation` column | `invocation::schema` (append at the end, never change/remove); `to_batch` |
| Add a child → parent request kind, or a new field in `ScanRequest` | the parent side sets every field carrying authority itself (tenant, range, budget); a size limit; a hostile-child case in `isolate.rs::tests` (Q-21) |
| Upgrade DataFusion | the whole `cairn-query` crate (`plan_shape_*`, `query_matches_oracle`, `sql_surface_is_locked`) |

## 4. Review checklist

- A new `invocation_status` write outside `stage_status`/`Woken`? (Q-1, Q-2)
- A new filter form marked `Exact` without a condition in `Row::admits`? (Q-7)
- A leaf declaring an order the path does not guarantee, or forgetting to sort before returning?
  (Q-8)
- A scan that does not start with the tenant prefix? (Q-9)
- A query-layer read going through the engine or the `Memory` level? (Q-10)
- A new field in `ScanRequest`/`ScanResponse` that an older node does not understand? Add the
  field with `#[serde(default)]`; a change of meaning bumps `WIRE_VERSION`. (Q-12)
- A descriptor changing layout without bumping `version`?
- The node trusting a field the child sends (tenant, range, budget, path, size)? An asker's query
  running in process on a new path (`Author::Server` for queries not built by the server)? (Q-21)

## 5. Common failure modes

- **`/query` queries running in process unintentionally**: the `cairn-query-worker` binary is not
  next to `cairnflow` (built with only `--bin cairnflow`) ⇒ `auto` mode falls back to in process,
  and the `query:` startup line says so. Package both binaries (Dockerfile, `release.yml`,
  `check.sh`).

- **`LIMIT` disappears**: the leaf declares `supports_limit_pushdown = true` — that flag means the
  limit passes *through* the node. Leave the leaf at the default; `LimitPushdown` calls
  `with_fetch`.
- **Every range is `indexing` right after a commit**: the `Remote` level may lag the commit by one
  tick; tests must wait until the mark is visible at that level (`sql.rs::fixture`).
- **The lifecycle core complains `LiveButUnqueued` in tests**: the `lifecycle-check` feature is on
  when the whole workspace builds together; test invocations must have a queue entry.
- **`createdAt` differs from entry 0's `create_ts` by a few ms**: by design — the source is the
  metadata's `created_ms` (the engine's clock).
