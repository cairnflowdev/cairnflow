// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

/**
 * Chaos — the acceptance test of durability under failure:
 *
 * > Random kills for 1 hour under load, without losing any acked invocation.
 *
 * Not a test file: run by hand, because it needs a real store (two processes
 * cannot share a `memory://`) and its duration is up to whoever runs it.
 *
 * ```sh
 * # Local SeaweedFS (scripts/dev-store/up.sh already run) — the default when
 * # AWS_ENDPOINT is not set:
 * CHAOS_STORE_URI=s3://walbench/chaos-$RANDOM CHAOS_DURATION_S=3600 \
 *   node harness/chaos/chaos.ts
 * ```
 *
 * `CHAOS_WAL_KIND=shared` runs the same scenario on the shared per-node WAL.
 *
 * **SeaweedFS, not MinIO.** A local MinIO (erasure coding) tears a lease object
 * into two halves when the client is `kill -9`ed in the middle of a CAS — chaos on
 * it measures the store's bugs, not cairn's. Without `AWS_ENDPOINT` this file points
 * at SeaweedFS on `localhost:18333` with the key from
 * `scripts/dev-store/seaweed-s3.json`; when set, it uses exactly what is set.
 *
 * **Range split and merge**: every `CHAOS_SPLIT_EVERY_S` seconds a range is split,
 * and its pair of children is merged back half a period later — interleaved with
 * `kill -9`, so the coordinating node can die at any step of the protocol. Default
 * **0 = off**; the `cluster-4p+split` row of `scripts/check.sh` uses 60. The
 * verdict adds three things: at least one split and one merge completed (otherwise
 * green for the wrong reason), no response outside the contract, and the range
 * table back at rest (covering all of `u64`, no intents, every range with a leader)
 * after the destruction stops.
 *
 * **Failpoints on real processes**: `--failpoints` (or `CHAOS_FAILPOINTS=1`). Every
 * `CHAOS_FAILPOINT_EVERY_S` seconds (default 10) a random `return(...)` point is
 * enabled on a live node for `CHAOS_FAILPOINT_HOLD_S` seconds (default 5) through
 * `POST /admin/failpoints`, then removed. `CHAOS_FAILPOINT_MATCH` (a regex on the
 * point name, e.g. `^(split|merge)::`) only draws among matching points — focusing
 * faults on one subsystem so its rollback path really runs. The binary must be
 * built with the `failpoints` feature — into its own directory so `check.sh`'s
 * binary is not overwritten:
 *
 * ```sh
 * cargo build --bin cairnflow --features failpoints --target-dir target/fp
 * CHAOS_BINARY=target/fp/debug/cairnflow CHAOS_STORE_URI=s3://walbench/fp-$RANDOM \
 *   CHAOS_DURATION_S=600 CHAOS_SPLIT_EVERY_S=60 node harness/chaos/chaos.ts --failpoints
 * ```
 *
 * **Auto split**: `--auto-split` (or `CHAOS_AUTO_SPLIT=1`) replaces `RangeChurner`
 * with the cluster itself: nodes run `--auto-split on` with thresholds lowered for
 * chaos's load (`CHAOS_AUTO_HOT_SHARE`, default 0.005 of the node's turn slots;
 * `CHAOS_AUTO_HOT_SECS` 5 windows; cooldown `CHAOS_AUTO_COOLDOWN_S` 60; each node
 * once every `CHAOS_AUTO_NODE_INTERVAL_S` 30). The load runs `CHAOS_CONCURRENCY`
 * (default 8) parallel call loops, each turn holding its slot for `CHAOS_RUN_MS`
 * (default 50) inside `ctx.run` — enough to make a range really hot. The verdict
 * adds: at least one auto split, and the table back at rest. Once the table reaches
 * `CHAOS_AUTO_MAX_RANGES` ranges (default 160; 0 = no cap), nodes respawn with
 * `--auto-split suggest`: with thresholds this low nothing makes splitting stop (there is
 * no automatic merge), and a 24 h soak grew from 8 to 528 ranges on one 12-core box —
 * measuring that box's saturation rather than endurance.
 *
 * ```sh
 * CHAOS_STORE_URI=s3://walbench/auto-$RANDOM CHAOS_DURATION_S=1200 \
 *   node harness/chaos/chaos.ts --auto-split
 * ```
 *
 * `CHAOS_TREE_WIDTH=W` (`CHAOS_TREE_DEPTH`, default 2): each `work` adds an unkeyed
 * `ctx.call` tree of width W and depth D — on a new bucket the `calls/prefix` gate
 * is on, so the tree is colocated on `work`'s range; the verdict checks all W^D leaves.
 *
 * `CHAOS_DISK_CACHE=<dir>`: each node runs `--disk-cache-dir <dir>/node-<i>`
 * (capped at `CHAOS_DISK_CACHE_BYTES` bytes per node if set, otherwise the binary's
 * default) — the directory follows the node **slot**, so a revive after `kill -9`
 * reopens exactly that slot's cache (leader changes under chaos). The summary
 * prints the hit rates of the RAM block cache and of the disk cache (summed per
 * process, a lower bound); without this flag only the RAM line.
 *
 * `CHAOS_SAGA=1`: each `work` adds a saga — 3 `ChaosEcho` virtual objects with
 * different keys called in sequence, each step taking the previous step's result.
 * `CHAOS_FANOUT=N`: each `work` adds a fan-out of N **keyed** children
 * (`Promise.all`) — the children spread across ranges, so every call goes through
 * the per-destination drainer. The verdict checks the saga yields exactly 3 and
 * the fan-out all N results.
 *
 * `CHAOS_AGENT=1`: each `work` writes `ctx.log` and a span around `ctx.run
 * "stamp"`, then emits an artifact of `CHAOS_AGENT_ARTIFACT_KB` KiB (256) with
 * `ctx.artifact.put`. The verdict also checks: the artifact reads back byte for
 * byte, and `$log` has the `stamp` line of the winning run.
 *
 * An injected fault can make a node stop **on its own** (fail-stop is designed
 * behaviour, e.g. the shared WAL failing a PUT three times): the harness revives it
 * as after a kill. `CHAOS_KILL_EVERY_S=0` disables kills to measure failpoints only.
 *
 * `CHAOS_SDK_RESTART_EVERY_S=N`: every N seconds the SDK endpoint is closed and
 * reopened on the **same port** — the SDK replica dies, the replay cache (wire v5)
 * is lost; the next delta turn must miss (`409`) and go again in full. Run together with
 * `CAIRNFLOW_E2E_SDK_MODE=REQUEST_RESPONSE`.
 *
 * ## Automatic mechanisms switched on together
 *
 * - `--gc` (`CHAOS_GC=1`): nodes run the D-d round (`--gc-retired-dbs`) every
 *   `CHAOS_GC_EVERY_S` (60) seconds, `min_age` shortened to `CHAOS_GC_MIN_AGE_S` (120).
 *   Counts deleted Dbs through node logs and `cairn-ctl ranges retired`.
 * - `--slow-node`: every `CHAOS_SLOW_EVERY_S` (180) seconds, a random live node has
 *   every PUT delayed by `CHAOS_SLOW_PUT` (`1400` ms; the form `<ms>[:<path
 *   substring>]`) for `CHAOS_SLOW_HOLD_S` (90) seconds, through the
 *   `node::put_delay` failpoint of the `failpoints` binary. Only a delay above 1 s
 *   becomes a "timeout" of the slowness detector (`slow_put`), and a session renewal
 *   slower than ⅓ TTL loses the lease before the node can hand over by itself — so
 *   this flag raises the default lease TTL to 6 000 ms (set `CHAOS_LEASE_TTL_MS` to
 *   use exactly that number).
 * - `--tenants N --tenant-rates`: N tenants (≥ 2), each with one API key and one
 *   deployment; tenant 0 is **noisy** (`CHAOS_NOISY_CONC` 16 back-to-back closed
 *   call loops), the other tenants are **quiet** (one call every
 *   `CHAOS_QUIET_EVERY_MS` 1 000 ms). `--tenant-rates` writes `rate` into
 *   `rules/2`: the noisy tenant `CHAOS_NOISY_RATE` (40) CU/s, quiet tenants
 *   `CHAOS_QUIET_RATE` (1 000) CU/s. With tenants the cluster runs with auth (a
 *   random bootstrap key).
 * - `--l0-pressure`: every `CHAOS_L0_EVERY_S` (240) seconds, the compactor of every
 *   live node is slowed by `CHAOS_L0_DELAY` (2 000) ms per GET/PUT for `CHAOS_L0_HOLD_S`
 *   (90) seconds, through the `compactor::io_delay` failpoint. Prints the largest L0
 *   view count seen and the number of `429 range_overloaded`.
 * - `--s3-profile <file>`: nodes carry a simulated S3 store profile
 *   (`CAIRNFLOW_STORE_PROFILE`); the binary must be built with the `testkit` feature.
 *   `bench/s3-model/profiles/s3-standard-faults.json` adds error rates to every
 *   write, including **ambiguous** errors (written, then an error is reported, or
 *   written and then the answer of object_store's own retry received) for the
 *   objects cairn manages itself.
 * - The node's state scrubber runs every `CHAOS_SCRUB_EVERY_S` (60; `0` = off): at
 *   the end of the run it prints the number of rounds, the largest number of
 *   findings still present per check and their log lines verbatim. **A signal only**,
 *   not an invariant — the arbiter is still the `verify` of invariant 7.
 * - `--profile all`: every flag above (4 tenants, the profile
 *   `bench/s3-model/profiles/s3-standard.json`), plus `--auto-split` and
 *   `--failpoints`. Needs a `--features failpoints,testkit` binary:
 *
 * ```sh
 * cargo build --release -p cairn-server --bin cairnflow --bin cairn-query-worker --bin cairn-ctl \
 *   --features failpoints,testkit --target-dir target/chaos
 * CHAOS_BINARY=target/chaos/release/cairnflow CHAOS_STORE_URI=s3://walbench/all-$RANDOM \
 *   CHAOS_DURATION_S=1200 node harness/chaos/chaos.ts --profile all
 * ```
 *
 * ## Tenant states, SSO, a long store outage
 *
 * Three separate flags, **not** part of `--profile all`: `--store-outage` stops a
 * Docker container (it must be named explicitly, so it cannot be a profile's
 * default); `--tenant-states` and `--sso` join `--profile all` only after their
 * first green run on a real cluster, not before.
 *
 * - `--tenant-states` (`CHAOS_TENANT_STATES=1`; needs `--tenants N`, N ≥ 2): every
 *   `CHAOS_TENANT_STATE_EVERY_S` (60) seconds picks a **quiet** tenant (never
 *   tenant 0, the noisy one) and moves it `active → limited → suspended →
 *   active` through `PUT /admin/tenants/{t}/state` with the bootstrap key, holding
 *   each state for `CHAOS_TENANT_STATE_HOLD_S` (15) seconds. `202 {pending}` (some
 *   node has not applied it yet) ⇒ resend the same command until `204`, capped;
 *   `409 tenant_state_disabled` ⇒ `cairn-ctl cluster enable-tenant-state --apply`
 *   once (a new bucket has the gate on already). Calls refused with `429
 *   tenant_limited` / `403 tenant_suspended` were never acked ⇒ **not** lost and not
 *   a contract violation — counted by state and code, printed at the end. Extra
 *   invariants: (a) zero loss still holds — every acked call of a tenant whose state
 *   changed finishes after it is `active` again; (b) while `suspended` (from the `204`
 *   plus the grace `CHAOS_TENANT_STATE_GRACE_MS`, 3 000 ms, for turns already granted
 *   a slot before) the engine calls none of that tenant's handlers (`work`,
 *   `ChaosEcho.ping`, `ChaosTree.grow` — the SDK records each entry) and none of its
 *   invocations has an `endedAt` inside that window; (c) invariant 6 holds as is,
 *   dropping the samples of a state-changed tenant overlapping its non-`active`
 *   window. A refusal on a tenant whose state did **not** change, or a refusal of a
 *   send after `active` answered `204` (plus the grace) ⇒ a violation.
 * - `--sso` (`CHAOS_SSO=1`): the fake OIDC issuer `ui/e2e/fixture/oidc.ts` runs in the
 *   harness process; the cluster receives `PUT /admin/oidc` and a client for the
 *   node's hostname (`127.0.0.1`) → tenant `chaos-sso`. Every `CHAOS_SSO_EVERY_S`
 *   (30) seconds, on a random live node: `GET /ui/login/oidc` → the issuer's
 *   `authorize` → the callback with the `__Host-cairn_oidc` cookie → there must be a
 *   `__Host-cairn_ui` cookie → `GET /auth/whoami` with that cookie ⇒ `via: "sso"`, the
 *   right tenant. This cadence is far below the login rate limits (`/login/oidc` 20
 *   per IP then 1/3 s, failed callbacks 10 per IP then 1/6 s, code exchange 20 then
 *   1/s per node). Failures because the node was just killed / is being rebuilt
 *   (lost connection, `503`, `sso_error=unavailable` — the registry not fresh yet)
 *   or during a store outage are **excused** and counted; every other failure is a
 *   violation. With `--sso` the cluster runs with auth even without `--tenants`.
 * - `--store-outage` (`CHAOS_STORE_OUTAGE=1`; `CHAOS_OUTAGE_CONTAINER` is
 *   **required**, no default — so a shared container is never stopped by mistake):
 *   once per run, at second `CHAOS_OUTAGE_AT_S` (⅓ of the duration), `docker stop`
 *   that container, wait `CHAOS_OUTAGE_S` (2 × the lease TTL: chaos sets 1 500 ms ⇒
 *   3 s; `--slow-node` 6 000 ms ⇒ 12 s; no lease flag ⇒ the binary's 10 s ⇒ 20 s), then
 *   `docker start` and wait for the S3 port to answer HTTP again. Before running, the
 *   harness checks that the container is running, is not `--rm` (stop would destroy
 *   it), and publishes exactly the port of `AWS_ENDPOINT` — otherwise it refuses. A
 *   node failing stop or losing its lease is by design; the revive loop rebuilds
 *   them (exponential backoff, capped at `CHAOS_RESPAWN_GIVEUP_S` 900 s). Random
 *   kills and coordinator kills pause from the stop until the cluster accepts work
 *   again. Calls failing in that time were not acked ⇒ not lost; invariant 6 drops
 *   samples overlapping that window. Invariants: zero loss, a clean `verify`, the
 *   table back at rest, every node alive again, and the cluster acking work again
 *   within `CHAOS_OUTAGE_RECOVER_S` (300) seconds after the store answers. Prints the
 *   stop/start marks and the seconds to the first ack. `CHAOS_S3_ENDPOINT` (default
 *   `http://localhost:18333`) changes the default SeaweedFS endpoint while keeping
 *   the key of `seaweed-s3.json` — to point at a dedicated container for this run.
 *   Ctrl-C while the container is stopped ⇒ the harness runs `docker start` before exiting.
 *
 * ```sh
 * CHAOS_BINARY=target/chaos/release/cairnflow CHAOS_STORE_URI=s3://walbench/s11-$RANDOM \
 *   CHAOS_DURATION_S=600 node harness/chaos/chaos.ts --tenants 4 --tenant-states --sso
 * CHAOS_S3_ENDPOINT=http://localhost:18433 CHAOS_OUTAGE_CONTAINER=cairn-chaos-outage \
 *   CHAOS_STORE_URI=s3://walbench/outage-$RANDOM CHAOS_DURATION_S=600 CHAOS_OUTAGE_S=30 \
 *   node harness/chaos/chaos.ts --store-outage
 * ```
 *
 * ## Long soak
 *
 * An earlier soak run died in hour 4 without a verdict: the disk filled up, and a
 * node-log `WriteStream` nobody listened to for `'error'` killed the harness with `ENOSPC`.
 *
 * - **Every file write has an error listener** (node logs, `.metrics`, `latency.tsv`,
 *   `stuck-read-*.json`, the pipes and `spawn` of child processes). A failed write
 *   makes that file stop receiving lines and the harness goes on; a disk error
 *   (`ENOSPC`, `EDQUOT`) anywhere, including an uncaught one, **stops the destructive phase cleanly** as below.
 * - **Disk watch:** every `CHAOS_DISK_EVERY_S` (30) seconds measures the free space
 *   (GiB, like `df -h`) of the log directory, the disk cache, a `file://` store and
 *   `CHAOS_DISK_PATHS` (`:`-separated — the volume of the SeaweedFS bucket, of
 *   `run.log`). Below `CHAOS_DISK_MIN_GB` (20; `0` = off) at the start ⇒ does not run
 *   (exit 2); mid-run ⇒ stops the destructive phase: load, kills, failpoints, … stop
 *   at the next tick; draining, the lost/wrong verdict and the final `verify` still
 *   run as in a run that ran out of time. The verdict adds `STOPPED` (a run short of
 *   its duration is never green) and a line `⛔ STOPPED: disk …`; the exit code is
 *   **3** when that is the only failure. The `billing` workload gets its end mark at
 *   the start: an early stop blocks every destructive loop, but the billing load runs until its own time.
 * - **Node log rotation:** over `CHAOS_LOG_MAX_MB` (256; `0` = no rotation)
 *   `node-0.log` → `node-0.log.1` → … → `.CHAOS_LOG_KEEP` (3), the oldest file is overwritten.
 * - **Periodic summary:** every `CHAOS_SUMMARY_EVERY_S` (3 600; `0` = off) and when
 *   the destructive phase ends, a `SUMMARY` line (acks, kills, failpoints, range count,
 *   violations per invariant so far, p50/p99 per tenant class of the window, free disk,
 *   RSS of the nodes and of the harness, log bytes) plus a `SUMMARY_JSON` line.
 * - **`--rr-tenant i`** (`CHAOS_RR_TENANT`; needs `--tenants N`, 1 ≤ i < N): tenant
 *   `chaos-t<i>` registers a second endpoint with the same services, declaring
 *   `REQUEST_RESPONSE` — the shape of a hosted deployment, where the SaaS profile
 *   registers `https://` endpoints as RR. That tenant runs at the quiet-tenant cadence
 *   and changes state like them (`--tenant-states`), every lost/wrong verdict holds;
 *   its latency is printed separately (class `rr`), outside invariant 6 — RR goes back
 *   and forth one request per suspension point, not comparable with the BIDI baseline.
 *   `CHAOS_SDK_RESTART_EVERY_S` reopens both endpoints.
 *
 * A 24 h soak: `CHAOS_DURATION_S=86400` with the standard soak configuration plus `--rr-tenant 3`.
 *
 * ## Quiet phase
 *
 * `CHAOS_IDLE_TAIL_S=N` (default 0 = none): after the destructive phase, the
 * lost/wrong verdict and the split/merge rest check, the cluster runs N more seconds
 * **quietly** — alive, no load, no kills, no failpoints (every point ever enabled is
 * disabled again on every node). The start mark is set after a grace of
 * `CHAOS_IDLE_GRACE_S` (60) seconds: the tail of the last kill/failpoint does not count
 * (a fail-stop during the grace is only printed). The final `verify` runs **after**
 * this phase (it opens every Db; its requests do not mix into the quiet phase's
 * counts). The harvester asks every live node's `/metrics` every `CHAOS_HARVEST_MS`;
 * the phase's numbers are the difference of the harvest ledger at the start and the
 * end of the phase. Every node's verbatim `/metrics` at both marks is copied to
 * `idle-{start,end}-<node>.metrics` in `CHAOS_LOG_DIR`. Measuring requests by caller
 * needs `cairnflow_testkit_store_requests_by_source_total`: with a quiet phase and no
 * `--s3-profile`, nodes run the `count-only.json` profile (count only, no delay, no
 * errors) — the binary must be built with the `testkit` feature. Verdicts (✅/❌, ❌
 * goes into the failure list):
 *
 * - **depth**: every range in the table at the end of the phase stands on ≤
 *   `CHAOS_IDLE_MAX_DEPTH` (2) ancestors. Two sources: the gauge
 *   `cairnflow_range_external_depth{range}` (printed by the leader, from the writer's
 *   **in-RAM** manifest — it lags behind SlateDB GC's detach until the 1 h manifest
 *   poll or a reopen, so it can only be higher than the truth) and the bucket
 *   (`cairn-ctl verify --no-open`: the total `external_dbs` entries of every live
 *   range, run after the last harvest). ✅ when the gauge of every range ≤ the cap, or the bucket total ≤ the cap.
 * - **scrubber**: the phase's requests from source `scrub`, per hour, ≤ the budget
 *   (`CHAOS_SCRUB_REQUESTS_PER_HOUR` — when set it is passed as
 *   `--scrub-requests-per-hour` to the nodes; otherwise the node's default,
 *   1 000 × 3 600 / `--scrub-every-s`, i.e. 60 000/h at chaos's 60 s cadence) plus one
 *   round at the window's edge (`cairnflow_scrub_round_requests` × 3 600 / window). Also
 *   prints the number of rounds, the current wait, the age of the latest complete round. No round ran ⇒ ❌.
 * - **split/merge**: a `500` whose detail the node hides (`code <code>`) is looked up
 *   in the node log; a detail that is an injected fault (`failpoint <name>: chaos`) is a
 *   deferral, not a response outside the contract (applies to every run with
 *   `--failpoints`, not just the quiet phase).
 * - **D-d within budget** (`--gc` only): requests from source `gc.dbs` per hour ≤
 *   `CHAOS_IDLE_DD_PER_HOUR` (default 1 000 × 3 600 / `--gc-every-s` — D-d has no
 *   request-budget flag, so it takes the same baseline as the scrubber at the same cadence).
 * - **D-d independent of split history** (`--gc` only): D-d's requests into `owners/`
 *   per round < ½ the number of tombstones (`cairnflow_owner_tombstones`). Equal or more ⇒
 *   the cost per round grows with the number of ranges that ever existed, with no cap ⇒ ❌.
 * - **fail-stop**: no dropped incarnation (log or `cairnflow_failstop_total`) and no
 *   node exiting on its own during the quiet phase. Also counts **machine stalls**
 *   (the harness's 1 s tick late by > 500 ms, each printed with the load average):
 *   nodes and harness share the machine, so CPU contention delays every process at
 *   once and chaos's 1 500 ms lease expires even though the store is not slow — the
 *   row is still ❌, the stall lines are the evidence for reading it. Prints
 *   `cairnflow_failstop_total` by `cause` for the whole run and for the phase, along with
 *   `cairnflow_lineage_compactions_total`.
 *
 * The phase ends by printing an `IDLE_JSON` line. The command (SeaweedFS, disk cache):
 *
 * ```sh
 * CHAOS_STORE_URI=s3://walbench/split-$RANDOM CHAOS_DURATION_S=3600 CHAOS_SPLIT_EVERY_S=60 \
 *   CHAOS_IDLE_TAIL_S=3600 CHAOS_DISK_CACHE=/path/to/cache CHAOS_BINARY=… \
 *   node harness/chaos/chaos.ts --failpoints --gc
 * ```
 *
 * ## Measuring read cost
 *
 * When nodes carry a store profile (`--s3-profile`, or `--cost`) the harness **scrapes
 * every node's `/metrics`** every `CHAOS_SCRAPE_EVERY_S` (5) seconds and **right before
 * every `kill -9`**, keeps the last scrape of each process, and at the end of the load
 * sums `cairnflow_testkit_store_requests_total` by `op` × `class` over every process
 * that ever lived. The counters live in the process and die with `kill -9`; scraping
 * before killing is what preserves them. A node exiting on its own (fail-stop) loses at
 * most one scrape tick. Prints a table of requests by source: total, per second, per
 * acked call, per step (estimated: [`STEPS_PER_WORK`] durable SDK commands per `work`),
 * and a `COST_JSON` line for recording the result.
 *
 * `--cost` (`CHAOS_COST=1`) is the **standard cost run**: nodes run with **the
 * binary's default flags** (no chaos `--tick-interval-ms`, `--reader-poll-ms`,
 * `--deploy-refresh-ms`, `--lease-ttl-ms` — those flags make the cluster fast so chaos
 * finds bugs, and multiply GETs over time), the profile
 * `bench/s3-model/profiles/count-only.json` (count only), a kill every 30 s. Needs a
 * binary `--features testkit`:
 *
 * ```sh
 * cargo build -p cairn-server --bin cairnflow --bin cairn-ctl --features testkit --target-dir target/tk
 * CHAOS_BINARY=target/tk/debug/cairnflow CHAOS_STORE_URI=s3://walbench/cost-$RANDOM \
 *   node harness/chaos/chaos.ts --cost
 * ```
 *
 * `cairn-ctl` is taken from next to the binary (`CHAOS_CTL` overrides). Without it the
 * two bucket-based invariants (GC, WAL floor) are skipped — unless `--gc`/`--profile all`
 * is on, in which case it stops loudly. `CHAOS_LOG_DIR` writes each node's log to a file.
 *
 * ## Counts
 *
 * `/metrics` counters live per process, and `kill -9` erases them. The harvester asks
 * every live node's `/metrics` every `CHAOS_HARVEST_MS` (2 000) ms and sums the
 * increase of each line per **process** (pid); a line dropping lower (a range engine
 * rebuilt) counts as starting again from 0. The increase after the last ask of a
 * killed process is lost — the printed number is a **lower bound**. It also counts the
 * meaningful log lines (intent rollback, tombstone on behalf, self-shedding, …), and
 * with `--s3-profile` the store requests by kind per million steps, to compare with
 * the cost model. **Steps** are estimated as `4 × finished invocations`: `work` records
 * `run`, `call`, `output`, and `ChaosEcho.ping` records `output` (the same way metering counts).
 *
 * ## Invariants
 *
 * Three original invariants, in the exact words of the acceptance criterion:
 *
 * 1. **Every acked invocation** (an id received from ingress, including a `429` with
 *    an id) **must finish** — no invocation vanishes or hangs forever after any
 *    sequence of kills.
 * 2. Its result must have **the right content** (the output matches the seq sent).
 * 3. The side effect (`ctx.run`) runs **at least once** for each completed
 *    invocation. It does not claim "exactly once": a `kill -9` between the SDK running
 *    the closure and the result becoming durable is a window that every
 *    durable-execution engine reruns — the promise is *a durable result never runs
 *    again*, not that the closure runs only once in every universe.
 *
 * Five more invariants, checked periodically and at the end of the run. Each has a
 * way to inject a fault (`CHAOS_INJECT=<name>`): an injected run is **green if and
 * only if** exactly that invariant fails.
 *
 * 4. **Never two leaders for one range** (every `CHAOS_LEADER_PROBE_MS`, 1 000). For
 *    each range, ask `POST /invocations/<id>/cancel` — an id whose key is the start
 *    of the range, with a tail that does not exist — on **every** live node at once.
 *    Only the leader holding a valid lease answers `404` (its engine answers "no
 *    such thing" and writes nothing); other nodes answer `503` (a node that lost its
 *    lease cannot answer). Two `404`s for one range ⇒ immediately ask those nodes
 *    twice more; still two ⇒ a violation. `GET /invocations/<id>` is not used: on a
 *    non-leading node it takes the **follower read** path and opens a reader for the
 *    range — the prober once opened readers for every range on every node, ~30% of
 *    the whole run's GETs. `CHAOS_PROBE=status` keeps the old path, to measure that part.
 *    Injection: `rogue-leader` — a node of **another cluster** (its own bucket) is
 *    added to the probed set; it leads every key, so every range has two leaders.
 * 5. **No overlapping split/merge operations** — when the cluster splits by itself
 *    (`--auto-split`, one cluster-wide intent). The watcher asks `/cluster` of every
 *    live node every `CHAOS_OVERLAP_POLL_MS` (250) ms and counts the pending intents
 *    at once. Manual split/merge commands (`CHAOS_SPLIT_EVERY_S`) do not exclude each
 *    other at the intent step (two intents can live together; the first operation's
 *    commit CAS fails and both roll back), so this is not checked in that mode.
 *    Injection: `overlap` — two manual splits on two ranges, the second sent once the
 *    first's intent is in the table, with PUTs under `ranges/` delayed 1 s (failpoint
 *    `node::put_delay`) so the first cannot commit yet.
 * 6. **A noisy tenant does not push a quiet tenant's p99 past the threshold** (over
 *    `CHAOS_QUIET_WINDOW_S` 300 s windows pooled until they hold
 *    `CHAOS_QUIET_MIN_SAMPLES` (1 000) quiet calls — a lone window's "p99" is its
 *    second-slowest call — and over the whole run). Latency = from sending until the invocation is
 *    `completed` (a synchronous wait, then asking again on `202`/`429`). Under
 *    `kill -9`, failpoints, slow nodes and S3 profiles, **every** tenant's p99 is
 *    already in the tens of seconds (leader handover time, retry backoff), so a plain
 *    absolute threshold measures nothing. The threshold is `max(CHAOS_QUIET_P99_MS
 *    (8 000), CHAOS_QUIET_VS_BASE (1.5) × the p99 of the baseline load)` — the baseline
 *    load (the default tenant, no `rate`) suffers the same chaos in the same window and
 *    is measured the same way. Not caught: a noisy tenant slowing **every** tenant
 *    evenly (that needs a quiet phase as a baseline). Injection: `quiet-throttle` — a
 *    `rate` cap of 1 CU/s set by mistake on a quiet tenant.
 * 7. **GC does not delete a Db still referenced** — `cairn-ctl verify` is the arbiter:
 *    every `CHAOS_VERIFY_EVERY_S` (300) runs `verify --no-open`; a `db` or
 *    `external_db` finding is rerun after 5 s (the table changed midway), still there
 *    ⇒ a violation. At the end of the run, once the table is at rest, a full `verify`
 *    (opening every Db): every finding is a violation, including `orphan_db` (capped at
 *    `CHAOS_FINAL_VERIFY_S`, 1 800 s). Also: a `ranges/<id>` directory of a retired
 *    range whose manifest is gone (an unfinished GC) is an **orphan object**.
 *    Injection: `gc-skip-refs` — failpoints `gc::skip_reference_check` and
 *    `gc::skip_checkpoint_check` on every node, so GC skips both the reference gate
 *    and the named-checkpoint fence.
 * 8. **The shared WAL floor never stands still longer than `CHAOS_WAL_FLOOR_STUCK_S`**
 *    (600) — every `CHAOS_FLOOR_EVERY_S` (120) reads `cairn-ctl wal floors`; a log that
 *    still has objects above its floor whose floor has not moved for longer than the
 *    threshold ⇒ a violation (`history.ts::watchFloor`; the clock runs from the latest
 *    floor change or the latest read that saw the log empty). At the end of the run:
 *    the floor must have advanced (or the log been swept) at least once. Injection:
 *    `floor-stuck` — the threshold lowered to 30 s, below the 60 s forced-flush period,
 *    so a naturally pinned floor must be caught. "Log swept" counts logs carrying the
 *    `forgotten` mark (`cairn-ctl wal floors`, field `forgotten`): a dead log that is
 *    swept for good still leaves a seal and the mark, so it no longer vanishes from the list.
 *
 * ## The `billing` workload and the liveness checks
 *
 * `--workload billing` (`CHAOS_WORKLOAD=billing`) replaces the `ChaosService` load with
 * the billing scenario of `workloads/billing.ts`: a hot virtual object, a workflow
 * with a `ctx.run` side effect calling a fake gateway, `ctx.promise` + `ctx.race`,
 * exponential retry with `maxAttempts` → `PAUSE`, `rate` and `concurrency` rules.
 * Kills, range splits (`--auto-split`), failpoints… still come from this harness. Its
 * lost/wrong verdict is the workload's I1–I6 (see the head of that file).
 *
 * ```sh
 * cargo build --release -p cairn-server --bin cairnflow --bin cairn-query-worker --bin cairn-ctl \
 *   --features failpoints,testkit --target-dir target/chaos
 * weed shell: s3.bucket.create -name billing -owner bench   # a separate bucket
 * CHAOS_BINARY=target/chaos/release/cairnflow CHAOS_STORE_URI=s3://billing/run-$RANDOM \
 *   CHAOS_DURATION_S=900 node harness/chaos/chaos.ts --workload billing --auto-split
 * # or a single process on RAM (no kill — see below):
 * CHAOS_BINARY=… CHAOS_STORE_URI=memory:// CHAOS_DURATION_S=900 \
 *   node harness/chaos/chaos.ts --workload billing --auto-split
 * ```
 *
 * **`memory://`** only lives inside one process: a run on it is **one** node, no
 * `kill -9` (killing it loses the whole bucket) and no `cairn-ctl` (it cannot read
 * another process's RAM). Range ownership changes still happen through auto split and
 * `--failpoints`.
 *
 * Four liveness checks, for **every** workload ((b) only when the workload has
 * `maxAttempts`). The lost/wrong verdict treats `paused` as a normal outcome; a whole
 * chain of lifecycle bugs once slipped through exactly there.
 *
 * 9. **(a) Drain** — after the load and the chaos stop (the `billing` workload cancels
 *    every subscription), the cluster has no `running`/`suspended` invocation left
 *    (the cluster-wide list, `GET /invocations?scope=cluster`) within the cap
 *    `CHAOS_DRAIN_S` (600). `paused` is an outcome. With `--tenant-states`, leftovers
 *    **only** of state-changed tenants wait longer, until the last unsuspension +
 *    `CHAOS_SUSPENDED_RETRY_S` (900) + 120 s, reported separately: the row scheduled
 *    at suspension time on a range whose leader changed right as the unsuspension ran
 *    fires after `SUSPENDED_RETRY`.
 * 10. **(b) Real attempts** — an invocation `paused` for exhausting `maxAttempts` (575)
 *    is only valid when the SDK received all `maxAttempts` turns in the final stretch
 *    (it saw every `ctx.attempt` `0 … maxAttempts − 1`); "attempts consumed without the
 *    SDK being called" must be 0. Jumps of `ctx.attempt` on every invocation are only printed
 *    (`history.ts`, `checkAttempts`).
 * 11. **(c) Queues** — scrapes `/scheduler` of every live node every
 *    `CHAOS_SCHED_EVERY_S` (30) seconds; within the `CHAOS_QUEUE_WINDOW_S` (600)
 *    second window after the load stops, the slope of each range's `waiting` must be
 *    ≤ 0 (a range sloping upward whose last sample still has waiting work is a violation).
 * 12. **(d) Deadlock** — no range with `locksHeld > 0`, `turnsInUse == 0`,
 *    `eligible == 0`, `waiting > 0` over `CHAOS_DEADLOCK_SCRAPES` (6) consecutive scrapes
 *    (the whole run, draining included). A scrape where the node's `rate` bucket is in
 *    debt or has waiters (`/scheduler` → `tenants`) does not count: a range standing
 *    still because of a rate cap is exactly the contract, and a real deadlock is still
 *    there once the bucket is positive again.
 *
 * ## Pre-soak fault classes
 *
 * One flag per case; `--pre-soak` (and `--profile all`) turns on all five — the
 * default set of the final pre-soak run. With any case on, the tripwires
 * (`flow::second_live_permit`, `flow::lock_released_by_non_holder`) are set to `panic`
 * on every node, revived nodes included (`CHAOS_TRIPWIRES=0` disables them) — the
 * binary must be built with `failpoints`; a `failpoint <name> panic` line in a node log
 * is a "tripwire" violation, and failing to set any tripwire is "green for the wrong reason".
 *
 * - `--hot-signal` (a): tenant `chaos-hot`, `CHAOS_HOT_KEYS` (2) object
 *   `ChaosHot` `EXCLUSIVE` objects, each key with `CHAOS_HOT_INFLIGHT` (6) one-way
 *   sends in flight. The handler enters the hold span in its first `ctx.run`,
 *   suspends waiting for a `poke` signal (or `CHAOS_HOT_WAIT_MS` 500) while still
 *   holding the lock, and leaves in its last `ctx.run`. Every `CHAOS_HOT_SIGNAL_MS`
 *   (100) ms (per sender among `CHAOS_HOT_SIGNALERS`, 1) a `poke` signal — or a cancel,
 *   at the share `CHAOS_HOT_CANCEL_SHARE` (0.05) — goes to one in-flight send, mostly
 *   **still queued**. Verdict: "no two concurrent SDK turns on one object key"
 *   (counted in the handler — `chaos-cases.ts::ExclusiveWatch`; excusing holders that
 *   were cancelled and the same invocation rerunning a closure), every acked send
 *   finishes with the right output or ends because of the very cancel that was sent.
 * - `--gate-faults` (b): adds `turn::gate_turn` (a failed status read at the turn
 *   gate), `turn::after_running`, `turn::retry_if`, `turn::release_queue_slot`,
 *   `actor::rescue_round::status` to the random failpoint set (on its own: draws only
 *   among them). Verdict: no `running` invocation standing still (no change of
 *   `modifiedAt`) longer than `CHAOS_STUCK_RUNNING_S` (300) seconds **while the cluster
 *   is healthy** — the clock runs from the most recent time the cluster became healthy
 *   again (after a kill, a node exiting on its own, a revive, a failpoint set/cleared,
 *   a slow node, L0 pressure, a store outage, an SDK reopen); the cluster-wide list is
 *   scraped every `CHAOS_STUCK_EVERY_S` (30) until the end of the drain phase, skipping
 *   state-changed tenants and the purge tenant. To give the check a chance to fire
 *   mid-run: **healthy windows** — every `CHAOS_HEALTHY_EVERY_S` (900) seconds, the last
 *   `CHAOS_HEALTHY_S` (360) seconds have no kills, no random failpoints, no slow
 *   node/L0 pressure/SDK reopen (the load keeps running); the number of windows and of
 *   `running` invocations examined in each window go into the summary line.
 * - `--tenant-purge` (c): tenant `chaos-purge`; a first phase creates
 *   `CHAOS_PURGE_INVOCATIONS` (46 000 — a size measured earlier, there is no knob to
 *   lower the batch cap) `ChaosBulk.note` `SHARED` calls on **one** key ⇒ one range,
 *   `CHAOS_PURGE_CONC` (32) in parallel; then `deleting`, nodes run
 *   `--tenant-purge-keep-s CHAOS_PURGE_KEEP_S` (60). Verdict: reaches `purged` within
 *   `CHAOS_PURGE_DEADLINE_S` (1 800) from `deleting` (continuing through the drain
 *   phase), with no held entries or objects left; the final `verify` (row
 *   `tenant-residue`) is the arbiter of leftover rows. Every failed or over-cap poll of
 *   `GET /admin/tenants/{t}/purge` is counted and printed; after
 *   `CHAOS_PURGE_STALE_POLLS` (3) consecutive failures the API answer is stale and the
 *   harness reads the `<prefix>/tenants` book on the bucket directly (aws CLI) — the
 *   book is the arbiter (`chaos-cases.ts::judgePurge`); it never rules "stuck" on a stale answer.
 * - `--backup-restore` (d), together with `--auto-split`: from second
 *   `CHAOS_BACKUP_FIRST_S` (60), every `CHAOS_BACKUP_EVERY_S` (900) one
 *   `cairn-ctl backup --to s3://<source bucket>/backup-<prefix>`, a `restore` into
 *   `s3://CHAOS_RESTORE_BUCKET/<prefix>` (the side bucket `walbench-restore`, created
 *   with the aws CLI — restore demands exactly the source prefix), then
 *   `backup check-restore` and `verify` on the restored copy. A backup refused (the
 *   range table changing across every attempt, code 3) is a valid "reported error"; a
 *   backup that times out (`CHAOS_BACKUP_TIMEOUT_S`), is killed or fails otherwise is a
 *   violation. After either, the partial copies (no `BACKUP`) are removed.
 *   `restore.lost_call`/`restore.lost_reply` are a documented limit (the copy is not
 *   consistent across ranges — runbook `restore-from-backup.md`; `check-restore`
 *   detects, the operator handles): counted, printed every round and in the summary
 *   line, not failing. A complete copy whose restore fails, or with any other finding
 *   no round heals, is a violation (the restored copy is kept for inspection, the loop
 *   stops). A clean round deletes the restored copy and its backup. No clean round ⇒
 *   "green for the wrong reason".
 * - `--par-branches` (e): every `CHAOS_PAR_EVERY_MS` (1 000) one one-way send of
 *   `ChaosPar.fan`: `CHAOS_PAR_BRANCHES` (3) parallel branches × `CHAOS_PAR_STEPS` (4)
 *   steps alternating `ctx.run`/`ctx.call`; a `ctx.run` closure throws an ordinary
 *   error with probability `CHAOS_PAR_FAIL` (0.15) (the turn dies, every branch
 *   replays), one send in four has a `TerminalError` step (the branch catches it).
 *   Verdict: each branch's output equals `chaos-cases.ts::parExpected`.
 */

import { execFile, execFileSync, spawn, type ChildProcess } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import {
  appendFileSync,
  createWriteStream,
  existsSync,
  mkdirSync,
  renameSync,
  statfsSync,
  statSync,
  unlinkSync,
  writeFileSync,
  type WriteStream,
} from "node:fs";
import { loadavg } from "node:os";
import { dirname, join } from "node:path";
import { monitorEventLoopDelay, PerformanceObserver } from "node:perf_hooks";
import { fileURLToPath } from "node:url";
import { type Context, type ProtocolMode, serve, service, type ServiceContext, TerminalError } from "../../sdk/typescript/src/index.ts";
import {
  backupIdsIn,
  type BookRead,
  type CtlRun,
  describeCtlExit,
  ExclusiveWatch,
  judgePurge,
  judgeRestoreCheck,
  maskedErrorOf,
  parDiff,
  parEcho,
  parIsRun,
  parseTenantBook,
  parTerminalAt,
  retryServerErrors,
  type RunningRow,
  StuckWatch,
  survivableUncaught,
} from "./chaos-cases.ts";
import {
  checkAttempts,
  checkHistory,
  classifyPaused,
  type DetailRead,
  deadLocks,
  type FloorTrack,
  type Observation,
  queueGrows,
  queueSlopes,
  HistoryTracker,
  type SchedSample,
  watchFloor,
} from "./history.ts";
import { IDENTITY_MASTER, kidOf } from "./identity.ts";
import { AckLedger, LogHist, SeqTable } from "./ledger.ts";
import { QuietPool } from "./quiet.ts";
import { RangeChurner, type RangeRow, rangesOf, tableProblem } from "../../sdk/typescript/test/ranges.ts";
import { BillingWorkload, type InvokeBody } from "./workloads/billing.ts";

/// The SDK mode declared in the manifest for this run — see `e2e.test.ts`.
const sdkMode: ProtocolMode =
  (process.env.CAIRNFLOW_E2E_SDK_MODE as ProtocolMode | undefined) ?? "BIDI_STREAM";

const here = fileURLToPath(new URL(".", import.meta.url));
const repoRoot = join(here, "..", "..");
const env = process.env;
const argv = process.argv.slice(2);
const has = (f: string) => argv.includes(f);
/** The value of `--flag v` or `--flag=v`. */
function flagValue(f: string): string | undefined {
  const i = argv.indexOf(f);
  if (i >= 0) return argv[i + 1];
  return argv.find((a) => a.startsWith(`${f}=`))?.slice(f.length + 1);
}

/// `CHAOS_PROFILE=release` runs `target/release/cairnflow` (`cargo build --release
/// --bin cairnflow` first) — an acceptance run measures the release build, not the debug build.
const PROFILE = env.CHAOS_PROFILE ?? "debug";
/// `CHAOS_BINARY` overrides the binary path (e.g. a `--features failpoints` build).
const binary = env.CHAOS_BINARY ?? join(repoRoot, "target", PROFILE, "cairnflow");
const CTL = env.CHAOS_CTL ?? join(dirname(binary), "cairn-ctl");

const PROFILE_FLAG = flagValue("--profile");
if (PROFILE_FLAG !== undefined && PROFILE_FLAG !== "all") {
  console.error(`--profile only accepts "all", not ${PROFILE_FLAG}`);
  process.exit(2);
}
/// `--profile all`: every mechanism switched on together.
const ALL = PROFILE_FLAG === "all";

const STORE = env.CHAOS_STORE_URI;
/// `memory://` lives inside one process: one node, no kill, no `cairn-ctl`.
const MEMORY = STORE?.startsWith("memory://") ?? false;
/// `file:///some/dir`: a local directory runs exactly **one** node (an exclusive
/// `flock` lock) — but the bucket lives outside the process, so that very node can be
/// `kill -9`ed and rebuilt: the rebuild wins the lock, lets the old session expire at
/// once and recovers every range from the bucket. The billing row of
/// `scripts/check.sh` runs here (no SeaweedFS needed). `cairn-ctl` can read it.
const LOCAL = STORE?.startsWith("file://") ?? false;
const NODES = MEMORY || LOCAL ? 1 : Number(env.CHAOS_NODES ?? "3");
const PARTITIONS = env.CHAOS_PARTITIONS ?? "8";
const DURATION_S = Number(env.CHAOS_DURATION_S ?? "300");
/// `--cost`: the binary's default lease (10 s) — a kill every 10 s would keep a third of
/// the cluster always recovering, and the run would measure handover time, not cost.
const KILL_EVERY_S = MEMORY
  ? 0
  : Number(env.CHAOS_KILL_EVERY_S ?? (has("--cost") || env.CHAOS_COST === "1" ? "30" : "10"));
/// The load: `echo` (`ChaosService`, the default) or `billing` (`workloads/billing.ts`).
const WORKLOAD = flagValue("--workload") ?? env.CHAOS_WORKLOAD ?? "echo";
if (WORKLOAD !== "echo" && WORKLOAD !== "billing") {
  console.error(`--workload only accepts echo or billing, not ${WORKLOAD}`);
  process.exit(2);
}
// ── Liveness checks — see the head of the file.
const DRAIN_S = Number(env.CHAOS_DRAIN_S ?? "600");
/**
 * Time cap of the final `verify` (opening every Db). The old 300 s was not enough: a
 * 60-minute run with ~320 k acks on SeaweedFS took ~316 s even on an idle machine.
 * A 24 h soak needs longer still: `CHAOS_FINAL_VERIFY_S`.
 */
const FINAL_VERIFY_S = Number(env.CHAOS_FINAL_VERIFY_S ?? "1800");
/** The engine's `SUSPENDED_RETRY` (`crates/cairn-server/src/flow/mod.rs`), seconds — the extended drain deadline of `--tenant-states`. */
const SUSPENDED_RETRY_S = Number(env.CHAOS_SUSPENDED_RETRY_S ?? "900");
const SCHED_EVERY_S = Number(env.CHAOS_SCHED_EVERY_S ?? "30");
const QUEUE_WINDOW_S = Number(env.CHAOS_QUEUE_WINDOW_S ?? "600");
const DEADLOCK_SCRAPES = Number(env.CHAOS_DEADLOCK_SCRAPES ?? "6");
const RESTART_AFTER_S = Number(env.CHAOS_RESTART_AFTER_S ?? "4");
const INVOKES_PER_S = Number(env.CHAOS_RATE ?? "5");
const SPLIT_EVERY_S = Number(env.CHAOS_SPLIT_EVERY_S ?? "0");
/// The node's WAL kind — `slatedb` (per-range WAL) or `shared` (shared per-node WAL).
/// Unset means **no flag is passed**: the node uses its own default (a cluster ⇒
/// `shared`). Passing `slatedb` as the default here once silently overrode the
/// node's default.
const WAL_KIND = env.CHAOS_WAL_KIND;
/// Failpoints on real processes — see the head of the file.
/// `--failpoints`: every `return(...)` point; `--gate-faults` (pre-soak case (b)) adds the turn-gate points — on its own it draws only among them.
const FAILPOINTS_ALL = ALL || has("--failpoints") || env.CHAOS_FAILPOINTS === "1";
const FAILPOINT_EVERY_S = Number(env.CHAOS_FAILPOINT_EVERY_S ?? "10");
const FAILPOINT_HOLD_S = Number(env.CHAOS_FAILPOINT_HOLD_S ?? "5");
const AUTO_SPLIT = ALL || has("--auto-split") || env.CHAOS_AUTO_SPLIT === "1";
/**
 * The query loop: `--query` (or `CHAOS_QUERY=1`) — throughout the run, every
 * `CHAOS_QUERY_EVERY_MS` (default 1000) one `POST /query` statement to any live node:
 * the newest page, counts by class, ordered by `transitioned_at`, the counters.
 * Invariants: never a `5xx`; a complete answer (`partial = false`) is in the declared
 * order, and `sys_invocation_counts` matches `count(*)` taken at the same time only
 * once there is no load left (checked once at the end).
 */
const QUERY_LOOP = ALL || has("--query") || env.CHAOS_QUERY === "1";
const QUERY_EVERY_MS = Number(env.CHAOS_QUERY_EVERY_MS ?? 1000);
/// `billing` turns are short (they do not hold a slot like `CHAOS_RUN_MS`), so the threshold is lower.
const AUTO_HOT_SHARE = env.CHAOS_AUTO_HOT_SHARE ?? (WORKLOAD === "billing" ? "0.002" : "0.005");
const AUTO_HOT_SECS = env.CHAOS_AUTO_HOT_SECS ?? "5";
const AUTO_COOLDOWN_S = Number(env.CHAOS_AUTO_COOLDOWN_S ?? "60");
const AUTO_NODE_INTERVAL_S = Number(env.CHAOS_AUTO_NODE_INTERVAL_S ?? "30");
/// Range count from which nodes respawn with `--auto-split suggest` (0 = never).
const AUTO_MAX_RANGES = Number(env.CHAOS_AUTO_MAX_RANGES ?? "160");
/// Range count of the most recent table read (the leader prober updates it); `nodeArgs`
/// reads it at every spawn.
let rangeCountSeen = 0;
/// Number of parallel call loops. One loop (the old default) almost never makes a
/// range hot.
const CONCURRENCY = Number(env.CHAOS_CONCURRENCY ?? (AUTO_SPLIT ? "8" : "1"));
/// Each `work` turn holds its slot for this many ms inside `ctx.run`.
const RUN_MS = Number(env.CHAOS_RUN_MS ?? (AUTO_SPLIT ? "50" : "0"));
/// Unkeyed call tree: each `work` also emits an unkeyed, unscoped `ctx.call` tree of
/// width `CHAOS_TREE_WIDTH` and depth `CHAOS_TREE_DEPTH` — with the `calls/prefix`
/// gate on (new buckets) the whole tree is colocated on `work`'s range, and auto split
/// must be able to cut inside it. `0` = off (the default).
const TREE_WIDTH = Number(env.CHAOS_TREE_WIDTH ?? "0");
const TREE_DEPTH = Number(env.CHAOS_TREE_DEPTH ?? "2");
/// Number of leaves of one tree: its result goes into `work`'s output for the verdict to check.
const TREE_LEAVES = TREE_WIDTH > 0 ? TREE_WIDTH ** TREE_DEPTH : 0;
/// A saga across entities: 3 steps, each on its own key.
const SAGA_STEPS = env.CHAOS_SAGA === "1" ? 3 : 0;
/// A keyed fan-out: the number of children of each `work`.
const FANOUT = Number(env.CHAOS_FANOUT ?? "0");
/// The agent load: `ctx.log` + a span around `ctx.run "stamp"`, and an artifact of
/// `CHAOS_AGENT_ARTIFACT_KB` KiB emitted with `ctx.artifact.put` inside a `ctx.run`.
/// The verdict also checks: every `completed` reads its artifact back byte for byte
/// and `$log` has its `stamp` line (the line inside the winning closure goes out with
/// the commit of the result).
const AGENT = env.CHAOS_AGENT === "1";
const AGENT_ARTIFACT_KB = Number(env.CHAOS_AGENT_ARTIFACT_KB ?? "256");
/** The deterministic artifact bytes of `seq` — the verdict rebuilds them to compare. */
function agentBytes(seq: number): Uint8Array {
  const unit = new TextEncoder().encode(`artifact-${seq};`);
  const out = new Uint8Array(AGENT_ARTIFACT_KB * 1024);
  for (let i = 0; i < out.length; i++) out[i] = unit[i % unit.length]!;
  return out;
}
if (AUTO_SPLIT && SPLIT_EVERY_S > 0) {
  console.error("--auto-split replaces CHAOS_SPLIT_EVERY_S — pick one");
  process.exit(2);
}

// ── Flags of the automatic mechanisms — see the head of the file.
const GC = ALL || has("--gc") || env.CHAOS_GC === "1";
const GC_EVERY_S = Number(env.CHAOS_GC_EVERY_S ?? "60");
const GC_MIN_AGE_S = Number(env.CHAOS_GC_MIN_AGE_S ?? "120");
/** The node's state scrubber cadence (`0` = off). A signal only, not an invariant. */
const SCRUB_EVERY_S = Number(env.CHAOS_SCRUB_EVERY_S ?? "60");
/**
 * The scrubber's request budget per hour. When set it is passed as
 * `--scrub-requests-per-hour` to the nodes; otherwise the node uses its default
 * (`scrub::default_budget`: 1 000 × 3 600 / `--scrub-every-s`) and the harness computes
 * exactly that number to judge the quiet phase.
 */
const SCRUB_REQUESTS_PER_HOUR = env.CHAOS_SCRUB_REQUESTS_PER_HOUR;
const SCRUB_BUDGET =
  SCRUB_REQUESTS_PER_HOUR !== undefined
    ? Number(SCRUB_REQUESTS_PER_HOUR)
    : Math.ceil((1_000 * 3_600) / Math.max(1, SCRUB_EVERY_S));
/** The quiet phase after the destructive phase — see the head of the file. `0` = none. */
const IDLE_TAIL_S = Number(env.CHAOS_IDLE_TAIL_S ?? "0");
/** Cap on the `external_dbs` depth of every live range at the end of the quiet phase. */
const IDLE_MAX_DEPTH = Number(env.CHAOS_IDLE_MAX_DEPTH ?? "2");
/**
 * Grace before the quiet phase's start mark, seconds: the tail of the last
 * kill/failpoint (the session of the incarnation just revived, ranges reopening) does
 * not count toward the phase. A fail-stop during the grace is only printed.
 */
const IDLE_GRACE_S = Number(env.CHAOS_IDLE_GRACE_S ?? "60");
/**
 * D-d's reference budget in the quiet phase, requests/hour. D-d has no request-budget
 * flag (only `--gc-every-s`), so the default takes exactly the scrubber's baseline at
 * the same cadence: the cost of one round over 64 ranges (1 000 requests) per `--gc-every-s`.
 */
const IDLE_DD_BUDGET = Number(env.CHAOS_IDLE_DD_PER_HOUR ?? String(Math.ceil((1_000 * 3_600) / Math.max(1, GC_EVERY_S))));
const SLOW_NODE = ALL || has("--slow-node") || env.CHAOS_SLOW_NODE === "1";
const SLOW_EVERY_S = Number(env.CHAOS_SLOW_EVERY_S ?? "180");
const SLOW_HOLD_S = Number(env.CHAOS_SLOW_HOLD_S ?? "90");
const SLOW_PUT = env.CHAOS_SLOW_PUT ?? "1400";
/// `--cost` passes no lease flag: the node uses its default.
const LEASE_TTL_MS: string | undefined =
  env.CHAOS_LEASE_TTL_MS ?? (has("--cost") || env.CHAOS_COST === "1" ? undefined : SLOW_NODE ? "6000" : "1500");
const TENANTS = Number(flagValue("--tenants") ?? env.CHAOS_TENANTS ?? (ALL && WORKLOAD === "echo" ? "4" : "0"));
if (WORKLOAD === "billing" && TENANTS > 0) {
  console.error("--workload billing sets its own tenant rules — not combinable with --tenants");
  process.exit(2);
}
const TENANT_RATES = ALL || has("--tenant-rates") || env.CHAOS_TENANT_RATES === "1";
const NOISY_CONC = Number(env.CHAOS_NOISY_CONC ?? "16");
const NOISY_RATE = Number(env.CHAOS_NOISY_RATE ?? "40");
const QUIET_RATE = Number(env.CHAOS_QUIET_RATE ?? "1000");
const QUIET_EVERY_MS = Number(env.CHAOS_QUIET_EVERY_MS ?? "1000");
const QUIET_WINDOW_S = Number(env.CHAOS_QUIET_WINDOW_S ?? "300");
const L0_PRESSURE = ALL || has("--l0-pressure") || env.CHAOS_L0_PRESSURE === "1";
const L0_EVERY_S = Number(env.CHAOS_L0_EVERY_S ?? "240");
const L0_HOLD_S = Number(env.CHAOS_L0_HOLD_S ?? "90");
const L0_DELAY = env.CHAOS_L0_DELAY ?? "2000";
// ── Tenant states, SSO, store outage — see the head of the file.
const TENANT_STATES = has("--tenant-states") || env.CHAOS_TENANT_STATES === "1";
const TENANT_STATE_EVERY_S = Number(env.CHAOS_TENANT_STATE_EVERY_S ?? "60");
const TENANT_STATE_HOLD_S = Number(env.CHAOS_TENANT_STATE_HOLD_S ?? "15");
/// Grace after a `204`: a turn granted a slot just before may still enter the handler/finish.
const TENANT_STATE_GRACE_MS = Number(env.CHAOS_TENANT_STATE_GRACE_MS ?? "3000");
/**
 * `--large-values`: tenant `chaos-lv` (`max-payload` 1 MiB) runs `ChaosHistory`, an
 * append-only history on `CHAOS_LV_KEYS` objects, starting at `CHAOS_LV_BASE_KB` and
 * going back to the base size once over `CHAOS_LV_RESET_KB` — over the payload cap,
 * so the state takes the segmented path (sealed on the node, SDK v6 reads through the
 * `proxy` URL, `set` reuses the ref), and every reset leaves orphan segments for the
 * sweep (`--value-sweep-every-s 60 --value-min-age-s 600`, `--trash-keep-s 600`):
 * sealing, the sweep and the real deletion run under kill -9 and failpoints. Every
 * call rechecks **every byte** of the value read (sha256 and length stored alongside
 * in the `meta` key) and returns the sequence number `n` of its append; verdict: no
 * corrupted value, no duplicate `n` (lost update), every acked call finished, and the
 * final check reads back correctly.
 */
const LARGE_VALUES = has("--large-values") || env.CHAOS_LARGE_VALUES === "1";
const LV_TENANT = "chaos-lv";
const LV_KEYS = Number(env.CHAOS_LV_KEYS ?? "6");
const LV_BASE_KB = Number(env.CHAOS_LV_BASE_KB ?? "600");
const LV_APPEND_KB = Number(env.CHAOS_LV_APPEND_KB ?? "16");
const LV_RESET_KB = Number(env.CHAOS_LV_RESET_KB ?? "3072");
const LV_EVERY_MS = Number(env.CHAOS_LV_EVERY_MS ?? "500");
const LV_MAX_PAYLOAD = 1 << 20;
const SSO = has("--sso") || env.CHAOS_SSO === "1";
const SSO_EVERY_S = Number(env.CHAOS_SSO_EVERY_S ?? "30");
const SSO_CLIENT = "chaos_sso";
const SSO_TENANT = "chaos-sso";
const STORE_OUTAGE = has("--store-outage") || env.CHAOS_STORE_OUTAGE === "1";
/// No default — see the head of the file.
const OUTAGE_CONTAINER = env.CHAOS_OUTAGE_CONTAINER;
const OUTAGE_AT_S = Number(env.CHAOS_OUTAGE_AT_S ?? String(Math.round(DURATION_S / 3)));
/// 2 × the lease TTL: the number chaos passes to the nodes, otherwise the binary's default 10 s.
const OUTAGE_S = Number(env.CHAOS_OUTAGE_S ?? String((2 * Number(LEASE_TTL_MS ?? "10000")) / 1000));
const OUTAGE_RECOVER_S = Number(env.CHAOS_OUTAGE_RECOVER_S ?? "300");
/// Total cap of one revive round (exponential backoff between attempts) — over it is a violation.
const RESPAWN_GIVEUP_S = Number(env.CHAOS_RESPAWN_GIVEUP_S ?? "900");
// ── Pre-soak cases — see the head of the file ("Pre-soak fault classes").
/// `--pre-soak` (and `--profile all`): turns on all five cases — the default set of the final pre-soak run.
const PRE_SOAK = ALL || has("--pre-soak") || env.CHAOS_PRE_SOAK === "1";
/// (a) a hot object receiving signals/cancels from outside for queued invocations.
const HOT = PRE_SOAK || has("--hot-signal") || env.CHAOS_HOT_SIGNAL === "1";
const HOT_TENANT = "chaos-hot";
const HOT_KEYS = Number(env.CHAOS_HOT_KEYS ?? "2");
const HOT_INFLIGHT = Number(env.CHAOS_HOT_INFLIGHT ?? "6");
const HOT_RUN_MS = Number(env.CHAOS_HOT_RUN_MS ?? "30");
const HOT_WAIT_MS = Number(env.CHAOS_HOT_WAIT_MS ?? "500");
const HOT_SIGNAL_MS = Number(env.CHAOS_HOT_SIGNAL_MS ?? "100");
const HOT_SIGNALERS = Number(env.CHAOS_HOT_SIGNALERS ?? "1");
const HOT_CANCEL_SHARE = Number(env.CHAOS_HOT_CANCEL_SHARE ?? "0.05");
/// (b) faults at the turn gate in the random failpoint set + the `running`-standing-still verdict.
const GATE_FAULTS = PRE_SOAK || has("--gate-faults") || env.CHAOS_GATE_FAULTS === "1";
const STUCK_RUNNING_S = Number(env.CHAOS_STUCK_RUNNING_S ?? (GATE_FAULTS ? "300" : "0"));
const STUCK_EVERY_S = Number(env.CHAOS_STUCK_EVERY_S ?? "30");
/// The healthy windows of (b): every `CHAOS_HEALTHY_EVERY_S` seconds, the last `CHAOS_HEALTHY_S`
/// seconds have no kills, no random failpoints, no slow node/L0 pressure/SDK reopen — the load keeps running.
const HEALTHY_EVERY_S = Number(env.CHAOS_HEALTHY_EVERY_S ?? (GATE_FAULTS ? "900" : "0"));
const HEALTHY_S = Number(env.CHAOS_HEALTHY_S ?? "360");
/// (c) a tenant purge on a large range.
const TENANT_PURGE = PRE_SOAK || has("--tenant-purge") || env.CHAOS_TENANT_PURGE === "1";
const PURGE_TENANT = "chaos-purge";
const PURGE_INVOCATIONS = Number(env.CHAOS_PURGE_INVOCATIONS ?? "46000");
const PURGE_CONC = Number(env.CHAOS_PURGE_CONC ?? "32");
const PURGE_KEEP_S = Number(env.CHAOS_PURGE_KEEP_S ?? "60");
const PURGE_DEADLINE_S = Number(env.CHAOS_PURGE_DEADLINE_S ?? "1800");
/// After this many consecutive failed polls of `GET …/purge`, the API answer is **stale**:
/// the harness reads the `tenants` book on the bucket directly (aws CLI) as the arbiter.
const PURGE_STALE_POLLS = Number(env.CHAOS_PURGE_STALE_POLLS ?? "3");
/// After the API reports `purged`: this many more polls for `held` to become empty **and complete**
/// (`heldComplete`). A poll is capped on the server: a cold cache over a large trash
/// ⇒ `heldComplete: false`, not yet conclusive; a held entry that an old runner wrote after
/// the tombstone (two overlapping rounds when the runner role moves node) is removed in the next round.
const PURGE_HELD_SETTLE_POLLS = Number(env.CHAOS_PURGE_HELD_SETTLE_POLLS ?? "6");
/// (d) periodic backups, restored into a side bucket, `verify` + `backup check-restore`.
const BACKUP_RESTORE = PRE_SOAK || has("--backup-restore") || env.CHAOS_BACKUP_RESTORE === "1";
const BACKUP_FIRST_S = Number(env.CHAOS_BACKUP_FIRST_S ?? "60");
const BACKUP_EVERY_S = Number(env.CHAOS_BACKUP_EVERY_S ?? "900");
const BACKUP_TIMEOUT_S = Number(env.CHAOS_BACKUP_TIMEOUT_S ?? "900");
const RESTORE_BUCKET = env.CHAOS_RESTORE_BUCKET ?? "walbench-restore";
/// (e) a multi-step parallel-branch handler, `ctx.run` failing at random.
const PAR = PRE_SOAK || has("--par-branches") || env.CHAOS_PAR === "1";
const PAR_BRANCHES = Number(env.CHAOS_PAR_BRANCHES ?? "3");
const PAR_STEPS = Number(env.CHAOS_PAR_STEPS ?? "4");
const PAR_FAIL = Number(env.CHAOS_PAR_FAIL ?? "0.15");
const PAR_EVERY_MS = Number(env.CHAOS_PAR_EVERY_MS ?? "1000");
/// The tripwires: `panic` on every node, revived nodes included.
const TRIPWIRES = env.CHAOS_TRIPWIRES !== "0";
const TRIPWIRE_POINTS = ["flow::second_live_permit", "flow::lock_released_by_non_holder"] as const;
const PRE_SOAK_CASES = HOT || GATE_FAULTS || TENANT_PURGE || BACKUP_RESTORE || PAR;
const FAILPOINTS = FAILPOINTS_ALL || GATE_FAULTS;
// ── Measuring read cost — see the head of the file.
const COST = has("--cost") || env.CHAOS_COST === "1";
/// **The quiet-renewal gate of the chaos bucket.** A node creating a new bucket turns
/// the gate on **with delegation** (`--quiet-lease delegation`, the binary's default;
/// the `delegation_unproven=` mark on the bucket — a chaos run is itself the
/// evidence), so with no flag at all delegation already runs. Other choices:
/// - `--quiet-lease` (or `CHAOS_QUIET_LEASE=1`):
///   solo mode only (`--quiet-lease solo` for the nodes) — a node alone enters
///   `mode=solo`, a rebuilt node challenges the `solo` record of the dead process;
/// - `--no-quiet-lease` (or `CHAOS_QUIET_LEASE=off`): a `direct` cluster as before
///   delegation existed (`--quiet-lease off`);
/// - `--delegation` (or `CHAOS_DELEGATION=1`): kept for compatibility — delegation is
///   already the default; once the cluster is up, the run still calls
///   `cairn-ctl cluster enable-quiet-lease --delegation --accept-missing-evidence
///   --apply` (writes nothing if the binary already enabled it; enables it for older
///   binaries). Needs `cairn-ctl`.
/// Nodes receive the `--quiet-lease` flag only when `solo`/`off` is chosen: older
/// binaries do not know that flag.
const DELEGATION = has("--delegation") || env.CHAOS_DELEGATION === "1";
const QUIET_MODE: "off" | "solo" | "delegation" =
  has("--no-quiet-lease") || env.CHAOS_QUIET_LEASE === "off"
    ? "off"
    : DELEGATION
      ? "delegation"
      : has("--quiet-lease") || env.CHAOS_QUIET_LEASE === "1"
        ? "solo"
        : "delegation";
if (DELEGATION && QUIET_MODE === "off") throw new Error("--delegation and --no-quiet-lease are mutually exclusive");
/// `--kill-coordinator-every-s <n>` (hay `CHAOS_KILL_COORDINATOR_EVERY_S`,
/// every `n` seconds `kill -9` exactly the node holding the coordinator role (gauge
/// `cairnflow_coordinator_term` > 0 on `/metrics`), next to the random kills; at least
/// one node is spared, as with the random kills. 0 = off.
/// `CHAOS_SPARE_NODE=i`: no kill loop picks node `i` — one incarnation lives through
/// the whole run among nodes dying every few dozen seconds (the shape of the orphan
/// process seen in an earlier soak run).
const SPARE_NODE = env.CHAOS_SPARE_NODE === undefined ? -1 : Number(env.CHAOS_SPARE_NODE);
const KILL_COORDINATOR_EVERY_S = Number(
  flagValue("--kill-coordinator-every-s") ?? env.CHAOS_KILL_COORDINATOR_EVERY_S ?? "0",
);
const S3_PROFILE =
  flagValue("--s3-profile") ??
  env.CHAOS_S3_PROFILE ??
  (ALL ? join(repoRoot, "bench", "s3-model", "profiles", "s3-standard.json") : undefined) ??
  (COST || IDLE_TAIL_S > 0 ? join(repoRoot, "bench", "s3-model", "profiles", "count-only.json") : undefined);
/// With a store profile there are request counters on `/metrics` to scrape.
const COUNTING = S3_PROFILE !== undefined;
const SCRAPE_EVERY_S = Number(env.CHAOS_SCRAPE_EVERY_S ?? "5");
/// Durable SDK commands (metering's "step" unit) per `work` call: `run`, `call`,
/// `work`'s output, `ChaosEcho.ping`'s output. Metering cannot be measured under
/// `kill -9` (it flushes every 60 s and dies with the process), so steps here are an
/// estimate from the number of acked calls.
const STEPS_PER_WORK = 4;
/// The probe path of the two-leader prober: `cancel` (default, not through a follower) or
/// `status` (the old path, opening a follower reader — only to measure that part).
const PROBE = env.CHAOS_PROBE ?? "cancel";
if (PROBE !== "cancel" && PROBE !== "status") {
  console.error(`CHAOS_PROBE only accepts cancel or status, not ${PROBE}`);
  process.exit(2);
}

// ── The extra invariants and the injected faults that prove them — see the head of the file.
const INJECTS = {
  "rogue-leader": "two leaders",
  overlap: "overlapping split/merge",
  "quiet-throttle": "noisy tenant",
  "gc-skip-refs": "GC/verify",
  "floor-stuck": "shared WAL floor",
} as const;
type Inject = keyof typeof INJECTS;
const INJECT = env.CHAOS_INJECT as Inject | undefined;
if (INJECT !== undefined && !(INJECT in INJECTS)) {
  console.error(`unknown CHAOS_INJECT: ${INJECT} — one of ${Object.keys(INJECTS).join(", ")}`);
  process.exit(2);
}
const LEADER_PROBE_MS = Number(env.CHAOS_LEADER_PROBE_MS ?? "1000");
/// The watcher cadence of invariant 5. Two overlapping intents only live until the first
/// operation's commit CAS fails (a few hundred ms), so an injected run polls more often.
const OVERLAP_POLL_MS = Number(env.CHAOS_OVERLAP_POLL_MS ?? (INJECT === "overlap" ? "20" : "250"));
const VERIFY_EVERY_S = Number(env.CHAOS_VERIFY_EVERY_S ?? (INJECT === "gc-skip-refs" ? "60" : "300"));
const FLOOR_EVERY_S = Number(env.CHAOS_FLOOR_EVERY_S ?? (INJECT === "floor-stuck" ? "15" : "120"));
const FLOOR_STUCK_S = Number(env.CHAOS_WAL_FLOOR_STUCK_S ?? (INJECT === "floor-stuck" ? "30" : "600"));
const QUIET_P99_MS = Number(env.CHAOS_QUIET_P99_MS ?? "8000");
const QUIET_VS_BASE = Number(env.CHAOS_QUIET_VS_BASE ?? "1.5");
/// The minimum number of samples for a window to be judged (below it p99 is the max).
const QUIET_MIN_SAMPLES = Number(env.CHAOS_QUIET_MIN_SAMPLES ?? "1000");
const LOG_DIR = env.CHAOS_LOG_DIR;
/// The root directory of the node slots' disk caches (`CHAOS_DISK_CACHE`, see the head of the file).
const DISK_CACHE = env.CHAOS_DISK_CACHE;
/// The disk cache cap per node (`--disk-cache-bytes`); absent ⇒ the binary's default (16 GiB).
const DISK_CACHE_BYTES = env.CHAOS_DISK_CACHE_BYTES;
/// The history checker: every this many ms it rereads `CHAOS_HISTORY_PROBE_IDS` ids
/// already seen `completed` (`0` = prober off; the final verdict still records history).
const HISTORY_PROBE_MS = Number(env.CHAOS_HISTORY_PROBE_MS ?? 2_000);
const HISTORY_PROBE_IDS = Number(env.CHAOS_HISTORY_PROBE_IDS ?? 10);

/// Total time cap of the verdict (seconds). An injected run only needs to know its
/// invariant failed; judging each id for 60 s on a deliberately broken bucket takes hours.
const VERDICT_BUDGET_S = Number(env.CHAOS_VERDICT_BUDGET_S ?? (INJECT ? "120" : "0"));
/**
 * `--retention-ms` of every node. Finished invocations must outlive the run and its verdict,
 * or the verdict reads 404 for the oldest acked ids: a 24 h soak on the binary's 24 h default
 * logged them as lost and judged 16 ids a minute while more expired. Default: the run, the
 * drain and 12 h more, at least the binary's 24 h.
 */
const RETENTION_MS = Number(
  env.CHAOS_RETENTION_MS ?? String(Math.max(24 * 3_600_000, (DURATION_S + DRAIN_S) * 1000 + 12 * 3_600_000)),
);
/// Number of ids judged at once. `1` = sequential as before.
const VERDICT_CONCURRENCY = Number(env.CHAOS_VERDICT_CONCURRENCY ?? "16");

// ── Long soak — see "Long soak" at the head of the file.
/** Free-space threshold (GB) of every watched path; below it the destructive phase stops early ("STOPPED: disk"). `0` = off. */
const DISK_MIN_GB = Number(env.CHAOS_DISK_MIN_GB ?? "20");
const DISK_EVERY_S = Number(env.CHAOS_DISK_EVERY_S ?? "30");
/**
 * Watched paths: where the harness itself writes (log directory, disk cache, a
 * `file://` store) plus `CHAOS_DISK_PATHS` (`:`-separated) — the volume of the
 * SeaweedFS bucket, of `run.log`, … that the harness cannot know by itself.
 */
const DISK_PATHS = [
  ...new Set(
    [
      LOG_DIR,
      DISK_CACHE,
      STORE?.startsWith("file://") ? STORE.slice("file://".length) : undefined,
      ...(env.CHAOS_DISK_PATHS ?? "").split(":"),
    ].filter((p): p is string => p !== undefined && p !== ""),
  ),
];
/** Cap of one node log file (MB) before rotating (`0` = no rotation), and the number of rotated files kept per node. */
const LOG_MAX_MB = Number(env.CHAOS_LOG_MAX_MB ?? "256");
const LOG_KEEP = Number(env.CHAOS_LOG_KEEP ?? "3");
/** Cadence of the summary printed while running (seconds; `0` = off). */
const SUMMARY_EVERY_S = Number(env.CHAOS_SUMMARY_EVERY_S ?? "3600");
/** Index of the tenant (≥ 1, never the noisy tenant) with its own REQUEST_RESPONSE deployment. */
const RR_TENANT_RAW = flagValue("--rr-tenant") ?? env.CHAOS_RR_TENANT;
const RR_TENANT = RR_TENANT_RAW === undefined ? undefined : Number(RR_TENANT_RAW);
if (RR_TENANT !== undefined) {
  if (WORKLOAD !== "echo" || !(Number.isInteger(RR_TENANT) && RR_TENANT >= 1 && RR_TENANT < TENANTS)) {
    console.error(
      `--rr-tenant ${RR_TENANT_RAW}: needs --tenants N (N ≥ 2) and 1 ≤ index < N — tenant 0 is the noisy tenant; echo workload only`,
    );
    process.exit(2);
  }
  if (sdkMode === "REQUEST_RESPONSE") {
    console.error("--rr-tenant is redundant: CAIRNFLOW_E2E_SDK_MODE=REQUEST_RESPONSE already runs every tenant as REQUEST_RESPONSE");
    process.exit(2);
  }
}

// With tenants the cluster runs with auth: the API key is what carries the tenant. SSO needs
// auth too (a session cookie only means something when requests must authenticate).
const AUTH = TENANTS > 0 || SSO || LARGE_VALUES || HOT || TENANT_PURGE;
const ADMIN_KEY = AUTH ? `cf_chaos_${randomBytes(16).toString("hex")}` : undefined;
// The pre-soak cases need the tripwires to be settable: the binary must have `failpoints`.
const NEED_FAILPOINTS =
  FAILPOINTS || SLOW_NODE || L0_PRESSURE || INJECT === "gc-skip-refs" || INJECT === "overlap" || (PRE_SOAK_CASES && TRIPWIRES);
/// `cairn-ctl` is another process — it cannot read a node's `memory://`.
const HAS_CTL = existsSync(CTL) && !MEMORY;
const FLOORS = HAS_CTL && WAL_KIND !== "slatedb";

/**
 * Points accepting `return(...)` — with a real error path — and the actions they can be set to.
 * Points that can only pause/sleep (`split::commitCas` onward, `nodewal::stripe::cut`,
 * `actor::*`, …) are not here: `return` there is a panic. The slowing points
 * (`node::put_delay`, `compactor::io_delay`) have their own cycles (`--slow-node`,
 */
const ALL_RETURN_POINTS: ReadonlyArray<readonly [string, readonly string[]]> = [
  ["writer::commit::before_write", ["return(chaos)"]],
  ["writer::commit::before_durable", ["return(chaos)"]],
  // A batch with outbox rows submitted whose `commit` returns an error — the write loses
  // its handle, the send ticket must take the fence path.
  ["writer::commit::after_submit", ["return(chaos)"]],
  ["writer::commit::after_durable", ["return(chaos)"]],
  ["writer::seal::before_flush", ["return(chaos)"]],
  ["lease::sync::wait", ["return(chaos)"]],
  ["session::renew::before_put", ["return(chaos)"]],
  ["session::renew::after_put", ["return(chaos)"]],
  ["session::claim", ["return(chaos)"]],
  ["owner::write", ["return(conflict)", "return(chaos)"]],
  ["cluster::update::before_cas", ["return(chaos)"]],
  ["split::precondition", ["return(chaos)"]],
  ["split::intentCas", ["return(chaos)"]],
  ["split::flush", ["return(chaos)"]],
  ["split::quiesce", ["return(chaos)"]],
  ["split::cloneLeft", ["return(chaos)"]],
  ["split::cloneRight", ["return(chaos)"]],
  ["split::ownerClaim", ["return(chaos)"]],
  ["merge::precondition", ["return(chaos)"]],
  ["merge::intentCas", ["return(chaos)"]],
  ["merge::gather", ["return(chaos)"]],
  ["merge::flush", ["return(chaos)"]],
  ["merge::quiesce", ["return(chaos)"]],
  ["merge::union", ["return(chaos)"]],
  ["merge::ownerClaim", ["return(chaos)"]],
  ["split::roll_back", ["return(chaos)"]],
  ["transport::send_once", ["return(409)", "return(503)"]],
  ["turn::fire_retries", ["return(chaos)"]],
  // Turn error paths that only the rescue net still wakes: a turn failing before
  // its reschedule point, or the reschedule itself failing.
  ["turn::ensure_queued", ["return(chaos)"]],
  ["turn::schedule_after_error", ["return(chaos)"]],
  ["nodewal::stripe::put", ["return(chaos)"]],
];

/**
 * Faults at the turn gate: a failed status read at the gate after the slot
 * (`turn::gate_turn`), a `Running` commit returning an error (`turn::after_running`),
 * rescheduling after a failed turn (`turn::retry_if`), a failed queue release
 * (`turn::release_queue_slot`), a failed status read in the rescue round
 * (`actor::rescue_round::status`). Every point only `return`s an error, with the engine held.
 * The accompanying verdict: no `running` invocation standing still longer than
 * `CHAOS_STUCK_RUNNING_S` while the cluster is healthy.
 */
const GATE_POINTS: ReadonlyArray<readonly [string, readonly string[]]> = [
  ["turn::gate_turn", ["return(chaos)"]],
  ["turn::after_running", ["return(chaos)"]],
  ["turn::retry_if", ["return(chaos)"]],
  ["turn::release_queue_slot", ["return(chaos)"]],
  ["actor::rescue_round::status", ["return(chaos)"]],
];

/** The points drawn from: every point, or only those matching `CHAOS_FAILPOINT_MATCH`. */
const POINT_POOL = [...(FAILPOINTS_ALL ? ALL_RETURN_POINTS : []), ...(GATE_FAULTS ? GATE_POINTS : [])];
const RETURN_POINTS = env.CHAOS_FAILPOINT_MATCH
  ? POINT_POOL.filter(([n]) => new RegExp(env.CHAOS_FAILPOINT_MATCH as string).test(n))
  : POINT_POOL;
if (FAILPOINTS && RETURN_POINTS.length === 0) throw new Error(`CHAOS_FAILPOINT_MATCH=${env.CHAOS_FAILPOINT_MATCH} matches no point`);

/** Points the dedicated cycles need in the binary. */
const OWN_POINTS = [
  ...(SLOW_NODE || INJECT === "overlap" ? ["node::put_delay"] : []),
  ...(L0_PRESSURE ? ["compactor::io_delay"] : []),
  ...(INJECT === "gc-skip-refs"
    ? ["gc::skip_reference_check", "gc::skip_checkpoint_check"]
    : []),
];

/** Failpoints set on **every** node, revived nodes included (the injected faults). */
const STANDING_FAILPOINTS: Array<readonly [string, string]> =
  INJECT === "gc-skip-refs"
    ? [
        ["gc::skip_reference_check", "return"],
        // D-d's second fence holds when only the scan is blinded — the run proving
        // `verify` must blind both.
        ["gc::skip_checkpoint_check", "return"],
      ]
    : [];

// SeaweedFS on localhost by default — see the head of the file for why not MinIO. The child
// `cairnflow` processes inherit `process.env`, so setting it here is enough.
if (STORE?.startsWith("s3://") && !env.AWS_ENDPOINT) {
  Object.assign(env, {
    AWS_ENDPOINT: env.CHAOS_S3_ENDPOINT ?? "http://localhost:18333",
    AWS_ACCESS_KEY_ID: "dev-only-access-key",
    AWS_SECRET_ACCESS_KEY: "dev-only-not-a-secret",
    AWS_REGION: "us-east-1",
    AWS_ALLOW_HTTP: "true",
    AWS_VIRTUAL_HOSTED_STYLE_REQUEST: "false",
  });
}

if (!STORE) {
  console.error("missing CHAOS_STORE_URI (e.g. s3://walbench/chaos-1)");
  process.exit(2);
}
if (!existsSync(binary)) {
  console.error(`missing binary ${binary} — run \`cargo build --bin cairnflow\` first`);
  process.exit(2);
}
if ((GC || ALL) && !HAS_CTL) {
  console.error(
    MEMORY
      ? "--gc/--profile all need cairn-ctl as the arbiter, and cairn-ctl cannot read memory:// — use s3://"
      : `--gc/--profile all need ${CTL} as the arbiter (cargo build --bin cairn-ctl, or CHAOS_CTL)`,
  );
  process.exit(2);
}
if (TENANTS === 1 || (INJECT === "quiet-throttle" && TENANTS < 2)) {
  console.error("--tenants needs ≥ 2: one noisy tenant and at least one quiet tenant");
  process.exit(2);
}
if (TENANT_STATES && TENANTS < 2) {
  console.error("--tenant-states needs --tenants N (N ≥ 2): only quiet tenants change state, never the noisy one");
  process.exit(2);
}
if (STORE_OUTAGE) {
  if (!OUTAGE_CONTAINER) {
    console.error("--store-outage needs CHAOS_OUTAGE_CONTAINER (the name of this run's own store container) — no default");
    process.exit(2);
  }
  if (!STORE.startsWith("s3://")) {
    console.error("--store-outage only runs on s3:// (a dedicated SeaweedFS container)");
    process.exit(2);
  }
  if (!(OUTAGE_AT_S > 0 && OUTAGE_AT_S < DURATION_S && OUTAGE_S > 0)) {
    console.error(`--store-outage: needs 0 < CHAOS_OUTAGE_AT_S (${OUTAGE_AT_S}) < CHAOS_DURATION_S (${DURATION_S}) and CHAOS_OUTAGE_S > 0`);
    process.exit(2);
  }
}
if (S3_PROFILE && !existsSync(S3_PROFILE)) {
  console.error(`no profile file ${S3_PROFILE}`);
  process.exit(2);
}
if (INJECT === "overlap" && SPLIT_EVERY_S > 0) {
  console.error("CHAOS_INJECT=overlap checks an auto-split invariant — do not set CHAOS_SPLIT_EVERY_S");
  process.exit(2);
}
if (INJECT === "gc-skip-refs" && !(GC && (SPLIT_EVERY_S > 0 || AUTO_SPLIT))) {
  console.error("CHAOS_INJECT=gc-skip-refs needs --gc and range splits (CHAOS_SPLIT_EVERY_S or --auto-split)");
  process.exit(2);
}

if (BACKUP_RESTORE) {
  if (!HAS_CTL || !STORE.startsWith("s3://")) {
    console.error("--backup-restore needs cairn-ctl and s3:// (the restored copy goes to a side bucket with the same prefix)");
    process.exit(2);
  }
  try {
    execFileSync("aws", ["--version"], { stdio: "ignore" });
  } catch {
    console.error("--backup-restore needs the aws CLI: to create the side bucket CHAOS_RESTORE_BUCKET and clean the restored copy between rounds");
    process.exit(2);
  }
}

function nodeArgs(store: string, opts: { gc: boolean }): string[] {
  return [
    "--main-store", store,
    "--cluster",
    "--partitions", PARTITIONS,
    // `--cost`: the binary's default flags — see the head of the file.
    ...(COST ? [] : ["--tick-interval-ms", "100", "--reader-poll-ms", "200", "--deploy-refresh-ms", "500"]),
    ...(LEASE_TTL_MS ? ["--lease-ttl-ms", LEASE_TTL_MS] : []),
    "--port", "0",
    "--retention-ms", String(RETENTION_MS),
    // Chaos measures loss/corruption under kill -9, not API keys — messages between
    // nodes are still HMAC-signed with the cluster secret in the bucket. With tenants auth is
    // needed: the API key is what carries the tenant.
    ...(AUTH ? [] : ["--no-auth"]),
    ...(WAL_KIND ? ["--wal-kind", WAL_KIND] : []),
    ...(AUTO_SPLIT
      ? [
          "--auto-split", AUTO_MAX_RANGES > 0 && rangeCountSeen >= AUTO_MAX_RANGES ? "suggest" : "on",
          "--auto-split-hot-share", AUTO_HOT_SHARE,
          "--auto-split-hot-secs", AUTO_HOT_SECS,
          "--auto-split-cooldown-ms", String(AUTO_COOLDOWN_S * 1000),
          "--auto-split-node-interval-ms", String(AUTO_NODE_INTERVAL_S * 1000),
        ]
      : []),
    ...(opts.gc
      ? ["--gc-retired-dbs", "--gc-every-s", String(GC_EVERY_S), "--gc-min-age-s", String(GC_MIN_AGE_S)]
      : []),
    "--scrub-every-s", String(SCRUB_EVERY_S),
    // `--tenant-purge` (pre-soak case (c)): the purge runs within the run — a `deleting` tenant
    // is reversible for `CHAOS_PURGE_KEEP_S` seconds, then deleted for real (binary default 7 days).
    ...(TENANT_PURGE ? ["--tenant-purge-keep-s", String(PURGE_KEEP_S)] : []),
    ...(SCRUB_REQUESTS_PER_HOUR !== undefined ? ["--scrub-requests-per-hour", SCRUB_REQUESTS_PER_HOUR] : []),
    // The default (`delegation`) passes no flag: older binaries do not know it.
    ...(QUIET_MODE !== "delegation" ? ["--quiet-lease", QUIET_MODE] : []),
    // `--large-values`: SDK v6 reads segments through the node (base URL = the bound
    // ingress address), and the segment sweep runs within the chaos run.
    ...(LARGE_VALUES
      ? [
          "--value-read-path", "proxy",
          "--value-sweep-every-s", "60",
          "--value-min-age-s", "600",
          "--trash-keep-s", "600",
        ]
      : []),
  ];
}

const NODE_ENV: NodeJS.ProcessEnv = {
  ...env,
  // The nodes sign requests: the `--rr-tenant` and `--large-values` endpoints verify them
  // (`identity.ts`), since the SDK serves its replay cache and wire v6 only to a verified runtime.
  CAIRNFLOW_IDENTITY_MASTER: IDENTITY_MASTER,
  ...(S3_PROFILE ? { CAIRNFLOW_STORE_PROFILE: S3_PROFILE } : {}),
  ...(ADMIN_KEY ? { CAIRNFLOW_ADMIN_KEY: ADMIN_KEY } : {}),
};

// ───────────────────────────────────────────── the service under load

/**
 * **Compact ledger** (`ledger.ts`): everything recorded per call lives in typed
 * arrays — the harness heap stays flat over the run time (it used to grow from 34 to
 * 799 MiB per hour; a 24 h soak ran out of memory).
 *
 * - `ackLedger`: every acked id → `seq` (the end-of-run lost/wrong verdict reads it here);
 * - `seqs`: by `seq` — tenant, the number of times the `ctx.run` side effect actually ran,
 *   the runs (history checker), the last entry into `work`;
 * - `historyTracker`: the history checker computed **incrementally** on each status read
 *   (time, source from the `x-cairn-read` header, status, output) — the same verdict as
 *   `checkHistory` (`ledger.test.ts`), without keeping every read. Reads of ids outside
 *   the ack ledger (large values) are kept as they are in `observations` (few).
 */
const ackLedger = new AckLedger();
const seqs = new SeqTable();
const historyTracker = new HistoryTracker();
const observations: Observation[] = [];
/** Ids outside the ack book that were read `completed` at least once — the history prober rereads them too. */
const looseCompletedSeen: string[] = [];
const looseCompletedSet = new Set<string>();
/** The tag of call `seq` (`<tenant>-<seq>`). */
const tagOfSeq = (seq: number) => `${seqs.tenantOf(seq) ?? "?"}-${seq}`;

// ── Tenant states — see the head of the file.
type TenantStateName = "active" | "limited" | "suspended";
/** Quiet tenants — only they get their state changed. */
const churnable = new Set<string>();
/** Tenants whose state has ever been changed (a refusal on any other tenant is a violation). */
const churnedEver = new Set<string>();
/** The state the harness has sent (changes to `active` only after the `204`). */
const tenantStateNow = new Map<string, TenantStateName>();
/** The time of the `204` of the most recent `active`, per tenant. */
const activeConfirmedAt = new Map<string, number>();
/** A tenant confirmed `suspended` → the time from which the engine must not call its handlers (204 + grace). */
const suspendedSince = new Map<string, number>();
/** Non-`active` windows (invariant 6 drops samples overlapping them) and confirmed `suspended` windows (invariant (b)). */
const nonActiveWindows: Array<{ tenant: string; from: number; to: number }> = [];
const suspendWindows: Array<{ tenant: string; from: number; to: number }> = [];
/** The tenant of call `seq` (child handlers identify it through seq): `seqs.tenantOf(seq)`. */
const seqTenant = { get: (seq: number) => seqs.tenantOf(seq) };
/** Violations of (b) seen right at the SDK: a handler of a `suspended` tenant was still called. */
const suspendProblems: string[] = [];
/*
 * The last entry into `work` of each call of a tenant whose state can change
 * (`seqs.setWork`/`seqs.workAt`) — only to **classify** a `SUSPENDED STILL COMPLETED`:
 * the handler entered before the suspend command (the turn was already at the SDK, the
 * outcome commits late because the WAL is held) or after. Does not change the verdict.
 */
/** `SUSPENDED STILL COMPLETED` whose handler entered before the suspend command (`204`). */
let suspendEndedInflight = 0;
let suspendEntryChecks = 0;
const tenantOfTag = (tag: string) => tag.slice(0, tag.lastIndexOf("-"));

/** The engine just called a handler carrying work of `tenant` — if it is confirmed `suspended` right then, that is violation (b). */
function noteHandlerEntry(tenant: string | undefined, what: string) {
  if (!TENANT_STATES || tenant === undefined || !churnable.has(tenant)) return;
  suspendEntryChecks++;
  const since = suspendedSince.get(tenant);
  if (since !== undefined && Date.now() >= since) {
    const line = `${new Date().toISOString()} ${tenant} suspended (confirmed + grace until ${new Date(since).toISOString()}) yet the engine still called ${what}`;
    if (suspendProblems.length < 50) console.error(`  SUSPENDED STILL RUNNING: ${line}`);
    suspendProblems.push(line);
  }
}

const chaosService = service("ChaosService", {
  async work(ctx: ServiceContext, input: { tag: string; seq: number }) {
    noteHandlerEntry(tenantOfTag(input.tag), `work ${input.tag}`);
    if (TENANT_STATES && churnable.has(tenantOfTag(input.tag))) seqs.setWork(input.seq, Date.now());
    const stamp = () => ctx.run("stamp", async () => {
      if (AGENT) ctx.log.info("stamp", { tag: input.tag });
      // A nonce per run, put into the output: the history checker knows which run
      // got committed (a rerun discarded or won), and a late run that gets
      // committed changes the output ⇒ flip. The lost/wrong verdict does not compare `stamped`.
      const nonce = randomBytes(6).toString("hex");
      seqs.noteRun(input.seq, { tag: input.tag, t: Date.now(), nonce, attempt: ctx.attempt, invocation: ctx.invocationId });
      // Hold the turn slot like a real `ctx.run` (an API call, an LLM) — so auto-split
      // has real load to measure.
      if (RUN_MS > 0) await sleep(RUN_MS);
      return `stamp-${input.seq}-${nonce}`;
    });
    const stamped = AGENT ? await ctx.trace.span("stamp", stamp, { seq: input.seq }) : await stamp();
    const artifact = AGENT
      ? (await ctx.run("artifact", () => ctx.artifact.put(agentBytes(input.seq)))).sha256
      : undefined;
    // **Cross-partition call**: the target is a
    // virtual object keyed by `seq`, so it lands across all partitions — the call
    // and its result cross partitions in both directions, under `kill -9`.
    const echoed = await ctx.call<{ echo: number }>(
      "ChaosEcho",
      "ping",
      { seq: input.seq },
      { key: `k-${input.seq}` },
    );
    // Unkeyed call tree: the leaf total comes back as exactly `TREE_LEAVES` (each leaf returns 1).
    const leaves =
      TREE_WIDTH > 0
        ? (await ctx.call<{ leaves: number }>("ChaosTree", "grow", { depth: TREE_DEPTH, seq: input.seq })).leaves
        : 0;
    // Saga: step `i` calls object `saga-<seq>-<i>` with the previous step's result —
    // exactly 3 when no step is lost or runs twice with a different result.
    let saga = 0;
    for (let i = 0; i < SAGA_STEPS; i++) {
      saga = (
        await ctx.call<{ echo: number }>("ChaosEcho", "ping", { seq: saga + 1 }, { key: `saga-${input.seq}-${i}` })
      ).echo;
    }
    // Keyed fan-out: each child returns 1.
    const fan = (
      await Promise.all(
        Array.from({ length: FANOUT }, (_, i) =>
          ctx.call<{ echo: number }>("ChaosEcho", "ping", { seq: 1 }, { key: `fan-${input.seq}-${i}` }),
        ),
      )
    ).reduce((a, k) => a + k.echo, 0);
    return { seq: input.seq, stamped, echo: echoed.echo, leaves, saga, fan, artifact };
  },
});

const chaosTree = service("ChaosTree", {
  async grow(ctx: ServiceContext, input: { depth: number; seq: number }) {
    noteHandlerEntry(seqTenant.get(input.seq), `ChaosTree.grow seq ${input.seq}`);
    if (input.depth <= 0) return { leaves: 1 };
    const kids = await Promise.all(
      Array.from({ length: TREE_WIDTH }, () =>
        ctx.call<{ leaves: number }>("ChaosTree", "grow", { depth: input.depth - 1, seq: input.seq }),
      ),
    );
    return { leaves: kids.reduce((a, k) => a + k.leaves, 0) };
  },
});

const chaosEcho = service(
  "ChaosEcho",
  {
    async ping(ctx: Context, input: { seq: number }) {
      // Keys `k-<seq>`, `saga-<seq>-<i>`, `fan-<seq>-<i>`: the seq of the parent `work`.
      const parent = /^(?:k|saga|fan)-(\d+)/.exec(ctx.key)?.[1];
      if (parent !== undefined) noteHandlerEntry(seqTenant.get(Number(parent)), `ChaosEcho.ping[${ctx.key}]`);
      return { echo: input.seq };
    },
  },
  { type: "VIRTUAL_OBJECT", handlerTypes: { ping: "EXCLUSIVE" } },
);

// ── The pre-soak cases — services and shared harness state.

/** (a) The exclusion watcher on hot objects (counted in the handler). */
const hotWatch = new ExclusiveWatch();
/** (a) Ids the harness has sent a cancel to — a holder cancelled mid-hold is excused. */
const hotCancelled = new Set<string>();
const hotExcuse = (other: string) => (hotCancelled.has(other) ? "the holder was cancelled" : undefined);

/**
 * (a) `ChaosHot.turn` — a hot `EXCLUSIVE` object: enters the hold span in its first closure,
 * waits for a `poke` signal (or `CHAOS_HOT_WAIT_MS`) — suspended while still holding the lock —
 * then leaves in its last closure. Outside signals/cancels to **invocations queued** behind it
 * take the path of a fixed bug (an issued ticket nacked, a second slot).
 */
const chaosHot = service(
  "ChaosHot",
  {
    async turn(ctx: Context, input: { n: number }) {
      const key = ctx.key;
      const inv = ctx.invocationId;
      const a = await ctx.run("enter", async () => {
        hotWatch.enter(key, inv, Date.now(), hotExcuse);
        if (HOT_RUN_MS > 0) await sleep(HOT_RUN_MS);
        return input.n;
      });
      const poked = await ctx.race([ctx.signal<number>("poke"), ctx.sleep(HOT_WAIT_MS)]);
      const b = await ctx.run("leave", async () => {
        if (HOT_RUN_MS > 0) await sleep(HOT_RUN_MS);
        hotWatch.leave(key, inv);
        return input.n;
      });
      return { n: input.n, a, b, poked: typeof poked === "number" };
    },
  },
  { type: "VIRTUAL_OBJECT", handlerTypes: { turn: "EXCLUSIVE" } },
);

/**
 * (c) `ChaosBulk.note` — `SHARED` on one key: every invocation of the purge tenant
 * lands in **one** range (partitioning is by object key), without queueing.
 */
const chaosBulk = service(
  "ChaosBulk",
  {
    async note(_ctx: Context, input: { n: number }) {
      return { n: input.n };
    },
  },
  { type: "VIRTUAL_OBJECT", handlerTypes: { note: "SHARED" } },
);

/** (e) Times a `ctx.run` closure of `ChaosPar` threw a transient error (the turn dies, reruns). */
let parTransient = 0;
/** (e) Times a closure threw `TerminalError` (a durable failure, caught by the branch). */
let parTerminal = 0;

/**
 * (e) `ChaosPar.fan` — `CHAOS_PAR_BRANCHES` parallel branches, each with
 * `CHAOS_PAR_STEPS` sequential steps alternating `ctx.run` (even) and `ctx.call` (odd).
 * A `ctx.run` closure throws an ordinary error with probability `CHAOS_PAR_FAIL` — the turn dies,
 * every branch replays from the journal; one call in four has a step that throws
 * `TerminalError` (a durable failure, caught by the branch). Each branch's output is deterministic in
 * `seq` (`parExpected`).
 */
const chaosPar = service("ChaosPar", {
  async fan(ctx: ServiceContext, input: { seq: number }) {
    const branch = async (b: number): Promise<string[]> => {
      const out: string[] = [];
      for (let i = 0; i < PAR_STEPS; i++) {
        if (parIsRun(i)) {
          try {
            out.push(
              await ctx.run(`b${b}.s${i}`, async () => {
                if (Math.random() < PAR_FAIL) {
                  parTransient++;
                  throw new Error(`chaos: transient failure in b${b}.s${i}`);
                }
                if (parTerminalAt(input.seq, b, i, PAR_BRANCHES)) {
                  parTerminal++;
                  throw new TerminalError(`chaos: terminal b${b}.s${i}`);
                }
                return `r${b}.${i}`;
              }),
            );
          } catch (e) {
            if (!(e instanceof TerminalError)) throw e;
            out.push(`T${b}.${i}`);
          }
        } else {
          const r = await ctx.call<{ echo: number }>(
            "ChaosEcho",
            "ping",
            { seq: parEcho(b, i) },
            { key: `par-${input.seq}-${b}` },
          );
          out.push(`e${r.echo}`);
        }
      }
      return out;
    };
    const branches = await Promise.all(Array.from({ length: PAR_BRANCHES }, (_, b) => branch(b)));
    return { seq: input.seq, branches };
  },
});

/** Tripwires that fired: node log lines (panic `failpoint <name> panic`). */
const tripwireHits: string[] = [];
let tripwireArmed = 0;
const tripwireArmFailures: string[] = [];
/**
 * Whether the tripwires are on: `CHAOS_TRIPWIRES`, and the binary knows their names — a binary
 * from before the fix (for example an older tip, used to prove case (a) catches the bug) does
 * not have those two names ⇒ off, one line printed, not an error.
 */
let tripwiresOn: boolean = TRIPWIRES;

/**
 * The most recent time the cluster became healthy again (case (b)): a kill, a node exiting on its
 * own, a revive, a failpoint set/cleared, a slow node, L0 pressure, a store outage, an SDK reopen
 * all move it. The `running`-standing-still watcher only counts from this time.
 */
let healthySince = Date.now();
const noteDisruption = () => {
  healthySince = Date.now();
};

/** A deterministic hex string of length `n` (no characters that need escaping in JSON). */
function lvText(seed: string, n: number): string {
  const parts: string[] = [];
  let len = 0;
  for (let i = 0; len < n; i++) {
    const h = createHash("sha256").update(`${seed}/${i}`).digest("hex");
    parts.push(h);
    len += h.length;
  }
  return parts.join("").slice(0, n);
}
const lvSha = (s: string) => createHash("sha256").update(s).digest("hex");
type LvMeta = { len: number; sha: string; n: number };
type LvOut = { n: number; len: number; corrupt?: string };

/** `--large-values`: an appended history larger than the payload cap — see `LARGE_VALUES`. */
const chaosHistory = service(
  "ChaosHistory",
  {
    async append(ctx: Context, input: { seq: number }): Promise<LvOut> {
      const h = (await ctx.get<string>("h")) ?? "";
      const meta = await ctx.get<LvMeta>("meta");
      let corrupt: string | undefined;
      if (meta === null) {
        if (h !== "") corrupt = `h has ${h.length} bytes but no meta`;
      } else if (h.length !== meta.len) {
        corrupt = `length ${h.length}, meta ${meta.len} (n ${meta.n})`;
      } else if (lvSha(h) !== meta.sha) {
        corrupt = `sha mismatch at length ${h.length} (n ${meta.n})`;
      }
      const reset = h === "" || h.length + LV_APPEND_KB * 1024 > LV_RESET_KB * 1024;
      const base = reset ? lvText(`${ctx.key}/${input.seq}/base`, LV_BASE_KB * 1024) : h;
      const next = base + lvText(`${ctx.key}/${input.seq}`, LV_APPEND_KB * 1024);
      const n = (meta?.n ?? 0) + 1;
      ctx.set("h", next);
      ctx.set("meta", { len: next.length, sha: lvSha(next), n } satisfies LvMeta);
      return corrupt === undefined ? { n, len: next.length } : { n, len: next.length, corrupt };
    },
    async check(ctx: Context): Promise<LvOut> {
      const h = (await ctx.get<string>("h")) ?? "";
      const meta = await ctx.get<LvMeta>("meta");
      if (meta === null) return { n: 0, len: h.length, ...(h === "" ? {} : { corrupt: "h without meta" }) };
      const corrupt =
        h.length !== meta.len ? `length ${h.length}, meta ${meta.len}` : lvSha(h) !== meta.sha ? "sha mismatch" : undefined;
      return corrupt === undefined ? { n: meta.n, len: h.length } : { n: meta.n, len: h.length, corrupt };
    },
  },
  { type: "VIRTUAL_OBJECT", handlerTypes: { append: "EXCLUSIVE", check: "EXCLUSIVE" } },
);

// ───────────────────────────────────────────── real node cluster

type Node = { proc: ChildProcess; url: string; alive: boolean; name: string };
const nodes: Node[] = [];
/** A node of another cluster — only under `CHAOS_INJECT=rogue-leader`. */
let rogue: Node | undefined;
/** Cleaning up at the end of the run: a node exiting now is our doing. */
let stopping = false;
/** Times a node exited on its own (fail-stop) without the harness killing it. */
let selfExits = 0;
/** "D-d: deleting the Db …" log lines seen from every node. */
let gcDeletesLogged = 0;
/** "shedding leadership" log lines. */
let shedsLogged = 0;
/**
 * Tripwire log lines of delegated renewal: a
 * `delegated` node sees its record already ended while its grant is still valid. Must
 * be 0 — together with the metric `cairnflow_lease_delegated_revoked_while_valid_total`.
 */
let delegatedRevokedLogged = 0;
const logFiles = new Map<string, RotatingLog>();

// ── Long soak: early stop, disk, log rotation — see the head of the file.
/**
 * The end of the destructive phase. `main` sets `startedAt + CHAOS_DURATION_S`; [`haltLoad`] pulls
 * it back to "now". Every loop of the destructive phase rereads it on each tick, and
 * [`sleepUntil`] wakes up when the destructive phase is stopped.
 */
let endAt = Number.POSITIVE_INFINITY;
/** Why the destructive phase was stopped early — `undefined` = ran its full duration. */
let haltReason: string | undefined;
let haltedAt: number | undefined;
/**
 * Stop the destructive phase **cleanly**: load, kills, failpoints, … stop at the next tick; the drain,
 * the lost/wrong verdict and the final `verify` still run as in a run that ran out of time, then
 * the verdict prints `STOPPED: <reason>` (exit code 3 if nothing else slipped).
 */
function haltLoad(reason: string) {
  if (haltReason !== undefined) return;
  haltReason = reason;
  haltedAt = Date.now();
  endAt = Math.min(endAt, haltedAt);
  console.error(`  [${ts()}] ⛔ STOPPED: ${reason} — stopping the destructive phase; drain, lost/wrong verdict and final verify still run`);
}
/** Uncaught errors the run survived ([`survivableUncaught`]) — printed in the summary. */
let survivedUncaught = 0;
const isDiskError = (e: unknown) => ["ENOSPC", "EDQUOT"].includes((e as NodeJS.ErrnoException | undefined)?.code ?? "");
/** Caught file write errors (node logs, `.metrics`, `latency.tsv`, …) — printed in the summary. */
let fileWriteErrors = 0;
/** A diagnostic file write failed: count it, print one line, and stop the destructive phase if it is a disk error. */
function noteWriteError(what: string, e: unknown) {
  fileWriteErrors++;
  if (fileWriteErrors <= 20) console.error(`  [${ts()}] writing ${what} failed: ${(e as Error).message}`);
  if (isDiskError(e)) haltLoad(`disk — writing ${what} failed (${(e as NodeJS.ErrnoException).code})`);
}

/** The watched path with the least free space (GiB, as `df -h`); `undefined` if no path could be measured. */
function diskFree(): { path: string; gib: number } | undefined {
  let worst: { path: string; gib: number } | undefined;
  for (const p of DISK_PATHS) {
    // The directory may not exist yet (logs, cache created later): measure at the nearest existing parent.
    let q = p;
    while (!existsSync(q) && dirname(q) !== q) q = dirname(q);
    try {
      const s = statfsSync(q);
      const gib = (Number(s.bavail) * Number(s.bsize)) / 2 ** 30;
      if (worst === undefined || gib < worst.gib) worst = { path: p, gib };
    } catch {
      // Could not measure: skip this path, the others are still watched.
    }
  }
  return worst;
}

/**
 * A node's log file. Every write error has a listener — an earlier soak run died in its
 * 4th hour from `ENOSPC` on a `WriteStream` nobody listened to for `'error'`. Past
 * `CHAOS_LOG_MAX_MB` it rotates: `node-0.log` → `node-0.log.1` → … →
 * `.CHAOS_LOG_KEEP` (the oldest file is overwritten). On a write failure the file stops taking lines (the
 * node's pipe is still read, so the node does not block); a failure from the disk stops the destructive phase.
 */
class RotatingLog {
  readonly path: string;
  private stream: WriteStream | undefined;
  private size = 0;
  /** Bytes written across all rotations. */
  written = 0;
  rotations = 0;
  broken: string | undefined;

  constructor(path: string) {
    this.path = path;
    this.open();
  }

  private open() {
    try {
      this.size = existsSync(this.path) ? statSync(this.path).size : 0;
    } catch {
      this.size = 0;
    }
    const s = createWriteStream(this.path, { flags: "a" });
    s.on("error", (e) => this.fail(e));
    this.stream = s;
  }

  private fail(e: unknown) {
    if (this.broken !== undefined) return;
    // A system error's message already starts with the code (`ENOSPC: no space left…`).
    this.broken = (e as Error).message;
    this.stream = undefined;
    noteWriteError(this.path, e);
  }

  write(chunk: string | Buffer) {
    if (this.broken !== undefined || this.stream === undefined) return;
    const n = typeof chunk === "string" ? Buffer.byteLength(chunk) : chunk.length;
    this.stream.write(chunk);
    this.size += n;
    this.written += n;
    if (LOG_MAX_MB > 0 && this.size >= LOG_MAX_MB * 2 ** 20) this.rotate();
  }

  private rotate() {
    // The old stream still draining keeps writing into the renamed file: the fd does not follow the name.
    this.stream?.end();
    this.stream = undefined;
    try {
      if (LOG_KEEP <= 0) unlinkSync(this.path);
      else {
        // `rename` overwrites the target (POSIX): the old `.KEEP` is replaced.
        for (let i = LOG_KEEP - 1; i >= 1; i--) {
          if (existsSync(`${this.path}.${i}`)) renameSync(`${this.path}.${i}`, `${this.path}.${i + 1}`);
        }
        renameSync(this.path, `${this.path}.1`);
      }
      this.rotations++;
    } catch (e) {
      this.fail(e);
      return;
    }
    this.open();
  }

  end() {
    this.stream?.end();
  }
}

/** Log lines counted for the numbers table: label → substring of the message. */
const LOG_PATTERNS: ReadonlyArray<readonly [string, string]> = [
  ["intent rolled back (child ranges cleaned up)", "rolled back intent"],
  ["split failed — rolled back", "split failed — rolling back"],
  ["merge failed — rolled back", "merge failed — rolling back"],
  ["range with abandoned intent — rolled back", "abandoned split/merge — rolling back"],
  ["tombstone on behalf", "tombstone on behalf: the range left the table"],
  ["tombstone-on-behalf pass failed", "tombstone-on-behalf pass failed"],
  ["D-d deleting Db", "D-d: deleting the Db"],
  // The `ops/` record of a settled op moved to the trash; the D-d checkpoint barrier
  // sees the scan and the checkpoints disagree.
  ["op record cleanup", "op record cleanup: op settled"],
  ["D-d: scan disagrees with checkpoint", "still has a named final checkpoint on it — keeping"],
  ["D-d pass failed", "D-d pass failed"],
  ["shedding leadership", "shedding leadership"],
  ["slow node handed range over", "slow node — range handed over"],
  ["auto-split a range", "auto-split range"],
  ["auto-split did not finish", "auto-split did not finish"],
  ["scrubber: still present after two rounds", "still present after two rounds"],
  ["scrubber failed", "state scrubber failed"],
];
/** Log lines of the state scrubber — printed verbatim at the end of the run. */
const scrubLines: string[] = [];
/** The largest still-present finding ever seen on `/metrics`, per check. */
const scrubPeak = new Map<string, number>();
/**
 * Retired ranges the D-d plan keeps because of the checkpoint barrier
 * — the largest ever seen on `/metrics`, per kind.
 */
const checkpointHeldPeak = new Map<string, number>();
const logCounts = new Map<string, number>();

/**
 * Incarnation drops by cause: each time
 * `session::renew::lose` logs exactly one "session lost — dropping every partition" line
 * with `cause` — at the same place that counts `cairnflow_failstop_total{cause}`. Counted from the log
 * because a line already in the pipe before `kill -9` can still be read, while the `/metrics` counter dies
 * with the process (the last scrape may lag by up to `CHAOS_HARVEST_MS`). The harvested `/metrics`
 * numbers are printed alongside for comparison.
 */
const FAILSTOP_LINE = "session lost — dropping every partition";
const FAILSTOP_CAUSES = [
  "wal_ambiguous", "wal_fenced", "wal_stall", "owner_cas_lost", "dirty_yield", "c1", "session_lost", "session_expired", "other",
] as const;
const failstopLogged = new Map<string, number>();
/**
 * A node's 500 errors hide the detail (the body only has `code <code>`, the detail is only in the log —
 * `ingress/error.rs`): code → detail, from the log line "internal error returned as
 * 500". The split/merge verdict looks it up here to recognise errors injected by the harness itself
 * (`failpoint <name>: chaos`) after the logs have been read in full.
 */
const maskedErrors = new Map<string, string>();
function failstopCause(line: string): string {
  // tracing's text log has colour codes around field names; JSON has `"cause":"…"`.
  // eslint-disable-next-line no-control-regex
  const plain = line.replace(/\x1b\[[0-9;]*m/g, "");
  return /\bcause"?[=:]"?([a-z0-9_]+)/.exec(plain)?.[1] ?? "unknown";
}

/**
 * `/metrics` counters harvested per process (see "Counts" at the head of the file). A line keeps
 * its labels while computing the increase, then drops the `range` label when summing.
 */
const HARVEST_MS = Number(env.CHAOS_HARVEST_MS ?? "2000");
const HARVEST = [
  "cairnflow_testkit_store_requests_total",
  "cairnflow_testkit_store_slowdown_total",
  "cairnflow_rescue_marked_total",
  "cairnflow_rescue_nacked_total",
  "cairnflow_rescue_requeued_total",
  "cairnflow_rescue_round_failed_total",
  "cairnflow_flow_throttled_total",
  "cairnflow_flow_blocked_total",
  "cairnflow_invoke_throttled_total",
  "cairnflow_range_l0_shed_total",
  "cairnflow_range_write_stalls_total",
  "cairnflow_auto_split_total",
  "cairnflow_auto_split_failed_total",
  "cairnflow_quarantine_detected_total",
  "cairnflow_invoke_completed_total",
  "cairnflow_scrub_rounds_total",
  "cairnflow_testkit_store_faults_total",
  // The shared retry / cap primitive, by the `op` label.
  "cairnflow_retry_retries_total",
  "cairnflow_retry_exhausted_total",
  "cairnflow_retry_permanent_total",
  "cairnflow_retry_ambiguous_total",
  // Incarnation drops by cause — compared
  // with the count from the log (`failstopLogged`).
  "cairnflow_failstop_total",
  // `ops/` records cleaned up; disagreements between the D-d scan and checkpoints.
  "cairnflow_gc_ops_reaped_total",
  "cairnflow_gc_proofs_disagree_total",
  // Lineage pass: full compactions submitted by reason, and finished.
  "cairnflow_lineage_compactions_submitted_total",
  "cairnflow_lineage_compactions_total",
  // Tripwire of delegated renewal — must be 0.
  "cairnflow_lease_delegated_revoked_while_valid_total",
  // RAM block cache and disk cache.
  "cairnflow_range_block_cache_hits_total",
  "cairnflow_range_block_cache_misses_total",
  "cairnflow_disk_cache_hits_total",
  "cairnflow_disk_cache_misses_total",
];
/** pid → (line with labels → value at the previous scrape). */
const harvestLast = new Map<number, Map<string, number>>();
/** Line with the `range` label dropped → total increase across all processes. */
const harvested = new Map<string, number>();

function harvestText(pid: number, text: string, names: readonly string[] = HARVEST) {
  let last = harvestLast.get(pid);
  if (!last) harvestLast.set(pid, (last = new Map()));
  for (const line of text.split("\n")) {
    const scrub = /^cairnflow_scrub_findings\{check="([^"]+)"\} (\d+)/.exec(line);
    if (scrub) scrubPeak.set(scrub[1]!, Math.max(scrubPeak.get(scrub[1]!) ?? 0, Number(scrub[2])));
    const held = /^cairnflow_gc_checkpoint_held\{kind="([^"]+)"\} (\d+)/.exec(line);
    if (held) checkpointHeldPeak.set(held[1]!, Math.max(checkpointHeldPeak.get(held[1]!) ?? 0, Number(held[2])));
    if (!names.some((n) => line.startsWith(n) && (line[n.length] === " " || line[n.length] === "{"))) continue;
    const sp = line.lastIndexOf(" ");
    const key = line.slice(0, sp);
    const v = Number(line.slice(sp + 1));
    if (!Number.isFinite(v)) continue;
    const prev = last.get(key);
    const delta = prev === undefined || v < prev ? v : v - prev;
    last.set(key, v);
    if (delta === 0) continue;
    const agg = key.replace(/range="[^"]*",?/, "").replace(/,}/, "}").replace("{}", "");
    harvested.set(agg, (harvested.get(agg) ?? 0) + delta);
  }
}

/**
 * The incarnation-drop-by-cause table — the baseline for
 * the goal "one stray store error does not take down a whole node". The log column is the main number; the
 * `/metrics` column is a lower bound (see `failstopLogged`). Also prints a
 * `FAILSTOP_JSON` line for soaks.
 */
function printFailStops(kills: number) {
  const causes = [...new Set<string>([...FAILSTOP_CAUSES, ...failstopLogged.keys()])];
  const rows = causes.map((c) => ({
    cause: c,
    log: failstopLogged.get(c) ?? 0,
    metrics: harvestedSum("cairnflow_failstop_total", `cause="${c}"`),
  }));
  const total = rows.reduce((a, r) => a + r.log, 0);
  console.log(`fail-stop by cause ("${FAILSTOP_LINE}" log lines; harvested /metrics is a lower bound): ${total} times, besides ${kills} kill -9`);
  for (const r of rows) {
    if (r.log === 0 && r.metrics === 0) continue;
    console.log(`  ${r.cause.padEnd(16)} log ${String(r.log).padStart(5)}   /metrics ${String(r.metrics).padStart(5)}`);
  }
  console.log(
    `FAILSTOP_JSON ${JSON.stringify({ total, kills, log: Object.fromEntries(rows.map((r) => [r.cause, r.log])), metrics: Object.fromEntries(rows.map((r) => [r.cause, r.metrics])) })}`,
  );
}

/** The sum of a harvested metric, filtered by label (substring `k="v"`). */
function harvestedSum(name: string, ...labels: string[]): number {
  return harvestedSumIn(harvested, name, ...labels);
}

/** Like [`harvestedSum`] on a snapshot of the harvest book (the quiet phase takes the difference of two snapshots). */
function harvestedSumIn(m: ReadonlyMap<string, number>, name: string, ...labels: string[]): number {
  let sum = 0;
  for (const [k, v] of m) {
    if (k !== name && !k.startsWith(`${name}{`)) continue;
    if (labels.every((l) => k.includes(l))) sum += v;
  }
  return sum;
}

const ts = () => new Date().toISOString();

/** The harness auth header (bootstrap key, cluster-admin role). */
function authed(init?: RequestInit, key: string | undefined = ADMIN_KEY): RequestInit {
  if (!key) return init ?? {};
  const headers = new Headers(init?.headers);
  if (!headers.has("authorization")) headers.set("authorization", `Bearer ${key}`);
  return { ...init, headers };
}

/** fetch to a specific node, with auth. */
const nfetch = (url: string, init?: RequestInit) => fetch(url, authed(init));

/**
 * Revive node `i` after `RESTART_AFTER_S` seconds. On failure (store down, node exiting
 * during startup) retry, with exponential backoff (×2 each time, capped at 60 s — `--store-outage`
 * makes every start fail until the store is back) — one failure must not leave the cluster
 * without a node until the end of the run. Past `CHAOS_RESPAWN_GIVEUP_S` give up and record a
 * "respawn" violation.
 */
async function respawn(i: number) {
  // One revive per slot: with two overlapping revives (two kill rounds hitting the same
  // process) the later one overwrites `nodes[i]` and the earlier one's process lives
  // on outside `nodes` — nobody kills it, nobody probes it, it piles up ranges until the end of the run
  // (seen in an earlier 4 h soak run).
  if (respawning.has(i)) return;
  respawning.add(i);
  try {
    const since = Date.now();
    for (let attempt = 1; ; attempt++) {
      await sleep(Math.min(60, RESTART_AFTER_S * 2 ** Math.min(attempt - 1, 6)) * 1000);
      if (stopping) return;
      if (nodes[i]?.alive) return;
      try {
        nodes[i] = await spawnNode(i);
        noteDisruption();
        console.log(`  [${ts()}] node ${i} respawned: ${nodes[i]!.url}${attempt > 1 ? ` (attempt ${attempt})` : ""}`);
        return;
      } catch (e) {
        console.error(`  node ${i} could not be respawned (attempt ${attempt}): ${String(e).slice(0, 400)}`);
        if (Date.now() - since > RESPAWN_GIVEUP_S * 1000) {
          respawnGiveUps.push(`${ts()} node ${i}: gave up after ${attempt} attempts in ${Math.round((Date.now() - since) / 1000)}s`);
          return;
        }
      }
    }
  } finally {
    respawning.delete(i);
  }
}
const respawning = new Set<number>();
/** Revive rounds that gave up (past `CHAOS_RESPAWN_GIVEUP_S`) — a violation. */
const respawnGiveUps: string[] = [];

// ── Long store outage — see the head of the file.
/** Inside a store outage window: the kill loop rests. */
let outageActive = false;
/** `docker stop` done but no `docker start` yet — every exit path must start it again. */
let outageStopped = false;
/** Store outage windows (from the stop to the first ack after the store is back) — invariant 6 drops samples overlapping them. */
const outageWindows: Array<{ from: number; to: number }> = [];

/** `docker <args>` with a time cap; returns stdout. */
function docker(args: string[], timeoutMs = 60_000): Promise<string> {
  return new Promise((resolve, reject) => {
    execFile("docker", args, { timeout: timeoutMs }, (err, stdout, stderr) => {
      if (err) reject(new Error(`docker ${args.join(" ")}: ${err.message} ${String(stderr).trim().slice(0, 300)}`));
      else resolve(String(stdout));
    });
  });
}

/** Start the container again if we stopped it and have not started it (abnormal exit, Ctrl-C). Synchronous, best-effort. */
function restoreOutageContainer() {
  if (!outageStopped || !OUTAGE_CONTAINER) return;
  try {
    execFileSync("docker", ["start", OUTAGE_CONTAINER], { timeout: 60_000, stdio: "ignore" });
    outageStopped = false;
    console.error(`  docker-started ${OUTAGE_CONTAINER} again before exiting`);
  } catch (e) {
    console.error(`  could NOT docker-start again ${OUTAGE_CONTAINER}: ${(e as Error).message} — start it by hand`);
  }
}

/** The S3 port answers HTTP (including a `403` for a missing signature) — the same probe as `scripts/dev-store/up.sh`. */
async function storeAnswers(): Promise<boolean> {
  try {
    const r = await fetch(`${env.AWS_ENDPOINT!.replace(/\/$/, "")}/`, { signal: AbortSignal.timeout(3_000) });
    await r.arrayBuffer().catch(() => undefined);
    return r.status < 500;
  } catch {
    return false;
  }
}

/** A latency sample `[from, to]` of `tenant` overlaps one of its non-`active` windows, or a store outage window. */
function excludedSample(tenant: string | undefined, from: number, to: number): boolean {
  const hit = (w: { from: number; to: number }) => from <= w.to && to >= w.from;
  if (outageWindows.some(hit)) return true;
  return tenant !== undefined && nonActiveWindows.some((w) => w.tenant === tenant && hit(w));
}


/**
 * Read stdout + stderr line by line — node logs go to stderr, and a pipe
 * nobody reads fills up (64 KB) and then blocks the process. Count a few meaningful lines,
 * write them to `CHAOS_LOG_DIR` if set.
 */
function drain(proc: ChildProcess, name: string, onLine: (line: string) => void) {
  let file: RotatingLog | undefined;
  if (LOG_DIR) {
    file = logFiles.get(name);
    if (!file) {
      try {
        mkdirSync(LOG_DIR, { recursive: true });
        file = new RotatingLog(join(LOG_DIR, `${name}.log`));
        logFiles.set(name, file);
      } catch (e) {
        // Without a log file still read the pipe (the node must not block), just do not copy.
        noteWriteError(join(LOG_DIR, `${name}.log`), e);
      }
    }
    file?.write(`\n===== ${ts()} new process pid ${proc.pid}\n`);
  }
  for (const stream of [proc.stdout, proc.stderr]) {
    let rest = "";
    // The pipe of a process just hit by `kill -9` has nothing to do — but
    // an `'error'` nobody listens to kills the harness.
    stream?.on("error", () => undefined);
    stream?.on("data", (c: Buffer) => {
      file?.write(c);
      const text = rest + c.toString();
      const lines = text.split("\n");
      rest = lines.pop() ?? "";
      for (const l of lines) onLine(l);
    });
  }
}

/**
 * The cap on waiting for a node to report its port. The boot path retries transient I/O errors until
 * `boot::BOOT_IO_DEADLINE` (120 s), so the cap must be longer.
 */
const BOOT_WAIT_MS = Number(env.CHAOS_BOOT_WAIT_S ?? "150") * 1000;

async function spawnProc(name: string, args: string[]): Promise<{ proc: ChildProcess; url: string }> {
  const proc = spawn(binary, args, { stdio: "pipe", env: NODE_ENV });
  const url = await new Promise<string>((resolve, reject) => {
    const head: string[] = [];
    // Past the cap **kill** the process: each revive gets a new `--node-id`,
    // so a node left behind would live on as an extra node nobody watches.
    const t = setTimeout(() => {
      proc.kill("SIGKILL");
      reject(new Error(`${name} did not report its port after ${BOOT_WAIT_MS / 1000}s:\n${head.join("\n")}`));
    }, BOOT_WAIT_MS);
    let found = false;
    drain(proc, name, (line) => {
      if (head.length < 50) head.push(line);
      if (line.includes("D-d: deleting the Db")) gcDeletesLogged++;
      if (line.includes("shedding leadership")) shedsLogged++;
      if (line.includes("DELEGATION PROTOCOL ERROR")) delegatedRevokedLogged++;
      const masked = maskedErrorOf(line);
      if (masked && maskedErrors.size < 10_000) maskedErrors.set(...masked);
      if (line.includes(FAILSTOP_LINE)) {
        const cause = failstopCause(line);
        failstopLogged.set(cause, (failstopLogged.get(cause) ?? 0) + 1);
      }
      if (line.includes("still present after two rounds") && scrubLines.length < 50) scrubLines.push(`${name}: ${line}`);
      // Tripwires: the `fail` crate's `panic` action prints `failpoint <name> panic`.
      for (const p of TRIPWIRE_POINTS) {
        if (line.includes(`failpoint ${p} panic`) && tripwireHits.length < 50) tripwireHits.push(`${name}: ${line.slice(0, 400)}`);
      }
      for (const [label, needle] of LOG_PATTERNS) {
        if (line.includes(needle)) logCounts.set(label, (logCounts.get(label) ?? 0) + 1);
      }
      const m = /http:\/\/[\d.]+:(\d+)/.exec(line);
      if (m && !found) {
        found = true;
        clearTimeout(t);
        resolve(m[0]!);
      }
    });
    proc.on("exit", (code) => reject(new Error(`${name} exited early (${code}):\n${head.join("\n")}`)));
    // Could not spawn (EAGAIN, ENOMEM, …): an error of this start, the revive
    // loop retries — not an `'error'` nobody listens to killing the harness.
    proc.on("error", (e) => {
      clearTimeout(t);
      reject(new Error(`${name}: ${e.message}`));
    });
  });
  return { proc, url };
}

async function spawnNode(i: number): Promise<Node> {
  const { proc, url } = await spawnProc(
    `node-${i}`,
    nodeArgs(STORE!, { gc: GC }).concat(
      ["--node-id", `chaos-${i}-${Date.now()}`],
      DISK_CACHE ? ["--disk-cache-dir", join(DISK_CACHE, `node-${i}`)] : [],
      DISK_CACHE && DISK_CACHE_BYTES ? ["--disk-cache-bytes", DISK_CACHE_BYTES] : [],
    ),
  );
  const node: Node = { proc, url, alive: true, name: `node ${i}` };
  // The node exited on its own (fail-stop after an injected error, panic) — not killed
  // by us: revive it as after a kill, so the cluster does not wear down.
  proc.on("exit", (code, signal) => {
    if (!node.alive || stopping) return;
    node.alive = false;
    selfExits++;
    noteDisruption();
    console.log(`  [${ts()}] node ${i} exited on its own (${code ?? signal}) — respawning after ${RESTART_AFTER_S}s`);
    void respawn(i);
  });
  for (const [name, action] of STANDING_FAILPOINTS) {
    const r = await setFailpoint(node, name, action).catch((e) => e as Error);
    if (r instanceof Error) console.log(`  could not set ${name} on node ${i}: ${r.message}`);
  }
  // Tripwires: `panic` on every node, revived nodes included. A binary without
  // `failpoints` only counts — the pre-soak cases require setting them (the "tripwire" verdict).
  if (tripwiresOn) {
    for (const name of TRIPWIRE_POINTS) {
      const r = await setFailpoint(node, name, "panic").catch((e) => e as Error);
      if (r instanceof Error) {
        if (tripwireArmFailures.length < 20) tripwireArmFailures.push(`node ${i} ${name}: ${r.message.slice(0, 200)}`);
      } else {
        tripwireArmed++;
      }
    }
  }
  return node;
}

async function setFailpoint(n: Node, name: string, action: string): Promise<void> {
  const r = await nfetch(`${n.url}/admin/failpoints`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ name, action }),
  });
  if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
}

/**
 * The agent workload's verdict for one `completed`: the artifact reads back byte for byte and
 * `$log` has the `stamp` line of `tag`. Returns the reason it is wrong, or `undefined`.
 */
async function agentCheck(id: string, tag: string, seq: number, sha: string | undefined): Promise<string | undefined> {
  if (!sha) return "output has no artifact sha";
  const a = await cfetch(`/invocations/${id}/artifacts/${sha}`).catch((e: unknown) => String(e));
  if (typeof a === "string" || !a.ok) {
    return `artifact ${sha}: ${typeof a === "string" ? a : a.status}`;
  }
  const got = new Uint8Array(await a.arrayBuffer());
  const want = agentBytes(seq);
  if (got.length !== want.length || got.some((b, i) => b !== want[i])) {
    return `artifact ${sha}: ${got.length} bytes, content differs`;
  }
  const l = await cfetch(`/invocations/${id}/logs?limit=1000`).catch((e: unknown) => String(e));
  if (typeof l === "string" || !l.ok) return `logs: ${typeof l === "string" ? l : l.status}`;
  const page = (await l.json()) as { records?: { msg?: string; attrs?: { tag?: string } }[] };
  if (!(page.records ?? []).some((r) => r.msg === "stamp" && r.attrs?.tag === tag)) {
    return `$log has no stamp line for ${tag} (${(page.records ?? []).length} lines)`;
  }
  return undefined;
}

/** A cluster-aware fetch: follows the `leader` field of a 503, backs off when it does not know the way. */
async function cfetch(path: string, init?: RequestInit, timeoutMs = 30_000, key = ADMIN_KEY): Promise<Response> {
  const deadline = Date.now() + timeoutMs;
  let base = nodes.find((n) => n.alive)?.url ?? nodes[0]!.url;
  const req = authed(init, key);
  for (;;) {
    try {
      const r = await fetch(base + path, req);
      if (r.status === 503) {
        const peek = (await r.clone().json().catch(() => undefined)) as
          | { leader?: string }
          | undefined;
        if (Date.now() > deadline) return r;
        if (peek?.leader) {
          base = peek.leader.replace(/\/$/, "");
        } else {
          await sleep(200);
          base = nodes.find((n) => n.alive)?.url ?? base;
        }
        continue;
      }
      if (r.status === 404 && path.startsWith("/invoke/") && Date.now() < deadline) {
        // A node that just restarted has not relearned the deployment registry yet.
        await sleep(200);
        continue;
      }
      return r;
    } catch (e) {
      if (Date.now() > deadline) throw e;
      base = nodes.find((n) => n.alive)?.url ?? base;
      await sleep(200);
    }
  }
}

/**
 * Snapshot the cluster when a synchronous read of the workload does not finish (I4): ask
 * every live node `/invocations/{id}` directly (not following the `leader` of a 503 — to see
 * which node returns what, from the leader or a follower), plus `/scheduler`, `/vqueues`,
 * `/limits`, `/cluster`. Writes it all to `CHAOS_LOG_DIR/stuck-read-<time>.json`;
 * returns a one-line summary.
 */
async function diagnoseStuckRead(what: string, ids: string[], trail: string[]): Promise<string> {
  const at = new Date().toISOString();
  const grab = async (url: string) => {
    try {
      const r = await nfetch(url, { signal: AbortSignal.timeout(5_000) });
      const text = await r.text();
      let body: unknown = text;
      try {
        body = JSON.parse(text);
      } catch {}
      return { status: r.status, source: r.headers.get("x-cairn-read"), body };
    } catch (e) {
      return { status: 0, source: null, body: (e as Error).message };
    }
  };
  const perNode = await Promise.all(
    nodes
      .filter((n) => n.alive)
      .map(async (n) => ({
        node: n.name,
        url: n.url,
        invocations: Object.fromEntries(await Promise.all(ids.map(async (id) => [id, await grab(`${n.url}/invocations/${id}`)] as const))),
        scheduler: await grab(`${n.url}/scheduler`),
        vqueues: await grab(`${n.url}/vqueues`),
        limits: await grab(`${n.url}/limits`),
        cluster: await grab(`${n.url}/cluster`),
      })),
  );
  const dump = { at, what, ids, trail, nodes: perNode };
  const dir = LOG_DIR ?? ".";
  const file = join(dir, `stuck-read-${at.replace(/[:.]/g, "-")}.json`);
  try {
    mkdirSync(dir, { recursive: true });
    writeFileSync(file, JSON.stringify(dump, null, 2));
  } catch (e) {
    noteWriteError(file, e);
  }
  const summary = perNode
    .map((n) => {
      const inv = ids.map((id) => {
        const g = n.invocations[id]!;
        const st = (g.body as { status?: string } | undefined)?.status ?? "";
        return `${id.slice(0, 8)}…=${g.status}${g.source ? `/${g.source}` : ""}${st ? `/${st}` : ""}`;
      });
      return `${n.node}: ${inv.join(" ")}`;
    })
    .join("; ");
  return `snapshot ${file}: ${summary}`;
}

/**
 * `GET /invocations/{id}` through `cfetch`, recording the read in the history. Returns the body
 * on `200`; `undefined` on an error or another status (`404` is still recorded).
 */
async function readStatus(
  id: string,
  timeoutMs = 30_000,
  key = ADMIN_KEY,
  idx?: number,
  onMissing?: () => void,
): Promise<{ status: string; output?: string; endedAt?: number } | undefined> {
  const r = await cfetch(`/invocations/${id}`, undefined, timeoutMs, key).catch(() => undefined);
  if (!r) return undefined;
  const h = r.headers.get("x-cairn-read");
  const source = h === "leader" || h === "follower" ? h : "unknown";
  // `idx` = index in the ack book: the incremental checker; absent (id outside the book) ⇒ keep the read as is.
  const record = (status: string, output?: string) => {
    const t = Date.now();
    if (idx !== undefined) {
      historyTracker.observe(idx, id, output === undefined ? { t, source, status } : { t, source, status, output });
      return;
    }
    observations.push(output === undefined ? { id, t, source, status } : { id, t, source, status, output });
    if (status === "completed" && !looseCompletedSet.has(id)) {
      looseCompletedSet.add(id);
      looseCompletedSeen.push(id);
    }
  };
  if (r.status === 404) {
    record("404");
    onMissing?.();
    return undefined;
  }
  if (!r.ok) return undefined;
  const body = (await r.json().catch(() => undefined)) as { status: string; output?: string; endedAt?: number } | undefined;
  if (!body) return undefined;
  record(body.status, body.output);
  return body;
}

/** Number of detail reads of a `paused` id in check (b), rotating across live nodes. */
const DETAIL_TRIES = Number(env.CHAOS_DETAIL_TRIES ?? "6");

/**
 * One `GET /invocations/{id}` read for check (b): attempt number `attempt` asks
 * live node number `attempt` (round robin), follows the `leader` of a `503` one step, caps
 * each request at 5 s. Errors return the reason instead of being swallowed into `undefined`. Only accepts
 * the leader's copy (`x-cairn-read: leader`).
 */
async function readDetail(id: string, attempt: number): Promise<DetailRead> {
  const alive = nodes.filter((n) => n.alive);
  const pool = alive.length > 0 ? alive : nodes;
  let base = pool[attempt % pool.length]!.url;
  for (let hop = 0; hop < 2; hop++) {
    const r = await fetch(`${base}/invocations/${id}`, { ...authed(), signal: AbortSignal.timeout(5_000) });
    const text = await r.text();
    let body: any;
    try {
      body = JSON.parse(text);
    } catch {
      return { ok: false, why: `${r.status} body is not JSON: ${text.slice(0, 120)}` };
    }
    // The follower copy is durable state but may be stale (`running` for an
    // invocation the leader already made `paused`): treat it as not read yet, try next time.
    if (r.ok && r.headers.get("x-cairn-read") === "follower") {
      return { ok: false, why: "only the follower copy could be read (may be stale)" };
    }
    if (r.ok) return { ok: true, body };
    if (r.status === 503 && typeof body?.leader === "string" && hop === 0) {
      base = body.leader.replace(/\/$/, "");
      continue;
    }
    return { ok: false, why: `${r.status} ${body?.error ?? text.slice(0, 120)}` };
  }
  return { ok: false, why: "503 following the leader is still 503" };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * Sleep until `ms` is over or until `until`, whichever comes first — and wake right away when
 * the destructive phase is stopped early ([`haltLoad`]): a 300 s `verify` loop or a 300 s tenant window
 * must not hold the run back once the disk is low.
 */
async function sleepUntil(ms: number, until: number) {
  const end = Math.min(Date.now() + ms, until);
  for (;;) {
    const left = Math.min(end, haltedAt ?? Number.POSITIVE_INFINITY) - Date.now();
    if (left <= 0) return;
    await sleep(Math.min(left, 1_000));
  }
}

/** [`cfetch`] for setup calls: retries `5xx` and network errors (injected store faults). */
const setupFetch = (...args: Parameters<typeof cfetch>): Promise<Response> =>
  retryServerErrors(() => cfetch(...args), 6, (ms) => sleep(ms).then(() => undefined));

const aliveNodes = () => nodes.filter((n) => n.alive);
const pick = <T>(xs: readonly T[]): T => xs[Math.floor(Math.random() * xs.length)]!;

/** Run `cairn-ctl --json <args>` on the cluster's bucket; exit code 0 or 1 (verify found problems) both carry JSON. */
async function ctl<T>(args: string[], timeoutMs = 300_000): Promise<T> {
  const proc = spawn(CTL, ["--main-store", STORE!, "--json", ...args], { stdio: "pipe", env });
  let out = "";
  let err = "";
  proc.stdout!.on("data", (c: Buffer) => (out += c.toString()));
  proc.stderr!.on("data", (c: Buffer) => (err += c.toString()));
  const t = setTimeout(() => proc.kill("SIGKILL"), timeoutMs);
  const code = await new Promise<number | null>((r) => {
    proc.on("exit", r);
    // Could not spawn: `exit` may never come.
    proc.on("error", (e) => {
      err += `\n${e.message}`;
      r(null);
    });
  });
  clearTimeout(t);
  if (code !== 0 && code !== 1) {
    throw new Error(`cairn-ctl ${args.join(" ")} exited ${code}: ${err.trim().slice(0, 500)}`);
  }
  return JSON.parse(out) as T;
}

/**
 * Run `cairn-ctl --main-store <store> --json <args>`, returning the exit code and the raw
 * output — the caller decides (backup/restore case (d): code 3 is a valid
 * "refusal", code 1 of `verify` carries JSON).
 */
async function ctlRun(
  store: string,
  args: string[],
  timeoutMs: number,
): Promise<{ code: number | null; signal: string | null; timedOut: boolean; out: string; err: string }> {
  const proc = spawn(CTL, ["--main-store", store, "--json", ...args], { stdio: "pipe", env });
  let out = "";
  let err = "";
  let timedOut = false;
  proc.stdout!.on("data", (c: Buffer) => (out += c.toString()));
  proc.stderr!.on("data", (c: Buffer) => (err += c.toString()));
  const t = setTimeout(() => {
    timedOut = true;
    proc.kill("SIGKILL");
  }, timeoutMs);
  const [code, signal] = await new Promise<[number | null, string | null]>((r) => {
    proc.on("exit", (c, sig) => r([c, sig]));
    proc.on("error", (e) => {
      err += `\n${e.message}`;
      r([null, null]);
    });
  });
  clearTimeout(t);
  return { code, signal, timedOut, out, err };
}

/** The aws CLI against the run's store (SeaweedFS's `AWS_ENDPOINT`). */
function awsCli(args: string[], timeoutMs = 600_000): Promise<string> {
  return new Promise<string>((resolve, reject) =>
    execFile("aws", ["--endpoint-url", env.AWS_ENDPOINT ?? "", ...args], { timeout: timeoutMs, env }, (e, out, err) =>
      e ? reject(new Error(`aws ${args.slice(0, 3).join(" ")}: ${String(err).trim().slice(0, 300) || e.message}`)) : resolve(out),
    ),
  );
}

/**
 * Read the `tenant` line of the `<prefix>/tenants` registry on the bucket directly (case (c)) —
 * the referee when `GET /admin/tenants/{t}/purge` fails or exceeds its cap. `cairn-ctl` has no
 * command that reads the tenant registry; `s3://` only.
 */
async function readTenantBook(tenant: string): Promise<BookRead> {
  if (!STORE?.startsWith("s3://")) return { ok: false, why: `${STORE} is not s3://` };
  const src = new URL(STORE);
  const prefix = src.pathname.replace(/^\/+|\/+$/g, "");
  const key = prefix === "" ? "tenants" : `${prefix}/tenants`;
  try {
    return parseTenantBook(await awsCli(["s3", "cp", `s3://${src.hostname}/${key}`, "-"], 60_000), tenant);
  } catch (e) {
    return { ok: false, why: (e as Error).message };
  }
}

/**
 * `PUT` a confirmed registry write (`registry_sync::confirms`: tenant
 * state, `/admin/oidc…`) until `204`. `202 {pending}` = some node has not applied it yet ⇒ resend
 * the very same command (idempotent). Returns the time of the `204`, or `undefined` past the cap;
 * `onStatus` lets the caller handle other statuses (return `true` = retry right away).
 */
async function putConfirmed(
  path: string,
  body: unknown,
  timeoutMs: number,
  onStatus?: (status: number, text: string) => Promise<boolean>,
): Promise<{ at?: number; pending: number; last: string }> {
  const deadline = Date.now() + timeoutMs;
  let pending = 0;
  let last = "not sent";
  while (Date.now() < deadline) {
    const r = await cfetch(
      path,
      { method: "PUT", headers: { "content-type": "application/json" }, body: JSON.stringify(body) },
      Math.min(30_000, Math.max(1_000, deadline - Date.now())),
    ).catch((e: unknown) => String(e));
    if (typeof r === "string") {
      last = r.slice(0, 200);
      await sleep(500);
      continue;
    }
    const text = await r.text().catch(() => "");
    last = `${r.status} ${text.slice(0, 200)}`;
    if (r.status === 202) {
      // The body's keys come `listed`, `note`, `pending`: past the cut above `pending` never
      // shows — name the nodes that have not confirmed explicitly.
      const v = (() => {
        try {
          return JSON.parse(text) as { pending?: unknown; listed?: unknown };
        } catch {
          return undefined;
        }
      })();
      if (v) last = `202 pending=${JSON.stringify(v.pending)} listed=${String(v.listed)}`;
    }
    if (r.status === 204) return { at: Date.now(), pending, last };
    if (r.status === 202) pending++;
    else if (onStatus && (await onStatus(r.status, text))) continue;
    await sleep(r.status === 202 ? 500 : 1_000);
  }
  return { pending, last };
}

/** Tenant state changes that reached `204`, by `old→new`; the `202`s met along the way. */
const tenantTransitions = new Map<string, number>();
let tenantStatePending = 0;
const tenantStateProblems: string[] = [];
let tenantStateGateEnabled = false;

/** `PUT /admin/tenants/{t}/state` until `204`. `409 tenant_state_disabled` ⇒ enable the gate with `cairn-ctl` once. */
async function putTenantState(tenant: string, state: TenantStateName, timeoutMs: number): Promise<number | undefined> {
  const r = await putConfirmed(`/admin/tenants/${tenant}/state`, { state }, timeoutMs, async (status, text) => {
    if (status !== 409 || !text.includes("tenant_state_disabled")) return false;
    if (tenantStateGateEnabled || !HAS_CTL) throw new Error(`the tenants/state gate is off and could not be enabled: ${text.slice(0, 200)}`);
    tenantStateGateEnabled = true;
    const rep = await ctl<unknown>(["cluster", "enable-tenant-state", "--apply"]);
    console.log(`  [${ts()}] tenants/state gate: enabled with cairn-ctl (${JSON.stringify(rep).slice(0, 200)})`);
    return true;
  });
  tenantStatePending += r.pending;
  if (r.at === undefined) {
    const line = `${ts()} ${tenant} → ${state}: no 204 within ${timeoutMs / 1000}s (last: ${r.last})`;
    // Printed now, not only in the verdict: a run that dies midway still shows it.
    console.error(`  TENANT STATE: ${line}`);
    tenantStateProblems.push(line);
  }
  return r.at;
}

type Finding = { check: string; range: number | null; detail: string };
type VerifyReport = { rangesChecked: number; rangeDirs: number; externalRefsChecked?: number; findings: Finding[] };
type RetiredView = { range: number; dbExists: boolean; referencedBy?: string | null };
type WalLogView = {
  log: string;
  sessionState?: string | null;
  floor: number;
  objects: number;
  pinnedByRange?: number | null;
  /** Seq of the seal (the log has been sealed) — `null` when not sealed yet. */
  sealedAt?: number | null;
  /** The log has been fully cleaned up (`forgotten` mark) — absent from older `cairn-ctl`. */
  forgotten?: boolean;
};

/** One row of `cairn-ctl --json owners` (the part the harness reads). */
type OwnerView = {
  range: number;
  inTable: boolean;
  session?: string | null;
  sessionState?: string | null;
  released?: boolean;
  retired?: boolean;
  /** `"<node>/<inc>:<stripe>@<first wal id>"`. */
  walSegments?: string[];
};

/**
 * `op:class` → number of requests, summed over every `tier`, from `cairnflow_testkit_store_requests_total`;
 * plus `src|<source>|op:class/object` from `cairnflow_testkit_store_requests_by_source_total`
 * (the caller — the `io_source` label; older binaries do not have it).
 */
type StoreCounts = Map<string, number>;
function storeCountsOf(text: string): StoreCounts | undefined {
  const name = "cairnflow_testkit_store_requests_total{";
  const bySource = "cairnflow_testkit_store_requests_by_source_total{";
  let out: StoreCounts | undefined;
  for (const line of text.split("\n")) {
    const plain = line.startsWith(name);
    if (!plain && !line.startsWith(bySource)) continue;
    const op = /op="([^"]*)"/.exec(line)?.[1];
    const cls = /class="([^"]*)"/.exec(line)?.[1];
    const src = /source="([^"]*)"/.exec(line)?.[1];
    const obj = /object="([^"]*)"/.exec(line)?.[1];
    const v = Number(line.slice(line.lastIndexOf(" ") + 1));
    if (!op || !cls || !Number.isFinite(v) || (!plain && !src)) continue;
    const key = plain ? `${op}:${cls}` : `src|${src}|${op}:${cls}${obj ? `/${obj}` : ""}`;
    out ??= new Map();
    out.set(key, (out.get(key) ?? 0) + v);
  }
  return out;
}

/** The last scrape of each node process (keyed by the `Node` object: each revive makes a new one). */
const storeTally = new Map<Node, StoreCounts>();
let scrapeMisses = 0;
async function scrapeStore(n: Node): Promise<void> {
  const text = await nfetch(`${n.url}/metrics`, { signal: AbortSignal.timeout(2_000) })
    .then((r) => r.text())
    .catch(() => undefined);
  const c = text === undefined ? undefined : storeCountsOf(text);
  if (c) storeTally.set(n, c);
  else scrapeMisses++;
  if (text !== undefined) dumpMetrics(n, text);
}

/** Copy a node's raw `/metrics` into `<node>.metrics` at the `CHAOS_METRICS_DUMP_S` cadence. */
function dumpMetrics(n: Node, text: string): void {
  if (!LOG_DIR || METRICS_DUMP_EVERY_S <= 0) return;
  const last = metricsDumpedAt.get(n.name) ?? 0;
  if (Date.now() - last < METRICS_DUMP_EVERY_S * 1000) return;
  metricsDumpedAt.set(n.name, Date.now());
  const file = join(LOG_DIR, `${n.name.replace(" ", "-")}.metrics`);
  try {
    appendFileSync(file, `# ===== ${ts()} ${n.url}\n${text}`);
  } catch (e) {
    noteWriteError(file, e);
  }
}
/// `CHAOS_METRICS_DUMP_S` (needs `CHAOS_LOG_DIR`): copy every node's raw `/metrics`
/// every this many seconds into `<node>.metrics` — for diagnosis after a long run
/// (done by the `/metrics` harvester, so it runs even without a store profile).
const METRICS_DUMP_EVERY_S = Number(env.CHAOS_METRICS_DUMP_S ?? "0");
/// `CHAOS_HARNESS_EVERY_S`: cadence for printing the harness's event-loop delay, GC and memory (0 = off).
const HARNESS_EVERY_S = Number(env.CHAOS_HARNESS_EVERY_S ?? "0");
const metricsDumpedAt = new Map<string, number>();

/** The largest value of a metric (any labels) in a `/metrics` text. */
function metricMax(text: string, name: string): number {
  let max = 0;
  for (const line of text.split("\n")) {
    if (!line.startsWith(name)) continue;
    const rest = line.slice(name.length);
    if (rest[0] !== " " && rest[0] !== "{") continue;
    const v = Number(line.slice(line.lastIndexOf(" ") + 1));
    if (Number.isFinite(v) && v > max) max = v;
  }
  return max;
}

const quantile = (xs: number[], q: number) => {
  if (xs.length === 0) return 0;
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor(q * s.length))]!;
};

/** Every line of a metric in a `/metrics` text: the label part (`{…}` or `""`) → value. */
function metricLines(text: string, name: string): Array<[string, number]> {
  const out: Array<[string, number]> = [];
  for (const line of text.split("\n")) {
    if (!line.startsWith(name)) continue;
    const rest = line.slice(name.length);
    if (rest[0] !== " " && rest[0] !== "{") continue;
    const sp = line.lastIndexOf(" ");
    const v = Number(line.slice(sp + 1));
    if (Number.isFinite(v)) out.push([line.slice(name.length, sp), v]);
  }
  return out;
}

const BY_SOURCE = "cairnflow_testkit_store_requests_by_source_total";

/**
 * The quiet phase — see the head of the file. The cluster is still alive, no
 * load, no kills, no failpoints for `IDLE_TAIL_S` seconds; the harvester scrapes
 * every live node's `/metrics` as during the run (plus requests by source from the store
 * profile), the phase's numbers are the difference of two harvest snapshots at the start and the end. Raw
 * `/metrics` of every node at the start and the end of the phase are copied to `idle-{start,end}-<node>.metrics`.
 */
async function idleTail(injectedNames: readonly string[]): Promise<{ summary: string; problems: Map<string, string[]> }> {
  const problems = new Map<string, string[]>();
  const fail = (what: string, items: string[]) => {
    if (items.length > 0) problems.set(what, [...(problems.get(what) ?? []), ...items]);
  };
  const names = [...HARVEST, BY_SOURCE];
  const aliveBy = Date.now() + 60_000;
  while (aliveNodes().length < nodes.length && Date.now() < aliveBy) await sleep(500);
  // No failpoint left on: clear every point ever set, on every live node.
  for (const n of aliveNodes()) {
    for (const name of injectedNames) await setFailpoint(n, name, "off").catch(() => undefined);
  }
  if (IDLE_GRACE_S > 0) {
    const graceFrom = new Map(failstopLogged);
    console.log(`\n[${ts()}] quiet phase: ${IDLE_GRACE_S}s grace before the start mark (the tail of the last kill/failpoint — not measured)`);
    await sleep(IDLE_GRACE_S * 1000);
    const inGrace = [...failstopLogged].filter(([c, n]) => n > (graceFrom.get(c) ?? 0));
    if (inGrace.length > 0) {
      console.log(`  fail-stop during the grace (print only): ${inGrace.map(([c, n]) => `${c} ${n - (graceFrom.get(c) ?? 0)}`).join(", ")}`);
    }
  }
  type Shot = { node: Node; pid: number | undefined; text: string };
  const snapshot = (): Promise<Shot[]> =>
    Promise.all(
      aliveNodes().map(async (n) => ({
        node: n,
        pid: n.proc.pid,
        text: await nfetch(`${n.url}/metrics`, { signal: AbortSignal.timeout(10_000) })
          .then((r) => (r.ok ? r.text() : ""))
          .catch(() => ""),
      })),
    );
  const harvest = (shots: Shot[]) => {
    for (const s of shots) {
      if (!s.text || s.pid === undefined) continue;
      harvestText(s.pid, s.text, names);
      dumpMetrics(s.node, s.text);
    }
    const alive = new Set(aliveNodes().map((n) => n.proc.pid));
    for (const pid of harvestLast.keys()) if (!alive.has(pid)) harvestLast.delete(pid);
  };
  const keep = (tag: string, shots: Shot[]) => {
    if (!LOG_DIR) return;
    for (const s of shots) {
      const file = join(LOG_DIR, `idle-${tag}-${s.node.name.replace(" ", "-")}.metrics`);
      try {
        writeFileSync(file, `# ===== ${ts()} ${s.node.url} pid ${s.pid}\n${s.text}`);
      } catch (e) {
        noteWriteError(file, e);
      }
    }
  };
  const depthOf = (shots: Shot[]) => {
    const m = new Map<string, number>();
    for (const s of shots) {
      for (const [labels, v] of metricLines(s.text, "cairnflow_range_external_depth")) {
        const r = /range="([^"]+)"/.exec(labels)?.[1];
        if (r !== undefined) m.set(r, Math.max(m.get(r) ?? 0, v));
      }
    }
    return m;
  };
  const fmtDepth = (m: Map<string, number>) =>
    [...m].sort((a, b) => Number(a[0]) - Number(b[0])).map(([r, d]) => `${r}:${d}`).join(" ") || "—";

  const startShots = await snapshot();
  harvest(startShots);
  keep("start", startShots);
  const before = new Map(harvested);
  const failstopBefore = new Map(failstopLogged);
  const exitsBefore = selfExits;
  const from = Date.now();
  const until = from + IDLE_TAIL_S * 1000;
  console.log(`\n[${ts()}] quiet phase: ${IDLE_TAIL_S}s, ${aliveNodes().length}/${nodes.length} nodes alive, no load/kill/failpoint`);
  console.log(`  external_dbs depth at phase start (range:depth): ${fmtDepth(depthOf(startShots))}`);
  const tick = HARVEST_MS > 0 ? HARVEST_MS : 5_000;
  let nextNote = from + 600_000;
  // Machine watch: the nodes and the harness share one machine, so a stall of the whole machine (CPU
  // contention, swap) delays every process at once — chaos's 1 500 ms lease
  // expires without the store being slow. If the harness's 1 s tick is more than 500 ms late, print the
  // load average too; at the end of the phase print the largest stall next to the fail-stop row.
  const host = { stalls: 0, maxMs: 0, maxAt: "", maxLoad: 0 };
  const hostWatch = (async () => {
    while (Date.now() < until && haltReason === undefined) {
      const t0 = performance.now();
      await sleep(1_000);
      const late = performance.now() - t0 - 1_000;
      const load = loadavg()[0] ?? 0;
      host.maxLoad = Math.max(host.maxLoad, load);
      if (late > host.maxMs) {
        host.maxMs = late;
        host.maxAt = ts();
      }
      if (late > 500) {
        host.stalls++;
        console.log(`  [${ts()}] machine stall: the harness's 1 s tick is ${Math.round(late)} ms late; load average ${loadavg().map((l) => l.toFixed(1)).join(" ")}`);
      }
    }
  })();
  while (Date.now() < until) {
    if (haltReason !== undefined) {
      fail("quiet phase", [`stopped early during the quiet phase: ${haltReason}`]);
      break;
    }
    await sleep(Math.min(tick, Math.max(0, until - Date.now())));
    harvest(await snapshot());
    if (Date.now() >= nextNote) {
      nextNote += 600_000;
      console.log(`  [${ts()}] quiet phase: ${Math.round((Date.now() - from) / 1000)}/${IDLE_TAIL_S}s, ${aliveNodes().length} nodes alive`);
    }
  }
  await hostWatch;
  let endShots = await snapshot();
  harvest(endShots);
  const windowS = (Date.now() - from) / 1000;
  const delta = (name: string, ...labels: string[]) =>
    harvestedSum(name, ...labels) - harvestedSumIn(before, name, ...labels);
  const perHour = (n: number) => (n * 3_600) / Math.max(1, windowS);
  const rows: string[] = [];
  const row = (ok: boolean, what: string, text: string) => {
    rows.push(`  ${ok ? "✅" : "❌"} ${what}: ${text}`);
    if (!ok) fail(`quiet phase: ${what}`, [text]);
  };

  // (1) The depth of every live range. A range without a number yet (its leader just reopened) ⇒ ask again a few times.
  const table = await rangesOf((p, i) => cfetch(p, i)).catch(() => [] as RangeRow[]);
  let depth = depthOf(endShots);
  for (let k = 0; k < 10 && table.some((r) => !depth.has(String(r.range))); k++) {
    await sleep(2_000);
    endShots = await snapshot();
    depth = depthOf(endShots);
  }
  keep("end", endShots);
  console.log(`  external_dbs depth at phase end (range:depth): ${fmtDepth(depth)}`);
  const missing = table.filter((r) => !depth.has(String(r.range))).map((r) => r.range);
  const deep = table.filter((r) => (depth.get(String(r.range)) ?? 0) > IDLE_MAX_DEPTH);
  const maxDepth = Math.max(0, ...table.map((r) => depth.get(String(r.range)) ?? 0));
  // The gauge reads the writer's **in-RAM** manifest: a detach by SlateDB's GC writes the
  // manifest on the bucket, which the writer only sees at its manifest poll (1 h) or when
  // it reopens — the gauge can only be **higher** than the truth (the quiet phase does not split, so no
  // entries are added). The truth on the bucket: `verify --no-open` counts every
  // `external_dbs` entry of every live range; total ≤ cap ⇒ no range is over the cap.
  // Runs after the last harvest, so its requests do not count toward the phase.
  let bucketRefs: number | undefined;
  if (HAS_CTL) {
    bucketRefs = await ctl<VerifyReport>(["verify", "--no-open"])
      .then((r) => r.externalRefsChecked)
      .catch(() => undefined);
  }
  const gaugeOk = table.length > 0 && missing.length === 0 && deep.length === 0;
  const bucketOk = table.length > 0 && bucketRefs !== undefined && bucketRefs <= IDLE_MAX_DEPTH;
  row(
    gaugeOk || bucketOk,
    "depth",
    table.length === 0
      ? "could not read the range table"
      : `${table.length} live ranges; bucket (verify --no-open): ${bucketRefs ?? "?"} external_dbs entries over all ranges; ` +
          `gauge (writer's in-RAM manifest) deepest ${maxDepth} (cap ${IDLE_MAX_DEPTH})` +
          (deep.length ? `; gauge over cap: ${deep.map((r) => `${r.range}:${depth.get(String(r.range))}`).join(" ")}` : "") +
          (missing.length ? `; gauge has no number: ranges ${missing.join(", ")}` : "") +
          (bucketOk && !gaugeOk ? " — gauge lags behind the detach on the bucket" : ""),
  );

  // The phase's requests by source (store profile of a `testkit` binary).
  const measured = [...harvested.keys()].some((k) => k.startsWith(`${BY_SOURCE}{`));
  const breakdown = (source: string) => {
    const m = new Map<string, number>();
    for (const [k, v] of harvested) {
      if (!k.startsWith(`${BY_SOURCE}{`) || !k.includes(`source="${source}"`)) continue;
      const d = v - (before.get(k) ?? 0);
      if (d <= 0) continue;
      const op = /op="([^"]*)"/.exec(k)?.[1];
      const obj = /object="([^"]*)"/.exec(k)?.[1];
      const key = `${op}/${obj}`;
      m.set(key, (m.get(key) ?? 0) + d);
    }
    return [...m].sort((a, b) => b[1] - a[1]).map(([k, v]) => `${k} ${v}`).join(", ") || "—";
  };
  if (measured) {
    const bySrc = new Map<string, number>();
    for (const [k, v] of harvested) {
      if (!k.startsWith(`${BY_SOURCE}{`)) continue;
      const src = /source="([^"]*)"/.exec(k)?.[1] ?? "?";
      const d = v - (before.get(k) ?? 0);
      if (d > 0) bySrc.set(src, (bySrc.get(src) ?? 0) + d);
    }
    const all = [...bySrc.values()].reduce((a, b) => a + b, 0);
    console.log(`  quiet-phase requests by source (${Math.round(windowS)}s; total ${all} = ${Math.round(perHour(all))}/h):`);
    for (const [src, n] of [...bySrc].sort((a, b) => b[1] - a[1]).slice(0, 15)) {
      console.log(`    ${src.padEnd(24)} ${String(n).padStart(8)}  ${String(Math.round(perHour(n))).padStart(8)}/h`);
    }
  }

  // (2a) Scrubber: the phase's requests per hour ≤ budget (plus one round at the window edge).
  const gaugeMax = (name: string) => Math.max(0, ...endShots.map((s) => metricMax(s.text, name)));
  const scrubRounds = delta("cairnflow_scrub_rounds_total");
  const roundReq = gaugeMax("cairnflow_scrub_round_requests");
  const ages = endShots.flatMap((s) => metricLines(s.text, "cairnflow_scrub_last_complete_age_seconds").map(([, v]) => v));
  const scrubInfo =
    `${scrubRounds} rounds; latest round ${roundReq} requests, waited ${gaugeMax("cairnflow_scrub_wait_seconds")}s, ` +
    `latest full round ${ages.length ? `${Math.min(...ages)}s` : "—"} ago`;
  const scrubReq = delta(BY_SOURCE, 'source="scrub"');
  if (SCRUB_EVERY_S <= 0) {
    console.log("  scrubber off (CHAOS_SCRUB_EVERY_S=0) — not judged");
  } else if (!measured) {
    row(false, "scrubber", "no cairnflow_testkit_store_requests_by_source_total (binary without testkit or no store profile) — cannot measure");
  } else {
    const slack = perHour(roundReq);
    row(
      SCRUB_BUDGET > 0 && scrubReq > 0 && perHour(scrubReq) <= SCRUB_BUDGET + slack,
      "scrubber",
      `${scrubReq} requests = ${Math.round(perHour(scrubReq))}/h; budget ${SCRUB_BUDGET}/h` +
        `${SCRUB_BUDGET === 0 ? " (0 = no cap)" : ""} + one round at the window edge ${Math.round(slack)}/h; ${scrubInfo}` +
        (scrubReq === 0 ? " — no round ran: green for the wrong reason" : ""),
    );
    console.log(`    scrubber by op/object: ${breakdown("scrub")}`);
  }

  // (2b) D-d: requests per hour ≤ the reference budget, and the cost per pass does not follow
  // the tombstone count (split history) — D-d has no request budget flag.
  const tombstones = gaugeMax("cairnflow_owner_tombstones");
  const retiredDbs = gaugeMax("cairnflow_retired_dbs");
  const ddReq = delta(BY_SOURCE, 'source="gc.dbs"');
  if (!GC) {
    console.log("  D-d off (no --gc) — not judged");
  } else if (!measured) {
    row(false, "D-d", "no cairnflow_testkit_store_requests_by_source_total — cannot measure");
  } else {
    const owners = delta(BY_SOURCE, 'source="gc.dbs"', 'object="owners"');
    const passes = Math.max(1, windowS / Math.max(1, GC_EVERY_S));
    row(
      ddReq > 0 && perHour(ddReq) <= IDLE_DD_BUDGET,
      "D-d within budget",
      `${ddReq} requests = ${Math.round(perHour(ddReq))}/h (≈ ${Math.round(ddReq / passes)}/pass, ${passes.toFixed(0)} passes); ` +
        `reference budget ${IDLE_DD_BUDGET}/h (1 000 × 3 600 / --gc-every-s ${GC_EVERY_S}; D-d has no budget flag)` +
        (ddReq === 0 ? " — no pass ran: green for the wrong reason" : ""),
    );
    const ownersPerPass = owners / passes;
    const grows = tombstones > 0 && ownersPerPass >= 0.5 * tombstones;
    row(
      !grows,
      "D-d independent of split history",
      `${ownersPerPass.toFixed(1)} owners requests per pass with ${tombstones} tombstones (${retiredDbs} still with a Db), ${table.length} live ranges ` +
        `(≈ ${Math.round(ownersPerPass * 6)}/h at the default cadence --gc-every-s 600, ${Math.round(ownersPerPass * (3_600 / Math.max(1, GC_EVERY_S)))}/h at cadence ${GC_EVERY_S} s) — ` +
        (grows ? "cost per pass grows with the tombstone count, no cap stops it" : "does not grow with the tombstone count"),
    );
    console.log(`    D-d by op/object: ${breakdown("gc.dbs")}`);
    for (const src of ["gc.orphans", "gc.trash", "lineage.compact"]) {
      const m = delta(BY_SOURCE, `source="${src}"`);
      if (m > 0) console.log(`    ${src} (print only): ${m} = ${Math.round(perHour(m))}/h — ${breakdown(src)}`);
    }
  }

  // (3) Fail-stop by cause and lineage compactions by reason: the whole run
  // (log + harvested /metrics, a lower bound) and the quiet phase alone. The quiet phase injects no errors,
  // so an incarnation drop during the phase is a violation.
  const causes = [...new Set<string>([...FAILSTOP_CAUSES, ...failstopLogged.keys()])];
  const idleFailstops = causes
    .map((c) => [c, (failstopLogged.get(c) ?? 0) - (failstopBefore.get(c) ?? 0), delta("cairnflow_failstop_total", `cause="${c}"`)] as const)
    .filter(([, l, m]) => l > 0 || m > 0);
  console.log("  fail-stop by cause (whole run: log / harvested metrics, lower bound; quiet phase: log / metrics):");
  for (const c of causes) {
    const log = failstopLogged.get(c) ?? 0;
    const met = harvestedSum("cairnflow_failstop_total", `cause="${c}"`);
    const inIdle = idleFailstops.find(([x]) => x === c);
    console.log(
      `    ${c.padEnd(16)} ${String(log).padStart(5)} / ${String(met).padStart(5)}   quiet phase ${inIdle ? `${inIdle[1]} / ${inIdle[2]}` : "0 / 0"}`,
    );
  }
  const idleExits = selfExits - exitsBefore;
  const hostNote =
    `machine: ${host.stalls} times the harness's 1 s tick was > 500 ms late, largest ${Math.round(host.maxMs)} ms` +
    `${host.maxAt ? ` (${host.maxAt})` : ""}, largest 1-minute load average ${host.maxLoad.toFixed(1)}`;
  row(
    idleFailstops.length === 0 && idleExits === 0,
    "fail-stop",
    (idleFailstops.length === 0 && idleExits === 0
      ? "0 times during the quiet phase"
      : `${idleExits} times a node exited on its own; ${idleFailstops.map(([c, l, m]) => `${c} ${l}/${m}`).join(", ")}`) + `; ${hostNote}`,
  );
  const reasons = ["depth", "ancestor_age"];
  const submitted = (r: string) => harvestedSum("cairnflow_lineage_compactions_submitted_total", `reason="${r}"`);
  console.log(
    `  lineage compactions submitted by reason (whole run, lower bound): ` +
      reasons.map((r) => `${r} ${submitted(r)} (quiet phase ${delta("cairnflow_lineage_compactions_submitted_total", `reason="${r}"`)})`).join(", ") +
      `; finished ${harvestedSum("cairnflow_lineage_compactions_total")} (quiet phase ${delta("cairnflow_lineage_compactions_total")})`,
  );
  console.log("quiet phase verdict:");
  for (const r of rows) console.log(r);
  console.log(
    `IDLE_JSON ${JSON.stringify({
      windowS: Math.round(windowS),
      ranges: table.length,
      depth: Object.fromEntries(depth),
      maxDepth,
      bucketExternalRefs: bucketRefs ?? null,
      scrub: measured
        ? { requests: scrubReq, perHour: Math.round(perHour(scrubReq)), budget: SCRUB_BUDGET, rounds: scrubRounds, roundRequests: roundReq }
        : null,
      dd: measured && GC ? { requests: ddReq, perHour: Math.round(perHour(ddReq)), budget: IDLE_DD_BUDGET, tombstones, retiredDbs } : null,
      failstopIdle: Object.fromEntries(idleFailstops.map(([c, l, m]) => [c, { log: l, metrics: m }])),
      selfExitsIdle: idleExits,
      host: { stalls: host.stalls, maxMs: Math.round(host.maxMs), maxAt: host.maxAt, maxLoad1m: Number(host.maxLoad.toFixed(1)) },
      lineageSubmitted: Object.fromEntries(reasons.map((r) => [r, submitted(r)])),
      lineageCompacted: harvestedSum("cairnflow_lineage_compactions_total"),
    })}`,
  );
  const summary =
    `${Math.round(windowS)}s, deepest ${maxDepth}` +
    (measured ? `, scrubber ${Math.round(perHour(scrubReq))}/h (budget ${SCRUB_BUDGET})` : "") +
    (measured && GC ? `, D-d ${Math.round(perHour(ddReq))}/h with ${tombstones} tombstones` : "") +
    `, ${problems.size === 0 ? "passed" : `failed ${[...problems.keys()].join(", ")}`}`;
  return { summary, problems };
}

// ───────────────────────────────────────────── scenario

/** The `billing` workload — `undefined` = the `ChaosService` load. */
const billing = WORKLOAD === "billing" ? new BillingWorkload() : undefined;

async function main() {
  console.log(`chaos: ${NODES} nodes × ${PARTITIONS} partitions on ${STORE} (WAL ${WAL_KIND ?? "the node's default"})`);
  if (MEMORY) console.log("  memory://: one process — no kill -9, no cairn-ctl (see the head of the file)");
  if (LOCAL) console.log("  file://: one node; kill -9 kills it, rebuilt from the directory (see the head of the file)");
  if (billing) console.log(`  workload ${billing.describe()}`);
  console.log(
    `  liveness: drain within ${DRAIN_S}s, scrape /scheduler every ${SCHED_EVERY_S}s, queue slope over ${QUEUE_WINDOW_S}s after the load, ` +
      `deadlock after ${DEADLOCK_SCRAPES} consecutive scrapes`,
  );
  console.log(`  binary ${binary}${HAS_CTL ? `, cairn-ctl ${CTL}` : " (no cairn-ctl — skipping the GC and WAL floor invariants)"}`);
  console.log(
    `  ${DURATION_S}s under load ${billing ? "billing" : `${INVOKES_PER_S}/s`}; ` +
      (KILL_EVERY_S > 0 ? `random kill every ${KILL_EVERY_S}s, respawn after ${RESTART_AFTER_S}s` : "no kills") +
      (KILL_COORDINATOR_EVERY_S > 0 ? `; kill the coordinator every ${KILL_COORDINATOR_EVERY_S}s` : "") +
      `; lease ${LEASE_TTL_MS ? `${LEASE_TTL_MS} ms` : "the node's default"}` +
      (SPARE_NODE >= 0 ? `; never killing node ${SPARE_NODE}` : ""),
  );
  console.log(
    AUTO_SPLIT
      ? `  auto-split: on, hot_share ${AUTO_HOT_SHARE}, ${AUTO_HOT_SECS} windows, cooldown ${AUTO_COOLDOWN_S}s, ` +
          `each node once every ${AUTO_NODE_INTERVAL_S}s` +
          (AUTO_MAX_RANGES > 0 ? `, suggest-only from ${AUTO_MAX_RANGES} ranges` : "") +
          (billing ? " " : `; ${CONCURRENCY} call loops, ctx.run ${RUN_MS} ms `) +
          `(store: ${env.AWS_ENDPOINT ?? "?"})`
      : SPLIT_EVERY_S > 0
        ? `  split one range every ${SPLIT_EVERY_S}s, merge the child pair back half a cycle later (store: ${env.AWS_ENDPOINT ?? "?"})`
        : "  no split/merge (CHAOS_SPLIT_EVERY_S=0)",
  );
  if (GC) console.log(`  GC D-d: every ${GC_EVERY_S}s, min_age ${GC_MIN_AGE_S}s`);
  if (IDLE_TAIL_S > 0) {
    console.log(
      `  quiet phase: ${IDLE_TAIL_S}s after the destructive phase and the lost/wrong verdict — depth ≤ ${IDLE_MAX_DEPTH}, ` +
        `scrubber ≤ ${SCRUB_BUDGET}/h${SCRUB_REQUESTS_PER_HOUR === undefined ? " (the node's default)" : ""}` +
        (GC ? `, D-d ≤ ${IDLE_DD_BUDGET}/h (reference)` : ""),
    );
  }
  if (SLOW_NODE) console.log(`  slow node: one node every ${SLOW_EVERY_S}s, PUT delayed ${SLOW_PUT} for ${SLOW_HOLD_S}s`);
  if (L0_PRESSURE) console.log(`  L0 pressure: every ${L0_EVERY_S}s the compactor is slowed by ${L0_DELAY} ms/operation for ${L0_HOLD_S}s`);
  if (TENANTS > 0) {
    console.log(
      `  ${TENANTS} tenants: 1 noisy (${NOISY_CONC} closed loops), ${TENANTS - 1} quiet (1 call/${QUIET_EVERY_MS} ms); ` +
        (TENANT_RATES
          ? `noisy rate ${NOISY_RATE} CU/s, quiet ${INJECT === "quiet-throttle" ? "1 (INJECTED)" : QUIET_RATE} CU/s`
          : "no rate") +
        `; quiet p99 threshold ${QUIET_P99_MS} ms`,
    );
  }
  if (TENANT_STATES) {
    console.log(
      `  tenant states: every ${TENANT_STATE_EVERY_S}s one quiet tenant active → limited → suspended → active, ` +
        `holding each state ${TENANT_STATE_HOLD_S}s, grace ${TENANT_STATE_GRACE_MS} ms`,
    );
  }
  if (SSO) console.log(`  SSO: in-process fake issuer, one login every ${SSO_EVERY_S}s on a live node`);
  if (STORE_OUTAGE) {
    console.log(
      `  long store outage: docker stop ${OUTAGE_CONTAINER} at second ${OUTAGE_AT_S}, held ${OUTAGE_S}s, ` +
        `the cluster must take work again within ${OUTAGE_RECOVER_S}s (store ${env.AWS_ENDPOINT})`,
    );
  }
  if (S3_PROFILE) console.log(`  store profile: ${S3_PROFILE}`);
  if (COST) console.log("  COST MEASUREMENT (--cost): the binary's default flags, counting requests only");
  if (COUNTING) console.log(`  request counting: scrape /metrics every ${SCRAPE_EVERY_S}s and before each kill; two-leader prober via ${PROBE}`);
  if (INJECT) console.log(`  FAULT INJECTION: ${INJECT} — this run is green if and only if invariant "${INJECTS[INJECT]}" fails`);
  if (DISK_MIN_GB > 0 && DISK_PATHS.length > 0) {
    const d = diskFree();
    console.log(
      `  disk: watching ${DISK_PATHS.join(", ")} every ${DISK_EVERY_S}s, stop the destructive phase below ${DISK_MIN_GB} GiB` +
        (d ? ` (lowest right now ${d.gib.toFixed(1)} GiB at ${d.path})` : " (no path could be measured)"),
    );
    if (d !== undefined && d.gib < DISK_MIN_GB) {
      console.error(`disk: ${d.path} has ${d.gib.toFixed(1)} GiB left < CHAOS_DISK_MIN_GB=${DISK_MIN_GB} — not starting`);
      process.exit(2);
    }
  }
  if (LOG_DIR) {
    console.log(
      `  node logs: ${LOG_DIR}, ` + (LOG_MAX_MB > 0 ? `rotating every ${LOG_MAX_MB} MiB, keeping ${LOG_KEEP} old files per node` : "no rotation"),
    );
  }
  if (SUMMARY_EVERY_S > 0) console.log(`  summary every ${SUMMARY_EVERY_S}s (SUMMARY and SUMMARY_JSON lines)`);
  if (PRE_SOAK_CASES) {
    console.log(
      "  pre-soak cases:" +
        (HOT
          ? ` (a) hot objects ${HOT_KEYS} keys × ${HOT_INFLIGHT} sends, signal every ${HOT_SIGNAL_MS} ms, cancel ${HOT_CANCEL_SHARE * 100}%;`
          : "") +
        (GATE_FAULTS ? ` (b) ${GATE_POINTS.length} turn-gate points, running standing still ≤ ${STUCK_RUNNING_S}s while the cluster is healthy;` : "") +
        (TENANT_PURGE ? ` (c) purge ${PURGE_TENANT} after ${PURGE_INVOCATIONS} invocations on one range, keep ${PURGE_KEEP_S}s;` : "") +
        (BACKUP_RESTORE
          ? ` (d) backups from second ${BACKUP_FIRST_S} every ${BACKUP_EVERY_S}s, restored into s3://${RESTORE_BUCKET}${AUTO_SPLIT ? "" : " (NO --auto-split)"};`
          : "") +
        (PAR ? ` (e) ${PAR_BRANCHES} branches × ${PAR_STEPS} steps, ctx.run failing ${PAR_FAIL * 100}%;` : "") +
        (TRIPWIRES ? " tripwires panic" : " tripwires OFF (CHAOS_TRIPWIRES=0)"),
    );
  }
  if (RR_TENANT !== undefined) {
    console.log(`  tenant chaos-t${RR_TENANT}: its own REQUEST_RESPONSE deployment (other tenants ${sdkMode}); latency printed separately, outside invariant 6`);
  }

  if (STORE_OUTAGE) {
    // Only stop exactly the container serving this run's store: running, not
    // `--rm` (stop means delete), publishing exactly the port of `AWS_ENDPOINT`.
    const c = OUTAGE_CONTAINER!;
    const port = new URL(env.AWS_ENDPOINT!).port || "80";
    const [state, ports] = await Promise.all([
      docker(["inspect", "-f", "{{.State.Running}} {{.HostConfig.AutoRemove}}", c]),
      docker(["port", c]),
    ]);
    const [running, autoRemove] = state.trim().split(/\s+/);
    if (running !== "true") throw new Error(`--store-outage: container ${c} is not running (${state.trim()})`);
    if (autoRemove === "true") throw new Error(`--store-outage: container ${c} runs with --rm — docker stop would delete it`);
    if (!ports.split("\n").some((l) => l.trim().endsWith(`:${port}`))) {
      throw new Error(
        `--store-outage: container ${c} does not publish port ${port} of AWS_ENDPOINT=${env.AWS_ENDPOINT} ` +
          `(docker port: ${ports.trim().replace(/\n/g, "; ") || "no ports"}) — refusing to stop a container other than this run's store`,
      );
    }
    if (!(await storeAnswers())) throw new Error(`--store-outage: ${env.AWS_ENDPOINT} does not answer before the start`);
    const restore = (sig: string) => {
      restoreOutageContainer();
      stopping = true;
      for (const n of nodes) n.proc.kill("SIGKILL");
      console.error(`chaos: got ${sig} — exiting`);
      process.exit(130);
    };
    process.once("SIGINT", () => restore("SIGINT"));
    process.once("SIGTERM", () => restore("SIGTERM"));
  }

  const sdkOpts = {
    services: [
      ...(billing ? billing.services : [chaosService, chaosEcho, chaosTree, ...(LARGE_VALUES ? [chaosHistory] : [])]),
      ...(HOT ? [chaosHot] : []),
      ...(TENANT_PURGE ? [chaosBulk] : []),
      // `ChaosPar` calls `ChaosEcho` — the billing workload does not carry it.
      ...(PAR ? [chaosPar, ...(billing ? [chaosEcho] : [])] : []),
    ],
    protocolMode: sdkMode,
  };
  let sdk = await serve({ ...sdkOpts, port: 0 });
  const sdkUrl = sdk.url;
  // `--rr-tenant`: the same services, a second endpoint declaring
  // REQUEST_RESPONSE — only that tenant's deployment points at it (the shape of a hosted deployment:
  // the SaaS profile registers `https://` endpoints as REQUEST_RESPONSE).
  // Verifies the tenant's identity: the replay cache is only served with `identityKeys`.
  const rrOpts = {
    ...sdkOpts,
    protocolMode: "REQUEST_RESPONSE" as ProtocolMode,
    identityKeys: [kidOf(IDENTITY_MASTER, `chaos-t${RR_TENANT ?? 0}`)],
  };
  let sdkRr = RR_TENANT !== undefined ? await serve({ ...rrOpts, port: 0 }) : undefined;
  const sdkRrUrl = sdkRr?.url;
  // `--large-values`: the same services on an endpoint that verifies `chaos-lv`'s identity —
  // segmented values (wire v6) are only served with `identityKeys` (or `segmentOrigins`).
  const lvOpts = { ...sdkOpts, identityKeys: [kidOf(IDENTITY_MASTER, LV_TENANT)] };
  let sdkLv = LARGE_VALUES ? await serve({ ...lvOpts, port: 0 }) : undefined;
  const sdkLvUrl = sdkLv?.url;
  if (DELEGATION && !HAS_CTL) throw new Error("--delegation needs cairn-ctl");
  console.log(`  quiet-renewal gate of a new bucket: --quiet-lease ${QUIET_MODE}`);
  for (let i = 0; i < NODES; i++) {
    nodes.push(await spawnNode(i));
  }
  if (DELEGATION) {
    // Newer binaries already enable it when creating the bucket ⇒ the command writes nothing; older
    // binaries ⇒ the command enables it (wait BOOT_WAIT because the cluster already has sessions).
    const rep = await ctl<{ applied: boolean; warnings: string[] }>([
      "cluster",
      "enable-quiet-lease",
      "--delegation",
      "--accept-missing-evidence",
      "--apply",
    ]);
    console.log(`  delegation: ${rep.applied ? "just enabled with cairn-ctl" : rep.warnings.join("; ")}`);
  }

  // Wait until every partition has a leader before registering + starting the load.
  const bootDeadline = Date.now() + 60_000;
  for (;;) {
    const c = (await cfetch("/cluster").then((r) => r.json())) as any;
    if (c.partitions.every((p: any) => p.leader)) break;
    if (Date.now() > bootDeadline) throw new Error("the cluster did not get leaders for every partition within 60s");
    await sleep(200);
  }
  if (NEED_FAILPOINTS) {
    // A binary without the `failpoints` feature returns 404 here — stop loudly, do not run
    // a "with failpoints" run where no point could be set.
    const r = await nfetch(`${nodes[0]!.url}/admin/failpoints`).catch(() => undefined);
    // Only the tripwires need failpoints (no other flag): a binary without `failpoints` ⇒ tripwires off, said clearly.
    const onlyTripwires = !(FAILPOINTS || SLOW_NODE || L0_PRESSURE || INJECT === "gc-skip-refs" || INJECT === "overlap");
    if (!r?.ok && onlyTripwires) {
      tripwiresOn = false;
      console.log(`  tripwires: ${binary} has no /admin/failpoints (${r?.status}) — not set`);
    } else if (!r?.ok) {
      throw new Error(
        `failpoints needed but ${binary} has no /admin/failpoints (${r?.status}) — ` +
          "build with `cargo build --bin cairnflow --features failpoints --target-dir target/fp` " +
          "then set CHAOS_BINARY=target/fp/debug/cairnflow",
      );
    }
    const { names } = r?.ok ? ((await r.json()) as { names: string[] }) : { names: [] as string[] };
    if (tripwiresOn && !TRIPWIRE_POINTS.every((n) => names.includes(n))) {
      tripwiresOn = false;
      console.log(`  tripwires: the binary does not have ${TRIPWIRE_POINTS.join(", ")} (a build from before the fix) — not set`);
    }
    const want = [...(FAILPOINTS ? RETURN_POINTS.map(([n]) => n) : []), ...OWN_POINTS];
    const unknown = want.filter((n) => !names.includes(n));
    if (unknown.length > 0) throw new Error(`failpoints missing from the binary: ${unknown.join(", ")}`);
    if (FAILPOINTS) {
      console.log(
        `  failpoints: every ${FAILPOINT_EVERY_S}s set one of ${RETURN_POINTS.length} return(...) points on one node, held ${FAILPOINT_HOLD_S}s`,
      );
    }
  }
  if (S3_PROFILE) {
    // A binary without `testkit` silently ignores the environment variable — stop loudly.
    const text = await nfetch(`${nodes[0]!.url}/metrics`).then((r) => r.text());
    if (!text.includes("cairnflow_testkit_store_requests_total")) {
      throw new Error(`--s3-profile but ${binary} does not carry store profiles (no cairnflow_testkit_store_*) — build with --features testkit`);
    }
  }
  const reg = await setupFetch("/deployments", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ uri: sdkUrl }),
  });
  if (!reg.ok) throw new Error(`deployment registration failed: ${reg.status} ${await reg.text()}`);

  // Tenants: each tenant has one key and one deployment; tenant 0 is noisy.
  const tenantKeys: Array<{ tenant: string; key: string; noisy: boolean; rr: boolean }> = [];
  for (let i = 0; i < TENANTS; i++) {
    const tenant = `chaos-t${i}`;
    const r = await setupFetch("/admin/keys", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ tenant, role: "admin" }),
    });
    if (!r.ok) throw new Error(`creating key ${tenant}: ${r.status} ${await r.text()}`);
    const body = (await r.json()) as { secret?: string; key?: string };
    const key = body.secret ?? body.key;
    if (!key) throw new Error(`key ${tenant} has no secret: ${JSON.stringify(body)}`);
    tenantKeys.push({ tenant, key, noisy: i === 0, rr: i === RR_TENANT });
    if (TENANT_RATES) {
      const rate = i === 0 ? NOISY_RATE : INJECT === "quiet-throttle" ? 1 : QUIET_RATE;
      const rr = await setupFetch("/rules", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ tenant, rate, burst: Math.max(1, rate) }),
      });
      if (!rr.ok) throw new Error(`rule of ${tenant}: ${rr.status} ${await rr.text()}`);
    }
  }
  if (tenantKeys.length > 0) {
    // Other nodes learn the new key after one registry refresh tick (`--deploy-refresh-ms` 500).
    await sleep(2_000);
    for (const t of tenantKeys) {
      const uri = t.rr ? sdkRrUrl! : sdkUrl;
      const r = await setupFetch(
        "/deployments",
        { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ uri }) },
        30_000,
        t.key,
      );
      if (!r.ok) throw new Error(`registering the deployment of ${t.tenant}: ${r.status} ${await r.text()}`);
      if (t.rr) {
        // It must really be REQUEST_RESPONSE — otherwise the run is green for the wrong reason.
        const mode = ((await r.json().catch(() => ({}))) as { protocolMode?: string }).protocolMode;
        if (mode !== "REQUEST_RESPONSE") {
          throw new Error(`the deployment of ${t.tenant} registered as ${mode}, not REQUEST_RESPONSE`);
        }
        console.log(`  tenant ${t.tenant}: deployment ${uri} (${mode})`);
      }
    }
  }
  if (TENANT_STATES) {
    // Check the `tenants/state` gate up front (a new bucket has it on; `409` ⇒ cairn-ctl
    // enables it): a no-op `PUT active` for each quiet tenant.
    for (const t of tenantKeys) {
      if (t.noisy) continue;
      churnable.add(t.tenant);
      tenantStateNow.set(t.tenant, "active");
      const at = await putTenantState(t.tenant, "active", 120_000);
      if (at === undefined) throw new Error(`--tenant-states: ${tenantStateProblems.at(-1)}`);
      activeConfirmedAt.set(t.tenant, at);
    }
    console.log(`  tenant states: gate on, ${churnable.size} tenants can change state (${[...churnable].join(", ")})`);
  }

  // `--large-values`: its own tenant, 1 MiB payload cap, deployment pointing at its own endpoint.
  let lvKey: string | undefined;
  if (LARGE_VALUES) {
    const r = await setupFetch("/admin/keys", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ tenant: LV_TENANT, role: "admin" }),
    });
    if (!r.ok) throw new Error(`creating key ${LV_TENANT}: ${r.status} ${await r.text()}`);
    const body = (await r.json()) as { secret?: string; key?: string };
    lvKey = body.secret ?? body.key;
    if (!lvKey) throw new Error(`key ${LV_TENANT} has no secret: ${JSON.stringify(body)}`);
    const lim = await setupFetch(`/admin/tenants/${LV_TENANT}`, {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ maxPayload: LV_MAX_PAYLOAD }),
    });
    if (!lim.ok) throw new Error(`max-payload of ${LV_TENANT}: ${lim.status} ${await lim.text()}`);
    await sleep(2_000);
    const d = await setupFetch(
      "/deployments",
      { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ uri: sdkLvUrl }) },
      30_000,
      lvKey,
    );
    if (!d.ok) throw new Error(`registering the deployment of ${LV_TENANT}: ${d.status} ${await d.text()}`);
    console.log(
      `  large values: tenant ${LV_TENANT} (max-payload ${LV_MAX_PAYLOAD}), ${LV_KEYS} objects, base ${LV_BASE_KB} KiB + ${LV_APPEND_KB} KiB/turn, back to base when > ${LV_RESET_KB} KiB`,
    );
  }

  // Pre-soak cases (a), (c): each case has its own tenant — an `admin` key and a deployment pointing at the same SDK.
  const caseTenant = async (tenant: string): Promise<string> => {
    const r = await setupFetch("/admin/keys", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ tenant, role: "admin" }),
    });
    if (!r.ok) throw new Error(`creating key ${tenant}: ${r.status} ${await r.text()}`);
    const body = (await r.json()) as { secret?: string; key?: string };
    const key = body.secret ?? body.key;
    if (!key) throw new Error(`key ${tenant} has no secret: ${JSON.stringify(body)}`);
    await sleep(2_000);
    const d = await setupFetch(
      "/deployments",
      { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ uri: sdkUrl }) },
      30_000,
      key,
    );
    if (!d.ok) throw new Error(`registering the deployment of ${tenant}: ${d.status} ${await d.text()}`);
    return key;
  };
  const hotKey = HOT ? await caseTenant(HOT_TENANT) : undefined;
  const purgeKey = TENANT_PURGE ? await caseTenant(PURGE_TENANT) : undefined;

  // SSO: an in-process fake issuer; a client for the node's hostname → tenant `chaos-sso`.
  let issuer: { url: string; close: () => Promise<void> } | undefined;
  if (SSO) {
    const { startFakeIssuer } = await import("../../ui/e2e/fixture/oidc.ts");
    issuer = await startFakeIssuer();
    const hostname = new URL(nodes[0]!.url).hostname;
    for (const [path, body] of [
      ["/admin/oidc", { issuer: issuer.url, displayName: "Chaos IdP" }],
      [`/admin/oidc/clients/${SSO_CLIENT}`, { hostname, tenant: SSO_TENANT, maxRole: "admin" }],
    ] as const) {
      const r = await putConfirmed(path, body, 120_000);
      if (r.at === undefined) throw new Error(`--sso: PUT ${path} did not reach 204: ${r.last}`);
    }
    // A binary without the `ui` feature (or with `--no-ui`) has no login page — stop loudly.
    const opts = (await fetch(`${nodes[0]!.url}/ui/login/options`)
      .then((r) => r.json())
      .catch(() => undefined)) as { sso?: boolean } | undefined;
    if (opts?.sso !== true) {
      throw new Error(`--sso: ${nodes[0]!.url}/ui/login/options does not report sso=true (${JSON.stringify(opts)}) — binary missing the ui feature?`);
    }
    console.log(`  SSO: issuer ${issuer.url}, client ${SSO_CLIENT} cho hostname ${hostname} → tenant ${SSO_TENANT}`);
  }

  if (INJECT === "rogue-leader") {
    // A one-node cluster on its own bucket: it leads every key of its own.
    const { proc, url } = await spawnProc("rogue", nodeArgs(`${STORE}-rogue`, { gc: false }).concat(["--node-id", "chaos-rogue"]));
    rogue = { proc, url, alive: true, name: "rogue" };
    console.log(`  INJECT: a node of another cluster ${url} in the leader-probe set`);
  }

  // Billing workload: rules, subscriptions — before the load start mark.
  if (billing) {
    await billing.setup({
      cfetch: (path, init, timeoutMs) => cfetch(path, init, timeoutMs),
      log: (l) => console.log(l),
      diagnose: diagnoseStuckRead,
      readLeader: async (id, attempt) => {
        const d = await readDetail(id, attempt);
        return d.ok ? (d.body as InvokeBody) : undefined;
      },
    });
  }

  // Load + saboteurs run in parallel.
  let sent = 0;
  let ackFailures = 0;
  let overloaded = 0;
  let kills = 0;
  /** The time of each `kill -9` (ms) — check (b) excuses the FL-19 gap. */
  const killsAt: number[] = [];

  /**
   * Kill process `victim` of slot `vi` then revive that slot — every kill loop goes
   * through here. Does not kill if `victim` is no longer the current live process of
   * that slot: a call loop usually `await`s (scraping `/metrics`) between choosing and killing,
   * and another loop may have killed (and revived) that very slot in that gap.
   */
  function killSlot(victim: Node, vi: number, what: string): boolean {
    if (!victim.alive || nodes[vi] !== victim) return false;
    victim.alive = false;
    victim.proc.kill("SIGKILL");
    kills++;
    noteDisruption();
    killsAt.push(Date.now());
    console.log(`  [${ts()}] kill -9 ${what}node ${vi} (${victim.url})`);
    void respawn(vi);
    return true;
  }
  const startedAt = Date.now();
  // A module-level `let`: `haltLoad` (low disk) pulls it in early. Stopped already while
  // the cluster was being built ⇒ the destructive phase is empty.
  endAt = haltReason !== undefined ? startedAt : startedAt + DURATION_S * 1000;
  let seqNext = 0;
  /** Calls refused because of the tenant state (not acked ⇒ not lost), by `harness state/HTTP code`. */
  const refusals = new Map<string, number>();
  const refusalProblems: string[] = [];
  /** Number of acked sends (the billing workload included) — `--store-outage` measures when the cluster takes work again. */
  const ackCount = () => (billing ? billing.ackedIds.size : ackLedger.size);

  /**
   * A call of `tenant` refused with `tenant_limited`/`tenant_suspended`: counted.
   * A violation when the tenant has never had its state changed, or the send went out after
   * `active` answered `204` (plus the grace) — the `204` promises every live node has applied it.
   */
  function noteRefusal(tenant: string, status: number, code: string, sentAt: number) {
    const state = tenantStateNow.get(tenant) ?? "active";
    // The `suspended` label is set **before** the PUT reaches 204: a send in that gap (or
    // sent while still `limited`) getting `429 tenant_limited` is correct. A separate label keeps the waiting
    // gap apart from real errors (a read-ordering bug was once suspected here).
    const since = suspendedSince.get(tenant);
    const confirmedSuspended = state === "suspended" && since !== undefined && sentAt > since;
    const label = state === "suspended" && !confirmedSuspended ? "suspended(awaiting 204)" : state;
    const k = `${label}/${status} ${code}`;
    refusals.set(k, (refusals.get(k) ?? 0) + 1);
    if (confirmedSuspended && code === "tenant_limited") {
      // `204 suspended` promises every live node has applied it: a node still answering `limited`
      // to a later send (plus the grace) is a state going backwards.
      refusalProblems.push(
        `${ts()} ${tenant}: ${status} ${code} for a send at ${new Date(sentAt).toISOString()}, ` +
          `after suspended had answered 204 (+grace until ${new Date(since).toISOString()})`,
      );
    }
    const confirmed = activeConfirmedAt.get(tenant);
    if (!churnedEver.has(tenant)) {
      refusalProblems.push(`${ts()} ${tenant} never had its state changed yet got ${status} ${code}`);
    } else if (state === "active" && confirmed !== undefined && sentAt > confirmed + TENANT_STATE_GRACE_MS) {
      refusalProblems.push(
        `${ts()} ${tenant}: ${status} ${code} for a send at ${new Date(sentAt).toISOString()}, ` +
          `after active had answered 204 at ${new Date(confirmed).toISOString()}`,
      );
    }
  }

  /**
   * One synchronous call (waits for the result). Having an id (`200`/`202`/`429`) means acked —
   * it goes into the verdict book. `settle` = wait until done (asking `/invocations` again),
   * to measure latency up to completion.
   */
  async function invokeOnce(
    tag: string,
    key: string | undefined,
    settle: boolean,
  ): Promise<{ acked: boolean; latencyMs?: number }> {
    const seq = seqNext++;
    sent++;
    // The tenant of every call (the caller's tag is derived from here); child handlers only
    // look at tenants whose state can change (`noteHandlerEntry`).
    seqs.setTenant(seq, tag);
    const t0 = Date.now();
    let r: Response;
    try {
      r = await cfetch(
        `/invoke/ChaosService/work`,
        { method: "POST", body: JSON.stringify({ tag: `${tag}-${seq}`, seq }) },
        20_000,
        key,
      );
    } catch {
      // Not acked means nothing was promised — not a loss.
      ackFailures++;
      return { acked: false };
    }
    const body = (await r.json().catch(() => ({}))) as { id?: string; status?: string; code?: string; retry_after_ms?: number };
    if (!body.id || !(r.ok || r.status === 202 || r.status === 429)) {
      if (r.status === 429 && body.code === "range_overloaded") overloaded++;
      if (body.code === "tenant_limited" || body.code === "tenant_suspended") noteRefusal(tag, r.status, body.code, t0);
      ackFailures++;
      return { acked: false };
    }
    const idx = ackLedger.add(body.id, seq);
    if (!settle || body.status === "completed") return { acked: true, latencyMs: Date.now() - t0 };
    if (r.status === 429) await sleep(Math.min(5_000, body.retry_after_ms ?? 200));
    const deadline = Date.now() + 120_000;
    while (Date.now() < deadline) {
      const st = await readStatus(body.id, 30_000, key, idx);
      if (st && (st.status === "completed" || st.status === "failed" || st.status === "paused")) break;
      await sleep(250);
    }
    return { acked: true, latencyMs: Date.now() - t0 };
  }

  /**
   * Latency up to completion: [send time, ms]. `rr` = the `--rr-tenant` tenant (at the
   * quiet-tenant cadence): print only, outside invariant 6 — REQUEST_RESPONSE goes back and forth
   * one request per suspension point, so its p99 is not comparable with the BIDI baseline.
   */
  //
  // Quiet and baseline keep **every** sample (invariant 6 over the whole run is one verdict; a few
  // tens of thousands of samples per hour). Noisy and RR are print only: raw samples only for the open
  // window (the window line, `latency.tsv`), the rest goes into a histogram — previously ~400 k pairs per
  // hour, the whole array refiltered every window.
  class PrintOnly {
    /** Samples not yet past any window. */
    private win: Array<[number, number]> = [];
    /** Since the previous summary. */
    readonly sinceSummary = new LogHist();
    readonly all = new LogHist();
    push(at: number, ms: number) {
      this.win.push([at, ms]);
      this.sinceSummary.add(ms);
      this.all.add(ms);
    }
    /** Samples with a send time in `[from, to)`; drops every sample before `to` (window already past). */
    take(from: number, to: number): Array<[number, number]> {
      const out = this.win.filter(([at]) => at >= from && at < to);
      this.win = this.win.filter(([at]) => at >= to);
      return out;
    }
  }
  const latency = {
    base: [] as Array<[number, number]>,
    noisy: new PrintOnly(),
    quiet: [] as Array<[number, number]>,
    rr: new PrintOnly(),
  };
  /** Samples dropped from invariant 6 (overlapping a non-`active` window of that same tenant, or a store outage window). */
  const latencyExcluded = { base: 0, noisy: 0, quiet: 0, rr: 0 };
  const sample = (k: keyof typeof latency, tenant: string | undefined, at: number, ms: number) => {
    if (excludedSample(tenant, at, at + ms)) latencyExcluded[k]++;
    else if (k === "noisy" || k === "rr") latency[k].push(at, ms);
    else latency[k].push([at, ms]);
  };

  const loadLoop = async () => {
    while (Date.now() < endAt) {
      const t = Date.now();
      // With tenants, the baseline load is the reference of invariant 6: measured the same way (until done).
      const r = await invokeOnce("chaos", undefined, TENANTS >= 2);
      if (r.latencyMs !== undefined) sample("base", undefined, t, r.latencyMs);
      await sleep(1000 / INVOKES_PER_S);
    }
  };
  const load = billing ? billing.run(endAt) : Promise.all(Array.from({ length: Math.max(1, CONCURRENCY) }, loadLoop));

  // Noisy tenant: a closed loop, calling again as soon as the previous call finishes. Quiet tenants: a steady cadence.
  const tenantLoops = Promise.all(
    tenantKeys.flatMap((t) =>
      t.noisy
        ? Array.from({ length: NOISY_CONC }, async () => {
            while (Date.now() < endAt) {
              const at = Date.now();
              const r = await invokeOnce(t.tenant, t.key, true);
              if (r.latencyMs !== undefined) sample("noisy", t.tenant, at, r.latencyMs);
              else await sleep(200);
            }
          })
        : [
            (async () => {
              while (Date.now() < endAt) {
                const at = Date.now();
                const r = await invokeOnce(t.tenant, t.key, true);
                if (r.latencyMs !== undefined) sample(t.rr ? "rr" : "quiet", t.tenant, at, r.latencyMs);
                await sleep(Math.max(0, QUIET_EVERY_MS - (Date.now() - at)));
              }
            })(),
          ],
    ),
  );

  // `--large-values`: one call every `LV_EVERY_MS`, rotating over the objects.
  const lvAcked = new Map<string, string>();
  const lvSeen = new Map<string, Map<number, string>>();
  const lvProblems: string[] = [];
  let lvSent = 0;
  let lvOutputs = 0;
  let lvResets = 0;
  let lvMaxLen = 0;
  const lvNote = (id: string, key: string, raw: string | undefined) => {
    if (raw === undefined) return;
    let out: LvOut;
    try {
      out = JSON.parse(raw) as LvOut;
    } catch {
      lvProblems.push(`${id} (${key}): output is not JSON: ${raw.slice(0, 120)}`);
      return;
    }
    const seen = lvSeen.get(key) ?? new Map<number, string>();
    lvSeen.set(key, seen);
    const was = seen.get(out.n);
    if (was === id) return;
    lvOutputs++;
    if (out.len === (LV_BASE_KB + LV_APPEND_KB) * 1024) lvResets++;
    lvMaxLen = Math.max(lvMaxLen, out.len);
    if (out.corrupt !== undefined) lvProblems.push(`${id} (${key}) n ${out.n}: the value read back is corrupt — ${out.corrupt}`);
    if (was !== undefined) lvProblems.push(`${key}: n ${out.n} duplicated at ${was} and ${id} — lost update`);
    seen.set(out.n, id);
  };
  const lvLoop = (async () => {
    if (!LARGE_VALUES) return;
    for (let i = 0; Date.now() < endAt; i++) {
      const at = Date.now();
      const key = `h${i % LV_KEYS}`;
      const seq = seqNext++;
      lvSent++;
      const r = await cfetch(
        `/invoke/ChaosHistory/append?key=${key}`,
        { method: "POST", body: JSON.stringify({ seq }) },
        20_000,
        lvKey,
      ).catch(() => undefined);
      const body = r ? ((await r.json().catch(() => ({}))) as { id?: string; status?: string; output?: string }) : {};
      // The same ack rule as `invokeOnce`: only `200`/`202`/`429` carrying an id is durable.
      // A not-leader `503` also carries an id (`from_actor_routed`), but the submit may have been
      // stopped before it was durable — that id promises nothing.
      if (body.id && r && (r.ok || r.status === 202 || r.status === 429)) {
        lvAcked.set(body.id, key);
        if (body.status === "completed") lvNote(body.id, key, body.output);
      }
      await sleep(Math.max(0, LV_EVERY_MS - (Date.now() - at)));
    }
  })();

  // Healthy windows of case (b): `[from, to)` fixed from the start of the run — destructive loops skip their turn
  // while inside a window; the `running`-standing-still watcher records how many ids it examined.
  type HealthyWindow = { from: number; to: number; ids: Set<string>; rounds: number; maxAgeMs: number };
  const healthyWindows: HealthyWindow[] = [];
  if (HEALTHY_EVERY_S > 0 && HEALTHY_S > 0) {
    for (let k = 1; startedAt + k * HEALTHY_EVERY_S * 1000 - HEALTHY_S * 1000 < endAt; k++) {
      const to = Math.min(endAt, startedAt + k * HEALTHY_EVERY_S * 1000);
      healthyWindows.push({ from: to - HEALTHY_S * 1000, to, ids: new Set(), rounds: 0, maxAgeMs: 0 });
    }
    console.log(
      `  healthy windows (b): ${healthyWindows.length} windows of ${HEALTHY_S}s without kill/failpoint, ` +
        `from second ${healthyWindows.map((w) => Math.round((w.from - startedAt) / 1000)).join(", ") || "—"}`,
    );
  }
  const healthyWindowAt = (t: number) => healthyWindows.find((w) => t >= w.from && t < w.to);
  const inHealthyWindow = (t: number) => healthyWindowAt(t) !== undefined;

  // The SDK replica dies and reopens on the same port: its replay cache is lost.
  const SDK_RESTART_EVERY_S = Number(env.CHAOS_SDK_RESTART_EVERY_S ?? "0");
  let sdkRestarts = 0;
  const sdkRestarter = (async () => {
    if (SDK_RESTART_EVERY_S <= 0) return;
    while (Date.now() < endAt) {
      await sleep(SDK_RESTART_EVERY_S * 1000);
      if (Date.now() >= endAt) break;
      if (inHealthyWindow(Date.now())) continue;
      // Also the REQUEST_RESPONSE endpoint of `--rr-tenant`: its replay cache is what
      // this run really wants to lose.
      const reopen = async (old: { port: number; close: () => Promise<void> }, opts: Parameters<typeof serve>[0]) => {
        await old.close();
        for (let i = 0; ; i++) {
          try {
            return await serve({ ...opts, port: old.port });
          } catch (e) {
            if (i > 50) throw e;
            await sleep(100);
          }
        }
      };
      noteDisruption();
      sdk = await reopen(sdk, sdkOpts);
      if (sdkRr) sdkRr = await reopen(sdkRr, rrOpts);
      if (sdkLv) sdkLv = await reopen(sdkLv, lvOpts);
      sdkRestarts++;
    }
  })();

  /** The node `--slow-node` is currently slowing down. */
  let slowTarget: Node | undefined;
  const killer = (async () => {
    if (KILL_EVERY_S <= 0) return;
    while (Date.now() < endAt) {
      await sleep(KILL_EVERY_S * 1000);
      if (Date.now() >= endAt) break;
      // `--store-outage`: do not kill while the store is down and the cluster is recovering.
      if (outageActive) continue;
      // A healthy window of (b): do not kill.
      if (inHealthyWindow(Date.now())) continue;
      // A slow node is a "grey" node — still alive: killing it would lose the slow spell
      // before the shedding mechanism has a chance to react.
      const alive = nodes.map((n, i) => [n, i] as const).filter(([n, i]) => n.alive && n !== slowTarget && i !== SPARE_NODE);
      // Always keep at least one live node so the cluster has someone to take work — unless
      // the cluster has only one node (`file://`): then dying for good and
      // rebuilding from the bucket is exactly what is being tested, and senders wait through the gap.
      if (alive.length === 0 || (alive.length === 1 && NODES > 1)) continue;
      const [victim, vi] = pick(alive);
      // Request counters die with the process: scrape one last time right before the kill.
      if (COUNTING) await scrapeStore(victim);
      killSlot(victim, vi, "");
    }
  })();

  // Kill the coordinator: the node with
  // `cairnflow_coordinator_term` > 0 — exactly the node `control/coordinator` points
  // to (the gauge is set while holding the role under the current incarnation).
  let coordinatorKills = 0;
  let coordinatorMisses = 0;
  const coordinatorKiller = (async () => {
    if (KILL_COORDINATOR_EVERY_S <= 0) return;
    while (Date.now() < endAt) {
      await sleep(KILL_COORDINATOR_EVERY_S * 1000);
      if (Date.now() >= endAt) break;
      if (outageActive || inHealthyWindow(Date.now())) continue;
      const alive = nodes.map((n, i) => [n, i] as const).filter(([n]) => n.alive);
      if (alive.length <= 1 && NODES > 1) continue;
      let found: (typeof alive)[number] | undefined;
      for (const [n, i] of alive) {
        const text = await nfetch(`${n.url}/metrics`, { signal: AbortSignal.timeout(2_000) })
          .then((r) => r.text())
          .catch(() => "");
        if (metricMax(text, "cairnflow_coordinator_term") > 0) {
          found = [n, i];
          break;
        }
      }
      if (!found) {
        coordinatorMisses++;
        console.log(`  [${ts()}] no coordinator found to kill`);
        continue;
      }
      const [victim, vi] = found;
      if (vi === SPARE_NODE) {
        coordinatorMisses++;
        console.log(`  [${ts()}] the coordinator is node ${vi} (CHAOS_SPARE_NODE) — not killing`);
        continue;
      }
      // The random kill loop may have killed (and revived) that very node while
      // we asked `/metrics`: killing the corpse again revives twice — an orphan process
      // outside `nodes` runs until the end of the run (and may even hold the coordinator role).
      if (!victim.alive || nodes[vi] !== victim) continue;
      if (COUNTING) await scrapeStore(victim);
      if (killSlot(victim, vi, "coordinator: ")) coordinatorKills++;
    }
  })();

  // Failpoints: each cycle one `return(...)` point on one live node,
  // held FAILPOINT_HOLD_S seconds then cleared. If the node dies meanwhile the point dies with
  // the process — a failed clear is normal.
  const injected = new Map<string, number>();
  let injectFailures = 0;
  const injector = (async () => {
    if (!FAILPOINTS) return;
    while (Date.now() < endAt) {
      await sleep(FAILPOINT_EVERY_S * 1000);
      if (Date.now() >= endAt) break;
      // A healthy window of (b): a set point must be cleared before the window opens.
      if (inHealthyWindow(Date.now()) || inHealthyWindow(Date.now() + FAILPOINT_HOLD_S * 1000)) continue;
      const alive = nodes.map((n, i) => [n, i] as const).filter(([n]) => n.alive);
      if (alive.length === 0) continue;
      const [target, ti] = pick(alive);
      const [name, actions] = pick(RETURN_POINTS);
      const action = pick(actions);
      try {
        await setFailpoint(target, name, action);
        noteDisruption();
        injected.set(name, (injected.get(name) ?? 0) + 1);
        console.log(`  [${ts()}] failpoint ${name}=${action} on node ${ti} (${FAILPOINT_HOLD_S}s)`);
      } catch (e) {
        injectFailures++;
        console.log(`  failpoint ${name} could not be set on node ${ti}: ${e}`);
        continue;
      }
      await sleep(FAILPOINT_HOLD_S * 1000);
      await setFailpoint(target, name, "off").catch(() => undefined);
      noteDisruption();
    }
  })();

  // Slow node: a random node delays every PUT, then it is cleared. While holding,
  // watch its `cairnflow_draining` to know whether it sheds on its own.
  let slowEpisodes = 0;
  let slowSheds = 0;
  const slower = (async () => {
    if (!SLOW_NODE) return;
    while (Date.now() < endAt) {
      await sleepUntil(SLOW_EVERY_S * 1000 - SLOW_HOLD_S * 1000, endAt);
      if (Date.now() >= endAt) break;
      if (inHealthyWindow(Date.now()) || inHealthyWindow(Date.now() + SLOW_HOLD_S * 1000)) continue;
      const alive = aliveNodes();
      if (alive.length < 2) continue;
      const target = pick(alive);
      try {
        await setFailpoint(target, "node::put_delay", `return(${SLOW_PUT})`);
      } catch (e) {
        console.log(`  slow node: could not set on ${target.name}: ${e}`);
        continue;
      }
      slowEpisodes++;
      slowTarget = target;
      console.log(`  [${ts()}] slow node: ${target.name} (${target.url}) PUT delayed ${SLOW_PUT} for ${SLOW_HOLD_S}s`);
      const until = Math.min(endAt, Date.now() + SLOW_HOLD_S * 1000);
      let shed = false;
      // `endAt` is reread every tick: if the destructive phase stops early, clear within ≤ 5 s.
      while (Date.now() < Math.min(until, endAt) && target.alive) {
        await sleep(5_000);
        const text = await nfetch(`${target.url}/metrics`, { signal: AbortSignal.timeout(3_000) })
          .then((r) => r.text())
          .catch(() => "");
        if (!shed && metricMax(text, "cairnflow_draining") >= 1) {
          shed = true;
          slowSheds++;
          console.log(`  [${ts()}] slow node: ${target.name} is shedding (cairnflow_draining=1)`);
        }
      }
      await setFailpoint(target, "node::put_delay", "off").catch(() => undefined);
      slowTarget = undefined;
      noteDisruption();
    }
  })();

  // L0 pressure: the compactor of every live node is slowed periodically.
  let l0Episodes = 0;
  let l0MaxViews = 0;
  const l0Pressure = (async () => {
    if (!L0_PRESSURE) return;
    while (Date.now() < endAt) {
      await sleepUntil(L0_EVERY_S * 1000 - L0_HOLD_S * 1000, endAt);
      if (Date.now() >= endAt) break;
      if (inHealthyWindow(Date.now()) || inHealthyWindow(Date.now() + L0_HOLD_S * 1000)) continue;
      const targets = aliveNodes();
      const on = await Promise.all(
        targets.map((n) =>
          setFailpoint(n, "compactor::io_delay", `return(${L0_DELAY})`).then(
            () => true,
            () => false,
          ),
        ),
      );
      l0Episodes++;
      noteDisruption();
      console.log(`  [${ts()}] L0 pressure: compactor slowed by ${L0_DELAY} ms on ${on.filter(Boolean).length} nodes for ${L0_HOLD_S}s`);
      const until = Math.min(endAt, Date.now() + L0_HOLD_S * 1000);
      while (Date.now() < Math.min(until, endAt)) {
        await sleep(5_000);
        for (const n of aliveNodes()) {
          const text = await nfetch(`${n.url}/metrics`, { signal: AbortSignal.timeout(3_000) })
            .then((r) => r.text())
            .catch(() => "");
          l0MaxViews = Math.max(l0MaxViews, metricMax(text, "cairnflow_range_l0_views"));
        }
      }
      await Promise.all(targets.map((n) => setFailpoint(n, "compactor::io_delay", "off").catch(() => undefined)));
      noteDisruption();
      console.log(`  [${ts()}] L0 pressure: cleared; largest L0 seen ${l0MaxViews} views`);
    }
  })();

  // ── Tenant states: one quiet tenant active → limited →
  // suspended → active each cycle. A spell once started always goes all the way back to `active`, even
  // past `endAt` — a tenant must not be left `suspended` at verdict time.
  let tenantEpisodes = 0;
  const tenantStater = (async () => {
    if (!TENANT_STATES) return;
    const hold = TENANT_STATE_HOLD_S * 1000;
    let next = startedAt + TENANT_STATE_EVERY_S * 1000;
    for (;;) {
      await sleep(Math.max(0, next - Date.now()));
      // Leave enough room for the whole spell (two holds, three waits for 204) before the load stops.
      if (endAt - Date.now() < 2 * hold + 15_000) break;
      next = Date.now() + TENANT_STATE_EVERY_S * 1000;
      const tenant = pick([...churnable]);
      const win = { tenant, from: Date.now(), to: Infinity };
      churnedEver.add(tenant);
      nonActiveWindows.push(win);
      tenantEpisodes++;
      console.log(`  [${ts()}] tenant state: ${tenant} → limited`);
      tenantStateNow.set(tenant, "limited");
      if ((await putTenantState(tenant, "limited", 60_000)) !== undefined) {
        tenantTransitions.set("active→limited", (tenantTransitions.get("active→limited") ?? 0) + 1);
      }
      await sleep(hold);
      tenantStateNow.set(tenant, "suspended");
      const suspendedAt = await putTenantState(tenant, "suspended", 60_000);
      if (suspendedAt !== undefined) {
        tenantTransitions.set("limited→suspended", (tenantTransitions.get("limited→suspended") ?? 0) + 1);
        suspendedSince.set(tenant, suspendedAt + TENANT_STATE_GRACE_MS);
        console.log(`  [${ts()}] tenant state: ${tenant} suspended (204)`);
      }
      await sleep(hold);
      // Remove the (b) window **before** sending `active`: from then on the engine may run again.
      suspendedSince.delete(tenant);
      if (suspendedAt !== undefined) {
        suspendWindows.push({ tenant, from: suspendedAt + TENANT_STATE_GRACE_MS, to: Date.now() });
      }
      // It must get back to `active`: a long cap (the cluster may be rebuilding after a kill).
      const activeAt = await putTenantState(tenant, "active", 300_000);
      if (activeAt !== undefined) {
        tenantTransitions.set("suspended→active", (tenantTransitions.get("suspended→active") ?? 0) + 1);
      }
      tenantStateNow.set(tenant, "active");
      activeConfirmedAt.set(tenant, activeAt ?? Date.now());
      win.to = activeAt ?? Date.now();
      console.log(`  [${ts()}] tenant state: ${tenant} active${activeAt === undefined ? " (could NOT be confirmed)" : ""}`);
    }
  })();

  // ───────────── The pre-soak cases (load loops; the verdict is at the end of the run).

  /** Count HTTP codes (or `error`) by label. */
  const bump = (m: Map<string, number>, k: string) => m.set(k, (m.get(k) ?? 0) + 1);
  /** The JSON body of a send; an `id` with `200`/`202`/`429` means acked (the same rule as `invokeOnce`). */
  const ackedId = async (r: Response | undefined): Promise<string | undefined> => {
    if (!r) return undefined;
    const body = (await r.json().catch(() => ({}))) as { id?: string };
    return body.id && (r.ok || r.status === 202 || r.status === 429) ? body.id : undefined;
  };

  // (a) Hot objects: each key has `HOT_INFLIGHT` one-way senders, each waits for
  // the outcome of its own send and then sends again — the queue per key is ≤ `HOT_INFLIGHT`.
  type HotRec = { n: number; key: string; status?: string; output?: string };
  const hotLedger = new Map<string, HotRec>();
  const hotInflight = new Map<string, number>();
  const hot = {
    sent: 0,
    notAcked: 0,
    signals: new Map<string, number>(),
    cancels: new Map<string, number>(),
  };
  let hotNext = 0;
  const hotRead = async (id: string) => {
    const r = await cfetch(`/invocations/${id}`, undefined, 10_000, hotKey).catch(() => undefined);
    if (!r?.ok) return undefined;
    return (await r.json().catch(() => undefined)) as { status: string; output?: string } | undefined;
  };
  const isFinal = (st: string) => st === "completed" || st === "failed" || st === "paused";
  const hotLoad = Promise.all(
    HOT
      ? Array.from({ length: HOT_KEYS * HOT_INFLIGHT }, async (_, w) => {
          const key = `hot-${w % HOT_KEYS}`;
          while (Date.now() < endAt) {
            const n = hotNext++;
            hot.sent++;
            const r = await cfetch(
              `/invoke/ChaosHot/turn/send?key=${key}`,
              { method: "POST", body: JSON.stringify({ n }) },
              20_000,
              hotKey,
            ).catch(() => undefined);
            const id = await ackedId(r);
            if (!id) {
              hot.notAcked++;
              await sleep(200);
              continue;
            }
            hotLedger.set(id, { n, key });
            hotInflight.set(id, n);
            const deadline = Date.now() + 120_000;
            while (Date.now() < deadline) {
              await sleep(500);
              const st = await hotRead(id);
              if (st && isFinal(st.status)) {
                const rec = hotLedger.get(id)!;
                rec.status = st.status;
                if (st.output !== undefined) rec.output = st.output;
                hotWatch.forget(id);
                break;
              }
            }
            hotInflight.delete(id);
          }
        })
      : [],
  );
  // A `poke` signal (or a cancel, `CHAOS_HOT_CANCEL_SHARE`) to one in-flight send —
  // mostly a send **queued** behind the lock holder.
  const hotSignaler = Promise.all(
    Array.from({ length: HOT ? HOT_SIGNALERS : 0 }, async () => {
      while (Date.now() < endAt) {
        await sleep(HOT_SIGNAL_MS);
        if (hotInflight.size === 0) continue;
        const id = pick([...hotInflight.keys()]);
        const cancel = Math.random() < HOT_CANCEL_SHARE;
        // Record before sending: the closure of whoever enters next may run before the response comes back.
        if (cancel) hotCancelled.add(id);
        const r = await cfetch(
          `/invocations/${id}/${cancel ? "cancel" : "signal"}`,
          cancel
            ? { method: "POST" }
            : {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({ signal: "poke", value: hotInflight.get(id) ?? -1 }),
              },
          10_000,
          hotKey,
        ).catch(() => undefined);
        bump(cancel ? hot.cancels : hot.signals, r ? String(r.status) : "error");
        await r?.body?.cancel().catch(() => undefined);
      }
    }),
  );

  // (e) Parallel branches: one one-way send every `CHAOS_PAR_EVERY_MS` (default tenant).
  const parLedger = new Map<string, number>();
  let parSent = 0;
  let parNotAcked = 0;
  const parLoad = (async () => {
    if (!PAR) return;
    for (let seq = 0; Date.now() < endAt; seq++) {
      const at = Date.now();
      parSent++;
      const r = await cfetch(`/invoke/ChaosPar/fan/send`, { method: "POST", body: JSON.stringify({ seq }) }, 20_000).catch(
        () => undefined,
      );
      const id = await ackedId(r);
      if (id) parLedger.set(id, seq);
      else parNotAcked++;
      await sleep(Math.max(0, PAR_EVERY_MS - (Date.now() - at)));
    }
  })();

  // (c) Tenant purge on a large range: build `CHAOS_PURGE_INVOCATIONS` invocations
  // `ChaosBulk.note[bulk]` (one range) in a first phase, then `deleting`, then follow
  // `GET /admin/tenants/{t}/purge` until `purged` (cap `CHAOS_PURGE_DEADLINE_S`
  // counted from `deleting` — keeps running through the drain phase).
  const purge = {
    acked: 0,
    notAcked: 0,
    bulkMs: 0,
    deletingAt: undefined as number | undefined,
    purgingAt: undefined as number | undefined,
    purgedAt: undefined as number | undefined,
    last: "",
    problems: [] as string[],
    /** `GET …/purge` polls: failed (error/non-2xx/over the cap) in total and in a row, time of the last fresh copy. */
    pollFailures: 0,
    pollTimeouts: 0,
    failStreak: 0,
    lastFreshAt: undefined as number | undefined,
    apiState: undefined as string | undefined,
    /** Reads of the `tenants` registry on the bucket (with an old API, and when arbitrating). */
    bookReads: 0,
    verdictNote: "",
    /** Polls after `purged` where `held` is not yet complete or not yet empty. */
    purgedPolls: 0,
    /** Polls returning `heldComplete: false` (the server ran out of its trash classification cap). */
    heldIncomplete: 0,
    /** The last `lastRound.blocked` printed (printed again only when it changes). */
    lastBlocked: "",
  };
  const purgeRun = (async () => {
    if (!TENANT_PURGE) return;
    const t0 = Date.now();
    let next = 0;
    await Promise.all(
      Array.from({ length: PURGE_CONC }, async () => {
        while (Date.now() < endAt && haltReason === undefined) {
          const n = next++;
          if (n >= PURGE_INVOCATIONS) return;
          const r = await cfetch(
            `/invoke/ChaosBulk/note/send?key=bulk`,
            { method: "POST", body: JSON.stringify({ n }) },
            20_000,
            purgeKey,
          ).catch(() => undefined);
          if (await ackedId(r)) purge.acked++;
          else {
            purge.notAcked++;
            await sleep(200);
          }
        }
      }),
    );
    purge.bulkMs = Date.now() - t0;
    console.log(
      `  [${ts()}] purge: ${purge.acked} invocations of ${PURGE_TENANT} on one range after ${Math.round(purge.bulkMs / 1000)}s ` +
        `(${purge.notAcked} not acked) — sending deleting`,
    );
    let stateGate = false;
    let purgeGate = false;
    const put = await putConfirmed(`/admin/tenants/${PURGE_TENANT}/state`, { state: "deleting" }, 300_000, async (status, text) => {
      if (status !== 409 || !HAS_CTL) return false;
      if (text.includes("tenant_state_disabled") && !stateGate) {
        stateGate = true;
        await ctl<unknown>(["cluster", "enable-tenant-state", "--apply"]);
        return true;
      }
      if (text.includes("tenant_purge_disabled") && !purgeGate) {
        purgeGate = true;
        await ctl<unknown>(["cluster", "enable-tenant-purge", "--apply"]);
        return true;
      }
      return false;
    });
    if (put.at === undefined) {
      purge.problems.push(`deleting did not reach 204 within 300s (last: ${put.last})`);
      return;
    }
    purge.deletingAt = put.at;
    const deletingAt = put.at;
    const deadline = deletingAt + PURGE_DEADLINE_S * 1000;
    let book: BookRead | undefined;
    const apiView = () => ({
      ...(purge.apiState !== undefined ? { state: purge.apiState } : {}),
      failures: purge.pollFailures,
      failStreak: purge.failStreak,
      ...(purge.lastFreshAt !== undefined ? { lastFreshAgeMs: Date.now() - purge.lastFreshAt } : {}),
    });
    const notePurged = (via: string) => {
      purge.purgedAt = Date.now();
      console.log(
        `  [${ts()}] purge: ${PURGE_TENANT} purged ${Math.round((purge.purgedAt - deletingAt) / 1000)}s after deleting (per ${via})`,
      );
    };
    while (Date.now() < deadline) {
      await sleep(10_000);
      const t = Date.now();
      let why: string | undefined;
      const r = await cfetch(`/admin/tenants/${PURGE_TENANT}/purge`, { signal: AbortSignal.timeout(15_000) }, 15_000).catch(
        (e: Error) => {
          why = e.name === "TimeoutError" || e.name === "AbortError" ? "over the cap (15s)" : e.message.slice(0, 120);
          return undefined;
        },
      );
      const v = r?.ok
        ? ((await r.json().catch(() => undefined)) as
            | {
                state?: string;
                purgingMs?: unknown;
                purgedMs?: unknown;
                held?: unknown;
                heldComplete?: boolean;
                heldUnchecked?: number;
                objects?: Record<string, number>;
              }
            | undefined)
        : undefined;
      if (!v) {
        // Do not silently keep the old copy: count, print, and past `PURGE_STALE_POLLS` in a row ask the registry.
        why ??= r ? `HTTP ${r.status}` : "no answer";
        if (why.startsWith("over the cap")) purge.pollTimeouts++;
        purge.pollFailures++;
        purge.failStreak++;
        console.log(
          `  [${ts()}] purge: poll GET /admin/tenants/${PURGE_TENANT}/purge failed after ${Math.round((Date.now() - t) / 1000)}s ` +
            `(${why}) — ${purge.failStreak} in a row, ${purge.pollFailures} in total`,
        );
        if (purge.failStreak >= PURGE_STALE_POLLS) {
          book = await readTenantBook(PURGE_TENANT);
          purge.bookReads++;
          if (book.ok && book.line?.purgingMs && purge.purgingAt === undefined) purge.purgingAt = Date.now();
          if (book.ok && book.line?.state === "purged") {
            notePurged("tenants registry on the bucket — old API");
            purge.verdictNote = judgePurge(apiView(), book, PURGE_STALE_POLLS).note;
            return;
          }
        }
        continue;
      }
      purge.failStreak = 0;
      purge.lastFreshAt = Date.now();
      purge.apiState = v.state;
      purge.last = JSON.stringify(v).slice(0, 400);
      // Print the runner's reason for not finishing whenever it changes: the final copy alone
      // does not say which step a slow purge spent its time on.
      const round = (v as { lastRound?: { blocked?: unknown; rangesPending?: unknown } }).lastRound;
      const blocked = typeof round?.blocked === "string" ? round.blocked.slice(0, 200) : "none";
      const pending = Array.isArray(round?.rangesPending) ? round.rangesPending.length : 0;
      if (blocked !== purge.lastBlocked) {
        purge.lastBlocked = blocked;
        console.log(`  [${ts()}] purge: round blocked: ${blocked} (ranges pending ${pending})`);
      }
      if (v.purgingMs != null && purge.purgingAt === undefined) {
        purge.purgingAt = Date.now();
        console.log(`  [${ts()}] purge: ${PURGE_TENANT} past purging-ms (real deletion starts)`);
      }
      if (v.heldComplete === false) purge.heldIncomplete++;
      if (v.state === "purged") {
        const held = Array.isArray(v.held) ? v.held.length : 0;
        // An older binary has no `heldComplete`: treat it as complete.
        const complete = v.heldComplete !== false;
        if (!complete || held > 0) {
          purge.purgedPolls++;
          if (purge.purgedPolls <= PURGE_HELD_SETTLE_POLLS) {
            console.log(
              `  [${ts()}] purge: ${PURGE_TENANT} purged, held entries ${complete ? `left ${held}` : `incomplete (heldUnchecked ${v.heldUnchecked})`} — ` +
                `polling again (${purge.purgedPolls}/${PURGE_HELD_SETTLE_POLLS})`,
            );
            continue;
          }
          purge.problems.push(
            complete
              ? `purged but ${held} entries still held in place after ${PURGE_HELD_SETTLE_POLLS} polls: ${purge.last}`
              : `purged but none of the ${PURGE_HELD_SETTLE_POLLS} polls after it classified the trash completely: ${purge.last}`,
          );
        }
        const left = Object.entries(v.objects ?? {}).filter(([, c]) => c > 0);
        if (left.length > 0) purge.problems.push(`purged but objects remain: ${left.map(([k, c]) => `${k}=${c}`).join(" ")}`);
        notePurged("API");
        purge.verdictNote = judgePurge(apiView(), undefined, PURGE_STALE_POLLS).note;
        return;
      }
    }
    if (purge.purgedPolls > 0) {
      purge.problems.push(`${PURGE_DEADLINE_S}s ran out waiting for held entries to become empty and complete after purged: ${purge.last}`);
    }
    // Deadline passed: the registry on the bucket is the referee — print both sources, judge by the registry.
    book = await readTenantBook(PURGE_TENANT);
    purge.bookReads++;
    const j = judgePurge(apiView(), book, PURGE_STALE_POLLS);
    purge.verdictNote = j.note;
    console.log(`  [${ts()}] purge: ${PURGE_DEADLINE_S}s since deleting ran out — ${j.note} ⇒ ${j.purged ? "purged" : "not purged"} (per ${j.source})`);
    if (j.purged) {
      purge.purgedAt ??= Date.now();
      return;
    }
    purge.problems.push(
      `did not reach purged within ${PURGE_DEADLINE_S}s since deleting${purge.purgingAt ? "" : " (not past purging-ms yet)"}: ` +
        `${j.problem} — ${j.note}; last fresh API copy: ${purge.last}`,
    );
  })();

  // (d) Periodic backups during auto-split, restored into a side bucket with the same prefix
  // (restore requires exactly the source prefix), `backup check-restore` + `verify` on the
  // restored copy. A refused backup (the range table changed across all attempts) is a valid
  // "reported error"; a complete backup whose restore fails or whose verify finds something
  // no round heals is a violation — the restored copy is kept as is for inspection.
  const backup = {
    cycles: 0,
    ok: 0,
    refused: [] as string[],
    problems: [] as string[],
    healed: new Map<string, number>(),
    /** Recorded limits (`restore.lost_*`, per `check-restore`) of each restored cycle: `cycle: name×count`. */
    lost: [] as Array<{ cycle: number; counts: Map<string, number> }>,
  };
  const lostText = (m: Map<string, number>) =>
    [...m].map(([k, v]) => `${k.replace(/^restore\./, "")}×${v}`).join(" ") || "0";
  const backupRun = (async () => {
    if (!BACKUP_RESTORE) return;
    const src = new URL(STORE!);
    const prefix = src.pathname.replace(/^\/+|\/+$/g, "");
    if (prefix === "") {
      backup.problems.push(`${STORE} has no prefix — the restored copy must live under exactly the source prefix`);
      return;
    }
    const backupRoot = `s3://${src.hostname}/backup-${prefix.replaceAll("/", "-")}`;
    const restoreStore = `s3://${RESTORE_BUCKET}/${prefix}`;
    const aws = (args: string[]) =>
      new Promise<string>((resolve, reject) =>
        execFile("aws", ["--endpoint-url", env.AWS_ENDPOINT ?? "", ...args], { timeout: 600_000, env }, (e, out, err) =>
          e ? reject(new Error(`aws ${args.slice(0, 3).join(" ")}: ${String(err).trim().slice(0, 300) || e.message}`)) : resolve(out),
        ),
      );
    try {
      await aws(["s3api", "create-bucket", "--bucket", RESTORE_BUCKET]).catch((e: Error) => {
        if (!/BucketAlreadyOwnedByYou|BucketAlreadyExists/.test(e.message)) throw e;
      });
      if ((await aws(["s3", "ls", `s3://${RESTORE_BUCKET}/${prefix}/`]).catch(() => "")).trim() !== "") {
        backup.problems.push(`${restoreStore} already has objects before the run — not restoring over them`);
        return;
      }
    } catch (e) {
      backup.problems.push(`could not set up the side bucket ${RESTORE_BUCKET}: ${(e as Error).message}`);
      return;
    }
    /**
     * Findings of `verify`/`check-restore` (`chaos-cases.ts::judgeRestoreCheck`):
     * `restore.lost_*` is a recorded limit of backups (runbook restore-from-backup,
     * "No common cut across ranges") — a correct finding, counted and printed, not a failure;
     * a round heals it ⇒ only counted; anything else ⇒ a violation. Returns `[violations, recorded limits]`.
     */
    const judge = (what: string, run: CtlRun): [string[], Map<string, number>] => {
      const j = judgeRestoreCheck(what, run);
      for (const [k, v] of j.healed) backup.healed.set(k, (backup.healed.get(k) ?? 0) + v);
      return [j.violations, j.known];
    };
    /** Remove every copy under `backupRoot` that has no `BACKUP` manifest (partial). */
    const removePartialBackups = async (): Promise<string> => {
      const root = new URL(backupRoot);
      const bucket = root.hostname;
      const key = root.pathname.replace(/^\/+/, "");
      const ids = backupIdsIn(await aws(["s3", "ls", `${backupRoot}/`]).catch(() => ""));
      let n = 0;
      let unsure = 0;
      for (const id of ids) {
        // Only a manifest that is surely absent (404) makes a copy partial; any other answer
        // keeps it.
        const head = await aws(["s3api", "head-object", "--bucket", bucket, "--key", `${key}/${id}/BACKUP`]).then(
          () => "complete",
          (e: Error) => (/\b404\b|Not Found/.test(e.message) ? "partial" : "unsure"),
        );
        if (head === "unsure") unsure++;
        if (head !== "partial") continue;
        await aws(["s3", "rm", "--recursive", "--only-show-errors", `${backupRoot}/${id}/`]);
        n++;
      }
      return `removed ${n} partial cop${n === 1 ? "y" : "ies"}${unsure > 0 ? `, ${unsure} kept (manifest check failed)` : ""}`;
    };
    let next = startedAt + BACKUP_FIRST_S * 1000;
    for (;;) {
      await sleepUntil(Math.max(0, next - Date.now()), endAt);
      if (Date.now() >= endAt || haltReason !== undefined) return;
      next = Date.now() + BACKUP_EVERY_S * 1000;
      const cycle = ++backup.cycles;
      const t0 = Date.now();
      const b = await ctlRun(STORE!, ["backup", "--to", backupRoot], BACKUP_TIMEOUT_S * 1000);
      if (b.code !== 0) {
        const how = describeCtlExit(b, BACKUP_TIMEOUT_S * 1000);
        // Code 3 = refused (the range table changed across every attempt): a valid reported
        // error. A timeout, a signal or any other code is a backup that does not work.
        if (b.code === 3 && !b.timedOut) backup.refused.push(`cycle ${cycle}: backup ${how}`);
        else backup.problems.push(`cycle ${cycle}: backup ${how}`);
        // A failed or refused run leaves partial copies (no `BACKUP`) that `restore` never
        // uses; only `backup prune` removes them, after 24 h. Remove them now: they filled
        // the disk of a 24 h soak.
        const removed = await removePartialBackups().catch((e: Error) => `could not remove partial copies: ${e.message}`);
        console.log(`  [${ts()}] backup cycle ${cycle}: backup refused/failed — ${how}; ${removed}`);
        continue;
      }
      const rep = JSON.parse(b.out) as { id: string; ranges: number[]; objects: number; bytes: number; warnings: string[] };
      const rs = await ctlRun(restoreStore, ["restore", "--from", backupRoot, "--id", rep.id], BACKUP_TIMEOUT_S * 1000);
      const found: string[] = [];
      let lost = new Map<string, number>();
      if (rs.code !== 0) {
        found.push(`restore ${rep.id} exited ${rs.code}: ${rs.err.trim().slice(0, 300)}`);
      } else {
        // `verify` opens the same Dbs with the same restore check: lost_* is counted per `check-restore`.
        const [v1, k1] = judge("backup check-restore", await ctlRun(restoreStore, ["backup", "check-restore"], BACKUP_TIMEOUT_S * 1000));
        const [v2] = judge("verify", await ctlRun(restoreStore, ["verify"], BACKUP_TIMEOUT_S * 1000));
        found.push(...v1, ...v2);
        lost = k1;
        backup.lost.push({ cycle, counts: k1 });
      }
      console.log(
        `  [${ts()}] backup cycle ${cycle}: ${rep.id} — ${rep.ranges.length} ranges, ${rep.objects} objects, ` +
          `${Math.round(rep.bytes / 2 ** 20)} MiB${rep.warnings.length > 0 ? `, ${rep.warnings.length} warnings` : ""}; ` +
          `restore + check ${found.length === 0 ? "clean" : `${found.length} violations`}` +
          `${lost.size > 0 ? ` (recorded limit — correct finding: ${lostText(lost)})` : ""} after ${Math.round((Date.now() - t0) / 1000)}s`,
      );
      if (found.length > 0) {
        backup.problems.push(...found.map((f) => `cycle ${cycle} (${rep.id}, restored copy kept at ${restoreStore}): ${f}`));
        return;
      }
      backup.ok++;
      // Clean up the restored copy and the backup of a clean cycle: the next cycle restores into an empty place.
      await aws(["s3", "rm", "--recursive", "--only-show-errors", `s3://${RESTORE_BUCKET}/${prefix}/`]).catch((e: Error) =>
        backup.problems.push(`could not clean up ${restoreStore}: ${e.message}`),
      );
      await aws(["s3", "rm", "--recursive", "--only-show-errors", `${backupRoot}/${rep.id}/`]).catch(() => undefined);
      if (backup.problems.length > 0) return;
    }
  })();

  // (b) `running` standing still while the cluster is healthy: scrape the cluster-wide list every
  // `CHAOS_STUCK_EVERY_S` from the start of the run until the end of the drain phase. Tenants whose state changed
  // (rescheduled while suspended, FL-24) and the purge tenant are not considered.
  const stuckWatch = new StuckWatch(STUCK_RUNNING_S * 1000);
  let stuckDone = false;
  let stuckMisses = 0;
  const stuckTenants: Array<string | undefined> = AUTH
    ? ["default", ...tenantKeys.map((t) => t.tenant), ...(LARGE_VALUES ? [LV_TENANT] : []), ...(HOT ? [HOT_TENANT] : [])]
    : [undefined];
  const stuckSampler = (async () => {
    if (STUCK_RUNNING_S <= 0) return;
    while (!stuckDone) {
      const until = Date.now() + STUCK_EVERY_S * 1000;
      while (!stuckDone && Date.now() < until) await sleep(500);
      if (stuckDone) return;
      if (aliveNodes().length < nodes.length || respawning.size > 0 || outageActive || slowTarget !== undefined) noteDisruption();
      const rows: RunningRow[] = [];
      for (const t of stuckTenants) {
        if (t !== undefined && churnedEver.has(t)) continue;
        const r = await cfetch(
          `/invocations?scope=cluster&status=running&limit=200${t ? `&tenant=${t}` : ""}`,
          { signal: AbortSignal.timeout(15_000) },
          10_000,
        ).catch(() => undefined);
        const b = (await r?.json().catch(() => undefined)) as { invocations?: RunningRow[] } | undefined;
        if (!b?.invocations) {
          stuckMisses++;
          continue;
        }
        rows.push(...b.invocations.map((x) => ({ ...x, tenant: t ?? "default" })));
      }
      const now = Date.now();
      stuckWatch.observe(rows, now, healthySince);
      const win = healthyWindowAt(now);
      if (win) {
        win.rounds++;
        for (const r of rows) {
          if (r.status !== "running") continue;
          win.ids.add(r.id);
          win.maxAgeMs = Math.max(win.maxAgeMs, now - Math.max(r.modifiedAt ?? now, healthySince));
        }
      }
    }
  })();

  // ── SSO: one login through the fake issuer on a live node.
  const sso = { ok: 0, tolerated: 0, failed: 0, tolerations: new Map<string, number>(), problems: [] as string[] };
  /** One full login on `base`: `undefined` = done; otherwise the reason and whether it can be excused. */
  async function ssoLogin(base: string): Promise<{ why: string; tolerable: boolean } | undefined> {
    const cookieOf = (r: Response, name: string) =>
      r.headers
        .getSetCookie()
        .map((c) => c.split(";")[0]!.trim())
        .find((c) => c.startsWith(`${name}=`) && c.length > name.length + 1);
    const step = async (what: string, url: string, cookie?: string) => {
      const headers: Record<string, string> = cookie ? { cookie } : {};
      try {
        return await fetch(url, { redirect: "manual", headers, signal: AbortSignal.timeout(15_000) });
      } catch (e) {
        const err = e as Error & { cause?: { code?: string } };
        // Running out of the time cap is not "node dead" — only excused when the node really died (the caller decides).
        const timeout = err.name === "TimeoutError";
        throw Object.assign(new Error(`${what}: ${timeout ? "15 s ran out" : (err.cause?.code ?? err.message)}`), { tolerable: !timeout });
      }
    };
    const unexpected = async (what: string, r: Response) => ({
      why: `${what}: ${r.status} ${r.headers.get("location") ?? (await r.text().catch(() => "")).slice(0, 160)}`,
      tolerable: r.status === 503,
    });
    try {
      const start = await step("start", `${base}/ui/login/oidc`);
      if (start.status !== 303) return unexpected("start", start);
      const flow = cookieOf(start, "__Host-cairn_oidc");
      const authorize = start.headers.get("location");
      if (!flow || !authorize) return { why: "start: missing the __Host-cairn_oidc cookie or Location", tolerable: false };
      const idp = await step("issuer", authorize);
      const callback = idp.headers.get("location");
      if (idp.status !== 302 || !callback) return unexpected("issuer", idp);
      if (!callback.startsWith(`${base}/ui/login/oidc/callback`)) {
        return { why: `the issuer does not come back to the node's callback: ${callback.slice(0, 160)}`, tolerable: false };
      }
      const back = await step("callback", callback, flow);
      if (back.status !== 303) return unexpected("callback", back);
      const landed = back.headers.get("location") ?? "";
      const ssoError = /[?&]sso_error=([a-z_]+)/.exec(landed)?.[1];
      // `unavailable`: the registry is not confirmed fresh, or the audit could not record the intent — the node was just rebuilt / the store is down.
      if (ssoError) return { why: `callback: sso_error=${ssoError}`, tolerable: ssoError === "unavailable" };
      const session = cookieOf(back, "__Host-cairn_ui");
      if (!session) return { why: `callback landed on ${landed} without a __Host-cairn_ui cookie`, tolerable: false };
      const who = await step("whoami", `${base}/auth/whoami`, session);
      if (who.status !== 200) return unexpected("whoami", who);
      const w = (await who.json().catch(() => ({}))) as { via?: string; tenant?: string };
      if (w.via !== "sso" || w.tenant !== SSO_TENANT) {
        return { why: `whoami: via=${w.via} tenant=${w.tenant}, want sso/${SSO_TENANT}`, tolerable: false };
      }
      return undefined;
    } catch (e) {
      return { why: (e as Error).message, tolerable: (e as { tolerable?: boolean }).tolerable ?? false };
    }
  }
  const ssoLoop = (async () => {
    if (!issuer) return;
    while (Date.now() < endAt) {
      await sleepUntil(SSO_EVERY_S * 1000, endAt);
      if (Date.now() >= endAt) break;
      const alive = nodes.map((n, i) => [n, i] as const).filter(([n]) => n.alive);
      if (alive.length === 0) continue;
      const [node, ni] = pick(alive);
      const outageBefore = outageActive;
      const bad = await ssoLogin(node.url);
      if (!bad) {
        sso.ok++;
        continue;
      }
      // The node was killed/exited on its own mid-login, or the store is down: excused, counted by reason.
      const died = !node.alive || nodes[ni] !== node;
      if (bad.tolerable || died || outageBefore || outageActive) {
        sso.tolerated++;
        const k = died ? "node died mid-login" : outageBefore || outageActive ? "store outage" : bad.why.replace(/\d+/g, "N").slice(0, 80);
        sso.tolerations.set(k, (sso.tolerations.get(k) ?? 0) + 1);
        continue;
      }
      sso.failed++;
      const line = `${ts()} ${node.name} (${node.url}): ${bad.why}`;
      console.error(`  SSO FAILED: ${line}`);
      sso.problems.push(line);
    }
  })();

  // ── Long store outage: once per run, stop the store container for longer than the lease TTL.
  const outage: {
    stoppedAt?: number;
    startedAgainAt?: number;
    storeBackAt?: number;
    acceptAt?: number;
    problems: string[];
  } = { problems: [] };
  const outager = (async () => {
    if (!STORE_OUTAGE) return;
    const c = OUTAGE_CONTAINER!;
    await sleep(Math.max(0, startedAt + OUTAGE_AT_S * 1000 - Date.now()));
    outageActive = true;
    const win = { from: Date.now(), to: Infinity };
    outageWindows.push(win);
    try {
      console.log(`  [${ts()}] STORE OUTAGE: docker stop ${c}`);
      await docker(["stop", "-t", "1", c]);
      outageStopped = true;
      outage.stoppedAt = Date.now();
    } catch (e) {
      outage.problems.push(`could not stop the container: ${(e as Error).message}`);
      outageActive = false;
      win.to = Date.now();
      return;
    }
    await sleep(OUTAGE_S * 1000);
    for (let k = 1; outageStopped; k++) {
      try {
        await docker(["start", c]);
        outageStopped = false;
        outage.startedAgainAt = Date.now();
        console.log(`  [${ts()}] STORE OUTAGE: docker start ${c} after ${((Date.now() - outage.stoppedAt) / 1000).toFixed(1)}s`);
      } catch (e) {
        if (k >= 5) {
          outage.problems.push(`could not start the container again after ${k} attempts: ${(e as Error).message}`);
          break;
        }
        await sleep(2_000);
      }
    }
    if (outage.startedAgainAt !== undefined) {
      const probeEnd = Date.now() + 120_000;
      while (Date.now() < probeEnd && outage.storeBackAt === undefined) {
        if (await storeAnswers()) outage.storeBackAt = Date.now();
        else await sleep(500);
      }
      if (outage.storeBackAt === undefined) outage.problems.push(`${env.AWS_ENDPOINT} did not answer within 120s after docker start`);
    }
    if (outage.storeBackAt !== undefined) {
      // The cluster takes work again = one send acked after the store is back. If the load is over
      // and none was seen yet, send one ourselves (echo).
      const before = ackCount();
      const recoverEnd = Date.now() + OUTAGE_RECOVER_S * 1000;
      while (Date.now() < recoverEnd && outage.acceptAt === undefined) {
        if (ackCount() > before) {
          outage.acceptAt = Date.now();
          break;
        }
        if (Date.now() >= endAt && !billing) await invokeOnce("chaos", undefined, false);
        else await sleep(250);
      }
      if (outage.acceptAt === undefined) {
        outage.problems.push(`the cluster acked no work within ${OUTAGE_RECOVER_S}s after the store answered`);
      } else {
        console.log(
          `  [${ts()}] STORE OUTAGE: the cluster takes work again ${((outage.acceptAt - outage.storeBackAt) / 1000).toFixed(1)}s ` +
            `after the store answered`,
        );
      }
    }
    win.to = outage.acceptAt ?? Date.now();
    outageActive = false;
  })();

  // Split/merge interleaved with kills: one job every half cycle — split, then merge the child pair.
  const churner =
    SPLIT_EVERY_S > 0
      ? new RangeChurner((path, init) => cfetch(path, init), {
          everyMs: (SPLIT_EVERY_S * 1000) / 2,
          log: (line) => console.log(line),
          // An error injected by the harness itself (`failpoint <name>: chaos`) is a deliberately
          // failed step, rolled back correctly — not a response outside the contract.
          ...(FAILPOINTS ? { injected: (_status: number, text: string) => text.includes("failpoint ") } : {}),
        })
      : undefined;
  churner?.start();

  // ── Invariant 5: the watcher counts split/merge operations **in progress at the same time** on
  // the table of every live node. One operation = one split intent (by left child),
  // or one pair of merge intents (by target range).
  const checkOverlap = AUTO_SPLIT || INJECT === "overlap";
  const opsSeen = new Set<string>();
  let maxConcurrentOps = 0;
  let concurrentAt = "";
  let watchDone = false;
  const watcher = (async () => {
    if (!checkOverlap) return;
    while (!watchDone) {
      await Promise.all(
        aliveNodes().map(async (n) => {
          const body = (await nfetch(`${n.url}/cluster`, { signal: AbortSignal.timeout(2_000) })
            .then((r) => r.json())
            .catch(() => undefined)) as { version?: number; ranges?: RangeRow[] } | undefined;
          const ops = new Set<string>();
          for (const r of body?.ranges ?? []) {
            const i = r.intent as { kind?: string; left?: number; into?: number } | undefined;
            if (!i) continue;
            ops.add(i.kind === "merging" ? `merge→${i.into}` : `split→${i.left}`);
          }
          for (const o of ops) {
            if (!opsSeen.has(o)) {
              opsSeen.add(o);
              console.log(`  [${ts()}] saw intent ${o} (table v${body?.version}, ${n.url})`);
            }
          }
          if (ops.size > maxConcurrentOps) {
            maxConcurrentOps = ops.size;
            concurrentAt = `${ts()} ${n.url} v${body?.version}: ${[...ops].join(", ")}`;
          }
        }),
      );
      await sleep(OVERLAP_POLL_MS);
    }
  })();
  const overlapInjection = (async () => {
    if (INJECT !== "overlap") return;
    // Two manual splits on two ranges. Sent at the same time, the two table CASes race
    // and the loser gets 409 — so the second command goes **after** the first
    // command's intent is in the table: manual splits do not exclude each other by
    // design, so it goes through and the two intents live together.
    for (let k = 0; k < 3 && Date.now() < endAt; k++) {
      await sleep(20_000);
      const rows = (await rangesOf((p, i) => cfetch(p, i)).catch(() => [])).filter((r) => !r.intent);
      if (rows.length < 2) continue;
      const [a, b] = [rows[0]!, rows[rows.length - 1]!];
      // A split on SeaweedFS finishes in a few tens of ms — the later command arrives after the first
      // has committed. Slow down PUTs under `ranges/` (clone, manifest) on every
      // node meanwhile, so the two intents really live together.
      const slowed = aliveNodes();
      await Promise.all(slowed.map((n) => setFailpoint(n, "node::put_delay", "return(1000:/ranges/)").catch(() => undefined)));
      const status = (x: Response) => x.text().then((t) => `${x.status} ${t.slice(0, 120)}`);
      const first = cfetch(`/ranges/${a.range}/split`, { method: "POST" }).then(status, (e) => `${e}`);
      const seen = Date.now() + 5_000;
      while (Date.now() < seen) {
        const now = await rangesOf((p, i) => cfetch(p, i, 2_000)).catch(() => [] as RangeRow[]);
        if (now.some((r) => r.range === a.range && r.intent)) break;
        await sleep(20);
      }
      // The node taking the later command may not have learned the new table yet (409 "range table changed
      // midway"): retry densely while the first command is still in flight.
      let second = "";
      let done = false;
      void first.then(() => (done = true));
      do {
        second = await cfetch(`/ranges/${b.range}/split`, { method: "POST" }).then(status, (e) => `${e}`);
        if (!second.startsWith("409")) break;
        await sleep(50);
      } while (!done);
      const firstDone = await first;
      await Promise.all(slowed.map((n) => setFailpoint(n, "node::put_delay", "off").catch(() => undefined)));
      console.log(`  [${ts()}] INJECT: manual split ${a.range} then ${b.range} while the first intent is alive → ${firstDone.slice(0, 80)} | ${second.slice(0, 80)}`);
    }
  })();

  // ── Request counting: scrape periodically, so a node exiting on its own loses only one tick.
  const scraper = (async () => {
    if (!COUNTING) return;
    while (Date.now() < endAt) {
      await sleepUntil(SCRAPE_EVERY_S * 1000, endAt);
      await Promise.all(aliveNodes().map(scrapeStore));
    }
  })();

  // ── Query loop.
  const query = {
    ok: 0,
    partial: 0,
    // 429 (slot, bucket), 503 (missing range) and network errors to a node just killed
    // are not errors of the query layer.
    refused: 0,
    serverErrors: [] as string[],
    wrong: [] as string[],
  };
  let queryDone = false;
  const QUERIES = [
    "SELECT id, created_at FROM sys_invocation ORDER BY created_at DESC, id LIMIT 50",
    "SELECT id, transitioned_at FROM sys_invocation ORDER BY transitioned_at DESC, id LIMIT 50",
    "SELECT class, count(*) FROM sys_invocation GROUP BY class",
    "SELECT class, sum(count) FROM sys_invocation_counts GROUP BY class",
    "SELECT range, gen FROM sys_range",
  ];
  const askQuery = async (sql: string) => {
    const alive = aliveNodes();
    if (alive.length === 0) return undefined;
    const n = alive[Math.floor(Math.random() * alive.length)]!;
    const r = await nfetch(`${n.url}/query`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ sql }),
      signal: AbortSignal.timeout(15_000),
    }).catch(() => undefined);
    if (!r) {
      query.refused++;
      return undefined;
    }
    const text = await r.text().catch(() => "");
    if (r.status >= 500 && r.status !== 503) {
      if (query.serverErrors.length < 20) query.serverErrors.push(`${ts()} ${n.name} ${r.status} ${sql}: ${text.slice(0, 200)}`);
      return undefined;
    }
    if (r.status !== 200) {
      query.refused++;
      return undefined;
    }
    const v = JSON.parse(text) as { rows: unknown[][]; partial: boolean };
    if (v.partial) query.partial++;
    else query.ok++;
    return v;
  };
  const querier = (async () => {
    if (!QUERY_LOOP) return;
    let i = 0;
    while (!queryDone) {
      const sql = QUERIES[i++ % QUERIES.length]!;
      const v = await askQuery(sql);
      if (v && !v.partial && sql.includes("ORDER BY")) {
        // Declared order: time descending. Compare parsed times, not strings:
        // Arrow prints whole seconds without `.000` (`…21Z` sorts after `…21.256Z` by bytes).
        const times = v.rows.map((r) => Date.parse(String(r[1])));
        for (let k = 1; k < times.length; k++) {
          if (times[k - 1]! < times[k]!) {
            if (query.wrong.length < 20) query.wrong.push(`${ts()} wrong order: ${sql}: ${times[k - 1]} < ${times[k]}`);
            break;
          }
        }
      }
      await sleep(QUERY_EVERY_MS);
    }
  })();

  // ── Invariant 4: never two leaders for one range.
  const PROBE_TAIL = "11".repeat(16);
  let probeRounds = 0;
  let probeSuspects = 0;
  const doubleLeaders: string[] = [];
  let probeDone = false;
  /** Range count of the most recent table read — the periodic summary prints it (it grew from 8 to ~130 ranges in an earlier soak). */
  let lastRangeCount = 0;
  // `cancel` of an id that does not exist: the leader answers 404 without writing anything, other
  // nodes answer 503 — it does not fall onto the follower read path like `GET` (see the head of the file).
  const probeStatus = (url: string, id: string) =>
    (PROBE === "status"
      ? nfetch(`${url}/invocations/${id}`, { signal: AbortSignal.timeout(1_500) })
      : nfetch(`${url}/invocations/${id}/cancel`, { method: "POST", signal: AbortSignal.timeout(1_500) }))
      .then(async (r) => {
        await r.arrayBuffer().catch(() => undefined);
        return r.status;
      })
      .catch(() => 0);
  const prober = (async () => {
    if (LEADER_PROBE_MS <= 0) return;
    while (!probeDone) {
      const rows = await rangesOf((p, i) => cfetch(p, i, 5_000)).catch(() => [] as RangeRow[]);
      if (rows.length > 0) {
        lastRangeCount = rows.length;
        if (AUTO_MAX_RANGES > 0 && rangeCountSeen < AUTO_MAX_RANGES && rows.length >= AUTO_MAX_RANGES) {
          console.log(`  [${ts()}] auto-split: ${rows.length} ranges ≥ CHAOS_AUTO_MAX_RANGES ${AUTO_MAX_RANGES} — nodes respawn with --auto-split suggest`);
        }
        rangeCountSeen = rows.length;
      }
      const targets = [...aliveNodes(), ...(rogue ? [rogue] : [])];
      await Promise.all(
        rows.map(async (row) => {
          const id = row.start + PROBE_TAIL;
          const st = await Promise.all(targets.map((t) => probeStatus(t.url, id)));
          let leaders = targets.filter((_, k) => st[k] === 404);
          if (leaders.length <= 1) return;
          probeSuspects++;
          // Two answers may fall on both sides of a fast handover:
          // ask again right away, twice, only the nodes that just answered.
          for (let k = 0; k < 2; k++) {
            const again = await Promise.all(leaders.map((t) => probeStatus(t.url, id)));
            leaders = leaders.filter((_, j) => again[j] === 404);
            if (leaders.length <= 1) return;
          }
          const line = `${ts()} range ${row.range} (key ${row.start}): ${leaders.map((t) => `${t.name} ${t.url}`).join(", ")}`;
          if (doubleLeaders.length < 50) console.error(`  TWO LEADERS: ${line}`);
          doubleLeaders.push(line);
        }),
      );
      probeRounds++;
      await sleep(LEADER_PROBE_MS);
    }
  })();

  // ── Invariant 7 (periodic): `cairn-ctl verify --no-open`.
  const FATAL_MIDRUN = new Set(["db", "external_db"]);
  let verifyRuns = 0;
  const verifyProblems: string[] = [];
  const verifyLoop = (async () => {
    if (!HAS_CTL) return;
    while (Date.now() < endAt) {
      await sleepUntil(VERIFY_EVERY_S * 1000, endAt);
      if (Date.now() >= endAt) break;
      try {
        const first = await ctl<VerifyReport>(["verify", "--no-open"]);
        verifyRuns++;
        const bad = first.findings.filter((f) => FATAL_MIDRUN.has(f.check));
        const info = first.findings.filter((f) => !FATAL_MIDRUN.has(f.check));
        if (info.length > 0) {
          console.log(`  [${ts()}] verify (mid-run, noted only): ${info.map((f) => `${f.check}@${f.range}`).join(" ")}`);
        }
        if (bad.length === 0) continue;
        await sleep(5_000);
        const again = await ctl<VerifyReport>(["verify", "--no-open"]);
        const key = (f: Finding) => `${f.check}@${f.range}`;
        const still = again.findings.filter((f) => FATAL_MIDRUN.has(f.check) && bad.some((b) => key(b) === key(f)));
        for (const f of still) {
          const line = `${ts()} ${f.check} range ${f.range}: ${f.detail}`;
          console.error(`  VERIFY: ${line}`);
          verifyProblems.push(line);
        }
      } catch (e) {
        console.log(`  [${ts()}] verify failed (skipping this round): ${(e as Error).message}`);
      }
    }
  })();

  // ── Invariant 8: the shared WAL floor does not stand still longer than FLOOR_STUCK_S.
  const floors = new Map<string, FloorTrack>();
  let floorRuns = 0;
  let floorAdvances = 0;
  let floorLogsGone = 0;
  /** Logs already seen carrying the `forgotten` mark — each log counted once into "logs cleaned up". */
  const forgottenLogs = new Set<string>();
  const floorProblems: string[] = [];
  /** Ranges whose owner record still lists `log` (`wal_segments`), with their current owner. */
  async function holdersOf(log: string): Promise<string> {
    return ctl<OwnerView[]>(["owners"])
      .then((owners) => {
        const holders = owners
          .filter((o) => (o.walSegments ?? []).some((seg) => seg.split("@")[0] === log))
          .map(
            (o) =>
              `${o.range}${o.inTable ? "" : "(off table)"}@${o.session ?? "?"}` +
              `(${o.sessionState ?? "?"}${o.released ? ",released" : ""}${o.retired ? ",tombstone" : ""})`,
          );
        return holders.length > 0 ? holders.slice(0, 8).join(" ") : "no owner record";
      })
      .catch((e) => `reading owners failed: ${(e as Error).message}`);
  }
  async function readFloors(final: boolean) {
    const logs = await ctl<WalLogView[]>(["wal", "floors"]);
    floorRuns++;
    const now = Date.now();
    const seen = new Set<string>();
    for (const l of logs) {
      seen.add(l.log);
      // Full cleanup (newer binaries): the seal and the mark stay, the floor reads 0 — the log no longer
      // disappears from the list, and that 0 is not a floor advance.
      if (l.forgotten) {
        if (!forgottenLogs.has(l.log)) {
          forgottenLogs.add(l.log);
          floorLogsGone++;
        }
        floors.delete(l.log);
        continue;
      }
      const step = watchFloor(floors, l.log, l.floor, l.objects, now, FLOOR_STUCK_S);
      if (step.advanced) floorAdvances++;
      const stuckS = step.stuckS;
      if (step.halfS != null) {
        // Noted only: who holds the log **while** it is held — at the threshold the last
        // holder may be gone already.
        console.log(
          `  [${ts()}] wal floor (noted only): log ${l.log} (${l.sessionState ?? "session cleaned up"}) floor ${l.floor} ` +
            `still for ${Math.round(step.halfS)}s, ${l.objects} objects, pinned by ${l.pinnedByRange ?? "?"}, ` +
            `listed by ${await holdersOf(l.log)}`,
        );
      }
      if (stuckS !== null) {
        // "pinned by ?" told nothing in an earlier 4 h soak — also print the ranges whose owner
        // record still lists the log (`wal_segments`) together with its current owner, and the seal.
        const listing = await holdersOf(l.log);
        const line =
          `${ts()} log ${l.log} (${l.sessionState ?? "session cleaned up"}): floor ${l.floor} still for ${Math.round(stuckS)}s, ` +
          `${l.objects} objects${l.sealedAt != null ? `, seal@${l.sealedAt}` : ""}, pinned by ${l.pinnedByRange ?? "?"}, ` +
          `listed by ${listing}${final ? " (end of run)" : ""}`;
        console.error(`  WAL FLOOR: ${line}`);
        floorProblems.push(line);
      }
    }
    for (const k of [...floors.keys()]) {
      // The log vanished from the bucket entirely (binaries from before full cleanup).
      if (!seen.has(k)) {
        floors.delete(k);
        floorLogsGone++;
      }
    }
  }
  const floorLoop = (async () => {
    if (!FLOORS) return;
    while (Date.now() < endAt) {
      try {
        await readFloors(false);
      } catch (e) {
        console.log(`  [${ts()}] wal floors failed (skipping this round): ${(e as Error).message}`);
      }
      await sleepUntil(FLOOR_EVERY_S * 1000, endAt);
    }
  })();

  // ── Invariant 6 (periodic): the quiet tenants' p99 per window, compared with the baseline load.
  const quietLimit = (base: number[]) => Math.max(QUIET_P99_MS, Math.round(QUIET_VS_BASE * quantile(base, 0.99)));
  const quietProblems: string[] = [];
  const quietPool = new QuietPool(QUIET_MIN_SAMPLES, quietLimit);
  const quietWatch = (async () => {
    if (TENANTS < 2) return;
    let from = startedAt;
    while (Date.now() < endAt) {
      await sleepUntil(QUIET_WINDOW_S * 1000, endAt);
      const to = Date.now();
      const inWin = (xs: Array<[number, number]>) => xs.filter(([at]) => at >= from && at < to);
      const winRows = {
        quiet: inWin(latency.quiet),
        noisy: latency.noisy.take(from, to),
        base: inWin(latency.base),
        rr: latency.rr.take(from, to),
      };
      const win = winRows.quiet.map(([, ms]) => ms);
      const noisy = winRows.noisy.map(([, ms]) => ms);
      const base = winRows.base.map(([, ms]) => ms);
      const p99 = quantile(win, 0.99);
      const limit = quietLimit(base);
      // Raw samples of the window (needs `CHAOS_LOG_DIR`): `class\tsend time\tms` — to tell
      // after the run whether a failed window is broken isolation or the noise of a p99 over few samples.
      if (LOG_DIR) {
        const rows = (["quiet", "noisy", "base", "rr"] as const).flatMap((k) =>
          winRows[k].map(([at, ms]) => `${k}\t${at}\t${ms}\n`),
        );
        try {
          appendFileSync(join(LOG_DIR, "latency.tsv"), rows.join(""));
        } catch (e) {
          // Diagnostics only — unless the disk is full (stops the destructive phase).
          noteWriteError(join(LOG_DIR, "latency.tsv"), e);
        }
      }
      const rr = winRows.rr.map(([, ms]) => ms);
      console.log(
        `  [${ts()}] tenants: window ${Math.round((to - from) / 1000)}s — quiet ${win.length} calls, p50 ${quantile(win, 0.5)} p99 ${p99} ms ` +
          `(threshold ${limit}); noisy ${noisy.length}, p99 ${quantile(noisy, 0.99)} ms; baseline ${base.length}, p99 ${quantile(base, 0.99)} ms` +
          (RR_TENANT !== undefined ? `; rr ${rr.length}, p99 ${quantile(rr, 0.99)} ms (print only)` : ""),
      );
      // A window is only printed: with ~160 quiet calls its "p99" is the second-slowest call,
      // and one leader handover fails it. Windows are pooled until there are
      // `QUIET_MIN_SAMPLES` quiet calls, and the pool is judged (see `quiet.ts`).
      const pooled = quietPool.add(win, base);
      if (pooled) {
        console.log(
          `  [${ts()}] tenants: pool of ${pooled.windows} windows up to ${new Date(to).toISOString()} — quiet ${pooled.quiet} calls, p99 ${pooled.p99} ms (threshold ${pooled.limit})`,
        );
        if (pooled.fails) {
          const line = `${pooled.windows} windows up to ${new Date(to).toISOString()}: quiet p99 ${pooled.p99} ms over ${pooled.quiet} calls > threshold ${pooled.limit} ms`;
          console.error(`  QUIET: ${line}`);
          quietProblems.push(line);
        }
      }
      from = to;
    }
  })();

  // ── Counts: harvest `/metrics` per process until the end of the destructive phase — the
  // requests per step only count within the window with load.
  let harvestRounds = 0;
  // ── History checker: reread ids already seen `completed`, at random,
  // while chaos is still running — a durable result vanishing midway (then rerun
  // to completion with the same content) only shows on a read that lands exactly in that gap.
  const historyProbe = (async () => {
    if (HISTORY_PROBE_MS <= 0) return;
    while (Date.now() < endAt) {
      await sleepUntil(HISTORY_PROBE_MS, endAt);
      const order = historyTracker.completedOrder;
      const total = order.length + looseCompletedSeen.length;
      for (let k = 0; k < Math.min(HISTORY_PROBE_IDS, total); k++) {
        const j = Math.floor(Math.random() * total);
        if (j < order.length) {
          const idx = order.get(j);
          await readStatus(ackLedger.id(idx), 5_000, ADMIN_KEY, idx).catch(() => undefined);
        } else {
          await readStatus(looseCompletedSeen[j - order.length]!, 5_000).catch(() => undefined);
        }
      }
    }
  })();

  const harvester = (async () => {
    if (HARVEST_MS <= 0) return;
    while (Date.now() < endAt) {
      await Promise.all(
        aliveNodes().map(async (n) => {
          const text = await nfetch(`${n.url}/metrics`, { signal: AbortSignal.timeout(HARVEST_MS) })
            .then((r) => (r.ok ? r.text() : ""))
            .catch(() => "");
          if (text && n.proc.pid !== undefined) harvestText(n.proc.pid, text);
          if (text) dumpMetrics(n, text);
        }),
      );
      harvestRounds++;
      // Drop the previous scrape of a dead process (each kill gives a new pid — the book
      // would be unbounded under a long soak; a reused pid starting clean is correct).
      const alive = new Set(aliveNodes().map((n) => n.proc.pid));
      for (const pid of harvestLast.keys()) if (!alive.has(pid)) harvestLast.delete(pid);
      await sleepUntil(HARVEST_MS, endAt);
    }
  })();

  // ── Liveness (c), (d): scrape `/scheduler` of every live node until the end of the drain phase.
  // Each scrape is one "round": one line per range; a range answered by two nodes (during
  // a handover) takes the largest number of each field — the "busy" side, no false alarm.
  const schedRounds: SchedSample[][] = [];
  let schedMisses = 0;
  let livenessDone = false;
  const schedScraper = (async () => {
    if (SCHED_EVERY_S <= 0) return;
    while (!livenessDone) {
      const t = Date.now();
      const merged = new Map<number, SchedSample>();
      await Promise.all(
        aliveNodes().map(async (n) => {
          const rows = (await nfetch(`${n.url}/scheduler`, { signal: AbortSignal.timeout(5_000) })
            .then((r) => (r.ok ? r.json() : undefined))
            .catch(() => undefined)) as
            | Array<{
                partition: number;
                waiting: number;
                eligible: number;
                turnsInUse: number;
                locksHeld: number;
                tenants?: Array<{ balance?: number; waiting: number }>;
              }>
            | undefined;
          if (!rows) {
            schedMisses++;
            return;
          }
          for (const r of rows) {
            const prev = merged.get(r.partition);
            merged.set(r.partition, {
              t,
              range: r.partition,
              waiting: Math.max(prev?.waiting ?? 0, r.waiting),
              eligible: Math.max(prev?.eligible ?? 0, r.eligible),
              turnsInUse: Math.max(prev?.turnsInUse ?? 0, r.turnsInUse),
              locksHeld: Math.max(prev?.locksHeld ?? 0, r.locksHeld),
              // The node's `rate` bucket (repeated on each range): in debt or someone waiting.
              rateBlocked:
                (prev?.rateBlocked ?? false) ||
                (r.tenants ?? []).some((t) => (t.balance ?? 0) < 0 || t.waiting > 0),
            });
          }
        }),
      );
      schedRounds.push([...merged.values()]);
      const until = Date.now() + SCHED_EVERY_S * 1000;
      while (!livenessDone && Date.now() < until) await sleep(500);
    }
  })();

  // ── Long soak: watch the disk periodically — below the threshold stop the destructive phase cleanly.
  let diskLowest: { path: string; gib: number } | undefined;
  const diskWatch = (async () => {
    if (DISK_MIN_GB <= 0 || DISK_PATHS.length === 0) return;
    while (Date.now() < endAt) {
      const d = diskFree();
      if (d !== undefined) {
        if (diskLowest === undefined || d.gib < diskLowest.gib) diskLowest = d;
        if (d.gib < DISK_MIN_GB) {
          haltLoad(`disk — ${d.path} has ${d.gib.toFixed(1)} GiB left < CHAOS_DISK_MIN_GB=${DISK_MIN_GB}`);
          break;
        }
      }
      await sleepUntil(DISK_EVERY_S * 1000, endAt);
    }
  })();

  // ── Long soak: one summary line every `CHAOS_SUMMARY_EVERY_S` and one line
  // when the destructive phase ends — a 24 h run's progress is readable without wading through logs, and a run that dies
  // midway still leaves the numbers up to its death. `SUMMARY_JSON` is for machines.
  /** RSS (MiB) of the live node processes, via `ps` (macOS and Linux). */
  const nodeRss = (): Promise<number[]> => {
    const pids = aliveNodes()
      .map((n) => n.proc.pid)
      .filter((p): p is number => p !== undefined);
    if (pids.length === 0) return Promise.resolve([]);
    return new Promise((resolve) => {
      execFile("ps", ["-o", "rss=", "-p", pids.join(",")], { timeout: 5_000 }, (_err, stdout) => {
        resolve(
          String(stdout ?? "")
            .split("\n")
            .map((l) => Number(l.trim()))
            .filter((kb) => kb > 0)
            .map((kb) => Math.round(kb / 1024)),
        );
      });
    });
  };
  let summaries = 0;
  const summarize = async (from: number, to: number, label: string) => {
    summaries++;
    const lat = (k: keyof typeof latency) => {
      const v = latency[k];
      if (v instanceof PrintOnly) {
        // Since the previous summary (print-only samples keep a histogram, not raw samples).
        const h = v.sinceSummary;
        const out = { n: h.count, p50: h.quantile(0.5), p99: h.quantile(0.99) };
        h.reset();
        return out;
      }
      const xs = v.filter(([at]) => at >= from && at < to).map(([, ms]) => ms);
      return { n: xs.length, p50: quantile(xs, 0.5), p99: quantile(xs, 0.99) };
    };
    const disk = diskFree();
    const s = {
      n: summaries,
      label,
      elapsedS: Math.round((to - startedAt) / 1000),
      windowS: Math.round((to - from) / 1000),
      durationS: DURATION_S,
      acked: ackCount(),
      ...(billing ? {} : { sent, ackFailures, overloaded }),
      kills,
      selfExits,
      coordinatorKills,
      failpoints: [...injected.values()].reduce((a, b) => a + b, 0),
      ranges: lastRangeCount,
      violations: {
        doubleLeaders: doubleLeaders.length,
        verify: verifyProblems.length,
        walFloor: floorProblems.length,
        quiet: quietProblems.length,
        suspended: suspendProblems.length,
        refusals: refusalProblems.length,
        tenantState: tenantStateProblems.length,
        sso: SSO ? sso.problems.length : 0,
        queryServerErrors: query.serverErrors.length,
        respawnGiveUps: respawnGiveUps.length,
      },
      verifyRuns,
      tenantEpisodes,
      latencyMs: { base: lat("base"), quiet: lat("quiet"), noisy: lat("noisy"), rr: lat("rr") },
      diskFreeGiB: disk === undefined ? null : Number(disk.gib.toFixed(1)),
      diskPath: disk?.path ?? null,
      nodeRssMiB: await nodeRss(),
      harnessRssMiB: Math.round(process.memoryUsage().rss / 2 ** 20),
      logMiB: Math.round([...logFiles.values()].reduce((a, f) => a + f.written, 0) / 2 ** 20),
      logRotations: [...logFiles.values()].reduce((a, f) => a + f.rotations, 0),
      fileWriteErrors,
      survivedUncaught,
    };
    const v = s.violations;
    const bad = Object.values(v).reduce((a, b) => a + b, 0);
    const l = (k: keyof typeof s.latencyMs) =>
      s.latencyMs[k].n === 0 ? "—" : `${s.latencyMs[k].p50}/${s.latencyMs[k].p99} (${s.latencyMs[k].n})`;
    console.log(
      `  [${ts()}] SUMMARY #${s.n} ${label} (${s.elapsedS}/${DURATION_S}s): ack ${s.acked}` +
        (billing ? "" : ` (sent ${sent}, not acked ${ackFailures})`) +
        `, kill ${kills}, self-exit ${selfExits}, failpoint ${s.failpoints}, ${s.ranges} ranges; ` +
        `violations ${bad}${bad > 0 ? ` (${Object.entries(v).filter(([, n]) => n > 0).map(([k, n]) => `${k} ${n}`).join(", ")})` : ""}; ` +
        `p50/p99 ms over the last ${s.windowS}s: baseline ${l("base")}, quiet ${l("quiet")}, noisy ${l("noisy")}` +
        (RR_TENANT !== undefined ? `, rr ${l("rr")}` : "") +
        `; free disk ${s.diskFreeGiB ?? "?"} GiB${s.diskPath ? ` (${s.diskPath})` : ""}` +
        `; RSS node ${s.nodeRssMiB.join("/") || "—"} MiB, harness ${s.harnessRssMiB} MiB; log ${s.logMiB} MiB` +
        (fileWriteErrors > 0 ? `; file write failures ${fileWriteErrors}` : "") +
        (survivedUncaught > 0 ? `; uncaught errors survived ${survivedUncaught}` : ""),
    );
    console.log(`SUMMARY_JSON ${JSON.stringify(s)}`);
  };
  const summarizer = (async () => {
    if (SUMMARY_EVERY_S <= 0) return;
    let from = startedAt;
    while (Date.now() < endAt) {
      await sleepUntil(SUMMARY_EVERY_S * 1000, endAt);
      const to = Date.now();
      if (to - from < 1_000) break;
      const label = to >= endAt ? (haltReason !== undefined ? "at early stop" : "end of destructive phase") : "periodic";
      await summarize(from, to, label).catch((e) => console.log(`  summary failed: ${(e as Error).message}`));
      from = to;
    }
  })();

  // ── Harness diagnostics (`CHAOS_HARNESS_EVERY_S`, off by default): the harness is also
  // the SDK of every handler, so when its event loop stalls (GC, machine memory
  // compressed/swapped) every turn slows down while the nodes are idle. Each tick prints the event-loop
  // delay, the total GC time and the memory of the harness process.
  const harnessWatch = (async () => {
    if (HARNESS_EVERY_S <= 0) return;
    const delay = monitorEventLoopDelay({ resolution: 10 });
    delay.enable();
    let gcMs = 0;
    const gc = new PerformanceObserver((list) => {
      for (const e of list.getEntries()) gcMs += e.duration;
    });
    gc.observe({ entryTypes: ["gc"] });
    while (Date.now() < endAt) {
      await sleepUntil(HARNESS_EVERY_S * 1000, endAt);
      const m = process.memoryUsage();
      const ms = (ns: number) => (ns / 1e6).toFixed(1);
      console.log(
        `  [${ts()}] harness: event-loop delay p50 ${ms(delay.percentile(50))} p99 ${ms(delay.percentile(99))} max ${ms(delay.max)} ms; ` +
          `GC ${gcMs.toFixed(0)} ms; heap ${(m.heapUsed / 2 ** 20).toFixed(0)} MiB, rss ${(m.rss / 2 ** 20).toFixed(0)} MiB`,
      );
      delay.reset();
      gcMs = 0;
    }
    delay.disable();
    gc.disconnect();
  })();

  await Promise.all([load, tenantLoops, lvLoop, killer, sdkRestarter, coordinatorKiller, injector, slower, l0Pressure, overlapInjection, quietWatch, harvester, historyProbe, tenantStater, ssoLoop, outager, diskWatch, summarizer, harnessWatch, hotLoad, hotSignaler, parLoad, backupRun]);
  await scraper;
  const loadEndedAt = Date.now();
  if (COUNTING) await Promise.all(aliveNodes().map(scrapeStore));
  await Promise.all([verifyLoop, floorLoop]);
  const churn = await churner?.stop();
  console.log(
    `load done: ` +
      (billing
        ? `billing ${billing.ackedIds.size} sends acked (details in the billing verdict)`
        : `${sent} sent, ${ackLedger.size} acked, ${ackFailures} not acked (${overloaded} times 429 range_overloaded)`) +
      `, ${kills} kills` +
      (churn ? `, ${churn.splits} splits, ${churn.merges} merges, ${churn.deferred} deferrals` : "") +
      `, ${selfExits} node self-exits`,
  );
  if (FAILPOINTS) {
    const total = [...injected.values()].reduce((a, b) => a + b, 0);
    console.log(
      `failpoints: ${total} set (${injectFailures} failed) — ` +
        [...injected.entries()].sort().map(([n, c]) => `${n}×${c}`).join(" "),
    );
  }
  if (SLOW_NODE) console.log(`slow node: ${slowEpisodes} spells, ${slowSheds} spells where the node shed on its own, ${shedsLogged} shedding log lines`);
  if (L0_PRESSURE) console.log(`L0 pressure: ${l0Episodes} spells, largest L0 ${l0MaxViews} views, ${overloaded} times 429 range_overloaded`);
  printFailStops(kills);

  // ── Liveness — after the load and the chaos are off. The node
  // killed last is revived first; the billing workload cancels every subscription (closing the last
  // period) so no more work spawns itself.
  const aliveDeadline = Date.now() + 60_000;
  while (aliveNodes().length < nodes.length && Date.now() < aliveDeadline) await sleep(500);
  if (billing) await billing.quiesce();
  type ListRow = {
    id: string;
    status: string;
    service?: string;
    handler?: string;
    key?: string;
    attempt?: number;
    /** The tenant of the request (attached by the harness, not an API field). */
    tenant?: string;
  };
  /**
   * The cluster-wide list by status, every tenant of the run; `partial` when some node/range did not answer.
   * Without the purge tenant (case (c)): after `deleting` every read of it is a `403`, and it has its own verdict.
   */
  const listTenants: Array<string | undefined> = AUTH
    ? ["default", ...tenantKeys.map((t) => t.tenant), ...(LARGE_VALUES ? [LV_TENANT] : []), ...(HOT ? [HOT_TENANT] : [])]
    : [undefined];
  /**
   * Why a list request came back incomplete (an earlier 4 h soak only printed "list
   * incomplete"): a node did not answer the fan-out (`missing`), a range no node answered
   * (`missingRanges`, with the leader per the queried node's routing table), or
   * the request itself failed (HTTP status / error / time).
   */
  const LIST_TRIES = 3;
  /** The cap of one list request, body included: above the node's 5 s fan-out cap + 5 s for the local part. */
  const LIST_TIMEOUT_MS = 15_000;
  type ListGap = {
    tenant: string;
    status: string;
    ms: number;
    http?: string;
    missing?: string[] | undefined;
    missingRanges?: number[] | undefined;
  };
  async function listCluster(
    status: string,
    limit: number,
  ): Promise<{ rows: ListRow[]; partial: boolean; gaps: ListGap[] }> {
    const rows: ListRow[] = [];
    let partial = false;
    const gaps: ListGap[] = [];
    for (const t of listTenants) {
      // An incomplete answer is asked again (at most LIST_TRIES times, 2 s apart) before
      // counting as incomplete: a range that just changed owner (the balancer hands over ~1 range every 12 s after
      // a revive spell, an ambiguous session renewal) missing for a few seconds is a valid gap;
      // a really stuck range is missing every time.
      let last: ListGap | undefined;
      let got: ListRow[] | undefined;
      for (let attempt = 1; attempt <= LIST_TRIES && got === undefined; attempt++) {
        if (attempt > 1) await sleep(2_000);
        const t0 = Date.now();
        let err: string | undefined;
        // A cap for both the request and the response body: `cfetch`
        // only has a deadline between attempts, a hung fetch (stuck node, half-open socket)
        // would hold the drain phase forever. Past the cap ⇒ this request failed, the retry loop above asks again.
        const r = await cfetch(
          `/invocations?scope=cluster&status=${status}&limit=${limit}${t ? `&tenant=${t}` : ""}`,
          { signal: AbortSignal.timeout(LIST_TIMEOUT_MS) },
          10_000,
        ).catch((e) => {
          err = (e as Error).message;
          return undefined;
        });
        const b = (await r?.json().catch(() => undefined)) as
          | { invocations?: ListRow[]; partial?: boolean; missing?: string[]; missingRanges?: number[] }
          | undefined;
        const ms = Date.now() - t0;
        if (!r?.ok || !b?.invocations) {
          last = { tenant: t ?? "-", status, ms, http: r ? `HTTP ${r.status}` : (err ?? "no answer") };
        } else if (b.partial) {
          last = { tenant: t ?? "-", status, ms, missing: b.missing, missingRanges: b.missingRanges };
        } else {
          got = b.invocations;
        }
      }
      if (got === undefined) {
        partial = true;
        gaps.push(last!);
        continue;
      }
      rows.push(...got.map((r) => ({ ...r, tenant: t ?? "default" })));
    }
    return { rows, partial, gaps };
  }
  const describeGaps = (gaps: ListGap[]) =>
    gaps
      .slice(0, 4)
      .map(
        (g) =>
          `${g.tenant}/${g.status} ${g.ms}ms ` +
          (g.http ??
            `missing nodes [${(g.missing ?? []).join(",")}] ranges [${(g.missingRanges ?? []).slice(0, 12).join(",")}` +
              `${(g.missingRanges?.length ?? 0) > 12 ? ` …+${g.missingRanges!.length - 12}` : ""}]`),
      )
      .join("; ");
  const describeRow = (r: ListRow) =>
    `${r.id} ${r.service ?? "?"}/${r.handler ?? "?"}${r.key ? `[${r.key}]` : ""} ${r.status} attempt ${r.attempt ?? 0}`;

  // (a) Drain: no `running`/`suspended` left in two consecutive requests (complete list).
  const drainFrom = Date.now();
  const drainDeadline = drainFrom + DRAIN_S * 1000;
  // Background turns of a suspended tenant are rescheduled after `SUSPENDED_RETRY` (15 minutes, FL-24). Unsuspending
  // pulls those rows back right away, except on a range that changes leader right at the
  // unsuspend: those ranges wait the full 15 minutes. Leftovers **only** of state-changed tenants
  // wait until the last unsuspend + `SUSPENDED_RETRY` + 2 minutes, and are reported separately.
  const lastActiveAt = Math.max(0, ...activeConfirmedAt.values());
  const suspendedDeadline = lastActiveAt + (SUSPENDED_RETRY_S + 120) * 1000;
  let extendedFor = 0;
  let drainedAt: number | undefined;
  let remaining: ListRow[] = [];
  let lastGaps: ListGap[] = [];
  /** How many requests each range was missing from — a range missing from every one is stuck, not a leader-change gap. */
  const rangeMisses = new Map<number, number>();
  let gapPolls = 0;
  let zeroStreak = 0;
  const onlyChurned = () => remaining.length > 0 && remaining.every((r) => r.tenant !== undefined && churnedEver.has(r.tenant));
  for (
    let poll = 1;
    Date.now() < drainDeadline || (TENANT_STATES && onlyChurned() && Date.now() < suspendedDeadline);
    poll++
  ) {
    if (Date.now() >= drainDeadline && extendedFor === 0) {
      extendedFor = remaining.length;
      console.log(
        `  [${ts()}] drain: the ${remaining.length} remaining invocations all belong to state-changed tenants — waiting until ` +
          `the last unsuspend + ${SUSPENDED_RETRY_S}s (rows rescheduled while suspended on a range that changed leader right at the unsuspend): ` +
          remaining.slice(0, 5).map(describeRow).join("; "),
      );
    }
    const [run, sus] = await Promise.all([listCluster("running", 200), listCluster("suspended", 200)]);
    remaining = [...run.rows, ...sus.rows];
    const gaps = [...run.gaps, ...sus.gaps];
    if (gaps.length > 0) {
      gapPolls++;
      lastGaps = gaps;
      for (const r of new Set(gaps.flatMap((g) => g.missingRanges ?? []))) rangeMisses.set(r, (rangeMisses.get(r) ?? 0) + 1);
    }
    if (!run.partial && !sus.partial && remaining.length === 0) {
      if (++zeroStreak >= 2) {
        drainedAt = Date.now();
        break;
      }
    } else {
      zeroStreak = 0;
    }
    if (poll % 12 === 0) {
      console.log(
        `  [${ts()}] drain: ${remaining.length}${run.partial || sus.partial ? ` (list incomplete: ${describeGaps(gaps)})` : ""} invocations not at an outcome yet`,
      );
    }
    await sleep(5_000);
  }
  // `paused` is an outcome for (a); (b) looks at those that exhausted `maxAttempts`.
  const pausedRows = (await listCluster("paused", 1000)).rows;
  const rowOf = new Map(pausedRows.map((r) => [r.id, r]));
  const { paused575, other: pausedOtherRows, unreadable } = await classifyPaused(
    pausedRows.map((r) => r.id),
    readDetail,
    { tries: DETAIL_TRIES, backoffMs: 500 },
  );
  const pausedOther = pausedOtherRows.map(
    (o) => `${describeRow(rowOf.get(o.id)!)} (detail read: ${o.status} ${o.code ?? ""} ${o.message ?? ""})`,
  );
  livenessDone = true;
  stuckDone = true;
  await schedScraper;
  await stuckSampler;
  const stuckRanges = [...rangeMisses].sort((a, b) => b[1] - a[1]).slice(0, 12);
  const drainProblems = drainedAt
    ? []
    : [
        `${remaining.length} invocations still running/suspended after ${DRAIN_S}s: ` +
          remaining.slice(0, 20).map(describeRow).join("; ") +
          (gapPolls > 0
            ? ` — list incomplete in ${gapPolls} requests, the last: ${describeGaps(lastGaps)}; ` +
              `ranges missing most often: ${stuckRanges.map(([r, n]) => `${r}×${n}`).join(" ") || "none (whole nodes missing or failed requests)"}`
            : ""),
      ];
  console.log(
    `\nliveness (a) drain: ${drainedAt ? `done after ${Math.round((drainedAt - drainFrom) / 1000)}s` : `NOT done within ${DRAIN_S}s — remaining ${remaining.length}`}` +
      `${extendedFor > 0 ? ` (past ${DRAIN_S}s: ${extendedFor} invocations of state-changed tenants, waiting for the suspend reschedule)` : ""}; ` +
      `${pausedRows.length} paused (${paused575.length} from exhausting maxAttempts)`,
  );
  for (const l of pausedOther.slice(0, 5)) console.log(`  paused other than 575: ${l}`);
  // Unreadable after DETAIL_TRIES attempts across every node: not filed under "other than 575"
  // (an earlier version did that and they fell out of (b)), but reported separately — (b) does not cover them.
  if (unreadable.length > 0) {
    console.log(
      `  (b) could NOT read the details of ${unreadable.length} paused ids after ${DETAIL_TRIES} attempts — (b) does not cover them:`,
    );
    for (const u of unreadable.slice(0, 5)) console.log(`    ${describeRow(rowOf.get(u.id)!)}: ${u.why}`);
  }
  // (b) Real attempts.
  const attemptProblems: string[] = [];
  if (billing) {
    const r = checkAttempts(billing.turns, paused575, billing.maxAttempts, killsAt);
    console.log(
      `liveness (b) real attempts: ${r.paused} paused 575, attempts spent without the SDK being called ${r.phantomAttempts} ` +
        `(${r.phantom.length} invocations), excused kill gap (FL-19) ${r.excusedAttempts}; reported only on every Invoice: ${r.gaps} attempts skipped (${r.gapIds.length} invocations)`,
    );
    for (const p of r.phantom.slice(0, 20)) {
      attemptProblems.push(
        `${p.id} (${billing.invoiceKeyOf(p.id) ?? "?"}): the SDK saw attempts ${p.seen.join(",") || "none"}, missing ${p.missing.join(",")}`,
      );
    }
    if (r.phantom.length > 20) attemptProblems.push(`… and ${r.phantom.length - 20} more invocations`);
  } else {
    console.log("liveness (b) real attempts: skipped — the workload has no maxAttempts");
  }
  // (c) Queues after the load.
  const slopes = queueSlopes(schedRounds.flat(), loadEndedAt, loadEndedAt + QUEUE_WINDOW_S * 1000);
  const growing = slopes.filter(queueGrows);
  const peak = new Map<number, number>();
  for (const s of schedRounds.flat()) peak.set(s.range, Math.max(peak.get(s.range) ?? 0, s.waiting));
  const topPeak = [...peak].sort((a, b) => b[1] - a[1]).slice(0, 5);
  console.log(
    `liveness (c) queues: ${schedRounds.length} scrapes (${schedMisses} failed), ${slopes.length} ranges with samples after the load, ` +
      `${growing.length} ranges growing; largest waiting of the run: ${topPeak.map(([r, w]) => `range ${r}=${w}`).join(", ") || "none"}`,
  );
  const queueProblems = growing.map(
    (q) => `range ${q.range}: waiting ${q.first} → ${q.last} for ${q.samples} samples, slope +${q.slopePerMin.toFixed(1)}/min`,
  );
  // (d) Deadlock.
  const dead = deadLocks(schedRounds, DEADLOCK_SCRAPES);
  console.log(`liveness (d) deadlock: ${dead.length} ranges holding locks without running for ≥ ${DEADLOCK_SCRAPES} consecutive scrapes`);
  const deadProblems = dead.map(
    (d) =>
      `range ${d.range} from ${new Date(d.from).toISOString()} to ${new Date(d.to).toISOString()} (${d.scrapes} scrapes): ` +
      `locksHeld ${d.last.locksHeld}, waiting ${d.last.waiting}`,
  );
  // The right/wrong verdict of the billing workload (I1–I6).
  const billingVerdict = billing
    ? await billing.verdict(new Set(pausedRows.filter((r) => r.service === "Invoice" && r.key).map((r) => r.key!)))
    : undefined;
  for (const l of billingVerdict?.lines ?? []) console.log(l);

  // ── Verdict: every acked invocation must finish, with the right content.
  /** Number of `completed` of state-changeable tenants whose `endedAt` was compared with the suspended windows. */
  let suspendEndChecks = 0;
  let lost = 0;
  let wrong = 0;
  /** Acked ids the node no longer knows, acked longer ago than `RETENTION_MS`: not judged. */
  let expired = 0;
  let checked = 0;
  const verdictEnd = VERDICT_BUDGET_S > 0 ? Date.now() + VERDICT_BUDGET_S * 1000 : Infinity;
  // Judge in parallel with a cap (`CHAOS_VERDICT_CONCURRENCY`): reading
  // sequentially at ~120 ids/s held the shared lock longer than the whole destructive phase (290 k ids
  // ≈ 40 minutes). Each id is still read sequentially with its own deadline; shared counters only change
  // on the event loop, never between two synchronous statements.
  const verdictTotal = ackLedger.size;
  let verdictNext = 0;
  let judged = 0;
  let budgetHit = false;
  const judgeOne = async (id: string, want: { seq: number; tag: string }, idx: number): Promise<void> => {
    // The deadline is PER invocation, and it is always checked at least once: most finished
    // long ago and answer right away; the deadline is only for those still finishing up.
    const idDeadline = Math.min(verdictEnd, Date.now() + 60_000);
    let verdict: "ok" | "wrong" | "lost" | "expired" = "lost";
    let lastSeen = "(not read yet)";
    do {
      let missing = false;
      const body = await readStatus(id, 30_000, ADMIN_KEY, idx, () => {
        missing = true;
      });
      if (!body && missing) {
        lastSeen = "404 (the node does not know the id)";
        // Past the retention a 404 is the cleanup, not a loss: no point waiting for it.
        const at = ackLedger.ackedAt(idx);
        if (at !== undefined && Date.now() - at > RETENTION_MS) {
          verdict = "expired";
          break;
        }
      }
      if (body) {
        lastSeen = JSON.stringify(body);
        if (body.status === "completed" && TENANT_STATES && body.endedAt !== undefined) {
          // (b): no invocation of the tenant ends inside a confirmed `suspended` window.
          const tenant = tenantOfTag(want.tag);
          const w = suspendWindows.find((x) => x.tenant === tenant && body.endedAt! > x.from && body.endedAt! < x.to);
          if (w) {
            // Classification (does not change the verdict): the last handler entry before the `204` of the
            // suspend command ⇒ the turn was already at the SDK when the command arrived.
            const entered = seqs.workAt(want.seq);
            const before = entered !== undefined && entered < w.from - TENANT_STATE_GRACE_MS;
            if (before) suspendEndedInflight++;
            const line =
              `${id} (${want.tag}) endedAt ${new Date(body.endedAt).toISOString()} inside the suspended window ` +
              `${new Date(w.from).toISOString()}…${new Date(w.to).toISOString()}` +
              (entered === undefined
                ? " — no handler entry seen"
                : ` — last handler entry ${new Date(entered).toISOString()}${before ? ", BEFORE the suspend command (the turn was already at the SDK)" : ""}`);
            console.error(`  SUSPENDED STILL COMPLETED: ${line}`);
            suspendProblems.push(line);
          }
          if (churnable.has(tenant)) suspendEndChecks++;
        }
        if (body.status === "completed") {
          const out = JSON.parse(body.output ?? "{}") as {
            seq?: number;
            echo?: number;
            leaves?: number;
            saga?: number;
            fan?: number;
            artifact?: string;
          };
          const agentProblem = AGENT ? await agentCheck(id, want.tag, want.seq, out.artifact) : undefined;
          if (agentProblem) {
            console.error(`  ${id}: WRONG (agent) — ${agentProblem}`);
            verdict = "wrong";
            break;
          }
          if (
            out.seq === want.seq &&
            out.echo === want.seq &&
            (out.leaves ?? 0) === TREE_LEAVES &&
            (out.saga ?? 0) === SAGA_STEPS &&
            (out.fan ?? 0) === FANOUT &&
            seqs.runCount(want.seq) >= 1
          ) {
            verdict = "ok";
          } else {
            console.error(
              `  ${id}: WRONG — want seq=${want.seq} (side effect ≥1), got ${body.output}, run=${seqs.runCount(want.seq)}`,
            );
            verdict = "wrong";
          }
          break;
        }
        if (body.status === "failed" || body.status === "paused") {
          console.error(`  ${id}: ended ${body.status} instead of completed`);
          verdict = "wrong";
          break;
        }
      }
      await sleep(250);
    } while (Date.now() < idDeadline);
    if (verdict === "lost") {
      console.error(`  LOST: ${id} (seq=${want.seq}) — last status: ${lastSeen}`);
      lost++;
    } else if (verdict === "expired") {
      if (expired < 20) console.error(`  EXPIRED: ${id} (seq=${want.seq}) — acked more than ${RETENTION_MS} ms ago, cleaned up`);
      expired++;
    } else if (verdict === "wrong") {
      wrong++;
    }
  };
  const verdictWorker = async (): Promise<void> => {
    while (verdictNext < verdictTotal) {
      if (Date.now() > verdictEnd) {
        if (!budgetHit) {
          budgetHit = true;
          console.log(`  verdict time cap of ${VERDICT_BUDGET_S}s ran out — ${verdictTotal - checked} invocations not judged`);
        }
        return;
      }
      const idx = verdictNext++;
      const seq = ackLedger.seq(idx);
      checked++;
      await judgeOne(ackLedger.id(idx), { seq, tag: tagOfSeq(seq) }, idx);
      judged++;
      if (judged % 200 === 0) {
        console.log(`  judged ${judged}/${verdictTotal}…`);
      }
    }
  };
  await Promise.all(Array.from({ length: Math.max(1, VERDICT_CONCURRENCY) }, () => verdictWorker()));

  if (!billing) {
    console.log(`\nverdict: ${ackLedger.size} acked → lost ${lost}, wrong ${wrong}` + (expired > 0 ? `, expired (not judged) ${expired}` : ""));
  }

  // ── History checker: flip, vanish, rerun — see `history.ts`. Ids in the ack
  // book: the incremental checker (`historyTracker`); reads of ids outside the book: `checkHistory`.
  const tracked = historyTracker.finish(
    ackLedger.size,
    (i) => ackLedger.id(i),
    (i) => {
      const seq = ackLedger.seq(i);
      const tag = tagOfSeq(seq);
      return { tag, runs: seqs.runsOf(seq, tag, ackLedger.id(i)) };
    },
  );
  const loose = checkHistory(observations, [], new Map());
  const history = {
    anomalies: [...tracked.anomalies, ...loose.anomalies],
    staleFollowerReads: tracked.staleFollowerReads + loose.staleFollowerReads,
    goneAfterCompleted: tracked.goneAfterCompleted + loose.goneAfterCompleted,
    completedIds: tracked.completedIds + loose.completedIds,
    duplicateSubmissions: tracked.duplicateSubmissions + loose.duplicateSubmissions,
  };
  if (!billing) console.log(
    `history: ${historyTracker.reads + observations.length} reads, ${history.completedIds} ids seen completed; ` +
      `stale follower reads ${history.staleFollowerReads}, 404 after completed ${history.goneAfterCompleted}, ` +
      `duplicate submissions (different id, same tag) ${history.duplicateSubmissions}, ` +
      `anomalies ${history.anomalies.length}`,
  );
  for (const a of history.anomalies.slice(0, 20)) console.error(`  ${a.kind.toUpperCase()}: ${a.id} — ${a.detail}`);

  // ── Read cost: requests by source, summed over every process that ever lived.
  if (COUNTING) {
    const total: StoreCounts = new Map();
    for (const c of storeTally.values()) for (const [k, v] of c) total.set(k, (total.get(k) ?? 0) + v);
    const seconds = (loadEndedAt - startedAt) / 1000;
    const steps = ackLedger.size * STEPS_PER_WORK;
    const keys = [...total.keys()].filter((k) => !k.startsWith("src|")).sort();
    const srcKeys = [...total.keys()].filter((k) => k.startsWith("src|"));
    const by = (op: string) => keys.filter((k) => k.startsWith(`${op}:`)).reduce((a, k) => a + total.get(k)!, 0);
    console.log(
      `read cost: ${seconds.toFixed(0)}s, ${storeTally.size} node processes, ${scrapeMisses} failed scrapes, ` +
        `${ackLedger.size} acks ≈ ${steps} steps (${STEPS_PER_WORK}/call)`,
    );
    console.log("  op:class            total      /s   /step");
    for (const k of keys) {
      const v = total.get(k)!;
      console.log(`  ${k.padEnd(16)} ${String(v).padStart(8)} ${(v / seconds).toFixed(1).padStart(7)} ${(steps ? v / steps : 0).toFixed(2).padStart(7)}`);
    }
    console.log(`  GET total ${by("get")} = ${(by("get") / seconds).toFixed(1)}/s, ${(steps ? by("get") / steps : 0).toFixed(2)}/step`);
    if (srcKeys.length > 0) {
      // GET and LIST by caller, largest first — sources under 0.5% dropped.
      const reads = srcKeys
        .filter((k) => /\|(get|list|head):/.test(k))
        .map((k) => [k.slice(4), total.get(k)!] as const)
        .sort((a, b) => b[1] - a[1]);
      const all = reads.reduce((a, [, v]) => a + v, 0);
      console.log("  reads by caller (source|op:class/object) total      /s   /step");
      for (const [k, v] of reads) {
        if (v < all * 0.005) continue;
        console.log(`  ${k.padEnd(52)} ${String(v).padStart(8)} ${(v / seconds).toFixed(1).padStart(7)} ${(steps ? v / steps : 0).toFixed(2).padStart(7)}`);
      }
    }
    console.log(
      `COST_JSON ${JSON.stringify({
        cost: COST, probe: PROBE, nodes: NODES, partitions: Number(PARTITIONS), killEveryS: KILL_EVERY_S,
        seconds: Math.round(seconds), processes: storeTally.size, scrapeMisses, kills, acked: ackLedger.size, steps,
        counts: Object.fromEntries(keys.map((k) => [k, total.get(k)!])),
        bySource: Object.fromEntries(srcKeys.sort().map((k) => [k.slice(4), total.get(k)!])),
      })}`,
    );
  }

  // ── Counts — lower bound, see the head of the file.
  if (HARVEST_MS > 0) {
    const sum = (name: string, ...l: string[]) => harvestedSum(name, ...l);
    console.log(`\ncounts harvested per process (${harvestRounds} scrape rounds, every ${HARVEST_MS} ms, ${harvestLast.size} processes; lower bound):`);
    console.log(
      `  rescue net: rescue_marked ${sum("cairnflow_rescue_marked_total")} (default nack ${sum("cairnflow_rescue_nacked_total")}), ` +
        `rescue_requeued ${sum("cairnflow_rescue_requeued_total")}, ` +
        `rescue_round_failed ${sum("cairnflow_rescue_round_failed_total")}`,
    );
    console.log(
      `  backpressure: l0_shed ingress ${sum("cairnflow_range_l0_shed_total", 'kind="ingress"')} inbound ${sum("cairnflow_range_l0_shed_total", 'kind="inbound"')} ` +
        `dispatch ${sum("cairnflow_range_l0_shed_total", 'kind="dispatch"')}; write_stalls ${sum("cairnflow_range_write_stalls_total")}; ` +
        `flow_throttled ${sum("cairnflow_flow_throttled_total")}; invoke_throttled (429) ${sum("cairnflow_invoke_throttled_total")}; ` +
        `flow_blocked lock ${sum("cairnflow_flow_blocked_total", 'resource="lock"')} concurrency ${sum("cairnflow_flow_blocked_total", 'resource="concurrency"')} ` +
        `turns ${sum("cairnflow_flow_blocked_total", 'resource="turns"')}`,
    );
    {
      const pct = (h: number, m: number) => (h + m === 0 ? "—" : `${((100 * h) / (h + m)).toFixed(1)} %`);
      const [rh, rm] = [sum("cairnflow_range_block_cache_hits_total"), sum("cairnflow_range_block_cache_misses_total")];
      const [dh, dm] = [sum("cairnflow_disk_cache_hits_total"), sum("cairnflow_disk_cache_misses_total")];
      console.log(
        `  cache: RAM block hits ${rh}, misses ${rm} (${pct(rh, rm)}); ` +
          (DISK_CACHE ? `disk hits ${dh}, misses ${dm} (${pct(dh, dm)}) — a disk miss reads down to the store` : "no disk cache — a RAM miss reads down to the store"),
      );
      console.log(`CACHE_JSON ${JSON.stringify({ disk: !!DISK_CACHE, ramHits: rh, ramMisses: rm, diskHits: dh, diskMisses: dm, completed: sum("cairnflow_invoke_completed_total") })}`);
    }
    console.log(
      `  auto-split (metric): ${sum("cairnflow_auto_split_total")} done, ${sum("cairnflow_auto_split_failed_total")} failed; ` +
        `quarantine: ${sum("cairnflow_quarantine_detected_total")} detected; invoke_completed ${sum("cairnflow_invoke_completed_total")}`,
    );
    console.log(
      `  GC: ops/ records cleaned up ${sum("cairnflow_gc_ops_reaped_total", 'outcome="reaped"')} ` +
        `(failed ${sum("cairnflow_gc_ops_reaped_total", 'outcome="failed"')}); ` +
        `D-d kept by the checkpoint barrier (gauge peak) ${["named", "unnamed", "disagree", "unreadable"].map((k) => `${k} ${checkpointHeldPeak.get(k) ?? 0}`).join(" ")}; ` +
        `disagreements at the delete step scan_clear ${sum("cairnflow_gc_proofs_disagree_total", 'direction="scan_clear"')} ` +
        `scan_referenced ${sum("cairnflow_gc_proofs_disagree_total", 'direction="scan_referenced"')}`,
    );
    // The shared retry primitive: every op with a non-zero count.
    const retryOps = new Map<string, Record<string, number>>();
    for (const [k, v] of harvested) {
      const m = /^cairnflow_retry_(\w+)_total\{op="([^"]+)"\}$/.exec(k);
      if (!m || v === 0) continue;
      const row = retryOps.get(m[2]!) ?? {};
      row[m[1]!] = (row[m[1]!] ?? 0) + v;
      retryOps.set(m[2]!, row);
    }
    console.log(
      `  retry / cap (op: retries/exhausted/permanent/ambiguous): ` +
        ([...retryOps].sort().map(([op, r]) => `${op} ${r.retries ?? 0}/${r.exhausted ?? 0}/${r.permanent ?? 0}/${r.ambiguous ?? 0}`).join("; ") || "no op non-zero"),
    );
    console.log(`  log lines: ${LOG_PATTERNS.map(([label]) => `${label} ${logCounts.get(label) ?? 0}`).join("; ")}`);
    if (S3_PROFILE) {
      console.log(
        `  injected profile faults: fail ${sum("cairnflow_testkit_store_faults_total", 'kind="fail"')}, ` +
          `ambiguous (written then reported an error) ${sum("cairnflow_testkit_store_faults_total", 'kind="ambiguous"')}, ` +
          `retried (written then saw the retry) ${sum("cairnflow_testkit_store_faults_total", 'kind="retried"')}`,
      );
    }
    if (SCRUB_EVERY_S > 0) {
      const peak = [...scrubPeak].filter(([, v]) => v > 0).map(([c, v]) => `${c} ${v}`);
      console.log(
        `  background scrubber (every ${SCRUB_EVERY_S}s, report only): clean rounds ${sum("cairnflow_scrub_rounds_total", 'result="clean"')}, ` +
          `with findings ${sum("cairnflow_scrub_rounds_total", 'result="found"')}, failed ${sum("cairnflow_scrub_rounds_total", 'result="failed"')}; ` +
          `largest still-present findings: ${peak.length ? peak.join(", ") : "none"}`,
      );
      for (const l of scrubLines) console.log(`    ${l}`);
    }
    if (S3_PROFILE) {
      const steps = 4 * (ackLedger.size - lost - wrong);
      const ops = ["put", "get", "head", "list", "delete", "copy"];
      const per = (n: number) => (steps > 0 ? Math.round((n * 1e6) / steps).toLocaleString("en-US") : "?");
      const row = ops.map((op) => [op, sum("cairnflow_testkit_store_requests_total", `op="${op}"`)] as const);
      const total = row.reduce((a, [, n]) => a + n, 0);
      console.log(
        `  store requests (destructive phase, ${DURATION_S}s, ≈${steps} steps): ` + row.map(([op, n]) => `${op} ${n}`).join(", ") + `, total ${total}`,
      );
      console.log(`  per million steps: ` + row.map(([op, n]) => `${op} ${per(n)}`).join(", ") + `, total ${per(total)}`);
      console.log(
        `  the profile's 503 SlowDown: ${sum("cairnflow_testkit_store_slowdown_total", 'what="rejected"')} rejected ` +
          `(${[...harvested.keys()].filter((k) => k.startsWith("cairnflow_testkit_store_slowdown_total")).map((k) => `${k.slice(k.indexOf("{"))}=${harvested.get(k)}`).join(" ")})`,
      );
    }
  }

  // `--large-values`: every acked call must finish (reading the output of calls not seen yet),
  // then one `check` call per object rereads the whole value.
  if (LARGE_VALUES) {
    const lvDeadline = Date.now() + 600_000;
    for (const [id, key] of lvAcked) {
      let st: Awaited<ReturnType<typeof readStatus>>;
      for (;;) {
        st = await readStatus(id, 30_000, lvKey);
        if (st?.status === "completed" || Date.now() > lvDeadline) break;
        await sleep(1_000);
      }
      if (st?.status !== "completed") lvProblems.push(`${id} (${key}): acked but not finished (${st?.status ?? "unreadable"})`);
      else lvNote(id, key, st.output);
    }
    for (let k = 0; k < LV_KEYS; k++) {
      const key = `h${k}`;
      const r = await cfetch(`/invoke/ChaosHistory/check?key=${key}`, { method: "POST", body: "{}" }, 120_000, lvKey).catch(
        (e: unknown) => String(e),
      );
      if (typeof r === "string") {
        lvProblems.push(`check ${key}: ${r.slice(0, 200)}`);
        continue;
      }
      const body = (await r.json().catch(() => ({}))) as { status?: string; output?: string };
      if (body.status !== "completed" || body.output === undefined) {
        lvProblems.push(`check ${key}: ${r.status} ${JSON.stringify(body).slice(0, 200)}`);
        continue;
      }
      const out = JSON.parse(body.output) as LvOut;
      const top = Math.max(0, ...(lvSeen.get(key)?.keys() ?? []));
      if (out.corrupt !== undefined) lvProblems.push(`check ${key}: final value corrupt — ${out.corrupt}`);
      if (out.n !== top) lvProblems.push(`check ${key}: final n ${out.n}, largest append seen ${top}`);
    }
    console.log(
      `large values: ${lvSent} sends, ${lvAcked.size} acks, ${lvOutputs} results, ${lvResets} resets to base, ` +
        `largest ${(lvMaxLen / 1024).toFixed(0)} KiB, ${lvProblems.length} violations`,
    );
  }

  const problems = new Map<string, string[]>();
  const fail = (what: string, items: string[]) => {
    if (items.length === 0) return;
    problems.set(what, [...(problems.get(what) ?? []), ...items]);
  };
  // Early stop: a run short of its duration is never green, even if every
  // verdict below passes — they only cover the part that ran.
  if (haltReason !== undefined) {
    fail("STOPPED", [`${haltReason} — the destructive phase ran ${Math.round(((haltedAt ?? startedAt) - startedAt) / 1000)}s / ${DURATION_S}s`]);
  }
  if (lost > 0 || wrong > 0) fail("lost/wrong", [`lost ${lost}, wrong ${wrong}`]);
  if (expired > 0) {
    fail("lost/wrong", [
      `${expired} acked invocations were cleaned up by the node's retention (${RETENTION_MS} ms) before the verdict read them — not judged; raise CHAOS_RETENTION_MS`,
    ]);
  }
  fail(
    "history",
    history.anomalies.map((a) => `${a.kind} ${a.id}: ${a.detail}`),
  );
  fail("drain (a)", drainProblems);
  if (LARGE_VALUES) {
    if (lvOutputs === 0) fail("large values", ["--large-values but no append finished — green for the wrong reason"]);
    fail("large values", lvProblems.slice(0, 20));
  }
  fail("real attempts (b)", attemptProblems);
  fail("queues (c)", queueProblems);
  fail("deadlock (d)", deadProblems);
  for (const [what, items] of billingVerdict?.problems ?? []) fail(what, items.slice(0, 20));

  // ── Verdict of the pre-soak cases (the cluster is still alive, after the drain phase).
  /** Read the final status of `ids` in parallel with a cap, asking again up to 3 times while not at an outcome. */
  const finalStatuses = async (ids: string[], key: string | undefined) => {
    const out = new Map<string, { status: string; output?: string } | undefined>();
    let next = 0;
    await Promise.all(
      Array.from({ length: VERDICT_CONCURRENCY }, async () => {
        while (next < ids.length) {
          const id = ids[next++]!;
          let st: { status: string; output?: string } | undefined;
          for (let attempt = 0; attempt < 3; attempt++) {
            const r = await cfetch(`/invocations/${id}`, undefined, 10_000, key).catch(() => undefined);
            st = r?.ok ? ((await r.json().catch(() => undefined)) as typeof st) : undefined;
            if (st && isFinal(st.status)) break;
            await sleep(1_000);
          }
          out.set(id, st);
        }
      }),
    );
    return out;
  };
  if (PRE_SOAK_CASES && TRIPWIRES && !tripwiresOn) {
    console.log("tripwires: the binary lacks the tripwire names — not set, not judged");
  } else if (PRE_SOAK_CASES && TRIPWIRES) {
    console.log(
      `tripwires: set ${tripwireArmed} times (${tripwireArmFailures.length} could not be set), fired ${tripwireHits.length}`,
    );
    if (tripwireArmed === 0) {
      fail("tripwire", [`could not set any tripwire — green for the wrong reason: ${tripwireArmFailures.slice(0, 3).join("; ")}`]);
    }
  }
  fail("tripwire", tripwireHits);
  if (HOT) {
    const pending = [...hotLedger].filter(([, r]) => r.status === undefined).map(([id]) => id);
    for (const [id, st] of await finalStatuses(pending, hotKey)) {
      const rec = hotLedger.get(id)!;
      if (st) {
        rec.status = st.status;
        if (st.output !== undefined) rec.output = st.output;
      }
    }
    const hotProblems: string[] = [];
    let completed = 0;
    let cancelledEnds = 0;
    let poked = 0;
    for (const [id, rec] of hotLedger) {
      if (rec.status === "completed") {
        completed++;
        let o: { n?: number; a?: number; b?: number; poked?: boolean } | undefined;
        try {
          o = JSON.parse(rec.output ?? "null") as typeof o;
        } catch {
          o = undefined;
        }
        if (o?.poked) poked++;
        if (o?.n !== rec.n || o.a !== rec.n || o.b !== rec.n) {
          hotProblems.push(`${id} [${rec.key}] n ${rec.n}: wrong output ${String(rec.output).slice(0, 160)}`);
        }
      } else if (rec.status === "failed" && hotCancelled.has(id)) {
        cancelledEnds++;
      } else {
        hotProblems.push(
          `${id} [${rec.key}] n ${rec.n}: ${rec.status ?? "unreadable"}${hotCancelled.has(id) ? " (cancel sent)" : " (nobody cancelled)"}`,
        );
      }
    }
    const codes = (m: Map<string, number>) => [...m].map(([k, v]) => `${k}×${v}`).join(" ") || "none";
    console.log(
      `hot object (a): ${hot.sent} sent, ${hotLedger.size} acks, ${completed} completed (${poked} got a signal), ` +
        `${cancelledEnds} ended by cancel; signal ${codes(hot.signals)}; cancel ${codes(hot.cancels)}; ` +
        `${hotWatch.enters} lock enters/leaves, excused ${codes(hotWatch.excused)}, most overlapping ${hotWatch.maxHolders}`,
    );
    if (completed === 0 || (hot.signals.get("200") ?? 0) === 0) {
      fail("hot object (a)", ["no call completed or no signal arrived — green for the wrong reason"]);
    }
    fail("hot object (a)", hotProblems.slice(0, 20));
    if (hotProblems.length > 20) fail("hot object (a)", [`… and ${hotProblems.length - 20} more calls`]);
    fail("hot object exclusion (a)", hotWatch.violations.slice(0, 20));
  }
  if (STUCK_RUNNING_S > 0) {
    console.log(
      `running standing still (b): ${stuckWatch.rounds} scrapes (${stuckMisses} failed requests), ${stuckWatch.rows} running rows, ` +
        `longest ${Math.round(stuckWatch.maxAgeMs / 1000)}s counted from when the cluster was healthy (cap ${STUCK_RUNNING_S}s)` +
        (GATE_FAULTS ? `; turn-gate points set ${GATE_POINTS.map(([n]) => `${n}×${injected.get(n) ?? 0}`).join(" ")}` : ""),
    );
    for (const [i, w] of healthyWindows.entries()) {
      console.log(
        `  healthy window ${i + 1}: seconds ${Math.round((w.from - startedAt) / 1000)}–${Math.round((w.to - startedAt) / 1000)}, ` +
          `${w.rounds} scrapes, ${w.ids.size} running invocations examined, longest ${Math.round(w.maxAgeMs / 1000)}s ` +
          `counted from healthy`,
      );
    }
    if (stuckWatch.rounds === 0) fail("running standing still (b)", ["no scrape at all — green for the wrong reason"]);
    fail("running standing still (b)", [...stuckWatch.flagged.values()].slice(0, 20));
  }
  if (TENANT_PURGE) {
    await purgeRun;
    console.log(
      `purge tenant (c): failed API polls ${purge.pollFailures} (over the cap ${purge.pollTimeouts}), incomplete held entries ${purge.heldIncomplete}, tenants registry read ${purge.bookReads} times` +
        `${purge.verdictNote ? `; ${purge.verdictNote}` : ""}`,
    );
    console.log(
      `purge tenant (c): ${purge.acked} invocations on one range (built in ${Math.round(purge.bulkMs / 1000)}s), ` +
        (purge.deletingAt === undefined
          ? "could not enter deleting"
          : `purging ${purge.purgingAt ? `after ${Math.round((purge.purgingAt - purge.deletingAt) / 1000)}s` : "not yet"}, ` +
            `purged ${purge.purgedAt ? `after ${Math.round((purge.purgedAt - purge.deletingAt) / 1000)}s` : "not yet"} since deleting`),
    );
    if (purge.acked === 0) fail("purge tenant (c)", ["no invocation of the purge tenant was acked — green for the wrong reason"]);
    fail("purge tenant (c)", purge.problems);
  }
  if (BACKUP_RESTORE) {
    console.log(
      `backup/restore (d): ${backup.cycles} cycles, ${backup.ok} restored + checked clean, ${backup.refused.length} refused` +
        `; recorded limits (cross-range messages lost, correctly found by check-restore, not a failure) per cycle: ` +
        `${backup.lost.map((l) => `cycle ${l.cycle} ${lostText(l.counts)}`).join(", ") || "—"}` +
        (backup.healed.size > 0 ? `; findings a round heals: ${[...backup.healed].map(([k, v]) => `${k}×${v}`).join(" ")}` : ""),
    );
    for (const r of backup.refused.slice(0, 5)) console.log(`  ${r}`);
    if (backup.ok === 0 && backup.problems.length === 0) {
      fail("backup/restore (d)", [`no cycle produced a complete backup that could be restored (${backup.cycles} cycles) — green for the wrong reason`]);
    }
    fail("backup/restore (d)", backup.problems);
  }
  if (PAR) {
    const st = await finalStatuses([...parLedger.keys()], undefined);
    const parProblems: string[] = [];
    let completed = 0;
    for (const [id, seq] of parLedger) {
      const s = st.get(id);
      if (s?.status !== "completed") {
        parProblems.push(`${id} seq ${seq}: ${s?.status ?? "unreadable"}`);
        continue;
      }
      completed++;
      let output: unknown;
      try {
        output = JSON.parse(s.output ?? "null");
      } catch {
        output = undefined;
      }
      const diff = parDiff(seq, PAR_BRANCHES, PAR_STEPS, output);
      if (diff.length > 0) parProblems.push(`${id} seq ${seq}: ${diff.join("; ")}`);
    }
    console.log(
      `parallel branches (e): ${parSent} sent, ${parLedger.size} acks (${parNotAcked} not), ${completed} completed; ` +
        `ctx.run closure transient failures ${parTransient}, durable failures ${parTerminal}`,
    );
    if (completed === 0 || parTransient === 0) {
      fail("parallel branches (e)", ["no call completed or no closure failed — green for the wrong reason"]);
    }
    fail("parallel branches (e)", parProblems.slice(0, 20));
    if (parProblems.length > 20) fail("parallel branches (e)", [`… and ${parProblems.length - 20} more calls`]);
  }

  // ── Verdict of split/merge and of auto-split. The table must come back to
  // rest before every final check based on the bucket.
  const splitProblems: string[] = [];
  if (churn) {
    // A 500 hides the detail: injected errors are recognised through the node logs (`maskedErrors`).
    const unmasked = churn.errors.filter((e) => {
      const code = /\bcode ([0-9a-f]{12})/.exec(e)?.[1];
      const detail = code ? maskedErrors.get(code) : undefined;
      if (FAILPOINTS && detail?.includes("failpoint ")) {
        console.log(`  churn: ${e.slice(0, 80)}… is an injected error (node log: ${detail.slice(0, 120)})`);
        return false;
      }
      return true;
    });
    splitProblems.push(...unmasked.map((e) => `response outside the contract: ${e}`));
    if (churn.splits === 0 || churn.merges === 0) {
      splitProblems.push(
        `not even one completed split and one completed merge (${churn.splits} splits, ${churn.merges} merges) — green for the wrong reason`,
      );
    }
  }
  const restDeadline = Date.now() + 60_000;
  let problem: string | null = "not read";
  while (Date.now() < restDeadline) {
    problem = await rangesOf((path, init) => cfetch(path, init))
      .then(tableProblem)
      .catch((e) => `reading /cluster failed: ${e}`);
    if (problem === null) break;
    await sleep(500);
  }
  if (problem !== null) splitProblems.push(`the range table did not come back to rest: ${problem}`);
  const rows = await rangesOf((path, init) => cfetch(path, init)).catch(() => []);
  console.log(`final range table: ${rows.length} ranges — ${rows.map((r) => `${r.range}@gen${r.gen}`).join(" ")}`);
  watchDone = true;
  await watcher;
  if (AUTO_SPLIT) {
    const suggestions = (await cfetch("/ranges/suggestions?scope=cluster")
      .then((r) => r.json())
      .catch(() => [])) as Array<{ reason: string; blockedBy?: string | null }>;
    const reasons = new Map<string, number>();
    for (const s of suggestions) {
      const k = `${s.reason}${s.blockedBy ? `/${s.blockedBy}` : ""}`;
      reasons.set(k, (reasons.get(k) ?? 0) + 1);
    }
    // No merges in this mode, so the added range count = the number of committed splits.
    const splits = rows.length - Number(PARTITIONS);
    console.log(
      `auto-split: ${splits} splits committed, ${opsSeen.size} in-progress operations ever seen; ` +
        `suggestions at the end: ${[...reasons].map(([k, v]) => `${k}×${v}`).join(" ") || "none"}`,
    );
    if (splits <= 0) splitProblems.push("no auto-split at all — green for the wrong reason");
  }
  fail("split/merge", splitProblems);
  if (checkOverlap) {
    console.log(`overlapping split/merge: at most ${maxConcurrentOps} operations in progress at once${concurrentAt ? ` (${concurrentAt})` : ""}`);
    if (maxConcurrentOps > 1) fail("overlapping split/merge", [`saw ${maxConcurrentOps} operations in progress at once: ${concurrentAt}`]);
  }

  if (FAILPOINTS && injected.size === 0) fail("failpoint", ["--failpoints but no point could be set — green for the wrong reason"]);
  {
    // The delegation tripwire — the log (survives `kill -9`) and the harvested metric.
    const revoked = harvestedSum("cairnflow_lease_delegated_revoked_while_valid_total");
    console.log(`delegation tripwire (revoked while the grant is still valid): log ${delegatedRevokedLogged}, metric ${revoked}`);
    if (delegatedRevokedLogged > 0 || revoked > 0)
      fail("delegation tripwire", [`log ${delegatedRevokedLogged}, metric ${revoked} — a protocol bug`]);
  }

  probeDone = true;
  await prober;
  queryDone = true;
  await querier;
  if (QUERY_LOOP) {
    // The load is over: the RAM counters and the scan must agree (waiting for the background rebuild after splits).
    let settled = "";
    for (let k = 0; k < 60 && settled === ""; k++) {
      const a = await askQuery("SELECT class, count(*) AS n FROM sys_invocation GROUP BY class ORDER BY class");
      const b = await askQuery("SELECT class, sum(count) AS n FROM sys_invocation_counts GROUP BY class ORDER BY class");
      if (a && b && !a.partial && !b.partial) {
        if (JSON.stringify(a.rows) === JSON.stringify(b.rows)) settled = JSON.stringify(a.rows);
        else if (k === 59) query.wrong.push(`counters disagree with the scan: ${JSON.stringify(b.rows)} ≠ ${JSON.stringify(a.rows)}`);
      }
      if (settled === "") await sleep(1_000);
    }
    if (settled === "" && query.wrong.length === 0) query.wrong.push("no complete answer at all after the load (60 s)");
    console.log(
      `query loop: ${query.ok} complete, ${query.partial} partial, ${query.refused} refused/connection lost, ` +
        `${query.serverErrors.length} 5xx errors; end of run by class: ${settled}`,
    );
    if (query.ok === 0) fail("query", ["no complete answer during the run — green for the wrong reason"]);
    fail("query", [...query.serverErrors, ...query.wrong]);
  }
  if (LEADER_PROBE_MS > 0) {
    console.log(`two leaders: ${probeRounds} probe rounds, ${probeSuspects} suspicions (asked again), ${doubleLeaders.length} confirmed`);
    if (probeRounds === 0) fail("two leaders", ["no probe round finished — green for the wrong reason"]);
    fail("two leaders", doubleLeaders.slice(0, 20));
  }

  // ── Invariant 6 (whole run).
  if (TENANTS >= 2) {
    const q = latency.quiet.map(([, ms]) => ms);
    const b = latency.base.map(([, ms]) => ms);
    const fmt = (xs: number[]) =>
      `${xs.length} calls, p50 ${quantile(xs, 0.5)} p90 ${quantile(xs, 0.9)} p99 ${quantile(xs, 0.99)} max ${quantile(xs, 1)} ms`;
    // Noisy and RR: a whole-run histogram (quantile error ≤ 1 %, exact max) — print only.
    const fmtHist = (h: LogHist) =>
      `${h.count} calls, p50 ${h.quantile(0.5)} p90 ${h.quantile(0.9)} p99 ${h.quantile(0.99)} max ${h.quantile(1)} ms`;
    console.log(`quiet tenants: ${fmt(q)}`);
    console.log(`noisy tenant:  ${fmtHist(latency.noisy.all)}`);
    console.log(`baseline load: ${fmt(b)}; quiet threshold ${quietLimit(b)} ms`);
    if (RR_TENANT !== undefined) {
      const rr = latency.rr.all;
      console.log(`RR tenant (chaos-t${RR_TENANT}, print only): ${fmtHist(rr)}`);
      if (rr.count === 0) fail("RR tenant", ["the REQUEST_RESPONSE tenant completed no call — green for the wrong reason"]);
    }
    if (q.length === 0) fail("noisy tenant", ["the quiet tenants completed no call — green for the wrong reason"]);
    else if (quantile(q, 0.99) > quietLimit(b)) {
      fail("noisy tenant", [`whole-run quiet p99 ${quantile(q, 0.99)} ms > threshold ${quietLimit(b)} ms`]);
    }
    fail("noisy tenant", quietProblems);
  }

  // ── The quiet phase — see the head of the file. Runs
  // after the lost/wrong verdict and the table at rest, before the final `verify` (verify opens every Db,
  // which must not mix into the quiet phase's request numbers).
  let idleSummary = "";
  if (IDLE_TAIL_S > 0 && haltReason === undefined) {
    const idle = await idleTail([...injected.keys()]);
    idleSummary = idle.summary;
    for (const [what, items] of idle.problems) fail(what, items);
  }

  // ── Invariant 7 (end of run): full verify on the table at rest + orphan objects.
  let retiredSummary = "";
  if (HAS_CTL) {
    try {
      const verifyFrom = Date.now();
      const rep = await ctl<VerifyReport>(["verify"], FINAL_VERIFY_S * 1000);
      console.log(`final verify took ${Math.round((Date.now() - verifyFrom) / 1000)}s (cap ${FINAL_VERIFY_S}s)`);
      const retired = await ctl<RetiredView[]>(["ranges", "retired"]);
      const withDb = retired.filter((r) => r.dbExists).length;
      const orphanFindings = rep.findings.filter((f) => f.check === "orphan_db").length;
      // Each `ranges/<id>` directory is a range in the table, a retired range that still has
      // its Db, or an already-reported `orphan_db`. Any surplus = a retired range that lost its manifest
      // but still has objects — an unfinished GC.
      const leftover = rep.rangeDirs - rep.rangesChecked - withDb - orphanFindings;
      retiredSummary =
        `${retired.length} retired ranges: ${retired.length - withDb} with the Db deleted by GC, ${withDb} still with a Db ` +
        `(${retired.filter((r) => r.dbExists && r.referencedBy).length} still referenced); ` +
        `${rep.rangeDirs} ranges/ directories, ${rep.rangesChecked} ranges in the table`;
      console.log(`final verify: ${rep.findings.length} findings; ${retiredSummary}; D-d Db deletion log lines: ${gcDeletesLogged}`);
      fail(
        "GC/verify",
        rep.findings.map((f) => `end of run ${f.check} range ${f.range}: ${f.detail}`),
      );
      if (leftover > 0) fail("GC/verify", [`${leftover} ranges/ directories of retired ranges with no manifest left but still objects (orphan objects)`]);
    } catch (e) {
      fail("GC/verify", [`final verify failed: ${(e as Error).message}`]);
    }
    console.log(`periodic verify: ${verifyRuns} runs, ${verifyProblems.length} findings still present after rechecking`);
    fail("GC/verify", verifyProblems);
  }

  // ── Invariant 8 (end of run).
  if (FLOORS) {
    try {
      await readFloors(true);
    } catch (e) {
      console.log(`final wal floors failed: ${(e as Error).message}`);
    }
    console.log(
      `shared WAL floor: ${floorRuns} reads, ${floorAdvances} floor advances, ${floorLogsGone} logs cleaned up, ` +
        `${floors.size} logs left, ${floorProblems.length} times standing still longer than ${FLOOR_STUCK_S}s`,
    );
    fail("shared WAL floor", floorProblems);
    if (floorRuns >= 3 && floorAdvances + floorLogsGone === 0) fail("shared WAL floor", ["the floor never advanced during the whole run"]);
  }

  // ── Tenant states, SSO, long store outage.
  if (TENANT_STATES) {
    const byCode = new Map<string, number>();
    for (const [k, v] of refusals) {
      const code = k.slice(k.indexOf("/") + 1);
      byCode.set(code, (byCode.get(code) ?? 0) + v);
    }
    console.log(
      `tenant states: ${tenantEpisodes} spells; transitions (204) ${[...tenantTransitions].map(([k, v]) => `${k}×${v}`).join(" ") || "none"}; ` +
        `202 pending ${tenantStatePending} times (resent)`,
    );
    console.log(
      `  refusals (not acked, not lost) by code: ${[...byCode].map(([k, v]) => `${k}×${v}`).join(" ") || "none"}; ` +
        `by harness state/code: ${[...refusals].sort().map(([k, v]) => `${k}×${v}`).join(" ") || "none"}`,
    );
    console.log(
      `  (b) suspended: ${suspendWindows.length} confirmed windows, ${suspendEntryChecks} handler entries examined, ` +
        `${suspendEndChecks} completed compared by endedAt, ${suspendProblems.length} violations` +
        (suspendEndedInflight > 0 ? ` (${suspendEndedInflight} "STILL COMPLETED" with the handler entered before the suspend command)` : "") +
        "; " +
        `(c) latency samples dropped from invariant 6: quiet ${latencyExcluded.quiet}, noisy ${latencyExcluded.noisy}, baseline ${latencyExcluded.base}`,
    );
    if (tenantEpisodes === 0) fail("tenant state", ["--tenant-states but no spell ran — green for the wrong reason (run too short?)"]);
    else if (suspendWindows.length === 0) fail("tenant state", ["no suspended window was confirmed (204)"]);
    fail("tenant state", tenantStateProblems.slice(0, 20));
    fail("tenant state", refusalProblems.slice(0, 20));
    fail("suspended still running", suspendProblems.slice(0, 20));
  }
  if (SSO) {
    console.log(
      `SSO: ${sso.ok} logins completed, ${sso.tolerated} excused (${[...sso.tolerations].map(([k, v]) => `${k}×${v}`).join("; ") || "—"}), ` +
        `${sso.failed} failed`,
    );
    if (sso.ok === 0) fail("SSO", ["no SSO login completed — green for the wrong reason"]);
    fail("SSO", sso.problems.slice(0, 20));
  }
  if (STORE_OUTAGE) {
    const iso = (t: number | undefined) => (t === undefined ? "—" : new Date(t).toISOString());
    const secs = (a: number | undefined, b: number | undefined) =>
      a === undefined || b === undefined ? "—" : `${((b - a) / 1000).toFixed(1)}s`;
    console.log(
      `long store outage (${OUTAGE_CONTAINER}): stopped ${iso(outage.stoppedAt)}, started again ${iso(outage.startedAgainAt)} ` +
        `(${secs(outage.stoppedAt, outage.startedAgainAt)}), store answered ${iso(outage.storeBackAt)}, ` +
        `cluster acked again ${iso(outage.acceptAt)} — ${secs(outage.storeBackAt, outage.acceptAt)} after the store answered, ` +
        `${secs(outage.stoppedAt, outage.acceptAt)} after the stop; latency samples dropped: quiet ${latencyExcluded.quiet}, noisy ${latencyExcluded.noisy}, baseline ${latencyExcluded.base}`,
    );
    console.log(`OUTAGE_JSON ${JSON.stringify({ container: OUTAGE_CONTAINER, holdS: OUTAGE_S, ...outage, problems: outage.problems.length })}`);
    if (outage.stoppedAt === undefined) fail("long store outage", ["could not stop the store even once — green for the wrong reason"]);
    fail("long store outage", outage.problems);
    const dead = nodes.map((n, i) => [n, i] as const).filter(([n]) => !n.alive);
    if (dead.length > 0) fail("long store outage", [`${dead.length} nodes not alive again at verdict time: ${dead.map(([, i]) => i).join(", ")}`]);
  }
  fail("respawn", respawnGiveUps);

  for (const [what, items] of problems) for (const p of items) console.error(`  [${what}] ${p}`);

  stopping = true;
  for (const n of nodes) n.proc.kill("SIGKILL");
  rogue?.proc.kill("SIGKILL");
  if (SDK_RESTART_EVERY_S > 0) console.log(`  SDK reopened ${sdkRestarts} times (replay cache lost each time)`);
  await sdk.close();
  await sdkRr?.close();
  await sdkLv?.close();
  await issuer?.close();
  for (const f of logFiles.values()) f.end();
  if (LOG_DIR || DISK_PATHS.length > 0) {
    const rot = [...logFiles.values()].reduce((a, f) => a + f.rotations, 0);
    const broken = [...logFiles.values()].filter((f) => f.broken !== undefined).map((f) => `${f.path}: ${f.broken}`);
    console.log(
      `long soak: lowest free disk ${diskLowest ? `${diskLowest.gib.toFixed(1)} GiB (${diskLowest.path})` : "—"}; ` +
        `node logs ${Math.round([...logFiles.values()].reduce((a, f) => a + f.written, 0) / 2 ** 20)} MiB, ${rot} rotations; ` +
        `file write failures ${fileWriteErrors}${broken.length > 0 ? ` (${broken.join("; ")})` : ""}; ${summaries} summary lines`,
    );
  }

  console.log(
    `\nsummary: ack ${billing ? billing.ackedIds.size : ackLedger.size}, lost ${lost}, wrong ${wrong}, kill ${kills}, self-exit ${selfExits}` +
      (KILL_COORDINATOR_EVERY_S > 0 ? ` (coordinator kills ${coordinatorKills}, not found ${coordinatorMisses})` : "") +
      (churn ? `, splits ${churn.splits}, merges ${churn.merges}` : "") +
      (AUTO_SPLIT ? `, auto-splits ${rows.length - Number(PARTITIONS)}` : "") +
      (GC ? `, GC Db deletions (log) ${gcDeletesLogged}` : "") +
      (TENANT_STATES ? `, tenant state changes ${[...tenantTransitions.values()].reduce((a, b) => a + b, 0)}, refusals ${[...refusals.values()].reduce((a, b) => a + b, 0)}` : "") +
      (SSO ? `, SSO ${sso.ok}/${sso.tolerated}/${sso.failed} (done/excused/failed)` : "") +
      (BACKUP_RESTORE
        ? `, backup ${backup.ok}/${backup.cycles} clean cycles (lost_* per cycle: ${backup.lost.map((l) => lostText(l.counts)).join("/") || "—"})`
        : "") +
      (healthyWindows.length > 0
        ? `, healthy windows ${healthyWindows.length} (running examined per window: ${healthyWindows.map((w) => w.ids.size).join("/")})`
        : "") +
      (STORE_OUTAGE && outage.acceptAt !== undefined && outage.storeBackAt !== undefined
        ? `, store outage: took work again after ${((outage.acceptAt - outage.storeBackAt) / 1000).toFixed(1)}s`
        : "") +
      (retiredSummary ? ` — ${retiredSummary}` : "") +
      (idleSummary ? ` — quiet phase: ${idleSummary}` : ""),
  );
  if (haltReason !== undefined) {
    console.error(
      `⛔ STOPPED: ${haltReason} — the destructive phase stopped at ${Math.round(((haltedAt ?? startedAt) - startedAt) / 1000)}s / ${DURATION_S}s; ` +
        `the lost/wrong verdict and the final verify above only cover the part that ran`,
    );
  }
  if (INJECT) {
    const want = INJECTS[INJECT];
    if (problems.has(want)) {
      console.log(`✅ inject ${INJECT}: invariant "${want}" caught the fault`);
      process.exit(0);
    }
    console.error(`❌ inject ${INJECT}: invariant "${want}" did NOT catch the injected fault`);
    process.exit(1);
  }
  if (problems.size > 0) {
    console.error(`❌ FAILED: ${[...problems.keys()].join(", ")}`);
    // Exit code 3: only the early stop (disk), every verdict on the part that ran passed.
    process.exit(problems.size === 1 && problems.has("STOPPED") ? 3 : 1);
  }
  console.log("✅ no acked invocation lost, every invariant holds");
  process.exit(0);
}

// A disk error anywhere without a listener: stop the destructive phase cleanly instead of dying
// midway without a verdict — an earlier soak run died like that in its 4th hour. Other errors
// keep the old behaviour (print, clean up the nodes, exit 1), not swallowed.
process.on("uncaughtException", (e) => {
  if (isDiskError(e)) {
    haltLoad(`disk — unlisted error: ${e.message}`);
    return;
  }
  const survived = survivableUncaught(e);
  if (survived !== undefined) {
    survivedUncaught++;
    if (survivedUncaught <= 20) console.log(`  [${ts()}] uncaught error survived (${survivedUncaught}): ${survived}`);
    return;
  }
  console.error(e);
  restoreOutageContainer();
  stopping = true;
  for (const n of nodes) n.proc.kill("SIGKILL");
  rogue?.proc.kill("SIGKILL");
  process.exit(1);
});

main().catch((e) => {
  console.error(e);
  restoreOutageContainer();
  stopping = true;
  for (const n of nodes) n.proc.kill("SIGKILL");
  rogue?.proc.kill("SIGKILL");
  process.exit(1);
});
