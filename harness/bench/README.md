# Load test — one partition

Two questions, one command: **how much can cairn do** and **does it stay correct
when many turns run at once**.

```sh
cargo build --release --bin cairnflow
node harness/bench/load.ts
```

Exits `0` if every check is green, `1` with a list of failures otherwise. This is
**not** an automated test (no `.test.ts` suffix): it occupies the machine for a few
minutes, and the numbers it prints only mean something together with the machine
that printed them.

## Three processes, and why

| Process | What it is |
|---|---|
| `cairnflow` (Rust, **release**) | the runtime, one node, temporary `--data-dir` ⇒ **one partition** |
| `deployment.ts` (Node) | the service under load + a side server exposing the counters |
| `load.ts` (Node) | load generator, latency measurement, scoring |

The deployment runs in a **separate** process from the load generator. SDK
handlers live on Node's event loop; in a shared process every time the generator
is busy parsing a response is a time the handler does not run, and the measured
number would be "how much Node can do" rather than "how much cairn can do".

A **release** build, not debug. A number measured on a debug build says nothing.

## Scenarios

| Name | What it measures |
|---|---|
| `echo` | empty handler — the floor of every measurement |
| `run1` | one `ctx.run`; the gap to `echo` is the price of one durable journal entry |
| `run5` | five sequential `ctx.run` — cost as a function of *journal length* |
| `state` | virtual object, one key per call — the state write path |
| `hot` | **every** call on **one** key — the exclusive lock must serialize them |
| `call` | `ctx.call` — one invocation spawns another and waits for it |
| `sleep` | `ctx.sleep` — suspend, timer, wake: the longest path through the engine |
| `send` | fire-and-forget: *accept* throughput, then wait for the queue to drain |
| `xcall` | keyed `ctx.call` between two virtual objects on **two partitions**, fired in both directions at once — needs `LOAD_ARGS`, see below |
| `tree` | unkeyed, unscoped call tree, depth `LOAD_TREE_DEPTH`, width `LOAD_TREE_WIDTH` — a workflow / agent tree |
| `fanout` | a parent calls `LOAD_FANOUT` unkeyed children at once — ETL / batch |
| `agent` | one `ctx.run` sleeping `LOAD_STEP_MS`, then 3 sequential unkeyed `ctx.call` |
| `saga` | 3 virtual objects with different keys, called in sequence — a saga across entities |

The run opens with a **concurrency sweep** on `echo`, on a clean store: does adding
waiters add work done? The `server inv/s` column is the number of completed calls
the server counts (`cairnflow_invoke_completed_total`) in the same window — an
independent cross-check of the load generator; a mismatch turns the run red.

At the end, the highest sweep level runs once more (the **residual state** line).
Clearly slower (below ×0.7) means earlier scenarios left behind work that makes
*every* later turn more expensive. This is a warning flag, not a check. The sweep
used to sit at the end of the run and reported 2–5× less than a standalone sweep.
The cause was not miscounting: `call`/`send`/`xcall` leave deleted outbox/inbound
rows behind, and the drainer scanned the index from `seq` 0 across every tombstone
on every turn. That bug is fixed (the writer's scan watermark,
`docs/guides/flow.md` FL-15). At level 64, on a machine doing other work, this line
is noisy up to ±40%; to compare two binaries run them interleaved,
`LOAD_SCENARIOS=xcall LOAD_SWEEP=16 LOAD_CONCURRENCY=16`.

## Checks

Load is a sieve. Bugs that a sequential test never sees only show up when many
turns run at once:

- **Exactly-once.** The counter lives *inside* the `ctx.run` closure. There is no
  `kill -9` anywhere in this run, so the promise is absolute: every tag exactly
  once. A 2 means the engine re-ran a side effect that was already durable.
- **No lost update** (`hot`). `touch` reads `count` and writes `count+1`. Every
  returned value must be pairwise distinct, **and** the final state must equal
  the number of calls.
- **FIFO order on one key** (`hot`). One worker sends sequentially, so the `count`
  it gets back — its execution position — must increase monotonically. Other
  workers interleaving is fine; reordering **one** worker's calls is not.
- **No misrouted output.** Every call checks that its output carries its own tag —
  catches two invocations receiving each other's results.
- **Timers never fire early** (`sleep`). Latency must be ≥ the sleep time.
- **Accepted work gets done** (`send`). `/send` answers once the invocation is
  durable; the run waits until the queue drains.
- **Idempotency.** 32 parallel calls with the same `Idempotency-Key` ⇒ one
  invocation, one execution, one id.
- **Contiguous journal.** Random samples check that indexes run contiguously
  from 0.
- **Nothing left hanging.** Once load stops, `running`/`suspended`/`paused` must
  be empty.
- **No turn past the SDK deadline** (`cairnflow_sdk_timeouts_total`).

## Knobs

| Env | Default | |
|---|---|---|
| `LOAD_CONCURRENCY` | `8` | calls in flight (closed loop) |
| `LOAD_DURATION_S` | `20` | duration of each scenario |
| `LOAD_WARMUP_S` | `3` | skip this many seconds at the start |
| `LOAD_SCENARIOS` | *(all)* | filter, e.g. `echo,run1,hot` |
| `LOAD_SWEEP` | `1,4,16,64` | concurrency levels to sweep (start of the run, clean store); `0` = disable both the sweep and the residual state line |
| `LOAD_SEND_COUNT` | `100` | cap on the number of calls of the `send` scenario |
| `LOAD_MAX_TURNS` | `= max(LOAD_CONCURRENCY, LOAD_SWEEP)` | the server's `--max-concurrent-turns` |
| `LOAD_NAP_MS` | `200` | sleep time of the `sleep` scenario |
| `LOAD_SDK_MODE` | `BIDI_STREAM` | or `REQUEST_RESPONSE` |
| `LOAD_BINARY` | `target/release/cairnflow` | |
| `LOAD_ARGS` | *(empty)* | extra arguments for `cairnflow`, appended at the end; with `--main-store`/`--data-dir` the run does not provide its own store |
| `LOAD_STALL_S` | `20` | `xcall`: no call completes for this many seconds ⇒ **WEDGED** |
| `LOAD_XCALL_KEYS` | `max(8, LOAD_CONCURRENCY)` | keys per side for `xcall` |
| `LOAD_XCALL_ONEWAY` | *(off)* | `1` ⇒ `xcall` fires only A → B (control) |
| `LOAD_TREE_DEPTH` / `LOAD_TREE_WIDTH` | `3` / `10` | tree shape of `tree` (1 111 invocations per tree) |
| `LOAD_TREE_CONCURRENCY` | `2` | trees in flight at once |
| `LOAD_FANOUT` | `100` | children of one `fanout` parent |
| `LOAD_FANOUT_CONCURRENCY` | `2` | `fanout` parents in flight at once |
| `LOAD_FANOUT_SPREAD` | — | deliberately spread the `fanout` children: `placement` (`placement: "spread"` — the cluster must enable the `calls/placement` gate), or `scope` / `idem` (one scope / idempotency key per child, the workaround from before `placement` existed); absent ⇒ unkeyed children, colocated |
| `LOAD_STEP_MS` | `1000` | sleep time of the `ctx.run` in `agent` |
| `LOAD_SETTLE_S` | `120` | cap on waiting for an invocation that received `202` to come to rest |

The default concurrency of `8` is there to keep the run fast, **not** because the
knee is there: with group commit, the knee of `echo` sits around 16–64 (see the
sweep at the start of the run). Set it higher to see the real ceiling.

## `xcall` — cross-partition, both directions

The scenarios above measure **one** partition. `xcall` needs at least two, and it
exists to find stalls, not to produce nice numbers:

```sh
LOAD_ARGS="--main-store memory:// --cluster --partitions 4" \
  LOAD_SCENARIOS=xcall LOAD_SWEEP=0 LOAD_CONCURRENCY=4 \
  node harness/bench/load.ts
```

Before firing, the run **asks** the runtime (an empty `where` handler, reading the
partition from the id) to pick `XcallA` keys that live on p0 and `XcallB` keys that
live on p1 — no client-side rehashing, so it holds for any hash function. Even calls
are `XcallA.ping` (p0) → `XcallB.pong` (p1), odd calls are the opposite direction;
both directions fly at once. `pong` is `SHARED`, and the keys receiving `ping` are
disjoint from the keys receiving `pong`: an `EXCLUSIVE` lock cycle between two
objects is an application bug, not an engine bug, so the run is built so that one
cannot happen.

`LOAD_XCALL_ONEWAY=1` fires only A → B. Note it is **not** a "one message
direction" control: `ctx.call` A → B still sends StartCall p0 → p1 *and* the
result p1 → p0.

The scenario has a watchdog: no call completes within `LOAD_STALL_S` seconds while
calls are still in flight ⇒ the run prints `WEDGED after N completions`, asks the
node what it still answers (`/health`, `/cluster`, `/invocations`, each capped at
3 s — telling a stuck *invocation* from a stuck *engine*), skips the global checks
(they would only wait on a partition that does not answer) and exits `1`.

## Cross-range — `tree`, `fanout`, `agent`, `saga`

Four call shapes of agent and workflow workloads. They run on one partition (every
call local) but are written to be measured on a multi-range cluster:

```sh
LOAD_ARGS="--main-store memory:// --cluster --partitions 4" \
  LOAD_SCENARIOS=tree,fanout,agent,saga,xcall LOAD_SWEEP=0 \
  node harness/bench/load.ts
```

Every scenario with a `ctx.call` (including `xcall`) prints an extra
**cross-range** line read from `/metrics` over the measurement window, and the
end of the run has a table summing them up:

- calls/s and the `placement=remote` share by lock level (`cairnflow_calls_total`):
  unkeyed callees are spread at random today, so on R ranges the share is
  ≈ (R−1)/R;
- cross-range messages/s and broken messages
  (`cairnflow_interrange_messages_total`); there is no `kill -9` in this run, so a
  broken message is a red check;
- mean time (and the bucket upper bound of p50/p99) of the two phases of the
  outbox drainer, `sync` (waiting for the outbox row to be durable) and `send`
  (HTTP plus the durable write on the receiver); the `sync+send` column is the sum
  of both phases per wall-clock second — roughly the number of continuously busy
  drainers.

Correctness checks: `tree` — the root receives exactly the number of nodes in the
tree and every leaf runs `ctx.run` exactly once; `fanout` — all children, one side
effect each; `agent` — `think` and three tools, each once; `saga` — every step sees
the empty state of its own key (`count` = 1).

## Reading the results

The `turns/inv` column is the number of times the handler body ran per invocation.
`>1` is **replay**, not a failure — a `ctx.get` taking the lazy path ends the turn
and runs again from the top with a longer journal.

`429` is not a failure: ingress says explicitly that the invocation **is durable
and has been rescheduled**. The generator counts it separately and still waits for
it to finish.

## The ceiling of one partition — two knobs, independent of each other

Every number above is governed by one thing: **an invocation costs a few commits,
and a commit costs one WAL flush tick**. `echo` costs ~5 commits, `run1` ~8,
`run5` ~20.

### Knob 1 — the WAL flush interval (tuned)

`PartitionWriter::commit` waits on `await_durable()`, and a write is only durable
once the WAL is flushed; SlateDB flushes on a tick. SlateDB's default is 100ms;
cairn now opens the db with 10ms ([`DEFAULT_FLUSH_INTERVAL`], changed with
`--flush-interval-ms`).

Measured on the same machine, same run, one knob changed:

| `--flush-interval-ms` | `echo` inv/s | `echo` p50 @ concurrency 1 | `run1` inv/s |
|---|---|---|---|
| 100 | 2.0 | 509 ms | 1.2 |
| **10** | **16.9** | **59 ms** | **10.6** |

**8.5×**, without changing a single line of logic. 59ms ÷ 5 commits ≈ 12ms/commit —
exactly the flush tick, so the model matches the measurement.

The cost is on the store side: flushing more often ⇒ less batching ⇒ more, smaller
WAL objects. On `file://` that is nearly free; on S3 those are paid requests and the
"IOPS amplification" that the WAL-store benchmark was built to measure. **Measure
before tuning for production.**

### Knob 2 — group commit (done)

Knob 1 cannot touch concurrency: throughput stayed flat at 17/s with 1 or 16
waiters, and latency grew perfectly linearly (59 → 236 → 874 ms). More clients
bought nothing.

**The cause is not in SlateDB**, and that is a measurement, not an inference.
`crates/cairn-store/tests/group_commit.rs` issues 50 writes into the same `Db`,
`flush_interval = 10ms`:

```
sequential (write → await_durable → write → …):  582.6 ms   11.65 ms/write
concurrent (issue all → wait for all):            13.1 ms    0.26 ms/write
                                                            ────── 44.6×
```

50 concurrent writes cost **one** flush tick. SlateDB gathers every in-flight
write into one `current_wal` and flushes once — batching is something it already
does, and does well. Cairn simply never handed it two writes at once:
`EngineLock = Arc<Mutex<Engine>>`, `PartitionWriter` lives *inside* `Engine`, and
`commit` waited for durability **while still holding the mutex**. The buffer never
held more than one entry to batch.

Group commit splits `commit` into two steps: hold the mutex across `db.write()`
(~0 ms, only appends to the buffer), **release** it across `await_durable()`
(`Lease::sync`).

```
concurrency   inv/s    p50 ms
16          368.8      45.7
64         1462.8      43.9     ← the knee: throughput ×4, latency UNCHANGED
256        1459.7     174.2
1024       1439.5     699.9
```

No longer flat. Up to level 64, p50 **stands still** at ~44 ms while throughput
quadruples — exactly the shape of many turns sharing one flush tick. Past 64,
throughput levels off at ~1460/s and latency grows in proportion to concurrency:
real saturation.

| | echo inv/s | p50 |
|---|---|---|
| flush 100 ms, no grouping | 2.0 | 509 ms |
| flush 10 ms, no grouping | 16.9 | 59 ms |
| **flush 10 ms + grouping** | **1460** | **44 ms** |

**730×** over the starting point, without changing a single line of semantics.

**What caps it at 1460/s is not yet known.** Not CPU (`cairnflow` ~20–30% of one
core, the deployment ~3%), not `--max-concurrent-turns`. Candidates: one engine
thread per partition (`LocalSet`), or the WAL throughput itself.

### On a real object store (MinIO)

`LOAD_STORE_URI=s3://…` — same run, same machine, only the store changes:

| concurrency | `file://` inv/s | MinIO inv/s | `file://` p50 | MinIO p50 |
|---|---|---|---|---|
| 1 | 21.3 | 18.4 | 46.8 ms | 47.9 ms |
| 16 | 368.8 | 289.0 | 45.7 ms | 54.6 ms |
| 64 | 1462.8 | 1040.2 | 43.9 ms | 60.2 ms |
| 256 | 1459.7 | 1046.0 | 174.2 ms | 241.3 ms |

~30% of throughput lost, latency at low concurrency **almost unchanged**. The reason
is worth more than the number: with `flush_interval = 10ms`, a store whose PUT is
faster than 10 ms is **invisible** — the flush tick hides it.

**And that is also the limit of this measurement.** MinIO here runs on `localhost`,
so it measures *the price of the S3 protocol* (request signing, HTTP,
serialization) and **not** the network latency to a real bucket. On real S3 with
PUTs of ~20–50 ms, the PUT itself will exceed the flush tick and become the
dominant factor — roughly three times the per-call latency is a reasonable
estimate, and it is still **unmeasured**. Do not quote the table above as if it
were a production number.

The `hot` scenario does **not** get faster (16.9/s) — and that is a good sign, not
a shortcoming: every call there contends for the same object key, so the exclusive
lock must serialize them however much the layer below batches.

### INV-5 after the two-step split

`commit` is no longer the fence; the fence moved to [`Committed`] — a ticket that
only `Pending::durable()` issues. All three kinds of cairn's **externally visible
effects** demand that ticket in their signature, so **the compiler keeps the list
instead of memory**:

| Demands the ticket | Which effect |
|---|---|
| `Shared::finish` | answering the client (16 call sites) |
| `Transport::send` | sending a cross-partition message (7 sites) |
| `awaiting_sdk` | letting the customer's handler run (5 sites) |
| `drive_client`'s `on_queued` | the early "queued" answer |

The last one is where the compiler did **not** catch it at first: it sent straight
into a `oneshot` instead of going through `finish`. The new `hello_durable` test
suite is what found it.

The consequence has to be said out loud: **the lost-on-crash window widens by
exactly one flush tick.** That is safe precisely because nothing leaves the process
inside that window — dying there is dying before anyone knows anything happened.
The fence moved too: a spike measured that a fenced writer still accepts `put` and
only fails at the forced-durability step, so a zombie can now *write* but can never
*issue a ticket*, i.e. can never ack anyone.

That argument has been **checked at the system level**, not just derived:
`harness/chaos/chaos.ts` ran for an hour on MinIO, 3 nodes × 8 partitions, `kill -9` of a
random node every 10 s, revived after 4 s.

```
verdict: 9466 acked → lost 0, wrong 0
✅ no acked invocation lost
```

`wrong 0` covers both: the output has the right content, **and** `ctx.run` ran at
least once for every completed invocation.
