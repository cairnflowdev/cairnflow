# Benchmarks

This page lists the measurements we publish and, for each, the exact command that reproduces it.
A number without its hardware, store and command is not useful, so every result here states all
three. All harnesses live in this repository; none of them needs a cloud account unless the
section says so.

> **No results are published yet.** The numbers measured so far come from shared development
> machines with other workloads running, so they are not reproduced here. Each section gives the
> exact command and says what it reports, so you can run it on your own hardware and store.
> Published results will state the CPU model, core count, memory, OS, cairnflow commit and store.

## How to read these numbers

- **The store sets the floor.** Every committed step is at least one object-store PUT on the
  WAL tier (see [`docs/architecture.md`](architecture.md) §4). On an in-memory store the engine
  itself is measured; on S3 Standard the PUT latency dominates. Results are therefore given per
  store profile.
- **Simulated S3.** Without a cloud account, the `testkit` build can wrap an in-memory store in a
  latency and throttling model of a real S3 tier (`CAIRNFLOW_STORE_PROFILE`, profiles in
  `bench/s3-model/profiles/`: `count-only`, `s3-standard`, `s3-express`, and `*-faults`
  variants). A simulated profile is labelled as such; it is not a substitute for a real store.
- **Release builds only.** Debug-build numbers say nothing.
- **Load and correctness together.** `load.ts` fails the run if any correctness check fails under
  load (exactly-once side effects, no lost update on a hot key, FIFO per key, timers never early,
  idempotency, no stuck invocations). A fast run that fails a check is not a result.

## 1. Single-range throughput and latency

What it measures: invocations per second and latency percentiles of one range, for a handler
with no steps (`echo`), one `ctx.run` (`run1`), five (`run5`), per-key state (`state`), one hot
key (`hot`), a call (`call`), a durable sleep (`sleep`) and fire-and-forget (`send`), plus a sweep
over concurrency 1, 4, 16 and 64.

```sh
cargo build --release --bin cairnflow
node harness/bench/load.ts                                  # BIDI_STREAM
LOAD_SDK_MODE=REQUEST_RESPONSE node harness/bench/load.ts   # REQUEST_RESPONSE
```

Knobs (`LOAD_CONCURRENCY`, `LOAD_DURATION_S`, `LOAD_SCENARIOS`, `LOAD_SWEEP`, …) are documented
in [`harness/bench/README.md`](../harness/bench/README.md).

**Setup to state.** Machine, commit, store (`--data-dir` on local SSD).

**Reports.** Table of `inv/s`, p50, p99 per scenario; sweep table.

## 2. Calls between ranges

What it measures: the cost of a call whose caller and callee live in different ranges (outbox →
internal HTTP → inbound, two durable writes and a round trip), in both directions at once
(`xcall`), and the shapes that keyless call trees, fan-outs, agent loops and sagas take on a
four-range cluster.

```sh
cargo build --release --bin cairnflow
BINARY=target/release/cairnflow PROFILE=memory OUT=target/bench/interrange \
  bench/interrange/run.sh

# Same on the simulated S3 Standard profile:
cargo build --release -p cairn-server --features testkit --bin cairnflow
BINARY=target/release/cairnflow PROFILE=s3-standard OUT=target/bench/interrange-s3 \
  bench/interrange/run.sh
```

**Reports.** `xcall` p50/p99 at concurrency 1 and 64; `tree`, `fanout`, `agent`,
`saga` completion rates; per store profile.

## 3. Latency versus store latency

What it measures: how commit latency and single-range throughput follow the object store's PUT
latency and the shared WAL's batching window (`--wal-window-ms`). Each point is a fresh node on a
clean store with an injected PUT latency.

```sh
cargo build --release -p cairn-server --features testkit --bin cairnflow
node harness/bench/latency-curve.ts                          # full grid, ~30 minutes
CURVE_PUT_P50=0,20 CURVE_WINDOWS=10 node harness/bench/latency-curve.ts   # two points
node harness/bench/latency-curve-report.ts <the JSONL file named by CURVE_OUT>
```

For a per-stage breakdown of one cross-range call (where the milliseconds go between ingress,
engine, WAL PUT, outbox, peer, and reply), `bench/latency-floor/run.sh` records stage timestamps
with a `--features testkit,probe` build; see the header of that script.

**Reports.** Curve of `send`, `echo`, `run1` latency and single-range throughput
against PUT p50, per window.

## 4. Idle cost in requests

What it measures: object-store requests per hour that an idle cluster makes — session renewals,
owner-record watches, manifest polls, GC and scrubber rounds — and how they grow with ranges and
nodes. This is counted, not timed, so it does not depend on hardware.

```sh
cargo test -p cairn-server --test idle_cost -- --nocapture
```

The tests assert caps per request source, for one node and for clusters of two and three nodes,
and print the measured counts.

**Reports.** Requests per hour per source for one node and for three nodes × 8
ranges, from the test output.

## 5. Streams under load

What it measures: end-to-end latency of durable stream records from a handler to a server-sent
events client, and the extra store requests that 256 concurrent SSE clients add, on a four-node
cluster.

```sh
scripts/dev-store/up.sh          # SeaweedFS on localhost:18333 (Docker)
cargo build --release -p cairn-server --bin cairnflow --bin cairn-ctl \
  --features failpoints,testkit --target-dir target/chaos
node harness/bench/agent-sse.ts --out target/bench/sse.jsonl
```

**Setup to state.** Machine, SeaweedFS version.

**Reports.** Record latency p50/p99; store requests per second with and without SSE
clients.

## 6. Query layer

What it measures: latency of the console's read-only SQL questions over invocations on a bucket
with a known number of invocations. See [`bench/query/README.md`](../bench/query/README.md) and
`harness/bench/console-queries.ts`.

**Reports.** Median latency and read units per question.

## Correctness under faults

Throughput is only half of the claim. The chaos harness on a real S3-compatible store (SeaweedFS
locally) kills nodes with `kill -9` at random under load, splits and merges ranges, and arms
failpoints on live processes; its verdict is "no acknowledged invocation lost or run twice". How
to run it is described in [`docs/testing.md`](testing.md) §10.

**Reports.** Duration, kills, splits/merges, acknowledged invocations, lost (must
be 0), duplicated (must be 0), for one documented run.
