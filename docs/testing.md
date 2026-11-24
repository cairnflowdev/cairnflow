# Testing

A diskless engine has no replica to fall back on: one wrong delete, one acknowledgement before a
write is durable, or one stale writer, and the only copy of a customer's state is gone. Ordinary
unit and integration tests reach the common paths. The failures that matter live in rare
interleavings — a node frozen across a lease expiry, a crash between two CASes, a retried PUT
whose first attempt already landed. Cairnflow therefore layers several kinds of checking, from
exhaustive models to hours of chaos on a real store. This page describes each layer and how to
run it.

| Layer | What it finds | Where | Typical run |
|---|---|---|---|
| [Protocol models](#1-protocol-models-m1m4) | Interleavings of leases, split/merge/GC, the shared WAL, the invocation lifecycle | `crates/cairn-model` | seconds; exhaustive variants minutes |
| [Decision cores and row tables](#2-decision-cores-and-row-tables) | Wrong decisions of the lifecycle and split/merge logic | `crates/cairn-lifecycle`, `crates/cairn-split` | seconds |
| [Store model contract](#3-the-store-model-contract) | Divergence between the range store and its model | `crates/cairn-store/src/testkit/range_model.rs` | seconds |
| [Deterministic simulation](#4-deterministic-simulation) | Whole-cluster bugs under seeded faults, with replayable traces | `crates/cairn-server/tests/sim_cluster.rs` | minutes |
| [Failpoints](#5-failpoints-and-tripwires) | Crashes and pauses at named steps of dangerous paths | `tests/failpoints.rs` | minutes |
| [Ambiguous-result injection](#6-ambiguous-store-results) | Conditional writes misread after client retries | `CAIRNFLOW_TEST_FAULTS` | minutes |
| [Library and store contracts](#7-library-and-store-contracts) | Changed behaviour of SlateDB, object_store or an S3 store | `library_contracts.rs`, `s3_store_contract.rs` | seconds; real store minutes |
| [Verify on drop](#8-verify-after-every-cluster-test) | Leaks and dangling references left by any cluster test | `crates/cairn-server/tests/common` | part of every cluster test |
| [End-to-end and conformance](#9-end-to-end-and-conformance) | SDK ↔ runtime behaviour on the wire, single node and clusters | `sdk/typescript/test`, `conformance/` | minutes |
| [Chaos](#10-chaos-on-a-real-store) | Lost or duplicated acknowledged work under `kill -9`, splits and failpoints | `harness/chaos/chaos.ts` | 10 minutes to hours |
| [Mutation testing](#11-mutation-testing-of-safety-checks) | Safety checks that no test notices when broken | `scripts/mutants.py` | long |
| [Ready](#12-the-ready-profile-scriptscheckssh) | Everything above that fits in one command | `scripts/check.sh` | ~5–25 minutes |

Prerequisites for the Rust layers: the toolchain pinned in `rust-toolchain.toml`, `protoc`, and
the SlateDB fork checked out (`git submodule update --init`). The TypeScript layers need
Node.js 22.18 or newer. The chaos layer needs Docker for a local SeaweedFS.

## 1. Protocol models (M1–M4)

`crates/cairn-model` checks the distributed protocols with the
[`stateright`](https://github.com/stateright/stateright) model checker. Each model's actions are
the atomic steps the real code takes, named after the real functions, and every fence the code
has is a configuration flag:

- **M1** — sessions, owner-record CAS, `writer_epoch`, delegated and solo renewal, the table read
  before opening a DB, and the quiesce and closed-flag fences of split/merge.
- **M2** — split, merge, retired-DB GC and fork. It runs the production decision core
  `cairn_split::step` against a modelled bucket.
- **M3** — the per-node shared WAL: sealing, takeover, the GC floor, dead-log sweeping, stripe
  rotation and follower exposure.
- **M4** — the invocation lifecycle. Its transition function *is* the production core
  (`cairn_lifecycle::step_mutated`); only the shell's event order and the environment are modelled.

The acceptance test of a model is two-sided: with every fence on, the checker explores the whole
state space and every property holds; with one historical fix switched off, the checker finds a
counterexample. Every bug fixed in these areas becomes a `Bugs` flag and a row of the model's
matrix (`crates/cairn-model/tests/m2.rs::matrix`, `m3.rs::matrix_one_fence_off`, `m4.rs`), so it
cannot come back unnoticed.

```sh
cargo test -p cairn-model                                   # default budgets, seconds to a minute
cargo test -p cairn-model -- --ignored                      # exhaustive M1 variants, minutes
cargo test --release -p cairn-model --test m4 -- --ignored  # larger M4 budgets
```

A model is a hypothesis about the code, and it drifts like any document. Changing the event order
of the shell (for example in `crates/cairn-server/src/turn/` or `split/`) means changing the
model's environment in the same commit; the mapping tables at the top of each model file say
which code each model state stands for.

## 2. Decision cores and row tables

The two most error-prone decision areas are pure functions with no I/O:

- `crates/cairn-lifecycle` — every invocation status change, attempt count, retry and pause is
  decided by `step`; the server executes its plans. A source lint rejects status changes outside
  the shell.
- `crates/cairn-split` — every split/merge/roll-back/GC decision is made by `step`; a source lint
  rejects table CASes and child clean-ups outside the shell.

Both are tested row by row (`crates/cairn-lifecycle/src/tests/`, `crates/cairn-split/src/tests/`):
each row is one situation and the decision expected for it. Changing a rule means changing its
row in the same commit; old behaviour that the product deliberately changed stays reachable
through a `Bugs` flag so the models can still show why it was wrong.

```sh
cargo test -p cairn-lifecycle -p cairn-split
```

The flow-control core (`crates/cairn-server/src/flow/`) has its own tokio-free simulator
(`flow/sim/`) that applies random event sequences and checks the FL invariants after every step
(`cargo test -p cairn-server --lib flow`).

## 3. The store model contract

`RangeStore` (the range's storage interface) has an in-memory model
(`crates/cairn-store/src/testkit/range_model.rs`). One contract suite runs against both the real
SlateDB-backed store and the model; a difference is a bug in one of them. The engine's randomised
crash-and-recover tests then run on the model at high speed.

```sh
cargo test -p cairn-store --test range_store
cargo test -p cairn-engine --test range_model
```

## 4. Deterministic simulation

`crates/cairn-server/tests/sim_cluster.rs` runs a three-node cluster on one shared in-memory
bucket inside a single thread: a `current_thread` tokio runtime with paused time, every range
engine on its local set, and SlateDB's compaction and GC on the same runtime. Every source of
non-determinism goes through one seeded generator:

- wall clock, process ids and random bytes (`cairn_store::entropy`);
- the network between nodes and to SDKs (`netfilter::LocalNet` — deliver, drop, delay, or hold
  and release, drawn from the seed);
- the store (each request goes through, is delayed in virtual time, or fails);
- the workload (callers, callees in other ranges, handlers that fail a seeded number of times).

Each seed also turns faults on or off as a **swarm**: store errors, network errors, killing a
node, ambiguous conditional writes, dying inside a library gap (SST uploaded, manifest not yet
written), restarting a node with different WAL settings, and flow-control limits. A run has a
fault phase, a healing phase, and a **liveness phase** that runs until no work is pending — every
invocation finished, every range led, no intent left — and then a full `cairn-ctl verify`. A red
seed is **shrunk** along the swarm dimensions and printed as a ready-to-paste `#[test]`. The same
seed produces the same event trace, byte for byte.

```sh
cargo test -p cairn-server --features testkit --test sim_cluster          # default seeds
CAIRN_SIM_SEEDS=64 cargo test -p cairn-server --features testkit --test sim_cluster
CAIRN_SIM_SEED=1234 CAIRN_SIM_SEEDS=1 \
  CAIRN_SIM_TRACE_DIR=target/sim-traces \
  cargo test -p cairn-server --features testkit --test sim_cluster        # replay one seed

# Determinism criterion: same seed ⇒ same trace hash, including tokio's own scheduling.
CARGO_TARGET_DIR=target/sim-unstable RUSTFLAGS="--cfg tokio_unstable" CAIRN_SIM_SEEDS=50 \
  cargo test -p cairn-server --features testkit --test sim_cluster
```

Determinism is enforced by lint, not by convention: `clippy.toml` forbids wall-clock time,
`std::time::Instant`, the standard `HashMap`, `getrandom` and direct HTTP clients outside the
seeded wrappers, and `scripts/lint-sources.sh` rejects bare `sleep(…).await` in background loops
and SlateDB builders that are not seeded from `entropy`. The SlateDB fork adds a spawn hook so
its background tasks run inside the simulation.

## 5. Failpoints and tripwires

Dangerous paths carry named `failpoint!` sites (the [`fail`](https://crates.io/crates/fail)
crate) at every step boundary: each phase of split and merge, the GC's check-then-delete,
WAL takeover, migrations, tenant purge, audit export, and so on. The list of names is
`cairn_store::failpoint::NAMES`. Tests live only in the two `tests/failpoints.rs` binaries,
built with `--features failpoints`:

```sh
cargo test -p cairn-store -p cairn-server --features failpoints --test failpoints
cargo test -p cairn-server --features failpoints --test failpoints split   # one area
```

Conventions, enforced in review:

- Every test starts with `StepController::new()`; tests that pause use a `multi_thread` runtime.
- In multi-node harnesses (all nodes share one process and one failpoint registry), a failpoint is
  conditioned on the node (`StepController::fail_on`, `pause_at_on`) unless the test means every
  node.
- Every race test begins with a doc comment that numbers the interleaving it reproduces and ends
  with "Must see: …".
- Waiting uses channels and bounded waits (`Paused::expect_stopped`, `FailGuard::callback`),
  never `sleep` as "probably done".
- **Tripwires.** A branch that must never run — committing to a range without a lease,
  acknowledging before durable, deleting something still referenced, reopening a quiesced source
  — carries a failpoint right on it, so tests can arm it with `panic`.

Failpoints also work on real processes: a binary built with `--features failpoints` exposes
`POST /admin/failpoints`, which the chaos harness uses to arm random points on live nodes
(section 10).

## 6. Ambiguous store results

The object-store client retries 5xx responses even for conditional PUTs, so a write can succeed
and still be reported as `Precondition` or `AlreadyExists`, and an error can hide a write that
landed. Cairnflow resolves every such case by reading back (registered as OS1 and RD3). To keep
it that way, the cluster tests can run with every cairn-managed write made ambiguous once:

```sh
CAIRNFLOW_TEST_FAULTS=each-once cargo test -p cairn-server --test split
CAIRNFLOW_TEST_FAULTS=each-once+nodewal cargo test -p cairn-server --test split   # WAL objects too
```

With `each-once`, the first write or delete of each (operation, path) pair on a cairn-managed
object reaches the store and is applied, and the caller then receives the answer of the client's
retry. `each-once+nodewal` extends this to the shared-WAL log objects. Every new conditional
write must come with a test of its ambiguous outcome.

For performance and fault work without a cloud account, `CAIRNFLOW_STORE_PROFILE=<file>` (with
`--features testkit`) wraps the store in a simulated S3 with latency distributions, throttling
(`503 SlowDown`) and per-path fault classes; profiles live in `bench/s3-model/profiles/`.

## 7. Library and store contracts

Cairnflow's safety rests on specific behaviours of SlateDB, the `object_store` client and the
S3 store. Each assumption is registered (`G*`, `DC*`, `OS*`, `S3-*` in
[`docs/design/decisions.md`](design/decisions.md)) and pinned by a test, so an upgrade that
changes it fails loudly:

```sh
cargo test -p cairn-store --test library_contracts       # SlateDB, disk cache, object_store
cargo test -p cairn-server --test s3_client_contract      # the S3 client against a fake S3

# The store itself (ignored by default; needs a real bucket):
CAIRNFLOW_STORE_CONTRACT_URI=s3://bucket/prefix \
AWS_ENDPOINT=http://localhost:18333 AWS_ALLOW_HTTP=true \
AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=… AWS_REGION=us-east-1 \
  cargo test -p cairn-store --test s3_store_contract -- --ignored --nocapture --test-threads 1
```

The store contract covers read-after-write and list-after-write consistency, atomic PUT under
`kill -9`, conditional PUT with versioning, `last_modified` versus node clocks, and conditional
GET never returning a stale `304`. Run it against any store before using it in production.

## 8. Verify after every cluster test

Cluster tests build their bucket with `common::bucket()` (never a bare in-memory store). When the
bucket is dropped, after the nodes have stopped, it runs `cairn-ctl verify` on it and fails the
test on any finding that no background pass would heal: a leaked DB, a dangling reference, a
range table that does not cover the key space, an orphan owner record. A test that leaves
debris on purpose says so with `bucket.allow(check, reason)`.

```sh
CAIRNFLOW_TEST_VERIFY=0   cargo test -p cairn-server --test split   # see the original error first
CAIRNFLOW_TEST_VERIFY=all cargo test -p cairn-server --test split   # treat every finding as an error
```

The same checks run in production as the background scrubber (`--scrub-every-s`), read-only.

## 9. End-to-end and conformance

**Wire conformance.** `conformance/traces/` holds recorded protocol traces; the runtime must
replay each of them exactly, and every SDK runs the same traces through its own runner.
Intentional wire changes regenerate them ([`conformance/README.md`](../conformance/README.md)).

```sh
cargo test -p cairn-runtime -p cairn-protocol
cd sdk/typescript && node --test --test-timeout=60000 test/conformance.test.ts
```

**End-to-end** tests run the TypeScript SDK against a real `cairnflow` binary, as one node and as
a four-range cluster, over both transport modes (`BIDI_STREAM`, `REQUEST_RESPONSE`), with an
optional saboteur that keeps splitting and merging ranges during the run:

```sh
cargo build --bin cairnflow
cd sdk/typescript
CAIRNFLOW_E2E_ARGS="--main-store memory:// --partitions 4 --tick-interval-ms 100" \
  node --test --test-reporter=spec --test-timeout=120000 test/e2e.test.ts
```

Always pass `--test-timeout` to `node --test`: its default is infinite, so a hung test looks like
a stuck run. Results of the end-to-end suite depend on test order; a run filtered with
`--test-name-pattern` is not a baseline.

**The web console** has its own suite (`cd ui && npm ci && npm run check`, then `npm run e2e` with
Playwright against a real binary).

## 10. Chaos on a real store

`harness/chaos/chaos.ts` is not a test file but a harness run by hand: a multi-node cluster
on a real S3-compatible store under steady load, with nodes killed by `kill -9` at random. The
verdict at the end is strict: no acknowledged invocation lost or run twice, no response outside
the API contract, and — after the faults stop — a range table at rest (full coverage, no intent,
every range led).

The harness lives in `harness/` (BUSL-1.1, its own `package.json`), next to the benchmark drivers
of `harness/bench/`. It imports the SDK by relative source path, so it runs straight from a
checkout with no build step. Its own unit tests (the chaos case table, the history and ledger
checkers) run with `cd harness && npm ci && npx tsc --noEmit && node --test
--test-timeout=60000 "*/*.test.ts"`.

Use SeaweedFS locally. Some erasure-coded stores tear small objects when a client is killed in the
middle of a conditional PUT, which measures the store, not cairnflow.

```sh
scripts/dev-store/up.sh                      # SeaweedFS on localhost:18333 (Docker)
cargo build --bin cairnflow
CHAOS_STORE_URI=s3://walbench/chaos-$RANDOM CHAOS_DURATION_S=600 \
  node harness/chaos/chaos.ts
```

Useful variations (environment variables are documented at the top of `chaos.ts`):

- `CHAOS_SPLIT_EVERY_S=60` — split a range and merge the children back every cycle, interleaved
  with kills, so a coordinator can die at any step of the protocol.
- `--failpoints` with a binary built with `--features failpoints` (`CHAOS_BINARY=…`) — arm random
  failpoints on live nodes; `CHAOS_FAILPOINT_MATCH='^(split|merge)::'` focuses on one subsystem.
- `--auto-split` — let the cluster split its own hot ranges.
- `--workload billing` — a payment-style workload with its own business invariants (no double
  charge, no missed invoice, ledger matches the payment gateway).
- `CHAOS_DISK_CACHE=<dir>` — run every node with the SST disk cache.

Two longer runs build on it: `scripts/mixed-binary.sh` runs a cluster that mixes the previous
and the current binary across a migration, and a long soak is the same harness run for many
hours (`CHAOS_DURATION_S=86400` for 24 hours) with every feature on (`--profile all`) and the
disk cache (`CHAOS_DISK_CACHE`) on a real store.

## 11. Mutation testing of safety checks

`scripts/mutants.py` breaks one safety check at a time — a fence, a version re-check, a proof
gate, a `verify` check — by a single targeted edit, and runs the tests that are supposed to catch
it. A mutant that survives means no test sees that check; it is a coverage hole to fix.

```sh
scripts/mutants.py --list
scripts/mutants.py            # all mutants; report in target/mutants/report.md
```

## 12. The Ready profile: `scripts/check.sh`

Two profiles are used day to day:

- **Work in progress:** `cargo test -p <crate>` plus the rows of the "change here, run this"
  table in [`AGENTS.md`](../AGENTS.md) section 2 for the area being changed.
- **Ready:** `scripts/check.sh` — the gate before anything is called done.

`scripts/check.sh` runs, in order: the console checks (`npm ci && npm run check` in `ui/`, before
any cargo step because the binary embeds the console), `cargo fmt --check`, the source lints
(`scripts/lint-sources.sh`), clippy with `-D warnings` in three feature configurations,
`cargo test --workspace`, the ambiguous-result rows (`each-once`, `each-once+nodewal`), the
failpoint row, the `testkit` row (network harness, simulated S3 profiles), a real-S3 row when a
local SeaweedFS answers, the billing workload with self-kills on a local directory, the release
build, the TypeScript type check, a smoke test of the packed npm tarball, the SDK unit tests, and
the end-to-end matrix {one node, four-range cluster, four-range cluster with continuous
split/merge, with the SST disk cache} × {`BIDI_STREAM`, `REQUEST_RESPONSE`}, then the console's
Playwright suites.

```sh
scripts/check.sh               # everything
scripts/check.sh --quick       # Rust only: fmt, clippy, tests, build
scripts/check.sh --no-cluster  # skip the cluster end-to-end rows
```

With [`cargo-nextest`](https://nexte.st) installed the Rust rows run through it (one process per
test, every binary in parallel) and a warm Ready run takes about 5 minutes; without it the script
falls back to `cargo test` (about 15 minutes warm) and says so on its first line. Logs are written
to `target/check/<step>.log`, with the list of tests that actually ran in
`target/check/<step>.tests` so two runs can be compared with `diff`.

Tests must pass under both runners: nextest runs each test in its own process, while `cargo test`
shares a process between the tests of one binary, and nextest does not run doctests.
