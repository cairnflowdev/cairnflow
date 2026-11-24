# Cairnflow

**A durable agent runtime on object storage. No disks.**

Cairnflow runs long-lived, failure-proof code — AI agents, workflows, sagas, scheduled jobs — and
keeps every step of it in an S3-compatible bucket. Nodes are stateless processes: kill one with
`kill -9`, start another anywhere with the same bucket URI, and every invocation continues from
its last recorded step. There is no database to run, no consensus cluster, and no local volume to
back up.

> **Status: beta.** The on-bucket format is frozen and upgrades migrate data in place, but APIs
> may still change between minor versions. There are three SDKs: TypeScript, Python and Go. Read
> [`docs/self-hosting-security.md`](docs/self-hosting-security.md) before exposing a cluster to
> tenants you do not trust.

Cairnflow is **source-available**, not open source: the engine and console are under the
Business Source License 1.1, the SDK and protocol under permissive licenses. See
[License](#license).

```
   your code (an SDK, any HTTP host)                    clients / your app
              ▲  turns: replay journal, run next step          │
              │                                                ▼
   ┌──────────┴──────────────────────────────────────────────────────┐
   │ cairnflow nodes — stateless; each range of keys has one leader  │
   │   engine per range · per-node shared WAL · leases by CAS        │
   └──────────────────────────────┬──────────────────────────────────┘
                                  │ conditional PUT / GET / LIST
                                  ▼
             S3-compatible object store — the only source of truth
```

## What you get

- **Durable handlers.** `ctx.run`, `ctx.sleep`, `ctx.call`, signals, promises and timers are
  journaled; a crash or redeploy resumes at the last recorded step instead of starting over.
- **Virtual objects and workflows.** Per-key state with exclusive and shared handlers, and
  run-once workflows with durable promises.
- **Agent primitives.** Streams that survive restarts, artifacts, large state values stored as
  segments, `$log`/`$trace` captured with the journal, and an AI SDK integration
  ([`docs/ai-sdk.md`](docs/ai-sdk.md)).
- **Flow control.** Concurrency limits per scope or key, per-tenant rate limits and turn caps,
  quarantine of poison handlers, server-side schedules.
- **Multi-tenant.** Tenants, API keys with roles, OIDC sign-in for the web console, audit export,
  per-tenant limits and purge.
- **Elastic ranges.** Partition-key ranges split and merge online, by command or automatically,
  without copying data.
- **Operable.** A web console, read-only SQL over invocations (`POST /query`), Prometheus metrics
  and runbooks, `cairn-ctl verify`/backup/restore/fork, and a trash that makes deletes undoable.

## Quickstart (about five minutes)

You need: Rust (the toolchain in `rust-toolchain.toml` installs itself through `rustup`),
`protoc`, CMake and a C++ compiler, and Node.js 22.18 or newer (or 22.6 with
`--experimental-strip-types`).

**1. Build.**

```sh
git clone https://github.com/cairnflowdev/cairnflow.git
cd cairnflow
git submodule update --init      # the SlateDB fork in vendor/slatedb — the build needs it
cargo build --release            # binaries: cairnflow (server), cairn (CLI), cairn-ctl (operator tool)
```

The web console is embedded at build time. To include it, build it first
(`cd ui && npm ci && npm run build`), then run `cargo build --release`; otherwise `/ui/` shows a
placeholder page.

**2. Run a node on a local directory.**

```sh
export CAIRNFLOW_ADMIN_KEY="cf_$(openssl rand -hex 24)"   # bootstrap key, at least 32 bytes
export CAIRNFLOW_TOKEN="$CAIRNFLOW_ADMIN_KEY"              # the key the `cairn` CLI sends
./target/release/cairnflow --data-dir ./cairnflow-data --port 8080
```

The node listens on `127.0.0.1:8080` (and `8081` for other nodes). All state — journals, timers,
queues — lives under `./cairnflow-data`. For production you point it at a bucket instead:
`--main-store s3://my-bucket/prefix` with the usual `AWS_*` environment variables.

**3. Run the example service** (in a second terminal):

```sh
cd sdk/typescript
node example/order-service.ts    # listens on :9080
```

It is a virtual object whose `submit` handler reads state, sleeps durably, runs a side effect once
and writes state back:

```ts
const orders = service("OrderService", {
  async submit(ctx: Context, input: { amount: number }) {
    const soFar = (await ctx.get<number>("total")) ?? 0;
    await ctx.sleep(1_000, "wait for confirmation");                  // durable timer
    const receipt = await ctx.run("charge", () => chargeCard(input.amount)); // recorded once
    ctx.set("total", soFar + input.amount);
    return { receipt, total: soFar + input.amount, order: ctx.key };
  },
}, { type: "VIRTUAL_OBJECT" });

serve({ services: [orders], port: 9080 });
```

**4. Register it and call it** (in a third terminal, with `CAIRNFLOW_TOKEN` exported as above):

```sh
./target/release/cairn register http://127.0.0.1:9080
./target/release/cairn invoke OrderService submit --key order-42 --input '{"amount": 100}'
./target/release/cairn status <id>        # if the call returned an id before finishing
./target/release/cairn journal <id>       # every recorded step
```

**5. Kill it.** Start a call without waiting for it (`/send` answers as soon as the invocation
is durable), kill the node during the one-second sleep, and start it again:

```sh
curl -s -X POST 'http://127.0.0.1:8080/invoke/OrderService/submit/send?key=order-43' \
  -H "authorization: Bearer $CAIRNFLOW_TOKEN" -H 'content-type: application/json' \
  -d '{"amount": 7}' && pkill -9 cairnflow     # prints JSON with the invocation id
./target/release/cairnflow --data-dir ./cairnflow-data --port 8080   # in the node's terminal
./target/release/cairn status <id>        # completed; the charge was recorded once
```

There is no recovery step to perform: the journal is in the store, and the process only reads it.

From application code, use the typed client (`import { connect } from "@cairnflow/sdk/client"`).
More: [`docs/quickstart.md`](docs/quickstart.md), [`sdk/typescript/README.md`](sdk/typescript/README.md).

## How it works, in one paragraph

The partition-key space is cut into ranges; each range has one leader node, one SlateDB database
in the bucket and one single-threaded engine. Leadership is a CAS on an owner record plus
SlateDB's `writer_epoch` fence — no Raft, no etcd. Every node multiplexes the write-ahead log of
all its ranges into a few log streams in the bucket, and nothing is acknowledged to anyone until
the WAL object holding it is durably PUT. Ranges split and merge by cloning SlateDB databases with
key projection, under an intent protocol with exactly one winning CAS. Deletes of customer data
need a typed proof built from fresh reads and go through a trash first. The full design, with the
invariants and the tests that prove them: [`docs/architecture.md`](docs/architecture.md) and
[`docs/design/decisions.md`](docs/design/decisions.md). How it is tested — protocol models,
deterministic simulation, failpoints, injected ambiguous store results, chaos on a real store:
[`docs/testing.md`](docs/testing.md).

## How it compares

All four systems give you durable execution: code that survives crashes by recording its progress.
They differ in where that record lives and what you have to operate.

| | Cairnflow | Temporal | Restate | DBOS |
|---|---|---|---|---|
| Where state lives | Object storage only (S3-compatible) | A database you run (Cassandra, PostgreSQL or MySQL; optional Elasticsearch for visibility) | Its own replicated log and RocksDB partitions on local disks, with snapshots to object storage | PostgreSQL |
| What you operate | Stateless `cairnflow` processes and a bucket | Temporal services plus the database | Restate server nodes with persistent volumes | Your application plus PostgreSQL |
| How code runs | The engine calls your service over HTTP (push), replaying the journal | Workers poll task queues; workflow code is replayed deterministically | The server calls your service over HTTP (push), replaying the journal | A library inside your process checkpoints steps to PostgreSQL |
| Commit latency is bounded by | One object-store PUT (tens of milliseconds on S3 Standard; lower on low-latency object stores) | Database write | Replicated log append on local disks (low milliseconds) | PostgreSQL commit |
| Tenants | Built in (tenants, per-tenant keys, limits, audit, purge) | Namespaces | No tenant concept in the server | Not provided by the library |
| SDKs | TypeScript | Many languages | Many languages | Several languages |
| Maturity | Beta | Mature, widely deployed | Production releases | Production releases |

When cairnflow fits: you want durable agents and workflows without operating a stateful
cluster, your traffic is bursty or idle much of the time, and tens of milliseconds per committed
step is acceptable. When it may not: you need single-digit-millisecond commits on every step, an
SDK in another language today, or a long production track record.

Cairnflow's programming model (services, virtual objects, workflows, journal replay over HTTP) is
close to Restate's, and its wire protocol definition is derived from Restate's MIT-licensed
service protocol. The two are not interchangeable: SDKs and servers of one do not work with the
other.

## Documentation

| Topic | Document |
|---|---|
| Getting started | [`docs/quickstart.md`](docs/quickstart.md) |
| Running a cluster: Docker, S3, flags, keys | [`docs/self-hosting.md`](docs/self-hosting.md), [`docs/self-hosting-security.md`](docs/self-hosting-security.md) |
| HTTP API | [`docs/admin-api.md`](docs/admin-api.md), [`docs/api/`](docs/api/) |
| Choosing keys and placement | [`docs/key-design.md`](docs/key-design.md) |
| TypeScript SDK, AI SDK integration | [`sdk/typescript/README.md`](sdk/typescript/README.md), [`docs/ai-sdk.md`](docs/ai-sdk.md) |
| Python SDK | [`sdk/python/README.md`](sdk/python/README.md) |
| Go SDK | [`sdk/go/README.md`](sdk/go/README.md) |
| Wire protocol and conformance | [`docs/protocol-v1.md`](docs/protocol-v1.md), [`conformance/README.md`](conformance/README.md) |
| Architecture and invariants | [`docs/architecture.md`](docs/architecture.md), [`docs/design/decisions.md`](docs/design/decisions.md), [`docs/reliability/irreversible.md`](docs/reliability/irreversible.md) |
| Subsystem guides | [`docs/guides/`](docs/guides/) |
| Testing | [`docs/testing.md`](docs/testing.md) |
| Operations runbooks | [`docs/runbooks/`](docs/runbooks/) |

## Contributing

Issues, bug reports and design feedback are welcome. Code contributions from outside the project
are not open yet; see [`CONTRIBUTING.md`](CONTRIBUTING.md). [`AGENTS.md`](AGENTS.md) lists, for
each part of the code, the tests a change must run. Report security issues privately as described in
[`SECURITY.md`](SECURITY.md).

## License

Cairnflow is **source-available**. You may read, modify and self-host it, including inside
commercial products; you may not offer it to third parties as a competing hosted service. The
exact terms are in each license file.

| Path | License |
|---|---|
| Everything not listed below: the engine (`crates/`), the web console (`ui/`), `deploy/`, `scripts/`, `bench/`, `harness/` | Business Source License 1.1 (`BUSL-1.1`) with an Additional Use Grant; each version converts to Apache-2.0 four years after its release — [`LICENSE`](LICENSE) |
| `sdk/typescript/` | Apache-2.0 — [`sdk/typescript/LICENSE`](sdk/typescript/LICENSE) |
| `sdk/python/` | Apache-2.0 — [`sdk/python/LICENSE`](sdk/python/LICENSE) |
| `sdk/go/` | Apache-2.0 — [`sdk/go/LICENSE`](sdk/go/LICENSE) |
| `conformance/` | Apache-2.0 — [`conformance/LICENSE`](conformance/LICENSE) |
| `crates/cairn-protocol/proto/protocol.proto` | MIT, derived from Restate's service protocol — see the file header and [`NOTICE`](NOTICE) |
| `vendor/slatedb` (git submodule, fork of SlateDB) | Apache-2.0, as upstream |

Copyright © 2026 Phuong Pham and the Cairnflow contributors. Third-party notices: [`NOTICE`](NOTICE).
