# Cairnflow — Quickstart

Ten minutes from zero to a durable workflow that runs, dies, and comes back to life.

**You need:** Rust (stable), `protoc` (protobuf-compiler), Node.js ≥ 22.6 (or Python ≥ 3.11, or Go ≥
1.25, to write the service in those languages). No database, no dedicated disk — a dev node pointed at a local directory is enough. Running with Docker, on S3 or as a cluster:
[`self-hosting.md`](./self-hosting.md).

---

## 1. Build

```sh
git submodule update --init   # the SlateDB fork in vendor/slatedb — missing from a fresh clone
cargo build --release
# Two binaries: target/release/cairnflow (runtime) and target/release/cairn (CLI)
```

## 2. Run the runtime

```sh
export CAIRNFLOW_ADMIN_KEY="cf_$(openssl rand -hex 24)"   # the runtime's bootstrap key
export CAIRNFLOW_TOKEN="$CAIRNFLOW_ADMIN_KEY"              # the key the `cairn` CLI sends
cairnflow --data-dir ./cairnflow-data --port 8080
```

Auth is on by default: every route except `/health` requires `Authorization: Bearer <key>`, and
without any key the runtime refuses to start. `CAIRNFLOW_ADMIN_KEY` is a cluster-admin key stored
nowhere; keys for your services are created with `cairn keys create default invoke` (see
[`self-hosting.md`](./self-hosting.md) §8). For a quick try on your own machine, use `--no-auth`. The
runtime listens on two ports: `8080` for you, `8081` for other nodes.

That is it for "infrastructure". All state — journals, timers, queues — lives in `--data-dir`. For
production, swap it for a bucket:

```sh
cairnflow --main-store s3://my-bucket/prod --wal-store s3://my-wal-bucket --port 8080
```

S3 credentials and endpoint come from the `AWS_*` environment variables — see
[`self-hosting.md`](./self-hosting.md) §3.

Logs go to stderr, at level `info` by default (tune with `RUST_LOG`). For a log collector, set
`CAIRNFLOW_LOG=json` to get line-delimited JSON.

**One default to know before writing handlers:** an SDK turn must not stay silent for more than
**60 seconds** (`--inactivity-timeout-ms`), otherwise it is dropped and rescheduled. `ctx.sleep` and
`ctx.call` do *not* count — they end the turn instead of holding the connection; only the closure of a
`ctx.run` is truly silent. So the practical rule is: a `ctx.run` that runs longer than a minute either
raises the cap, or becomes a separate invocation through `ctx.send`.

## 3. Write a service

A service is an HTTP process of yours, written with the TypeScript SDK. The full example is in
`sdk/typescript/example/order-service.ts`:

```ts
import { type Context, serve, service } from "@cairnflow/sdk";

const orders = service("OrderService", {
  async submit(ctx: Context, input: { amount: number }) {
    const soFar = (await ctx.get<number>("total")) ?? 0;

    await ctx.sleep(1_000, "await confirmation");  // durable: wakes up in the right place after a crash

    const receipt = await ctx.run("charge", () =>  // the side effect runs exactly once
      chargeCard(input.amount)
    );

    await ctx.set("total", soFar + input.amount);
    return { receipt, total: soFar + input.amount };
  },
}, { type: "VIRTUAL_OBJECT" });                     // state needs an object — call with a key

serve({ services: [orders], port: 9080 });
```

State and promises only exist on **virtual objects** and **workflows** — they belong to the object
with that key. A handler of a plain service (`service(name, handlers)` with no `type`) gets a
`ServiceContext`, without `ctx.get`/`set`/`promise`; the runtime refuses them too (terminal error 574)
because a service with no key and no scope has no object for the state to belong to.

Run it:

```sh
node example/order-service.ts     # the SDK listens on :9080
```

**In Python** — the same service with the Python SDK (`sdk/python`, package `cairnflow`, Python ≥
3.11, no runtime dependencies; `BIDI_STREAM` needs the `cairnflow[bidi]` extra). The full example
is in `sdk/python/example/order_service.py`:

```python
import asyncio
from cairnflow import Context, VirtualObject, serve

orders = VirtualObject("OrderService")

@orders.handler()
async def submit(ctx: Context, order: dict) -> dict:
    so_far = (await ctx.get("total")) or 0
    await ctx.sleep(1)                                        # durable timer
    receipt = await ctx.run("charge", charge_card, order["amount"])   # runs exactly once
    ctx.set("total", so_far + order["amount"])
    return {"receipt": receipt, "total": so_far + order["amount"]}

asyncio.run(serve([orders], port=9080))
```

Handler code runs on a deterministic event loop that refuses sockets: do I/O inside `ctx.run`.
`cairnflow.app(services)` is an ASGI application (uvicorn, hypercorn) and
`cairnflow.aws_lambda.lambda_handler(services)` serves AWS Lambda; see
[`sdk/python/README.md`](../sdk/python/README.md).

**In Go** — the same service with the Go SDK (`sdk/go`, module
`github.com/cairnflowdev/cairnflow/sdk/go`, Go ≥ 1.25, no dependencies outside the standard library).
The full example is in `sdk/go/example/orderservice/main.go`:

```go
func submit(ctx cairn.ObjectContext, order Order) (Receipt, error) {
	soFar, _, err := cairn.Get[int](ctx, "total")
	if err != nil {
		return Receipt{}, err
	}
	if err := cairn.Sleep(ctx, time.Second); err != nil { // durable timer
		return Receipt{}, err
	}
	receipt, err := cairn.Run(ctx, "charge", func(rc cairn.RunContext) (string, error) {
		return chargeCard(rc, order.Amount) // runs exactly once; rc is a context.Context
	})
	if err != nil {
		return Receipt{}, err
	}
	if err := cairn.Set(ctx, "total", soFar+order.Amount); err != nil {
		return Receipt{}, err
	}
	return Receipt{Receipt: receipt, Total: soFar + order.Amount}, nil
}

var orders = cairn.NewObject("OrderService", cairn.Exclusive("submit", submit))

func main() {
	if err := cairn.Serve(context.Background(), "127.0.0.1:9080", orders); err != nil {
		log.Fatal(err)
	}
}
```

```sh
go run ./example/orderservice     # from sdk/go; the SDK listens on 127.0.0.1:9080
```

Every durable step is a function that takes the context, and every step that can fail returns an
`error`. Handler code stays on its one goroutine (parallel work is futures plus `cairn.WaitFirst`,
`cairn.All`…) and does I/O only inside `cairn.Run`. `cairn.Bidi()` serves `BIDI_STREAM` on the same
port, `cairn.HTTPHandler` mounts the services in a server of your own and `cairn.LambdaHandler`
serves AWS Lambda; see [`sdk/go/README.md`](../sdk/go/README.md).

## 4. Register and call

```sh
cairn register http://127.0.0.1:9080
cairn services
# OrderService  [virtualObject]  @ http://127.0.0.1:9080
#   handlers: submit (exclusive), total (shared)

cairn invoke OrderService submit --key order-42 --input '{"amount": 100}'
# waiting    00004581...0001        ← parked at ctx.sleep, as it should be
```

`POST /invoke` **waits** for the invocation to stop (at most `--invoke-wait-ms`, default 5 seconds)
before answering, so most calls get the result directly — even when it goes through `ctx.call` to
another partition. Past the cap it answers `202` with the id; ask again with `cairn status <id>`. To
fire and forget, append `/send` to the path: it answers as soon as the invocation is durable, without
waiting for it to run.

`cairn` talks to `http://127.0.0.1:8080` by default; change it with `--server` or the env variable
`CAIRNFLOW_SERVER`. Every CLI command is just one HTTP request — add `--json` to see it verbatim (see
[`admin-api.md`](./admin-api.md)).

**From your code** — a typed client, subpath `@cairnflow/sdk/client` (Node ≥ 22.6 as in the package's
`engines`, browsers, edge; no `@types/node` needed). Input/output types come from the service
definition through `import type` — calling the wrong handler or with the wrong input is a `tsc`
error:

```ts
import { connect } from "@cairnflow/sdk/client";
import type { orders } from "./order-service.ts";      // types only, no service code runs

const c = connect({ url: "http://127.0.0.1:8080", apiKey: process.env.CAIRNFLOW_INVOKE_KEY });
const order = c.object<typeof orders>("OrderService", "order-42");

const r = await order.submit({ amount: 100 });         // waits until done — the 202 never shows
const h = await order.send.submit({ amount: 5 });      // send and go: the handle has an id
await h.attach();                                      // wait by id, long-polling 25 s at a time
```

In Python, `cairnflow.connect` is the same client:

```python
import os
from cairnflow import connect

c = connect("http://127.0.0.1:8080", api_key=os.environ["CAIRNFLOW_INVOKE_KEY"])
order = c.object("OrderService", "order-42")
r = await order.call("submit", {"amount": 100})
h = await order.send("submit", {"amount": 5})
await h.attach()
```

In Go, the `client` package of the SDK:

```go
c := client.Connect("http://127.0.0.1:8080", client.WithAPIKey(os.Getenv("CAIRNFLOW_INVOKE_KEY")))
order := c.Object("OrderService", "order-42")
r, err := client.Call[Receipt](ctx, order, "submit", Order{Amount: 100})
h, err := client.Send(ctx, order, "submit", Order{Amount: 5})
_, err = client.Attach[Receipt](ctx, h)
```

The client generates an `Idempotency-Key` for each call and keeps it across every attempt, so
resending after a network error or a `503` does not create a second invocation; it follows the
`leader` of a `503` and waits for the `Retry-After` of a `429`. A handler's `TerminalError` reaches the
caller as `InvocationFailed` (still a `TerminalError`, with the same `code`; in Go, `errors.As` finds
both a `*client.InvocationFailed` and a `*cairn.TerminalError`). An `invoke` key is enough
for everything in this guide (`cairn keys create default invoke`).

One second later (when the timer is due, the background loop ticks by itself):

```sh
cairn status 00004581...0001
# completed  00004581...0001
# {"receipt":"receipt-100","total":100,"order":"order-42"}
```

## 5. Look inside

```sh
cairn invocations                       # who is running, who is parked, who is done
cairn invocations --status suspended    # filter by status
cairn journal 00004581...0001           # every step of one invocation
#   0  Input
#   1  GetState
#   2  GetStateCompletion
#   3  Sleep
#   ...
cairn cancel <id>                       # cancel a parked invocation
cairn cluster                           # who holds which partition (cluster)
cairn ranges                            # the range table: version, key span, gen, intent
cairn ranges split 0                    # split range 0 in the middle of its key span (needs a store with CAS);
                                        # --json also prints `report`: the write pause, each phase
cairn ranges merge 1 2                  # merge two adjacent ranges back
```

Prometheus metrics are at `GET /metrics`.

## 6. Try the thing that matters most: kill it

```sh
cairn invoke OrderService submit --key order-43 --input '{"amount": 7}'
# waiting — asleep in the middle of the workflow

pkill -9 cairnflow                      # dies without last words
cairnflow --data-dir ./cairnflow-data --port 8080   # back up
sleep 1 && cairn status <id>
# completed — continued exactly where it stopped, the side effect did not run again
```

There is no "recovery" step to perform. The journal lives in the store; the process is only its
reader.

## 7. Going to a cluster

Several nodes, one shared bucket, no files to move around:

```sh
cairnflow --main-store s3://my-bucket/prod --partitions 8 --port 8080  # node 1 (internal 8081)
cairnflow --main-store s3://my-bucket/prod --partitions 8 --port 8090  # node 2 (internal 8091), ...
```

Every node uses the same `CAIRNFLOW_ADMIN_KEY` (or keys created with `cairn keys create`). Messages
between nodes go through the internal port and are signed with the cluster secret — the first node
creates it in the bucket, later nodes read it back; nothing to configure.

(Two nodes on one machine as above: the default `--host` `127.0.0.1` is enough. Nodes on different
machines: `--host` is an address peers can reach, **not** `0.0.0.0` — a node advertises exactly the
address it binds; see [`self-hosting.md`](./self-hosting.md) §4.)

- The store **must** pass two checks at startup: conditional PUT (D12) and CAS on etags. SeaweedFS
  passes and is the store the chaos tests run on; MinIO passes the checks but tears lease objects when
  a client is `kill -9`ed in the middle of a CAS — not recommended for a cluster; `file://` only runs a
  single node.
- The number of partitions is written into the bucket on first initialisation; **every node passes the
  same number** — while the table has never been split or merged, a node configured with a different
  number refuses to start. Change it at runtime with `cairn ranges split|merge` (section 5), but still
  over-provision from the start (8–64 is reasonable for a small cluster).
- On a cluster, **the call's key decides the range**: the scope, then the object key, then the
  idempotency key; a call between two ranges costs two durable writes and a round trip. A workflow /
  agent tree with no key and no scope is spread over every range — set a scope on the root call to
  keep it together. See [`key-design.md`](./key-design.md).
- Partitions **balance themselves** across nodes: a node that joins later gets its share
  (⌈partitions/nodes⌉), a dead node's share is taken over within a few TTLs. There is no rebalance
  command to run — `cairn cluster` shows who holds what.
- Hit the wrong node? The server answers `503` with a `leader` field — `cairn` and `connect()` follow it
  by themselves; a hand-written client reads the body and resends there.
- The cluster uses a **per-node shared WAL** (the only WAL): each node writes the WAL of every
  partition it holds into a few `nodewal/<node>/…` logs, so the number of WAL PUTs follows the number
  of nodes, not the number of partitions. The per-partition WAL (SlateDB's native WAL) is gone — old
  buckets are migrated when opened. Knobs: `--wal-stripes`, `--wal-inflight`, `--wal-window-ms`,
  `--wal-object-cap-bytes` (see `cairnflow --help`).

---

## 8. When a human has to step in

There are three ways to end a failed turn, and picking the wrong one fails in three different ways:

```ts
throw new Error("network glitch");      // retried forever, with backoff
throw new TerminalError("bad order");   // dies for good, the waiter gets the error
throw new PauseError("out of stock");   // stops, KEEPS the journal, waits for a human
```

`PauseError` moves the invocation to `paused`; once fixed, `cairn resume <id>` and it continues
**exactly where it stopped**. One rule to remember: throw it from **inside** the closure of a
`ctx.run`, where the world is read. Thrown outside, the value that was read is already in the journal,
and `resume` replays exactly that old value — parked again forever even though you fixed it.

An outside system wakes a workflow up with one request, no intermediate handler needed:

```sh
cairn signal <id> approve --value '{"by":"boss"}'           # ctx.signal / ctx.awaitSignal
cairn resolve OrderService order-42 approve --value '"ok"'  # ctx.promise().get()
cairn promise OrderService order-42 approve                 # does it have a result yet
```

The same from code, with an `invoke` key:

```ts
await c.invocation(id).signal("approve", { by: "boss" });
await c.promise("OrderService", "order-42", "approve").resolve("ok");
await c.awakeable(awkId).resolve({ ok: true });          // the id from ctx.awakeable()
await c.invocation(id).cancel();                         // cooperative cancel: the handler's finally still runs
```

For an approval wait **with a deadline**, put the signal and a timer in the same `race`:

```ts
const ok = await ctx.race([ctx.signal<{ by: string }>("approve"), ctx.sleep(24 * 3600_000)]);
if (ok === undefined) throw new TerminalError("approval expired", 408);
```

---

Read next: [`self-hosting.md`](./self-hosting.md) (Docker, S3, clusters, flags and defaults) ·
[`admin-api.md`](./admin-api.md) (endpoint reference) ·
[`architecture.md`](./architecture.md) (how it works) ·
[`protocol-v1.md`](./protocol-v1.md) (the wire spec for SDK authors).
