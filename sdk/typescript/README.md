# @cairnflow/sdk

The TypeScript SDK for Cairnflow. It implements [`docs/protocol-v1.md`](../../docs/protocol-v1.md)
on the side of whoever writes the code.

**No dependencies.** No protobufjs, no HTTP framework, no build step. The protocol surface is
fixed and small (varint, length-delimited, 15 messages), so writing it ourselves is cheaper than
carrying a dependency tree — and an SDK that never breaks because of someone else's lockfile is
worth the trade.

Needs Node ≥ 22.6 (runs `.ts` directly via type stripping).


## Client (`@cairnflow/sdk/client`)

Call services from an app, a script or a browser — no `node:*` import. Types come from `typeof`
of the service definitions (`import type`):

```ts
import { connect, InvocationFailed } from "@cairnflow/sdk/client";
import type { orders, signup } from "./services.ts";

const c = connect({ url: "https://cairn.example.com", apiKey: process.env.CAIRN_KEY! });
await c.service<typeof orders>("Orders").submit({ amount: 5 });       // waits for the result
const h = await c.service<typeof orders>("Orders").send.submit(input); // fire and forget
await h.attach({ timeoutMs: 60_000 });                                 // or h.output(), h.status()
await h.signal("approve", { ok: true });  await h.cancel();            // cooperative cancel
const wf = c.workflow<typeof signup>("Signup", "u1");  await wf.run(user);  await wf.attach();
await c.awakeable(awkId).resolve(v);  await c.promise("Signup", "u1", "email").resolve(v);
```

- A `202` never surfaces: the client attaches by id (`POST /invocations/{id}/attach?wait_ms=25000`)
  until done or until `timeoutMs` runs out (`AttachTimeout`, the invocation keeps running).
- Every call carries a generated `Idempotency-Key`, kept across every attempt (`idempotencyKey: null`
  turns it off; a workflow's `run` does not need one). It follows the `leader` of a `503` with the
  same scheme, waits for the `Retry-After` of a `429` that created nothing, and caps attempts and
  time (`transport.ts`).
- Errors: `InvocationFailed` (a `TerminalError`), `InvocationPaused`, `AlreadyInvoked`,
  `CairnHttpError`.
- There is no `send(…, { delay })` from the client: use `ctx.send(…, { delay })` in a handler.
- `apiKey` is an optional `string`: under `exactOptionalPropertyTypes` you cannot pass
  `string | undefined` directly. For a required environment variable use `process.env.CAIRN_KEY!`
  (as above); for an optional one (a `--no-auth` node in dev) use
  `...(process.env.CAIRN_KEY ? { apiKey: process.env.CAIRN_KEY } : {})`.

### Server-side schedules

```ts
await c.schedules.put("nightly", {                     // call at app startup: same definition ⇒ no-op
  cron: "0 2 * * *", timeZone: "Europe/Berlin",       // or everyMs: 60_000 (≥ --schedule-min-period-ms)
  target: { service: "Report", handler: "build" },    // key: … for a virtual object (overlap "allow" only)
  input: { full: true },
});
const run = await c.schedules.trigger("nightly");     // fire now; same idempotencyKey ⇒ same run
await run.attach();
await c.schedules.pause("nightly");  await c.schedules.resume("nightly");  await c.schedules.delete("nightly");
```

In a handler, `ctx.schedule` is `{ name, at }` when the run was fired by a schedule (`at`: the
nominal time, ms), `undefined` otherwise. Missed times (a node dead across several of them) fire
**once** for the latest one, the rest go into `missed`. DST is taken literally: a time that does not
exist does not fire, a repeated time fires twice — use UTC or avoid 1:00–3:00 local time.

The published package is `dist/` built by `npm pack` (`prepack`); inside the repo the code runs
straight from `src/` with type stripping. `node scripts/smoke-pack.ts` installs the tarball outside
the repo and runs it on Node 22/24.

## Usage

```ts
import { type Context, serve, service, TerminalError } from "@cairnflow/sdk";

const orders = service("OrderService", {
  async submit(ctx: Context, input: { amount: number }) {
    if (typeof input?.amount !== "number") {
      throw new TerminalError("amount must be a number", 400);   // bad data ⇒ retrying is useless
    }
    const soFar = (await ctx.get<number>("total")) ?? 0;   // null = the key does not exist
    await ctx.sleep(1_000);                                 // durable: the timer lives in the store
    const receipt = await ctx.run("charge", () => charge(input.amount));
    ctx.set("total", soFar + input.amount);
    return { receipt };
  },
}, { type: "VIRTUAL_OBJECT", handlerTypes: { submit: "EXCLUSIVE" } });

await serve({ services: [orders], port: 9080 });
```

`serve()` listens on `127.0.0.1` by default — only processes on the same machine can call it. For a
runtime on another machine or container (Docker, Kubernetes, Fly…), set `hostname`:

```ts
await serve({ services: [orders], port: 9080, hostname: "0.0.0.0" });
```

A `ctx.run` closure that throws an ordinary error (not a `TerminalError`) ⇒ the result does not go
into the journal and **that turn dies**: later `ctx` commands throw `TurnFailed`, and the turn ends
with an error so the runtime retries it — even if the code catches the error and carries on. The
retry reruns the closure at the same position; carrying on past the error would let later steps rely
on a result the retry is going to replace. A `TerminalError` can be caught and carried on from
(compensation); to put a failed result into the journal, throw `RunFailure` from the closure.

**Cancellation and `ctx.run`.** A cooperative cancel (`POST /invocations/{id}/cancel?mode=signal`)
makes every wait point throw a 409 `TerminalError`. By default `ctx.run` is **not** a cancellation
point: a saga's compensation steps in `catch` are `ctx.run`, and they must be able to run after the
cancel. Expensive forward steps (model calls, tools) declare `{ cancellable: true }`: if the
invocation was cancelled and the result is not there yet, it throws 409 instead of running, and the
closure gets an `AbortSignal` that aborts when the cancel arrives midway (`BIDI_STREAM`; RR has no
channel into the middle of a turn):

```ts
const page = await ctx.run("fetch", (signal) => fetch(url, { signal }).then((r) => r.text()), {
  cancellable: true,
});
```

**The name of a `ctx.run` is part of the journal.** Replay matches both kind and name: renaming a
step between two deploys stops an invocation that is mid-way to that step with 570 (`paused`). An
invocation is bound to the deployment it started on (the URL registered at that time), not to the
service's new revision; `resume` calls that same URL again. So there are two ways out:

- bring the code **with the old name** back at the URL of the deployment the invocation is bound to
  (the `deployment` column of `GET /invocations?scope=cluster`; `GET /deployments/{id}` → `endpoint`),
  then `POST /invocations/{id}/resume`. Registering the new code at a different URL does not
  unblock it;
- or move the invocation to a deployment whose code matches the journal:
  `POST /invocations/{id}/migrate?deployment=<dp_…>` (a trial replay on the target first, a mismatch
  ⇒ `409` with a report; see "Upgrading code" below). `POST /invocations/{id}/restart?from=<n>`
  instead reruns it as a new invocation, keeping the first `n` entries.

New turns go with the new name. Do not put things that change with configuration (model id,
prompt version) in the name; put them in span attributes.

State and promises only exist on virtual objects and workflows. A plain service (no declared
`type`) gets a `ServiceContext` — no `ctx.get`/`set`/`clear`/`getKeys`/`promise`, blocked right at
`tsc`; the runtime also rejects them (terminal error 574): without a key and a scope, state belongs
to no object.

A `shared` handler of a virtual object (`handlerTypes: { x: "SHARED" }`) runs **without holding the
object's lock**, in parallel with the exclusive handlers of the same key, so it **only reads** state:
the runtime rejects its `ctx.set`/`clear`/`clearAll` with a terminal PROTOCOL_VIOLATION error (571,
`state writes are not allowed in a SHARED handler…`) — allowing it would make a read-modify-write on
both sides a lost update. Move the writes to an exclusive handler. `handlerTypes` does not change the
type of `ctx`: declare `ctx: SharedContext` for that handler so `tsc` catches it early. Invocations
created before the runtime had this rule run to the end under the old rule.

```sh
node example/order-service.ts                     # SDK on :9080
cargo run --bin cairnflow -- --data-dir ./data --no-auth   # runtime on :8080 (dev: no API key)

curl -X POST localhost:8080/deployments -d '{"uri":"http://127.0.0.1:9080"}'
curl -X POST 'localhost:8080/invoke/OrderService/submit?key=order-42' -d '{"amount":100}'
curl -X POST localhost:8080/admin/tick            # make ctx.sleep come due
curl localhost:8080/invocations/<id>
```

## Serverless: Vercel, Cloudflare Workers, AWS Lambda (`@cairnflow/sdk/fetch`)

`serve()` needs a long-lived process holding a port. On platforms that only accept a function
`(Request) => Promise<Response>`, use the Fetch adapter — the same core as `serve()`, the same
conformance traces:

```ts
import { fetchHandler, service, type ServiceContext } from "@cairnflow/sdk/fetch";

const orders = service("OrderService", {
  async submit(ctx: ServiceContext, input: { amount: number }) {
    const receipt = await ctx.run("charge", () => charge(input.amount));
    return { receipt };
  },
});

export const handler = fetchHandler({
  services: [orders],
  identityKeys: ["publickeyv1_…"], // the runtime's GET /identity (admin key), or the console
});
```

Register the deployment with the **base URL** where the handler is mounted; the runtime calls
`<url>/discover` and `<url>/invoke/{service}/{handler}`. The adapter routes on the path suffix, so it
can be mounted under any prefix (with `basePath: "/api/cairn"` it only accepts that exact prefix):

```sh
curl -X POST "$CELL/deployments" -H "authorization: Bearer $KEY" \
  -d '{"uri":"https://my-app.vercel.app/api/cairn"}'
```

| Platform | Mount | Notes |
|---|---|---|
| **Vercel** (Next.js App Router) | `app/api/cairn/[...path]/route.ts`: `export const GET = handler; export const POST = handler;` | **Node.js** runtime (the default for route handlers). Register the production domain or an aliased domain: Deployment Protection on preview deployments answers the runtime with a login page |
| **Cloudflare Workers** (untested) | `export default { fetch: handler };` | Turn on `compatibility_flags = ["nodejs_compat"]` in `wrangler.toml` — the turn loop uses `node:async_hooks` and `node:crypto`. Compressed responses set `encodeBody: "manual"`, but this has not run on real Workers: if turns fail with bodies ≥ 8 KiB, set `compression: false` |
| **AWS Lambda** (Function URL or API Gateway HTTP API, payload v2) | `export const handler = lambdaHandler(fetchHandler({…}));` | Bodies go base64 in both directions (protocol frames are binary). A Function URL with `AuthType: NONE` is fine — `identityKeys` is what keeps strangers out |
| Deno, Bun | `Deno.serve(handler)`, `Bun.serve({ fetch: handler })` | — |

Three differences from `serve()`:

- **`REQUEST_RESPONSE` only.** The Fetch API gives no full-duplex HTTP/2 stream from the runtime to
  the handler, so every wait point whose result is not yet at the SDK (`ctx.sleep`, waiting for a
  child call, a signal, a promise) ends the request, and the runtime resends the journal in the next
  request. It runs anywhere; it costs more bytes and latency than `BIDI_STREAM` for handlers with
  many steps.
  To reduce the resent bytes, the SDK declares `requestCompression: "gzip"` (the default): newer
  runtimes compress bodies from 8 KiB, and the SDK compresses responses when invited. Payloads that
  do not compress (images, random strings) gain nothing. Each frame (entry) is compressed on its own
  in both directions, so the bytes of one entry are never compressed together with another's (a
  defence against length-measuring attacks of the BREACH kind). Do not return secrets (tokens, API
  keys) as `ctx.run` results or state: keep them in environment variables on the endpoint side.
  Large values (long conversation histories, big tool results) do not have to fit in one frame:
  state, `ctx.run` results and outputs over ~256 KiB are cut by the runtime into immutable
  segments on object storage, the journal keeps only refs, and the tenant's `max-payload=` (1 MiB
  on a `--profile saas` cell) applies only to the inline part. One value is capped at the tenant's
  `max-value-bytes=` (`579 value_bytes_limit`), and ingress assembles an output up to 64 MiB
  (`413 value_too_large`); see `docs/ai-sdk.md` §6. Files and images an invocation produces go
  through `ctx.artifact`, which journals a reference. Turn compression off with `compression: false`
  (`serve()` too) when self-hosting; a `--profile saas` cell does not accept uncompressed
  `REQUEST_RESPONSE`.
  **Replay cache (wire v5):** `replayCache: true` makes the SDK keep in RAM the journal received for
  each running invocation (LRU by bytes, default 64 MiB; TTL 15 minutes) — the runtime only sends the
  new entries each turn, with a SHA-256 digest of the part the SDK already has; on a mismatch or a
  miss the endpoint answers `409` and the turn goes again in full right away, so turning it on is
  never wrong. The cache needs `identityKeys`: a delta names an invocation id, so an endpoint that does
  not verify the runtime keeps no cache, and `replayCache: true` without `identityKeys` is a
  configuration error. With `identityKeys`, `serve()` turns it on by default (`replayCache: false` to
  turn it off); `fetchHandler` turns it off by default — only turn it on for long-lived runtimes
  (Bun/Deno server), not for Lambda/Workers/Vercel Edge (the process does not live across turns). Several replicas behind a
  load balancer only hit the cache with affinity: the runtime sends back the cookie that the previous
  response of **that same** invocation set (sticky cookies of ALB, Heroku, GCP…), and on Fly the SDK
  answers `cairn-affinity: fly=$FLY_MACHINE_ID` itself so the runtime sends `fly-force-instance-id`.
  Without affinity a turn that lands on an older replica misses (`409` with what that replica still
  holds) and the runtime resends from there — one more HTTP round trip, still cheaper than a full
  replay; the runtime only turns deltas off by itself when they stop saving bytes (≥ 95 % of the full
  bytes over 20 turns).
- **A turn must finish within the function's time cap** (Vercel's `maxDuration`, the Workers CPU
  limit, the Lambda timeout). A `ctx.run` longer than the cap gets the function killed mid-turn and
  the runtime retries from the start of that step — split long work into several short `ctx.run`s.
  The handler's `inactivityTimeout` should be below the platform's cap.
- **Turn on `identityKeys`.** The function's URL is public; without a signature anyone who knows the
  URL can call the handlers. A SaaS cell (`--profile saas`) refuses to register an endpoint that does
  not declare verification (`400 identity_required`) and only accepts `https://`. The JWT covers both
  the body (`bsh`) and the host (`host`): a captured JWT cannot carry a forged body. To bind the JWT to
  this exact endpoint, set `identityHosts` to the registered public hosts — when unset, `host` is only
  compared with the request's own `Host` header (written by the sender), and a request carrying no
  host is refused. A proxy between the runtime and the function that rewrites the `Host` header ⇒
  `identityHosts` is required. Segmented values (wire v6): `segmentOrigins` limits the URL origins the
  SDK may fetch segments from. An endpoint with neither `identityKeys` nor `segmentOrigins` does not
  serve wire v6 (it declares `maxProtocolVersion` 5, and pinning 6 there is a configuration error):
  the runtime then sends large values inline. The SDK always only accepts `http(s)`, stops reading at
  exactly the ref's length (≤ 16 MiB), does not follow redirects, refuses an assembled value larger
  than `maxValueBytes` (default 64 MiB, like the tenant's default `max-value-bytes`), a value with more
  than `maxValueBytes / 64 KiB` refs and a turn assembling more than `maxTurnValueBytes` (default
  256 MiB) before fetching, keeps at most 32 segment fetches in flight per process, reports every
  failed read with one text (no status or length), and refuses a request body larger than 256 MiB
  before reading it. `ctx.artifact.get` checks a download against the ref's sha256 and size, follows
  no redirect and gives up after 60 s.

`@cairnflow/sdk/fetch` does not pull in `serve()` (no `node:http`, `node:http2`, `node:net`); it
re-exports `service`, `workflow`, the `Context` types and `TerminalError` so a function only needs to
import from one place.

Tested in the repo (`test/fetch.test.ts`, `test/conformance.test.ts`): the adapter on Node's Fetch
API behind a `node:http` bridge and hand-built Lambda payload v2 events, against the real
`cairnflow` binary. **Not yet** run on real Vercel, Workers, Lambda, Deno or Bun.

## Workflow

A multi-step process, **one run per workflow id**:

```ts
import { type Context, serve, workflow } from "@cairnflow/sdk";

const signup = workflow("UserSignup", {
  async run(ctx: Context, user: { email: string }) {
    ctx.set("email", user.email);                          // only `run` can write state
    const secret = await ctx.run("secret", () => uuid());
    await ctx.run("mail", () => sendVerification(user, secret));
    const clicked = await ctx.promise<string>("email-link-clicked").get();
    return clicked === secret;
  },
  // A SHARED handler: runs **in parallel** with `run`, even while `run` is suspended.
  async click(ctx: Context, req: { secret: string }) {
    await ctx.promise<string>("email-link-clicked").resolve(req.secret);
  },
});
```

```sh
curl -X POST 'localhost:8080/invoke/UserSignup/run/send?key=u-1' -d '{"email":"a@b.c"}'
curl -X POST 'localhost:8080/invoke/UserSignup/click?key=u-1'   -d '{"secret":"..."}'
curl -X POST localhost:8080/workflow/UserSignup/u-1/attach       # the result, no id needed
```

`ctx.key` in `run` **is** the workflow id. Three differences from `service(...)`:

- `run` is **required and unique** — the runtime refuses to register a workflow that does not
  declare exactly one `WORKFLOW` handler.
- Calling `run` a second time for the same key gets `409`, **not** the result of the first run (the
  second call's input is discarded, so returning the old result would be a lie).
- `SHARED` handlers **cannot write state** — they run in parallel with `run`, so a read-modify-write
  on both sides is a lost update. The `SharedContext` type blocks it at `tsc`, and the runtime blocks
  it again with PROTOCOL_VIOLATION.

## The scope decides the range

On a cluster, each invocation lives in one **range**, and the range is decided by the call's keys:
the **scope** beats everything, then the object's `key`, then `idempotencyKey`; with none of them
the callee lives **next to the caller** (inheriting the pk prefix — the whole unkeyed call tree is
colocated on the root's range), unless the call declares `placement: "spread"`. A call within the
same range is local; a call to another range goes outbox → HTTP and costs two durable writes (p50
~160 ms round trip on `memory://`, slower on S3).

```ts
// The root of the tree sets the scope once — everything below inherits it.
const agent = ctx.scoped(`user-${userId}`);
const plan = await agent.call<Plan>("Planner", "plan", task);   // same range
// inside Planner.plan: ctx.call("Search", "query", q) — still scope user-…, still the same range
```

- **The scope decides the range.** Every target in a scope — every service, every key — shares
  **one** partition key, hence one range. Calls within a scope never go far.
- **The whole subtree inherits the scope.** A `ctx.call`/`ctx.send` that declares no scope carries
  the caller's scope, down every level. Switching to another scope is possible
  (`ctx.scoped("other")`); leaving the scope back to "no scope" is not.
- **The cost: a scope is bounded by the cap of one range.** A range is one stream with each commit
  through the object store; split cannot divide a scope, and a `concurrency` cap set above the
  range's capacity gives nothing extra. One scope per user / tenant is fine; one scope per request,
  or one scope for the whole system (funnelling everything into one stream), is not.
- **No scope, no key**: a workflow / agent tree is colocated on the root's range without declaring
  anything (the `calls/prefix` gate, on by default in new clusters). On an older cluster without the
  gate, ~(R−1)/R of such calls are cross-range.
- **Deliberate spreading: `placement: "spread"`.** A CPU-heavy child fan-out that wants to run on
  many ranges:

  ```ts
  await Promise.all(files.map((f) => ctx.call("Resize", "run", f, { placement: "spread" })));
  ```

  It only changes placement (`H(caller id, completion id)`, ≈ (R−1)/R on another range); the id
  stays deterministic, with no change of identity or deduplication — do not use a separate
  `key`/`idempotencyKey` per child just to spread. It is ignored (colocated as by default, no error)
  when the call has a scope (including an inherited one), a `key` or an `idempotencyKey`, when the
  cluster has not turned on the `calls/placement` gate, or when the running invocation predates the
  gate. A value other than `"spread"` throws a `TypeError` at once.
- The scope is also **identity**: `Cart/a` in scope `bob` is a different object from unscoped
  `Cart/a`.

The full derivation table, when a call is cross-range, and how to read the
`cairnflow_calls_total` metric: [`docs/key-design.md`](../../docs/key-design.md).

## Long work, external systems, and safe upgrades

**The handler contract** — declared in `service(...)`, captured into the deployment at registration
(changing it means registering a new version). The field names on the wire are the same as in
Restate's endpoint manifest.

```ts
service("Agent", { think, quick }, {
  type: "VIRTUAL_OBJECT",
  options: {
    inactivityTimeout: 10 * 60_000,                  // overrides the node's --inactivity-timeout-ms
    retryPolicy: { maxAttempts: 5, onMaxAttempts: "PAUSE", initialInterval: 200 },
  },
  handlerOptions: { quick: { inactivityTimeout: 5_000 } },
});
```

After `maxAttempts` turns (counting the first): the invocation is `paused` (or `failed` with `KILL`);
`POST /invocations/{id}/resume` gives it one more round.

**Long closures** — in `BIDI_STREAM`, when the runtime speaks wire v3, the SDK sends `Keepalive` by
itself while a `ctx.run` closure is still running, so a closure longer than the silence cap is not
cancelled. In `REQUEST_RESPONSE` use `inactivityTimeout`: when the service declares none, the SDK
declares 15 minutes (`DEFAULT_INACTIVITY_TIMEOUT_MS`) — a model-calling step of a few minutes is not
cut by the node's default 60 s cap and rerun. A SaaS cell clamps every value to 15 minutes.
`chatObject` declares `retryPolicy.maxAttempts` 5 (`CHAT_MAX_ATTEMPTS`) when none is declared: out of
attempts ⇒ `paused`.

**Awakeables** — a wait point an external system completes by id:

```ts
const { id, promise } = ctx.awakeable<Approval>();
await ctx.run("send approval request", () => mailer.send(approver, id));
const approval = await promise;   // POST /awakeables/{id}/resolve (an invoke key of the same tenant)
```

**Waiting with a deadline** — `ctx.signal(id)` and `awakeable().promise` are `DurableFuture`s, so they
can go into `ctx.race`/`ctx.all` together with `ctx.sleep`, `ctx.call`…:

```ts
const r = await ctx.race([ctx.signal<Approval>("approve"), ctx.sleep(24 * 3600_000)]);
if (r === undefined) throw new TerminalError("approval expired", 408);
```

The winner is whatever arrived **first in the journal**, so replay picks it again. A signal that has
arrived stays satisfied: if `sleep` wins a `race`, a signal arriving later is still in the journal
and a later `ctx.signal` with the same name is satisfied at once with that value — to wait for a new
one, use a new name (protocol-v1 §8.7). Each losing `sleep` still fires when due and costs a commit:
use long deadlines, do not loop on short ones. `ctx.awaitSignal(id)` is kept for old code
(= `await ctx.signal(id)`).

**Long loops** — `ctx.continueAsNew(state)` ends the invocation and starts a new one with the same
target and an empty journal, on the service's current revision; the caller gets the final result of
the chain.

```ts
async loop(ctx, s: { step: number; done: boolean }) {
  for (let i = 0; i < 50 && !s.done; i++) s = await ctx.run(`step ${s.step}`, () => next(s));
  if (!s.done) ctx.continueAsNew(s);
  return s;
}
```

**Upgrading code while invocations are still running** — an invocation is bound to its deployment;
a new version only takes new invocations. Try it first with
`POST /deployments/{id}/replay-check?service=` (a read-only replay — no closure of the new code
runs), then move invocations one by one with `POST /invocations/{id}/migrate` if you want.

## Streams, logs and traces (wire v4)

Three things that run **alongside** the journal: they add no command and no durable wait to the
turn (AP-4), and are not resent on replay.

```ts
async run(ctx: Context, prompt: string) {
  const ui = ctx.stream("ui");                 // ephemeral stream: tokens for the UI, in the leader's RAM
  ctx.log.info("started", { prompt_len: prompt.length });
  return ctx.trace.span("llm", async (s) => {
    s.set({ model: "m-1" });
    return ctx.run("llm", async () => {
      let text = "";
      for await (const tok of model.stream(prompt)) { ui.append({ tok }); text += tok; }
      s.set({ input_tokens: 1200, output_tokens: text.length });
      return text;
    });
  });
}
```

- `ctx.stream(name)` is an **ephemeral stream**: it only promises to keep **the running step**. It
  lives until the invocation is sealed + 60 s; an invocation suspended on an external wait (approval,
  `sleep`, the next message) that stays idle for more than 60 s has what was written released
  (readers get a `gap`, the next run continues at the right offset); it is lost when the leader
  changes (readers get `moved`, then `reset`). To keep it longer, use a durable stream.
  `ctx.stream(name, { durable: true })` is a **durable stream**: it rides along with existing
  commits and lives until the invocation is cleaned up. Read it via SSE
  `GET /invocations/{id}/streams/{name}` (`invoke` key).
- `ctx.log.{debug,info,warn,error}(msg, attrs?)` writes to `$log` (durable). The minimum level is
  `info`; `HandlerOptions.logLevel` changes it per handler. Read `GET /invocations/{id}/logs` (admin
  key).
- `ctx.trace.span(name, fn, attrs?)` writes `start`/`end` to `$trace` (durable), nested by call
  scope; `span_id` is deterministic. `GET /invocations/{id}/spans` builds the tree and adds up
  `input_tokens`/`output_tokens`. `HandlerOptions.trace: "runtime"` adds the runtime's spans (one per
  turn, one per `ctx.run`): a `ctx.run` span carries `attrs.entry` (the index of the `Run` entry in
  the journal); a transiently failed turn closes `error`, a run unfinished when the turn ends closes
  `error` with `abandoned`; a run rerun on a later attempt has a span with `start_estimated` (start
  counted from the beginning of the turn).
- Durable records become durable at the invocation's next commit (a `ctx.run` result, a suspension,
  the end). `HandlerOptions.logFlushMs` (ms, `BIDI_STREAM`) flushes on an interval even in the
  middle of a long `ctx.run` — keeping logs when a node dies, at the cost of one commit per interval.
- Caps per invocation: `$log` 1 MiB, `$trace` 1 MiB, user durable streams 8 MiB; over the cap,
  records are dropped with one `gap`. A runtime below v4 ⇒ records are dropped (with one warning),
  the handler still runs.

### Artifacts

`ctx.artifact.put(bytes, {contentType})` takes large bytes (at most 16 MiB minus 1 KiB) out of the
journal: the runtime PUTs them to the object store **before** processing the next command, and `put`
returns a ref `{sha256, size, contentType}` computed in place — return that ref from `ctx.run`
instead of the bytes. `ctx.artifact.get(ref)` reads from the process's memory, otherwise from
cairnflow after `configureArtifacts({ baseUrl, headers })`. Clients read via
`GET /invocations/{id}/artifacts/{sha}` (`invoke` key). Needs wire v4 and the cluster's
`streams/table` gate.

```ts
const ref = await ctx.run("render", async () => ctx.artifact.put(await render(), { contentType: "image/png" }));
```

### Reading streams, and the Vercel AI SDK adapter

`readStream(base, id, name, {from, headers})` reads an invocation's stream from any node: it follows
`503` + `leader`, `moved`, `reset` and the maximum lifetime of an SSE connection, returning every
event to the caller (a UI clears what it has shown on `reset`).

The adapter for the **Vercel AI SDK v7** (`ai@7`) does not pull the `ai` package into the SDK — the
types in `src/vercel.ts` are structural; `test/ai-sdk.test.ts` runs it with the real `ai` package.
The full guide: [`docs/ai-sdk.md`](../../docs/ai-sdk.md); a runnable example:
[`example/ai-chat/`](./example/ai-chat/).

```ts
// handler: virtual object `Chat`, keyed by chatId
const chat = chatObject("Chat", async (ctx, turn) =>
  streamText({
    model: wrapLanguageModel({ model: openai("…"), middleware: cairnMiddleware(ctx) }),
    tools: cairnTools(ctx, tools, { approve: ["refund"] }),
    messages: await convertToModelMessages(turn.messages),
    stopWhen: stepCountIs(5),
    onError: turn.onError,
  }),
);

// browser
const transport = cairnChatTransport({ baseUrl, service: "Chat" });
const { messages } = useChat({ id: chatId, transport, resume: true });
```

- Each model call and tool is a `ctx.run` (not called again after a failure) wrapped in a
  `ctx.trace.span` carrying `model`/`input_tokens`/`output_tokens`.
- The ephemeral stream `ui` carries **UI chunks** (`UIMessageChunk`) — text, reasoning, tool inputs
  and results, step boundaries — emitted as soon as they arrive, from inside the `ctx.run` closure
  (replay does not emit them again).
- `chatObject` keeps the history (`messages`) and the id of the running turn (`active`) in the
  object's state; the transport only sends the last message, and on a page reload
  `reconnectToStream` asks `active`.
- Tools in `approve` wait for a human approver via `ctx.promise` (durable, without holding a
  process); `transport.approve(chatId, approvalId, { approved })` answers.
- `stop` ⇒ `cancel`; `reset` (leader change mid-step) or `superseded` (the SDK died mid-step) ⇒
  `reset-step`, and the next run regenerates the step.

## Examples

| | |
|---|---|
| [`example/order-service.ts`](./example/order-service.ts) | The smallest runnable one: `ctx.get`, `ctx.sleep`, `ctx.run`, `ctx.set` on a virtual object |
| [`example/ai-pipeline/`](./example/ai-pipeline/) | The full one: durable fan-out, a per-tenant concurrency cap, a human approval gate, a compensating saga, `ctx.attempt`. With a `demo.ts` running all seven scenes over plain HTTP |
| [`example/ai-chat/`](./example/ai-chat/) | A chat agent with the Vercel AI SDK v7: `streamText` + durable tools, tool approval, history in the object, reconnecting after a page reload. Runs without an API key (fake model), or with OpenAI/Anthropic |

## Three rules for user code (protocol-v1 §8.4)

| Rule | Why |
|---|---|
| Do not `Math.random()` — use `ctx.random()` | Replay must produce the same sequence, otherwise completion ids shift |
| Do not read `Date.now()` directly — wrap it in `ctx.run` | Time is a source of non-determinism; `ctx.sleep` already computes an absolute time once and journals it |
| Do not wrap `ctx` commands in a `try/catch` that swallows the error | The SDK has a defence (see below), but such wrapping still makes the code hard to follow |

The SDK **cannot enforce** these three rules. It can only half-enforce the third.

## A few decisions worth knowing

**Results are delivered in journal order; waiting for something not yet there suspends instead of
throwing.** A result already in the journal is not handed straight to the code asking for it: the
turn *delivers* results one by one in exactly the recorded order, and between two deliveries lets
user code run to its next wait point. Waiting for something not yet there (a timer, a `ctx.call`
result, a signal, a just-proposed `ctx.run` result) is a promise that stays pending until the end of
the turn — other parallel branches keep going; the turn ends when every branch is waiting, with a
`Suspension` merging every wait point. That makes the command sequence a function of the journal,
not of which closure finished first — parallel multi-step branches replay correctly. As a
consequence, code after `await ctx.run(...)` never runs in the same turn as its closure — do not let
a closure wait for something that only the code after that `await` would unblock. After a transient
error or a journal mismatch the next `ctx` command emits nothing more, even if user code swallows the
error.

**`ctx.run` waits for a durable result.** It proposes the result, then its branch suspends, and only
continues in the next turn, once `ProposeRunCompletionAck` has come back — the ack is the runtime
saying *written*, and the runtime only sends it after the commit. protocol-v1 §4 allows a `ctx.run`
to end the turn with a suspension, and that is the only way for the next turn to trust this result.

The previous version carried on right after proposing, and its failure window was wider than it
looked: it extended to the **next suspension point**, not just to the next command. Three `ctx.run`s
in a row with the process dying at the third reran all three, including two that had long finished.
In `BIDI_STREAM` this wait costs no HTTP request — it is one rerun of the handler on the journal in
RAM.

**One port, two protocols.** `serve()` declares `BIDI_STREAM` by default and sniffs the HTTP/2
preface to serve both h2c (`/invoke`, two-way) and h1 (`/discover`) on the same port. In this mode, a
wait point the runtime can resolve in place — lazy `ctx.get`, a `ctx.run` result — no longer costs an
HTTP request and a resent journal: the runtime pushes the notification into the open stream, and the
SDK reruns the handler from the journal in RAM. Set `protocolMode: "REQUEST_RESPONSE"` when there is
an h1-only proxy between the runtime and the SDK.

**Dying while `fn` is running reruns the side effect.** A journal with the command but no result
means the result is *unknown*. That is the unavoidable at-least-once part — nobody knows how far a
network call got — and the reason the work inside `ctx.run` should be idempotent. The *avoidable*
part (dying after `fn` has finished) is what the ack closes.

**`null` from `ctx.get` means the key does not exist**, as opposed to an empty value. Merging the
two is a classic bug: a user sets state to `""`, reads it back and sees "not set".

**Strip-only constraints.** Node runs `.ts` by erasing types, without generating code. So there are
no parameter properties, no `enum`, and types must be imported with `import type`. `tsconfig.json`
turns on `erasableSyntaxOnly` so the compiler rejects exactly that syntax instead of letting it blow
up at run time.

## Tests

```sh
node --test "test/*.test.ts"                          # the whole suite
CAIRNFLOW_E2E_SDK_MODE=REQUEST_RESPONSE node --test "test/*.test.ts"
npx tsc --noEmit                                      # type check
```

The e2e suite runs **verbatim** on both transports — the only way to know `BIDI_STREAM` does not
quietly change semantics. `test/journal-parity.test.ts` goes further: the same program, two
runtimes, two modes, journals compared byte for byte.

`test/e2e.test.ts` is the most valuable test: it runs **the real Rust binary** talking to this SDK.
Every other test on either side uses a fake for one end; this one uses two independent
implementations of the same specification, written in two languages, sharing not a single line of
code. One bit off in a header, a message number or a field layout and it goes red. Needs
`cargo build --bin cairnflow` first.

## Not done yet

- `REQUESTED_ACK` — the transport can carry it, the SDK does not set the flag. In the rerun model
  nothing waits for it, so asking for an ack only adds frames; the flag becomes worth having when
  `demand()` turns into real async-await.
- `demand()` is still rerun-based, not async-await. Every notification reruns the handler from the
  start — cheap because it is in RAM, but not free for long handlers.

## Known limitations

- **Journal matching** compares every field of a recorded command — kind, name, target, key,
  parameters, completion id — except the wake-up time of `ctx.sleep`/the `delay` of `ctx.send`
  (computed once and recorded) and the value of `ctx.set` (the runtime may have sealed it into refs).
  Changing a value written to state between two deploys therefore does not produce 570.
- **The runtime speaks a wire version above the SDK's cap** (the deployment settled on a new version,
  the SDK process was downgraded to an old build without re-registering) ⇒ the endpoint answers
  `415`; the turn is retried up to `maxAttempts` and then `paused`. Re-register the deployment.
- **Segmented-value read URLs in `BIDI_STREAM` (wire v6)** issued mid-turn live for 60 s, but the SDK
  only fetches segments when the turn ends: a lazy `ctx.get` running in parallel with a `ctx.run`
  longer than 60 s may meet an expired URL ⇒ the turn fails transiently and is retried with a new
  URL. Accepted: it heals itself after one attempt.
- **A CANCEL mid-turn in `BIDI_STREAM`** aborts cancellable closures and delivers the 409 error to
  user code at once. With parallel branches, the compensation commands in `catch` may swap places on
  replay.
