# cairnflow (Python SDK)

The Python SDK for Cairnflow. It implements [`docs/protocol-v1.md`](../../docs/protocol-v1.md)
on the side of whoever writes the code, and passes every trace of the language-neutral
conformance suite in [`conformance/`](../../conformance) byte for byte.

**No runtime dependencies.** The protocol surface is small and fixed, so the codec, the state
machine, the HTTP/1.1 server, the client and Ed25519 verification are all standard library.
Two optional extras:

| Extra | Adds | Needed for |
|---|---|---|
| `cairnflow[bidi]` | `h2` | `BIDI_STREAM` (h2c with prior knowledge, same port as HTTP/1.1) |
| `cairnflow[crypto]` | `cryptography` | faster request-identity verification (a pure-Python Ed25519 is built in) |

Python ≥ 3.11.

## Usage

```python
import asyncio
from dataclasses import dataclass
from cairnflow import Context, TerminalError, VirtualObject, serve

orders = VirtualObject("OrderService")

@dataclass
class Order:
    amount: int

@orders.handler()
async def submit(ctx: Context, order: Order) -> dict:
    if order.amount <= 0:
        raise TerminalError("amount must be positive", 400)   # bad data: retrying is useless
    total = (await ctx.get("total")) or 0                     # None = the key does not exist
    await ctx.sleep(1)                                        # durable: the timer lives in the store
    receipt = await ctx.run("charge", charge, order.amount)   # runs once per durable result
    ctx.set("total", total + order.amount)
    return {"receipt": receipt, "total": total + order.amount}

@orders.handler(kind="shared")                                # runs in parallel, read-only state
async def total(ctx: Context) -> int:
    return (await ctx.get("total")) or 0

asyncio.run(serve([orders], port=9080))                       # REQUEST_RESPONSE on HTTP/1.1
```

```sh
cairn register http://127.0.0.1:9080
cairn invoke OrderService submit --key order-42 --input '{"amount": 100}'
```

A runnable copy is in [`example/order_service.py`](example/order_service.py).

### Services

- `Service(name)`: stateless, no key. `@svc.handler()`.
- `VirtualObject(name)`: keyed, durable state. Handlers are `EXCLUSIVE` by default (one at a time
  per key); `@obj.handler(kind="shared")` runs in parallel and must not write state.
- `Workflow(name)`: exactly one `@wf.main()` handler (named `run` by default, runs once per key) and
  `@wf.handler()` shared handlers that read state, send signals or complete the workflow's promises.

A handler is `async def h(ctx)` or `async def h(ctx, input)`. Input and output are JSON; an input
annotated with a dataclass or a Pydantic model is converted for you, and dataclasses/Pydantic
models are serialised on the way out. `HandlerOptions(retry_policy=RetryPolicy(...),
inactivity_timeout=..., journal_retention=..., log_level=...)` declares the contract in the manifest.

### The context

| | |
|---|---|
| State | `await ctx.get(k)`, `ctx.set(k, v)`, `ctx.clear(k)`, `ctx.clear_all()`, `await ctx.state_keys()` |
| Time | `await ctx.sleep(seconds or timedelta)`; the wake-up time is absolute and journaled once |
| Side effects | `await ctx.run(name, fn, *args, cancellable=False, **kwargs)`: `fn` sync or async |
| Calls | `await ctx.call(svc, handler, input, key=..., idempotency_key=..., placement="spread")`, `ctx.send(..., delay=...)`, `ctx.scoped(scope, limit_key).call(...)` |
| Signals | `await ctx.signal(name_or_idx)`, `ctx.send_signal(id, signal, value)`, `awk_id, fut = ctx.awakeable()` |
| Promises | `await ctx.promise(name).get()`, `.peek()`, `.resolve(v)`, `.reject(msg, code)` |
| Others | `await ctx.attach(id)`, `await ctx.get_output(id)`, `ctx.continue_as_new(input)` |
| Combinators | `await ctx.all([f1, f2])`, `await ctx.race([f1, f2])`: suspend durably on one tree; the winner of a race is decided by journal order |
| Streams | `ctx.stream(name).append(record)`, `ctx.log.info(msg, attrs)`, `async with ctx.trace.span(name) as s:` |
| Artifacts | `ref = ctx.artifact.put(bytes)`, `await ctx.artifact.get(ref)` |
| Determinism | `ctx.random()`, `ctx.key`, `ctx.invocation_id`, `ctx.attempt` |

`asyncio.gather` over branches works too (each branch suspends independently); `ctx.all` is the
single-tree form.

Three rules for user code (protocol-v1 §8.4):

1. Do not use `random` directly: use `ctx.random()`.
2. Do not read the clock directly: wrap it in `ctx.run`.
3. Do I/O only inside `ctx.run`. Handler code runs on a deterministic event loop that refuses
   sockets on purpose; `ctx.run` closures run on the server's real loop.

Errors: `TerminalError(message, code)` fails the invocation without retry; `PauseError` pauses it
for a human (`POST /invocations/{id}/resume` continues from the same point); any other exception
is retried with backoff. Raised inside a `ctx.run` closure, `TerminalError`/`RunFailure` is a
durable failed result of that step.

### How it runs

The same model as the TypeScript SDK: each turn reruns the handler from the start on the journal;
commands already journaled are matched (kind, name and every field, or 570) instead of re-emitted,
and results are delivered to user code **in journal order**, one at a time, so a replay takes
exactly the same path. A `ctx.run` result is proposed, then waited for until durable.

### Serving

- `await serve(services, port=9080, protocol_mode="REQUEST_RESPONSE")`: the built-in server.
  `protocol_mode="BIDI_STREAM"` (with the `bidi` extra) serves h2c and HTTP/1.1 discovery on one port.
- `app(services)`: an ASGI application for uvicorn, hypercorn or any ASGI server (`REQUEST_RESPONSE`).
- `cairnflow.aws_lambda.lambda_handler(services)`: AWS Lambda (Function URL / API Gateway HTTP API,
  payload v2).

Endpoint options (`Endpoint(...)` or keyword arguments of the above):

- `identity_keys=[kid]`: verify the runtime's signature on every request (the `kid` from
  `GET /identity`); unsigned or misbound requests get 401 before any handler runs. Without it,
  `serve()` warns once.
- `max_protocol_version`: pin an older wire version (default: the SDK's maximum, 6, when
  `identity_keys` or `segment_origins` is set; otherwise 5, and pinning 6 there is a `ValueError`:
  wire v6 makes the endpoint fetch URLs from the request).
- `replay_cache` (wire v5, `REQUEST_RESPONSE`): keep the journals of running invocations in memory
  so the runtime only sends what is new. On by default with `identity_keys` (off on Lambda); off
  without them, and asking for it without `identity_keys` is a `ValueError` (a delta names an
  invocation id, so an unverified caller could run the handler on another invocation's journal).
- `segment_origins`, `value_cache_bytes`, `max_value_bytes` (wire v6 segmented values).
- `compression`: accept gzip request bodies, compress large responses (default on).

## Client

```python
from cairnflow import connect, InvocationFailed

c = connect("https://cairn.example.com", api_key=os.environ["CAIRN_KEY"])
r = await c.object("OrderService", "order-42").call("submit", {"amount": 5})   # waits for the result
h = await c.service("Mailer").send("welcome", {"to": "a@example.com"})       # fire and forget
await h.attach(timeout=60)                                                    # or h.output(), h.status()
await c.workflow("Signup", "u1").run(user)
await c.awakeable(awk_id).resolve({"ok": True})
async for ev in read_stream("https://cairn.example.com", invocation_id, "ui"): ...
```

Every call carries a generated `Idempotency-Key` kept across retries (`idempotency_key=None`
turns it off); the client follows the `leader` of a `503` and waits for the `Retry-After` of a `429`.

## Tests

```sh
cd sdk/python
python3 -m unittest discover -s tests            # unit, conformance (REQUEST_RESPONSE), client
uv venv .venv && uv pip install --python .venv/bin/python h2
.venv/bin/python -m unittest discover -s tests   # + BIDI_STREAM traces and tests
```

- `tests/test_conformance.py` is the conformance runner: a fake runtime replays every trace of
  `conformance/traces` over real HTTP (and through the Lambda adapter) and compares every frame.
- `tests/test_e2e.py` runs the real binary against this SDK in both modes (`cargo build --bin
  cairnflow` first, or `CAIRNFLOW_BINARY=<path>`); skipped without it.

```sh
uvx --python 3.11 pyright@1.1.407 --pythonpath .venv/bin/python src example tests   # 0 errors
uv run --no-project --python 3.11 python scripts/smoke_wheel.py                      # the built wheel
```

- `scripts/smoke_wheel.py` builds the sdist and the wheel, installs the wheel alone into a fresh
  environment outside the repo, runs an app against the binary through the client, and checks
  with `pyright` that the shipped types reach user code.

Not ported from the TypeScript SDK: the Vercel AI SDK integration (`vercel.ts`), which is specific
to that library.
