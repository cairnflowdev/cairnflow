# Cairnflow Service Protocol conformance suite

**Language-neutral** golden traces for `docs/protocol-v1.md`: each file in `traces/` is a frame
conversation between the runtime and an SDK, byte by byte, together with the expected final journal.
An SDK in any language (Python, Go, …) proves it speaks the protocol correctly by implementing the
**twenty-two handlers of §Handlers**, then running a runner — a fake runtime that replays the trace
into the SDK over real HTTP and compares every frame the SDK sends. No Rust, no `cairnflow` binary
needed.

Reference runner: `sdk/typescript/test/conformance.test.ts`. Python runner:
`sdk/python/tests/test_conformance.py` (every trace; `REQUEST_RESPONSE` traces also through the
AWS Lambda adapter; `BIDI_STREAM` traces need the SDK's `bidi` extra, `h2`). Go runner:
`sdk/go/conformance_test.go` (`go test ./...` in `sdk/go`: every trace, h2c for `BIDI_STREAM`
from the standard library; every trace below wire 6 also against the SDK pinned to its version;
`REQUEST_RESPONSE` traces whose connections carry no `replay` field also through the AWS Lambda
adapter).

## Where the traces come from

`crates/cairn-runtime/tests/conformance_gen.rs` generates the traces from the runtime's **real state
machine** (`InvocationMachine`), a deterministic driver in place of the engine, the transport rules
of `cairn-server/src/turn/sdk.rs`, and a **reference SDK** written in Rust that encodes with
`prost`. The scenarios are taken from the harness in
`crates/cairn-runtime/tests/{mandatory,phase4}.rs` (field `source`).

- `cargo test -p cairn-runtime --test conformance_gen` (a normal run, part of
  `cargo test --workspace`): regenerates in memory and compares **character by character** with the
  committed files. Red means the runtime's on-the-wire behaviour has changed.
- For an intentional change, regenerate — the files are written **only** when the environment
  variable is set (the same trap as the sample buckets of the format fixture, AGENTS.md §5):

  ```sh
  CAIRNFLOW_REGENERATE_CONFORMANCE=1 cargo test -p cairn-runtime --test conformance_gen -- --ignored
  ```

  then run the runner of **every** SDK.

## Format

Schema: [`trace.schema.json`](trace.schema.json) (JSON Schema 2020-12). Main fields:

| Field | Meaning |
|---|---|
| `wire_version` | Negotiated version (§2). The runner sends `content-type: application/vnd.cairnflow.invocation.v<N>` |
| `mode` | `BIDI_STREAM` (one h2c stream per connection) or `REQUEST_RESPONSE` (one h1 request per connection) |
| `handler` | `{service, name}` — the conformance handler the SDK must implement |
| `clock_ms` | The SDK's wall clock, **fixed** for the whole trace. `ctx.sleep(ms)` journals `clock_ms + ms` |
| `initial_journal_len` | Number of entries durable before the first connection (1 = only `Input`) |
| `connections[]` | Connections in order; each has `frames[]`, `close`, `verify`, and in v5 traces `replay` |
| `journal[]` | Expected final journal, from entry 0 |
| `outcome` | The outcome the runtime records (`completed`, `failed`, `replay_report`) |
| `matrix_cells` | Cells of the "Mandatory tests" table in §10 that the trace covers |

Each frame: `from` (`runtime` \| `sdk`), `channel` (`journal` \| `side`), `type`, `flags`, `hex`
(**the whole frame**: 8-byte header + payload), `summary` (a decoding for human readers — not part of
the contract), and, for SDK frames, an optional `match`.

### Side channel (`channel: side`)

Frames that **travel on the connection but never become journal entries, never wait for a durable
ack, and change nothing in the journal wherever they are interleaved** (`protocol-v1.md` §9.2).
Today there are: `Keepalive` (`0x0008`, SDK → runtime, v3), `StreamAppend` (`0x000B`, SDK → runtime,
v4 — the records of `ctx.stream`) and the custom range `0xFC00+`. Every other frame — including
`Start`, acks, `Suspension` — is `journal`: its position in the stream is meaningful.

### Incremental replay (`replay`, wire v5)

Only v5 traces (`protocol-v1.md` §9.3) have a `replay` field on a connection; older traces have none
and are unchanged. The connection's `runtime` frames are **exactly the body that was sent** — on a
delta connection, `Start` carries `replay_from`/`prefix_digest` and the body omits the first `B`
entries. Fields:

| Field | Meaning |
|---|---|
| `sdk_restart` | The runner restarts the SDK (losing its replay cache) **before** this connection |
| `miss` | The SDK must return `409` with header `cairn-replay: miss` and an empty body — the connection has no `sdk` frame |
| `cache` | The value of the `cairn-replay-cache` header the SDK must return with `200`. `replay` present but `cache` absent ⇒ the SDK must **not** return that header |
| `held`, `held_digest` | (round 2) The header `cairn-replay-held: <held>:<held_digest>` the SDK must return with `409` — the part of the journal it still holds, `k = min(held, N)`. Absent ⇒ unconstrained (traces from before round 2) |
| `stale_sdk` | (round 2) This connection and every later one go to a **second replica** of the SDK, fresh, that received every earlier connection **except** the last `200` one — the stale replica behind a load balancer without affinity |

The traces: `delta-hit-rr-v5` (hits on every turn after the first), `delta-miss-rr-v5` (the third
connection carries a mismatching digest), `delta-after-restart-rr-v5` (the SDK loses its cache before
the third connection), `delta-miss-held-rr-v5` (round 2: the fourth connection lands on the stale
replica, the SDK returns what it still holds, the runtime resends from `k`). In the first three
traces the runtime resends everything after a miss — still valid: resending from `k` is an optional
runtime optimisation. Each v5 trace runs against a **fresh** SDK (the cache is keyed by invocation
id, and every trace uses the same id), with the replay cache enabled in the Fetch adapter too.

## Runner rules

The runner is a fake runtime. For each trace:

1. **Negotiation.** `GET /discover`. The runner speaks `[1, wire_version]`; the chosen value
   `min(manifest.maxProtocolVersion, wire_version)` must equal `wire_version` and be ≥
   `manifest.minProtocolVersion`. `protocolMode` must equal `mode`.
2. **Clock.** Fix the SDK's wall clock at `clock_ms` while the trace runs.
3. **Each connection**, sent to `POST /invoke/{service}/{name}`:
   - `REQUEST_RESPONSE`: the request body = every `runtime` frame concatenated; the response body is
     split into frames and compared in turn with every `sdk` frame.
   - `BIDI_STREAM` (h2c prior-knowledge): follow `frames` in order. A run of consecutive `runtime`
     frames is **one write**; at an `sdk` frame, read the next frame the SDK sends and compare. At
     the end of the frames: `close: runtime` ⇒ the runner closes its side (END_STREAM) and the SDK
     must close too; `close: sdk` ⇒ the SDK must close on its own. After that the SDK must not send
     any further `journal` frame.
   - The response content-type must be `application/vnd.cairnflow.invocation.v<M>` with `M ≤ N`.
   - v5 traces: follow the connection's `replay` (section above) — `409` miss, `cairn-replay-cache`
     header.
4. **Comparing** an SDK frame, according to `match`:
   - absent / `bytes`: the whole frame, byte by byte;
   - `type`: only the first 4 header bytes (type + flags) — the payload carries text the SDK chooses
     itself (the message of a failed `Output`);
   - `error`: like `type`, plus `ErrorMessage.code` (field 1) and `.behavior` (field 9) equal to
     `error_code`, `error_behavior`.
5. **SDK side channel.** `side` frames from the SDK that are **not in the trace** are ignored (the
   number of `Keepalive`s depends on timing). `side` frames that are in the trace must arrive, in
   order.
6. **Journal.** The command frames (`0x04xx`) the SDK sends outside `verify` connections, in order,
   must equal the command part of `journal` after `initial_journal_len` (payload byte by byte, except
   entries with `match: type`). A `verify: true` connection replays the whole `journal`; the SDK may
   only return `End` — §10: `End` is not an entry, `Output` is.

### Canonical encoding

Byte-by-byte comparison only makes sense when two SDKs encode the same message to the same byte
sequence. The traces use **proto3 canonical form**, exactly what every official protobuf library
produces: fields in **ascending tag number** order, numeric `repeated` fields **packed**, default
values not written except for members of a `oneof`. The runtime still accepts every valid form
(`protocol-v1.md` §11).

## Handlers

One service `Conformance`, of kind `VIRTUAL_OBJECT`. Values are JSON; `ctx.*` is the equivalent API
of your SDK. Completion ids are assigned in call order, starting at 1.

| Handler | Behaviour |
|---|---|
| `counter` | `n = await get("count")`; `await sleep(1000)`; `await run("charge", () => "charged")`; `set("count", (n ?? 0) + 1)`; return `"done"` |
| `counterChanged` | `await get("count")`; `set("count", 9)`; return `"other"` (the "code changed" version of `counter`) |
| `counterRenamed` | like `counter`, but `run("charge-v2", …)` (the "step renamed" version of `counter`: same entry kind, different name ⇒ 570) |
| `outOfOrder` | `a = sleep(1000)`; `b = sleep(2000)`; `await a`; `await b`; return `"both"` |
| `sleepRunSleep` | `a = sleep(1000)`; `b = sleep(2000)`; `await run("pay", () => "result")`; `await a`; `await b`; return `"done"` |
| `eagerCount` | `n = await get("count")`; `set("count", (n ?? 0) + 1)`; return `"done"` |
| `stateKeys` | return `await getKeys()` |
| `promise` | `await promise("approve").resolve("ok")`; return `await promise("approve").get()` |
| `allOf` | `a = sleep(1000)`; `b = sleep(2000)`; `await all([a, b])`; return `"both"` |
| `raceSignal` | `v = await race([sleep(60000), signal("approve")])`; return `v` if the signal wins, `"sleep"` otherwise. Suspends on **one** tree: the sleep's completion (field 1) and the signal name (field 3) |
| `raceAwakeable` | `{ promise } = awakeable()` (signal numbered 17); `v = await race([promise, sleep(60000)])`; return `v` or `"sleep"`. Tree: field 1 and field 2 |
| `allSignal` | `[, s] = await all([sleep(1000), signal(4)])`; return `s`. The `ALL_COMPLETED` child node carries the completion and the signal |
| `scopedCall` | return `await scoped("tenant-a", "k1").call("Callee", "work", 1)`. Wire < 2 ⇒ terminal error, no `Call` emitted |
| `spreadCall` | return `await call("Callee", "work", 1, { placement: "spread" })` — `Call.placement` (field 13) = `CALL_PLACEMENT_SPREAD`, at every wire version |
| `parallelBranches` | `branch = async n => [await run(n+"1", () => n+"1"), await run(n+"2", () => n+"2")]`; return `await Promise.all([branch("a"), branch("b")])`. Results are delivered in journal order; a branch waiting for its proposal does not block the other branch; suspends on `FIRST` of every id proposed in the turn |
| `callRetargeted` | `await call("Callee", "b", 1)`; return `"x"` (the journal has a `Call` to `Callee/a` ⇒ 570: commands match on every field, not only the kind) |
| `stateKeyChanged` | `await get("total")`; return `"x"` (the journal has `GetState "count"` ⇒ 570) |
| `segmentedGet` | return `await get("history")` — sealed state (wire v6) |
| `segmentedSetReuseTail` | `h = await get("history")`; `set("history", h + "b")`; return `"ok"` |
| `segmentedRunResult` | `h = await get("history")`; `await run("extend", () => h + "c")`; return `"ok"` |
| `slowRun` | return `await run("slow", closure)`; the closure waits for a signal from the runner, which signals when it reads the first `Keepalive` |
| `streamRun` | `ui = stream("ui")`; `await run("llm", () => { ui.append({tok: "a"}); ui.append({tok: "b"}); return "ab" })`; return `"done"`. Wire v4: both records travel in **one** `StreamAppend` (side channel) right before the frame that ends the turn, identified by `(step, attempt, k)` = (the position of the `Run` command itself, `retry_count_since_last_stored_entry`, order); a step already journaled is not resent |

Naming convention (the command's `name`, pinned byte for byte): `get`/`set` ⇒ the name is the key;
`sleep` ⇒ `"sleep"`; `getKeys` ⇒ `"getKeys"`; promise ⇒ `"promise/<name>"`; `call` ⇒
`"<service>/<handler>"`; `run` ⇒ the name the user gave; `Output` ⇒ `"output"`. Also:

- `ctx.run` suspends the turn waiting for durability after proposing; `BIDI_STREAM` sets
  `REQUESTED_ACK` on `ProposeRunCompletion`, `REQUEST_RESPONSE` does **not** (§4).
- Suspending on one completion: `Suspension { waiting_completions: [id] }`; `all`: one
  `ALL_COMPLETED` child node (§7).
- In `BIDI_STREAM`, the SDK reads the replayed part (`known_entries` entries), then every frame that
  has arrived, then runs the handler; when suspended, it reads frame by frame until what it waits for
  is complete **and every proposal of the turn has been acked**, then reruns the handler **from the
  start** on the local journal (§4 "Turns on one stream").
- Results already in the journal are **delivered** to user code in journal order, one per tick;
  waiting for something not yet there does not block other parallel branches; the turn suspends on
  `FIRST` of every pending wait (one wait ⇒ exactly its tree). Traces `parallel-branches-*`.
- Commands match the journal on **every field** (target, key, parameters, completion id), except the
  wake-up time of `sleep`/`send` and the value of `set` — a mismatch is 570. Traces
  `call-retargeted-rr-v1`, `state-key-changed-rr-v1`.
- **Wire v6** (segmented values): the `segmented-*-rr-v6` traces carry a `segments` field (`url`,
  `hex`). The runner serves exactly those bytes when the SDK does `GET url` (a `fetch` hook in the
  runner process, or an HTTP server if the SDK runs in another process — the URL is the constant
  `https://segments.conformance.invalid/…`, unsigned). `Start.segment_urls` carries the URLs of every
  ref in the batch that opens the turn (no duplicate `(sha, gen)`, in order of appearance). The SDK
  assembles the value (segment ‖ tail) and checks sha256 and length; writing back a value whose
  prefix is byte-identical to an assembled value ⇒ reuse the ref, send only the rest (`SetState`,
  `Output`, `ProposeRunCompletion.segmented_value`). The `completed` outcome records the **logical**
  bytes. `REQUEST_RESPONSE` only: BIDI's mid-turn batches (`SegmentUrls`) follow the batch boundaries
  of the runtime shell, which the generator does not model.
- The runtime speaks a wire version **higher** than the SDK's declared ceiling ⇒ the SDK returns
  `415`, no frames, does not run the handler. Traces `version-too-new-*`: field
  `sdk_max_protocol_version` (the runner pins the SDK at that number, no negotiation), the
  connection carries `status`, outcome `refused`.

## Coverage

`matrix_cells` of each trace; the summary table is in `protocol-v1.md` §10.

## Suspension tree vectors (`suspension-vectors.json`)

The rule "is the suspension tree satisfied" exists twice: the runtime's `suspension_satisfied_by`
(`crates/cairn-runtime/src/machine.rs`) and the TS SDK's `treeSatisfied` (the wait loop of `runBidi`
reads until it is satisfied). A mismatch means an extra turn (the SDK reruns when the runtime says
not yet) or a stuck stream (the SDK waits for something the runtime considers satisfied). This file
keeps the two in agreement (CG-4): each vector is a tree the SDK emits — as JSON and as
`SuspensionMessage` payload bytes — together with every combination of leaves that have arrived and
the expected verdict (`first` ⇒ any leaf, `all` ⇒ every leaf; noise outside the tree does not satisfy
it).

- `crates/cairn-runtime/tests/suspension_vectors.rs`: decodes `hex`, asks `suspension_satisfied`.
- `sdk/typescript/test/conformance.test.ts` (test "CG-4"): encodes the tree to exactly `hex`, asks
  `treeSatisfied`.
- `sdk/go/internal/turn/tree_test.go` (`TestSuspensionVectors`): encodes the tree to exactly `hex`,
  asks the Go SDK's own satisfaction rule.

An SDK that emits a new tree shape adds a vector; SDKs in other languages run the same file.
