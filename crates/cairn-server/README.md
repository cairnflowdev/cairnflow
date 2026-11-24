# cairn-server

HTTP ingress, discovery and the invoker. This is where the whole chain can be driven with `curl`.

## An architecture dictated by `!Send`

`PartitionWriter` is deliberately not `Send` (INV-4), so the engine **cannot** enter axum
handlers running on a thread pool. It lives on its own thread with a `current_thread` runtime;
everything else talks to it over channels ([`actor`](src/actor/mod.rs)).

This is not an inconvenience to work around — it is exactly what we want: the writer lives in
exactly one task, and the compiler guarantees it instead of human discipline. `EngineHandle` is
`Clone + Send` because it is only the sending end of a channel; it grants no write authority.

## Why `REQUEST_RESPONSE` before `BIDI_STREAM`

It is the mode with **narrower** semantics: the runtime sends the whole journal, the SDK returns
a whole response, and whenever it needs to wait the SDK suspends and ends the turn. Everything
hard about `BIDI_STREAM` — both directions in parallel, `REQUESTED_ACK`, `ctx.run` not ending
the turn — is an *addition*, not a *change*.

An unexpected bonus: because every turn rebuilds the state machine from the journal, the replay
code path runs on **every** invocation, not only after an incident. The crash-recovery path is
no longer a rarely exercised path.

## API

```sh
# register a deployment (calls /discover, negotiates the two version axes)
curl -X POST localhost:8080/deployments -d '{"uri":"http://localhost:9080"}'

# call a service
curl -X POST 'localhost:8080/invoke/OrderService/submit?key=order-42' -d 'payload'
#   200 + {"status":"completed","output":...}
#   202 + {"status":"waiting","id":...}     ← waiting for a timer
#   409 + {"status":"paused",...}           ← needs a human
#   500 + {"status":"failed",...}

curl localhost:8080/invocations/{id}
curl -X POST 'localhost:8080/admin/tick?now_ms=...'
```

A background loop runs out of the box (`--tick-interval-ms`, 200ms by default) and handles both
timers and retries. `/admin/tick` **is kept** because tests need to control time: a background
loop on the real clock would make every test with `ctx.sleep` wait for real. Set
`--tick-interval-ms 0` to turn the background loop off entirely.

The background work runs as tasks on the partition's engine thread: the engine is `!Send` and
lives on that one thread, and tasks take it in turn through the handover lock of `src/lease.rs`
(which also forbids network calls while it is held). An error in one tick is only logged —
exiting would stop every timer and every retry because one deployment was temporarily
unreachable.

## Six easy mistakes, learned the hard way

**Timers are loaded *after* replay, not before.** A state machine just loaded from the journal
does not yet know which completions are still pending — it only knows after replaying. Feeding a
completion into a machine that has not replayed makes it treat it as a notification for an
undeclared id, a protocol violation, and the invocation dies for an entirely wrong reason.

**`random_seed` is derived from the invocation id, not generated anew each turn.** The protocol
requires it to be stable across every replay. Generating it anew is the classic determinism bug:
user code seeding an RNG from it gets different results between two turns, and the journal no
longer matches.

**Retries are scheduled, not looped in place.** Looping in place means calling the SDK again
immediately with exactly the error that just happened — turning their incident into our attack.
Exponential backoff from 100ms up to a 60s cap; the SDK's `next_retry_delay` overrides it for
exactly one attempt.

**The next attempt is scheduled BEFORE running again.** If we die midway there is still a future
attempt scheduled, so an invocation never gets stuck in "should have run again but nobody
called". The price is at-least-once retries — exactly what we want for retries.

**Results in the outbox are keyed by the *waiter*, not by the invocation returning them.** Several
waiters may wait on the same invocation. Keyed by the returning side, two waiters both carrying
completion id 2 would overwrite each other, and one would lose its result forever.

**An outgoing call is not resent once a receipt exists.** The caller replays on *every* turn
while waiting for the result. If the task sending the call were tied to the *result* completion,
every replay would send one more call — the target service runs several times, and no error ever
fires. The right receipt is the **invocation id** completion.

## Beyond the first version

The first version of this crate drove only `REQUEST_RESPONSE`, on one partition, in one process.
What it listed as missing has since been built:

- **`BIDI_STREAM`** over full-duplex HTTP/2 (`invoker::open_bidi`); the transport is chosen per
  deployment at registration (`--deploy-transport`).
- **`OneWayCall.invoke_time`** — a delayed one-way call waits in the outbox until it is due
  (`cairn-engine/src/engine/effects.rs`).
- **Signals** — `SendSignal` writes a signal-outbox row in the same commit as its journal entry;
  the drainer delivers it to the target invocation.
- **Durable deployment registration** — the registry lives on the bucket
  (`cairn_store::DeploymentStore`); `src/registry.rs` is only the in-RAM cache, reloaded on
  refresh.
- **Ids unique across nodes** — an invocation id is 24 bytes: the 8-byte partition key followed
  by 16 bytes, so it names the range it lives in (`docs/key-design.md`).
- **Multiple partitions** — the key space is a table of contiguous ranges, each with one writer,
  that can be split and merged (`src/split/`, `docs/guides/split-merge.md`).
