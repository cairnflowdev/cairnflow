# ai-pipeline — a model-driven pipeline, multi-tenant

A full example, and it exists to answer **one** question:

> What can cairn do that a task queue cannot?

The setting is the workload cairn targets: every running invocation **is money**.
Six research sections = six model calls. A load spike here does not slow the system
down — it drives the bill up. That is why the first item in the list below is the cap,
not the journal.

```
POST /scope/acme/invoke/ResearchWorkflow/run/send?key=job-1
        │
        ├── fan-out 6 × Researcher.dig      ← cap acme/llm = 2 holds here
        │      └── ctx.send → Ledger/acme   ← EXCLUSIVE, serialized per tenant
        ├── ctx.run("draft")
        ├── ⏸  race(promise "approval", sleep 30s)
        │      └── POST /scope/acme/promises/.../approval/resolve   ← a human clicks
        └── publish → index → notify
               └── permanent failure ⇒ compensate backwards: deindex, unpublish, refund
```

## Run

```sh
cargo build --bin cairnflow                        # at the repo root
./target/debug/cairnflow --data-dir ./data --port 8080 --no-auth   # dev: no API key

cd sdk/typescript
node example/ai-pipeline/app.ts                    # SDK on :9080
node example/ai-pipeline/demo.ts                   # the walkthrough, ~2 minutes
```

`demo.ts` is a **plain HTTP client** — it imports nothing from the SDK. Everything it does
can be done with `curl`; see [`docs/admin-api.md`](../../../docs/admin-api.md).

Each run uses a new key suffix (`job-1-<run>`), because **a workflow id runs exactly
once**: calling `run` a second time for the same key gets `409`, not the old result. Set
`DEMO_RUN=abc` to choose it by hand.

## Six capabilities, and where they live

| Capability | Where | Why it is hard without a runtime |
|---|---|---|
| **Per-tenant concurrency cap** | `ctx.scoped(tenant, "llm")` + `POST /rules` | The only valve that closes **before** money leaves the account. An in-process semaphore dies with the process |
| **Durable fan-out** | `ctx.all([...ctx.call])` | Six parallel calls, wait for all, and dying midway wakes up at the right spot — none runs again |
| **A human gate** | `ctx.promise("approval")` racing `ctx.sleep(30s)` | Wait three days without holding a process, a connection, or a single line of RAM. The timer lives in the store |
| **Saga** | an `undo[]` array in a `try/catch` | No saga API — just code, and it runs to the end because the `catch` block is as durable as the `try`. Even when [cancelled](#two-kinds-of-cancel-and-only-one-gives-the-money-back) |
| **Giving up deliberately** | `ctx.attempt` + `RunFailure` | Retrying forever is the right default for transient errors and wrong for a permanently broken API |
| **Serialization per key** | virtual object `Ledger`, `EXCLUSIVE` handlers | Six parallel `charge`s add up correctly, with no lock, no transaction, no `SELECT … FOR UPDATE` |

## Five details worth reading closely in `app.ts`

**`approval.order() < deadline.order()`, do not use `.ready()`.** An approver clicking one
second late still puts a completion in the journal, and from then on `ready()` says "yes"
on **every later replay** — the same code giving two different answers. A position in the
journal cannot change, so `order()` is the only deterministic comparison.

**Compensation steps are registered before forward steps.** Dying right in the middle of
`publishToCdn` can happen; then what we need is an undo command already in the list, not a
`url` nobody got to return yet. That is also why the undo functions take `ctx.key` rather
than a `url`/`handle`.

**Compensate only on `TerminalError`.** A transient error is not an outcome yet — the
runtime will retry, and taking the article down while waiting for the retry would be
wrecking with our own hands what is about to finish. The `notify` step turns a `410 Gone`
into a `RunFailure`: **which errors are permanent is your code's decision**, not the remote
client library's.

**`ctx.scoped(tenant, "llm")` here is not there to set the scope.** The scope is
*inherited* from the caller anyway — the workflow already runs in the tenant's scope. It is
there to set the **limit key**, and a limit key must come with a scope.

**Every step in `undo[]` is `ctx.run` or `ctx.send` — never `ctx.call`.** A cooperative
cancel enters the `catch` block through exactly the door of a permanent failure, and from
that moment every *waiting* command throws again immediately. A
`ctx.call("Ledger","refund")` inside `catch` would die at its first `await` — the
compensation stops right before the refund step. The price of `ctx.send` is that it has no
ordering: a `charge` in flight may arrive *after* the refund command. The place that closes
that door is `Ledger.charge`, with the `closed[job]` ledger — the same object, the same
`EXCLUSIVE` queue as the spending, so there is no race window.

## The seven scenes of `demo.ts`

1. Register the deployment, and see the three services appear with each handler's `ty`.
2. `POST /rules {"pattern": "acme/llm", "concurrency": 2}` — then wait ~2 seconds, because
   the rule book polls periodically. Compare `version` at `/rules` with `rulesVersion` at
   `/scheduler` to see that delay in numbers.
3. **Happy path.** Watch through the `SHARED` handler while `run` runs, approve with one
   HTTP request, then `attach`. `/vqueues` shows six `dig` turns going through exactly the
   queue `acme/[llm]/Researcher` — the limit key reached the right counter — and the cap
   holding back **2/6 turns**: six sections asked to run, two held a slot.
4. **Saga.** `chaos: "notify-down"` ⇒ the last step fails permanently. Check the ledger
   before/after: no trace of that job is left.
5. **A flaky vendor — a controlled measurement.** Two jobs, the same section list, differing
   in exactly one variable: one has a flaky vendor (`chaos: "vendor-flaky"`, each section
   fails twice and then passes), one does not. Compare **the bill in the ledger**, not
   promises: equal to the cent. What retries take away is **latency**, not money — a
   durable result is never charged twice, and that is what `ctx.run` buys you.
6. **`429`, and the cap holding on the HTTP path.** Scope `burst` with a cap of 1, fire 5
   calls at once: 4/5 get `429`, and the queue's `blockedTimes` climbs. A `429` in cairn
   **is not a "refusal"**: the invocation is durable, already queued, already scheduled to
   run, and the body still carries the `id`. At the end of the scene the `burst` rule is
   deleted — a rule that outlives the scene that set it would make the next scene measure
   its shadow.
7. **Introspection, cancellation, and the price of a fan-out.** Measure the same call
   (`Ledger/spend`) while the partition is idle and while a fan-out is draining; then cancel
   the measuring job with `?mode=signal` and check the ledger. Finally the walkthrough
   accounts for its own time — see below.

### Two kinds of cancel, and only one gives the money back

`POST /invocations/{id}/cancel` is a **hard** cancel by default (`572`): the runtime sets a
terminal state and that is it. User code never gets to run one more turn, so **the saga
does not run** — the journal of a job cancelled this way stops right at the last `Call`
command, and the money already spent on the research sections stays on the ledger. That is
a deliberate trade-off: the hard cancel does not need the SDK alive, so it is the fallback
when the SDK is dead.

`?mode=signal` is a **cooperative** cancel: the runtime delivers a CANCEL signal, every
pending future throws `TerminalError`, and the handler runs exactly its `catch` block. The
journal of the measuring job in scene 7 says so:

```
… 3 Call · 4 Call · 5,6 CallInvocationIdCompletion   ← fan-out waiting
   7 Signal          ← CANCEL arrives
   8 SetState        ← stage = rolling-back
   9 OneWayCall      ← Ledger/refund
  10 SetState · 11 Output
```

Two conditions for it to work right, and both are in `app.ts`: every compensation step must
be a command that **waits for no one** (`ctx.run`/`ctx.send`), and `Ledger.charge` must know
to **refuse** spending that arrives after the refund command (`closed[job]`) — because at
cancel time, the `dig`s in flight still record their spending.

## How long the walkthrough takes, and why

~2 minutes, and scene 7 accounts for it in its own numbers:

```
62 invocation runs (/invoke, /attach, /resolve, /cancel): 111s — 1785ms on average
 7 store reads (GET /invocations, /vqueues…):                0s —   59ms on average
whole walkthrough: 119s, of which 93% is on the run path
```

The gap between the two lines is the whole story: an `/invoke` has to wake the state
machine, run an SDK turn, then wait for the invocation to stop for good
(`--invoke-wait-ms`, default 5s) — on the order of **seconds**; a `GET` only reads the
store — on the order of **milliseconds** (a few ms when idle; the average above is higher
because most reads land exactly when things are busy). This walkthrough calls a `SHARED`
handler in a loop to track progress, so every poll is one more invocation. It does so on
purpose (that is how it proves `SHARED` runs in parallel with `run`, and it is also the
shape that found the `PROTOCOL_VIOLATION` bug below), but **a real client should not**: poll
`/invocations/{id}`.

There used to be another, much bigger cause, and it is gone now: the outbox drain loop held
**the whole partition's lock** and ran a whole SDK turn for each child right inside it, so
an N-branch fan-out held the entrance for about N turns — `Ledger/spend` used to slow down
**~14×** while a fan-out was draining. After *emitting* was separated from *running*, scene
7 measures **~1.5×**. That number is printed on every run; if it climbs again, something has
regressed.

## Worth trying next

```sh
# 1. Kill the runtime while a job is running, then start it again. There is no recovery
#    step to do. Side effects ALREADY done do not run again — `ctx.run` waits for the
#    runtime to confirm the write before moving on — while the one in flight when it was
#    killed does, because nobody knows how far it got. That is the at-least-once
#    boundary, and it is unavoidable.
pkill -9 cairnflow && ./target/debug/cairnflow --data-dir ./data --port 8080 --no-auth   # dev: no API key

# 2. Fire a job and DO NOT approve it. It stays suspended for the full 30 seconds holding
#    nothing at all, then compensates by itself. Change the deadline with
#    DEMO_APPROVAL_TIMEOUT_MS.

# 3. Raise the cap and run again: same code, different bill per second.
#    `-H content-type` is required — the runtime rejects bodies not declared as JSON.
curl -X POST localhost:8080/rules -H 'content-type: application/json' \
     -d '{"pattern":"acme/llm","concurrency":6}'

# 4. Drop `?mode=signal` in scene 7 and run again: the same job stops, but the saga does
#    not run and the money stays on the ledger. See "Two kinds of cancel" above.
```

## Two runtime bugs this example found (fixed)

**`571 PROTOCOL_VIOLATION: completion N already has a notification`.** A client looping on
both `POST /workflow/{svc}/{key}/attach` and `POST /invoke/{svc}/status` while `run` was
fanning out made `run` die — 8 times out of 8. Cause: the state machine replayed a
`Task::PromiseGet` for a completion still pending after replay
(`crates/cairn-runtime/src/machine.rs`, `replay_task_for`), the promise already had a result
by then, so the notification was produced *twice* — once from the replay path, once from
the outbox row that `complete_promise_external` had queued. Fixed in `Engine::deliver`: the
only place every completion-loading path goes through. Regression test:
`crates/cairn-engine/tests/phase4.rs`.

**Cancellation did not propagate to children.** Cancelling this workflow mid-fan-out left
every `Researcher.dig` running to the end — each one a model call whose result nobody would
read anymore. Fixed in two places: `Engine::cancel` queues a cancel for the children **still
running**, and the outbox drain loop **does not emit** a `ctx.call` whose caller has stopped
for good. `ctx.send` still runs — it is detached work. Regression test:
`crates/cairn-server/tests/cancel_tree.rs`.

Measured on this very example (6 sections, `DEMO_MODEL_LATENCY_MS=3000`, cancelled at the
fourth second): before the fix, all 6 model calls still ran to completion; after the fix,
only 1 — the one in flight inside `ctx.run`. An admin-level cancel **does not cut a running
SDK turn** (exactly as `admin-api.md` says), so that one runs to the end; the other five
never start.
