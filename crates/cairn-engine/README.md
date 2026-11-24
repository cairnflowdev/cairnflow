# cairn-engine

Driver: connects the state machine ([`cairn-runtime`](../cairn-runtime)) to the storage layer
([`cairn-store`](../cairn-store)).

The pure state machine can only *describe* two contracts. This crate **enforces** them.

## Contract 1 — durable before sending

Everything of a step that must be durable is gathered into **one** `commit`; nothing is sent
down to the SDK until that commit is done. Sending before it is durable promises the SDK
something not yet certain — the same class of mistake as acking before `await_durable`, which
was measured early on (INV-5).

## Contract 2 — pending work lives in the same commit as the journal entry that produced it

This is the place where it is easy to think it is excess caution, but it is not.

The state machine does **not** replay `WriteState` on replay — it only replays tasks for
*pending completions*. So if `SetState` became durable in a different commit than its journal
entry, a death exactly between the two commits would leave the journal saying *"set"* and the
store saying *"not yet"*, and **no replay can fix it**. In the same commit it is either both or
neither.

That applies to all pending work: `state`, `timers`, `outbox`, `invocation_status`.

Timers have one more direction: the timer row is deleted in the **same commit** as the
`SleepCompletion` it produced. Split apart, a death exactly between the two commits would make
the timer fire again forever, and every refire is a second notification for the same
completion id — which the service protocol forbids.

## `ReadState` runs after the commit, not before

Reads need not be durable, but they must run **after** the current step's commit, so that they
see that step's own mutations too. Reading before is reading a state older than the journal.

## Tests

| Test | What it forces |
|---|---|
| `hello_durable` | `ctx.run` + `ctx.sleep` + state run correctly; a fired timer disappears from the table |
| **`crash_at_every_step_then_restart`** | **The core durability criterion** — die at every step, restart from real storage, identical journal and result |
| `pending_timer_survives_restart` | INV-3: a pending timer lives in the state table, not in RAM |
| `state_and_journal_durable_in_one_commit` | Contract 2 |
| `journal_with_holes_is_rejected` | A journal with holes is broken data, not "temporarily missing" |

`cairn-runtime` also has a "die at every index" test, but it runs on an in-RAM journal. The two
do not replace each other: that one covers **every** index very cheaply, this one also forces
the storage layer and the commit order.

## Beyond the first version

The first version of this crate was driven only by tests pushing `Event`s straight into the
engine. What it listed as missing has since been built, mostly around it in `cairn-server`:

- **HTTP ingress** — `cairn-server/src/ingress/`.
- **`Call` routing** — the drainer consumes the outbox (`cairn-server/src/drainer.rs`). The outbox
  row format is a versioned binary encoding with the version byte first ([`outbox`](src/outbox.rs)),
  so rows written before a binary upgrade stay readable after it.
- **Timer loop** — each range's background tick fires due timers in bounded batches
  (`cairn-server/src/turn/timers.rs`).
- **Waking from suspension** — a completion arriving for a `suspended` invocation hands it a new
  turn (the drainer's wake path).
- **Retry** — `FailureAction::Retry` leaves a retry row; the background tick reruns it with
  backoff, through the lifecycle core (`crates/cairn-lifecycle`).
