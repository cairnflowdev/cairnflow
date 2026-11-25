# cairn-runtime

The journal and the invocation state machine. Implements sections 6 and 8 of
[`docs/protocol-v1.md`](../../docs/protocol-v1.md).

## Architecture decision: a pure state machine

`InvocationMachine` takes an `Event` and returns `Effect`s. No storage, no
network, no clock, no randomness.

That is not a matter of taste. Mandatory test number 1 of section 10 is *"kill at
every command index, replay, confirm the journal and output are identical"* — it
only runs if rebuilding the machine from a journal is a cheap and deterministic
function call. Put storage in here and that test becomes an infrastructure
exercise, and nobody will run it at **every** index.

## Two contracts the driver must keep

1. **`Persist` before `Send`.** The machine emits effects in exactly that order;
   the driver must finish committing the persist part before sending. Sending
   before durability promises the SDK something not yet certain — the same kind
   of mistake as acking before `await_durable` (INV-5).
2. **`Task` must be idempotent.** When an invocation restarts, the machine
   re-issues tasks for every pending completion; it does not know how far the old
   task got. For `Invoke`, idempotency comes from `idempotency_key` (spec section
   8.6).

## Two index systems, do not mix them up

| | Counts | Used by |
|---|---|---|
| *entry index* | Commands **and** notifications | `StartMessage.known_entries`, replay order |
| *command index* | Commands only | `CommandAckMessage.command_index`, `ErrorMessage.related_command_index` |

Mixing them up is a silent bug: an ack points at the wrong entry, and it only
shows when notifications sit between commands — that is, every invocation with a
`ctx.sleep`.

## How spec section 8.2 is solved

The journal **always** stores `RunCompletionNotification`. Only what goes onto the
wire changes by phase: PROCESSING sends an ack (if the SDK asked), REPLAY sends
that very notification. Since both occupy **the same journal position**, the
notification stream on replay lines up exactly with the first run — no separate
ordering rule needed.

## Three things the tests caught, worth remembering

- **A completion arriving after the invocation closed is NOT a protocol
  violation.** It comes from the runtime's own background task (a scheduled
  timer), not from the SDK. Section 8.3 only forbids the SDK from sending more.
  Mistaking it for a violation would overwrite the real reason the invocation
  stopped.
- **A `ctx.run` that dies between the command and its result MUST run the side
  effect again.** A journal with the command but no result means the result is
  unknown. This is at-least-once, the correct semantics.
- **`End` is not a journal entry** — see section 10 of the spec.

## Not done in this crate

- The driver connecting `Effect` to `cairn-store` and HTTP.
- `Call`/`OneWayCall`/`SendSignal` produce the right `Task`, but routing is not
  here.
- A suspension only stops the invocation; waking it again is the driver's job.
- `Input` (entry 0) is written by the invocation-creation path, not the state
  machine.
