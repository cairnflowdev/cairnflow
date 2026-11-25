// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * The SDK side of the `BIDI_STREAM` turn loop — `runBidi` in `handler.ts`.
 *
 * No socket here, and that is the point: [`FrameStream`] is exactly as narrow as
 * "frames in, bytes out", so a whole conversation can be built from two arrays.
 * The wiring to `node:http2` is `serve.ts`'s job and only the e2e tests measure it.
 *
 * What these tests guard is something only `BIDI_STREAM` has: the runtime does
 * **not** resend the journal on the second turn on the same stream, so the SDK's
 * local journal is the only source of truth. One wrong entry there and a command
 * is emitted again, or a side effect runs twice.
 */

import assert from "node:assert/strict";
import { test } from "node:test";
import {
  type Context,
  FrameSplitter,
  type Frame,
  type FrameStream,
  type Handler,
  MT,
  runBidi,
} from "../src/index.ts";
import { encodeFrame } from "../src/wire.ts";
import { eachField, Writer } from "../src/protobuf.ts";

const enc = new TextEncoder();
const dec = new TextDecoder();

function start(knownEntries: number, key = "", seed = 42n): Uint8Array {
  return encodeFrame(
    MT.START,
    0,
    new Writer().uint32(3, knownEntries).string(6, key).uint64(9, seed).finish(),
  );
}

function inputEntry(value: unknown): Uint8Array {
  return encodeFrame(
    MT.INPUT,
    0,
    new Writer().message(14, new Writer().bytes(1, enc.encode(JSON.stringify(value)))).finish(),
  );
}

function valueNotification(type: number, completionId: number, value: unknown): Uint8Array {
  return encodeFrame(
    type,
    0,
    new Writer()
      .uint32(1, completionId)
      .message(5, new Writer().bytes(1, enc.encode(JSON.stringify(value))))
      .finish(),
  );
}

/**
 * `ProposeRunCompletionAck` — the runtime says "your proposal has been written".
 *
 * Only a completion id, no value: the value lives in the SDK, which just computed it.
 */
function runAck(completionId: number): Uint8Array {
  return encodeFrame(
    MT.PROPOSE_RUN_COMPLETION_ACK,
    0,
    new Writer().uint32(1, completionId).finish(),
  );
}

function voidNotification(type: number, completionId: number): Uint8Array {
  return encodeFrame(
    type,
    0,
    new Writer().uint32(1, completionId).message(4, new Writer()).finish(),
  );
}

/**
 * The other end of the stream, driven by the test.
 *
 * Deliberately answers **nothing** on its own: each test decides what the runtime
 * pushes down and when. A fake runtime that knows things would hide exactly the spots we want to inspect.
 */
class Wire implements FrameStream {
  /** Frames the SDK sent up, in order. */
  readonly sent: Frame[] = [];
  /** Whether the SDK has closed its side. */
  ended = false;

  private readonly inbox: Frame[] = [];
  private readonly split = new FrameSplitter();
  private waiter: ((f: Frame | null) => void) | null = null;
  private closed = false;

  /** The runtime pushes bytes down. */
  push(...parts: Uint8Array[]): void {
    for (const p of parts) {
      for (const f of this.split.push(p)) {
        const w = this.waiter;
        if (w !== null) {
          this.waiter = null;
          w(f);
        } else {
          this.inbox.push(f);
        }
      }
    }
  }

  /** The runtime closes the stream — its way of saying "the turn is over, waiting on the outside world". */
  close(): void {
    this.closed = true;
    const w = this.waiter;
    if (w !== null && this.inbox.length === 0) {
      this.waiter = null;
      w(null);
    }
  }

  next(): Promise<Frame | null> {
    const head = this.inbox.shift();
    if (head !== undefined) return Promise.resolve(head);
    if (this.closed) return Promise.resolve(null);
    return new Promise((resolve) => {
      this.waiter = resolve;
    });
  }

  buffered(): Frame[] {
    return this.inbox.splice(0, this.inbox.length);
  }

  write(bytes: Uint8Array): void {
    for (const f of new FrameSplitter().push(bytes)) this.sent.push(f);
  }

  end(): void {
    this.ended = true;
  }
}

/** Waits until the condition holds — the SDK runs on microtasks, not on a clock. */
async function until(what: string, cond: () => boolean, tries = 500): Promise<void> {
  for (let i = 0; i < tries; i++) {
    if (cond()) return;
    await new Promise((r) => setTimeout(r, 1));
  }
  throw new Error(`timed out waiting for: ${what}`);
}

function types(frames: readonly Frame[]): number[] {
  return frames.map((f) => f.type);
}

// ─────────────────────────────────────────────────────────── test

test("a suspension resolvable in place: runs again on the same stream, without receiving the journal again", async () => {
  const h: Handler = async (ctx: Context) => {
    const n = (await ctx.get<number>("count")) ?? 0;
    return { n: n + 1 };
  };
  const wire = new Wire();
  const done = runBidi(h, wire);
  wire.push(start(1), inputEntry(null));

  await until("SDK suspended", () => wire.sent.length >= 2);
  assert.deepEqual(types(wire.sent), [MT.GET_STATE, MT.SUSPENSION]);
  assert.equal(wire.ended, false, "suspended but already closed — nowhere left to receive the notification");

  // The runtime can resolve it at once: pushes a notification, does NOT resend Start or the journal.
  wire.push(valueNotification(MT.GET_STATE_COMPLETION, 1, 41));

  await done;
  assert.deepEqual(types(wire.sent), [MT.GET_STATE, MT.SUSPENSION, MT.OUTPUT, MT.END]);
  assert.equal(wire.ended, true);

  const output = wire.sent[2]!;
  assert.match(dec.decode(output.payload), /"n":42/);
});

test("waiting on the outside: the runtime closes the stream ⇒ the turn is over, nothing more is emitted", async () => {
  const h: Handler = async (ctx: Context) => {
    await ctx.sleep(3_600_000);
    return "never reached in this turn";
  };
  const wire = new Wire();
  const done = runBidi(h, wire);
  wire.push(start(1), inputEntry(null));

  await until("SDK suspended on a timer", () => wire.sent.length >= 2);
  assert.deepEqual(types(wire.sent), [MT.SLEEP, MT.SUSPENSION]);

  wire.close();
  await done;
  assert.deepEqual(types(wire.sent), [MT.SLEEP, MT.SUSPENSION], "emitted more after the stream closed");
  assert.equal(wire.ended, true);
});

test("ctx.run suspends waiting for the ack, and the rerun does NOT rerun the side effect", async () => {
  // Two properties in one test, and they are two sides of the same decision:
  //
  // 1. `ctx.run` does NOT continue after proposing — it suspends. Continuing would be
  //    trusting a result nobody has guaranteed is written yet, and that window extends
  //    to the next suspension point: three chained `ctx.run`s dying at the third would
  //    rerun all three.
  // 2. After the ack arrives, the rerun reads the result out of the local journal
  //    instead of calling `fn` again.
  let charges = 0;
  const h: Handler = async (ctx: Context) => {
    const receipt = await ctx.run("charge", () => {
      charges++;
      return `receipt-${charges}`;
    });
    const total = (await ctx.get<number>("total")) ?? 0;
    return { receipt, total };
  };

  const wire = new Wire();
  const done = runBidi(h, wire);
  wire.push(start(1), inputEntry(null));

  await until("SDK suspended waiting for the ctx.run ack", () => wire.sent.length >= 3);
  assert.deepEqual(types(wire.sent), [MT.RUN, MT.PROPOSE_RUN_COMPLETION, MT.SUSPENSION]);
  assert.equal(charges, 1);
  assert.ok(
    wire.sent[1]!.flags !== 0,
    "the proposal must ask for an ack — otherwise the runtime echoes back the whole value",
  );

  // The ack arrived ⇒ the result is durable ⇒ the rerun may trust it.
  wire.push(runAck(1));

  await until("SDK suspended at ctx.get", () => wire.sent.length >= 5);
  assert.deepEqual(types(wire.sent).slice(3), [MT.GET_STATE, MT.SUSPENSION]);
  assert.equal(charges, 1, "the side effect reran on the second turn on the same stream");

  wire.push(valueNotification(MT.GET_STATE_COMPLETION, 2, 7));

  await done;
  assert.equal(charges, 1);
  assert.deepEqual(types(wire.sent).slice(5), [MT.OUTPUT, MT.END]);
  assert.match(dec.decode(wire.sent[5]!.payload), /"receipt-1"/);
});

test("the runtime echoing back a RunCompletion instead of an ack also works", async () => {
  // The runtime may return the full notification instead of an ack (when the SDK did
  // not ask for an ack, or an older runtime). That path must lead to exactly one place:
  // one entry, one completion id, `fn` not run again. Two copies with the same key are
  // merged into one — if they became two, `ctx.race` would pick a different winner on replay.
  let charges = 0;
  const h: Handler = async (ctx: Context) => {
    const receipt = await ctx.run("charge", () => `receipt-${++charges}`);
    const total = (await ctx.get<number>("total")) ?? 0;
    return { receipt, total };
  };
  const wire = new Wire();
  const done = runBidi(h, wire);
  wire.push(start(1), inputEntry(null));

  await until("SDK suspended waiting for the ack", () => wire.sent.length >= 3);
  // Both at once: the ack AND the full notification. Merged into one.
  wire.push(runAck(1), valueNotification(MT.RUN_COMPLETION, 1, "receipt-1"));

  await until("SDK suspended at ctx.get", () => wire.sent.length >= 5);
  wire.push(valueNotification(MT.GET_STATE_COMPLETION, 2, 7));

  await done;
  assert.equal(charges, 1);
  assert.deepEqual(types(wire.sent).slice(5), [MT.OUTPUT, MT.END]);
  assert.match(dec.decode(wire.sent[5]!.payload), /"receipt-1".*"total":7/);
});

test("an ack is not a journal entry: receiving a CommandAck does not shift the indexes", async () => {
  // `CommandAck` (0x0004) and `ProposeRunCompletionAck` (0x0007) are messages, they
  // take no slot in the journal. Putting them in the local journal would shift every
  // later entry by one, and the next command would be reported as a mismatch against
  // an entry that is not its own.
  const h: Handler = async (ctx: Context) => {
    const a = (await ctx.get<number>("a")) ?? 0;
    const b = (await ctx.get<number>("b")) ?? 0;
    return a + b;
  };
  const wire = new Wire();
  const done = runBidi(h, wire);
  wire.push(start(1), inputEntry(null));

  await until("suspended at get a", () => wire.sent.length >= 2);
  wire.push(
    encodeFrame(MT.COMMAND_ACK, 0, new Writer().uint32(1, 1).finish()),
    valueNotification(MT.GET_STATE_COMPLETION, 1, 20),
  );

  await until("suspended at get b", () => wire.sent.length >= 4);
  assert.deepEqual(types(wire.sent), [
    MT.GET_STATE,
    MT.SUSPENSION,
    MT.GET_STATE,
    MT.SUSPENSION,
  ]);
  wire.push(valueNotification(MT.GET_STATE_COMPLETION, 2, 22));

  await done;
  assert.match(dec.decode(wire.sent[4]!.payload), /42/);
});

test("a signal arriving mid-stream wakes awaitSignal", async () => {
  const h: Handler = async (ctx: Context) => {
    const who = await ctx.awaitSignal<{ by: string }>("approve");
    return { by: who?.by ?? null };
  };
  const wire = new Wire();
  const done = runBidi(h, wire);
  wire.push(start(1), inputEntry(null));

  await until("suspended waiting for the signal", () => wire.sent.length >= 1);
  assert.deepEqual(types(wire.sent), [MT.SUSPENSION]);

  wire.push(
    encodeFrame(
      MT.SIGNAL,
      0,
      new Writer()
        .string(3, "approve")
        .message(5, new Writer().bytes(1, enc.encode(JSON.stringify({ by: "an" }))))
        .finish(),
    ),
  );

  await done;
  assert.deepEqual(types(wire.sent), [MT.SUSPENSION, MT.OUTPUT, MT.END]);
  assert.match(dec.decode(wire.sent[1]!.payload), /"an"/);
});

test("a first message that is not Start is a protocol violation", async () => {
  const wire = new Wire();
  const done = runBidi(async () => 1, wire);
  wire.push(voidNotification(MT.SLEEP_COMPLETION, 1));
  await done;
  assert.deepEqual(types(wire.sent), [MT.ERROR]);
  assert.equal(wire.ended, true);
});

test("the runtime closes before saying anything: the SDK leaves quietly", async () => {
  const wire = new Wire();
  const done = runBidi(async () => 1, wire);
  wire.close();
  await done;
  assert.deepEqual(wire.sent, []);
});

// ────────────────────────────────────────────────── FrameSplitter

test("FrameSplitter reassembles correctly even when bytes arrive one at a time", () => {
  const wire = encodeFrame(MT.INPUT, 0, enc.encode("hello"));
  const split = new FrameSplitter();
  const out: Frame[] = [];
  for (const b of wire) out.push(...split.push(new Uint8Array([b])));
  assert.equal(out.length, 1);
  assert.equal(dec.decode(out[0]!.payload), "hello");
  assert.equal(split.pending, 0);
});

test("FrameSplitter: half a frame means WAIT FOR MORE, not an error", () => {
  // Quite unlike `decodeFrames`: there the request body has ended, so leftover bytes
  // are a broken stream; here the stream is still open, so it is just a piece not yet arrived.
  const whole = encodeFrame(MT.END, 0, new Uint8Array());
  const half = encodeFrame(MT.OUTPUT, 0, enc.encode("half done")).subarray(0, 5);
  const split = new FrameSplitter();
  const out = split.push(new Uint8Array([...whole, ...half]));
  assert.deepEqual(types(out), [MT.END]);
  assert.equal(split.pending, 5, "the incomplete part must stay intact");
});

test("FrameSplitter rejects a lying length right at the header", () => {
  const head = new Uint8Array(8);
  new DataView(head.buffer).setUint16(0, MT.INPUT, false);
  new DataView(head.buffer).setUint32(4, 0xffff_ffff, false);
  assert.throws(() => new FrameSplitter().push(head), /exceeds the limit/);
});

/** `SignalNotification` CANCEL (idx 1, void) — as the runtime delivers a cooperative cancel. */
function cancelSignal(): Uint8Array {
  return encodeFrame(MT.SIGNAL, 0, new Writer().uint32(2, 1).message(4, new Writer()).finish());
}

/** `OutputCommandMessage.failure.code` (field 15 → field 1), 0 if not a failure. */
function failureCode(f: Frame): number {
  let code = 0;
  eachField(f.payload, (field, _w, r) => {
    if (field !== 15) return false;
    eachField(r.bytes(), (g, _w2, s) => {
      if (g !== 1) return false;
      code = Number(s.varint());
      return true;
    });
    return true;
  });
  return code;
}

test("CANCEL arriving inside a cancellable closure: the closure gets the signal, the compensation step still runs, the rerun stops at the same spot", async () => {
  // 1. First turn: `charge` (cancellable) runs, not finished yet.
  // 2. The runtime pushes CANCEL down the open stream — mid-turn, the SDK has not suspended.
  // 3. The closure gets `abort`, throws; `ctx.run` throws 409 and proposes nothing.
  // 4. `catch` runs `refund` (not cancellable) — the saga's compensation step.
  // 5. The `refund` ack arrives ⇒ rerun on a journal with CANCEL: `charge` (a Run
  //    without a result) throws 409 at once, without running the closure; `refund` comes from the journal.
  // Must see: the `charge` closure runs once and is interrupted, `refund` runs once,
  // ending with an Output failure 409.
  let started = 0;
  let aborted = 0;
  let undone = 0;
  const h: Handler = async (ctx: Context) => {
    try {
      await ctx.run(
        "charge",
        (signal) =>
          new Promise((_, rej) => {
            started++;
            signal.addEventListener("abort", () => {
              aborted++;
              rej(new Error("interrupted"));
            });
          }),
        { cancellable: true },
      );
      return "unreachable";
    } catch (e) {
      await ctx.run("refund", () => ++undone);
      throw e;
    }
  };
  const wire = new Wire();
  const done = runBidi(h, wire);
  wire.push(start(1), inputEntry(null));
  await until("closure charge running", () => started === 1);
  wire.push(cancelSignal());
  await until("SDK suspended waiting for the refund ack", () => wire.sent.at(-1)?.type === MT.SUSPENSION);
  assert.deepEqual(types(wire.sent), [MT.RUN, MT.RUN, MT.PROPOSE_RUN_COMPLETION, MT.SUSPENSION]);
  assert.equal(aborted, 1);
  wire.push(runAck(2));
  await done;
  assert.deepEqual(types(wire.sent).slice(4), [MT.OUTPUT, MT.END]);
  assert.equal(failureCode(wire.sent[4]!), 409);
  assert.deepEqual([started, aborted, undone], [1, 1, 1]);
});

test("a cancellable ctx.run on a journal that already has CANCEL: the closure does not run, 409; a non-cancellable one runs", async () => {
  const ran: string[] = [];
  const h: Handler = async (ctx: Context) => {
    await ctx.run("undo", () => ran.push("undo"));
    await ctx.run("forward", () => ran.push("forward"), { cancellable: true });
    return "unreachable";
  };
  const wire = new Wire();
  const done = runBidi(h, wire);
  wire.push(start(2), inputEntry(null), cancelSignal());
  await until("SDK suspended waiting for the undo ack", () => wire.sent.at(-1)?.type === MT.SUSPENSION);
  wire.push(runAck(1));
  await done;
  assert.deepEqual(ran, ["undo"]);
  assert.deepEqual(types(wire.sent).slice(-3), [MT.RUN, MT.OUTPUT, MT.END]);
  assert.equal(failureCode(wire.sent.at(-2)!), 409);
});

test("parallel ctx.run (Promise.all): a slow closure finishing after the other branch suspended is still proposed in that turn, not rerun, and no frame goes on the wire twice", async () => {
  // The interleaving (reproduced on the binary: BIDI ⇒ `failed 571`, the closure ran twice):
  // 1. `Run a` and `Run b` are emitted; closure a is slow (60 ms), closure b finishes at once.
  // 2. b proposes its result and suspends waiting for the ack ⇒ `Promise.all` rejects, the handler throws.
  // 3. Old bug: the turn ended at once (Suspension waiting on b) while closure a was still running.
  //    b's ack arrived ⇒ the next turn's replay met `Run a` without a completion ⇒ reran a.
  //    The keepalive's early flush tick (old turn) resent the whole `out` because `flushed`
  //    did not advance ⇒ the runtime saw a `Run` with a duplicate completion id ⇒ 571.
  // Must see: a's result goes before that turn's Suspension; the next turn starts only
  // when **every** proposal is acked; each closure runs once; each `Run` once.
  let runsA = 0;
  let runsB = 0;
  const h: Handler = async (ctx: Context) => {
    const [a, b] = await Promise.all([
      ctx.run("a", async () => {
        runsA++;
        await new Promise((r) => setTimeout(r, 60));
        return 1;
      }),
      ctx.run("b", () => {
        runsB++;
        return 2;
      }),
    ]);
    return a + b;
  };
  const wire = new Wire();
  const done = runBidi(h, wire);
  // `keepalive_ms` (field 10) turns on the early flush tick — the mid-turn write path.
  wire.push(
    encodeFrame(MT.START, 0, new Writer().uint32(3, 1).uint64(9, 42n).uint32(10, 30000).finish()),
    inputEntry(null),
  );
  const real = () => types(wire.sent).filter((t) => t !== MT.KEEPALIVE);

  await until("turn suspended", () => real().includes(MT.SUSPENSION));
  assert.deepEqual(real(), [
    MT.RUN,
    MT.RUN,
    MT.PROPOSE_RUN_COMPLETION,
    MT.PROPOSE_RUN_COMPLETION,
    MT.SUSPENSION,
  ]);

  // b's ack arrives first: the next turn must not start while a's proposal is not durable.
  wire.push(runAck(2));
  await new Promise((r) => setTimeout(r, 30));
  assert.equal(runsA, 1, "closure a reran while its proposal was not acked");
  assert.equal(real().length, 5);

  wire.push(runAck(1));
  await done;
  assert.equal(runsA, 1);
  assert.equal(runsB, 1);
  assert.deepEqual(real().slice(5), [MT.OUTPUT, MT.END]);
  assert.match(dec.decode(wire.sent.filter((f) => f.type === MT.OUTPUT)[0]!.payload), /3/);
});
