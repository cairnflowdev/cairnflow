// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

import assert from "node:assert/strict";
import { test } from "node:test";
import { type Context, type Handler, runTurn, TerminalError } from "../src/index.ts";
import { eachField, Writer } from "../src/protobuf.ts";
import { describeJournal, driveRR, Ledger, Runtime } from "./parallel-sim.ts";
import {
  MT,
  decodeFrames,
  encodeFrame,
  encodeGetEagerStateKeys,
  encodeSetState,
  encodeSleep,
  type Frame,
  JOURNAL_MISMATCH,
} from "../src/wire.ts";

// ── Builds the byte stream the runtime would send. The SDK side has no encoder for these
//    messages (they only travel one way), so the test builds them itself.

const enc = new TextEncoder();

function start(knownEntries: number, key = "", seed = 42n): Uint8Array {
  return encodeFrame(
    MT.START,
    0,
    new Writer().uint32(3, knownEntries).string(6, key).uint64(9, seed).finish(),
  );
}

function inputEntry(value: unknown): Uint8Array {
  const payload = new Writer()
    .message(14, new Writer().bytes(1, enc.encode(JSON.stringify(value))))
    .finish();
  return encodeFrame(MT.INPUT, 0, payload);
}

function commandEntry(type: number, payload = new Uint8Array()): Uint8Array {
  return encodeFrame(type, 0, payload);
}

/** `GetState` exactly as the SDK emits it — replay compares the target, key and completion id. */
function getStateEntry(key: string, completionId: number): Uint8Array {
  return encodeFrame(MT.GET_STATE, 0, new Writer().bytes(1, enc.encode(key)).uint32(11, completionId).string(12, key).finish());
}

/** `Sleep` (arbitrary wake-up time — not compared on replay). */
function sleepEntry(completionId: number, name = "sleep"): Uint8Array {
  return encodeFrame(MT.SLEEP, 0, new Writer().uint64(1, 1n).uint32(11, completionId).string(12, name).finish());
}

function runEntry(completionId: number, name: string): Uint8Array {
  return encodeFrame(MT.RUN, 0, new Writer().uint32(11, completionId).string(12, name).finish());
}

/** `ErrorMessage.code` (field 1). */
function errorCode(payload: Uint8Array): number {
  let code = 0;
  eachField(payload, (field, _wire, r) => {
    if (field !== 1) return false;
    code = Number(r.varint());
    return true;
  });
  return code;
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

function voidNotification(type: number, completionId: number): Uint8Array {
  return encodeFrame(
    type,
    0,
    new Writer().uint32(1, completionId).message(4, new Writer()).finish(),
  );
}

function stream(...parts: Uint8Array[]): Uint8Array {
  const total = parts.reduce((n, p) => n + p.length, 0);
  const out = new Uint8Array(total);
  let at = 0;
  for (const p of parts) {
    out.set(p, at);
    at += p.length;
  }
  return out;
}

async function turn(handler: Handler, ...parts: Uint8Array[]): Promise<Frame[]> {
  return decodeFrames(await runTurn(handler, stream(...parts)));
}

function types(frames: Frame[]): number[] {
  return frames.map((f) => f.type);
}

// ─────────────────────────────────────────────────────────── test

test("ctx.invocationId is the hex of StartMessage.id, ctx.debugId is a label for humans", async () => {
  // The `debug_id` of a scoped invocation carries a `"{scope}/"` prefix — so it
  // **cannot** serve as an id, and `invocationId` must no longer return it.
  let seen: { id: string; debug: string } | undefined;
  const h: Handler = async (ctx: Context) => {
    seen = { id: ctx.invocationId, debug: ctx.debugId };
    return null;
  };
  const startWithId = encodeFrame(
    MT.START,
    0,
    new Writer()
      .bytes(1, Uint8Array.from([0x00, 0x0a, 0xff, 0x10]))
      .string(2, "acme/000aff10")
      .uint32(3, 1)
      .finish(),
  );
  await turn(h, startWithId, inputEntry(null));
  assert.deepEqual(seen, { id: "000aff10", debug: "acme/000aff10" });
});

test("first turn: emit the command, then suspend because there is no result yet", async () => {
  const h: Handler = async (ctx: Context) => await ctx.get<number>("count");
  const out = await turn(h, start(1), inputEntry(null));
  assert.deepEqual(types(out), [MT.GET_STATE, MT.SUSPENSION]);
});

test("later turn: with the result in the journal, move on without re-emitting the command", async () => {
  const h: Handler = async (ctx: Context) => {
    const n = (await ctx.get<number>("count")) ?? 0;
    return n + 1;
  };
  const out = await turn(
    h,
    start(3),
    inputEntry(null),
    getStateEntry("count", 1),
    valueNotification(MT.GET_STATE_COMPLETION, 1, 41),
  );
  assert.deepEqual(types(out), [MT.OUTPUT, MT.END], "re-emitted a command already in the journal");
});

test("void means the key does not exist, distinct from an empty value", async () => {
  const seen: unknown[] = [];
  const h: Handler = async (ctx: Context) => {
    seen.push(await ctx.get("not-set"));
    return "ok";
  };
  await turn(
    h,
    start(3),
    inputEntry(null),
    getStateEntry("not-set", 1),
    voidNotification(MT.GET_STATE_COMPLETION, 1),
  );
  assert.deepEqual(seen, [null]);
});

test("journal mismatch reports 570 and behavior PAUSE", async () => {
  // The journal has GetState, but the new code starts with Sleep.
  const h: Handler = async (ctx: Context) => {
    await ctx.sleep(10);
    return "other";
  };
  const out = await turn(h, start(2), inputEntry(null), getStateEntry("x", 1));
  assert.deepEqual(types(out), [MT.ERROR]);

  let code = 0;
  let behavior = -1;
  const r = out[0]!.payload;
  // ErrorMessage: code=1, message=2, behavior=9
  let at = 0;
  const view = r;
  while (at < view.length) {
    const key = view[at++]!;
    const field = key >>> 3;
    const wire = key & 7;
    if (wire === 0) {
      let v = 0;
      let shift = 0;
      for (;;) {
        const b = view[at++]!;
        v |= (b & 0x7f) << shift;
        if ((b & 0x80) === 0) break;
        shift += 7;
      }
      if (field === 1) code = v;
      if (field === 9) behavior = v;
    } else if (wire === 2) {
      const len = view[at++]!;
      at += len;
    }
  }
  assert.equal(code, JOURNAL_MISMATCH, "must report 570");
  // 570 is PAUSE — not RETRY (a retry only reproduces the same error), not FAIL
  // (fixable by deploying the right version and then `resume`).
  assert.equal(behavior, 1, "570 must be PAUSE");
});

test("ctx.run runs the side effect once, and not again once the result is durable", async () => {
  let calls = 0;
  const h: Handler = async (ctx: Context) => await ctx.run("charge", () => {
    calls++;
    return "receipt";
  });

  // First turn: really runs, proposes the result, then **suspends waiting for the ack** —
  // `docs/protocol-v1.md` §4 allows a `ctx.run` to end a turn with a suspension, and that is the
  // only way for a later turn to trust this result. Returning `Output` right here would answer
  // before knowing whether the result was written.
  const first = await turn(h, start(1), inputEntry(null));
  assert.deepEqual(types(first), [MT.RUN, MT.PROPOSE_RUN_COMPLETION, MT.SUSPENSION]);
  assert.equal(calls, 1);

  // Later turn, the journal already has the result: do NOT run again.
  const second = await turn(
    h,
    start(3),
    inputEntry(null),
    runEntry(1, "charge"),
    valueNotification(MT.RUN_COMPLETION, 1, "receipt"),
  );
  assert.equal(calls, 1, "re-ran a durable side effect");
  assert.deepEqual(types(second), [MT.OUTPUT, MT.END]);
});

test("ctx.run dying between the command and the result runs again — at-least-once", async () => {
  let calls = 0;
  const h: Handler = async (ctx: Context) => await ctx.run("charge", () => {
    calls++;
    return "receipt";
  });
  // The journal has the Run command but NO result: the result is unknown ⇒ must run again.
  //
  // This is the at-least-once part that **cannot be avoided** — dying exactly while `fn` is
  // running means nobody knows how far it got. The avoidable part (dying *after*
  // `fn` finished) is what the ack closes; see bidi.test.ts.
  const out = await turn(h, start(2), inputEntry(null), runEntry(1, "charge"));
  assert.equal(calls, 1);
  assert.deepEqual(types(out), [MT.PROPOSE_RUN_COMPLETION, MT.SUSPENSION]);
});

test("ctx.run renamed relative to the journal ⇒ 570, the closure does not run", async () => {
  // A misaligned journal (see the swallowed-transient-error test below) or code renaming a step
  // between two deployments: the entry at this position is the `Run` of a **different** step. Same
  // kind, so it used to match, and the new closure reran under the old name — or the journal result
  // of the other step was returned to this one. Must report 570 like any journal mismatch, before
  // the closure.
  let calls = 0;
  const h: Handler = async (ctx: Context) => await ctx.run("charge-v2", () => {
    calls++;
    return "receipt";
  });
  for (const known of [false, true]) {
    calls = 0;
    const parts = [runEntry(1, "charge")];
    if (known) parts.push(valueNotification(MT.RUN_COMPLETION, 1, "receipt"));
    const out = await turn(h, start(1 + parts.length), inputEntry(null), ...parts);
    assert.deepEqual(types(out), [MT.ERROR], `known=${known}`);
    assert.equal(errorCode(out[0]!.payload), JOURNAL_MISMATCH, `known=${known}`);
    assert.equal(calls, 0, `known=${known}: the closure ran under a mismatched name`);
  }
});

test("ctx.run with the same name as the journal matches — guard for the name comparison", async () => {
  let calls = 0;
  const h: Handler = async (ctx: Context) => {
    const a = await ctx.run("a", () => "x");
    const b = await ctx.run("b", () => {
      calls++;
      return "y";
    });
    return [a, b];
  };
  const out = await turn(
    h,
    start(4),
    inputEntry(null),
    runEntry(1, "a"),
    valueNotification(MT.RUN_COMPLETION, 1, "x"),
    runEntry(2, "b"),
  );
  assert.equal(calls, 1);
  assert.deepEqual(types(out), [MT.PROPOSE_RUN_COMPLETION, MT.SUSPENSION]);
});

test("user code swallowing Suspend still suspends correctly", async () => {
  // A real trap: a `try/catch` around a ctx command. The turn's outcome must be decided by
  // state, not by how the exception propagates.
  const h: Handler = async (ctx: Context) => {
    try {
      await ctx.get("count");
    } catch {
      /* swallowed */
    }
    return "should-not-return-this";
  };
  const out = await turn(h, start(1), inputEntry(null));
  assert.deepEqual(types(out), [MT.GET_STATE, MT.SUSPENSION], "swallowing Suspend broke the turn");
});

test("after suspending, no further command may be emitted", async () => {
  const h: Handler = async (ctx: Context) => {
    try {
      await ctx.get("a");
    } catch {
      /* swallowed */
    }
    try {
      ctx.set("b", 1); // must not reach the journal
    } catch {
      /* swallowed */
    }
    return "x";
  };
  const out = await turn(h, start(1), inputEntry(null));
  assert.deepEqual(types(out), [MT.GET_STATE, MT.SUSPENSION]);
});

test("TerminalError becomes Output with a failure, not an Error retry", async () => {
  const h: Handler = async () => {
    throw new TerminalError("invalid order", 400);
  };
  const out = await turn(h, start(1), inputEntry(null));
  assert.deepEqual(types(out), [MT.OUTPUT, MT.END]);
});

// ── `Output` is a journal entry, `End` is not.
//
// The runtime commits frame by frame, so dying right between the `Output` commit and the
// `End` commit can happen. On the restart after that the program has no steps left:
// the SDK MUST resend `End` (otherwise the invocation hangs forever at its last step), and
// MUST NOT resend `Output` (otherwise the journal grows one entry per
// recovery).

function outputEntry(value: unknown): Uint8Array {
  const payload = new Writer()
    .message(14, new Writer().bytes(1, enc.encode(JSON.stringify(value))))
    .string(12, "output")
    .finish();
  return encodeFrame(MT.OUTPUT, 0, payload);
}

test("a durable Output only resends End, without re-emitting Output", async () => {
  const h: Handler = async () => "done";
  const out = await turn(h, start(2), inputEntry(null), outputEntry("done"));
  assert.deepEqual(
    types(out),
    [MT.END],
    "re-emitting Output makes the runtime append one more entry to the journal",
  );
});

test("a durable Output failure also only resends End", async () => {
  const h: Handler = async () => {
    throw new TerminalError("invalid order", 400);
  };
  const out = await turn(h, start(2), inputEntry(null), outputEntry("anything"));
  assert.deepEqual(types(out), [MT.END]);
});

test("Output is emitted normally when the journal does not have it yet", async () => {
  const h: Handler = async (ctx: Context) => {
    await ctx.sleep(10);
    return "done";
  };
  // The journal already has Sleep and its result ⇒ this turn runs to the end.
  const out = await turn(
    h,
    start(3),
    inputEntry(null),
    sleepEntry(1),
    voidNotification(MT.SLEEP_COMPLETION, 1),
  );
  assert.deepEqual(types(out), [MT.OUTPUT, MT.END]);
});

test("the position of Output in the journal holding another command ⇒ 570", async () => {
  // Old code: sleep, then return. New code: return at once. The position that should be Output
  // in the journal holds a Sleep — that is a journal mismatch, not an "overwrite".
  const h: Handler = async () => "done";
  const out = await turn(h, start(2), inputEntry(null), sleepEntry(1));
  assert.deepEqual(types(out), [MT.ERROR]);
  const text = new TextDecoder().decode(out[0]!.payload);
  assert.match(text, /SLEEP/);
  assert.equal(out[0]!.payload[1], JOURNAL_MISMATCH & 0x7f | 0x80, "first varint byte of 570");
});

test("an ordinary error becomes Error with behavior RETRY", async () => {
  const h: Handler = async () => {
    throw new Error("flaky network");
  };
  const out = await turn(h, start(1), inputEntry(null));
  assert.deepEqual(types(out), [MT.ERROR]);
  assert.equal(out[0]!.payload[0], 0x08, "field 1 (code) must be present");
});

/**
 * A `ctx.run` closure throws a transient error: the result is not proposed, so that the retry
 * runs the side effect again. If user code swallows that error and moves on —
 * the AI SDK's `streamText` treats it as a tool error and continues the agent loop — the
 * retry reruns the closure (`Run` without a completion) **on a journal that already has
 * the steps after it**: the new (successful) result differs from the result the later steps
 * relied on, and the later `ctx.run`s are misaligned.
 * That turn must die: the next ctx command throws, nothing more is emitted, the outcome is Error
 * RETRY whatever the handler returns.
 */
test(
  "a ctx.run closure throws a transient error that the code swallows: the turn still ends with Error RETRY, the next ctx command throws",
  async () => {
    let after: unknown;
    const h: Handler = async (ctx: Context) => {
      try {
        await ctx.run("a", () => {
          throw new Error("flaky network");
        });
      } catch {
        // swallowed like streamText does
      }
      try {
        await ctx.run("b", () => 1);
      } catch (e) {
        after = e;
      }
      return "ok";
    };
    const out = await turn(h, start(1), inputEntry(null));
    assert.deepEqual(types(out), [MT.RUN, MT.ERROR]);
    assert.ok(new TextDecoder().decode(out[1]!.payload).includes("flaky network"), "Error carries the original error");
    assert.ok(after instanceof Error, "ctx.run after the error throws");
  },
);

test("a ctx.run closure throws TerminalError: not a transient error — the failed result is proposed, the turn suspends waiting for it to be durable", async () => {
  const h: Handler = async (ctx: Context) => {
    try {
      await ctx.run("a", () => {
        throw new TerminalError("invalid");
      });
    } catch {
      // compensation
    }
    return ctx.run("b", () => 1);
  };
  const out = await turn(h, start(1), inputEntry(null));
  assert.deepEqual(types(out), [MT.RUN, MT.PROPOSE_RUN_COMPLETION, MT.SUSPENSION]);
});

/**
 * A `TerminalError` thrown inside a `ctx.run` closure is
 * a **result** of the step, not an error of the turn — it must be durable like any other
 * result.
 *
 * 1. Turn 1: closure `a` throws `TerminalError`; the code catches it and runs the compensation
 *    step `b`, proposes, suspends. Journal: `Run a` (no completion), `Run b`,
 *    `RunCompletion(b)`.
 * 2. Turn 2: `Run a` matches but has no result ⇒ closure `a` runs again — one more side
 *    effect in every later turn, and `ctx.attempt` is back to 0 so this time it calls
 *    the real provider.
 *
 * Must see: closure `a` runs exactly once across the whole invocation; replay rethrows
 * `TerminalError` with the same `code`/`message`; the compensation step runs once.
 */
test("a ctx.run closure throws TerminalError: the failed result goes into the journal, the closure does not run again in later turns", async () => {
  let runsA = 0;
  let runsB = 0;
  const seen: { code: number; message: string }[] = [];
  const h: Handler = async (ctx: Context) => {
    try {
      await ctx.run("a", () => {
        runsA++;
        throw new TerminalError("vendor down", 503);
      });
    } catch (e) {
      if (e instanceof TerminalError) seen.push({ code: e.code, message: e.message });
      await ctx.run("b", () => {
        runsB++;
        return "refunded";
      });
      return "compensated";
    }
    return "charged";
  };
  const ledger = new Ledger();
  const d = await driveRR(h, ledger);
  assert.deepEqual(ledger.violations, [], describeJournal(d.runtime.journal));
  assert.equal(d.output, "compensated");
  assert.equal(runsA, 1, "closure a ran again after it had thrown TerminalError");
  assert.equal(runsB, 1);
  assert.ok(seen.length >= 1);
  for (const e of seen) assert.deepEqual(e, { code: 503, message: "vendor down" });
});

test("ctx.random is deterministic per seed, and differs when the seed differs", async () => {
  const pull = async (seed: bigint): Promise<number[]> => {
    const got: number[] = [];
    const h: Handler = async (ctx: Context) => {
      got.push(ctx.random(), ctx.random(), ctx.random());
      return null;
    };
    await turn(h, start(1, "", seed), inputEntry(null));
    return got;
  };
  const a = await pull(12345n);
  const b = await pull(12345n);
  const c = await pull(999n);
  assert.deepEqual(a, b, "the same seed must give the same sequence — docs/protocol-v1.md §8.4");
  assert.notDeepEqual(a, c);
  for (const v of a) assert.ok(v >= 0 && v < 1, `outside [0,1): ${v}`);
});

test("completion ids are assigned in command order, stable across replay", async () => {
  const h: Handler = async (ctx: Context) => {
    await ctx.get("a");
    await ctx.sleep(5);
    return null;
  };
  // Turn 1: only the first command is emitted, then it suspends on completion 1.
  const first = await turn(h, start(1), inputEntry(null));
  assert.deepEqual(types(first), [MT.GET_STATE, MT.SUSPENSION]);

  // Turn 2: completion 1 is present ⇒ move on, the sleep gets completion 2.
  const second = await turn(
    h,
    start(3),
    inputEntry(null),
    getStateEntry("a", 1),
    valueNotification(MT.GET_STATE_COMPLETION, 1, "x"),
  );
  assert.deepEqual(types(second), [MT.SLEEP, MT.SUSPENSION]);
});

test("JSON input reaches user code", async () => {
  let got: unknown = undefined;
  const h: Handler = async (_ctx, input) => {
    got = input;
    return null;
  };
  await turn(h, start(1), inputEntry({ amount: 42 }));
  assert.deepEqual(got, { amount: 42 });
});

test("a first message other than Start is a protocol violation", async () => {
  const out = await turn(async () => null, inputEntry(null));
  assert.deepEqual(types(out), [MT.ERROR]);
});

// ─────────────────────────────────────────────── flow control: ctx.scoped

/// Reads field `n` (as a string) out of an encoded command's payload.
function stringField(payload: Uint8Array, field: number): string | undefined {
  let at = 0;
  const dec = new TextDecoder();
  const varint = (): number => {
    let shift = 0;
    let out = 0;
    for (;;) {
      const b = payload[at++]!;
      out |= (b & 0x7f) << shift;
      if ((b & 0x80) === 0) return out >>> 0;
      shift += 7;
    }
  };
  while (at < payload.length) {
    const tag = varint();
    const num = tag >>> 3;
    const wire = tag & 7;
    if (wire === 2) {
      const len = varint();
      const body = payload.subarray(at, at + len);
      at += len;
      if (num === field) return dec.decode(body);
    } else if (wire === 0) {
      varint();
    } else if (wire === 5) {
      at += 4;
    } else if (wire === 1) {
      at += 8;
    } else {
      throw new Error(`wire type ${wire} not supported`);
    }
  }
  return undefined;
}

test("ctx.scoped puts the scope and limit key in fields 7/8 of Call", async () => {
  const frames = await turn(
    (ctx: Context) => {
      ctx.scoped("acme", "growth/alice").send("Svc", "h", { a: 1 });
      return "done";
    },
    start(0),
    inputEntry(null),
  );
  const oneWay = frames.find((f) => f.type === MT.ONE_WAY_CALL);
  assert.ok(oneWay, "must emit OneWayCall");
  // `OneWayCall` puts the scope at 8 and the limit key at 9 (Call uses 7/8) — the two messages have
  // two different field layouts, and that is the easiest thing to get wrong.
  assert.equal(stringField(oneWay.payload, 8), "acme");
  assert.equal(stringField(oneWay.payload, 9), "growth/alice");
});

test("ctx.scoped on ctx.call uses fields 7/8", async () => {
  const frames = await turn(
    async (ctx: Context) => {
      // No `await`: we only need the command in the journal, not its result.
      ctx.scoped("acme").call("Svc", "h", {});
      return "done";
    },
    start(0),
    inputEntry(null),
  );
  const call = frames.find((f) => f.type === MT.CALL);
  assert.ok(call, "must emit Call");
  assert.equal(stringField(call.payload, 7), "acme");
  assert.equal(stringField(call.payload, 8), undefined, "not declared ⇒ not sent");
});

/** The varint value of the top-level `field`; absent ⇒ `undefined`. */
function varintField(payload: Uint8Array, field: number): number | undefined {
  let found: number | undefined;
  eachField(payload, (num, wireType, r) => {
    if (num === field && wireType === 0) {
      found = Number(r.varint());
      return true;
    }
    r.skip(wireType);
    return true;
  });
  return found;
}

test("placement: spread sends field 13 = 1 on Call and OneWayCall, including through ctx.scoped", async () => {
  const frames = await turn(
    (ctx: Context) => {
      ctx.call("Svc", "h", {}, { placement: "spread" });
      ctx.send("Svc", "h", {}, { placement: "spread" });
      ctx.call("Svc", "h", {});
      // With a scope the runtime ignores `spread` (the scope tier decides), but the SDK still
      // carries it as is: the decision lives in one place (the engine), by the journal.
      ctx.scoped("acme").send("Svc", "h", {}, { placement: "spread" });
      return "done";
    },
    start(0),
    inputEntry(null),
  );
  const calls = frames.filter((f) => f.type === MT.CALL);
  const sends = frames.filter((f) => f.type === MT.ONE_WAY_CALL);
  assert.equal(calls.length, 2);
  assert.equal(sends.length, 2);
  assert.equal(varintField(calls[0]!.payload, 13), 1);
  assert.equal(varintField(calls[1]!.payload, 13), undefined, "not declared ⇒ not sent");
  assert.equal(varintField(sends[0]!.payload, 13), 1);
  assert.equal(varintField(sends[1]!.payload, 13), 1);
  assert.equal(stringField(sends[1]!.payload, 8), "acme");
});

test("an unknown placement fails AT ONCE, no command reaches the journal", async () => {
  const frames = await turn(
    (ctx: Context) => {
      for (const bad of ["SPREAD", "pack", ""]) {
        assert.throws(
          () => ctx.call("Svc", "h", {}, { placement: bad as "spread" }),
          TypeError,
          `placement ${JSON.stringify(bad)} must be rejected`,
        );
        assert.throws(() => ctx.send("Svc", "h", {}, { placement: bad as "spread" }), TypeError);
      }
      return "done";
    },
    start(0),
    inputEntry(null),
  );
  assert.equal(frames.filter((f) => f.type === MT.CALL || f.type === MT.ONE_WAY_CALL).length, 0);
});

test("a malformed scope fails AT ONCE on the client side", async () => {
  // Waiting for ingress to report it makes people debug through a network round trip — and
  // worse: the command is already in the journal, so the error shows up as a failed
  // invocation rather than as a wrong line of code.
  const bad = ["", "a/b", "a b", "*", "x".repeat(37)];
  for (const s of bad) {
    const frames = await turn(
      (ctx: Context) => {
        assert.throws(() => ctx.scoped(s), TypeError, `scope ${JSON.stringify(s)} must be rejected`);
        return "done";
      },
      start(0),
      inputEntry(null),
    );
    // And no command got into the journal.
    assert.equal(frames.filter((f) => f.type === MT.CALL || f.type === MT.ONE_WAY_CALL).length, 0);
  }
  await turn(
    (ctx: Context) => {
      assert.throws(() => ctx.scoped("acme", "a/b/c"), TypeError, "a 3-level limit key must be rejected");
      assert.throws(() => ctx.scoped("acme", "a b"), TypeError);
      return "done";
    },
    start(0),
    inputEntry(null),
  );
});

test("an option given in place wins over the view's scope", async () => {
  const frames = await turn(
    (ctx: Context) => {
      ctx.scoped("acme", "l1").send("Svc", "h", {}, { scope: "other" });
      return "done";
    },
    start(0),
    inputEntry(null),
  );
  const oneWay = frames.find((f) => f.type === MT.ONE_WAY_CALL)!;
  assert.equal(stringField(oneWay.payload, 8), "other");
  assert.equal(stringField(oneWay.payload, 9), "l1", "the view's limit key is still kept");
});

/**
 * **Pinned:** waiting on **one** signal encodes the Suspension in exactly
 * field 2 (numbered, packed) or 3 (named) — the combinator design keeps exactly these bytes for a
 * one-leaf tree, so the 28 older conformance traces do not drift (CG-5).
 */
test("awaitSignal with one leaf: Suspension field 2 (number) or 3 (name), nothing else", async () => {
  const bytesOf = async (signal: number | string): Promise<number[]> => {
    const out = await turn(
      async (ctx: Context) => ctx.awaitSignal(signal),
      start(1),
      inputEntry({}),
    );
    assert.deepEqual(types(out), [MT.SUSPENSION]);
    return [...out[0]!.payload];
  };
  assert.deepEqual(await bytesOf(3), [0x12, 0x01, 0x03]);
  assert.deepEqual(await bytesOf("s"), [0x1a, 0x01, 0x73]);
  // An awakeable is a signal by index ≥ 17: the same field 2.
  const out = await turn(
    async (ctx: Context) => ctx.awakeable().promise,
    start(1),
    inputEntry({}),
  );
  assert.deepEqual([...out.at(-1)!.payload], [0x12, 0x01, 17]);
});

/**
 * **Type level:** `ctx.signal`
 * and `awakeable().promise` are `DurableFuture`s ⇒ they can go into `ctx.race`/`ctx.all`.
 * `awaitSignal` still returns a `Promise` (kept for existing code), so it cannot — each
 * `@ts-expect-error` is a `tsc --noEmit` check.
 *
 * The handler bodies never run.
 */
test("ctx.signal and awakeable go into ctx.race; awaitSignal does not (type level)", () => {
  const _h: Handler = async (ctx: Context) => {
    const awk = ctx.awakeable<string>();
    const a: string | void = await ctx.race([awk.promise, ctx.sleep(1000)]);
    const s: { ok: boolean } | void = await ctx.race([ctx.signal<{ ok: boolean }>("approve"), ctx.sleep(1000)]);
    // @ts-expect-error — awaitSignal returns a Promise, not a DurableFuture
    await ctx.race([ctx.awaitSignal("approve"), ctx.sleep(1000)]);
    return [a, s];
  };
  void _h;
});

// ── signals as combinator leaves ─────────────────────────────────────────

function signalEntry(name: string | number, value?: unknown): Uint8Array {
  const w = new Writer();
  if (typeof name === "number") w.uint32(2, name);
  else w.string(3, name);
  if (value === undefined) w.message(4, new Writer());
  else w.message(5, new Writer().bytes(1, enc.encode(JSON.stringify(value))));
  return encodeFrame(MT.SIGNAL, 0, w.finish());
}

/** Suspension fields: `[field, value]` — numbers for varint/packed, strings for names. */
function suspensionFields(payload: Uint8Array): Array<[number, unknown]> {
  const out: Array<[number, unknown]> = [];
  eachField(payload, (field, wire, r) => {
    if (field === 1 || field === 2) {
      const bytes = r.bytes();
      const ids: number[] = [];
      for (let i = 0; i < bytes.length; i++) ids.push(bytes[i]!);
      out.push([field, ids]);
    } else if (field === 3) {
      out.push([field, new TextDecoder().decode(r.bytes())]);
    } else if (field === 4) {
      out.push([field, suspensionFields(r.bytes())]);
    } else if (field === 5) {
      out.push([field, Number(r.varint())]);
    } else {
      r.skip(wire);
    }
    return true;
  });
  return out;
}

const dec = new TextDecoder();

/** The value bytes of an `OutputCommandMessage` (field 14 → 1). */
function outputValue(payload: Uint8Array): Uint8Array {
  let out: Uint8Array = new Uint8Array();
  eachField(payload, (field, wire, r) => {
    if (field === 14) {
      eachField(r.bytes(), (f, w, rr) => {
        if (f === 1) out = rr.bytes();
        else rr.skip(w);
        return true;
      });
    } else {
      r.skip(wire);
    }
    return true;
  });
  return out;
}

const raceSignalSleep: Handler = async (ctx: Context) => {
  const v = await ctx.race([ctx.signal<{ by: string }>("approve"), ctx.sleep(60_000)]);
  return { winner: v === undefined ? "sleep" : "signal", v: v ?? null };
};

test("race(signal, sleep): the first turn emits Sleep and suspends on ONE tree — fields 1 and 3", async () => {
  const out = await turn(raceSignalSleep, start(1), inputEntry(null));
  assert.deepEqual(types(out), [MT.SLEEP, MT.SUSPENSION]);
  assert.deepEqual(suspensionFields(out[1]!.payload), [
    [1, [1]],
    [3, "approve"],
  ]);
});

test("race(signal, sleep): the winner follows journal order, stable across replay (CG-3)", async () => {
  const base = [start(3), inputEntry(null), sleepEntry(1)] as const;
  const outOf = async (...rest: Uint8Array[]) => {
    const out = await turn(raceSignalSleep, ...base, ...rest);
    assert.deepEqual(types(out), [MT.OUTPUT, MT.END]);
    return JSON.parse(dec.decode(outputValue(out[0]!.payload)));
  };
  // Only the signal.
  assert.deepEqual(await outOf(signalEntry("approve", { by: "an" })), { winner: "signal", v: { by: "an" } });
  // Only the timer.
  assert.deepEqual(await outOf(voidNotification(MT.SLEEP_COMPLETION, 1)), { winner: "sleep", v: null });
  // Both: the one earlier in the journal wins — the same result on every replay.
  for (let i = 0; i < 3; i++) {
    assert.deepEqual(
      await outOf(signalEntry("approve", { by: "an" }), voidNotification(MT.SLEEP_COMPLETION, 1)),
      { winner: "signal", v: { by: "an" } },
    );
    assert.deepEqual(
      await outOf(voidNotification(MT.SLEEP_COMPLETION, 1), signalEntry("approve", { by: "an" })),
      { winner: "sleep", v: null },
    );
  }
});

test("race(awakeable, sleep): an awakeable is a DurableFuture — fields 1 and 2 (index 17)", async () => {
  const h: Handler = async (ctx: Context) => {
    const awk = ctx.awakeable<string>();
    const v = await ctx.race([awk.promise, ctx.sleep(60_000)]);
    return v ?? "sleep";
  };
  const first = await turn(h, start(1), inputEntry(null));
  assert.deepEqual(types(first), [MT.SLEEP, MT.SUSPENSION]);
  assert.deepEqual(suspensionFields(first[1]!.payload), [
    [1, [1]],
    [2, [17]],
  ]);
  const out = await turn(h, start(3), inputEntry(null), sleepEntry(1), signalEntry(17, "approved"));
  assert.deepEqual(types(out), [MT.OUTPUT, MT.END]);
  assert.equal(JSON.parse(dec.decode(outputValue(out[0]!.payload))), "approved");
});

test("all(call, signal): a nested ALL tree carries both a completion and a signal", async () => {
  const h: Handler = async (ctx: Context) => {
    const [, s] = await ctx.all([ctx.sleep(10), ctx.signal<number>(4)]);
    return s;
  };
  const first = await turn(h, start(1), inputEntry(null));
  assert.deepEqual(types(first), [MT.SLEEP, MT.SUSPENSION]);
  assert.deepEqual(suspensionFields(first[1]!.payload), [
    [
      4,
      [
        [1, [1]],
        [2, [4]],
        [5, 2],
      ],
    ],
  ]);
  // The signal arrives first, the timer not yet: the ALL tree is not satisfied ⇒ suspends again (an
  // extra turn, still correct).
  const again = await turn(h, start(3), inputEntry(null), sleepEntry(1), signalEntry(4, 7));
  assert.deepEqual(types(again), [MT.SUSPENSION]);
  const done = await turn(
    h,
    start(3),
    inputEntry(null),
    sleepEntry(1),
    signalEntry(4, 7),
    voidNotification(MT.SLEEP_COMPLETION, 1),
  );
  assert.deepEqual(types(done), [MT.OUTPUT, MT.END]);
  assert.equal(JSON.parse(dec.decode(outputValue(done[0]!.payload))), 7);
});

test("REQUEST_RESPONSE: parallel ctx.run (Promise.all) — a slow closure still proposes in that turn, the next turn does not run it again", async () => {
  // The fast branch suspends waiting for durability ⇒ `Promise.all` rejects before the slow closure
  // finishes. The old bug: the response body closed at once (missing a's proposal), and the next
  // call saw `Run a` without a completion ⇒ ran a again.
  let runsA = 0;
  const h: Handler = async (ctx: Context) => {
    const [a, b] = await Promise.all([
      ctx.run("a", async () => {
        runsA++;
        await new Promise((r) => setTimeout(r, 30));
        return 1;
      }),
      ctx.run("b", () => 2),
    ]);
    return a + b;
  };
  const first = await turn(h, start(1), inputEntry(null));
  assert.deepEqual(types(first), [
    MT.RUN,
    MT.RUN,
    MT.PROPOSE_RUN_COMPLETION,
    MT.PROPOSE_RUN_COMPLETION,
    MT.SUSPENSION,
  ]);

  // The runtime writes the two proposals in the order received and calls again.
  const second = await turn(
    h,
    start(5),
    inputEntry(null),
    runEntry(1, "a"),
    runEntry(2, "b"),
    valueNotification(MT.RUN_COMPLETION, 2, 2),
    valueNotification(MT.RUN_COMPLETION, 1, 1),
  );
  assert.equal(runsA, 1);
  assert.deepEqual(types(second), [MT.OUTPUT, MT.END]);
  assert.equal(JSON.parse(dec.decode(outputValue(second[0]!.payload))), 3);
});

test("a swallowed 570: later ctx commands are not emitted, closures do not run, stream records do not go out, the outcome is still 570", async () => {
  // `streamText` swallows tool errors — including the `JournalMismatch` of a renamed
  // `ctx.run` — and continues the agent loop. The old bug: `command()` only
  // blocked after `failedWith`/`suspendedOn`, so `Run "model"` was emitted, the closure ran,
  // its result went into the journal; deploying the fix and then `resume` produced a dirty output.
  // Must see: a single `Error` 570 frame (PAUSE), no `Run`, no closure.
  let calls = 0;
  const h: Handler = async (ctx: Context) => {
    try {
      await ctx.run("charge-v2", () => "receipt");
    } catch {
      // swallowed the way the AI SDK swallows tool errors
    }
    ctx.stream("ui").append("after mismatch");
    ctx.log.info("after mismatch");
    let swallowedAgain = false;
    try {
      await ctx.run("model", () => {
        calls++;
        return "saw err";
      });
    } catch {
      swallowedAgain = true;
    }
    return { swallowedAgain };
  };
  const out = await decodeFrames(
    await runTurn(
      h,
      stream(start(3), inputEntry(null), runEntry(1, "charge"), valueNotification(MT.RUN_COMPLETION, 1, "receipt")),
      4,
    ),
  );
  assert.equal(calls, 0, "a closure ran after the journal had diverged");
  assert.deepEqual(types(out), [MT.ERROR]);
  assert.equal(errorCode(out[0]!.payload), JOURNAL_MISMATCH);
});

// ─────────────────────────────────────────────────────────── journal matching beyond the kind

/**
 * Commands other than `Run` used to match the journal only by **kind**. Code changed
 * between two deployments (`ctx.call("S","a")` → `ctx.call("S","b")`, `set("a")` →
 * `set("b")`) was accepted silently on replay: the result of `S/a` returned as that of `S/b`, local
 * state diverging from durable state. Must see: 570 (PAUSE), a single `Error` frame.
 */
function callEntry(service: string, handler: string, input: unknown, idCompletion: number, result: number) {
  return encodeFrame(
    MT.CALL,
    0,
    new Writer()
      .string(1, service)
      .string(2, handler)
      .bytes(3, enc.encode(JSON.stringify(input)))
      .uint32(10, idCompletion)
      .uint32(11, result)
      .string(12, `${service}/${handler}`)
      .finish(),
  );
}

const d5Cases: [string, Handler, Uint8Array[]][] = [
  [
    "Call target changed (S/a → S/b)",
    (async (ctx: Context) => ctx.call("S", "b", 1)) as Handler,
    [callEntry("S", "a", 1, 1, 2), valueNotification(MT.CALL_COMPLETION, 2, "result-of-a")],
  ],
  [
    "Call argument changed",
    (async (ctx: Context) => ctx.call("S", "a", 2)) as Handler,
    [callEntry("S", "a", 1, 1, 2), valueNotification(MT.CALL_COMPLETION, 2, "res")],
  ],
  [
    "SetState key changed (a → b)",
    (async (ctx: Context) => {
      ctx.set("b", 1);
      return "x";
    }) as Handler,
    [encodeFrame(MT.SET_STATE, 0, new Writer().bytes(1, enc.encode("a")).message(3, new Writer().bytes(1, enc.encode("1"))).string(12, "a").finish())],
  ],
  [
    "GetState key changed",
    (async (ctx: Context) => ctx.get("b")) as Handler,
    [
      encodeFrame(MT.GET_STATE, 0, new Writer().bytes(1, enc.encode("a")).uint32(11, 1).string(12, "a").finish()),
      valueNotification(MT.GET_STATE_COMPLETION, 1, 7),
    ],
  ],
];
for (const [what, h, journal] of d5Cases) {
  test(`${what} between two deployments ⇒ 570, not accepted silently`, async () => {
    const out = await turn(h, start(1 + journal.length), inputEntry(null), ...journal);
    assert.deepEqual(types(out), [MT.ERROR]);
    assert.equal(errorCode(out[0]!.payload), JOURNAL_MISMATCH);
  });
}

test("a Sleep differing only in its wake-up time (a journaled nondeterministic value) still matches", async () => {
  const h: Handler = async (ctx: Context) => {
    await ctx.sleep(1000);
    return "done";
  };
  const sleep = encodeFrame(MT.SLEEP, 0, new Writer().uint64(1, 12345n).uint32(11, 1).string(12, "sleep").finish());
  const out = await turn(h, start(3), inputEntry(null), sleep, voidNotification(MT.SLEEP_COMPLETION, 1));
  assert.deepEqual(types(out), [MT.OUTPUT, MT.END]);
});

/**
 * `ctx.all` throws in **journal** order, not by position in the array.
 *
 * 1. Original turn: `f2` has already failed, `f1` has no result yet ⇒ `all` throws `f2`'s error;
 *    the handler catches it and runs a compensation step based on `e.message`.
 * 2. Replay after `f1` failed too (its completion comes **after** f2's)
 *    ⇒ the old code threw `f1`'s error (earlier in the array) ⇒ a different branch.
 *
 * Must see: both turns see f2's error. (Green since the SDK became deterministic with parallel
 * branches.)
 */
test("ctx.all throws the error of the future that failed earliest in the journal, stable across replay", async () => {
  const seen: string[] = [];
  const h: Handler = async (ctx: Context) => {
    try {
      await ctx.all([ctx.call("S", "f1", 1), ctx.call("S", "f2", 2)]);
    } catch (e) {
      seen.push((e as Error).message);
    }
    return "x";
  };
  const failure = (id: number, message: string) =>
    encodeFrame(MT.CALL_COMPLETION, 0, new Writer().uint32(1, id).message(6, new Writer().uint32(1, 500).string(2, message)).finish());
  const calls = [callEntry("S", "f1", 1, 1, 2), callEntry("S", "f2", 2, 3, 4)];
  await turn(h, start(4), inputEntry(null), ...calls, failure(4, "f2 failed"));
  await turn(h, start(5), inputEntry(null), ...calls, failure(4, "f2 failed"), failure(2, "f1 failed"));
  assert.deepEqual(seen, ["f2 failed", "f2 failed"]);
});

/**
 * A `ctx.run` name carrying half of a surrogate pair (a slice cutting an emoji in two:
 * `title.slice(0, 40)`). `TextEncoder` writes U+FFFD into the journal; the next turn recomputes the
 * original name ⇒ a different string ⇒ 570, and `resume` hits it again. Same root: a state key with
 * half a surrogate ⇒ two different JS keys sharing one durable key.
 *
 * Must see: the invocation completes, no 570; `get` reads back exactly the value just `set`.
 */
test("ctx.run names and state keys with half a surrogate pair do not misalign the journal", async () => {
  const title = "🌧 rain".slice(0, 1); // "\uD83C" — the first half of the emoji
  const h: Handler = async (ctx: Context) => {
    const a = await ctx.run(`summarize ${title}`, () => 1);
    const b = await ctx.run("b", () => 2);
    ctx.set(`note ${title}`, a + b);
    return { sum: a + b, again: await ctx.get<number>(`note ${"🌧".slice(0, 1)}`) };
  };
  const ledger = new Ledger();
  const d = await driveRR(h, ledger);
  assert.deepEqual(ledger.violations, [], describeJournal(d.runtime.journal));
  assert.deepEqual(d.output, { sum: 3, again: 3 });
});

/**
 * A `ctx.run` returns a result larger than the frame limit (16 MiB, `docs/protocol-v1.md` §4). The
 * SDK used to emit a 17 MiB `ProposeRunCompletion` ⇒ the runtime's `FrameTooLarge` ⇒ a transient
 * transport error ⇒ retry ⇒ the closure ran again every time (repeated side effects), with the same
 * result, up to `maxAttempts` or forever.
 *
 * Must see: the SDK never emits a frame over the limit; a result that is too large is a
 * **terminal** failure of the step (413, durable) — the closure runs once, the invocation ends
 * failed with a clear message. The runtime side (treating `FrameTooLarge` from the SDK as
 * deterministic ⇒ PAUSE) is not covered by this SDK test.
 */
test("a ctx.run result over the frame limit ⇒ terminal 413 failure of the step, the closure runs once, no frame > 16 MiB", async () => {
  let runs = 0;
  const h: Handler = async (ctx: Context) =>
    ctx.run("big", () => {
      runs++;
      return "x".repeat(16 * 1024 * 1024);
    });
  const ledger = new Ledger();
  const rt = new Runtime(ledger);
  let biggest = 0;
  const apply = rt.apply.bind(rt);
  rt.apply = (f) => {
    biggest = Math.max(biggest, f.payload.length);
    apply(f);
  };
  const d = await driveRR(h, ledger, () => 0.99, rt);
  assert.ok(biggest <= 16 * 1024 * 1024, `the SDK emitted a ${biggest}-byte frame`);
  assert.equal(runs, 1);
  assert.ok(d.completed, describeJournal(rt.journal));
  assert.equal(d.output, undefined, "the outcome must be a failure, not a value");
});

/**
 * A `TerminalError`/`RunFailure` with a negative or non-integer `code` ⇒ `Writer.uint32` threw
 * `RangeError` in the middle of the error path ⇒ the step had no durable result (the same family as
 * the closure TerminalError case above) or the turn failed in transport. Must see: the code is
 * clamped to 500, the outcome is a terminal failure of the step like any `TerminalError`, the
 * closure runs once.
 */
test("TerminalError with a negative/non-integer code still becomes a durable failed result (code 500)", async () => {
  for (const code of [-1, 1.5, Number.NaN, 2 ** 40]) {
    let runs = 0;
    let seen: number | undefined;
    const h: Handler = async (ctx: Context) => {
      try {
        await ctx.run("x", () => {
          runs++;
          throw new TerminalError("bad", code);
        });
      } catch (e) {
        if (e instanceof TerminalError) seen = e.code;
      }
      return "ok";
    };
    const ledger = new Ledger();
    const d = await driveRR(h, ledger);
    assert.deepEqual(ledger.violations, [], `${code}: ${describeJournal(d.runtime.journal)}`);
    assert.equal(d.output, "ok", String(code));
    assert.equal(runs, 1, String(code));
    assert.equal(seen, 500, String(code));
  }
});

// ── eager getKeys on replay ──────────────────────────────────────────────

/** `StartMessage` with a complete eager snapshot (field 4 entries, field 5 absent). */
function startWithState(knownEntries: number, key: string, state: Record<string, unknown>): Uint8Array {
  const w = new Writer().uint32(3, knownEntries).string(6, key).uint64(9, 42n);
  for (const [k, v] of Object.entries(state)) {
    w.message(4, new Writer().bytes(1, enc.encode(k)).bytes(2, enc.encode(JSON.stringify(v))));
  }
  return encodeFrame(MT.START, 0, w.finish());
}

/**
 * The runtime captures the eager snapshot when it loads the turn, so it already holds every
 * key the journal set before the suspension; the key list embedded in a journaled
 * `GetEagerStateKeys` is the one read back then.
 *
 * 1. Turn 1: `getKeys()` answers `[a]` from the complete snapshot and journals it, then
 *    `set("b")`, then `sleep` suspends.
 * 2. Turn 2: the snapshot is now `{a, b}`; replay rebuilds `GetEagerStateKeys` from it.
 *
 * Must see: no 570 — the embedded list (field 14) is a recorded result, not a parameter;
 * replay returns the journaled `[a]` and the turn runs to `Output`.
 */
test("replayed eager getKeys after a set does not report 570", async () => {
  const h: Handler = async (ctx: Context) => {
    const keys = await ctx.getKeys();
    ctx.set("b", 2);
    await ctx.sleep(10);
    return keys;
  };
  const out = await turn(
    h,
    startWithState(5, "obj", { a: 1, b: 2 }),
    inputEntry(null),
    encodeFrame(MT.GET_EAGER_STATE_KEYS, 0, encodeGetEagerStateKeys([enc.encode("a")], "getKeys")),
    encodeFrame(MT.SET_STATE, 0, encodeSetState("b", enc.encode("2"), "b")),
    encodeFrame(MT.SLEEP, 0, encodeSleep(1n, 1, "sleep")),
    voidNotification(MT.SLEEP_COMPLETION, 1),
  );
  assert.deepEqual(types(out), [MT.OUTPUT, MT.END], new TextDecoder().decode(out[0]?.payload));
  assert.match(new TextDecoder().decode(out[0]!.payload), /\["a"\]/);
});
