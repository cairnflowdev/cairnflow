// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * `ctx.log` and `ctx.trace.span`, together with
 * `ctx.stream(name, {durable: true})`: records travel on the durable streams `$log`
 * and `$trace` via `StreamAppend` (wire v4), **replay-aware** — a log before a
 * command already in the journal is not resent, a span around a step already
 * there does not emit `start`/`end` again.
 */
import assert from "node:assert/strict";
import { test } from "node:test";
import { type Context, type Handler, runTurn } from "../src/index.ts";
import { eachField, Writer } from "../src/protobuf.ts";
import { decodeFrames, encodeFrame, type Frame, MT } from "../src/wire.ts";

const enc = new TextEncoder();
const dec = new TextDecoder();
const STREAM_APPEND = 0x000b;

function start(knownEntries: number, retryCount = 0): Uint8Array {
  return encodeFrame(
    MT.START,
    0,
    new Writer()
      .bytes(1, new Uint8Array(24).fill(7))
      .uint32(3, knownEntries)
      .uint32(7, retryCount)
      .uint64(9, 42n)
      .finish(),
  );
}

function inputEntry(value: unknown, headers: Record<string, string> = {}): Uint8Array {
  const w = new Writer();
  for (const [k, v] of Object.entries(headers)) {
    w.message(1, new Writer().string(1, k).string(2, v));
  }
  w.message(14, new Writer().bytes(1, enc.encode(JSON.stringify(value))));
  return encodeFrame(MT.INPUT, 0, w.finish());
}

function runEntry(name: string, cid: number): Uint8Array {
  return encodeFrame(MT.RUN, 0, new Writer().uint32(11, cid).string(12, name).finish());
}

function runCompletion(cid: number, value: unknown): Uint8Array {
  return encodeFrame(
    MT.RUN_COMPLETION,
    0,
    new Writer()
      .uint32(1, cid)
      .message(5, new Writer().bytes(1, enc.encode(JSON.stringify(value))))
      .finish(),
  );
}

function cat(...parts: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let at = 0;
  for (const p of parts) {
    out.set(p, at);
    at += p.length;
  }
  return out;
}

interface Rec {
  stream: string;
  durable: boolean;
  step: number;
  attempt: number;
  k: number;
  // deno-lint-ignore no-explicit-any
  data: any;
}

function decodeAppend(payload: Uint8Array): Rec[] {
  let name = "";
  let durable = false;
  const raw: Omit<Rec, "stream" | "durable">[] = [];
  eachField(payload, (field, _wire, r) => {
    if (field === 1) name = dec.decode(r.bytes());
    else if (field === 2) durable = r.varint() !== 0n;
    else if (field === 3) {
      const rec = { step: 0, attempt: 0, k: 0, data: null as unknown };
      eachField(r.bytes(), (g, _w, s) => {
        if (g === 1) rec.step = Number(s.varint());
        else if (g === 2) rec.attempt = Number(s.varint());
        else if (g === 3) rec.k = Number(s.varint());
        else if (g === 4) rec.data = JSON.parse(dec.decode(s.bytes()));
        else return false;
        return true;
      });
      raw.push(rec);
    } else return false;
    return true;
  });
  return raw.map((r) => ({ stream: name, durable, ...r }));
}

function records(frames: readonly Frame[], stream: string): Rec[] {
  return frames
    .filter((f) => f.type === STREAM_APPEND)
    .flatMap((f) => decodeAppend(f.payload))
    .filter((r) => r.stream === stream);
}

async function run(h: Handler, ...body: Uint8Array[]): Promise<Frame[]> {
  return decodeFrames(await runTurn(h, cat(...body), 4));
}

test("ctx.stream(name, {durable: true}) sends StreamAppend with durable", async () => {
  const out = await run(async (ctx) => {
    ctx.stream("notes", { durable: true }).append({ a: 1 });
    ctx.stream("ui").append({ b: 2 });
    return "ok";
  }, start(1), inputEntry(null));
  assert.equal(records(out, "notes")[0]?.durable, true);
  assert.equal(records(out, "ui")[0]?.durable, false);
});

test("ctx.log: four levels, debug dropped by default; $log is durable, carries ts/level/msg/attrs", async () => {
  const out = await run(async (ctx) => {
    ctx.log.debug("dropped");
    ctx.log.info("calling tool", { tool: "search" });
    ctx.log.warn("slow");
    ctx.log.error("failed", { code: 7 });
    return "ok";
  }, start(1), inputEntry(null));
  const got = records(out, "$log");
  assert.deepEqual(
    got.map((r) => [r.data.level, r.data.msg]),
    [
      ["info", "calling tool"],
      ["warn", "slow"],
      ["error", "failed"],
    ],
  );
  assert.ok(got.every((r) => r.durable), "$log is always durable");
  assert.deepEqual(got[0]!.data.attrs, { tool: "search" });
  assert.equal(typeof got[0]!.data.ts, "number");
  assert.deepEqual(got.map((r) => r.k), [0, 1, 2]);
});

test("log level: the invocation's cairnflow-log-level header turns on debug", async () => {
  const out = await run(async (ctx) => {
    ctx.log.debug("seen");
    return "ok";
  }, start(1), inputEntry(null, { "cairnflow-log-level": "debug" }));
  assert.deepEqual(records(out, "$log").map((r) => r.data.msg), ["seen"]);
  const quiet = await run(async (ctx) => {
    ctx.log.info("dropped");
    ctx.log.error("kept");
    return "ok";
  }, start(1), inputEntry(null, { "cairnflow-log-level": "error" }));
  assert.deepEqual(records(quiet, "$log").map((r) => r.data.msg), ["kept"]);
});

test("ctx.trace.span: start/end, nested by call scope, set, errors", async () => {
  const out = await run(async (ctx) => {
    const v = await ctx.trace.span("plan", async (s) => {
      s.set({ model: "m", input_tokens: 1200 });
      return await ctx.trace.span("tool", async () => 5, { tool: "search" });
    });
    await assert.rejects(
      ctx.trace.span("bad", async () => {
        throw new Error("boom");
      }),
      /boom/,
    );
    return v;
  }, start(1), inputEntry(null));
  const tr = records(out, "$trace").map((r) => r.data);
  assert.deepEqual(
    tr.map((d) => [d.kind, d.name ?? null]),
    [
      ["start", "plan"],
      ["start", "tool"],
      ["end", "tool"],
      ["end", "plan"],
      ["start", "bad"],
      ["end", "bad"],
    ],
  );
  const [plan, tool, toolEnd, planEnd, bad, badEnd] = tr;
  assert.match(plan.span_id, /^[0-9a-f]{16}$/);
  assert.equal(plan.parent_id, null);
  assert.equal(tool.parent_id, plan.span_id, "a child span takes its parent from the call scope");
  assert.deepEqual(tool.attrs, { tool: "search" });
  assert.equal(toolEnd.span_id, tool.span_id);
  assert.equal(planEnd.span_id, plan.span_id);
  assert.equal(planEnd.status, "ok");
  assert.deepEqual(planEnd.attrs, { model: "m", input_tokens: 1200 });
  assert.equal(bad.parent_id, null);
  assert.equal(badEnd.status, "error");
  assert.match(String(badEnd.error), /boom/);
  assert.ok(records(out, "$trace").every((r) => r.durable));
});

test("deterministic span_id: same invocation, step, k ⇒ same id, regardless of the attempt", async () => {
  const h: Handler = async (ctx) => {
    await ctx.trace.span("plan", async () => 1);
    return ctx.trace.span("act", async () => 2);
  };
  const a = records(await run(h, start(1), inputEntry(null)), "$trace");
  const b = records(await run(h, start(1), inputEntry(null)), "$trace");
  assert.equal(a[0]!.data.span_id, b[0]!.data.span_id);
  const c = records(await run(h, start(1, 1), inputEntry(null)), "$trace");
  assert.equal(c[0]!.data.span_id, a[0]!.data.span_id, "the attempt is not part of the id");
  assert.notEqual(a[0]!.data.span_id, a[2]!.data.span_id, "different k ⇒ different span");
});

test("replay-aware: logs and spans around a step already in the journal are not emitted again", async () => {
  const h: Handler = async (ctx: Context) => {
    ctx.log.info("before");
    const v = await ctx.trace.span("llm", () => ctx.run("a", () => 1));
    ctx.log.info("after");
    return v;
  };
  const out = await run(
    h,
    start(3),
    inputEntry(null),
    runEntry("a", 1),
    runCompletion(1, 1),
  );
  assert.deepEqual(records(out, "$log").map((r) => r.data.msg), ["after"]);
  assert.deepEqual(
    records(out, "$trace").filter((r) => r.data.kind === "start"),
    [],
    "the start of a span opened at a point already in the journal is not emitted again",
  );
});

/**
 * REQUEST_RESPONSE: `ctx.run` waits for the ack, so turn 1 stops at `Suspension` (`Suspend`
 * is flow control, not a span error) — the span around it only emits
 * `start`. Turn 2 replays up to the head of the journal and then closes the span
 * in live mode: `end` must be emitted (it carries the token counts), **with the
 * same `span_id`** as turn 1's `start` (the identity is recomputed on replay, not
 * sent). An `end` duplicating one already sent in an earlier turn (BIDI) has the same
 * identity — the server drops it (AP-11).
 */
test(
  "RR: a span around ctx.run emits end in the turn after the ack, with the same span_id as start",
  async () => {
    const h: Handler = async (ctx: Context) =>
      ctx.trace.span("llm", async (s) => {
        const v = await ctx.run("a", () => 1);
        s.set({ input_tokens: 7 });
        return v;
      });
    const first = await run(h, start(1), inputEntry(null));
    const t1 = records(first, "$trace");
    assert.deepEqual(t1.map((r) => r.data.kind), ["start"], "turn 1 stops at the ack");
    const second = await run(h, start(3), inputEntry(null), runEntry("a", 1), runCompletion(1, 1));
    const t2 = records(second, "$trace");
    assert.deepEqual(t2.map((r) => r.data.kind), ["end"], "turn 2: only end");
    assert.equal(t2[0]!.data.span_id, t1[0]!.data.span_id, "same span_id");
    assert.equal(t2[0]!.data.attrs.input_tokens, 7);
  },
);

/**
 * The identity's `attempt` is the attempt count **since the last durable entry**, so it
 * changes between when `start` is sent and when the span closes: a span opened at attempt 2
 * (the two before failed), and once `ctx.run` is durable the next turn has `attempt` 0. Dying
 * mid-closure under BIDI is the same, the other way round: `start` sent at attempt 0,
 * attempt 5 reruns the closure and then closes the span. A `start` at a point already in the
 * journal is not resent, so `end` can only match if `span_id` does not depend
 * on `attempt`.
 */
test(
  "RR: a span opened at attempt 2, closed in a later turn (attempt back to 0) — end has the same span_id as start",
  async () => {
    const h: Handler = async (ctx: Context) => ctx.trace.span("llm", () => ctx.run("a", () => 1));
    const t1 = records(await run(h, start(1, 2), inputEntry(null)), "$trace");
    assert.deepEqual(t1.map((r) => r.data.kind), ["start"]);
    const t2 = records(await run(h, start(3, 0), inputEntry(null), runEntry("a", 1), runCompletion(1, 1)), "$trace");
    assert.deepEqual(t2.map((r) => r.data.kind), ["end"]);
    assert.equal(t2[0]!.data.span_id, t1[0]!.data.span_id, "orphan end: the span shows as 'abandoned' forever");
  },
);

/**
 * A `start` that is not yet durable is lost when the runtime dies, and the rerun
 * does not resend it (that point is already in the journal) — only `end`
 * reaches the server.
 * So `end` carries its own `name` and `parent_id`, so `/spans` and OTLP can draw the span
 * from it alone. Turn 2 here is that rerun: it replays the `turn` and `llm` spans
 * and then closes both in live mode.
 */
test(
  "end carries name and parent_id: the span can be drawn when start was lost",
  async () => {
    const h: Handler = async (ctx: Context) =>
      ctx.trace.span("turn", () => ctx.trace.span("llm", () => ctx.run("a", () => 1)));
    const t1 = records(await run(h, start(1), inputEntry(null)), "$trace").map((r) => r.data);
    assert.deepEqual(t1.map((d) => [d.kind, d.name]), [["start", "turn"], ["start", "llm"]]);
    const t2 = records(
      await run(h, start(3), inputEntry(null), runEntry("a", 1), runCompletion(1, 1)),
      "$trace",
    ).map((r) => r.data);
    assert.deepEqual(
      t2.map((d) => [d.kind, d.span_id, d.name, d.parent_id]),
      [
        ["end", t1[1]!.span_id, "llm", t1[0]!.span_id],
        ["end", t1[0]!.span_id, "turn", null],
      ],
    );
  },
);

test("log level: the handler's logLevel (withLogLevel), the header still overrides it", async () => {
  const { withLogLevel } = await import("../src/handler.ts");
  const h: Handler = async (ctx) => {
    ctx.log.debug("d");
    ctx.log.info("i");
    return "ok";
  };
  const own = await run(withLogLevel(h, "debug"), start(1), inputEntry(null));
  assert.deepEqual(records(own, "$log").map((r) => r.data.msg), ["d", "i"]);
  const over = await run(
    withLogLevel(h, "debug"),
    start(1),
    inputEntry(null, { "cairnflow-log-level": "warn" }),
  );
  assert.deepEqual(records(over, "$log"), []);
});

test("a closure rerun when a later command is already in the journal still sends stream records (not a replay)", async () => {
  // Died after both `Run`s of `Promise.all` were in the journal, before there were results: the
  // rerun runs **both** closures again — real work. The old bug: "replay?" looked at the cursor
  // (`cursor < journal`), and the synchronous part of closure a ran while the cursor was still at
  // `Run a` and `Run b` was already in the journal ⇒ a's first record was treated as a replay and
  // not sent (for an artifact, the ref pointed at an object not yet PUT). Must see: both "a" and
  // "b" reach `$log`.
  const h: Handler = async (ctx: Context) => {
    await Promise.all([
      ctx.run("a", () => {
        ctx.log.info("a");
        return 1;
      }),
      ctx.run("b", () => {
        ctx.log.info("b");
        return 2;
      }),
    ]);
    return null;
  };
  const out = await run(h, start(3), inputEntry(null), runEntry("a", 1), runEntry("b", 2));
  const msgs = records(out, "$log").map((r) => r.data.msg);
  assert.deepEqual(msgs.sort(), ["a", "b"], JSON.stringify(msgs));
});

test("ctx.log and stream.append never throw: BigInt becomes a string, a circular value becomes a record saying it cannot be serialized", async () => {
  // The old bug: a bare `JSON.stringify` threw `TypeError` (BigInt, circular object) out
  // of `log.info`/`append` — an API promised not to throw — so the turn failed and retried
  // forever with the same value.
  const loop: Record<string, unknown> = {};
  loop.self = loop;
  const h: Handler = async (ctx: Context) => {
    ctx.log.info("usage", { tokens: 10n });
    ctx.stream("ui").append(loop);
    return "ok";
  };
  const out = await run(h, start(1), inputEntry(null));
  assert.ok(!out.some((f) => f.type === MT.ERROR), "the turn failed because of the stream's JSON");
  assert.ok(out.some((f) => f.type === MT.OUTPUT));
  const log = records(out, "$log");
  assert.equal(log[0]?.data.attrs.tokens, "10");
  const ui = records(out, "ui");
  assert.equal(ui.length, 1);
  assert.equal(typeof ui[0]?.data.unserializable, "string", JSON.stringify(ui));
});
