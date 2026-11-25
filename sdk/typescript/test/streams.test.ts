// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * `ctx.stream`: stream records go up to the runtime in the side-channel frame
 * `StreamAppend` (`0x000B`, wire v4), are **replay-aware** (a step already in
 * the journal is not resent) and are batched by 50 ms or 1 KiB.
 */
import assert from "node:assert/strict";
import { test } from "node:test";
import { type Context, type Handler, runBidi, runTurn } from "../src/index.ts";
import { eachField, Writer } from "../src/protobuf.ts";
import {
  decodeFrames,
  encodeFrame,
  type Frame,
  FrameSplitter,
  MT,
} from "../src/wire.ts";

const enc = new TextEncoder();
const dec = new TextDecoder();
const STREAM_APPEND = 0x000b;

function start(knownEntries: number, retryCount = 0): Uint8Array {
  return encodeFrame(
    MT.START,
    0,
    new Writer().uint32(3, knownEntries).uint32(7, retryCount).uint64(9, 42n).finish(),
  );
}

function inputEntry(value: unknown): Uint8Array {
  const payload = new Writer()
    .message(14, new Writer().bytes(1, enc.encode(JSON.stringify(value))))
    .finish();
  return encodeFrame(MT.INPUT, 0, payload);
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
  step: number;
  attempt: number;
  k: number;
  data: unknown;
}

function decodeAppend(payload: Uint8Array): { name: string; durable: boolean; records: Rec[] } {
  let name = "";
  let durable = false;
  const records: Rec[] = [];
  eachField(payload, (field, _wire, r) => {
    if (field === 1) name = dec.decode(r.bytes());
    else if (field === 2) durable = r.varint() !== 0n;
    else if (field === 3) {
      const rec: Rec = { step: 0, attempt: 0, k: 0, data: null };
      eachField(r.bytes(), (g, _w, s) => {
        if (g === 1) rec.step = Number(s.varint());
        else if (g === 2) rec.attempt = Number(s.varint());
        else if (g === 3) rec.k = Number(s.varint());
        else if (g === 4) rec.data = JSON.parse(dec.decode(s.bytes()));
        else return false;
        return true;
      });
      records.push(rec);
    } else return false;
    return true;
  });
  return { name, durable, records };
}

function appended(frames: readonly Frame[]): Rec[] {
  return frames
    .filter((f) => f.type === STREAM_APPEND)
    .flatMap((f) => decodeAppend(f.payload).records);
}

function ui(ctx: Context): { append(v: unknown): void } {
  return ctx.stream("ui");
}

test(
  "RR wire v4: records travel in StreamAppend before Output, identity (step, attempt, k)",
  async () => {
    const h: Handler = async (ctx) => {
      ui(ctx).append({ tok: 1 });
      ui(ctx).append({ tok: 2 });
      return "ok";
    };
    const out = decodeFrames(await runTurn(h, cat(start(1, 3), inputEntry(null)), 4));
    const at = out.findIndex((f) => f.type === STREAM_APPEND);
    const outputAt = out.findIndex((f) => f.type === MT.OUTPUT);
    assert.ok(at >= 0 && at < outputAt, `StreamAppend must come before Output: ${out.map((f) => f.type)}`);
    assert.deepEqual(appended(out), [
      { step: 0, attempt: 3, k: 0, data: { tok: 1 } },
      { step: 0, attempt: 3, k: 1, data: { tok: 2 } },
    ]);
    assert.equal(decodeAppend(out[at]!.payload).name, "ui");
  },
);

test(
  "replay-aware: an append before a command already in the journal is not resent",
  async () => {
    const h: Handler = async (ctx) => {
      ui(ctx).append("before");
      await ctx.run("a", () => 1);
      ui(ctx).append("after");
      return "ok";
    };
    const out = decodeFrames(
      await runTurn(h, cat(start(3), inputEntry(null), runEntry("a", 1), runCompletion(1, 1)), 4),
    );
    assert.deepEqual(appended(out), [{ step: 1, attempt: 0, k: 0, data: "after" }]);
  },
);

test(
  "under wire v4 no StreamAppend is sent (the runtime would answer 571)",
  async () => {
    const h: Handler = async (ctx) => {
      ui(ctx).append("x");
      return "ok";
    };
    const out = decodeFrames(await runTurn(h, cat(start(1), inputEntry(null)), 3));
    assert.equal(appended(out).length, 0);
    assert.ok(out.some((f) => f.type === MT.OUTPUT));
  },
);

class Pipe {
  readonly sent: Frame[] = [];
  private readonly inbox: Frame[] = [];
  private readonly split = new FrameSplitter();
  push(...parts: Uint8Array[]): void {
    for (const p of parts) for (const f of this.split.push(p)) this.inbox.push(f);
  }
  next(): Promise<Frame | null> {
    return new Promise((resolve) => {
      const tick = () => {
        const f = this.inbox.shift();
        if (f !== undefined) resolve(f);
        else setTimeout(tick, 1);
      };
      tick();
    });
  }
  buffered(): Frame[] {
    return this.inbox.splice(0, this.inbox.length);
  }
  write(bytes: Uint8Array): void {
    for (const f of new FrameSplitter().push(bytes)) this.sent.push(f);
  }
  end(): void {}
}

test(
  "BIDI: records go on the wire while the ctx.run closure is still running (batched ≤ 50 ms)",
  async () => {
    let release: () => void = () => {};
    const gate = new Promise<void>((r) => {
      release = r;
    });
    const h: Handler = async (ctx) =>
      ctx.run("llm", async () => {
        ui(ctx).append("tok");
        await gate;
        return 1;
      });
    const pipe = new Pipe();
    pipe.push(start(1), inputEntry(null));
    const done = runBidi(h, pipe, 4);
    const t0 = Date.now();
    while (appended(pipe.sent).length === 0) {
      assert.ok(Date.now() - t0 < 2000, "records not on the wire while the closure runs");
      await new Promise((r) => setTimeout(r, 5));
    }
    // The step of a record inside a closure is the position of its `Run` command.
    assert.deepEqual(appended(pipe.sent), [{ step: 0, attempt: 0, k: 0, data: "tok" }]);
    release();
    // The SDK waits for the ProposeRunCompletion ack; close it by sending the completion.
    pipe.push(runCompletion(1, 1));
    await Promise.race([done, new Promise((r) => setTimeout(r, 500))]);
  },
);

/** `(step, attempt, k)` compared lexicographically — what the runtime uses to drop duplicates (AP-11). */
function after(a: Rec, b: Rec): boolean {
  return a.step !== b.step ? a.step > b.step : a.attempt !== b.attempt ? a.attempt > b.attempt : a.k > b.k;
}

test(
  "an append right after ctx.run returns has a larger identity than the record inside the closure",
  // Turn 1 emits "inside" at (1, 0, 0) from the closure. Turn 2 replays `Run` (the
  // closure does not run, step 1's `k` does not advance) and then emits "after" — also
  // at (1, 0, 0). The runtime saw an identity no larger than the last record ⇒ dropped
  // it as a duplicate, and "after" was lost. The fix: a record appended inside a
  // closure takes the step of its own `Run` command (see "parallel closures" below).
  async () => {
    const h: Handler = async (ctx) => {
      await ctx.run("a", () => {
        ui(ctx).append("inside");
        return 1;
      });
      ui(ctx).append("after");
      return "ok";
    };
    const first = appended(decodeFrames(await runTurn(h, cat(start(1), inputEntry(null)), 4)));
    assert.deepEqual(first.map((r) => r.data), ["inside"]);
    const second = appended(
      decodeFrames(await runTurn(h, cat(start(3), inputEntry(null), runEntry("a", 1), runCompletion(1, 1)), 4)),
    );
    assert.deepEqual(second.map((r) => r.data), ["after"]);
    assert.ok(after(second[0]!, first[0]!), `"after" ${JSON.stringify(second[0])} must come after "inside" ${JSON.stringify(first[0])}`);
  },
);

test(
  "a suspended turn sends no more stream records, even when user code swallows Suspend",
  // The AI SDK's `streamText` turns every error of `execute` into a tool error and continues the
  // agent loop. A `ctx.run` in a tool suspends waiting for the ack ⇒ `Suspend` is swallowed, and
  // the continuation (already dead — the turn ended in a suspension) opens a new `llm` span on
  // `$trace`. That record takes the identity the rerun needs for the tool span's `end` ⇒ the server
  // drops the real `end` (AP-11), and the tool span is "abandoned".
  async () => {
    const h: Handler = async (ctx) => {
      try {
        await ctx.run("tool", () => {
          ui(ctx).append("inside");
          return 1;
        });
      } catch {
        // swallow Suspend like streamText
      }
      ui(ctx).append("ghost");
      await ctx.trace.span("ghost", () => 0);
      return "ok";
    };
    const frames = decodeFrames(await runTurn(h, cat(start(1), inputEntry(null)), 4));
    assert.equal(frames.at(-1)?.type, MT.SUSPENSION);
    assert.deepEqual(appended(frames).map((r) => r.data), ["inside"]);
  },
);

test(
  "a closure started before the turn suspended still sends records after that point — parallel tools",
  // `a` and `b` run in parallel. `b` finishes first ⇒ `b`'s branch suspends waiting for
  // a durable result. `a` is still running and finishes after that: its result is
  // proposed and the rerun does not run the closure again, so the record it emits after
  // that point is the only copy. (The gate opens inside closure `b`: code after `await`
  // of a `ctx.run` that was just proposed does not run in the same turn — it used to run
  // because `Suspend` was swallowed.)
  async () => {
    const h: Handler = async (ctx) => {
      let release: () => void = () => {};
      const gate = new Promise<void>((r) => {
        release = r;
      });
      const a = ctx.run("a", async () => {
        await gate;
        ui(ctx).append("a");
        return 1;
      });
      const b = ctx.run("b", () => {
        ui(ctx).append("b");
        setImmediate(release);
        return 2;
      });
      await Promise.allSettled([a, b]);
      return "ok";
    };
    const frames = decodeFrames(await runTurn(h, cat(start(1), inputEntry(null)), 4));
    assert.equal(frames.at(-1)?.type, MT.SUSPENSION);
    assert.deepEqual(appended(frames).map((r) => r.data).sort(), ["a", "b"]);
  },
);

test(
  "parallel closures: a record carries the step of its own Run command",
  // `a` (Run at position 0) and `b` (Run at position 1) run in parallel and both append
  // to `ui`. `a`'s record must be at step 0 even though the cursor was at 2 when it
  // appended — the identity belongs to the step, not to the send time — so that appends
  // after `ctx.run` returns (step 1, 2…) never share an identity with a record inside a closure.
  async () => {
    const h: Handler = async (ctx) => {
      let release: () => void = () => {};
      const gate = new Promise<void>((r) => {
        release = r;
      });
      const a = ctx.run("a", async () => {
        ui(ctx).append("a0");
        await gate;
        ui(ctx).append("a1");
        return 1;
      });
      const b = ctx.run("b", () => {
        ui(ctx).append("b0");
        release();
        return 2;
      });
      await Promise.allSettled([a, b]);
      return "ok";
    };
    const frames = decodeFrames(await runTurn(h, cat(start(1), inputEntry(null)), 4));
    const recs = appended(frames).map((r) => [r.data, r.step, r.k]);
    assert.deepEqual(
      recs.sort((x, y) => String(x[0]).localeCompare(String(y[0]))),
      [
        ["a0", 0, 0],
        ["a1", 0, 1],
        ["b0", 1, 0],
      ],
    );
  },
);
