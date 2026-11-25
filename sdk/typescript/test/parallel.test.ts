// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * A deterministic SDK with parallel branches.
 *
 * Invariant: *the sequence of commands the handler emits is a function of the
 * journal*, not of when the `ctx.run` closures finish. The three pinned cases below
 * are the smallest counterexamples; the property test at the end of the file is the
 * arbiter of the design (generator and fake runtime in `parallel-sim.ts`).
 *
 * Every case runs on a fake runtime that follows the rules of the shell in
 * `turn/sdk.rs` (`driveRR`): commands enter the journal in the order received,
 * proposals become a `RunCompletion` when received, an `Error` retry keeps what
 * was written.
 */

import assert from "node:assert/strict";
import { test } from "node:test";
import type { Context, Handler } from "../src/index.ts";
import { runTurn } from "../src/index.ts";
import { decodeFrames, MT } from "../src/wire.ts";
import {
  checkSeed,
  decodeSuspension,
  describeJournal,
  describeSuspension,
  describeTree,
  driveBidi,
  driveRR,
  inputFrame,
  Ledger,
  onWire,
  Runtime,
  satisfiedBy,
  startFrame,
} from "./parallel-sim.ts";

function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
  const out = new Uint8Array(a.length + b.length);
  out.set(a, 0);
  out.set(b, a.length);
  return out;
}


const tick = (n: number) => (async () => {
  for (let i = 0; i < n; i++) await new Promise<void>((r) => setImmediate(r));
})();

/**
 * The original counterexample, verbatim: two branches, two `ctx.run` each.
 *
 * 1. Turn 1: emits `Run a1`(1), `Run b1`(2). Closure `b1` finishes ⇒ proposes (2);
 *    closure `a1` throws a transient error. The runtime writes `[Run a1, Run b1, RunCompletion(2)]`
 *    and schedules a retry.
 * 2. Turn 2: `a1` runs its closure again (slow). `b1` is known ⇒ branch b moves on:
 *    `Run b2` gets id 3, command position 2. Then `a1` finishes ⇒ proposes (1).
 * 3. Turn 3 (both durable): branch a goes first ⇒ `ctx.run("a2")` at position 2.
 *
 * Must see: the invocation completes, no 570, each branch gets its own result,
 * each closure with a durable result runs exactly once.
 */
function d1Handler(names: { a: [string, string]; b: [string, string] }, runs: Map<string, number>): Handler {
  let attemptsA1 = 0;
  const closure = (label: string, body: () => Promise<void> = async () => {}) => async () => {
    runs.set(label, (runs.get(label) ?? 0) + 1);
    await body();
    return label;
  };
  return async (ctx: Context) => {
    const branch = async (n: "a" | "b") => {
      const [s1, s2] = names[n];
      const x = await ctx.run(
        s1,
        closure(`${n}1`, async () => {
          if (n === "a") {
            attemptsA1++;
            // Turn 1: throws after b1 has proposed; turn 2: slower than all of branch b.
            await tick(attemptsA1 === 1 ? 2 : 6);
            if (attemptsA1 === 1) throw new Error("429 from the model provider");
          }
        }),
      );
      const y = await ctx.run(s2, closure(`${n}2`));
      return [x, y];
    };
    return Promise.all([branch("a"), branch("b")]);
  };
}

test("two branches of two steps, different names — a transient error in one branch does not become 570", async () => {
  const runs = new Map<string, number>();
  const ledger = new Ledger();
  const d = await driveRR(d1Handler({ a: ["a1", "a2"], b: ["b1", "b2"] }, runs), ledger);
  assert.deepEqual(ledger.violations, [], describeJournal(d.runtime.journal));
  assert.ok(d.completed, "the invocation must complete");
  assert.deepEqual(d.output, [
    ["a1", "a2"],
    ["b1", "b2"],
  ]);
  assert.deepEqual(Object.fromEntries(runs), { a1: 2, b1: 1, a2: 1, b2: 1 });
});

test("two branches of two steps, same names — branch a does not get b's result, each branch's second-step closure runs exactly once", async () => {
  const runs = new Map<string, number>();
  const ledger = new Ledger();
  const d = await driveRR(
    d1Handler({ a: ["fetch", "summarize"], b: ["fetch", "summarize"] }, runs),
    ledger,
  );
  assert.deepEqual(ledger.violations, [], describeJournal(d.runtime.journal));
  assert.ok(d.completed, "the invocation must complete");
  assert.deepEqual(d.output, [
    ["a1", "a2"],
    ["b1", "b2"],
  ]);
  assert.equal(runs.get("a2"), 1, "branch a's second-step closure");
  assert.equal(runs.get("b2"), 1, "branch b's second-step closure");
});

/**
 * A counterexample that **needs no error at all**: a slow closure in one branch,
 * `ctx.sleep` in the other. A known `ctx.run` result returns after one microtask, a
 * known `ctx.sleep` after three; a closure that really runs, after one event loop turn.
 *
 * 1. Turn 1: `Run a1`(1), `Sleep`(2); a1 finishes ⇒ proposes; suspends waiting for the sleep.
 * 2. Turn 2 (sleep done): branch a moves on at once ⇒ `Run a2`(3), slow closure;
 *    branch b wakes after three microtasks ⇒ `Run b2`(4) — while a2 is still running.
 * 3. Turn 3: a2 is known ⇒ branch a reaches `ctx.run("a3")` after two microtasks, before
 *    branch b wakes ⇒ position 3 is `Run a3`, the journal has `Run b2`.
 *
 * No `Run` lacks a result in turn 3 — a design where "a replay hole blocks new
 * commands" (one of the options considered) does not touch this case.
 */
test("a branch with a slow closure and a branch with ctx.sleep: commands do not change places once the closure is durable (counterexample to the replay-hole design)", async () => {
  const runs = new Map<string, number>();
  const run = (ctx: Context, name: string, slow = 0) =>
    ctx.run(name, async () => {
      runs.set(name, (runs.get(name) ?? 0) + 1);
      await tick(slow);
      return name;
    });
  const handler: Handler = async (ctx) =>
    Promise.all([
      (async () => [await run(ctx, "a1"), await run(ctx, "a2", 3), await run(ctx, "a3")])(),
      (async () => {
        await ctx.sleep(1000);
        return [await run(ctx, "b2")];
      })(),
    ]);
  const ledger = new Ledger();
  const d = await driveRR(handler, ledger);
  assert.deepEqual(ledger.violations, [], describeJournal(d.runtime.journal));
  assert.ok(d.completed);
  assert.deepEqual(d.output, [["a1", "a2", "a3"], ["b2"]]);
  assert.deepEqual(Object.fromEntries(runs), { a1: 1, a2: 1, a3: 1, b2: 1 });
});

/** Number of seeds of the property test — `CAIRNFLOW_PROP_SEEDS=rr,bidi` to change it. */
function seeds(): { rr: number; bidi: number } {
  const [rr, bidi] = (process.env.CAIRNFLOW_PROP_SEEDS ?? "10000,2000").split(",").map(Number);
  return { rr: rr ?? 10000, bidi: bidi ?? 2000 };
}

for (const mode of ["rr", "bidi"] as const) {
  test(
    `property test (${mode === "rr" ? "REQUEST_RESPONSE" : "BIDI_STREAM, with connection cuts"}): ` +
      "no 570, replaying every prefix emits the right command sequence, a durable closure does not run again, each branch gets its own result",
    // 10⁴ seeds take ~40 s on an idle machine — a dedicated limit, not the shared `--test-timeout`.
    { timeout: 900_000 },
    async () => {
      const n = seeds()[mode];
      let bad = 0;
      let first: string | null = null;
      for (let seed = 1; seed <= n; seed++) {
        const { run, tree } = await checkSeed(seed, mode);
        if (run.ledger.violations.length === 0) continue;
        bad++;
        first ??=
          `seed ${seed} (${run.turns} turns)\n${describeTree(tree)}\n` +
          `${run.ledger.violations.join("\n")}\n${describeJournal(run.journal)}`;
      }
      assert.equal(bad, 0, `${bad}/${n} seeds violate; first:\n${first}`);
    },
  );
}

// ───────────────────────────────────────── awakeable sent by a sibling branch


/**
 * A branch `render → send(approval.id)`, while the other branch waits on that
 * same awakeable.
 *
 * 1. Turn 1: `Run render` is emitted, the closure runs; `approval.promise` suspends the turn
 *    on signal 17. The closure finishes ⇒ proposes (1), but the suspension point keeps the tree
 *    `{signal 17}`.
 * 2. The runtime writes `RunCompletion(1)`; the tree `{signal 17}` is not satisfied ⇒ waits
 *    outside.
 * 3. The `send` step never runs ⇒ nobody receives `approval.id` ⇒ suspended forever.
 *
 * Must see: turn 1's suspension tree can be satisfied by `render`'s completion, and the invocation
 * reaches the `send` step without any signal (then completes when the signal arrives).
 */
function d3Handler(sent: string[], onSend: () => void = () => {}): Handler {
  return async (ctx: Context) => {
    const approval = ctx.awakeable<string>();
    const [, verdict] = await Promise.all([
      (async () => {
        const text = await ctx.run("render", async () => {
          await tick(2);
          return "please approve";
        });
        await ctx.run("send", () => {
          sent.push(`${text} ${approval.id}`);
          onSend();
          return "sent";
        });
      })(),
      approval.promise,
    ]);
    return verdict;
  };
}

test("awakeable in a sibling branch (unit): the Suspension of a turn that proposed a ctx.run is satisfied by the completion of that same proposal", async () => {
  const sent: string[] = [];
  const out = decodeFrames(await runTurn(d3Handler(sent), concat(startFrame(1, 0), onWire([inputFrame()])), 4));
  assert.deepEqual(
    out.map((f) => f.type),
    [MT.RUN, MT.PROPOSE_RUN_COMPLETION, MT.SUSPENSION],
  );
  const tree = decodeSuspension(out[2]!.payload);
  assert.ok(satisfiedBy(tree, [1]), `suspension tree ${describeSuspension(tree)} not satisfied by render's RunCompletion(1)`);
});

for (const mode of ["rr", "bidi"] as const) {
  test(`awakeable in a sibling branch (${mode === "rr" ? "REQUEST_RESPONSE" : "BIDI_STREAM"}): the invocation reaches the send step without a signal, then completes when the signal arrives`, async () => {
    const ledger = new Ledger();
    const rt = new Runtime(ledger);
    const sent: string[] = [];
    // The approver only answers after receiving the message.
    const onSend = () => rt.queueSignal(17, "approved");
    const d =
      mode === "rr"
        ? await driveRR(d3Handler(sent, onSend), ledger, () => 0.99, rt)
        : await driveBidi(() => d3Handler(sent, onSend), ledger, () => 0.99, false, rt);
    assert.deepEqual(ledger.violations, [], describeJournal(rt.journal));
    assert.ok(d.completed);
    assert.equal(d.output, "approved");
    assert.equal(sent.length, 1);
  });
}

test("awakeable in a sibling branch (light): race(slow ctx.run, ctx.sleep 60 s) completes when the closure completes, without waiting for the timer", async () => {
  const handler: Handler = async (ctx) =>
    Promise.race([
      ctx.run("llm", async () => {
        await tick(3);
        return "answer";
      }),
      ctx.sleep(60_000).then(() => "timeout"),
    ]);
  const ledger = new Ledger();
  const d = await driveRR(handler, ledger);
  assert.deepEqual(ledger.violations, [], describeJournal(d.runtime.journal));
  assert.equal(d.output, "answer");
  assert.ok(
    !d.runtime.journal.some((f) => f.type === MT.SLEEP_COMPLETION),
    `the turn waited for the 60 s timer:\n${describeJournal(d.runtime.journal)}`,
  );
});
