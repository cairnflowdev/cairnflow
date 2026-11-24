// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Three properties that can only be proven with the **real** runtime running the **real** SDK.
 *
 * The tests in `cairn-runtime` and `cairn-engine` use a fake SDK — same author, same
 * understanding. Here the SDK is an independent replay implementation, and the
 * runtime is killed midway as in real life.
 *
 * Needs `cargo build --bin cairnflow --bin cairn`.
 */

import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { appendFileSync, existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, test } from "node:test";
import { fileURLToPath } from "node:url";
import {
  type Context,
  type Endpoint,
  type ProtocolMode,
  serve,
  service,
} from "../src/index.ts";

/// The SDK mode declared in the manifest for this run — see `e2e.test.ts`.
const sdkMode: ProtocolMode =
  (process.env.CAIRNFLOW_E2E_SDK_MODE as ProtocolMode | undefined) ?? "BIDI_STREAM";


const here = fileURLToPath(new URL(".", import.meta.url));
const binary = join(here, "..", "..", "..", "target", "debug", "cairnflow");
/** The `cairn` CLI — for `replay --bundle`. */
const cli = join(here, "..", "..", "..", "target", "debug", "cairn");

let sdk: Endpoint | undefined;

/** node:test buffers stdout until the end of the file, so progress must be written straight to disk. */
const TRACE = process.env.CF_TRACE;
function trace(msg: string): void {
  if (TRACE) appendFileSync(TRACE, `${new Date().toISOString()} ${msg}\n`);
}

/** State driven by the test, shared with the handlers. */
const ctl = {
  turns: 0,
  killAtTurn: 0,
  kill: (): void => {},
  lastInvocationId: "",
  sideEffects: 0,
  variant: "a" as "a" | "b",
  /** The version of `Mender`'s code currently "deployed" — see the deploy-fix test. */
  mender: "v1" as "v1" | "v2" | "v3",
  /** Number of times `LongRun`'s long closure actually ran — see the keepalive test. */
  longRuns: 0,
  /** The version of `Evolve`'s code — the replay-check tests. */
  evolve: "v1" as "v1" | "v2" | "v3",
  /** The file that `Evolve` v2's closure writes — replay must not create it. */
  evolveMarker: "",
  /** Counts `Steps`'s closures (restart-as-new) and the sleep time between the two steps. */
  stepsA: 0,
  stepsB: 0,
  stepsSleepMs: 60_000,
  /** The awakeable id that `Hook` just "sent to the webhook". */
  awakeableId: "",
};

/**
 * A program long enough to have many different points of death.
 *
 * A virtual object, called with a key: state in an unkeyed, unscoped service is
 * rejected.
 */
const work = service("Work", {
  async pipeline(ctx: Context, input: { n: number }) {
    ctl.turns++;
    ctl.lastInvocationId = ctx.invocationId;
    // Kill the runtime **before** answering: it dies exactly while waiting on us.
    if (ctl.turns === ctl.killAtTurn) ctl.kill();

    const seen = (await ctx.get<number>("seen")) ?? 0;
    const a = await ctx.run("step-1", () => {
      ctl.sideEffects++;
      return input.n * 2;
    });
    await ctx.sleep(50, "rest");
    const b = await ctx.run("step-2", () => {
      ctl.sideEffects++;
      return a + 1;
    });
    ctx.set("seen", seen + 1);
    return { a, b, seen: seen + 1 };
  },
}, { type: "VIRTUAL_OBJECT" });

/**
 * User code changing between two runs — to check 570. A virtual object because
 * `GetState` in an unkeyed service is rejected at once — the test would fail for
 * another reason, not a journal mismatch.
 */
const shifty = service("Shifty", {
  async go(ctx: Context) {
    ctl.turns++;
    ctl.lastInvocationId = ctx.invocationId;
    if (ctl.turns === ctl.killAtTurn) ctl.kill();
    if (ctl.variant === "a") {
      await ctx.get("x");
    } else {
      await ctx.sleep(10);
    }
    return "done";
  },
}, { type: "VIRTUAL_OBJECT" });

/**
 * A service across three deploys: v1 sleeps and then reads state; v2 **diverges**
 * (reads state before sleeping); v3 is the fix — matching v1 again. Same endpoint,
 * the code changes through `ctl.mender`: exactly what a deploy does to an
 * invocation in progress.
 */
const mender = service("Mender", {
  async go(ctx: Context) {
    ctl.lastInvocationId = ctx.invocationId;
    if (ctl.mender === "v2") {
      await ctx.get("x");
      await ctx.sleep(400, "rest");
    } else {
      await ctx.sleep(400, "rest");
      await ctx.get("x");
    }
    return "fixed";
  },
}, { type: "VIRTUAL_OBJECT" });

/** A `ctx.run` longer than the runtime's inactivity cap — the keepalive test. */
const longRun = service("LongRun", {
  async go(ctx: Context) {
    return await ctx.run("slow", async () => {
      ctl.longRuns++;
      await new Promise((res) => setTimeout(res, 6_000));
      return "done";
    });
  },
}, { type: "VIRTUAL_OBJECT" });

/**
 * A service across three versions: v1 sleeps; v2 adds a `ctx.run` with a side effect
 * **after** the sleep (compatible — append only); v3 reads state before sleeping (diverges).
 */
const evolve = service("Evolve", {
  async go(ctx: Context) {
    if (ctl.evolve === "v3") await ctx.get("x");
    await ctx.sleep(60_000, "long rest");
    if (ctl.evolve === "v2") {
      await ctx.run("write file", () => {
        writeFileSync(ctl.evolveMarker, "ran");
        return 1;
      });
    }
    return ctl.evolve;
  },
}, { type: "VIRTUAL_OBJECT" });

/** `restart-as-new`: step a, sleep, step b. */
const steps = service("Steps", {
  async go(ctx: Context) {
    const a = await ctx.run("a", () => ++ctl.stepsA);
    await ctx.sleep(ctl.stepsSleepMs, "between");
    const b = await ctx.run("b", () => ++ctl.stepsB);
    return { a, b };
  },
}, { type: "VIRTUAL_OBJECT" });

/** `ctx.continueAsNew`: each invocation does five steps, then hands over to a new one. */
const looper = service("Looper", {
  async go(ctx: Context, s: { i: number; n: number }) {
    let i = s.i;
    for (let k = 0; k < 5 && i < s.n; k++) i = await ctx.run(`step ${i}`, () => i + 1);
    if (i < s.n) ctx.continueAsNew({ i, n: s.n });
    return { done: i };
  },
}, { type: "VIRTUAL_OBJECT" });

/** Calls `Looper` through `ctx.call` — the waiter must get the final result of the chain. */
const loopCaller = service("LoopCaller", {
  async go(ctx: Context) {
    return await ctx.call("Looper", "go", { i: 0, n: 23 }, { key: "via-call" });
  },
}, { type: "VIRTUAL_OBJECT" });

/** Awakeable: sends the id to a "webhook" inside `ctx.run`, then waits. */
const hook = service("Hook", {
  async go(ctx: Context) {
    const { id, promise } = ctx.awakeable<{ ok: boolean }>();
    await ctx.run("send id to webhook", () => {
      ctl.awakeableId = id;
      return 1;
    });
    try {
      return await promise;
    } catch (e) {
      return { rejected: (e as Error).message };
    }
  },
}, { type: "VIRTUAL_OBJECT" });

/**
 * A **bounded** wait for approval — `race(signal, sleep)` between two `ctx.run`s.
 * Any turn may be the turn where the runtime dies.
 */
const gate = service("Gate", {
  async approve(ctx: Context, input: { ms: number }) {
    ctl.turns++;
    ctl.lastInvocationId = ctx.invocationId;
    if (ctl.turns === ctl.killAtTurn) ctl.kill();
    const before = await ctx.run("before", () => {
      ctl.sideEffects++;
      return 1;
    });
    const v = await ctx.race([ctx.signal<{ by: string }>("approve"), ctx.sleep(input.ms, "deadline")]);
    const after = await ctx.run("after", () => {
      ctl.sideEffects++;
      return 2;
    });
    return { winner: v === undefined ? "sleep" : "signal", by: v?.by ?? null, before, after };
  },
}, { type: "VIRTUAL_OBJECT" });

/** One `ctx.run`, then return — the yardstick of the commit path. */
const oneRun = service("OneRun", {
  async go(ctx: Context) {
    return await ctx.run("one step", () => 42);
  },
}, { type: "VIRTUAL_OBJECT" });

/** Reads an object's balance — used by the EXCLUSIVE test. */
const bank = service(
  "Bank",
  {
    async deposit(ctx: Context, input: { amount: number }) {
      const before = (await ctx.get<number>("balance")) ?? 0;
      // The suspension point sits **between** the read and the write — exactly where EXCLUSIVE must protect.
      await ctx.sleep(200, "think");
      ctx.set("balance", before + input.amount);
      return before + input.amount;
    },
    async balance(ctx: Context) {
      return (await ctx.get<number>("balance")) ?? 0;
    },
  },
  { type: "VIRTUAL_OBJECT", handlerTypes: { deposit: "EXCLUSIVE", balance: "SHARED" } },
);

before(async () => {
  if (!existsSync(binary)) {
    throw new Error(`missing binary ${binary} — run \`cargo build --bin cairnflow\` first`);
  }
  sdk = await serve({
    services: [work, bank, shifty, mender, longRun, oneRun, evolve, steps, looper, loopCaller, hook, gate],
    port: 0,
    protocolMode: sdkMode,
  });
});

/**
 * Every runtime started, so they are cleaned up even if a test fails midway.
 *
 * Without cleanup a still-living child process holds the stdio pipe, node **cannot
 * exit**, and the test output — error messages included — is never flushed. A
 * failing test looks exactly like a hanging one.
 */
const spawned: ChildProcess[] = [];

after(async () => {
  for (const p of spawned) p.kill("SIGKILL");
  await sdk?.close();
});

interface Runtime {
  url: string;
  proc: ChildProcess;
  stop(): void;
}

async function startRuntime(dataDir: string, extra: string[] = []): Promise<Runtime> {
  // `--no-auth`: this test measures durability across kill -9, not API keys.
  const proc = spawn(binary, ["--data-dir", dataDir, "--port", "0", "--tick-interval-ms", "50", "--no-auth", ...extra], {
    stdio: "pipe",
  });
  spawned.push(proc);
  // Drain stderr, otherwise a full 64KB pipe stalls the runtime while it logs.
  proc.stderr!.resume();
  const url = await new Promise<string>((resolve, reject) => {
    let buf = "";
    const timer = setTimeout(() => reject(new Error(`runtime did not come up:\n${buf}`)), 20_000);
    proc.stdout!.on("data", (c: Buffer) => {
      buf += c.toString();
      const m = /http:\/\/[\d.]+:\d+/.exec(buf);
      if (m) {
        clearTimeout(timer);
        resolve(m[0]);
      }
    });
  });
  const healthy = Date.now() + 20_000;
  for (;;) {
    try {
      if ((await fetch(`${url}/health`)).ok) break;
    } catch {
      /* not up yet */
    }
    if (Date.now() > healthy) throw new Error(`${url}/health did not come up`);
    await new Promise((r) => setTimeout(r, 20));
  }
  return { url, proc, stop: () => proc.kill("SIGKILL") };
}

async function register(url: string): Promise<void> {
  const r = await fetch(`${url}/deployments`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ uri: sdk!.url }),
  });
  assert.equal(r.status, 200, await r.text());
}

async function journalOf(url: string, id: string): Promise<unknown[]> {
  const r = await fetch(`${url}/invocations/${id}/journal`);
  const text = await r.text();
  assert.ok(r.ok, `reading the journal failed: ${r.status} ${text}`);
  return JSON.parse(text) as unknown[];
}

/** The journal's sequence of message types — the deterministic part between two different runs. */
function shapeOf(journal: unknown[]): string[] {
  return journal.map((e) => (e as { ty: string }).ty);
}

async function waitCompleted(url: string, id: string, timeoutMs = 20_000): Promise<unknown> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try {
      const r = await fetch(`${url}/invocations/${id}`);
      if (r.ok) {
        const body = (await r.json()) as { status: string; output?: string };
        if (body.status === "completed") return JSON.parse(body.output ?? "null");
        if (body.status === "failed") throw new Error(`invocation failed: ${body.output}`);
      }
    } catch (e) {
      if (Date.now() > deadline) throw e;
    }
    if (Date.now() > deadline) throw new Error(`not done after ${timeoutMs}ms`);
    await new Promise((r) => setTimeout(r, 50));
  }
}

/**
 * ## Killed at every turn, with the real SDK
 *
 * Kill the runtime at **every turn**, restart it from exactly the old data directory.
 * The runtime is killed *before* the SDK answers — it dies exactly while waiting,
 * after writing the journal for the commands before. That is the hardest moment.
 *
 * **What is compared, and why not byte for byte here.** The two runs are two
 * different invocations, and `ctx.sleep` records an **absolute** wake-up time
 * (`protocol-v1.md` §8.4) — they start at two different times so their `Sleep`
 * entries differ, and differing there is *correct*. Comparing bytes between two
 * invocations compares the wrong thing.
 *
 * Bit-exactness of **one and the same** invocation is checked at the runtime layer,
 * where journals can be compared directly: `cairn-runtime/tests/mandatory.rs` and
 * `cairn-engine/tests/hello_durable.rs`. It cannot be observed over HTTP: the engine
 * runs on one thread, and while it drives, every query queues behind it —
 * `/invocations/{id}/journal` included.
 */
test("kill the runtime at every turn, restart, identical result and journal shape", async () => {
  // The reference run.
  ctl.killAtTurn = 0;
  ctl.turns = 0;
  ctl.sideEffects = 0;
  const refDir = mkdtempSync(join(tmpdir(), "cf-ref-"));
  const ref = await startRuntime(refDir);
  await register(ref.url);
  const r = await fetch(`${ref.url}/invoke/Work/pipeline?key=w-1`, {
    method: "POST",
    body: JSON.stringify({ n: 21 }),
  });
  const refId = ((await r.json()) as { id: string }).id;
  const expected = await waitCompleted(ref.url, refId);
  assert.deepEqual(expected, { a: 42, b: 43, seen: 1 });
  const refShape = shapeOf(await journalOf(ref.url, refId));
  const totalTurns = ctl.turns;
  ref.stop();
  rmSync(refDir, { recursive: true, force: true });
  assert.ok(totalTurns >= 3, `program too short for the test to mean anything: ${totalTurns} turns`);

  for (let k = 1; k <= totalTurns; k++) {
    trace(`k=${k} start`);
    const dir = mkdtempSync(join(tmpdir(), `cf-kill${k}-`));
    ctl.turns = 0;
    ctl.killAtTurn = k;
    ctl.lastInvocationId = "";
    ctl.sideEffects = 0;

    const rt1 = await startRuntime(dir);
    ctl.kill = () => rt1.stop();
    await register(rt1.url);
    // This call may never answer — the runtime dies midway.
    await fetch(`${rt1.url}/invoke/Work/pipeline?key=w-1`, {
      method: "POST",
      body: JSON.stringify({ n: 21 }),
    }).catch(() => undefined);
    rt1.stop();

    const id = ctl.lastInvocationId;
    assert.ok(id, `k=${k}: the SDK had not yet seen any invocation`);
    trace(`k=${k} killed, id=${id.slice(0, 8)}, restarting`);

    // Restart. Do NOT register again: the deployment must be durable.
    ctl.killAtTurn = 0;
    ctl.kill = () => {};
    const rt2 = await startRuntime(dir);
    const got = await waitCompleted(rt2.url, id);
    assert.deepEqual(got, expected, `k=${k}: different result after the restart`);
    assert.deepEqual(
      shapeOf(await journalOf(rt2.url, id)),
      refShape,
      `k=${k}: the journal has a different shape from the reference run`,
    );
    // `ctx.run` reruns at most once for each result that was not yet durable.
    assert.ok(
      ctl.sideEffects >= 2 && ctl.sideEffects <= 4,
      `k=${k}: side effect ran ${ctl.sideEffects} times, outside the plausible range`,
    );

    trace(`k=${k} done`);
    rt2.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

/**
 * ## The deployment must be durable
 *
 * If it were not, after a restart the runtime would not know whom to call: every
 * timer and every retry would stand still, with no error to see.
 */
test("the deployment survives a restart", async () => {
  const dir = mkdtempSync(join(tmpdir(), "cf-reg-"));
  const rt1 = await startRuntime(dir);
  await register(rt1.url);
  rt1.stop();

  const rt2 = await startRuntime(dir);
  const list = (await (await fetch(`${rt2.url}/deployments`)).json()) as unknown[];
  assert.equal(list.length, 1, "deployment lost after the restart");

  // And callable at once, without registering again.
  ctl.killAtTurn = 0;
  const r = await fetch(`${rt2.url}/invoke/Work/pipeline?key=w-1`, {
    method: "POST",
    body: JSON.stringify({ n: 1 }),
  });
  const id = ((await r.json()) as { id: string }).id;
  assert.deepEqual(await waitCompleted(rt2.url, id), { a: 2, b: 3, seen: 1 });
  rt2.stop();
  rmSync(dir, { recursive: true, force: true });
});

/**
 * ## `EXCLUSIVE` is a promise, not a hint
 *
 * The manifest declares `"ty": "EXCLUSIVE"`. Two concurrent deposits into the same
 * object key, each reading, then sleeping, then writing. Without serialization both
 * read the same old value and one update vanishes.
 */
test("EXCLUSIVE: two concurrent invocations on the same object key lose no update", async () => {
  const dir = mkdtempSync(join(tmpdir(), "cf-excl-"));
  const rt = await startRuntime(dir);
  await register(rt.url);
  ctl.killAtTurn = 0;

  const [a, b] = await Promise.all([
    fetch(`${rt.url}/invoke/Bank/deposit?key=acct-1`, {
      method: "POST",
      body: '{"amount":100}',
    }),
    fetch(`${rt.url}/invoke/Bank/deposit?key=acct-1`, {
      method: "POST",
      body: '{"amount":100}',
    }),
  ]);
  assert.ok([200, 202].includes(a.status) && [200, 202].includes(b.status));

  // Wait for both to finish, then ask for the balance.
  await new Promise((r) => setTimeout(r, 3_000));
  const bal = await fetch(`${rt.url}/invoke/Bank/balance?key=acct-1`, {
    method: "POST",
    body: "{}",
  });
  const body = (await bal.json()) as { status: string; output?: string };
  assert.equal(
    body.output,
    "200",
    `lost update: balance ${body.output}, must be 200 — EXCLUSIVE was not held`,
  );
  rt.stop();
  rmSync(dir, { recursive: true, force: true });
});

/**
 * ## Mandatory test 4 of `protocol-v1.md` §10, with the real SDK
 *
 * A condition that must hold for the test to mean anything: the invocation must
 * still be **in progress** when the user code changes. Changing the code after it
 * finished leaves nothing to replay, and the test goes green without checking anything.
 *
 * So: run until the journal has a `GetState`, kill the runtime, change the code to
 * `Sleep`, restart. The SDK must report 570 and the runtime MUST NOT retry forever.
 */
test("journal mismatch with the real SDK: reports 570 and does not retry forever", async () => {
  const dir = mkdtempSync(join(tmpdir(), "cf-mismatch-"));
  ctl.variant = "a";
  ctl.turns = 0;
  ctl.killAtTurn = 2; // turn 1 finished writing GetState; kill at the start of turn 2
  ctl.lastInvocationId = "";

  const rt1 = await startRuntime(dir);
  ctl.kill = () => rt1.stop();
  await register(rt1.url);
  await fetch(`${rt1.url}/invoke/Shifty/go?key=s-1`, { method: "POST", body: "{}" }).catch(
    () => undefined,
  );
  rt1.stop();
  const id = ctl.lastInvocationId;
  assert.ok(id, "the SDK had not yet seen any invocation");

  // The user code HAS CHANGED: the first command is now Sleep, not GetState.
  ctl.variant = "b";
  ctl.killAtTurn = 0;
  ctl.kill = () => {};
  const rt2 = await startRuntime(dir);

  const deadline = Date.now() + 15_000;
  let status = "";
  while (Date.now() < deadline) {
    const s = (await (await fetch(`${rt2.url}/invocations/${id}`)).json()) as { status: string };
    status = s.status;
    if (status === "failed" || status === "paused") break;
    await new Promise((res) => setTimeout(res, 100));
  }
  assert.equal(status, "paused", "a journal mismatch must stop the invocation in paused");

  const j = await journalOf(rt2.url, id);
  const kinds = JSON.stringify(j);
  assert.ok(kinds.includes("0x0402"), `the journal must still have the old GetState: ${kinds}`);
  assert.ok(!kinds.includes("0x040C"), `a command of the new code slipped into the journal: ${kinds}`);

  // The journal does not grow ⇒ no infinite retry loop.
  await new Promise((res) => setTimeout(res, 1_000));
  assert.deepEqual(await journalOf(rt2.url, id), j, "the journal grew — an infinite retry");
  rt2.stop();
  rmSync(dir, { recursive: true, force: true });
});

/**
 * ## Journal mismatch ⇒ `paused`, deploy the fix, then `resume` ⇒ `completed`
 *
 * One bad deploy must not permanently kill every invocation in progress. v1
 * suspends at `sleep`; "deploy" a diverging v2; the timer fires ⇒ replay on v2 ⇒
 * 570 ⇒ must be `paused`, not `failed`. "Deploy" v3 (matching v1), `resume` ⇒
 * runs on to `completed`.
 */
test("a journal mismatch leaves the invocation paused; resume after the fix completes it", async () => {
  const dir = mkdtempSync(join(tmpdir(), "cf-mender-"));
  ctl.mender = "v1";
  ctl.lastInvocationId = "";
  const rt = await startRuntime(dir);
  try {
    await register(rt.url);
    const r = await fetch(`${rt.url}/invoke/Mender/go/send?key=m-1`, { method: "POST", body: "{}" });
    const sent = await r.text();
    assert.ok(r.ok, sent);
    const id = (JSON.parse(sent) as { id: string }).id;

    // Wait until v1 has written `Sleep` into the journal before changing the code.
    const slept = Date.now() + 10_000;
    for (;;) {
      const kinds = JSON.stringify(await journalOf(rt.url, id));
      if (kinds.includes("0x040C")) break;
      if (Date.now() > slept) throw new Error(`v1 has not slept: ${kinds}`);
      await new Promise((res) => setTimeout(res, 20));
    }
    ctl.mender = "v2";

    const statusOf = async (): Promise<string> =>
      ((await (await fetch(`${rt.url}/invocations/${id}`)).json()) as { status: string }).status;
    let status = "";
    const paused = Date.now() + 15_000;
    while (Date.now() < paused) {
      status = await statusOf();
      if (status === "paused" || status === "failed" || status === "completed") break;
      await new Promise((res) => setTimeout(res, 50));
    }
    assert.equal(status, "paused", "a journal mismatch must leave the invocation paused");

    ctl.mender = "v3";
    const resumed = await fetch(`${rt.url}/invocations/${id}/resume`, { method: "POST" });
    assert.ok(resumed.ok, await resumed.text());
    assert.equal(await waitCompleted(rt.url, id), "fixed");
  } finally {
    rt.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

/**
 * ## A closure longer than the inactivity cap is not cancelled
 *
 * Cap 2 s, a 6 s `ctx.run` closure under `BIDI_STREAM`: the SDK flushes the `Run`
 * command onto the wire before running the closure and then sends a `Keepalive`
 * every cap/3, so the turn is not dropped — the closure runs **exactly once**.
 * Without keepalives the turn would be cancelled at second 2 and the side effect
 * would rerun on every attempt. `REQUEST_RESPONSE` has no channel to send on
 * mid-turn — there the manifest's `inactivityTimeout` is used.
 */
test(
  "a ctx.run longer than the cap survives thanks to keepalives, runs exactly once",
  { skip: sdkMode !== "BIDI_STREAM" ? "keepalives only exist in BIDI_STREAM" : false },
  async () => {
    const dir = mkdtempSync(join(tmpdir(), "cf-keepalive-"));
    ctl.longRuns = 0;
    const rt = await startRuntime(dir, ["--inactivity-timeout-ms", "2000"]);
    try {
      await register(rt.url);
      const r = await fetch(`${rt.url}/invoke/LongRun/go/send?key=k`, { method: "POST", body: "{}" });
      const sent = await r.text();
      assert.ok(r.ok, sent);
      const id = (JSON.parse(sent) as { id: string }).id;
      assert.equal(await waitCompleted(rt.url, id, 30_000), "done");
      assert.equal(ctl.longRuns, 1, "the closure was cancelled and rerun — keepalives did not keep the turn");
    } finally {
      rt.stop();
      rmSync(dir, { recursive: true, force: true });
    }
  },
);

async function durableWaits(url: string): Promise<number> {
  const text = await (await fetch(`${url}/metrics`)).text();
  const m = /^cairnflow_durable_waits_total (\d+)$/m.exec(text);
  assert.ok(m, "cairnflow_durable_waits_total missing from /metrics");
  return Number(m[1]);
}

/**
 * ## Number of durable waits of a one-`ctx.run` invocation
 *
 * Each wait is one WAL flush tick — on S3, tens of ms. Before the commit-path
 * rework: BIDI 7 (`sync` after **every** frame, even though the SDK sends `[Run,
 * ProposeRunCompletion, Suspension]` in one write, plus a separate `dequeue`
 * commit after `End`), RR 3.
 *
 * **The floor is 3 in both modes**, by the rule "nothing leaves before the commit
 * containing it is durable": the `Input` must be durable before the SDK sees it, the
 * `ctx.run` result must be durable before the SDK sees it, and the invocation's
 * result must be durable before the client sees it. RR ≤ 2 was a goal that cannot be
 * reached without breaking that rule. Takes the min of a few measurements — a
 * background turn sneaking in only makes the number larger.
 */
test("one ctx.run costs few durable waits", async () => {
  const dir = mkdtempSync(join(tmpdir(), "cf-waits-"));
  const rt = await startRuntime(dir);
  try {
    await register(rt.url);
    let best = Infinity;
    for (let i = 0; i < 5; i++) {
      const before = await durableWaits(rt.url);
      const r = await fetch(`${rt.url}/invoke/OneRun/go?key=k${i}`, { method: "POST", body: "{}" });
      assert.ok(r.ok, await r.text());
      best = Math.min(best, (await durableWaits(rt.url)) - before);
    }
    // The latency histograms are present and have counted the runs just made.
    const metrics = await (await fetch(`${rt.url}/metrics`)).text();
    for (const h of ["cairnflow_commit_wait_seconds", "cairnflow_drive_seconds"]) {
      const m = new RegExp(`^${h}_count (\\d+)$`, "m").exec(metrics);
      assert.ok(m && Number(m[1]) >= 5, `${h}_count missing or too small: ${m?.[1]}`);
      assert.ok(metrics.includes(`${h}_bucket{le="+Inf"}`), `${h} has no +Inf bucket`);
    }
    const limit = 3;
    trace(`durable waits ${sdkMode}: ${best}`);
    process.stderr.write(`durable waits ${sdkMode}: ${best}\n`);
    assert.ok(best <= limit, `${sdkMode}: ${best} durable waits, cap ${limit}`);
  } finally {
    rt.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

/**
 * ## Replay check
 *
 * An invocation suspended at `sleep` on v1. Replay on v2 (only appends a `ctx.run`
 * after the sleep): matches, and the closure **does not run** — the file does not
 * appear. Replay on v3 (reads state before sleeping): diverges at command 0, with
 * both command kinds. `replay-check` on the same deployment flags that invocation.
 * Nothing is written: the journal is identical before and after.
 */
test("replay check: a match runs no new side effect, a divergence is reported at the right spot", async () => {
  const dir = mkdtempSync(join(tmpdir(), "cf-replay-"));
  ctl.evolve = "v1";
  ctl.evolveMarker = join(dir, "marker");
  const rt = await startRuntime(dir);
  try {
    const reg = await fetch(`${rt.url}/deployments`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ uri: sdk!.url }),
    });
    const dep = (await reg.json()) as { id: string };
    const r = await fetch(`${rt.url}/invoke/Evolve/go/send?key=e`, { method: "POST", body: "{}" });
    const sent = await r.text();
    assert.ok(r.ok, sent);
    const id = (JSON.parse(sent) as { id: string }).id;
    const slept = Date.now() + 10_000;
    for (;;) {
      if (JSON.stringify(await journalOf(rt.url, id)).includes("0x040C")) break;
      if (Date.now() > slept) throw new Error("v1 has not slept");
      await new Promise((res) => setTimeout(res, 20));
    }
    const before = await journalOf(rt.url, id);

    ctl.evolve = "v2";
    const ok = await fetch(`${rt.url}/invocations/${id}/replay?deployment=${dep.id}`, { method: "POST" });
    const okBody = await ok.text();
    assert.equal(ok.status, 200, okBody);
    const okReport = JSON.parse(okBody) as { matches: boolean; upto: number };
    assert.equal(okReport.matches, true, okBody);
    assert.equal(okReport.upto, 1, okBody);
    assert.ok(!existsSync(ctl.evolveMarker), "replay ran the new code's closure");

    ctl.evolve = "v3";
    const bad = await fetch(`${rt.url}/invocations/${id}/replay`, { method: "POST" });
    const badBody = await bad.text();
    assert.equal(bad.status, 200, badBody);
    const badReport = JSON.parse(badBody) as {
      matches: boolean;
      mismatchAt: number;
      expected: string;
      got: string;
    };
    assert.equal(badReport.matches, false, badBody);
    assert.equal(badReport.mismatchAt, 0, badBody);
    assert.equal(badReport.expected, "SLEEP", badBody);
    assert.equal(badReport.got, "GET_STATE", badBody);

    const check = await fetch(`${rt.url}/deployments/${dep.id}/replay-check?service=Evolve`, { method: "POST" });
    const checkBody = await check.text();
    assert.equal(check.status, 200, checkBody);
    const c = JSON.parse(checkBody) as { checked: number; matched: number; failures: unknown[] };
    assert.equal(c.checked, 1, checkBody);
    assert.equal(c.failures.length, 1, checkBody);

    assert.deepEqual(await journalOf(rt.url, id), before, "the replay check wrote something into the journal");
    const st = (await (await fetch(`${rt.url}/invocations/${id}`)).json()) as { status: string };
    assert.equal(st.status, "suspended");
  } finally {
    ctl.evolve = "v1";
    rt.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

/**
 * ## Capturing the next run
 *
 * `POST /admin/capture` for `Work/pipeline`, then one invocation runs to the end:
 * it leaves exactly one bundle. Download the bundle to disk, **stop the runtime**,
 * then `cairn replay --bundle` runs a replay check on the real SDK without a
 * cluster: everything matches, and no `ctx.run` closure reruns.
 */
test("capture the next run, then replay the bundle outside the cluster", async () => {
  assert.ok(existsSync(cli), `missing ${cli} — cargo build --bin cairn`);
  const dir = mkdtempSync(join(tmpdir(), "cf-capture-"));
  const rt = await startRuntime(dir);
  try {
    await register(rt.url);
    const cap = await fetch(`${rt.url}/admin/capture`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ tenant: "default", service: "Work", handler: "pipeline", count: 1, ttl: 600 }),
    });
    const capBody = await cap.text();
    assert.equal(cap.status, 200, capBody);

    const r = await fetch(`${rt.url}/invoke/Work/pipeline/send?key=cap-1`, { method: "POST", body: JSON.stringify({ n: 4 }) });
    const sent = await r.text();
    assert.ok(r.ok, sent);
    const id = (JSON.parse(sent) as { id: string }).id;
    await waitCompleted(rt.url, id);

    let bundles: string[] = [];
    const listed = Date.now() + 10_000;
    while (bundles.length === 0) {
      const l = (await (await fetch(`${rt.url}/admin/capture?tenant=default`)).json()) as { bundles: string[] };
      bundles = l.bundles;
      if (bundles.length === 0) {
        if (Date.now() > listed) throw new Error("no bundle after 10 s");
        await new Promise((res) => setTimeout(res, 50));
      }
    }
    assert.deepEqual(bundles, [id]);
    const raw = await fetch(`${rt.url}/admin/capture/default/${id}`);
    assert.equal(raw.status, 200);
    const file = join(dir, `${id}.bundle`);
    writeFileSync(file, Buffer.from(await raw.arrayBuffer()));
    rt.stop();

    const before = ctl.sideEffects;
    // Not `spawnSync`: the SDK being replayed lives in this very node process, and
    // blocking the event loop would make both wait on each other.
    const out = await new Promise<{ status: number | null; stdout: string; stderr: string }>((resolve) => {
      const p = spawn(cli, ["--json", "replay", "--bundle", file], { stdio: "pipe" });
      let stdout = "";
      let stderr = "";
      p.stdout!.on("data", (c: Buffer) => (stdout += c.toString()));
      p.stderr!.on("data", (c: Buffer) => (stderr += c.toString()));
      const timer = setTimeout(() => p.kill("SIGKILL"), 30_000);
      p.on("close", (status) => {
        clearTimeout(timer);
        resolve({ status, stdout, stderr });
      });
    });
    assert.equal(out.status, 0, `cairn replay: ${out.stdout}\n${out.stderr}`);
    const report = JSON.parse(out.stdout) as { id: string; matches: boolean; upto: number };
    assert.equal(report.id, id);
    assert.equal(report.matches, true, out.stdout);
    assert.ok(report.upto >= 3, out.stdout);
    assert.equal(ctl.sideEffects, before, "replaying the bundle reran a ctx.run closure");
  } finally {
    rt.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

/** The status + output of an invocation. */
async function statusOf(url: string, id: string): Promise<{ status: string; output?: string }> {
  return (await (await fetch(`${url}/invocations/${id}`)).json()) as { status: string; output?: string };
}

/**
 * ## `restart-as-new` from a prefix
 *
 * Step a finishes, suspends at a 60 s `sleep`. Restart from entry 3 (`Input`, `Run a`,
 * result a): the new copy **reuses** result a (the closure does not rerun), sleeps
 * per the current code (10 ms), runs b, finishes. The old copy ends with a pointer.
 */
test("restart from a prefix reuses the durable results", async () => {
  const dir = mkdtempSync(join(tmpdir(), "cf-restart-"));
  ctl.stepsA = 0;
  ctl.stepsB = 0;
  ctl.stepsSleepMs = 60_000;
  const rt = await startRuntime(dir);
  try {
    await register(rt.url);
    const r = await fetch(`${rt.url}/invoke/Steps/go/send?key=s`, { method: "POST", body: "{}" });
    const sent = await r.text();
    assert.ok(r.ok, sent);
    const id = (JSON.parse(sent) as { id: string }).id;
    const until = Date.now() + 10_000;
    while ((await statusOf(rt.url, id)).status !== "suspended") {
      if (Date.now() > until) throw new Error("not yet suspended at sleep");
      await new Promise((res) => setTimeout(res, 20));
    }
    const kinds = (await journalOf(rt.url, id)).map((e) => (e as { ty: string }).ty);
    assert.deepEqual(kinds.slice(0, 4), ["0x0400", "0x0411", "0x8011", "0x040C"], JSON.stringify(kinds));

    ctl.stepsSleepMs = 10;
    const re = await fetch(`${rt.url}/invocations/${id}/restart?from=3`, { method: "POST" });
    const reBody = await re.text();
    assert.equal(re.status, 200, reBody);
    const fresh = (JSON.parse(reBody) as { new: string }).new;
    assert.deepEqual(await waitCompleted(rt.url, fresh), { a: 1, b: 1 });
    assert.equal(ctl.stepsA, 1, "closure a reran — the prefix was not used");
    const old = await statusOf(rt.url, id);
    assert.equal(old.status, "completed");
    assert.deepEqual(JSON.parse(old.output ?? "null"), { continuedAsNew: fresh });
  } finally {
    ctl.stepsSleepMs = 60_000;
    rt.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

/**
 * ## `ctx.continueAsNew` keeps each invocation's journal bounded
 *
 * A 60-step loop, each invocation 5 steps before handing over to a new one: 12
 * invocations, each with a journal ≤ 1 + 2·5 + a few entries — not growing with the total number of steps.
 */
test("continueAsNew: a long loop, each invocation's journal bounded", async () => {
  const dir = mkdtempSync(join(tmpdir(), "cf-can-"));
  const rt = await startRuntime(dir);
  try {
    await register(rt.url);
    const r = await fetch(`${rt.url}/invoke/Looper/go/send?key=l`, {
      method: "POST",
      body: JSON.stringify({ i: 0, n: 60 }),
    });
    const sent = await r.text();
    assert.ok(r.ok, sent);
    let id = (JSON.parse(sent) as { id: string }).id;
    let hops = 0;
    let longest = 0;
    for (;;) {
      const out = (await waitCompleted(rt.url, id, 30_000)) as { continuedAsNew?: string; done?: number };
      longest = Math.max(longest, (await journalOf(rt.url, id)).length);
      if (out.continuedAsNew === undefined) {
        assert.equal(out.done, 60);
        break;
      }
      id = out.continuedAsNew;
      hops++;
      assert.ok(hops < 20, "the continueAsNew chain does not stop");
    }
    assert.equal(hops, 11, "60 steps, 5 steps per invocation ⇒ 12 invocations");
    assert.ok(longest <= 14, `one invocation's journal is ${longest} long — not bounded`);
  } finally {
    rt.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

/** A caller through `ctx.call` waits for the final copy of the chain, not the pointer. */
test("continueAsNew: a ctx.call caller gets the final result of the chain", async () => {
  const dir = mkdtempSync(join(tmpdir(), "cf-can-call-"));
  const rt = await startRuntime(dir);
  try {
    await register(rt.url);
    const r = await fetch(`${rt.url}/invoke/LoopCaller/go/send?key=c`, { method: "POST", body: "{}" });
    const sent = await r.text();
    assert.ok(r.ok, sent);
    const id = (JSON.parse(sent) as { id: string }).id;
    assert.deepEqual(await waitCompleted(rt.url, id, 30_000), { done: 23 });
  } finally {
    rt.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

/**
 * ## Awakeable
 *
 * The invocation sends the awakeable id to a "webhook", then suspends; the webhook
 * calls `/awakeables/{id}/resolve` ⇒ the invocation wakes with that value. `/reject` ⇒
 * the waiter gets a terminal error (which the handler can catch).
 */
test("awakeable: a webhook resolve/reject wakes the invocation", async () => {
  const dir = mkdtempSync(join(tmpdir(), "cf-awk-"));
  const rt = await startRuntime(dir);
  try {
    await register(rt.url);
    for (const [key, action, body, want] of [
      ["a", "resolve", JSON.stringify({ ok: true }), { ok: true }],
      ["b", "reject", JSON.stringify({ message: "denied" }), { rejected: "denied" }],
    ] as const) {
      ctl.awakeableId = "";
      const r = await fetch(`${rt.url}/invoke/Hook/go/send?key=${key}`, { method: "POST", body: "{}" });
      const sent = await r.text();
      assert.ok(r.ok, sent);
      const id = (JSON.parse(sent) as { id: string }).id;
      const until = Date.now() + 10_000;
      while (ctl.awakeableId === "" || (await statusOf(rt.url, id)).status !== "suspended") {
        if (Date.now() > until) throw new Error("not yet suspended waiting for the awakeable");
        await new Promise((res) => setTimeout(res, 20));
      }
      assert.match(ctl.awakeableId, /^awk_[0-9a-f]{56}$/);
      const done = await fetch(`${rt.url}/awakeables/${ctl.awakeableId}/${action}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body,
      });
      assert.equal(done.status, 200, await done.text());
      assert.deepEqual(await waitCompleted(rt.url, id), want);
    }
    const bad = await fetch(`${rt.url}/awakeables/awk_xyz/resolve`, { method: "POST", body: "1" });
    assert.equal(bad.status, 400);
  } finally {
    rt.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

/**
 * ## A bounded signal across `kill -9`
 *
 * `Gate/approve`: `run` → `race(signal "approve", sleep 1 hour)` → `run`. The
 * reference run does not die: it suspends, the test sends the signal, it finishes. Then for each turn `k`:
 *
 * 1. runtime 1 receives the invocation; the handler kills it at turn `k` (before answering);
 * 2. if it is still alive and the invocation has suspended ⇒ the test sends the signal **to runtime 1**;
 * 3. runtime 1 dies ⇒ runtime 2 opens the same directory; if the signal was not yet sent,
 *    it is sent to runtime 2 once the invocation suspends again.
 *
 * Must see: the signal wins (the 1-hour timer does not fire), the same result and
 * the same journal shape as the reference run, `ctx.run` reruns at most once per
 * result not yet durable. Runs in both modes (`CAIRNFLOW_E2E_SDK_MODE`).
 */
test("race(signal, sleep) — kill the runtime at every turn, the signal still wins, identical journal", async () => {
  const signal = (url: string, id: string) =>
    fetch(`${url}/invocations/${id}/signal`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ signal: "approve", value: { by: "boss" } }),
    });
  // `completed`: the signal was already durable on the previous runtime, which then
  // died before answering the test — that send is still the only delivery.
  const untilSuspendedOrDead = async (rt: Runtime, id: string): Promise<"suspended" | "completed" | "dead"> => {
    const until = Date.now() + 20_000;
    for (;;) {
      if (rt.proc.exitCode !== null || rt.proc.signalCode !== null) return "dead";
      try {
        const st = (await statusOf(rt.url, id)).status;
        if (st === "suspended" || st === "completed") return st;
      } catch {
        /* the runtime just died */
      }
      if (Date.now() > until) {
        const last = await statusOf(rt.url, id).catch((e) => String(e));
        throw new Error(`${id} not suspended yet while the runtime has not died: ${JSON.stringify(last)} (turn ${ctl.turns}, k=${ctl.killAtTurn})`);
      }
      await new Promise((r) => setTimeout(r, 20));
    }
  };

  ctl.killAtTurn = 0;
  ctl.turns = 0;
  ctl.sideEffects = 0;
  const refDir = mkdtempSync(join(tmpdir(), "cf-gate-ref-"));
  const ref = await startRuntime(refDir);
  await register(ref.url);
  const sent = await fetch(`${ref.url}/invoke/Gate/approve/send?key=g`, {
    method: "POST",
    body: JSON.stringify({ ms: 3_600_000 }),
  });
  const refId = ((await sent.json()) as { id: string }).id;
  assert.equal(await untilSuspendedOrDead(ref, refId), "suspended");
  assert.equal((await signal(ref.url, refId)).status, 200);
  const expected = await waitCompleted(ref.url, refId);
  assert.deepEqual(expected, { winner: "signal", by: "boss", before: 1, after: 2 });
  const refShape = shapeOf(await journalOf(ref.url, refId));
  const totalTurns = ctl.turns;
  ref.stop();
  rmSync(refDir, { recursive: true, force: true });
  assert.ok(totalTurns >= 2, `program too short: ${totalTurns} turns`);

  for (let k = 1; k <= totalTurns; k++) {
    trace(`gate k=${k} start`);
    const dir = mkdtempSync(join(tmpdir(), `cf-gate${k}-`));
    ctl.turns = 0;
    ctl.killAtTurn = k;
    ctl.lastInvocationId = "";
    ctl.sideEffects = 0;
    const rt1 = await startRuntime(dir);
    ctl.kill = () => rt1.stop();
    await register(rt1.url);
    const r = await fetch(`${rt1.url}/invoke/Gate/approve/send?key=g`, {
      method: "POST",
      body: JSON.stringify({ ms: 3_600_000 }),
    }).catch(() => undefined);
    const id = r?.ok ? ((await r.json()) as { id: string }).id : ctl.lastInvocationId;
    assert.ok(id, `k=${k}: id unknown`);
    let signalled = false;
    if ((await untilSuspendedOrDead(rt1, id)) === "suspended") {
      signalled = (await signal(rt1.url, id).catch(() => undefined))?.status === 200;
    }
    // Wait for runtime 1 to die at turn k (after the signal, turn k may be the continuing turn).
    const dead = Date.now() + 20_000;
    while (rt1.proc.exitCode === null && rt1.proc.signalCode === null) {
      if (Date.now() > dead) break;
      await new Promise((res) => setTimeout(res, 20));
    }
    rt1.stop();
    ctl.killAtTurn = 0;
    ctl.kill = () => {};
    const rt2 = await startRuntime(dir);
    if (!signalled && (await untilSuspendedOrDead(rt2, id)) === "suspended") {
      assert.equal((await signal(rt2.url, id)).status, 200, `k=${k}`);
    }
    const got = await waitCompleted(rt2.url, id);
    assert.deepEqual(got, expected, `k=${k}: result differs from the reference run`);
    assert.deepEqual(shapeOf(await journalOf(rt2.url, id)), refShape, `k=${k}: journal shape differs`);
    assert.ok(
      ctl.sideEffects >= 2 && ctl.sideEffects <= 4,
      `k=${k}: side effect ran ${ctl.sideEffects} times`,
    );
    trace(`gate k=${k} done`);
    rt2.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});
