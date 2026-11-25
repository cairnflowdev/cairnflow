// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Cross-language test: the **Rust** runtime talking to the **TypeScript** SDK.
 *
 * This is the one proof that cannot be faked. Every other test on either side
 * uses a fake for one end; this test uses two independent implementations of the same
 * specification, written in two languages, sharing not a single line of code. If the header
 * layout, a message number or a protobuf field layout differs by one bit, it goes red.
 *
 * Needs the `cairnflow` binary built: `cargo build --bin cairnflow`.
 */

import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { randomBytes } from "node:crypto";
import { createWriteStream, existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, test } from "node:test";
import { fileURLToPath } from "node:url";
import {
  type Context,
  type Endpoint,
  MAX_PROTOCOL_VERSION,
  type ProtocolMode,
  serve,
  service,
  PauseError,
  TerminalError,
  workflow,
  type ServiceContext,
} from "../src/index.ts";
import { ApiSchema, checkResponse } from "./api-schema.ts";
import { RangeChurner, rangesOf, tableProblem } from "./ranges.ts";

const here = fileURLToPath(new URL(".", import.meta.url));
const repoRoot = join(here, "..", "..", "..");
const binary = join(repoRoot, "target", "debug", "cairnflow");

let sdk: Endpoint | undefined;
let runtime: ChildProcess | undefined;
/// Every node of the cluster. `runtimes[0]` is `runtime` itself; the other nodes only exist
/// when `CAIRNFLOW_E2E_NODES > 1` (needs a shared store, i.e. MinIO/SeaweedFS).
let runtimes: ChildProcess[] = [];
let urls: string[] = [];
let runtimeUrl = "";
let dataDir = "";

/// The SDK mode declared in the manifest for this run.
///
/// Set `CAIRNFLOW_E2E_SDK_MODE=REQUEST_RESPONSE` to run **this very suite**
/// on the older transport. That is the proof of the biggest promise of the
/// `BIDI_STREAM` design: the same program, the same journal, only the I/O differs.
const sdkMode: ProtocolMode =
  (process.env.CAIRNFLOW_E2E_SDK_MODE as ProtocolMode | undefined) ?? "BIDI_STREAM";

/// The **"split midway"** shape: set
/// `CAIRNFLOW_E2E_SPLIT_MS=<ms>` (together with `CAIRNFLOW_E2E_ARGS` containing `--cluster`) and
/// throughout the suite a saboteur splits a range and then merges the child pair back, alternating,
/// every `<ms>` — see `ranges.ts`. Same services, same assertions, only the
/// range table changes under their feet. `0` (default) = off.
const splitMs = Number(process.env.CAIRNFLOW_E2E_SPLIT_MS ?? "0") || 0;

/// Pins the SDK to an older wire version: set
/// `CAIRNFLOW_E2E_SDK_MAX_WIRE=1` (or `2`) and the manifest declares exactly that limit,
/// the runtime settles on it, and **this very suite** runs on the older version — the
/// `e2e 1-node-wire1`/`-wire2` rows of `check.sh`.
// Without `identityKeys` (the e2e runtime does not sign) the SDK does not serve wire v6.
const sdkMaxWire = Number(process.env.CAIRNFLOW_E2E_SDK_MAX_WIRE || Math.min(MAX_PROTOCOL_VERSION, 5));
let churner: RangeChurner | undefined;

/// The number of cluster nodes in this run.
function nodeCount(): number {
  return Math.max(1, Number(process.env.CAIRNFLOW_E2E_NODES ?? "1") || 1);
}

/**
 * The runtime's bootstrap key: the suite runs **with auth on** —
 * `CAIRNFLOW_ADMIN_KEY` for the node, `Authorization: Bearer` on every request
 * the test sends. A shape running `--no-auth` (through `CAIRNFLOW_E2E_ARGS`) ignores the
 * header, so the same suite runs in both.
 */
const ADMIN_KEY = `cf_e2e_${randomBytes(16).toString("hex")}`;

const realFetch: typeof globalThis.fetch = (input, init) => {
  const headers = new Headers(init?.headers);
  if (!headers.has("authorization")) headers.set("authorization", `Bearer ${ADMIN_KEY}`);
  return globalThis.fetch(input, { ...init, headers });
};

/**
 * This test file's own `fetch` — **cluster-aware**.
 *
 * Shadows the global `fetch` so every test keeps its wording and still runs
 * on a multi-node cluster, where these things are normal rather than errors:
 *
 * 1. **Hitting the wrong node.** A node that does not hold the partition returns `503` with a
 *    machine-readable `leader` field — retry at exactly that address. This is the API
 *    contract, not an inference from the human-readable text in `error`.
 * 2. **Leader handover window.** A `503` without `leader` (nobody holds it yet) — back off
 *    one beat and ask again, the queue and the lease sort themselves out.
 * 3. **The deployment registry spreads periodically.** A node that has not learned a freshly
 *    registered service yet answers `/invoke` with 404 — only on a multi-node cluster, and only on
 *    the `/invoke` path, that 404 is eventual consistency, not a verdict.
 * 4. **Overloaded range** (L0 throttling). A `429` carrying
 *    `code: "range_overloaded"` means **nothing was created** — resending after
 *    `Retry-After` is per contract. The split/merge row can push a freshly
 *    merged range over the throttle threshold (child ranges inherit the parent's L0 view).
 *
 * `/admin/tick` is **broadcast to every node**: each node only ticks the partitions
 * it leads, so "give it time to run" means telling all of them.
 */
async function fetch(input: string | URL, init?: RequestInit): Promise<Response> {
  const r = await clusterFetch(input, init);
  await checkResponse(apiSchema, init?.method ?? "GET", String(input), r);
  return r;
}

/**
 * Machine-readable contract: **every** response the suite receives on a path present in
 * `docs/api/openapi.json` is checked in strict mode — a body that deviates (a missing field, an
 * undeclared field, a wrong type, an HTTP code outside the contract) turns the test that sent the
 * request red, with the violation.
 */
const apiSchema = new ApiSchema();

async function clusterFetch(input: string | URL, init?: RequestInit): Promise<Response> {
  const original = String(input);
  if (urls.length > 1 && original.includes("/admin/tick")) {
    // Each node only ticks the partitions it leads, so the cluster's `advanced` is the **sum** over
    // the nodes — return a combined response so that the
    // `assert.deepEqual(tick.json, {advanced: n})` checks keep their meaning. A node returning an
    // error (e.g. the deployment registry has not reached it yet) contributes nothing — as long as
    // at least one node accepted.
    const path = original.slice(original.indexOf("/admin/tick"));
    let advanced = 0;
    let anyOk = false;
    let lastErr: Response | undefined;
    for (const u of urls) {
      try {
        const r = await realFetch(u + path, init);
        if (r.ok) {
          anyOk = true;
          const body = (await r.json()) as { advanced?: number };
          advanced += body.advanced ?? 0;
        } else {
          lastErr = r;
        }
      } catch {
        /* a node just killed in a kill test — the remaining nodes still tick */
      }
    }
    if (!anyOk) return lastErr ?? Promise.reject(new Error("no node accepted /admin/tick"));
    return new Response(JSON.stringify({ advanced }), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  }

  let url = original;
  const deadline = Date.now() + 20_000;
  for (;;) {
    let r: Response;
    try {
      r = await realFetch(url, init);
    } catch (e) {
      // The node really died (a kill -9 test). Go back to the original address and wait for the
      // cluster to heal.
      if (Date.now() > deadline) throw e;
      url = original;
      await new Promise((s) => setTimeout(s, 200));
      continue;
    }
    if (r.status === 503) {
      const peek = (await r
        .clone()
        .json()
        .catch(() => undefined)) as { leader?: string } | undefined;
      if (Date.now() > deadline) return r;
      if (peek?.leader) {
        const u = new URL(url);
        url = peek.leader.replace(/\/$/, "") + u.pathname + u.search;
      } else {
        await new Promise((s) => setTimeout(s, 150));
      }
      continue;
    }
    if (r.status === 429 && Date.now() < deadline) {
      const peek = (await r
        .clone()
        .json()
        .catch(() => undefined)) as { code?: string } | undefined;
      if (peek?.code === "range_overloaded") {
        const secs = Number(r.headers.get("retry-after") ?? "1");
        await new Promise((s) => setTimeout(s, Math.min(1000, 1000 * (secs || 1))));
        continue;
      }
    }
    if (
      urls.length > 1 &&
      r.status === 404 &&
      original.includes("/invoke/") &&
      Date.now() < deadline
    ) {
      await new Promise((s) => setTimeout(s, 150));
      continue;
    }
    return r;
  }
}
let sideEffectRuns = 0;
let chargeCalls = 0;
let flakyAttempts = 0;
let idemRuns = 0;
let cancelCleanups = 0;
const seenAttempts: number[] = [];
const progressAttempts: number[] = [];
let step1Runs = 0;
let stockRestocked = false;
const auditLog: string[] = [];

/** A service called by `ctx.call` — runs on the same deployment. */
const payments = service("PaymentService", {
  async charge(_ctx: ServiceContext, input: { amount: number }) {
    chargeCalls++;
    if (input.amount > 1_000) {
      throw new TerminalError("over quota", 402);
    }
    return { receipt: `paid-${input.amount}` };
  },
});

/** Counts the real runs — to check `docs/protocol-v1.md` §8.6. */
const once = service("OnceService", {
  async work(_ctx: ServiceContext, input: { tag: string }) {
    idemRuns++;
    return { ran: idemRuns, tag: input.tag };
  },
});

/** Fails twice, then recovers — to check the retry scheduler and the background loop. */
const flaky = service("FlakyService", {
  async work() {
    flakyAttempts++;
    if (flakyAttempts < 3) {
      // A plain Error ⇒ ErrorMessage with behavior RETRY (`docs/protocol-v1.md` §8.5).
      throw new Error(`transient failure, attempt ${flakyAttempts}`);
    }
    return { attempts: flakyAttempts };
  },

  /**
   * Fails permanently, but **gives up by itself** after 2 attempts thanks to `ctx.attempt`.
   *
   * Without it this is the case of "a tool calling a permanently broken API, retrying forever
   * with a 60s backoff" — only `cancel` can stop it, and someone has to notice it
   * first.
   */
  async giveUp(ctx: ServiceContext) {
    seenAttempts.push(ctx.attempt);
    if (ctx.attempt >= 2) {
      throw new TerminalError(`gave up after ${ctx.attempt} attempts`, 503);
    }
    throw new Error("the remote API is broken");
  },

  /**
   * Step one fails once and then succeeds; step two fails forever.
   *
   * So the journal grows in a turn **after** the first reschedule — exactly the condition under
   * which `ctx.attempt` must count again. A handler whose step one succeeds right in the first turn
   * checks *nothing*: from turn two on it is really stuck at the same place, and counting
   * cumulatively is correct.
   */
  /**
   * Suspends again waiting for a human: out of stock ⇒ `PauseError`, in stock ⇒ ship.
   *
   * `stockRestocked` plays the "outside world" that the operator fixes between two
   * `resume`s.
   *
   * **`PauseError` is thrown from INSIDE the `ctx.run` closure, not after it** — and that is the
   * whole lesson. `ctx.run` journals its result so the side effect is not rerun; so if the stock
   * read *succeeded* (value 0) and the pause happened outside, the next `resume` would replay
   * exactly the stored 0 and pause again forever, even though the stock is full. Thrown from
   * inside, nothing is journaled (`ctx.run` only proposes a result once the closure *finishes*), so
   * next time the closure runs again and sees the new world.
   */
  async shipStock(ctx: ServiceContext) {
    const stock = await ctx.run("read stock", () => {
      if (!stockRestocked) throw new PauseError("out of stock — restock needed");
      return 10;
    });
    return { shipped: stock };
  },

  async progressThenFail(ctx: ServiceContext) {
    progressAttempts.push(ctx.attempt);
    await ctx.run("step one", () => {
      step1Runs++;
      if (step1Runs < 2) throw new Error("step one failed");
      return "done";
    });
    throw new Error("step two failed");
  },
});

/** Target of `ctx.send` — nobody waits for the result. */
const audit = service("AuditService", {
  async record(_ctx: ServiceContext, input: { note: string }) {
    auditLog.push(input.note);
    return null;
  },
});

const orders = service(
  "OrderService",
  {
    async submit(ctx: Context, input: { amount: number }) {
      if (typeof input?.amount !== "number") {
        throw new TerminalError("amount must be a number", 400);
      }
      const soFar = (await ctx.get<number>("total")) ?? 0;
      // Sleep very far: the runtime's background loop uses real time, and we want these
      // tests deterministic — it only fires when /admin/tick says it is due.
      await ctx.sleep(3_600_000, "awaiting confirmation");
      const receipt = await ctx.run("charge", () => {
        sideEffectRuns++;
        return `receipt-${input.amount}`;
      });
      ctx.set("total", soFar + input.amount);
      return { receipt, total: soFar + input.amount, key: ctx.key };
    },

    /** Calls another service and waits for the result. */
    async checkout(ctx: Context, input: { amount: number }) {
      const paid = await ctx.call<{ receipt: string }>(
        "PaymentService",
        "charge",
        { amount: input.amount },
        { key: `acct-${ctx.key}` },
      );
      return { paid: paid.receipt };
    },

    /** Two calls with the same idempotency key ⇒ exactly one invocation (`docs/protocol-v1.md` §8.6). */
    async twice(ctx: Context, input: { tag: string }) {
      const a = await ctx.call<{ ran: number }>("OnceService", "work", input, {
        idempotencyKey: input.tag,
      });
      const b = await ctx.call<{ ran: number }>("OnceService", "work", input, {
        idempotencyKey: input.tag,
      });
      return { a: a.ran, b: b.ran };
    },

    /** One-way call: no waiting, no suspending. */
    async note(ctx: Context, input: { note: string }) {
      ctx.send("AuditService", "record", { note: input.note });
      return { sent: true };
    },

    /** A one-way call WITH A DELAY (`OneWayCall.invoke_time`). */
    async noteLater(ctx: Context, input: { note: string; delayMs: number }) {
      ctx.send("AuditService", "record", { note: input.note }, { delay: input.delayMs });
      return { sent: true };
    },

    /** Waits for a named signal — woken by `approve`, or cancelled cooperatively. */
    async waitApproval(ctx: Context, _input: unknown) {
      try {
        const ok = await ctx.awaitSignal<{ by: string }>("approval");
        return { approvedBy: ok?.by ?? null };
      } catch (e) {
        // `Suspend` also goes through here — only real cancellations are counted.
        if (e instanceof TerminalError) cancelCleanups++;
        throw e;
      }
    },

    /** Sends the signal "approval" to a waiting invocation. */
    async approve(ctx: Context, input: { target: string; by: string }) {
      ctx.sendSignal(input.target, "approval", { by: input.by });
      return { sent: true };
    },

    /**
     * ONE call with an idempotency key — so that two different CALLERS (two object keys ⇒ two
     * invocations, possibly on two partitions) use the same key. Differs from `twice` in that
     * `twice` checks dedup within one caller; this one checks dedup **across callers** — a path
     * that once broke because the target partition was derived from the caller's `spread` instead
     * of from the key itself.
     */
    async callOnce(ctx: Context, input: { tag: string }) {
      const r = await ctx.call<{ ran: number }>("OnceService", "work", input, {
        idempotencyKey: input.tag,
      });
      return { ran: r.ran };
    },

    /**
     * Two one-way `ctx.send`s with the same idempotency key. `docs/protocol-v1.md` §8.6 has no
     * exception for one-way calls — the cross-partition path once skipped dedup when there was no
     * `reply_to`.
     */
    async noteTwice(ctx: Context, input: { tag: string }) {
      ctx.send("OnceService", "work", input, { idempotencyKey: input.tag });
      ctx.send("OnceService", "work", input, { idempotencyKey: input.tag });
      return { sent: true };
    },
  },
  { type: "VIRTUAL_OBJECT", handlerTypes: { submit: "EXCLUSIVE" } },
);

/** Signals and awakeables as leaves of `ctx.race`. */
const awakeableIds = new Map<string, string>();
const raceSignal = service("RaceSignalService", {
  async signalOrSleep(ctx: ServiceContext, input: { ms: number }) {
    const v = await ctx.race([ctx.signal<{ by: string }>("approval"), ctx.sleep(input.ms)]);
    return { winner: v === undefined ? "sleep" : "signal", value: v ?? null };
  },
  async awakeableOrSleep(ctx: ServiceContext, input: { ms: number; tag: string }) {
    const { id, promise } = ctx.awakeable<string>();
    await ctx.run("publish", () => {
      awakeableIds.set(input.tag, id);
      return id;
    });
    const v = await ctx.race([promise, ctx.sleep(input.ms)]);
    return { winner: v === undefined ? "sleep" : "awakeable", value: v ?? null };
  },
  /** Parallel `ctx.run` with `Promise.all` (like the AI SDK's parallel tools). */
  async parallelRuns(ctx: ServiceContext, input: { tag: string }) {
    const bump = (k: string) => parallelRuns.set(k, (parallelRuns.get(k) ?? 0) + 1);
    const [a, b] = await Promise.all([
      ctx.run("slow", async () => {
        bump(`${input.tag}/slow`);
        await new Promise((r) => setTimeout(r, 150));
        return 1;
      }),
      ctx.run("fast", () => {
        bump(`${input.tag}/fast`);
        return 2;
      }),
    ]);
    return a + b;
  },
});
/** How many times each closure of `parallelRuns` really ran, by `tag/step`. */
const parallelRuns = new Map<string, number>();

/** Phase 4 features: eager state, getKeys/clearAll, combinators, promises, attach. */
const phase4 = service(
  "Phase4Service",
  {
    /** The full state round trip: set → getKeys → get → clearAll → read again. */
    async stateOps(ctx: Context) {
      ctx.set("a", 1);
      ctx.set("b", 2);
      const keys = await ctx.getKeys();
      const a = await ctx.get<number>("a");
      ctx.clearAll();
      const after = await ctx.getKeys();
      const gone = await ctx.get<number>("a");
      return { keys, a, after, gone };
    },
    async seed(ctx: Context) {
      ctx.set("a", 41);
      return { ok: true };
    },
    /** Reads state written by an EARLIER invocation — through the StartMessage's state_map. */
    async readBack(ctx: Context) {
      return { a: await ctx.get<number>("a"), keys: await ctx.getKeys() };
    },
    /**
     * Eager getKeys, a new key, then a suspension: the turn after the sleep gets a snapshot
     * that already holds `b`, and replays the journaled `getKeys` (`[a]`) without a 570.
     */
    async keysAcrossSleep(ctx: Context) {
      const keys = await ctx.getKeys();
      ctx.set("b", 2);
      await ctx.sleep(10);
      return { keys, after: await ctx.getKeys() };
    },
    /** race: a fast call vs a one-hour sleep — the call must win. */
    async raceCallVsSleep(ctx: Context, input: { amount: number }) {
      const paid = ctx.call<{ receipt: string }>("PaymentService", "charge", {
        amount: input.amount,
      });
      const timer = ctx.sleep(3_600_000, "timeout");
      const winner = await ctx.race([paid, timer]);
      return { winner: winner === undefined ? "timeout" : (winner as { receipt: string }).receipt };
    },
    /** all: two calls in parallel, waits for both. */
    async allCalls(ctx: Context) {
      const a = ctx.call<{ receipt: string }>("PaymentService", "charge", { amount: 1 });
      const b = ctx.call<{ receipt: string }>("PaymentService", "charge", { amount: 2 });
      const [ra, rb] = await ctx.all([a, b] as const);
      return { a: ra.receipt, b: rb.receipt };
    },
    /** Waits on this object's durable promise — another handler approves it. */
    async waitApproval(ctx: Context) {
      const v = await ctx.promise<string>("approval").get();
      return { got: v };
    },
    async approve(ctx: Context, input: { value: string }) {
      await ctx.promise<string>("approval").resolve(input.value);
      return { done: true };
    },
    async peekApproval(ctx: Context) {
      return { v: await ctx.promise<string>("approval").peek() };
    },
    /** Waits for the result of another invocation by id. */
    async attachTo(ctx: Context, input: { id: string }) {
      return { got: await ctx.attach<unknown>(input.id) };
    },
    /** Peeks at the result without waiting: null if not done yet. */
    async peekOutputOf(ctx: Context, input: { id: string }) {
      return { out: await ctx.getOutput<unknown>(input.id) };
    },
    /** Returns the id the handler sees itself — to check it works as an HTTP id. */
    async whoAmI(ctx: Context) {
      return { invocationId: ctx.invocationId, debugId: ctx.debugId };
    },
  },
  // `waitApproval` holds the VO lock while suspended (an undeclared VO handler is
  // EXCLUSIVE), so the approver and the peeker on the same key must be SHARED.
  { type: "VIRTUAL_OBJECT", handlerTypes: { approve: "SHARED", peekApproval: "SHARED" } },
);

/**
 * Large frames — criterion (1) of the design: "one 16MiB frame kills nobody".
 *
 * Four MiB go **both ways** on the same stream: up in
 * `ProposeRunCompletion`, back down in `RunCompletion` when the runtime resolves
 * the next suspension point. HTTP/2's default receive window is 64KiB, so this number
 * forces the stream through flow control dozens of times — and a side that forgets to read
 * while writing hangs hard here instead of hanging in production.
 */
// A virtual object, called with a key: `ctx.get` only makes sense when the state has an owner — the
// state of an unkeyed, unscoped service is rejected (D-a).
const bulky = service(
  "BulkService",
  {
    async big(ctx: Context, input: { bytes: number }) {
      const blob = await ctx.run("blob", () => "x".repeat(input.bytes));
      // Force a suspension point *after* `ctx.run`: otherwise that big result never
      // has to travel back down the wire and the test only measures one direction.
      const seen = (await ctx.get<number>("not-set")) ?? 0;
      return { len: blob.length, head: blob.slice(0, 3), seen };
    },
  },
  { type: "VIRTUAL_OBJECT" },
);

/**
 * Counts round trips down to the SDK — criterion (c) of the `BIDI_STREAM` design.
 *
 * Three consecutive `ctx.run`s and one **lazy** `ctx.get`. Lazy is the crux:
 * this object has no state yet, so the `StartMessage` carries no eager snapshot, and
 * `ctx.get` must ask the runtime and suspend. The runtime can answer **within the turn**
 * — it has just read — so that is exactly where `REQUEST_RESPONSE` must end the turn,
 * open a second request and resend the whole journal, while `BIDI_STREAM` only pushes one
 * notification into the open stream.
 */
const roundtrip = service(
  "RoundTripService",
  {
    async chain(ctx: Context) {
      const a = await ctx.run("a", () => 1);
      const seen = (await ctx.get<number>("not-set")) ?? 0;
      const b = await ctx.run("b", () => a + 1);
      const c = await ctx.run("c", () => b + 1);
      return { a, b, c, seen };
    },
  },
  { type: "VIRTUAL_OBJECT" },
);

/**
 * A real workflow: `run` waits on a promise, a `SHARED` handler knocks on it.
 *
 * This is the shape users will write — manual approval, onboarding, a provider's callback — and it
 * only works if three things hold together: `run` does **not** lock by key (if it did, `click`
 * would queue behind it and the two would wait on each other), the promise lives by
 * `(service, workflow key)`, and the run has a deterministic identity so `attach` can find it again
 * without an id.
 */
const signup = workflow("SignupWorkflow", {
  async run(ctx: Context, input: { email: string }) {
    ctx.set("email", input.email);
    const secret = await ctx.run("secret", () => "s3cret");
    const clicked = await ctx.promise<string>("email-link-clicked").get();
    return { key: ctx.key, ok: clicked === secret };
  },
  async click(ctx: Context, input: { secret: string }) {
    await ctx.promise<string>("email-link-clicked").resolve(input.secret);
    return { ok: true };
  },
  /** Read-only — proves a SHARED handler sees the state `run` wrote. */
  async peek(ctx: Context) {
    return { email: await ctx.get<string>("email") };
  },
});

/**
 * Starts a workflow **from inside another invocation** — the `ctx.send` /
 * `ctx.call` path, not the HTTP door.
 *
 * This is the path where the deterministic id has to stand on its own: the caller computes the id
 * from `(service, key)` and, on a multi-partition cluster, sends it to the workflow's partition —
 * where the duplicate check really runs. A small mistake means two runs for the same key, exactly
 * the one thing a workflow promises never happens.
 */
const starter = service("WorkflowStarter", {
  async startTwice(ctx: ServiceContext, input: { key: string }) {
    ctx.send("SignupWorkflow", "run", { email: "called-from-service@x" }, { key: input.key });
    try {
      await ctx.call("SignupWorkflow", "run", { email: "second-call@x" }, { key: input.key });
      return { second: "ran" };
    } catch (e) {
      // The error code is what lets user code tell "someone is already running it" from
      // "it ran and failed" — so it must reach this point, not just the message text.
      return { second: "rejected", code: e instanceof TerminalError ? e.code : null };
    }
  },
});

/**
 * Rare outcomes for the shape check: `paused` for running out of
 * `maxAttempts` (575), and a workflow's `run` turn `failed`.
 */
const shapes = service(
  "ShapeService",
  {
    async exhaust() {
      throw new Error("fails every time (checks the shape of paused 575)");
    },
  },
  { options: { retryPolicy: { initialInterval: 20, maxInterval: 50, maxAttempts: 2, onMaxAttempts: "PAUSE" } } },
);

const shapeFlow = workflow("ShapeWorkflow", {
  async run() {
    throw new TerminalError("workflow fails on purpose", 422);
  },
});

/**
 * Reads the body **once**, then parses it.
 *
 * An `await r.text()` placed in an `assert` message argument is still evaluated eagerly, so it
 * consumes the body before `r.json()` gets to read it. That bug does not turn the test red where it
 * happens but in the *next* test, as missing data — very easy to wrongly blame on the runtime.
 */
async function readBody(r: Response): Promise<{ status: number; text: string; json: any }> {
  const text = await r.text();
  let json: any = undefined;
  try {
    json = JSON.parse(text);
  } catch {
    /* not JSON */
  }
  return { status: r.status, text, json };
}

async function waitFor(url: string, timeoutMs = 15_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try {
      const r = await fetch(url);
      if (r.ok) return;
    } catch {
      /* not up yet */
    }
    if (Date.now() > deadline) throw new Error(`${url} did not come up within ${timeoutMs}ms`);
    await new Promise((r) => setTimeout(r, 50));
  }
}

/// Waits until an invocation has fully stopped, then returns its final status.
///
/// **Needed when a call crosses a partition boundary.** Within one partition,
/// `ctx.call` finishes inside the caller's turn — the runtime creates the callee, runs
/// it and feeds the result back, all before answering HTTP. Across partitions it
/// cannot: the receiver **writes the message down and acks at once**, and processes it at its
/// own pace. Processing synchronously while the caller waits would deadlock two engine
/// threads on a call cycle A→B→A.
///
/// So `waiting` on a multi-partition cluster is **correct**, not slow. What must be
/// asserted is that it *finishes*, not that it finishes in one turn.
/** Waits for an invocation to suspend at a wait point (`suspended`). */
async function suspended(id: string, timeoutMs = 20_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const r = await fetch(`${runtimeUrl}/invocations/${id}`);
    if (r.ok && ((await r.json()) as any).status === "suspended") return;
    if (Date.now() > deadline) throw new Error(`${id} did not suspend within ${timeoutMs}ms`);
    await new Promise((res) => setTimeout(res, 50));
  }
}

async function settle(id: string, timeoutMs = 20_000): Promise<any> {
  const deadline = Date.now() + timeoutMs;
  let last = "(not read yet)";
  for (;;) {
    const r = await fetch(`${runtimeUrl}/invocations/${id}`);
    if (r.ok) {
      const body = (await r.json()) as any;
      if (body.status !== "running" && body.status !== "suspended") return body;
      last = JSON.stringify(body);
    } else {
      last = `${r.status} ${await r.text().catch(() => "")}`;
    }
    if (Date.now() > deadline) {
      throw new Error(`invocation ${id} did not stop within ${timeoutMs}ms — last read: ${last}`);
    }
    await new Promise((r) => setTimeout(r, 100));
  }
}

before(async () => {
  if (!existsSync(binary)) {
    throw new Error(
      `missing binary ${binary} — run \`cargo build --bin cairnflow\` first`,
    );
  }
  sdk = await serve({
    services: [
      orders,
      payments,
      audit,
      flaky,
      once,
      phase4,
      roundtrip,
      bulky,
      signup,
      starter,
      shapes,
      shapeFlow,
      raceSignal,
    ],
    port: 0,
    protocolMode: sdkMode,
    maxProtocolVersion: sdkMaxWire,
  });

  dataDir = mkdtempSync(join(tmpdir(), "cairnflow-e2e-"));

  // Default: one node, one partition, data in a temporary directory.
  //
  // Set `CAIRNFLOW_E2E_ARGS` to run **this very suite** in another
  // shape — for example a multi-partition cluster on MinIO. That is the cheapest way to know
  // whether `ctx.call` across a partition boundary really works: the same
  // services, the same assertions, only where they run changes.
  const extra = process.env.CAIRNFLOW_E2E_ARGS;
  const args = extra
    ? extra.split(/\s+/).filter(Boolean).concat(["--port", "0"])
    : ["--data-dir", dataDir, "--port", "0"];

  // Port 0 ⇒ the OS picks a port; read it back from the first log line.
  const spawnNode = async (nodeId: string): Promise<[ChildProcess, string]> => {
    const proc = spawn(binary, args.concat(["--node-id", nodeId]), {
      stdio: "pipe",
      env: { ...process.env, CAIRNFLOW_ADMIN_KEY: ADMIN_KEY },
    });
    // `CAIRNFLOW_E2E_LOG_DIR=<dir>` ⇒ keep each node's log — the only way
    // to track down an invocation stuck while the range table changes midway.
    const logDir = process.env.CAIRNFLOW_E2E_LOG_DIR;
    if (logDir) {
      const out = createWriteStream(join(logDir, `${nodeId}.log`), { flags: "a" });
      proc.stdout!.pipe(out);
      proc.stderr!.pipe(out);
    }
    const url = await new Promise<string>((resolve, reject) => {
      let buf = "";
      const timer = setTimeout(() => reject(new Error("runtime did not report a port")), 15_000);
      proc.stdout!.on("data", (c: Buffer) => {
        buf += c.toString();
        const m = /http:\/\/[\d.]+:(\d+)/.exec(buf);
        if (m) {
          clearTimeout(timer);
          resolve(m[0]!);
        }
      });
      proc.on("exit", (code) => reject(new Error(`runtime exited early: ${code}\n${buf}`)));
    });
    return [proc, url];
  };

  if (nodeCount() > 1 && !extra?.includes("--cluster")) {
    throw new Error("CAIRNFLOW_E2E_NODES > 1 needs CAIRNFLOW_E2E_ARGS with --cluster and a shared store");
  }
  for (let i = 0; i < nodeCount(); i++) {
    const [proc, url] = await spawnNode(`e2e-node-${i}`);
    runtimes.push(proc);
    urls.push(url);
  }
  runtime = runtimes[0];
  runtimeUrl = urls[0]!;
  for (const u of urls) {
    await waitFor(`${u}/health`);
  }

  // In cluster mode `/health` only says "HTTP is up" — it does **not** say which partitions
  // have a leader yet, and a node that has not won its lease returns 503 NotLeader
  // (per principle N3). Wait until every partition has an owner, otherwise the test
  // measures the startup window instead of what it means to measure.
  if (extra?.includes("--cluster")) {
    const deadline = Date.now() + 30_000;
    for (;;) {
      const r = await fetch(`${runtimeUrl}/cluster`).then((x) => x.json() as any);
      if (r.partitions.every((p: any) => p.leader)) break;
      if (Date.now() > deadline) {
        throw new Error(`a partition has no leader yet: ${JSON.stringify(r)}`);
      }
      await new Promise((r) => setTimeout(r, 100));
    }
  }

  if (splitMs > 0) {
    if (!extra?.includes("--cluster")) {
      throw new Error("CAIRNFLOW_E2E_SPLIT_MS needs CAIRNFLOW_E2E_ARGS with --cluster");
    }
    churner = new RangeChurner((path, init) => fetch(`${runtimeUrl}${path}`, init), {
      everyMs: splitMs,
    });
    churner.start();
  }
});

after(async () => {
  // Stop the saboteur **before** killing nodes: never abandon a split midway
  // and let `after` report its own error.
  await churner?.stop();
  for (const p of runtimes) {
    p.kill("SIGKILL");
  }
  await sdk?.close();
  if (dataDir) rmSync(dataDir, { recursive: true, force: true });
});

test("the Rust runtime runs a TypeScript service end to end", async () => {
  // ── Discovery: the runtime calls the SDK's /discover, reads the manifest, settles the version.
  const reg = await readBody(
    await fetch(`${runtimeUrl}/deployments`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ uri: sdk!.url }),
    }),
  );
  assert.equal(reg.status, 200, reg.text);
  // v3 = keepalive + replay_only (v2 = flow control). Both ends declare v3, so they
  // settle on v3 (or the limit the SDK pinned), and the runtime repeats that number in
  // the content-type of `/invoke` — which is what `ctx.scoped(...)` and keepalive rely on.
  assert.equal(reg.json.wireVersion, sdkMaxWire, "wrong wire version negotiated");
  assert.equal(reg.json.protocolMode, sdkMode, "the runtime settled on the wrong transport");
  assert.deepEqual(reg.json.services, [
    "OrderService",
    "PaymentService",
    "AuditService",
    "FlakyService",
    "OnceService",
    "Phase4Service",
    "RoundTripService",
    "BulkService",
    "SignupWorkflow",
    "WorkflowStarter",
    "ShapeService",
    "ShapeWorkflow",
    "RaceSignalService",
  ]);

  // ── Invoke. There is a ctx.sleep, so it must suspend, not block.
  const inv = await readBody(
    await fetch(`${runtimeUrl}/invoke/OrderService/submit?key=order-42`, {
      method: "POST",
      body: JSON.stringify({ amount: 100 }),
    }),
  );
  assert.equal(inv.status, 202, inv.text);
  assert.equal(inv.json.status, "waiting");

  // ── The timer is not due yet.
  const early = await readBody(
    await fetch(`${runtimeUrl}/admin/tick?now_ms=1`, { method: "POST" }),
  );
  assert.deepEqual(early.json, { advanced: 0 }, "the timer fired before it was due");

  // ── Due: runs on to the end.
  const tick = await readBody(
    await fetch(`${runtimeUrl}/admin/tick?now_ms=${Date.now() + 7_200_000}`, { method: "POST" }),
  );
  assert.deepEqual(tick.json, { advanced: 1 }, tick.text);

  // `settle` rather than a single read: on a multi-node cluster the read may be
  // served by a follower with staleness up to the reader's poll interval — "completed"
  // arrives, just not instantly.
  const st = await settle(inv.json.id);
  assert.equal(st.status, "completed", JSON.stringify(st));
  assert.deepEqual(JSON.parse(st.output), {
    receipt: "receipt-100",
    total: 100,
    key: "order-42",
  });

  // ── ctx.run runs exactly once, even though the invocation goes through several HTTP turns.
  assert.equal(sideEffectRuns, 1, "the side effect ran several times across replay turns");
});

test("virtual object state is durable across two invocations", async () => {
  const second = await readBody(
    await fetch(`${runtimeUrl}/invoke/OrderService/submit?key=order-42`, {
      method: "POST",
      body: JSON.stringify({ amount: 50 }),
    }),
  );
  assert.equal(second.status, 202, second.text);

  await fetch(`${runtimeUrl}/admin/tick?now_ms=${Date.now() + 7_200_000}`, { method: "POST" });

  const st = await settle(second.json.id);
  const out = JSON.parse(st.output) as { total: number };
  assert.equal(out.total, 150, `state did not accumulate across two invocations: ${JSON.stringify(st)}`);
  assert.equal(sideEffectRuns, 2, "the second invocation must run its own side effect");
});

test("ctx.call: one service calls another and gets the result", async () => {
  const before = chargeCalls;
  const r = await readBody(
    await fetch(`${runtimeUrl}/invoke/OrderService/checkout?key=order-7`, {
      method: "POST",
      body: JSON.stringify({ amount: 250 }),
    }),
  );
  // **This is the safety net of the uniform ingress contract.** Before `/invoke` knew how to wait,
  // this line had to branch on the partition count: with one partition `ctx.call` finished within
  // the turn ⇒ 200; across partitions it was `waiting` ⇒ 202 and had to `settle`. Same code, same
  // request, two shapes — depending on where the callee hashed to. Now the rule is the same in
  // every cluster shape, so the assertion is the same too. If this line goes red on a
  // multi-partition cluster, the contract has slipped back.
  assert.equal(r.status, 200, `the /invoke contract is no longer uniform: ${r.text}`);
  const done = r.json;
  assert.equal(done.status, "completed", JSON.stringify(done));
  assert.deepEqual(JSON.parse(done.output), { paid: "paid-250" });
  assert.equal(chargeCalls, before + 1, "the callee must run exactly once");
});

test("ctx.call: the callee's error surfaces at the caller", async () => {
  const r = await readBody(
    await fetch(`${runtimeUrl}/invoke/OrderService/checkout?key=order-8`, {
      method: "POST",
      body: JSON.stringify({ amount: 5_000 }),
    }),
  );
  // The callee throws TerminalError ⇒ the caller gets that error, no suspending and no retry.
  const r2 = { json: await settle(r.json.id), text: r.text };
  const out = r2.json.output === undefined ? (r2.text || JSON.stringify(r2.json)) : String(r2.json.output);
  assert.ok(
    /over quota|402/.test(out) || r2.json.status === "failed",
    `the callee's error did not reach the caller: ${r.text}`,
  );
});

test("ctx.send: a one-way call, no waiting for the result", async () => {
  const before = auditLog.length;
  const r = await readBody(
    await fetch(`${runtimeUrl}/invoke/OrderService/note?key=order-9`, {
      method: "POST",
      body: JSON.stringify({ note: "seen" }),
    }),
  );
  // `ctx.send` is one-way: the caller finishes at once, and **the target runs on a later beat** —
  // in every shape, not only across partitions.
  //
  // An earlier version branched on topology: within one partition it required the target to have
  // finished by the time `/invoke` answered. That promise was dropped. It is exactly the kind of
  // contract that depends on topology — green on a one-partition dev setup, broken on an
  // eight-partition production — so no shape offers a "same-partition target runs at once"
  // shortcut.
  //
  // What still holds, and is worth holding: the call is **durable and dispatched** before
  // `/invoke` answers. It is not dropped; it only runs at its own pace.
  assert.equal(r.status, 200, r.text);
  await settle(r.json.id);
  const deadline = Date.now() + 20_000;
  while (auditLog.length === before && Date.now() < deadline) {
    await new Promise((res) => setTimeout(res, 100));
  }
  assert.equal(auditLog.length, before + 1, "ctx.send did not reach its target");
  assert.equal(auditLog.at(-1), "seen");
});

test("ctx.send with a delay: runs on time, not immediately", async () => {
  const before = auditLog.length;
  const delayMs = 1_500;
  const sentAt = Date.now();
  const r = await readBody(
    await fetch(`${runtimeUrl}/invoke/OrderService/noteLater?key=order-10`, {
      method: "POST",
      body: JSON.stringify({ note: "scheduled", delayMs }),
    }),
  );
  await settle(r.json.id);

  // The first half of "on time": NOT run before it is due. This is exactly the old
  // bug — invoke_time was encoded and stored all along the wire but nobody read it, so
  // a delayed send ran immediately, silently wrong.
  await new Promise((res) => setTimeout(res, 400));
  assert.equal(
    auditLog.length,
    before,
    "the delayed send ran BEFORE its scheduled time — invoke_time is being ignored",
  );

  // The second half: runs once due (the precision is the tick interval, 200ms).
  const deadline = Date.now() + 20_000;
  while (auditLog.length === before && Date.now() < deadline) {
    await new Promise((res) => setTimeout(res, 100));
  }
  assert.equal(auditLog.length, before + 1, "the delayed send never ran");
  assert.equal(auditLog.at(-1), "scheduled");
  const elapsed = Date.now() - sentAt;
  assert.ok(
    elapsed >= delayMs,
    `the delayed send ran after ${elapsed}ms, earlier than the scheduled ${delayMs}ms`,
  );
});

test("signal: awaitSignal is woken by sendSignal from another invocation", async () => {
  // A suspends waiting for the signal "approval".
  const a = await readBody(
    await fetch(`${runtimeUrl}/invoke/OrderService/waitApproval?key=approval-1`, {
      method: "POST",
      body: "{}",
    }),
  );
  assert.equal(a.json.status, "waiting", a.text);
  const aId = a.json.id as string;

  // B (a different object key — possibly on another partition) sends a signal to A by id.
  const b = await readBody(
    await fetch(`${runtimeUrl}/invoke/OrderService/approve?key=approval-2`, {
      method: "POST",
      body: JSON.stringify({ target: aId, by: "boss" }),
    }),
  );
  await settle(b.json.id);

  // A wakes up with exactly the signal's payload.
  const done = await settle(aId);
  assert.equal(done.status, "completed", JSON.stringify(done));
  assert.deepEqual(JSON.parse(done.output), { approvedBy: "boss" });
});

test("outside door: send a signal over HTTP, no intermediate invocation needed", async () => {
  const a = await readBody(
    await fetch(`${runtimeUrl}/invoke/OrderService/waitApproval/send?key=external-approval`, {
      method: "POST",
      body: "{}",
    }),
  );
  assert.equal(a.status, 202, a.text);
  const aId = a.json.id as string;

  // **No `approve` handler is called.** The outside system wakes it directly by id.
  const r = await readBody(
    await fetch(`${runtimeUrl}/invocations/${aId}/signal`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ signal: "approval", value: { by: "webhook" } }),
    }),
  );
  assert.equal(r.status, 200, r.text);

  const done = await settle(aId);
  assert.equal(done.status, "completed", JSON.stringify(done));
  assert.deepEqual(JSON.parse(done.output), { approvedBy: "webhook" });

  // Resending is a no-op: dedupe by signal identity lives in the state machine, so
  // a webhook retry reruns nothing. The invocation is done ⇒ 200 with its status.
  const again = await readBody(
    await fetch(`${runtimeUrl}/invocations/${aId}/signal`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ signal: "approval", value: { by: "second time" } }),
    }),
  );
  assert.equal(again.status, 200, again.text);
  assert.equal(again.json.status, "completed", again.text);
  const settled = await settle(aId);
  assert.deepEqual(
    JSON.parse(settled.output),
    { approvedBy: "webhook" },
    "resending a delivered signal must not change the result",
  );
});

test("cooperative cancel (?mode=signal): the SDK gets to clean up, the waiter gets an error", async () => {
  const r = await readBody(
    await fetch(`${runtimeUrl}/invoke/OrderService/waitApproval?key=approval-9`, {
      method: "POST",
      body: "{}",
    }),
  );
  assert.equal(r.json.status, "waiting", r.text);
  const id = r.json.id as string;
  const before = cancelCleanups;

  const cancel = await readBody(
    await fetch(`${runtimeUrl}/invocations/${id}/cancel?mode=signal`, { method: "POST" }),
  );
  assert.equal(cancel.json.cancelled, true, cancel.text);
  assert.equal(cancel.json.status, "failed", cancel.text);
  assert.equal(
    cancelCleanups,
    before + 1,
    "user code must get to run its cleanup — that is the whole meaning of 'cooperative'",
  );

  const st = await settle(id);
  assert.equal(st.status, "failed", JSON.stringify(st));
});

test("retry: a transient error is retried by the background loop, with backoff", async () => {
  // `/send`, not `/invoke`: what is measured here is "the runtime does NOT call again on
  // the spot", and `/invoke` now waits until the invocation fully stops — it would return after
  // all three attempts had finished, and could no longer see the intermediate state.
  // `/send` answers as soon as the invocation is durable, so it is the only door that
  // can observe what needs asserting.
  const r = await readBody(
    await fetch(`${runtimeUrl}/invoke/FlakyService/work/send`, {
      method: "POST",
      body: JSON.stringify({}),
    }),
  );
  assert.equal(r.status, 202, r.text);
  // `<= 1` rather than `=== 1`: `/send` answers between "durable" and "ran the first
  // turn", so at the moment of the read it may be 0. What must be ruled out is 3 — calling again
  // on the spot, without backoff.
  assert.ok(
    flakyAttempts <= 1,
    `the runtime called again immediately instead of scheduling: ${flakyAttempts} attempts`,
  );

  // The background loop reruns by itself, nobody needs to call /admin/tick.
  const deadline = Date.now() + 15_000;
  let st = await readBody(await fetch(`${runtimeUrl}/invocations/${r.json.id}`));
  while (st.json?.status !== "completed" && Date.now() < deadline) {
    await new Promise((res) => setTimeout(res, 100));
    st = await readBody(await fetch(`${runtimeUrl}/invocations/${r.json.id}`));
  }
  assert.equal(st.json.status, "completed", `the background loop did not rerun: ${st.text}`);
  assert.equal(flakyAttempts, 3, "the attempt count does not match");
  assert.deepEqual(JSON.parse(st.json.output), { attempts: 3 });
});

test("PauseError → paused → resume: once fixed, it continues from the right place", async () => {
  stockRestocked = false;

  // Out of stock ⇒ the handler suspends again instead of retrying forever or dying for good.
  const r = await readBody(
    await fetch(`${runtimeUrl}/invoke/FlakyService/shipStock`, {
      method: "POST",
      body: "{}",
    }),
  );
  assert.equal(r.status, 409, `PauseError must yield paused: ${r.text}`);
  const id = r.json.id as string;

  const st = await readBody(await fetch(`${runtimeUrl}/invocations/${id}`));
  assert.equal(st.json.status, "paused", st.text);

  // Resume BEFORE anything is fixed: it suspends again. An honest answer, not "ok".
  const resumedEarly = await readBody(
    await fetch(`${runtimeUrl}/invocations/${id}/resume`, { method: "POST" }),
  );
  assert.equal(resumedEarly.status, 200, resumedEarly.text);
  assert.equal(resumedEarly.json.status, "paused", `with no restock it must pause again: ${resumedEarly.text}`);

  // The operator restocks, then resumes.
  stockRestocked = true;
  const ok = await readBody(
    await fetch(`${runtimeUrl}/invocations/${id}/resume`, { method: "POST" }),
  );
  assert.equal(ok.status, 200, ok.text);
  assert.equal(ok.json.status, "completed", `after the fix it must run to the end: ${ok.text}`);

  const done = await readBody(await fetch(`${runtimeUrl}/invocations/${id}`));
  assert.deepEqual(JSON.parse(done.json.output), { shipped: 10 });

  // Resuming something already done is a 409 — an operator who typed the wrong id needs to know.
  const lai = await readBody(
    await fetch(`${runtimeUrl}/invocations/${id}/resume`, { method: "POST" }),
  );
  assert.equal(lai.status, 409, lai.text);
});

test("ctx.attempt: the handler gives up by itself instead of retrying forever", async () => {
  seenAttempts.length = 0;
  const r = await readBody(
    await fetch(`${runtimeUrl}/invoke/FlakyService/giveUp/send`, {
      method: "POST",
      body: "{}",
    }),
  );
  assert.equal(r.status, 202, r.text);

  // The background loop retries; the handler stops itself on the third attempt with TerminalError.
  const done = await settle(r.json.id, 30_000);
  assert.equal(
    done.status,
    "failed",
    `the handler must give up by itself, not retry forever: ${JSON.stringify(done)}`,
  );

  // This is the part proving field 7 really arrives: before it was wired up,
  // `ctx.attempt` was always 0 and this loop never ended.
  assert.deepEqual(
    seenAttempts,
    [0, 1, 2],
    `ctx.attempt must count 0,1,2 across attempts: ${JSON.stringify(seenAttempts)}`,
  );
});

test("ctx.attempt: counts AGAIN when the handler makes progress", async () => {
  progressAttempts.length = 0;
  const r = await readBody(
    await fetch(`${runtimeUrl}/invoke/FlakyService/progressThenFail/send`, {
      method: "POST",
      body: "{}",
    }),
  );
  assert.equal(r.status, 202, r.text);

  // Wait for at least five **handler entries**, not five attempts.
  //
  // The two differ since `ctx.run` suspends waiting for the ack: a rerun right
  // inside the same stream also enters the handler, but it reuses the old `StartMessage`, so
  // `ctx.attempt` stays the same. The sequence therefore has repeats — `[0,1,1,…]` — and the
  // *drop* only comes at the next real attempt.
  const deadline = Date.now() + 30_000;
  while (progressAttempts.length < 5 && Date.now() < deadline) {
    await new Promise((res) => setTimeout(res, 100));
  }

  const seq = JSON.stringify(progressAttempts);
  assert.equal(progressAttempts[0], 0, seq);
  // There must be at least one **drop**: that is the recount after the journal grows. It does not
  // assert at which index it drops — the next reschedule is set BEFORE the turn runs (so dying
  // midway still leaves it scheduled), so the progress measurement always sees the length of the
  // *previous* turn, and the drop is therefore one beat late. Asserting the position would assert
  // an implementation detail.
  assert.ok(
    progressAttempts.some((v, i) => i > 0 && v < progressAttempts[i - 1]!),
    `the journal grew ⇒ ctx.attempt must recount somewhere, got ${seq}`,
  );

  await fetch(`${runtimeUrl}/invocations/${r.json.id}/cancel`, { method: "POST" });
});

test("idempotency: two calls with the same idempotency_key run only one invocation", async () => {
  const before = idemRuns;
  const r = await readBody(
    await fetch(`${runtimeUrl}/invoke/OrderService/twice?key=order-10`, {
      method: "POST",
      body: JSON.stringify({ tag: "idem-abc" }),
    }),
  );
  assert.equal(r.status, 200, r.text);
  const done = r.json;
  const out = JSON.parse(done.output) as { a: number; b: number };
  assert.equal(
    idemRuns,
    before + 1,
    "two calls with the same key ran two invocations — docs/protocol-v1.md §8.6 says AT MOST one",
  );
  assert.equal(out.a, out.b, "duplicate calls must get the SAME result");
});

test("idempotency: different idempotency keys are still two invocations", async () => {
  const before = idemRuns;
  await readBody(
    await fetch(`${runtimeUrl}/invoke/OrderService/checkout?key=order-11`, {
      method: "POST",
      body: JSON.stringify({ amount: 1 }),
    }),
  );
  const r = await readBody(
    await fetch(`${runtimeUrl}/invoke/OrderService/twice?key=order-12`, {
      method: "POST",
      body: JSON.stringify({ tag: "idem-other" }),
    }),
  );
  assert.equal(r.status, 200, r.text);
  assert.equal(idemRuns, before + 1, "a new key must run exactly once more");
});

test("idempotency: two different CALLERS with the same idempotency_key are still one invocation", async () => {
  const before = idemRuns;
  const tag = "idem-shared-two-callers";
  const r1 = await readBody(
    await fetch(`${runtimeUrl}/invoke/OrderService/callOnce?key=order-20`, {
      method: "POST",
      body: JSON.stringify({ tag }),
    }),
  );
  const r2 = await readBody(
    await fetch(`${runtimeUrl}/invoke/OrderService/callOnce?key=order-21`, {
      method: "POST",
      body: JSON.stringify({ tag }),
    }),
  );
  assert.equal(r1.status, 200, r1.text);
  assert.equal(r2.status, 200, r2.text);
  const d1 = r1.json;
  const d2 = r2.json;
  assert.equal(
    idemRuns,
    before + 1,
    "two callers with the same key ran two invocations — the idempotency table lives on the " +
      "target partition, so every call with the same key MUST route to the same partition",
  );
  const o1 = JSON.parse(d1.output) as { ran: number };
  const o2 = JSON.parse(d2.output) as { ran: number };
  assert.equal(o1.ran, o2.ran, "two callers with the same key must get the SAME result");

  // And the id handed to each caller (CallInvocationId, 0x800E) must be the REAL
  // id of the invocation holding the key — i.e. both callers see the SAME id.
  // The cross-partition path used to return a "would-be-created" id derived from
  // (caller, completion), so each side saw a different id and both were wrong.
  // An id derived from the idempotency key tuple itself closes this hole.
  const calleeSeenBy = async (caller: string): Promise<string> => {
    const rows = (
      await readBody(await fetch(`${runtimeUrl}/invocations/${caller}/journal`))
    ).json as { ty: string; payload: string }[];
    const row = rows.find((r) => r.ty === "0x800E");
    assert.ok(row, `the journal of ${caller} must have a CallInvocationId (0x800E)`);
    // Read exactly the field `invocation_id = 16` (tag 0x82 0x01, then the length) instead of
    // scanning for a fixed-length hex string in the payload: the id length is the runtime's
    // business and has changed once. Today's id is shorter than 128 bytes, so its length is a
    // single varint byte.
    const payload = Buffer.from(row!.payload, "hex");
    const at = payload.indexOf(Buffer.from([0x82, 0x01]));
    assert.ok(at >= 0, "the 0x800E payload must have the invocation_id field (16)");
    const len = payload[at + 2]!;
    assert.ok(len < 0x80, `id length ${len} does not fit in one varint byte`);
    const id = payload.subarray(at + 3, at + 3 + len).toString("latin1");
    assert.match(id, /^[0-9a-f]+$/, "the id in 0x800E must be hex");
    assert.equal(id.length % 2, 0, `id ${id} has an odd number of characters`);
    return id;
  };
  const id1 = await calleeSeenBy(r1.json.id);
  const id2 = await calleeSeenBy(r2.json.id);
  assert.equal(
    id1,
    id2,
    "two callers with the same key must be handed THE SAME CallInvocationId — " +
      "the id is what users store, a 'would-be-created' id is a ghost id",
  );
});

/**
 * `ctx.invocationId` is the **real id**, usable everywhere HTTP accepts an id.
 *
 * It used to return `debug_id` — and for a scoped invocation `debug_id` is
 * `"{scope}/{hex}"` (a prefix so log readers know the tenant). A handler that stored that value
 * and then looked up `/invocations/{id}` got 400. The id users keep must be the
 * id the runtime accepts, not a label for humans.
 */
test("the ctx.invocationId of a scoped invocation can be looked up with GET /invocations/{id}", async () => {
  const r = await readBody(
    await fetch(`${runtimeUrl}/scope/tenant-e2e/invoke/Phase4Service/whoAmI?key=who-1`, {
      method: "POST",
      body: "{}",
    }),
  );
  const done = await outcome(r);
  assert.equal(done.status, "completed", JSON.stringify(done));
  const out = JSON.parse(done.output) as { invocationId: string; debugId: string };
  const seen = out.invocationId;
  assert.equal(seen, r.json.id, "ctx.invocationId must be exactly the id the runtime returned to the caller");
  // The label for humans is still there, in its own place.
  assert.equal(out.debugId, `tenant-e2e/${seen}`);

  const s = await readBody(await fetch(`${runtimeUrl}/invocations/${seen}`));
  assert.equal(s.status, 200, `GET /invocations/${seen}: ${s.text}`);
  assert.equal(s.json.status, "completed");
});

test("idempotency: one-way ctx.send with the same idempotency_key runs only once", async () => {
  const before = idemRuns;
  const r = await readBody(
    await fetch(`${runtimeUrl}/invoke/OrderService/noteTwice?key=order-22`, {
      method: "POST",
      body: JSON.stringify({ tag: "idem-send-oneway" }),
    }),
  );
  assert.equal(r.status, 200, r.text);
  await settle(r.json.id);
  // The side effect runs on a background beat. Wait for it to show up, then give enough time for
  // an EXTRA run (if any) to appear — asserting "exactly one" needs both halves.
  const deadline = Date.now() + 10_000;
  while (idemRuns === before && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 50));
  }
  await new Promise((r) => setTimeout(r, 1_000));
  assert.equal(
    idemRuns,
    before + 1,
    "two ctx.sends with the same idempotency key must run the side effect exactly once — " +
      "docs/protocol-v1.md §8.6 has no exception for one-way calls",
  );
});

test("an SDK TerminalError becomes a failed invocation, not an infinite retry", async () => {
  const bad = await readBody(
    await fetch(`${runtimeUrl}/invoke/OrderService/submit?key=order-99`, {
      method: "POST",
      body: JSON.stringify({ amount: "not a number" }),
    }),
  );
  // TerminalError → Output with a failure → the invocation *completes* with an error result.
  assert.ok(
    bad.json.status === "completed" || bad.json.status === "failed",
    `TerminalError must end the invocation, got: ${bad.text}`,
  );
});

/**
 * Failover with a **really killed process** — not a simulated
 * freeze: `SIGKILL` gives the node no chance to say anything, just like the OOM
 * killer or a machine losing power.
 *
 * Only runs when `CAIRNFLOW_E2E_NODES > 1` (needs a shared store — MinIO or
 * SeaweedFS — because two processes cannot share `memory://`).
 */
test(
  "kill -9 of a real node: the cluster heals, new work runs, old data intact",
  { skip: nodeCount() < 2 },
  async () => {
    // One invocation completed before the kill — the byte-for-byte reference.
    const r1 = await readBody(
      await fetch(`${runtimeUrl}/invoke/OrderService/checkout?key=order-kill-before`, {
        method: "POST",
        body: JSON.stringify({ amount: 3 }),
      }),
    );
    const d1 = await settle(r1.json.id, 30_000);
    assert.equal(d1.status, "completed", JSON.stringify(d1));
    const j1 = await readBody(await fetch(`${runtimeUrl}/invocations/${r1.json.id}/journal`));

    // The victim is the second node. The first node (where all of the test's requests go)
    // survives — what we measure is the *cluster* healing, not the client switching addresses.
    runtimes[1]!.kill("SIGKILL");

    // The cluster heals: every partition has a leader, and no leader is the dead
    // node. The deadline is generous because the lease TTL has to really expire.
    const deadline = Date.now() + 60_000;
    for (;;) {
      const c = (await fetch(`${runtimeUrl}/cluster`).then((x) => x.json())) as any;
      if (c.partitions.every((p: any) => p.leader && p.address !== urls[1])) break;
      if (Date.now() > deadline) {
        throw new Error(`the cluster did not heal within 60s: ${JSON.stringify(c)}`);
      }
      await new Promise((s) => setTimeout(s, 200));
    }

    // New work — including a cross-partition `ctx.call` — still runs all the way through.
    const r2 = await readBody(
      await fetch(`${runtimeUrl}/invoke/OrderService/checkout?key=order-kill-after`, {
        method: "POST",
        body: JSON.stringify({ amount: 4 }),
      }),
    );
    const d2 = await settle(r2.json.id, 30_000);
    assert.equal(d2.status, "completed", JSON.stringify(d2));

    // And the data from before the kill is intact **byte for byte** — 0 bytes of data
    // movement is the architecture's promise: the state stays in place on the bucket,
    // the new leader only opens it.
    const j2 = await readBody(await fetch(`${runtimeUrl}/invocations/${r1.json.id}/journal`));
    assert.deepEqual(j2.json, j1.json, "the journal changed after failover");
  },
);

// ───────────────────────────────────────────────────────────── Phase 4

/// Finished at once (200) or having to wait (202) both come down to the final status.
/** Waits for an invocation to reach exactly the expected status (not "fully stopped"). */
async function waitForStatus(id: string, want: string, timeoutMs = 20_000): Promise<any> {
  const deadline = Date.now() + timeoutMs;
  let last = "";
  for (;;) {
    const r = await fetch(`${runtimeUrl}/invocations/${id}`);
    if (r.ok) {
      const body = (await r.json()) as any;
      last = body.status;
      if (body.status === want) return body;
    }
    if (Date.now() > deadline) {
      throw new Error(`invocation ${id} did not reach ${want} within ${timeoutMs}ms (currently ${last})`);
    }
    await new Promise((r) => setTimeout(r, 25));
  }
}

async function outcome(r: { status: number; json: any; text: string }): Promise<any> {
  if (r.json?.status && r.json.status !== "running" && r.json.status !== "waiting" && r.json.status !== "suspended") {
    return r.json;
  }
  return settle(r.json.id);
}

test("Phase 4: getKeys, clearAll, and the eager path after clearAll", async () => {
  const r = await readBody(
    await fetch(`${runtimeUrl}/invoke/Phase4Service/stateOps?key=p4-state`, {
      method: "POST",
      body: "{}",
    }),
  );
  const done = await outcome(r);
  assert.equal(done.status, "completed", JSON.stringify(done));
  assert.deepEqual(JSON.parse(done.output), {
    keys: ["a", "b"],
    a: 1,
    after: [],
    gone: null,
  });
});

test("Phase 4: eager state — a later invocation reads state through the StartMessage", async () => {
  const seed = await outcome(
    await readBody(
      await fetch(`${runtimeUrl}/invoke/Phase4Service/seed?key=p4-eager`, {
        method: "POST",
        body: "{}",
      }),
    ),
  );
  assert.equal(seed.status, "completed", JSON.stringify(seed));

  const read = await outcome(
    await readBody(
      await fetch(`${runtimeUrl}/invoke/Phase4Service/readBack?key=p4-eager`, {
        method: "POST",
        body: "{}",
      }),
    ),
  );
  assert.equal(read.status, "completed", JSON.stringify(read));
  assert.deepEqual(JSON.parse(read.output), { a: 41, keys: ["a"] });
});

test("eager getKeys, then a set and a sleep: the next turn replays it without a journal mismatch", async () => {
  const seed = await outcome(
    await readBody(
      await fetch(`${runtimeUrl}/invoke/Phase4Service/seed?key=eager-keys-sleep`, { method: "POST", body: "{}" }),
    ),
  );
  assert.equal(seed.status, "completed", JSON.stringify(seed));
  const done = await outcome(
    await readBody(
      await fetch(`${runtimeUrl}/invoke/Phase4Service/keysAcrossSleep?key=eager-keys-sleep`, {
        method: "POST",
        body: "{}",
      }),
    ),
  );
  assert.equal(done.status, "completed", JSON.stringify(done));
  assert.deepEqual(JSON.parse(done.output), { keys: ["a"], after: ["a", "b"] });
});

test("Phase 4: ctx.race — the call beats the timer, no one-hour wait", async () => {
  const r = await readBody(
    await fetch(`${runtimeUrl}/invoke/Phase4Service/raceCallVsSleep?key=p4-race`, {
      method: "POST",
      body: JSON.stringify({ amount: 9 }),
    }),
  );
  const done = await outcome(r);
  assert.equal(done.status, "completed", JSON.stringify(done));
  assert.deepEqual(JSON.parse(done.output), { winner: "paid-9" });
});

test("Phase 4: ctx.all — waits for both parallel calls", async () => {
  const r = await readBody(
    await fetch(`${runtimeUrl}/invoke/Phase4Service/allCalls?key=p4-all`, {
      method: "POST",
      body: "{}",
    }),
  );
  const done = await outcome(r);
  assert.equal(done.status, "completed", JSON.stringify(done));
  assert.deepEqual(JSON.parse(done.output), { a: "paid-1", b: "paid-2" });
});

test("outside door: resolve a promise over HTTP, no approving handler needed", async () => {
  // The waiter suspends first.
  const w = await readBody(
    // `/send`: waiting for a human to press a button must not hold an HTTP connection —
  // exactly what this door exists to avoid.
  await fetch(`${runtimeUrl}/invoke/Phase4Service/waitApproval/send?key=p4-external`, {
      method: "POST",
      body: "{}",
    }),
  );
  assert.equal(w.status, 202, w.text);
  // `/send` answers BEFORE the first turn runs, so we must wait until the workflow really reaches
  // its wait point before approving — otherwise `woken` is a race. The "approved before
  // anyone waits" case has its own test right below.
  await suspended(w.json.id);

  // Nobody has approved ⇒ it sees "no result yet". This answers
  // "is the workflow stuck because nobody approved, or because the approval never arrived".
  const p0 = await readBody(
    await fetch(`${runtimeUrl}/promises/Phase4Service/p4-external/approval`),
  );
  assert.equal(p0.status, 200, p0.text);
  assert.equal(p0.json.completed, false, p0.text);

  // **No `approve` handler is called.** An HTTP request from an outside system — an approval
  // form, a webhook, a callback — wakes the workflow.
  const r = await readBody(
    await fetch(`${runtimeUrl}/promises/Phase4Service/p4-external/approval/resolve`, {
      method: "POST",
      body: JSON.stringify("approved from outside"),
    }),
  );
  assert.equal(r.status, 200, r.text);
  assert.equal(r.json.completed, true, r.text);
  assert.equal(r.json.woken, 1, `must wake exactly the waiter: ${r.text}`);

  const done = await settle(w.json.id);
  assert.equal(done.status, "completed", JSON.stringify(done));
  assert.deepEqual(JSON.parse(done.output), { got: "approved from outside" });

  // Calling again is a **no-op, not an error** — webhook retries must be safe.
  const again = await readBody(
    await fetch(`${runtimeUrl}/promises/Phase4Service/p4-external/approval/resolve`, {
      method: "POST",
      body: JSON.stringify("late"),
    }),
  );
  assert.equal(again.status, 200, again.text);
  assert.equal(again.json.completed, false, "a second resolve must be a no-op");
});

test("outside door: a resolve BEFORE anyone waits still arrives", async () => {
  // The most fragile case of every wake-up mechanism, and it really happens: the outside system
  // answers faster than the workflow reaches its wait point. The promise is **durable**, so there
  // is no race to lose — whoever comes later reads the recorded result directly.
  const r = await readBody(
    await fetch(`${runtimeUrl}/promises/Phase4Service/p4-early/approval/resolve`, {
      method: "POST",
      body: JSON.stringify("came first"),
    }),
  );
  assert.equal(r.status, 200, r.text);
  assert.equal(r.json.woken, 0, "nobody waiting means nobody to wake — it must still be recorded");

  const w = await outcome(
    await readBody(
      await fetch(`${runtimeUrl}/invoke/Phase4Service/waitApproval?key=p4-early`, {
        method: "POST",
        body: "{}",
      }),
    ),
  );
  assert.equal(w.status, "completed", JSON.stringify(w));
  assert.deepEqual(JSON.parse(w.output), { got: "came first" });
});

test("outside door: rejecting a promise ⇒ the waiter gets a terminal error", async () => {
  const w = await readBody(
    // `/send`: waiting for a human to press a button must not hold an HTTP connection —
  // exactly what this door exists to avoid.
  await fetch(`${runtimeUrl}/invoke/Phase4Service/waitApproval/send?key=p4-rejected`, {
      method: "POST",
      body: "{}",
    }),
  );
  assert.equal(w.status, 202, w.text);

  const r = await readBody(
    await fetch(`${runtimeUrl}/promises/Phase4Service/p4-rejected/approval/reject`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ code: 403, message: "not approved" }),
    }),
  );
  assert.equal(r.status, 200, r.text);

  const done = await settle(w.json.id);
  assert.equal(done.status, "failed", `reject must make the waiter fail: ${JSON.stringify(done)}`);
});

test("Phase 4: promise — one handler waits, another approves", async () => {
  // The waiter suspends first.
  const w = await readBody(
    // `/send`: waiting for a human to press a button must not hold an HTTP connection —
  // exactly what this door exists to avoid.
  await fetch(`${runtimeUrl}/invoke/Phase4Service/waitApproval/send?key=p4-wf`, {
      method: "POST",
      body: "{}",
    }),
  );
  assert.equal(w.status, 202, w.text);

  // Look before approving: nothing yet.
  const peek = await outcome(
    await readBody(
      await fetch(`${runtimeUrl}/invoke/Phase4Service/peekApproval?key=p4-wf`, {
        method: "POST",
        body: "{}",
      }),
    ),
  );
  assert.deepEqual(JSON.parse(peek.output), { v: null });

  // Approving — the waiter must receive the value.
  const a = await outcome(
    await readBody(
      await fetch(`${runtimeUrl}/invoke/Phase4Service/approve?key=p4-wf`, {
        method: "POST",
        body: JSON.stringify({ value: "approved already" }),
      }),
    ),
  );
  assert.equal(a.status, "completed", JSON.stringify(a));

  const done = await settle(w.json.id);
  assert.equal(done.status, "completed", JSON.stringify(done));
  assert.deepEqual(JSON.parse(done.output), { got: "approved already" });

  // Once there is a result: peek sees it, and a second approval fails (409).
  const peek2 = await outcome(
    await readBody(
      await fetch(`${runtimeUrl}/invoke/Phase4Service/peekApproval?key=p4-wf`, {
        method: "POST",
        body: "{}",
      }),
    ),
  );
  assert.deepEqual(JSON.parse(peek2.output), { v: "approved already" });

  const again = await outcome(
    await readBody(
      await fetch(`${runtimeUrl}/invoke/Phase4Service/approve?key=p4-wf`, {
        method: "POST",
        body: JSON.stringify({ value: "late" }),
      }),
    ),
  );
  assert.equal(again.status, "failed", `a second approval must be 409: ${JSON.stringify(again)}`);
});

test("workflow: run waits on a promise, a shared handler knocks on it, attach gets the result", async () => {
  // `/send` because `run` will suspend until someone presses the button.
  const started = await readBody(
    await fetch(`${runtimeUrl}/invoke/SignupWorkflow/run/send?key=u-1`, {
      method: "POST",
      body: JSON.stringify({ email: "a@b.c" }),
    }),
  );
  assert.equal(started.status, 202, started.text);

  // Wait until `run` **really suspends** before knocking.
  //
  // `/send` promises exactly one thing: the invocation is durable. It does **not** promise the
  // invocation has run any step — so "send, then read state right away" is a race, and that race is
  // real: `/send`'s force-durable step releases the engine lock, so a call arriving right after it
  // can overtake even `run`'s first turn. Before commits could be batched that window was so narrow
  // nobody saw it; now it is visible.
  //
  // Waiting here does **not** weaken the test — it makes the test say exactly what it means to
  // say: a `SHARED` handler runs *while* `run` is suspended.
  await waitForStatus(started.json.id, "suspended");

  // A SHARED handler runs **while `run` is suspended** and can read its state. With a virtual
  // object this call would have to queue behind it; with a workflow it does not, and that is
  // exactly what makes this model usable.
  const peeked = await outcome(
    await readBody(
      await fetch(`${runtimeUrl}/invoke/SignupWorkflow/peek?key=u-1`, {
        method: "POST",
        body: "{}",
      }),
    ),
  );
  assert.equal(peeked.status, "completed", JSON.stringify(peeked));
  assert.deepEqual(JSON.parse(peeked.output), { email: "a@b.c" });

  const clicked = await outcome(
    await readBody(
      await fetch(`${runtimeUrl}/invoke/SignupWorkflow/click?key=u-1`, {
        method: "POST",
        body: JSON.stringify({ secret: "s3cret" }),
      }),
    ),
  );
  assert.equal(clicked.status, "completed", JSON.stringify(clicked));

  // `attach` needs no id: it recomputes it from (service, key).
  const attached = await readBody(
    await fetch(`${runtimeUrl}/workflow/SignupWorkflow/u-1/attach`, { method: "POST" }),
  );
  const done = await outcome(attached);
  assert.equal(done.status, "completed", JSON.stringify(done));
  assert.deepEqual(JSON.parse(done.output), { key: "u-1", ok: true });
  assert.equal(done.id, started.json.id, "the deterministic id must equal the id from the run call");
});

test("workflow: a second run for the same key is rejected, not attached to the old run", async () => {
  const first = await readBody(
    await fetch(`${runtimeUrl}/invoke/SignupWorkflow/run/send?key=u-2`, {
      method: "POST",
      body: JSON.stringify({ email: "x@y.z" }),
    }),
  );
  assert.equal(first.status, 202, first.text);

  const second = await readBody(
    await fetch(`${runtimeUrl}/invoke/SignupWorkflow/run/send?key=u-2`, {
      method: "POST",
      body: JSON.stringify({ email: "other@y.z" }),
    }),
  );
  assert.equal(second.status, 409, second.text);
  assert.equal(second.json.status, "already_invoked");
  assert.equal(second.json.id, first.json.id);

  // Nobody has called it ⇒ 404, quite different from "running".
  const missing = await fetch(`${runtimeUrl}/workflow/SignupWorkflow/never-called/output`);
  assert.equal(missing.status, 404);
});

test("workflow: started from ctx.send, and a second ctx.call is rejected", async () => {
  const r = await outcome(
    await readBody(
      await fetch(`${runtimeUrl}/invoke/WorkflowStarter/startTwice`, {
        method: "POST",
        body: JSON.stringify({ key: "u-called-from-service" }),
      }),
    ),
  );
  assert.equal(r.status, "completed", JSON.stringify(r));
  const out = JSON.parse(r.output);
  assert.equal(
    out.second,
    "rejected",
    `the second run call must fail, not attach to the running run: ${r.output}`,
  );
  // 573 = WORKFLOW_ALREADY_INVOKED. The code must reach user code, not
  // as a generic error: the handler needs to tell "someone is already running it" from
  // "the workflow ran and failed".
  assert.equal(out.code, 573, `the error code did not arrive: ${r.output}`);

  // The run created by `ctx.send` must exist at exactly the deterministic id.
  const peeked = await fetch(`${runtimeUrl}/workflow/SignupWorkflow/u-called-from-service/output`);
  assert.equal(peeked.status, 202, "the workflow must be running, not 404");
});

test("Phase 4: attach gets another invocation's result; getOutput does not wait", async () => {
  // The target is done.
  const t = await readBody(
    await fetch(`${runtimeUrl}/invoke/Phase4Service/seed?key=p4-att`, {
      method: "POST",
      body: "{}",
    }),
  );
  const target = await outcome(t);
  assert.equal(target.status, "completed", JSON.stringify(target));

  const att = await outcome(
    await readBody(
      await fetch(`${runtimeUrl}/invoke/Phase4Service/attachTo?key=p4-att-2`, {
        method: "POST",
        body: JSON.stringify({ id: t.json.id }),
      }),
    ),
  );
  assert.equal(att.status, "completed", JSON.stringify(att));
  assert.deepEqual(JSON.parse(att.output), { got: { ok: true } });

  const peekDone = await outcome(
    await readBody(
      await fetch(`${runtimeUrl}/invoke/Phase4Service/peekOutputOf?key=p4-att-3`, {
        method: "POST",
        body: JSON.stringify({ id: t.json.id }),
      }),
    ),
  );
  assert.deepEqual(JSON.parse(peekDone.output), { out: { ok: true } });

  // The target is still suspended (waiting on a promise nobody approves) → getOutput returns null
  // at once.
  const hanging = await readBody(
    await fetch(`${runtimeUrl}/invoke/Phase4Service/waitApproval?key=p4-att-4`, {
      method: "POST",
      body: "{}",
    }),
  );
  assert.equal(hanging.json.status, "waiting", hanging.text);
  const peekHanging = await outcome(
    await readBody(
      await fetch(`${runtimeUrl}/invoke/Phase4Service/peekOutputOf?key=p4-att-5`, {
        method: "POST",
        body: JSON.stringify({ id: hanging.json.id }),
      }),
    ),
  );
  assert.deepEqual(JSON.parse(peekHanging.output), { out: null });
});

/**
 * A raw snapshot of `/metrics`. Parsed later, because it must be taken **before**
 * knowing which partition the invocation lands on.
 */
async function sdkMetrics(): Promise<string> {
  return await (await fetch(`${runtimeUrl}/metrics`)).text();
}

/// The partition of an invocation, **asked of the runtime** through `GET /invocations`.
///
/// Not read from the id: the id is an opaque string, and since id format v2 it carries the partition *key*,
/// not the partition number — the key → partition mapping is the cluster's range table.
/// The list only covers the partitions this node currently
/// leads, so this function is only used where a single node is known.
async function partitionOf(id: string): Promise<number> {
  const rows = (
    await readBody(await fetch(`${runtimeUrl}/invocations?limit=100000`))
  ).json as { id: string; partition: number }[];
  const row = rows.find((r) => r.id === id);
  assert.ok(row, `GET /invocations does not see ${id}`);
  return row!.partition;
}

/**
 * The SDK downlink counter **of one partition**, read from a `/metrics` snapshot.
 *
 * Counters belong to a process, and a process holding several partitions runs several
 * engines in parallel — so the total cannot be subtracted into "the cost of one
 * invocation". The `_by_range_total` series narrows "someone else adding in between"
 * down to invocations **of the same range** (the `partition` of `/invocations` is the range
 * id).
 *
 * No line ⇒ 0, not an error: `render` only prints ranges that have been bumped.
 */
function sdkCounters(
  text: string,
  partition: number,
): { rr: number; streams: number; resumes: number } {
  const num = (name: string): number =>
    Number(
      new RegExp(`^${name}_by_range_total\\{range="${partition}"\\} (\\d+)$`, "m").exec(
        text,
      )?.[1] ?? "0",
    );
  return {
    rr: num("cairnflow_sdk_rr_turns"),
    streams: num("cairnflow_sdk_bidi_streams"),
    resumes: num("cairnflow_sdk_bidi_resumes"),
  };
}

test("a chain of ctx.run + a lazy get: BIDI_STREAM costs exactly ONE connection", async () => {
  // Counters belong to a **process**, so the subtraction only means something when nobody
  // else in that process adds in between the two reads. On a multi-node cluster we
  // do not know which node ran this invocation, so it cannot be measured there — and
  // only there.
  //
  // A multi-partition cluster **can be measured**, thanks to the `_by_range_total` series: one
  // process has several engines, but we only subtract the part of exactly the partition this
  // invocation landed on. Before that series existed this test was red at 8 partitions —
  // and red the same way on an older commit, i.e. a limit of the *measurement*, not a
  // regression.
  //
  // The split-midway shape cannot be measured either, for the same reason: the invocation's
  // range may be split between the two snapshots, and the counter of the old range id
  // stops growing exactly there.
  if (urls.length > 1 || splitMs > 0) return;

  const beforeText = await sdkMetrics();
  const r = await readBody(
    await fetch(`${runtimeUrl}/invoke/RoundTripService/chain?key=rt-1`, {
      method: "POST",
      body: "{}",
    }),
  );
  const done = await outcome(r);
  assert.equal(done.status, "completed", JSON.stringify(done));
  assert.deepEqual(JSON.parse(done.output), { a: 1, b: 2, c: 3, seen: 0 });

  // The partition is only known **after** the id exists, so both snapshots must be raw and
  // the parsing must come after.
  const p = await partitionOf(r.json.id);
  const before = sdkCounters(beforeText, p);
  const after = sdkCounters(await sdkMetrics(), p);
  const opened = after.streams - before.streams;
  const resumes = after.resumes - before.resumes;
  const turns = after.rr - before.rr;

  if (sdkMode === "BIDI_STREAM") {
    // **One** connection for the whole invocation — that is what the question asks, and it does not
    // change.
    //
    // The number of reruns does change, and this is where the price is recorded: four, one for each
    // `ctx.run` waiting for its ack plus one for the lazy `ctx.get`. Before `ctx.run` waited for
    // acks there was only one. The three extra are the price of the promise "the result is durable
    // before the code moves on" — they stay in the same stream, with no HTTP round trip, and none
    // of them resends the journal.
    assert.equal(opened, 1, `opened ${opened} streams for one invocation`);
    assert.equal(resumes, 4, `reran ${resumes} times within the stream`);
    assert.equal(turns, 0, "must not fall back to REQUEST_RESPONSE");
  } else {
    // Same program, same result — and here the price is far higher, because every
    // suspension is a real HTTP request plus a resend of the whole journal.
    // That is why `BIDI_STREAM` is the default.
    assert.equal(opened, 0, "REQUEST_RESPONSE opened a stream");
    assert.equal(turns, 5, `REQUEST_RESPONSE took ${turns} turns`);
  }
});

test("a 4MiB frame travels both ways on one stream without anyone hanging", async () => {
  const bytes = 4 * 1024 * 1024;
  const r = await readBody(
    await fetch(`${runtimeUrl}/invoke/BulkService/big?key=bulk-1`, {
      method: "POST",
      body: JSON.stringify({ bytes }),
    }),
  );
  const done = await outcome(r);
  assert.equal(done.status, "completed", (done.output ?? done.message ?? "").slice(0, 300));
  assert.deepEqual(JSON.parse(done.output), { len: bytes, head: "xxx", seen: 0 });
});

/**
 * API contract: rare outcomes also go through `fetch`'s schema checker,
 * and assert exactly what the documentation says.
 */
test("API contract: paused 575, workflow failed, the list, waiting cannot be filtered", async () => {
  // paused 575: `/send`, then wait for the 2 attempts to run out.
  const sent = await readBody(await fetch(`${runtimeUrl}/invoke/ShapeService/exhaust/send`, { method: "POST", body: "{}" }));
  assert.equal(sent.status, 202, sent.text);
  assert.equal(sent.json.status, "waiting");
  const paused = await settle(sent.json.id, 30_000);
  assert.equal(paused.status, "paused", JSON.stringify(paused));
  assert.equal(paused.code, 575, JSON.stringify(paused));
  assert.equal(typeof paused.message, "string");
  assert.equal(paused.streams, undefined, "streams is a reserved slot — the runtime must not return it yet");
  // 575 states the error of the last attempt; `lastFailure` is structured when read from the
  // leader.
  assert.ok(paused.message.includes("fails every time"), JSON.stringify(paused));
  const direct = await fetch(`${runtimeUrl}/invocations/${sent.json.id}`);
  const detail = (await direct.json()) as any;
  if (direct.headers.get("x-cairn-read") === "leader") {
    assert.equal(detail.lastFailure?.code, 500, JSON.stringify(detail));
    assert.ok(String(detail.lastFailure.message).includes("fails every time"), JSON.stringify(detail));
    assert.equal(detail.lastFailure.attempt, 1, JSON.stringify(detail));
  }

  // A `failed` run turn ⇒ `/output` and `/attach` return **500** with `status: failed`.
  const run = await readBody(await fetch(`${runtimeUrl}/invoke/ShapeWorkflow/run?key=shape-1`, { method: "POST", body: "{}" }));
  const ran = await outcome(run);
  assert.equal(ran.status, "failed", JSON.stringify(ran));
  for (const [method, door] of [["GET", "output"], ["POST", "attach"]] as const) {
    const r = await readBody(await fetch(`${runtimeUrl}/workflow/ShapeWorkflow/shape-1/${door}`, { method }));
    assert.equal(r.status, 500, `${door}: ${r.text}`);
    assert.deepEqual(
      { status: r.json.status, id: r.json.id, code: r.json.code },
      { status: "failed", id: run.json.id, code: 422 },
      `${door}: ${r.text}`,
    );
  }

  // The rows of GET /invocations (a local array; an object with scope=cluster). On a multi-node
  // cluster the row may live on another node — the cluster-wide version always sees it, unless it
  // reports itself `partial` (the `+split` row: a child range has no engine yet in the middle of a
  // split) — then ask again, bounded.
  let all = await readBody(await fetch(`${runtimeUrl}/invocations?scope=cluster&status=paused&limit=1000`));
  for (const end = Date.now() + 15_000; all.status === 200 && all.json.partial && Date.now() < end; ) {
    await new Promise((r) => setTimeout(r, 200));
    all = await readBody(await fetch(`${runtimeUrl}/invocations?scope=cluster&status=paused&limit=1000`));
  }
  assert.equal(all.status, 200, all.text);
  const row = all.json.invocations.find((x: any) => x.id === sent.json.id);
  assert.ok(row, `missing ${sent.json.id} within ${all.text.slice(0, 500)}`);
  assert.equal(row.service, "ShapeService");
  assert.equal(row.handler, "exhaust");
  assert.equal(row.status, "paused");
  const local = await readBody(await fetch(`${runtimeUrl}/invocations?limit=5`));
  assert.equal(local.status, 200, local.text);
  assert.ok(Array.isArray(local.json), local.text);

  // `waiting` is only the answer of the calling door, not a durable status.
  const bad = await readBody(await fetch(`${runtimeUrl}/invocations?status=waiting`));
  assert.equal(bad.status, 400, bad.text);
  assert.equal(typeof bad.json.error, "string");
});

/**
 * **The split-midway shape really ran** — runs last, after every other
 * test. Without it a mute saboteur (every split refused, or the
 * endpoint returning 404) gives a suite that is green for the wrong reason.
 *
 * Three things: at least one split and one merge completed; no response outside
 * the contract (`200`, or `409`/`503` to retry); and after stopping, the table returns to its
 * resting state within a limit — covering `u64` completely, no intents, every range with a
 * leader.
 */
test(
  "split midway: ranges were really split and merged, the table returns to its resting state",
  { skip: splitMs === 0 ? "only in the split-midway shape (CAIRNFLOW_E2E_SPLIT_MS)" : false },
  async (t) => {
    const stats = await churner!.stop();
    t.diagnostic(`split/merge: ${JSON.stringify(stats)}`);
    assert.deepEqual(stats.errors, [], `responses outside the contract: ${JSON.stringify(stats)}`);
    assert.ok(
      stats.splits >= 1 && stats.merges >= 1,
      `the saboteur did nothing — the suite is green for the wrong reason: ${JSON.stringify(stats)}`,
    );
    const deadline = Date.now() + 30_000;
    let problem: string | null = "not read yet";
    while (Date.now() < deadline) {
      problem = tableProblem(await rangesOf((path, init) => fetch(`${runtimeUrl}${path}`, init)));
      if (problem === null) break;
      await new Promise((r) => setTimeout(r, 200));
    }
    assert.equal(problem, null, `the range table did not return to its resting state after ${JSON.stringify(stats)}`);
  },
);

// ── Signals with a timeout — run under both BIDI_STREAM and
//    REQUEST_RESPONSE (`CAIRNFLOW_E2E_SDK_MODE`).

async function sendRace(handler: string, body: unknown): Promise<string> {
  const r = await readBody(
    await fetch(`${runtimeUrl}/invoke/RaceSignalService/${handler}/send`, {
      method: "POST",
      body: JSON.stringify(body),
    }),
  );
  assert.equal(r.status, 202, r.text);
  return r.json.id as string;
}

async function untilSuspended(id: string, timeoutMs = 20_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const r = await readBody(await fetch(`${runtimeUrl}/invocations/${id}`));
    if (r.status === 200 && r.json.status === "suspended") return;
    if (Date.now() > deadline) throw new Error(`${id} not suspended yet: ${r.text}`);
    await new Promise((res) => setTimeout(res, 25));
  }
}

test("race(signal, sleep 1 hour) — the signal arrives and wins", async () => {
  const id = await sendRace("signalOrSleep", { ms: 3_600_000 });
  await untilSuspended(id);
  const r = await readBody(
    await fetch(`${runtimeUrl}/invocations/${id}/signal`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ signal: "approval", value: { by: "boss" } }),
    }),
  );
  assert.equal(r.status, 200, r.text);
  const done = await settle(id);
  assert.equal(done.status, "completed", JSON.stringify(done));
  assert.deepEqual(JSON.parse(done.output), { winner: "signal", value: { by: "boss" } });
});

test("race(signal, sleep 300 ms) — on expiry the sleep wins, a later signal does not change the result", async () => {
  const id = await sendRace("signalOrSleep", { ms: 300 });
  const done = await settle(id);
  assert.equal(done.status, "completed", JSON.stringify(done));
  assert.deepEqual(JSON.parse(done.output), { winner: "sleep", value: null });
  const late = await readBody(
    await fetch(`${runtimeUrl}/invocations/${id}/signal`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ signal: "approval", value: { by: "late" } }),
    }),
  );
  assert.equal(late.status, 200, late.text);
  assert.equal(late.json.status, "completed", late.text);
  assert.deepEqual(JSON.parse((await settle(id)).output), { winner: "sleep", value: null });
});

test("race(awakeable, sleep 1 hour) — resolved over HTTP, the awakeable wins", async () => {
  const tag = `awk-${Date.now()}`;
  const id = await sendRace("awakeableOrSleep", { ms: 3_600_000, tag });
  await untilSuspended(id);
  const awk = awakeableIds.get(tag);
  assert.ok(awk, "the handler has not published the awakeable id yet");
  const r = await readBody(
    await fetch(`${runtimeUrl}/awakeables/${awk}/resolve`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify("agreed"),
    }),
  );
  assert.equal(r.status, 200, r.text);
  const done = await settle(id);
  assert.equal(done.status, "completed", JSON.stringify(done));
  assert.deepEqual(JSON.parse(done.output), { winner: "awakeable", value: "agreed" });
});

// ── Timestamps — on a cluster, including the
//    `cluster-4p+split` row of check.sh (split midway).

test("GET /invocations?status=failed&created_after= sees exactly the failed invocation created after the timestamp", async () => {
  const before = await readBody(
    await fetch(`${runtimeUrl}/invoke/OrderService/submit?key=mark-c-0`, {
      method: "POST",
      body: JSON.stringify({ amount: "wrong" }),
    }),
  );
  const first = await settle(before.json.id);
  assert.equal(first.status, "failed", JSON.stringify(first));
  assert.ok(Number.isInteger(first.createdAt), JSON.stringify(first));
  assert.equal(first.endedAt, first.modifiedAt, JSON.stringify(first));
  // A ms timestamp; wait past one ms so the next one is surely created after it.
  await new Promise((r) => setTimeout(r, 5));
  const after = await readBody(
    await fetch(`${runtimeUrl}/invoke/OrderService/submit?key=mark-c-1`, {
      method: "POST",
      body: JSON.stringify({ amount: "wrong" }),
    }),
  );
  const second = await settle(after.json.id);
  assert.equal(second.status, "failed", JSON.stringify(second));
  assert.ok(second.createdAt > first.createdAt, `${JSON.stringify(first)} / ${JSON.stringify(second)}`);
  const deadline = Date.now() + 20_000;
  for (;;) {
    const r = await readBody(
      await fetch(`${runtimeUrl}/invocations?scope=cluster&status=failed&created_after=${second.createdAt}`),
    );
    assert.equal(r.status, 200, r.text);
    const ids = (r.json.invocations as Array<{ id: string; createdAt: number }>).map((x) => x.id);
    if (!r.json.partial || ids.includes(after.json.id)) {
      assert.ok(ids.includes(after.json.id), `missing ${after.json.id}: ${r.text}`);
      assert.ok(!ids.includes(before.json.id), `${before.json.id} was created before the timestamp: ${r.text}`);
      for (const row of r.json.invocations) assert.ok(row.createdAt >= second.createdAt, r.text);
      break;
    }
    // `partial` (a range is splitting, a node has not answered): ask again.
    if (Date.now() > deadline) assert.fail(`still partial after 20 s: ${r.text}`);
    await new Promise((res) => setTimeout(res, 200));
  }
});

test("parallel ctx.run (Promise.all): a slow closure finishing after the fast branch has suspended — each closure runs exactly once, the invocation completes", async () => {
  // Repro before the fix: BIDI ⇒ `failed 571` ("completion id 1 was already assigned to
  // another command"), RR ⇒ `completed` but the slow closure ran twice.
  const id = await sendRace("parallelRuns", { tag: "par-1" });
  const done = await settle(id);
  assert.equal(done.status, "completed", JSON.stringify(done));
  assert.equal(JSON.parse(done.output), 3);
  assert.equal(parallelRuns.get("par-1/slow"), 1, "the slow closure ran again");
  assert.equal(parallelRuns.get("par-1/fast"), 1, "the fast closure ran again");
});
