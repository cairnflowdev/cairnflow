// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Same program, two transports, **the same journal byte for byte**.
 *
 * This is the central promise of the `BIDI_STREAM` design, and the only promise
 * that cannot be checked by running the same test suite twice: a suite green in
 * both modes only says "the results are the same". What must hold is stronger —
 * *identical entries in identical order* — because that is what lets an invocation
 * start in one mode, its node die, and continue on a node that only speaks the other.
 *
 * Two runtimes, two data directories, two SDKs. Same service name, same handler,
 * same input, same object key — so the two journals must match exactly, entry 0
 * included (`Input`, which carries the routing target).
 *
 * Needs the `cairnflow` binary built: `cargo build --bin cairnflow`.
 */

import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
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

const here = fileURLToPath(new URL(".", import.meta.url));
const binary = join(here, "..", "..", "..", "target", "debug", "cairnflow");

/** How many times the side effect really ran — a durable `ctx.run` must not run again. */
let runs = 0;

const shapes = service(
  "ShapeService",
  {
    /**
     * The program for **comparing journals**: every kind of entry a turn can produce,
     * and not a single nondeterministic source.
     *
     * No `ctx.sleep` (the wake-up time is `Date.now()`, different every run)
     * and no `ctx.call` (the callee id derives from the caller id). What is left is
     * enough to cover where the two transports differ most: `ctx.run`
     * (`ProposeRunCompletion` → `RunCompletion`) and lazy state reads — suspension
     * points the runtime resolves on the spot, i.e. where `BIDI_STREAM` keeps the
     * stream open while `REQUEST_RESPONSE` must open a second request.
     */
    async walk(ctx: Context, input: { tag: string }) {
      const a = await ctx.run("a", () => `${input.tag}-a`);
      const missing = await ctx.get<number>("not-set");
      ctx.set("k", 1);
      const keys = await ctx.getKeys();
      const b = await ctx.run("b", () => `${input.tag}-b`);
      return { a, b, missing, keys };
    },

    /**
     * The program for **switching transport midway**: suspends on something only the
     * outside world can answer (a timer), with a `ctx.run` on both sides of the
     * suspension point. The first part writes the journal in one mode, the second
 * replays in the other.
     */
    async slowly(ctx: Context) {
      const a = await ctx.run("a", () => `a-${++runs}`);
      await ctx.sleep(3_600_000, "cho");
      const b = await ctx.run("b", () => `b-${++runs}`);
      return { a, b };
    },
  },
  { type: "VIRTUAL_OBJECT" },
);

interface Side {
  sdk: Endpoint;
  runtime: ChildProcess;
  url: string;
  dataDir: string;
}

const sides: Partial<Record<ProtocolMode, Side>> = {};

async function bring(mode: ProtocolMode): Promise<Side> {
  const sdk = await serve({ services: [shapes], port: 0, protocolMode: mode });
  const dataDir = mkdtempSync(join(tmpdir(), `cairnflow-parity-${mode}-`));
  // `--no-auth`: this test compares journals, it does not exercise API keys.
  const runtime = spawn(binary, ["--data-dir", dataDir, "--port", "0", "--no-auth"], { stdio: "pipe" });
  const url = await new Promise<string>((resolve, reject) => {
    let buf = "";
    const timer = setTimeout(() => reject(new Error("runtime did not report a port")), 15_000);
    runtime.stdout!.on("data", (c: Buffer) => {
      buf += c.toString();
      const m = /http:\/\/[\d.]+:(\d+)/.exec(buf);
      if (m) {
        clearTimeout(timer);
        resolve(m[0]!);
      }
    });
    runtime.on("exit", (code) => reject(new Error(`runtime exited early: ${code}\n${buf}`)));
  });

  const deadline = Date.now() + 15_000;
  for (;;) {
    try {
      if ((await fetch(`${url}/health`)).ok) break;
    } catch {
      /* not up yet */
    }
    if (Date.now() > deadline) throw new Error(`${url}/health did not come up`);
    await new Promise((r) => setTimeout(r, 50));
  }

  const reg = await fetch(`${url}/deployments`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ uri: sdk.url }),
  });
  const body = (await reg.json()) as { protocolMode?: string };
  assert.equal(reg.status, 200, JSON.stringify(body));
  assert.equal(body.protocolMode, mode, "the runtime settled on the wrong transport");

  return { sdk, runtime, url, dataDir };
}

before(async () => {
  if (!existsSync(binary)) {
    throw new Error(`missing binary ${binary} — run \`cargo build --bin cairnflow\` first`);
  }
  sides.BIDI_STREAM = await bring("BIDI_STREAM");
  sides.REQUEST_RESPONSE = await bring("REQUEST_RESPONSE");
});

after(async () => {
  for (const side of Object.values(sides)) {
    if (side === undefined) continue;
    side.runtime.kill("SIGKILL");
    await side.sdk.close();
    rmSync(side.dataDir, { recursive: true, force: true });
  }
});

interface JournalRow {
  index: number;
  ty: string;
  name?: string;
  payload: string;
}

/**
 * Entry 0 carries the `cairnflow-deployment` header — the deployment id of **each
 * runtime**, so it is right for the two sides to differ. Drop exactly that
 * header before comparing:
 * the submessage `Header { key = "cairnflow-deployment", value = "dp_" + 28 hex }`.
 */
function withoutDeployment(rows: JournalRow[]): JournalRow[] {
  const header = /0a370a14636169726e666c6f772d6465706c6f796d656e74121f[0-9a-f]{62}/;
  return rows.map((r) => (r.index === 0 ? { ...r, payload: r.payload.replace(header, "") } : r));
}

async function walk(side: Side): Promise<{ output: unknown; journal: JournalRow[] }> {
  const r = await fetch(`${side.url}/invoke/ShapeService/walk?key=parity`, {
    method: "POST",
    body: JSON.stringify({ tag: "t" }),
  });
  const body = (await r.json()) as { id: string; status: string; output?: string };
  assert.equal(r.status, 200, JSON.stringify(body));
  assert.equal(body.status, "completed", JSON.stringify(body));
  const journal = (await (await fetch(`${side.url}/invocations/${body.id}/journal`)).json()) as
    JournalRow[];
  return { output: JSON.parse(body.output ?? "null"), journal: withoutDeployment(journal) };
}

test("BIDI_STREAM and REQUEST_RESPONSE write byte-identical journals", async () => {
  const bidi = await walk(sides.BIDI_STREAM!);
  const rr = await walk(sides.REQUEST_RESPONSE!);

  assert.deepEqual(bidi.output, rr.output, "the two transports gave two different results");
  assert.deepEqual(bidi.output, {
    a: "t-a",
    b: "t-b",
    missing: null,
    keys: ["k"],
  });

  // Compare the whole table at once: wherever it is wrong, the diff points straight at
  // that entry, instead of stopping at the first mismatching entry and hiding the rest.
  assert.deepEqual(bidi.journal, rr.journal, "the journals of the two transports differ");

  // And the right shape is the shape the program produces — if both were wrong
  // the same way the comparison above would still be green.
  assert.deepEqual(
    bidi.journal.map((e) => e.name ?? e.ty),
    [
      "Input",
      "Run",
      "RunCompletion",
      "GetState",
      "GetStateCompletion",
      "SetState",
      "GetStateKeys",
      "GetStateKeysCompletion",
      "Run",
      "RunCompletion",
      "Output",
    ],
  );
});

/**
 * Switching transport **in the middle of an invocation**.
 *
 * This is what the journal comparison above exists to allow, and the reason it is
 * worth doing: a node dies while an invocation is suspended, and the node that picks
 * it up may see a deployment declaring another mode. A journal written in the old mode
 * must replay in the new one, and a durable `ctx.run` **must not run again**.
 *
 * Rebuild the SDK on **the same old port** with a different manifest and register
 * again — a **new** deployment. An in-flight invocation is bound to the old deployment,
 * so moving it to the new one is a deliberate operation: `migrate`, which requires a
 * trial replay on the new deployment to match first.
 *
 * Runs **last** in the file: it replaces `sides.BIDI_STREAM.sdk`.
 */
test("an invocation started under BIDI_STREAM continues under REQUEST_RESPONSE", async () => {
  const side = sides.BIDI_STREAM!;

  // ── First half, under BIDI_STREAM: run `ctx.run` "a" and suspend on the timer.
  const before = runs;
  const r = await fetch(`${side.url}/invoke/ShapeService/slowly?key=failover`, {
    method: "POST",
    body: "{}",
  });
  const started = (await r.json()) as { id: string; status: string };
  assert.equal(started.status, "waiting", JSON.stringify(started));
  assert.equal(runs, before + 1, "the `ctx.run` before the suspension point must have run");

  // ── Switch transport: same port, different manifest, registered over the old one.
  const port = side.sdk.port;
  await side.sdk.close();
  side.sdk = await serve({ services: [shapes], port, protocolMode: "REQUEST_RESPONSE" });
  const reg = await fetch(`${side.url}/deployments`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ uri: side.sdk.url }),
  });
  const regBody = (await reg.json()) as { protocolMode?: string; id: string };
  assert.equal(reg.status, 200, JSON.stringify(regBody));
  assert.equal(regBody.protocolMode, "REQUEST_RESPONSE");
  const moved = await fetch(
    `${side.url}/invocations/${started.id}/migrate?deployment=${regBody.id}`,
    { method: "POST" },
  );
  assert.equal(moved.status, 200, await moved.text());

  // ── Second half, under REQUEST_RESPONSE.
  await fetch(`${side.url}/admin/tick?now_ms=${Date.now() + 7_200_000}`, { method: "POST" });
  const deadline = Date.now() + 20_000;
  let done: { status: string; output?: string } = { status: "?" };
  for (;;) {
    done = (await (await fetch(`${side.url}/invocations/${started.id}`)).json()) as typeof done;
    if (done.status !== "running" && done.status !== "waiting" && done.status !== "suspended") break;
    if (Date.now() > deadline) throw new Error(`did not stop: ${JSON.stringify(done)}`);
    await new Promise((s) => setTimeout(s, 100));
  }
  assert.equal(done.status, "completed", JSON.stringify(done));

  // `a` keeps the value it had durably in the old mode; only `b` is new. If the journal
  // did not replay across the transport boundary, `a` would carry the number of the second
  // run — i.e. the side effect ran again.
  assert.deepEqual(JSON.parse(done.output ?? "null"), {
    a: `a-${before + 1}`,
    b: `b-${before + 2}`,
  });
});
