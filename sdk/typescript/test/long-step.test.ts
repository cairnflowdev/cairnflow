// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * A long step under `REQUEST_RESPONSE` runs **exactly once**.
 *
 * Under `REQUEST_RESPONSE` the SDK inactivity limit wraps the whole turn: a `ctx.run` calling a
 * model for longer than the node's default limit is dropped, the turn is rescheduled, and the next
 * attempt runs that same step again — forever when no `retryPolicy` is declared (each attempt is
 * another token bill). Deployments that register `https://` endpoints run them as
 * `REQUEST_RESPONSE`. The SDK declares a 15-minute `inactivityTimeout` at the service level when
 * the user declares none, so the node's limit (1.5 s here instead of 60 s) does not cut a 2.5 s
 * step. A limit the service declares itself wins — a step longer than it is still cut.
 *
 * Needs the binary: `cargo build --bin cairnflow`.
 */

import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, test } from "node:test";
import { fileURLToPath } from "node:url";
import { type Endpoint, serve, service } from "../src/index.ts";

const here = fileURLToPath(new URL(".", import.meta.url));
const binary = join(here, "..", "..", "..", "target", "debug", "cairnflow");

/** The node's default SDK inactivity limit in the test (production: 60 s). */
const NODE_TIMEOUT_MS = 1_500;
/** A "model call" step longer than the node's limit. */
const STEP_MS = 2_500;

const calls = { long: 0, capped: 0 };
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

const long = service("LongStep", {
  async run(ctx) {
    return ctx.run("model", async () => {
      calls.long++;
      await sleep(STEP_MS);
      return "done";
    });
  },
});

/** Declares its own limit, shorter than the step: the user's limit wins over the SDK default. */
const capped = service(
  "CappedStep",
  {
    async run(ctx) {
      return ctx.run("model", async () => {
        calls.capped++;
        await sleep(STEP_MS);
        return "done";
      });
    },
  },
  { options: { inactivityTimeout: 1_000, retryPolicy: { maxAttempts: 2, initialInterval: 50 } } },
);

let runtime: ChildProcess | undefined;
let endpoint: Endpoint | undefined;
let dataDir = "";
let url = "";

before(async () => {
  if (!existsSync(binary)) throw new Error(`missing binary ${binary} — run \`cargo build --bin cairnflow\` first`);
  dataDir = mkdtempSync(join(tmpdir(), "cairn-long-step-"));
  runtime = spawn(
    binary,
    [
      "--data-dir",
      dataDir,
      "--port",
      "0",
      "--tick-interval-ms",
      "50",
      "--no-auth",
      "--inactivity-timeout-ms",
      String(NODE_TIMEOUT_MS),
    ],
    { stdio: "pipe" },
  );
  runtime.stderr!.resume();
  url = await new Promise<string>((resolve, reject) => {
    let buf = "";
    const timer = setTimeout(() => reject(new Error(`runtime did not start:\n${buf}`)), 20_000);
    runtime!.stdout!.on("data", (c: Buffer) => {
      buf += c.toString();
      const m = /http:\/\/[\d.]+:\d+/.exec(buf);
      if (m) {
        clearTimeout(timer);
        resolve(m[0]);
      }
    });
  });
  for (const deadline = Date.now() + 20_000; ; ) {
    try {
      if ((await fetch(`${url}/health`)).ok) break;
    } catch {
      /* not up yet */
    }
    if (Date.now() > deadline) throw new Error(`${url}/health did not come up`);
    await sleep(20);
  }
  endpoint = await serve({ services: [long, capped], port: 0, protocolMode: "REQUEST_RESPONSE" });
  const r = await fetch(`${url}/deployments`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ uri: endpoint.url }),
  });
  assert.equal(r.status, 200, await r.text());
});

after(async () => {
  await endpoint?.close();
  runtime?.kill("SIGKILL");
  if (dataDir) rmSync(dataDir, { recursive: true, force: true });
});

/** Sends one `/send` and waits for the invocation to reach an outcome (`completed`/`failed`/`paused`). */
async function finish(svc: string): Promise<{ status: string; output?: string }> {
  const r = await fetch(`${url}/invoke/${svc}/run/send`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify("x"),
  });
  const text = await r.text();
  assert.ok(r.ok, `${r.status} ${text}`);
  const { id } = JSON.parse(text) as { id: string };
  for (const deadline = Date.now() + 30_000; Date.now() < deadline; await sleep(100)) {
    const s = (await (await fetch(`${url}/invocations/${id}`)).json()) as { status: string; output?: string };
    if (s.status === "completed" || s.status === "failed" || s.status === "paused") return s;
  }
  throw new Error(`${svc} did not finish`);
}

test("REQUEST_RESPONSE: a ctx.run longer than the node's default limit runs exactly once", async () => {
  const s = await finish("LongStep");
  assert.equal(s.status, "completed", JSON.stringify(s));
  assert.equal(calls.long, 1, "the long step was cut and run again");
});

test("REQUEST_RESPONSE: a limit the user declares still cuts a step longer than it", async () => {
  const s = await finish("CappedStep");
  assert.notEqual(s.status, "completed", JSON.stringify(s));
  assert.ok(calls.capped >= 2, `1 s limit but the 2.5 s step ran ${calls.capped} times`);
});
