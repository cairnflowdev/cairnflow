// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Server-side schedules in the SDK: `ctx.schedule` reads the
 * `cairnflow-schedule` header of entry 0, and the client's `c.schedules` on a
 * fake `fetch`. The real path against the binary: `client.e2e.test.ts`.
 */

import assert from "node:assert/strict";
import { test } from "node:test";
import { CairnHttpError, connect } from "../src/client.ts";
import { type Handler, runTurn } from "../src/index.ts";
import { Writer } from "../src/protobuf.ts";
import { decodeFrames, decodeInput, encodeFrame, MT } from "../src/wire.ts";

const enc = new TextEncoder();
const dec = new TextDecoder();
const ID = "0123456789abcdef0123456789abcdef0123456789abcdef";

function start(): Uint8Array {
  return encodeFrame(
    MT.START,
    0,
    new Writer().bytes(1, new Uint8Array(24).fill(7)).uint32(3, 1).uint64(9, 42n).finish(),
  );
}

function inputEntry(headers: Record<string, string>): Uint8Array {
  const w = new Writer();
  for (const [k, v] of Object.entries(headers)) w.message(1, new Writer().string(1, k).string(2, v));
  w.message(14, new Writer().bytes(1, enc.encode("null")));
  return encodeFrame(MT.INPUT, 0, w.finish());
}

/** JSON output of a turn that finishes immediately. */
async function output(h: Handler, headers: Record<string, string>): Promise<unknown> {
  const a = start();
  const b = inputEntry(headers);
  const body = new Uint8Array(a.length + b.length);
  body.set(a);
  body.set(b, a.length);
  const frames = decodeFrames(await runTurn(h, body, 4));
  const out = frames.find((f) => f.type === MT.OUTPUT);
  assert.ok(out, "no Output");
  // `Output` and `Input` share the value layout: field 14 wraps `Value.content`.
  return JSON.parse(dec.decode(decodeInput(out.payload)));
}

test("ctx.schedule: cairnflow-schedule header ⇒ { name, at }; absent ⇒ undefined", async () => {
  const h: Handler = async (ctx) => ({ s: ctx.schedule ?? null });
  assert.deepEqual(await output(h, { "cairnflow-schedule": "nightly@1793041200000" }), {
    s: { name: "nightly", at: 1793041200000 },
  });
  assert.deepEqual(await output(h, {}), { s: null });
  // A broken header (no `@`, a timestamp that is not a number) ⇒ undefined, no throw.
  assert.deepEqual(await output(h, { "cairnflow-schedule": "nightly" }), { s: null });
  assert.deepEqual(await output(h, { "cairnflow-schedule": "nightly@x" }), { s: null });
});

interface Call {
  url: string;
  method: string;
  headers: Record<string, string>;
  body: string | undefined;
}

function fake(replies: Response[]): { fetch: typeof fetch; calls: Call[] } {
  const calls: Call[] = [];
  return {
    calls,
    fetch: (async (input: string | URL | Request, init?: RequestInit) => {
      calls.push({
        url: String(input),
        method: init?.method ?? "GET",
        headers: (init?.headers ?? {}) as Record<string, string>,
        body: init?.body === undefined ? undefined : String(init.body),
      });
      const r = replies.shift();
      if (r === undefined) throw new Error("out of fake replies");
      return r;
    }) as typeof fetch,
  };
}

const reply = (status: number, body: unknown) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });

const VIEW = {
  name: "nightly",
  id: ID,
  target: { service: "Report", handler: "build" },
  everyMs: 60000,
  overlap: "skip",
  paused: false,
  jitterMs: 0,
  nextAt: 120000,
  fireAt: 120000,
  last: [],
  fired: 0,
  missed: 0,
  skipped: 0,
  createdAt: 1,
  updatedAt: 1,
};

test("c.schedules: put/get/pause/resume/delete/list — path, method, body", async () => {
  const f = fake([
    reply(200, { ...VIEW, created: true, changed: true }),
    reply(200, { ...VIEW, upcoming: [120000] }),
    reply(404, { error: "no such schedule" }),
    reply(200, { ...VIEW, paused: true, nextAt: null }),
    reply(200, VIEW),
    reply(200, { name: "nightly", deleted: true }),
    reply(404, { error: "no such schedule" }),
    reply(200, { schedules: [VIEW], partial: false, missing: [], missingRanges: [] }),
  ]);
  const c = connect({ url: "http://a", apiKey: "cf_k", fetch: f.fetch });
  const put = await c.schedules.put("nightly", {
    everyMs: 60_000,
    target: { service: "Report", handler: "build" },
    input: { x: 1 },
  });
  assert.equal(put.created, true);
  assert.equal(put.nextAt, 120000);
  assert.equal(f.calls[0]!.method, "PUT");
  assert.equal(f.calls[0]!.url, "http://a/schedules/nightly");
  assert.equal(f.calls[0]!.headers["authorization"], "Bearer cf_k");
  assert.deepEqual(JSON.parse(f.calls[0]!.body!), {
    everyMs: 60000,
    target: { service: "Report", handler: "build" },
    input: { x: 1 },
  });
  assert.deepEqual((await c.schedules.get("nightly"))?.upcoming, [120000]);
  assert.equal(await c.schedules.get("nope"), undefined, "404 ⇒ undefined");
  assert.equal((await c.schedules.pause("nightly")).nextAt, null);
  assert.equal((await c.schedules.resume("nightly")).paused, false);
  assert.equal(await c.schedules.delete("nightly"), true);
  assert.equal(await c.schedules.delete("nightly"), false, "404 ⇒ false");
  const l = await c.schedules.list({ scope: "acme" });
  assert.equal(l.schedules.length, 1);
  assert.deepEqual(
    f.calls.map((x) => `${x.method} ${x.url.replace("http://a", "")}`),
    [
      "PUT /schedules/nightly",
      "GET /schedules/nightly",
      "GET /schedules/nope",
      "POST /schedules/nightly/pause",
      "POST /schedules/nightly/resume",
      "DELETE /schedules/nightly",
      "DELETE /schedules/nightly",
      "GET /scope/acme/schedules",
    ],
  );
});

test("c.schedules.trigger: auto-generated idempotency key, kept across retries; returns the run's handle", async () => {
  const f = fake([
    reply(503, { error: "electing" }),
    reply(200, { id: ID, created: true }),
    reply(409, { error: "gate off", code: "schedules_disabled" }),
  ]);
  const c = connect({ url: "http://a", fetch: f.fetch, maxAttempts: 3 });
  const h = await c.schedules.trigger("nightly", { scope: "acme" });
  assert.equal(h.id, ID);
  assert.equal(f.calls[0]!.url, "http://a/scope/acme/schedules/nightly/trigger");
  const k = f.calls[0]!.headers["idempotency-key"];
  assert.ok(k !== undefined && k.length > 0);
  assert.equal(f.calls[1]!.headers["idempotency-key"], k, "the retry uses the same key");
  await assert.rejects(c.schedules.trigger("nightly", { idempotencyKey: "r1" }), (e: unknown) => {
    assert.ok(e instanceof CairnHttpError);
    assert.equal(e.status, 409);
    assert.equal(e.code, "schedules_disabled");
    return true;
  });
  assert.equal(f.calls[2]!.headers["idempotency-key"], "r1");
});
