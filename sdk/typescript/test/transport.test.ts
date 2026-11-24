// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * The shared transport layer on a fake `fetch`: the `leader` rule, backoff on
 * a `503` without a leader, `Retry-After` of a `429` that created nothing,
 * network error at the leader ⇒ back to the origin node, attempt limit.
 */

import assert from "node:assert/strict";
import { test } from "node:test";
import { Transport } from "../src/transport.ts";

type Reply = Response | Error;

/** Fake `fetch`: returns `replies` in order and records the URLs called. */
function fake(replies: Reply[]): { fetch: typeof fetch; calls: string[] } {
  const calls: string[] = [];
  return {
    calls,
    fetch: (async (input: string | URL | Request) => {
      calls.push(String(input));
      const r = replies.shift();
      if (r === undefined) throw new Error("out of fake replies");
      if (r instanceof Error) throw r;
      return r;
    }) as typeof fetch,
  };
}

const json = (status: number, body: unknown, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json", ...headers } });

test("503 with a leader: one hop to the leader, the leader's reply goes to the caller", async () => {
  const f = fake([json(503, { error: "x", leader: "http://b:2" }), json(200, { ok: 1 })]);
  const t = new Transport({ url: "a:1", fetch: f.fetch });
  const { res, node } = await t.request("/invoke/S/h", { method: "POST" });
  assert.equal(res.status, 200);
  assert.equal(node, "http://b:2");
  assert.deepEqual(f.calls, ["http://a:1/invoke/S/h", "http://b:2/invoke/S/h"]);
});

test("a leader with another scheme is not followed — the key never goes from https to http", async () => {
  const f = fake([json(503, { error: "x", leader: "http://b:2" }), json(200, {})]);
  const t = new Transport({ url: "https://a", fetch: f.fetch, maxAttempts: 2 });
  const { res } = await t.request("/x");
  assert.equal(res.status, 200);
  assert.deepEqual(f.calls, ["https://a/x", "https://a/x"]);
});

test("followLeader: false ⇒ not followed, back off then ask the origin node again", async () => {
  const f = fake([json(503, { error: "x", leader: "http://b:2" }), json(200, {})]);
  const t = new Transport({ url: "http://a", fetch: f.fetch, followLeader: false });
  await t.request("/x");
  assert.deepEqual(f.calls, ["http://a/x", "http://a/x"]);
});

test("the leader answers 503 again: no further hop — back off then return to the origin node", async () => {
  const f = fake([
    json(503, { error: "x", leader: "http://b" }),
    json(503, { error: "x", leader: "http://c" }),
    json(200, {}),
  ]);
  const t = new Transport({ url: "http://a", fetch: f.fetch });
  await t.request("/x");
  assert.deepEqual(f.calls, ["http://a/x", "http://b/x", "http://a/x"]);
});

test("503 without a leader up to the limit ⇒ returns the last 503, with the exact attempt count", async () => {
  const f = fake([json(503, { error: "x" }), json(503, { error: "x" }), json(503, { error: "x" })]);
  const t = new Transport({ url: "http://a", fetch: f.fetch, maxAttempts: 3 });
  const { res } = await t.request("/x");
  assert.equal(res.status, 503);
  assert.equal(f.calls.length, 3);
});

test("429 without an id (nothing created) ⇒ wait Retry-After and resend; 429 with an id ⇒ return at once", async () => {
  const f = fake([
    json(429, { error: "overloaded", code: "range_overloaded", retry_after_ms: 1 }, { "retry-after": "0" }),
    json(200, {}),
  ]);
  const t = new Transport({ url: "http://a", fetch: f.fetch });
  assert.equal((await t.request("/x")).res.status, 200);
  assert.equal(f.calls.length, 2);
  const g = fake([json(429, { status: "throttled", id: "ab", retry_after_ms: 5 }, { "retry-after": "1" })]);
  const u = new Transport({ url: "http://a", fetch: g.fetch });
  assert.equal((await u.request("/x")).res.status, 429);
  assert.equal(g.calls.length, 1);
});

test("network error at the leader ⇒ back to the origin node; at the origin node ⇒ back off and retry (or throw if retryNetwork: false)", async () => {
  const f = fake([json(503, { error: "x", leader: "http://b" }), new TypeError("fetch failed"), json(200, {})]);
  const t = new Transport({ url: "http://a", fetch: f.fetch });
  assert.equal((await t.request("/x")).res.status, 200);
  assert.deepEqual(f.calls, ["http://a/x", "http://b/x", "http://a/x"]);
  const g = fake([new TypeError("fetch failed"), json(200, {})]);
  assert.equal((await new Transport({ url: "http://a", fetch: g.fetch }).request("/x")).res.status, 200);
  const h = fake([new TypeError("fetch failed")]);
  await assert.rejects(new Transport({ url: "http://a", fetch: h.fetch, retryNetwork: false }).request("/x"));
});

test("transport headers and request headers both go out; cancelling stops the wait", async () => {
  let seen: Record<string, string> = {};
  const t = new Transport({
    url: "http://a",
    headers: { authorization: "Bearer k" },
    fetch: (async (_u: unknown, init?: RequestInit) => {
      seen = init?.headers as Record<string, string>;
      return json(503, { error: "x" });
    }) as typeof fetch,
  });
  const ac = new AbortController();
  const p = t.request("/x", { headers: { "idempotency-key": "i" }, signal: ac.signal });
  setTimeout(() => ac.abort(new Error("cancelled")), 20);
  await assert.rejects(p, /cancelled/);
  assert.deepEqual(seen, { authorization: "Bearer k", "idempotency-key": "i" });
});

test("dead leader: not followed again within the same call — back off, ask the origin node until a new leader appears", async () => {
  const f = fake([
    json(503, { error: "x", leader: "http://b" }),
    new TypeError("fetch failed"),
    json(503, { error: "x", leader: "http://b" }),
    json(503, { error: "x", leader: "http://c" }),
    json(200, {}),
  ]);
  const t = new Transport({ url: "http://a", fetch: f.fetch });
  assert.equal((await t.request("/x")).res.status, 200);
  assert.deepEqual(f.calls, ["http://a/x", "http://b/x", "http://a/x", "http://a/x", "http://c/x"]);
});

/**
 * `sleepAbortable` did not remove its `abort` listener after sleeping ⇒ a
 * shared `AbortSignal` (a long-lived client) piled up a listener on every
 * backoff, up to `MaxListenersExceededWarning`. Must see: after sleeping the
 * signal has no listener left.
 */
test("sleepAbortable removes the abort listener after sleeping", async () => {
  const { getEventListeners } = await import("node:events");
  const { sleepAbortable } = await import("../src/transport.ts");
  const ac = new AbortController();
  for (let i = 0; i < 20; i++) await sleepAbortable(0, ac.signal);
  assert.equal(getEventListeners(ac.signal, "abort").length, 0);
});
