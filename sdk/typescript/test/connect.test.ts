// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * `connect()` over a fake `fetch`: the route of each method, the idempotency key
 * generated automatically and kept across retries, 202 ⇒ attach, errors by the
 * `InvokeResponse` body. The real path on the binary: `client.e2e.test.ts`.
 */

import assert from "node:assert/strict";
import { test } from "node:test";
import {
  AlreadyInvoked,
  AttachTimeout,
  CairnHttpError,
  connect,
  InvocationFailed,
  InvocationPaused,
} from "../src/client.ts";
import { TerminalError } from "../src/index.ts";

const ID = "0123456789abcdef0123456789abcdef0123456789abcdef";

interface Call {
  url: string;
  method: string;
  headers: Record<string, string>;
  body: string | undefined;
}

function fake(replies: Array<Response | Error>): { fetch: typeof fetch; calls: Call[] } {
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
      if (r instanceof Error) throw r;
      return r;
    }) as typeof fetch,
  };
}

const reply = (status: number, body: unknown, id: string | null = ID) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", ...(id === null ? {} : { "x-cairn-invocation-id": id }) },
  });

test("invoke: 202 ⇒ attach by id until done; the idempotency key is generated and kept across retries", async () => {
  const f = fake([
    reply(503, { error: "electing" }, null),
    reply(202, { status: "waiting", id: ID }),
    reply(202, { status: "waiting", id: ID }),
    reply(200, { status: "completed", id: ID, output: '{"ok":true}' }),
  ]);
  const c = connect({ url: "http://a", apiKey: "cf_k", fetch: f.fetch, attachWaitMs: 1000 });
  const out = await c.service("Orders").submit!({ amount: 5 });
  assert.deepEqual(out, { ok: true });
  assert.deepEqual(
    f.calls.map((x) => `${x.method} ${x.url}`),
    [
      "POST http://a/invoke/Orders/submit",
      "POST http://a/invoke/Orders/submit",
      `POST http://a/invocations/${ID}/attach?wait_ms=1000`,
      `POST http://a/invocations/${ID}/attach?wait_ms=1000`,
    ],
  );
  const k0 = f.calls[0]!.headers["idempotency-key"];
  assert.match(k0 ?? "", /^[0-9a-f-]{36}$/);
  assert.equal(f.calls[1]!.headers["idempotency-key"], k0, "a retry must carry the same key");
  assert.equal(f.calls[0]!.headers["authorization"], "Bearer cf_k");
  assert.equal(f.calls[0]!.body, '{"amount":5}');
  assert.equal(f.calls[2]!.headers["idempotency-key"], undefined, "attach needs no key");
});

test("idempotencyKey: null ⇒ no key; a workflow's run does not generate a key; key, scope, limitKey go into the path and headers", async () => {
  const f = fake([
    reply(200, { status: "completed", id: ID, output: "1" }),
    reply(200, { status: "completed", id: ID, output: "true" }),
    reply(200, { status: "completed", id: ID, output: '"s"' }),
  ]);
  const c = connect({ url: "http://a", fetch: f.fetch });
  assert.equal(await c.object("Cart", "u 1").add!("x", { idempotencyKey: null, limitKey: "acme" }), 1);
  assert.equal(await c.workflow("Signup", "u1", { scope: "s" }).run!({}), true);
  assert.equal(await c.workflow("Signup", "u1").click!("k"), "s");
  assert.equal(f.calls[0]!.url, "http://a/invoke/Cart/add?key=u%201");
  assert.equal(f.calls[0]!.headers["idempotency-key"], undefined);
  assert.equal(f.calls[0]!.headers["x-cairn-limit-key"], "acme");
  assert.equal(f.calls[1]!.url, "http://a/scope/s/invoke/Signup/run?key=u1");
  assert.equal(f.calls[1]!.headers["idempotency-key"], undefined);
  assert.ok(f.calls[2]!.headers["idempotency-key"], "a workflow's SHARED handler still generates a key");
});

test("outcomes: failed ⇒ InvocationFailed (a TerminalError), paused ⇒ InvocationPaused, already_invoked ⇒ AlreadyInvoked, 404 ⇒ CairnHttpError", async () => {
  const f = fake([
    reply(500, { status: "failed", id: ID, code: 409, message: "out of stock" }),
    reply(409, { status: "paused", id: ID, code: 575, message: "waiting for a human" }),
    reply(409, { status: "already_invoked", id: ID }),
    reply(404, { error: "no such service" }, null),
  ]);
  const c = connect({ url: "http://a", fetch: f.fetch });
  const s = c.service("S");
  await assert.rejects(s.h!(1), (e: unknown) => {
    assert.ok(e instanceof InvocationFailed && e instanceof TerminalError);
    assert.equal(e.code, 409);
    assert.equal(e.message, "out of stock");
    assert.equal(e.invocationId, ID);
    return true;
  });
  await assert.rejects(s.h!(1), (e: unknown) => e instanceof InvocationPaused && e.code === 575);
  await assert.rejects(c.workflow("W", "k").run!(1), (e: unknown) => e instanceof AlreadyInvoked && e.invocationId === ID);
  await assert.rejects(s.h!(1), (e: unknown) => e instanceof CairnHttpError && e.status === 404);
});

test("send: returns a handle with the id as soon as it is durable; a handler named send is callable; send.<h> is a send", async () => {
  const f = fake([
    reply(202, { status: "waiting", id: ID }),
    reply(200, { status: "completed", id: ID, output: "3" }),
    reply(202, { status: "waiting", id: ID }),
  ]);
  const c = connect({ url: "http://a", fetch: f.fetch });
  const chat = c.object("Chat", "c1");
  const h = await chat.send.ask!({ q: 1 });
  assert.equal(h.id, ID);
  assert.equal(await (chat.send as unknown as (i: unknown) => Promise<unknown>)({ text: "hi" }), 3);
  const { id } = await c.service("S").send.go!(undefined, { idempotencyKey: "k" });
  assert.equal(id, ID);
  assert.deepEqual(
    f.calls.map((x) => x.url),
    ["http://a/invoke/Chat/ask/send?key=c1", "http://a/invoke/Chat/send?key=c1", "http://a/invoke/S/go/send"],
  );
  assert.equal(f.calls[2]!.body, undefined, "input absent ⇒ empty body");
  assert.equal(f.calls[2]!.headers["idempotency-key"], "k");
});

test("invocation: signal, cancel (cooperative by default), status, output without waiting; awakeable; promise", async () => {
  const ack = (body: unknown) => reply(200, body, null);
  const f = fake([
    ack({ id: ID, status: "suspended" }),
    ack({ id: ID, cancelled: false, status: "running" }),
    ack({ id: ID, cancelled: true, status: "failed" }),
    ack({ id: ID, status: "completed", output: "1", createdAt: 5 }),
    reply(202, { status: "waiting", id: ID }),
    ack({ id: "awk_1", status: "running" }),
    ack({ id: "awk_1", status: "running" }),
    ack({ service: "W", key: "k", name: "p", completed: true, woken: 1 }),
    ack({ service: "W", key: "k", name: "p", completed: true, value: "1" }),
  ]);
  const c = connect({ url: "http://a", fetch: f.fetch });
  const inv = c.invocation(ID);
  assert.equal((await inv.signal("approve", { by: "x" })).status, "suspended");
  await inv.cancel();
  await inv.cancel({ mode: "hard" });
  assert.equal((await inv.status()).createdAt, 5);
  assert.deepEqual(await inv.output(), { ready: false });
  await c.awakeable("awk_1").resolve({ ok: 1 });
  await c.awakeable("awk_1").reject("no", 400);
  assert.equal((await c.promise("W", "k", "p").resolve(1)).woken, 1);
  assert.equal((await c.promise("W", "k", "p").peek()).value, "1");
  assert.deepEqual(
    f.calls.map((x) => `${x.method} ${x.url} ${x.body ?? ""}`),
    [
      `POST http://a/invocations/${ID}/signal {"signal":"approve","value":{"by":"x"}}`,
      `POST http://a/invocations/${ID}/cancel?mode=signal `,
      `POST http://a/invocations/${ID}/cancel `,
      `GET http://a/invocations/${ID} `,
      `POST http://a/invocations/${ID}/attach?wait_ms=0 `,
      `POST http://a/awakeables/awk_1/resolve {"ok":1}`,
      `POST http://a/awakeables/awk_1/reject {"message":"no","code":400}`,
      `POST http://a/promises/W/k/p/resolve 1`,
      `GET http://a/promises/W/k/p `,
    ],
  );
});

test("attach: timeoutMs runs out ⇒ AttachTimeout; wait_ms never exceeds the remaining time", async () => {
  const f = fake(Array.from({ length: 50 }, () => reply(202, { status: "waiting", id: ID })));
  const slow = (async (u: string | URL | Request, i?: RequestInit) => {
    await new Promise((r) => setTimeout(r, 30));
    return f.fetch(u, i);
  }) as typeof fetch;
  const c2 = connect({ url: "http://a", fetch: slow, attachWaitMs: 25_000 });
  await assert.rejects(c2.invocation(ID).attach({ timeoutMs: 100 }), (e: unknown) => e instanceof AttachTimeout);
  const waits = f.calls.map((x) => Number(new URL(x.url).searchParams.get("wait_ms")));
  assert.ok(waits.length >= 2 && waits.every((w) => w <= 100), `wait_ms ${waits}`);
});

/**
 * attach receiving `429 too_many_open_streams` (the per-tenant cap on long-lived
 * connections) — it carries no id, the invocation **is running**. `attachLoop` only
 * treated `503` as retryable ⇒ `call()` threw; the caller retrying created a second
 * invocation (a freshly generated idempotency key), the side effect ran twice.
 *
 * Must see: attach is a read — on `429` it waits and asks again, `call()` returns the result.
 */
test("attach meeting 429 too_many_open_streams waits and asks again, does not throw", async () => {
  const busy = () =>
    new Response(JSON.stringify({ error: "too many open streams", code: "too_many_open_streams" }), {
      status: 429,
      headers: { "content-type": "application/json", "retry-after": "0" },
    });
  const f = fake([
    reply(202, { status: "waiting", id: ID }),
    busy(),
    busy(),
    busy(),
    reply(200, { status: "completed", id: ID, output: '"done"' }),
  ]);
  const c = connect({ url: "http://a", fetch: f.fetch, attachWaitMs: 1000, maxAttempts: 1 });
  assert.equal(await c.service("Orders").submit!({}), "done");
  assert.equal(f.calls.filter((x) => x.url.includes("/invoke/")).length, 1, "no second invoke");
});

/**
 * (a) `send` meeting a `409 paused` of the invocation with the same key threw
 *     `InvocationPaused` instead of returning a handle — the docs say a `409` with the
 *     same key "is still the same invocation".
 * (b) a workflow's `run` carries no idempotency key: the transport resent after a
 *     network error (the first attempt may have reached the server) ⇒ `409
 *     already_invoked` for its **own** run ⇒ `AlreadyInvoked`.
 * Must see: (a) returns a handle with the right id; (b) `run` waits for that invocation and returns its result.
 */
test("send meeting 409 paused returns a handle; a workflow run resent after a network error that gets already_invoked waits for itself", async () => {
  const f = fake([
    reply(409, { status: "paused", id: ID, code: 575, message: "waiting for a human" }),
    new TypeError("fetch failed"),
    reply(409, { status: "already_invoked", id: ID }),
    reply(200, { status: "completed", id: ID, output: '"ran"' }),
  ]);
  const c = connect({ url: "http://a", fetch: f.fetch, attachWaitMs: 1000 });
  const h = await c.service("S").send.go!(undefined, { idempotencyKey: "k" });
  assert.equal(h.id, ID);
  assert.equal(await c.workflow("W", "k").run!(1), "ran");
});
