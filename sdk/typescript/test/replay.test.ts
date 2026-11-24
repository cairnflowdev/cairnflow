// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * The `REQUEST_RESPONSE` replay cache — wire v5 (`docs/protocol-v1.md` §9.3),
 * in process, through `EndpointCore` (shared by `serve()` and
 * `fetchHandler`).
 *
 * Must see:
 * - the digest chain matches the golden vector (same values as `cairn_runtime::replay`);
 * - a delta turn that hits ⇒ a response **byte-identical** to a full-replay turn on a
 *   fresh endpoint; a miss (no entry, digest mismatch, missing frame) ⇒ `409` +
 *   `cairn-replay: miss`, empty body, the handler does **not** run;
 * - the identity check comes before the cache: an unsigned request ⇒ `401`, nothing recorded;
 * - a finished turn (`End`) drops the entry, no `cairn-replay-cache` returned;
 * - a runtime speaking v4 ⇒ no v5 header, no cache (SDK v5 × runtime v4);
 * - LRU by bytes and TTL; an entry over the limit is not kept;
 * - defaults: `serve()` with `identityKeys` declares `replayCache`, `fetchHandler` does not; pinned
 *   to v4 it is not declared; without `identityKeys` there is no cache, and asking for one is a
 *   configuration error (a caller could otherwise splice a running invocation's journal);
 * - `FLY_MACHINE_ID` ⇒ `cairn-affinity: fly=<id>`.
 */

import assert from "node:assert/strict";
import { test } from "node:test";
import { EndpointCore, MAX_PROTOCOL_VERSION } from "../src/endpoint.ts";
import { fetchHandler, service, type ServiceContext } from "../src/fetch.ts";
import { serve } from "../src/serve.ts";
import { Writer } from "../src/protobuf.ts";
import { chainRoot, chainStep, ReplayCache } from "../src/replay.ts";
import { decodeFrames, encodeFrame, MT } from "../src/wire.ts";
import { signer } from "./signer.ts";

const hex = (b: Uint8Array) => Buffer.from(b).toString("hex");
const ID = new Uint8Array(24).fill(0x11);
/** `Input("req")` — a whole frame, as in the conformance traces. */
const INPUT = new Uint8Array(Buffer.from("040000000000000972070a052272657122", "hex"));

const v5 = MAX_PROTOCOL_VERSION >= 5 ? false : `SDK only speaks up to v${MAX_PROTOCOL_VERSION}`;

let runs = 0;
const svc = service("R", {
  async nap(ctx: ServiceContext) {
    runs++;
    await ctx.sleep(1000);
    return "done";
  },
  async done() {
    runs++;
    return "done";
  },
});

function start(known: number, from = 0, digest?: Uint8Array): Uint8Array {
  const w = new Writer().bytes(1, ID).uint32(3, known).uint64(9, 7n);
  if (from > 0) w.uint32(18, from).bytes(19, digest ?? new Uint8Array(32));
  return encodeFrame(MT.START, 0, w.finish());
}

function cat(...parts: Uint8Array[]): Uint8Array {
  return new Uint8Array(Buffer.concat(parts));
}

const CT = (v: number) => `application/vnd.cairnflow.invocation.v${v}`;

/** The runtime's signing key: the replay cache is only served with `identityKeys`. */
const rt = await signer();

async function post(core: EndpointCore, handler: string, body: Uint8Array, wire = 5) {
  const path = `/invoke/R/${handler}`;
  const h: Record<string, string> = { "content-type": CT(wire), ...(await rt.headers(path, body)) };
  return core.answer("POST", path, path, (n) => h[n], async () => body);
}

function core(replayCache: boolean | { maxBytes?: number; ttlMs?: number } = true, maxWire = MAX_PROTOCOL_VERSION) {
  return new EndpointCore([svc], "REQUEST_RESPONSE", maxWire, [rt.kid], false, replayCache);
}

test("the digest chain matches the runtime's golden vector", { skip: v5 }, async () => {
  const h0 = await chainRoot(ID);
  assert.equal(hex(h0), "1f64388c3fc28f0042a5c57665b4b9a00b95f335a30260a54323acffdd41bc74");
  assert.equal(hex(await chainStep(h0, INPUT)), "caedb1a740dbe983c6a7f0a4be6a7f94e8f0b48fd0324664f1b51c6d3994367a");
});

test("delta hit ⇒ exactly the bytes of a full replay; header cairn-replay-cache = N", { skip: v5 }, async () => {
  const a = core();
  // Turn 1 (full): Input ⇒ Sleep + Suspension, holds 1 entry.
  const first = await post(a, "nap", cat(start(1), INPUT));
  assert.equal(first.status, 200);
  assert.equal(first.headers?.["cairn-replay-cache"], "1");
  assert.equal(a.replay!.size, 1);
  // Turn 2: journal = Input + Sleep (the command just emitted, re-encoded by the runtime).
  const sleep = decodeFrames(first.body).find((f) => f.type === MT.SLEEP)!;
  const sleepRaw = encodeFrame(sleep.type, 0, sleep.payload);
  const h1 = await chainStep(await chainRoot(ID), INPUT);
  const delta = await post(a, "nap", cat(start(2, 1, h1), sleepRaw));
  // The same turn, full replay, on a fresh endpoint.
  const full = await post(core(), "nap", cat(start(2), INPUT, sleepRaw));
  assert.equal(delta.status, 200);
  assert.equal(hex(delta.body), hex(full.body), "assembling the delta must run exactly like a full replay");
  assert.equal(delta.headers?.["cairn-replay-cache"], "2");
});

test("miss ⇒ 409 + cairn-replay: miss, empty body, handler does not run", { skip: v5 }, async () => {
  const a = core();
  await post(a, "nap", cat(start(1), INPUT));
  const before = runs;
  const h1 = await chainStep(await chainRoot(ID), INPUT);
  const bad = new Uint8Array(h1);
  bad[0]! ^= 1;
  const cases: Array<[string, Uint8Array, EndpointCore]> = [
    ["digest mismatch", cat(start(2, 1, bad), INPUT), a],
    ["no entry", cat(start(2, 1, h1), INPUT), core()],
    ["B beyond the cache", cat(start(3, 2, h1), INPUT), a],
    ["missing frame", cat(start(3, 1, h1), INPUT), a],
  ];
  for (const [why, body, c] of cases) {
    const r = await post(c, "nap", body);
    assert.equal(r.status, 409, why);
    assert.equal(r.headers?.["cairn-replay"], "miss", why);
    assert.equal(r.body.length, 0, why);
  }
  assert.equal(runs, before, "the handler must not run on a miss turn");
});

test("identity before cache: unsigned request ⇒ 401, nothing recorded", { skip: v5 }, async () => {
  const c = new EndpointCore([svc], "REQUEST_RESPONSE", MAX_PROTOCOL_VERSION, ["publickeyv1_11111111111111111111111111111111"], false, true);
  const r = await post(c, "nap", cat(start(1), INPUT));
  assert.equal(r.status, 401);
  assert.equal(c.replay!.size, 0);
});

test("a finished turn drops the entry; runtime v4 ⇒ no header, no cache", { skip: v5 }, async () => {
  const a = core();
  await post(a, "nap", cat(start(1), INPUT));
  assert.equal(a.replay!.size, 1);
  const r = await post(a, "done", cat(start(1), INPUT));
  assert.equal(r.headers?.["cairn-replay-cache"], undefined);
  assert.equal(a.replay!.size, 0, "End ⇒ entry dropped");
  const b = core();
  const v4 = await post(b, "nap", cat(start(1), INPUT), 4);
  assert.equal(v4.status, 200);
  assert.deepEqual(v4.headers ?? {}, {});
  assert.equal(b.replay!.size, 0);
});

test("LRU by bytes, TTL, an entry over the limit is not kept", { skip: v5 }, async () => {
  const f = (n: number) => [new Uint8Array(n)];
  const ch = [new Uint8Array(32), new Uint8Array(32)];
  const c = new ReplayCache({ maxBytes: 1000, ttlMs: 50 });
  assert.ok(c.put("a", f(400), ch));
  assert.ok(c.put("b", f(400), ch));
  assert.ok(c.take("a", 1, ch[1]!), "using a ⇒ b becomes the oldest");
  assert.ok(c.put("c", f(400), ch));
  assert.equal(c.take("b", 1, ch[1]!), undefined, "b evicted");
  assert.ok(c.bytes <= 1000);
  assert.equal(c.put("big", f(2000), ch), false);
  const real = Date.now;
  try {
    const t0 = real();
    Date.now = () => t0 + 1000;
    assert.equal(c.take("a", 1, ch[1]!), undefined, "past the TTL");
  } finally {
    Date.now = real;
  }
  // Endpoint: an entry over the limit ⇒ no header returned.
  const tiny = core({ maxBytes: 8 });
  const r = await post(tiny, "nap", cat(start(1), INPUT));
  assert.equal(r.headers?.["cairn-replay-cache"], undefined);
});

test("defaults: serve() declares replayCache, fetchHandler does not; pinned to v4 it is not declared", { skip: v5 }, async () => {
  const m = (c: EndpointCore) => c.manifest() as { replayCache?: boolean };
  assert.equal(m(core(true)).replayCache, true);
  assert.equal(m(core(true, 4)).replayCache, undefined);
  assert.equal(m(core(false)).replayCache, undefined);
  const h = fetchHandler({ services: [svc] });
  const r = await h(new Request("http://x/discover"));
  const body = (await r.json()) as { replayCache?: boolean };
  assert.equal(body.replayCache, undefined, "fetchHandler off by default");
});

test("FLY_MACHINE_ID ⇒ cairn-affinity: fly=<id>", { skip: v5 }, async () => {
  const env = process.env;
  const old = env.FLY_MACHINE_ID;
  try {
    env.FLY_MACHINE_ID = "0123456789abcd";
    const r = await post(core(), "nap", cat(start(1), INPUT));
    assert.equal(r.headers?.["cairn-affinity"], "fly=0123456789abcd");
    env.FLY_MACHINE_ID = "bad id";
    const s = await post(core(), "nap", cat(start(1), INPUT));
    assert.equal(s.headers?.["cairn-affinity"], undefined);
  } finally {
    if (old === undefined) delete env.FLY_MACHINE_ID;
    else env.FLY_MACHINE_ID = old;
  }
});

// A miss carries the part still held.

test("miss carries cairn-replay-held = k:h_k, k = min(held, N); no entry ⇒ no header", { skip: v5 }, async () => {
  const a = core();
  // Holds 1 entry (Input), then 2 (Input + Sleep).
  const first = await post(a, "nap", cat(start(1), INPUT));
  const sleep = decodeFrames(first.body).find((f) => f.type === MT.SLEEP)!;
  const sleepRaw = encodeFrame(sleep.type, 0, sleep.payload);
  const h1 = await chainStep(await chainRoot(ID), INPUT);
  const h2 = await chainStep(h1, sleepRaw);
  await post(a, "nap", cat(start(2, 1, h1), sleepRaw));
  assert.equal(a.replay!.size, 1);
  const bad = new Uint8Array(32);
  // N = 3 ≥ held 2 ⇒ k = 2.
  const r = await post(a, "nap", cat(start(3, 2, bad), INPUT));
  assert.equal(r.status, 409);
  assert.equal(r.headers?.["cairn-replay-held"], `2:${hex(h2)}`);
  // N = 1 < held 2 ⇒ k = 1, h_1.
  const s = await post(a, "nap", cat(start(1, 1, bad)));
  assert.equal(s.status, 409);
  assert.equal(s.headers?.["cairn-replay-held"], `1:${hex(h1)}`);
  // No entry ⇒ only `cairn-replay: miss`.
  const t = await post(core(), "nap", cat(start(2, 1, h1), sleepRaw));
  assert.equal(t.status, 409);
  assert.equal(t.headers?.["cairn-replay-held"], undefined);
  // Resent from k: hit, runs like a full replay.
  const again = await post(a, "nap", cat(start(3, 2, h2), INPUT));
  assert.equal(again.status, 200);
});

// ─────────────────────────────────────────── an endpoint without identityKeys

const SECRET = encodeFrame(
  MT.INPUT,
  0,
  new Writer().message(14, new Writer().bytes(1, new TextEncoder().encode('"victim-secret"'))).finish(),
);
const seen: unknown[] = [];
const victim = service("V", {
  async leak(ctx: ServiceContext, input: unknown) {
    seen.push(input);
    await ctx.sleep(1000);
    return "done";
  },
});

/**
 * The replay cache on an endpoint that does not verify request identity: anyone who can
 * reach it and knows (or guesses) a running invocation id can run the handler on that
 * invocation's journal.
 *
 * 1. The runtime runs a turn of invocation `V` (input `victim-secret`), full replay; the
 *    endpoint keeps the journal prefix (`cairn-replay-cache: 1`).
 * 2. A caller sends `Start{id V, replay_from 1, prefix_digest garbage}`: `409` with
 *    `cairn-replay-held: 1:h_1` — the id is running, and here is its digest.
 * 3. The caller sends `Start{id V, replay_from 1, prefix_digest h_1}`: the endpoint splices
 *    the victim's prefix and runs the handler on `victim-secret`, for the caller.
 *
 * Must see: `serve()` without `identityKeys` declares no `replayCache`, answers no `409`,
 * and the handler never sees the victim's input a second time.
 */
test(
  "no identityKeys: serve() keeps no journal — a caller who knows a running id cannot splice its prefix",
  async () => {
    seen.length = 0;
    const ep = await serve({ services: [victim], port: 0, protocolMode: "REQUEST_RESPONSE" });
    try {
      const m = (await (await fetch(`${ep.url}/discover`)).json()) as { replayCache?: boolean };
      assert.equal(m.replayCache, undefined, "the manifest declares a replay cache without identityKeys");
      const send = async (body: Uint8Array) => {
        const r = await fetch(`${ep.url}/invoke/V/leak`, {
          method: "POST",
          headers: { "content-type": CT(5) },
          body: body as Uint8Array<ArrayBuffer>,
        });
        await r.arrayBuffer();
        return r;
      };
      assert.equal((await send(cat(start(1), SECRET))).status, 200);
      assert.deepEqual(seen, ["victim-secret"]);
      const probe = await send(start(1, 1, new Uint8Array(32)));
      assert.notEqual(probe.status, 409, "a 409 tells the caller that the id is running");
      assert.equal(probe.headers.get("cairn-replay-held"), null, "the held digest lets the caller splice the prefix");
      const h1 = await chainStep(await chainRoot(ID), SECRET);
      await send(start(1, 1, h1));
      assert.equal(
        seen.filter((x) => x === "victim-secret").length,
        1,
        "the handler ran again on the victim's input, for the caller",
      );
    } finally {
      await ep.close();
    }
  },
);

test(
  "replayCache without identityKeys is a configuration error; with identityKeys serve() declares it by default",
  async () => {
    assert.throws(() => fetchHandler({ services: [svc], replayCache: true }), /replayCache needs identityKeys/);
    assert.throws(
      () => new EndpointCore([svc], "REQUEST_RESPONSE", 5, undefined, false, { maxBytes: 1 << 20 }),
      /replayCache needs identityKeys/,
    );
    await assert.rejects(
      async () => serve({ services: [svc], port: 0, protocolMode: "REQUEST_RESPONSE", replayCache: true }),
      /replayCache needs identityKeys/,
    );
    // `false` stays allowed: it is what an unverified endpoint gets anyway.
    fetchHandler({ services: [svc], replayCache: false });
    const rt = await signer();
    const ep = await serve({ services: [svc], port: 0, protocolMode: "REQUEST_RESPONSE", identityKeys: [rt.kid] });
    try {
      const r = await fetch(`${ep.url}/discover`, { headers: await rt.headers("/discover") });
      assert.equal(r.status, 200);
      const m = (await r.json()) as { replayCache?: boolean };
      assert.equal(m.replayCache, true, "serve() with identityKeys keeps the default on");
    } finally {
      await ep.close();
    }
  },
);
