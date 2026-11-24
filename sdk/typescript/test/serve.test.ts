// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * The SDK's port — preface sniffing, two protocols on one port, and clean shutdown.
 *
 * `node:http2.connect("http://…")` speaks h2c prior-knowledge exactly like the
 * runtime's client, so the whole `BIDI_STREAM` path can be checked here without the Rust binary.
 * The cross-language test (`e2e.test.ts`) is still the final proof; this one catches
 * bugs earlier and points straight at `serve.ts`.
 */

import assert from "node:assert/strict";
import * as http from "node:http";
import * as http2 from "node:http2";
import * as net from "node:net";
import { after, test } from "node:test";
import {
  chatObject,
  type Context,
  type Endpoint,
  type Frame,
  FrameSplitter,
  manifest,
  MAX_PROTOCOL_VERSION,
  MT,
  serve,
  service,
  workflow,
  type ServiceContext,
} from "../src/index.ts";
import { encodeFrame } from "../src/wire.ts";
import { Writer } from "../src/protobuf.ts";

const enc = new TextEncoder();
const dec = new TextDecoder();

let blockRelease: (() => void) | undefined;

const svc = service("S", {
  async echo(_ctx: ServiceContext, input: { v: number }) {
    return { v: input.v * 2 };
  },
  /** Hangs until the test lets it continue — to check that `close()` waits for in-flight requests. */
  async block() {
    await new Promise<void>((r) => {
      blockRelease = r;
    });
    return { done: true };
  },
});

const opened: Endpoint[] = [];

async function up(protocolMode?: "BIDI_STREAM" | "REQUEST_RESPONSE"): Promise<Endpoint> {
  const ep = await serve({
    services: [svc],
    port: 0,
    ...(protocolMode !== undefined ? { protocolMode } : {}),
  });
  opened.push(ep);
  return ep;
}

after(async () => {
  for (const ep of opened) await ep.close().catch(() => undefined);
});

/** Waits for the `block` handler to reach its hang point, returns the latch to open. */
async function waitForBlock(): Promise<() => void> {
  const deadline = Date.now() + 5_000;
  for (;;) {
    const release = blockRelease;
    if (release !== undefined) {
      blockRelease = undefined; // ready for next time
      return release;
    }
    if (Date.now() > deadline) throw new Error("handler `block` did not run");
    await new Promise((r) => setTimeout(r, 10));
  }
}

function startFrame(knownEntries: number): Uint8Array {
  return encodeFrame(MT.START, 0, new Writer().uint32(3, knownEntries).uint64(9, 1n).finish());
}

function inputFrame(value: unknown): Uint8Array {
  return encodeFrame(
    MT.INPUT,
    0,
    new Writer().message(14, new Writer().bytes(1, enc.encode(JSON.stringify(value)))).finish(),
  );
}

// ─────────────────────────────────────────────────────────── one port

test("h1 and h2c on the same port: discovery over h1, invoke over h2c", async () => {
  const ep = await up("BIDI_STREAM");

  // ── h1: `/discover` must work even when the manifest declares BIDI_STREAM.
  //    The runtime reads the manifest BEFORE it knows the mode, so this path must not
  //    depend on its own answer.
  const r = await fetch(`${ep.url}/discover`);
  assert.equal(r.status, 200);
  assert.match(r.headers.get("content-type") ?? "", /endpointmanifest\.v1\+json/);
  const m = (await r.json()) as { protocolMode: string; services: { name: string }[] };
  assert.equal(m.protocolMode, "BIDI_STREAM");
  assert.deepEqual(m.services.map((s) => s.name), ["S"]);

  // ── h2c on **that same port**: the preface is sniffed and the socket goes to the
  //    http2 server.
  const client = http2.connect(ep.url);
  try {
    const frames = await new Promise<Frame[]>((resolve, reject) => {
      const stream = client.request({
        ":method": "POST",
        ":path": "/invoke/S/echo",
        "content-type": "application/vnd.cairnflow.invocation.v1",
      });
      const split = new FrameSplitter();
      const got: Frame[] = [];
      stream.on("data", (c: Buffer) => got.push(...split.push(new Uint8Array(c))));
      stream.on("end", () => resolve(got));
      stream.on("error", reject);
      stream.write(Buffer.from(startFrame(1)));
      // `end`, not just `write`: this handler finishes in one turn, so the real runtime
      // also lets go of the stream right after. Leaving it open would keep the stream open
      // forever, and `close()` would wait for it — correct, but not what this test
      // means to measure.
      stream.end(Buffer.from(inputFrame({ v: 21 })));
    });
    assert.deepEqual(frames.map((f) => f.type), [MT.OUTPUT, MT.END]);
    assert.match(dec.decode(frames[0]!.payload), /"v":42/);
  } finally {
    client.close();
  }
});

test("REQUEST_RESPONSE: no h2 server is built, and h1 still serves /invoke", async () => {
  const ep = await up("REQUEST_RESPONSE");
  const m = (await (await fetch(`${ep.url}/discover`)).json()) as { protocolMode: string };
  assert.equal(m.protocolMode, "REQUEST_RESPONSE");

  const body = Buffer.concat([Buffer.from(startFrame(1)), Buffer.from(inputFrame({ v: 5 }))]);
  const r = await fetch(`${ep.url}/invoke/S/echo`, { method: "POST", body });
  assert.equal(r.status, 200);
  const split = new FrameSplitter();
  const frames = split.push(new Uint8Array(await r.arrayBuffer()));
  assert.deepEqual(frames.map((f) => f.type), [MT.OUTPUT, MT.END]);
  assert.match(dec.decode(frames[0]!.payload), /"v":10/);
});

// ─────────────────────────────────────────────────────────── workflow

test("workflow(): `run` is WORKFLOW, the rest are SHARED", async () => {
  const wf = workflow("Signup", {
    async run(ctx: Context) {
      return { key: ctx.key };
    },
    async click() {
      return null;
    },
  });
  const ep = await serve({ services: [wf], port: 0 });
  opened.push(ep);

  const m = (await (await fetch(`${ep.url}/discover`)).json()) as {
    services: { ty: string; handlers: { name: string; ty?: string }[] }[];
  };
  assert.equal(m.services[0]!.ty, "WORKFLOW");
  const byName = new Map(m.services[0]!.handlers.map((h) => [h.name, h.ty]));
  // The runtime refuses registration if the number of WORKFLOW handlers is not 1 — the two lines
  // below are what keeps `workflow()` from producing a manifest that gets refused.
  assert.equal(byName.get("run"), "WORKFLOW");
  assert.equal(byName.get("click"), "SHARED");
});

// ─────────────────────────────────────────────────────────── shutdown

test("close() drops idle keep-alive connections instead of waiting for them forever", async () => {
  // Accepting sockets at the `net` layer loses `http.Server.close()`'s automatic cleanup
  // (it only knows sockets it accepted itself). Without our own ledger `close()` hangs
  // forever on a keep-alive discovery connection the runtime has not let go of.
  const ep = await up("BIDI_STREAM");
  const agent = new http.Agent({ keepAlive: true });
  await new Promise<void>((res, rej) => {
    http
      .get({ port: ep.port, path: "/discover", agent }, (r) => {
        r.resume();
        r.on("end", () => res());
      })
      .on("error", rej);
  });

  await Promise.race([
    ep.close(),
    new Promise((_, rej) => setTimeout(() => rej(new Error("close() hung")), 5_000)),
  ]);
  agent.destroy();
});

test("close() waits for an in-flight request to finish answering", async () => {
  const ep = await up("REQUEST_RESPONSE");
  const body = Buffer.concat([Buffer.from(startFrame(1)), Buffer.from(inputFrame(null))]);
  const inflight = fetch(`${ep.url}/invoke/S/block`, { method: "POST", body });

  const release = await waitForBlock();

  let closed = false;
  const closing = ep.close().then(() => {
    closed = true;
  });
  await new Promise((r) => setTimeout(r, 200));
  assert.equal(closed, false, "close() cut off a request being served");

  release();
  const r = await inflight;
  assert.equal(r.status, 200, "an in-flight request was killed on close");
  const frames = new FrameSplitter().push(new Uint8Array(await r.arrayBuffer()));
  assert.deepEqual(frames.map((f) => f.type), [MT.OUTPUT, MT.END]);
  await closing;
});

test("close() also waits for an in-flight h2c invocation", async () => {
  // Symmetric with the test above, but over `BIDI_STREAM`: here "being served"
  // means an h2 stream still open. `Http2Server.close()` sends GOAWAY and lets
  // open streams finish — so `close()` must NOT drop h2 sockets itself, even though
  // idle h1 sockets must be dropped (the test just above).
  const ep = await up("BIDI_STREAM");
  const client = http2.connect(ep.url);
  const stream = client.request({
    ":method": "POST",
    ":path": "/invoke/S/block",
    "content-type": "application/vnd.cairnflow.invocation.v1",
  });
  const split = new FrameSplitter();
  const got: Frame[] = [];
  stream.on("data", (c: Buffer) => got.push(...split.push(new Uint8Array(c))));
  const ended = new Promise<void>((res, rej) => {
    stream.on("end", () => res());
    stream.on("error", rej);
  });
  stream.write(Buffer.from(startFrame(1)));
  stream.write(Buffer.from(inputFrame(null)));

  const release = await waitForBlock();

  let closed = false;
  const closing = ep.close().then(() => {
    closed = true;
  });
  await new Promise((r) => setTimeout(r, 200));
  assert.equal(closed, false, "close() cut off an h2c invocation that was running");

  release();
  await ended;
  assert.deepEqual(got.map((f) => f.type), [MT.OUTPUT, MT.END]);
  assert.match(dec.decode(got[0]!.payload), /"done":true/);

  // The runtime lets go of the stream when the turn ends; here we do it in its place. Without
  // that the stream stays half-open, `Http2Server.close()` keeps waiting for it, and `close()`
  // hangs — exactly the "wait for in-flight work" semantics, just with work that never ends.
  stream.end();
  client.close();
  await closing;
});

test("a socket that opens and stays silent does not hold close() back", async () => {
  // A connection that has said nothing has not been sniffed yet, so it belongs to neither h1 nor h2
  // — and if nobody puts it in the ledger, `front.close()` waits for it forever. A health check
  // that knocks on the port and walks away is enough to create this situation.
  const ep = await up("BIDI_STREAM");
  const silent = net.connect(ep.port, "127.0.0.1");
  await new Promise<void>((res, rej) => {
    silent.once("connect", () => res());
    silent.once("error", rej);
  });

  await Promise.race([
    ep.close(),
    new Promise((_, rej) => setTimeout(() => rej(new Error("close() hung on a silent socket")), 5_000)),
  ]);
  silent.destroy();
});

/**
 * **D-a at the type level.** The handler of a plain service gets a `ServiceContext` —
 * no state, no promises: the runtime rejects them (574) because a service has no
 * key, and without a scope there is no object for the state to belong to. Each
 * `@ts-expect-error` below is a `tsc --noEmit` check: if the types allowed the call
 * again, the directive becomes unused and `tsc` reports an error.
 *
 * The handler bodies never run — the test only needs them to compile as expected.
 */
test("a plain service has no state or promises at the type level", () => {
  const plain = service("Plain", {
    async h(ctx) {
      // @ts-expect-error — state exists only on virtual objects/workflows (D-a)
      await ctx.get("k");
      // @ts-expect-error
      ctx.set("k", 1);
      // @ts-expect-error
      ctx.clear("k");
      // @ts-expect-error
      ctx.clearAll();
      // @ts-expect-error
      await ctx.getKeys();
      // @ts-expect-error — promises belong to objects
      ctx.promise("p");
      // The rest of `Context` is all there.
      await ctx.run("r", () => 1);
      ctx.send("S", "echo", {});
      return ctx.key;
    },
  });
  service("Plain2", {
    // @ts-expect-error — a handler that requires the full `Context` is not assignable to a plain
    // service
    async h(ctx: Context) {
      return ctx.key;
    },
  });
  // A virtual object has everything.
  const obj = service(
    "Obj",
    {
      async h(ctx: Context) {
        ctx.set("k", (await ctx.get<number>("k")) ?? 0);
        return null;
      },
    },
    { type: "VIRTUAL_OBJECT" },
  );
  assert.equal(plain.type, "SERVICE");
  assert.equal(obj.type, "VIRTUAL_OBJECT");
});

/**
 * The handler contract: field names **match the endpoint manifest of Restate's service protocol**,
 * the service declares at the service level, a handler overrides at the handler level, and when
 * nothing is declared the manifest only carries the SDK's default inactivity limit (15 minutes) —
 * every other field follows the node's defaults.
 */
test("the manifest carries the handler contract with Restate's field names", () => {
  const svc = service(
    "Agent",
    { think: async () => 1, quick: async () => 2 },
    {
      options: { inactivityTimeout: 600_000, retryPolicy: { maxAttempts: 5, onMaxAttempts: "PAUSE" } },
      handlerOptions: {
        quick: { inactivityTimeout: 1_000, retryPolicy: { initialInterval: 50 }, trace: "runtime" },
      },
    },
  );
  const m = manifest([svc, service("Plain", { a: async () => 0 })]) as {
    services: Array<Record<string, unknown> & { handlers: Array<Record<string, unknown>> }>;
  };
  const agent = m.services[0]!;
  assert.equal(agent.inactivityTimeout, 600_000);
  assert.equal(agent.retryPolicyMaxAttempts, 5);
  assert.equal(agent.retryPolicyOnMaxAttempts, "PAUSE");
  const quick = agent.handlers.find((h) => h.name === "quick")!;
  assert.equal(quick.inactivityTimeout, 1_000);
  assert.equal(quick.retryPolicyInitialInterval, 50);
  assert.equal(quick.trace, "runtime", "trace: runtime");
  const think = agent.handlers.find((h) => h.name === "think")!;
  assert.deepEqual(Object.keys(think), ["name"]);
  // Nothing declared: only the SDK's default inactivity limit.
  assert.deepEqual(Object.keys(m.services[1]!).sort(), ["handlers", "inactivityTimeout", "name", "ty"]);
});

/**
 * Nobody declares `inactivityTimeout` ⇒ the manifest declares 15 minutes at the service level.
 * Deployments that register `https://` endpoints run them as `REQUEST_RESPONSE`, where the
 * inactivity limit wraps the whole turn, and the node's default limit (60 s) cuts a long
 * model-calling `ctx.run` and reruns it forever. A value the service or handler declares wins; a
 * handler's own value still overrides the service.
 */
test("the manifest declares a 15-minute inactivityTimeout when nobody declares one", () => {
  const m = manifest([
    service("Plain", { a: async () => 0 }),
    service("Own", { b: async () => 0 }, { options: { inactivityTimeout: 5_000 } }),
    service("ByHandler", { c: async () => 0 }, { handlerOptions: { c: { inactivityTimeout: 7_000 } } }),
  ]) as { services: Array<Record<string, unknown> & { handlers: Array<Record<string, unknown>> }> };
  assert.equal(m.services[0]!.inactivityTimeout, 900_000);
  assert.equal(m.services[1]!.inactivityTimeout, 5_000);
  assert.equal(m.services[2]!.inactivityTimeout, 900_000);
  assert.equal(m.services[2]!.handlers[0]!.inactivityTimeout, 7_000);
});

/**
 * The AI helper builds the service with `retryPolicy.maxAttempts` 5 when the user declares none —
 * without it the runtime retries forever, and each attempt of a model-calling step is another token
 * bill. A value the user declares wins.
 */
test("chatObject defaults to maxAttempts 5", () => {
  const run = () => ({ toUIMessageStream: async function* () {} });
  const fields = (svc: Parameters<typeof manifest>[0][number]) =>
    (manifest([svc]) as { services: Array<Record<string, unknown>> }).services[0]!;
  assert.equal(fields(chatObject("Chat", run)).retryPolicyMaxAttempts, 5);
  assert.equal(
    fields(chatObject("Own", run, { options: { retryPolicy: { maxAttempts: 2, initialInterval: 10 } } }))
      .retryPolicyMaxAttempts,
    2,
  );
  const keep = fields(chatObject("Keep", run, { options: { trace: "runtime" } }));
  assert.equal(keep.retryPolicyMaxAttempts, 5);
  assert.equal(keep.trace, "runtime");
});

/** `logFlushMs` goes into the manifest at the handler level. */
test("the manifest carries the handler's logFlushMs", () => {
  const svc = service(
    "Agent",
    { chat: async () => 1 },
    { handlerOptions: { chat: { logFlushMs: 500 } } },
  );
  const m = manifest([svc]) as { services: Array<{ handlers: Array<Record<string, unknown>> }> };
  assert.equal(m.services[0]!.handlers[0]!.logFlushMs, 500);
});

// ─────────────────────────────────────────────────────────── ghim wire version

/**
 * `maxProtocolVersion` pins the SDK to an older wire version: the manifest declares exactly
 * that number, and a higher content-type from the runtime is clamped to it — `ctx.scoped` (v2)
 * is still rejected even though the runtime speaks v3.
 */
test("maxProtocolVersion: the manifest declares exactly the limit; invoke above the limit ⇒ 415, at the limit ctx.scoped (v2) is still rejected", async () => {
  const scoped = service(
    "Scoped",
    {
      async go(ctx: Context) {
        return await ctx.scoped("tenant-a").call<number>("Callee", "work", 1);
      },
    },
    { type: "VIRTUAL_OBJECT" },
  );
  const ep = await serve({
    services: [scoped],
    port: 0,
    protocolMode: "REQUEST_RESPONSE",
    maxProtocolVersion: 1,
  });
  opened.push(ep);
  const m = (await (await fetch(`${ep.url}/discover`)).json()) as {
    minProtocolVersion: number;
    maxProtocolVersion: number;
  };
  assert.deepEqual([m.minProtocolVersion, m.maxProtocolVersion], [1, 1]);

  const body = Buffer.concat([Buffer.from(startFrame(1)), Buffer.from(inputFrame(null))]);
  const above = await fetch(`${ep.url}/invoke/Scoped/go`, {
    method: "POST",
    body,
    headers: { "content-type": "application/vnd.cairnflow.invocation.v3" },
  });
  assert.equal(above.status, 415);
  await above.arrayBuffer();
  const r = await fetch(`${ep.url}/invoke/Scoped/go`, {
    method: "POST",
    body,
    headers: { "content-type": "application/vnd.cairnflow.invocation.v1" },
  });
  const frames = new FrameSplitter().push(new Uint8Array(await r.arrayBuffer()));
  // A terminal error (Output failure + End), no `Call` carrying a scope is emitted.
  assert.deepEqual(frames.map((f) => f.type), [MT.OUTPUT, MT.END]);
  assert.match(dec.decode(frames[0]!.payload), /wire version 1/);

  // Above the SDK's limit — computed from the constant, not a hard-coded number that would go stale
  // exactly at the next wire bump.
  const beyond = MAX_PROTOCOL_VERSION + 1;
  assert.throws(
    () => serve({ services: [scoped], port: 0, maxProtocolVersion: beyond }),
    new RegExp(`maxProtocolVersion ${beyond}`),
  );
});

/**
 * A 24 h soak died after 5 h 10 min: a runtime node was `kill -9`ed right as it opened an h2 stream
 * ⇒ the h2 server emitted `stream` for a stream already cancelled (the session died with the
 * socket) ⇒ `stream.respond()` threw `ERR_HTTP2_INVALID_STREAM` in an event handler ⇒ nobody caught
 * it ⇒ the whole SDK process died. The window is too narrow to reproduce deterministically over a
 * real network, so the test pins it at the level of the function that sends headers.
 *
 * Must see: a cancelled/closed stream ⇒ `respond` not called, returns `false`; `respond`
 * throws `ERR_HTTP2_INVALID_STREAM` ⇒ swallowed, returns `false`; any other error ⇒ rethrown.
 */
test("h2: sending headers to a cancelled stream does not crash the SDK process", async () => {
  const mod = (await import("../src/serve.ts")) as Record<string, unknown>;
  const respondIfOpen = mod.respondIfOpen as
    | ((s: { respond(h: object): void; destroyed: boolean; closed: boolean }, h: object) => boolean)
    | undefined;
  assert.equal(typeof respondIfOpen, "function");
  let calls = 0;
  const dead = { destroyed: true, closed: true, respond: () => void calls++ };
  assert.equal(respondIfOpen!(dead, { ":status": 200 }), false);
  assert.equal(calls, 0);
  const racing = {
    destroyed: false,
    closed: false,
    respond: () => {
      throw Object.assign(new Error("The stream has been destroyed"), { code: "ERR_HTTP2_INVALID_STREAM" });
    },
  };
  assert.equal(respondIfOpen!(racing, { ":status": 200 }), false);
  const other = {
    destroyed: false,
    closed: false,
    respond: () => {
      throw Object.assign(new Error("other"), { code: "ERR_OTHER" });
    },
  };
  assert.throws(() => respondIfOpen!(other, { ":status": 200 }), /other/);
  const alive = { destroyed: false, closed: false, respond: () => void calls++ };
  assert.equal(respondIfOpen!(alive, { ":status": 200 }), true);
  assert.equal(calls, 1);
});

/**
 * The runtime speaks a version **higher** than the limit the SDK declares — the deployment settled
 * on v6 but the SDK process was downgraded to an old build without registering again. Clamping the
 * version to the limit would read data in another build's layout (v6 ⇒ `content` is the tail of a
 * segmented value). Must see: `415`, the handler does not run — over both h1 and h2c.
 */
test("a content-type carrying a version above the SDK's limit ⇒ 415, the handler does not run", async () => {
  let ran = 0;
  const svc = service("V", {
    async go() {
      ran++;
      return 1;
    },
  });
  const body = Buffer.concat([Buffer.from(startFrame(1)), Buffer.from(inputFrame(null))]);
  const tooNew = `application/vnd.cairnflow.invocation.v${MAX_PROTOCOL_VERSION + 1}`;

  const rr = await serve({ services: [svc], port: 0, protocolMode: "REQUEST_RESPONSE" });
  opened.push(rr);
  const r = await fetch(`${rr.url}/invoke/V/go`, { method: "POST", body, headers: { "content-type": tooNew } });
  assert.equal(r.status, 415);
  await r.arrayBuffer();

  const bidi = await serve({ services: [svc], port: 0 });
  opened.push(bidi);
  const client = http2.connect(bidi.url);
  try {
    const status = await new Promise<number>((resolve, reject) => {
      const stream = client.request({ ":method": "POST", ":path": "/invoke/V/go", "content-type": tooNew });
      let status = 0;
      stream.on("response", (h) => {
        status = Number(h[":status"]);
      });
      stream.resume();
      stream.on("end", () => resolve(status));
      stream.on("error", reject);
      stream.end(body);
    });
    assert.equal(status, 415);
  } finally {
    client.close();
  }
  assert.equal(ran, 0);
});

/**
 * `serve()` over h1 used to read the whole body (unbounded) **before** checking identity — a
 * stranger could push a body of any size into the endpoint's RAM. Must see: a wrong signature ⇒ 401
 * at once, without waiting for the body; a `content-length` over the body limit ⇒ 413 at once.
 */
test("h1 — the signature is checked before the body is read; content-length over the limit ⇒ 413 without reading", async () => {
  const { request } = await import("node:http");
  const svc = service("Big", {
    async go() {
      return 1;
    },
  });
  const signed = await serve({
    services: [svc],
    port: 0,
    protocolMode: "REQUEST_RESPONSE",
    identityKeys: ["publickeyv1_3CPYZuphHAqFNsix9y91xhW8L6Cn9sTuL2tfTVDhZJdr"],
  });
  opened.push(signed);
  const open = await serve({ services: [svc], port: 0, protocolMode: "REQUEST_RESPONSE" });
  opened.push(open);
  // Declares a very large body, sends a few bytes and holds the connection: the endpoint must
  // answer at once.
  const status = (url: string, declared: number) =>
    new Promise<number>((resolve, reject) => {
      const u = new URL(`${url}/invoke/Big/go`);
      const req = request(
        {
          host: u.hostname,
          port: u.port,
          path: u.pathname,
          method: "POST",
          headers: { "content-type": "application/vnd.cairnflow.invocation.v1", "content-length": String(declared) },
        },
        (res) => {
          res.resume();
          resolve(res.statusCode ?? 0);
          req.destroy();
        },
      );
      req.on("error", () => {});
      req.write(Buffer.alloc(16));
      setTimeout(() => {
        reject(new Error("the endpoint waited for the body instead of answering"));
        req.destroy();
      }, 3_000);
    });
  assert.equal(await status(signed.url, 100 << 20), 401, "no signature");
  assert.equal(await status(open.url, 1 << 30), 413, "content-length over the limit");
});

/**
 * An endpoint without `identityKeys` accepts the journal, input, state
 * and `segment_urls` of **anyone** — the warning used to say only "source not
 * verified". Must see: the warning spells out the consequence (the handler runs on forged data;
 * no replay cache; segment URLs, which the caller chooses, fetched only with segmentOrigins) and
 * the fix.
 */
test("the warning without identityKeys says the handler runs on forged data, and what stays off", async () => {
  const { spawnSync } = await import("node:child_process");
  const src = new URL("../src/endpoint.ts", import.meta.url).href;
  const r = spawnSync(
    process.execPath,
    ["--input-type=module", "-e", `const m = await import(${JSON.stringify(src)}); m.warnUnverified("serve()");`],
    { encoding: "utf8" },
  );
  assert.match(r.stderr, /CAIRNFLOW_UNVERIFIED/);
  assert.match(r.stderr, /forged/, "says journal/input/state can be forged");
  assert.match(r.stderr, /segment_urls|segment URL/, "says segment URLs are only fetched with segmentOrigins");
  assert.match(r.stderr, /segmentOrigins/);
  assert.match(r.stderr, /replay cache stays off/);
  assert.match(r.stderr, /identityKeys/, "points at the fix");
});
