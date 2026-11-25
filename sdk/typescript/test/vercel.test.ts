// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * The Vercel AI SDK v7 adapter, without depending on the `ai` package: structural
 * types matching `ai@7`'s `LanguageModelV4Middleware` and `ChatTransport`. The
 * version that runs with the real `ai` package and the real runtime:
 * `ai-sdk.test.ts`.
 *
 * - `cairnMiddleware(ctx)`: the model call runs inside `ctx.run` (one durable step),
 *   wrapped in a `ctx.trace.span` carrying `model`/`input_tokens`/`output_tokens`
 *   (v4-style usage `{ total }`), and each part of the model stream is translated into
 *   a UI chunk and appended to the transient stream `ui`. Replay: the part already in the
 *   journal is replayed to the caller, the model is not called, nothing is re-emitted to `ui`.
 * - `cairnTools(ctx, tools)`: each tool `execute` is a `ctx.run` + span;
 *   input and result go to `ui`.
 * - `cairnChatTransport`: `sendMessages` sends to the handler and reads that
 *   invocation's `ui` through `readStream`; `reconnectToStream` reads again by the remembered id,
 *   and without one asks the object's `active`; cancel ⇒ `POST /invocations/{id}/cancel`;
 *   a `reset` in the middle of a step ⇒ `reset-step`.
 */
import assert from "node:assert/strict";
import * as http from "node:http";
import { after, test } from "node:test";
import { type Handler, runTurn } from "../src/index.ts";
import { eachField, Writer } from "../src/protobuf.ts";
import { decodeFrames, encodeFrame, type Frame, MT } from "../src/wire.ts";

const enc = new TextEncoder();
const dec = new TextDecoder();

// The new API through a type cast: the test compiles against code that does not have it yet.
type Part = Record<string, unknown> & { type: string };
interface Middleware {
  specificationVersion: string;
  wrapGenerate(o: { doGenerate: () => Promise<unknown>; params: unknown; model: { modelId: string } }): Promise<unknown>;
  wrapStream(o: {
    doStream: () => Promise<{ stream: ReadableStream<Part> }>;
    params: unknown;
    model: { modelId: string };
  }): Promise<{ stream: ReadableStream<Part> }>;
}
interface Transport {
  sendMessages(o: {
    chatId: string;
    messages: unknown[];
    abortSignal?: AbortSignal;
  }): Promise<ReadableStream<Part>>;
  reconnectToStream(o: { chatId: string }): Promise<ReadableStream<Part> | null>;
  messages(chatId: string): Promise<unknown[]>;
  approve(chatId: string, approvalId: string, answer: { approved: boolean; reason?: string }): Promise<void>;
}
interface Vercel {
  cairnMiddleware(ctx: unknown, opts?: { stream?: string }): Middleware;
  cairnTools<T extends Record<string, { execute?: (...a: never[]) => unknown }>>(ctx: unknown, tools: T): T;
  uiChunks(p: Part): Part[];
  cairnChatTransport(o: { baseUrl: string; service: string; handler: string }): Transport;
}
async function vercel(): Promise<Vercel> {
  return (await import("../src/index.ts")) as unknown as Vercel;
}

function start(knownEntries: number): Uint8Array {
  return encodeFrame(
    MT.START,
    0,
    new Writer().bytes(1, new Uint8Array(24).fill(3)).uint32(3, knownEntries).uint64(9, 42n).finish(),
  );
}
function inputEntry(v: unknown): Uint8Array {
  return encodeFrame(MT.INPUT, 0, new Writer().message(14, new Writer().bytes(1, enc.encode(JSON.stringify(v)))).finish());
}
function runEntry(name: string, cid: number): Uint8Array {
  return encodeFrame(MT.RUN, 0, new Writer().uint32(11, cid).string(12, name).finish());
}

/** The result turn 1 proposes (`ProposeRunCompletion`, field 14). */
function proposed(frames: readonly Frame[]): Uint8Array {
  const f = frames.find((f) => f.type === MT.PROPOSE_RUN_COMPLETION);
  assert.ok(f, "turn 1 must propose the ctx.run result");
  let v: Uint8Array = new Uint8Array();
  eachField(f.payload, (field, _w, r) => {
    if (field === 14) v = r.bytes();
    else if (field === 1) r.varint();
    else return false;
    return true;
  });
  return v;
}

/** Turn 2's journal: the runtime's ack for turn 1's `ctx.run`. */
function runCompletion(cid: number, value: Uint8Array): Uint8Array {
  return encodeFrame(
    MT.RUN_COMPLETION,
    0,
    new Writer().uint32(1, cid).message(5, new Writer().bytes(1, value)).finish(),
  );
}

function cat(...p: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(p.reduce((n, x) => n + x.length, 0));
  let at = 0;
  for (const x of p) {
    out.set(x, at);
    at += x.length;
  }
  return out;
}

/** `(stream name, payload)` of every `StreamAppend` record. */
function appended(frames: readonly Frame[]): [string, unknown][] {
  const out: [string, unknown][] = [];
  for (const f of frames.filter((f) => f.type === 0x000b)) {
    let name = "";
    eachField(f.payload, (field, _w, r) => {
      if (field === 1) name = dec.decode(r.bytes());
      else if (field === 3) {
        eachField(r.bytes(), (g, _w2, s) => {
          if (g === 4) out.push([name, JSON.parse(dec.decode(s.bytes()))]);
          else if (g <= 3) s.varint();
          else return false;
          return true;
        });
      } else if (field === 2) r.varint();
      else return false;
      return true;
    });
  }
  return out;
}

const USAGE = {
  inputTokens: { total: 12, noCache: 12, cacheRead: 0, cacheWrite: 0 },
  outputTokens: { total: 3, text: 3, reasoning: 0 },
};

/** The model stream (`LanguageModelV4StreamPart`). */
const PARTS: Part[] = [
  { type: "stream-start", warnings: [] },
  { type: "text-start", id: "t" },
  { type: "text-delta", id: "t", delta: "say " },
  { type: "text-delta", id: "t", delta: "hello" },
  { type: "text-end", id: "t" },
  { type: "finish", finishReason: { unified: "stop", raw: "stop" }, usage: USAGE },
];

/** UI chunks (`UIMessageChunk`) of `PARTS`. */
const CHUNKS: Part[] = [
  { type: "start-step" },
  { type: "text-start", id: "t" },
  { type: "text-delta", id: "t", delta: "say " },
  { type: "text-delta", id: "t", delta: "hello" },
  { type: "text-end", id: "t" },
  { type: "finish-step" },
];

function partsStream(parts: Part[]): ReadableStream<Part> {
  return new ReadableStream({
    start(c) {
      for (const p of parts) c.enqueue(p);
      c.close();
    },
  });
}

async function drain<T>(s: ReadableStream<T>): Promise<T[]> {
  const out: T[] = [];
  for await (const x of s as unknown as AsyncIterable<T>) out.push(x);
  return out;
}

test(
  "middleware wrapStream: ctx.run + span (model, v4 tokens) + UI chunks into the ui stream",
  async () => {
    const v = await vercel();
    let calls = 0;
    let seen: Part[] = [];
    let modelId = "m-1";
    const h: Handler = async (ctx) => {
      const mw = v.cairnMiddleware(ctx);
      const r = await mw.wrapStream({
        doStream: async () => {
          calls += 1;
          return { stream: partsStream(PARTS) };
        },
        params: {},
        model: { modelId },
      });
      seen = await drain(r.stream);
      return "ok";
    };
    // Turn 1 (REQUEST_RESPONSE): the model runs inside `ctx.run`, each part goes to
    // `ui`, then the turn stops waiting for the runtime's ack.
    const out = decodeFrames(await runTurn(h, cat(start(1), inputEntry(null)), 4));
    assert.equal(calls, 1);
    assert.ok(out.some((f) => f.type === MT.RUN), "the model call is a ctx.run");
    const ui = appended(out).filter(([n]) => n === "ui").map(([, p]) => p);
    assert.deepEqual(ui, CHUNKS);
    assert.equal(v.cairnMiddleware({ stream: () => ({}) }).specificationVersion, "v4");
    const t1 = appended(out).filter(([n]) => n === "$trace").map(([, p]) => p as Record<string, any>);
    assert.deepEqual(t1.map((t) => t.kind), ["start"]);
    // Turn 2: the result comes from the journal — the model does not run again, `ui` is not
    // re-emitted, the span closes with the model and tokens.
    const out2 = decodeFrames(
      await runTurn(
        h,
        cat(start(3), inputEntry(null), runEntry("model.stream", 1), runCompletion(1, proposed(out))),
        4,
      ),
    );
    assert.equal(calls, 1, "replay does not call the model");
    assert.deepEqual(seen, PARTS, "the caller sees every part");
    assert.deepEqual(appended(out2).filter(([n]) => n === "ui"), []);
    const t2 = appended(out2).filter(([n]) => n === "$trace").map(([, p]) => p as Record<string, any>);
    assert.deepEqual(t2.map((t) => t.kind), ["end"]);
    assert.equal(t2[0]?.span_id, t1[0]?.span_id);
    assert.deepEqual(
      [t2[0]?.attrs?.model, t2[0]?.attrs?.input_tokens, t2[0]?.attrs?.output_tokens],
      ["m-1", 12, 3],
    );
    // A new deployment changes the model: the step name does not carry the model id, so the
    // journal of m-1 still matches (no 570), and nothing is called again.
    modelId = "m-2";
    const out3 = decodeFrames(
      await runTurn(
        h,
        cat(start(3), inputEntry(null), runEntry("model.stream", 1), runCompletion(1, proposed(out))),
        4,
      ),
    );
    assert.deepEqual(out3.filter((f) => f.type !== MT.STREAM_APPEND).map((f) => f.type), [MT.OUTPUT, MT.END]);
    assert.equal(calls, 1, "changing the model does not call a durable step again");
  },
);

test(
  "cairnTools: execute runs inside ctx.run, the result is unchanged; input and result go to ui",
  async () => {
    const v = await vercel();
    let ran = 0;
    let got: unknown;
    const h: Handler = async (ctx) => {
      const tools = v.cairnTools(ctx, {
        search: { execute: async (q: { q: string }) => ((ran += 1), `result ${q.q}`) },
      });
      got = await (tools.search.execute as (q: { q: string }, o: unknown) => Promise<string>)(
        { q: "a" },
        { toolCallId: "call-1" },
      );
      return "ok";
    };
    const out = decodeFrames(await runTurn(h, cat(start(1), inputEntry(null)), 4));
    assert.equal(ran, 1);
    assert.ok(out.some((f) => f.type === MT.RUN));
    assert.deepEqual(
      appended(out).filter(([n]) => n === "ui").map(([, p]) => p),
      [
        { type: "tool-input-available", toolCallId: "call-1", toolName: "search", input: { q: "a" } },
        { type: "tool-output-available", toolCallId: "call-1", output: "result a" },
      ],
    );
    const out2 = decodeFrames(
      await runTurn(
        h,
        cat(start(3), inputEntry(null), runEntry("tool:search", 1), runCompletion(1, proposed(out))),
        4,
      ),
    );
    assert.equal(ran, 1, "replay does not run the tool again");
    assert.equal(got, "result a");
    assert.deepEqual(appended(out2).filter(([n]) => n === "ui"), [], "replay does not re-emit to ui");
  },
);

test("uiChunks: the tool parts of the model stream become tool UI chunks", async () => {
  const v = await vercel();
  assert.deepEqual(v.uiChunks({ type: "tool-input-start", id: "c", toolName: "search" }), [
    { type: "tool-input-start", toolCallId: "c", toolName: "search" },
  ]);
  assert.deepEqual(v.uiChunks({ type: "tool-input-delta", id: "c", delta: '{"q"' }), [
    { type: "tool-input-delta", toolCallId: "c", inputTextDelta: '{"q"' },
  ]);
  assert.deepEqual(v.uiChunks({ type: "tool-input-end", id: "c" }), []);
  assert.deepEqual(
    v.uiChunks({ type: "tool-call", toolCallId: "c", toolName: "search", input: '{"q":"a"}' }),
    [{ type: "tool-input-available", toolCallId: "c", toolName: "search", input: { q: "a" } }],
  );
  assert.deepEqual(v.uiChunks({ type: "response-metadata", id: "r" }), []);
  assert.deepEqual(v.uiChunks({ type: "error", error: "failed" }), [{ type: "error", errorText: "failed" }]);
});

const servers: http.Server[] = [];
after(() => {
  for (const s of servers) s.close();
});

/** SSE body of a sequence of records, offsets from `from`. */
function sse(records: Part[], from = 0): string {
  return records
    .map((p, i) => `event: record\nid: ${from + i}\ndata: ${JSON.stringify({ offset: from + i, durable: false, data: p })}\n\n`)
    .join("");
}

async function listen(s: http.Server): Promise<string> {
  servers.push(s);
  await new Promise<void>((r) => s.listen(0, "127.0.0.1", () => r()));
  return `http://127.0.0.1:${(s.address() as { port: number }).port}`;
}

test(
  "transport: sendMessages calls /send (last message only), reads the id's ui stream; reconnect reads again, a lost id asks active; cancel ⇒ cooperative cancel",
  async () => {
    const ID = "cd".repeat(24);
    const calls: string[] = [];
    const bodies: unknown[] = [];
    let active: string | null = null;
    const s = http.createServer((req, res) => {
      calls.push(`${req.method} ${req.url}`);
      let body = "";
      req.on("data", (c: Buffer) => (body += c.toString()));
      req.on("end", () => {
        if (req.method === "POST" && req.url?.startsWith("/invoke/Chat/send/send")) {
          bodies.push(JSON.parse(body));
          res.writeHead(202, { "content-type": "application/json" });
          res.end(JSON.stringify({ status: "waiting", id: ID }));
        } else if (req.method === "POST" && req.url?.startsWith("/invoke/Chat/active?key=")) {
          res.writeHead(200, { "content-type": "application/json" });
          res.end(JSON.stringify({ status: "completed", id: "ef".repeat(24), output: JSON.stringify(active) }));
        } else if (req.method === "GET" && req.url?.startsWith(`/invocations/${ID}/streams/ui`)) {
          res.writeHead(200, { "content-type": "text/event-stream" });
          res.write(sse(CHUNKS));
          res.end(`event: end\ndata: {"next":${CHUNKS.length}}\n\n`);
        } else if (req.method === "POST" && req.url === `/invocations/${ID}/cancel?mode=signal`) {
          res.writeHead(200, { "content-type": "application/json" });
          res.end("{}");
        } else {
          res.writeHead(404).end();
        }
      });
    });
    const baseUrl = await listen(s);
    const v = await vercel();
    const t = v.cairnChatTransport({ baseUrl, service: "Chat", handler: "send" });
    const user = { id: "u1", role: "user", parts: [{ type: "text", text: "hello" }] };
    const chunks = await drain(await t.sendMessages({ chatId: "c1", messages: [{ id: "u0" }, user] }));
    assert.deepEqual(chunks[0], { type: "start", messageId: ID, messageMetadata: { cairnInvocationId: ID } });
    assert.equal(chunks.at(-1)?.type, "finish");
    assert.deepEqual(chunks.slice(1, -1), CHUNKS);
    assert.ok(calls[0]!.includes("key=c1"), `VO Chat by chatId: ${calls}`);
    assert.deepEqual(bodies[0], { chatId: "c1", trigger: "submit-message", message: user, after: "u0" }, "only the last message is sent (with the id of the one before it)");
    const again = await t.reconnectToStream({ chatId: "c1" });
    assert.ok(again !== null);
    assert.equal((await drain(again!)).filter((c) => c.type === "text-delta").length, 2);
    // A new page (new transport, no remembered id): asks the object's `active`.
    const fresh = v.cairnChatTransport({ baseUrl, service: "Chat", handler: "send" });
    assert.equal(await fresh.reconnectToStream({ chatId: "c1" }), null, "no turn running");
    active = ID;
    const resumed = await fresh.reconnectToStream({ chatId: "c1" });
    assert.ok(resumed !== null, "active returns an id ⇒ read that id's stream");
    assert.equal((await drain(resumed!)).filter((c) => c.type === "text-delta").length, 2);
    const ac = new AbortController();
    const live = await t.sendMessages({ chatId: "c2", messages: [], abortSignal: ac.signal });
    ac.abort();
    await drain(live).catch(() => []);
    await new Promise((r) => setTimeout(r, 50));
    // Cooperative cancel: the running step gets the signal, the history keeps what was generated.
    assert.ok(calls.includes(`POST /invocations/${ID}/cancel?mode=signal`), `${calls}`);
  },
);

test(
  "transport: reset in the middle of a step ⇒ reset-step, the rerun step does not add a second step-start",
  async () => {
    const ID = "ab".repeat(24);
    let conn = 0;
    const s = http.createServer((req, res) => {
      if (req.method === "POST" && req.url?.startsWith("/invoke/Chat/send/send")) {
        req.resume();
        res.writeHead(202, { "content-type": "application/json" });
        res.end(JSON.stringify({ status: "waiting", id: ID }));
      } else if (req.method === "GET" && req.url?.startsWith(`/invocations/${ID}/streams/ui`)) {
        res.writeHead(200, { "content-type": "text/event-stream" });
        if (conn++ === 0) {
          // Step one done, step two half-way when the leader changes: the transient stream is lost.
          res.write(sse([...CHUNKS, { type: "start-step" }, { type: "text-start", id: "u" }]));
          res.write(`event: moved\ndata: {"leader":null}\n\n`);
          res.end(`event: reset\ndata: {}\n\n`);
        } else {
          const step2: Part[] = [
            { type: "start-step" },
            { type: "text-start", id: "u" },
            { type: "text-delta", id: "u", delta: "again" },
            { type: "text-end", id: "u" },
            { type: "finish-step" },
          ];
          res.write(sse(step2));
          res.end(`event: end\ndata: {"next":${step2.length}}\n\n`);
        }
      } else {
        res.writeHead(404).end();
      }
    });
    const baseUrl = await listen(s);
    const v = await vercel();
    const t = v.cairnChatTransport({ baseUrl, service: "Chat", handler: "send" });
    const chunks = await drain(await t.sendMessages({ chatId: "c1", messages: [] }));
    assert.deepEqual(
      chunks.map((c) => c.type),
      [
        "start",
        ...CHUNKS.map((c) => c.type),
        "start-step",
        "text-start",
        "reset-step",
        "text-start",
        "text-delta",
        "text-end",
        "finish-step",
        "finish",
      ],
    );
  },
);

test(
  "transport: superseded for an in-flight attempt ⇒ reset-step — the SDK died while the model was generating, the next attempt generates again",
  // Measured with a real GLM-5.3: kill -9 of the SDK app while generating ⇒ the runtime retries,
  // inserts `superseded{step, attempt}` and the new attempt regenerates the whole step. Without
  // `reset-step` the UI appends the dead attempt's partial text to the new attempt's text.
  async () => {
    const ID = "cd".repeat(24);
    const at = (p: Part, offset: number, step: number, attempt: number) =>
      `event: record\nid: ${offset}\ndata: ${JSON.stringify({ offset, durable: false, step, attempt, k: offset, data: p })}\n\n`;
    const s = http.createServer((req, res) => {
      if (req.method === "POST" && req.url?.startsWith("/invoke/Chat/send/send")) {
        req.resume();
        res.writeHead(202, { "content-type": "application/json" });
        res.end(JSON.stringify({ status: "waiting", id: ID }));
      } else if (req.method === "GET" && req.url?.startsWith(`/invocations/${ID}/streams/ui`)) {
        res.writeHead(200, { "content-type": "text/event-stream" });
        const dead: Part[] = [{ type: "start-step" }, { type: "text-start", id: "t" }, { type: "text-delta", id: "t", delta: "partial" }];
        const again: Part[] = [
          { type: "start-step" },
          { type: "text-start", id: "t" },
          { type: "text-delta", id: "t", delta: "again" },
          { type: "text-end", id: "t" },
          { type: "finish-step" },
        ];
        res.write(dead.map((p, i) => at(p, i, 3, 0)).join(""));
        res.write(`event: superseded\nid: 3\ndata: {"offset":3,"step":3,"attempt":0}\n\n`);
        res.write(again.map((p, i) => at(p, 4 + i, 3, 5)).join(""));
        res.end(`event: end\ndata: {"next":${4 + again.length}}\n\n`);
      } else {
        res.writeHead(404).end();
      }
    });
    const baseUrl = await listen(s);
    const v = await vercel();
    const t = v.cairnChatTransport({ baseUrl, service: "Chat", handler: "send" });
    const chunks = await drain(await t.sendMessages({ chatId: "c1", messages: [] }));
    assert.deepEqual(
      chunks.map((c) => c.type),
      ["start", "start-step", "text-start", "text-delta", "reset-step", "text-start", "text-delta", "text-end", "finish-step", "finish"],
    );
  },
);

test(
  "transport: stream gone after the runtime died (410 stream_expired on reconnect) ⇒ close quietly, no error",
  // RR, kill -9 of the runtime after the model step finished: the new process only replays, emits
  // nothing to `ui`, the invocation completes ⇒ the stream is gone. The UI already has everything
  // that turn emitted before dying; an error here would only make `useChat` report a failure for a
  // turn that is `completed`.
  async () => {
    const ID = "ef".repeat(24);
    let conn = 0;
    const s = http.createServer((req, res) => {
      if (req.method === "POST" && req.url?.startsWith("/invoke/Chat/send/send")) {
        req.resume();
        res.writeHead(202, { "content-type": "application/json" });
        res.end(JSON.stringify({ status: "waiting", id: ID }));
      } else if (req.method === "GET" && req.url?.startsWith(`/invocations/${ID}/streams/ui`)) {
        if (conn++ === 0) {
          res.writeHead(200, { "content-type": "text/event-stream" });
          res.write(sse(CHUNKS));
          setTimeout(() => res.socket?.destroy(), 20);
        } else {
          res.writeHead(410, { "content-type": "application/json" });
          res.end(JSON.stringify({ error: "stream gone", code: "stream_expired" }));
        }
      } else {
        res.writeHead(404).end();
      }
    });
    const baseUrl = await listen(s);
    const v = await vercel();
    const t = v.cairnChatTransport({ baseUrl, service: "Chat", handler: "send" });
    const chunks = await drain(await t.sendMessages({ chatId: "c1", messages: [] }));
    assert.deepEqual(chunks.map((c) => c.type), ["start", ...CHUNKS.map((c) => c.type), "finish"]);
  },
);

test(
  "transport in a cluster: 503 + leader ⇒ resent exactly once at the leader (same Idempotency-Key); messages/active/approve follow the leader too",
  // Measured with GLM-5.3 on a 3-node cluster: any chat whose range is not held by the
  // client's node gets `503 {leader}` on `/send` and `useChat` reports an error — behind
  // a load balancer, most messages. The contract (`docs/admin-api.md`, error contract): retry
  // once at `leader`; an error carrying an `id` is only resent with an `Idempotency-Key`.
  async () => {
    const ID = "12".repeat(24);
    const keys: (string | undefined)[] = [];
    const json = (res: http.ServerResponse, status: number, v: unknown) => {
      res.writeHead(status, { "content-type": "application/json" });
      res.end(JSON.stringify(v));
    };
    const leader = http.createServer((req, res) => {
      req.resume();
      const u = req.url ?? "";
      if (u.startsWith("/invoke/Chat/send/send")) {
        keys.push(req.headers["idempotency-key"] as string | undefined);
        json(res, 202, { status: "waiting", id: ID });
      } else if (u.startsWith("/invoke/Chat/messages")) json(res, 200, { status: "completed", id: "x", output: JSON.stringify([{ id: "m" }]) });
      else if (u.startsWith("/invoke/Chat/active")) json(res, 200, { status: "completed", id: "x", output: JSON.stringify(ID) });
      else if (u.startsWith("/promises/Chat/")) json(res, 200, {});
      else if (u.startsWith(`/invocations/${ID}/streams/ui`)) {
        res.writeHead(200, { "content-type": "text/event-stream" });
        res.end(`${sse(CHUNKS)}event: end\ndata: {"next":${CHUNKS.length}}\n\n`);
      } else res.writeHead(404).end();
    });
    const leaderUrl = await listen(leader);
    const follower = http.createServer((req, res) => {
      req.resume();
      if ((req.url ?? "").startsWith("/invoke/Chat/send/send")) keys.push(req.headers["idempotency-key"] as string | undefined);
      json(res, 503, { error: "not the leader", leader: leaderUrl, id: ID });
    });
    const baseUrl = await listen(follower);
    const v = await vercel();
    const t = v.cairnChatTransport({ baseUrl, service: "Chat", handler: "send" });
    const chunks = await drain(await t.sendMessages({ chatId: "c1", messages: [{ id: "u", role: "user", parts: [] }] }));
    assert.deepEqual(chunks.map((c) => c.type), ["start", ...CHUNKS.map((c) => c.type), "finish"]);
    assert.equal(keys.length, 2, "follower then leader, exactly one retry");
    assert.ok(keys[0] !== undefined && keys[0].length > 0 && keys[0] === keys[1], `same Idempotency-Key: ${keys}`);
    assert.deepEqual(await t.messages("c1"), [{ id: "m" }]);
    await t.approve("c1", "approve-x", { approved: true });
    const fresh = v.cairnChatTransport({ baseUrl, service: "Chat", handler: "send" });
    assert.ok((await fresh.reconnectToStream({ chatId: "c1" })) !== null, "active via leader");
  },
);

test(
  "middleware wrapStream: an `error` part mid-stream is the step's error — transient ⇒ the turn fails to be retried, isRetryable: false ⇒ a failed result (RunFailure) goes into the journal",
  async () => {
    // `@ai-sdk/openai-compatible` reports overload/429 mid-stream with an
    // `{type: "error"}` part instead of throwing. The old bug: the closure read on, and the step's
    // result — truncated text, `finishReason: error` — went into the journal like a successful
    // call, and the turn was never retried.
    const v = await vercel();
    const turnWith = async (error: unknown) => {
      const h: Handler = async (ctx) => {
        const mw = v.cairnMiddleware(ctx);
        const r = await mw.wrapStream({
          doStream: async () => ({
            stream: partsStream([
              { type: "stream-start", warnings: [] },
              { type: "text-start", id: "t" },
              { type: "text-delta", id: "t", delta: "say " },
              { type: "error", error },
              { type: "finish", finishReason: { unified: "error", raw: "error" }, usage: USAGE },
            ]),
          }),
          params: {},
          model: { modelId: "m-1" },
        });
        await drain(r.stream);
        return "ok";
      };
      return decodeFrames(await runTurn(h, cat(start(1), inputEntry(null)), 4));
    };
    const transient = await turnWith({ message: "overloaded", statusCode: 529 });
    const kinds = transient.filter((f) => f.type !== MT.STREAM_APPEND).map((f) => f.type);
    assert.ok(!kinds.includes(MT.PROPOSE_RUN_COMPLETION), `a transient error went into the journal as a result: ${kinds}`);
    assert.equal(kinds.at(-1), MT.ERROR, `the turn must fail to be retried: ${kinds}`);

    const fatal = await turnWith({ message: "prompt too long", statusCode: 400, isRetryable: false });
    const fk = fatal.filter((f) => f.type !== MT.STREAM_APPEND).map((f) => f.type);
    assert.ok(fk.includes(MT.PROPOSE_RUN_COMPLETION), `permanent error: the failed result must be durable: ${fk}`);
    let failure = false;
    const f = fatal.find((x) => x.type === MT.PROPOSE_RUN_COMPLETION)!;
    eachField(f.payload, (field, w, r) => {
      if (field === 15) failure = true;
      r.skip(w);
      return true;
    });
    assert.ok(failure, "the proposal must be a failed result (field 15), not a value");
    const ui = appended(fatal).filter(([n]) => n === "ui").map(([, p]) => p as Part);
    assert.ok(ui.some((c) => c.type === "error"), `the UI must receive an error chunk: ${JSON.stringify(ui)}`);
  },
);

/**
 * `cairnTools` with an approval step writes the key `pending-approval/<id>` before waiting on
 * `ctx.promise(id).get()`, and used to delete it only when `get()` returned. A cancel (the Stop
 * button: CANCEL ⇒ 409) or a rejected promise ⇒ `get()` throws `TerminalError` ⇒ the key stayed
 * forever ⇒ `messages` appended a fake approval message, and the user's next message hit
 * `STALE_HISTORY`.
 *
 * Must see: a rejected promise ⇒ the journal has a `ClearState` for exactly the key that was
 * `SetState`.
 */
test("approval cancelled/rejected — the pending-approval key is still cleared", async () => {
  const { cairnTools } = await import("../src/vercel.ts");
  const { driveRR, frame, Ledger, Runtime, describeJournal } = await import("./parallel-sim.ts");
  const h: Handler = async (ctx) => {
    const tools = cairnTools(ctx, { refund: { execute: async () => "refunded" } }, { approve: ["refund"] });
    try {
      await (tools.refund.execute as (i: unknown, o: unknown) => Promise<unknown>)({}, { toolCallId: "tc1" });
    } catch {
      return "stopped";
    }
    return "done";
  };
  const ledger = new Ledger();
  const rt = new Runtime(ledger);
  rt.onCommand = (f, id) => {
    if (f.type !== MT.GET_PROMISE || id === null) return;
    // The user presses Stop: the promise ends with a terminal error.
    rt.queueCompletion(
      id,
      frame(MT.GET_PROMISE_COMPLETION, new Writer().uint32(1, id).message(6, new Writer().uint32(1, 409).string(2, "cancelled")).finish()),
    );
  };
  const d = await driveRR(h, ledger, () => 0.99, rt);
  assert.deepEqual(ledger.violations, [], describeJournal(rt.journal));
  assert.equal(d.output, "stopped");
  const keyOf = (f: Frame) => {
    let k = "";
    eachField(f.payload, (field, wire, r) => {
      if (field !== 1 || wire !== 2) return false;
      k = dec.decode(r.bytes());
      return true;
    });
    return k;
  };
  const set = rt.journal.filter((f) => f.type === MT.SET_STATE).map(keyOf);
  const cleared = rt.journal.filter((f) => f.type === MT.CLEAR_STATE).map(keyOf);
  assert.deepEqual(set, ["pending-approval/approve-tc1"]);
  assert.deepEqual(cleared, ["pending-approval/approve-tc1"], describeJournal(rt.journal));
});

/**
 * `cairnChatTransport.ask` used to treat `202 waiting` (cold endpoint, the turn
 * not finished within `/invoke`'s wait limit) as `null` ⇒ `messages()` returned `[]` ⇒ `useChat`'s
 * next message carried `after: null` ⇒ `STALE_HISTORY`. Must see: a `202` with an id
 * ⇒ attach by id until it completes.
 *
 * `approve()` used to ignore `completed: false` — a second tab answering differently from
 * the first still saw success. Must see: a promise that already has a **different** decision ⇒
 * `approve` throws; the same decision (resent) ⇒ quiet.
 */
test("messages() follows a 202 to the result; approve() throws when the promise already has a different decision", async () => {
  const ID = "34".repeat(24);
  const json = (res: http.ServerResponse, status: number, v: unknown) => {
    res.writeHead(status, { "content-type": "application/json" });
    res.end(JSON.stringify(v));
  };
  const server = http.createServer((req, res) => {
    req.resume();
    const u = req.url ?? "";
    if (u.startsWith("/invoke/Chat/messages")) json(res, 202, { status: "waiting", id: ID });
    else if (u.startsWith(`/invocations/${ID}/attach`)) json(res, 200, { status: "completed", id: ID, output: JSON.stringify([{ id: "m1" }]) });
    else if (req.method === "POST" && u.includes("/promises/Chat/")) json(res, 200, { completed: false });
    else if (req.method === "GET" && u.includes("/promises/Chat/")) {
      json(res, 200, { service: "Chat", key: "c1", name: "approve-x", completed: true, value: JSON.stringify({ approved: false }) });
    } else res.writeHead(404).end();
  });
  const baseUrl = await listen(server);
  const v = await vercel();
  const t = v.cairnChatTransport({ baseUrl, service: "Chat", handler: "send" });
  assert.deepEqual(await t.messages("c1"), [{ id: "m1" }]);
  await assert.rejects(t.approve("c1", "approve-x", { approved: true }), /already/);
  await t.approve("c1", "approve-x", { approved: false });
});

/**
 * The chunk stream of `cairnChatTransport` had no `cancel()` — a consumer cancelling the stream
 * without going through `abortSignal` kept the SSE connection (and the node's `TenantSlot`) alive
 * until the next record; for a turn waiting on approval: hours. Must see: `reader.cancel()` closes
 * the SSE connection at once.
 */
test("cancelling the chunk stream (reader.cancel) closes the SSE connection", async () => {
  const ID = "56".repeat(24);
  let closed!: () => void;
  const gone = new Promise<void>((r) => {
    closed = r;
  });
  const server = http.createServer((req, res) => {
    req.resume();
    const u = req.url ?? "";
    if (u.startsWith("/invoke/Chat/send/send")) {
      res.writeHead(202, { "content-type": "application/json", "x-cairn-invocation-id": ID });
      res.end(JSON.stringify({ status: "waiting", id: ID }));
    } else if (u.startsWith(`/invocations/${ID}/streams/ui`)) {
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.write(`event: record\nid: 0\ndata: ${JSON.stringify({ offset: 0, durable: false, data: { type: "text-start", id: "t" } })}\n\n`);
      // No `end`: the turn is waiting on approval.
      res.on("close", () => closed());
    } else res.writeHead(404).end();
  });
  const baseUrl = await listen(server);
  const v = await vercel();
  const t = v.cairnChatTransport({ baseUrl, service: "Chat", handler: "send" });
  const stream = await t.sendMessages({ chatId: "c1", messages: [{ id: "u", role: "user", parts: [] }] });
  const reader = stream.getReader();
  await reader.read(); // start
  await reader.read(); // text-start
  await reader.cancel();
  const t0 = Date.now();
  await Promise.race([gone, new Promise((_, rej) => setTimeout(() => rej(new Error("SSE still open 2 s after cancel()")), 2000))]);
  assert.ok(Date.now() - t0 < 2000);
});
