// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * The Vercel AI SDK v7 adapter with the **real** `ai` package and the **real**
 * runtime. `vercel.test.ts` checks each piece with fake types; here `ai@7`'s
 * `streamText` runs in the handler, `ai@7`'s `AbstractChat` (what `useChat` wraps)
 * runs on the "browser" side, and between the two sits `cairnflow` — so a chunk in
 * the wrong order or of the wrong shape makes the AI SDK's own
 * `processUIMessageStream` throw.
 *
 * The model is a `MockLanguageModelV4`: the first call calls a tool, once it sees the
 * tool result it answers. The answer depends on the prompt, not on the call count —
 * replay does not call the model, so the number of calls is what the test counts.
 *
 * Two SDK endpoints, one per mode (`BIDI_STREAM`, `REQUEST_RESPONSE`): RR replays
 * the whole handler every turn, i.e. `streamText` runs again over the part from the
 * journal — wherever a `Date` or an `Error` through JSON breaks, it breaks here.
 *
 * Needs a built `cairnflow` binary: `cargo build --bin cairnflow`.
 */

import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, test } from "node:test";
import { fileURLToPath } from "node:url";
import {
  AbstractChat,
  type ChatState,
  type ChatStatus,
  convertToModelMessages,
  stepCountIs,
  streamText,
  tool,
  type UIMessage,
  wrapLanguageModel,
} from "ai";
import { MockLanguageModelV4 } from "ai/test";
import { APICallError } from "@ai-sdk/provider";
import { z } from "zod";
import {
  type Endpoint,
  type ProtocolMode,
  STALE_HISTORY,
  STALE_HISTORY_CODE,
  serve,
  service,
  type ServiceDefinition,
  TerminalError,
} from "../src/index.ts";

// The chat adapter API through a type cast: the test compiles against code that does not have it yet.
type Sdk = typeof import("../src/index.ts");
interface Adapter {
  chatObject(
    name: string,
    run: (ctx: any, chat: { messages: UIMessage[]; onError: (e: { error: unknown }) => void }) => unknown,
  ): ServiceDefinition;
  cairnMiddleware(ctx: unknown): any;
  cairnTools<T>(ctx: unknown, tools: T, opts?: { approve?: string[] }): T;
  cairnChatTransport(o: { baseUrl: string; service: string }): any;
}
async function adapter(): Promise<Adapter> {
  return (await import("../src/index.ts")) as unknown as Sdk & Adapter;
}

const here = fileURLToPath(new URL(".", import.meta.url));
const binary = join(here, "..", "..", "..", "target", "debug", "cairnflow");

/** Counts real calls — replay must not increment them. */
const ctl = { modelCalls: 0, weather: 0, refunds: 0 };

const USAGE = {
  inputTokens: { total: 10, noCache: 10, cacheRead: 0, cacheWrite: 0 },
  outputTokens: { total: 4, text: 4, reasoning: 0 },
};

/** First time: call `tool`; once the prompt has the tool result: answer in text. */
function model(toolName: string, input: unknown) {
  return new MockLanguageModelV4({
    modelId: "mock-1",
    doStream: async ({ prompt }) => {
      ctl.modelCalls++;
      const answered = prompt.some((m) => m.role === "tool");
      const parts: any[] = [
        { type: "stream-start", warnings: [] },
        { type: "response-metadata", id: `r${ctl.modelCalls}`, modelId: "mock-1", timestamp: new Date(0) },
      ];
      if (answered) {
        parts.push(
          { type: "text-start", id: "t" },
          { type: "text-delta", id: "t", delta: "It is " },
          { type: "text-delta", id: "t", delta: "sunny." },
          { type: "text-end", id: "t" },
          { type: "finish", finishReason: { unified: "stop", raw: "stop" }, usage: USAGE },
        );
      } else {
        const json = JSON.stringify(input);
        parts.push(
          { type: "tool-input-start", id: "call-1", toolName },
          { type: "tool-input-delta", id: "call-1", delta: json },
          { type: "tool-input-end", id: "call-1" },
          { type: "tool-call", toolCallId: "call-1", toolName, input: json },
          { type: "finish", finishReason: { unified: "tool-calls", raw: "tool_calls" }, usage: USAGE },
        );
      }
      return {
        stream: new ReadableStream({
          start(c) {
            for (const p of parts) c.enqueue(p);
            c.close();
          },
        }),
      };
    },
  });
}

const tools = {
  weather: tool({
    description: "weather",
    inputSchema: z.object({ city: z.string() }),
    execute: async ({ city }: { city: string }) => {
      ctl.weather++;
      return { city, sky: "sunny" };
    },
  }),
  refund: tool({
    description: "refund",
    inputSchema: z.object({ amount: z.number() }),
    execute: async ({ amount }: { amount: number }) => {
      ctl.refunds++;
      return { refunded: amount };
    },
  }),
};

/** Faults injected into the `Faulty*` services; each counter is the number of failures left. */
const faults = { retryable: 0, fatal: 0, toolTransient: 0, toolTerminal: 0 };
const fctl = { model: 0, lookup: 0 };

/** First calls `lookup`; once it sees the tool result it answers from it: "done" or "error: …". */
const faultyModel = () =>
  new MockLanguageModelV4({
    modelId: "mock-f",
    doStream: async ({ prompt }) => {
      fctl.model++;
      if (faults.retryable > 0) {
        faults.retryable--;
        throw new APICallError({ message: "429 overloaded", url: "x", requestBodyValues: {}, statusCode: 429, isRetryable: true });
      }
      if (faults.fatal > 0) {
        faults.fatal--;
        throw new APICallError({ message: "400 prompt too long", url: "x", requestBodyValues: {}, statusCode: 400, isRetryable: false });
      }
      const result = prompt.flatMap((m: any) => (m.role === "tool" ? m.content : [])).at(-1) as any;
      const parts: any[] = [{ type: "stream-start", warnings: [] }];
      if (result !== undefined) {
        const said = String(result.output?.type ?? "").startsWith("error") ? `error: ${result.output.value}` : "done";
        parts.push({ type: "text-start", id: "t" }, { type: "text-delta", id: "t", delta: said }, { type: "text-end", id: "t" });
      } else {
        parts.push({ type: "tool-call", toolCallId: "c1", toolName: "lookup", input: JSON.stringify({ q: "a" }) });
      }
      parts.push({ type: "finish", finishReason: { unified: result ? "stop" : "tool-calls", raw: "x" }, usage: USAGE });
      return { stream: new ReadableStream({ start(c) { for (const p of parts) c.enqueue(p); c.close(); } }) };
    },
  });

const lookup = tool({
  description: "lookup",
  inputSchema: z.object({ q: z.string() }),
  execute: async () => {
    fctl.lookup++;
    if (faults.toolTransient > 0) {
      faults.toolTransient--;
      throw new Error("flaky network");
    }
    if (faults.toolTerminal > 0) {
      faults.toolTerminal--;
      throw new TerminalError("not found");
    }
    return { ok: 1 };
  },
});

/** Cooperative cancellation: the `ctx.run` closure sees the cancel signal, the next step does not run. */
const cancelProbe = { ranA: 0, aborted: 0, ranB: 0 };
/** A slow model: text every 150 ms, twelve chunks, then calls `lookup`; honours `abortSignal`. */
const slow = { calls: 0, aborted: 0, tool: 0 };
const slowModel = () =>
  new MockLanguageModelV4({
    modelId: "mock-s",
    doStream: async ({ prompt, abortSignal }) => {
      slow.calls++;
      const answered = prompt.some((m) => m.role === "tool");
      let timer: ReturnType<typeof setTimeout> | undefined;
      let open = true;
      return {
        stream: new ReadableStream({
          start(c) {
            c.enqueue({ type: "stream-start", warnings: [] });
            if (answered) {
              c.enqueue({ type: "text-start", id: "t" });
              c.enqueue({ type: "text-delta", id: "t", delta: "done" });
              c.enqueue({ type: "text-end", id: "t" });
              c.enqueue({ type: "finish", finishReason: { unified: "stop", raw: "stop" }, usage: USAGE });
              c.close();
              return;
            }
            abortSignal?.addEventListener(
              "abort",
              () => {
                if (!open) return;
                open = false;
                clearTimeout(timer);
                slow.aborted++;
                c.error(new DOMException("aborted", "AbortError"));
              },
              { once: true },
            );
            c.enqueue({ type: "text-start", id: "t" });
            let i = 0;
            const tick = () => {
              if (!open) return;
              if (i++ < 12) {
                c.enqueue({ type: "text-delta", id: "t", delta: `chunk ${i} ` });
                timer = setTimeout(tick, 150);
                return;
              }
              open = false;
              c.enqueue({ type: "text-end", id: "t" });
              c.enqueue({ type: "tool-call", toolCallId: "s1", toolName: "lookup", input: JSON.stringify({ q: "a" }) });
              c.enqueue({ type: "finish", finishReason: { unified: "tool-calls", raw: "x" }, usage: USAGE });
              c.close();
            };
            tick();
          },
        }),
      };
    },
  });
const slowLookup = tool({
  description: "lookup",
  inputSchema: z.object({ q: z.string() }),
  execute: async () => {
    slow.tool++;
    return { ok: 1 };
  },
});

/** The text of a message. */
function textOf(m: UIMessage): string {
  return m.parts.filter((p) => p.type === "text").map((p: any) => p.text).join("");
}

/** JSON with sorted keys; drops the id of text parts (the UI takes the provider's id, the history generates its own). */
function canon(x: unknown): string {
  const norm = (v: any): any =>
    Array.isArray(v)
      ? v.map(norm)
      : v !== null && typeof v === "object"
        ? Object.fromEntries(
            Object.keys(v)
              .filter((k) => !(k === "id" && (v.type === "text" || v.type === "reasoning")))
              .sort()
              .map((k) => [k, norm(v[k])]),
          )
        : v;
  return JSON.stringify(norm(x));
}

async function chatService(a: Adapter, name: string, toolName: "weather" | "refund"): Promise<ServiceDefinition> {
  return a.chatObject(name, async (ctx, chat) =>
    streamText({
      model: wrapLanguageModel({
        model: model(toolName, toolName === "weather" ? { city: "Hanoi" } : { amount: 5 }),
        middleware: a.cairnMiddleware(ctx),
      }),
      tools: a.cairnTools(ctx, { [toolName]: tools[toolName] }, { approve: ["refund"] }),
      messages: await convertToModelMessages(chat.messages),
      stopWhen: stepCountIs(3),
      onError: chat.onError,
    }),
  );
}

/** `useChat` without React: `AbstractChat` over an array. */
class TestChat extends AbstractChat<UIMessage> {
  constructor(id: string, transport: any) {
    const state: ChatState<UIMessage> = {
      status: "ready" as ChatStatus,
      error: undefined,
      messages: [],
      pushMessage: (m) => void state.messages.push(m),
      popMessage: () => void state.messages.pop(),
      replaceMessage: (i, m) => void (state.messages[i] = m),
      snapshot: (t) => structuredClone(t),
    };
    super({ id, transport, state });
  }
}

let runtime: ChildProcess | undefined;
let url = "";
let dataDir = "";
const sdks: Endpoint[] = [];

before(async () => {
  if (!existsSync(binary)) throw new Error(`missing binary ${binary} — run \`cargo build --bin cairnflow\` first`);
  const a = await adapter();
  dataDir = mkdtempSync(join(tmpdir(), "cairn-ai-sdk-"));
  runtime = spawn(binary, ["--data-dir", dataDir, "--port", "0", "--tick-interval-ms", "50", "--no-auth"], {
    stdio: "pipe",
  });
  runtime.stderr!.resume();
  url = await new Promise<string>((resolve, reject) => {
    let buf = "";
    const timer = setTimeout(() => reject(new Error(`runtime did not come up:\n${buf}`)), 20_000);
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
    await new Promise((r) => setTimeout(r, 20));
  }
  for (const mode of ["BIDI_STREAM", "REQUEST_RESPONSE"] as ProtocolMode[]) {
    const tag = mode === "BIDI_STREAM" ? "Bidi" : "Rr";
    const broken = a.chatObject(`Broken${tag}`, async () => {
      throw new TerminalError("invalid request", 400);
    });
    const faulty = a.chatObject(`Faulty${tag}`, async (ctx, chat) =>
      streamText({
        model: wrapLanguageModel({ model: faultyModel(), middleware: a.cairnMiddleware(ctx) }),
        tools: a.cairnTools(ctx, { lookup }),
        messages: await convertToModelMessages(chat.messages),
        stopWhen: stepCountIs(3),
        onError: chat.onError,
      }),
    );
    const slowChat = a.chatObject(`Slow${tag}`, async (ctx, chat) =>
      streamText({
        model: wrapLanguageModel({ model: slowModel(), middleware: a.cairnMiddleware(ctx) }),
        tools: a.cairnTools(ctx, { lookup: slowLookup }),
        messages: await convertToModelMessages(chat.messages),
        stopWhen: stepCountIs(3),
        onError: chat.onError,
      }),
    );
    // The cancellable-run API through a type cast: `ctx.run(name, fn, { cancellable })`, the closure receives an `AbortSignal`.
    const cancellable = service(`Cancel${tag}`, {
      async work(ctx: any) {
        await ctx.run(
          "a",
          (signal?: AbortSignal) =>
            new Promise((res, rej) => {
              cancelProbe.ranA++;
              const t = setTimeout(() => res("a"), 1500);
              signal?.addEventListener("abort", () => {
                clearTimeout(t);
                cancelProbe.aborted++;
                rej(new Error("closure interrupted"));
              });
            }),
          { cancellable: true },
        );
        await ctx.run("b", () => ++cancelProbe.ranB, { cancellable: true });
        return "done";
      },
    });
    const sdk = await serve({
      services: [
        await chatService(a, `Chat${tag}`, "weather"),
        await chatService(a, `Refund${tag}`, "refund"),
        broken,
        faulty,
        slowChat,
        cancellable,
      ],
      port: 0,
      protocolMode: mode,
    });
    sdks.push(sdk);
    const r = await fetch(`${url}/deployments`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ uri: sdk.url }),
    });
    assert.equal(r.status, 200, await r.text());
  }
});

after(async () => {
  runtime?.kill("SIGKILL");
  for (const s of sdks) await s.close();
  if (dataDir) rmSync(dataDir, { recursive: true, force: true });
});

async function waitFor<T>(what: string, f: () => T | undefined | Promise<T | undefined>, ms = 20_000): Promise<T> {
  for (const deadline = Date.now() + ms; ; ) {
    const v = await f();
    if (v !== undefined) return v;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 25));
  }
}

for (const tag of ["Bidi", "Rr"]) {
  test(
    `${tag}: chat with a tool — ai@7's Chat sees the tool and the text; the model is called exactly twice; history stored in the object; tokens summed on the span`,
    async () => {
      const a = await adapter();
      const before = { ...ctl };
      const transport = a.cairnChatTransport({ baseUrl: url, service: `Chat${tag}` });
      const chat = new TestChat(`c-${tag}`, transport);
      await chat.sendMessage({ text: "Weather in Hanoi?" });
      assert.equal(chat.error, undefined, String(chat.error));
      assert.equal(chat.messages.length, 2);
      const reply = chat.messages[1]!;
      assert.equal(reply.role, "assistant");
      const toolPart = reply.parts.find((p) => p.type === "tool-weather") as any;
      assert.ok(toolPart, `tool part: ${JSON.stringify(reply.parts)}`);
      assert.equal(toolPart.state, "output-available");
      assert.deepEqual(toolPart.input, { city: "Hanoi" });
      assert.deepEqual(toolPart.output, { city: "Hanoi", sky: "sunny" });
      const text = reply.parts.filter((p) => p.type === "text").map((p: any) => p.text).join("");
      assert.equal(text, "It is sunny.");
      assert.equal(ctl.modelCalls - before.modelCalls, 2, "each model step exactly one real call");
      assert.equal(ctl.weather - before.weather, 1, "the tool ran exactly once");

      // History in the object, the assistant message carries the invocation id — the same id as on the client.
      const saved = (await transport.messages(`c-${tag}`)) as UIMessage[];
      assert.deepEqual(saved.map((m) => m.role), ["user", "assistant"]);
      assert.equal(saved[1]!.id, reply.id);
      assert.equal(reply.id, (reply.metadata as any)?.cairnInvocationId);
      const spans = (await (await fetch(`${url}/invocations/${reply.id}/spans`)).json()) as any;
      assert.deepEqual(spans.tokens, { input: 20, output: 8 }, JSON.stringify(spans));

      // The second turn builds the history from state (the transport only sends the last message).
      await chat.sendMessage({ text: "Thanks" });
      assert.equal(chat.error, undefined, String(chat.error));
      const saved2 = (await transport.messages(`c-${tag}`)) as UIMessage[];
      assert.deepEqual(saved2.map((m) => m.role), ["user", "assistant", "user", "assistant"]);

      // Regenerate the last message: the object cuts the history at that message and stores the new one in its place.
      await chat.regenerate();
      assert.equal(chat.error, undefined, String(chat.error));
      const saved3 = (await transport.messages(`c-${tag}`)) as UIMessage[];
      assert.deepEqual(saved3.map((m) => m.role), ["user", "assistant", "user", "assistant"]);
      assert.notEqual(saved3[3]!.id, saved2[3]!.id);
      assert.equal(saved3[3]!.id, chat.lastMessage!.id);
    },
  );

  test(
    `${tag}: a turn with a tool — every span is closed, no ghost spans`,
    async () => {
      const a = await adapter();
      const chat = new TestChat(`spans-${tag}`, a.cairnChatTransport({ baseUrl: url, service: `Chat${tag}` }));
      await chat.sendMessage({ text: "Weather in Hanoi?" });
      assert.equal(chat.error, undefined, String(chat.error));
      const spans = (await (await fetch(`${url}/invocations/${chat.lastMessage!.id}/spans`)).json()) as any;
      const seen = spans.spans.map((s: any) => `${s.name}${s.unfinished ? " (unfinished)" : ""}`);
      assert.deepEqual(seen, ["llm mock-1", "tool weather", "llm mock-1"], JSON.stringify(spans));
    },
  );

  test(
    `${tag}: a tool needing approval — waits on ctx.promise; a new page reconnects through active; once approved the tool runs once`,
    async () => {
      const a = await adapter();
      const before = { ...ctl };
      const chatId = `r-${tag}`;
      const transport = a.cairnChatTransport({ baseUrl: url, service: `Refund${tag}` });
      const chat = new TestChat(chatId, transport);
      const sent = chat.sendMessage({ text: "Refund 5" });
      const asked = await waitFor("tool-approval-request", () => {
        const p = chat.lastMessage?.parts.find((p) => p.type === "tool-refund") as any;
        return p?.state === "approval-requested" ? p : undefined;
      });
      assert.equal(ctl.refunds - before.refunds, 0, "before approval the tool has not run");

      // Page reload: a new transport does not remember the id, it asks the object's `active`.
      const page2 = new TestChat(chatId, a.cairnChatTransport({ baseUrl: url, service: `Refund${tag}` }));
      const resumed = page2.resumeStream();
      await waitFor("the new page sees the approval request", () =>
        page2.lastMessage?.parts.some((p: any) => p.type === "tool-refund" && p.state === "approval-requested")
          ? true
          : undefined,
      );

      await transport.approve(chatId, asked.approval.id, { approved: true });
      await sent;
      await resumed;
      for (const c of [chat, page2]) {
        assert.equal(c.error, undefined, String(c.error));
        const p = c.lastMessage!.parts.find((p) => p.type === "tool-refund") as any;
        assert.equal(p.state, "output-available", JSON.stringify(c.lastMessage!.parts));
        assert.deepEqual(p.output, { refunded: 5 });
      }
      assert.equal(ctl.refunds - before.refunds, 1, "the tool runs exactly once after approval");
    },
  );

  test(
    `${tag}: a pending approval request is readable from the object's messages — not only on the transient stream`,
    async () => {
      // The transient stream only holds the running step: a turn suspended waiting
      // for approval stands still, so `ui` is released, and a leader change loses it.
      // Old bug: the approval request lived only on `ui`, and `messages` only had
      // finished turns — a page reload had nothing left to approve, while the
      // EXCLUSIVE `send` held the chat until an answer came.
      const a = await adapter();
      const chatId = `pend-${tag}`;
      const transport = a.cairnChatTransport({ baseUrl: url, service: `Refund${tag}` });
      const chat = new TestChat(chatId, transport);
      const sent = chat.sendMessage({ text: "Refund 5" });
      const asked = await waitFor("tool-approval-request", () => {
        const p = chat.lastMessage?.parts.find((p) => p.type === "tool-refund") as any;
        return p?.state === "approval-requested" ? p : undefined;
      });
      const pending = await waitFor("messages has a pending request", async () => {
        const ms = (await transport.messages(chatId)) as UIMessage[];
        const p = ms.at(-1)?.parts.find((p) => p.type === "tool-refund") as any;
        return p?.state === "approval-requested" ? p : undefined;
      });
      assert.equal(pending.approval?.id, asked.approval.id, JSON.stringify(pending));
      assert.deepEqual(pending.input, { amount: 5 }, JSON.stringify(pending));
      await transport.approve(chatId, asked.approval.id, { approved: true });
      await sent;
      const saved = (await transport.messages(chatId)) as UIMessage[];
      const after = saved.flatMap((m) => m.parts).filter((p) => p.type === "tool-refund") as any[];
      assert.deepEqual(after.map((p) => p.state), ["output-available"], JSON.stringify(saved));
    },
  );

  test(
    `${tag}: a running turn — messages has the user message just sent, before the pending assistant message`,
    async () => {
      // Old bug: `messages` was only written at the end of a turn — a page reloaded mid-turn
      // (waiting hours for approval) saw the approve button but not the question just sent.
      const a = await adapter();
      const chatId = `inp-${tag}`;
      const transport = a.cairnChatTransport({ baseUrl: url, service: `Refund${tag}` });
      const chat = new TestChat(chatId, transport);
      const sent = chat.sendMessage({ text: "Refund 5" });
      const asked = await waitFor("tool-approval-request", () => {
        const p = chat.lastMessage?.parts.find((p) => p.type === "tool-refund") as any;
        return p?.state === "approval-requested" ? p : undefined;
      });
      const ms = await waitFor("messages has a pending request", async () => {
        const ms = (await transport.messages(chatId)) as UIMessage[];
        return ms.at(-1)?.role === "assistant" ? ms : undefined;
      });
      assert.deepEqual(
        ms.map((m) => [m.role, m.id]),
        [
          ["user", chat.messages[0]!.id],
          ["assistant", chat.lastMessage!.id],
        ],
        JSON.stringify(ms),
      );
      assert.equal((ms[0]!.parts[0] as any).text, "Refund 5");
      await transport.approve(chatId, asked.approval.id, { approved: true });
      await sent;
      const saved = (await transport.messages(chatId)) as UIMessage[];
      assert.deepEqual(saved.map((m) => m.id), chat.messages.map((m) => m.id), "turn done: history matches the UI");
    },
  );

  test(
    `${tag}: a page with stale history sends a message — rejected, history unchanged; reload then send succeeds`,
    async () => {
      // Old bug: a stale tab (not having seen another tab's turn) sent on top of the newer
      // history — the model answered on top of turns the sender had never seen.
      const a = await adapter();
      const chatId = `stale-${tag}`;
      const transport = a.cairnChatTransport({ baseUrl: url, service: `Chat${tag}` });
      const pageA = new TestChat(chatId, transport);
      const pageB = new TestChat(chatId, transport);
      await pageA.sendMessage({ text: "Weather in Hanoi?" });
      assert.equal(pageA.error, undefined, String(pageA.error));
      await pageB.sendMessage({ text: "Thanks" });
      // The UI compares by machine code, not by wording.
      assert.ok(String(pageB.error?.message).startsWith(`${STALE_HISTORY_CODE}:`), String(pageB.error));
      assert.equal(pageB.error?.message, STALE_HISTORY);
      const saved = (await transport.messages(chatId)) as UIMessage[];
      assert.deepEqual(saved.map((m) => m.id), pageA.messages.map((m) => m.id), "the stale tab's message is not in the history");

      const reloaded = new TestChat(chatId, transport);
      reloaded.messages = saved;
      await reloaded.sendMessage({ text: "Thanks" });
      assert.equal(reloaded.error, undefined, String(reloaded.error));
      const after = (await transport.messages(chatId)) as UIMessage[];
      assert.deepEqual(after.map((m) => m.role), ["user", "assistant", "user", "assistant"]);
    },
  );
}

test(
  "approval denied — the tool does not run, the UI sees output-denied, the model receives { denied } and answers",
  async () => {
    const a = await adapter();
    const before = { ...ctl };
    const chatId = "deny-1";
    const transport = a.cairnChatTransport({ baseUrl: url, service: "RefundBidi" });
    const chat = new TestChat(chatId, transport);
    const sent = chat.sendMessage({ text: "Refund 5" });
    const asked = await waitFor("tool-approval-request", () => {
      const p = chat.lastMessage?.parts.find((p) => p.type === "tool-refund") as any;
      return p?.state === "approval-requested" ? p : undefined;
    });
    await transport.approve(chatId, asked.approval.id, { approved: false, reason: "invalid" });
    await sent;
    assert.equal(chat.error, undefined, String(chat.error));
    const parts = chat.lastMessage!.parts;
    const p = parts.find((p) => p.type === "tool-refund") as any;
    assert.equal(p.state, "output-denied", JSON.stringify(parts));
    assert.equal(ctl.refunds - before.refunds, 0, "a denied tool does not run");
    assert.ok(parts.some((p: any) => p.type === "text" && p.text === "It is sunny."), "the model still answers");
    const saved = (await transport.messages(chatId)) as UIMessage[];
    const sp = saved[1]!.parts.find((p) => p.type === "tool-refund") as any;
    assert.equal(sp?.state, "output-denied", "history stored as in the UI");
    assert.deepEqual(sp?.approval, { id: asked.approval.id, approved: false, reason: "invalid" });
  },
);

test(
  "regenerate from a user message keeps that message (as AbstractChat cuts on the client)",
  async () => {
    const a = await adapter();
    const transport = a.cairnChatTransport({ baseUrl: url, service: "ChatBidi" });
    const chat = new TestChat("regen-user", transport);
    await chat.sendMessage({ text: "Weather in Hanoi?" });
    const user = chat.messages[0]!;
    await chat.regenerate({ messageId: user.id });
    assert.equal(chat.error, undefined, String(chat.error));
    const saved = (await transport.messages("regen-user")) as UIMessage[];
    assert.deepEqual(saved.map((m) => m.role), ["user", "assistant"], JSON.stringify(saved.map((m) => m.role)));
    assert.equal(saved[0]!.id, user.id);
  },
);

for (const tag of ["Bidi", "Rr"]) {
  test(
    `${tag}: a terminally failed turn clears active — a page reload does not reconnect to the dead invocation`,
    async () => {
      const chatId = `broken-${tag}`;
      const send = await fetch(`${url}/invoke/Broken${tag}/send/send?key=${chatId}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ chatId, trigger: "submit-message", message: { id: "u1", role: "user", parts: [] } }),
      });
      const { id } = (await send.json()) as { id: string };
      await waitFor("invocation finished", async () => {
        const st = (await (await fetch(`${url}/invocations/${id}`)).json()) as { status: string };
        return st.status === "running" || st.status === "waiting" ? undefined : st.status;
      });
      const r = await fetch(`${url}/invoke/Broken${tag}/active?key=${chatId}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: "null",
      });
      assert.equal(JSON.parse(((await r.json()) as { output: string }).output), null);
    },
  );
}

for (const tag of ["Bidi", "Rr"]) {
  /** Sends one message to `Faulty${tag}`; returns the chat, the stored history, the invocation status. */
  const turnOf = async (chatId: string) => {
    const a = await adapter();
    const transport = a.cairnChatTransport({ baseUrl: url, service: `Faulty${tag}` });
    const chat = new TestChat(chatId, transport);
    await chat.sendMessage({ text: "look up a" });
    const saved = (await transport.messages(chatId)) as UIMessage[];
    const id = chat.lastMessage!.id;
    const inv = await waitFor("invocation done", async () => {
      const st = (await (await fetch(`${url}/invocations/${id}`)).json()) as { status: string };
      return st.status === "running" || st.status === "waiting" || st.status === "backing-off" ? undefined : st;
    });
    return { chat, saved, inv };
  };
  const sameAsSaved = (chat: TestChat, saved: UIMessage[]) =>
    assert.deepEqual(
      chat.messages.map((m) => m.parts.map(canon)),
      saved.map((m) => m.parts.map(canon)),
      "UI = stored history",
    );
  const skip = {};

  test(`${tag}: a transient tool error — the turn retries, the tool runs again, the model sees the successful result`, skip, async () => {
    const before = { ...fctl };
    faults.toolTransient = 1;
    const { chat, saved, inv } = await turnOf(`ft-${tag}`);
    assert.equal(inv.status, "completed");
    assert.equal(fctl.lookup - before.lookup, 2, "the failure + the retry");
    assert.equal(textOf(chat.lastMessage!), "done", "the model answers on the successful result, not on the error");
    const p = chat.lastMessage!.parts.find((p) => p.type === "tool-lookup") as any;
    assert.equal(p?.state, "output-available", JSON.stringify(chat.lastMessage!.parts));
    sameAsSaved(chat, saved);
  });

  test(`${tag}: a transient provider error (429) — the tool gets exactly its own result, no journal mismatch`, skip, async () => {
    const before = { ...fctl };
    faults.retryable = 1;
    const { chat, saved, inv } = await turnOf(`fr-${tag}`);
    assert.equal(inv.status, "completed");
    assert.equal(fctl.lookup - before.lookup, 1, "the tool ran exactly once");
    const p = chat.lastMessage!.parts.find((p) => p.type === "tool-lookup") as any;
    assert.deepEqual(p?.output, { ok: 1 }, JSON.stringify(chat.lastMessage!.parts));
    assert.equal(fctl.model - before.model, 3, "the failure + the tool step + the answer step");
    assert.equal(textOf(chat.lastMessage!), "done");
    sameAsSaved(chat, saved);
  });

  test(`${tag}: a provider 400 error — no retry, the UI sees the error`, skip, async () => {
    const before = { ...fctl };
    faults.fatal = 1;
    const { chat, inv } = await turnOf(`ff-${tag}`);
    assert.equal(inv.status, "completed", JSON.stringify(inv));
    assert.equal(fctl.model - before.model, 1, "a non-retryable error is not called again");
    assert.match(String(chat.error?.message), /400 prompt too long/);
  });

  test(`${tag}: a tool's TerminalError goes into the journal — the tool runs once, the history keeps errorText`, skip, async () => {
    const before = { ...fctl };
    faults.toolTerminal = 1;
    const { chat, saved, inv } = await turnOf(`fe-${tag}`);
    assert.equal(inv.status, "completed");
    assert.equal(fctl.lookup - before.lookup, 1, "replay does not rerun the failed tool");
    // The AI SDK hands the tool error to the model as `String(error)` ("TerminalError: …").
    assert.match(textOf(chat.lastMessage!), /^error: .*not found$/);
    const p = saved.at(-1)!.parts.find((p) => p.type === "tool-lookup") as any;
    assert.equal(p?.errorText, "not found", JSON.stringify(p));
    sameAsSaved(chat, saved);
  });

  test(`${tag}: the stored history keeps approval and output-denied as in the UI`, skip, async () => {
    const a = await adapter();
    for (const approved of [true, false]) {
      const chatId = `apv-${tag}-${approved}`;
      const transport = a.cairnChatTransport({ baseUrl: url, service: `Refund${tag}` });
      const chat = new TestChat(chatId, transport);
      const sent = chat.sendMessage({ text: "Refund 5" });
      const asked = await waitFor("tool-approval-request", () => {
        const p = chat.lastMessage?.parts.find((p) => p.type === "tool-refund") as any;
        return p?.state === "approval-requested" ? p : undefined;
      });
      await transport.approve(chatId, asked.approval.id, approved ? { approved } : { approved, reason: "invalid" });
      await sent;
      const saved = (await transport.messages(chatId)) as UIMessage[];
      const p = saved[1]!.parts.find((p) => p.type === "tool-refund") as any;
      assert.equal(p.state, approved ? "output-available" : "output-denied", JSON.stringify(p));
      assert.equal(p.approval?.approved, approved, JSON.stringify(p));
      sameAsSaved(chat, saved);
    }
  });
}

/** The final status of an invocation (no longer `running`/`suspended`). */
async function settled(id: string): Promise<any> {
  return waitFor(`invocation ${id} to stop`, async () => {
    const j = (await (await fetch(`${url}/invocations/${id}`)).json()) as any;
    return j.status === "completed" || j.status === "failed" || j.status === "paused" ? j : undefined;
  });
}

for (const tag of ["Bidi", "Rr"]) {
  test(`${tag}: cooperative cancellation in a cancellable ctx.run closure — the next step does not run; BIDI interrupts the closure at once`, async () => {
    Object.assign(cancelProbe, { ranA: 0, aborted: 0, ranB: 0 });
    const r = await fetch(`${url}/invoke/Cancel${tag}/work/send`, { method: "POST", body: "null" });
    assert.equal(r.status, 202, await r.clone().text());
    const { id } = (await r.json()) as { id: string };
    await waitFor("closure a running", () => (cancelProbe.ranA > 0 ? true : undefined));
    const t0 = Date.now();
    const c = await fetch(`${url}/invocations/${id}/cancel?mode=signal`, { method: "POST" });
    assert.equal(c.status, 200, await c.clone().text());
    const st = await settled(id);
    const took = Date.now() - t0;
    assert.equal(st.status, "failed", JSON.stringify(st));
    assert.equal(st.code, 409, JSON.stringify(st));
    assert.equal(cancelProbe.ranB, 0, "the cancellable step after the cancel still ran");
    if (tag === "Bidi") {
      // The runtime delivers CANCEL into the open stream; the closure gets the signal at once.
      assert.equal(cancelProbe.aborted, 1, "the closure did not see the cancel signal");
      assert.ok(took < 1000, `cancel took ${took} ms — the closure ran the full 1500 ms`);
    } else {
      // RR: there is no channel into the middle of a turn — the closure runs to the end, the cancel takes effect at the next step.
      assert.equal(cancelProbe.aborted, 0);
    }
  });

  test(`${tag}: stop() stops a chat turn — no tool runs, the model is not called a second time; BIDI interrupts the model call`, async () => {
    const a = await adapter();
    Object.assign(slow, { calls: 0, aborted: 0, tool: 0 });
    const chatId = `stop-${tag}`;
    const transport = a.cairnChatTransport({ baseUrl: url, service: `Slow${tag}` });
    const chat = new TestChat(chatId, transport);
    const sent = chat.sendMessage({ text: "Tell a long story" });
    await waitFor("the model starts generating", () => (slow.calls > 0 ? true : undefined));
    const id = await waitFor("id invocation", () => {
      const m = chat.lastMessage;
      return m?.role === "assistant" ? ((m.metadata as any)?.cairnInvocationId ?? m.id) : undefined;
    });
    await new Promise((r) => setTimeout(r, 300));
    const t0 = Date.now();
    await chat.stop();
    await sent.catch(() => {});
    const st = await settled(id);
    const took = Date.now() - t0;
    assert.notEqual(st.status, "paused", JSON.stringify(st));
    assert.equal(slow.tool, 0, "the tool ran after stop()");
    assert.equal(slow.calls, 1, "the model was called again after stop()");
    // streamText swallows the step's abort error (like every tool/model error): the turn
    // ends normally, and the history keeps the part already generated.
    assert.equal(st.status, "completed", JSON.stringify(st));
    const saved = (await transport.messages(chatId)) as UIMessage[];
    assert.deepEqual(saved.map((m) => m.role), ["user", "assistant"]);
    assert.match(textOf(saved[1]!), /^chunk 1 /);
    if (tag === "Bidi") {
      assert.equal(slow.aborted, 1, "the model call did not see abortSignal");
      assert.ok(took < 1200, `stop() took ${took} ms — the model ran to the end`);
      assert.ok(!textOf(saved[1]!).includes("chunk 12"), "the history has the full answer");
    }
  });
}
