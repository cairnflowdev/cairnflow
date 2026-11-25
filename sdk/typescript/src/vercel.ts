// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Adapter for the Vercel AI SDK **v7** — **no** dependency on the `ai` package:
 * the types here are structural types matching `LanguageModelV4Middleware`,
 * `ToolSet`, `ChatTransport` of `ai@7`, so this SDK still pulls in no external
 * package. `test/ai-sdk.test.ts` runs it with the real `ai` package
 * (devDependency) and `tsc` checks the types are assignable.
 *
 * Handler side — a virtual object keyed by `chatId`:
 *
 * ```ts
 * const chat = chatObject("Chat", (ctx, req) =>
 *   streamText({
 *     model: wrapLanguageModel({ model: openai("gpt-…"), middleware: cairnMiddleware(ctx) }),
 *     tools: cairnTools(ctx, tools, { approve: ["refund"] }),
 *     messages: await convertToModelMessages(req.messages),
 *     stopWhen: stepCountIs(5),
 *   }),
 * );
 * ```
 *
 * - Each model call is **one `ctx.run`** (a durable step: rerun after a failure
 *   takes the result from the journal instead of calling the model again), wrapped
 *   in a `ctx.trace.span` carrying `model`, `input_tokens`, `output_tokens` — what
 *   metering adds up.
 * - Each part of the model stream is translated into a **UI chunk** (`UIMessageChunk`)
 *   and appended to the **ephemeral stream** `ui` as soon as it arrives: the browser
 *   sees tokens while the `ctx.run` is still running (wire v4). The append is inside
 *   the `ctx.run` closure, so replay does not emit it again.
 * - `cairnTools`: each `execute` is a `ctx.run` + span; the tool's input, result and
 *   (terminal) error also go to `ui`. Tools in `approve` wait for a human approver
 *   via `ctx.promise` — durable, without holding a process.
 * - `chatObject`: the `send` handler (EXCLUSIVE) builds the history from state, runs
 *   the callback, consumes all of `toUIMessageStream`, saves the new history; two
 *   SHARED handlers, `active` (id of the running invocation) and `messages` (the history).
 *
 * Browser side: `cairnChatTransport` sends messages to `send` via `/send`, then
 * reads that invocation's `ui` stream with {@link readStream}; `reconnectToStream`
 * reads again by the remembered id, and when the id is lost (page reload) asks the
 * VO's `active`; cancel ⇒ `POST /invocations/{id}/cancel`. An ephemeral stream that
 * is `reset` (leader change mid-generation) or an attempt that is `superseded` (the
 * SDK died mid-generation) in the middle of a step ⇒ a `reset-step` chunk: the UI
 * drops the unfinished part of that step, the new leader reruns the step and emits
 * it again from the start.
 */

import { readStream, StreamError } from "./client.ts";
import { Transport } from "./transport.ts";
import { type Context, RunFailure, Suspend, TerminalError } from "./handler.ts";
import { type HandlerOptions, service, type ServiceDefinition } from "./endpoint.ts";

/** A part of the model stream or a UI chunk — only `type` is needed. */
export type StreamPart = { readonly type: string } & Record<string, unknown>;

/** The part of `ctx` the middleware uses — both `Context` and `ServiceContext` have it. */
type Ctx = Pick<Context, "run" | "stream" | "trace">;

/**
 * A provider error that calling again would repeat (`APICallError` with `isRetryable:
 * false` — 400 prompt too long, 401 wrong key) ⇒ `RunFailure`: the failed result goes
 * into the journal, the step is not retried. Other errors are rethrown as is: the turn
 * dies and the runtime retries it.
 */
function settleModelError(ui: { append(v: unknown): void }, e: unknown): never {
  const fatal = (e as { isRetryable?: unknown } | null)?.isRetryable === false || e instanceof TerminalError;
  if (!fatal) throw e;
  ui.append({ type: "error", errorText: errorText(e) });
  const status = (e as { statusCode?: unknown }).statusCode;
  throw new RunFailure(errorText(e), typeof status === "number" ? status : 500);
}

/**
 * A transient provider error leaving the middleware is **no longer** a retryable
 * `APICallError`: the AI SDK would retry it itself with a new call — a new `ctx.run`
 * in a turn that already died. The runtime retries the whole turn, with backoff and
 * an attempt cap.
 */
function notRetryableHere(e: unknown): never {
  if ((e as { isRetryable?: unknown } | null)?.isRetryable === true) {
    throw new Error(errorText(e), { cause: e });
  }
  throw e;
}

/** The JSON copy of a value (Date ⇒ string, functions ⇒ dropped): what the journal can hold. */
function json<T>(v: T): T {
  return v === undefined ? v : (JSON.parse(JSON.stringify(v)) as T);
}

function errorText(e: unknown): string {
  if (e instanceof Error) return e.message;
  return typeof e === "string" ? e : JSON.stringify(e) ?? String(e);
}

/** Token count of a `usage` field: `number` (spec v2) or `{ total }` (v3, v4). */
function count(v: unknown): number | undefined {
  if (typeof v === "number") return v;
  const t = (v as { total?: unknown } | null | undefined)?.total;
  return typeof t === "number" ? t : undefined;
}

function tokensOf(usage: unknown): { input_tokens?: number; output_tokens?: number } {
  const u = (usage ?? {}) as { inputTokens?: unknown; outputTokens?: unknown };
  const out: { input_tokens?: number; output_tokens?: number } = {};
  const i = count(u.inputTokens);
  const o = count(u.outputTokens);
  if (i !== undefined) out.input_tokens = i;
  if (o !== undefined) out.output_tokens = o;
  return out;
}

/** `timestamp` becomes a string through the journal; the AI SDK reads it as a `Date`. */
function reviveTimestamp<T>(v: T): T {
  const o = v as { timestamp?: unknown } | undefined;
  if (o !== undefined && o !== null && typeof o.timestamp === "string") {
    return { ...o, timestamp: new Date(o.timestamp) } as T;
  }
  return v;
}

/** A part of the model stream in the form the journal can hold: `error` carries a string, not an `Error`. */
function journalPart(p: StreamPart): StreamPart {
  if (p.type === "error") return { type: "error", error: errorText(p["error"]) };
  return json(p);
}

/**
 * The parts that close a model stream cut off midway: `*-end` for text/reasoning
 * still open, then `finish` with `finishReason` `other` (`raw: "cancelled"`) — tokens
 * unknown (the provider did not get to report them).
 */
function cutShort(parts: readonly StreamPart[]): StreamPart[] {
  const open = new Map<string, string>();
  for (const p of parts) {
    const t = String(p.type);
    if (t === "text-start" || t === "reasoning-start") open.set(String(p["id"]), t.replace("-start", "-end"));
    if (t === "text-end" || t === "reasoning-end") open.delete(String(p["id"]));
  }
  if (parts.some((p) => p.type === "finish")) return [];
  return [
    ...[...open].map(([id, type]) => ({ type, id })),
    {
      type: "finish",
      finishReason: { unified: "other", raw: "cancelled" },
      usage: {
        inputTokens: { total: undefined, noCache: undefined, cacheRead: undefined, cacheWrite: undefined },
        outputTokens: { total: undefined, text: undefined, reasoning: undefined },
      },
    },
  ];
}

function parseInput(s: unknown): unknown {
  if (typeof s !== "string") return s;
  try {
    return JSON.parse(s);
  } catch {
    return s;
  }
}

/** Takes the fields that are present — `exactOptionalPropertyTypes` forbids an explicit `undefined`. */
function pick(p: StreamPart, keys: readonly string[]): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const k of keys) if (p[k] !== undefined) out[k] = p[k];
  return out;
}

const TOOL_FLAGS = ["providerExecuted", "dynamic", "title"] as const;

/**
 * UI chunks of a model stream part (`LanguageModelV4StreamPart` →
 * `UIMessageChunk`), as `streamText`'s `toUIMessageStream` translates them. Parts
 * with no presence in the UI (`response-metadata`, `raw`, `file`, `tool-input-end`) ⇒ empty.
 */
export function uiChunks(p: StreamPart): StreamPart[] {
  switch (p.type) {
    case "stream-start":
      return [{ type: "start-step" }];
    case "finish":
      return [{ type: "finish-step" }];
    case "text-start":
    case "text-end":
    case "reasoning-start":
    case "reasoning-end":
      return [{ type: p.type, ...pick(p, ["id"]) }];
    case "text-delta":
    case "reasoning-delta":
      return [{ type: p.type, ...pick(p, ["id", "delta"]) }];
    case "tool-input-start":
      return [
        { type: "tool-input-start", toolCallId: p["id"], ...pick(p, ["toolName", ...TOOL_FLAGS]) },
      ];
    case "tool-input-delta":
      return [{ type: "tool-input-delta", toolCallId: p["id"], inputTextDelta: p["delta"] }];
    case "tool-call":
      return [
        {
          type: "tool-input-available",
          ...pick(p, ["toolCallId", "toolName", ...TOOL_FLAGS]),
          input: parseInput(p["input"]),
        },
      ];
    case "tool-result":
      // Only provider-executed tools get here — user tools go through `cairnTools`.
      return [
        p["isError"] === true
          ? {
              type: "tool-output-error",
              toolCallId: p["toolCallId"],
              errorText: errorText(p["result"]),
              providerExecuted: true,
            }
          : {
              type: "tool-output-available",
              toolCallId: p["toolCallId"],
              output: p["result"],
              providerExecuted: true,
            },
      ];
    case "source":
      return p["sourceType"] === "url"
        ? [{ type: "source-url", sourceId: p["id"], ...pick(p, ["url", "title"]) }]
        : [];
    case "error":
      return [{ type: "error", errorText: errorText(p["error"]) }];
    default:
      return [];
  }
}

/** UI chunks of a `doGenerate` result (`generateText`): the whole step at once. */
function generateChunks(content: unknown): StreamPart[] {
  const out: StreamPart[] = [{ type: "start-step" }];
  (Array.isArray(content) ? (content as StreamPart[]) : []).forEach((c, i) => {
    if (c.type === "text" || c.type === "reasoning") {
      const id = `${c.type}-${i}`;
      out.push(
        { type: `${c.type}-start`, id },
        { type: `${c.type}-delta`, id, delta: c["text"] },
        { type: `${c.type}-end`, id },
      );
    } else {
      out.push(...uiChunks(c));
    }
  });
  out.push({ type: "finish-step" });
  return out;
}

export interface CairnMiddlewareOptions {
  /** Ephemeral stream receiving UI chunks (default `ui`). */
  readonly stream?: string;
}

interface ModelRef {
  readonly modelId: string;
  // Method syntax (bivariant): a real model accepts `LanguageModelV4CallOptions`.
  doGenerate?(params: Record<string, unknown>): PromiseLike<unknown>;
  doStream?(params: Record<string, unknown>): PromiseLike<unknown>;
}

/** The AI SDK's `abortSignal` plus the closure's cancellation signal. */
function withAbort(params: unknown, signal: AbortSignal): Record<string, unknown> {
  const p = (params ?? {}) as Record<string, unknown>;
  const own = p["abortSignal"];
  return { ...p, abortSignal: own instanceof AbortSignal ? AbortSignal.any([own, signal]) : signal };
}

/**
 * Middleware (`LanguageModelV4Middleware`) — see the top of the file.
 *
 * The journal keeps the result **except** `request` (the request body sent to the
 * provider — the whole chat history, one copy per call) and `response.body`: they are
 * only for debugging, and keeping them multiplies journal bytes by the conversation length.
 */
export function cairnMiddleware(ctx: Ctx, opts: CairnMiddlewareOptions = {}) {
  const ui = ctx.stream(opts.stream ?? "ui");
  return {
    specificationVersion: "v4" as const,
    async wrapGenerate<R>(o: {
      doGenerate: () => PromiseLike<R>;
      params?: unknown;
      model: ModelRef;
    }): Promise<R> {
      const modelId = o.model.modelId;
      const r = await ctx.trace.span(`llm ${modelId}`, async (s) => {
        // The step name carries no model id: the `Run` name must match the journal on
        // replay, and switching models between two deploys must not stop every
        // in-progress chat with 570 — the result in the journal is still the right
        // history. The two paths have different names because their results have different shapes.
        // A pass-through step: it can stop when the invocation is cancelled — the
        // closure's signal goes into the `abortSignal` of the provider call.
        const call = async (signal: AbortSignal): Promise<R> => {
          const sent =
            o.model.doGenerate === undefined
              ? o.doGenerate()
              : (o.model.doGenerate(withAbort(o.params, signal)) as PromiseLike<R>);
          const { request: _request, ...rest } = (await Promise.resolve(sent).then(
            (v) => v,
            (e) => settleModelError(ui, e),
          )) as R & { request?: unknown };
          const { body: _body, ...response } = (rest as { response?: { body?: unknown } }).response ?? {};
          const safe = json({ ...rest, response }) as unknown as R;
          for (const c of generateChunks((safe as { content?: unknown }).content)) ui.append(c);
          return safe;
        };
        const r = await ctx.run("model.generate", call, { cancellable: true });
        s.set({ model: modelId, ...tokensOf((r as { usage?: unknown }).usage) });
        return r;
      }).catch(notRetryableHere);
      const res = (r as { response?: unknown }).response;
      return { ...(r as object), response: reviveTimestamp(res) } as R;
    },
    async wrapStream<R extends { stream: ReadableStream<any> }>(o: {
      doStream: () => PromiseLike<R>;
      params?: unknown;
      model: ModelRef;
    }): Promise<R> {
      const modelId = o.model.modelId;
      const done = await ctx.trace.span(`llm ${modelId}`, async (s) => {
        const call = async (signal: AbortSignal) => {
          const parts: StreamPart[] = [];
          let rest: unknown = {};
          try {
            const sent =
              o.model.doStream === undefined ? o.doStream() : o.model.doStream(withAbort(o.params, signal));
            const { stream, request: _request, ...more } = (await sent) as R & {
              request?: unknown;
            };
            rest = json(more);
            for await (const p of stream as unknown as AsyncIterable<StreamPart>) {
              // The provider reports an error **inside** the stream instead of throwing
              // (`openai-compatible` with overload/429 midway): a step error, like a thrown
              // one — transient means the turn fails so the runtime retries it, `isRetryable:
              // false` means the failed result goes into the journal. It does not go into the
              // journal as a successful call with truncated text.
              if (p.type === "error") throw p["error"] ?? new Error("the provider reported an error in the stream");
              const safe = journalPart(p);
              parts.push(safe);
              for (const c of uiChunks(safe)) ui.append(c);
            }
            return { parts, rest };
          } catch (e) {
            // Cancelled mid-generation: what was generated — what the UI already shows —
            // is the step's result, ended early. It goes into the journal like any result,
            // `streamText` sees a finished step and stops its loop, and the history keeps
            // exactly the text the user saw (like ChatGPT's "Stop").
            if (signal.aborted) {
              const tail = cutShort(parts);
              for (const p of tail) for (const c of uiChunks(p)) ui.append(c);
              return { parts: [...parts, ...tail], rest };
            }
            return settleModelError(ui, e);
          }
        };
        // A pass-through step that can stop when cancelled — like `wrapGenerate`.
        const r = await ctx.run("model.stream", call, { cancellable: true });
        const fin = r.parts.find((p) => p.type === "finish");
        s.set({ model: modelId, ...tokensOf(fin?.["usage"]) });
        return r;
      }).catch(notRetryableHere);
      // The caller (streamText) reads exactly the same parts again — from the real call
      // or from the journal on replay.
      const stream = new ReadableStream<StreamPart>({
        start(c) {
          for (const p of done.parts) c.enqueue(p.type === "response-metadata" ? reviveTimestamp(p) : p);
          c.close();
        },
      });
      return { ...(done.rest as object), stream } as unknown as R;
    },
  };
}

/** The approver's answer — the body of `POST /promises/{svc}/{key}/{approvalId}/resolve`. */
export interface Approval {
  readonly approved: boolean;
  readonly reason?: string;
}

/** What the tool returns to the model when the approver denies. */
export interface Denied {
  readonly denied: true;
  readonly reason?: string;
}

export interface CairnToolsOptions {
  /** Ephemeral stream receiving the tool's UI chunks (default `ui`). */
  readonly stream?: string;
  /**
   * Tools that must wait for a human approver before running: a list of names, or a
   * function deciding from the input. Needs `ctx.promise` — i.e. a virtual object or workflow.
   */
  readonly approve?: readonly string[] | ((name: string, input: unknown) => boolean);
}

/** Approval id of a tool call — the promise name on the object. */
export function approvalIdOf(toolCallId: string): string {
  return `approve-${toolCallId}`;
}

type ToolCtx = Ctx & Partial<Pick<Context, "promise" | "set" | "clear">>;

/** State key prefix of pending approval requests — see {@link cairnTools}. */
const PENDING_PREFIX = "pending-approval/";

/** A pending approval request, as {@link cairnTools} writes it into the object's state. */
interface PendingApproval {
  readonly approvalId: string;
  readonly toolCallId: string;
  readonly toolName: string;
  readonly input: unknown;
}

/** The turn's approval decisions, by `toolCallId` — so {@link chatObject} saves them as the UI saw them. */
type Decision = { id: string; approved: boolean; reason?: string };
const decisions = new WeakMap<object, Map<string, Decision>>();
function decisionsOf(ctx: object): Map<string, Decision> {
  let m = decisions.get(ctx);
  if (m === undefined) decisions.set(ctx, (m = new Map()));
  return m;
}

/**
 * A history built by `toUIMessageStream` does not know cairn's approvals (the tool runs
 * as usual and returns {@link Denied} when denied): attach `approval` and turn the
 * denied part into `output-denied` — exactly as the UI builds it from chunks.
 */
function withDecisions(messages: unknown[], ctx: object): unknown[] {
  const m = decisions.get(ctx);
  if (m === undefined) return messages;
  return messages.map((msg) => {
    const parts = (msg as { parts?: unknown[] }).parts;
    if (!Array.isArray(parts)) return msg;
    return {
      ...(msg as object),
      parts: parts.map((p) => {
        const d = m.get(String((p as { toolCallId?: unknown }).toolCallId));
        if (d === undefined) return p;
        if (d.approved) return { ...(p as object), approval: d };
        const { output: _output, errorText: _errorText, ...rest } = p as Record<string, unknown>;
        return { ...rest, state: "output-denied", approval: d };
      }),
    };
  });
}

/**
 * Wraps every tool's `execute` in `ctx.run` + span (`tool`), and puts the input,
 * result and error onto the `ui` stream — `tool-input-available` is emitted again at
 * the start of the step so the UI still has the tool part when the stream lost the
 * step that called it (leader change).
 *
 * Tools in `opts.approve`: a `ctx.run` emits `tool-approval-request`
 * (`approvalId` = {@link approvalIdOf}), then waits on
 * `ctx.promise<Approval>(approvalId)`. Denied ⇒ `tool-output-denied` to the UI, the
 * model gets {@link Denied}; the tool does not run. While waiting, the request stays
 * in the object's state (`pending-approval/<approvalId>`), deleted when the answer
 * arrives: the ephemeral stream only holds the running step, so a reloaded page reads
 * the request from the `messages` of {@link chatObject}.
 *
 * **Every append is inside the closure of a `ctx.run`**: only the real run emits,
 * and the record carries the step of that very `Run` command — parallel tools are
 * interleaved steps, and the runtime keeps the order within each step.
 */
export function cairnTools<T extends Record<string, object>>(
  ctx: ToolCtx,
  tools: T,
  opts: CairnToolsOptions = {},
): T {
  const ui = ctx.stream(opts.stream ?? "ui");
  const gate = opts.approve;
  const needs = (name: string, input: unknown): boolean =>
    gate === undefined ? false : typeof gate === "function" ? gate(name, input) : gate.includes(name);
  const out: Record<string, object> = {};
  for (const [name, tool] of Object.entries(tools)) {
    const exec = (tool as { execute?: unknown }).execute;
    if (typeof exec !== "function") {
      out[name] = tool;
      continue;
    }
    out[name] = {
      ...tool,
      execute: async (input: unknown, options: unknown) => {
        const toolCallId = String((options as { toolCallId?: unknown } | undefined)?.toolCallId ?? name);
        const call = { type: "tool-input-available", toolCallId, toolName: name, input: json(input) };
        let answer: StreamPart[] = [];
        if (needs(name, input)) {
          if (ctx.promise === undefined) {
            throw new TerminalError(`tool ${name} needs approval, but ctx has no promises (it must run in a virtual object)`);
          }
          const approvalId = approvalIdOf(toolCallId);
          await ctx.run(`approval:${name}`, () => {
            ui.append(call);
            ui.append({ type: "tool-approval-request", approvalId, toolCallId });
            return approvalId;
          });
          const pendingKey = `${PENDING_PREFIX}${approvalId}`;
          ctx.set?.(pendingKey, { approvalId, toolCallId, toolName: name, input: json(input) } satisfies PendingApproval);
          let a: Approval;
          try {
            a = await ctx.promise<Approval>(approvalId).get();
          } finally {
            // A cancel (CANCEL ⇒ 409) or a rejected promise also deletes the key: a
            // leftover key becomes a fake approval message in `messages`, and the user's
            // next message hits `STALE_HISTORY`. Deterministic: the error comes from the journal.
            ctx.clear?.(pendingKey);
          }
          const reason = typeof a?.reason === "string" ? { reason: a.reason } : {};
          decisionsOf(ctx).set(toolCallId, { id: approvalId, approved: a?.approved === true, ...reason });
          answer = [{ type: "tool-approval-response", approvalId, approved: a?.approved === true, ...reason }];
          if (a?.approved !== true) {
            return ctx.run(`denied:${name}`, () => {
              for (const c of answer) ui.append(c);
              ui.append({ type: "tool-output-denied", toolCallId });
              return { denied: true, ...reason } satisfies Denied;
            });
          }
        }
        return ctx.trace.span(
          `tool ${name}`,
          () =>
            ctx.run(`tool:${name}`, async (signal) => {
              ui.append(call);
              for (const c of answer) ui.append(c);
              try {
                const output = json(await exec.call(tool, input, withAbort(options, signal)));
                ui.append({ type: "tool-output-available", toolCallId, output });
                return output;
              } catch (e) {
                // A transient error kills the turn and the runtime retries it;
                // only a terminal error is a tool outcome the model (and the UI)
                // sees — `ctx.run` records it in the journal, so replay does not
                // rerun the failed tool.
                if (e instanceof TerminalError) {
                  ui.append({ type: "tool-output-error", toolCallId, errorText: e.message });
                }
                throw e;
              }
            }, { cancellable: true }),
          { tool: name },
        );
      },
    };
  }
  return out as T;
}

/**
 * `onError` for `streamText`: `Suspend` (the turn suspending to wait for an ack or for
 * an approver — the SDK's normal path, not an error) is ignored; other errors go into
 * the invocation's `$log`. `streamText`'s default is `console.error`, i.e. one
 * `Suspend` line per step.
 */
export function cairnOnError(ctx: Pick<Context, "log">): (event: { error: unknown }) => void {
  return ({ error }) => {
    if (error instanceof Suspend) return;
    ctx.log.error("streamText", { error: errorText(error) });
  };
}

/** The `/send` body {@link cairnChatTransport} sends to the `send` handler. */
export interface ChatRequest {
  readonly chatId: string;
  readonly trigger?: "submit-message" | "regenerate-message";
  /** Id of the message to regenerate (`regenerate-message`). */
  readonly messageId?: string;
  /** The user's new message (by default the transport only sends the last message). */
  readonly message?: { readonly id: string };
  /**
   * Id of the message right before {@link message} on the client side (`null`: none) —
   * {@link chatObject} refuses messages from a page holding an old history.
   * Absent (old transport) ⇒ not checked.
   */
  readonly after?: string | null;
  /** The whole history, when the transport turns on `sendHistory`. */
  readonly messages?: readonly { readonly id: string }[];
  readonly body?: unknown;
}

/** What the {@link chatObject} callback receives: the history, already built. */
export interface ChatTurn extends ChatRequest {
  /** The UI history (`UIMessage[]`) to pass to `convertToModelMessages`. */
  readonly messages: any[];
  /** {@link cairnOnError} of `ctx` — pass it to `streamText({ onError })`. */
  readonly onError: (event: { error: unknown }) => void;
}

/** A `streamText` result — only the part {@link chatObject} needs. */
export interface UIMessageStreamSource {
  toUIMessageStream(options: {
    originalMessages: any[];
    generateMessageId: () => string;
    onError: (error: unknown) => string;
    onFinish: (event: { messages: any[] }) => void | PromiseLike<void>;
  }): AsyncIterable<unknown>;
}

/**
 * `retryPolicy.maxAttempts` of {@link chatObject} when the user declares none: without
 * one the runtime retries forever, and each attempt of a step calling a model costs
 * tokens. Out of attempts ⇒ the invocation is `paused` (the runtime's default), and can be fixed then `resume`d.
 */
export const CHAT_MAX_ATTEMPTS = 5;

export interface ChatObjectOptions {
  /**
   * Contract shared by every handler (`retryPolicy`, `trace`, …). No declared
   * `retryPolicy.maxAttempts` ⇒ {@link CHAT_MAX_ATTEMPTS}.
   */
  readonly options?: HandlerOptions;
  /** Ephemeral stream the transport reads (default `ui`) — receives the object's own errors. */
  readonly stream?: string;
}

/**
 * Stable machine code of the stale-history error: the `errorText` of the `error` chunk
 * `send` emits to the UI always starts with `"STALE_HISTORY:"`. The UI compares with
 * `errorText.startsWith(STALE_HISTORY_CODE)` instead of comparing wording (wording may change).
 */
export const STALE_HISTORY_CODE = "STALE_HISTORY";

/**
 * The full `errorText` `send` emits to the UI when the page holds an old history:
 * `"STALE_HISTORY: chat history is out of date; reload the chat and send again"`.
 * Starts with {@link STALE_HISTORY_CODE} and a `:`.
 */
export const STALE_HISTORY = "STALE_HISTORY: chat history is out of date; reload the chat and send again";

/**
 * The page sent a message holding a history older than the state: the message before
 * the new one on the client is not the message before where it goes in the saved history.
 */
function stale(saved: readonly { id: string }[], req: ChatRequest): boolean {
  if (req.after === undefined || req.messages !== undefined || req.trigger === "regenerate-message") return false;
  if (req.message === undefined) return false;
  const i = saved.findIndex((m) => m.id === req.message!.id);
  const before = (i >= 0 ? saved[i - 1] : saved.at(-1))?.id ?? null;
  return before !== req.after;
}

/**
 * This turn's history, from the saved state and the new message: append the message
 * (replace if the id matches), or cut from the message to regenerate.
 */
function historyOf(saved: readonly { id: string }[], req: ChatRequest): { id: string }[] {
  if (req.messages !== undefined) return [...req.messages];
  if (req.trigger === "regenerate-message") {
    // Like `AbstractChat.regenerate` cuts on the client: no id ⇒ the last message; an
    // assistant message is dropped, a user message is kept.
    const at = req.messageId === undefined ? saved.length - 1 : saved.findIndex((m) => m.id === req.messageId);
    if (at < 0) return [...saved];
    return saved.slice(0, (saved[at] as { role?: string }).role === "assistant" ? at : at + 1);
  }
  if (req.message === undefined) return [...saved];
  const msg = req.message;
  const i = saved.findIndex((m) => m.id === msg.id);
  return i >= 0 ? [...saved.slice(0, i), msg] : [...saved, msg];
}

/** State key of the running turn's input — see {@link chatObject}. */
const INPUT = "turn-input";

/**
 * The running turn's input, relative to the saved history: the turn's history is
 * `saved[0..base) ++ tail` — usually `tail` is just the message the user just sent.
 */
interface TurnInput {
  readonly base: number;
  readonly tail: unknown[];
}

function inputOf(saved: readonly unknown[], messages: readonly unknown[], req: ChatRequest): TurnInput {
  let base = 0;
  // `historyOf` cuts from `saved` itself (same object); a client history
  // (`sendHistory`) is stored as is.
  if (req.messages === undefined) {
    while (base < saved.length && base < messages.length && messages[base] === saved[base]) base++;
  }
  return { base, tail: json(messages.slice(base)) };
}

/**
 * Virtual object of one chat (key = `chatId`) — see the top of the file.
 *
 * - `send` (EXCLUSIVE): writes `active` = this invocation's id, builds the history,
 *   keeps the turn's input in state (`turn-input`), calls
 *   `run`, consumes all of `toUIMessageStream` (assistant message id = invocation id —
 *   deterministic across replay, and equal to the `messageId` of the `start` chunk the
 *   transport emits), saves `messages`, clears `active`. Returns `{ id }`.
 * - `active` (SHARED): id of the running `send` invocation, or `null`. In
 *   `REQUEST_RESPONSE` the state only shows after the first durable step (the first
 *   model call); in `BIDI_STREAM` (the default of `serve`) it shows at once.
 * - `messages` (SHARED): the saved history (`UIMessage[]`). Mid-turn: the history of
 *   the running turn — including the message the user just sent — plus an assistant
 *   message carrying the pending approval request, if any.
 */
export function chatObject(
  name: string,
  run: (ctx: Context, chat: ChatTurn) => UIMessageStreamSource | PromiseLike<UIMessageStreamSource>,
  opts: ChatObjectOptions = {},
): ServiceDefinition {
  return service(
    name,
    {
      async send(ctx: Context, req: ChatRequest) {
        ctx.set("active", ctx.invocationId);
        try {
          const saved = (await ctx.get<{ id: string }[]>("messages")) ?? [];
          if (stale(saved, req)) {
            // Run no turn, change no history: the UI sees the error, reloads, then sends.
            await ctx.run("stale-history", () => {
              ctx.stream(opts.stream ?? "ui").append({ type: "error", errorText: STALE_HISTORY });
            });
            ctx.clear("active");
            return { id: ctx.invocationId, stale: true };
          }
          const messages = historyOf(saved, req);
          // The history is only written at the end of the turn; a page reloaded mid-turn
          // (waiting hours for an approval) must still see the message just sent.
          ctx.set(INPUT, inputOf(saved, messages, req));
          const result = await run(ctx, { ...req, messages, onError: cairnOnError(ctx) });
          let final: unknown[] = messages;
          for await (const _ of result.toUIMessageStream({
            originalMessages: messages,
            generateMessageId: () => ctx.invocationId,
            // Like the adapter's chunks: a tool error (TerminalError) is something the model sees too.
            onError: errorText,
            onFinish: (e) => {
              final = e.messages;
            },
          })) {
            // Chunks already went to `ui` from the middleware and tools; here we only need to run to the end.
          }
          ctx.set("messages", json(withDecisions(final, ctx)));
          ctx.clear(INPUT);
          ctx.clear("active");
          return { id: ctx.invocationId };
        } catch (e) {
          // Terminal errors only: the invocation ends here, no replay. On a transient
          // error the turn is retried from the journal — a `ClearState` slipped in there
          // would make the journal diverge on the next attempt.
          if (e instanceof TerminalError) {
            ctx.clear(INPUT);
            ctx.clear("active");
          }
          throw e;
        }
      },
      async active(ctx: Context) {
        return (await ctx.get<string>("active")) ?? null;
      },
      async messages(ctx: Context) {
        const stored = (await ctx.get<unknown[]>("messages")) ?? [];
        const input = await ctx.get<TurnInput>(INPUT);
        const saved = input === null ? stored : [...stored.slice(0, input.base), ...input.tail];
        // A turn waiting for approval: the request is in state, not only on the
        // ephemeral stream — an assistant message (id = the `send` invocation's id, equal
        // to the `messageId` of the `start` chunk) carries the `approval-requested` tool part.
        const keys = (await ctx.getKeys()).filter((k) => k.startsWith(PENDING_PREFIX)).sort();
        if (keys.length === 0) return saved;
        // No turn is running ⇒ nobody is still waiting for approval: a leftover key (state
        // written by an older SDK version) does not become a fake message.
        const active = await ctx.get<string>("active");
        if (active === null) return saved;
        const parts = [];
        for (const k of keys) {
          const p = await ctx.get<PendingApproval>(k);
          if (p === null) continue;
          parts.push({
            type: `tool-${p.toolName}`,
            toolCallId: p.toolCallId,
            state: "approval-requested",
            input: p.input,
            approval: { id: p.approvalId },
          });
        }
        if (parts.length === 0) return saved;
        return [...saved, { id: active, role: "assistant", parts }];
      },
    },
    {
      type: "VIRTUAL_OBJECT",
      handlerTypes: { send: "EXCLUSIVE", active: "SHARED", messages: "SHARED" },
      options: {
        ...opts.options,
        retryPolicy: { maxAttempts: CHAT_MAX_ATTEMPTS, ...opts.options?.retryPolicy },
      },
    },
  );
}

export interface CairnChatTransportOptions {
  /** Ingress port of a cairnflow node. */
  readonly baseUrl: string;
  /** Virtual object receiving messages, keyed by `chatId` — usually a {@link chatObject}. */
  readonly service: string;
  /** Handler receiving messages (default `send`). */
  readonly handler?: string;
  /** Scope (tenant) of the object, if any: `/scope/{scope}/…`. */
  readonly scope?: string;
  /** Extra headers — e.g. `authorization: Bearer <invoke key>`. */
  readonly headers?: Record<string, string>;
  /** Ephemeral stream to read (default `ui`). */
  readonly stream?: string;
  /**
   * Send the whole history (`messages`) instead of only the last message (`message`).
   * Default off: {@link chatObject} builds the history from state (that read already
   * recorded one copy in the journal), and the `/send` body is entry 0 — sending the
   * whole history adds a second copy every turn. When on, the object uses the client's
   * history instead of state.
   */
  readonly sendHistory?: boolean;
}

/**
 * The chunks the transport returns to `AbstractChat`. The element type is `any` rather
 * than {@link StreamPart}: `ai`'s `ChatTransport` requires
 * `ReadableStream<UIMessageChunk>`, a union a loose structural type cannot be assigned
 * to, and this SDK does not import `ai`'s types.
 */
type UIChunkStream = ReadableStream<any>;

/** `start`/`finish` emitted by the transport — records of the same type on the stream are dropped. */
const OWN_CHUNKS = new Set(["start", "finish"]);

/** The AI SDK v7 `ChatTransport` — see the top of the file. */
export function cairnChatTransport(o: CairnChatTransportOptions) {
  const base = o.baseUrl.replace(/\/+$/, "");
  const scopePath = o.scope === undefined ? "" : `/scope/${encodeURIComponent(o.scope)}`;
  const svc = encodeURIComponent(o.service);
  const name = o.stream ?? "ui";
  const headers = o.headers ?? {};
  /** `chatId` → the latest invocation id in this page. */
  const ids = new Map<string, string>();

  const chunks = (id: string, outer?: AbortSignal): UIChunkStream => {
    // A consumer cancel (`reader.cancel()`) also closes the SSE at once —
    // otherwise the connection and the node's `TenantSlot` live until the next record.
    const stop = new AbortController();
    const signal = outer === undefined ? stop.signal : AbortSignal.any([outer, stop.signal]);
    return new ReadableStream<StreamPart>({
      cancel() {
        stop.abort();
      },
      async start(c) {
        c.enqueue({ type: "start", messageId: id, messageMetadata: { cairnInvocationId: id } });
        // The step in progress when the stream was `reset` (leader change, ephemeral stream
        // lost) or when its attempt was `superseded` (the SDK died mid-generation): the
        // next run regenerates the whole step. `stepAt` identifies the `start-step`.
        let inStep = false;
        let stepAt: { step?: number | undefined; attempt?: number | undefined } = {};
        let dropStart = false;
        const reset = (c: ReadableStreamDefaultController<StreamPart>) => {
          c.enqueue({ type: "reset-step" });
          inStep = false;
          dropStart = true;
        };
        try {
          for await (const e of readStream(base, id, name, {
            headers,
            ...(signal === undefined ? {} : { signal }),
          })) {
            if (e.kind === "record") {
              const p = e.data as StreamPart | undefined;
              if (p === undefined || p === null || typeof p.type !== "string" || OWN_CHUNKS.has(p.type)) {
                continue;
              }
              const restart = dropStart;
              dropStart = false;
              if (p.type === "start-step") {
                inStep = true;
                stepAt = { step: e.step, attempt: e.attempt };
                if (restart) continue; // the previous run's `step-start` is still in the UI.
              } else if (p.type === "finish-step") {
                inStep = false;
              }
              c.enqueue(p);
            } else if (e.kind === "reset" && inStep) {
              reset(c);
            } else if (e.kind === "superseded" && inStep && e.step === stepAt.step && e.attempt === stepAt.attempt) {
              reset(c);
            }
          }
          c.enqueue({ type: "finish" });
          c.close();
        } catch (err) {
          // The ephemeral stream was lost with the process (the runtime died) and the new
          // process emits nothing more — the turn already finished, only replay is left: the
          // UI has everything that turn emitted before dying; the full history is in `messages`.
          if (err instanceof StreamError && err.status === 410) {
            c.enqueue({ type: "finish" });
            c.close();
            return;
          }
          if (stop.signal.aborted) return; // the consumer cancelled
          c.error(err);
        }
      },
    });
  };

  /**
   * POST under the cluster's error contract (`docs/admin-api.md`, the error contract
   * section): `503` + `leader` ⇒ retry exactly once at that leader — the chat's range
   * may be on a different node than the one the client called; `503` without `leader`
   * (electing, the range is changing) ⇒ back off, then ask the original node again. A
   * call creating an invocation must carry an `Idempotency-Key` so a resend points at
   * that same invocation.
   */
  const transport = new Transport({ url: base, maxAttempts: 7, retryNetwork: false });
  const post = async (path: string, init: RequestInit): Promise<Response> =>
    (await transport.request(path, { method: "POST", ...init })).res;

  /** Calls a SHARED handler of the object, returns the JSON-decoded output. */
  const ask = async (handler: string, chatId: string, signal?: AbortSignal): Promise<unknown> => {
    const res = await post(
      `${scopePath}/invoke/${svc}/${encodeURIComponent(handler)}?key=${encodeURIComponent(chatId)}`,
      {
        headers: { "content-type": "application/json", ...headers },
        body: "null",
        ...(signal === undefined ? {} : { signal }),
      },
    );
    if (!res.ok && res.status !== 429) {
      throw new Error(`cairnflow ${o.service}.${handler}: HTTP ${res.status}: ${await res.text()}`);
    }
    let body = (await res.json()) as { status?: string; id?: string; output?: string };
    // `202 waiting` / `429 throttled` with an id: the turn did not finish within the
    // `/invoke` wait cap (cold endpoint) — wait for it via attach, do not treat it as
    // "nothing" (an empty `messages()` ⇒ the next message hits `STALE_HISTORY`).
    for (let tries = 0; body.status !== "completed" && typeof body.id === "string"; tries++) {
      if (tries >= 10) throw new Error(`cairnflow ${o.service}.${handler}: invocation ${body.id} did not finish`);
      const a = await post(`/invocations/${encodeURIComponent(body.id)}/attach?wait_ms=25000`, {
        headers,
        ...(signal === undefined ? {} : { signal }),
      });
      if (!a.ok && a.status !== 429 && a.status !== 202) {
        throw new Error(`cairnflow ${o.service}.${handler}: HTTP ${a.status}: ${await a.text()}`);
      }
      body = { id: body.id, ...((await a.json()) as typeof body) };
    }
    if (body.status !== undefined && body.status !== "completed") {
      throw new Error(`cairnflow ${o.service}.${handler}: invocation ${body.status}`);
    }
    return body.output === undefined ? null : JSON.parse(body.output);
  };

  return {
    /** Always reads again from the start of the stream: the first `start` chunk resets the message on screen. */
    resumeStreamIsReplay: true as const,
    async sendMessages(m: {
      chatId: string;
      messages: unknown[];
      abortSignal: AbortSignal | undefined;
      trigger: "submit-message" | "regenerate-message";
      messageId: string | undefined;
      body?: object | undefined;
    }): Promise<UIChunkStream> {
      const path = `${scopePath}/invoke/${svc}/${encodeURIComponent(o.handler ?? "send")}/send?key=${encodeURIComponent(m.chatId)}`;
      const last = m.messages.at(-1);
      const trigger = m.trigger ?? "submit-message";
      const req: Record<string, unknown> = { chatId: m.chatId, trigger };
      if (m.messageId !== undefined) req["messageId"] = m.messageId;
      if (m.body !== undefined) req["body"] = m.body;
      if (o.sendHistory === true) req["messages"] = m.messages;
      else if (trigger === "submit-message" && last !== undefined) {
        req["message"] = last;
        // So the object refuses messages from a page holding an old history.
        req["after"] = (m.messages.at(-2) as { id?: string } | undefined)?.id ?? null;
      }
      const res = await post(path, {
        // A resend (following the leader) points at the same invocation, no second turn.
        headers: { "content-type": "application/json", "idempotency-key": crypto.randomUUID(), ...headers },
        body: JSON.stringify(req),
        ...(m.abortSignal === undefined ? {} : { signal: m.abortSignal }),
      });
      if (!res.ok) throw new Error(`cairnflow /send: HTTP ${res.status}: ${await res.text()}`);
      const id =
        res.headers.get("x-cairn-invocation-id") ?? ((await res.json()) as { id?: string }).id;
      if (typeof id !== "string") throw new Error("cairnflow /send did not return an invocation id");
      ids.set(m.chatId, id);
      m.abortSignal?.addEventListener(
        "abort",
        () => {
          // `stop` ⇒ cancel the invocation; the reader stops via `abortSignal`.
          // Cooperative cancel: the runtime delivers CANCEL into the running turn, the
          // running model step or tool gets the signal, and the history saves what was
          // generated. A hard cancel (the endpoint's default) waits for the turn to stop.
          void post(`/invocations/${id}/cancel?mode=signal`, { headers }).catch(() => {});
        },
        { once: true },
      );
      return chunks(id, m.abortSignal);
    },
    async reconnectToStream(m: {
      chatId: string;
      abortSignal?: AbortSignal | undefined;
    }): Promise<UIChunkStream | null> {
      let id = ids.get(m.chatId);
      if (id === undefined) {
        // Page reload: ask the object whether a turn is running.
        const active = await ask("active", m.chatId, m.abortSignal);
        if (typeof active !== "string") return null;
        id = active;
        ids.set(m.chatId, id);
      }
      return chunks(id, m.abortSignal);
    },
    /** The saved history (`UIMessage[]`) — the initial `messages` of `useChat` after a page reload. */
    async messages(chatId: string): Promise<unknown[]> {
      const v = await ask("messages", chatId);
      return Array.isArray(v) ? v : [];
    },
    /** Answers a `tool-approval-request`: resolves the object's `approvalId` promise. */
    async approve(chatId: string, approvalId: string, answer: Approval): Promise<void> {
      const path = `${scopePath}/promises/${svc}/${encodeURIComponent(chatId)}/${encodeURIComponent(approvalId)}/resolve`;
      const res = await post(path, {
        headers: { "content-type": "application/json", ...headers },
        body: JSON.stringify(answer),
      });
      if (!res.ok) throw new Error(`cairnflow approve ${approvalId}: HTTP ${res.status}: ${await res.text()}`);
      // `completed: false` ⇒ the promise already had a result: our own resend is
      // fine, a different decision (another tab) is reported.
      if (((await res.json().catch(() => ({}))) as { completed?: boolean }).completed !== false) return;
      const view = await (await transport.request(path.replace(/\/resolve$/, ""), { headers })).res.json().catch(() => ({}));
      const stored = (view as { value?: string }).value;
      let prior: Approval | undefined;
      try {
        prior = stored === undefined ? undefined : (JSON.parse(stored) as Approval);
      } catch {
        prior = undefined;
      }
      if (prior?.approved !== answer.approved || (prior?.reason ?? undefined) !== (answer.reason ?? undefined)) {
        throw new Error(`cairnflow approve ${approvalId}: already answered with a different decision`);
      }
    },
  };
}
