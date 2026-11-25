// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * The cairnflow client — subpath `@cairnflow/sdk/client`. No `node:*` import:
 * runs on Node ≥ 22, browsers and the edge. Two parts:
 *
 * - {@link connect}: typed calls to services, objects, workflows; waits for results
 *   via attach by id; signals, cancel, awakeables, promises.
 * - {@link readStream}: reads the streams of an invocation — the viewer's side:
 *   UIs, CLIs, adapters.
 *
 * Streams can only be read at the range's **leader** (AP-13). `readStream` follows on its own:
 *
 * - `503` with `leader` ⇒ ask again at that leader (no `leader` ⇒ wait, then ask again);
 * - `429` ⇒ wait `Retry-After`, then ask again;
 * - a `moved` event ⇒ the leader's term ended: reconnect at the new leader, from the
 *   next offset (durable stream), or from 0 if a `reset` follows (an ephemeral stream
 *   is lost with the term — the new attempt regenerates it from the start);
 * - the connection closes without an `end` (the maximum lifetime of an SSE
 *   connection) ⇒ reconnect from the next offset;
 * - the connection **breaks** midway (the node died) ⇒ reconnect from the last record
 *   and compare: if the stream is a new generation (the node was rebuilt, the range
 *   moved to another node), return `reset` and read again from 0; a followed `leader`
 *   node that does not answer ⇒ ask the original node again;
 * - `end` ⇒ stop; `410 stream_expired`, `404`, … ⇒ throw {@link StreamError}.
 *
 * Every event, including `moved` and `reset`, is returned to the caller: a UI needs
 * `reset` to clear what it has shown.
 */

import { TerminalError } from "./errors.ts";
import { backoffMs, normalizeUrl as normalize, sleepAbortable as sleep, Transport } from "./transport.ts";

/** A stream event — the SSE contract of `GET /invocations/{id}/streams/{name}`. */
export interface StreamEvent {
  readonly kind: "record" | "gap" | "superseded" | "end" | "moved" | "reset";
  readonly offset?: number;
  /** `record`: a durable record (AP-12). */
  readonly durable?: boolean;
  readonly step?: number;
  readonly attempt?: number;
  readonly k?: number;
  /** `record`: the writer's JSON payload. */
  readonly data?: unknown;
  /** `gap`: number of records dropped or overwritten. */
  readonly dropped?: number;
  /** `moved`: the new leader's ingress port, if known. */
  readonly leader?: string | null;
  /** `end`: the next offset (the stream's number of records). */
  readonly next?: number;
}

/** An error that cannot be continued past: `410 stream_expired`, `404`, `403`, … */
export class StreamError extends Error {
  readonly status: number;
  readonly code: string | undefined;
  constructor(message: string, status: number, code?: string) {
    super(message);
    this.name = "StreamError";
    this.status = status;
    this.code = code;
  }
}

export interface ReadStreamOptions {
  /** Starting offset (default 0). */
  readonly from?: number;
  /** Extra headers — e.g. `authorization: Bearer <invoke key>`. */
  readonly headers?: Record<string, string>;
  readonly signal?: AbortSignal;
  /** Maximum consecutive failures before giving up (default 10). */
  readonly retries?: number;
}


interface RawEvent {
  event: string;
  id: string | null;
  data: string;
}

/** Splits an SSE body into events; drops comment lines (`: keepalive`). */
async function* parseSse(body: ReadableStream<Uint8Array>): AsyncGenerator<RawEvent> {
  const dec = new TextDecoder();
  let buf = "";
  for await (const chunk of body as unknown as AsyncIterable<Uint8Array>) {
    buf += dec.decode(chunk, { stream: true }).replace(/\r\n/g, "\n");
    let at: number;
    while ((at = buf.indexOf("\n\n")) >= 0) {
      const block = buf.slice(0, at);
      buf = buf.slice(at + 2);
      const ev: RawEvent = { event: "message", id: null, data: "" };
      let any = false;
      for (const line of block.split("\n")) {
        if (line.startsWith(":")) continue;
        const colon = line.indexOf(":");
        const field = colon < 0 ? line : line.slice(0, colon);
        const value = colon < 0 ? "" : line.slice(colon + 1).replace(/^ /, "");
        if (field === "event") {
          ev.event = value;
          any = true;
        } else if (field === "id") {
          ev.id = value;
        } else if (field === "data") {
          ev.data += ev.data.length > 0 ? `\n${value}` : value;
          any = true;
        }
      }
      if (any) yield ev;
    }
  }
}

function toEvent(raw: RawEvent): StreamEvent {
  let d: Record<string, unknown> = {};
  try {
    const v: unknown = JSON.parse(raw.data);
    if (v !== null && typeof v === "object") d = v as Record<string, unknown>;
  } catch {
    // Body is not JSON: keep `kind`, drop the fields.
  }
  return { ...d, kind: raw.event as StreamEvent["kind"] };
}

async function errorOf(res: Response): Promise<StreamError> {
  let msg = `HTTP ${res.status}`;
  let code: string | undefined;
  try {
    const v = (await res.json()) as { error?: string; code?: string };
    if (typeof v.error === "string") msg = v.error;
    if (typeof v.code === "string") code = v.code;
  } catch {
    // Body is not JSON.
  }
  return new StreamError(msg, res.status, code);
}

/**
 * Reads stream `name` of invocation `id` from node `base` (URL or `host:port`
 * of the ingress port) — see the top of the file for how it follows the leader.
 */
export async function* readStream(
  base: string,
  id: string,
  name: string,
  opts: ReadStreamOptions = {},
): AsyncGenerator<StreamEvent> {
  const home = normalize(base);
  let node = home;
  // `503`/`429`/network errors of **one** connection attempt: the shared transport
  // layer (`transport.ts`). The loop below handles the stream's part: reconnects, `moved`, new generations.
  const transport = new Transport({
    url: home,
    headers: { accept: "text/event-stream", ...opts.headers },
    maxAttempts: (opts.retries ?? 10) + 1,
  });
  let next = opts.from ?? 0;
  const max = opts.retries ?? 10;
  let failures = 0;
  /**
   * The last record returned, for reconnecting after the connection **breaks**: if
   * the node died and was rebuilt (or the range moved to another node), an ephemeral
   * stream is a **new generation** — the new hub numbers offsets from 0 again, the
   * rerun regenerates steps from the start — and asking `from=next` would lose the
   * start of the new generation. So reconnect from the last record itself and compare:
   * match ⇒ same generation, carry on; different (or the stream is shorter) ⇒ return
   * `reset` and read again from 0, like `moved` + `reset`. A clean close (the
   * connection's maximum lifetime) needs no comparison.
   */
  let last: { offset: number; key: string } | null = null;
  let verify = false;
  const keyOf = (e: StreamEvent) => JSON.stringify([e.step, e.attempt, e.k, e.data]);
  const failed = async (e: unknown): Promise<void> => {
    failures += 1;
    if (failures > max) throw e;
    await sleep(backoffMs(failures), opts.signal);
  };
  for (;;) {
    const path = `/invocations/${encodeURIComponent(id)}/streams/${encodeURIComponent(name)}?from=${next}`;
    let res: Response;
    try {
      const sent = await transport.request(path, opts.signal === undefined ? {} : { signal: opts.signal }, node);
      res = sent.res;
      node = sent.node;
    } catch (e) {
      if (opts.signal?.aborted) throw e;
      node = home;
      await failed(e);
      continue;
    }
    // The transport layer already followed `leader` and waited `Retry-After` up to its cap.
    if (res.status === 503 || res.status === 429) {
      await failed(await errorOf(res));
      continue;
    }
    if (!res.ok || res.body === null) throw await errorOf(res);
    let moved: string | null | undefined;
    let reborn = false;
    try {
      for await (const raw of parseSse(res.body)) {
        failures = 0;
        const e = toEvent(raw);
        // The last record has left the buffer: a `gap` covers it — generations cannot be
        // compared, and nothing proves a new generation. Treat it as the same
        // generation; the reader still sees the `gap`. (A new generation renumbers
        // offsets from 0, so a `gap` covering `last.offset` needs a new generation
        // longer than the old one — rare, and even then what follows is in order.)
        if (
          verify &&
          last !== null &&
          e.kind === "gap" &&
          typeof e.offset === "number" &&
          e.offset <= last.offset &&
          last.offset < e.offset + (e.dropped ?? 0)
        ) {
          verify = false;
        }
        if (verify && last !== null && (e.kind === "record" || e.kind === "end")) {
          verify = false;
          if (e.kind === "record" && e.offset === last.offset && keyOf(e) === last.key) {
            next = last.offset + 1;
            continue; // overlap: already returned
          }
          reborn = true;
          break;
        }
        if (raw.id !== null && /^\d+$/.test(raw.id)) {
          next = Number(raw.id) + 1;
        } else if (e.kind === "gap" && typeof e.offset === "number") {
          next = e.offset + (e.dropped ?? 0);
        }
        if (e.kind === "record" && typeof e.offset === "number") last = { offset: e.offset, key: keyOf(e) };
        yield e;
        if (e.kind === "end") return;
        if (e.kind === "moved") moved = e.leader ?? null;
        if (e.kind === "reset") {
          next = 0;
          last = null;
        }
      }
    } catch (e) {
      if (opts.signal?.aborted) throw e;
      // The SSE body broke midway (the node died): reconnect from the last record and compare generations.
      if (last !== null) {
        verify = true;
        next = last.offset;
      }
      await failed(e);
      continue;
    }
    if (reborn) {
      yield { kind: "reset" };
      next = 0;
      last = null;
      continue;
    }
    // Same rule as `Transport`'s `503 + leader`: do not follow a leader with
    // a different scheme — a Bearer key does not go over `http://` to the internal
    // address the leader advertises when the client speaks `https://`. Skip ⇒ keep reading at the original node.
    if (typeof moved === "string" && moved.length > 0) {
      const to = normalize(moved);
      node = new URL(to).protocol === new URL(home).protocol ? to : home;
    }
    // Closed without `end` (maximum lifetime, or a leader change): reconnect from `next`.
  }
}

// ---------------------------------------------------------------------------
// connect()
// ---------------------------------------------------------------------------

export interface ConnectOptions {
  /** Ingress port of any node (`http://host:port` or `host:port`). */
  readonly url: string;
  /** API key (`cf_…`); the tenant is the key's tenant. */
  readonly apiKey?: string;
  /** Headers added to every request. */
  readonly headers?: Readonly<Record<string, string>>;
  /** Follow the `leader` of a `503` (default `true`) — see `transport.ts`. */
  readonly followLeader?: boolean;
  /** Cap on the attempts of one request (default 8). */
  readonly maxAttempts?: number;
  /** Time cap of one request, including the waits (default 30 000 ms). */
  readonly maxElapsedMs?: number;
  /**
   * `wait_ms` of each attach (default 25 000) — below the 30 s idle cap of the
   * GCP LB and the 60 s of ALB/nginx; the server clamps it to `--attach-max-wait-ms`.
   * Each attach is a read-barrier GET at the leader: 25 s ⇒ ~144 per hour for one waiter.
   */
  readonly attachWaitMs?: number;
  /** Replacement `fetch`. */
  readonly fetch?: typeof fetch;
}

/** Options of a call that waits for the result. */
export interface CallOptions {
  /**
   * Idempotency key. Absent ⇒ the client generates a random key, kept across every
   * attempt of this call — resending after a `503`/network error does not create a
   * second invocation (CG-1). `null` ⇒ no key is sent. A workflow's `run` does not
   * need one: it is already once per key.
   */
  readonly idempotencyKey?: string | null;
  /** Cap on waiting for the result, ms (default: wait until done). Exceeded ⇒ {@link AttachTimeout}. */
  readonly timeoutMs?: number;
  readonly signal?: AbortSignal;
  /** `x-cairn-limit-key` (flow control). */
  readonly limitKey?: string;
  /** The invocation's `cairnflow-log-level`. The runtime only accepts these four levels (others ⇒ `400`). */
  readonly logLevel?: "debug" | "info" | "warn" | "error";
  readonly headers?: Readonly<Record<string, string>>;
}

/** Options of a send (does not wait for the run). */
export type SendOptions = Omit<CallOptions, "timeoutMs">;

/** Options of a service/object/workflow handle. */
export interface TargetOptions {
  /** Scope (flow control) — the `/scope/{scope}/…` path. */
  readonly scope?: string;
}

/** `GET /invocations/{id}` — `docs/api/openapi.json` `InvocationStatus`. */
export interface InvocationStatusView {
  readonly id: string;
  readonly status: "pending" | "running" | "suspended" | "completed" | "failed" | "paused" | string;
  /** Output bytes (a string holding JSON) when `completed`. */
  readonly output?: string;
  readonly code?: number;
  readonly message?: string;
  readonly createdAt?: number;
  readonly modifiedAt?: number;
  readonly endedAt?: number;
  readonly [field: string]: unknown;
}

/** Reply to a signal and to resolving/rejecting an awakeable. */
export interface SignalAck {
  readonly id: string;
  readonly status: string;
}

/** Reply to a cancel. */
export interface CancelAck {
  readonly id: string;
  readonly cancelled: boolean;
  readonly status: string;
}

/** Reply to resolving/rejecting a promise; `completed: false` ⇒ it already had a result. */
export interface PromiseAck {
  readonly service: string;
  readonly key: string;
  readonly name: string;
  readonly completed: boolean;
  readonly woken?: number;
}

/** `GET` of a promise. */
export interface PromiseView {
  readonly service: string;
  readonly key: string;
  readonly name: string;
  readonly completed: boolean;
  readonly value?: string;
  readonly failure?: string;
}

/** Result of `output()`: does not wait. */
export type OutputResult<O> = { readonly ready: true; readonly value: O } | { readonly ready: false };

/** The invocation ended `failed`. Still a {@link TerminalError} (`code`, `message`). */
export class InvocationFailed extends TerminalError {
  readonly invocationId: string;
  constructor(invocationId: string, message: string, code: number) {
    super(message, code);
    this.name = "InvocationFailed";
    this.invocationId = invocationId;
  }
}

/** The invocation is `paused` (out of attempts, waiting for a human) — `POST /invocations/{id}/resume`. */
export class InvocationPaused extends Error {
  readonly invocationId: string;
  readonly code: number;
  constructor(invocationId: string, message: string, code: number) {
    super(message);
    this.name = "InvocationPaused";
    this.invocationId = invocationId;
    this.code = code;
  }
}

/** `409 already_invoked`: this workflow key already has a run — `invocationId` is that run. */
export class AlreadyInvoked extends Error {
  readonly invocationId: string;
  constructor(invocationId: string) {
    super(`workflow already has a run: ${invocationId}`);
    this.name = "AlreadyInvoked";
    this.invocationId = invocationId;
  }
}

/** `timeoutMs` ran out before the invocation stopped for good. It is still running; attach again by id. */
export class AttachTimeout extends Error {
  readonly invocationId: string;
  constructor(invocationId: string) {
    super(`invocation ${invocationId} did not finish before the timeout`);
    this.name = "AttachTimeout";
    this.invocationId = invocationId;
  }
}

/** An HTTP error outside the invoke contract (`400`, `403`, `404`, `503` past the cap, …). */
export class CairnHttpError extends Error {
  readonly status: number;
  readonly code: string | undefined;
  /**
   * The invocation was already created when the error happened while **waiting** for
   * it (attach): do not call again — `invocation(id).attach()` is how to keep waiting.
   */
  invocationId: string | undefined;
  constructor(message: string, status: number, code?: string) {
    super(message);
    this.name = "CairnHttpError";
    this.status = status;
    this.code = code;
  }
}

/** Input of handler `F` — the second parameter, after `ctx`. */
type InputOf<F> = F extends (ctx: any, input: infer I, ...rest: any[]) => unknown ? I : never;
/** Output of handler `F`. */
type OutputOf<F> = F extends (...a: any[]) => infer O ? Awaited<O> : never;
/** Call parameters: a handler that does not read input (`unknown`) makes input optional. */
type ArgsOf<F, Opts> = unknown extends InputOf<F> ? [input?: InputOf<F>, opts?: Opts] : [input: InputOf<F>, opts?: Opts];
/**
 * The minimal shape of a declared service the client needs — `ServiceDefinition`
 * of `serve.ts` matches it. The client does not import `serve.ts`: the `.d.ts` of the
 * `./client` subpath does not pull `node:*` types into browsers.
 */
export interface ServiceLike {
  readonly name: string;
  readonly handlers: Readonly<Record<string, (ctx: any, input: any) => unknown>>;
}
type AnyHandlerLike = (ctx: any, input: any) => unknown;
type HandlersOf<D> = D extends { readonly handlers: infer H } ? H : Readonly<Record<string, AnyHandlerLike>>;
/** Output of `run` (workflow). */
type RunOutput<D> = HandlersOf<D> extends { readonly run: infer F } ? OutputOf<F> : unknown;

/** Call, then wait for the result. */
export type Caller<F> = (...args: ArgsOf<F, CallOptions>) => Promise<OutputOf<F>>;
/** Fire and forget: returns the handle of the durable invocation. */
export type Sender<F> = (...args: ArgsOf<F, SendOptions>) => Promise<InvocationHandle<OutputOf<F>>>;

export type SendClient<H> = { readonly [K in keyof H & string]: Sender<H[K]> };

/**
 * Client of a service/object: each handler is a call-and-wait function; `send.<h>`
 * is fire and forget. A handler named `send` can still be called: `send(...)` is
 * the handler itself, `send.<h>` is a send.
 */
export type ServiceClient<D> = {
  readonly [K in Exclude<keyof HandlersOf<D> & string, "send" | "then">]: Caller<HandlersOf<D>[K]>;
} & {
  readonly send: SendClient<HandlersOf<D>> &
    ("send" extends keyof HandlersOf<D> ? Caller<HandlersOf<D>["send"]> : unknown);
};

/** Client of a workflow: `run` + `SHARED` handlers, plus `attach`/`output` by key. */
export type WorkflowClient<D> = Omit<ServiceClient<D>, "attach" | "output"> & {
  /** Waits until this key's `run` stops for good. */
  attach(opts?: { timeoutMs?: number; signal?: AbortSignal }): Promise<RunOutput<D>>;
  /** Result of the `run`, without waiting. */
  output(): Promise<OutputResult<RunOutput<D>>>;
};

/** An invocation by id. */
export interface InvocationHandle<O = unknown> {
  readonly id: string;
  /** Waits until it stops for good — attach by id, long-polling `attachWaitMs` each time. */
  attach(opts?: { timeoutMs?: number; signal?: AbortSignal }): Promise<O>;
  /** The result if done, without waiting (`failed` throws, `paused` throws). */
  output(): Promise<OutputResult<O>>;
  status(): Promise<InvocationStatusView>;
  /** Delivers a signal by name (string) or index (number); absent `value` = Void. */
  signal(name: string | number, value?: unknown): Promise<SignalAck>;
  /** Cancels. Default is **cooperative** (`mode: "signal"`, `invoke` key); `"hard"` needs `admin`. */
  cancel(opts?: { mode?: "signal" | "hard" }): Promise<CancelAck>;
}

export interface AwakeableHandle {
  resolve(value?: unknown): Promise<SignalAck>;
  reject(message: string, code?: number): Promise<SignalAck>;
}

export interface PromiseHandle {
  resolve(value?: unknown): Promise<PromiseAck>;
  reject(message: string, code?: number): Promise<PromiseAck>;
  peek(): Promise<PromiseView>;
}

/** Target of a schedule: service/handler, plus a key for a virtual object. */
export interface ScheduleTarget {
  readonly service: string;
  readonly handler: string;
  readonly key?: string;
}

/**
 * Definition for `PUT /schedules/{name}`. Exactly one of `cron` (5 fields, IANA
 * `timeZone`, default UTC) / `everyMs` (anchored at the epoch).
 */
export interface ScheduleDefinition {
  readonly cron?: string;
  readonly timeZone?: string;
  readonly everyMs?: number;
  readonly target: ScheduleTarget;
  readonly input?: unknown;
  readonly limitKey?: string;
  /** `skip` (default for an unkeyed target) / `allow` (the only option for a keyed or scoped target). */
  readonly overlap?: "skip" | "allow";
  readonly paused?: boolean;
  readonly jitterMs?: number;
}

/** A recent firing of a schedule. `status` is only present when the run is on the same range as the schedule. */
export interface ScheduleLast {
  readonly at: number;
  readonly id: string;
  readonly outcome: "fired" | "skipped";
  readonly status?: string;
}

/** A schedule as the server returns it. */
export interface ScheduleView {
  readonly name: string;
  readonly id: string;
  readonly scope?: string;
  readonly cron?: string;
  readonly timeZone?: string;
  readonly everyMs?: number;
  readonly target: ScheduleTarget;
  readonly input?: unknown;
  readonly limitKey?: string;
  readonly overlap: "skip" | "allow";
  readonly paused: boolean;
  readonly jitterMs: number;
  /** The next nominal firing time; `null` when paused. */
  readonly nextAt: number | null;
  /** The actual firing deadline of `nextAt` (jitter added). */
  readonly fireAt?: number;
  /** The next 10 firing times — `get` only. */
  readonly upcoming?: readonly number[];
  readonly last: readonly ScheduleLast[];
  readonly fired: number;
  readonly missed: number;
  readonly skipped: number;
  readonly createdAt: number;
  readonly updatedAt: number;
  /** `put`: a new schedule. */
  readonly created?: boolean;
  /** `put`: the definition changed (`false` ⇒ no-op, `nextAt` not moved). */
  readonly changed?: boolean;
}

export interface ScheduleList {
  readonly schedules: readonly ScheduleView[];
  readonly partial: boolean;
  readonly missing: readonly string[];
  readonly missingRanges: readonly number[];
}

/** `c.schedules` — the server-side schedules of the key's tenant. */
export interface SchedulesClient {
  /** Create or update; the same definition ⇒ no-op (safe to call at app startup). */
  put(name: string, def: ScheduleDefinition, opts?: TargetOptions): Promise<ScheduleView>;
  /** `undefined` when absent. */
  get(name: string, opts?: TargetOptions): Promise<ScheduleView | undefined>;
  /** `false` when absent. */
  delete(name: string, opts?: TargetOptions): Promise<boolean>;
  pause(name: string, opts?: TargetOptions): Promise<ScheduleView>;
  /** The next firing is computed from the current time — no catch-up firing. */
  resume(name: string, opts?: TargetOptions): Promise<ScheduleView>;
  /**
   * Fires once right away (ignores `paused`, `overlap`; does not move `nextAt`). The
   * same `idempotencyKey` ⇒ the same run; absent, the client generates a key, kept
   * across every attempt.
   */
  trigger(name: string, opts?: TargetOptions & { idempotencyKey?: string }): Promise<InvocationHandle<unknown>>;
  list(opts?: TargetOptions): Promise<ScheduleList>;
}

export interface Client {
  /** Plain service (no key). */
  service<D extends ServiceLike = ServiceLike>(name: string, opts?: TargetOptions): ServiceClient<D>;
  /** Virtual object theo key. */
  object<D extends ServiceLike = ServiceLike>(
    name: string,
    key: string,
    opts?: TargetOptions,
  ): ServiceClient<D>;
  /** Workflow theo key. */
  workflow<D extends ServiceLike = ServiceLike>(
    name: string,
    key: string,
    opts?: TargetOptions,
  ): WorkflowClient<D>;
  invocation<O = unknown>(id: string): InvocationHandle<O>;
  awakeable(id: string): AwakeableHandle;
  promise(service: string, key: string, name: string, opts?: TargetOptions): PromiseHandle;
  /** Server-side schedules. */
  readonly schedules: SchedulesClient;
}

/** Raw output bytes (a string) ⇒ a value: JSON if it parses, otherwise the string itself; empty ⇒ `undefined`. */
function decodeOutput(raw: string | undefined): unknown {
  if (raw === undefined || raw === "") return undefined;
  try {
    return JSON.parse(raw);
  } catch {
    return raw;
  }
}

async function httpError(res: Response): Promise<CairnHttpError> {
  let msg = `HTTP ${res.status}`;
  let code: string | undefined;
  try {
    const v = (await res.json()) as { error?: unknown; message?: unknown; code?: unknown };
    if (typeof v.error === "string") msg = v.error;
    else if (typeof v.message === "string") msg = v.message;
    if (typeof v.code === "string") code = v.code;
  } catch {
    // Body is not JSON.
  }
  return new CairnHttpError(msg, res.status, code);
}

type Settled = { readonly done: true; readonly value: unknown } | { readonly done: false; readonly id: string };

/**
 * The body of `/invoke`, `/send` and every attach (`InvokeResponse`): `200` done,
 * `202`/`429 throttled` still running (durable), `500` failed, `409` paused or
 * already_invoked. Anything else ⇒ {@link CairnHttpError}.
 */
async function settle(res: Response): Promise<Settled> {
  let body: Record<string, unknown> = {};
  try {
    const v: unknown = await res.clone().json();
    if (v !== null && typeof v === "object") body = v as Record<string, unknown>;
  } catch {
    // Not JSON: falls through to the HTTP error below.
  }
  const id = res.headers.get("x-cairn-invocation-id") ?? (typeof body["id"] === "string" ? body["id"] : undefined);
  const status = body["status"];
  const code = typeof body["code"] === "number" ? body["code"] : 500;
  const message = typeof body["message"] === "string" ? body["message"] : "";
  if (id !== undefined) {
    if (res.status === 200 && status === "completed") {
      return { done: true, value: decodeOutput(body["output"] as string | undefined) };
    }
    if ((res.status === 202 && status === "waiting") || (res.status === 429 && status === "throttled")) {
      return { done: false, id };
    }
    if (res.status === 500 && status === "failed") throw new InvocationFailed(id, message, code);
    if (res.status === 409 && status === "paused") throw new InvocationPaused(id, message, code);
    if (res.status === 409 && status === "already_invoked") throw new AlreadyInvoked(id);
  }
  throw await httpError(res);
}

/** The invocation id carried by a response (header or body) — the `429 throttled` of a durable invocation. */
async function carriesId(res: Response): Promise<boolean> {
  if (res.headers.get("x-cairn-invocation-id") !== null) return true;
  try {
    const v: unknown = await res.clone().json();
    return v !== null && typeof v === "object" && typeof (v as Record<string, unknown>)["id"] === "string";
  } catch {
    return false;
  }
}

/** A proxy that blocks `then`: a handle is not mistakenly `await`ed as a thenable. */
const NOT_THENABLE = new Set<PropertyKey>(["then", "catch", "finally"]);

/** Creates a client — see the doc at the top of the file and `docs/quickstart.md`. */
export function connect(opts: ConnectOptions): Client {
  const headers: Record<string, string> = { ...opts.headers };
  if (opts.apiKey !== undefined) headers["authorization"] = `Bearer ${opts.apiKey}`;
  const transport = new Transport({
    url: opts.url,
    headers,
    ...(opts.followLeader === undefined ? {} : { followLeader: opts.followLeader }),
    ...(opts.maxAttempts === undefined ? {} : { maxAttempts: opts.maxAttempts }),
    ...(opts.maxElapsedMs === undefined ? {} : { maxElapsedMs: opts.maxElapsedMs }),
    ...(opts.fetch === undefined ? {} : { fetch: opts.fetch }),
  });
  const attachWaitMs = opts.attachWaitMs ?? 25_000;

  const post = async (path: string, body: unknown, extra: Record<string, string> = {}, signal?: AbortSignal) => {
    const init: RequestInit = { method: "POST", headers: { ...extra } };
    if (body !== undefined) {
      init.body = JSON.stringify(body);
      (init.headers as Record<string, string>)["content-type"] = "application/json";
    }
    if (signal !== undefined) init.signal = signal;
    return (await transport.request(path, init)).res;
  };
  /** As [`post`], plus the "resent after a network error" flag — see `Sent.resent`. */
  const postSent = async (path: string, body: unknown, extra: Record<string, string> = {}, signal?: AbortSignal) => {
    const init: RequestInit = { method: "POST", headers: { ...extra } };
    if (body !== undefined) {
      init.body = JSON.stringify(body);
      (init.headers as Record<string, string>)["content-type"] = "application/json";
    }
    if (signal !== undefined) init.signal = signal;
    return transport.request(path, init);
  };
  const json = async <T>(res: Response): Promise<T> => {
    if (!res.ok) throw await httpError(res);
    return (await res.json()) as T;
  };

  /**
   * Waits via attach (`path` takes `?wait_ms=`) until done or until `timeoutMs` runs out.
   * Attach is a read — a `503` past the transport's cap or a network error means ask
   * again, with a cap on consecutive attempts.
   */
  const attachLoop = async (
    id: string,
    path: string,
    o: { timeoutMs?: number | undefined; signal?: AbortSignal | undefined } = {},
  ): Promise<unknown> => {
    const deadline = o.timeoutMs === undefined ? Number.POSITIVE_INFINITY : Date.now() + o.timeoutMs;
    let failures = 0;
    for (;;) {
      const left = deadline - Date.now();
      if (left <= 0) throw new AttachTimeout(id);
      const wait = Math.max(0, Math.floor(Math.min(attachWaitMs, left)));
      let res: Response;
      try {
        res = await post(`${path}?wait_ms=${wait}`, undefined, {}, o.signal);
      } catch (e) {
        if (o.signal?.aborted || ++failures > 10) throw e;
        await sleep(backoffMs(failures), o.signal);
        continue;
      }
      // Attach is a read: `503` (no leader yet) and a `429` without an id (the per-tenant
      // cap on long-lived connections — `too_many_open_streams`) both mean "ask again
      // later", not an outcome. Throwing here would make the caller `call()` again, i.e.
      // a second invocation.
      if (res.status === 503 || (res.status === 429 && !(await carriesId(res)))) {
        if (++failures > 10) {
          const e = await httpError(res);
          e.invocationId = id;
          throw e;
        }
        const after = Number(res.headers.get("retry-after") ?? "");
        await sleep(Math.max(backoffMs(failures), Number.isFinite(after) ? after * 1000 : 0), o.signal);
        continue;
      }
      failures = 0;
      const s = await settle(res);
      if (s.done) return s.value;
    }
  };

  const invocation = <O>(id: string): InvocationHandle<O> => {
    const enc = encodeURIComponent(id);
    return {
      id,
      attach: async (o = {}) => (await attachLoop(id, `/invocations/${enc}/attach`, o)) as O,
      output: async () => {
        const s = await settle(await post(`/invocations/${enc}/attach?wait_ms=0`, undefined));
        return s.done ? { ready: true, value: s.value as O } : { ready: false };
      },
      status: async () => json<InvocationStatusView>((await transport.request(`/invocations/${enc}`)).res),
      signal: async (name, value) =>
        json<SignalAck>(
          await post(`/invocations/${enc}/signal`, value === undefined ? { signal: name } : { signal: name, value }),
        ),
      cancel: async (o = {}) =>
        json<CancelAck>(await post(`/invocations/${enc}/cancel${(o.mode ?? "signal") === "signal" ? "?mode=signal" : ""}`, undefined)),
    };
  };

  /** Headers of a call; the idempotency key is generated once and kept across every attempt. */
  const callHeaders = (o: SendOptions, autoKey: boolean): Record<string, string> => {
    const h: Record<string, string> = { ...o.headers };
    const key = o.idempotencyKey === undefined ? (autoKey ? globalThis.crypto.randomUUID() : null) : o.idempotencyKey;
    if (key !== null) h["idempotency-key"] = key;
    if (o.limitKey !== undefined) h["x-cairn-limit-key"] = o.limitKey;
    if (o.logLevel !== undefined) h["cairnflow-log-level"] = o.logLevel;
    return h;
  };

  /** `/invoke/{svc}/{h}[/send][?key=]`. */
  const invokePath = (prefix: string, svc: string, handler: string, key: string | undefined, send: boolean) =>
    `${prefix}/invoke/${encodeURIComponent(svc)}/${encodeURIComponent(handler)}${send ? "/send" : ""}${
      key === undefined ? "" : `?key=${encodeURIComponent(key)}`
    }`;

  const target = (svc: string, key: string | undefined, t: TargetOptions | undefined, workflowRun: boolean) => {
    const prefix = t?.scope === undefined ? "" : `/scope/${encodeURIComponent(t.scope)}`;
    const call = async (handler: string, input: unknown, o: CallOptions = {}): Promise<unknown> => {
      const auto = !(workflowRun && handler === "run");
      const sent = await postSent(invokePath(prefix, svc, handler, key, false), input, callHeaders(o, auto), o.signal);
      let s: Settled;
      try {
        s = await settle(sent.res);
      } catch (e) {
        // A workflow's `run` has no key: an earlier send that failed with a network error may
        // have created this very invocation — wait for it instead of reporting a duplicate.
        if (!(e instanceof AlreadyInvoked && sent.resent && !auto)) throw e;
        s = { done: false, id: e.invocationId };
      }
      if (s.done) return s.value;
      return attachLoop(s.id, `/invocations/${encodeURIComponent(s.id)}/attach`, o);
    };
    const send = async (handler: string, input: unknown, o: SendOptions = {}): Promise<InvocationHandle<unknown>> => {
      const auto = !(workflowRun && handler === "run");
      const sent = await postSent(invokePath(prefix, svc, handler, key, true), input, callHeaders(o, auto), o.signal);
      const res = sent.res;
      const id = res.headers.get("x-cairn-invocation-id");
      // `/send` answers `202` once durable; `200`/`500`/`409` when a call with the same
      // idempotency key already has an outcome — still the same invocation.
      if (id !== null && [200, 202, 409, 429, 500].includes(res.status)) {
        // A `409 paused` with the same key is still a handle; only an `already_invoked`
        // of **someone else** (a workflow run, not our own resend) throws.
        if (res.status === 409 && !(auto || sent.resent)) {
          const status = ((await res.clone().json().catch(() => ({}))) as { status?: string }).status;
          if (status === "already_invoked") await settle(res);
        }
        await res.body?.cancel();
        return invocation(id);
      }
      throw await httpError(res);
    };
    const sender = new Proxy(
      (input?: unknown, o?: CallOptions) => call("send", input, o),
      {
        get: (fn, prop) =>
          typeof prop === "string" && !NOT_THENABLE.has(prop)
            ? (input?: unknown, o?: SendOptions) => send(prop, input, o)
            : Reflect.get(fn, prop),
      },
    );
    return { prefix, call, sender };
  };

  const handlerProxy = (t: ReturnType<typeof target>, extra: Record<string, unknown> = {}) =>
    new Proxy(extra, {
      get: (obj, prop) => {
        if (typeof prop !== "string" || NOT_THENABLE.has(prop)) return undefined;
        if (prop === "send") return t.sender;
        if (prop in obj) return obj[prop];
        return (input?: unknown, o?: CallOptions) => t.call(prop, input, o);
      },
    });

  return {
    service: <D extends ServiceLike>(name: string, o?: TargetOptions) =>
      handlerProxy(target(name, undefined, o, false)) as ServiceClient<D>,
    object: <D extends ServiceLike>(name: string, key: string, o?: TargetOptions) =>
      handlerProxy(target(name, key, o, false)) as ServiceClient<D>,
    workflow: <D extends ServiceLike>(name: string, key: string, o?: TargetOptions) => {
      const t = target(name, key, o, true);
      const path = `${t.prefix}/workflow/${encodeURIComponent(name)}/${encodeURIComponent(key)}/attach`;
      return handlerProxy(t, {
        attach: (a: { timeoutMs?: number; signal?: AbortSignal } = {}) => attachLoop(`${name}/${key}`, path, a),
        output: async () => {
          const s = await settle(await post(`${path}?wait_ms=0`, undefined));
          return s.done ? { ready: true, value: s.value } : { ready: false };
        },
      }) as unknown as WorkflowClient<D>;
    },
    invocation,
    awakeable: (id) => {
      const base = `/awakeables/${encodeURIComponent(id)}`;
      return {
        resolve: async (value) => json<SignalAck>(await post(`${base}/resolve`, value)),
        reject: async (message, code) =>
          json<SignalAck>(await post(`${base}/reject`, code === undefined ? { message } : { message, code })),
      };
    },
    schedules: (() => {
      const base = (o: TargetOptions | undefined) =>
        `${o?.scope === undefined ? "" : `/scope/${encodeURIComponent(o.scope)}`}/schedules`;
      const one = (name: string, o: TargetOptions | undefined) => `${base(o)}/${encodeURIComponent(name)}`;
      const send = async (path: string, method: string, body?: unknown, extra: Record<string, string> = {}) => {
        const init: RequestInit = { method, headers: { ...extra } };
        if (body !== undefined) {
          init.body = JSON.stringify(body);
          (init.headers as Record<string, string>)["content-type"] = "application/json";
        }
        return (await transport.request(path, init)).res;
      };
      /** `404` ⇒ `missing`; otherwise as {@link json}. */
      const orMissing = async <T, M>(res: Response, missing: M): Promise<T | M> => {
        if (res.status === 404) {
          await res.body?.cancel();
          return missing;
        }
        return json<T>(res);
      };
      return {
        put: async (name, def, o) => json<ScheduleView>(await send(one(name, o), "PUT", def)),
        get: async (name, o) => orMissing<ScheduleView, undefined>(await send(one(name, o), "GET"), undefined),
        delete: async (name, o) =>
          (await orMissing<{ deleted: boolean }, false>(await send(one(name, o), "DELETE"), false)) !== false,
        pause: async (name, o) => json<ScheduleView>(await send(`${one(name, o)}/pause`, "POST")),
        resume: async (name, o) => json<ScheduleView>(await send(`${one(name, o)}/resume`, "POST")),
        trigger: async (name, o) => {
          // The key is generated once and kept across every transport attempt: a retry
          // after a lost response lands on the run already created.
          const key = o?.idempotencyKey ?? globalThis.crypto.randomUUID();
          const r = await json<{ id: string; created: boolean }>(
            await send(`${one(name, o)}/trigger`, "POST", undefined, { "idempotency-key": key }),
          );
          return invocation(r.id);
        },
        list: async (o) => json<ScheduleList>(await send(base(o), "GET")),
      } satisfies SchedulesClient;
    })(),
    promise: (service, key, name, o) => {
      const prefix = o?.scope === undefined ? "" : `/scope/${encodeURIComponent(o.scope)}`;
      const base = `${prefix}/promises/${[service, key, name].map(encodeURIComponent).join("/")}`;
      return {
        resolve: async (value) => json<PromiseAck>(await post(`${base}/resolve`, value)),
        reject: async (message, code) =>
          json<PromiseAck>(await post(`${base}/reject`, code === undefined ? { message } : { message, code })),
        peek: async () => json<PromiseView>((await transport.request(base)).res),
      };
    },
  };
}
