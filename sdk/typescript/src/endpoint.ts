// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * The **platform-independent** part of the SDK endpoint: service declaration, the
 * manifest (protocol-v1 §3), and the `REQUEST_RESPONSE` core (protocol-v1 §4) — one
 * request in, one response out.
 *
 * No `node:http*`/`node:net` import: [`serve`](./serve.ts) connects this core to
 * `node:http` (plus h2c for `BIDI_STREAM`), [`fetchHandler`](./fetch.ts) connects it
 * to the Fetch API (Vercel, Cloudflare Workers, Lambda, Deno, Bun). Both paths run
 * the same core, so the `REQUEST_RESPONSE` conformance traces cover both.
 */

import { type AnyHandler, type LogLevel, runTurn, type ServiceHandler, withLogLevel } from "./handler.ts";
import { bodyHash, DISCOVER_JTI, expectedJti, IdentityVerifier, SCHEME_V1 } from "./identity.ts";
import { extendChain, chainRoot, ReplayCache, type ReplayCacheOptions } from "./replay.ts";
import { SegmentCache, WIRE_SEGMENTED } from "./values.ts";
import { decodeFrames, decodeStart, encodeFrame, type Frame, MT } from "./wire.ts";

export { WIRE_SEGMENTED } from "./values.ts";

export const DISCOVERY_V1 = "application/vnd.cairnflow.endpointmanifest.v1+json";
export const INVOCATION_V1 = "application/vnd.cairnflow.invocation.v1";

/** Wire versions this SDK speaks (protocol-v1 §2). */
export const MIN_PROTOCOL_VERSION = 1;
/**
 * v2 = flow control (`scope`, `limit_key`). Declaring v2 here tells the runtime
 * that this SDK *understands* those two fields; the runtime settles the version and
 * answers with the `/invoke` content-type, so that `ctx.scoped(...)` refuses when
 * the other end only speaks v1 instead of sending a scope that would be silently ignored.
 *
 * v3 = `KEEPALIVE` during long `ctx.run` closures and the `replay_only` flag.
 *
 * v4 = the `StreamAppend` side channel (`ctx.stream`).
 *
 * v5 = incremental `REQUEST_RESPONSE` replay (`replayCache`, protocol-v1 §9.3).
 *
 * v6 = segmented values: large state comes down as ref + tail with read URLs,
 * the SDK assembles on receipt (`valueCacheBytes`) and `set` reuses the refs of
 * an already-read prefix.
 */
export const MAX_PROTOCOL_VERSION = 6;

/** First wire version with incremental replay (protocol-v1 §9.3). */
export const WIRE_REPLAY_DELTA = 5;

/** The version the runtime speaks, read from the content-type of the `/invoke` request. */
export function wireVersionOf(contentType: string | undefined): number {
  const media = (contentType ?? "").split(";")[0]!.trim().toLowerCase();
  const m = /^application\/vnd\.cairnflow\.invocation\.v(\d+)$/.exec(media);
  // Unreadable ⇒ **assume v1**, the safe side: `ctx.scoped` fails with a clear
  // error instead of sending a scope that might be ignored.
  return m ? Number(m[1]) : MIN_PROTOCOL_VERSION;
}

export type ServiceType = "SERVICE" | "VIRTUAL_OBJECT" | "WORKFLOW";
export type HandlerType = "EXCLUSIVE" | "SHARED" | "WORKFLOW";
export type ProtocolMode = "BIDI_STREAM" | "REQUEST_RESPONSE";


/**
 * What remains when the user gives up after `maxAttempts` attempts: `PAUSE` keeps
 * the invocation waiting for `resume`, `KILL` ends it with an error. Same names as
 * Restate's.
 */
export type OnMaxAttempts = "PAUSE" | "KILL";

/** Retry policy of a service or a handler. All durations are ms. */
export interface RetryPolicy {
  readonly initialInterval?: number;
  readonly maxInterval?: number;
  /** Backoff multiplier between two attempts. */
  readonly exponentiationFactor?: number;
  /** Total number of attempts, **including the first**. Absent = unlimited. */
  readonly maxAttempts?: number;
  readonly onMaxAttempts?: OnMaxAttempts;
}

/**
 * The contract of a service or handler. A handler's declaration overrides the
 * service's; when neither declares, the runtime uses the node's default. The values
 * are captured into the deployment at registration — changing them means
 * registering a new deployment.
 */
export interface HandlerOptions {
  /** Cap on how long the SDK may stay silent within a turn, ms. */
  readonly inactivityTimeout?: number;
  readonly retryPolicy?: RetryPolicy;
  /** How long to keep the journal of a finished invocation, ms. */
  readonly journalRetention?: number;
  /**
   * Minimum level of `ctx.log` (default `info`). Used by the SDK only — not in the
   * manifest; the invocation's `cairnflow-log-level` header overrides it.
   */
  readonly logLevel?: LogLevel;
  /**
   * `"runtime"` ⇒ the runtime writes one span per turn and per `ctx.run` into
   * `$trace`. Default off.
   */
  readonly trace?: "runtime" | "off";
  /**
   * Flush durable stream records (`ctx.log`, spans, `durable: true`) that have not
   * ridden along with any commit at this interval, ms. Keeps logs when a node dies
   * in the middle of a long `ctx.run`, at the cost of one commit per interval.
   * Default off. Only has an effect with `BIDI_STREAM`.
   */
  readonly logFlushMs?: number;
}

/**
 * A declared service. The two type parameters live only at the type level:
 * `connect()` reads handler names, inputs and outputs from `typeof svc` without
 * importing the service's code. The defaults keep the old shape — a bare
 * `ServiceDefinition` accepts any service.
 */
export interface ServiceDefinition<
  Name extends string = string,
  Handlers extends Readonly<Record<string, AnyHandler>> = Readonly<Record<string, AnyHandler>>,
> {
  readonly name: Name;
  readonly type: ServiceType;
  readonly handlers: Handlers;
  readonly handlerTypes?: Readonly<Record<string, HandlerType>> | undefined;
  readonly options?: HandlerOptions | undefined;
  readonly handlerOptions?: Readonly<Record<string, HandlerOptions>> | undefined;
}

interface ServiceOpts {
  handlerTypes?: Record<string, HandlerType>;
  /** Contract shared by every handler of the service. */
  options?: HandlerOptions;
  /** Per-handler contracts — override `options`. */
  handlerOptions?: Record<string, HandlerOptions>;
}

/**
 * Declares a service.
 *
 * Two forms, chosen by `opts.type`:
 *
 * - **Plain service** (default, `type: "SERVICE"`): handlers receive a
 *   [`ServiceContext`] — no state, no promises. An unkeyed service has no object
 *   for state to belong to.
 * - **Virtual object / workflow** (`type: "VIRTUAL_OBJECT"` or `"WORKFLOW"`):
 *   handlers receive the full `Context` — called with a key, state belongs to the
 *   object with that key.
 */
export function service<const N extends string, H extends Record<string, ServiceHandler>>(
  name: N,
  handlers: H,
  opts?: { type?: "SERVICE" } & ServiceOpts,
): ServiceDefinition<N, H>;
export function service<const N extends string, H extends Record<string, AnyHandler>>(
  name: N,
  handlers: H,
  opts: { type: "VIRTUAL_OBJECT" | "WORKFLOW" } & ServiceOpts,
): ServiceDefinition<N, H>;
export function service(
  name: string,
  handlers: Record<string, AnyHandler> | Record<string, ServiceHandler>,
  opts: { type?: ServiceType } & ServiceOpts = {},
): ServiceDefinition {
  if (Object.keys(handlers).length === 0) {
    throw new Error(`service ${JSON.stringify(name)} has no handlers`);
  }
  return {
    name,
    type: opts.type ?? "SERVICE",
    // They differ only in type: at run time every handler receives the same `Context`.
    handlers: handlers as Record<string, AnyHandler>,
    handlerTypes: opts.handlerTypes,
    options: opts.options,
    handlerOptions: opts.handlerOptions,
  };
}

/**
 * Declares a workflow: **one** `run` handler, and any number of `SHARED`
 * handlers.
 *
 * ```ts
 * const signup = workflow("UserSignup", {
 *   run: async (ctx, user: User) => {
 *     await ctx.run("create", () => createUser(ctx.key, user));
 *     const clicked = await ctx.promise<string>("email-link-clicked").get();
 *     return clicked === secret;
 *   },
 *   click: async (ctx, secret: string) => {
 *     await ctx.promise<string>("email-link-clicked").resolve(secret);
 *   },
 * });
 * ```
 *
 * `run` is **required and unique** — the runtime refuses to register a workflow
 * that does not declare exactly one `WORKFLOW` handler, because the run's id is a
 * function of `(scope, service, key)` and carries no handler name. This function
 * names it `run` so that rule does not have to be restated at every use site.
 *
 * `ctx.key` in `run` is the **workflow id**. It runs exactly once per value;
 * a second call gets `409`, not the result of the first run.
 */
export function workflow<const N extends string, H extends { run: AnyHandler } & Record<string, AnyHandler>>(
  name: N,
  handlers: H,
  opts: Omit<ServiceOpts, "handlerTypes"> = {},
): ServiceDefinition<N, H> {
  const handlerTypes: Record<string, HandlerType> = {};
  for (const h of Object.keys(handlers)) {
    handlerTypes[h] = h === "run" ? "WORKFLOW" : "SHARED";
  }
  return service(name, handlers, { type: "WORKFLOW", handlerTypes, ...opts });
}

/**
 * Manifest fields of a contract — **same names as Restate's endpoint manifest**
 * (`inactivityTimeout`, `retryPolicyInitialInterval`, …): costs nothing, and keeps
 * the path to compatibility open.
 */
function contractFields(o: HandlerOptions | undefined): Record<string, unknown> {
  if (o === undefined) return {};
  const out: Record<string, unknown> = {};
  const put = (k: string, v: unknown): void => {
    if (v !== undefined) out[k] = v;
  };
  put("inactivityTimeout", o.inactivityTimeout);
  put("journalRetention", o.journalRetention);
  put("trace", o.trace);
  put("logFlushMs", o.logFlushMs);
  const r = o.retryPolicy;
  if (r !== undefined) {
    put("retryPolicyInitialInterval", r.initialInterval);
    put("retryPolicyMaxInterval", r.maxInterval);
    put("retryPolicyExponentiationFactor", r.exponentiationFactor);
    put("retryPolicyMaxAttempts", r.maxAttempts);
    put("retryPolicyOnMaxAttempts", r.onMaxAttempts);
  }
  return out;
}

/**
 * The `inactivityTimeout` the manifest declares at service level when the service
 * declares none: 15 minutes. In `REQUEST_RESPONSE` this cap covers the **whole** turn — a `ctx.run` calling a model for longer than
 * the node's default cap (60 s) would be dropped and rerun forever. The runtime clamps it to the cell's cap (SaaS
 * profile: 15 minutes). A value declared by the handler or service wins.
 */
export const DEFAULT_INACTIVITY_TIMEOUT_MS = 15 * 60 * 1000;

export function manifest(
  services: readonly ServiceDefinition[],
  protocolMode: ProtocolMode = "REQUEST_RESPONSE",
  maxProtocolVersion: number = MAX_PROTOCOL_VERSION,
  verifiesIdentity = false,
  compression = false,
  replayCache = false,
): unknown {
  return {
    protocolMode,
    minProtocolVersion: MIN_PROTOCOL_VERSION,
    maxProtocolVersion,
    // Tell the runtime this endpoint verifies request identity: the runtime records
    // it in the deployment; the SaaS profile refuses deployments without it.
    ...(verifiesIdentity ? { requestIdentity: SCHEME_V1 } : {}),
    // Accept gzip-compressed REQUEST_RESPONSE bodies (same shape as the Restate SDK's
    // `lambdaCompression`). Old runtimes ignore it ⇒ plain bodies.
    ...(compression ? { requestCompression: "gzip" } : {}),
    // Keep the journal of running invocations, accept deltas on wire v5 (protocol-v1 §9.3).
    // Old runtimes ignore it ⇒ full replay.
    ...(replayCache && maxProtocolVersion >= WIRE_REPLAY_DELTA ? { replayCache: true } : {}),
    services: services.map((s) => ({
      name: s.name,
      ty: s.type,
      inactivityTimeout: DEFAULT_INACTIVITY_TIMEOUT_MS,
      ...contractFields(s.options),
      handlers: Object.keys(s.handlers).map((h) => ({
        name: h,
        ...(s.handlerTypes?.[h] !== undefined ? { ty: s.handlerTypes[h] } : {}),
        ...contractFields(s.handlerOptions?.[h]),
      })),
    })),
  };
}

/** Reads a header by its lower-case name. */
export type HeaderOf = (name: string) => string | undefined;

/** Checks the user's `maxProtocolVersion` — out of range is a configuration error. */
export function checkMaxWire(maxWire: number): number {
  if (!Number.isInteger(maxWire) || maxWire < MIN_PROTOCOL_VERSION || maxWire > MAX_PROTOCOL_VERSION) {
    throw new RangeError(
      `maxProtocolVersion ${maxWire} is outside [${MIN_PROTOCOL_VERSION}, ${MAX_PROTOCOL_VERSION}]`,
    );
  }
  return maxWire;
}

/**
 * The wire cap an endpoint declares. Wire v6 makes the endpoint fetch the URLs in
 * `Start.segment_urls`: without `identityKeys` that body is whoever's reaches the endpoint,
 * and without `segmentOrigins` any http(s) URL is fetched — so such an endpoint declares v5
 * (the runtime then sends values inline), and pinning v6 there is a configuration error.
 */
export function wireCap(maxWire: number | undefined, verified: boolean, origins: boolean): number {
  if (maxWire === undefined) return verified || origins ? MAX_PROTOCOL_VERSION : WIRE_SEGMENTED - 1;
  checkMaxWire(maxWire);
  if (maxWire >= WIRE_SEGMENTED && !verified && !origins) {
    throw new RangeError(
      `maxProtocolVersion ${maxWire} (segmented values) needs identityKeys or segmentOrigins: without them ` +
        "anyone who reaches this endpoint can make it fetch URLs of their choosing",
    );
  }
  return maxWire;
}

/** Whether the "identity not verified" warning was already printed (once per process). */
let warnedUnverified = false;

/**
 * Warns once per process when the endpoint has no `identityKeys`.
 * `process.emitWarning` when available (Node, Workers `nodejs_compat`), otherwise
 * `console.warn`.
 */
export function warnUnverified(who: string): void {
  if (warnedUnverified) return;
  warnedUnverified = true;
  const msg =
    `${who} has no identityKeys: anyone who can reach this endpoint can run your handlers on a forged ` +
    "journal, input and state (real side effects on fake data). Without them the REQUEST_RESPONSE replay " +
    "cache stays off, and segmented values (wire v6, which make the endpoint fetch segment URLs from " +
    "Start.segment_urls) are only served when segmentOrigins lists the runtime's value read origins. " +
    "Set identityKeys to the kid listed at the runtime's GET /identity";
  const proc = (globalThis as { process?: { emitWarning?: (m: string, o: { code: string }) => void } }).process;
  if (typeof proc?.emitWarning === "function") proc.emitWarning(msg, { code: "CAIRNFLOW_UNVERIFIED" });
  else console.warn(`CAIRNFLOW_UNVERIFIED: ${msg}`);
}

/**
 * The parts of the request the JWT binds to: the host the request went to (or the
 * trusted hosts when a proxy between the runtime and the SDK rewrites `Host`), and
 * the body before compression. Absent ⇒ that part is not checked (headers are
 * checked before the body is read).
 */
export interface Bound {
  readonly host?: string | undefined;
  readonly hosts?: readonly string[] | undefined;
  readonly body?: Uint8Array | undefined;
}

/** `ok` with the verified `jti` (and `bsh`), or the reason for refusal. */
export async function checkIdentity(
  verifier: IdentityVerifier | undefined,
  header: HeaderOf,
  path: string,
  jti: string | ((claimed: string) => boolean),
  bound: Bound = {},
): Promise<{ ok: true; jti: string; bsh?: string; exp?: number } | { ok: false; why: string }> {
  if (verifier === undefined) return { ok: true, jti: "" };
  try {
    const c = await verifier.verify(header, path);
    if (typeof jti === "string" ? c.jti !== jti : !jti(c.jti)) {
      return { ok: false, why: "jti does not match the request" };
    }
    // A present claim must match (old runtimes do not sign them). Without
    // `identityHosts`, compare with the request's own host — sender-chosen text:
    // it only stops a JWT copied to an endpoint on another host when that endpoint
    // routes by host. A request carrying no host ⇒ nothing to compare ⇒ refused.
    if (c.host !== undefined) {
      const trusted = bound.hosts !== undefined && bound.hosts.length > 0 ? bound.hosts : bound.host === undefined ? [] : [bound.host];
      if (!trusted.some((h) => h.toLowerCase() === c.host!.toLowerCase())) {
        return { ok: false, why: `JWT was signed for host ${c.host}` };
      }
    }
    if (c.bsh !== undefined && bound.body !== undefined && (await bodyHash(bound.body)) !== c.bsh) {
      return { ok: false, why: "body does not match the signed hash" };
    }
    return { ok: true, jti: c.jti, exp: c.exp, ...(c.bsh === undefined ? {} : { bsh: c.bsh }) };
  } catch (e) {
    return { ok: false, why: e instanceof Error ? e.message : String(e) };
  }
}

/** An endpoint's caps on assembling segmented values. */
export interface ValueLimits {
  readonly maxValueBytes?: number | undefined;
  readonly maxTurnValueBytes?: number | undefined;
}

/** The core's response: status, content-type, body (and `content-encoding` if compressed). */
export interface Answer {
  readonly status: number;
  readonly contentType: string;
  readonly body: Uint8Array;
  readonly contentEncoding?: "gzip";
  /** Extra response headers (wire v5: `cairn-replay-cache`, `cairn-replay`, `cairn-affinity`). */
  readonly headers?: Readonly<Record<string, string>>;
}

/** The process's Fly machine id (`FLY_MACHINE_ID`) — alphanumeric, ≤ 64 characters — or `undefined`. */
function flyMachineId(): string | undefined {
  const env = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env;
  const id = env?.FLY_MACHINE_ID;
  return id !== undefined && /^[A-Za-z0-9]{1,64}$/.test(id) ? id : undefined;
}

function hexOf(b: Uint8Array): string {
  let s = "";
  for (const x of b) s += x.toString(16).padStart(2, "0");
  return s;
}

function concat(parts: readonly Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let at = 0;
  for (const p of parts) {
    out.set(p, at);
    at += p.length;
  }
  return out;
}

const raw = (f: Frame): Uint8Array => encodeFrame(f.type, f.flags, f.payload);

/** Responses smaller than this go uncompressed — the same threshold as on the runtime side. */
export const COMPRESS_MIN = 8 << 10;
/** Cap on the request body **after decompression** (a "zip bomb" gets `413`). */
export const MAX_REQUEST_BODY = 256 << 20;

/** The request body exceeds [`MAX_REQUEST_BODY`] — the body reader throws, the core answers `413`. */
export class BodyTooLarge extends Error {
  override readonly name = "BodyTooLarge";
}

async function collect(stream: ReadableStream<Uint8Array>, max: number): Promise<Uint8Array | null> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.length;
    if (total > max) {
      await reader.cancel();
      return null;
    }
    chunks.push(value);
  }
  const out = new Uint8Array(total);
  let at = 0;
  for (const c of chunks) {
    out.set(c, at);
    at += c.length;
  }
  return out;
}

/** The body is gzip (first bytes `1f 8b`) — not when the platform already decompressed it. */
function looksGzip(b: Uint8Array): boolean {
  return b.length >= 2 && b[0] === 0x1f && b[1] === 0x8b;
}

/**
 * gzip via Web Streams (`CompressionStream`): Node ≥ 18, Deno, Bun, Workers —
 * no `node:zlib`, so the core runs on every platform. A new compression stream each time.
 */
export async function gzip(bytes: Uint8Array): Promise<Uint8Array> {
  const src = new Blob([bytes as Uint8Array<ArrayBuffer>]).stream();
  return (await collect(src.pipeThrough(new CompressionStream("gzip")), Number.MAX_SAFE_INTEGER))!;
}

/**
 * gzip of a response body, **one gzip member per frame** (RFC 1952 allows
 * concatenated members; the runtime decodes every member). No compression across
 * frames — a defence against the BREACH class, the same rule as on the runtime side:
 * `CompressionStream` has no full flush, so each frame gets a new compression stream.
 * A body that cannot be split into frames ⇒ `null` (sent uncompressed).
 */
export async function gzipFrames(bytes: Uint8Array): Promise<Uint8Array | null> {
  const parts: Uint8Array[] = [];
  let at = 0;
  while (at < bytes.length) {
    if (bytes.length - at < 8) return null;
    const view = new DataView(bytes.buffer, bytes.byteOffset + at, 8);
    const end = at + 8 + view.getUint32(4);
    if (end > bytes.length) return null;
    parts.push(await gzip(bytes.subarray(at, end)));
    at = end;
  }
  if (parts.length === 0) return null;
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let o = 0;
  for (const p of parts) {
    out.set(p, o);
    o += p.length;
  }
  return out;
}

/** Gunzips, `null` when over `max` bytes after decompression; broken gzip ⇒ throws. */
export async function gunzip(bytes: Uint8Array, max: number): Promise<Uint8Array | null> {
  const src = new Blob([bytes as Uint8Array<ArrayBuffer>]).stream();
  return collect(src.pipeThrough(new DecompressionStream("gzip")), max);
}

/** Whether `accept-encoding` invites gzip. */
export function acceptsGzip(v: string | undefined): boolean {
  return (v ?? "")
    .split(",")
    .some((p) => p.trim().split(";")[0]!.trim().toLowerCase() === "gzip");
}

function jsonAnswer(status: number, body: unknown, contentType = "application/json"): Answer {
  return { status, contentType, body: new TextEncoder().encode(JSON.stringify(body)) };
}

/**
 * The one-request-one-response core: `GET /discover` and `POST /invoke/{s}/{h}` in
 * `REQUEST_RESPONSE` mode. Knows nothing about the HTTP server — the caller passes
 * the method, path, headers and a way to read the body.
 */
export class EndpointCore {
  readonly services: readonly ServiceDefinition[];
  /** Mode declared in the manifest. */
  readonly mode: ProtocolMode;
  readonly maxWire: number;
  /** `undefined` = request identity not verified (warned once). */
  readonly verifier: IdentityVerifier | undefined;
  /** Declare and accept gzip compression of REQUEST_RESPONSE bodies. */
  readonly compression: boolean;
  /** Replay cache (wire v5, protocol-v1 §9.3); `undefined` = `replayCache` not declared. */
  readonly replay: ReplayCache | undefined;
  /** Segment cache for segmented values (wire v6), shared by every turn of the endpoint. */
  readonly values: SegmentCache;
  /** Trusted hosts for the `host` claim — absent ⇒ the request's own host. */
  readonly identityHosts: readonly string[] | undefined;
  /** Trusted segment read URL origins — absent ⇒ any `http(s)`. */
  readonly segmentOrigins: readonly string[] | undefined;
  /** Caps on assembling segmented values — absent ⇒ the defaults of `values.ts`. */
  readonly valueLimits: ValueLimits;
  private readonly byName: ReadonlyMap<string, ServiceDefinition>;

  constructor(
    services: readonly ServiceDefinition[],
    mode: ProtocolMode,
    maxWire: number | undefined,
    identityKeys: readonly string[] | undefined,
    compression = true,
    replayCache?: boolean | ReplayCacheOptions,
    valueCacheBytes?: number,
    identityHosts?: readonly string[],
    segmentOrigins?: readonly string[],
    valueLimits: ValueLimits = {},
  ) {
    this.valueLimits = valueLimits;
    this.identityHosts = identityHosts;
    this.segmentOrigins = segmentOrigins;
    this.services = services;
    this.mode = mode;
    this.verifier = identityKeys !== undefined ? new IdentityVerifier(identityKeys) : undefined;
    this.maxWire = wireCap(maxWire, this.verifier !== undefined, (segmentOrigins ?? []).length > 0);
    this.compression = compression;
    // Only `REQUEST_RESPONSE` at wire ≥ 5 has deltas: BIDI keeps the journal on the stream.
    // The cache needs request identity: a delta names an invocation id and a prefix digest,
    // and a `409` reports what is held — without identity anyone who reaches the endpoint
    // could probe for running ids and run the handler on a held journal. `undefined` ⇒ on
    // exactly when identity is verified.
    const usable = mode === "REQUEST_RESPONSE" && this.maxWire >= WIRE_REPLAY_DELTA;
    const want = replayCache ?? this.verifier !== undefined;
    if (want !== false && usable && this.verifier === undefined) {
      throw new RangeError(
        "replayCache needs identityKeys: without request identity anyone who reaches this endpoint can " +
          "replay the journal of a running invocation it holds; set identityKeys or leave replayCache off",
      );
    }
    this.replay = want !== false && usable ? new ReplayCache(want === true ? {} : want) : undefined;
    this.values = new SegmentCache(valueCacheBytes);
    this.byName = new Map(services.map((s) => [s.name, s]));
  }

  /**
   * The version the runtime speaks, clamped to `maxWire`: if the runtime speaks a
   * higher version, the SDK still only uses the declared version's features.
   */
  wireOf(contentType: string | undefined): number {
    return Math.min(wireVersionOf(contentType), this.maxWire);
  }

  /**
   * Error when the runtime speaks a version **higher** than the declared cap: the
   * deployment settled on the new version but the SDK process was downgraded to an
   * old build without re-registering. Clamping to the cap would mean reading data in
   * another build's layout (v6: `content` is only the tail of a segmented value) —
   * answer `415` instead of running the handler.
   */
  tooNew(contentType: string | undefined): string | undefined {
    const asked = wireVersionOf(contentType);
    if (asked <= this.maxWire) return undefined;
    return `the runtime speaks wire v${asked}, this SDK supports up to v${this.maxWire}; register the deployment again`;
  }

  /** Handler for `POST /invoke/{service}/{handler}`, with its declared log level. */
  route(path: string, method: string): AnyHandler | undefined {
    const m = /^\/invoke\/([^/]+)\/([^/]+)$/.exec(path);
    if (method !== "POST" || m === null) return undefined;
    const svc = this.byName.get(decodeURIComponent(m[1]!));
    const name = decodeURIComponent(m[2]!);
    const h = svc?.handlers[name];
    const level: LogLevel | undefined = svc?.handlerOptions?.[name]?.logLevel ?? svc?.options?.logLevel;
    return h === undefined || level === undefined ? h : withLogLevel(h, level);
  }

  /** The host the request went to (`Host` header) and the trusted hosts — see [`Bound`]. */
  bound(header: HeaderOf): Bound {
    return { host: header("host"), hosts: this.identityHosts };
  }

  manifest(): unknown {
    return manifest(
      this.services,
      this.mode,
      this.maxWire,
      this.verifier !== undefined,
      this.compression,
      this.replay !== undefined,
    );
  }

  /**
   * Answers one request. `route` is the routing path (with the endpoint's mount
   * prefix removed, if any); `signedPath` is the path the runtime sent — what the
   * identity JWT signs as `aud`.
   */
  async answer(
    method: string,
    signedPath: string,
    route: string,
    header: HeaderOf,
    body: () => Promise<Uint8Array>,
  ): Promise<Answer> {
    if (method === "GET" && route === "/discover") {
      const id = await checkIdentity(this.verifier, header, signedPath, DISCOVER_JTI, this.bound(header));
      if (!id.ok) return jsonAnswer(401, { error: id.why });
      return jsonAnswer(200, this.manifest(), DISCOVERY_V1);
    }
    const handler = this.route(route, method);
    if (handler !== undefined) {
      const tooNew = this.tooNew(header("content-type"));
      if (tooNew !== undefined) return jsonAnswer(415, { error: tooNew });
      // Header signature **before** reading the body: a stranger cannot push a body
      // of any size into the endpoint's RAM. The body (`bsh`, `jti`) is checked afterwards.
      const pre = await checkIdentity(this.verifier, header, signedPath, () => true, this.bound(header));
      if (!pre.ok) return jsonAnswer(401, { error: pre.why });
      const declared = Number(header("content-length") ?? Number.NaN);
      if (Number.isFinite(declared) && declared > MAX_REQUEST_BODY) {
        return jsonAnswer(413, { error: `body exceeds ${MAX_REQUEST_BODY} bytes` });
      }
      const read = await body().catch((e: unknown) => (e instanceof BodyTooLarge ? null : Promise.reject(e)));
      if (read === null) return jsonAnswer(413, { error: `body exceeds ${MAX_REQUEST_BODY} bytes` });
      let bytes = read;
      const enc = header("content-encoding")?.trim().toLowerCase();
      if (enc !== undefined && enc !== "" && enc !== "identity") {
        if (enc !== "gzip" || !this.compression) {
          return jsonAnswer(415, { error: `content-encoding ${enc} is not enabled (requestCompression)` });
        }
        // The signature was checked above: a stranger cannot make the endpoint decode a large body.
        // The platform already decompressed it (header left as is) ⇒ plain body.
        if (looksGzip(bytes)) {
          const raw = await gunzip(bytes, MAX_REQUEST_BODY).catch(() => undefined);
          if (raw === undefined) return jsonAnswer(400, { error: "corrupt gzip body" });
          if (raw === null) return jsonAnswer(413, { error: `body exceeds ${MAX_REQUEST_BODY} bytes after decompression` });
          bytes = raw;
        }
      }
      // `jti` binds the JWT to the `Start` of this very body: a JWT copied from a
      // request of another invocation (or another attempt) does not pass.
      // `bsh` binds the JWT to the body **itself** (after decompression): a forged body
      // with the same `id`/`retryCount` does not pass.
      const id = await checkIdentity(
        this.verifier,
        header,
        signedPath,
        (claimed) => {
          const first = decodeFrames(bytes)[0];
          return first !== undefined && first.type === MT.START && expectedJti(decodeStart(first.payload)) === claimed;
        },
        { ...this.bound(header), body: bytes },
      );
      if (!id.ok) return jsonAnswer(401, { error: id.why });
      const wire = this.wireOf(header("content-type"));
      // Wire v5 (protocol-v1 §9.3) — only after the request passed the identity check.
      const turn = this.replay !== undefined && wire >= WIRE_REPLAY_DELTA ? await this.rejoin(bytes) : { bytes };
      if (typeof turn === "object" && "miss" in turn) {
        // Second round (protocol-v1 §9.3): include what is still held, `k:h_k` — the
        // runtime checks `h_k` against its journal and resends from `k`.
        const headers: Record<string, string> = { "cairn-replay": "miss" };
        if (turn.held !== undefined) headers["cairn-replay-held"] = `${turn.held.k}:${hexOf(turn.held.digest)}`;
        return { status: 409, contentType: INVOCATION_V1, body: new Uint8Array(), headers };
      }
      const out = await runTurn(handler, turn.bytes, wire, {
        cache: this.values,
        origins: this.segmentOrigins,
        ...this.valueLimits,
      });
      const headers: Record<string, string> = {};
      if (turn.keep !== undefined) {
        const k = await this.keep(turn.keep, out);
        if (k > 0) {
          headers["cairn-replay-cache"] = String(k);
          const fly = flyMachineId();
          if (fly !== undefined) headers["cairn-affinity"] = `fly=${fly}`;
        }
      }
      if (this.compression && out.length >= COMPRESS_MIN && acceptsGzip(header("accept-encoding"))) {
        const z = await gzipFrames(out);
        if (z !== null && z.length < out.length) {
          return { status: 200, contentType: INVOCATION_V1, body: z, contentEncoding: "gzip", headers };
        }
      }
      return { status: 200, contentType: INVOCATION_V1, body: out, headers };
    }
    return jsonAnswer(404, { error: `no route for ${method} ${route}` });
  }

  /**
   * The turn body after splicing in the cache (protocol-v1 §9.3): `replay_from = B > 0` ⇒
   * `Start` ‖ cache `[0, B)` ‖ the rest, when the cache has ≥ `B` entries, `h_B`
   * matches and the body still has ≥ `N − B` frames; otherwise `"miss"` — **before**
   * running anything. `keep` is what is kept after the turn (entries `[0, N)` and their chain).
   */
  private async rejoin(
    bytes: Uint8Array,
  ): Promise<{ miss: true; held?: { k: number; digest: Uint8Array } } | { bytes: Uint8Array; keep?: Keep }> {
    const cache = this.replay!;
    const frames = decodeFrames(bytes);
    const first = frames[0];
    if (first === undefined || first.type !== MT.START) return { bytes };
    const start = decodeStart(first.payload);
    const n = start.knownEntries;
    const b = start.replayFrom;
    const id = hexOf(start.id);
    const rest = frames.slice(1);
    if (b === 0) {
      // Full replay: if entries are missing compared with `known_entries`, the turn
      // fails by itself as in v4 — keep nothing.
      if (rest.length < n) return { bytes };
      const entries = rest.slice(0, n).map(raw);
      const chain = [await chainRoot(start.id)];
      await extendChain(chain, entries);
      return { bytes, keep: { id, entries, chain } };
    }
    const held = b <= n && rest.length >= n - b ? cache.take(id, b, start.prefixDigest) : undefined;
    if (held === undefined) {
      const have = cache.held(id, n);
      return have === undefined ? { miss: true } : { miss: true, held: have };
    }
    const delta = rest.slice(0, n - b).map(raw);
    const entries = [...held.frames, ...delta];
    const chain = [...held.chain];
    await extendChain(chain, delta);
    return { bytes: concat([raw(first), ...held.frames, ...rest.map(raw)]), keep: { id, entries, chain } };
  }

  /**
   * After the turn: a turn that ends the invocation (`End`, `Error`) ⇒ drop the entry;
   * otherwise keep entries `[0, N)`. Returns `N` when kept (`cairn-replay-cache` header), `0` when not.
   */
  private async keep(k: Keep, out: Uint8Array): Promise<number> {
    const cache = this.replay!;
    const ended = decodeFrames(out).some((f) => f.type === MT.END || f.type === MT.ERROR);
    if (ended || k.entries.length === 0) {
      cache.delete(k.id);
      return 0;
    }
    return cache.put(k.id, k.entries, k.chain) ? k.entries.length : 0;
  }
}

/** What is kept after a v5 turn. */
interface Keep {
  readonly id: string;
  readonly entries: Uint8Array[];
  readonly chain: Uint8Array[];
}
