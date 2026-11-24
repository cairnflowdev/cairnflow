// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Fetch API adapter: the SDK endpoint in `REQUEST_RESPONSE` mode as a function
 * `(Request) => Promise<Response>` — runs on any serverless platform that
 * accepts the Fetch API, with no port and no long-lived process:
 *
 * ```ts
 * // Vercel (Next.js, app/api/cairn/[...path]/route.ts — Node.js runtime)
 * import { fetchHandler, service } from "@cairnflow/sdk/fetch";
 * const handler = fetchHandler({ services: [orders], identityKeys: ["publickeyv1_…"] });
 * export const POST = handler;
 * export const GET = handler;
 *
 * // Cloudflare Workers (nodejs_compat flag)
 * export default { fetch: fetchHandler({ services: [orders], identityKeys }) };
 *
 * // AWS Lambda (Function URL or API Gateway HTTP API, payload v2)
 * export const handler = lambdaHandler(fetchHandler({ services: [orders], identityKeys }));
 * ```
 *
 * The deployment registers with the endpoint's base URL (`https://app.vercel.app/api/cairn`):
 * the runtime calls `<url>/discover` and `<url>/invoke/{service}/{handler}`. The adapter
 * routes on the path **suffix** (`…/discover`, `…/invoke/S/H`), so it can be mounted
 * under any prefix; with `basePath` it accepts only that exact prefix.
 *
 * **`REQUEST_RESPONSE` only.** The Fetch API gives no full-duplex HTTP/2 request
 * from the runtime to the handler, so every suspension point is a new request and
 * the runtime resends the journal — bytes traded for running anywhere. The core is
 * [`EndpointCore`](./endpoint.ts), shared with `serve()`: the `REQUEST_RESPONSE`
 * conformance traces run through both (`test/conformance.test.ts`).
 *
 * Request identity as in `serve()`: `identityKeys` ⇒ every request must carry the
 * runtime's JWT, for exactly the path that was sent (`aud`), with `jti` matching
 * `Start`; otherwise `401`. The runtime's SaaS profile refuses to register an
 * endpoint that does not verify.
 *
 * This module does not import `node:http*`/`node:net`; the turn loop core uses
 * `node:async_hooks` and `node:crypto` — available on Node (Vercel, Lambda), Bun,
 * Deno, and Workers with `nodejs_compat`.
 */

import {
  type Answer,
  BodyTooLarge,
  EndpointCore,
  MAX_PROTOCOL_VERSION,
  MAX_REQUEST_BODY,
  type ServiceDefinition,
  warnUnverified,
} from "./endpoint.ts";
import type { ReplayCacheOptions } from "./replay.ts";

export {
  DISCOVERY_V1,
  type HandlerOptions,
  type HandlerType,
  INVOCATION_V1,
  manifest,
  MAX_PROTOCOL_VERSION,
  MIN_PROTOCOL_VERSION,
  type OnMaxAttempts,
  type ProtocolMode,
  type RetryPolicy,
  service,
  type ServiceDefinition,
  type ServiceType,
  workflow,
} from "./endpoint.ts";
export {
  type AnyHandler,
  Context,
  DurableFuture,
  type DurablePromise,
  type Handler,
  PauseError,
  RunFailure,
  type RunOptions,
  type ServiceContext,
  type ServiceHandler,
  type SharedContext,
  type SharedHandler,
  TerminalError,
} from "./handler.ts";

export interface FetchHandlerOptions {
  readonly services: readonly ServiceDefinition[];
  /** As `ServeOptions.maxProtocolVersion`. */
  readonly maxProtocolVersion?: number;
  /** As `ServeOptions.identityKeys`: trusted runtime `kid`s. */
  readonly identityKeys?: readonly string[];
  /**
   * As `ServeOptions.identityHosts`: the registered public hosts. Absent
   * ⇒ the `host` claim is compared with the request URL's host (sender-chosen text).
   */
  readonly identityHosts?: readonly string[];
  /** As `ServeOptions.segmentOrigins`: trusted URL origins for reading segments. */
  readonly segmentOrigins?: readonly string[];
  /**
   * Prefix the endpoint is mounted at (`/api/cairn`). Set ⇒ only paths under
   * exactly this prefix are accepted. Absent ⇒ routing on the path suffix.
   */
  readonly basePath?: string;
  /**
   * Declare `requestCompression: "gzip"`: the runtime compresses
   * REQUEST_RESPONSE bodies from 8 KiB, and the endpoint compresses responses when
   * invited. Default `true`. Turn it off when a body mixes third-party-chosen input
   * with secret data and someone on the runtime → endpoint path can observe
   * lengths (the BREACH class).
   */
  readonly compression?: boolean;
  /**
   * `REQUEST_RESPONSE` replay cache (wire v5, protocol-v1 §9.3): keeps in RAM the
   * journal received for each running invocation, so the runtime sends only the
   * new entries each turn instead of the whole journal — bytes to the endpoint
   * grow linearly instead of quadratically with the number of steps. Safe by
   * construction: the runtime sends a SHA-256 digest of the prefix; on a mismatch
   * or a miss the endpoint answers `409` and the turn goes again with a full
   * replay. Default **off**: Lambda, Workers, Vercel Edge keep no process across turns and have no affinity. Turn it on for long-lived runtimes (Bun/Deno server). `true`/options (`maxBytes`, default 64 MiB; `ttlMs`,
   * default 15 minutes) turn it on and need `identityKeys` (as `ServeOptions.replayCache`), `false` off. Several replicas behind a load
   * balancer only hit with affinity (sticky cookie, or Fly: `FLY_MACHINE_ID` is sent automatically).
   */
  readonly replayCache?: boolean | ReplayCacheOptions;
  /**
   * As `ServeOptions.valueCacheBytes`: RAM cap of the segment cache (wire v6),
   * default 64 MiB. On platforms that keep no process across turns (Lambda,
   * Workers) the cache only hits within one turn — segments are fetched again each turn.
   */
  readonly valueCacheBytes?: number;
  /** As `ServeOptions.maxValueBytes`. Default 64 MiB. */
  readonly maxValueBytes?: number;
  /** As `ServeOptions.maxTurnValueBytes`. Default 256 MiB. */
  readonly maxTurnValueBytes?: number;
}

/** `(Request) => Promise<Response>` — the signature of Workers, Vercel, Deno, Bun. */
export type FetchHandler = (request: Request) => Promise<Response>;

/** Routing path (`/discover`, `/invoke/S/H`) of `pathname`, or `null`. */
function routeOf(pathname: string, basePath: string | undefined): string | null {
  if (basePath !== undefined) {
    const base = basePath.replace(/\/+$/, "");
    if (base !== "" && !(pathname === base || pathname.startsWith(`${base}/`))) return null;
    return pathname.slice(base.length) || "/";
  }
  const m = /(\/discover|\/invoke\/[^/]+\/[^/]+)$/.exec(pathname);
  return m === null ? null : m[1]!;
}

function toResponse(a: Answer): Response {
  return new Response(a.body as Uint8Array<ArrayBuffer>, {
    status: a.status,
    headers: {
      "content-type": a.contentType,
      "content-length": String(a.body.length),
      ...(a.contentEncoding !== undefined ? { "content-encoding": a.contentEncoding } : {}),
      ...a.headers,
    },
    // Cloudflare Workers: the body is already compressed — by default Workers
    // compresses again when it sees `content-encoding`. Other platforms ignore
    // unknown `ResponseInit` keys. Not yet tried on real Workers (README).
    ...(a.contentEncoding !== undefined ? { encodeBody: "manual" } : {}),
  } as ResponseInit);
}

/** SDK `REQUEST_RESPONSE` endpoint on the Fetch API. */
export function fetchHandler(opts: FetchHandlerOptions): FetchHandler {
  const core = new EndpointCore(
    opts.services,
    "REQUEST_RESPONSE",
    opts.maxProtocolVersion,
    opts.identityKeys,
    opts.compression ?? true,
    opts.replayCache ?? false,
    opts.valueCacheBytes,
    opts.identityHosts,
    opts.segmentOrigins,
    { maxValueBytes: opts.maxValueBytes, maxTurnValueBytes: opts.maxTurnValueBytes },
  );
  if (core.verifier === undefined) warnUnverified("fetchHandler()");
  return async (request) => {
    // The path **as the runtime sent it** (not %-decoded): what the JWT signs as `aud`.
    const pathname = new URL(request.url).pathname;
    const route = routeOf(pathname, opts.basePath);
    if (route === null) {
      return toResponse({
        status: 404,
        contentType: "application/json",
        body: new TextEncoder().encode(JSON.stringify({ error: `no route for ${request.method} ${pathname}` })),
      });
    }
    try {
      // The Fetch API does not expose `Host`: the request URL's host is it.
      const host = new URL(request.url).host;
      const header = (name: string): string | undefined =>
        name === "host" ? host : (request.headers.get(name) ?? undefined);
      // Streamed read with a cap — no `arrayBuffer()` of a whole body of any size.
      const body = async (): Promise<Uint8Array> => {
        if (request.body === null) return new Uint8Array();
        const reader = request.body.getReader();
        const parts: Uint8Array[] = [];
        let total = 0;
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          total += value.length;
          if (total > MAX_REQUEST_BODY) {
            await reader.cancel().catch(() => {});
            throw new BodyTooLarge(`body exceeds ${MAX_REQUEST_BODY} bytes`);
          }
          parts.push(value);
        }
        const out = new Uint8Array(total);
        let at = 0;
        for (const p of parts) {
          out.set(p, at);
          at += p.length;
        }
        return out;
      };
      return toResponse(await core.answer(request.method, pathname, route, header, body));
    } catch (e) {
      // As in `serve()`: an error outside the turn loop (not from user code —
      // the turn loop already wraps handlers) ⇒ `500`, the runtime retries with backoff.
      return toResponse({
        status: 500,
        contentType: "application/json",
        body: new TextEncoder().encode(JSON.stringify({ error: e instanceof Error ? e.message : String(e) })),
      });
    }
  };
}

// ─────────────────────────────────────────────────────────── AWS Lambda

/**
 * Lambda HTTP event, payload v2 (Function URL, API Gateway HTTP API) — the part
 * the adapter reads.
 */
export interface LambdaHttpEvent {
  readonly rawPath: string;
  readonly rawQueryString?: string;
  readonly headers?: Readonly<Record<string, string | undefined>>;
  readonly body?: string;
  readonly isBase64Encoded?: boolean;
  readonly requestContext: { readonly http: { readonly method: string } };
}

/** Result returned to Lambda: the body is always base64 (protocol frames are binary). */
export interface LambdaHttpResult {
  readonly statusCode: number;
  readonly headers: Record<string, string>;
  readonly body: string;
  readonly isBase64Encoded: true;
}

function fromBase64(s: string): Uint8Array<ArrayBuffer> {
  const bin = atob(s);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

function toBase64(b: Uint8Array): string {
  let bin = "";
  // In chunks: `String.fromCharCode(...b)` exceeds the argument limit on large bodies.
  for (let i = 0; i < b.length; i += 0x8000) {
    bin += String.fromCharCode(...b.subarray(i, i + 0x8000));
  }
  return btoa(bin);
}

/**
 * Wraps a [`FetchHandler`] into a Lambda handler (payload v2). Lambda returns the
 * body via JSON, so binary frames go base64 in both directions.
 */
export function lambdaHandler(handler: FetchHandler): (event: LambdaHttpEvent) => Promise<LambdaHttpResult> {
  return async (event) => {
    const headers = new Headers();
    for (const [k, v] of Object.entries(event.headers ?? {})) {
      if (v !== undefined) headers.set(k, v);
    }
    const host = headers.get("host") ?? "lambda.local";
    // The host goes into the URL the endpoint routes on: with a `/`, `?`, `#`, `@`, a
    // backslash or white space in it, the parsed path would no longer be `rawPath`.
    if (/[/?#@\\\s]/.test(host)) {
      return {
        statusCode: 400,
        headers: { "content-type": "application/json" },
        body: toBase64(new TextEncoder().encode(JSON.stringify({ error: "invalid host header" }))),
        isBase64Encoded: true,
      };
    }
    const query = event.rawQueryString ? `?${event.rawQueryString}` : "";
    const method = event.requestContext.http.method;
    const body =
      event.body === undefined || method === "GET" || method === "HEAD"
        ? undefined
        : event.isBase64Encoded
          ? fromBase64(event.body)
          : new TextEncoder().encode(event.body);
    const res = await handler(
      new Request(`https://${host}${event.rawPath}${query}`, { method, headers, ...(body ? { body } : {}) }),
    );
    const headers_: Record<string, string> = {
      "content-type": res.headers.get("content-type") ?? "application/octet-stream",
    };
    // Compressed body: without `content-encoding` the runtime would read gzip as a
    // bare frame — a deterministic turn failure. Wire v5 (protocol-v1 §9.3): the
    // replay control headers pass through untouched.
    for (const h of ["content-encoding", "cairn-replay", "cairn-replay-cache", "cairn-replay-held", "cairn-affinity"]) {
      const v = res.headers.get(h);
      if (v !== null) headers_[h] = v;
    }
    return {
      statusCode: res.status,
      headers: headers_,
      body: toBase64(new Uint8Array(await res.arrayBuffer())),
      isBase64Encoded: true,
    };
  };
}
