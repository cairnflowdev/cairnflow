// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * The SDK's HTTP endpoint — protocol-v1 §3 (`/discover`) and §4 (`/invoke`).
 *
 * Uses `node:http`, `node:http2` and `node:net`, no external packages.
 *
 * **One port, two protocols.** `BIDI_STREAM` needs full-duplex HTTP/2; discovery
 * reads the manifest *before* anyone knows the mode, so it still runs over
 * HTTP/1.1. Node does not let one server serve both over cleartext — `allowHTTP1`
 * of `http2.createSecureServer` requires TLS/ALPN — so we sniff the
 * `PRI * HTTP/2.0` preface ourselves on a `net.Server` and hand the socket to the
 * right server. Forty lines, and the only way to keep "one port" without the SDK
 * user having to know anything about it.
 */

import { createServer as createHttpServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import {
  constants as http2constants,
  createServer as createHttp2Server,
  type Http2Server,
  type IncomingHttpHeaders,
  type ServerHttp2Stream,
} from "node:http2";
import { createServer as createNetServer, type Socket } from "node:net";
import { createGunzip } from "node:zlib";
import {
  BodyTooLarge,
  type Bound,
  checkIdentity,
  DISCOVERY_V1,
  EndpointCore,
  INVOCATION_V1,
  MAX_PROTOCOL_VERSION,
  MAX_REQUEST_BODY,
  type ProtocolMode,
  type ServiceDefinition,
  warnUnverified,
} from "./endpoint.ts";
import { type AnyHandler, type FrameStream, runBidi } from "./handler.ts";
import { bodyHash, DISCOVER_JTI, expectedJti, JWT_HEADER } from "./identity.ts";
import type { ReplayCacheOptions } from "./replay.ts";
import { concatFrames, decodeStart, encodeFrame, type Frame, FrameSplitter, MT } from "./wire.ts";

// Service declaration, the manifest and the REQUEST_RESPONSE core live in `endpoint.ts`
// (no `node:http*` dependency, shared with the Fetch adapter); re-exported here
// so the public API does not change.
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
  wireVersionOf,
  workflow,
} from "./endpoint.ts";

/**
 * The HTTP/2 cleartext preface (RFC 9113 §3.4). A prior-knowledge client sends
 * exactly this string as the first 24 bytes, before any frame.
 */
const H2_PREFACE = Buffer.from("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n", "latin1");

/** An h1 request body, capped at [`MAX_REQUEST_BODY`]: over ⇒ `BodyTooLarge`. */
function readBody(req: IncomingMessage): Promise<Uint8Array> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let total = 0;
    req.on("data", (c: Buffer) => {
      total += c.length;
      if (total > MAX_REQUEST_BODY) {
        req.pause();
        reject(new BodyTooLarge(`body exceeds ${MAX_REQUEST_BODY} bytes`));
        return;
      }
      chunks.push(c);
    });
    req.on("end", () => resolve(new Uint8Array(Buffer.concat(chunks))));
    req.on("error", reject);
  });
}

function json(res: ServerResponse, status: number, body: unknown, contentType: string): void {
  const payload = Buffer.from(JSON.stringify(body));
  res.writeHead(status, {
    "content-type": contentType,
    "content-length": String(payload.length),
  });
  res.end(payload);
}

export interface ServeOptions {
  readonly services: readonly ServiceDefinition[];
  readonly port?: number;
  /**
   * Listen address. Default `127.0.0.1`: only processes on the same machine can call.
   * A runtime on another machine or container ⇒ set `"0.0.0.0"`.
   */
  readonly hostname?: string;
  /**
   * Mode declared in the manifest. Default `BIDI_STREAM`: it saves one HTTP
   * round trip and one journal resend at **every** suspension point the runtime
   * can resolve in place (lazy `ctx.get`, `ctx.run`).
   *
   * Set `REQUEST_RESPONSE` when there is an h1-only proxy between the runtime and
   * the SDK — h2c cannot get through then, and a mode that works beats a faster one.
   */
  readonly protocolMode?: ProtocolMode;
  /**
   * Highest wire version declared in the manifest — default
   * [`MAX_PROTOCOL_VERSION`] when `identityKeys` or `segmentOrigins` is set, otherwise 5:
   * wire v6 (segmented values) makes the endpoint fetch URLs from the request, so an endpoint
   * that verifies neither who sends them nor where they point does not serve it, and pinning
   * v6 there is a configuration error. Lower it to pin the SDK to an older version: the
   * SDK × runtime matrix tests (`check.sh`, rows `wire1`/`wire2`), or a rolling
   * upgrade that wants to keep the SDK at a version every node still speaks.
   *
   * The runtime's `/invoke` content-type is clamped to this number too: if the
   * runtime speaks a higher version, the SDK still only uses the declared version's features.
   */
  readonly maxProtocolVersion?: number;
  /**
   * Trusted runtime `kid`s (`publickeyv1_…`) — from the runtime's `GET /identity`
   * or the console (`identity.ts`). Set ⇒ every `/discover`
   * and `/invoke` must carry an identity JWT signed with one of these keys, for the
   * right path, unexpired, with `jti` matching the request's own `Start`; otherwise
   * `401` (BIDI: a `jti` mismatch ⇒ the stream is closed), the handler does not run.
   * Key rotation: add the new `kid` here **before** the runtime activates it, remove
   * the old `kid` afterwards.
   *
   * Not set ⇒ every request is accepted, its origin unverified (warned once).
   *
   * BIDI: the JWT binds the turn-opening part (`bsh`), and each JWT opens only one
   * stream; frames **after** the turn-opening part are not signed — over `http://`
   * without TLS, someone who can get into that very stream can still steer what
   * follows. Use `https` (or a private network) between the runtime and the endpoint.
   */
  readonly identityKeys?: readonly string[];
  /**
   * Trusted hosts for the identity JWT's `host` claim: the runtime signs the host
   * it sends to. Absent ⇒ it must equal the request's own `Host` header (h2:
   * `:authority`) — that header is written by the sender, so when this is absent the
   * `host` claim only stops a JWT copied to another endpoint **if** that endpoint
   * routes by host; a request carrying no host is refused. Set it to bind the JWT
   * to this exact endpoint — list the registered **public** hosts (required when a
   * proxy between the runtime and the SDK rewrites `Host`).
   */
  readonly identityHosts?: readonly string[];
  /**
   * Trusted URL origins for `Start.segment_urls` (wire v6): the SDK only fetches
   * segments from these origins (`https://values.example.com`). Absent ⇒ any
   * `http(s)` — only safe when `identityKeys` is set (the URLs are in the signed body); an
   * endpoint with neither does not serve wire v6.
   */
  readonly segmentOrigins?: readonly string[];
  /**
   * Declare `requestCompression: "gzip"` (same shape as the Restate SDK's
   * `lambdaCompression`): in REQUEST_RESPONSE the runtime resends the whole journal
   * every turn, so it compresses bodies from 8 KiB; the SDK compresses responses
   * when invited. Default `true`. Turn it off when a body mixes third-party-chosen
   * input with secret data and someone on the runtime → endpoint path can observe
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
   * replay. Default on **when `identityKeys` is set**, off otherwise; `true`/options
   * (`maxBytes`, default 64 MiB; `ttlMs`, default 15 minutes) without `identityKeys` is a
   * configuration error: a delta names an invocation id, so an unverified caller could
   * probe for running ids and run the handler on another invocation's held journal.
   * `false` turns it off. Several replicas behind a load balancer only hit with affinity
   * (sticky cookie, or Fly: `FLY_MACHINE_ID` is sent automatically).
   */
  readonly replayCache?: boolean | ReplayCacheOptions;
  /**
   * RAM cap in bytes of the segment cache (wire v6): large state values arrive
   * as refs, the SDK fetches segments through the read URLs the runtime sends,
   * checks sha256, then keeps them here (LRU) so the next turn does not fetch again.
   * Default 64 MiB; `0` = hold nothing (fetch again every turn).
   */
  readonly valueCacheBytes?: number;
  /**
   * Maximum logical size of one segmented value the SDK assembles (wire v6):
   * larger ⇒ the turn fails (and is retried) before fetching or allocating memory.
   * Default 64 MiB — equal to the tenant's default `max-value-bytes`; a cluster that
   * raises that cap raises this one too.
   */
  readonly maxValueBytes?: number;
  /**
   * Maximum total bytes of segmented values assembled in one turn (Σ logical
   * size, including segments that hit the cache). Default 256 MiB. BIDI: the whole stream.
   */
  readonly maxTurnValueBytes?: number;
}

export interface Endpoint {
  readonly port: number;
  readonly url: string;
  close(): Promise<void>;
}

/**
 * Connects a `ServerHttp2Stream` to the turn loop of [`runBidi`].
 *
 * Reads **continuously** and queues frames, rather than reading on demand. That is
 * the condition for not deadlocking: the runtime pushes notifications down while the
 * handler is still running, and if we only read when the handler suspends, the h2
 * receive window drains until the runtime gets stuck in `send`.
 */
class Http2Frames implements FrameStream {
  private readonly queue: Frame[] = [];
  private readonly split = new FrameSplitter();
  private waiter: ((f: Frame | null) => void) | null = null;
  private closed = false;

  private readonly stream: ServerHttp2Stream;

  /**
   * `gzipped`: the request body is `content-encoding: gzip` — compressed
   * REQUEST_RESPONSE arriving through an edge that converts to h2 towards the app.
   * Decompression is capped at [`MAX_REQUEST_BODY`] bytes.
   */
  constructor(stream: ServerHttp2Stream, gzipped = false) {
    this.stream = stream;
    const src = gzipped ? stream.pipe(createGunzip()) : stream;
    let total = 0;
    const broken = (): void => {
      this.finish();
      stream.destroy();
    };
    if (src !== stream) src.on("error", broken);
    src.on("data", (c: Buffer) => {
      total += c.length;
      if (gzipped && total > MAX_REQUEST_BODY) {
        broken();
        return;
      }
      let frames: Frame[];
      try {
        frames = this.split.push(new Uint8Array(c));
      } catch {
        // Broken byte stream: no way to find the frame boundary again. Close.
        broken();
        return;
      }
      for (const f of frames) this.offer(f);
    });
    src.on("end", () => this.finish());
    // `close` and `error` are merged into one way of ending: the runtime closing the
    // stream **is** how it says "turn over" when what the SDK awaits must come from
    // the outside world. Treating RST_STREAM as an error would turn every normal suspension into an exception.
    stream.on("close", () => this.finish());
    stream.on("error", () => this.finish());
  }

  private offer(f: Frame): void {
    const w = this.waiter;
    if (w !== null) {
      this.waiter = null;
      w(f);
      return;
    }
    this.queue.push(f);
  }

  private finish(): void {
    this.closed = true;
    const w = this.waiter;
    if (w !== null && this.queue.length === 0) {
      this.waiter = null;
      w(null);
    }
  }

  next(): Promise<Frame | null> {
    const head = this.queue.shift();
    if (head !== undefined) return Promise.resolve(head);
    if (this.closed) return Promise.resolve(null);
    return new Promise((resolve) => {
      this.waiter = resolve;
    });
  }

  buffered(): Frame[] {
    return this.queue.splice(0, this.queue.length);
  }

  write(bytes: Uint8Array): void {
    if (bytes.length > 0 && !this.stream.closed) this.stream.write(Buffer.from(bytes));
  }

  end(): void {
    if (!this.stream.closed) this.stream.end();
  }
}

/**
 * Sends headers for an h2 stream if it is still alive; `false` otherwise (the caller
 * drops the stream and does not run the turn).
 *
 * The h2 server emits `stream` on a later tick than when it received `HEADERS`; if
 * the session dies with its socket in between (the runtime was `kill -9`ed), the
 * stream is already destroyed by the time we get it, and `respond` throws
 * `ERR_HTTP2_INVALID_STREAM` **inside the event handler** — nobody catches it, and
 * the whole SDK process dies (seen in a 24 h soak). The runtime re-delivers the
 * turn at the new leader on its own, so dropping the stream is right. Other errors still throw.
 */
export function respondIfOpen(
  stream: Pick<ServerHttp2Stream, "respond" | "destroyed" | "closed">,
  headers: Parameters<ServerHttp2Stream["respond"]>[0],
): boolean {
  if (stream.destroyed || stream.closed) return false;
  try {
    stream.respond(headers);
    return true;
  } catch (e) {
    if ((e as { code?: string }).code === "ERR_HTTP2_INVALID_STREAM") return false;
    throw e;
  }
}


/** Reads a header (`node:http` and `node:http2` have the same shape). */
function headerOf(h: IncomingHttpHeaders): (name: string) => string | undefined {
  return (name) => {
    const v = h[name];
    return Array.isArray(v) ? v[0] : v;
  };
}

/**
 * The `jti` (and `bsh`) gate of a BIDI stream: the headers were verified on open,
 * but `Start` only arrives after the response headers were sent — a first frame
 * with a mismatched `jti` closes the stream. `bsh` present ⇒ collect the **whole**
 * turn-opening body (`Start` + exactly `known_entries` replayed frames, as the
 * runtime sent them), hash and compare it **before releasing any frame** to the turn
 * loop: `runBidi` loads `Start.segment_urls` and assembles replayed entries as soon
 * as it receives them, so releasing early means fetching URLs of an unverified body.
 * A mismatch, a turn-opening body over [`MAX_REQUEST_BODY`], or a stream cut midway
 * ⇒ close the stream; the turn loop sees `null` and does not run the handler.
 */
/**
 * JWTs that have opened a BIDI stream, until their `exp`: in BIDI only the
 * turn-opening part is bound to the JWT (`bsh`), so an eavesdropper with both the
 * JWT and the turn-opening body could open a second stream and send a forged
 * continuation. The runtime signs a new JWT for every request (a fresh `nonce`
 * claim — `iat` and the turn-opening body alone repeat when a turn is resent within
 * one second), so one JWT opening one stream is enough — keyed by signature,
 * not by `jti` (a continuation turn after a suspension reuses `<id>:0`). Size cap:
 * full ⇒ purge expired entries; still full ⇒ refuse (close when in doubt).
 */
export class UsedTokens {
  private readonly seen = new Map<string, number>();
  private readonly max: number;
  constructor(max = 100_000) {
    this.max = max;
  }

  /** `true` the first time `token` is seen (expiry `expS`, seconds); `false` if already used or the ledger is full. */
  claim(token: string, expS: number, nowMs = Date.now()): boolean {
    if (this.seen.has(token)) return false;
    if (this.seen.size >= this.max) {
      for (const [k, exp] of this.seen) if (exp * 1000 <= nowMs) this.seen.delete(k);
      if (this.seen.size >= this.max) return false;
    }
    this.seen.set(token, expS);
    return true;
  }
}

class JtiGate implements FrameStream {
  private first = true;
  private readonly inner: FrameStream;
  private readonly jti: string;
  private readonly bsh: string | undefined;
  private readonly reject: () => void;
  /** Turn-opening frames that passed the comparison, waiting to be released in order. */
  private pending: Frame[] = [];

  constructor(inner: FrameStream, jti: string, bsh: string | undefined, reject: () => void) {
    this.inner = inner;
    this.jti = jti;
    this.bsh = bsh;
    this.reject = reject;
  }

  async next(): Promise<Frame | null> {
    const held = this.pending.shift();
    if (held !== undefined) return held;
    const f = await this.inner.next();
    if (f === null || !this.first) return f;
    this.first = false;
    if (f.type !== MT.START) {
      this.reject();
      return null;
    }
    const start = decodeStart(f.payload);
    if (expectedJti(start) !== this.jti) {
      this.reject();
      return null;
    }
    if (this.bsh === undefined) return f;
    const opening = [encodeFrame(f.type, f.flags, f.payload)];
    let bytes = opening[0]!.length;
    const frames = [f];
    for (let left = start.knownEntries; left > 0; left--) {
      const g = await this.inner.next();
      if (g === null) {
        this.reject();
        return null;
      }
      const raw = encodeFrame(g.type, g.flags, g.payload);
      bytes += raw.length;
      if (bytes > MAX_REQUEST_BODY) {
        this.reject();
        return null;
      }
      opening.push(raw);
      frames.push(g);
    }
    if ((await bodyHash(concatFrames(opening))) !== this.bsh) {
      this.reject();
      return null;
    }
    this.pending = frames.slice(1);
    return f;
  }

  buffered(): Frame[] {
    return [...this.pending.splice(0, this.pending.length), ...this.inner.buffered()];
  }

  write(bytes: Uint8Array): void {
    this.inner.write(bytes);
  }

  end(): void {
    this.inner.end();
  }
}

export function serve(opts: ServeOptions): Promise<Endpoint> {
  const mode: ProtocolMode = opts.protocolMode ?? "BIDI_STREAM";
  const core = new EndpointCore(
    opts.services,
    mode,
    opts.maxProtocolVersion,
    opts.identityKeys,
    opts.compression ?? true,
    opts.replayCache,
    opts.valueCacheBytes,
    opts.identityHosts,
    opts.segmentOrigins,
    { maxValueBytes: opts.maxValueBytes, maxTurnValueBytes: opts.maxTurnValueBytes },
  );
  const verifier = core.verifier;
  const usedTokens = new UsedTokens();
  if (verifier === undefined) warnUnverified("serve()");

  /**
   * Open **non-h2** sockets — h1, and also those not yet sniffed — together with
   * which of them are busy serving a request.
   *
   * We must keep this ledger ourselves because we accept sockets at the `net` layer.
   * Since Node 19, `http.Server.close()` closes idle keep-alive connections itself —
   * but it only knows the sockets **it** accepted, and sockets we push in with
   * `emit("connection")` are not in its ledger (`closeIdleConnections()` skips them).
   * Without the ledger, `close()` hangs forever waiting on a keep-alive discovery
   * connection nobody closes — or on a socket that opened and went silent.
   *
   * h2c sockets leave this ledger as soon as sniffing is done: `Http2Server.close()`
   * already handles them — it sends GOAWAY, lets open streams finish, then closes
   * the session. Keeping them here would cut off an invocation midway.
   */
  const plain = new Set<Socket>();
  const busy = new Set<Socket>();
  let closing = false;

  // ── HTTP/1.1: discovery, and `/invoke` in REQUEST_RESPONSE mode.
  const h1: Server = createHttpServer((req, res) => {
    // "Busy" = has a request not fully answered. `close()` waits for these sockets and
    // only ends idle ones — the same semantics as `http.Server.close()`.
    const socket = req.socket;
    busy.add(socket);
    res.on("close", () => {
      busy.delete(socket);
      if (closing) socket.destroy();
    });
    void handle(req, res).catch((e: unknown) => {
      if (!res.headersSent) {
        json(res, 500, { error: e instanceof Error ? e.message : String(e) }, "application/json");
      }
    });
  });

  async function handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const path = new URL(req.url ?? "/", "http://localhost").pathname;
    const a = await core.answer(req.method ?? "GET", path, path, headerOf(req.headers), () => readBody(req));
    res.writeHead(a.status, {
      "content-type": a.contentType,
      "content-length": String(a.body.length),
      ...(a.contentEncoding !== undefined ? { "content-encoding": a.contentEncoding } : {}),
      ...a.headers,
    });
    res.end(a.body);
  }

  // ── HTTP/2 h2c: `/invoke` in BIDI_STREAM mode.
  const h2: Http2Server | undefined =
    mode === "BIDI_STREAM" ? createHttp2Server() : undefined;

  h2?.on("stream", (stream: ServerHttp2Stream, headers: IncomingHttpHeaders) => {
    // If the runtime dies (`kill -9`) midway, the stream errors before the turn loop
    // gets to attach a listener — an unheard `error` crashes the whole SDK process.
    stream.on("error", () => {});
    const method = String(headers[":method"] ?? "GET");
    const path = String(headers[":path"] ?? "/");

    const bare = path.split("?")[0]!;
    if (method === "GET" && bare === "/discover") {
      void checkIdentity(verifier, headerOf(headers), bare, DISCOVER_JTI, h2Bound(headers)).then((id) => {
        if (!id.ok) {
          if (!respondIfOpen(stream, { ":status": 401, "content-type": "application/json" })) return;
          stream.end(JSON.stringify({ error: id.why }));
          return;
        }
        if (!respondIfOpen(stream, { ":status": 200, "content-type": DISCOVERY_V1 })) return;
        stream.end(JSON.stringify(core.manifest()));
      });
      return;
    }

    const handler = core.route(bare, method);
    if (handler === undefined) {
      if (!respondIfOpen(stream, { ":status": 404, "content-type": "application/json" })) return;
      stream.end(JSON.stringify({ error: `no route for ${method} ${path}` }));
      return;
    }

    // Identity headers are verified at once (signature, path, expiry); `jti` is compared
    // with `Start` when the first frame arrives — see [`JtiGate`].
    void checkIdentity(verifier, headerOf(headers), bare, () => true, h2Bound(headers)).then((id) => {
      if (!id.ok) {
        if (!respondIfOpen(stream, { ":status": 401, "content-type": "application/json" })) return;
        stream.end(JSON.stringify({ error: id.why }));
        return;
      }
      // One JWT, one stream: keyed by its signature (the third part).
      const jwt = headerOf(headers)(JWT_HEADER);
      if (verifier !== undefined && !usedTokens.claim(jwt?.split(".")[2] ?? "", id.exp ?? 0)) {
        if (!respondIfOpen(stream, { ":status": 401, "content-type": "application/json" })) return;
        stream.end(JSON.stringify({ error: "this request identity token has already opened a stream" }));
        return;
      }
      openBidi(stream, headers, handler, verifier === undefined ? undefined : { jti: id.jti, bsh: id.bsh });
    });
  });

  /** h2 has no `Host`: the stream's authority. */
  function h2Bound(headers: IncomingHttpHeaders): Bound {
    const authority = headers[":authority"];
    return { host: typeof authority === "string" ? authority : undefined, hosts: core.identityHosts };
  }

  function openBidi(
    stream: ServerHttp2Stream,
    headers: IncomingHttpHeaders,
    handler: AnyHandler,
    gate: { jti: string; bsh: string | undefined } | undefined,
  ): void {
    // Send headers **immediately**, before reading any byte of the request body.
    //
    // On the runtime side, `send()` only returns when the response headers arrive.
    // Waiting to read all of `Start` before `respond` means both sides wait for each
    // other — and that hang only shows under load, never in a one-turn test.
    const enc = String(headers["content-encoding"] ?? "").trim().toLowerCase();
    const gzipped = enc === "gzip";
    if (enc !== "" && enc !== "identity" && !(gzipped && core.compression)) {
      if (!respondIfOpen(stream, { ":status": 415, "content-type": "application/json" })) return;
      stream.end(JSON.stringify({ error: `content-encoding ${enc} is not enabled (requestCompression)` }));
      return;
    }
    const tooNew = core.tooNew(headers["content-type"] as string | undefined);
    if (tooNew !== undefined) {
      if (!respondIfOpen(stream, { ":status": 415, "content-type": "application/json" })) return;
      stream.end(JSON.stringify({ error: tooNew }));
      return;
    }
    if (!respondIfOpen(stream, { ":status": 200, "content-type": INVOCATION_V1 })) return;
    const wire = core.wireOf(headers["content-type"] as string | undefined);
    const frames = new Http2Frames(stream, gzipped);
    const io =
      gate === undefined
        ? frames
        : new JtiGate(frames, gate.jti, gate.bsh, () => stream.close(http2constants.NGHTTP2_REFUSED_STREAM));
    void runBidi(handler, io, wire, { cache: core.values, origins: core.segmentOrigins, ...core.valueLimits }).catch((e: unknown) => {
      // An exception here is an SDK bug, not user code — the turn loop already wraps
      // the handler. There is no channel left to report on: close and say it loudly.
      process.emitWarning(`runBidi failed: ${e instanceof Error ? e.stack ?? e.message : String(e)}`);
      if (!stream.closed) stream.destroy();
    });
  }

  // ── The port: sniff the preface, then hand the socket to the right server.
  const front = h2 === undefined ? h1 : createNetServer((socket) => sniff(socket, h1, h2));

  function sniff(socket: Socket, h1c: Server, h2c: Http2Server): void {
    // Into the ledger right away, before knowing what it speaks. A socket that opened
    // and went silent holds `front.close()` back just like an idle keep-alive
    // connection — and if it has said nothing, there is surely no unfinished work to wait for.
    plain.add(socket);
    socket.on("close", () => {
      plain.delete(socket);
      busy.delete(socket);
    });

    let seen: Buffer = Buffer.alloc(0);

    const decide = (target: Server | Http2Server): void => {
      socket.removeListener("readable", onReadable);
      socket.removeListener("error", onError);
      // h2 is handled by `Http2Server.close()` — it lets open streams finish. Keeping
      // it in this ledger would end a running invocation ourselves.
      if (target === h2c) plain.delete(socket);
      if (seen.length > 0) socket.unshift(seen);
      target.emit("connection", socket);
    };

    const onReadable = (): void => {
      for (;;) {
        const chunk = socket.read() as Buffer | null;
        if (chunk === null) break;
        seen = seen.length === 0 ? Buffer.from(chunk) : Buffer.concat([seen, chunk]);
        if (seen.length >= H2_PREFACE.length) break;
      }
      if (seen.length === 0) return;
      const n = Math.min(seen.length, H2_PREFACE.length);
      // One differing byte is enough to conclude h1 — "PRI " and "POST" already differ
      // at the second byte, so almost every request is decided on the first chunk.
      if (!seen.subarray(0, n).equals(H2_PREFACE.subarray(0, n))) {
        decide(h1c);
        return;
      }
      if (seen.length < H2_PREFACE.length) return; // still ambiguous, wait for more
      decide(h2c);
    };

    const onError = (): void => {
      socket.destroy();
    };

    socket.on("readable", onReadable);
    socket.on("error", onError);
    onReadable(); // data may have arrived before the listener was attached
  }

  return new Promise((resolve, reject) => {
    front.once("error", reject);
    front.listen(opts.port ?? 9080, opts.hostname ?? "127.0.0.1", () => {
      const addr = front.address();
      const port = typeof addr === "object" && addr !== null ? addr.port : (opts.port ?? 9080);
      resolve({
        port,
        url: `http://${opts.hostname ?? "127.0.0.1"}:${port}`,
        close: () =>
          new Promise<void>((done, fail) => {
            closing = true;
            // `h2`/`h1` never `listen`, but they still hold every socket handed to
            // them. Close all three, or the process does not exit.
            // `h2.close()` sends GOAWAY then ends idle sessions itself; on the h1 side
            // we have to do it ourselves — see `live`/`busy`.
            h2?.close();
            if (front !== h1) h1.close();
            front.close((err) => (err ? fail(err) : done()));
            for (const s of plain) {
              if (!busy.has(s)) s.destroy();
            }
          }),
      });
    });
  });
}
