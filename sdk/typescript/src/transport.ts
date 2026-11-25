// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * One transport layer for every ingress client: `connect()`
 * (`client.ts`), `readStream`, `cairnChatTransport` (`vercel.ts`). No `node:*`
 * import: runs on Node ≥ 22, browsers and the edge — it only needs `fetch`.
 *
 * The cluster's error contract (`docs/admin-api.md`, the error contract section):
 *
 * - `503` with `leader` ⇒ ask again at that leader — **one hop per attempt**,
 *   only when it has the same scheme as the original `url` (a Bearer key does not
 *   go to `http://` from an `https://`), can be turned off with `followLeader: false`;
 * - `503` without `leader` (the cluster is electing, a range is changing) ⇒ capped
 *   exponential backoff, then ask the original node again;
 * - `429` **without** `id` (`range_overloaded`, `sse_limit`: nothing created yet) ⇒
 *   wait `Retry-After` and resend; `429` with `id` (`throttled`: already durable) ⇒
 *   return it to the caller — resending is its job (usually an attach by id);
 * - a network error at a followed node ⇒ back off and return to the original node
 *   (the leader may have died, or the cluster's internal address is unreachable from
 *   the browser), and **do not follow** that node **again** in this call: the
 *   original node keeps pointing at the old leader until its lease expires, so
 *   following again burns every attempt within a few milliseconds;
 * - a cap on attempts **and** a cap on time; past the cap, return the last response
 *   or throw the last network error.
 *
 * A request that creates an invocation must carry an `Idempotency-Key` so that a
 * resend points at that same invocation (CG-1) — the transport does not add one;
 * `connect()` does.
 */

export interface TransportOptions {
  /** Ingress port of any node (`http://host:port` or `host:port`). */
  readonly url: string;
  /** Headers added to every request — e.g. `authorization: Bearer <key>`. */
  readonly headers?: Readonly<Record<string, string>>;
  /** Follow the `leader` of a `503` (default `true`). */
  readonly followLeader?: boolean;
  /** Cap on the attempts of one request (default 8). */
  readonly maxAttempts?: number;
  /** Time cap of one request, including the waits (default 30 000 ms). */
  readonly maxElapsedMs?: number;
  /**
   * Network error at the original node itself: retry (default `true`) or throw at
   * once. The Vercel transport throws at once — if the browser cannot reach the
   * original node, waiting does not help.
   */
  readonly retryNetwork?: boolean;
  /** Replacement `fetch` (tests, environments without a global `fetch`). */
  readonly fetch?: typeof fetch;
}

/** One send: the last response and the node that answered it. */
export interface Sent {
  readonly res: Response;
  readonly node: string;
  /**
   * An earlier send of the **same** request failed with a network error and was
   * resent — that one may have reached the server: a `409 already_invoked` may
   * then be the caller's own.
   */
  readonly resent: boolean;
}

/** `host:port` ⇒ `http://host:port`; drops a trailing `/`. */
export function normalizeUrl(node: string): string {
  const n = node.replace(/\/+$/, "");
  return /^https?:\/\//.test(n) ? n : `http://${n}`;
}

/** Waits `ms`, stops early (throws `signal.reason`) when `signal` aborts. */
export function sleepAbortable(ms: number, signal?: AbortSignal | null): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(signal.reason);
    const onAbort = () => {
      clearTimeout(t);
      reject(signal?.reason);
    };
    // Remove the listener once the sleep is over: the shared signal of a long-lived
    // client does not accumulate a listener per backoff.
    const t = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

const scheme = (url: string): string => url.slice(0, url.indexOf(":"));

/** Exponential backoff for attempt `n` (from 1): 100 ms, 200 ms, … capped at 2 s. */
export const backoffMs = (n: number): number => Math.min(2000, 100 * 2 ** Math.max(0, n - 1));

export class Transport {
  readonly url: string;
  readonly headers: Readonly<Record<string, string>>;
  private readonly followLeader: boolean;
  private readonly maxAttempts: number;
  private readonly maxElapsedMs: number;
  private readonly retryNetwork: boolean;
  private readonly doFetch: typeof fetch;

  constructor(opts: TransportOptions) {
    this.url = normalizeUrl(opts.url);
    this.headers = opts.headers ?? {};
    this.followLeader = opts.followLeader ?? true;
    this.maxAttempts = Math.max(1, opts.maxAttempts ?? 8);
    this.maxElapsedMs = opts.maxElapsedMs ?? 30_000;
    this.retryNetwork = opts.retryNetwork ?? true;
    this.doFetch = opts.fetch ?? ((...a) => fetch(...a));
  }

  /**
   * Sends `path` (with query) under the error contract at the top of the file.
   * `node`: start at this node instead of the original `url` (a stream reader
   * keeps the leader between reconnects).
   */
  async request(path: string, init: RequestInit = {}, node: string = this.url): Promise<Sent> {
    const started = Date.now();
    const signal = init.signal ?? undefined;
    const headers = { ...this.headers, ...(init.headers as Record<string, string> | undefined) };
    let at = normalizeUrl(node);
    let followed = false;
    /** Followed leaders that did not answer — not followed again in this call. */
    const dead = new Set<string>();
    let resent = false;
    for (let attempt = 1; ; attempt++) {
      const last = attempt >= this.maxAttempts || Date.now() - started >= this.maxElapsedMs;
      let res: Response;
      try {
        res = await this.doFetch(`${at}${path}`, { ...init, headers });
      } catch (e) {
        if (signal?.aborted) throw e;
        if (at !== this.url) {
          // A followed node did not answer: back off and return to the original
          // node — it learns the new leader when the old lease expires.
          dead.add(at);
          at = this.url;
          followed = false;
          if (last) throw e;
          await this.wait(backoffMs(attempt), started, signal);
          continue;
        }
        if (!this.retryNetwork || last) throw e;
        resent = true;
        await this.wait(backoffMs(attempt), started, signal);
        continue;
      }
      if (res.status === 503) {
        const leader = await peekField(res, "leader");
        if (
          this.followLeader &&
          !followed &&
          typeof leader === "string" &&
          leader.length > 0 &&
          scheme(normalizeUrl(leader)) === scheme(this.url) &&
          normalizeUrl(leader) !== at &&
          !dead.has(normalizeUrl(leader)) &&
          !last
        ) {
          await res.body?.cancel();
          at = normalizeUrl(leader);
          followed = true;
          continue;
        }
        if (last) return { res, node: at, resent };
        await res.body?.cancel();
        await this.wait(backoffMs(attempt), started, signal);
        at = this.url;
        followed = false;
        continue;
      }
      if (res.status === 429 && (await peekField(res, "id")) === undefined && !last) {
        const after = Number(res.headers.get("retry-after") ?? "1");
        await res.body?.cancel();
        await this.wait(Math.max(0, Number.isFinite(after) ? after : 1) * 1000, started, signal);
        continue;
      }
      return { res, node: at, resent };
    }
  }

  /** Waits `ms`, no longer than what is left of the time cap. */
  private async wait(ms: number, started: number, signal?: AbortSignal | null): Promise<void> {
    const left = this.maxElapsedMs - (Date.now() - started);
    await sleepAbortable(Math.max(0, Math.min(ms, left)), signal);
  }
}

/** A field of the JSON body (read from a `clone()`), `undefined` if absent. */
async function peekField(res: Response, name: string): Promise<unknown> {
  try {
    const v: unknown = await res.clone().json();
    return v !== null && typeof v === "object" ? (v as Record<string, unknown>)[name] : undefined;
  } catch {
    return undefined;
  }
}
