// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Large segmented values — wire v6, the TypeScript SDK side.
 *
 * The runtime sends large state values down as segment refs + tail (exactly as
 * in the journal), and the read URL of each ref travels **outside** the entry
 * (`Start.segment_urls`, the `SegmentUrls` frame). The SDK:
 *
 * - **assembles on receipt**: before user code sees a message, every segmented
 *   `Value` is replaced by its whole bytes — cache by sha first, on a miss
 *   `fetch(url)`, check length and sha256 (WebCrypto). A mismatch ⇒ a turn error
 *   (retried), no guessing. The replay digest and the replay cache work on the
 *   **raw frames**, not on the assembled copy: this module never modifies a frame's bytes;
 * - **reuses refs on `set`**: [`reusablePrefix`] finds the longest prefix of the
 *   new value that is byte-identical to the segments already read for that same key.
 *
 * No `node:*` import (usable from `fetch.ts`: Workers, Lambda, Deno, Bun).
 * Never logs a URL or puts one in an error message: a URL is a read capability.
 */

import {
  decodeGetEagerStateValue,
  decodeNotification,
  type Frame,
  isNotification,
  MT,
  type SegmentRef,
  type SegmentUrl,
  type WireValue,
} from "./wire.ts";

/** First wire version with segmented values. */
export const WIRE_SEGMENTED = 6;

/** Default cap of [`SegmentCache`], bytes. */
export const DEFAULT_VALUE_CACHE_BYTES = 64 << 20;

/** Maximum number of segments fetched in parallel while assembling one value. */
const FETCH_CONCURRENCY = 8;

/**
 * Maximum segment fetches in flight across the whole process: each assembly has its own 8
 * workers, and the number of turns assembling at once is the callers' choice.
 */
export const MAX_PROCESS_FETCHES = 32;

/**
 * The runtime never seals a segment below 64 KiB (`MIN_SEAL_BYTES`; merges only grow
 * them), so a value of `maxValueBytes` holds at most `maxValueBytes / 64 KiB` refs. More
 * refs than that are a forged value — refused before fetching (each ref costs a request).
 */
export const MIN_SEGMENT_BYTES = 64 << 10;

/** Process-wide fetch slots: a counter and the assemblies waiting for one (FIFO). */
let fetchesInFlight = 0;
const fetchWaiters: (() => void)[] = [];

async function withFetchSlot<T>(f: () => Promise<T>): Promise<T> {
  if (fetchesInFlight >= MAX_PROCESS_FETCHES) {
    await new Promise<void>((r) => fetchWaiters.push(r));
  } else {
    fetchesInFlight++;
  }
  try {
    return await f();
  } finally {
    const next = fetchWaiters.shift();
    // The slot passes straight to the next waiter; with none, it is freed.
    if (next !== undefined) next();
    else fetchesInFlight--;
  }
}

/** The most refs a value of `maxBytes` (or a turn of `maxBytes` assembled) can carry. */
function refsFor(maxBytes: number): number {
  return Math.max(1, Math.ceil(maxBytes / MIN_SEGMENT_BYTES));
}

/** Time cap of one segment fetch, ms — the turn fails (and is retried) instead of hanging. */
const FETCH_TIMEOUT_MS = 60_000;

/**
 * Size cap of one segment, bytes: the runtime seals from one frame (≤ 16 MiB) and
 * merges up to `MERGE_MAX` 16 MiB, so a larger ref is a forged ref — rejected before fetching.
 */
export const MAX_SEGMENT_BYTES = 16 << 20;

/**
 * Default cap on the logical size of an assembled value, bytes: equal to the
 * tenant's default `max-value-bytes` (64 MiB) and the runtime's `MAX_ASSEMBLE`
 * (`cairn-engine/src/output.rs`) — the runtime does not write a larger value while
 * the tenant keeps the default. A self-hosted cluster that raises `max-value-bytes=`
 * raises the SDK's `maxValueBytes` along with it.
 */
export const DEFAULT_MAX_VALUE_BYTES = 64 << 20;

/**
 * Default cap on the total bytes **assembled** in one turn: Σ `value.len` over
 * every segmented value assembled in the turn — segments that hit the cache count
 * too, because the assembly buffer is allocated at `value.len` even when nothing is
 * fetched. It also covers the fetched bytes (≤ assembled bytes). Equal to the
 * request body cap.
 */
export const DEFAULT_MAX_TURN_VALUE_BYTES = 256 << 20;

/** Assembly caps. */
export interface AssembleLimits {
  /** Maximum logical size of one value; default [`DEFAULT_MAX_VALUE_BYTES`]. */
  readonly maxValueBytes?: number | undefined;
  /**
   * What is left of the turn's budget — `value.len` is subtracted from `left` and the ref
   * count from `refs` before fetching or allocating.
   */
  readonly budget?: { left: number; refs: number } | undefined;
}

/** The minimal `fetch` [`assemble`] needs — `globalThis.fetch` satisfies it. */
export type FetchLike = (url: string, init?: { signal?: AbortSignal; redirect?: "manual" }) => Promise<{
  readonly ok: boolean;
  readonly status: number;
  arrayBuffer(): Promise<ArrayBuffer>;
  readonly headers?: { get(name: string): string | null };
  readonly body?: {
    cancel(): Promise<void>;
    getReader?(): { read(): Promise<{ done: boolean; value?: Uint8Array }>; cancel(): Promise<void> };
  } | null;
}>;

/**
 * Whether a segment read URL may be fetched: only `http:`/`https:`; `origins`
 * set ⇒ only those origins. `segment_urls` is supplied by the caller — on an
 * endpoint that does not verify identity that is a blind SSRF.
 */
function allowedUrl(url: string, origins: readonly string[] | undefined): boolean {
  let u: URL;
  try {
    u = new URL(url);
  } catch {
    return false;
  }
  if (u.protocol !== "https:" && u.protocol !== "http:") return false;
  if (origins === undefined || origins.length === 0) return true;
  return origins.some((o) => {
    try {
      return new URL(o).origin === u.origin;
    } catch {
      return false;
    }
  });
}

/**
 * The body of one segment, **at most** `len` bytes: a larger `content-length` ⇒
 * error before reading; read as a stream and stop at byte `len + 1`.
 * `null` ⇒ the body is longer than `len`. The buffer grows with the bytes
 * **received**, not with `len`: `len` is written by the caller.
 */
export async function readAtMost(res: Awaited<ReturnType<FetchLike>>, len: number): Promise<Uint8Array | null> {
  const declared = Number(res.headers?.get("content-length") ?? Number.NaN);
  if (Number.isFinite(declared) && declared > len) {
    await res.body?.cancel().catch(() => {});
    return null;
  }
  const reader = res.body?.getReader?.();
  if (reader === undefined) {
    const all = new Uint8Array(await res.arrayBuffer());
    return all.length > len ? null : all;
  }
  const chunks: Uint8Array[] = [];
  let at = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    if (value === undefined) continue;
    if (at + value.length > len) {
      await reader.cancel().catch(() => {});
      return null;
    }
    chunks.push(value);
    at += value.length;
  }
  if (chunks.length === 1) return chunks[0]!;
  const out = new Uint8Array(at);
  let off = 0;
  for (const c of chunks) {
    out.set(c, off);
    off += c.length;
  }
  return out;
}

/**
 * Assembling or checking a segmented value failed: missing URL, HTTP error, wrong
 * length or sha. A **transient** turn error (Error RETRY), not a `TerminalError`,
 * not a journal mismatch.
 */
export class SegmentedValueError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SegmentedValueError";
  }
}

export function hexOf(b: Uint8Array): string {
  let s = "";
  for (const x of b) s += x.toString(16).padStart(2, "0");
  return s;
}

/** Key of the URL table: a ref is `(sha, gen)`, not just the sha. */
export function urlKey(sha256: Uint8Array, gen: bigint): string {
  return `${hexOf(sha256)}:${BigInt.asUintN(64, gen).toString(16)}`;
}

function equal(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

/**
 * Segment cache by sha256 (hex), LRU, capped by **total bytes**. Content is
 * identified by sha, so `gen` is not part of the key: two seals of the same bytes
 * are the same entry. An entry larger than the cap is not held. `maxBytes = 0` ⇒ holds nothing.
 */
export class SegmentCache {
  readonly maxBytes: number;
  /** `Map` keeps insertion order: the first entry is the least recently used (delete + reinsert on use). */
  private readonly items = new Map<string, Uint8Array>();
  private total = 0;

  constructor(maxBytes: number = DEFAULT_VALUE_CACHE_BYTES) {
    if (!Number.isFinite(maxBytes) || maxBytes < 0) {
      throw new RangeError(`valueCacheBytes ${maxBytes} must be a non-negative number`);
    }
    this.maxBytes = maxBytes;
  }

  /** Bytes held. */
  get bytes(): number {
    return this.total;
  }

  get size(): number {
    return this.items.size;
  }

  get(shaHex: string): Uint8Array | undefined {
    const b = this.items.get(shaHex);
    if (b === undefined) return undefined;
    this.items.delete(shaHex);
    this.items.set(shaHex, b);
    return b;
  }

  has(shaHex: string): boolean {
    return this.items.has(shaHex);
  }

  /** Holds `bytes` (sha already checked) under `shaHex`; evicts least recently used entries until it fits the cap. */
  put(shaHex: string, bytes: Uint8Array): void {
    this.delete(shaHex);
    if (bytes.length > this.maxBytes) return;
    this.items.set(shaHex, bytes);
    this.total += bytes.length;
    for (const [k, v] of this.items) {
      if (this.total <= this.maxBytes) break;
      this.items.delete(k);
      this.total -= v.length;
    }
  }

  delete(shaHex: string): void {
    const b = this.items.get(shaHex);
    if (b === undefined) return;
    this.items.delete(shaHex);
    this.total -= b.length;
  }
}

/** Process-wide shared cache when the caller passes none of its own. */
let sharedCache: SegmentCache | undefined;
export function defaultSegmentCache(): SegmentCache {
  sharedCache ??= new SegmentCache();
  return sharedCache;
}

async function fetchSegment(
  index: number,
  ref: SegmentRef,
  url: string,
  fetchImpl: FetchLike,
  origins?: readonly string[],
): Promise<Uint8Array> {
  const hex = hexOf(ref.sha256);
  const what = `segment ${index} (sha256 ${hex})`;
  if (!Number.isSafeInteger(ref.len) || ref.len < 0 || ref.len > MAX_SEGMENT_BYTES) {
    throw new SegmentedValueError(`${what}: the ref declares ${ref.len} bytes, over the ${MAX_SEGMENT_BYTES}-byte segment limit`);
  }
  if (!allowedUrl(url, origins)) {
    throw new SegmentedValueError(`${what}: read URL is not an allowed http(s) origin (segmentOrigins)`);
  }
  // From here on the request goes out: every failure is the same text. The text returns to
  // the runtime and, on an endpoint without identityKeys, to whoever sent the turn — a
  // status, a byte count or a dial error would tell them what is behind the URL.
  const failed = (): SegmentedValueError => new SegmentedValueError(`${what}: ${READ_FAILED}`);
  let res: Awaited<ReturnType<FetchLike>>;
  try {
    const signal = typeof AbortSignal.timeout === "function" ? AbortSignal.timeout(FETCH_TIMEOUT_MS) : undefined;
    // Do not follow redirects: the origin check only covers the first URL. `3xx` (or
    // `opaqueredirect`, status 0, in browsers) ⇒ not `ok` ⇒ the error below.
    res = await fetchImpl(url, signal === undefined ? { redirect: "manual" } : { signal, redirect: "manual" });
  } catch {
    throw failed();
  }
  if (!res.ok) {
    await res.body?.cancel().catch(() => {});
    throw failed();
  }
  let read: Uint8Array | null;
  try {
    read = await readAtMost(res, ref.len);
  } catch {
    throw failed();
  }
  if (read === null || read.length !== ref.len) throw failed();
  const bytes = read;
  const sha = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes as Uint8Array<ArrayBuffer>));
  if (!equal(sha, ref.sha256)) throw failed();
  return bytes;
}

/** The one text of a segment read that failed after the request went out. */
const READ_FAILED = "could not be read, or did not match its ref";

/**
 * The whole bytes of `value`: `segments` in order ‖ `content` (tail). A segment
 * found in `cache` is taken from there; otherwise it is fetched from the URL of
 * `(sha, gen)` in `urls`, length and sha256 checked, then stored in the cache. A
 * missing URL, a non-2xx HTTP status, a length/sha mismatch or a total other than
 * `len` ⇒ [`SegmentedValueError`].
 */
export async function assemble(
  value: WireValue,
  urls: ReadonlyMap<string, string>,
  cache: SegmentCache,
  fetchImpl: FetchLike = globalThis.fetch as unknown as FetchLike,
  origins?: readonly string[],
  limits: AssembleLimits = {},
): Promise<Uint8Array> {
  // Caps before any fetch and any allocation: a ref ≤ 16 MiB repeated is fetched once
  // (deduplicated by sha) but the assembly buffer is `value.len` in size.
  const maxValue = limits.maxValueBytes ?? DEFAULT_MAX_VALUE_BYTES;
  if (!Number.isSafeInteger(value.len) || value.len > maxValue) {
    throw new SegmentedValueError(
      `segmented value declares ${value.len} bytes, over the ${maxValue}-byte value limit (maxValueBytes)`,
    );
  }
  const maxRefs = refsFor(maxValue);
  if (value.segments.length > maxRefs) {
    throw new SegmentedValueError(
      `segmented value has ${value.segments.length} segment refs, over the ${maxRefs}-ref limit of maxValueBytes`,
    );
  }
  // The turn's budget: subtract the assembled size and the refs, even when every segment
  // hits the cache.
  if (limits.budget !== undefined) {
    if (value.len > limits.budget.left) {
      throw new SegmentedValueError(
        "segmented values of this turn exceed the per-turn assembled-bytes limit (maxTurnValueBytes)",
      );
    }
    if (value.segments.length > limits.budget.refs) {
      throw new SegmentedValueError(
        "segmented values of this turn exceed the per-turn segment-ref limit of maxTurnValueBytes",
      );
    }
    limits.budget.left -= value.len;
    limits.budget.refs -= value.segments.length;
  }
  const sum = value.segments.reduce((n, r) => n + r.len, value.content.length);
  if (sum !== value.len) {
    throw new SegmentedValueError(`segmented value declares length ${value.len} but its parts sum to ${sum}`);
  }
  const parts: (Uint8Array | undefined)[] = value.segments.map((r) => {
    const hit = cache.get(hexOf(r.sha256));
    return hit !== undefined && hit.length === r.len ? hit : undefined;
  });
  // Refs that miss the cache, deduplicated by sha: two refs with the same bytes are fetched once.
  const misses = new Map<string, number[]>();
  parts.forEach((p, i) => {
    if (p !== undefined) return;
    const hex = hexOf(value.segments[i]!.sha256);
    const at = misses.get(hex);
    if (at === undefined) misses.set(hex, [i]);
    else at.push(i);
  });
  const jobs = [...misses.values()].map((at) => {
    const i = at[0]!;
    const ref = value.segments[i]!;
    const url = urls.get(urlKey(ref.sha256, ref.gen));
    if (url === undefined) {
      throw new SegmentedValueError(`segment ${i} (sha256 ${hexOf(ref.sha256)}) is not cached and has no read URL`);
    }
    return { at, ref, url };
  });
  let next = 0;
  const worker = async (): Promise<void> => {
    while (next < jobs.length) {
      const job = jobs[next++]!;
      const bytes = await withFetchSlot(() => fetchSegment(job.at[0]!, job.ref, job.url, fetchImpl, origins));
      cache.put(hexOf(job.ref.sha256), bytes);
      for (const i of job.at) parts[i] = bytes;
    }
  };
  await Promise.all(Array.from({ length: Math.min(FETCH_CONCURRENCY, jobs.length) }, worker));

  const out = new Uint8Array(value.len);
  let at = 0;
  for (const p of parts) {
    out.set(p!, at);
    at += p!.length;
  }
  out.set(value.content, at);
  return out;
}

/** A segmented value already read for a key: refs and whole bytes (the refs are a prefix). */
export interface KnownValue {
  readonly refs: readonly SegmentRef[];
  readonly bytes: Uint8Array;
}

/**
 * The largest `k` (≥ 1) such that `next` starts with the bytes of `known.refs[0..k]`;
 * `0` when no ref can be reused. Compared byte by byte — the SDK has the bytes, no
 * hashing needed.
 */
export function reusablePrefix(known: KnownValue, next: Uint8Array): number {
  let k = 0;
  let off = 0;
  for (const r of known.refs) {
    const end = off + r.len;
    if (end > next.length || end > known.bytes.length) break;
    for (let i = off; i < end; i++) if (next[i] !== known.bytes[i]) return k;
    k++;
    off = end;
  }
  return k;
}

/** Assembly result of a frame: the whole bytes and refs of the segmented value. */
export interface Assembled {
  readonly bytes: Uint8Array;
  readonly refs: readonly SegmentRef[];
}

/** Options for reading the segmented values of a turn (cache and `fetch`, for tests). */
export interface ValueOptions {
  readonly cache?: SegmentCache;
  readonly fetch?: FetchLike;
  /** Trusted segment read URL origins — absent ⇒ any `http(s)`. */
  readonly origins?: readonly string[] | undefined;
  /** Maximum logical size of an assembled value; default [`DEFAULT_MAX_VALUE_BYTES`]. */
  readonly maxValueBytes?: number | undefined;
  /**
   * Maximum total bytes assembled in one turn (Σ `value.len`, including segments that
   * hit the cache); default [`DEFAULT_MAX_TURN_VALUE_BYTES`]. BIDI: the whole stream.
   */
  readonly maxTurnValueBytes?: number | undefined;
}

/**
 * Segmented-value state of **one** invocation on one request/stream: the URL table
 * `(sha, gen) → url` (from `Start.segment_urls` and every `SegmentUrls` frame that
 * arrives later) and the assembled copy of each frame. Raw frames do not change —
 * the assembled copy sits alongside, looked up by the frame object itself.
 */
export class Segments {
  private readonly urls = new Map<string, string>();
  private readonly assembled = new WeakMap<Frame, Assembled>();
  /** Every assembled copy of the turn, in order — the source of reusable refs for `Output`/`ctx.run`. */
  private readonly all: Assembled[] = [];
  private readonly cache: SegmentCache;
  private readonly fetchImpl: FetchLike | undefined;
  private readonly origins: readonly string[] | undefined;
  private readonly maxValueBytes: number | undefined;
  /** Assembly bytes and segment refs still allowed in this turn. */
  private readonly budget: { left: number; refs: number };
  /** The turn's wire version: below [`WIRE_SEGMENTED`] nothing is ever fetched. */
  private readonly wire: number;

  /**
   * `wire` = the version the turn runs at, already clamped to the endpoint's cap. Below v6
   * the runtime never sends refs, so URLs are ignored and a ref is refused without a fetch:
   * an endpoint that does not serve v6 (no `identityKeys`, no `segmentOrigins`) must not
   * fetch URLs a caller put in a turn labelled with a lower version.
   */
  constructor(opts: ValueOptions = {}, wire: number = WIRE_SEGMENTED) {
    this.wire = wire;
    this.cache = opts.cache ?? defaultSegmentCache();
    this.fetchImpl = opts.fetch;
    this.origins = opts.origins;
    this.maxValueBytes = opts.maxValueBytes;
    const turn = opts.maxTurnValueBytes ?? DEFAULT_MAX_TURN_VALUE_BYTES;
    this.budget = { left: turn, refs: refsFor(turn) };
  }

  addUrls(urls: readonly SegmentUrl[]): void {
    if (this.wire < WIRE_SEGMENTED) return;
    for (const u of urls) this.urls.set(urlKey(u.sha256, u.gen), u.url);
  }

  /**
   * Assembles every segmented value of `frame`, if any: the result of `GetEagerState`
   * (field 14) and the `value` of a notification (field 5; not signals — their read
   * site only accepts inline). Broken layout or failed assembly ⇒ throws.
   */
  async admit(frame: Frame): Promise<void> {
    let v: WireValue | null = null;
    if (frame.type === MT.GET_EAGER_STATE) {
      v = decodeGetEagerStateValue(frame.payload);
    } else if (isNotification(frame.type) && frame.type !== MT.SIGNAL) {
      const r = decodeNotification(frame.payload).result;
      if (r.kind === "segmented") v = r.value;
    }
    if (v === null || v.segments.length === 0 || this.assembled.has(frame)) return;
    if (this.wire < WIRE_SEGMENTED) {
      throw new SegmentedValueError(
        `a segmented value arrived on wire v${this.wire}; segment refs need wire v${WIRE_SEGMENTED}`,
      );
    }
    const bytes = await assemble(v, this.urls, this.cache, this.fetchImpl, this.origins, {
      maxValueBytes: this.maxValueBytes,
      budget: this.budget,
    });
    const a = { bytes, refs: v.segments };
    this.assembled.set(frame, a);
    this.all.push(a);
  }

  /**
   * Reusable refs for a write-once value (`Output`, a `ctx.run` result): the
   * longest prefix of `next` matching the bytes of a segmented value assembled
   * in this turn. Every such value comes from a journal entry of this same
   * invocation — the runtime holds them under the `Invocation(id)` holder and only
   * accepts refs there. Nothing matches ⇒ empty (sent inline, the node seals it itself).
   */
  reuseFor(next: Uint8Array): readonly SegmentRef[] {
    let best: readonly SegmentRef[] = [];
    let bestLen = 0;
    for (const a of this.all) {
      const k = reusablePrefix(a, next);
      const len = a.refs.slice(0, k).reduce((n, r) => n + r.len, 0);
      if (len > bestLen) {
        best = a.refs.slice(0, k);
        bestLen = len;
      }
    }
    return best;
  }

  /** The assembled copy of `frame` (after [`admit`]), or `undefined`. */
  of(frame: Frame): Assembled | undefined {
    return this.assembled.get(frame);
  }
}
