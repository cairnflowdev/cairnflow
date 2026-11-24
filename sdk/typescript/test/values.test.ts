// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Segmented values — wire v6, SDK side,
 * in process, no binary.
 *
 * Must see:
 * - `Value` with `segments`/`len`, `SegmentRef` (`gen` fixed64), `SegmentUrl`,
 *   `Start.segment_urls` and the `SegmentUrls` frame (0x000D) round-trip through encode/decode;
 *   a wrong layout (`len` mismatch, short sha) is an error, never a guess;
 * - `assemble`: a cache hit downloads nothing; a miss downloads, checks length + sha256
 *   and stores in the cache; wrong sha, wrong length, missing URL, HTTP 404 ⇒ error — and the
 *   error message carries no URL, and after the request went out one generic text;
 * - `SegmentCache` evicts by bytes (LRU), an entry over the limit is not kept;
 * - `reusablePrefix`: the largest k with a matching byte prefix; a mismatch in the first segment ⇒
 *   0;
 * - an RR v6 turn: `get` sees the assembled bytes, `set` sends refs of the matching prefix + tail;
 *   a replayed `set` forgets its refs (the next `set` is inline); a value read and then overwritten
 *   before the result is taken is not remembered as a ref; a failed assembly ⇒ `Error` retry, the
 *   handler does not run; a v5 runtime ⇒ `set` inline;
 * - BIDI v6: a `SegmentUrls` frame mid-turn grants URLs to the completions that follow it.
 */

import assert from "node:assert/strict";
import { type ChildProcess, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { join } from "node:path";
import { after, test } from "node:test";
import { fileURLToPath } from "node:url";
import { EndpointCore } from "../src/endpoint.ts";
import { fetchHandler } from "../src/fetch.ts";
import { serve } from "../src/serve.ts";
import { type Context, type Frame, FrameSplitter, type FrameStream, type Handler, runBidi, runTurn, service } from "../src/index.ts";
import { eachField, Writer } from "../src/protobuf.ts";
import { chainRoot, chainStep } from "../src/replay.ts";
import {
  assemble,
  type FetchLike,
  hexOf,
  reusablePrefix,
  SegmentCache,
  SegmentedValueError,
  Segments,
  urlKey,
  type ValueOptions,
  WIRE_SEGMENTED,
} from "../src/values.ts";
import {
  decodeFrames,
  decodeSegmentUrls,
  decodeStart,
  decodeWireValue,
  encodeFrame,
  encodeGetState,
  encodeSegmentUrls,
  encodeSetState,
  ERROR_RETRY,
  MT,
  type SegmentRef,
  type SegmentUrl,
  ValueLayoutError,
  writeValue,
} from "../src/wire.ts";

const enc = new TextEncoder();
const dec = new TextDecoder();

async function sha256(b: Uint8Array): Promise<Uint8Array> {
  return new Uint8Array(await crypto.subtle.digest("SHA-256", b as Uint8Array<ArrayBuffer>));
}

/** One segment: ref, bytes, fake URL. */
interface Seg {
  readonly ref: SegmentRef;
  readonly bytes: Uint8Array;
  readonly url: SegmentUrl;
}

let nextGen = 0xfedcba9876543210n;
async function seg(bytes: Uint8Array): Promise<Seg> {
  const sha = await sha256(bytes);
  const gen = nextGen++;
  return {
    ref: { sha256: sha, len: bytes.length, gen },
    bytes,
    url: { sha256: sha, gen, url: `https://store.example/values/${hexOf(sha)}?X-Secret-Signature=hunter2` },
  };
}

/** Fake `fetch`: URL → bytes (or an HTTP code), counts calls. */
function stubFetch(routes: Map<string, Uint8Array | number>): FetchLike & { calls: string[] } {
  const calls: string[] = [];
  const f = (async (url: string) => {
    calls.push(url);
    const r = routes.get(url);
    if (r === undefined || typeof r === "number") {
      const status = typeof r === "number" ? r : 404;
      return { ok: false, status, arrayBuffer: async () => new ArrayBuffer(0), body: null };
    }
    return { ok: true, status: 200, arrayBuffer: async () => r.slice().buffer, body: null };
  }) as FetchLike & { calls: string[] };
  f.calls = calls;
  return f;
}

function urlMap(segs: readonly Seg[]): Map<string, string> {
  return new Map(segs.map((s) => [urlKey(s.url.sha256, s.url.gen), s.url.url]));
}

function routesOf(segs: readonly Seg[]): Map<string, Uint8Array | number> {
  return new Map(segs.map((s) => [s.url.url, s.bytes]));
}

/** The JSON value `s` cut into segments `[0, a)`, `[a, b)`, tail `[b, …)`. */
async function segmented(value: unknown, cuts: readonly number[]) {
  const bytes = enc.encode(JSON.stringify(value));
  const segs: Seg[] = [];
  let at = 0;
  for (const c of cuts) {
    segs.push(await seg(bytes.slice(at, c)));
    at = c;
  }
  return { bytes, segs, tail: bytes.slice(at) };
}

// ─────────────────────────────────────────────────────────── layout

test("segmented Value round trip: segments, len, gen fixed64", async () => {
  const a = await seg(enc.encode("hello "));
  const b = await seg(enc.encode("big "));
  const payload = writeValue(enc.encode("world"), [a.ref, b.ref], 15).finish();
  const v = decodeWireValue(payload);
  assert.equal(v.len, 15);
  assert.equal(dec.decode(v.content), "world");
  assert.equal(v.segments.length, 2);
  assert.deepEqual(v.segments[0], a.ref);
  assert.deepEqual(v.segments[1], b.ref);
  assert.equal(v.segments[0]!.gen, a.ref.gen, "gen intact over 64 bits");

  // Inline: bytes identical to the old layout, `len` not written.
  assert.deepEqual(writeValue(enc.encode("x")).finish(), new Writer().bytes(1, enc.encode("x")).finish());
  assert.equal(decodeWireValue(new Writer().bytes(1, enc.encode("abc")).uint64(3, 3n).finish()).len, 3);
});

test("broken Value: len mismatch or short sha ⇒ error", async () => {
  const a = await seg(enc.encode("hello"));
  assert.throws(() => decodeWireValue(writeValue(enc.encode("x"), [a.ref], 7).finish()), ValueLayoutError);
  const short = { ...a.ref, sha256: a.ref.sha256.slice(0, 31) };
  assert.throws(() => decodeWireValue(writeValue(enc.encode("x"), [short], 6).finish()), ValueLayoutError);
  // Inline khai `len` sai.
  assert.throws(() => decodeWireValue(new Writer().bytes(1, enc.encode("abc")).uint64(3, 4n).finish()), ValueLayoutError);
});

test("Start.segment_urls and SegmentUrls round trip", async () => {
  const a = await seg(enc.encode("a"));
  const b = await seg(enc.encode("b"));
  const urls = [a.url, b.url];
  assert.deepEqual(decodeSegmentUrls(encodeSegmentUrls(urls)), urls);

  const w = new Writer().uint32(3, 2).uint64(9, 7n);
  for (const u of urls) w.message(20, new Writer().bytes(1, u.sha256).fixed64(2, u.gen).string(3, u.url));
  const start = decodeStart(w.finish());
  assert.equal(start.knownEntries, 2);
  assert.deepEqual(start.segmentUrls, urls);
  assert.deepEqual(decodeStart(new Writer().uint32(3, 1).finish()).segmentUrls, []);
});

test("SetState reuses refs: segments = refs, content = the rest, len = the whole", async () => {
  const a = await seg(enc.encode("0123456789"));
  const value = enc.encode("0123456789abc");
  const payload = encodeSetState("h", value, "h", [a.ref]);
  let v: ReturnType<typeof decodeWireValue> | undefined;
  eachField(payload, (field, _w, r) => {
    if (field !== 3) return false;
    v = decodeWireValue(r.bytes());
    return true;
  });
  assert.equal(v!.len, 13);
  assert.equal(dec.decode(v!.content), "abc");
  assert.deepEqual(v!.segments, [a.ref]);
  // No refs ⇒ bytes identical to the old layout.
  assert.deepEqual(
    encodeSetState("h", value, "h"),
    new Writer().bytes(1, enc.encode("h")).message(3, new Writer().bytes(1, value)).string(12, "h").finish(),
  );
});

// ─────────────────────────────────────────────────────────── assembly

test("assemble: miss ⇒ download and store; hit ⇒ no download", async () => {
  const { bytes, segs, tail } = await segmented("x".repeat(3000), [1000, 2000]);
  const value = { content: tail, segments: segs.map((s) => s.ref), len: bytes.length };
  const cache = new SegmentCache();
  const f = stubFetch(routesOf(segs));
  assert.deepEqual(await assemble(value, urlMap(segs), cache, f), bytes);
  assert.equal(f.calls.length, 2);
  assert.equal(cache.size, 2);
  assert.equal(cache.bytes, 2000);
  // Hit: no URL needed, no download.
  assert.deepEqual(await assemble(value, new Map(), cache, f), bytes);
  assert.equal(f.calls.length, 2);
});

test("assemble: wrong sha, wrong length, missing URL, HTTP 404 ⇒ error without the URL", async () => {
  const { bytes, segs, tail } = await segmented("y".repeat(300), [100]);
  const value = { content: tail, segments: segs.map((s) => s.ref), len: bytes.length };
  const url = segs[0]!.url.url;
  const cases: [string, Map<string, string>, Map<string, Uint8Array | number>, RegExp][] = [
    ["wrong sha", urlMap(segs), new Map([[url, new Uint8Array(100)]]), /could not be read/],
    ["wrong length", urlMap(segs), new Map([[url, segs[0]!.bytes.slice(0, 99)]]), /could not be read/],
    ["missing URL", new Map(), routesOf(segs), /no read URL/],
    ["404", urlMap(segs), new Map([[url, 404]]), /could not be read/],
  ];
  for (const [what, urls, routes, why] of cases) {
    const cache = new SegmentCache();
    await assert.rejects(
      assemble(value, urls, cache, stubFetch(routes)),
      (e: unknown) => {
        assert.ok(e instanceof SegmentedValueError, what);
        assert.match(e.message, why, what);
        assert.doesNotMatch(e.message, /hunter2|store\.example/, `${what}: the URL leaked into the message`);
        return true;
      },
    );
    assert.equal(cache.size, 0, `${what}: unchecked bytes must not be stored`);
  }
  // `fetch` throws (network): the message carries no URL.
  const boom: FetchLike = async (u) => {
    throw new TypeError(`fetch failed for ${u}`);
  };
  await assert.rejects(assemble(value, urlMap(segs), new SegmentCache(), boom), (e: unknown) => {
    assert.ok(e instanceof SegmentedValueError);
    assert.doesNotMatch(e.message, /hunter2|store\.example/);
    return true;
  });
});

test("SegmentCache: LRU by bytes, an entry over the limit is not kept", () => {
  const c = new SegmentCache(100);
  c.put("a", new Uint8Array(40));
  c.put("b", new Uint8Array(40));
  assert.ok(c.get("a") !== undefined); // a just used ⇒ b is the oldest
  c.put("c", new Uint8Array(40)); // 120 > 100 ⇒ evict b
  assert.equal(c.has("b"), false);
  assert.equal(c.has("a"), true);
  assert.equal(c.has("c"), true);
  assert.equal(c.bytes, 80);
  c.put("huge", new Uint8Array(101));
  assert.equal(c.has("huge"), false);
  assert.equal(c.bytes, 80);
  const none = new SegmentCache(0);
  none.put("a", new Uint8Array(1));
  assert.equal(none.size, 0);
  assert.throws(() => new SegmentCache(-1), RangeError);
});

test("reusablePrefix: the largest k with matching bytes, a mismatch in the first segment ⇒ 0", async () => {
  const bytes = enc.encode("aaaabbbbcccctail");
  const segs = [await seg(bytes.slice(0, 4)), await seg(bytes.slice(4, 8)), await seg(bytes.slice(8, 12))];
  const known = { refs: segs.map((s) => s.ref), bytes };
  assert.equal(reusablePrefix(known, enc.encode("aaaabbbbcccctail+more")), 3);
  assert.equal(reusablePrefix(known, enc.encode("aaaabbbbcccc")), 3);
  assert.equal(reusablePrefix(known, enc.encode("aaaabbbbXccc")), 2, "mismatch in segment 3");
  assert.equal(reusablePrefix(known, enc.encode("aaaabbb")), 1, "the new value is shorter than segment 2");
  assert.equal(reusablePrefix(known, enc.encode("Xaaabbbbcccc")), 0);
  assert.equal(reusablePrefix(known, new Uint8Array()), 0);
});

// ─────────────────────────────────────────────────────────── RR turns

const ID = new Uint8Array(24).fill(0x22);

function startFrame(known: number, urls: readonly SegmentUrl[] = []): Uint8Array {
  const w = new Writer().bytes(1, ID).uint32(3, known).string(6, "k").uint64(9, 7n);
  for (const u of urls) w.message(20, new Writer().bytes(1, u.sha256).fixed64(2, u.gen).string(3, u.url));
  return encodeFrame(MT.START, 0, w.finish());
}

function inputFrame(value: unknown): Uint8Array {
  return encodeFrame(MT.INPUT, 0, new Writer().message(14, new Writer().bytes(1, enc.encode(JSON.stringify(value)))).finish());
}

function getStateFrame(key: string, id: number): Uint8Array {
  return encodeFrame(MT.GET_STATE, 0, encodeGetState(key, id, key));
}

function segmentedCompletion(id: number, tail: Uint8Array, refs: readonly SegmentRef[], len: number): Uint8Array {
  return encodeFrame(MT.GET_STATE_COMPLETION, 0, new Writer().uint32(1, id).message(5, writeValue(tail, refs, len)).finish());
}

function cat(...parts: Uint8Array[]): Uint8Array {
  return new Uint8Array(Buffer.concat(parts));
}

/** The `Value` of every `SetState` in the response. */
function setValues(out: Uint8Array): ReturnType<typeof decodeWireValue>[] {
  const vs: ReturnType<typeof decodeWireValue>[] = [];
  for (const f of decodeFrames(out)) {
    if (f.type !== MT.SET_STATE) continue;
    eachField(f.payload, (field, _w, r) => {
      if (field !== 3) return false;
      vs.push(decodeWireValue(r.bytes()));
      return true;
    });
  }
  return vs;
}

const appendZ: Handler = async (ctx: Context) => {
  const h = (await ctx.get<string>("h")) ?? "";
  ctx.set("h", `${h}zz`);
  return h.length;
};

test("RR v6: get sees the assembled bytes, set sends refs of the prefix + tail", async () => {
  const history = "m".repeat(2500);
  const { bytes, segs, tail } = await segmented(history, [1000, 2000]);
  const refs = segs.map((s) => s.ref);
  const body = cat(
    startFrame(3, segs.map((s) => s.url)),
    inputFrame(null),
    getStateFrame("h", 1),
    segmentedCompletion(1, tail, refs, bytes.length),
  );
  const f = stubFetch(routesOf(segs));
  const out = await runTurn(appendZ, body, WIRE_SEGMENTED, { cache: new SegmentCache(), fetch: f });
  const frames = decodeFrames(out);
  assert.deepEqual(frames.map((x) => x.type), [MT.SET_STATE, MT.OUTPUT, MT.END]);
  assert.match(dec.decode(frames[1]!.payload), /2500/, "the handler sees the whole value, not the tail");
  const [v] = setValues(out);
  const next = enc.encode(JSON.stringify(`${history}zz`));
  assert.equal(v!.len, next.length);
  assert.deepEqual(v!.segments, refs, "both segments are a prefix of the new value");
  assert.deepEqual(v!.content, next.subarray(2000));
  assert.equal(f.calls.length, 2);
});

/** The `Value` of the `Output` frame in the response. */
function outputValue(out: Uint8Array): ReturnType<typeof decodeWireValue> | undefined {
  let v: ReturnType<typeof decodeWireValue> | undefined;
  for (const f of decodeFrames(out)) {
    if (f.type !== MT.OUTPUT) continue;
    eachField(f.payload, (field, _w, r) => {
      if (field !== 14) return false;
      v = decodeWireValue(r.bytes());
      return true;
    });
  }
  return v;
}

test("RR v6: Output returns exactly the value just read ⇒ refs reused + tail", async () => {
  const history = "m".repeat(2500);
  const { bytes, segs, tail } = await segmented(history, [1000, 2000]);
  const refs = segs.map((s) => s.ref);
  const body = cat(
    startFrame(3, segs.map((s) => s.url)),
    inputFrame(null),
    getStateFrame("h", 1),
    segmentedCompletion(1, tail, refs, bytes.length),
  );
  const messages: Handler = async (ctx: Context) => (await ctx.get<string>("h")) ?? "";
  const out = await runTurn(messages, body, WIRE_SEGMENTED, { cache: new SegmentCache(), fetch: stubFetch(routesOf(segs)) });
  const v = outputValue(out);
  assert.ok(v !== undefined, "there is an Output");
  assert.equal(v.len, bytes.length, "len is the logical length");
  assert.deepEqual(v.segments, refs, "both segments are a prefix of the output");
  assert.deepEqual(v.content, bytes.subarray(2000), "only the rest goes inline");
});

test("RR v6: Output differs from the value read ⇒ inline, no refs", async () => {
  const { bytes, segs, tail } = await segmented("m".repeat(2500), [1000, 2000]);
  const body = cat(
    startFrame(3, segs.map((s) => s.url)),
    inputFrame(null),
    getStateFrame("h", 1),
    segmentedCompletion(1, tail, segs.map((s) => s.ref), bytes.length),
  );
  const other: Handler = async (ctx: Context) => `x${(await ctx.get<string>("h")) ?? ""}`;
  const out = await runTurn(other, body, WIRE_SEGMENTED, { cache: new SegmentCache(), fetch: stubFetch(routesOf(segs)) });
  const v = outputValue(out);
  assert.equal(v?.segments.length, 0);
});

test("RR v6: a ref read and then overwritten before the result is taken ⇒ not reused", async () => {
  const history = "q".repeat(1500);
  const { bytes, segs, tail } = await segmented(history, [1000]);
  const body = cat(
    startFrame(3, segs.map((s) => s.url)),
    inputFrame(null),
    getStateFrame("h", 1),
    segmentedCompletion(1, tail, segs.map((s) => s.ref), bytes.length),
  );
  const h: Handler = async (ctx: Context) => {
    const f = ctx.get<string>("h");
    ctx.set("h", "x");
    const old = (await f) ?? "";
    ctx.set("h", `${old}!`);
    return null;
  };
  const out = await runTurn(h, body, WIRE_SEGMENTED, { cache: new SegmentCache(), fetch: stubFetch(routesOf(segs)) });
  const vs = setValues(out);
  assert.equal(vs.length, 2);
  assert.ok(vs.every((v) => v.segments.length === 0), "a set after an overwrite must be inline");
});

test("RR v6: a replayed set forgets its refs ⇒ the next set is inline", async () => {
  const history = "r".repeat(1500);
  const { bytes, segs, tail } = await segmented(history, [1000]);
  const refs = segs.map((s) => s.ref);
  const first = enc.encode(JSON.stringify(`${history}zz`));
  const replayedSet = encodeFrame(MT.SET_STATE, 0, encodeSetState("h", first, "h", refs));
  const body = cat(
    startFrame(4, segs.map((s) => s.url)),
    inputFrame(null),
    getStateFrame("h", 1),
    segmentedCompletion(1, tail, refs, bytes.length),
    replayedSet,
  );
  const h: Handler = async (ctx: Context) => {
    const old = (await ctx.get<string>("h")) ?? "";
    ctx.set("h", `${old}zz`); // already in the journal
    ctx.set("h", `${old}yy`); // new
    return null;
  };
  const out = await runTurn(h, body, WIRE_SEGMENTED, { cache: new SegmentCache(), fetch: stubFetch(routesOf(segs)) });
  const vs = setValues(out);
  assert.equal(vs.length, 1, "a replayed set is not resent");
  assert.equal(vs[0]!.segments.length, 0);
  assert.equal(dec.decode(vs[0]!.content), JSON.stringify(`${history}yy`));
});

test("RR v6: a new set after a new set ⇒ refs already sent keep being reused", async () => {
  const history = "s".repeat(1500);
  const { bytes, segs, tail } = await segmented(history, [1000]);
  const refs = segs.map((s) => s.ref);
  const body = cat(
    startFrame(3, segs.map((s) => s.url)),
    inputFrame(null),
    getStateFrame("h", 1),
    segmentedCompletion(1, tail, refs, bytes.length),
  );
  const h: Handler = async (ctx: Context) => {
    const old = (await ctx.get<string>("h")) ?? "";
    ctx.set("h", `${old}a`);
    ctx.set("h", `${old}ab`);
    ctx.set("h", "short");
    return null;
  };
  const out = await runTurn(h, body, WIRE_SEGMENTED, { cache: new SegmentCache(), fetch: stubFetch(routesOf(segs)) });
  const vs = setValues(out);
  assert.deepEqual(vs.map((v) => v.segments.length), [1, 1, 0]);
});

test("RR v6: failed assembly ⇒ Error retry, the handler does not run, no URL leaked", async () => {
  const { bytes, segs, tail } = await segmented("t".repeat(1500), [1000]);
  const body = cat(
    startFrame(3, segs.map((s) => s.url)),
    inputFrame(null),
    getStateFrame("h", 1),
    segmentedCompletion(1, tail, segs.map((s) => s.ref), bytes.length),
  );
  let ran = false;
  const h: Handler = async () => {
    ran = true;
    return null;
  };
  const out = await runTurn(h, body, WIRE_SEGMENTED, { cache: new SegmentCache(), fetch: stubFetch(new Map()) });
  const frames = decodeFrames(out);
  assert.deepEqual(frames.map((x) => x.type), [MT.ERROR]);
  assert.equal(ran, false);
  let code = 0;
  let behavior = -1;
  let message = "";
  eachField(frames[0]!.payload, (field, _w, r) => {
    if (field === 1) code = Number(r.varint());
    else if (field === 2) message = r.string();
    else if (field === 9) behavior = Number(r.varint());
    else return false;
    return true;
  });
  assert.equal(code, 500);
  assert.equal(behavior === -1 ? ERROR_RETRY : behavior, ERROR_RETRY);
  assert.match(message, /could not be read/);
  assert.doesNotMatch(message, /hunter2|store\.example/);
});

test("RR v6: refs of a journaled GetEagerState are not reused (the runtime sealed that entry)", async () => {
  const history = "e".repeat(1500);
  const { bytes, segs, tail } = await segmented(history, [1000]);
  const eager = encodeFrame(
    MT.GET_EAGER_STATE,
    0,
    new Writer().bytes(1, enc.encode("h")).string(12, "h").message(14, writeValue(tail, segs.map((s) => s.ref), bytes.length)).finish(),
  );
  const body = cat(startFrame(2, segs.map((s) => s.url)), inputFrame(null), eager);
  const out = await runTurn(appendZ, body, WIRE_SEGMENTED, { cache: new SegmentCache(), fetch: stubFetch(routesOf(segs)) });
  const frames = decodeFrames(out);
  assert.deepEqual(frames.map((x) => x.type), [MT.SET_STATE, MT.OUTPUT, MT.END]);
  assert.match(dec.decode(frames[1]!.payload), /1500/, "a segmented GetEagerState is assembled");
  assert.equal(setValues(out)[0]!.segments.length, 0);
});

test("RR v6: an in-place eager read after a new set keeps the refs already sent", async () => {
  const history = "f".repeat(1500);
  const { bytes, segs, tail } = await segmented(history, [1000]);
  // A partial eager snapshot (field 5): `h` absent ⇒ the first get is lazy, the get after the set
  // reads in place.
  const w = new Writer().bytes(1, ID).uint32(3, 3).uint32(5, 1).string(6, "k").uint64(9, 7n);
  for (const s of segs) w.message(20, new Writer().bytes(1, s.url.sha256).fixed64(2, s.url.gen).string(3, s.url.url));
  const body = cat(
    encodeFrame(MT.START, 0, w.finish()),
    inputFrame(null),
    getStateFrame("h", 1),
    segmentedCompletion(1, tail, segs.map((s) => s.ref), bytes.length),
  );
  const h: Handler = async (ctx: Context) => {
    const old = (await ctx.get<string>("h")) ?? "";
    ctx.set("h", `${old}a`);
    const again = (await ctx.get<string>("h")) ?? "";
    ctx.set("h", `${again}b`);
    return null;
  };
  const out = await runTurn(h, body, WIRE_SEGMENTED, { cache: new SegmentCache(), fetch: stubFetch(routesOf(segs)) });
  assert.deepEqual(
    decodeFrames(out).map((x) => x.type),
    [MT.SET_STATE, MT.GET_EAGER_STATE, MT.SET_STATE, MT.OUTPUT, MT.END],
  );
  assert.deepEqual(setValues(out).map((v) => v.segments.length), [1, 1]);
});

test("RR wire 5 meeting a ref: Error retry without fetching, never reads the tail as the value", async () => {
  // The runtime never sends refs below v6 (it assembles inline): a ref there comes from
  // a forged turn, and fetching its URL is what an endpoint capped at v5 must never do.
  const history = "w".repeat(1500);
  const { bytes, segs, tail } = await segmented(history, [1000]);
  const body = cat(
    startFrame(3, segs.map((s) => s.url)),
    inputFrame(null),
    getStateFrame("h", 1),
    segmentedCompletion(1, tail, segs.map((s) => s.ref), bytes.length),
  );
  const f = stubFetch(routesOf(segs));
  const out = await runTurn(appendZ, body, 5, { cache: new SegmentCache(), fetch: f });
  assert.deepEqual(decodeFrames(out).map((x) => x.type), [MT.ERROR]);
  assert.deepEqual(f.calls, [], "below v6 nothing is fetched");
});

test("RR v5: set is always inline (no ref is sent before v6 is settled)", async () => {
  const history = "u".repeat(10);
  const body = cat(
    startFrame(3),
    inputFrame(null),
    getStateFrame("h", 1),
    encodeFrame(MT.GET_STATE_COMPLETION, 0, new Writer().uint32(1, 1).message(5, writeValue(enc.encode(JSON.stringify(history)))).finish()),
  );
  const out = await runTurn(appendZ, body, 5);
  const vs = setValues(out);
  assert.equal(vs.length, 1);
  assert.equal(vs[0]!.segments.length, 0);
});

// ─────────────────────────────────────────────────────────── BIDI

/** The runtime end of the stream (as in `bidi.test.ts`). */
class Wire implements FrameStream {
  readonly sent: Frame[] = [];
  ended = false;
  private readonly inbox: Frame[] = [];
  private readonly split = new FrameSplitter();
  private waiter: ((f: Frame | null) => void) | null = null;

  push(...parts: Uint8Array[]): void {
    for (const p of parts) {
      for (const f of this.split.push(p)) {
        const w = this.waiter;
        if (w !== null) {
          this.waiter = null;
          w(f);
        } else {
          this.inbox.push(f);
        }
      }
    }
  }

  next(): Promise<Frame | null> {
    const head = this.inbox.shift();
    if (head !== undefined) return Promise.resolve(head);
    return new Promise((resolve) => {
      this.waiter = resolve;
    });
  }

  buffered(): Frame[] {
    return this.inbox.splice(0, this.inbox.length);
  }

  write(bytes: Uint8Array): void {
    for (const f of new FrameSplitter().push(bytes)) this.sent.push(f);
  }

  end(): void {
    this.ended = true;
  }
}

async function until(what: string, cond: () => boolean, tries = 500): Promise<void> {
  for (let i = 0; i < tries; i++) {
    if (cond()) return;
    await new Promise((r) => setTimeout(r, 1));
  }
  throw new Error(`timed out waiting for: ${what}`);
}

test("BIDI v6: SegmentUrls mid-turn grants URLs to the completions after it", async () => {
  const history = "b".repeat(1500);
  const { bytes, segs, tail } = await segmented(history, [1000]);
  const refs = segs.map((s) => s.ref);
  const f = stubFetch(routesOf(segs));
  const wire = new Wire();
  const done = runBidi(appendZ, wire, WIRE_SEGMENTED, { cache: new SegmentCache(), fetch: f });
  wire.push(startFrame(1), inputFrame(null));
  await until("SDK suspended at GetState", () => wire.sent.length >= 2);
  assert.deepEqual(wire.sent.map((x) => x.type), [MT.GET_STATE, MT.SUSPENSION]);
  wire.push(
    encodeFrame(MT.SEGMENT_URLS, 0, encodeSegmentUrls(segs.map((s) => s.url))),
    segmentedCompletion(1, tail, refs, bytes.length),
  );
  await done;
  const types = wire.sent.map((x) => x.type);
  assert.deepEqual(types, [MT.GET_STATE, MT.SUSPENSION, MT.SET_STATE, MT.OUTPUT, MT.END]);
  assert.match(dec.decode(wire.sent[3]!.payload), /1500/);
  const set = wire.sent[2]!;
  let v: ReturnType<typeof decodeWireValue> | undefined;
  eachField(set.payload, (field, _w, r) => {
    if (field !== 3) return false;
    v = decodeWireValue(r.bytes());
    return true;
  });
  assert.deepEqual(v!.segments, refs);
  assert.equal(f.calls.length, 1);
});

test("BIDI v6: a segmented completion without a URL ⇒ Error retry and the stream closes", async () => {
  const { bytes, segs, tail } = await segmented("c".repeat(1500), [1000]);
  const wire = new Wire();
  const done = runBidi(appendZ, wire, WIRE_SEGMENTED, { cache: new SegmentCache(), fetch: stubFetch(routesOf(segs)) });
  wire.push(startFrame(1), inputFrame(null));
  await until("SDK suspended at GetState", () => wire.sent.length >= 2);
  wire.push(segmentedCompletion(1, tail, segs.map((s) => s.ref), bytes.length));
  await done;
  assert.deepEqual(wire.sent.map((x) => x.type), [MT.GET_STATE, MT.SUSPENSION, MT.ERROR]);
  assert.equal(wire.ended, true);
});

test("RR v6 + replay cache: the cache keeps and hashes the **raw** frames (ref + tail), not the assembled copy", async () => {
  const history = "d".repeat(1500);
  const { bytes, segs, tail } = await segmented(history, [1000]);
  const refs = segs.map((s) => s.ref);
  const entries = [inputFrame(null), getStateFrame("h", 1), segmentedCompletion(1, tail, refs, bytes.length)];
  const body = cat(startFrame(3, segs.map((s) => s.url)), ...entries);
  // Suspend after `set` ⇒ the turn does not end, the replay cache keeps the entry.
  const nap = async (ctx: Context) => {
    await appendZ(ctx, null);
    await ctx.sleep(1000);
    return null;
  };
  const svc = service("V", { run: nap }, { type: "VIRTUAL_OBJECT" });
  // The replay cache is only served with `identityKeys`.
  const { signer } = await import("./signer.ts");
  const rt = await signer();
  const core = new EndpointCore([svc], "REQUEST_RESPONSE", WIRE_SEGMENTED, [rt.kid], false, true, 1 << 20);
  const real = globalThis.fetch;
  const f = stubFetch(routesOf(segs));
  globalThis.fetch = f as unknown as typeof fetch;
  try {
    const h: Record<string, string> = {
      "content-type": `application/vnd.cairnflow.invocation.v${WIRE_SEGMENTED}`,
      ...(await rt.headers("/invoke/V/run", body)),
    };
    const a = await core.answer("POST", "/invoke/V/run", "/invoke/V/run", (n) => h[n], async () => body);
    assert.equal(a.status, 200);
    assert.equal(a.headers?.["cairn-replay-cache"], "3");
  } finally {
    globalThis.fetch = real;
  }
  assert.equal(f.calls.length, 1);
  assert.equal(core.values.size, 1, "the endpoint's segment cache is used");
  const chain = [await chainRoot(ID)];
  for (const e of entries) chain.push(await chainStep(chain.at(-1)!, e));
  const held = core.replay!.take(hexOf(ID), 3, chain[3]!);
  assert.ok(held !== undefined, "the digest over the raw frames must match");
  assert.deepEqual(held.frames, entries);
});

// ─────────────────────────────────────────────────────────── real binary

const here = fileURLToPath(new URL(".", import.meta.url));
const binary = process.env.CAIRNFLOW_BINARY ?? join(here, "..", "..", "..", "target", "debug", "cairnflow");
const spawned: ChildProcess[] = [];
const closers: (() => Promise<void>)[] = [];
after(async () => {
  for (const p of spawned) p.kill("SIGKILL");
  for (const c of closers) await c();
});

async function runtime(args: readonly string[]): Promise<string> {
  const proc = spawn(
    binary,
    ["--main-store", "memory://", "--host", "127.0.0.1", "--port", "0", "--no-auth", "--tick-interval-ms", "50", ...args],
    // The node signs requests: wire v6 is only served by an SDK with `identityKeys` (or `segmentOrigins`).
    { stdio: "pipe", env: { ...process.env, CAIRNFLOW_IDENTITY_MASTER: "master-of-values-test-ts-longer-than-32-bytes" } },
  );
  spawned.push(proc);
  proc.stderr!.resume();
  return new Promise((resolve, reject) => {
    let buf = "";
    const t = setTimeout(() => reject(new Error(`node did not report a port:\n${buf}`)), 30_000);
    proc.stdout!.on("data", (c: Buffer) => {
      buf += c.toString();
      const m = /http:\/\/[\d.]+:\d+/.exec(buf);
      if (m) {
        clearTimeout(t);
        resolve(m[0]);
      }
    });
    proc.on("exit", (code) => reject(new Error(`node exited early (${code}):\n${buf}`)));
  });
}

/** A deterministic pseudo-random string — non-repeating, so nothing "compresses away" the measurement. */
function noise(n: number, seed: number): string {
  let x = seed >>> 0;
  let s = "";
  for (let i = 0; i < n; i++) {
    x = (Math.imul(x, 1103515245) + 12345) >>> 0;
    s += String.fromCharCode(97 + ((x >>> 16) % 26));
  }
  return s;
}

const sha256Hex = (s: string) => createHash("sha256").update(s).digest("hex");

const history = service(
  "Hist",
  {
    async put(ctx: Context, n: number) {
      ctx.set("h", noise(n, 1));
      return n;
    },
    async append(ctx: Context, more: string) {
      const h = (await ctx.get<string>("h")) ?? "";
      ctx.set("h", h + more);
      return h.length;
    },
    async read(ctx: Context) {
      const h = (await ctx.get<string>("h")) ?? "";
      return { len: h.length, sha: sha256Hex(h) };
    },
  },
  { type: "VIRTUAL_OBJECT" },
);

/**
 * Three calls on one object: `put` 600 KiB (over the 256 KiB sealing threshold),
 * `append` (get + set appending more), `read`. Must see: the value round-trips correctly
 * (length + sha256), the SDK downloads segments through the node's `proxy` URL, and once the value
 * is segmented the runtime → SDK bodies (RR: every request) and SDK → runtime bodies
 * (`set` reusing refs) are all under 64 KiB.
 */
test(
  "real binary v6 (--value-read-path proxy): 600 KiB round trip, small bodies once segmented",
  {
    skip: existsSync(binary) ? false : "cairnflow binary not built",
    timeout: 120_000,
  },
  async () => {
    const url = await runtime(["--value-read-path", "proxy"]);
    const id = (await (await fetch(`${url}/identity`)).json()) as { keys: { kid: string; active: boolean }[] };
    const identityKeys = id.keys.filter((k) => k.active).map((k) => k.kid);
    // RR through the Fetch adapter, behind a `node:http` server that measures each request/response
    // body.
    const h = fetchHandler({ services: [history], compression: false, identityKeys });
    const sizes: { path: string; req: number; res: number; ct: string }[] = [];
    const server = createServer((req, res) => {
      void (async () => {
        const chunks: Buffer[] = [];
        for await (const c of req) chunks.push(c as Buffer);
        const body = Buffer.concat(chunks);
        const headers = new Headers();
        for (const [k, v] of Object.entries(req.headers)) if (typeof v === "string") headers.set(k, v);
        const method = req.method ?? "GET";
        const r = await h(
          // The host the runtime signed (`host` claim): the request's own.
          new Request(`http://${req.headers.host}${req.url}`, { method, headers, ...(method === "GET" ? {} : { body }) }),
        );
        const out = Buffer.from(await r.arrayBuffer());
        if ((req.url ?? "").startsWith("/invoke/")) {
          sizes.push({ path: req.url!, req: body.length, res: out.length, ct: req.headers["content-type"] ?? "" });
        }
        const hs: Record<string, string> = {};
        r.headers.forEach((v, k) => {
          hs[k] = v;
        });
        res.writeHead(r.status, hs);
        res.end(out);
      })();
    });
    await new Promise<void>((ok) => server.listen(0, "127.0.0.1", ok));
    closers.push(() => new Promise<void>((ok) => server.close(() => ok())));
    const origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;

    const reg = await fetch(`${url}/deployments`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ uri: origin }),
    });
    assert.equal(reg.status, 200, await reg.text());

    // Counts the SDK's segment downloads (the node's `proxy` URL: `/values/v1/<token>`).
    const real = globalThis.fetch;
    let segmentFetches = 0;
    globalThis.fetch = ((input: string | URL | Request, init?: RequestInit) => {
      const u = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
      if (u.includes("/values/v1/")) segmentFetches++;
      return real(input, init);
    }) as typeof fetch;
    try {
      const call = async (handler: string, body: unknown) => {
        const r = await real(`${url}/invoke/Hist/${handler}?key=k1`, { method: "POST", body: JSON.stringify(body) });
        let v = (await r.json()) as { status: string; output?: string; id?: string };
        // Not finished within `/invoke`'s wait ⇒ ask again until done.
        for (let i = 0; i < 100 && v.status !== "completed" && v.id !== undefined; i++) {
          await new Promise((ok) => setTimeout(ok, 200));
          v = (await (await real(`${url}/invocations/${v.id}`)).json()) as typeof v;
        }
        assert.equal(v.status, "completed", `${handler}: ${JSON.stringify(v)}`);
        return JSON.parse(v.output!) as unknown;
      };
      const n = 600 << 10;
      const more = noise(1000, 2);
      assert.equal(await call("put", n), n);
      const afterPut = sizes.length;
      assert.equal(await call("append", more), n);
      const expected = noise(n, 1) + more;
      assert.deepEqual(await call("read", null), { len: expected.length, sha: sha256Hex(expected) });
      const later = sizes.slice(afterPut);
      assert.ok(later.length >= 2, JSON.stringify(sizes));
      assert.ok(
        later.every((s) => s.ct.endsWith(`.v${WIRE_SEGMENTED}`)),
        `the runtime must send content-type v${WIRE_SEGMENTED} for a deployment settled on v${WIRE_SEGMENTED}: ${[...new Set(later.map((s) => s.ct))]}`,
      );
      for (const s of later) {
        assert.ok(s.req < 64 << 10, `runtime → SDK ${s.path}: ${s.req} byte`);
        assert.ok(s.res < 64 << 10, `SDK → runtime ${s.path}: ${s.res} bytes (set must reuse refs)`);
      }
      assert.ok(segmentFetches >= 1, "the SDK must download segments through the proxy URL");
      process.stderr.write(`# /invoke bodies after put: ${JSON.stringify(later.map((s) => [s.req, s.res]))}, segment downloads: ${segmentFetches}\n`);
    } finally {
      globalThis.fetch = real;
    }
  },
);

const chatShape = (name: string) =>
  service(
    name,
    {
      async send(ctx: Context, n: number) {
        ctx.set("messages", [{ id: "m1", text: noise(n, 5) }]);
        return "ok";
      },
      async messages(ctx: Context) {
        return (await ctx.get<unknown[]>("messages")) ?? [];
      },
      async tool(ctx: Context, n: number) {
        const r = await ctx.run("fetch", () => noise(n, 6));
        return { len: r.length, sha: sha256Hex(r) };
      },
    },
    { type: "VIRTUAL_OBJECT", handlerTypes: { messages: "SHARED" } },
  );

/**
 * The `chatObject` shape on a tenant with `max-payload=1 MiB`, both BIDI and RR: `send` writes a
 * 1.5 MiB history, `messages` returns the whole history (output > the limit), `tool` has a 1.5 MiB
 * `ctx.run` result. Before segmented values the latter two `failed` with 413. Must see: all three
 * `completed`, `messages` returns the exact history, `tool` has the right length + sha.
 */
test(
  "real binary v6: history and ctx.run results larger than max-payload, BIDI and RR",
  {
    skip: existsSync(binary) ? false : "cairnflow binary not built",
    timeout: 180_000,
  },
  async () => {
    const url = await runtime(["--value-read-path", "proxy"]);
    const limit = await fetch(`${url}/admin/tenants/default`, {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ maxPayload: 1 << 20 }),
    });
    assert.equal(limit.status, 200, await limit.text());
    const id = (await (await fetch(`${url}/identity`)).json()) as { keys: { kid: string; active: boolean }[] };
    const identityKeys = id.keys.filter((k) => k.active).map((k) => k.kid);
    const n = 3 << 19;
    for (const mode of ["BIDI_STREAM", "REQUEST_RESPONSE"] as const) {
      const name = mode === "BIDI_STREAM" ? "ChatB" : "ChatR";
      const ep = await serve({ services: [chatShape(name)], port: 0, protocolMode: mode, identityKeys });
      closers.push(() => ep.close());
      const reg = await fetch(`${url}/deployments`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ uri: ep.url }),
      });
      assert.equal(reg.status, 200, `${mode}: ${await reg.text()}`);
      const call = async (handler: string, body: unknown) => {
        const r = await fetch(`${url}/invoke/${name}/${handler}?key=c1`, { method: "POST", body: JSON.stringify(body) });
        let v = (await r.json()) as { status: string; output?: string; id?: string };
        for (let i = 0; i < 150 && v.status !== "completed" && v.id !== undefined; i++) {
          await new Promise((ok) => setTimeout(ok, 200));
          v = (await (await fetch(`${url}/invocations/${v.id}`)).json()) as typeof v;
        }
        assert.equal(v.status, "completed", `${mode} ${handler}: ${JSON.stringify(v).slice(0, 400)}`);
        return JSON.parse(v.output!) as unknown;
      };
      assert.equal(await call("send", n), "ok");
      assert.deepEqual(await call("messages", null), [{ id: "m1", text: noise(n, 5) }], `${mode}: messages`);
      const want = noise(n, 6);
      assert.deepEqual(await call("tool", n), { len: want.length, sha: sha256Hex(want) }, `${mode}: tool`);
    }
  },
);

// ─────────────────────────────────────────── bounded segment downloads

/**
 * `fetchSegment` used to read the whole body (`res.arrayBuffer()`) **before**
 * comparing with `ref.len` — a URL returning an endless body filled the endpoint's RAM. Must
 * see: a body longer than `ref.len` ⇒ an error after ≤ `ref.len` + one chunk, connection closed;
 * `content-length` > `ref.len` ⇒ an error before reading.
 */
test("segment download stops at ref.len — an endless body or a larger content-length ⇒ error, not read to the end", async () => {
  const { bytes, segs, tail } = await segmented("z".repeat(200), [100]);
  const value = { content: tail, segments: segs.map((s) => s.ref), len: bytes.length };
  let sent = 0;
  let closed = false;
  const srv = createServer((req, res) => {
    if (req.url === "/declared") {
      res.writeHead(200, { "content-length": String(1 << 30) });
      res.write(Buffer.alloc(16));
      return;
    }
    res.writeHead(200);
    const pump = () => {
      while (!closed && res.write(Buffer.alloc(64 << 10, 0x7a))) {
        sent += 64 << 10;
        if (sent > 512 << 20) return;
      }
      if (!closed) res.once("drain", pump);
    };
    res.on("close", () => {
      closed = true;
    });
    pump();
  });
  await new Promise<void>((r) => srv.listen(0, "127.0.0.1", () => r()));
  const port = (srv.address() as AddressInfo).port;
  try {
    for (const path of ["/endless", "/declared"]) {
      const urls = new Map([[urlKey(segs[0]!.ref.sha256, segs[0]!.ref.gen), `http://127.0.0.1:${port}${path}`]]);
      const t0 = Date.now();
      await assert.rejects(
        Promise.race([
          assemble(value, urls, new SegmentCache()),
          new Promise((_, rej) => setTimeout(() => rej(new Error("reading did not stop at ref.len")), 5_000)),
        ]),
        (e: unknown) => e instanceof SegmentedValueError,
        path,
      );
      assert.ok(Date.now() - t0 < 5_000, path);
    }
    assert.ok(sent < 64 << 20, `the endpoint read ${sent} bytes for a 100-byte segment`);
  } finally {
    closed = true;
    srv.closeAllConnections();
    srv.close();
  }
});

/**
 * `Start.segment_urls` is supplied by the caller; the SDK used to `fetch` any URL
 * — a blind SSRF when the endpoint does not verify identity. Must see: only `http:`/`https:`;
 * with `segmentOrigins` set ⇒ URLs outside those origins are rejected **before** downloading.
 */
test("segment_urls — unknown schemes are always rejected; segmentOrigins restricts origins", async () => {
  const { bytes, segs, tail } = await segmented("w".repeat(200), [100]);
  const value = { content: tail, segments: segs.map((s) => s.ref), len: bytes.length };
  const key = urlKey(segs[0]!.ref.sha256, segs[0]!.ref.gen);
  const stub = stubFetch(new Map([["file:///etc/passwd", segs[0]!.bytes], [segs[0]!.url.url, segs[0]!.bytes]]));
  await assert.rejects(assemble(value, new Map([[key, "file:///etc/passwd"]]), new SegmentCache(), stub), SegmentedValueError);
  assert.deepEqual(stub.calls, [], "scheme file: must not be downloaded");
  // An origin outside the list (store.example is not in segmentOrigins).
  const only = ["https://values.cairn.example"];
  await assert.rejects(
    (assemble as (...a: unknown[]) => Promise<Uint8Array>)(value, urlMap(segs), new SegmentCache(), stub, only),
    SegmentedValueError,
  );
  assert.deepEqual(stub.calls, [], "an origin outside segmentOrigins must not be downloaded");
  // An origin in the list ⇒ downloaded.
  const ok = await (assemble as (...a: unknown[]) => Promise<Uint8Array>)(
    value,
    urlMap(segs),
    new SegmentCache(),
    stub,
    ["https://store.example"],
  );
  assert.deepEqual(ok, bytes);
});

// ─────────────────────────────────────────── caller-chosen sizes and redirects


/**
 * (a) `readAtMost` used to allocate `new Uint8Array(ref.len)` **before** reading — `ref.len`
 * is written by the caller (an endpoint without `identityKeys` accepts turns from anyone), so
 * a 1 GiB ref whose URL returns 10 bytes without `content-length` still allocated 1 GiB.
 *
 * Must see: a ref over the runtime's segment limit (16 MiB) ⇒ error before downloading; a ref
 * of 8 MiB whose body is only 10 bytes ⇒ error, `ArrayBuffer` memory does not grow with `ref.len`.
 */
test("a large ref.len does not preallocate a buffer — over the limit ⇒ error before downloading, a short body ⇒ allocated by bytes received", async () => {
  let hits = 0;
  const srv = createServer((_req, res) => {
    hits++;
    res.writeHead(200); // no content-length: a chunked body
    res.end(Buffer.alloc(10, 0x61));
  });
  await new Promise<void>((r) => srv.listen(0, "127.0.0.1", () => r()));
  const port = (srv.address() as AddressInfo).port;
  try {
    const ref = (len: number) => ({ sha256: new Uint8Array(32).fill(7), len, gen: 1n });
    const huge = ref(1 << 30);
    const urls = new Map([[urlKey(huge.sha256, huge.gen), `http://127.0.0.1:${port}/huge`]]);
    await assert.rejects(
      assemble({ content: new Uint8Array(0), segments: [huge], len: huge.len }, urls, new SegmentCache()),
      SegmentedValueError,
    );
    assert.equal(hits, 0, "a ref over the segment limit was still downloaded");
    const mid = ref(8 << 20);
    const before = process.memoryUsage().arrayBuffers;
    await assert.rejects(
      assemble({ content: new Uint8Array(0), segments: [mid], len: mid.len }, urls, new SegmentCache()),
      SegmentedValueError,
    );
    const grew = process.memoryUsage().arrayBuffers - before;
    assert.ok(grew < 4 << 20, `ArrayBuffer grew ${grew} bytes for a 10-byte body (8 MiB ref)`);
  } finally {
    srv.closeAllConnections();
    srv.close();
  }
});

/**
 * (b) The segment `fetch` used to follow redirects (default `"follow"`) — with
 * `segmentOrigins` set, a `3xx` from a trusted origin led to an origin outside the list:
 * the origin check only covered the first URL.
 *
 * Must see: a trusted origin answering `302` to another origin ⇒ error; the other origin receives
 * no request.
 */
test("segment download does not follow a redirect to another origin", async () => {
  const { bytes, segs, tail } = await segmented("r".repeat(200), [100]);
  const value = { content: tail, segments: segs.map((s) => s.ref), len: bytes.length };
  let elsewhere = 0;
  const other = createServer((_req, res) => {
    elsewhere++;
    res.writeHead(200, { "content-length": String(segs[0]!.bytes.length) });
    res.end(Buffer.from(segs[0]!.bytes));
  });
  await new Promise<void>((r) => other.listen(0, "127.0.0.1", () => r()));
  const otherPort = (other.address() as AddressInfo).port;
  const trusted = createServer((_req, res) => {
    res.writeHead(302, { location: `http://127.0.0.1:${otherPort}/seg` });
    res.end();
  });
  await new Promise<void>((r) => trusted.listen(0, "127.0.0.1", () => r()));
  const port = (trusted.address() as AddressInfo).port;
  try {
    const urls = new Map([[urlKey(segs[0]!.ref.sha256, segs[0]!.ref.gen), `http://127.0.0.1:${port}/seg`]]);
    await assert.rejects(
      (assemble as (...a: unknown[]) => Promise<Uint8Array>)(value, urls, new SegmentCache(), globalThis.fetch, [
        `http://127.0.0.1:${port}`,
      ]),
      SegmentedValueError,
    );
    assert.equal(elsewhere, 0, "a redirect led the download to an origin outside segmentOrigins");
  } finally {
    for (const s of [trusted, other]) {
      s.closeAllConnections();
      s.close();
    }
  }
});

// ─────────────────────────────────────────── value size limit and bytes downloaded per turn

/**
 * `assemble` allocates `new Uint8Array(value.len)` after downloading — each ref is ≤ 16 MiB, but a
 * fake `Value` repeating one ref many times (downloaded once thanks to dedup by sha) amplifies into
 * an arbitrary buffer; and one turn could assemble any number of values.
 *
 * Must see: `value.len` > the limit (64 MiB by default, like the default `max-value-bytes`
 * and the runtime's `MAX_ASSEMBLE`) ⇒ error before downloading.
 */
test("an assembled value larger than the limit ⇒ error before downloading", async () => {
  const ref = { sha256: new Uint8Array(32).fill(3), len: 16 << 20, gen: 9n };
  const url = "https://store.example/values/huge";
  const stub = stubFetch(new Map([[url, 404]]));
  const value = { content: new Uint8Array(0), segments: [ref, ref, ref, ref, ref], len: 5 * ref.len };
  await assert.rejects(
    assemble(value, new Map([[urlKey(ref.sha256, ref.gen), url]]), new SegmentCache(), stub),
    SegmentedValueError,
  );
  assert.deepEqual(stub.calls, [], "a value over the limit was still downloaded");
});

/**
 * A limit on total bytes per turn (`Segments`, one RR turn or one BIDI stream; counted in assembled
 * bytes, see the next test). Must see: a 160-byte limit, two values of 152 bytes each with one
 * 100-byte segment ⇒ the first value assembles, the second fails **before** downloading.
 */
test("total bytes downloaded per turn are bounded", async () => {
  const a = await segmented("a".repeat(150), [100]);
  const b = await segmented("b".repeat(150), [100]);
  const stub = stubFetch(routesOf([...a.segs, ...b.segs]));
  const segs = new Segments({ cache: new SegmentCache(0), fetch: stub, maxTurnValueBytes: 160 } as ValueOptions);
  segs.addUrls([...a.segs, ...b.segs].map((s) => s.url));
  const frame = (x: typeof a, id: number) =>
    new FrameSplitter().push(segmentedCompletion(id, x.tail, x.segs.map((s) => s.ref), x.bytes.length))[0]!;
  await segs.admit(frame(a, 1));
  await assert.rejects(segs.admit(frame(b, 2)), SegmentedValueError);
  assert.equal(stub.calls.length, 1, "downloaded despite exceeding the turn's byte limit");
});

/**
 * The per-turn byte limit used to subtract only segments that **missed** the cache — a segment
 * hitting the cache still allocated an assembly buffer of size `value.len` without subtracting
 * anything, so many entries carrying the same ref multiplied memory without bound.
 *
 * Must see: a 200-byte limit per turn, two 150-byte values with the same segment (the second
 * time hits the cache) ⇒ the second value fails before assembling.
 */
test("the per-turn byte limit counts segments that hit the cache too", async () => {
  const a = await segmented("c".repeat(150), [100]);
  const stub = stubFetch(routesOf(a.segs));
  const segs = new Segments({
    cache: new SegmentCache(1 << 20),
    fetch: stub,
    maxTurnValueBytes: 200,
  } as ValueOptions);
  segs.addUrls(a.segs.map((s) => s.url));
  const frame = (id: number) =>
    new FrameSplitter().push(segmentedCompletion(id, a.tail, a.segs.map((s) => s.ref), a.bytes.length))[0]!;
  await segs.admit(frame(1));
  await assert.rejects(segs.admit(frame(2)), SegmentedValueError, "a cache hit without subtracting from the turn's limit");
});

// ─────────────────────────────────────────── an endpoint without identityKeys

const plain = service("P", { async ok() { return 1; } });

async function declaredWire(h: (r: Request) => Promise<Response>, headers: Record<string, string> = {}): Promise<number> {
  const r = await h(new Request("http://sdk.local/discover", { headers }));
  assert.equal(r.status, 200);
  return ((await r.json()) as { maxProtocolVersion: number }).maxProtocolVersion;
}

/**
 * Wire v6 makes the endpoint fetch the URLs in `Start.segment_urls`. Without
 * `identityKeys` that body is written by whoever reaches the endpoint, and without
 * `segmentOrigins` any http(s) URL is fetched: a blind SSRF. Must see: an endpoint with
 * neither declares v5 (the runtime then sends values inline); pinning v6 there is a
 * configuration error; either option brings v6 back.
 */
test(
  "no identityKeys and no segmentOrigins ⇒ wire v6 is not served: the manifest declares v5, pinning v6 is an error",
  async () => {
    assert.equal(await declaredWire(fetchHandler({ services: [plain] })), 5);
    assert.equal(await declaredWire(fetchHandler({ services: [plain], maxProtocolVersion: 4 })), 4);
    assert.throws(
      () => fetchHandler({ services: [plain], maxProtocolVersion: WIRE_SEGMENTED }),
      /needs identityKeys or segmentOrigins/,
    );
    assert.equal(
      await declaredWire(fetchHandler({ services: [plain], segmentOrigins: ["https://values.cairn.example"] })),
      WIRE_SEGMENTED,
    );
    const { signer } = await import("./signer.ts");
    const rt = await signer();
    assert.equal(
      await declaredWire(fetchHandler({ services: [plain], identityKeys: [rt.kid] }), await rt.headers("/discover")),
      WIRE_SEGMENTED,
    );
    const ep = await serve({ services: [plain], port: 0, protocolMode: "REQUEST_RESPONSE" });
    try {
      const m = (await (await fetch(`${ep.url}/discover`)).json()) as { maxProtocolVersion: number };
      assert.equal(m.maxProtocolVersion, 5, "serve() without identityKeys or segmentOrigins");
    } finally {
      await ep.close();
    }
  },
);

/**
 * What a failed segment read returns to the runtime is the turn's error text, which the
 * caller of an unverified endpoint reads back. Distinct texts for a status, a body
 * length or a dial error turn the endpoint into a port and size scanner of its own network.
 * Must see: one text for every outcome after the request went out.
 */
test(
  "a failed segment read reaches the runtime as one generic text: no HTTP status, byte count or network error",
  async () => {
    const { bytes, segs, tail } = await segmented("q".repeat(300), [100]);
    const value = { content: tail, segments: segs.map((s) => s.ref), len: bytes.length };
    const url = segs[0]!.url.url;
    const refused: FetchLike = async () => {
      throw Object.assign(new TypeError("fetch failed"), { cause: { code: "ECONNREFUSED" } });
    };
    const cases: [string, FetchLike][] = [
      ["404", stubFetch(new Map([[url, 404]]))],
      ["500", stubFetch(new Map([[url, 500]]))],
      ["short body", stubFetch(new Map([[url, new Uint8Array(10)]]))],
      ["wrong bytes", stubFetch(new Map([[url, new Uint8Array(100)]]))],
      ["connection refused", refused],
    ];
    const texts = new Set<string>();
    for (const [what, f] of cases) {
      const e: unknown = await assemble(value, urlMap(segs), new SegmentCache(), f).then(
        () => undefined,
        (x: unknown) => x,
      );
      assert.ok(e instanceof SegmentedValueError, what);
      assert.doesNotMatch(e.message, /HTTP|\b(404|500|10|100)\b|ECONNREFUSED|TypeError|bytes/, what);
      texts.add(e.message);
    }
    assert.equal(texts.size, 1, `distinct texts tell the caller what is behind the URL: ${[...texts].join(" | ")}`);
  },
);

/** `n` one-byte refs with distinct shas, each with its own URL. */
function tinyRefs(n: number, salt: number): { value: { content: Uint8Array; segments: SegmentRef[]; len: number }; urls: Map<string, string> } {
  const segments: SegmentRef[] = [];
  const urls = new Map<string, string>();
  for (let i = 0; i < n; i++) {
    const sha = new Uint8Array(32);
    new DataView(sha.buffer).setUint32(0, i);
    new DataView(sha.buffer).setUint32(4, salt);
    const ref = { sha256: sha, len: 1, gen: BigInt(i) };
    segments.push(ref);
    urls.set(urlKey(ref.sha256, ref.gen), `https://store.example/${salt}/${i}`);
  }
  return { value: { content: new Uint8Array(), segments, len: n }, urls };
}

/**
 * The number of refs in a value is the sender's choice: thousands of one-byte refs cost
 * nothing to send and one outbound request each. The runtime seals segments of at least
 * 64 KiB, so a value of `maxValueBytes` holds at most `maxValueBytes / 64 KiB` refs.
 * Must see: a value over that is refused before any fetch.
 */
test(
  "refs per value are capped before any fetch",
  async () => {
    const n = (64 << 20) / (64 << 10) + 1;
    const { value, urls } = tinyRefs(n, 1);
    const stub = stubFetch(new Map());
    await assert.rejects(assemble(value, urls, new SegmentCache(), stub), SegmentedValueError);
    assert.equal(stub.calls.length, 0, "refs were fetched before the count was checked");
  },
);

/**
 * Each assembly fetches with 8 workers, but nothing bounded the turns running at once:
 * 8 turns of 8 refs each meant 64 requests in flight from one process.
 * Must see: segment fetches in flight across the process stay at the process cap.
 */
test(
  "segment fetches in flight across the whole process are capped",
  async () => {
    const values = await import("../src/values.ts");
    const cap = (values as { MAX_PROCESS_FETCHES?: number }).MAX_PROCESS_FETCHES ?? 32;
    let inFlight = 0;
    let max = 0;
    const waiting: (() => void)[] = [];
    const slow: FetchLike = async () => {
      inFlight++;
      max = Math.max(max, inFlight);
      await new Promise<void>((r) => waiting.push(r));
      inFlight--;
      return { ok: false, status: 404, arrayBuffer: async () => new ArrayBuffer(0), body: null };
    };
    let settled = false;
    const all = Promise.all(
      Array.from({ length: 8 }, (_, k) => {
        const { value, urls } = tinyRefs(8, 100 + k);
        return assemble(value, urls, new SegmentCache(), slow).catch(() => {});
      }),
    ).then(() => {
      settled = true;
    });
    for (let i = 0; i < 1000 && !settled; i++) {
      await new Promise((r) => setTimeout(r, 5));
      for (const r of waiting.splice(0)) r();
    }
    await all;
    assert.ok(max <= cap, `${max} segment fetches in flight at once, the process cap is ${cap}`);
  },
);

/**
 * The v6 gate used to change only the manifest: an endpoint capped at v5 (no identityKeys,
 * no segmentOrigins) still assembled every segmented ref, at whatever wire the request said.
 *
 * 1. The endpoint declares v5; a caller sends a turn labelled v5 whose `Start.segment_urls`
 *    points at a host of its choosing, and a `GetState` completion carrying a ref.
 * 2. The SDK fetched the URL from its own network position.
 *
 * Must see: no fetch at all; refs below wire v6 are a protocol violation, refused without
 * reading them.
 */
test(
  "an endpoint capped at v5 fetches nothing for a turn that carries segment refs anyway",
  async () => {
    const { bytes, segs, tail } = await segmented("r".repeat(1500), [1000]);
    const body = cat(
      startFrame(3, segs.map((s) => ({ ...s.url, url: "http://169.254.169.254/latest/meta-data/" }))),
      inputFrame(null),
      getStateFrame("h", 1),
      segmentedCompletion(1, tail, segs.map((s) => s.ref), bytes.length),
    );
    const svc = service("V", { run: appendZ }, { type: "VIRTUAL_OBJECT" });
    const h = fetchHandler({ services: [svc], compression: false });
    const real = globalThis.fetch;
    const f = stubFetch(new Map());
    globalThis.fetch = f as unknown as typeof fetch;
    try {
      for (const wire of [5, 1]) {
        await h(
          new Request("http://sdk.local/invoke/V/run", {
            method: "POST",
            headers: { "content-type": `application/vnd.cairnflow.invocation.v${wire}` },
            body: body as Uint8Array<ArrayBuffer>,
          }),
        );
      }
    } finally {
      globalThis.fetch = real;
    }
    assert.deepEqual(f.calls, [], "the endpoint fetched a URL from the request");
  },
);
