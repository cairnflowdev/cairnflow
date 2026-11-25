// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Framing and messages — sections 4, 5 and 7 of `docs/protocol-v1.md`.
 *
 * This is the TypeScript twin of `crates/cairn-protocol`. The two sides must match
 * bit for bit, so every constant here is copied straight from the specification,
 * not derived from the other side.
 */

import { eachField, Reader, WIRE_FIXED64, WIRE_LEN, WIRE_VARINT, Writer } from "./protobuf.ts";

// ─────────────────────────────────────────────────────────── message type

export const MT = {
  START: 0x0000,
  SUSPENSION: 0x0001,
  ERROR: 0x0002,
  END: 0x0003,
  COMMAND_ACK: 0x0004,
  PROPOSE_RUN_COMPLETION: 0x0005,
  PROPOSE_RUN_COMPLETION_ACK: 0x0007,
  /**
   * Wire v3: SDK → runtime, empty payload. "I am still alive, the `ctx.run`
   * closure is still running." Not journaled; the runtime counts its silence cap
   * from the last frame, so one such frame is enough to keep the turn from being cancelled.
   */
  KEEPALIVE: 0x0008,
  /**
   * Wire v3: SDK → runtime, ends a `replay_only` turn. "This code matches the
   * journal up to command N" (it stops *before* the first command outside the
   * journal, so no closure runs), or where it diverges.
   */
  REPLAY_REPORT: 0x0009,
  /** Wire v3: SDK → runtime, `ctx.continueAsNew(input)` — ends the turn. */
  CONTINUE_AS_NEW: 0x000a,
  /**
   * Wire v4: SDK → runtime, **side channel** — a record of a stream
   * (`ctx.stream`). Not journaled, no wait for a durable ack.
   */
  STREAM_APPEND: 0x000b,
  /** Wire v4: SDK → runtime, side channel — an artifact. */
  ARTIFACT_PUT: 0x000c,
  /**
   * Wire v6: runtime → SDK, **side channel** — read URLs of the segment refs in
   * the messages that follow it on the stream (BIDI mid-turn). Not journaled,
   * not acked.
   */
  SEGMENT_URLS: 0x000d,

  INPUT: 0x0400,
  OUTPUT: 0x0401,
  GET_STATE: 0x0402,
  SET_STATE: 0x0403,
  CLEAR_STATE: 0x0404,
  CLEAR_ALL_STATE: 0x0405,
  GET_STATE_KEYS: 0x0406,
  GET_EAGER_STATE: 0x0407,
  GET_EAGER_STATE_KEYS: 0x0408,
  GET_PROMISE: 0x0409,
  PEEK_PROMISE: 0x040a,
  COMPLETE_PROMISE: 0x040b,
  SLEEP: 0x040c,
  CALL: 0x040d,
  ONE_WAY_CALL: 0x040e,
  SEND_SIGNAL: 0x0410,
  RUN: 0x0411,
  ATTACH_INVOCATION: 0x0412,
  GET_INVOCATION_OUTPUT: 0x0413,

  GET_STATE_COMPLETION: 0x8002,
  GET_STATE_KEYS_COMPLETION: 0x8006,
  GET_PROMISE_COMPLETION: 0x8009,
  PEEK_PROMISE_COMPLETION: 0x800a,
  COMPLETE_PROMISE_COMPLETION: 0x800b,
  SLEEP_COMPLETION: 0x800c,
  CALL_COMPLETION: 0x800d,
  CALL_INVOCATION_ID_COMPLETION: 0x800e,
  RUN_COMPLETION: 0x8011,
  ATTACH_INVOCATION_COMPLETION: 0x8012,
  GET_INVOCATION_OUTPUT_COMPLETION: 0x8013,
  SIGNAL: 0xfbff,
} as const;

export const JOURNAL_MISMATCH = 570;
export const PROTOCOL_VIOLATION = 571;

/** protocol-v1 §4: bit 47 of the 64-bit header — the high bit of the 16-bit flags field. */
export const FLAG_REQUESTED_ACK = 0x8000;

export const HEADER_LEN = 8;

/** protocol-v1 §4: payload length cap. Over ⇒ PROTOCOL_VIOLATION. */
export const MAX_FRAME_LEN = 16 * 1024 * 1024;

/** A notification is every code with bit 0x8000, except the custom range `0xFC00+`. */
export function isNotification(type: number): boolean {
  return type >= 0x8000 && type <= 0xfbff;
}

export function isCommand(type: number): boolean {
  return type >= 0x0400 && type <= 0x04ff;
}

export function typeName(type: number): string {
  for (const [k, v] of Object.entries(MT)) if (v === type) return k;
  return `0x${type.toString(16).padStart(4, "0")}`;
}

// ─────────────────────────────────────────────────────────── khung tin

export interface Frame {
  readonly type: number;
  readonly flags: number;
  readonly payload: Uint8Array;
}

export function encodeFrame(type: number, flags: number, payload: Uint8Array): Uint8Array {
  const out = new Uint8Array(HEADER_LEN + payload.length);
  const view = new DataView(out.buffer);
  view.setUint16(0, type, false);
  view.setUint16(2, flags, false);
  view.setUint32(4, payload.length, false);
  out.set(payload, HEADER_LEN);
  return out;
}

export function concatFrames(frames: Uint8Array[]): Uint8Array {
  const total = frames.reduce((n, f) => n + f.length, 0);
  const out = new Uint8Array(total);
  let at = 0;
  for (const f of frames) {
    out.set(f, at);
    at += f.length;
  }
  return out;
}

export class FrameError extends Error {}

/**
 * Splits a whole byte stream into frames.
 *
 * Trailing extra bytes are an **error**, not "wait for more": in `REQUEST_RESPONSE`
 * the request body has ended, so there is nothing left to wait for.
 */
export function decodeFrames(buf: Uint8Array): Frame[] {
  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  const out: Frame[] = [];
  let at = 0;
  while (at + HEADER_LEN <= buf.length) {
    const type = view.getUint16(at, false);
    const flags = view.getUint16(at + 2, false);
    const len = view.getUint32(at + 4, false);
    // Bits 46..32 MUST be 0.
    if ((flags & ~FLAG_REQUESTED_ACK) !== 0) {
      throw new FrameError(`reserved flag bits must be 0, got 0x${flags.toString(16)}`);
    }
    if (len > MAX_FRAME_LEN) {
      throw new FrameError(`payload of ${len} bytes exceeds the limit of ${MAX_FRAME_LEN}`);
    }
    if (at + HEADER_LEN + len > buf.length) break;
    out.push({ type, flags, payload: buf.slice(at + HEADER_LEN, at + HEADER_LEN + len) });
    at += HEADER_LEN + len;
  }
  if (at !== buf.length) {
    throw new FrameError(`truncated byte stream: ${buf.length - at} trailing bytes do not form a frame`);
  }
  return out;
}

/**
 * Splits frames out of a byte stream arriving **in chunks** — the twin of the
 * Rust `FrameDecoder`.
 *
 * Differs from [`decodeFrames`] in exactly one point, and that point is the whole
 * reason it exists: extra bytes at the end of the buffer mean "wait for more", not
 * an error. On `BIDI_STREAM` the request body has not ended, so half a header left
 * over is normal HTTP/2 rather than a broken stream.
 */
export class FrameSplitter {
  private buf: Uint8Array<ArrayBufferLike> = new Uint8Array(0);

  /** Feeds one chunk, returns the frames that just became complete. */
  push(chunk: Uint8Array): Frame[] {
    if (this.buf.length === 0) {
      this.buf = chunk;
    } else {
      const merged = new Uint8Array(this.buf.length + chunk.length);
      merged.set(this.buf);
      merged.set(chunk, this.buf.length);
      this.buf = merged;
    }

    const out: Frame[] = [];
    let at = 0;
    const view = new DataView(this.buf.buffer, this.buf.byteOffset, this.buf.byteLength);
    while (at + HEADER_LEN <= this.buf.length) {
      const type = view.getUint16(at, false);
      const flags = view.getUint16(at + 2, false);
      const len = view.getUint32(at + 4, false);
      // Check the header BEFORE waiting for the full payload: wait first for a lying
      // `length` and check afterwards, and we would already be holding a huge buffer.
      if ((flags & ~FLAG_REQUESTED_ACK) !== 0) {
        throw new FrameError(`reserved flag bits must be 0, got 0x${flags.toString(16)}`);
      }
      if (len > MAX_FRAME_LEN) {
        throw new FrameError(`payload of ${len} bytes exceeds the limit of ${MAX_FRAME_LEN}`);
      }
      if (at + HEADER_LEN + len > this.buf.length) break;
      out.push({ type, flags, payload: this.buf.slice(at + HEADER_LEN, at + HEADER_LEN + len) });
      at += HEADER_LEN + len;
    }
    this.buf = at === 0 ? this.buf : this.buf.slice(at);
    return out;
  }

  /** Number of incomplete bytes left over. Non-zero when the stream closes = stream cut midway. */
  get pending(): number {
    return this.buf.length;
  }
}

// ─────────────────────────────────────────────────────────── decoding

export interface StateEntry {
  readonly key: Uint8Array;
  readonly value: Uint8Array;
}

/**
 * A segment of a large value — `SegmentRef { bytes sha256 = 1; uint64 len =
 * 2; fixed64 gen = 3; }`. `gen` is newly issued every time the runtime seals.
 */
export interface SegmentRef {
  readonly sha256: Uint8Array;
  readonly len: number;
  readonly gen: bigint;
}

/** `SegmentUrl { bytes sha256 = 1; fixed64 gen = 2; string url = 3; }` (wire v6). */
export interface SegmentUrl {
  readonly sha256: Uint8Array;
  readonly gen: bigint;
  readonly url: string;
}

/**
 * `Value { bytes content = 1; repeated SegmentRef segments = 2; uint64 len = 3; }`.
 *
 * `content` is the **tail**: the logical value = the bytes of each segment in order ‖
 * `content`. No segments ⇒ inline, `content` is the whole value. Never read
 * `content` as the whole value when `segments` is not empty.
 */
export interface WireValue {
  readonly content: Uint8Array;
  readonly segments: readonly SegmentRef[];
  /** Logical length (checked `= Σ segments.len + content.length`). */
  readonly len: number;
}

export interface StartInfo {
  readonly id: Uint8Array;
  readonly debugId: string;
  readonly knownEntries: number;
  readonly key: string;
  readonly randomSeed: bigint;
  /** Eager state: the object's state, sent along to answer `ctx.get` in place. */
  readonly stateMap: StateEntry[];
  /** `true` = `stateMap` is partial only; absent keys must be read lazily. */
  readonly partialState: boolean;
  /**
   * How many times this turn has been retried **since the journal last grew**.
   *
   * `0` on the first run, and reset to 0 each time the handler makes one more
   * step of progress. So it answers "is this spot stuck", not "how many times has
   * this invocation failed" — a ten-step workflow where every step fails
   * transiently once still sees 0, 1, 0, 1, …
   *
   * Old runtimes do not send this field ⇒ always `0`, and every `>= n` comparison
   * errs on the safe side (never gives up).
   */
  readonly retryCount: number;
  /**
   * Wire v3: the interval for sending `KEEPALIVE` while a `ctx.run` closure is still
   * running, ms. `0` = the runtime set no cap (or speaks v2) — nothing is sent.
   */
  readonly keepaliveMs: number;
  /**
   * Wire v3: a **trial replay** turn — when the SDK meets a command outside the
   * journal it stops *before* running the closure and reports "matches up to entry N",
   * without running side effects.
   */
  readonly replayOnly: boolean;
  /**
   * Wire v5 (protocol-v1 §9.3): the request body **omits** the first `replayFrom`
   * entries — the SDK splices them back from the replay cache when `prefixDigest`
   * matches. `0` = full replay.
   */
  readonly replayFrom: number;
  /** `h_B` of the SHA-256 chain over entry frames `[0, replayFrom)`; empty when `0`. */
  readonly prefixDigest: Uint8Array;
  /**
   * Wire v6: read URLs of every segment ref in **all** messages of the opening
   * turn — including the prefix part that the RR delta cuts off.
   */
  readonly segmentUrls: SegmentUrl[];
}

function decodeStateEntry(payload: Uint8Array): StateEntry {
  let key: Uint8Array = new Uint8Array();
  let value: Uint8Array = new Uint8Array();
  eachField(payload, (field, _wire, r) => {
    if (field === 1) {
      key = r.bytes();
      return true;
    }
    if (field === 2) {
      value = r.bytes();
      return true;
    }
    return false;
  });
  return { key, value };
}

/** Wire types of the known fields of `StartMessage` (0 varint, 2 length-delimited). */
const START_WIRE: Readonly<Record<number, number>> = {
  1: 2, 2: 2, 3: 0, 4: 2, 5: 0, 6: 2, 7: 0, 9: 0, 16: 0, 17: 0, 18: 0, 19: 2, 20: 2,
};

export function decodeStart(payload: Uint8Array): StartInfo {
  let id: Uint8Array = new Uint8Array();
  let debugId = "";
  let knownEntries = 0;
  let key = "";
  let randomSeed = 0n;
  const stateMap: StateEntry[] = [];
  let partialState = false;
  let retryCount = 0;
  let keepaliveMs = 0;
  let replayOnly = false;
  let replayFrom = 0;
  let prefixDigest: Uint8Array = new Uint8Array();
  const segmentUrls: SegmentUrl[] = [];
  eachField(payload, (field, wire, r) => {
    // A known field with the wrong wire type ⇒ a decode error as in prost, rather
    // than misreading bytes.
    const want = START_WIRE[field];
    if (want !== undefined && want !== wire) throw new RangeError(`Start field ${field}: wire type ${wire}, expected ${want}`);
    switch (field) {
      case 1:
        id = r.bytes();
        return true;
      case 2:
        debugId = r.string();
        return true;
      case 3:
        knownEntries = Number(r.varint());
        return true;
      case 4:
        stateMap.push(decodeStateEntry(r.bytes()));
        return true;
      case 5:
        partialState = r.varint() !== 0n;
        return true;
      case 6:
        key = r.string();
        return true;
      case 7:
        retryCount = Number(r.varint());
        return true;
      case 9:
        randomSeed = r.varint();
        return true;
      case 16:
        keepaliveMs = Number(r.varint());
        return true;
      case 17:
        replayOnly = r.varint() !== 0n;
        return true;
      case 18:
        replayFrom = Number(r.varint());
        return true;
      case 19:
        prefixDigest = r.bytes();
        return true;
      case 20:
        segmentUrls.push(decodeSegmentUrl(r.bytes()));
        return true;
      default:
        return false; // a v2 field — skip, see `Reader.skip`
    }
  });
  return {
    id,
    debugId,
    knownEntries,
    key,
    randomSeed,
    stateMap,
    partialState,
    retryCount,
    keepaliveMs,
    replayOnly,
    replayFrom,
    prefixDigest,
    segmentUrls,
  };
}

/** Wrong `Value` layout (length, sha) — a broken entry, no guessing. */
export class ValueLayoutError extends Error {}

/** Length of a ref's sha256. */
const SHA_LEN = 32;

function decodeSegmentRef(payload: Uint8Array): SegmentRef {
  let sha256: Uint8Array = new Uint8Array();
  let len = 0n;
  let gen = 0n;
  eachField(payload, (field, wire, r) => {
    if (field === 1 && wire === WIRE_LEN) {
      sha256 = r.bytes();
      return true;
    }
    if (field === 2 && wire === WIRE_VARINT) {
      len = r.varint();
      return true;
    }
    if (field === 3 && wire === WIRE_FIXED64) {
      gen = r.fixed64();
      return true;
    }
    return false;
  });
  if (len > BigInt(Number.MAX_SAFE_INTEGER)) throw new ValueLayoutError(`segment length ${len} is too large`);
  return { sha256, len: Number(len), gen };
}

/** `SegmentUrl { bytes sha256 = 1; fixed64 gen = 2; string url = 3; }` */
export function decodeSegmentUrl(payload: Uint8Array): SegmentUrl {
  let sha256: Uint8Array = new Uint8Array();
  let gen = 0n;
  let url = "";
  eachField(payload, (field, wire, r) => {
    if (field === 1 && wire === WIRE_LEN) {
      sha256 = r.bytes();
      return true;
    }
    if (field === 2 && wire === WIRE_FIXED64) {
      gen = r.fixed64();
      return true;
    }
    if (field === 3 && wire === WIRE_LEN) {
      url = r.string();
      return true;
    }
    return false;
  });
  return { sha256, gen, url };
}

/** `SegmentUrlsMessage { repeated SegmentUrl urls = 1; }` (0x000D, wire v6). */
export function decodeSegmentUrls(payload: Uint8Array): SegmentUrl[] {
  const out: SegmentUrl[] = [];
  eachField(payload, (field, wire, r) => {
    if (field === 1 && wire === WIRE_LEN) {
      out.push(decodeSegmentUrl(r.bytes()));
      return true;
    }
    return false;
  });
  return out;
}

function writeSegmentUrl(u: SegmentUrl): Writer {
  return new Writer().bytes(1, u.sha256).fixed64(2, u.gen).string(3, u.url);
}

/** Inverse of [`decodeSegmentUrls`] — for tests and fake runtimes. */
export function encodeSegmentUrls(urls: readonly SegmentUrl[]): Uint8Array {
  const w = new Writer();
  for (const u of urls) w.message(1, writeSegmentUrl(u));
  return w.finish();
}

/**
 * A `Value` with its full layout, checked like the Rust `Logical::of`: with segments ⇒
 * `len = Σ segments.len + content.length` and a full 32-byte sha; inline ⇒ `len`
 * is 0 (the old layout) or exactly `content.length`. Mismatch ⇒ [`ValueLayoutError`].
 */
export function decodeWireValue(payload: Uint8Array): WireValue {
  let content: Uint8Array = new Uint8Array();
  const segments: SegmentRef[] = [];
  let len = 0n;
  eachField(payload, (field, wire, r) => {
    if (field === 1 && wire === WIRE_LEN) {
      content = r.bytes();
      return true;
    }
    if (field === 2 && wire === WIRE_LEN) {
      segments.push(decodeSegmentRef(r.bytes()));
      return true;
    }
    if (field === 3 && wire === WIRE_VARINT) {
      len = r.varint();
      return true;
    }
    return false;
  });
  if (segments.length === 0) {
    if (len !== 0n && len !== BigInt(content.length)) {
      throw new ValueLayoutError(`inline value declares length ${len} but carries ${content.length} bytes`);
    }
    return { content, segments, len: content.length };
  }
  let sum = BigInt(content.length);
  segments.forEach((s, i) => {
    if (s.sha256.length !== SHA_LEN) {
      throw new ValueLayoutError(`segment ${i} has a ${s.sha256.length}-byte sha256, expected ${SHA_LEN}`);
    }
    sum += BigInt(s.len);
  });
  if (sum !== len) throw new ValueLayoutError(`segmented value declares length ${len} but its parts sum to ${sum}`);
  if (len > BigInt(Number.MAX_SAFE_INTEGER)) throw new ValueLayoutError(`value length ${len} is too large`);
  return { content, segments, len: Number(len) };
}

/**
 * A `Value` whose read site **only accepts inline** (input, signal, call result…):
 * the runtime only sends refs for state values, so a segment here is an explicit
 * error instead of reading the tail as the whole value.
 */
function decodeValue(payload: Uint8Array): Uint8Array {
  const v = decodeWireValue(payload);
  if (v.segments.length > 0) throw new ValueLayoutError("segmented value where only an inline value is allowed");
  return v.content;
}

/**
 * A `Value` going onto the wire: inline when `refs` is empty (bytes identical to the
 * old layout, conformance traces unchanged), otherwise `segments = refs`, `content` = tail,
 * `len` = logical length (wire v6).
 */
export function writeValue(tail: Uint8Array, refs: readonly SegmentRef[] = [], len = 0): Writer {
  const w = new Writer().bytes(1, tail);
  if (refs.length === 0) return w;
  for (const r of refs) {
    w.message(2, new Writer().bytes(1, r.sha256).uint64(2, BigInt(r.len)).fixed64(3, r.gen));
  }
  return w.uint64(3, BigInt(len));
}

/** Headers of entry 0 (`Input`, field 1: `Header{key=1, value=2}`). */
export function decodeInputHeaders(payload: Uint8Array): Map<string, string> {
  const out = new Map<string, string>();
  eachField(payload, (field, _wire, r) => {
    if (field !== 1) return false;
    let key = "";
    let value = "";
    eachField(r.bytes(), (g, _w, s) => {
      if (g === 1) key = s.string();
      else if (g === 2) value = s.string();
      else return false;
      return true;
    });
    out.set(key, value);
    return true;
  });
  return out;
}

export function decodeInput(payload: Uint8Array): Uint8Array {
  let value: Uint8Array = new Uint8Array();
  eachField(payload, (field, _wire, r) => {
    if (field === 14) {
      value = decodeValue(r.bytes());
      return true;
    }
    return false;
  });
  return value;
}

/**
 * The result of a completion.
 *
 * `void` (the key does not exist) and an empty `value` (exists, empty value) are
 * **two different things** — merging them is the classic bug of a state layer.
 */
export type CompletionResult =
  | { readonly kind: "void" }
  /** `refs` is present when `value` is the assembled copy of a segmented value (wire v6). */
  | { readonly kind: "value"; readonly value: Uint8Array; readonly refs?: readonly SegmentRef[] }
  /**
   * Wire v6: a segmented value **not yet assembled** — only `decodeNotification` returns
   * it; `Turn` replaces it with the assembled `value` before user code sees it.
   */
  | { readonly kind: "segmented"; readonly value: WireValue }
  | { readonly kind: "failure"; readonly code: number; readonly message: string }
  | { readonly kind: "stateKeys"; readonly keys: Uint8Array[] };

export interface Notification {
  readonly completionId: number;
  readonly result: CompletionResult;
}

function decodeFailure(payload: Uint8Array): { code: number; message: string } {
  let code = 0;
  let message = "";
  eachField(payload, (field, _wire, r) => {
    if (field === 1) {
      code = Number(r.varint());
      return true;
    }
    if (field === 2) {
      message = r.string();
      return true;
    }
    return false;
  });
  return { code, message };
}

/**
 * Every notification shares the same field layout (`NotificationTemplate` in
 * protocol-v1 §7), so one reader is enough — exactly the intent of that duck type.
 */
export function decodeNotification(payload: Uint8Array): Notification {
  let completionId = 0;
  let result: CompletionResult = { kind: "void" };
  eachField(payload, (field, _wire, r) => {
    switch (field) {
      case 1:
        completionId = Number(r.varint());
        return true;
      case 4:
        r.bytes();
        result = { kind: "void" };
        return true;
      case 5: {
        const v = decodeWireValue(r.bytes());
        result = v.segments.length === 0 ? { kind: "value", value: v.content } : { kind: "segmented", value: v };
        return true;
      }
      case 6: {
        const f = decodeFailure(r.bytes());
        result = { kind: "failure", ...f };
        return true;
      }
      case 17:
        result = { kind: "stateKeys", keys: decodeStateKeys(r.bytes()) };
        return true;
      default:
        return false;
    }
  });
  return { completionId, result };
}

/** `StateKeys { repeated bytes keys = 1 }` */
export function decodeStateKeys(payload: Uint8Array): Uint8Array[] {
  const keys: Uint8Array[] = [];
  eachField(payload, (field, _wire, r) => {
    if (field === 1) {
      keys.push(r.bytes());
      return true;
    }
    return false;
  });
  return keys;
}

// ─────────────────────────────────────────────────────────── encoding

export function encodeGetState(key: string, completionId: number, name: string): Uint8Array {
  return new Writer()
    .bytes(1, new TextEncoder().encode(key))
    .uint32(11, completionId)
    .string(12, name)
    .finish();
}

/**
 * `SetState`. `reuse` (wire v6) = refs of a byte prefix the SDK already read for this
 * same key: sends `Value{segments: reuse, content: the rest, len}` instead of the
 * whole `value`. The caller guarantees `value` starts with the bytes of `reuse`.
 */
export function encodeSetState(
  key: string,
  value: Uint8Array,
  name: string,
  reuse: readonly SegmentRef[] = [],
): Uint8Array {
  const off = reuse.reduce((n, r) => n + r.len, 0);
  return new Writer()
    .bytes(1, new TextEncoder().encode(key))
    .message(3, writeValue(reuse.length === 0 ? value : value.subarray(off), reuse, value.length))
    .string(12, name)
    .finish();
}

export function encodeClearState(key: string, name: string): Uint8Array {
  return new Writer().bytes(1, new TextEncoder().encode(key)).string(12, name).finish();
}

export function encodeClearAllState(name: string): Uint8Array {
  return new Writer().string(12, name).finish();
}

export function encodeGetStateKeys(completionId: number, name: string): Uint8Array {
  return new Writer().uint32(11, completionId).string(12, name).finish();
}

/**
 * `GetEagerState` — the result is embedded in the command itself: void (13) if the
 * key did not exist at read time, value (14) if it did. Replay reads it back from this entry.
 */
export function encodeGetEagerState(
  key: string,
  value: Uint8Array | null,
  name: string,
): Uint8Array {
  const w = new Writer().bytes(1, new TextEncoder().encode(key)).string(12, name);
  if (value === null) {
    w.message(13, new Writer());
  } else {
    w.message(14, new Writer().bytes(1, value));
  }
  return w.finish();
}

/** The result embedded in a journaled `GetEagerState` entry — inline only. */
export function decodeGetEagerState(payload: Uint8Array): Uint8Array | null {
  const v = decodeGetEagerStateValue(payload);
  if (v === null) return null;
  if (v.segments.length > 0) throw new ValueLayoutError("GetEagerState carries a segmented value that was not assembled");
  return v.content;
}

/** As [`decodeGetEagerState`], with the full `Value` layout (wire v6: may be segmented). */
export function decodeGetEagerStateValue(payload: Uint8Array): WireValue | null {
  let value: WireValue | null = null;
  eachField(payload, (field, _wire, r) => {
    if (field === 13) {
      r.bytes();
      value = null;
      return true;
    }
    if (field === 14) {
      value = decodeWireValue(r.bytes());
      return true;
    }
    return false;
  });
  return value;
}

export function encodeGetEagerStateKeys(keys: Uint8Array[], name: string): Uint8Array {
  const sk = new Writer();
  for (const k of keys) sk.bytesAlways(1, k);
  return new Writer().string(12, name).message(14, sk).finish();
}

export function decodeGetEagerStateKeys(payload: Uint8Array): Uint8Array[] {
  let keys: Uint8Array[] = [];
  eachField(payload, (field, _wire, r) => {
    if (field === 14) {
      keys = decodeStateKeys(r.bytes());
      return true;
    }
    return false;
  });
  return keys;
}

// ─────────────────────────────────────────────────────────── promise

export function encodeGetPromise(key: string, completionId: number, name: string): Uint8Array {
  return new Writer().string(1, key).uint32(11, completionId).string(12, name).finish();
}

export function encodePeekPromise(key: string, completionId: number, name: string): Uint8Array {
  return new Writer().string(1, key).uint32(11, completionId).string(12, name).finish();
}

export function encodeCompletePromise(
  key: string,
  completion: { value: Uint8Array } | { code: number; message: string },
  completionId: number,
  name: string,
): Uint8Array {
  const w = new Writer().string(1, key);
  if ("value" in completion) {
    w.message(2, new Writer().bytes(1, completion.value));
  } else {
    w.message(3, new Writer().uint32(1, completion.code).string(2, completion.message));
  }
  return w.uint32(11, completionId).string(12, name).finish();
}

// ─────────────────────────────────────────────────── attach / get output

export function encodeAttachInvocation(
  invocationId: string,
  completionId: number,
  name: string,
): Uint8Array {
  return new Writer().string(1, invocationId).uint32(11, completionId).string(12, name).finish();
}

export function encodeGetInvocationOutput(
  invocationId: string,
  completionId: number,
  name: string,
): Uint8Array {
  return new Writer().string(1, invocationId).uint32(11, completionId).string(12, name).finish();
}

export function encodeSleep(wakeUpTime: bigint, completionId: number, name: string): Uint8Array {
  return new Writer()
    .uint64(1, wakeUpTime)
    .uint32(11, completionId)
    .string(12, name)
    .finish();
}

export interface CallFields {
  readonly service: string;
  readonly handler: string;
  readonly parameter: Uint8Array;
  readonly key: string;
  readonly idempotencyKey?: string | undefined;
  readonly idCompletion: number;
  readonly name: string;
  /**
   * Flow control (v2). Absent = **inherit the caller's scope** on the runtime
   * side, not "no scope": a workflow running in a tenant's scope makes everything
   * it calls belong to that tenant too. With a scope, the target's partition key
   * is `H(scope)`, beating the object key and the idempotency key — the scope
   * decides the range (`docs/key-design.md`).
   */
  readonly scope?: string | undefined;
  /** Only affects queueing — not part of the target invocation's identity. */
  readonly limitKey?: string | undefined;
  /**
   * `placement` (field 13). Only `"spread"` is written to the wire; absent =
   * `CALL_PLACEMENT_DEFAULT`, not written (proto3).
   */
  readonly placement?: CallPlacement | undefined;
  /** The callee's user tags (field 14) — see `CallOptions.tags`. */
  readonly tags?: Readonly<Record<string, string>> | undefined;
}

/** `repeated Header tags = 14`: one `Header {key = 1, value = 2}` per tag. */
function writeTags(w: Writer, f: CallFields): Writer {
  for (const [k, v] of Object.entries(f.tags ?? {})) {
    w.bytes(14, new Writer().string(1, k).string(2, v).finish());
  }
  return w;
}

/** Placement of an unkeyed callee — see `CallOptions.placement`. */
export type CallPlacement = "spread";

/** `CallPlacement.CALL_PLACEMENT_SPREAD` of `protocol.proto`. */
const CALL_PLACEMENT_SPREAD = 1;

function placementOf(f: CallFields): number {
  return f.placement === "spread" ? CALL_PLACEMENT_SPREAD : 0;
}

export function encodeCall(f: CallFields, resultCompletion: number): Uint8Array {
  const w = new Writer()
    .string(1, f.service)
    .string(2, f.handler)
    .bytes(3, f.parameter)
    .string(5, f.key);
  if (f.idempotencyKey !== undefined) w.string(6, f.idempotencyKey);
  if (f.scope !== undefined) w.string(7, f.scope);
  if (f.limitKey !== undefined) w.string(8, f.limitKey);
  w.uint32(10, f.idCompletion)
    .uint32(11, resultCompletion)
    .string(12, f.name)
    .uint32(13, placementOf(f));
  return writeTags(w, f).finish();
}

export function encodeOneWayCall(f: CallFields, invokeTime: bigint): Uint8Array {
  const w = new Writer()
    .string(1, f.service)
    .string(2, f.handler)
    .bytes(3, f.parameter)
    .uint64(4, invokeTime)
    .string(6, f.key);
  if (f.idempotencyKey !== undefined) w.string(7, f.idempotencyKey);
  if (f.scope !== undefined) w.string(8, f.scope);
  if (f.limitKey !== undefined) w.string(9, f.limitKey);
  w.uint32(10, f.idCompletion)
    .string(12, f.name)
    .uint32(13, placementOf(f));
  return writeTags(w, f).finish();
}

export function encodeRun(completionId: number, name: string): Uint8Array {
  return new Writer().uint32(11, completionId).string(12, name).finish();
}

/** `name` (field 12) of a journaled command — absent is the empty string. */
export function decodeCommandName(payload: Uint8Array): string {
  let name = "";
  eachField(payload, (field, wire, r) => {
    if (field !== 12 || wire !== 2) return false;
    name = r.string();
    return true;
  });
  return name;
}

/**
 * Command fields **not** compared on replay: non-deterministic values computed once
 * and then recorded (the wake-up time of `Sleep`, the send time of `OneWayCall`),
 * and values the runtime shell may have sealed into refs (`SetState.value`, the
 * embedded result of `GetEagerState*` — replay reads them from the journal, not
 * from code).
 */
const REPLAY_IGNORED: Readonly<Record<number, readonly number[]>> = {
  [MT.SLEEP]: [1],
  [MT.ONE_WAY_CALL]: [4],
  [MT.SET_STATE]: [3],
  [MT.GET_EAGER_STATE]: [13, 14],
  [MT.GET_EAGER_STATE_KEYS]: [14],
};

function fieldsOf(payload: Uint8Array, ignored: readonly number[]): Map<number, string[]> {
  const out = new Map<number, string[]>();
  eachField(payload, (field, wire, r) => {
    if (ignored.includes(field)) return false;
    let v: string;
    if (wire === 0) v = `v${r.varint()}`;
    else if (wire === 2) v = `b${Array.from(r.bytes(), (b) => b.toString(16).padStart(2, "0")).join("")}`;
    else return false;
    const list = out.get(field) ?? [];
    list.push(v);
    out.set(field, list);
    return true;
  });
  return out;
}

/**
 * The first field that differs between a journaled command and the command the
 * current code rebuilds (same kind), `null` when they match. Target, key,
 * parameters, completion id — every field except [`REPLAY_IGNORED`].
 */
export function commandDiff(type: number, journaled: Uint8Array, current: Uint8Array): number | null {
  const ignored = REPLAY_IGNORED[type] ?? [];
  const a = fieldsOf(journaled, ignored);
  const b = fieldsOf(current, ignored);
  for (const f of [...new Set([...a.keys(), ...b.keys()])].sort((x, y) => x - y)) {
    const x = a.get(f) ?? [];
    const y = b.get(f) ?? [];
    if (x.length !== y.length || x.some((v, i) => v !== y[i])) return f;
  }
  return null;
}

/**
 * `ProposeRunCompletion` (0x0005). Non-empty `reuse` (wire v6) ⇒ the result goes in
 * `segmented_value` (16): refs of the prefix + the rest; the runtime checks that the
 * refs are in this invocation's own journal.
 */
export function encodeProposeRunCompletion(
  completionId: number,
  value: Uint8Array,
  reuse: readonly SegmentRef[] = [],
): Uint8Array {
  if (reuse.length === 0) return new Writer().uint32(1, completionId).bytesAlways(14, value).finish();
  const off = reuse.reduce((n, r) => n + r.len, 0);
  return new Writer()
    .uint32(1, completionId)
    .message(16, writeValue(value.subarray(off), reuse, value.length))
    .finish();
}

export function encodeProposeRunFailure(
  completionId: number,
  code: number,
  message: string,
): Uint8Array {
  return new Writer()
    .uint32(1, completionId)
    .message(15, new Writer().uint32(1, code).string(2, message))
    .finish();
}

/**
 * `ProposeRunCompletionAckMessage` (0x0007) — the runtime reports the proposal is **durable**.
 *
 * It only carries `completion_id` in field 1, the same field layout as every
 * notification, so the shared reader reads it correctly — even though it is *not*
 * itself a notification and takes no journal slot (see `isJournalEntry`).
 *
 * The ack carries no value: the value is already in the SDK, it just computed it.
 * That is exactly the saving compared with the runtime echoing back the whole `RunCompletion`.
 */
export function decodeProposeRunAck(payload: Uint8Array): number {
  return decodeNotification(payload).completionId;
}

/**
 * `RunCompletionNotificationMessage` (0x8011) — the journal entry the runtime writes
 * when it receives `ProposeRunCompletion`.
 *
 * The SDK builds one in advance and **holds it back** until the ack arrives; only then
 * does it go into the local journal. Adding it earlier would claim something nobody
 * has guaranteed yet — see `Context.run`.
 */
export function encodeRunCompletion(completionId: number, value: Uint8Array): Uint8Array {
  return new Writer()
    .uint32(1, completionId)
    .message(5, new Writer().bytes(1, value))
    .finish();
}

export function encodeRunCompletionFailure(
  completionId: number,
  code: number,
  message: string,
): Uint8Array {
  return new Writer()
    .uint32(1, completionId)
    .message(6, new Writer().uint32(1, code).string(2, message))
    .finish();
}

/**
 * `Output` (0x0400). Non-empty `reuse` (wire v6) ⇒ `value` carries refs of the
 * prefix + the rest — `messages` returns the history it just read without uploading it again.
 */
export function encodeOutput(value: Uint8Array, name: string, reuse: readonly SegmentRef[] = []): Uint8Array {
  const off = reuse.reduce((n, r) => n + r.len, 0);
  return new Writer()
    .string(12, name)
    .message(14, writeValue(reuse.length === 0 ? value : value.subarray(off), reuse, value.length))
    .finish();
}

export function encodeOutputFailure(code: number, message: string, name: string): Uint8Array {
  return new Writer()
    .string(12, name)
    .message(15, new Writer().uint32(1, code).string(2, message))
    .finish();
}

export function encodeEnd(): Uint8Array {
  return new Uint8Array();
}

/** `CombinatorType` of SuspensionMessage (v2). 0/1 = FIRST, 2 = ALL. */
export const COMBINATOR_FIRST = 1;
export const COMBINATOR_ALL = 2;

/**
 * What the SDK awaits when it suspends. The three short forms (`first`, `all`,
 * `signal`) are a one-leaf tree or a single kind of leaf; `tree` is a one-level tree
 * mixing completions and signals. [`suspensionTree`] turns every form into a `tree`.
 */
export type SuspensionSpec =
  | { readonly first: number[] }
  | { readonly all: number[] }
  /** Awaits a signal (number = built-in / system-numbered, string = named). */
  | { readonly signal: number | string }
  | { readonly tree: SuspensionTree };

/**
 * Tree: completion leaves (field 1), numbered signals (field 2), named signals
 * (field 3), child nodes (field 4, `nested_futures`), combinator kind (field 5).
 * Child nodes only occur under a `first` root that merges several waits of one
 * turn: each multi-leaf `ALL` wait is one child node.
 */
export interface SuspensionTree {
  readonly combinator: "first" | "all";
  readonly completions: readonly number[];
  readonly signals: readonly number[];
  readonly names: readonly string[];
  readonly nested?: readonly SuspensionTree[];
}

/** Turns every form of [`SuspensionSpec`] into one tree. */
export function suspensionTree(spec: SuspensionSpec): SuspensionTree {
  if ("tree" in spec) return spec.tree;
  if ("first" in spec) return { combinator: "first", completions: spec.first, signals: [], names: [] };
  if ("all" in spec) return { combinator: "all", completions: spec.all, signals: [], names: [] };
  return typeof spec.signal === "number"
    ? { combinator: "first", completions: [], signals: [spec.signal], names: [] }
    : { combinator: "first", completions: [], signals: [], names: [spec.signal] };
}

export function encodeSuspension(waitingCompletions: number[]): Uint8Array {
  // proto3: `repeated uint32` defaults to **packed** — the form prost produces. The
  // runtime reads the one-tag-per-element form too, but conformance traces compare byte by byte.
  return new Writer().packedUint32(1, waitingCompletions).finish();
}

/**
 * A suspension from a spec. A `first` tree writes leaves directly at the root
 * (field 1/2/3, no field 5 — the default combinator is already FIRST_COMPLETED):
 * one completion, a list of completions, or a signal encode exactly as before mixed
 * trees existed (old conformance traces keep their bytes, CG-5). An `all` tree wraps
 * the leaves in ONE `ALL_COMPLETED` child node: the root (FIRST) is satisfied when
 * the child is, i.e. when ALL leaves are.
 */
export function encodeSuspensionSpec(spec: SuspensionSpec): Uint8Array {
  const t = suspensionTree(spec);
  const leaves = (w: Writer, n: SuspensionTree): Writer => {
    w.packedUint32(1, n.completions).packedUint32(2, n.signals);
    for (const x of n.names) w.string(3, x);
    return w;
  };
  const allNode = (n: SuspensionTree): Writer => leaves(new Writer(), n).uint32Always(5, COMBINATOR_ALL);
  if (t.combinator === "first") {
    const w = leaves(new Writer(), t);
    for (const n of t.nested ?? []) w.message(4, n.combinator === "all" ? allNode(n) : leaves(new Writer(), n));
    return w.finish();
  }
  return new Writer().message(4, allNode(t)).finish();
}

// ─────────────────────────────────────────────────────────── signal

/** `BuiltInSignal.CANCEL` — sent by the runtime on a cooperative admin cancel. */
export const CANCEL_SIGNAL = 1;

/** A received signal (`SignalNotificationMessage`, 0xFBFF). */
export interface SignalNotification {
  readonly idx?: number | undefined;
  readonly name?: string | undefined;
  readonly result: CompletionResult;
}

export function decodeSignal(payload: Uint8Array): SignalNotification {
  let idx: number | undefined;
  let name: string | undefined;
  let result: CompletionResult = { kind: "void" };
  eachField(payload, (field, _wire, r) => {
    switch (field) {
      case 2:
        idx = Number(r.varint());
        return true;
      case 3:
        name = r.string();
        return true;
      case 4:
        r.bytes();
        result = { kind: "void" };
        return true;
      case 5:
        result = { kind: "value", value: decodeValue(r.bytes()) };
        return true;
      case 6: {
        const f = decodeFailure(r.bytes());
        result = { kind: "failure", ...f };
        return true;
      }
      default:
        return false;
    }
  });
  return { idx, name, result };
}

/** `SendSignalCommandMessage` (0x0410). */
export function encodeSendSignal(f: {
  target: string;
  signal: number | string;
  value?: Uint8Array | undefined;
  name: string;
}): Uint8Array {
  const w = new Writer().string(1, f.target);
  if (typeof f.signal === "number") w.uint32Always(2, f.signal);
  else w.string(3, f.signal);
  if (f.value !== undefined) {
    w.message(5, new Writer().bytes(1, f.value));
  } else {
    w.message(4, new Writer());
  }
  return w.string(12, f.name).finish();
}

export const ERROR_RETRY = 0;
export const ERROR_PAUSE = 1;
export const ERROR_FAIL = 2;

/**
 * `ReplayReportMessage { uint32 matched = 1; bool mismatch = 2;
 * string expected = 3; string got = 4; string message = 5 }` — wire v3.
 */
export function encodeReplayReport(r: {
  matched: number;
  mismatch: boolean;
  expected?: string | undefined;
  got?: string | undefined;
  message?: string | undefined;
}): Uint8Array {
  const w = new Writer().uint32(1, r.matched);
  if (r.mismatch) w.uint32(2, 1);
  if (r.expected) w.string(3, r.expected);
  if (r.got) w.string(4, r.got);
  if (r.message) w.string(5, r.message);
  return w.finish();
}

/** A stream record — `StreamRecord` of `protocol.proto`. */
export interface StreamRecordWire {
  readonly step: number;
  readonly attempt: number;
  readonly k: number;
  readonly payload: Uint8Array;
}

/**
 * `StreamAppendMessage { name = 1, durable = 2, repeated StreamRecord records =
 * 3, close = 4 }` — wire v4.
 */
/** `ArtifactPutMessage` (wire v4). */
export function encodeArtifactPut(sha256: Uint8Array, content: Uint8Array, contentType: string): Uint8Array {
  const w = new Writer().bytes(1, sha256).bytes(2, content);
  if (contentType.length > 0) w.string(3, contentType);
  return w.finish();
}

export function encodeStreamAppend(
  name: string,
  durable: boolean,
  records: readonly StreamRecordWire[],
  close: boolean,
): Uint8Array {
  const w = new Writer().string(1, name);
  if (durable) w.uint32(2, 1);
  for (const r of records) {
    w.message(
      3,
      new Writer()
        .uint32(1, r.step)
        .uint32(2, r.attempt)
        .uint32(3, r.k)
        .bytes(4, r.payload),
    );
  }
  if (close) w.uint32(4, 1);
  return w.finish();
}

/** `ContinueAsNewMessage { bytes input = 1 }` — wire v3. */
export function encodeContinueAsNew(input: Uint8Array): Uint8Array {
  return new Writer().bytesAlways(1, input).finish();
}

export function encodeError(code: number, message: string, behavior: number): Uint8Array {
  return new Writer()
    .uint32(1, code)
    .string(2, message)
    .uint32(9, behavior)
    .finish();
}
