// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

import assert from "node:assert/strict";
import { test } from "node:test";
import { Reader, Writer, eachField } from "../src/protobuf.ts";
import * as wire from "../src/wire.ts";
import {
  FLAG_REQUESTED_ACK,
  FrameError,
  HEADER_LEN,
  MAX_FRAME_LEN,
  MT,
  decodeFrames,
  decodeNotification,
  decodeStart,
  encodeFrame,
  isCommand,
  isNotification,
} from "../src/wire.ts";

test("header follows the docs/protocol-v1.md §4 layout: type u16 | flags u16 | length u32, big-endian", () => {
  const f = encodeFrame(MT.CALL, FLAG_REQUESTED_ACK, new Uint8Array(0x1234));
  assert.deepEqual([...f.subarray(0, 2)], [0x04, 0x0d], "type in the wrong place");
  assert.deepEqual([...f.subarray(2, 4)], [0x80, 0x00], "flags in the wrong place");
  assert.deepEqual([...f.subarray(4, 8)], [0x00, 0x00, 0x12, 0x34], "length in the wrong place");
});

test("REQUESTED_ACK sits exactly at bit 47 of the 64-bit header", () => {
  const f = encodeFrame(MT.RUN, FLAG_REQUESTED_ACK, new Uint8Array());
  const hi = new DataView(f.buffer).getUint32(0, false);
  // bit 47 of the u64 = bit 15 of the high u32.
  assert.equal(hi & 0x8000, 0x8000);
});

test("non-zero reserved bits are rejected", () => {
  for (let bit = 0; bit < 15; bit++) {
    const f = encodeFrame(MT.RUN, 1 << bit, new Uint8Array());
    assert.throws(() => decodeFrames(f), FrameError, `bit ${bit} slipped through`);
  }
});

test("an empty payload is valid", () => {
  // EndMessage{} encodes to 0 bytes — must not be treated as missing data.
  const frames = decodeFrames(encodeFrame(MT.END, 0, new Uint8Array()));
  assert.equal(frames.length, 1);
  assert.equal(frames[0]!.type, MT.END);
});

test("trailing bytes at the end of the stream are an error, not a wait for more", () => {
  const good = encodeFrame(MT.END, 0, new Uint8Array());
  const buf = new Uint8Array(good.length + 3);
  buf.set(good);
  assert.throws(() => decodeFrames(buf), FrameError);
});

test("a length over the limit is rejected right at the header", () => {
  const f = new Uint8Array(HEADER_LEN);
  new DataView(f.buffer).setUint16(0, MT.INPUT, false);
  new DataView(f.buffer).setUint32(4, MAX_FRAME_LEN + 1, false);
  assert.throws(() => decodeFrames(f), FrameError);
});

test("several frames in one stream", () => {
  const buf = new Uint8Array([
    ...encodeFrame(MT.END, 0, new Uint8Array()),
    ...encodeFrame(MT.INPUT, 0, new Uint8Array([1, 2, 3])),
  ]);
  const frames = decodeFrames(buf);
  assert.deepEqual(
    frames.map((f) => f.type),
    [MT.END, MT.INPUT],
  );
  assert.deepEqual([...frames[1]!.payload], [1, 2, 3]);
});

test("classification: custom 0xFC00+ is not mistaken for a notification", () => {
  assert.equal(isNotification(0xfbff), true);
  assert.equal(isNotification(0xfc00), false, "the custom range was mistaken for a notification");
  assert.equal(isCommand(0x0402), true);
  assert.equal(isCommand(0x8002), false);
});

test("varint round trip, including large 64-bit numbers", () => {
  for (const v of [0n, 1n, 127n, 128n, 300n, 2n ** 32n, 2n ** 63n - 1n]) {
    const buf = new Writer().uint64(1, v).finish();
    if (v === 0n) {
      assert.equal(buf.length, 0, "proto3: a default value is not written to the wire");
      continue;
    }
    const r = new Reader(buf);
    assert.equal(r.tag().field, 1);
    assert.equal(r.varint(), v);
  }
});

test("an unknown field is skipped instead of breaking the whole message", () => {
  // docs/protocol-v1.md §9 leaves room for v2: a newer runtime will send fields
  // an old SDK does not know. Being able to skip them means an old SDK keeps
  // working — that is what `reserved` protects.
  const payload = new Writer()
    .uint32(3, 7) // known_entries
    .string(99, "a field from the future")
    .uint64(9, 12345n) // random_seed
    .finish();
  const start = decodeStart(payload);
  assert.equal(start.knownEntries, 7);
  assert.equal(start.randomSeed, 12345n);
});

test("notification: void differs from an empty value", () => {
  const voidPayload = new Writer().uint32(1, 5).message(4, new Writer()).finish();
  const emptyValue = new Writer()
    .uint32(1, 5)
    .message(5, new Writer().bytes(1, new Uint8Array()))
    .finish();

  assert.equal(decodeNotification(voidPayload).result.kind, "void");
  const v = decodeNotification(emptyValue).result;
  assert.equal(v.kind, "value", "an empty value was read as void");
  assert.equal(v.kind === "value" && v.value.length, 0);
});

test("eachField walks everything and does not get stuck", () => {
  const buf = new Writer().uint32(1, 1).string(2, "x").uint32(3, 3).finish();
  const seen: number[] = [];
  eachField(buf, (field, _wire, r) => {
    seen.push(field);
    if (field === 2) {
      r.string();
      return true;
    }
    return false;
  });
  assert.deepEqual(seen, [1, 2, 3]);
});

/**
 * The canonical encoding (`conformance/README.md`): top-level fields in increasing
 * tag order, numeric `repeated` packed. The runtime reads every valid form, but the
 * conformance traces compare byte for byte with `prost` — an encoder writing out of
 * order is a red trace, and this test names the encoder before anyone has to
 * read hex.
 */
test("every encoder writes fields in increasing tag order, numeric repeated packed", () => {
  const b = new TextEncoder().encode("x");
  const call = {
    service: "S",
    handler: "h",
    parameter: b,
    key: "k",
    idempotencyKey: "i",
    idCompletion: 1,
    name: "n",
    scope: "t",
    limitKey: "l",
    placement: "spread" as const,
  };
  const cases: Record<string, Uint8Array> = {
    getState: wire.encodeGetState("k", 1, "n"),
    setState: wire.encodeSetState("k", b, "n"),
    clearState: wire.encodeClearState("k", "n"),
    clearAllState: wire.encodeClearAllState("n"),
    getStateKeys: wire.encodeGetStateKeys(1, "n"),
    getEagerStateVoid: wire.encodeGetEagerState("k", null, "n"),
    getEagerStateValue: wire.encodeGetEagerState("k", b, "n"),
    getEagerStateKeys: wire.encodeGetEagerStateKeys([b, b], "n"),
    getPromise: wire.encodeGetPromise("k", 1, "n"),
    peekPromise: wire.encodePeekPromise("k", 1, "n"),
    completePromiseValue: wire.encodeCompletePromise("k", { value: b }, 1, "n"),
    completePromiseFailure: wire.encodeCompletePromise("k", { code: 4, message: "m" }, 1, "n"),
    attach: wire.encodeAttachInvocation("i", 1, "n"),
    getOutput: wire.encodeGetInvocationOutput("i", 1, "n"),
    sleep: wire.encodeSleep(5n, 1, "n"),
    call: wire.encodeCall(call, 2),
    oneWayCall: wire.encodeOneWayCall(call, 5n),
    run: wire.encodeRun(1, "n"),
    proposeRun: wire.encodeProposeRunCompletion(1, b),
    proposeRunFailure: wire.encodeProposeRunFailure(1, 4, "m"),
    runCompletion: wire.encodeRunCompletion(1, b),
    runCompletionFailure: wire.encodeRunCompletionFailure(1, 4, "m"),
    output: wire.encodeOutput(b, "n"),
    outputFailure: wire.encodeOutputFailure(4, "m", "n"),
    suspensionFirst: wire.encodeSuspensionSpec({ first: [1, 2] }),
    suspensionAll: wire.encodeSuspensionSpec({ all: [1, 2] }),
    suspensionSignal: wire.encodeSuspensionSpec({ signal: 3 }),
    suspensionNamed: wire.encodeSuspensionSpec({ signal: "s" }),
    sendSignalValue: wire.encodeSendSignal({ target: "t", signal: 3, value: b, name: "n" }),
    sendSignalVoid: wire.encodeSendSignal({ target: "t", signal: "s", name: "n" }),
    replayReport: wire.encodeReplayReport({ matched: 1, mismatch: true, expected: "e", got: "g", message: "m" }),
    continueAsNew: wire.encodeContinueAsNew(b),
    error: wire.encodeError(500, "m", 1),
  };
  for (const [what, payload] of Object.entries(cases)) {
    const tags: number[] = [];
    eachField(payload, (field, wireType, r) => {
      tags.push(field);
      r.skip(wireType);
      return true;
    });
    const sorted = [...tags].sort((x, y) => x - y);
    assert.deepEqual(tags, sorted, `${what}: fields ${tags.join(",")} not in increasing tag order`);
  }
  // Suspension's `repeated uint32`: one LEN tag (packed), not one tag per id.
  const fields: Array<[number, number]> = [];
  eachField(cases.suspensionFirst!, (field, wireType, r) => {
    fields.push([field, wireType]);
    r.skip(wireType);
    return true;
  });
  assert.deepEqual(fields, [[1, 2]], "waiting_completions must be packed");
});

/**
 * `placement` (field 13): `"spread"` writes varint 1 at the end of both `Call`
 * and `OneWayCall`; absence writes nothing — the bytes of a call without
 * `placement` stay as they were before this field (old conformance traces do
 * not drift).
 */
test("placement spread is field 13 = 1, absence writes nothing", () => {
  const base = {
    service: "S",
    handler: "h",
    parameter: new TextEncoder().encode("1"),
    key: "",
    idCompletion: 1,
    name: "S/h",
  };
  const fieldsOf = (payload: Uint8Array) => {
    const out: Array<[number, number]> = [];
    eachField(payload, (field, wireType, r) => {
      out.push([field, field === 13 ? Number(r.varint()) : -1]);
      if (field !== 13) r.skip(wireType);
      return true;
    });
    return out;
  };
  for (const [what, enc] of [
    ["call", (f: typeof base & { placement?: "spread" }) => wire.encodeCall(f, 2)],
    ["oneWayCall", (f: typeof base & { placement?: "spread" }) => wire.encodeOneWayCall(f, 0n)],
  ] as const) {
    const plain = enc(base);
    const spread = enc({ ...base, placement: "spread" });
    assert.ok(!fieldsOf(plain).some(([f]) => f === 13), `${what}: absence writes no field 13`);
    assert.deepEqual(fieldsOf(spread).at(-1), [13, 1], `${what}: spread is field 13 = 1, last`);
    assert.deepEqual(spread.subarray(0, plain.length), plain, `${what}: the leading bytes are unchanged`);
  }
});

/**
 * `tags` (field 14): one `Header {key=1, value=2}` per tag, after every other
 * field, in both `Call` and `OneWayCall`; no tags writes nothing — the bytes of
 * a call without tags stay unchanged.
 */
test("tags is field 14 repeating Header, absence writes nothing", () => {
  const base = {
    service: "S",
    handler: "h",
    parameter: new TextEncoder().encode("1"),
    key: "",
    idCompletion: 1,
    name: "S/h",
  };
  const tagsOf = (payload: Uint8Array) => {
    const out: Array<[string, string]> = [];
    eachField(payload, (field, wireType, r) => {
      if (field !== 14) {
        r.skip(wireType);
        return true;
      }
      let k = "";
      let v = "";
      eachField(r.bytes(), (f, wt, h) => {
        if (f === 1) k = h.string();
        else if (f === 2) v = h.string();
        else h.skip(wt);
        return true;
      });
      out.push([k, v]);
      return true;
    });
    return out;
  };
  for (const [what, enc] of [
    ["call", (f: typeof base & { tags?: Record<string, string> }) => wire.encodeCall(f, 2)],
    ["oneWayCall", (f: typeof base & { tags?: Record<string, string> }) => wire.encodeOneWayCall(f, 0n)],
  ] as const) {
    const plain = enc(base);
    const tagged = enc({ ...base, tags: { team: "pay", env: "prod" } });
    assert.deepEqual(tagsOf(plain), [], `${what}: no tags, no field 14`);
    assert.deepEqual(tagsOf(tagged), [["team", "pay"], ["env", "prod"]], `${what}: tags round-trip`);
    assert.deepEqual(tagged.subarray(0, plain.length), plain, `${what}: the leading bytes are unchanged`);
  }
});

/**
 * `Reader` used to be looser than prost — `skip` of wire 1/5 did not check bounds
 * (a truncated message accepted silently), a varint whose 10th byte is > 1 (64-bit
 * overflow) was accepted, `decodeStart` did not check the wire type. Must see:
 * all three throw `RangeError`.
 */
test("Reader rejects truncated fixed, 64-bit varint overflow, and Start with the wrong wire type", async () => {
  const { Reader, eachField } = await import("../src/protobuf.ts");
  const { decodeStart } = await import("../src/wire.ts");
  // field 2, wire 5 (fixed32) with only 2 bytes left.
  assert.throws(() => eachField(Uint8Array.from([0x15, 0x01, 0x02]), () => false), RangeError);
  // field 2, wire 1 (fixed64) with only 3 bytes left.
  assert.throws(() => eachField(Uint8Array.from([0x11, 1, 2, 3]), () => false), RangeError);
  // a 10-byte varint, last byte 0x02 ⇒ 64-bit overflow.
  assert.throws(() => new Reader(Uint8Array.from([0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02])).varint(), RangeError);
  // `known_entries` (field 3, varint) sent length-delimited.
  assert.throws(() => decodeStart(Uint8Array.from([0x1a, 0x01, 0x05])), RangeError);
});

/**
 * `Writer` used to keep each byte as one element of a `number[]` — an 8 MiB value
 * (a `ctx.run` result, a chat history in `ctx.set`) ⇒ ~265 MiB peak heap (measured),
 * over the 128–256 MiB Lambda memory limit. Must see: encoding 8 MiB grows the heap by
 * no more than ~4 × the value.
 */
test("Writer encodes 8 MiB without the heap growing per byte (≤ 4 × the value)", () => {
  const v = new Uint8Array(8 << 20).fill(7);
  const before = process.memoryUsage().heapUsed;
  const out = new Writer().uint32(1, 5).message(5, new Writer().bytes(1, v)).finish();
  const grown = process.memoryUsage().heapUsed - before;
  assert.equal(out.length, v.length + 12);
  assert.ok(grown < 4 * v.length, `heap grew ${(grown / 2 ** 20).toFixed(0)} MiB for 8 MiB`);
});
