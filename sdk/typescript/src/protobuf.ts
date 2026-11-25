// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Minimal protobuf — just enough for `docs/protocol-v1.md` §7.
 *
 * **Why hand-written instead of protobufjs.** The surface we need is fixed and
 * small: varint, length-delimited, and exactly 15 messages. An SDK with no
 * dependencies installs anywhere and never breaks because of someone else's
 * lockfile — for an SDK that is a feature, not stubbornness.
 *
 * The trade-off: anything outside the part in use is NOT supported. A new field
 * has to be added here. That constraint is acceptable because the `.proto` is a
 * settled contract (protocol-v1 §9 leaves room for v2).
 */

export const WIRE_VARINT = 0;
export const WIRE_LEN = 2;
export const WIRE_FIXED64 = 1;

/**
 * Writes a protobuf message. **Call in increasing tag order**: the Writer writes
 * in exactly the order it is called, and the canonical form — what the
 * conformance traces compare byte for byte (`conformance/README.md`) — is
 * increasing tag order.
 */
export class Writer {
  /**
   * Byte buffer, doubled when full. The previous version kept each byte as an
   * element of a `number[]`: an 8 MiB value became ~265 MiB of heap, over Lambda's memory cap.
   */
  private buf = new Uint8Array(64);
  private len = 0;

  private reserve(n: number): void {
    if (this.len + n <= this.buf.length) return;
    let cap = this.buf.length * 2;
    while (cap < this.len + n) cap *= 2;
    const next = new Uint8Array(cap);
    next.set(this.buf.subarray(0, this.len));
    this.buf = next;
  }

  private push(byte: number): void {
    this.reserve(1);
    this.buf[this.len++] = byte;
  }

  private varint(value: bigint): void {
    let v = value;
    if (v < 0n) throw new RangeError("negative varints are not supported");
    do {
      let byte = Number(v & 0x7fn);
      v >>= 7n;
      if (v > 0n) byte |= 0x80;
      this.push(byte);
    } while (v > 0n);
  }

  private tag(field: number, wire: number): void {
    this.varint(BigInt((field << 3) | wire));
  }

  /** proto3: default values are NOT written to the wire. */
  uint32(field: number, value: number): this {
    if (value !== 0) {
      this.tag(field, WIRE_VARINT);
      this.varint(BigInt(value));
    }
    return this;
  }

  uint64(field: number, value: bigint): this {
    if (value !== 0n) {
      this.tag(field, WIRE_VARINT);
      this.varint(value);
    }
    return this;
  }

  /** `fixed64` (wire 1, 8 bytes little-endian) — proto3: 0 is not written. */
  fixed64(field: number, value: bigint): this {
    if (value === 0n) return this;
    this.tag(field, WIRE_FIXED64);
    let v = BigInt.asUintN(64, value);
    for (let i = 0; i < 8; i++) {
      this.push(Number(v & 0xffn));
      v >>= 8n;
    }
    return this;
  }

  /**
   * Numeric `repeated`, **packed** — the proto3 default and the form `prost`
   * (and every official library) produces: one tag, one length, then the
   * varints back to back. An empty list writes nothing.
   */
  packedUint32(field: number, values: readonly number[]): this {
    if (values.length === 0) return this;
    const inner = new Writer();
    for (const v of values) inner.varint(BigInt(v));
    return this.bytesAlways(field, inner.finish());
  }

  /** Written even when 0 — for fields inside a `oneof`, where "present" is information. */
  uint32Always(field: number, value: number): this {
    this.tag(field, WIRE_VARINT);
    this.varint(BigInt(value));
    return this;
  }

  bytes(field: number, value: Uint8Array): this {
    if (value.length === 0) return this;
    return this.bytesAlways(field, value);
  }

  bytesAlways(field: number, value: Uint8Array): this {
    this.tag(field, WIRE_LEN);
    this.varint(BigInt(value.length));
    this.reserve(value.length);
    this.buf.set(value, this.len);
    this.len += value.length;
    return this;
  }

  string(field: number, value: string): this {
    return this.bytes(field, new TextEncoder().encode(value));
  }

  /** Nested message. Written even when empty: `Void{}` carries meaning by being present. */
  message(field: number, sub: Writer): this {
    return this.bytesAlways(field, sub.finish());
  }

  finish(): Uint8Array {
    return this.buf.slice(0, this.len);
  }
}

export interface Field {
  readonly field: number;
  readonly wire: number;
}

export class Reader {
  private pos = 0;
  private readonly buf: Uint8Array;

  // No parameter properties: Node runs `.ts` in strip-only mode, removing types
  // without generating code. That is the price of "no build step", and a price
  // worth paying for an SDK with no dependencies.
  constructor(buf: Uint8Array) {
    this.buf = buf;
  }

  get eof(): boolean {
    return this.pos >= this.buf.length;
  }

  private byte(): number {
    if (this.pos >= this.buf.length) throw new RangeError("truncated protobuf");
    return this.buf[this.pos++]!;
  }

  varint(): bigint {
    let result = 0n;
    let shift = 0n;
    for (;;) {
      const b = this.byte();
      // The 10th byte only has room for bit 63: anything larger overflows 64 bits —
      // prost rejects it, and so do we.
      if (shift === 63n && b > 1) throw new RangeError("varint overflows 64 bits");
      result |= BigInt(b & 0x7f) << shift;
      if ((b & 0x80) === 0) return result;
      shift += 7n;
      if (shift > 63n) throw new RangeError("varint longer than 10 bytes");
    }
  }

  /** Skips `n` bytes — a truncated message throws instead of being accepted silently. */
  private advance(n: number): void {
    if (this.pos + n > this.buf.length) throw new RangeError("truncated protobuf");
    this.pos += n;
  }

  /** `fixed64` — 8 byte little-endian. */
  fixed64(): bigint {
    if (this.pos + 8 > this.buf.length) throw new RangeError("truncated protobuf");
    let v = 0n;
    for (let i = 7; i >= 0; i--) v = (v << 8n) | BigInt(this.buf[this.pos + i]!);
    this.pos += 8;
    return v;
  }

  tag(): Field {
    const key = Number(this.varint());
    return { field: key >>> 3, wire: key & 7 };
  }

  /**
   * Returns a **copy**, not a view.
   *
   * A view keeps the whole request buffer alive with it — one small retained
   * state value is enough to keep the entire body in memory forever. At the
   * message sizes here, the copy cost is negligible next to that trap.
   */
  bytes(): Uint8Array {
    const len = Number(this.varint());
    if (this.pos + len > this.buf.length) throw new RangeError("truncated protobuf");
    const out = this.buf.slice(this.pos, this.pos + len);
    this.pos += len;
    return out;
  }

  string(): string {
    return new TextDecoder().decode(this.bytes());
  }

  /**
   * Skips a field we do not care about.
   *
   * Required: protocol-v1 §9 leaves room for v2, so a newer runtime will send
   * fields we do not know yet. Being able to skip them means an old SDK still
   * works with a new runtime — exactly what the `reserved` mechanism exists to protect.
   */
  skip(wire: number): void {
    switch (wire) {
      case WIRE_VARINT:
        this.varint();
        return;
      case WIRE_LEN:
        this.bytes();
        return;
      case 5:
        this.advance(4);
        return;
      case 1:
        this.advance(8);
        return;
      default:
        throw new RangeError(`unsupported wire type ${wire}`);
    }
  }
}

/** Walks every field of a message. */
export function eachField(
  buf: Uint8Array,
  visit: (field: number, wire: number, r: Reader) => boolean,
): void {
  const r = new Reader(buf);
  while (!r.eof) {
    const { field, wire } = r.tag();
    if (!visit(field, wire, r)) r.skip(wire);
  }
}
