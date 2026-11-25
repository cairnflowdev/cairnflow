// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

/**
 * The chaos harness's compact ledger: everything `chaos.ts` records **per
 * call** lives in growing typed arrays, not in `Map`/`Array` of objects and
 * strings. Previously the heap grew 34 → 799 MiB per hour (one run: ~415 k
 * calls) — a 24 h soak ran out of memory or was strangled by GC. Here each
 * call costs about 60 bytes, and the end-of-run verdict still has **all** the
 * data: every acked id, every side-effect run, the first time each id was seen `completed`.
 *
 * - [`AckLedger`]: acked id (24 binary bytes) → `seq`, in ack order.
 * - [`SeqTable`]: by `seq` (dense count from 0): number of side-effect runs, the
 *   first run (timestamp, nonce, attempt, hash of the invocation id), tenant; the
 *   second run onward (rare: kill mid-turn, retry) is kept verbatim in a side `Map`.
 * - [`LogHist`]: log histogram for latency samples that are only printed (noisy tenant, RR).
 *
 * Timestamps are stored as `Uint32` ms from `base` (by default one hour before
 * the ledger is built): good for ~49 days.
 */

import type { Run } from "./history.ts";

type Typed = Float64Array | Uint32Array | Uint16Array | Uint8Array;

/** Growing typed array (× 1.5). Unwritten cells read as 0. */
export class Grow<A extends Typed> {
  a: A;
  length = 0;
  private readonly make: (n: number) => A;
  constructor(make: (n: number) => A, cap = 1024) {
    this.make = make;
    this.a = make(cap);
  }
  ensure(len: number): void {
    if (len <= this.a.length) return;
    let c = this.a.length;
    while (c < len) c = Math.ceil(c * 1.5);
    const b = this.make(c);
    b.set(this.a as never);
    this.a = b;
  }
  get(i: number): number {
    return i < this.a.length ? (this.a[i] as number) : 0;
  }
  set(i: number, v: number): void {
    this.ensure(i + 1);
    this.a[i] = v;
    if (i >= this.length) this.length = i + 1;
  }
  push(v: number): number {
    const i = this.length;
    this.set(i, v);
    return i;
  }
  /** Bytes in use (capacity, not length). */
  bytes(): number {
    return this.a.byteLength;
  }
}

/** Timestamp ms ↔ `Uint32` from `base` (0 = absent). */
export class Clock32 {
  readonly base: number;
  constructor(base = Date.now() - 3_600_000) {
    this.base = base;
  }
  put(t: number): number {
    return Math.max(1, Math.min(0xffffffff, Math.round(t - this.base) + 1));
  }
  get(v: number): number | undefined {
    return v === 0 ? undefined : this.base + v - 1;
  }
}

/** FNV-1a 32 bit — compares strings without keeping them. */
export function fnv(s: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h >>> 0;
}

const HEX = /^[0-9a-f]+$/;

/** Writes the hex string `hex` (length `2 × n`) as `n` bytes at `dst[off…]`. */
function putHex(dst: Uint8Array, off: number, hex: string): void {
  for (let k = 0; k < hex.length / 2; k++) dst[off + k] = parseInt(hex.slice(2 * k, 2 * k + 2), 16);
}

function getHex(src: Uint8Array, off: number, n: number): string {
  let s = "";
  for (let k = 0; k < n; k++) s += src[off + k]!.toString(16).padStart(2, "0");
  return s;
}

/**
 * Table of fixed-width ids of `width` bytes (`2 × width` hex chars). An id of
 * the wrong shape (rare — the id format changed) is kept verbatim in a side `Map`, not lost.
 */
class IdColumn {
  private readonly bytesCol: Grow<Uint8Array>;
  private readonly odd = new Map<number, string>();
  private readonly width: number;
  constructor(width: number) {
    this.width = width;
    this.bytesCol = new Grow((n) => new Uint8Array(n), width * 1024);
  }
  set(i: number, id: string): void {
    this.bytesCol.ensure((i + 1) * this.width);
    if (id.length === 2 * this.width && HEX.test(id)) {
      this.odd.delete(i);
      putHex(this.bytesCol.a, i * this.width, id);
    } else {
      this.odd.set(i, id);
    }
  }
  get(i: number): string {
    return this.odd.get(i) ?? getHex(this.bytesCol.a, i * this.width, this.width);
  }
  bytes(): number {
    return this.bytesCol.bytes();
  }
}

/** A cairn invocation id: 24 bytes (48 hex). */
const ID_BYTES = 24;

/**
 * Every acked invocation, in ack order: id → `seq`, and when it was acked. The index (`idx`)
 * is the ack order.
 */
export class AckLedger {
  private readonly ids = new IdColumn(ID_BYTES);
  private readonly seqs = new Grow((n) => new Uint32Array(n));
  private readonly at = new Grow((n) => new Uint32Array(n));
  private readonly clock: Clock32;
  size = 0;
  constructor(clock = new Clock32()) {
    this.clock = clock;
  }
  add(id: string, seq: number, t = Date.now()): number {
    const i = this.size++;
    this.ids.set(i, id);
    this.seqs.set(i, seq);
    this.at.set(i, this.clock.put(t));
    return i;
  }
  id(i: number): string {
    return this.ids.get(i);
  }
  seq(i: number): number {
    return this.seqs.get(i);
  }
  /** When `i` was acked (timestamp ms). */
  ackedAt(i: number): number | undefined {
    return this.clock.get(this.at.get(i));
  }
  bytes(): number {
    return this.ids.bytes() + this.seqs.bytes() + this.at.bytes();
  }
}

/** Nonce of a side-effect run: 6 bytes (12 hex) — `randomBytes(6)` in `chaos.ts`. */
const NONCE_BYTES = 6;
/** `attempt` absent (or ≥ 255) in a `Uint8` cell. */
const NO_ATTEMPT = 255;

/**
 * By `seq`: tenant (index into the name table), number of side-effect runs, the first run
 * (timestamp, nonce, attempt, hash of the invocation id), the last handler entry (sparse — only
 * tenants that can change state). The second run onward is in `more` (verbatim).
 *
 * The first run's invocation id is kept as a **hash** (`fnv`): the reader ([`Self::runsOf`])
 * passes the acked id; hash matches ⇒ returns exactly that id, no match ⇒ `(hash …)` — the history
 * checker's comparison "is this run the acked id's" stays intact (hash collision
 * 2⁻³²).
 */
export class SeqTable {
  private readonly clock: Clock32;
  private readonly tenantNames: string[] = [];
  private readonly tenantIdx = new Map<string, number>();
  /** Tenant index + 1 (0 = not written). */
  private readonly tenant = new Grow((n) => new Uint8Array(n));
  private readonly runs = new Grow((n) => new Uint16Array(n));
  private readonly runT = new Grow((n) => new Uint32Array(n));
  private readonly runAttempt = new Grow((n) => new Uint8Array(n));
  private readonly runNonce = new IdColumn(NONCE_BYTES);
  /** Hash of the first run's invocation id; 0 = absent. */
  private readonly runInv = new Grow((n) => new Uint32Array(n));
  private readonly workT = new Map<number, number>();
  private readonly more = new Map<number, Run[]>();

  constructor(clock = new Clock32()) {
    this.clock = clock;
  }

  setTenant(seq: number, tenant: string): void {
    let k = this.tenantIdx.get(tenant);
    if (k === undefined) {
      k = this.tenantNames.length;
      if (k >= 255) throw new Error(`SeqTable: more than 255 tenants (${tenant})`);
      this.tenantNames.push(tenant);
      this.tenantIdx.set(tenant, k);
    }
    this.tenant.set(seq, k + 1);
  }
  tenantOf(seq: number): string | undefined {
    const k = this.tenant.get(seq);
    return k === 0 ? undefined : this.tenantNames[k - 1];
  }
  /** Last handler entry of `seq` (timestamp ms). */
  setWork(seq: number, t: number): void {
    this.workT.set(seq, t);
  }
  workAt(seq: number): number | undefined {
    return this.workT.get(seq);
  }
  /** One side-effect run of `seq`. `run.tag` is not kept (the reader passes it back). */
  noteRun(seq: number, run: Run): void {
    const n = this.runs.get(seq);
    this.runs.set(seq, Math.min(0xffff, n + 1));
    if (n === 0) {
      this.runT.set(seq, this.clock.put(run.t));
      this.runAttempt.set(seq, run.attempt === undefined ? NO_ATTEMPT : Math.min(NO_ATTEMPT - 1, run.attempt));
      this.runNonce.set(seq, run.nonce ?? "");
      this.runInv.set(seq, run.invocation === undefined ? 0 : fnv(run.invocation) || 1);
      return;
    }
    const xs = this.more.get(seq);
    if (xs) xs.push(run);
    else this.more.set(seq, [run]);
  }
  runCount(seq: number): number {
    return this.runs.get(seq);
  }
  /** Every run of `seq`, in write order, tagged with `tag`; `ackedId` to resolve the invocation id hash. */
  runsOf(seq: number, tag: string, ackedId: string): Run[] {
    if (this.runs.get(seq) === 0) return [];
    const first: Run = { tag, t: this.clock.get(this.runT.get(seq)) ?? 0 };
    const nonce = this.runNonce.get(seq);
    if (nonce !== "") first.nonce = nonce;
    const attempt = this.runAttempt.get(seq);
    if (attempt !== NO_ATTEMPT) first.attempt = attempt;
    const inv = this.runInv.get(seq);
    if (inv !== 0) first.invocation = inv === (fnv(ackedId) || 1) ? ackedId : `(hash ${inv.toString(16)})`;
    return [first, ...(this.more.get(seq) ?? []).map((r) => ({ ...r, tag }))];
  }
  /** Bytes in use (approximate, not counting the two sparse `Map`s). */
  bytes(): number {
    return (
      this.tenant.bytes() +
      this.runs.bytes() +
      this.runT.bytes() +
      this.runAttempt.bytes() +
      this.runNonce.bytes() +
      this.runInv.bytes()
    );
  }
  /** Number of `seq`s with a second run or more. */
  extraRuns(): number {
    return this.more.size;
  }
}

/**
 * Log histogram of latency (ms): cell `floor(log(1 + ms) / log(1 + ε))`, relative
 * error ≤ ε = 1 %. Max is kept exactly. Replaces sample arrays for data that is only printed.
 */
export class LogHist {
  private static readonly STEP = Math.log(1.01);
  private readonly buckets = new Grow((n) => new Float64Array(n), 2048);
  count = 0;
  max = 0;
  add(ms: number): void {
    const v = Math.max(0, ms);
    const b = Math.floor(Math.log1p(v) / LogHist.STEP);
    this.buckets.set(b, this.buckets.get(b) + 1);
    this.count++;
    if (v > this.max) this.max = v;
  }
  /** Same convention as `quantile` in `chaos.ts` (rank `floor(q × n)`); `q = 1` ⇒ max. 0 when empty. */
  quantile(q: number): number {
    if (this.count === 0) return 0;
    if (q >= 1) return this.max;
    const rank = Math.min(this.count - 1, Math.floor(q * this.count));
    let seen = 0;
    for (let b = 0; b < this.buckets.length; b++) {
      seen += this.buckets.get(b);
      if (seen > rank) {
        // Mid-cell, rounded like a sample (whole ms); never above max.
        const lo = Math.expm1(b * LogHist.STEP);
        const hi = Math.expm1((b + 1) * LogHist.STEP);
        return Math.min(this.max, Math.round((lo + hi) / 2));
      }
    }
    return this.max;
  }
  reset(): void {
    this.buckets.a.fill(0);
    this.count = 0;
    this.max = 0;
  }
}
