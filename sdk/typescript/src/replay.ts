// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * `REQUEST_RESPONSE` replay cache — wire v5 (`docs/protocol-v1.md` §9.3).
 *
 * In `REQUEST_RESPONSE` the runtime resends the whole journal every turn. With
 * this cache the SDK keeps in RAM the journal frames **already received** for
 * each running invocation; on the next turn the runtime sends only the new entries
 * with `Start.replay_from = B` and `Start.prefix_digest = h_B`. The SDK compares
 * `h_B` with its own chain — on a match it splices and runs exactly like a full
 * replay, on a mismatch or a miss it answers a `409` miss (the runtime resends
 * everything at once). Correctness does not depend on the cache: any doubt ⇒ miss.
 *
 * No `node:*` import (runs on Node, Bun, Deno, Workers): SHA-256 via
 * `crypto.subtle`.
 */

/** Domain separation of the chain's root — must match `cairn_runtime::replay::DOMAIN`. */
const DOMAIN = new TextEncoder().encode("cairn-replay-v1");

function u32be(n: number): Uint8Array {
  const b = new Uint8Array(4);
  new DataView(b.buffer).setUint32(0, n, false);
  return b;
}

async function sha256(parts: readonly Uint8Array[]): Promise<Uint8Array> {
  const total = parts.reduce((n, p) => n + p.length, 0);
  const buf = new Uint8Array(total);
  let at = 0;
  for (const p of parts) {
    buf.set(p, at);
    at += p.length;
  }
  return new Uint8Array(await crypto.subtle.digest("SHA-256", buf));
}

/** `h₀ = SHA-256("cairn-replay-v1" ‖ u32be(len id) ‖ id)`. */
export function chainRoot(id: Uint8Array): Promise<Uint8Array> {
  return sha256([DOMAIN, u32be(id.length), id]);
}

/** `hᵢ₊₁ = SHA-256(hᵢ ‖ u32be(len fᵢ) ‖ fᵢ)` — `frame` is the whole frame (8-byte header + payload). */
export function chainStep(h: Uint8Array, frame: Uint8Array): Promise<Uint8Array> {
  return sha256([h, u32be(frame.length), frame]);
}

/** Appends `frames` to the chain `chain` (currently `h₀ … h_k`), in place. */
export async function extendChain(chain: Uint8Array[], frames: readonly Uint8Array[]): Promise<void> {
  for (const f of frames) chain.push(await chainStep(chain[chain.length - 1]!, f));
}

function equal(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  let d = 0;
  for (let i = 0; i < a.length; i++) d |= a[i]! ^ b[i]!;
  return d === 0;
}

export interface ReplayCacheOptions {
  /** RAM cap in bytes (frames + chain), default 64 MiB. Full ⇒ evict the least recently used entry. */
  readonly maxBytes?: number;
  /** Entries unused for longer than this are evicted, ms. Default 15 minutes. */
  readonly ttlMs?: number;
}

interface Item {
  /** Whole frames of entries `[0, k)`. */
  readonly frames: Uint8Array[];
  /** `h₀ … h_k`. */
  readonly chain: Uint8Array[];
  readonly bytes: number;
  usedAt: number;
}

/** A held prefix matching the digest the runtime sent. */
export interface Held {
  readonly frames: readonly Uint8Array[];
  readonly chain: readonly Uint8Array[];
}

export class ReplayCache {
  readonly maxBytes: number;
  readonly ttlMs: number;
  /** `Map` keeps insertion order: the first entry is the least recently used (delete + reinsert on use). */
  private readonly items = new Map<string, Item>();
  private total = 0;

  constructor(opts: ReplayCacheOptions = {}) {
    this.maxBytes = opts.maxBytes ?? 64 << 20;
    this.ttlMs = opts.ttlMs ?? 15 * 60 * 1000;
  }

  /** Bytes held. */
  get bytes(): number {
    return this.total;
  }

  get size(): number {
    return this.items.size;
  }

  /**
   * The prefix `[0, b)` of `id` if the cache has ≥ `b` entries and `h_b` equals
   * `digest`; otherwise `undefined` (miss). Entries past the TTL are evicted.
   */
  take(id: string, b: number, digest: Uint8Array): Held | undefined {
    const it = this.items.get(id);
    if (it === undefined) return undefined;
    const now = Date.now();
    if (now - it.usedAt > this.ttlMs) {
      this.delete(id);
      return undefined;
    }
    if (b <= 0 || it.frames.length < b || !equal(it.chain[b]!, digest)) return undefined;
    this.items.delete(id);
    it.usedAt = now;
    this.items.set(id, it);
    return { frames: it.frames.slice(0, b), chain: it.chain.slice(0, b + 1) };
  }

  /**
   * What is still held for `id`, reported with a `409` (`cairn-replay-held`):
   * `k = min(held, n)` and `h_k` — taken straight from the stored chain, not rehashed.
   * `undefined` when there is no entry, it is past the TTL, or `k = 0`. Does not change LRU order.
   */
  held(id: string, n: number): { k: number; digest: Uint8Array } | undefined {
    const it = this.items.get(id);
    if (it === undefined || Date.now() - it.usedAt > this.ttlMs) return undefined;
    const k = Math.min(it.frames.length, n);
    return k > 0 ? { k, digest: it.chain[k]! } : undefined;
  }

  /** Holds entries `[0, k)` (`frames`) and `h₀ … h_k` of `id`. An entry over the cap is not held. */
  put(id: string, frames: Uint8Array[], chain: Uint8Array[]): boolean {
    this.delete(id);
    const bytes = frames.reduce((n, f) => n + f.length, 0) + chain.length * 32 + id.length;
    if (bytes > this.maxBytes) return false;
    this.items.set(id, { frames, chain, bytes, usedAt: Date.now() });
    this.total += bytes;
    for (const [k, v] of this.items) {
      if (this.total <= this.maxBytes) break;
      this.items.delete(k);
      this.total -= v.bytes;
    }
    return true;
  }

  delete(id: string): void {
    const it = this.items.get(id);
    if (it === undefined) return;
    this.items.delete(id);
    this.total -= it.bytes;
  }
}
