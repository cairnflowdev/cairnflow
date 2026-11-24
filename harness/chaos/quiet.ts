// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Invariant 6 of the chaos harness (a noisy tenant does not slow a quiet one): the quiet
// tenants' p99 is judged over pooled windows, never over a window with few samples.
//
// A 300 s window under constant `kill -9` holds ~160 quiet calls, so its "p99" is the
// second-slowest call: one leader handover lands it over 1.5 × the baseline's p99 (a
// window with ~1 000 samples, whose p99 is the tenth-slowest). A 24 h soak flagged five
// such windows while the quiet tenants' hourly p99 stayed at or under the baseline's.
// Pooling windows until there are `minSamples` quiet calls puts at least
// `minSamples / 100` calls above the p99, so one slow call no longer decides it.

/** The `q` quantile of `xs` (nearest rank, no interpolation); 0 for an empty list. */
export function quantile(xs: number[], q: number): number {
  if (xs.length === 0) return 0;
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor(q * s.length))]!;
}

/** One judged pool. */
export type QuietVerdict = {
  /** Windows pooled. */
  windows: number;
  /** Quiet calls pooled. */
  quiet: number;
  p99: number;
  limit: number;
  fails: boolean;
};

/** Pools windows of quiet and baseline latencies until there are enough quiet calls to judge. */
export class QuietPool {
  private readonly minSamples: number;
  private readonly limitOf: (base: number[]) => number;
  private quiet: number[] = [];
  private base: number[] = [];
  private windows = 0;
  constructor(minSamples: number, limitOf: (base: number[]) => number) {
    this.minSamples = Math.max(1, minSamples);
    this.limitOf = limitOf;
  }
  /** Adds one window; returns a verdict once the pool holds `minSamples` quiet calls, then starts a new pool. */
  add(quiet: number[], base: number[]): QuietVerdict | undefined {
    this.quiet.push(...quiet);
    this.base.push(...base);
    this.windows++;
    if (this.quiet.length < this.minSamples) return undefined;
    const p99 = quantile(this.quiet, 0.99);
    const limit = this.limitOf(this.base);
    const verdict = { windows: this.windows, quiet: this.quiet.length, p99, limit, fails: p99 > limit };
    this.quiet = [];
    this.base = [];
    this.windows = 0;
    return verdict;
  }
}
