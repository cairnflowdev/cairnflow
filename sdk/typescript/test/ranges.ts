// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Splitting and merging ranges **while** the cluster is under load — shared by the
 * "split midway" shape of `e2e.test.ts` and by `chaos.ts`.
 *
 * Not a test file (`node --test test/*.test.ts` does not pick it up): just a
 * disciplined saboteur. Each tick it does **one** thing, alternating:
 *
 * - **split** a range (round-robin over the current ranges, skipping ranges with an intent);
 * - **merge** exactly the child pair of the latest split back — two adjacent ranges, so the
 *   merge is always valid if both are still there.
 *
 * That way the range count oscillates around the initial count instead of growing forever, and
 * every split has a merge to go with it — both paths of the protocol get exercised.
 *
 * Responses are classified, because the final verdict must tell "the cluster refused for a reason"
 * apart from "the cluster is broken":
 *
 * - `200` — done.
 * - `409`, `503`, network errors — **not possible right now** (the range has an intent, the leader
 *   is changing, a node was just killed). Counted in `deferred`, retried on a later tick.
 * - `404` for a range the current table **no longer has** — the previous attempt finished even
 *   though the response never arrived (the node died after the commit). Counted in `deferred`; a
 *   `404` for a range still in the table is an error.
 * - Any other code — an **error**, recorded verbatim. The caller's verdict treats it
 *   as a failure.
 */

/** One row of `GET /cluster` → `ranges`. */
export interface RangeRow {
  range: number;
  start: string;
  end: string;
  gen: number;
  intent?: unknown;
  leader?: string | null;
}

export interface ChurnStats {
  splits: number;
  merges: number;
  /** A refusal with a reason (409/503/network) — retry later. */
  deferred: number;
  /** Responses outside the contract — each line is a real error. */
  errors: string[];
}

export interface ChurnOptions {
  /** Interval between two actions, milliseconds. */
  everyMs: number;
  /** Print one line per action (chaos wants it, e2e does not). */
  log?: (line: string) => void;
  /**
   * Error responses the caller causes **on purpose** — `chaos.ts --failpoints` injects
   * `return(...)` into the phases of split/merge: the step really fails and really rolls back, but
   * it is not the cluster's fault. `true` ⇒ counted as deferred, not in `errors`.
   */
  injected?: (status: number, text: string) => boolean;
}

type Fetch = (path: string, init?: RequestInit) => Promise<Response>;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** The current range table, in key order. */
export async function rangesOf(fetchPath: Fetch): Promise<RangeRow[]> {
  const r = await fetchPath("/cluster");
  const body = (await r.json()) as { ranges?: RangeRow[] };
  return [...(body.ranges ?? [])].sort((a, b) => (a.start < b.start ? -1 : a.start > b.start ? 1 : 0));
}

/**
 * The range table **covers** `0..=u64::MAX` completely, no gaps, no overlaps, every range has a
 * leader, no range still has an intent — the resting state a cluster must return to after
 * any sequence of splits/merges/kills. `null` = correct; a string = what is wrong.
 */
export function tableProblem(rows: RangeRow[]): string | null {
  if (rows.length === 0) return "empty table";
  if (rows[0]!.start !== "0000000000000000") return `the first range starts at ${rows[0]!.start}`;
  if (rows[rows.length - 1]!.end !== "ffffffffffffffff") {
    return `the last range ends at ${rows[rows.length - 1]!.end}`;
  }
  for (let i = 1; i < rows.length; i++) {
    const prevEnd = BigInt(`0x${rows[i - 1]!.end}`);
    const start = BigInt(`0x${rows[i]!.start}`);
    if (prevEnd + 1n !== start) {
      return `gap/overlap between range ${rows[i - 1]!.range} (..${rows[i - 1]!.end}) and ${rows[i]!.range} (${rows[i]!.start}..)`;
    }
  }
  const pending = rows.filter((r) => r.intent);
  if (pending.length) return `intent still on range ${pending.map((r) => r.range).join(", ")}`;
  const headless = rows.filter((r) => !r.leader);
  if (headless.length) return `ranges without a leader: ${headless.map((r) => r.range).join(", ")}`;
  return null;
}

/** The two children of `parent` in the table after a split: the ranges lying inside it. */
function childrenOf(parent: RangeRow, after: RangeRow[]): [number, number] | null {
  const lo = BigInt(`0x${parent.start}`);
  const hi = BigInt(`0x${parent.end}`);
  const inside = after.filter(
    (r) => BigInt(`0x${r.start}`) >= lo && BigInt(`0x${r.end}`) <= hi && r.range !== parent.range,
  );
  return inside.length === 2 ? [inside[0]!.range, inside[1]!.range] : null;
}

export class RangeChurner {
  readonly stats: ChurnStats = { splits: 0, merges: 0, deferred: 0, errors: [] };
  private stopped = false;
  private loop: Promise<void> | undefined;
  /** Child pair of the latest split, waiting to be merged back. */
  private lastPair: [number, number] | null = null;
  private turn = 0;

  private readonly fetchPath: Fetch;
  private readonly opts: ChurnOptions;

  constructor(fetchPath: Fetch, opts: ChurnOptions) {
    this.fetchPath = fetchPath;
    this.opts = opts;
  }

  start(): void {
    this.loop = (async () => {
      while (!this.stopped) {
        await sleep(this.opts.everyMs);
        if (this.stopped) break;
        try {
          await this.once();
        } catch (e) {
          // Network/node died midway: not possible right now, not the cluster's fault.
          this.stats.deferred++;
          this.opts.log?.(`  churn: deferred (${(e as Error).message})`);
        }
      }
    })();
  }

  /** Stops and waits for the action in progress to finish — never abandons a split midway. */
  async stop(): Promise<ChurnStats> {
    this.stopped = true;
    await this.loop;
    return this.stats;
  }

  /** One action: merge the waiting pair if any, otherwise split. */
  private async once(): Promise<void> {
    if (this.lastPair) {
      const [left, right] = this.lastPair;
      const rows = await rangesOf(this.fetchPath);
      if (!rows.some((r) => r.range === left) || !rows.some((r) => r.range === right)) {
        // The pair is gone (someone else merged/split further) — drop it, go split.
        this.lastPair = null;
      } else {
        const r = await this.fetchPath(`/ranges/merge?left=${left}&right=${right}`, {
          method: "POST",
        });
        if (await this.judge(r, `merge ${left}+${right}`, [left, right])) {
          this.stats.merges++;
          this.lastPair = null;
        }
        return;
      }
    }

    const rows = await rangesOf(this.fetchPath);
    const free = rows.filter((r) => !r.intent);
    if (free.length === 0) {
      this.stats.deferred++;
      return;
    }
    const parent = free[this.turn++ % free.length]!;
    const r = await this.fetchPath(`/ranges/${parent.range}/split`, { method: "POST" });
    const body = (await r.clone().json().catch(() => undefined)) as { ranges?: RangeRow[] } | undefined;
    if (await this.judge(r, `split ${parent.range}`, [parent.range])) {
      this.stats.splits++;
      const after = body?.ranges ?? (await rangesOf(this.fetchPath));
      this.lastPair = childrenOf(parent, after);
      if (!this.lastPair) {
        this.stats.errors.push(
          `split ${parent.range} returned 200 but the table does not have exactly two children: ${JSON.stringify(after)}`,
        );
      }
    }
  }

  /**
   * `true` = done; `false` = deferred or error (errors are recorded). `named` are the
   * ranges the request mentions — to tell a `404` for a range that has disappeared (the previous
   * attempt finished) from a `404` for a range that is still there.
   */
  private async judge(r: Response, what: string, named: number[]): Promise<boolean> {
    if (r.status === 200) {
      this.opts.log?.(`  [${new Date().toISOString()}] churn: ${what} ✓`);
      return true;
    }
    const text = await r.text().catch(() => "");
    if (r.status === 409 || r.status === 503) {
      this.stats.deferred++;
      this.opts.log?.(`  churn: ${what} deferred (${r.status} ${text.slice(0, 120)})`);
      return false;
    }
    if (r.status === 404) {
      const rows = await rangesOf(this.fetchPath).catch(() => undefined);
      if (rows && named.some((n) => !rows.some((row) => row.range === n))) {
        this.stats.deferred++;
        this.lastPair = null;
        this.opts.log?.(`  churn: ${what} — the range is already gone (the previous attempt finished), skipping`);
        return false;
      }
    }
    if (this.opts.injected?.(r.status, text)) {
      this.stats.deferred++;
      this.opts.log?.(`  churn: ${what} failed from an injected error (${r.status} ${text.slice(0, 120)})`);
      return false;
    }
    this.stats.errors.push(`${what}: ${r.status} ${text.slice(0, 300)}`);
    this.opts.log?.(`  churn: ${what} ERROR ${r.status} ${text.slice(0, 200)}`);
    return false;
  }
}
