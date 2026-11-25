// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

/**
 * History checker of invocation results for chaos — judges by the **observed
 * history**, Jepsen/Elle style, instead of by a single final read.
 *
 * The old verdict of `chaos.ts` asked about each acked id until it saw
 * `completed` with the right content. It could not see three things the
 * engine's promise forbids: a durable result **disappearing** and coming back
 * (rerun to completion with the same content), a result **changing** between
 * two reads, and a side effect **running again after** the result was durable.
 * This checker records every status read (time, source — the `x-cairn-read`
 * header, status, output) and every side-effect run, then checks:
 *
 * - **flip** — two `completed` reads of the same id with different outputs;
 * - **vanish** — after `completed` was first seen, a read from the **leader** sees a status other
 *   than `completed` (not finished, or `failed`/`paused`). A read from a follower is a stale read
 *   (durable but possibly behind): only counted, not a violation;
 * - **rerun** — a side effect of the invocation starts running **after**
 *   `completed` was first seen (every source only exposes durable state): "a
 *   durable result never runs again". Each run carries a `nonce` that the side
 *   effect returns (it goes into the output), so the detail can say whether the
 *   late run **got committed**: its nonce is in a `completed` output that was
 *   read ⇒ the late run's result won (a durable result was recomputed); it is
 *   not ⇒ a discarded attempt still ran the closure (the overlapping-attempt
 *   window). A commit after the first `completed` with a different output is also a **flip**.
 *
 *   Only runs of the **same invocation id** (`ctx.invocationId`) count. The
 *   chaos sender retries `POST /invoke` (same body, no idempotency key) on a
 *   network error or `503`, so a send the node accepted just before dying
 *   creates a **second** invocation with the same tag. Its side effect is not a
 *   rerun: it is counted separately in `duplicateSubmissions`.
 *
 * A `404` after `completed` does not count as a vanish: an invocation past its
 * retention is cleaned up. Chaos runs shorter than the default retention, so
 * that `404` is still counted separately for the reader to see.
 */

import { Clock32, fnv, Grow } from "./ledger.ts";

export type ReadSource = "leader" | "follower" | "unknown";

/** One status read of `id` at `t` (ms). `status` is `"404"` when absent. */
export type Observation = {
  id: string;
  t: number;
  source: ReadSource;
  status: string;
  output?: string | undefined;
};

/**
 * The side effect of the invocation tagged `tag` starts running at `t`. `nonce` is what
 * the side effect returns (goes into the output), `attempt` is the run's `ctx.attempt`,
 * `invocation` is `ctx.invocationId` (absent ⇒ treated as the same id as the read).
 */
export type Run = { tag: string; t: number; nonce?: string; attempt?: number; invocation?: string };

export type Anomaly = { kind: "flip" | "vanish" | "rerun"; id: string; detail: string };

export type HistoryReport = {
  anomalies: Anomaly[];
  /** A follower read that saw a status older than a `completed` already seen. */
  staleFollowerReads: number;
  /** A `404` read after `completed`. */
  goneAfterCompleted: number;
  /** Ids with at least one `completed` read. */
  completedIds: number;
  /** Ids seen completed whose tag also has side effects of **another id** (duplicate submission). */
  duplicateSubmissions: number;
};

/** Checks a history. `tagOf` maps an acked id to its side-effect tag. */
export function checkHistory(
  observations: readonly Observation[],
  runs: readonly Run[],
  tagOf: ReadonlyMap<string, string>,
): HistoryReport {
  const byId = new Map<string, Observation[]>();
  for (const o of observations) {
    const xs = byId.get(o.id);
    if (xs) xs.push(o);
    else byId.set(o.id, [o]);
  }
  const runsByTag = new Map<string, Run[]>();
  for (const r of runs) {
    const xs = runsByTag.get(r.tag);
    if (xs) xs.push(r);
    else runsByTag.set(r.tag, [r]);
  }
  const report: HistoryReport = {
    anomalies: [],
    staleFollowerReads: 0,
    goneAfterCompleted: 0,
    completedIds: 0,
    duplicateSubmissions: 0,
  };
  for (const [id, xs] of byId) {
    xs.sort((a, b) => a.t - b.t);
    const first = xs.find((o) => o.status === "completed");
    if (!first) continue;
    report.completedIds++;
    for (const o of xs) {
      if (o.status === "completed" && o.output !== first.output) {
        report.anomalies.push({
          kind: "flip",
          id,
          detail: `output ${first.output} at ${first.t} then ${o.output} at ${o.t} (${o.source})`,
        });
      }
      if (o.t <= first.t) continue;
      if (o.status === "404") {
        report.goneAfterCompleted++;
      } else if (o.status !== "completed") {
        if (o.source === "leader") {
          report.anomalies.push({
            kind: "vanish",
            id,
            detail: `completed at ${first.t} (${first.source}), leader returned ${o.status} at ${o.t}`,
          });
        } else {
          report.staleFollowerReads++;
        }
      }
    }
    const tag = tagOf.get(id);
    const ofTag = tag ? (runsByTag.get(tag) ?? []) : [];
    const all = ofTag.filter((r) => r.invocation === undefined || r.invocation === id);
    if (all.length < ofTag.length) report.duplicateSubmissions++;
    const late = all.filter((r) => r.t > first.t);
    if (late.length > 0) {
      const outputs = xs.filter((o) => o.status === "completed").map((o) => o.output ?? "");
      const describe = (r: Run) => {
        const won = r.nonce !== undefined && outputs.some((o) => o.includes(r.nonce!));
        return `${r.t} (attempt ${r.attempt ?? "?"}, nonce ${r.nonce ?? "?"}${won ? ", COMMITTED" : ""})`;
      };
      const reads = xs.map((o) => `${o.t} ${o.source} ${o.status}`).join("; ");
      report.anomalies.push({
        kind: "rerun",
        id,
        detail:
          `side effect of ${tag} ran at ${late.map(describe).join(", ")} after completed was seen at ` +
          `${first.t} (${first.source}); all runs: ${all.map(describe).join(", ")}; ` +
          `first output ${first.output}; reads: ${reads}`,
      });
    }
  }
  return report;
}

// ───────────────────────────────────── incremental history checker

const SOURCES: ReadSource[] = ["leader", "follower", "unknown"];

/**
 * **Same verdict as [`checkHistory`]**, but computed incrementally per read instead of keeping
 * every read until the end of the run: flip, vanish, stale follower read and `404` after
 * `completed` are decided at read time; rerun and duplicate submission are decided in
 * [`Self::finish`] from the first `completed` sighting of each id and the side-effect runs.
 *
 * Each id (index `idx` in the ack ledger) keeps the timestamp, source and output hash of
 * its first `completed` sighting, plus the read count — not the output. The first output
 * is kept verbatim for at most the [`FIRST_OUTPUT_KEEP`] most recent ids (to print flip
 * details); beyond that the detail prints the hash. Reads arrive in `t` order (the
 * `Date.now()` timestamp at receipt, one event loop) — exactly the order `checkHistory` sorts by.
 */
export class HistoryTracker {
  static readonly FIRST_OUTPUT_KEEP = 20_000;
  reads = 0;
  readonly anomalies: Anomaly[] = [];
  staleFollowerReads = 0;
  goneAfterCompleted = 0;
  completedIds = 0;
  private readonly clock: Clock32;
  /** Timestamp of the first `completed` sighting ([`Clock32`]; 0 = not yet). */
  private readonly firstT = new Grow((n) => new Uint32Array(n));
  private readonly firstSource = new Grow((n) => new Uint8Array(n));
  private readonly firstHash = new Grow((n) => new Uint32Array(n));
  /** Read count (saturates at 65 535 — only for printing). */
  private readonly readCount = new Grow((n) => new Uint16Array(n));
  private readonly firstOutput = new Map<number, string>();
  /** Id index in order of first `completed` sighting. */
  readonly completedOrder = new Grow((n) => new Uint32Array(n));

  constructor(clock = new Clock32()) {
    this.clock = clock;
  }

  /** One status read of the id with index `idx` (id `id`). */
  observe(idx: number, id: string, o: Omit<Observation, "id">): void {
    this.reads++;
    this.readCount.set(idx, Math.min(0xffff, this.readCount.get(idx) + 1));
    const t0 = this.clock.get(this.firstT.get(idx));
    if (t0 === undefined) {
      if (o.status !== "completed") return;
      this.firstT.set(idx, this.clock.put(o.t));
      this.firstSource.set(idx, SOURCES.indexOf(o.source));
      this.firstHash.set(idx, fnv(o.output ?? "\u0000"));
      this.firstOutput.set(idx, o.output ?? "");
      if (this.firstOutput.size > HistoryTracker.FIRST_OUTPUT_KEEP) {
        const oldest = this.firstOutput.keys().next().value as number;
        this.firstOutput.delete(oldest);
      }
      this.completedIds++;
      this.completedOrder.push(idx);
      return;
    }
    const kept = this.firstOutput.get(idx);
    const differs =
      o.status === "completed" &&
      (kept !== undefined ? (o.output ?? "") !== kept : fnv(o.output ?? "\u0000") !== this.firstHash.get(idx));
    if (differs) {
      const first = kept ?? `(hash ${this.firstHash.get(idx).toString(16)})`;
      this.anomalies.push({
        kind: "flip",
        id,
        detail: `output ${first} at ${t0} then ${o.output} at ${o.t} (${o.source})`,
      });
    }
    if (o.t <= t0) return;
    if (o.status === "404") {
      this.goneAfterCompleted++;
    } else if (o.status !== "completed") {
      if (o.source === "leader") {
        this.anomalies.push({
          kind: "vanish",
          id,
          detail: `completed at ${t0} (${SOURCES[this.firstSource.get(idx)]}), leader returned ${o.status} at ${o.t}`,
        });
      } else {
        this.staleFollowerReads++;
      }
    }
  }

  /** Timestamp of the first `completed` sighting of `idx`, if seen. */
  firstCompletedAt(idx: number): number | undefined {
    return this.clock.get(this.firstT.get(idx));
  }

  /**
   * The part decided at the end of the run: rerun and duplicate submission, for every id
   * `0 … size − 1` seen `completed`. `idOf`/`runsOf` read from the ack ledger and the `seq` table.
   */
  finish(size: number, idOf: (idx: number) => string, runsOf: (idx: number) => { tag: string; runs: Run[] }): HistoryReport {
    let duplicateSubmissions = 0;
    const anomalies = [...this.anomalies];
    for (let idx = 0; idx < size; idx++) {
      const t0 = this.clock.get(this.firstT.get(idx));
      if (t0 === undefined) continue;
      const id = idOf(idx);
      const { tag, runs } = runsOf(idx);
      const all = runs.filter((r) => r.invocation === undefined || r.invocation === id);
      if (all.length < runs.length) duplicateSubmissions++;
      const late = all.filter((r) => r.t > t0);
      if (late.length === 0) continue;
      const out = this.firstOutput.get(idx);
      const describe = (r: Run) => {
        const won = r.nonce !== undefined && out !== undefined && out.includes(r.nonce);
        return `${r.t} (attempt ${r.attempt ?? "?"}, nonce ${r.nonce ?? "?"}${won ? ", COMMITTED" : ""})`;
      };
      anomalies.push({
        kind: "rerun",
        id,
        detail:
          `side effect of ${tag} ran at ${late.map(describe).join(", ")} after completed was seen at ` +
          `${t0} (${SOURCES[this.firstSource.get(idx)]}); all runs: ${all.map(describe).join(", ")}; ` +
          `first output ${out ?? `(hash ${this.firstHash.get(idx).toString(16)})`}; ${this.readCount.get(idx)} reads`,
      });
    }
    return {
      anomalies,
      staleFollowerReads: this.staleFollowerReads,
      goneAfterCompleted: this.goneAfterCompleted,
      completedIds: this.completedIds,
      duplicateSubmissions,
    };
  }

  /** Bytes in use (approximate). */
  bytes(): number {
    return (
      this.firstT.bytes() +
      this.firstSource.bytes() +
      this.firstHash.bytes() +
      this.readCount.bytes() +
      this.completedOrder.bytes()
    );
  }
}

// ───────────────────────────────────── liveness checks
//
// The lost/wrong verdict above treats `paused` as a normal outcome and does not
// look at queues. A whole series of lifecycle bugs found by a sample billing
// scenario slipped through exactly there: nothing lost, nothing wrong, but
// attempts consumed without the SDK being called, swelling queues, or a range
// holding locks while running nothing. The functions below are pure (no I/O) so
// unit tests can check them; `chaos.ts` gathers the data and prints.

/**
 * One time the SDK **actually runs** the handler of invocation `id`: `attempt` is
 * `ctx.attempt` (`StartMessage.retry_count`) at handler entry, `t` is the ms timestamp.
 * Under `BIDI_STREAM` one turn runs the handler several times (replay after each
 * `ctx.run` ack) with the same `attempt` — so only **distinct values** count.
 */
export type TurnSeen = { id: string; attempt: number; t: number };

export type PhantomPause = {
  id: string;
  /** The `ctx.attempt` values the SDK saw in the last segment (from the last `attempt = 0` turn). */
  seen: number[];
  /** Attempts in `0 … maxAttempts − 1` the SDK did not see. */
  missing: number[];
};

export type AttemptReport = {
  /** `paused` 575 invocations checked. */
  paused: number;
  /** Paused 575 where the SDK saw fewer than `maxAttempts` turns — a violation of (b). */
  phantom: PhantomPause[];
  /** Total attempts consumed without the SDK being called, over the paused 575. Must be 0. */
  phantomAttempts: number;
  /**
   * Every invocation: attempts skipped between two consecutive turns of a segment
   * (`attempt` grows by more than 1), or a segment that opens at `attempt > 0`. Report only — a
   * `kill -9` between the `Running` commit and the request reaching the SDK produces it too.
   */
  gaps: number;
  gapIds: string[];
  /**
   * An attempt missing from a paused 575 with a node `kill -9` landing exactly in the
   * gap between the two SDK turns next to it: the deliberate FL-19 gap (the attempt is
   * counted at the `Running` commit, the node dies before the request reaches the SDK).
   * Not a violation of (b); counted separately.
   */
  excusedAttempts: number;
};

/**
 * Splits an invocation's turns into **segments**: `ctx.attempt` returns to 0 whenever
 * the journal advances, so each segment opens at an `attempt = 0` turn (or at the first
 * turn the SDK saw, if turn `0` never reached the SDK).
 */
function segments(turns: readonly TurnSeen[]): TurnSeen[][] {
  const out: TurnSeen[][] = [];
  for (const t of turns) {
    const cur = out[out.length - 1];
    const last = cur?.[cur.length - 1];
    // `attempt` going back without reaching 0 is also a new segment (the journal advanced in a
    // turn the SDK did not see) — nothing more is inferred from it.
    if (cur === undefined || last === undefined || t.attempt === 0 || t.attempt < last.attempt) out.push([t]);
    else cur.push(t);
  }
  return out;
}

/**
 * (b) **Real attempts.** An invocation `paused` for running out of `maxAttempts` (575) is
 * only valid when the SDK received all `maxAttempts` turns in the last segment — that is,
 * saw every `ctx.attempt` `0 … maxAttempts − 1`. The runtime's preflight stops at
 * `retry_count ≥ maxAttempts`, so an attempt counted without reaching the SDK (a turn
 * throttled, cancelled while waiting for a slot, an owner change) shows up as a missing number.
 */
export function checkAttempts(
  turns: readonly TurnSeen[],
  paused575: readonly string[],
  maxAttempts: number,
  killsAt: readonly number[] = [],
): AttemptReport {
  const byId = new Map<string, TurnSeen[]>();
  for (const t of turns) {
    const xs = byId.get(t.id);
    if (xs) xs.push(t);
    else byId.set(t.id, [t]);
  }
  const report: AttemptReport = {
    paused: paused575.length,
    phantom: [],
    phantomAttempts: 0,
    gaps: 0,
    gapIds: [],
    excusedAttempts: 0,
  };
  for (const [id, xs] of byId) {
    xs.sort((a, b) => a.t - b.t);
    let gaps = 0;
    for (const seg of segments(xs)) {
      gaps += seg[0]!.attempt;
      for (let i = 1; i < seg.length; i++) {
        const d = seg[i]!.attempt - seg[i - 1]!.attempt;
        if (d > 1) gaps += d - 1;
      }
    }
    if (gaps > 0) {
      report.gaps += gaps;
      report.gapIds.push(id);
    }
  }
  for (const id of paused575) {
    const segs = segments(byId.get(id) ?? []);
    const last = segs[segs.length - 1] ?? [];
    const seen = [...new Set(last.map((t) => t.attempt))].sort((a, b) => a - b);
    const missing: number[] = [];
    for (let a = 0; a < maxAttempts; a++) {
      if (seen.includes(a)) continue;
      if (killedInGap(last, a, killsAt)) report.excusedAttempts++;
      else missing.push(a);
    }
    if (missing.length > 0) {
      report.phantom.push({ id, seen, missing });
      report.phantomAttempts += missing.length;
    }
  }
  return report;
}

/** Bound of a gap open at one end (an attempt missing at the start or end of a segment). */
const KILL_GAP_MS = 60_000;

/**
 * Whether a `kill -9` fell into the gap of attempt `a`: between the segment's last SDK turn
 * with `attempt < a` and its first turn with `attempt > a`. Missing at the start or end of
 * the segment ⇒ the gap is open at one end, bounded by `KILL_GAP_MS`.
 */
function killedInGap(seg: readonly TurnSeen[], a: number, killsAt: readonly number[]): boolean {
  const before = seg.filter((t) => t.attempt < a).map((t) => t.t);
  const after = seg.filter((t) => t.attempt > a).map((t) => t.t);
  const from = before.length > 0 ? Math.max(...before) : (after.length > 0 ? Math.min(...after) : 0) - KILL_GAP_MS;
  const to = after.length > 0 ? Math.min(...after) : from + KILL_GAP_MS;
  return killsAt.some((k) => k > from && k < to);
}

/** One read of `GET /invocations/{id}`: the body on `200`, otherwise the reason. */
export type DetailRead = { ok: true; body: { status?: string; code?: number | undefined; message?: string | undefined } } | { ok: false; why: string };

export type PausedClass = {
  /** `paused` with code 575 (out of `maxAttempts`) — the input of check (b). */
  paused575: string[];
  /** Readable, but not `paused` 575 (another code, or already left `paused`). */
  other: Array<{ id: string; status: string; code?: number | undefined; message?: string | undefined }>;
  /** Unreadable after every attempt — classified **nowhere**, reported separately. */
  unreadable: Array<{ id: string; why: string }>;
};

/**
 * Classifies the `paused` ids of the cluster-wide list by a detail read (check
 * (b)). Each id is read up to `tries` times through `read(id, attempt)` — the
 * caller rotates nodes by `attempt` —, backing off `backoffMs·attempt` between two failures.
 * Still failing after the last attempt puts the id in `unreadable`, not in `other`: the
 * earlier version read once and swallowed the error as `undefined`, so three invoices
 * that really ran out of `maxAttempts` were reported as "paused other than 575" and
 * fell out of (b).
 */
export async function classifyPaused(
  ids: readonly string[],
  read: (id: string, attempt: number) => Promise<DetailRead>,
  opts: { tries?: number; backoffMs?: number; sleep?: (ms: number) => Promise<void> } = {},
): Promise<PausedClass> {
  const tries = Math.max(1, opts.tries ?? 6);
  const backoff = opts.backoffMs ?? 500;
  const sleep = opts.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
  const out: PausedClass = { paused575: [], other: [], unreadable: [] };
  for (const id of ids) {
    let last = "not read";
    let body: Extract<DetailRead, { ok: true }>["body"] | undefined;
    for (let i = 0; i < tries && !body; i++) {
      if (i > 0) await sleep(backoff * i);
      const r = await read(id, i).catch((e: unknown): DetailRead => ({ ok: false, why: String(e) }));
      if (r.ok && typeof r.body?.status === "string") body = r.body;
      else last = r.ok ? `unexpected body ${JSON.stringify(r.body)}` : r.why;
    }
    if (!body) out.unreadable.push({ id, why: last });
    else if (body.status === "paused" && body.code === 575) out.paused575.push(id);
    else out.other.push({ id, status: body.status!, code: body.code, message: body.message });
  }
  return out;
}

/** One row of `GET /scheduler` (a range on the node leading it) at `t`. */
export type SchedSample = {
  t: number;
  range: number;
  waiting: number;
  eligible: number;
  turnsInUse: number;
  locksHeld: number;
  /**
   * The `rate` bucket of a tenant on a node that is in debt or has waiters: the range is
   * stalled by the **rate limit**, not by a lock. Absent = `false`.
   */
  rateBlocked?: boolean;
};

/**
 * A range in the state "holds locks, runs nothing, has waiters" — unless the node is
 * held back by the `rate` limit (then no turn running is per contract; a real deadlock is
 * still there once the bucket turns positive again, especially after the load stops).
 */
const looksDead = (s: SchedSample) =>
  s.locksHeld > 0 && s.turnsInUse === 0 && s.eligible === 0 && s.waiting > 0 && !s.rateBlocked;

export type DeadLock = { range: number; from: number; to: number; scrapes: number; last: SchedSample };

/**
 * (d) **Deadlock.** `rounds` are the scrapes in time order, each one a row per range
 * whose leader answered. A range with `locksHeld > 0`, `turnsInUse == 0`,
 * `eligible == 0`, `waiting > 0` in `n` **consecutive** scrapes is a violation (a
 * sample billing scenario once had ten such ranges). A range absent from a
 * scrape (leader changing), or whose node is held back by the `rate` limit, breaks
 * the chain — no guessing.
 */
export function deadLocks(rounds: ReadonlyArray<ReadonlyArray<SchedSample>>, n: number): DeadLock[] {
  const streak = new Map<number, { from: number; count: number }>();
  const found = new Map<number, DeadLock>();
  for (const round of rounds) {
    const seen = new Set<number>();
    for (const s of round) {
      seen.add(s.range);
      if (!looksDead(s)) {
        streak.delete(s.range);
        continue;
      }
      const cur = streak.get(s.range) ?? { from: s.t, count: 0 };
      cur.count++;
      streak.set(s.range, cur);
      if (cur.count >= n) {
        found.set(s.range, { range: s.range, from: cur.from, to: s.t, scrapes: cur.count, last: s });
      }
    }
    for (const r of [...streak.keys()]) if (!seen.has(r)) streak.delete(r);
  }
  return [...found.values()];
}

export type QueueSlope = { range: number; slopePerMin: number; first: number; last: number; samples: number };

/**
 * (c) **Queues.** Least-squares slope of `waiting` per range (jobs/minute) over the
 * samples in `[from, to]` — the window after the load stops. The `chaos.ts` check:
 * a range with ≥ 3 samples, slope > 0 **and** jobs still waiting in the last sample
 * is a queue swelling linearly (FL-21).
 */
export function queueSlopes(samples: readonly SchedSample[], from: number, to: number): QueueSlope[] {
  const byRange = new Map<number, SchedSample[]>();
  for (const s of samples) {
    if (s.t < from || s.t > to) continue;
    const xs = byRange.get(s.range);
    if (xs) xs.push(s);
    else byRange.set(s.range, [s]);
  }
  const out: QueueSlope[] = [];
  for (const [range, xs] of byRange) {
    xs.sort((a, b) => a.t - b.t);
    const n = xs.length;
    const mx = xs.reduce((a, s) => a + s.t, 0) / n;
    const my = xs.reduce((a, s) => a + s.waiting, 0) / n;
    let num = 0;
    let den = 0;
    for (const s of xs) {
      num += (s.t - mx) * (s.waiting - my);
      den += (s.t - mx) ** 2;
    }
    out.push({
      range,
      slopePerMin: den === 0 ? 0 : (num / den) * 60_000,
      first: xs[0]!.waiting,
      last: xs[n - 1]!.waiting,
      samples: n,
    });
  }
  return out.sort((a, b) => a.range - b.range);
}

/** A range check (c) considers swelling: enough samples, rising slope, jobs still waiting in the last sample. */
export const queueGrows = (q: QueueSlope) => q.samples >= 3 && q.slopePerMin > 0 && q.last > 0;

/** The floor of one log as invariant 8 of `chaos.ts` sees it across reads. */
export type FloorTrack = { floor: number; since: number; reported: boolean; noted?: boolean };

/** Result of one `watchFloor` call. */
export type FloorStep = {
  /** The floor changed since the previous read of the same log. */
  advanced: boolean;
  /** Seconds the floor has stalled, when this read first crosses the threshold of the stall; otherwise `null`. */
  stuckS: number | null;
  /**
   * Seconds the floor has stalled, when this read first crosses **half** the threshold;
   * otherwise `null`. Noted only: the caller prints who still holds the log while it is
   * still held — at the threshold the holder may already be gone (a 24 h soak printed
   * "listed by no owner record" 30 s after the last holder let go).
   */
  halfS?: number | null;
};

/**
 * (e) **Shared-WAL floor stalled.** One `cairn-ctl wal floors` read for `log`
 * at `now` (ms): floor `floor`, `objects` objects from the floor up. A floor change
 * resets the clock; a floor unchanged with objects still there past `limitS` seconds is a
 * violation, reported once per stall.
 *
 * An **empty** log (0 objects on the floor) does not stall — there is nothing to clean —
 * so an empty read also resets the clock, like the node's `FloorState::since`. The clock
 * counted from the latest empty read is an upper bound of when the first object appeared:
 * the check may report at most one read period early, never miss. Otherwise a node idle
 * for a long time (self-drained by the slow-node detector, handing off every range) and
 * then writing again was reported stalled for the whole idle stretch.
 */
export function watchFloor(
  floors: Map<string, FloorTrack>,
  log: string,
  floor: number,
  objects: number,
  now: number,
  limitS: number,
): FloorStep {
  const prev = floors.get(log);
  if (!prev || prev.floor !== floor) {
    floors.set(log, { floor, since: now, reported: false, noted: false });
    return { advanced: prev !== undefined, stuckS: null, halfS: null };
  }
  if (objects === 0) {
    prev.since = now;
    prev.reported = false;
    prev.noted = false;
    return { advanced: false, stuckS: null, halfS: null };
  }
  if (prev.reported) return { advanced: false, stuckS: null, halfS: null };
  const stuckS = (now - prev.since) / 1000;
  if (stuckS <= limitS) {
    if (prev.noted || stuckS <= limitS / 2) return { advanced: false, stuckS: null, halfS: null };
    prev.noted = true;
    return { advanced: false, stuckS: null, halfS: stuckS };
  }
  prev.reported = true;
  return { advanced: false, stuckS, halfS: null };
}
