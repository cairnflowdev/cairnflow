// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Builds the span tree of the Trace tab from the flat rows of `sys_span`
// (question 37): `start_ts`/`end_ts` are the record `ts` (ms epoch, JSON string).
// Display order: depth-first, children sorted by start time. Orphan spans (parent
// not in the stream — e.g. the parent's `start` is not durable yet) sit at the root.

export type SpanRow = {
  span_id: string;
  parent_id: string | null;
  name: string | null;
  start_ts: string | null;
  end_ts: string | null;
  status: string | null;
  unfinished: boolean;
  start_lost: boolean;
  attrs: string | null;
  input_tokens: number | null;
  output_tokens: number | null;
  step: number | null;
  attempt: number | null;
};

export type Span = {
  id: string;
  name: string;
  depth: number;
  /** ms epoch; a span missing its `start` takes the `end` timestamp. */
  start: number;
  /** null ⇒ not closed yet. */
  end: number | null;
  status: "ok" | "error" | "open";
  attrs: Record<string, unknown>;
  inputTokens: number | null;
  outputTokens: number | null;
  step: number | null;
  attempt: number | null;
  startLost: boolean;
};

const num = (s: string | null): number | null => {
  if (s === null || s === "") return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
};

function parseAttrs(s: string | null): Record<string, unknown> {
  if (!s) return {};
  try {
    const v = JSON.parse(s);
    return v && typeof v === "object" && !Array.isArray(v) ? v : {};
  } catch {
    return {};
  }
}

export function buildSpans(rows: SpanRow[]): Span[] {
  const byId = new Map<string, Span & { parent: string | null }>();
  for (const r of rows) {
    const start = num(r.start_ts);
    const end = num(r.end_ts);
    const at = start ?? end;
    if (at === null) continue;
    byId.set(r.span_id, {
      id: r.span_id,
      name: r.name ?? r.span_id,
      depth: 0,
      start: at,
      end: r.unfinished ? null : end,
      status: r.unfinished || end === null ? "open" : r.status === "error" ? "error" : "ok",
      attrs: parseAttrs(r.attrs),
      inputTokens: r.input_tokens,
      outputTokens: r.output_tokens,
      step: r.step,
      attempt: r.attempt,
      startLost: r.start_lost,
      parent: r.parent_id,
    });
  }
  const kids = new Map<string | null, (Span & { parent: string | null })[]>();
  for (const s of byId.values()) {
    const p = s.parent !== null && byId.has(s.parent) ? s.parent : null;
    kids.set(p, [...(kids.get(p) ?? []), s]);
  }
  const out: Span[] = [];
  const walk = (parent: string | null, depth: number) => {
    const list = (kids.get(parent) ?? []).sort(
      (a, b) => a.start - b.start || a.id.localeCompare(b.id),
    );
    for (const s of list) {
      const { parent: _, ...span } = s;
      out.push({ ...span, depth });
      walk(s.id, depth + 1);
    }
  };
  walk(null, 0);
  return out;
}

/** Time span of the whole trace; open spans extend to `now`. */
export function extent(spans: Span[], now: number): { t0: number; t1: number } {
  let t0 = Number.POSITIVE_INFINITY;
  let t1 = Number.NEGATIVE_INFINITY;
  for (const s of spans) {
    t0 = Math.min(t0, s.start);
    t1 = Math.max(t1, s.end ?? Math.max(now, s.start));
  }
  if (!Number.isFinite(t0)) return { t0: 0, t1: 1 };
  return { t0, t1: Math.max(t1, t0 + 1) };
}

/** "850 ms", "2.4 s", "3 min 5 s". */
export function duration(ms: number, locale?: string): string {
  const f = (n: number, d = 0) =>
    new Intl.NumberFormat(locale, { maximumFractionDigits: d }).format(n);
  if (ms < 1000) return `${f(ms)} ms`;
  if (ms < 60_000) return `${f(ms / 1000, 1)} s`;
  const min = Math.floor(ms / 60_000);
  const sec = Math.round((ms % 60_000) / 1000);
  return sec === 0 ? `${f(min)} min` : `${f(min)} min ${f(sec)} s`;
}

// ───────────────────────────── timeline per attempt

/** Name of the runtime's turn span (`turn/rtrace.rs`, `trace: "runtime"`). */
export const TURN_SPAN = "cairnflow.turn";

/** Spans written by the runtime (turns, each `ctx.run`) — `attrs["cairnflow.runtime"]`. */
export function isRuntime(s: Span): boolean {
  return s.attrs["cairnflow.runtime"] === true;
}

export type Lane = {
  attempt: number;
  start: number;
  /** null ⇒ the attempt is still open (a turn is running). */
  end: number | null;
  /**
   * Failed attempt: there is a later attempt (`attempt` only increases on a
   * retry), or one of its spans ended in `error`, or it is the last attempt of an
   * invocation that failed/paused. Newer runtimes close a transiently failed turn
   * with `error`; spans from older servers still close `ok`, so the other signals
   * are kept as well.
   */
  failed: boolean;
  /** The runtime's `ctx.run` spans in the attempt, by start time. */
  runs: Span[];
};

/**
 * One lane per attempt (the `attempt` column of `sys_span`), only from runtime
 * spans — the SDK's `ctx.trace.span` spans sit in the waterfall below. Lanes are
 * ordered by attempt. `finalFailed`: the invocation is `failed`/`paused` ⇒ the
 * last attempt failed too.
 */
export function buildLanes(spans: Span[], opts: { finalFailed?: boolean } = {}): Lane[] {
  const by = new Map<number, Span[]>();
  for (const s of spans) {
    if (!isRuntime(s) || s.attempt === null) continue;
    by.set(s.attempt, [...(by.get(s.attempt) ?? []), s]);
  }
  const sorted = [...by.entries()].sort(([a], [b]) => a - b);
  return sorted.map(([attempt, list], i) => {
    const start = Math.min(...list.map((s) => s.start));
    const open = list.some((s) => s.end === null && s.name === TURN_SPAN);
    const end = open ? null : Math.max(...list.map((s) => s.end ?? s.start));
    const last = i === sorted.length - 1;
    return {
      attempt,
      start,
      end,
      failed:
        !last ||
        list.some((s) => s.status === "error") ||
        (last && end !== null && opts.finalFailed === true),
      runs: list.filter((s) => s.name !== TURN_SPAN).sort((a, b) => a.start - b.start),
    };
  });
}

/** Retry wait between two adjacent lanes (hatched on the timeline). */
export function retryGaps(lanes: Lane[]): { after: number; from: number; to: number }[] {
  const out: { after: number; from: number; to: number }[] = [];
  for (let i = 0; i + 1 < lanes.length; i++) {
    const a = lanes[i] as Lane;
    const b = lanes[i + 1] as Lane;
    if (a.end !== null && b.start > a.end) out.push({ after: a.attempt, from: a.end, to: b.start });
  }
  return out;
}

/** Enough of an entry's header to join on: index, type, name. */
export type EntryRef = { index: number; kind?: string | null; name?: string | null };

/**
 * Joins the runtime's `ctx.run` spans to the journal's `Run` entries. The `step`
 * column of a runtime span is **not** the entry index (`append_runtime` takes the
 * `step` of the stream's last record), so the join is by **name + order**: walk
 * the spans by time, each span attaches to the first `Run` entry of the same name
 * that is not yet "done"; an `ok` span marks that entry done (a later span with the
 * same name moves to the next entry), a failed/open span does not (the next attempt
 * re-runs exactly that entry). A span whose `step` points at exactly one `Run`
 * entry of the same name is used directly. Newer runtimes write `attrs.entry` —
 * the real `Run` entry index — and that wins over every other join.
 */
export function linkRuns(spans: Span[], entries: EntryRef[]): Map<string, number> {
  const runs = entries.filter((e) => e.kind === "Run").sort((a, b) => a.index - b.index);
  const byIndex = new Map(runs.map((e) => [e.index, e]));
  const done = new Set<number>();
  const out = new Map<string, number>();
  const ordered = spans
    .filter((s) => isRuntime(s) && s.name !== TURN_SPAN)
    .sort((a, b) => a.start - b.start || a.id.localeCompare(b.id));
  const nameOf = (e: EntryRef) => (e.name ? e.name : "run");
  for (const s of ordered) {
    const exact = typeof s.attrs.entry === "number" ? byIndex.get(s.attrs.entry) : undefined;
    if (exact) {
      out.set(s.id, exact.index);
      if (s.status === "ok") done.add(exact.index);
      continue;
    }
    const direct = s.step !== null ? byIndex.get(s.step) : undefined;
    const e =
      direct && nameOf(direct) === s.name
        ? direct
        : runs.find((r) => !done.has(r.index) && nameOf(r) === s.name);
    if (!e) continue;
    out.set(s.id, e.index);
    if (s.status === "ok") done.add(e.index);
  }
  return out;
}

export type Milestone = {
  kind: "created" | "transitioned" | "lastFailure" | "retryAt" | "ended";
  at: number;
};

/**
 * Raw milestone strip when the handler does not enable `trace: "runtime"`: the
 * invocation's milestones that have a value, in time order. `transitioned` equal
 * to `created` (class `active`) is dropped.
 */
export function milestones(r: {
  created_at: string;
  transitioned_at: string | null;
  ended_at: string | null;
  retry_at: string | null;
  last_failure_at: string | null;
}): Milestone[] {
  const at = (v: string | null) => (v ? Date.parse(v) : Number.NaN);
  const raw: [Milestone["kind"], number][] = [
    ["created", at(r.created_at)],
    ["transitioned", at(r.transitioned_at)],
    ["lastFailure", at(r.last_failure_at)],
    ["retryAt", at(r.retry_at)],
    ["ended", at(r.ended_at)],
  ];
  const created = at(r.created_at);
  return raw
    .filter(([k, t]) => Number.isFinite(t) && !(k === "transitioned" && t === created))
    .map(([kind, t]) => ({ kind, at: t }))
    .sort((a, b) => a.at - b.at);
}

/** Attempt number as the reader sees it: the `attempt` column counts from 0, "attempt 1" is the first. */
export const attemptNumber = (attempt: number) => attempt + 1;
