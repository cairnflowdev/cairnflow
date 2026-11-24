// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { describe, expect, it } from "vitest";
import {
  attemptNumber,
  buildLanes,
  buildSpans,
  duration,
  extent,
  linkRuns,
  milestones,
  retryGaps,
  type SpanRow,
  TURN_SPAN,
} from "./spans";

const row = (r: Partial<SpanRow> & { span_id: string }): SpanRow => ({
  parent_id: null,
  name: r.span_id,
  start_ts: null,
  end_ts: null,
  status: null,
  unfinished: false,
  start_lost: false,
  attrs: null,
  input_tokens: null,
  output_tokens: null,
  step: null,
  attempt: null,
  ...r,
});

describe("buildSpans", () => {
  it("depth-first, children by start time; not closed ⇒ open; orphans go to the root", () => {
    const spans = buildSpans([
      row({ span_id: "c", parent_id: "a", start_ts: "30", end_ts: "40", status: "ok" }),
      row({ span_id: "a", start_ts: "10", end_ts: "50", status: "ok" }),
      row({ span_id: "b", parent_id: "a", start_ts: "20", end_ts: "25", status: "error" }),
      row({ span_id: "d", start_ts: "60", unfinished: true }),
      row({ span_id: "e", parent_id: "gone", end_ts: "70", start_lost: true, status: "ok" }),
    ]);
    expect(spans.map((s) => `${s.id}:${s.depth}:${s.status}`)).toEqual([
      "a:0:ok",
      "b:1:error",
      "c:1:ok",
      "d:0:open",
      "e:0:ok",
    ]);
    expect(spans.find((s) => s.id === "e")?.start).toBe(70);
  });

  it("broken attrs ⇒ empty, does not throw", () => {
    const [s] = buildSpans([row({ span_id: "a", start_ts: "1", end_ts: "2", attrs: "{oops" })]);
    expect(s?.attrs).toEqual({});
  });
});

describe("extent and duration", () => {
  it("an open span extends to now", () => {
    const spans = buildSpans([row({ span_id: "a", start_ts: "100", unfinished: true })]);
    expect(extent(spans, 400)).toEqual({ t0: 100, t1: 400 });
  });
  it("readable units", () => {
    expect(duration(850, "en")).toBe("850 ms");
    expect(duration(2_400, "en")).toBe("2.4 s");
    expect(duration(185_000, "en")).toBe("3 min 5 s");
    expect(duration(120_000, "en")).toBe("2 min");
  });
});

// Lanes per attempt, retry waits, joining run spans ↔ entries.
const RT = '{"cairnflow.runtime":true}';
const rt = (r: Partial<SpanRow> & { span_id: string }) => row({ attrs: RT, ...r });

/** Two attempts: attempt 0 failed (run `llm` errored), attempt 1 succeeded (run `llm`, then `save`). */
function twoAttempts() {
  return buildSpans([
    rt({
      span_id: "t0",
      name: TURN_SPAN,
      start_ts: "100",
      end_ts: "180",
      status: "error",
      attempt: 0,
    }),
    rt({
      span_id: "r0",
      parent_id: "t0",
      name: "llm",
      start_ts: "110",
      end_ts: "170",
      status: "error",
      attempt: 0,
      step: 0,
    }),
    rt({
      span_id: "t1",
      name: TURN_SPAN,
      start_ts: "400",
      end_ts: "520",
      status: "ok",
      attempt: 1,
    }),
    rt({
      span_id: "r1",
      parent_id: "t1",
      name: "llm",
      start_ts: "410",
      end_ts: "450",
      status: "ok",
      attempt: 1,
      step: 0,
    }),
    rt({
      span_id: "r2",
      parent_id: "t1",
      name: "save",
      start_ts: "460",
      end_ts: "500",
      status: "ok",
      attempt: 1,
      step: 0,
    }),
    // SDK spans (not runtime) do not go into lanes.
    row({ span_id: "p", name: "plan", start_ts: "405", end_ts: "510", status: "ok", attempt: 1 }),
  ]);
}

describe("buildLanes / retryGaps", () => {
  it("one lane per attempt, by attempt; error span ⇒ failed; SDK spans stay out", () => {
    const lanes = buildLanes(twoAttempts());
    expect(
      lanes.map((l) => [l.attempt, l.start, l.end, l.failed, l.runs.map((s) => s.id)]),
    ).toEqual([
      [0, 100, 180, true, ["r0"]],
      [1, 400, 520, false, ["r1", "r2"]],
    ]);
    expect(retryGaps(lanes)).toEqual([{ after: 0, from: 180, to: 400 }]);
  });

  it("real rtrace shape: a failed turn still closes ok, the run does not close — a later attempt ⇒ the earlier one failed", () => {
    const spans = buildSpans([
      rt({
        span_id: "t0",
        name: TURN_SPAN,
        start_ts: "10",
        end_ts: "40",
        status: "ok",
        attempt: 0,
      }),
      rt({
        span_id: "r",
        parent_id: "t0",
        name: "llm",
        start_ts: "20",
        unfinished: true,
        attempt: 0,
      }),
      rt({
        span_id: "t1",
        name: TURN_SPAN,
        start_ts: "300",
        end_ts: "320",
        status: "ok",
        attempt: 1,
      }),
    ]);
    const lanes = buildLanes(spans);
    expect(lanes.map((l) => [l.attempt, l.end, l.failed])).toEqual([
      [0, 40, true],
      [1, 320, false],
    ]);
    // The last attempt of an invocation that failed/paused is also a failed attempt.
    expect(buildLanes(spans, { finalFailed: true }).map((l) => l.failed)).toEqual([true, true]);
  });

  it("an unclosed turn ⇒ open lane, no wait after it", () => {
    const lanes = buildLanes(
      buildSpans([
        rt({ span_id: "t", name: TURN_SPAN, start_ts: "5", unfinished: true, attempt: 2 }),
      ]),
    );
    expect(lanes).toEqual([expect.objectContaining({ attempt: 2, end: null, failed: false })]);
    expect(retryGaps(lanes)).toEqual([]);
  });

  it("no runtime spans ⇒ no lanes", () => {
    expect(
      buildLanes(buildSpans([row({ span_id: "x", start_ts: "1", end_ts: "2", attempt: 0 })])),
    ).toEqual([]);
  });
});

describe("linkRuns", () => {
  const entries = [
    { index: 0, kind: "Input" },
    { index: 1, kind: "Run", name: "llm" },
    { index: 2, kind: "Run", name: "save" },
    { index: 3, kind: "Run", name: "llm" },
  ];

  it("name + order: the failed attempt and the re-run share an entry; once ok it moves to the next entry", () => {
    const more = [
      ...twoAttempts(),
      ...buildSpans([
        rt({
          span_id: "r3",
          name: "llm",
          start_ts: "505",
          end_ts: "515",
          status: "ok",
          attempt: 1,
        }),
      ]),
    ];
    const link = linkRuns(more, entries);
    expect(Object.fromEntries(link)).toEqual({ r0: 1, r1: 1, r2: 2, r3: 3 });
  });

  it("a step pointing at exactly one Run entry of the same name is used directly; a name matching no entry is not joined", () => {
    const spans = buildSpans([
      rt({ span_id: "a", name: "llm", start_ts: "1", end_ts: "2", status: "ok", step: 3 }),
      rt({ span_id: "b", name: "other", start_ts: "3", end_ts: "4", status: "ok" }),
    ]);
    expect(Object.fromEntries(linkRuns(spans, entries))).toEqual({ a: 3 });
  });

  // The runtime writes `attrs.entry` — the real `Run` entry index — and then it is
  // used, even when name + order would join wrongly (the first span is `ok` but at entry 3).
  it("attrs.entry (written by the runtime) wins over joining by name + order", () => {
    const at = (entry: number) => JSON.stringify({ "cairnflow.runtime": true, entry });
    const spans = buildSpans([
      row({ span_id: "x", name: "llm", start_ts: "1", end_ts: "2", status: "ok", attrs: at(3) }),
      row({ span_id: "y", name: "llm", start_ts: "3", end_ts: "4", status: "ok", attrs: at(1) }),
    ]);
    expect(Object.fromEntries(linkRuns(spans, entries))).toEqual({ x: 3, y: 1 });
  });
});

describe("milestones", () => {
  it("milestones with a value, in time order; transitioned equal to created is dropped", () => {
    const t = (ms: number) => new Date(ms).toISOString();
    expect(
      milestones({
        created_at: t(1000),
        transitioned_at: t(1000),
        last_failure_at: t(3000),
        retry_at: t(9000),
        ended_at: null,
      }).map((x) => [x.kind, x.at]),
    ).toEqual([
      ["created", 1000],
      ["lastFailure", 3000],
      ["retryAt", 9000],
    ]);
  });
});

// The `attempt` column counts from 0; readers count attempts from 1 ("Attempts: 1"
// right above the "Attempt 0" lane would be two numbers for the same thing).
describe("attempt numbers", () => {
  it("lanes show the attempt number counted from 1", () => {
    expect(attemptNumber(0)).toBe(1);
  });
});
