// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { resetForTest, run } from "./run";
import {
  groupBySql,
  type QueryEvent,
  record,
  resetStatsForTest,
  STATS_MAX,
  statsSnapshot,
} from "./stats";

const ev = (sql: string, extra: Partial<QueryEvent> = {}): QueryEvent => ({
  at: 1,
  sql,
  questions: [1],
  status: 200,
  wallMs: 10,
  ms: 4,
  read: 2,
  rows: 1,
  partial: false,
  ...extra,
});

describe("Query stats", () => {
  beforeEach(() => {
    resetStatsForTest();
    resetForTest();
  });
  afterEach(() => vi.unstubAllGlobals());

  it("groups by sql: count, errors, partial, read, ms; sorted by total read", () => {
    const g = groupBySql([
      ev("a", { ms: 4, read: 2 }),
      ev("a", { ms: 8, read: 3, partial: true, at: 5 }),
      ev("a", { status: 429, ms: undefined, read: undefined, error: "busy" }),
      ev("b", { read: 100, questions: [10, 11] }),
    ]);
    expect(g.map((x) => x.sql)).toEqual(["b", "a"]);
    expect(g[1]).toMatchObject({
      count: 3,
      errors: 1,
      partial: 1,
      read: 5,
      avgMs: 6,
      maxMs: 8,
      last: 5,
    });
    expect(g[0]?.questions).toEqual([10, 11]);
  });

  it("the ring buffer keeps the last STATS_MAX statements", () => {
    for (let i = 0; i < STATS_MAX + 5; i++) record(ev(`q${i}`));
    const s = statsSnapshot();
    expect(s).toHaveLength(STATS_MAX);
    expect(s[0]?.sql).toBe("q5");
  });

  it("run() records successful and failed statements (each 429 attempt is a row)", async () => {
    let n = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => {
        n++;
        if (n === 1) {
          return new Response(JSON.stringify({ error: "busy" }), {
            status: 429,
            headers: { "content-type": "application/json", "retry-after": "0" },
          });
        }
        return new Response(
          JSON.stringify({
            columns: [{ name: "n", type: "Int64" }],
            rows: [[1]],
            partial: true,
            warnings: [],
            read: 7,
            ms: 3,
          }),
          { headers: { "content-type": "application/json" } },
        );
      }),
    );
    await run({ questions: [33], sql: "SELECT 1", params: [] });
    expect(statsSnapshot().map((e) => [e.status, e.error ?? null])).toEqual([
      [429, "busy"],
      [200, null],
    ]);
    expect(groupBySql(statsSnapshot())[0]).toMatchObject({
      sql: "SELECT 1",
      count: 2,
      errors: 1,
      partial: 1,
      read: 7,
      questions: [33],
    });
  });
});
