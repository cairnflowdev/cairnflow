// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { describe, expect, it } from "vitest";
import { hourKey, hourKeyMs } from "./hours";
import { deploymentState } from "./manifest-diff";
import { byModel, costLabel, formatMs, type MeterHour } from "./usage";

const hour = (h: Partial<MeterHour>): MeterHour => ({
  hour: "2026-10-29T10",
  invocations: 0,
  steps: 0,
  runMs: 0,
  inputTokens: 0,
  outputTokens: 0,
  costMicros: 0,
  models: {},
  ...h,
});

describe("byModel", () => {
  it("sums per model across hours, the unknown-model part is a null row at the end", () => {
    const rows = byModel([
      hour({
        inputTokens: 100,
        outputTokens: 10,
        costMicros: 50,
        models: { a: { inputTokens: 60, outputTokens: 6, costMicros: 30 } },
      }),
      hour({
        hour: "2026-10-29T11",
        inputTokens: 300,
        outputTokens: 30,
        costMicros: 0,
        models: {
          a: { inputTokens: 40, outputTokens: 4, costMicros: 20 },
          b: { inputTokens: 260, outputTokens: 26, costMicros: 0 },
        },
      }),
    ]);
    expect(rows.map((r) => r.model)).toEqual(["b", "a", null]);
    expect(rows[1]).toEqual({ model: "a", input: 100, output: 10, cost: 50 });
    expect(rows[2]).toEqual({ model: null, input: 40, output: 4, cost: 20 });
  });

  it("no remainder ⇒ no null row", () => {
    const rows = byModel([
      hour({ inputTokens: 5, models: { a: { inputTokens: 5, outputTokens: 0, costMicros: 0 } } }),
    ]);
    expect(rows.map((r) => r.model)).toEqual(["a"]);
  });
});

describe("hourKey", () => {
  it("round trip with the hour format of /metering/hours", () => {
    const ms = Date.UTC(2026, 9, 29, 7, 42);
    expect(hourKey(ms)).toBe("2026-10-29T07");
    expect(hourKeyMs("2026-10-29T07")).toBe(Date.UTC(2026, 9, 29, 7));
  });
});

// Formats and statuses.

describe("formats and statuses", () => {
  it("one duration format on every screen", () => {
    expect(formatMs(4416)).toBe("4.4 s");
    expect(formatMs(60)).toBe("60 ms");
  });

  it("no price table ⇒ does not print $0.00", () => {
    expect(costLabel(0, false)).toBeNull();
    expect(costLabel(1_500_000, true)).toBe("$1.50");
  });

  it("deployment status: current / draining / removable", () => {
    expect(deploymentState({ currentFor: ["Shop"], pinnedInvocations: 3 })).toEqual({
      kind: "current",
    });
    expect(deploymentState({ currentFor: [], pinnedInvocations: 2 })).toEqual({
      kind: "draining",
      pinned: 2,
    });
    expect(deploymentState({ currentFor: [], pinnedInvocations: 0 })).toEqual({
      kind: "removable",
    });
    expect(deploymentState({ currentFor: [], pinnedInvocations: null })).toEqual({
      kind: "unknown",
    });
  });
});
