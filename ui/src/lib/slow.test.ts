// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { describe, expect, it } from "vitest";
import { mostly } from "./slow";

const run = (
  p: Partial<Record<"runMs" | "sdkMs" | "durableWaitMs" | "engineWaitMs" | "permitWaitMs", number>>,
) => ({
  atMs: 1,
  tenant: "t",
  service: "S",
  handler: "h",
  deployment: "dp_x",
  invocation: "0".repeat(48),
  outcome: "completed",
  totalMs: 0,
  thresholdMs: 0,
  turns: 1,
  steps: 1,
  runMs: 0,
  sdkMs: 0,
  durableWaits: 0,
  durableWaitMs: 0,
  engineWaitMs: 0,
  permitWaitMs: 0,
  ...p,
});

describe("mostly", () => {
  it("largest part", () => {
    expect(mostly(run({ runMs: 10, durableWaitMs: 900, sdkMs: 50 }))).toEqual({
      label: "durable waits",
      ms: 900,
    });
    expect(mostly(run({ permitWaitMs: 5 })).ms).toBe(5);
  });
});
