// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { describe, expect, it } from "vitest";
import { adaptive, HISTORY_REFRESH, OVERVIEW_REFRESH } from "./hooks";

describe("adaptive", () => {
  it("clamps 10 × the statement time into [min, max]", () => {
    expect(adaptive(5, OVERVIEW_REFRESH)).toBe(10_000);
    expect(adaptive(2_000, OVERVIEW_REFRESH)).toBe(20_000);
    expect(adaptive(60_000, OVERVIEW_REFRESH)).toBe(60_000);
    expect(adaptive(undefined, false)).toBe(false);
  });

  // Hourly statements never refresh faster than the flush tick (60 s).
  it("hourly statements do not refresh faster than once a minute", () => {
    expect(adaptive(5, HISTORY_REFRESH)).toBe(60_000);
    expect(adaptive(undefined, HISTORY_REFRESH)).toBe(60_000);
    expect(adaptive(1_000_000, HISTORY_REFRESH)).toBe(300_000);
  });
});
