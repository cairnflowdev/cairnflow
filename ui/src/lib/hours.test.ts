// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { describe, expect, it } from "vitest";
import { fillHours, HOUR_MS, hourFloor } from "./hours";

describe("fillHours", () => {
  it("one row per hour, empty hours = 0, existing values kept", () => {
    const t0 = hourFloor(1_760_000_000_000);
    const rows = [
      { hour_ms: t0, completed: 2, failed: null },
      { hour_ms: t0 + 2 * HOUR_MS, completed: 5, failed: 1 },
    ];
    expect(fillHours(rows, ["completed", "failed"], t0 + 123, t0 + 2 * HOUR_MS + 5)).toEqual([
      { hour_ms: t0, completed: 2, failed: 0 },
      { hour_ms: t0 + HOUR_MS, completed: 0, failed: 0 },
      { hour_ms: t0 + 2 * HOUR_MS, completed: 5, failed: 1 },
    ]);
  });

  it("30 days ⇒ 721 hours", () => {
    const now = 1_760_000_000_000;
    expect(fillHours([], ["cu"], now - 720 * HOUR_MS, now)).toHaveLength(721);
  });
});
