// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { describe, expect, it } from "vitest";
import { describeCron, describeEvery, weekdayName } from "./cron";

const en = weekdayName("en");

describe("describeCron", () => {
  it("common shapes", () => {
    expect(describeCron("* * * * *", en)).toBe("Every minute");
    expect(describeCron("*/15 * * * *", en)).toBe("Every 15 minutes");
    expect(describeCron("5 * * * *", en)).toBe("Every hour at minute 05");
    expect(describeCron("0 */6 * * *", en)).toBe("Every 6 hours at minute 00");
    expect(describeCron("30 2 * * *", en)).toBe("Every day at 02:30");
    expect(describeCron("0 9 * * 1-5", en)).toBe("Weekdays at 09:00");
    expect(describeCron("0 9 * * sat,sun", en)).toBe("At 09:00 on Sat, Sun");
  });

  it("shapes it cannot describe ⇒ null, no guessing", () => {
    expect(describeCron("0 0 1 * *", en)).toBeNull();
    expect(describeCron("0 0 * * 9", en)).toBeNull();
    expect(describeCron("bad", en)).toBeNull();
    expect(describeCron("61 * * * *", en)).toBeNull();
  });
});

describe("describeEvery", () => {
  it("period in the largest unit that divides evenly", () => {
    expect(describeEvery(60_000)).toBe("Every minute");
    expect(describeEvery(300_000)).toBe("Every 5 min");
    expect(describeEvery(3_600_000)).toBe("Every hour");
    expect(describeEvery(7_200_000)).toBe("Every 2 h");
    expect(describeEvery(90_000)).toBe("Every 90 s");
    expect(describeEvery(1_500)).toBe("Every 1.5 s");
  });
});
