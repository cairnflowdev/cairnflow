// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { describe, expect, it } from "vitest";
import { bytesLabel, rateLabel, retentionLabel } from "./limits";

describe("limit labels", () => {
  it("retention: absent, forever, days, hours, minutes, seconds", () => {
    expect(retentionLabel(null, "en")).toBe("No limit");
    expect(retentionLabel(0, "en")).toBe("Forever");
    expect(retentionLabel(7 * 86_400_000, "en")).toBe("7 days");
    expect(retentionLabel(36 * 3_600_000, "en")).toBe("36 hours");
    expect(retentionLabel(90 * 60_000, "en")).toBe("90 minutes");
    expect(retentionLabel(1500, "en")).toBe("1.5 seconds");
  });
  it("bytes in exact multiples of 1024", () => {
    expect(bytesLabel(null, "en")).toBe("No limit");
    expect(bytesLabel(4 * 1024 * 1024, "en")).toBe("4 MiB");
    expect(bytesLabel(1536, "en")).toBe("1,536 B");
    expect(bytesLabel(2048, "en")).toBe("2 KiB");
    expect(bytesLabel(1000, "en")).toBe("1,000 B");
  });
  it("registration rate", () => {
    expect(rateLabel(null)).toBe("No limit");
    expect(rateLabel("10/60")).toBe("10 per 60 s");
    expect(rateLabel("odd")).toBe("odd");
  });
});
