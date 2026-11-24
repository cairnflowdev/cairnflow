// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { describe, expect, it } from "vitest";
import { leaderColors, midKey, segments, shortNode, splitKeyProblem } from "./keyspace";

describe("segments", () => {
  it("one range covers everything; two halves; sorted by start key", () => {
    expect(segments([{ range: 0, start: "0000000000000000", end: "ffffffffffffffff" }])).toEqual([
      { range: 0, left: 0, width: 100, leader: null },
    ]);
    const s = segments([
      { range: 2, start: "8000000000000000", end: "ffffffffffffffff", leader: "b" },
      { range: 1, start: "0000000000000000", end: "7fffffffffffffff", leader: "a" },
    ]);
    expect(s.map((x) => [x.range, x.left, x.width])).toEqual([
      [1, 0, 50],
      [2, 50, 50],
    ]);
  });

  it("high keys keep their precision; a range < 10⁻⁶ of the space ⇒ width 0 (drawn at least 2 px)", () => {
    const [x] = segments([{ range: 9, start: "fffffffffffff000", end: "ffffffffffffffff" }]);
    expect(x?.left).toBe(99.9999);
    expect(x?.width).toBe(0);
  });
});

describe("leaderColors", () => {
  it("fixed order by name, ≤ 8 colours, the rest neutral", () => {
    const names = Array.from({ length: 9 }, (_, i) => `n${i}`);
    const m = leaderColors(["n8", null, ...names]);
    expect(m.get("n0")).toBe("var(--chart-1)");
    expect(m.get("n7")).toBe("var(--chart-8)");
    expect(m.get("n8")).toBe("var(--muted-foreground)");
  });
});

describe("split point", () => {
  it("midKey matches the server's formula: start + (end − start) / 2 + 1", () => {
    expect(midKey("0000000000000000", "ffffffffffffffff")).toBe("8000000000000000");
    expect(midKey("0000000000000000", "3fffffffffffffff")).toBe("2000000000000000");
    expect(midKey("0000000000000005", "0000000000000006")).toBe("0000000000000006");
  });

  it("splitKeyProblem: empty = midpoint; 16 hex; within (start, end]", () => {
    const [s, e] = ["4000000000000000", "7fffffffffffffff"];
    expect(splitKeyProblem("", s, e)).toBeNull();
    expect(splitKeyProblem("  ", s, e)).toBeNull();
    expect(splitKeyProblem("5000000000000000", s, e)).toBeNull();
    expect(splitKeyProblem("7FFFFFFFFFFFFFFF", s, e)).toBeNull();
    expect(splitKeyProblem("4000000000000000", s, e)).toBe("outside");
    expect(splitKeyProblem("8000000000000000", s, e)).toBe("outside");
    expect(splitKeyProblem("500000000000000", s, e)).toBe("format");
    expect(splitKeyProblem("50000000000000zz", s, e)).toBe("format");
  });
});

it("shortNode", () => {
  expect(shortNode("node/1348379/1793220928824000032")).toBe("node/1348379");
  expect(shortNode("a")).toBe("a");
});
