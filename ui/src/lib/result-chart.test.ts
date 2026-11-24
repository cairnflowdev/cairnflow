// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { describe, expect, it } from "vitest";
import { toCsv } from "./csv";
import { type Col, chartShape, toSeconds } from "./result-chart";

const c = (name: string, type: string): Col => ({ name, type });
const TS = 'Timestamp(Millisecond, Some("UTC"))';

describe("chartShape", () => {
  it("time + numbers ⇒ lines; > 4 series drops the rest (and reports it)", () => {
    const cols = [c("hour_ms", "Int64"), ...["a", "b", "c", "d", "e"].map((n) => c(n, "UInt64"))];
    const rows = [
      [1_760_000_000_000, 1, 2, 3, 4, 5],
      [1_760_003_600_000, 2, 3, 4, 5, 6],
    ];
    expect(chartShape(cols, rows)).toEqual({ kind: "line", x: 0, ys: [1, 2, 3, 4], dropped: [5] });
  });

  it("Arrow timestamps and ISO strings are time; duration_ms is not", () => {
    expect(
      chartShape(
        [c("created_at", TS), c("n", "Int64")],
        [
          ["2026-10-29T10:00:00Z", 1],
          ["2026-10-29T11:00:00Z", 2],
        ],
      )?.kind,
    ).toBe("line");
    expect(
      chartShape(
        [c("hour", "Utf8"), c("n", "Int64")],
        [
          ["2026-10-29T10", 1],
          ["2026-10-29T11", 2],
        ],
      )?.kind,
    ).toBe("line");
    // duration_ms is a number, not a time axis ⇒ horizontal bars by service.
    expect(chartShape([c("service", "Utf8"), c("duration_ms", "Int64")], [["Shop", 1200]])).toEqual(
      {
        kind: "bar",
        label: [0],
        values: [1],
      },
    );
  });

  it("text + number ⇒ horizontal bars; decimal is a number", () => {
    const cols = [c("service", "Utf8"), c("class", "Utf8"), c("n", "Decimal128(30, 0)")];
    expect(chartShape(cols, [["Shop", "failed", 3]])).toEqual({
      kind: "bar",
      label: [0, 1],
      values: [2],
    });
  });

  it("no chart: empty, no numeric column, a one-point line, numbers only", () => {
    expect(chartShape([c("n", "Int64")], [])).toBeNull();
    expect(chartShape([c("id", "Utf8")], [["x"]])).toBeNull();
    expect(chartShape([c("at", TS), c("n", "Int64")], [["2026-10-29T10:00:00Z", 1]])).toBeNull();
    expect(chartShape([c("a", "Int64"), c("b", "Int64")], [[1, 2]])).toBeNull();
  });

  it("toSeconds", () => {
    expect(toSeconds(1_760_000_000_000)).toBe(1_760_000_000);
    expect(toSeconds("2026-10-29T00:00:00Z")).toBe(Date.parse("2026-10-29T00:00:00Z") / 1000);
    expect(toSeconds("2026-10-29T10")).toBe(Date.parse("2026-10-29T10:00:00Z") / 1000);
    expect(toSeconds("nope")).toBeNull();
  });
});

describe("toCsv", () => {
  it("quotes when needed, null empty, objects as JSON", () => {
    expect(
      toCsv(
        ["a", "b", "c"],
        [
          ["x,y", null, { k: 1 }],
          ['say "hi"', 2, true],
        ],
      ),
    ).toBe('a,b,c\n"x,y",,"{""k"":1}"\n"say ""hi""",2,true');
  });

  // A spreadsheet runs a cell that starts with `=`, `+`, `-` or `@` as a formula, and the
  // strings in these files come from tenants' data.
  it("prefixes a string cell a spreadsheet would run as a formula", () => {
    expect(
      toCsv(
        ["a"],
        [
          ['=HYPERLINK("https://x.example","open")'],
          ["+1"],
          ["-1"],
          ["@SUM(A1)"],
          ["\tx"],
          ["\rx"],
        ],
      ),
    ).toBe(
      'a\n"\'=HYPERLINK(""https://x.example"",""open"")"\n\'+1\n\'-1\n\'@SUM(A1)\n\'\tx\n"\'\rx"',
    );
    // Numbers are data, not text: a negative number stays a number.
    expect(toCsv(["n"], [[-5], [1.5], [-2n]])).toBe("n\n-5\n1.5\n-2");
    // The header is under the same rule (query column aliases are free text).
    expect(toCsv(["=x"], [])).toBe("'=x");
  });
});
