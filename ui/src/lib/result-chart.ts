// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Chart shape for `/query` results of the SQL console. Chosen by what the data is
// for: a time column + numeric columns ⇒ lines (one axis, ≤ 4 series — categorical
// colours in a fixed order); one text column + a numeric column ⇒ single-colour
// horizontal bars; anything else ⇒ no chart (the table is enough). Pure — testable.

export type Col = { name: string; type: string };

/** Maximum number of series on a line chart — `--chart-1..4`, directly labelled. */
export const MAX_SERIES = 4;
/** Maximum number of horizontal bars — beyond that a table reads better. */
export const MAX_BARS = 30;

export type Shape =
  | { kind: "line"; x: number; ys: number[]; dropped: number[] }
  /** `label`: all text columns, joined into a label (`Shop / decline`). */
  | { kind: "bar"; label: number[]; values: number[] }
  | null;

const NUMERIC = /^(U?Int\d+|Float\d+|Decimal\d+)/;
const ISO = /^\d{4}-\d{2}-\d{2}([T ]\d{2}(:\d{2})?)?/;
/** Epoch ms after 2001 — tells `hour_ms`, `at_ms` apart from durations such as `duration_ms`. */
const EPOCH_MS_FLOOR = 1e12;

/**
 * Time string ⇒ ms. Also accepts `yyyy-mm-ddThh` (the hour of `sys_metering`, UTC),
 * which `Date.parse` does not.
 */
export function parseTime(s: string): number | null {
  if (!ISO.test(s)) return null;
  const t = Date.parse(/^\d{4}-\d{2}-\d{2}T\d{2}$/.test(s) ? `${s}:00:00Z` : s);
  return Number.isNaN(t) ? null : t;
}

function values(rows: unknown[][], i: number): unknown[] {
  return rows.map((r) => r[i]).filter((v) => v !== null && v !== undefined);
}

export function isNumeric(c: Col, rows: unknown[][], i: number): boolean {
  return NUMERIC.test(c.type) && values(rows, i).every((v) => typeof v === "number");
}

export function isTime(c: Col, rows: unknown[][], i: number): boolean {
  const vs = values(rows, i);
  if (vs.length === 0) return false;
  if (c.type.startsWith("Timestamp") || c.type.startsWith("Date")) return true;
  if (c.name.endsWith("_ms") && isNumeric(c, rows, i))
    return vs.every((v) => (v as number) >= EPOCH_MS_FLOOR);
  return c.type === "Utf8" && vs.every((v) => typeof v === "string" && parseTime(v) !== null);
}

/** A cell's time value ⇒ Unix seconds (uPlot uses seconds). */
export function toSeconds(v: unknown): number | null {
  if (typeof v === "number") return v / 1000;
  if (typeof v === "string") {
    const t = parseTime(v);
    return t === null ? null : t / 1000;
  }
  return null;
}

export function chartShape(columns: Col[], rows: unknown[][]): Shape {
  if (rows.length === 0) return null;
  const idx = columns.map((_, i) => i);
  const time = idx.find((i) => isTime(columns[i] as Col, rows, i));
  const nums = idx.filter(
    (i) =>
      i !== time && isNumeric(columns[i] as Col, rows, i) && !isTime(columns[i] as Col, rows, i),
  );
  if (nums.length === 0) return null;
  if (time !== undefined) {
    if (rows.length < 2) return null;
    return {
      kind: "line",
      x: time,
      ys: nums.slice(0, MAX_SERIES),
      dropped: nums.slice(MAX_SERIES),
    };
  }
  const label = idx.filter((i) => !nums.includes(i));
  if (label.length === 0) return null;
  return { kind: "bar", label, values: nums };
}
