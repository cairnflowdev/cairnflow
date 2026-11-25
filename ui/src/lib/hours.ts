// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Hour axis of the Usage chart. `sys_handler_history` only has hours with activity;
// empty hours must be a real 0 on the axis, otherwise the line runs straight across
// the gap as if there were steady activity.

export const HOUR_MS = 3_600_000;

/** Start of the hour ≤ `ms`. */
export const hourFloor = (ms: number) => Math.floor(ms / HOUR_MS) * HOUR_MS;

/**
 * One row per hour from `fromMs` to `toMs` (both ends included, by start of hour); an
 * hour missing from `rows` ⇒ every `keys` = 0.
 */
export function fillHours<K extends string>(
  rows: ({ hour_ms: number } & Record<K, number | null>)[],
  keys: K[],
  fromMs: number,
  toMs: number,
): ({ hour_ms: number } & Record<K, number>)[] {
  const by = new Map(rows.map((r) => [r.hour_ms, r]));
  const out: ({ hour_ms: number } & Record<K, number>)[] = [];
  for (let h = hourFloor(fromMs); h <= hourFloor(toMs); h += HOUR_MS) {
    const r = by.get(h);
    const row = { hour_ms: h } as { hour_ms: number } & Record<K, number>;
    for (const k of keys) (row as Record<K, number>)[k] = Number(r?.[k] ?? 0);
    out.push(row);
  }
  return out;
}

/** `yyyy-mm-ddThh` (UTC) of a timestamp — the hour format of `/metering/hours`. */
export function hourKey(ms: number): string {
  return new Date(hourFloor(ms)).toISOString().slice(0, 13);
}

/** Timestamp in ms (UTC) of `yyyy-mm-ddThh`; `NaN` when malformed. */
export function hourKeyMs(key: string): number {
  return Date.parse(`${key}:00:00Z`);
}
