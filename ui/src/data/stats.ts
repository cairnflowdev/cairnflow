// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// "Query stats" (an idea borrowed from Restate's UI): every `/query` statement the
// console sent during the session — the server's `ms`/`read`, wall time on the
// browser side, `partial`, errors. Operators see how much the UI costs. Only in the
// tab's memory (ring buffer `STATS_MAX`), never stored.
import { useSyncExternalStore } from "react";

export const STATS_MAX = 500;

export type QueryEvent = {
  at: number;
  sql: string;
  questions: number[];
  /** `200`, or the HTTP error code (each `429` retry is a row too). */
  status: number;
  /** Wall time on the browser side, including queueing (≤ 2 statements in flight). */
  wallMs: number;
  ms?: number;
  read?: number;
  rows?: number;
  partial?: boolean;
  error?: string;
};

let events: QueryEvent[] = [];
const listeners = new Set<() => void>();

export function record(e: QueryEvent) {
  events = [...events.slice(-(STATS_MAX - 1)), e];
  for (const l of listeners) l();
}

export function clearStats() {
  events = [];
  for (const l of listeners) l();
}

export function useQueryEvents(): QueryEvent[] {
  return useSyncExternalStore(
    (l) => {
      listeners.add(l);
      return () => listeners.delete(l);
    },
    () => events,
  );
}

export type QueryGroup = {
  sql: string;
  questions: number[];
  count: number;
  errors: number;
  partial: number;
  read: number;
  avgMs: number;
  maxMs: number;
  avgWallMs: number;
  last: number;
};

/** Grouped by SQL statement (bind parameters are not in `sql`), sorted by total `read`. */
export function groupBySql(list: QueryEvent[]): QueryGroup[] {
  const by = new Map<string, QueryEvent[]>();
  for (const e of list) by.set(e.sql, [...(by.get(e.sql) ?? []), e]);
  return [...by.entries()]
    .map(([sql, es]) => {
      const ok = es.filter((e) => e.status === 200);
      const ms = ok.map((e) => e.ms ?? 0);
      return {
        sql,
        questions: [...new Set(es.flatMap((e) => e.questions))].sort((a, b) => a - b),
        count: es.length,
        errors: es.length - ok.length,
        partial: ok.filter((e) => e.partial).length,
        read: ok.reduce((s, e) => s + (e.read ?? 0), 0),
        avgMs: ms.length ? Math.round(ms.reduce((s, x) => s + x, 0) / ms.length) : 0,
        maxMs: ms.length ? Math.max(...ms) : 0,
        avgWallMs: Math.round(es.reduce((s, e) => s + e.wallMs, 0) / es.length),
        last: Math.max(...es.map((e) => e.at)),
      };
    })
    .sort((a, b) => b.read - a.read || b.count - a.count);
}

/** The current ring buffer (outside React). */
export function statsSnapshot(): QueryEvent[] {
  return events;
}

/** Tests only. */
export function resetStatsForTest() {
  events = [];
  listeners.clear();
}
