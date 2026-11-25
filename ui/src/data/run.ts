// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Runs the console's `/query` statements: at most 2 statements in flight per tab (the
// server returns 429 at 4 per node), `429` ⇒ back off by `Retry-After` then retry, at
// most 3 times. Results keep `partial`/`warnings` — the screen shows a banner, never hides it.
import { ApiError, ok, rest } from "./api";
import type { Param, Query } from "./queries";
import { record } from "./stats";

export type Warning = { range: number | null; reason: string };

export type Rows<T> = {
  rows: T[];
  /** Columns (name, Arrow type) and rows as raw arrays — the SQL console draws its table from these. */
  columns: { name: string; type: string }[];
  raw: unknown[][];
  partial: boolean;
  warnings: Warning[];
  read: number;
  ms: number;
};

const MAX_IN_FLIGHT = 2;
const MAX_RETRIES = 3;

let inFlight = 0;
const waiting: (() => void)[] = [];

async function slot<T>(f: () => Promise<T>): Promise<T> {
  if (inFlight >= MAX_IN_FLIGHT) await new Promise<void>((r) => waiting.push(r));
  inFlight++;
  try {
    return await f();
  } finally {
    inFlight--;
    waiting.shift()?.();
  }
}

const sleep = (ms: number, signal?: AbortSignal) =>
  new Promise<void>((resolve, reject) => {
    const t = setTimeout(resolve, ms);
    signal?.addEventListener("abort", () => {
      clearTimeout(t);
      reject(signal.reason);
    });
  });

/** Turns array `rows` into objects keyed by column name. */
export function objects<T>(columns: { name: string }[], rows: unknown[][]): T[] {
  return rows.map((r) => Object.fromEntries(columns.map((c, i) => [c.name, r[i]])) as T);
}

export async function run<T>(query: Query<Param[]>, signal?: AbortSignal): Promise<Rows<T>> {
  for (let attempt = 0; ; attempt++) {
    const t0 = performance.now();
    const base = { at: Date.now(), sql: query.sql, questions: query.questions };
    try {
      const out = await slot(() =>
        ok(
          rest.POST("/query", {
            body: { sql: query.sql, params: query.params, allow_partial: true },
            signal,
          }),
        ),
      );
      record({
        ...base,
        status: 200,
        wallMs: Math.round(performance.now() - t0),
        ms: out.ms,
        read: out.read,
        rows: out.rows.length,
        partial: out.partial,
      });
      return {
        rows: objects<T>(out.columns, out.rows as unknown[][]),
        columns: out.columns,
        raw: out.rows as unknown[][],
        partial: out.partial,
        warnings: out.warnings as Warning[],
        read: out.read,
        ms: out.ms,
      };
    } catch (e) {
      // A cancelled statement (left the screen) is not a server cost — not recorded.
      if (e instanceof ApiError) {
        record({
          ...base,
          status: e.status,
          wallMs: Math.round(performance.now() - t0),
          error: e.message,
        });
      }
      if (e instanceof ApiError && e.status === 429 && attempt < MAX_RETRIES) {
        await sleep(e.retryAfterMs ?? 1000 * 2 ** attempt, signal);
        continue;
      }
      throw e;
    }
  }
}

/** Tests only: resets the queue. */
export function resetForTest() {
  inFlight = 0;
  waiting.length = 0;
}
