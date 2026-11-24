// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Data hooks of the screens. The TanStack Query key is `[sql, params]`: two screens
// asking the same statement share the result. Adaptive refresh: 10 × the previous
// statement's time, clamped to [min, max] (as Restate's UI does), paused while the
// tab is hidden (TanStack Query's default).
import { type QueryKey, useQuery } from "@tanstack/react-query";
import type { Param, Query } from "./queries";
import { type Rows, run } from "./run";

export type Refresh = { minMs: number; maxMs: number } | false;

export const OVERVIEW_REFRESH: Refresh = { minMs: 10_000, maxMs: 60_000 };

/**
 * Hourly statements (`sys_handler_history`): figures are flushed every minute and by
 * hour, so refreshing faster than once a minute shows nothing new — it only costs
 * LISTs.
 */
export const HISTORY_REFRESH: Refresh = { minMs: 60_000, maxMs: 300_000 };

export function adaptive(lastMs: number | undefined, r: Refresh): number | false {
  if (r === false) return false;
  return Math.min(r.maxMs, Math.max(r.minMs, 10 * (lastMs ?? 0)));
}

export function rowsKey(q: Query<Param[]>): QueryKey {
  return ["query", q.sql, q.params];
}

export function useRows<T>(q: Query<Param[]>, opts: { refresh?: Refresh; enabled?: boolean } = {}) {
  const refresh = opts.refresh ?? false;
  return useQuery<Rows<T>>({
    queryKey: rowsKey(q),
    queryFn: ({ signal }) => run<T>(q, signal),
    enabled: opts.enabled ?? true,
    refetchInterval: (query) => adaptive(query.state.data?.ms, refresh),
    retry: (n, e) =>
      n < 1 && !(e instanceof Error && "status" in e && (e as { status: number }).status < 500),
  });
}
