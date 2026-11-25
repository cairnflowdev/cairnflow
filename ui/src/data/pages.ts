// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// List pagination by timestamp: `/query` has no cursors. The next page reads `<= the
// last row's timestamp` (or `>=` for "held up") and drops the ids already seen at
// exactly that timestamp. Each page asks for extra rows to cover ties so it still fills `size`.
import { useInfiniteQuery } from "@tanstack/react-query";
import { type InvocationFilter, type InvocationOrder, invocationList } from "./queries";
import { run, type Warning } from "./run";

export type InvocationRow = {
  id: string;
  service: string;
  handler: string;
  key: string;
  class: string;
  status: string | null;
  created_at: string;
  transitioned_at: string;
  attempt: number | null;
  /** Final error (class `failed`); `last_failure_message` is the latest attempt's error. */
  failure_message: string | null;
  last_failure_message: string | null;
};

export type Cursor = { atMs: number; seen: string[] };

export type Page = {
  rows: InvocationRow[];
  next: Cursor | null;
  partial: boolean;
  warnings: Warning[];
};

export const PAGE_SIZE = 50;

const at = (r: InvocationRow, order: InvocationOrder) =>
  Date.parse(order === "transitioned" ? r.transitioned_at : r.created_at);

/** One page from the raw result: drops ids already seen at the cursor timestamp, computes the next cursor. */
export function page(
  raw: InvocationRow[],
  order: InvocationOrder,
  cursor: Cursor | null,
  size: number,
): Page["rows"] {
  const seen = new Set(cursor?.seen ?? []);
  return raw
    .filter((r) => !(cursor && at(r, order) === cursor.atMs && seen.has(r.id)))
    .slice(0, size);
}

export function nextCursor(
  rows: InvocationRow[],
  order: InvocationOrder,
  prev: Cursor | null,
  full: boolean,
): Cursor | null {
  const last = rows.at(-1);
  if (!full || !last) return null;
  const atMs = at(last, order);
  const tied = rows.filter((r) => at(r, order) === atMs).map((r) => r.id);
  return { atMs, seen: prev && prev.atMs === atMs ? [...prev.seen, ...tied] : tied };
}

export function useInvocationPages(filter: InvocationFilter, order: InvocationOrder) {
  return useInfiniteQuery({
    queryKey: ["invocations", filter, order],
    initialPageParam: null as Cursor | null,
    queryFn: async ({ pageParam, signal }): Promise<Page> => {
      const extra = pageParam?.seen.length ?? 0;
      const out = await run<InvocationRow>(
        invocationList(filter, order, PAGE_SIZE + extra, pageParam?.atMs),
        signal,
      );
      const rows = page(out.rows, order, pageParam, PAGE_SIZE);
      return {
        rows,
        next: nextCursor(rows, order, pageParam, rows.length === PAGE_SIZE),
        partial: out.partial,
        warnings: out.warnings,
      };
    },
    getNextPageParam: (last) => last.next,
  });
}
