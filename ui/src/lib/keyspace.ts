// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Keyspace bar of the Cluster screen: each range is a segment on [0, 2⁶⁴), as wide
// as its key range. Keys are 16-digit hex — beyond the precision of `Number`, so the
// math is in BigInt. Pure — testable.

const SPACE = 1n << 64n;

export type RangeLike = { range: number; start: string; end: string; leader?: string | null };

export type Segment = {
  range: number;
  /** Position and width, as a percentage of the whole keyspace. */
  left: number;
  width: number;
  leader: string | null;
};

const key = (hex: string) => BigInt(`0x${hex}`);

export function segments(ranges: RangeLike[]): Segment[] {
  return [...ranges]
    .sort((a, b) => (key(a.start) < key(b.start) ? -1 : 1))
    .map((r) => {
      const from = key(r.start);
      // `end` is the last key **included** in the range.
      const size = key(r.end) - from + 1n;
      return {
        range: r.range,
        left: Number((from * 1_000_000n) / SPACE) / 10_000,
        width: Number((size * 1_000_000n) / SPACE) / 10_000,
        leader: r.leader ?? null,
      };
    });
}

/**
 * Colour by leader: a fixed order by sorted node name (colour follows the entity,
 * not the rank), at most 8 (`--chart-1..8`); the 9th node onwards and ranges with
 * no leader use a neutral colour.
 */
export function leaderColors(leaders: (string | null)[]): Map<string, string> {
  const names = [...new Set(leaders.filter((l): l is string => !!l))].sort();
  return new Map(
    names.map((n, i) => [n, i < 8 ? `var(--chart-${i + 1})` : "var(--muted-foreground)"]),
  );
}

const hex16 = (n: bigint) => n.toString(16).padStart(16, "0");

/**
 * Default split point of `POST /ranges/{id}/split` (no `?at=`): the first key of the
 * right half, `start + (end − start) / 2 + 1` — the same formula as the server's
 * `split_with`/`autosplit::midpoint`.
 */
export function midKey(start: string, end: string): string {
  const s = key(start);
  return hex16(s + (key(end) - s) / 2n + 1n);
}

/**
 * Split point typed by the user: must be 16-digit hex within `(start, end]` — the
 * server checks exactly this (`400`); checking first reports it immediately.
 * Empty ⇒ use the midpoint (`null`).
 */
export function splitKeyProblem(
  at: string,
  start: string,
  end: string,
): "format" | "outside" | null {
  const t = at.trim();
  if (t === "") return null;
  if (!/^[0-9a-fA-F]{16}$/.test(t)) return "format";
  const k = key(t);
  return key(start) < k && k <= key(end) ? null : "outside";
}

/** `node/<pid>/<inc>` ⇒ a short readable part. */
export function shortNode(node: string): string {
  const parts = node.split("/");
  return parts.length >= 3 ? `${parts[0]}/${parts[1]}` : node;
}
