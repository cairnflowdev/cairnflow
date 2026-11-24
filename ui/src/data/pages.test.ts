// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { describe, expect, it } from "vitest";
import { type InvocationRow, nextCursor, page } from "./pages";

const row = (id: string, ms: number): InvocationRow => ({
  id,
  service: "S",
  handler: "h",
  key: "",
  class: "active",
  status: "running",
  created_at: new Date(ms).toISOString(),
  transitioned_at: new Date(ms).toISOString(),
  attempt: null,
  failure_message: null,
  last_failure_message: null,
});

describe("pagination by timestamp", () => {
  it("the next page drops ids already seen at exactly that timestamp, not other ids with the same timestamp", () => {
    const first = [row("a", 30), row("b", 20), row("c", 20)];
    const cur = nextCursor(first, "created", null, true);
    expect(cur).toEqual({ atMs: 20, seen: ["b", "c"] });
    // The server returns `<= 20`: b, c (already seen), d (same timestamp, new), e.
    const raw = [row("b", 20), row("c", 20), row("d", 20), row("e", 10)];
    expect(page(raw, "created", cur, 2).map((r) => r.id)).toEqual(["d", "e"]);
  });

  it("a timestamp spanning several pages accumulates the seen ids", () => {
    const cur = nextCursor([row("d", 20)], "created", { atMs: 20, seen: ["b", "c"] }, true);
    expect(cur).toEqual({ atMs: 20, seen: ["b", "c", "d"] });
  });

  it("a page that is not full ⇒ done", () => {
    expect(nextCursor([row("a", 1)], "created", null, false)).toBeNull();
  });
});
