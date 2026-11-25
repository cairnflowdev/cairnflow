// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { describe, expect, it } from "vitest";
import {
  type AuditRecord,
  actorLabel,
  entries,
  outcomeTone,
  rangeBounds,
  targetText,
} from "./audit";

const rec = (r: Partial<AuditRecord> & Pick<AuditRecord, "name" | "kind" | "ts">): AuditRecord => ({
  tenant: "acme",
  action: "x",
  outcome: "ok",
  ...r,
});

describe("entries", () => {
  it("pairs intent/done with the same root, an intent without an outcome is pending, newest first", () => {
    const out = entries([
      rec({ name: "1-n-b-0.done", kind: "done", ts: 12, outcome: "ok", status: 204 }),
      rec({ name: "r-00ff.json", kind: "row", ts: 5, action: "invocation.cancel" }),
      rec({
        name: "1-n-b-0.intent",
        kind: "intent",
        ts: 10,
        outcome: "pending",
        action: "key.revoke",
        target: { id: "k_1" },
      }),
      rec({ name: "2-n-b-1.intent", kind: "intent", ts: 20, outcome: "pending" }),
      rec({ name: "3-n-b-2.denied", kind: "denied", ts: 1, outcome: "denied", count: 7 }),
    ]);
    expect(out.map((e) => [e.key, e.source, e.outcome])).toEqual([
      ["2-n-b-1", "object", "pending"],
      ["1-n-b-0", "object", "ok"],
      ["r-00ff.json", "row", "ok"],
      ["3-n-b-2.denied", "denied", "denied"],
    ]);
    expect(out[1]).toMatchObject({ ts: 10, status: 204, action: "key.revoke" });
    expect(out[3]?.count).toBe(7);
  });

  // No-op outcomes aggregated per minute: a `….noop` object carries `count`, like
  // `denied` — a row with its own source, not lumped into "other". The Playwright row
  // `responsive.spec.ts › /audit` is red too while `kind: "noop"` is missing from openapi.json.
  it("an aggregated noop record is one row with source noop, keeping count", () => {
    const out = entries([
      rec({
        name: "4-n-c-3.noop",
        kind: "noop" as AuditRecord["kind"],
        ts: 2,
        action: "invocation.cancel",
        outcome: "not_found",
        count: 283,
      }),
    ]);
    expect(out.map((e) => [e.key, e.source, e.outcome, e.count])).toEqual([
      ["4-n-c-3.noop", "noop", "not_found", 283],
    ]);
  });

  it("only a done (the intent is outside the range) still becomes a row", () => {
    const out = entries([rec({ name: "9.done", kind: "done", ts: 3, outcome: "error" })]);
    expect(out).toHaveLength(1);
    expect(out[0]?.outcome).toBe("error");
  });
});

describe("text", () => {
  it("actor: SSO user's email, key id", () => {
    expect(actorLabel({ type: "user", id: "sub-1", email: "ada@example.com" })).toBe(
      "ada@example.com",
    );
    expect(actorLabel({ type: "user", id: "sub-1" })).toBe("sub-1");
    expect(actorLabel({ type: "key", id: "k_abc" })).toBe("k_abc");
    expect(actorLabel({})).toBe("—");
  });

  it("target", () => {
    expect(targetText({ id: "abc", gone: null })).toBe("id=abc");
    expect(targetText(["a", "b"])).toBe("a, b");
    expect(targetText("x")).toBe("x");
    expect(targetText(undefined)).toBe("");
    expect(targetText({ ids: ["a"] })).toBe('ids=["a"]');
  });

  it("outcome tone", () => {
    expect(outcomeTone("ok")).toBe("ok");
    expect(outcomeTone("pending")).toBe("pending");
    expect(outcomeTone("denied")).toBe("bad");
    expect(outcomeTone("noop")).toBe("neutral");
  });

  it("time range", () => {
    expect(rangeBounds("24h", 100_000_000)).toEqual({ from: 13_600_000, to: 100_000_000 });
  });
});
