// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { describe, expect, it } from "vitest";
import {
  blockingCounter,
  type LimitRow,
  type QueueEntry,
  queueFacts,
  queueOf,
  type VQueueRow,
} from "./queue";

const e = (
  id: string,
  stage: string,
  seq: number,
  extra: Partial<QueueEntry> = {},
): QueueEntry => ({
  id,
  stage,
  seq,
  scope: null,
  limit_key: null,
  range: 0,
  ...extra,
});

describe("queueFacts", () => {
  it("waiting: the lock holder, position by seq, total waiting", () => {
    const rows = [e("c", "inbox", 7), e("a", "suspended", 3), e("b", "inbox", 5)];
    expect(queueFacts(rows, "c")).toEqual({
      mine: rows[0],
      holders: [rows[1]],
      ahead: 1,
      waiting: 2,
    });
    expect(queueFacts(rows, "b")?.ahead).toBe(0);
  });

  it("holding the lock: nobody ahead, counts those waiting behind", () => {
    const f = queueFacts([e("a", "running", 1), e("b", "inbox", 2)], "a");
    expect(f?.holders).toEqual([]);
    expect(f?.ahead).toBe(0);
    expect(f?.waiting).toBe(1);
  });

  it("not in a queue ⇒ null", () => {
    expect(queueFacts([e("a", "running", 1)], "z")).toBeNull();
  });
});

const vq = (extra: Partial<VQueueRow>): VQueueRow => ({
  range: 2,
  scope: "acme",
  limit_key: "eu/t1",
  object_key: null,
  exclusive: false,
  waiting: 3,
  blocked_on: null,
  lock_holder: null,
  ...extra,
});

describe("queueOf", () => {
  it("matches range, scope, limit key, object key (null = empty)", () => {
    const mine = e("x", "inbox", 1, { range: 2, scope: "acme", limit_key: "eu/t1" });
    const rows = [vq({ range: 3 }), vq({ object_key: "" }), vq({ limit_key: "eu" })];
    expect(queueOf(rows, mine, "")).toBe(rows[1]);
    expect(queueOf(rows, mine, "k")).toBeNull();
  });
});

describe("blockingCounter", () => {
  const lim = (counter: string, level: number, range = 2): LimitRow => ({
    range,
    counter,
    level,
    used: 4,
    limit: 4,
    available: 0,
    rule: "acme/*",
  });

  it("the blocked_on level picks the right counter on the scope/l1/l2 path", () => {
    const limits = [lim("acme", 1), lim("acme/eu", 2), lim("acme/eu/t1", 3), lim("acme/eu", 2, 9)];
    expect(blockingCounter(vq({ blocked_on: "concurrency_scope" }), limits)).toBe(limits[0]);
    expect(blockingCounter(vq({ blocked_on: "concurrency_l1" }), limits)).toBe(limits[1]);
    expect(blockingCounter(vq({ blocked_on: "concurrency_l2" }), limits)).toBe(limits[2]);
  });

  it("counters of tenants other than default carry a tenant: prefix", () => {
    const limits = [lim("zeta:acme/eu", 2)];
    expect(blockingCounter(vq({ blocked_on: "concurrency_l1" }), limits)).toBe(limits[0]);
  });

  it("a reason that is not concurrency, or no matching counter ⇒ null", () => {
    expect(blockingCounter(vq({ blocked_on: "lock" }), [lim("acme", 1)])).toBeNull();
    expect(blockingCounter(vq({ blocked_on: "rate" }), [lim("acme", 1)])).toBeNull();
    expect(blockingCounter(vq({ blocked_on: "concurrency_scope" }), [lim("other", 1)])).toBeNull();
    expect(blockingCounter(vq({}), [lim("acme", 1)])).toBeNull();
  });
});
