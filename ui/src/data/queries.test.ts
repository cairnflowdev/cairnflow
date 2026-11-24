// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// `ui_queries_are_catalogued`: every console statement declares its question
// numbers, and each number has a server test `ui_question_<n>` pinning the cost of
// that statement shape. Same file: user values never become SQL text.
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  type InvocationFilter,
  invocationCount,
  invocationList,
  type Query,
  queries,
  tagPattern,
} from "./queries";

const ROOT = join(__dirname, "..", "..", "..");
const SERVER_TESTS = ["crates/cairn-query/tests/sql.rs", "crates/cairn-server/tests/query_api.rs"];

function catalogued(): Set<number> {
  const out = new Set<number>();
  for (const f of SERVER_TESTS) {
    for (const m of readFileSync(join(ROOT, f), "utf8").matchAll(/async fn ui_question_(\d+)\b/g)) {
      out.add(Number(m[1]));
    }
  }
  return out;
}

const ID = "3fa9c21b0e6d4a7f9be1d2c3a4b5c6d7e8f9a0b1c2d3ec21";
const HOSTILE = "x' OR 1=1; DROP TABLE t; --";

/** Every statement the console can send, including every fragment of the list filters. */
function everyQuery(): [string, Query][] {
  const fixed: [string, Query][] = [
    ["invocationCounts", queries.invocationCounts()],
    ["handlerStats", queries.handlerStats()],
    ["topFailing", queries.topFailing(5)],
    ["serviceHealth", queries.serviceHealth()],
    ["deploymentsPinned", queries.deploymentsPinned()],
    ["history", queries.history(1)],
    ["historyByHandler", queries.historyByHandler(1)],
    ["meteringNow", queries.meteringNow()],
    ["limits", queries.limits()],
    ["vqueues", queries.vqueues()],
    ["scheduler", queries.scheduler()],
    ["serviceQueues", queries.serviceQueues(HOSTILE)],
    [
      "objectQueue",
      queries.objectQueue(HOSTILE, HOSTILE, HOSTILE, { scope: null, limitKey: null }),
    ],
    ["queueEntry", queries.queueEntry(HOSTILE, HOSTILE, ID, { scope: null, limitKey: null })],
    ["invocation", queries.invocation(ID)],
    ["journalLite", queries.journalLite(ID, 0)],
    ["journalBody", queries.journalBody(ID, 3)],
    ["logsAfter", queries.logsAfter(ID, 10)],
    ["spans", queries.spans(ID)],
    ["streams", queries.streams(ID)],
    ["streamRecords", queries.streamRecords(ID, HOSTILE, 4)],
    ["tree", queries.tree(ID)],
    ["children", queries.children(ID)],
    ["state", queries.state(HOSTILE, HOSTILE)],
    ["objects", queries.objects(HOSTILE, 500)],
    ["promises", queries.promises(HOSTILE, HOSTILE)],
  ];
  expect(fixed.map(([n]) => n).sort()).toEqual(Object.keys(queries).sort());
  // Queue variant with a scope (a different WHERE fragment).
  fixed.push(
    [
      "objectQueue scope",
      queries.objectQueue(HOSTILE, HOSTILE, HOSTILE, { scope: HOSTILE, limitKey: HOSTILE }),
    ],
    [
      "queueEntry scope",
      queries.queueEntry(HOSTILE, HOSTILE, ID, { scope: HOSTILE, limitKey: HOSTILE }),
    ],
  );
  const full: InvocationFilter = {
    classes: ["failed", "paused"],
    retrying: true,
    rootOnly: true,
    service: HOSTILE,
    handler: HOSTILE,
    key: HOSTILE,
    deployment: HOSTILE,
    parentId: ID,
    idempotencyKey: HOSTILE,
    idPrefix: "3fa9",
    tag: HOSTILE,
    createdFromMs: 1,
    createdToMs: 2,
  };
  const lists: [string, Query][] = (["created", "transitioned", "heldUp"] as const).flatMap((o) => [
    [`list ${o}`, invocationList({}, o, 50)],
    [`list ${o} all filters`, invocationList(full, o, 50, 123)],
  ]);
  return [...fixed, ...lists, ["count", invocationCount(full)]];
}

describe("ui_queries_are_catalogued", () => {
  it("every statement declares question numbers and each number has a server ui_question_<n>", () => {
    const have = catalogued();
    expect(have.size).toBeGreaterThan(30);
    for (const [name, query] of everyQuery()) {
      expect(query.questions.length, name).toBeGreaterThan(0);
      for (const n of query.questions)
        expect(have.has(n), `${name}: question ${n} has no ui_question_${n}`).toBe(true);
    }
  });

  it("values only go through parameters: SQL holds no values, the $n count matches params", () => {
    for (const [name, query] of everyQuery()) {
      expect(query.sql, name).not.toContain("DROP");
      expect(query.sql, name).not.toContain(ID);
      const used = new Set([...query.sql.matchAll(/\$(\d+)/g)].map((m) => Number(m[1])));
      expect(
        [...used].sort((a, b) => a - b),
        name,
      ).toEqual(query.params.map((_, i) => i + 1));
    }
  });

  it("list sorted by index: held up scans backwards", () => {
    expect(invocationList({}, "created", 10).sql).toMatch(
      /ORDER BY created_at DESC, id LIMIT \$1$/,
    );
    expect(invocationList({}, "heldUp", 10).sql).toMatch(
      /ORDER BY created_at ASC, id DESC LIMIT \$1$/,
    );
    expect(invocationList({}, "transitioned", 10).questions).toContain(14);
  });

  it("count wraps exactly the filters, no ORDER BY, capped at 10 000", () => {
    const c = invocationCount({ service: "Order", classes: ["failed"] });
    expect(c.sql).toBe(
      "SELECT count(*) AS n FROM (SELECT id FROM sys_invocation WHERE service = $1 AND class = $2 LIMIT $3)",
    );
    expect(c.params).toEqual(["Order", "failed", 10_000]);
  });
});

describe("why banner: queues and flow control", () => {
  it("an object's queue filters by tenant + service + key, sorted by seq, bounded", () => {
    const q = queries.objectQueue("acme", "Cart", "cart-1", { scope: null, limitKey: null });
    expect(q.questions).toEqual([41]);
    expect(q.sql).toBe(
      "SELECT id, stage, seq, scope, limit_key, range FROM sys_vqueue_entry WHERE tenant = $1 AND service = $2 AND key = $3 ORDER BY seq LIMIT $4",
    );
    expect(q.params).toEqual(["acme", "Cart", "cart-1", 200]);
  });

  it("an unkeyed service's entry filters by id; RAM queues by service", () => {
    expect(queries.queueEntry("acme", "Shop", ID, { scope: null, limitKey: null }).params).toEqual([
      "acme",
      "Shop",
      ID,
    ]);
    // A queue's identity includes the scope: an invocation with a scope also filters on `scope`.
    const scoped = queries.queueEntry("acme", "Shop", ID, { scope: "eu", limitKey: null });
    expect(scoped.sql).toMatch(
      /WHERE tenant = \$1 AND scope = \$2 AND service = \$3 AND id = \$4$/,
    );
    expect(scoped.params).toEqual(["acme", "eu", "Shop", ID]);
    expect(
      queries.objectQueue("acme", "Cart", "k", { scope: "eu", limitKey: null }).params,
    ).toEqual(["acme", "eu", "Cart", "k", 200]);
    // The limit key is also part of the queue identity: also filter on `limit_key`.
    const limited = queries.queueEntry("acme", "Shop", ID, { scope: "eu", limitKey: "alice" });
    expect(limited.sql).toMatch(
      /WHERE tenant = \$1 AND scope = \$2 AND limit_key = \$3 AND service = \$4 AND id = \$5$/,
    );
    expect(limited.params).toEqual(["acme", "eu", "alice", "Shop", ID]);
    const s = queries.serviceQueues("Shop");
    expect(s.questions).toEqual([6]);
    expect(s.sql).toMatch(/FROM sys_vqueue WHERE service = \$1 /);
  });

  it("invocation detail reads the tenant (the queue's key)", () => {
    expect(queries.invocation(ID).sql).toMatch(/^SELECT id, tenant, service,/);
  });
});

describe("tagPattern", () => {
  it("matches the JSON shape of the tags column (question 27)", () => {
    expect(tagPattern("team=a")).toBe('%"team":"a"%');
    expect(tagPattern("env")).toBe('%"env":%');
    expect(tagPattern("k=a=b")).toBe('%"k":"a=b"%');
  });
});
