// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * The public API contract: the `docs/api/openapi.json` file itself and its
 * checker (`api-schema.ts`). No binary needed — the "every real response matches
 * the schema" part runs in `e2e.test.ts`.
 */

import assert from "node:assert/strict";
import { test } from "node:test";
import { ApiSchema } from "./api-schema.ts";

const api = new ApiSchema();
const ID = "0123456789abcdef0123456789abcdef0123456789abcdef";
const json = (h: Record<string, string> = {}) => ({
  get: (n: string) => ({ "content-type": "application/json", ...h })[n.toLowerCase()] ?? null,
});
const check = (method: string, url: string, status: number, body: unknown, h?: Record<string, string>) =>
  api.check(method, url, status, json(h), JSON.stringify(body));

test("schema: every $ref resolves, the checker understands every keyword", () => {
  const walk = (n: unknown): void => {
    if (Array.isArray(n)) return n.forEach(walk);
    if (n && typeof n === "object") {
      for (const [k, v] of Object.entries(n)) {
        if (k === "$ref") api.resolve(v as string);
        else walk(v);
      }
    }
  };
  walk(api.doc);
  // Walk every component schema with an arbitrary value: an unknown keyword throws.
  for (const s of Object.values<any>(api.doc.components.schemas)) api.validate(s, null);
  assert.equal(api.doc.openapi, "3.1.0");
});

test("schema: all the core routes are present", () => {
  for (const [m, p] of [
    ["POST", "/invoke/S/h"],
    ["POST", "/invoke/S/h/send"],
    ["POST", "/scope/sc/invoke/S/h"],
    ["POST", "/scope/sc/invoke/S/h/send"],
    ["GET", "/invocations"],
    ["GET", `/invocations/${ID}`],
    ["GET", "/workflow/W/k/output"],
    ["POST", "/workflow/W/k/attach"],
    ["GET", "/scope/sc/workflow/W/k/output"],
    ["POST", "/scope/sc/workflow/W/k/attach"],
  ] as const) {
    assert.ok(api.route(m, p), `${m} ${p} is not in the schema`);
  }
  // A sub-route is not mistaken for another: the journal matches its own template,
  // not `/invocations/{id}`.
  assert.equal(api.route("GET", `/invocations/${ID}/journal`)?.template, "/invocations/{id}/journal");
  assert.equal(api.route("GET", `/invocations/${ID}/logs`)?.template, "/invocations/{id}/logs");
  assert.equal(api.route("GET", `/invocations/${ID}/missing`), undefined);
  // A static segment beats a parameter: `batch` is not taken as an id.
  assert.equal(api.route("POST", "/invocations/batch/cancel")?.template, "/invocations/batch/{op}");
  assert.equal(api.route("POST", `/invocations/${ID}/purge`)?.template, "/invocations/{id}/purge");
});

test("schema: entry points for external systems and cancellation — as they behave today", () => {
  const AWK = `awk_${ID}00000011`;
  for (const [m, p] of [
    ["POST", `/invocations/${ID}/signal`],
    ["POST", `/invocations/${ID}/cancel`],
    ["POST", `/awakeables/${AWK}/resolve`],
    ["POST", `/awakeables/${AWK}/reject`],
    ["POST", "/promises/W/k/email/resolve"],
    ["POST", "/promises/W/k/email/reject"],
    ["GET", "/promises/W/k/email"],
    ["POST", "/scope/sc/promises/W/k/email/resolve"],
    ["POST", "/scope/sc/promises/W/k/email/reject"],
    ["GET", "/scope/sc/promises/W/k/email"],
  ] as const) {
    assert.ok(api.route(m, p), `${m} ${p} is not in the schema`);
  }
  assert.deepEqual(check("POST", `/invocations/${ID}/signal`, 200, { id: ID, status: "running" }), []);
  assert.deepEqual(check("POST", `/invocations/${ID}/signal`, 403, { error: "admin role required" }), []);
  assert.deepEqual(check("POST", `/invocations/${ID}/cancel?mode=signal`, 200, { id: ID, cancelled: false, status: "suspended" }), []);
  assert.deepEqual(check("POST", `/invocations/${ID}/cancel`, 200, { id: ID, cancelled: true, status: "failed" }), []);
  assert.deepEqual(check("POST", `/awakeables/${AWK}/resolve`, 200, { id: AWK, status: "suspended" }), []);
  assert.deepEqual(check("POST", "/promises/W/k/email/resolve", 200, { service: "W", key: "k", name: "email", completed: true, woken: 0 }), []);
  assert.deepEqual(check("POST", "/promises/W/k/email/reject", 200, { service: "W", key: "k", name: "email", completed: false }), []);
  assert.deepEqual(
    check("GET", "/promises/W/k/email", 200, { service: "W", key: "k", name: "email", completed: true, value: '"a@b"' }),
    [],
  );
  assert.deepEqual(
    check("GET", "/promises/W/k/email", 200, { service: "W", key: "k", name: "email", completed: true, failure: "403: no" }),
    [],
  );
  // A shape mismatch is caught.
  assert.ok(check("POST", `/invocations/${ID}/signal`, 200, { id: ID, status: "waiting" })!.length > 0);
  assert.ok(check("POST", `/invocations/${ID}/cancel`, 200, { id: ID, status: "failed" })!.length > 0);
  assert.ok(check("POST", `/awakeables/${AWK}/resolve`, 200, { id: "awk_x", status: "running" })!.length > 0);
  assert.ok(check("GET", "/promises/W/k/email", 200, { service: "W", key: "k", name: "email" })!.length > 0);
  // The headers and request body of the invoke entry point are documented.
  const names = (p: string) =>
    api.route("POST", p)!.op.parameters.map((x: any) => (x.$ref ? api.resolve(x.$ref).name : x.name));
  for (const h of ["Idempotency-Key", "cairnflow-log-level"]) {
    assert.ok(names("/invoke/S/h").includes(h), h);
    assert.ok(names("/invoke/S/h/send").includes(h), h);
  }
  assert.ok(names("/scope/sc/invoke/S/h").includes("x-cairn-limit-key"));
  assert.ok(!names("/invoke/S/h").includes("x-cairn-limit-key"), "a limit key without a scope is a 400");
  assert.ok(api.route("POST", "/invoke/S/h")!.op.requestBody);
  assert.ok(api.route("POST", `/invocations/${ID}/signal`)!.op.requestBody);
});

test("schema: the /invoke response bodies by status code", () => {
  assert.deepEqual(check("POST", "/invoke/S/h?key=k", 200, { status: "completed", id: ID, output: '{"a":1}' }), []);
  assert.deepEqual(check("POST", "/invoke/S/h/send", 202, { status: "waiting", id: ID }), []);
  assert.deepEqual(check("POST", "/invoke/S/h", 409, { status: "paused", id: ID, code: 575, message: "3 attempts exhausted" }), []);
  assert.deepEqual(check("POST", "/invoke/W/run", 409, { status: "already_invoked", id: ID }), []);
  assert.deepEqual(check("POST", "/invoke/S/h", 500, { status: "failed", id: ID, code: 402, message: "x" }), []);
  assert.deepEqual(check("POST", "/invoke/S/h", 500, { error: "SDK unreachable", id: ID }), []);
  assert.deepEqual(
    check("POST", "/invoke/S/h", 429, { status: "throttled", id: ID, retry_after_ms: 250 }, { "retry-after": "1" }),
    [],
  );
  assert.deepEqual(
    check(
      "POST",
      "/invoke/S/h",
      429,
      { error: "overloaded", code: "range_overloaded", retry_after_ms: 1000 },
      { "retry-after": "1" },
    ),
    [],
  );
  assert.deepEqual(check("POST", "/invoke/S/h", 503, { error: "not the leader", leader: "http://n:1", id: ID }), []);
  assert.deepEqual(check("POST", "/invoke/S/h", 401, { error: "API key required" }), []);
});

test("schema: shape mismatches are caught", () => {
  // `output` must be a string (JSON nested in a string), not an object.
  assert.ok(check("POST", "/invoke/S/h", 200, { status: "completed", id: ID, output: { a: 1 } })!.length > 0);
  // A malformed id.
  assert.ok(check("POST", "/invoke/S/h", 202, { status: "waiting", id: "abc" })!.length > 0);
  // An undeclared field: strict.
  assert.ok(check("POST", "/invoke/S/h", 202, { status: "waiting", id: ID, extra: 1 })!.length > 0);
  // A throttled 429 must carry Retry-After.
  assert.ok(check("POST", "/invoke/S/h", 429, { status: "throttled", id: ID, retry_after_ms: 1 })!.length > 0);
  // `waiting` is not a durable status.
  assert.ok(check("GET", `/invocations/${ID}`, 200, { id: ID, status: "waiting" }, { "x-cairn-read": "leader" })!.length > 0);
  // A failed workflow is a 500, not a 200.
  assert.ok(check("GET", "/workflow/W/k/output", 200, { status: "failed", id: ID, code: 1, message: "x" })!.length > 0);
  // An error body that is not JSON.
  assert.ok(api.check("GET", `/invocations/${ID}`, 404, { get: () => "text/plain" }, "not found")!.length > 0);
  // An unknown status code.
  assert.ok(check("GET", `/invocations/${ID}`, 404, { error: "x", code: "nope" })!.length > 0);
});

test("schema: GET /invocations/{id} — read-source header, slots reserved for streams/lastFailure", () => {
  const h = { "x-cairn-read": "follower" };
  assert.deepEqual(check("GET", `/invocations/${ID}`, 200, { id: ID, status: "completed", output: "1" }, h), []);
  assert.deepEqual(check("GET", `/invocations/${ID}`, 200, { id: ID, status: "paused", code: 575, message: "m" }, h), []);
  assert.ok(check("GET", `/invocations/${ID}`, 200, { id: ID, status: "running" })!.length > 0, "missing x-cairn-read");
  // `streams` (name, durable offset, sealed) — optional, the runtime does not return it yet.
  assert.deepEqual(
    check(
      "GET",
      `/invocations/${ID}`,
      200,
      { id: ID, status: "running", streams: [{ name: "tokens", durableOffset: 42, sealed: false }] },
      h,
    ),
    [],
  );
  assert.ok(
    check("GET", `/invocations/${ID}`, 200, { id: ID, status: "running", streams: [{ name: "t" }] }, h)!.length > 0,
  );
  // The last attempt's error (optional; the runtime returns it when reading from the leader).
  assert.deepEqual(
    check(
      "GET",
      `/invocations/${ID}`,
      200,
      { id: ID, status: "paused", code: 575, message: "m", lastFailure: { code: 500, message: "boom", attempt: 2 } },
      h,
    ),
    [],
  );
  // Error code reserved for an expired stream.
  assert.deepEqual(check("GET", `/invocations/${ID}`, 410, { error: "stream expired", code: "stream_expired" }), []);
  assert.equal(api.doc.components.schemas.InvocationStatus.properties.streams["x-cairn-reserved"], true);
  assert.equal(api.doc.components.schemas.InvocationStatus.properties.lastFailure["x-cairn-reserved"], undefined);
  for (const name of ["streams", "lastFailure"]) {
    assert.ok(!api.doc.components.schemas.InvocationStatus.required.includes(name), `${name} must be optional`);
  }
});

test("schema: GET /invocations — a local array or a cluster-wide object", () => {
  const row = { id: ID, partition: 3, status: "suspended", service: "S", handler: "h", key: "k", deployment: "dp_1", attempt: 0 };
  assert.deepEqual(check("GET", "/invocations?limit=5", 200, [row]), []);
  assert.deepEqual(
    check("GET", "/invocations?scope=cluster", 200, {
      scope: "cluster",
      nodes: ["a"],
      partial: false,
      missing: [],
      missingRanges: [],
      limit: 1000,
      invocations: [{ ...row, node: "a" }],
    }),
    [],
  );
  assert.ok(check("GET", "/invocations", 200, [{ ...row, status: "waiting" }])!.length > 0);
  assert.deepEqual(check("GET", "/invocations?status=waiting", 400, { error: "no such status \"waiting\"" }), []);
});

test("schema: optional timestamps on the status and on list rows", () => {
  const h = { "x-cairn-read": "leader" };
  const t = { createdAt: 1_700_000_000_000, modifiedAt: 1_700_000_000_500 };
  assert.deepEqual(check("GET", `/invocations/${ID}`, 200, { id: ID, status: "suspended", ...t }, h), []);
  assert.deepEqual(
    check("GET", `/invocations/${ID}`, 200, { id: ID, status: "completed", output: "1", ...t, endedAt: t.modifiedAt }, h),
    [],
  );
  const row = { id: ID, partition: 0, status: "running", attempt: 0, ...t };
  assert.deepEqual(check("GET", "/invocations", 200, [row]), []);
  assert.ok(check("GET", `/invocations/${ID}`, 200, { id: ID, status: "running", createdAt: "x" }, h)!.length > 0);
  assert.ok(check("GET", `/invocations/${ID}`, 200, { id: ID, status: "running", createdAt: 0 }, h)!.length > 0);
  for (const s of ["InvocationStatus", "InvocationRow"]) {
    for (const f of ["createdAt", "modifiedAt", "endedAt"]) {
      assert.ok(!(api.doc.components.schemas[s].required ?? []).includes(f), `${s}.${f} must be optional`);
    }
  }
});

test("schema: attach by id, wait_ms, header x-cairn-invocation-id", () => {
  assert.ok(api.route("POST", `/invocations/${ID}/attach`));
  const h = { "x-cairn-invocation-id": ID };
  assert.deepEqual(check("POST", `/invocations/${ID}/attach?wait_ms=25000`, 200, { status: "completed", id: ID, output: "1" }, h), []);
  assert.deepEqual(check("POST", `/invocations/${ID}/attach`, 202, { status: "waiting", id: ID }, h), []);
  assert.deepEqual(check("POST", `/invocations/${ID}/attach`, 409, { status: "paused", id: ID, code: 575, message: "m" }), []);
  assert.deepEqual(check("POST", `/invocations/${ID}/attach`, 500, { status: "failed", id: ID, code: 1, message: "x" }), []);
  assert.deepEqual(check("POST", `/invocations/${ID}/attach`, 404, { error: "no such invocation" }), []);
  assert.ok(check("POST", `/invocations/${ID}/attach`, 202, { status: "waiting", id: ID }, { "x-cairn-invocation-id": "x" })!.length > 0);
  assert.deepEqual(check("POST", "/invoke/S/h", 200, { status: "completed", id: ID, output: "1" }, h), []);
  const names = (m: string, p: string) =>
    api.route(m, p)!.op.parameters.map((x: any) => (x.$ref ? api.resolve(x.$ref).name : x.name));
  assert.ok(names("POST", `/invocations/${ID}/attach`).includes("wait_ms"));
  assert.ok(names("POST", "/workflow/W/k/attach").includes("wait_ms"));
});

test("schema: server-side schedules", () => {
  const sched = {
    name: "nightly",
    id: ID,
    target: { service: "Report", handler: "build" },
    everyMs: 60000,
    overlap: "skip",
    paused: false,
    jitterMs: 0,
    nextAt: 120000,
    fireAt: 120000,
    last: [{ at: 60000, id: ID, outcome: "fired", status: "completed" }],
    fired: 1,
    missed: 0,
    skipped: 0,
    createdAt: 1,
    updatedAt: 1,
  };
  for (const p of ["/schedules/nightly", "/scope/acme/schedules/nightly"]) {
    assert.deepEqual(check("PUT", p, 200, { ...sched, created: true, changed: true }), []);
    assert.deepEqual(check("GET", p, 200, { ...sched, upcoming: [120000, 180000] }), []);
    assert.deepEqual(check("POST", `${p}/pause`, 200, { ...sched, paused: true, nextAt: null }), []);
    assert.deepEqual(check("POST", `${p}/trigger`, 200, { id: ID, created: false }), []);
    assert.deepEqual(check("DELETE", p, 200, { name: "nightly", deleted: true }), []);
    assert.deepEqual(check("PUT", p, 409, { error: "x", code: "schedules_disabled" }), []);
  }
  assert.ok(check("GET", "/schedules/nightly", 200, { ...sched, overlap: "sometimes" })!.length > 0);
  assert.deepEqual(
    check("GET", "/schedules", 200, { schedules: [sched], partial: false, missing: [], missingRanges: [] }),
    [],
  );
});

// A flaky row of the 4p+split cluster e2e: the leader-wait hook calls `GET /cluster`
// during start-up, a range without an owner yet ⇒ the server returns
// `leader`/`address`/`term` = `null` (`ingress/cluster.rs::RangeView`, an `Option`
// does not drop the field) — the schema must accept exactly that response.
test("schema: GET /cluster — a range without an owner returns leader/address/term null", () => {
  const unowned = { leader: null, address: null, term: null };
  const body = {
    version: 1,
    nextRangeId: 2,
    ranges: [
      { range: 0, start: "0000000000000000", end: "7fffffffffffffff", gen: 0, ...unowned },
      { range: 1, start: "8000000000000000", end: "ffffffffffffffff", gen: 0, leader: "a", address: "127.0.0.1:1", term: 3 },
    ],
    partitionCount: 2,
    partitions: [
      { partition: 0, ...unowned },
      { partition: 1, leader: "a", address: "127.0.0.1:1", term: 3 },
    ],
    readers: [],
  };
  assert.deepEqual(check("GET", "/cluster", 200, body), []);
  // The type is still strict: leader is a string or null, not a number.
  assert.ok(check("GET", "/cluster", 200, { ...body, ranges: [{ ...body.ranges[1], leader: 7 }] })!.length > 0);
});
