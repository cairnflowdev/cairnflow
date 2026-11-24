// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Unit tests of the pure part of the chaos cases (`chaos-cases.ts`).
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import {
  ExclusiveWatch,
  judgePurge,
  judgeRestoreCheck,
  maskedErrorOf,
  parDiff,
  parExpected,
  parseTenantBook,
  StuckWatch,
  survivableUncaught,
} from "./chaos-cases.ts";

const none = () => undefined;

test("ExclusiveWatch: two invocations holding a lock one after the other — no violation", () => {
  const w = new ExclusiveWatch();
  w.enter("k", "a", 1, none);
  w.leave("k", "a");
  w.enter("k", "b", 3, none);
  w.leave("k", "b");
  assert.deepEqual(w.violations, []);
  assert.equal(w.holders("k"), 0);
});

test("ExclusiveWatch: a second invocation enters while the first still holds — a violation, one line per pair", () => {
  const w = new ExclusiveWatch();
  w.enter("k", "a", 1, none);
  w.enter("k", "b", 2, none);
  w.leave("k", "b");
  assert.equal(w.violations.length, 1);
  assert.match(w.violations[0]!, /^k: b entered .* while a still held it/);
  // Another key is unrelated.
  w.enter("k2", "c", 4, none);
  assert.equal(w.violations.length, 1);
});

test("ExclusiveWatch: the same invocation rerunning the enter closure (turn died before durability) — no violation", () => {
  const w = new ExclusiveWatch();
  w.enter("k", "a", 1, none);
  w.enter("k", "a", 2, none);
  w.leave("k", "a");
  w.enter("k", "b", 4, none);
  assert.deepEqual(w.violations, []);
});

test("ExclusiveWatch: the holder was cancelled — excused, counted by reason, holder removed", () => {
  const w = new ExclusiveWatch();
  w.enter("k", "a", 1, none);
  const excuse = (other: string) => (other === "a" ? "cancelled" : undefined);
  w.enter("k", "b", 2, excuse);
  assert.deepEqual(w.violations, []);
  assert.equal(w.excused.get("cancelled"), 1);
  assert.equal(w.holders("k"), 1);
});

test("ExclusiveWatch: forget removes a holder that has ended", () => {
  const w = new ExclusiveWatch();
  w.enter("k", "a", 1, none);
  w.forget("a");
  w.enter("k", "b", 2, none);
  assert.deepEqual(w.violations, []);
});

test("StuckWatch: the clock runs from the later of modifiedAt and the time the cluster became healthy again", () => {
  const w = new StuckWatch(60_000);
  const row = { id: "x", status: "running", modifiedAt: 0 };
  // The cluster just became healthy again at 50 s: only 30 s ⇒ not yet.
  w.observe([row], 80_000, 50_000);
  assert.equal(w.flagged.size, 0);
  // Healthy all along since 0: 80 s > 60 s ⇒ a violation, once.
  w.observe([row], 80_000, 0);
  w.observe([row], 90_000, 0);
  assert.equal(w.flagged.size, 1);
  // A row that is not `running` is not examined; a recently changed row is not flagged.
  w.observe([{ id: "y", status: "suspended", modifiedAt: 0 }, { id: "z", status: "running", modifiedAt: 85_000 }], 90_000, 0);
  assert.equal(w.flagged.size, 1);
});

test("StuckWatch: without modifiedAt it counts from the first sighting", () => {
  const w = new StuckWatch(10_000);
  w.observe([{ id: "x", status: "running" }], 100_000, 0);
  w.observe([{ id: "x", status: "running" }], 105_000, 0);
  assert.equal(w.flagged.size, 0);
  w.observe([{ id: "x", status: "running" }], 111_000, 0);
  assert.equal(w.flagged.size, 1);
});

test("parExpected/parDiff: a correct output has no mismatch; a wrong branch is reported as that branch", () => {
  const want = parExpected(8, 3, 4);
  // seq 8: 8 % 4 == 0 ⇒ branch 8 % 3 = 2 fails durably at step 0.
  assert.deepEqual(want, [
    ["r0.0", "e1", "r0.2", "e3"],
    ["r1.0", "e101", "r1.2", "e103"],
    ["T2.0", "e201", "r2.2", "e203"],
  ]);
  assert.deepEqual(parDiff(8, 3, 4, { seq: 8, branches: want }), []);
  const bad = structuredClone(want);
  bad[1]![2] = "r1.9";
  const d = parDiff(8, 3, 4, { seq: 8, branches: bad });
  assert.equal(d.length, 1);
  assert.match(d[0]!, /^branch 1:/);
  assert.equal(parDiff(8, 3, 4, { seq: 9, branches: want }).length, 1);
  assert.equal(parDiff(8, 3, 4, null).length, 2);
  // No seq divisible by 4 ⇒ no durably failing step.
  assert.ok(parExpected(5, 3, 4).flat().every((v) => !v.startsWith("T")));
});

test("judgeRestoreCheck: restore.lost_* is a documented limit — counted, not failing", () => {
  const out = JSON.stringify({
    findings: [
      { check: "restore.lost_call", range: 1, detail: "invocation a waits for the result (completion 3) of a call" },
      { check: "restore.lost_call", range: 2, detail: "…" },
      { check: "restore.lost_reply", range: 6, detail: "…" },
    ],
  });
  const j = judgeRestoreCheck("backup check-restore", { code: 1, out, err: "" });
  assert.deepEqual(j.violations, []);
  assert.equal(j.known.get("restore.lost_call"), 2);
  assert.equal(j.known.get("restore.lost_reply"), 1);
});

test("judgeRestoreCheck: any other finding still fails, even alongside lost_*; with a healing round it is only counted", () => {
  const out = JSON.stringify({
    findings: [
      { check: "restore.lost_call", range: 1, detail: "…" },
      { check: "db", range: 7, detail: "range 7 has no Db" },
      { check: "orphan_db", range: 9, detail: "…", heals: "D-d" },
    ],
  });
  const j = judgeRestoreCheck("verify", { code: 1, out, err: "" });
  assert.equal(j.violations.length, 1);
  assert.match(j.violations[0]!, /^verify: db range 7: range 7 has no Db/);
  assert.equal(j.known.get("restore.lost_call"), 1);
  assert.equal(j.healed.get("orphan_db (D-d)"), 1);
});

test("judgeRestoreCheck: a failed run, non-JSON output, exit 1 without findings — failing; clean — not", () => {
  assert.equal(judgeRestoreCheck("verify", { code: 4, out: "", err: "store error: x" }).violations.length, 1);
  assert.equal(judgeRestoreCheck("verify", { code: null, out: "", err: "killed" }).violations.length, 1);
  assert.equal(judgeRestoreCheck("verify", { code: 0, out: "CLEAN", err: "" }).violations.length, 1);
  assert.equal(judgeRestoreCheck("verify", { code: 1, out: "{\"findings\":[]}", err: "" }).violations.length, 1);
  const clean = judgeRestoreCheck("verify", { code: 0, out: "{\"findings\":[]}", err: "" });
  assert.deepEqual(clean.violations, []);
  assert.equal(clean.known.size, 0);
});

const BOOK = "cairnflow-tenants/1\nchaos-t1 state=limited\nchaos-purge state=purged deleting-ms=1 purging-ms=2 purged-ms=3\n";

test("parseTenantBook: reads the tenant's line, no line is active, an unknown header is unreadable", () => {
  assert.deepEqual(parseTenantBook(BOOK, "chaos-purge"), {
    ok: true,
    line: { state: "purged", deletingMs: "1", purgingMs: "2", purgedMs: "3" },
  });
  assert.deepEqual(parseTenantBook(BOOK, "chaos-t9"), { ok: true, line: null });
  assert.equal(parseTenantBook("cairnflow-tenants/2\n", "x").ok, false);
});

test("judgePurge: a fresh API says purged ⇒ done; book purged while the API is stale ⇒ done per the book", () => {
  assert.equal(judgePurge({ state: "purged", failures: 0, failStreak: 0 }, undefined, 3).source, "api");
  const j = judgePurge({ state: "deleting", failures: 170, failStreak: 170 }, parseTenantBook(BOOK, "chaos-purge"), 3);
  assert.equal(j.purged, true);
  assert.equal(j.source, "book");
  assert.match(j.note, /API deleting.*170 in a row — STALE; tenants book: state=purged/);
});

test("judgePurge: book not yet purged ⇒ stuck per the book (even when the API is stale)", () => {
  const book = parseTenantBook("cairnflow-tenants/1\nchaos-purge state=deleting deleting-ms=1\n", "chaos-purge");
  const j = judgePurge({ state: "deleting", failures: 9, failStreak: 9 }, book, 3);
  assert.equal(j.purged, false);
  assert.equal(j.source, "book");
  assert.match(j.problem!, /stuck per the tenants book \(state=deleting deleting-ms=1\)/);
});

test("judgePurge: book unreadable — stale API ⇒ undecidable, never 'stuck'; fresh API ⇒ stuck per the API", () => {
  const unread = { ok: false as const, why: "aws: NoSuchKey" };
  const stale = judgePurge({ state: "deleting", failures: 5, failStreak: 5 }, unread, 3);
  assert.equal(stale.source, "undecidable");
  assert.doesNotMatch(stale.problem!, /stuck/);
  const fresh = judgePurge({ state: "deleting", failures: 1, failStreak: 0 }, unread, 3);
  assert.equal(fresh.source, "api");
  assert.match(fresh.problem!, /^stuck per the API/);
});

// A real line from a node log of chaos run 7.3 (split every 60 s, --failpoints), colours kept.
const MASKED_TEXT =
  "\x1b[2m2026-11-13T11:05:41.311744Z\x1b[0m \x1b[31mERROR\x1b[0m \x1b[2mcairn_server::ingress::error\x1b[0m\x1b[2m:\x1b[0m " +
  "internal error returned as 500 — details only in the log \x1b[3mcode\x1b[0m\x1b[2m=\x1b[0m2989ee991ba1 " +
  '\x1b[3mdetail\x1b[0m\x1b[2m=\x1b[0m"I/O error with the object store: failpoint split::flush: chaos"';
const MASKED_JSON =
  '{"timestamp":"2026-11-13T11:05:41.311744Z","level":"ERROR","fields":{"message":"internal error returned as 500 — details only in the log",' +
  '"code":"2989ee991ba1","detail":"I/O error with the object store: failpoint split::flush: chaos"},"target":"cairn_server::ingress::error"}';

test(
  "maskedErrorOf: reads code and detail of a masked 500 from the node log, text and JSON",
  () => {
    for (const line of [MASKED_TEXT, MASKED_JSON]) {
      const got = maskedErrorOf(line);
      assert.ok(got, `no entry from: ${line}`);
      assert.equal(got[0], "2989ee991ba1");
      assert.match(got[1], /^detail"?[=:]"?.*failpoint split::flush: chaos/);
    }
    assert.equal(maskedErrorOf("some other line code=2989ee991ba1"), undefined);
  },
);

test(
  "maskedErrorOf: the field names it parses are the ones ingress/error.rs logs",
  () => {
    const src = readFileSync(new URL("../../crates/cairn-server/src/ingress/error.rs", import.meta.url), "utf8");
    assert.ok(
      src.includes('tracing::error!(code = %code, detail = detail, "internal error returned as 500'),
      "ingress/error.rs changed the fields of its 500 log line — update maskedErrorOf (chaos-cases.ts) with it",
    );
  },
);

test(
  "survivableUncaught: undici's setTypeOfService EINVAL on a broken socket does not end the run",
  () => {
    // Node 24.18's undici calls `socket.setTypeOfService` on every HTTP/1 write; on macOS a
    // socket the peer already reset throws `EINVAL` synchronously from a socket callback, no
    // listener catches it, and a 24 h soak died in its 8th hour this way.
    const survivable = survivableUncaught;
    const tos = Object.assign(new Error("setTypeOfService EINVAL"), { errno: -22, code: "EINVAL", syscall: "setTypeOfService" });
    assert.match(survivable(tos) ?? "", /setTypeOfService/);
    // Anything else still ends the run.
    const other = Object.assign(new Error("write EINVAL"), { code: "EINVAL", syscall: "write" });
    assert.equal(survivable(other), undefined);
    assert.equal(survivable(new TypeError("x is undefined")), undefined);
    assert.equal(survivable("a string"), undefined);
  },
);

test(
  "describeCtlExit: a cairn-ctl the harness killed at its timeout says so",
  async () => {
    // A 24 h soak printed "backup refused/failed (null) — " for 25 cycles: every backup ran
    // for the full 900 s and was SIGKILLed by the harness, which looked like an instant exit.
    const cases = (await import("./chaos-cases.ts")) as Record<string, unknown>;
    const describe = cases.describeCtlExit as
      | ((r: { code: number | null; signal: string | null; timedOut: boolean; err: string }, timeoutMs: number) => string)
      | undefined;
    assert.ok(describe, "chaos-cases.ts exports describeCtlExit");
    assert.match(describe({ code: null, signal: "SIGKILL", timedOut: true, err: "" }, 900_000), /timed out after 900s/);
    assert.match(describe({ code: null, signal: "SIGSEGV", timedOut: false, err: "" }, 900_000), /killed by SIGSEGV/);
    assert.equal(describe({ code: 3, signal: null, timedOut: false, err: "refused: busy\n" }, 900_000), "exited 3: refused: busy");
  },
);

test(
  "backupIdsIn: the ids an `aws s3 ls <root>/` listing shows",
  async () => {
    // Failed and refused backup cycles left their partial copies behind: 46 of them, most of an
    // 88 GB bucket, stopped a 24 h soak on a full disk.
    const cases = (await import("./chaos-cases.ts")) as Record<string, unknown>;
    const ids = cases.backupIdsIn as ((ls: string) => string[]) | undefined;
    assert.ok(ids, "chaos-cases.ts exports backupIdsIn");
    const ls = [
      "                           PRE 1794656926468-b49c/",
      "                           PRE 1794657826478-32d8/",
      "2026-11-14 10:19:08        133 stray-object",
      "",
    ].join("\n");
    assert.deepEqual(ids(ls), ["1794656926468-b49c", "1794657826478-32d8"]);
    assert.deepEqual(ids(""), []);
  },
);

test(
  "retryServerErrors: a setup call retries 5xx and network errors, not 4xx",
  async () => {
    // A short soak died while setting up: `POST /admin/keys` returned 500 because the S3 fault
    // profile failed one GET of `auth/keys`, and the harness did not try again.
    const cases = (await import("./chaos-cases.ts")) as Record<string, unknown>;
    const retry = cases.retryServerErrors as
      | ((call: () => Promise<Response>, attempts: number, pause: (ms: number) => Promise<void>) => Promise<Response>)
      | undefined;
    assert.ok(retry, "chaos-cases.ts exports retryServerErrors");
    const noPause = async () => {};
    const seq = (xs: Array<number | Error>) => {
      let i = 0;
      return async () => {
        const x = xs[Math.min(i++, xs.length - 1)]!;
        if (x instanceof Error) throw x;
        return new Response(null, { status: x });
      };
    };
    assert.equal((await retry(seq([500, new Error("ECONNRESET"), 503, 200]), 5, noPause)).status, 200);
    assert.equal((await retry(seq([409, 200]), 5, noPause)).status, 409);
    assert.equal((await retry(seq([500]), 3, noPause)).status, 500);
    await assert.rejects(retry(seq([new Error("ECONNREFUSED")]), 2, noPause), /ECONNREFUSED/);
  },
);
