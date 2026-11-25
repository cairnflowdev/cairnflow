// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Unit tests of the chaos history checker: every kind of anomaly is caught, and
// a stale read from a follower is not treated as a violation.
import assert from "node:assert/strict";
import { test } from "node:test";
import {
  checkAttempts,
  classifyPaused,
  checkHistory,
  deadLocks,
  type Observation,
  queueGrows,
  queueSlopes,
  type Run,
  type SchedSample,
  type TurnSeen,
  type FloorTrack,
  watchFloor,
} from "./history.ts";

const tags = new Map([["a", "t-a"]]);
const obs = (t: number, source: Observation["source"], status: string, output?: string): Observation => ({
  id: "a",
  t,
  source,
  status,
  output,
});

test("clean history: no anomalies", () => {
  const r = checkHistory(
    [obs(1, "leader", "running"), obs(2, "leader", "completed", "x"), obs(3, "follower", "completed", "x")],
    [{ tag: "t-a", t: 1 }],
    tags,
  );
  assert.deepEqual(r.anomalies, []);
  assert.equal(r.completedIds, 1);
});

test("stale follower read after completed: counted, not a violation", () => {
  const r = checkHistory([obs(1, "leader", "completed", "x"), obs(2, "follower", "running")], [], tags);
  assert.deepEqual(r.anomalies, []);
  assert.equal(r.staleFollowerReads, 1);
});

test("leader returns an unfinished status after completed: vanish", () => {
  const r = checkHistory(
    [obs(1, "follower", "completed", "x"), obs(2, "leader", "running"), obs(3, "leader", "completed", "x")],
    [],
    tags,
  );
  assert.deepEqual(
    r.anomalies.map((a) => a.kind),
    ["vanish"],
  );
});

test("two different outputs: flip", () => {
  const r = checkHistory([obs(1, "leader", "completed", "x"), obs(2, "leader", "completed", "y")], [], tags);
  assert.deepEqual(
    r.anomalies.map((a) => a.kind),
    ["flip"],
  );
});

test("side effect runs after completed was seen: rerun", () => {
  const runs: Run[] = [
    { tag: "t-a", t: 1 },
    { tag: "t-a", t: 5 },
  ];
  const r = checkHistory([obs(2, "leader", "completed", "x")], runs, tags);
  assert.deepEqual(
    r.anomalies.map((a) => a.kind),
    ["rerun"],
  );
});

test("404 after completed: counted separately (retention), not vanish", () => {
  const r = checkHistory([obs(1, "leader", "completed", "x"), obs(2, "leader", "404")], [], tags);
  assert.deepEqual(r.anomalies, []);
  assert.equal(r.goneAfterCompleted, 1);
});

test("rerun: the detail says whether the late run got committed (nonce in the output)", () => {
  const runs: Run[] = [
    { tag: "t-a", t: 1, nonce: "n1", attempt: 0 },
    { tag: "t-a", t: 5, nonce: "n2", attempt: 0 },
  ];
  const discarded = checkHistory([obs(2, "follower", "completed", "stamp-n1")], runs, tags);
  assert.deepEqual(
    discarded.anomalies.map((a) => a.kind),
    ["rerun"],
  );
  assert.match(discarded.anomalies[0]!.detail, /at 5 \(attempt 0, nonce n2\) after/);
  // The late run wins: the output changes ⇒ flip, and rerun records COMMITTED.
  const won = checkHistory(
    [obs(2, "follower", "completed", "stamp-n1"), obs(6, "leader", "completed", "stamp-n2")],
    runs,
    tags,
  );
  assert.deepEqual(won.anomalies.map((a) => a.kind).sort(), ["flip", "rerun"]);
  assert.match(won.anomalies.find((a) => a.kind === "rerun")!.detail, /n2, COMMITTED/);
});

test("side effect of another id with the same tag (duplicate submission): not a rerun, counted separately", () => {
  const runs: Run[] = [
    { tag: "t-a", t: 1, nonce: "n1", invocation: "a" },
    { tag: "t-a", t: 5, nonce: "n2", invocation: "b" },
  ];
  const r = checkHistory([obs(2, "leader", "completed", "stamp-n1")], runs, tags);
  assert.deepEqual(r.anomalies, []);
  assert.equal(r.duplicateSubmissions, 1);
  // The same id running again after completed is still a rerun.
  const same = checkHistory(
    [obs(2, "leader", "completed", "stamp-n1")],
    [...runs, { tag: "t-a", t: 6, nonce: "n3", invocation: "a" }],
    tags,
  );
  assert.deepEqual(
    same.anomalies.map((a) => a.kind),
    ["rerun"],
  );
});

// ── Liveness checks.

const turn = (id: string, attempt: number, t: number): TurnSeen => ({ id, attempt, t });

test("real attempts: paused after the SDK saw all maxAttempts turns — valid", () => {
  // Turn 0 advances the journal and then fails; retries 1, 2, then preflight stops at 3.
  const turns = [turn("a", 0, 1), turn("a", 0, 2), turn("a", 1, 3), turn("a", 2, 4)];
  const r = checkAttempts(turns, ["a"], 3);
  assert.deepEqual(r.phantom, []);
  assert.equal(r.phantomAttempts, 0);
  assert.equal(r.gaps, 0);
});

test("real attempts: an attempt consumed without the SDK being called", () => {
  // Turn 1 is followed by turn 4: attempts 2 and 3 were counted when the turn was
  // throttled/cancelled before Running.
  const turns = [turn("a", 0, 1), turn("a", 1, 2), turn("a", 4, 3)];
  const r = checkAttempts(turns, ["a"], 5);
  assert.deepEqual(r.phantom, [{ id: "a", seen: [0, 1, 4], missing: [2, 3] }]);
  assert.equal(r.phantomAttempts, 2);
  assert.equal(r.gaps, 2);
  assert.deepEqual(r.gapIds, ["a"]);
});

test("real attempts: the SDK never saw the paused invocation — every attempt is a ghost", () => {
  const r = checkAttempts([], ["z"], 3);
  assert.equal(r.phantomAttempts, 3);
});

test("real attempts: only the last segment counts, the earlier one (before the journal advanced) does not make up", () => {
  const turns = [turn("a", 0, 1), turn("a", 1, 2), turn("a", 2, 3), turn("a", 0, 4), turn("a", 2, 5)];
  const r = checkAttempts(turns, ["a"], 3);
  assert.deepEqual(r.phantom, [{ id: "a", seen: [0, 2], missing: [1] }]);
});

test("real attempts: the handler re-running with the same attempt within one BIDI turn is neither a ghost nor a jump", () => {
  const turns = [turn("a", 0, 1), turn("a", 0, 2), turn("a", 1, 3), turn("a", 1, 4), turn("a", 2, 5)];
  const r = checkAttempts(turns, ["a"], 3);
  assert.deepEqual(r.phantom, []);
  assert.equal(r.gaps, 0);
});

test("real attempts: a missing attempt with a kill -9 in its gap is the FL-19 gap, forgiven and counted separately", () => {
  // attempt 4 at 100, kill at 150, attempt 6 at 200: attempt 5 was counted at Running and then the
  // node died.
  const turns = [0, 1, 2, 3, 4].map((a) => turn("a", a, a * 10)).concat([turn("a", 6, 200), turn("a", 7, 210)]);
  const r = checkAttempts(turns, ["a"], 8, [150]);
  assert.deepEqual(r.phantom, []);
  assert.equal(r.excusedAttempts, 1);
  // No kill in the gap ⇒ still a violation.
  const r2 = checkAttempts(turns, ["a"], 8, [205]);
  assert.deepEqual(r2.phantom, [{ id: "a", seen: [0, 1, 2, 3, 4, 6, 7], missing: [5] }]);
  assert.equal(r2.excusedAttempts, 0);
});

const sample = (t: number, range: number, s: Partial<SchedSample> = {}): SchedSample => ({
  t,
  range,
  waiting: 0,
  eligible: 0,
  turnsInUse: 0,
  locksHeld: 0,
  ...s,
});
const dead = { locksHeld: 2, turnsInUse: 0, eligible: 0, waiting: 5 };

test("deadlock: N consecutive scrapes in a lock-holding, non-running state — violation", () => {
  const rounds = [1, 2, 3].map((t) => [sample(t, 7, dead), sample(t, 8, { waiting: 3, turnsInUse: 1, locksHeld: 1 })]);
  const r = deadLocks(rounds, 3);
  assert.deepEqual(
    r.map((d) => [d.range, d.from, d.to, d.scrapes]),
    [[7, 1, 3, 3]],
  );
});

test("deadlock: the chain breaks when the range runs again or is absent from a scrape", () => {
  const ran = [[sample(1, 7, dead)], [sample(2, 7, { ...dead, turnsInUse: 1 })], [sample(3, 7, dead)], [sample(4, 7, dead)]];
  assert.deepEqual(deadLocks(ran, 3), []);
  const gone = [[sample(1, 7, dead)], [sample(2, 7, dead)], [], [sample(4, 7, dead)]];
  assert.deepEqual(deadLocks(gone, 3), []);
  // The node is held back by the `rate` limit: standing still is per contract, not a deadlock.
  const throttled = [1, 2, 3].map((t) => [sample(t, 7, { ...dead, rateBlocked: true })]);
  assert.deepEqual(deadLocks(throttled, 3), []);
});

test("queues: a range swelling linearly after the load stops is caught, a draining range is not", () => {
  const xs: SchedSample[] = [];
  for (let i = 0; i < 5; i++) {
    xs.push(sample(i * 30_000, 1, { waiting: 100 + 50 * i }));
    xs.push(sample(i * 30_000, 2, { waiting: Math.max(0, 200 - 80 * i) }));
  }
  const slopes = queueSlopes(xs, 0, 200_000);
  assert.deepEqual(
    slopes.map((q) => [q.range, Math.round(q.slopePerMin), queueGrows(q)]),
    [
      [1, 100, true],
      [2, -104, false],
    ],
  );
  // Outside the window it does not count; under three samples there is no verdict.
  assert.deepEqual(
    queueSlopes(xs, 0, 30_000).map(queueGrows),
    [false, false],
  );
});

test("check (b): a failed detail read is retried, after the last attempt it is reported unreadable — never misclassified", async () => {
  const fails = new Map([
    ["flaky", 2],
    ["dead", 99],
  ]);
  const calls: Array<[string, number]> = [];
  const slept: number[] = [];
  const r = await classifyPaused(
    ["p575", "flaky", "dead", "other", "resumed"],
    async (id, attempt) => {
      calls.push([id, attempt]);
      const left = fails.get(id) ?? 0;
      if (left > 0) {
        fails.set(id, left - 1);
        if (id === "dead" && attempt % 2 === 0) throw new Error("ECONNREFUSED");
        return { ok: false, why: "503 not the leader" };
      }
      if (id === "other") return { ok: true, body: { status: "paused", code: 577, message: "stuck on infrastructure" } };
      if (id === "resumed") return { ok: true, body: { status: "completed" } };
      return { ok: true, body: { status: "paused", code: 575, message: "out of 8 attempts" } };
    },
    { tries: 4, backoffMs: 10, sleep: async (ms) => void slept.push(ms) },
  );
  assert.deepEqual(r.paused575, ["p575", "flaky"]);
  assert.deepEqual(r.other.map((o) => [o.id, o.status, o.code]), [
    ["other", "paused", 577],
    ["resumed", "completed", undefined],
  ]);
  assert.deepEqual(r.unreadable.map((u) => u.id), ["dead"]);
  assert.match(r.unreadable[0]!.why, /503|ECONNREFUSED/);
  // Attempts are numbered so the caller can rotate nodes; bounded.
  assert.deepEqual(calls.filter(([id]) => id === "flaky").map(([, a]) => a), [0, 1, 2]);
  assert.equal(calls.filter(([id]) => id === "dead").length, 4);
  assert.deepEqual(slept.slice(0, 2), [10, 20]);
});

/** Reads `[seconds, floor, objects]` of one log → the reads (seconds) reported stalled, with the stalled seconds. */
function floorReports(reads: Array<[number, number, number]>, limitS = 600): Array<[number, number]> {
  const floors = new Map<string, FloorTrack>();
  const out: Array<[number, number]> = [];
  for (const [t, floor, objects] of reads) {
    const step = watchFloor(floors, "n/1:0", floor, objects, t * 1000, limitS);
    if (step.stuckS !== null) out.push([t, step.stuckS]);
  }
  return out;
}

test("floor stalled with objects past the threshold: reported once per stall", () => {
  const reads: Array<[number, number, number]> = [];
  for (let t = 0; t <= 1200; t += 120) reads.push([t, 100, 40]);
  assert.deepEqual(floorReports(reads), [[720, 720]]);
});

test("a floor stalled past half the threshold is noted once, before the report", () => {
  const floors = new Map<string, FloorTrack>();
  const halves: number[] = [];
  const reports: number[] = [];
  for (let t = 0; t <= 1200; t += 120) {
    const step = watchFloor(floors, "n/1:0", 100, 40, t * 1000, 600);
    if (step.halfS != null) halves.push(t);
    if (step.stuckS !== null) reports.push(t);
  }
  assert.deepEqual(halves, [360]);
  assert.deepEqual(reports, [720]);
  // A floor that moves before half the threshold is never noted.
  const moving = new Map<string, FloorTrack>();
  let noted = 0;
  for (let t = 0; t <= 1200; t += 120) {
    if (watchFloor(moving, "n/1:0", t, 40, t * 1000, 600).halfS != null) noted++;
  }
  assert.equal(noted, 0);
});

test("an advancing floor resets the clock", () => {
  assert.deepEqual(
    floorReports([
      [0, 100, 40],
      [480, 100, 40],
      [540, 130, 40],
      [1080, 130, 40],
      [1200, 130, 40],
    ]),
    [[1200, 660]],
  );
});

// A spare node drained itself because of the slow-node detector, handed off
// every range and wrote nothing for ~10 minutes; GC had cleaned everything, so
// the log had 0 objects on the floor. When it stopped draining, the ranges it
// took back wrote new frames to the log; the next read saw them waiting to be
// flushed (normal, < 60 s) while the clock still counted from the last floor
// advance, before the idle stretch: reported "stalled 605 s". An empty log does
// not stall: the clock resets at the latest empty read (an upper bound of when
// the first object appeared).
test("an empty log does not count as stalled: the clock counts from the latest empty read", () => {
  const reads: Array<[number, number, number]> = [[0, 100, 40], [120, 18594, 0]];
  for (let t = 240; t <= 600; t += 120) reads.push([t, 18594, 0]);
  // Draining stops: new objects, waiting to be flushed.
  reads.push([725, 18594, 832]);
  assert.deepEqual(floorReports(reads), []);
  // Stalling further is still caught — counted from the last empty read (600).
  for (let t = 845; t <= 1325; t += 120) reads.push([t, 18594, 832]);
  assert.deepEqual(floorReports(reads), [[1205, 605]]);
});
