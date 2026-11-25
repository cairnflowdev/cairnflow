// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Unit tests of the chaos harness's compact ledger: reads back exactly what
// was written, histogram within its error bound, and the incremental history
// checker reaches the same verdict as `checkHistory` on random histories.
import assert from "node:assert/strict";
import { test } from "node:test";
import { checkHistory, HistoryTracker, type Observation, type ReadSource, type Run } from "./history.ts";
import { AckLedger, Clock32, LogHist, SeqTable } from "./ledger.ts";

/** Seeded PRNG (mulberry32) — repeatable tests. */
function rng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const hex = (r: () => number, bytes: number) =>
  Array.from({ length: bytes }, () => Math.floor(r() * 256).toString(16).padStart(2, "0")).join("");

test("ack ledger: 48-hex ids and odd ids read back correctly, seq correct", () => {
  const r = rng(1);
  const l = new AckLedger();
  const ids: string[] = [];
  for (let i = 0; i < 5000; i++) {
    const id = i % 997 === 0 ? `odd-${i}` : hex(r, 24);
    ids.push(id);
    assert.equal(l.add(id, i * 3), i);
  }
  assert.equal(l.size, 5000);
  for (let i = 0; i < 5000; i++) {
    assert.equal(l.id(i), ids[i]);
    assert.equal(l.seq(i), i * 3);
  }
});

test("seq table: first run compact, later runs verbatim, tenant and handler entries", () => {
  const t = new SeqTable(new Clock32(0));
  const inv = "ab".repeat(24);
  t.noteRun(7, { tag: "x-7", t: 100, nonce: "0102030405ff", attempt: 2, invocation: inv });
  t.noteRun(7, { tag: "x-7", t: 200, nonce: "aaaaaaaaaaaa", attempt: 3, invocation: "other" });
  t.noteRun(9, { tag: "x-9", t: 300 });
  assert.equal(t.runCount(7), 2);
  assert.equal(t.runCount(8), 0);
  assert.deepEqual(t.runsOf(7, "x-7", inv), [
    { tag: "x-7", t: 100, nonce: "0102030405ff", attempt: 2, invocation: inv },
    { tag: "x-7", t: 200, nonce: "aaaaaaaaaaaa", attempt: 3, invocation: "other" },
  ]);
  assert.deepEqual(t.runsOf(9, "x-9", inv), [{ tag: "x-9", t: 300 }]);
  assert.deepEqual(t.runsOf(8, "x-8", inv), []);
  // An acked id other than the first run's id ⇒ no fake match.
  assert.match(t.runsOf(7, "x-7", "cd".repeat(24))[0]!.invocation!, /^\(hash /);
  t.setTenant(7, "chaos-t1");
  t.setTenant(9, "chaos-t2");
  assert.equal(t.tenantOf(7), "chaos-t1");
  assert.equal(t.tenantOf(9), "chaos-t2");
  assert.equal(t.tenantOf(8), undefined);
  t.setWork(7, 123);
  assert.equal(t.workAt(7), 123);
  assert.equal(t.workAt(9), undefined);
});

test("log histogram: quantiles within 1 % (± 1 ms) of the true quantiles, exact max", () => {
  const r = rng(2);
  const xs: number[] = [];
  const h = new LogHist();
  for (let i = 0; i < 50_000; i++) {
    const v = Math.round(r() < 0.9 ? 20 + r() * 200 : r() * 60_000);
    xs.push(v);
    h.add(v);
  }
  xs.sort((a, b) => a - b);
  for (const q of [0.5, 0.9, 0.99]) {
    const exact = xs[Math.min(xs.length - 1, Math.floor(q * xs.length))]!;
    const got = h.quantile(q);
    assert.ok(Math.abs(got - exact) <= Math.max(1, exact * 0.01), `q=${q}: ${got} vs ${exact}`);
  }
  assert.equal(h.quantile(1), xs[xs.length - 1]);
  assert.equal(h.count, 50_000);
  h.reset();
  assert.equal(h.quantile(0.5), 0);
});

/** Random history: reads in time order, side-effect runs per tag. */
function randomHistory(seed: number) {
  const r = rng(seed);
  const n = 60;
  const ids = Array.from({ length: n }, () => hex(r, 24));
  const tags = ids.map((_, i) => `chaos-${i}`);
  const sources: ReadSource[] = ["leader", "follower", "unknown"];
  const observations: Observation[] = [];
  const runs: Run[] = [];
  let t = 1_000;
  for (let step = 0; step < 900; step++) {
    t += 1 + Math.floor(r() * 3);
    const i = Math.floor(r() * n);
    const x = r();
    if (x < 0.15) {
      runs.push({
        tag: tags[i]!,
        t,
        nonce: hex(r, 6),
        attempt: Math.floor(r() * 3),
        invocation: r() < 0.9 ? ids[i]! : r() < 0.5 ? hex(r, 24) : undefined!,
      });
      if (runs[runs.length - 1]!.invocation === undefined) delete runs[runs.length - 1]!.invocation;
      continue;
    }
    const status = x < 0.6 ? "completed" : x < 0.75 ? "running" : x < 0.85 ? "404" : x < 0.92 ? "failed" : "suspended";
    const o: Observation = { id: ids[i]!, t, source: sources[Math.floor(r() * 3)]!, status };
    if (status === "completed") o.output = r() < 0.9 ? `out-${i}` : `out-${i}-${Math.floor(r() * 3)}`;
    observations.push(o);
  }
  return { ids, tags, observations, runs };
}

test("incremental history checker: same verdict as checkHistory on 200 random histories", () => {
  for (let seed = 1; seed <= 200; seed++) {
    const { ids, tags, observations, runs } = randomHistory(seed);
    const tagOf = new Map(ids.map((id, i) => [id, tags[i]!]));
    const want = checkHistory(observations, runs, tagOf);

    const idxOf = new Map(ids.map((id, i) => [id, i]));
    const tr = new HistoryTracker(new Clock32(0));
    for (const o of observations) {
      const { id, ...rest } = o;
      tr.observe(idxOf.get(id)!, id, rest);
    }
    const got = tr.finish(
      ids.length,
      (i) => ids[i]!,
      (i) => {
        // Through `SeqTable` as in `chaos.ts`: first run compact (hashed id), later runs verbatim.
        const st = new SeqTable(new Clock32(0));
        for (const x of runs.filter((x) => x.tag === tags[i])) st.noteRun(i, x);
        return { tag: tags[i]!, runs: st.runsOf(i, tags[i]!, ids[i]!) };
      },
    );
    const key = (a: { kind: string; id: string }) => `${a.kind} ${a.id}`;
    assert.deepEqual(got.anomalies.map(key).sort(), want.anomalies.map(key).sort(), `seed ${seed}`);
    assert.equal(got.completedIds, want.completedIds, `seed ${seed}`);
    assert.equal(got.staleFollowerReads, want.staleFollowerReads, `seed ${seed}`);
    assert.equal(got.goneAfterCompleted, want.goneAfterCompleted, `seed ${seed}`);
    assert.equal(got.duplicateSubmissions, want.duplicateSubmissions, `seed ${seed}`);
    assert.equal(tr.reads, observations.length);
  }
});

test("compact ledger: 400 k calls take under 40 MiB (the harness's flat-heap criterion: ≤ ~50 MiB/hour)", () => {
  const r = rng(3);
  const l = new AckLedger();
  const t = new SeqTable();
  const h = new HistoryTracker();
  const now = Date.now();
  for (let seq = 0; seq < 400_000; seq++) {
    const id = hex(r, 24);
    const idx = l.add(id, seq);
    if (seq % 4 === 0) t.setTenant(seq, `chaos-t${seq % 3}`);
    t.noteRun(seq, { tag: `chaos-t${seq % 4}-${seq}`, t: now + seq, nonce: hex(r, 6), attempt: 0, invocation: id });
    h.observe(idx, id, { t: now + seq + 5, source: "leader", status: "completed", output: `{"seq":${seq}}` });
  }
  const mib = (l.bytes() + t.bytes() + h.bytes()) / (1 << 20);
  assert.ok(mib < 40, `compact ledger takes ${mib.toFixed(1)} MiB for 400 k calls`);
  assert.equal(l.id(123_456).length, 48);
  assert.equal(t.runsOf(123_456, "x", l.id(123_456))[0]!.invocation, l.id(123_456));
  assert.equal(h.firstCompletedAt(123_456), now + 123_456 + 5);
});

test("ack ledger: the ack time reads back to the millisecond", () => {
  const base = Date.now() - 3_600_000;
  const l = new AckLedger(new Clock32(base));
  l.add("a".repeat(48), 1, base);
  l.add("b".repeat(48), 2, base + 86_400_000 * 2);
  l.add("c".repeat(48), 3);
  assert.equal(l.ackedAt(0), base);
  assert.equal(l.ackedAt(1), base + 86_400_000 * 2);
  assert.ok(Math.abs(l.ackedAt(2)! - Date.now()) < 5_000);
  assert.equal(l.ackedAt(3), undefined);
});
