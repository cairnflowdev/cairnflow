// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

/**
 * Idle cost by caller (`io_source`) from the results of `s3-cost.ts
 * --scenarios idle`. Prints markdown.
 *
 * ```sh
 * node harness/bench/idle-sources.ts bench/s3-model/results/<run>.jsonl... [--skip 300]
 * ```
 *
 * **Steady state**: `s3-cost.ts` records cumulative counters every `COST_SLICE_S`
 * seconds (`slices`). The tables take the part from the first slice with
 * `t ≥ --skip` (default 300 s) to the end of the window, so the start-up tail
 * (`range.open`, the first round of every background loop) does not mix in. A run
 * without slices uses the whole window, and the table says so.
 *
 * Five tables: (1) total Class A (PUT, LIST, COPY) / Class B (GET, HEAD) per hour,
 * whole window and steady; (2) by source × op, per hour, cluster; (3) **background
 * cost per range**: requests carrying a range label
 * (`cairnflow_testkit_store_requests_by_range_total` — the range's Db and its
 * SlateDB tasks) by source × op, per hour, divided by the number of ranges; (4) like
 * (3) over the whole window, start-up tail included; (5) **marginal cost per range**
 * by source when two runs have the same node count and different range counts
 * (also covering cairn's loops that touch each range, which the range label does
 * not cover). Counts are requests (an idle LIST is nearly always one page).
 */

import { readFileSync } from "node:fs";

type Counts = Record<string, number>;
type Row = {
  scenario: string;
  nodes: number;
  partitions: number;
  seconds: number;
  counts: Counts;
  slices?: { t: number; counts: Counts }[];
};

const argv = process.argv.slice(2);
const si = argv.indexOf("--skip");
const SKIP = si >= 0 ? Number(argv[si + 1]) : 300;
const files = argv.filter((a, i) => !a.startsWith("--") && argv[i - 1] !== "--skip");
if (files.length === 0) {
  console.error("usage: node harness/bench/idle-sources.ts <result.jsonl>... [--skip seconds]");
  process.exit(2);
}

const rows: Row[] = files
  .flatMap((f) =>
    readFileSync(f, "utf8")
      .split("\n")
      .filter((l) => l.trim())
      .map((l) => JSON.parse(l) as Row),
  )
  .filter((r) => r.scenario === "idle");

function parse(key: string): [string, Record<string, string>] {
  const m = /^([a-z_]+)(?:\{(.*)\})?$/.exec(key);
  if (!m) return [key, {}];
  const labels: Record<string, string> = {};
  for (const kv of (m[2] ?? "").matchAll(/(\w+)="([^"]*)"/g)) labels[kv[1]!] = kv[2]!;
  return [m[1]!, labels];
}

const CLASS_A = new Set(["put", "put_multipart", "copy", "rename", "list"]);
const CLASS_B = new Set(["get", "head"]);

/** The steady part of a run: counters and seconds. */
function steady(r: Row): { counts: Counts; seconds: number; from: number } {
  const base = (r.slices ?? []).filter((s) => s.t >= SKIP).sort((a, b) => a.t - b.t)[0];
  if (!base) return { counts: r.counts, seconds: r.seconds, from: 0 };
  const counts: Counts = {};
  for (const k of new Set([...Object.keys(r.counts), ...Object.keys(base.counts)])) {
    const d = (r.counts[k] ?? 0) - (base.counts[k] ?? 0);
    if (d !== 0) counts[k] = d;
  }
  return { counts, seconds: r.seconds - base.t, from: base.t };
}

/** Requests per hour by `key(labels)` of a family. */
function perHour(c: Counts, seconds: number, family: string, key: (l: Record<string, string>) => string | undefined) {
  const out: Record<string, number> = {};
  for (const [k, v] of Object.entries(c)) {
    const [name, l] = parse(k);
    if (name !== family) continue;
    const g = key(l);
    if (g !== undefined) out[g] = (out[g] ?? 0) + (v * 3600) / seconds;
  }
  return out;
}

function classes(c: Counts, seconds: number) {
  const ops = perHour(c, seconds, "cairnflow_testkit_store_requests_total", (l) => l.op);
  let a = 0;
  let b = 0;
  for (const [op, n] of Object.entries(ops)) {
    if (CLASS_A.has(op)) a += n;
    else if (CLASS_B.has(op)) b += n;
  }
  return { a, b };
}

const fmt = (n: number | undefined) =>
  n === undefined || n === 0 ? "" : n >= 10 ? Math.round(n).toLocaleString("en-US") : n.toFixed(1);
const name = (r: Row) => `${r.nodes}n×${r.partitions}d`;
const opClass = (op: string) => (CLASS_A.has(op) ? "A" : CLASS_B.has(op) ? "B" : "-");

console.log(`# Idle cost by source — ${files.join(", ")}\n`);
console.log(`Steady = from the first slice with t ≥ ${SKIP} s to the end of the window. Unit: requests per hour.\n`);

console.log("## Total\n");
console.log("| run | window (s) | Class A/h whole window | Class B/h whole window | steady from (s) | Class A/h steady | Class B/h steady |");
console.log("|---|---|---|---|---|---|---|");
for (const r of rows) {
  const all = classes(r.counts, r.seconds);
  const s = steady(r);
  const st = classes(s.counts, s.seconds);
  console.log(
    `| ${name(r)} | ${Math.round(r.seconds)} | ${fmt(all.a)} | ${fmt(all.b)} | ${s.from || "— (no slices)"} | ${fmt(st.a)} | ${fmt(st.b)} |`,
  );
}

function table(title: string, family: string, scale: (r: Row) => number, whole = false) {
  const per = rows.map((r) => {
    const s = whole ? { counts: r.counts, seconds: r.seconds } : steady(r);
    const h = perHour(s.counts, s.seconds, family, (l) => (l.source ? `${l.source}\t${l.op}` : undefined));
    const k = scale(r);
    for (const g of Object.keys(h)) h[g]! /= k;
    return h;
  });
  const keys = [...new Set(per.flatMap((h) => Object.keys(h)))].sort((x, y) => {
    const mx = Math.max(...per.map((h) => h[x] ?? 0));
    const my = Math.max(...per.map((h) => h[y] ?? 0));
    return my - mx;
  });
  console.log(`\n## ${title}\n`);
  if (keys.length === 0) {
    console.log("No requests.");
    return;
  }
  console.log(`| source | op | class | ${rows.map(name).join(" | ")} |`);
  console.log(`|---|---|---|${rows.map(() => "---").join("|")}|`);
  for (const g of keys) {
    const [src, op] = g.split("\t") as [string, string];
    console.log(`| \`${src}\` | ${op} | ${opClass(op)} | ${per.map((h) => fmt(h[g])).join(" | ")} |`);
  }
  const sum = (cls: Set<string>) =>
    per.map((h) =>
      fmt(Object.entries(h).reduce((a, [g, n]) => (cls.has(g.split("\t")[1]!) ? a + n : a), 0)),
    );
  console.log(`| **Class A** | | | ${sum(CLASS_A).join(" | ")} |`);
  console.log(`| **Class B** | | | ${sum(CLASS_B).join(" | ")} |`);
}

table("By source, whole cluster (steady, /h)", "cairnflow_testkit_store_requests_by_source_total", () => 1);
table(
  "Background cost per range (steady, /h per range — range-labelled requests divided by the number of ranges)",
  "cairnflow_testkit_store_requests_by_range_total",
  (r) => r.partitions,
);
table(
  "Background cost per range, whole window including the start-up tail (/h per range)",
  "cairnflow_testkit_store_requests_by_range_total",
  (r) => r.partitions,
  true,
);

// Marginal cost per range by source: two runs with the same node count, different range
// counts. The per-node part cancels out, what remains grows with ranges — including cairn's
// loops touching each range (`gc.orphans`, `scrub`, `nodewal.gc`), which the range label does not cover.
for (const n of [...new Set(rows.map((r) => r.nodes))]) {
  const same = rows.filter((r) => r.nodes === n).sort((a, b) => a.partitions - b.partitions);
  if (same.length < 2) continue;
  const lo = same[0]!;
  const hi = same[same.length - 1]!;
  if (lo.partitions === hi.partitions) continue;
  const rate = (r: Row) => {
    const s = steady(r);
    return perHour(s.counts, s.seconds, "cairnflow_testkit_store_requests_by_source_total", (l) =>
      l.source ? `${l.source}\t${l.op}` : undefined,
    );
  };
  const a = rate(lo);
  const b = rate(hi);
  const dp = hi.partitions - lo.partitions;
  const marginal: Record<string, number> = {};
  for (const g of new Set([...Object.keys(a), ...Object.keys(b)])) {
    marginal[g] = ((b[g] ?? 0) - (a[g] ?? 0)) / dp;
  }
  const keys = Object.keys(marginal)
    .filter((g) => Math.abs(marginal[g]!) >= 0.5)
    .sort((x, y) => marginal[y]! - marginal[x]!);
  console.log(
    `\n## Marginal cost per range, ${n} node (steady, /h per extra range: (${name(hi)} − ${name(lo)}) ÷ ${dp})\n`,
  );
  console.log("| source | op | class | /h per range |");
  console.log("|---|---|---|---|");
  for (const g of keys) {
    const [src, op] = g.split("\t") as [string, string];
    console.log(`| \`${src}\` | ${op} | ${opClass(op)} | ${marginal[g]!.toFixed(1)} |`);
  }
  const sum = (cls: Set<string>) =>
    Object.entries(marginal).reduce((acc, [g, v]) => (cls.has(g.split("\t")[1]!) ? acc + v : acc), 0);
  console.log(`| **Class A** | | | ${sum(CLASS_A).toFixed(1)} |`);
  console.log(`| **Class B** | | | ${sum(CLASS_B).toFixed(1)} |`);
}
