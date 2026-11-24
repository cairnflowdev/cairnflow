// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

/**
 * Report of `latency-curve.ts`: reads the JSONL file, prints a Markdown table and
 * the "read-back" coefficients — a least-squares line `ack ≈ a + k · PUT p50` per
 * window — then draws an SVG figure next to the result file.
 *
 * ```sh
 * node harness/bench/latency-curve-report.ts bench/s3-model/results/2026-10-12-latency-curve.jsonl
 * ```
 *
 * Measures nothing, makes no network calls: every rerun prints the same table.
 */

import { readFileSync, writeFileSync } from "node:fs";

interface Phase {
  n: number;
  fail: number;
  rps: number;
  p50: number;
  p99: number;
}

interface Row {
  put_p50_ms: number;
  put_p99_ms: number;
  get_p50_ms: number;
  wal_window_ms: number;
  send: Phase;
  echo: Phase;
  run1: Phase;
  tput: Phase;
  wal_put_mean_ms: number | null;
  store_per_invocation?: Record<string, number>;
}

const file = process.argv[2];
if (!file) {
  console.error("usage: latency-curve-report.ts <result.jsonl> [figure.svg]");
  process.exit(2);
}
const svgPath = process.argv[3] ?? file.replace(/\.jsonl$/, ".svg");
const rows = readFileSync(file, "utf8")
  .split("\n")
  .filter((l) => l.trim())
  .map((l) => JSON.parse(l) as Row);

const f = (x: number, d = 0) => x.toFixed(d);
const windows = [...new Set(rows.map((r) => r.wal_window_ms))].sort((a, b) => a - b);
const puts = [...new Set(rows.map((r) => r.put_p50_ms))].sort((a, b) => a - b);
const at = (w: number, p: number) => rows.filter((r) => r.wal_window_ms === w && r.put_p50_ms === p).at(-1);

// ── Full table per window
for (const w of windows) {
  console.log(`\n**WAL window ${w} ms**\n`);
  console.log(
    "| PUT p50 / p99 (ms) | ack `/send` p50 / p99 | `echo` p50 / p99 | `ctx.run` p50 / p99 | 1-range throughput (inv/s, 64 loops) | measured WAL PUT (mean, ms) |",
  );
  console.log("|---|---|---|---|---|---|");
  for (const p of puts) {
    const r = at(w, p);
    if (!r) continue;
    const pair = (x: Phase) => `${f(x.p50)} / ${f(x.p99)}`;
    console.log(
      `| ${p === 0 ? "0 (baseline)" : `${p} / ${r.put_p99_ms}`} | ${pair(r.send)} | ${pair(r.echo)} | ${pair(r.run1)} | ` +
        `${f(r.tput.rps)} | ${r.wal_put_mean_ms === null ? "—" : f(r.wal_put_mean_ms, 1)} |`,
    );
  }
}

// ── Read-back: a line through the points with injected latency
function fit(xs: number[], ys: number[]): { a: number; k: number; r2: number } {
  const n = xs.length;
  const mx = xs.reduce((s, x) => s + x, 0) / n;
  const my = ys.reduce((s, y) => s + y, 0) / n;
  let sxy = 0;
  let sxx = 0;
  let syy = 0;
  for (let i = 0; i < n; i++) {
    sxy += (xs[i]! - mx) * (ys[i]! - my);
    sxx += (xs[i]! - mx) ** 2;
    syy += (ys[i]! - my) ** 2;
  }
  const k = sxy / sxx;
  return { a: my - k * mx, k, r2: (sxy * sxy) / (sxx * syy) };
}

console.log("\n**Read-back** — `ack ≈ a + k · PUT p50` (ms), least squares over the points with PUT p50 > 0\n");
console.log("| window | measure | p50: a + k·PUT (R²) | p99: a + k·PUT (R²) |");
console.log("|---|---|---|---|");
for (const w of windows) {
  const pts = puts.filter((p) => p > 0).map((p) => at(w, p)).filter((r): r is Row => !!r);
  if (pts.length < 2) continue;
  for (const [name, get] of [
    ["ack `/send`", (r: Row) => r.send],
    ["`echo`", (r: Row) => r.echo],
    ["`ctx.run`", (r: Row) => r.run1],
  ] as const) {
    const x = pts.map((r) => r.put_p50_ms);
    const a = fit(x, pts.map((r) => get(r).p50));
    const b = fit(x, pts.map((r) => get(r).p99));
    console.log(
      `| ${w} ms | ${name} | ${f(a.a)} + ${f(a.k, 2)}·PUT (${f(a.r2, 3)}) | ${f(b.a)} + ${f(b.k, 2)}·PUT (${f(b.r2, 3)}) |`,
    );
  }
}

// ── Figure: p50 against PUT p50 at the default window (10 ms, or the middle window)
const w0 = windows.includes(10) ? 10 : windows[Math.floor(windows.length / 2)]!;
const series = [
  { name: "ack /send", color: "var(--s1)", get: (r: Row) => r.send.p50 },
  { name: "echo", color: "var(--s2)", get: (r: Row) => r.echo.p50 },
  { name: "ctx.run", color: "var(--s3)", get: (r: Row) => r.run1.p50 },
];
const pts = puts.map((p) => at(w0, p)).filter((r): r is Row => !!r);
const W = 640;
const H = 360;
const m = { l: 56, r: 96, t: 52, b: 48 };
const xMax = Math.max(...pts.map((r) => r.put_p50_ms));
const yMaxRaw = Math.max(...pts.flatMap((r) => series.map((s) => s.get(r))));
const yStep = yMaxRaw > 800 ? 200 : yMaxRaw > 400 ? 100 : 50;
const yMax = Math.ceil(yMaxRaw / yStep) * yStep;
const X = (v: number) => m.l + (v / xMax) * (W - m.l - m.r);
const Y = (v: number) => H - m.b - (v / yMax) * (H - m.t - m.b);
const out: string[] = [];
out.push(
  `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" font-family="system-ui, sans-serif" font-size="12" role="img" aria-label="cairn p50 latency against the store's PUT p50, WAL window ${w0} ms">`,
  `<style>
  svg { --bg:#fcfcfb; --ink:#0b0b0b; --ink2:#52514e; --muted:#898781; --grid:#e1e0d9; --axis:#c3c2b7; --s1:#2a78d6; --s2:#eb6834; --s3:#1baf7a; }
  @media (prefers-color-scheme: dark) { svg { --bg:#1a1a19; --ink:#ffffff; --ink2:#c3c2b7; --grid:#2c2c2a; --axis:#383835; --s1:#3987e5; --s2:#d95926; --s3:#199e70; } }
</style>`,
  `<rect width="${W}" height="${H}" fill="var(--bg)"/>`,
  `<text x="${m.l}" y="20" fill="var(--ink)" font-size="14" font-weight="600">cairn p50 latency against the store's PUT p50 (WAL window ${w0} ms, p99 = 4× p50)</text>`,
);
for (let y = 0; y <= yMax; y += yStep) {
  out.push(
    `<line x1="${m.l}" x2="${W - m.r}" y1="${Y(y)}" y2="${Y(y)}" stroke="${y === 0 ? "var(--axis)" : "var(--grid)"}" stroke-width="1"/>`,
    `<text x="${m.l - 8}" y="${Y(y) + 4}" text-anchor="end" fill="var(--muted)">${y}</text>`,
  );
}
// x-axis labels: skip a label too close to the previous one (0, 5, 10 bunch up on a linear axis).
let lastLabel = -Infinity;
for (const p of puts) {
  if (!pts.some((r) => r.put_p50_ms === p) || X(p) - lastLabel < 28) continue;
  lastLabel = X(p);
  out.push(`<text x="${X(p)}" y="${H - m.b + 18}" text-anchor="middle" fill="var(--muted)">${p}</text>`);
}
// A one-row legend under the title; the direct labels at the end of each line stay.
series.forEach((s, i) => {
  const x = m.l + i * 110;
  out.push(
    `<line x1="${x}" x2="${x + 16}" y1="32" y2="32" stroke="${s.color}" stroke-width="2"/>`,
    `<text x="${x + 22}" y="36" fill="var(--ink2)">${s.name}</text>`,
  );
});
out.push(
  `<text x="${(m.l + W - m.r) / 2}" y="${H - 10}" text-anchor="middle" fill="var(--ink2)">PUT p50 injected into the store (ms)</text>`,
  `<text x="14" y="${(m.t + H - m.b) / 2}" text-anchor="middle" fill="var(--ink2)" transform="rotate(-90 14 ${(m.t + H - m.b) / 2})">ms</text>`,
);
for (const s of series) {
  const d = pts.map((r, i) => `${i === 0 ? "M" : "L"}${f(X(r.put_p50_ms), 1)},${f(Y(s.get(r)), 1)}`).join(" ");
  out.push(`<path d="${d}" fill="none" stroke="${s.color}" stroke-width="2" stroke-linejoin="round"/>`);
  for (const r of pts) {
    out.push(
      `<circle cx="${f(X(r.put_p50_ms), 1)}" cy="${f(Y(s.get(r)), 1)}" r="4" fill="${s.color}" stroke="var(--bg)" stroke-width="2"><title>${s.name}: PUT p50 ${r.put_p50_ms} ms → ${f(s.get(r))} ms</title></circle>`,
    );
  }
  const last = pts.at(-1)!;
  out.push(
    `<text x="${f(X(last.put_p50_ms) + 8, 1)}" y="${f(Y(s.get(last)) + 4, 1)}" fill="var(--ink)">${s.name} ${f(s.get(last))}</text>`,
  );
}
out.push("</svg>");
writeFileSync(svgPath, out.join("\n") + "\n");
console.log(`\nfigure: ${svgPath}`);
