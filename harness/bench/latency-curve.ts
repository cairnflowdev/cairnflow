// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

/**
 * Sensitivity curve: what function of the object store's latency is cairn's
 * latency?
 *
 * Sweeps the latency injected into the store (PUT p50 × a tail factor, GET and the
 * other ops in fixed ratios) × the shared WAL flush window. Each point is a fresh
 * node (`--cluster --partitions 1 --wal-kind shared`) on a clean store, measuring four things:
 *
 * | phase | measures |
 * |---|---|
 * | `send` | the ack of `/send`: the invocation is durable, not yet run — **one** commit |
 * | `echo` | `/invoke` of a handler with no step besides its result, waiting until done |
 * | `run1` | `/invoke` of a handler with one `ctx.run`, waiting until done |
 * | `tput` | `echo` at `CURVE_TPUT_CONC` concurrent loops: the throughput of **one** range |
 *
 * ```sh
 * # A release binary WITH the testkit feature (reads CAIRNFLOW_STORE_PROFILE):
 * cargo build --release -p cairn-server --features testkit --bin cairnflow
 * node harness/bench/latency-curve.ts            # the whole grid, ~30 minutes
 * CURVE_PUT_P50=0,20 CURVE_WINDOWS=10 node harness/bench/latency-curve.ts
 * # Table + figure:
 * node harness/bench/latency-curve-report.ts bench/s3-model/results/<date>-latency-curve.jsonl
 * ```
 *
 * Variants worth running (each with its own `CURVE_OUT` file): a long tail
 * `CURVE_TAIL=10`, several PUTs in flight `CURVE_WAL_INFLIGHT=4`, high
 * concurrency `CURVE_TPUT_CONC=256`, and a ready-made profile
 * `CURVE_PROFILE=<file>` to check the read-back.
 *
 * **The store is `memory://`**, not SeaweedFS: the store's baseline latency must be
 * small compared with the injected latency, and `memory://` has a baseline ≈ 0 (the
 * grid's `0` point measures exactly that), while a local SeaweedFS adds 1–5 ms per
 * request and a tail caused by the machine doing other work. The path through
 * `object_store`'s HTTP client is therefore **not** measured — that is a separate measurement.
 *
 * The injected latency is lognormal by p50/p99 (`Action::Latency`, seeded), plugged
 * in through a JSON profile generated per point (`StoreProfile`). The ratios between
 * ops come from the `bench/s3-model/profiles/s3-standard.json` profile (an
 * assumption): GET p50 = 0.6 × PUT p50; HEAD = GET;
 * LIST = 1.2 × PUT; DELETE = 0.8 × PUT. **Conditional PUT = PUT**: `OpSet`
 * does not separate the two, and the PUT on the ack path (the shared WAL log
 * object) is a conditional PUT anyway (`If-None-Match`).
 *
 * Each result line is appended to `CURVE_OUT` (JSONL) as soon as it is measured, so
 * an interrupted run keeps the points it already has.
 */

import { spawn, type ChildProcess } from "node:child_process";
import { appendFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { Agent, request as httpRequest } from "node:http";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = fileURLToPath(new URL(".", import.meta.url));
const repoRoot = join(here, "..", "..");

const list = (v: string) => v.split(",").map(Number).filter((x) => Number.isFinite(x) && x >= 0);

const BINARY = process.env.CURVE_BINARY ?? join(repoRoot, "target", "release", "cairnflow");
/** PUT p50 (ms) of each point. `0` = nothing injected (the baseline latency of `memory://`). */
/**
 * A ready-made store profile (for example `bench/s3-model/profiles/s3-standard.json`)
 * instead of the grid: measures exactly one point per window, to check the grid's
 * "read-back" on a distribution other than lognormal. `CURVE_PUT_P50` is ignored.
 */
const FIXED_PROFILE = process.env.CURVE_PROFILE;
const fixed = FIXED_PROFILE
  ? (JSON.parse(readFileSync(FIXED_PROFILE, "utf8")) as {
      name?: string;
      latency?: { put?: { p50_ms: number; p99_ms: number }; get?: { p50_ms: number } };
    })
  : undefined;
const PUT_P50 = fixed ? [fixed.latency?.put?.p50_ms ?? 0] : list(process.env.CURVE_PUT_P50 ?? "0,5,10,20,50,100,200");
/** p99 = p50 × this factor, for every op. */
const TAIL = Number(process.env.CURVE_TAIL ?? "4");
const GET_RATIO = Number(process.env.CURVE_GET_RATIO ?? "0.6");
/** Shared WAL flush window (ms), `--wal-window-ms`. */
const WINDOWS = list(process.env.CURVE_WINDOWS ?? "5,10,20");
/**
 * PUTs in flight per log stream of the shared WAL (`--wal-inflight`, W). Default 1 as
 * in the binary: a commit arriving while the stream has a PUT in flight must wait for
 * that PUT to finish before issuing its own, so on a slow store this is the biggest lever.
 */
const WAL_INFLIGHT = Number(process.env.CURVE_WAL_INFLIGHT ?? "1");
/** Concurrent loops of the latency phases — enough for samples, not enough to queue. */
const LAT_CONC = Number(process.env.CURVE_LAT_CONC ?? "8");
const TPUT_CONC = Number(process.env.CURVE_TPUT_CONC ?? "64");
/** Each latency phase runs until it has this many samples, within [MIN_S, MAX_S] seconds. */
const MIN_SAMPLES = Number(process.env.CURVE_MIN_SAMPLES ?? "400");
const MIN_S = Number(process.env.CURVE_MIN_S ?? "8");
const MAX_S = Number(process.env.CURVE_MAX_S ?? "40");
const TPUT_S = Number(process.env.CURVE_TPUT_S ?? "10");
const WARMUP_S = Number(process.env.CURVE_WARMUP_S ?? "3");
const SEED = Number(process.env.CURVE_SEED ?? "1");
const OUT =
  process.env.CURVE_OUT ??
  join(repoRoot, "bench", "s3-model", "results", `${new Date().toISOString().slice(0, 10)}-latency-curve.jsonl`);

// ───────────────────────────────────────────── HTTP (like load.ts: a self-declared pool)

const agent = new Agent({ keepAlive: true, maxSockets: 256, maxFreeSockets: 256 });

function http(method: string, url: string, body?: string): Promise<{ status: number; body: string }> {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const req = httpRequest(
      {
        agent,
        method,
        hostname: u.hostname,
        port: u.port,
        path: u.pathname + u.search,
        headers: body === undefined ? {} : { "content-type": "application/json", "content-length": Buffer.byteLength(body) },
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (c: Buffer) => chunks.push(c));
        res.on("end", () => resolve({ status: res.statusCode ?? 0, body: Buffer.concat(chunks).toString() }));
      },
    );
    req.on("error", reject);
    req.end(body);
  });
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function firstMatch(proc: ChildProcess, re: RegExp, what: string): Promise<string> {
  return new Promise((resolve, reject) => {
    let buf = "";
    const t = setTimeout(() => reject(new Error(`${what} did not report its port:\n${buf}`)), 60_000);
    proc.stdout!.on("data", (c: Buffer) => {
      buf += c.toString();
      const m = re.exec(buf);
      if (m) {
        clearTimeout(t);
        resolve(m[0]);
      }
    });
    proc.on("exit", (code) => reject(new Error(`${what} exited early (${code}):\n${buf}`)));
  });
}

async function metrics(server: string): Promise<Map<string, number>> {
  const r = await http("GET", `${server}/metrics`);
  const m = new Map<string, number>();
  for (const line of r.body.split("\n")) {
    if (line.startsWith("#") || !line.trim()) continue;
    const sp = line.lastIndexOf(" ");
    m.set(line.slice(0, sp), Number(line.slice(sp + 1)));
  }
  return m;
}

// ───────────────────────────────────────────── the store profile of one point

function profileFor(putP50: number): object {
  if (fixed) return fixed;
  if (putP50 === 0) return { name: "curve-0", note: "count only — the baseline latency of memory://" };
  const d = (p50: number) => ({ p50_ms: p50, p99_ms: p50 * TAIL });
  const get = putP50 * GET_RATIO;
  return {
    name: `curve-put${putP50}`,
    note:
      `latency-curve.ts: PUT p50 ${putP50} ms, p99 = ${TAIL}× p50, GET = ${GET_RATIO}× PUT; ` +
      "op ratios taken from s3-standard.json (an assumption). Generated, do not edit by hand.",
    seed: SEED,
    latency: {
      put: d(putP50),
      get: d(get),
      head: d(get),
      list: d(putP50 * 1.2),
      delete: d(putP50 * 0.8),
      all: d(putP50),
    },
  };
}

// ───────────────────────────────────────────── one node

interface Node {
  server: string;
  stop(): void;
}

let deployment: ChildProcess | undefined;
let sdkUrl = "";

async function startNode(putP50: number, windowMs: number, dir: string): Promise<Node> {
  const profilePath = join(dir, `profile-${putP50}.json`);
  writeFileSync(profilePath, JSON.stringify(profileFor(putP50), null, 2));
  const proc = spawn(
    BINARY,
    [
      "--main-store", "memory://",
      "--cluster", "--partitions", "1",
      "--wal-kind", "shared",
      "--wal-window-ms", String(windowMs),
      "--wal-inflight", String(WAL_INFLIGHT),
      "--port", "0",
      "--node-id", "curve-node",
      "--no-auth",
      "--max-concurrent-turns", String(Math.max(TPUT_CONC, LAT_CONC) * 2),
      "--queue-wait-ms", "60000",
      "--invoke-wait-ms", "60000",
    ],
    { stdio: ["ignore", "pipe", "pipe"], env: { ...process.env, CAIRNFLOW_STORE_PROFILE: profilePath } },
  );
  let stderr = "";
  proc.stderr!.on("data", (c: Buffer) => {
    stderr = (stderr + c.toString()).slice(-4000);
  });
  const server = await firstMatch(proc, /http:\/\/[\d.]+:\d+/, "cairnflow");
  const node = { server, stop: () => proc.kill("SIGKILL") };
  try {
    const deadline = Date.now() + 60_000;
    for (;;) {
      const r = await http("GET", `${server}/cluster`).catch(() => undefined);
      if (r?.status === 200) {
        const c = JSON.parse(r.body) as { partitions: { leader?: unknown }[] };
        if (c.partitions.length > 0 && c.partitions.every((p) => p.leader)) break;
      }
      if (Date.now() > deadline) throw new Error(`range has no leader after 60 s\n${stderr}`);
      await sleep(100);
    }
    const m = await metrics(server);
    if (![...m.keys()].some((k) => k.startsWith("cairnflow_testkit_store_"))) {
      throw new Error(
        `${BINARY} lacks the testkit feature (no cairnflow_testkit_store_* on /metrics) — ` +
          "the latency profile is ignored. Build: cargo build --release -p cairn-server --features testkit --bin cairnflow",
      );
    }
    const reg = await http("POST", `${server}/deployments`, JSON.stringify({ uri: sdkUrl }));
    if (reg.status !== 200) throw new Error(`deployment registration failed: ${reg.status} ${reg.body}`);
    return node;
  } catch (e) {
    node.stop();
    throw e;
  }
}

// ───────────────────────────────────────────── load loop

interface Phase {
  n: number;
  fail: number;
  firstErr?: string;
  /** Calls **completed within the window** divided by the window's length (see the throughput note in load.ts). */
  rps: number;
  p50: number;
  p90: number;
  p99: number;
  max: number;
  windowS: number;
}

function pct(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))]!;
}

/**
 * Closed loop with `conc` loops, running at least `minS` seconds, then until there are
 * `minSamples` samples or `maxS` is reached. Throughput counts only calls completed
 * **within** the window `[t0, window end]`; a tail of calls still in flight when the
 * window closes does not stretch the denominator.
 */
async function drive(
  fire: (n: number) => Promise<{ ok: boolean; err?: string }>,
  conc: number,
  minS: number,
  maxS: number,
  minSamples: number,
  cap = Infinity,
): Promise<Phase> {
  const t0 = performance.now();
  let closed = false;
  let closeAt = Infinity;
  let issued = 0;
  const lat: number[] = [];
  let inWindow = 0;
  let fail = 0;
  let firstErr: string | undefined;
  const isOpen = () => {
    if (closed) return false;
    const el = (performance.now() - t0) / 1000;
    if (el >= maxS || (el >= minS && lat.length >= minSamples) || issued >= cap) {
      closed = true;
      closeAt = performance.now();
      return false;
    }
    return true;
  };
  await Promise.all(
    Array.from({ length: conc }, async () => {
      while (isOpen()) {
        const n = issued++;
        const s = performance.now();
        const r = await fire(n).catch((e: unknown) => ({ ok: false, err: String(e) }));
        const e = performance.now();
        if (!closed || e <= closeAt) inWindow++;
        if (r.ok) lat.push(e - s);
        else {
          fail++;
          firstErr ??= r.err;
        }
      }
    }),
  );
  const windowS = (closeAt - t0) / 1000;
  lat.sort((a, b) => a - b);
  return {
    n: lat.length + fail,
    fail,
    ...(firstErr ? { firstErr } : {}),
    rps: inWindow / windowS,
    p50: pct(lat, 50),
    p90: pct(lat, 90),
    p99: pct(lat, 99),
    max: lat[lat.length - 1] ?? 0,
    windowS,
  };
}

let tagSeq = 0;
function invoke(server: string, handler: string, suffix = "") {
  return async (): Promise<{ ok: boolean; err?: string }> => {
    const n = tagSeq++;
    const r = await http("POST", `${server}/invoke/LoadService/${handler}${suffix}`, JSON.stringify({ tag: `c/${n}`, n }));
    if (suffix === "/send") return r.status === 200 || r.status === 202 ? { ok: true } : { ok: false, err: `${r.status} ${r.body.slice(0, 120)}` };
    if (r.status !== 200) return { ok: false, err: `${r.status} ${r.body.slice(0, 120)}` };
    const j = JSON.parse(r.body) as { status?: string };
    return j.status === "completed" ? { ok: true } : { ok: false, err: `status=${j.status}` };
  };
}

async function quiesce(server: string, timeoutMs = 120_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    let busy = 0;
    for (const s of ["running", "suspended"]) {
      const r = await http("GET", `${server}/invocations?status=${s}&limit=1000`);
      busy += (JSON.parse(r.body) as unknown[]).length;
    }
    if (busy === 0 || Date.now() > deadline) return;
    await sleep(200);
  }
}

// ───────────────────────────────────────────── run

const f = (x: number, d = 1) => x.toFixed(d);

deployment = spawn(process.execPath, [join(here, "deployment.ts")], { stdio: ["ignore", "pipe", "inherit"] });
const dline = await firstMatch(deployment, /\{"sdk".*\}/, "deployment");
sdkUrl = (JSON.parse(dline) as { sdk: string }).sdk;
const dir = mkdtempSync(join(tmpdir(), "cairnflow-curve-"));
mkdirSync(dirname(OUT), { recursive: true });

console.log(
  `latency-curve — PUT p50 ${PUT_P50.join(",")} ms × window ${WINDOWS.join(",")} ms; ` +
    `p99 = ${TAIL}× p50, GET = ${GET_RATIO}× PUT; W = ${WAL_INFLIGHT}; measuring loops ${LAT_CONC}, throughput at ${TPUT_CONC}\n` +
    `binary ${BINARY}\nresults appended to ${OUT}\n`,
);
console.log(
  "put  win  | send p50/p99   | echo p50/p99     | run1 p50/p99     | tput inv/s (p50) | PUT WAL mean | GET/inv",
);

let failed = false;
try {
  for (const windowMs of WINDOWS) {
    for (const putP50 of PUT_P50) {
      const node = await startNode(putP50, windowMs, dir);
      try {
        const s = node.server;
        // Warm-up: SlateDB, the SDK connection, the JIT. Not counted.
        await drive(invoke(s, "run1"), LAT_CONC, WARMUP_S, WARMUP_S, 0);
        await quiesce(s);
        const m0 = await metrics(s);
        const echo = await drive(invoke(s, "echo"), LAT_CONC, MIN_S, MAX_S, MIN_SAMPLES);
        const run1 = await drive(invoke(s, "run1"), LAT_CONC, MIN_S, MAX_S, MIN_SAMPLES);
        const tput = await drive(invoke(s, "echo"), TPUT_CONC, TPUT_S, TPUT_S, 0);
        await quiesce(s);
        // `/send` goes last: accepted work runs later, and a draining queue would
        // dirty every phase after it. A cap on the number of calls keeps the queue short.
        const send = await drive(invoke(s, "echo", "/send"), LAT_CONC, MIN_S, MAX_S, MIN_SAMPLES, MIN_SAMPLES);
        const m1 = await metrics(s);
        await quiesce(s);
        const d = (k: string) => (m1.get(k) ?? 0) - (m0.get(k) ?? 0);
        const putCount = d('cairnflow_object_put_seconds_count{kind="wal"}');
        const putSum = d('cairnflow_object_put_seconds_sum{kind="wal"}');
        // Store requests by op within the measurement window (every tier, every path class).
        const ops: Record<string, number> = {};
        for (const [k, v] of m1) {
          const m = /^cairnflow_testkit_store_requests_total\{.*op="([a-z_]+)"/.exec(k);
          if (m) ops[m[1]!] = (ops[m[1]!] ?? 0) + v - (m0.get(k) ?? 0);
        }
        const invocations = send.n + echo.n + run1.n + tput.n;
        const row = {
          at: new Date().toISOString(),
          ...(fixed ? { profile: fixed.name ?? FIXED_PROFILE } : {}),
          put_p50_ms: putP50,
          put_p99_ms: fixed ? (fixed.latency?.put?.p99_ms ?? 0) : putP50 * TAIL,
          get_p50_ms: fixed ? (fixed.latency?.get?.p50_ms ?? 0) : putP50 * GET_RATIO,
          tail: TAIL,
          wal_window_ms: windowMs,
          wal_inflight: WAL_INFLIGHT,
          lat_conc: LAT_CONC,
          tput_conc: TPUT_CONC,
          send,
          echo,
          run1,
          tput,
          /** Shared WAL PUT measured on the server side (histogram `cairnflow_object_put_seconds`): the mean. */
          wal_put_mean_ms: putCount > 0 ? (putSum / putCount) * 1000 : null,
          wal_puts: putCount,
          /** Store requests per invocation within the measurement window (including background work of the same window). */
          store_per_invocation: Object.fromEntries(Object.entries(ops).map(([k, v]) => [k, v / invocations])),
        };
        appendFileSync(OUT, JSON.stringify(row) + "\n");
        const bad = [send, echo, run1, tput].filter((p) => p.fail > 0);
        if (bad.length > 0) {
          failed = true;
          console.error(`  ✗ put ${putP50} win ${windowMs}: ${bad.map((p) => `${p.fail} failed (${p.firstErr})`).join("; ")}`);
        }
        console.log(
          `${String(putP50).padEnd(5)}${String(windowMs).padEnd(5)}| ` +
            `${`${f(send.p50)}/${f(send.p99)}`.padEnd(15)}| ` +
            `${`${f(echo.p50)}/${f(echo.p99)}`.padEnd(17)}| ` +
            `${`${f(run1.p50)}/${f(run1.p99)}`.padEnd(17)}| ` +
            `${`${f(tput.rps)} (${f(tput.p50, 0)})`.padEnd(17)}| ` +
            `${(row.wal_put_mean_ms === null ? "—" : f(row.wal_put_mean_ms)).padEnd(11)}| ` +
            `${f(row.store_per_invocation.get ?? 0, 2)}`,
        );
      } finally {
        node.stop();
      }
    }
  }
} finally {
  deployment.kill("SIGTERM");
  rmSync(dir, { recursive: true, force: true });
}
process.exit(failed ? 1 : 0);
