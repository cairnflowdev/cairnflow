// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

/**
 * Unit cost by feature: object-store requests per unit (one `/query` statement,
 * one audited action, one row/object of a tenant purge, one split/merge…) on the
 * **real** binary, `count-only` profile.
 *
 * ```sh
 * cargo build --release --bin cairnflow --features testkit --target-dir target/tk
 * node harness/bench/cost-units.ts --out bench/s3-model/results/cost-units.jsonl
 * CU_UNITS=query,purge node harness/bench/cost-units.ts
 * ```
 *
 * **Method** as in `core-gaps.ts`: each unit gets a fresh process, one node
 * `--cluster --partitions CU_PARTITIONS` on `memory://` (or `CU_STORE`), an admin
 * key; warm-up, measure the idle background for `CU_IDLE_S` seconds, then the
 * unit's window; the window's counts minus the background in proportion to time,
 * divided by the number of units. Counted by `op`, by `object` (the path class:
 * `audit`, `trash`, `artifacts`, `tenants`…) and by `source` (the background loop
 * label: `artifact.sweep`, …).
 *
 * | unit | work | divided by |
 * |---|---|---|
 * | `query` | each Overview statement (`ui/src/data/queries.ts`) and every console sample (`ui/src/data/samples.json`) × `CU_REPEAT` over 300 existing invocations | statement |
 * | `audit` | `PUT /admin/tenants/t {slowMs}` (intent/done), `POST /admin/keys`, `POST /invocations/{id}/cancel` (a row in the range) | action |
 * | `purge` | a tenant with `CU_PURGE_N` invocations (each 1 state key + 1 artifact of 4 KiB) — `PUT …/state deleting`, `--tenant-purge-keep-s` = `CU_PURGE_KEEP_S` (default 5), until `purged`; records the estimated number of rounds (`extra.rounds`) | row / object, the whole run |
 * | `limits` | the same load on a tenant without limits and on a tenant with every limit (`maxLive`… `maxArtifactBytes`), then idle; `CU_ARTIFACTS=<n>` seeds `n` extra 4 KiB artifacts for the limited tenant before the idle window (scales with LIST pages if usage is counted by listing) | invocation; extra requests/hour while idle |
 * | `state-keys` | a virtual object writing a **new** key per call, without a limit / with `maxStateKeys` | call |
 * | `split-merge` | split the first range and merge it back, `CU_SPLITS` rounds | operation |
 *
 * One JSON line per unit (`--out`): the window's raw `counts`, `idle`, `units`.
 * A markdown table on stdout.
 */

import { spawn, type ChildProcess } from "node:child_process";
import { randomBytes } from "node:crypto";
import { appendFileSync, existsSync, mkdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { type Context, serve, service, type ServiceContext } from "../../sdk/typescript/src/index.ts";
import { queries } from "./console-queries.ts";

const here = fileURLToPath(new URL(".", import.meta.url));
const repoRoot = join(here, "..", "..");
const env = process.env;
const binary = env.CU_BINARY ?? join(repoRoot, "target", "tk", "release", "cairnflow");
const profile = join(repoRoot, "bench", "s3-model", "profiles", "count-only.json");
const STORE = env.CU_STORE ?? "memory://";
const PARTITIONS = Number(env.CU_PARTITIONS ?? "4");
const IDLE_S = Number(env.CU_IDLE_S ?? "30");
const REPEAT = Number(env.CU_REPEAT ?? "10");
const PURGE_N = Number(env.CU_PURGE_N ?? "200");
/** `--tenant-purge-keep-s` of the `purge` run; the purge round runs every `keep / 8` (≥ 1 s). */
const PURGE_KEEP_S = Number(env.CU_PURGE_KEEP_S ?? "5");
const LIMITS_N = Number(env.CU_LIMITS_N ?? "200");
const LIMITS_IDLE_S = Number(env.CU_LIMITS_IDLE_S ?? "120");
/** `limits`: number of extra 4 KiB artifacts seeded for the limited tenant before the idle window (0 = none). */
const ARTIFACTS = Number(env.CU_ARTIFACTS ?? "0");
const SPLITS = Number(env.CU_SPLITS ?? "3");
const ADMIN_KEY = `cf_cu_${randomBytes(16).toString("hex")}`;
const ALL = ["query", "audit", "purge", "limits", "state-keys", "split-merge"];
const UNITS = (env.CU_UNITS ?? ALL.join(",")).split(",").filter(Boolean);
const argv = process.argv.slice(2);
const oi = argv.indexOf("--out");
const OUT = oi >= 0 ? argv[oi + 1] : env.CU_OUT;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// ───────────────────────────────────────────── deployment

const echo = service("Echo", {
  async echo(_ctx: ServiceContext, input: unknown) {
    return input;
  },
  /** Sleeps a long time — so `cancel` has something to cancel. */
  async nap(ctx: ServiceContext) {
    await ctx.sleep(3_600_000);
    return null;
  },
  /** The `limits` load: one `ctx.run`, one short timer, one stream, one small artifact. */
  async mixed(ctx: ServiceContext, input: { n: number }) {
    await ctx.run("a", () => input.n);
    await ctx.sleep(10);
    ctx.stream("ui").append({ n: input.n });
    await ctx.run("art", () => ctx.artifact.put(randomBytes(1024).toString("base64")));
    return null;
  },
  /** One 4 KiB artifact (the `limits` seed with `CU_ARTIFACTS`). */
  async art(ctx: ServiceContext) {
    await ctx.run("art", () => ctx.artifact.put(randomBytes(3072).toString("base64")));
    return null;
  },
});

const box = service(
  "Box",
  {
    /** One new state key per call (key by `n`), plus a 4 KiB artifact when `art`. */
    async put(ctx: Context, input: { n: number; art?: boolean }) {
      ctx.set(`k${input.n}`, input.n);
      if (input.art) await ctx.run("art", () => ctx.artifact.put(randomBytes(3072).toString("base64")));
      return null;
    },
  },
  { type: "VIRTUAL_OBJECT", handlerTypes: { put: "EXCLUSIVE" } },
);

// ───────────────────────────────────────────── node

type Node = { proc: ChildProcess; url: string };

async function spawnNode(extra: string[] = []): Promise<Node> {
  const proc = spawn(
    binary,
    ["--main-store", STORE, "--cluster", "--partitions", String(PARTITIONS), "--port", "0", ...extra],
    {
      stdio: "pipe",
      env: { ...env, CAIRNFLOW_STORE_PROFILE: profile, CAIRNFLOW_ADMIN_KEY: ADMIN_KEY, CAIRNFLOW_INVOKE_WAIT_MS: "60000" },
    },
  );
  const url = await new Promise<string>((resolve, reject) => {
    let buf = "";
    const t = setTimeout(() => reject(new Error(`node did not report its port:\n${buf.slice(-2000)}`)), 60_000);
    const take = (c: Buffer) => {
      buf += c.toString();
      if (buf.length > 1 << 16) buf = buf.slice(-(1 << 15));
      const m = /listening on (http:\/\/[\d.]+:\d+)/.exec(buf);
      if (m) {
        clearTimeout(t);
        resolve(m[1]!);
      }
    };
    proc.stdout!.on("data", take);
    proc.stderr!.on("data", take);
    proc.on("exit", (code) => reject(new Error(`node exited early (${code}):\n${buf.slice(-2000)}`)));
  });
  return { proc, url };
}

async function stop(n: Node) {
  if (n.proc.exitCode !== null) return;
  const done = new Promise((r) => n.proc.once("exit", r));
  n.proc.kill("SIGKILL");
  await done;
}

/** Calls the node; retries on `503` (range without a leader yet). */
async function call(n: Node, method: string, path: string, body?: unknown, key = ADMIN_KEY): Promise<Response> {
  for (let i = 0; ; i++) {
    const r = await fetch(n.url + path, {
      method,
      headers: { authorization: `Bearer ${key}`, ...(body === undefined ? {} : { "content-type": "application/json" }) },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
    if (r.status !== 503 || i > 300) return r;
    await r.arrayBuffer();
    await sleep(100);
  }
}

async function ok(r: Promise<Response>, what: string): Promise<unknown> {
  const x = await r;
  const text = await x.text();
  if (!x.ok) throw new Error(`${what}: ${x.status} ${text.slice(0, 500)}`);
  return text ? JSON.parse(text) : null;
}

async function createKey(n: Node, tenant: string): Promise<string> {
  const b = (await ok(call(n, "POST", "/admin/keys", { tenant, role: "admin" }), `key ${tenant}`)) as {
    secret?: string;
    key?: string;
  };
  return (b.secret ?? b.key)!;
}

async function deploy(n: Node, key: string, uri: string) {
  // The key registry learns a new key after one refresh tick (≤ 5 s): retry 401.
  for (let i = 0; ; i++) {
    const r = await call(n, "POST", "/deployments", { uri }, key);
    if (r.ok) return void (await r.arrayBuffer());
    if (i > 40) throw new Error(`register: ${r.status} ${await r.text()}`);
    await r.arrayBuffer();
    await sleep(250);
  }
}

// ───────────────────────────────────────────── counters

type Counts = Record<string, number>;

async function scrape(n: Node): Promise<Counts> {
  const text = await (await fetch(`${n.url}/metrics`, { headers: { authorization: `Bearer ${ADMIN_KEY}` } })).text();
  const out: Counts = {};
  for (const line of text.split("\n")) {
    if (!line.startsWith("cairnflow_testkit_store_")) continue;
    const sp = line.lastIndexOf(" ");
    const key = line.slice(0, sp).replace(/tier="[^"]*",?/, "").replace("{}", "");
    out[key] = (out[key] ?? 0) + Number(line.slice(sp + 1));
  }
  return out;
}

function diff(a: Counts, b: Counts): Counts {
  const out: Counts = {};
  for (const k of new Set([...Object.keys(a), ...Object.keys(b)])) {
    const d = (b[k] ?? 0) - (a[k] ?? 0);
    if (d !== 0) out[k] = d;
  }
  return out;
}

/** Requests by `op` (from `requests_by_source`, LIST = number of pages), optionally filtered by label. */
function ops(c: Counts, filter: (labels: Record<string, string>) => boolean = () => true): Record<string, number> {
  const out: Record<string, number> = {};
  for (const [k, v] of Object.entries(c)) {
    const m = /^cairnflow_testkit_store_requests_by_source_total\{(.*)\}$/.exec(k);
    if (!m) continue;
    const l = Object.fromEntries([...m[1]!.matchAll(/(\w+)="([^"]*)"/g)].map((x) => [x[1]!, x[2]!]));
    if (!filter(l)) continue;
    const op = l.op === "put_multipart" ? "put" : l.op === "rename" ? "copy" : l.op!;
    out[op] = (out[op] ?? 0) + v;
  }
  return out;
}

/** Requests by one label (`object`, `source`, `class`), every op grouped as `label:op`. */
function by(c: Counts, label: string): Record<string, number> {
  const out: Record<string, number> = {};
  for (const [k, v] of Object.entries(c)) {
    const m = /^cairnflow_testkit_store_requests_by_source_total\{(.*)\}$/.exec(k);
    if (!m) continue;
    const l = Object.fromEntries([...m[1]!.matchAll(/(\w+)="([^"]*)"/g)].map((x) => [x[1]!, x[2]!]));
    const key = `${l[label]}:${l.op}`;
    out[key] = (out[key] ?? 0) + v;
  }
  return out;
}

const putBytes = (c: Counts) =>
  Object.entries(c)
    .filter(([k]) => k.startsWith("cairnflow_testkit_store_put_bytes_total"))
    .reduce((a, [, v]) => a + v, 0);

type Window = { name: string; units: number; seconds: number; counts: Counts; net: Counts; extra?: Record<string, unknown> };

/** Measurement window around `body`: counts, minus the idle background (per second) in proportion to time. */
async function window(
  n: Node,
  idle: { perS: Counts },
  name: string,
  body: () => Promise<{ units: number; extra?: Record<string, unknown> }>,
): Promise<Window> {
  const a = await scrape(n);
  const t0 = Date.now();
  const { units, extra } = await body();
  const seconds = (Date.now() - t0) / 1000;
  const counts = diff(a, await scrape(n));
  const net: Counts = {};
  for (const [k, v] of Object.entries(counts)) {
    const x = v - (idle.perS[k] ?? 0) * seconds;
    if (Math.abs(x) > 0.5) net[k] = Math.max(0, x);
  }
  const w: Window = { name, units, seconds, counts, net, ...(extra ? { extra } : {}) };
  const per = (x: number) => (x / Math.max(1, units)).toFixed(2);
  const o = ops(net);
  console.log(
    `| ${name} | ${units} | ${["put", "get", "head", "list", "delete", "copy"].map((op) => per(o[op] ?? 0)).join(" | ")} | ${per(putBytes(net))} | ${topObjects(net, units)} |`,
  );
  if (OUT) {
    mkdirSync(dirname(OUT), { recursive: true });
    appendFileSync(OUT, JSON.stringify({ unit: name, units, seconds, counts, net, idle_per_s: idle.perS, extra, partitions: PARTITIONS, store: STORE, binary, at: new Date(t0).toISOString() }) + "\n");
  }
  return w;
}

function topObjects(c: Counts, units: number): string {
  return Object.entries(by(c, "object"))
    .filter(([, v]) => v / Math.max(1, units) >= 0.05)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 6)
    .map(([k, v]) => `${k} ${(v / Math.max(1, units)).toFixed(2)}`)
    .join(", ");
}

async function idleRate(n: Node): Promise<{ perS: Counts }> {
  const a = await scrape(n);
  const t0 = Date.now();
  await sleep(IDLE_S * 1000);
  const d = diff(a, await scrape(n));
  const s = (Date.now() - t0) / 1000;
  return { perS: Object.fromEntries(Object.entries(d).map(([k, v]) => [k, v / s])) };
}

/** A fresh process: start, register a deployment for tenant `default`, warm up, idle background. */
async function withNode<T>(sdkUrl: string, extra: string[], f: (n: Node, idle: { perS: Counts }) => Promise<T>): Promise<T> {
  const n = await spawnNode(extra);
  try {
    await deploy(n, ADMIN_KEY, sdkUrl);
    for (let i = 0; i < 20; i++) await ok(call(n, "POST", "/invoke/Echo/echo", { warm: i }), "warm-up");
    await sleep(1000);
    return await f(n, await idleRate(n));
  } finally {
    await stop(n);
  }
}

// ───────────────────────────────────────────── units

async function unitQuery(sdkUrl: string) {
  await withNode(sdkUrl, [], async (n, idle) => {
    for (let i = 0; i < 300; i++) await ok(call(n, "POST", "/invoke/Echo/echo", { i }), "seed");
    await sleep(2000);
    const since = Date.now() - 24 * 3_600_000;
    const overview = {
      invocationCounts: queries.invocationCounts(),
      handlerStats: queries.handlerStats(),
      serviceHealth: queries.serviceHealth(),
      deploymentsPinned: queries.deploymentsPinned(),
      history: queries.history(since),
    };
    const samples = JSON.parse(readFileSync(join(repoRoot, "ui", "src", "data", "samples.json"), "utf8")) as { pq: number; sql: string }[];
    const runSql = async (sql: string, params: unknown[]) => {
      for (let i = 0; i < REPEAT; i++) {
        const r = await call(n, "POST", "/query", { sql, params, allow_partial: true });
        await r.arrayBuffer();
      }
      return { units: REPEAT };
    };
    for (const [name, q] of Object.entries(overview)) await window(n, idle, `query ${name}`, () => runSql(q.sql, q.params));
    // Samples: grouped by PQ# (several statements of the same shape).
    const byPq = new Map<number, string[]>();
    for (const s of samples) byPq.set(s.pq, [...(byPq.get(s.pq) ?? []), s.sql]);
    for (const [pq, sqls] of byPq) {
      await window(n, idle, `query sample PQ${pq} (${sqls.length})`, async () => {
        for (const s of sqls) await runSql(s, []);
        return { units: REPEAT * sqls.length };
      });
    }
  });
}

async function unitAudit(sdkUrl: string) {
  await withNode(sdkUrl, [], async (n, idle) => {
    const N = 30;
    await window(n, idle, "audit PUT /admin/tenants (settings)", async () => {
      for (let i = 0; i < N; i++) await ok(call(n, "PUT", "/admin/tenants/aud", { slowMs: 1000 + i }), "slowMs");
      return { units: N };
    });
    await window(n, idle, "audit POST /admin/keys", async () => {
      for (let i = 0; i < N; i++) await createKey(n, `aud${i % 3}`);
      return { units: N };
    });
    const ids: string[] = [];
    for (let i = 0; i < N; i++) {
      const b = (await ok(call(n, "POST", "/invoke/Echo/nap/send", {}), "nap")) as { id?: string; invocationId?: string };
      ids.push((b.id ?? b.invocationId)!);
    }
    await sleep(2000);
    await window(n, idle, "audit cancel (a row in the range)", async () => {
      for (const id of ids) await ok(call(n, "POST", `/invocations/${id}/cancel`), "cancel");
      await sleep(1000);
      return { units: N };
    });
    // Baseline: the same number of calls without a human action (no audit) — separates
    // the cost of the action itself from what the audit adds.
    await window(n, idle, "baseline invoke echo (no audit)", async () => {
      for (let i = 0; i < N; i++) await ok(call(n, "POST", "/invoke/Echo/echo", { i }), "echo");
      return { units: N };
    });
  });
}

async function unitPurge(sdkUrl: string) {
  await withNode(sdkUrl, ["--tenant-purge-keep-s", String(PURGE_KEEP_S)], async (n, idle) => {
    const key = await createKey(n, "gone");
    await deploy(n, key, sdkUrl);
    let seeded = 0;
    await Promise.all(
      Array.from({ length: 8 }, async () => {
        while (seeded < PURGE_N) {
          const i = seeded++;
          await ok(call(n, "POST", `/invoke/Box/put?key=o${i % 50}`, { n: i, art: true }, key), "seed");
        }
      }),
    );
    await sleep(3000);
    const t0 = Date.now();
    await window(n, idle, `purge tenant (${PURGE_N} invocation, ${PURGE_N} state keys, ${PURGE_N} artifact, keep ${PURGE_KEEP_S}s)`, async () => {
      for (;;) {
        const r = await call(n, "PUT", "/admin/tenants/gone/state", { state: "deleting" });
        await r.arrayBuffer();
        if (r.status === 204) break;
        if (r.status !== 202) throw new Error(`deleting: ${r.status}`);
        await sleep(500);
      }
      for (;;) {
        const t = (await ok(call(n, "GET", "/admin/tenants"), "registry")) as { states?: Record<string, string> };
        if (t.states?.gone === "purged") break;
        if (Date.now() - t0 > 900_000) throw new Error(`purge not done after 15 minutes: ${JSON.stringify(t.states)}`);
        await sleep(1000);
      }
      const ms = Date.now() - t0;
      // Estimated rounds: a `keep / 8` tick clamped to [1 s, 300 s] (`node/setup.rs`).
      const every = Math.min(300, Math.max(1, PURGE_KEEP_S / 8));
      return { units: PURGE_N, extra: { ms, keep_s: PURGE_KEEP_S, rounds: Math.round(ms / 1000 / every) } };
    });
  });
}

const ALL_LIMITS = {
  maxLive: 100_000,
  maxDeployments: 100,
  maxSchedules: 100,
  maxTimers: 1000,
  maxStreams: 100,
  maxStateKeys: 100_000,
  maxArtifactBytes: 1 << 30,
};

async function unitLimits(sdkUrl: string) {
  await withNode(sdkUrl, [], async (n, idle) => {
    const free = await createKey(n, "free");
    const capped = await createKey(n, "capped");
    await ok(call(n, "PUT", "/admin/tenants/capped", ALL_LIMITS), "limits");
    await deploy(n, free, sdkUrl);
    await deploy(n, capped, sdkUrl);
    const load = (key: string) => async () => {
      for (let i = 0; i < LIMITS_N; i++) await ok(call(n, "POST", "/invoke/Echo/mixed", { n: i }, key), "mixed");
      await sleep(1000);
      return { units: LIMITS_N };
    };
    await window(n, idle, "limits load, tenant without limits", load(free));
    await window(n, idle, "limits load, tenant with every limit", load(capped));
    // `CU_ARTIFACTS`: many `artifacts/capped/` pages (1 000 objects per page) —
    // a counting loop that LISTed the whole directory every 5 s made idle Class A grow
    // with the pages; counting from the `Artifacts` rows in RAM must add 0.
    if (ARTIFACTS > 0) {
      let seeded = 0;
      await Promise.all(
        Array.from({ length: 8 }, async () => {
          while (seeded < ARTIFACTS) {
            seeded++;
            await ok(call(n, "POST", "/invoke/Echo/art", {}, capped), "seed artifact");
          }
        }),
      );
      await sleep(3000);
    }
    const seededLabel = ARTIFACTS > 0 ? `, ${ARTIFACTS} artifact 4 KiB` : "";
    // Idle with limits: the `maxLive`/`maxArtifactBytes` counting loop (RAM counters, 0 store requests).
    await window(n, idle, `limits idle with limits (${LIMITS_IDLE_S}s${seededLabel}, unit = hour)`, async () => {
      await sleep(LIMITS_IDLE_S * 1000);
      return { units: LIMITS_IDLE_S / 3600 };
    });
  });
}

async function unitStateKeys(sdkUrl: string) {
  await withNode(sdkUrl, [], async (n, idle) => {
    const free = await createKey(n, "sk-free");
    const capped = await createKey(n, "sk-capped");
    await ok(call(n, "PUT", "/admin/tenants/sk-capped", { maxStateKeys: 100_000 }), "limits");
    await deploy(n, free, sdkUrl);
    await deploy(n, capped, sdkUrl);
    const N = 300;
    for (const [name, key] of [["no limit", free], ["maxStateKeys", capped]] as const) {
      await window(n, idle, `state new key, ${name}`, async () => {
        for (let i = 0; i < N; i++) await ok(call(n, "POST", `/invoke/Box/put?key=o${i % 10}`, { n: i }, key), "put");
        await sleep(1000);
        return { units: N };
      });
    }
  });
}

async function unitSplitMerge(sdkUrl: string) {
  await withNode(sdkUrl, ["--gc-retired-dbs"], async (n, idle) => {
    // Put data in the range before splitting.
    for (let i = 0; i < 200; i++) await ok(call(n, "POST", `/invoke/Box/put?key=s${i}`, { n: i }), "seed");
    await sleep(2000);
    for (let s = 0; s < SPLITS; s++) {
      const c = (await ok(call(n, "GET", "/cluster"), "cluster")) as { ranges: { range: number }[] };
      const first = c.ranges[0]!.range;
      let left = first;
      let right = first;
      await window(n, idle, `split range ${first}`, async () => {
        const r = (await ok(call(n, "POST", `/ranges/${first}/split`), "split")) as { ranges: { range: number }[] };
        const ids = r.ranges.map((x) => x.range).sort((a, b) => b - a);
        [left, right] = [ids[1]!, ids[0]!];
        return { units: 1 };
      });
      await sleep(2000);
      await window(n, idle, `merge ${left}+${right}`, async () => {
        await ok(call(n, "POST", `/ranges/merge?left=${Math.min(left, right)}&right=${Math.max(left, right)}`), "merge");
        return { units: 1 };
      });
      await sleep(2000);
    }
  });
}

async function main() {
  if (!existsSync(binary)) {
    console.error(`missing ${binary} — cargo build --release --bin cairnflow --features testkit --target-dir target/tk`);
    process.exit(2);
  }
  const sdk = await serve({ services: [echo, box], port: 0 });
  console.log(`\ncost-units: ${binary}, ${STORE}, ${PARTITIONS} ranges, count-only; numbers per unit, idle background subtracted\n`);
  console.log("| unit | n | PUT | GET | HEAD | LIST | DELETE | COPY | bytes PUT | object:op (≥ 0.05/unit) |");
  console.log("|---|---:|---:|---:|---:|---:|---:|---:|---:|---|");
  const table: Record<string, (u: string) => Promise<void>> = {
    query: unitQuery,
    audit: unitAudit,
    purge: unitPurge,
    limits: unitLimits,
    "state-keys": unitStateKeys,
    "split-merge": unitSplitMerge,
  };
  try {
    for (const u of UNITS) {
      if (!table[u]) throw new Error(`unknown unit: ${u} (available: ${ALL.join(", ")})`);
      await table[u]!(sdk.url).catch((e) => console.error(`${u}: ${e}`));
    }
  } finally {
    await sdk.close?.();
  }
  process.exit(0);
}

await main();
