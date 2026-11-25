// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

/**
 * Cost measurement of the core gaps: object-store requests per call or per
 * firing, on the **real** binary, by scenario. Later changes (server-side
 * schedules, time watermarks) rerun the same script to compare against the
 * baseline.
 *
 * ```sh
 * cargo build --release --bin cairnflow --features testkit --target-dir target/tk
 * node harness/bench/core-gaps.ts                 # every scenario
 * CG_SCENARIOS=echo-c1,echo-c16 node harness/bench/core-gaps.ts
 * ```
 *
 * **Method.** Each scenario gets a fresh `cairnflow` process: one node
 * `--cluster --partitions 4`, `memory://`, the `count-only` profile
 * (`CAIRNFLOW_STORE_PROFILE`, counts requests by source — see `s3-cost.ts`). The
 * real TS SDK, `BIDI_STREAM`, in the same process as the generator (the counts do
 * not depend on that). Counters are summed at the start and the end of the
 * measurement window and divided by the number of units (calls or firings). The
 * idle background (renewals, scans) is measured separately on the same process
 * before the load and **subtracted** in proportion to the window's duration.
 *
 * | Column | Source |
 * |---|---|
 * | commit | `cairnflow_commit_wait_seconds_count` — times a turn waited for a durable commit |
 * | PUT WAL | `cairnflow_testkit_store_requests_by_source_total{source="nodewal.stripe",op="put"}` — each shared WAL flush |
 * | GET hot | `cairnflow_read_fence_total` — one session GET per read-fence batch |
 * | GET store | `cairnflow_testkit_store_requests_total{op="get"}` — every GET to the store |
 * | bytes written | `cairnflow_testkit_store_put_bytes_total`, every kind of path |
 *
 * Scenarios (`CG_SCENARIOS`, default all):
 *
 * | name | unit | shape |
 * |---|---|---|
 * | `echo-c1` | call | `/invoke` empty handler, level 1 |
 * | `echo-c1-idem` | call | as above, a random `Idempotency-Key` per call |
 * | `echo-c16` / `echo-c16-idem` | call | level 16 |
 * | `call` | call | handler does one `ctx.call` (unkeyed target) |
 * | `race` | call | `ctx.race([call, sleep 4 s])` — the call wins |
 * | `race-loser` | call | 8 s after `race`: the losing timer fires on a finished invocation (the extra part only) |
 * | `cron-lib` | firing | schedule as a library: `ctx.run` time + `send` to the target with an idempotency key + a delayed `send` to itself |
 * | `chain` | firing | a bare delayed chain: only a delayed `send` to itself |
 * | `sched-local` | firing | server-side schedule `everyMs = CG_PERIOD_MS`, unkeyed target — the run lands on the same range, created in the firing commit |
 * | `sched-remote` | firing | as above, virtual object target — a `Fire` row + `StartCall` to the target's range |
 * | `attach-wf` | attach turn | a workflow sleeping 1 hour; a client calls `/workflow/…/attach` back to back (waiting `--invoke-wait-ms`) |
 * | `xcall-c1` / `xcall-c64` | call | keyed `ctx.call` between two virtual objects (random keys ⇒ mostly different ranges), level 1 / 64 |
 * | `list-service` / `list-time` | query | 300 existing `echo` calls; each query is `GET /invocations?limit=1000` filtered by `service=Echo` or `created_after=0` — the same number of rows scanned |
 *
 * Output: a markdown table on stdout, and one JSON line per scenario into
 * `CG_OUT` (if set).
 *
 * **Several nodes**: `CG_STORE=s3://walbench/cg-$RANDOM CG_NODES=3` starts
 * `CG_NODES` processes on a shared store (local SeaweedFS when `AWS_ENDPOINT` is
 * unset, like `s3-cost.ts`), one prefix per scenario. Every call goes to the first
 * node (which forwards to the range's leader); the counters sum over every node.
 */

import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { appendFileSync, existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { type Context, type Endpoint, serve, service, type ServiceContext, workflow } from "../../sdk/typescript/src/index.ts";

const here = fileURLToPath(new URL(".", import.meta.url));
const repoRoot = join(here, "..", "..");
const binary = process.env.CG_BINARY ?? join(repoRoot, "target", "tk", "release", "cairnflow");
const profile = join(repoRoot, "bench", "s3-model", "profiles", "count-only.json");
const PARTITIONS = Number(process.env.CG_PARTITIONS ?? "4");
/** Number of calls per call-based scenario. */
const N = Number(process.env.CG_N ?? "300");
/** Window of the time-based scenarios (delayed chains, attach), seconds. */
const WINDOW_S = Number(process.env.CG_WINDOW_S ?? "30");
/** Period of the delayed chain (`cron-lib`, `chain`), ms. */
const PERIOD_MS = Number(process.env.CG_PERIOD_MS ?? "1000");
/** Number of chains running in parallel in `cron-lib`/`chain`. */
const CHAINS = Number(process.env.CG_CHAINS ?? "1");
const IDLE_S = Number(process.env.CG_IDLE_S ?? "10");
const OUT = process.env.CG_OUT;
const STORE = process.env.CG_STORE ?? "memory://";
const NODES = Number(process.env.CG_NODES ?? "1");
if (STORE.startsWith("s3://") && !process.env.AWS_ENDPOINT) {
  const cfg = JSON.parse(
    readFileSync(join(repoRoot, "scripts", "dev-store", "seaweed-s3.json"), "utf8"),
  ) as { identities: { credentials: { accessKey: string; secretKey: string }[] }[] };
  const cred = cfg.identities[0]!.credentials[0]!;
  Object.assign(process.env, {
    AWS_ENDPOINT: "http://localhost:18333",
    AWS_ACCESS_KEY_ID: cred.accessKey,
    AWS_SECRET_ACCESS_KEY: cred.secretKey,
    AWS_REGION: "us-east-1",
    AWS_ALLOW_HTTP: "true",
    AWS_VIRTUAL_HOSTED_STYLE_REQUEST: "false",
  });
}
const ALL = [
  "echo-c1",
  "echo-c1-idem",
  "echo-c16",
  "echo-c16-idem",
  "call",
  "race",
  "race-loser",
  "cron-lib",
  "chain",
  "sched-local",
  "sched-remote",
  "attach-wf",
  "xcall-c1",
  "xcall-c64",
  "list-service",
  "list-time",
];
const SCENARIOS = (process.env.CG_SCENARIOS ?? ALL.join(",")).split(",").filter(Boolean);

// ───────────────────────────────────────────── deployment

/** Firings seen at the target, by scenario — the unit of the chain scenarios. */
const fired = new Map<string, number>();
const bump = (k: string) => fired.set(k, (fired.get(k) ?? 0) + 1);

const echo = service("Echo", {
  async echo(_ctx: ServiceContext, input: unknown) {
    return input;
  },
  async target(_ctx: ServiceContext, input: { tag: string }) {
    bump(input.tag);
    return null;
  },
});

const caller = service("Caller", {
  async once(ctx: ServiceContext) {
    return ctx.call("Echo", "echo", { x: 1 });
  },
  async race(ctx: ServiceContext) {
    return ctx.race([ctx.call<unknown>("Echo", "echo", { x: 1 }), ctx.sleep(4000)]);
  },
});

const cron = service(
  "CronLib",
  {
    /** Schedule as a library (the Restate pattern): durable time, send to the target once per mark, schedule the next one. */
    async fire(ctx: Context, input: { tag: string; period: number }) {
      const at = await ctx.run("now", () => Date.now());
      ctx.send("Echo", "target", { tag: input.tag }, { idempotencyKey: `cron/${ctx.key}/${at}` });
      ctx.send("CronLib", "fire", input, { key: ctx.key, delay: input.period });
    },
    /** A bare delayed chain: only schedules the next one. */
    async tick(ctx: Context, input: { tag: string; period: number }) {
      bump(input.tag);
      ctx.send("CronLib", "tick", input, { key: ctx.key, delay: input.period });
    },
  },
  { type: "VIRTUAL_OBJECT" },
);

function xcallSide(name: string, peer: string) {
  return service(
    name,
    {
      async ping(ctx: Context, input: { peerKey: string }) {
        return ctx.call(peer, "pong", {}, { key: input.peerKey });
      },
      async pong(ctx: Context) {
        return ctx.key;
      },
    },
    { type: "VIRTUAL_OBJECT", handlerTypes: { ping: "EXCLUSIVE", pong: "SHARED" } },
  );
}
const xcallA = xcallSide("XcallA", "XcallB");
const xcallB = xcallSide("XcallB", "XcallA");

/** The keyed target of `sched-remote`. */
const schedTarget = service(
  "SchedTarget",
  {
    async hit(_ctx: Context, input: { tag: string }) {
      bump(input.tag);
      return null;
    },
  },
  { type: "VIRTUAL_OBJECT" },
);

const sleeper = workflow("Sleeper", {
  async run(ctx: Context) {
    await ctx.sleep(3_600_000);
    return "awake";
  },
});

// ───────────────────────────────────────────── node

/** The node receiving calls; `peers` is every node of the cluster (itself included) — the counters sum over all. */
type Node = { proc: ChildProcess; url: string; peers: Node[] };

let cluster = 0;

async function spawnNode(): Promise<Node> {
  const store = STORE.startsWith("memory") ? STORE : `${STORE.replace(/\/$/, "")}/c${Date.now()}-${cluster++}`;
  const peers: Node[] = [];
  for (let i = 0; i < NODES; i++) peers.push(await spawnOne(store, i, peers));
  return peers[0]!;
}

async function spawnOne(store: string, i: number, peers: Node[]): Promise<Node> {
  const proc = spawn(
    binary,
    [
      "--no-auth",
      "--main-store",
      store,
      "--node-id",
      `cg-${i}`,
      "--cluster",
      "--partitions",
      String(PARTITIONS),
      "--port",
      "0",
      // The `sched-*` schedules fire every `CG_PERIOD_MS` (default 1 s, below the 10 s floor).
      "--schedule-min-period-ms",
      String(PERIOD_MS),
    ],
    { stdio: "pipe", env: { ...process.env, CAIRNFLOW_STORE_PROFILE: profile } },
  );
  const url = await new Promise<string>((resolve, reject) => {
    let buf = "";
    const t = setTimeout(() => reject(new Error(`node did not report its port:\n${buf}`)), 120_000);
    const take = (c: Buffer) => {
      buf += c.toString();
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
  // Wait until every range has a leader: one call to each range no longer gets 503.
  return { proc, url, peers };
}

async function stop(n: Node): Promise<void> {
  for (const p of n.peers) {
    if (p.proc.exitCode !== null) continue;
    const done = new Promise((r) => p.proc.once("exit", r));
    p.proc.kill("SIGKILL");
    await done;
  }
}

/** POST in the style of a minimal client: retry on 503 (range without a leader yet). */
async function post(url: string, body: unknown, headers: Record<string, string> = {}): Promise<Response> {
  for (let i = 0; ; i++) {
    const r = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json", ...headers },
      body: JSON.stringify(body),
    });
    if (r.status !== 503 || i > 200) return r;
    await r.arrayBuffer();
    await sleep(50);
  }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

type Counts = {
  commits: number;
  putWal: number;
  getHot: number;
  putBytes: number;
  getAll: number;
  /** Every PUT (Class A) and LIST page — to multiply by prices. */
  putAll: number;
  listAll: number;
};
const KEYS = ["commits", "putWal", "getHot", "putBytes", "getAll", "putAll", "listAll"] as const;

async function scrape(n: Node): Promise<Counts> {
  const c: Counts = { commits: 0, putWal: 0, getHot: 0, putBytes: 0, getAll: 0, putAll: 0, listAll: 0 };
  for (const p of n.peers) scrapeInto(await (await fetch(`${p.url}/metrics`)).text(), c);
  return c;
}

function scrapeInto(text: string, c: Counts) {
  for (const line of text.split("\n")) {
    if (line.startsWith("#")) continue;
    const sp = line.lastIndexOf(" ");
    const v = Number(line.slice(sp + 1));
    const k = line.slice(0, sp);
    if (k === "cairnflow_commit_wait_seconds_count") c.commits += v;
    else if (
      k.startsWith("cairnflow_testkit_store_requests_by_source_total{") &&
      k.includes('source="nodewal.stripe"') &&
      k.includes('op="put"')
    )
      c.putWal += v;
    else if (k.startsWith("cairnflow_read_fence_total{")) c.getHot += v;
    else if (k.startsWith("cairnflow_testkit_store_put_bytes_total")) c.putBytes += v;
    else if (k.startsWith("cairnflow_testkit_store_requests_total{") && k.includes('op="get"')) c.getAll += v;
    else if (k.startsWith("cairnflow_testkit_store_requests_total{") && /op="put(_multipart)?"/.test(k)) c.putAll += v;
    else if (k.startsWith("cairnflow_testkit_store_list_pages_total")) c.listAll += v;
  }
}

const minus = (a: Counts, b: Counts): Counts =>
  Object.fromEntries(KEYS.map((k) => [k, a[k] - b[k]])) as Counts;

/** Idle background per second, measured on the process just started (after warming up). */
async function idleRate(n: Node): Promise<Counts> {
  const a = await scrape(n);
  const t0 = Date.now();
  await sleep(IDLE_S * 1000);
  const b = await scrape(n);
  const s = (Date.now() - t0) / 1000;
  const d = minus(b, a);
  return Object.fromEntries(KEYS.map((k) => [k, d[k] / s])) as Counts;
}

type Result = {
  scenario: string;
  units: number;
  seconds: number;
  perUnit: Counts;
  idlePerS: Counts;
  extra?: Record<string, number>;
};

/**
 * Measures `body` on a fresh node: warm-up (registration, a few calls), idle
 * background, then the load window. `body` returns the number of units; the cost has
 * the background subtracted in proportion to the window's duration.
 */
async function measure(
  name: string,
  sdk: Endpoint,
  body: (n: Node) => Promise<{ units: number; extra?: Record<string, number> }>,
): Promise<Result> {
  const n = await spawnNode();
  try {
    const reg = await post(`${n.url}/deployments`, { uri: sdk.url });
    if (!reg.ok) throw new Error(`register: ${reg.status} ${await reg.text()}`);
    // Warm-up: every range has a leader, the Db is open, the SDK connection exists.
    for (let i = 0; i < 20; i++) await post(`${n.url}/invoke/Echo/echo`, { warm: i });
    await prepares[name]?.(n);
    await sleep(1000);
    const idle = await idleRate(n);
    const a = await scrape(n);
    const t0 = Date.now();
    const { units, extra } = await body(n);
    const seconds = (Date.now() - t0) / 1000;
    const d = minus(await scrape(n), a);
    const net = (k: keyof Counts) => Math.max(0, d[k] - idle[k] * seconds) / Math.max(1, units);
    const r: Result = {
      scenario: name,
      units,
      seconds,
      perUnit: Object.fromEntries(KEYS.map((k) => [k, net(k)])) as Counts,
      idlePerS: idle,
      ...(extra ? { extra } : {}),
    };
    if (OUT) appendFileSync(OUT, JSON.stringify(r) + "\n");
    return r;
  } finally {
    await stop(n);
  }
}

/** `N` `/invoke` calls at level `conc`, each checked for `200`. */
async function closedLoop(n: Node, path: string, conc: number, headers: () => Record<string, string> = () => ({})) {
  let left = N;
  const worker = async () => {
    while (left-- > 0) {
      const r = await post(`${n.url}${path}`, { x: 1 }, headers());
      if (r.status !== 200) throw new Error(`${path}: ${r.status} ${await r.text()}`);
      await r.arrayBuffer();
    }
  };
  await Promise.all(Array.from({ length: conc }, worker));
  return { units: N };
}

const idemHeader = () => ({ "idempotency-key": randomUUID() });

/** `N` `XcallA/ping` calls (key `a<i>` calls `XcallB/pong` key `b<i>`), level `conc`. */
async function xcallLoop(n: Node, conc: number) {
  let next = 0;
  const worker = async () => {
    for (let i = next++; i < N; i = next++) {
      const r = await post(`${n.url}/invoke/XcallA/ping?key=a${i}`, { peerKey: `b${i}` });
      if (r.status !== 200) throw new Error(`xcall: ${r.status} ${await r.text()}`);
      await r.arrayBuffer();
    }
  };
  await Promise.all(Array.from({ length: conc }, worker));
  return { units: N };
}

async function chainScenario(n: Node, handler: "fire" | "tick", tag: string) {
  fired.set(tag, 0);
  for (let i = 0; i < CHAINS; i++) {
    const r = await post(`${n.url}/invoke/CronLib/${handler}/send?key=c${i}`, { tag, period: PERIOD_MS });
    if (r.status !== 202 && r.status !== 200) throw new Error(`start chain: ${r.status} ${await r.text()}`);
  }
  // Skip the first firing (start-up), measure the later ones in the window.
  await sleep(PERIOD_MS * 2);
  const before = fired.get(tag)!;
  await sleep(WINDOW_S * 1000);
  return { units: fired.get(tag)! - before };
}

/** Sum and sample count of `cairnflow_schedule_fire_lag_seconds`. */
async function lag(n: Node): Promise<{ sum: number; count: number }> {
  let [sum, count] = [0, 0];
  for (const p of n.peers) {
    const text = await (await fetch(`${p.url}/metrics`)).text();
    const get = (k: string) => Number(/ (\S+)$/.exec(text.split("\n").find((l) => l.startsWith(`${k} `)) ?? " 0")![1]);
    sum += get("cairnflow_schedule_fire_lag_seconds_sum");
    count += get("cairnflow_schedule_fire_lag_seconds_count");
  }
  return { sum, count };
}

/** A server-side schedule every `PERIOD_MS` to `target`; counts the firings seen at the target in the window. */
async function schedScenario(n: Node, tag: string, target: Record<string, string>) {
  fired.set(tag, 0);
  for (let i = 0; ; i++) {
    const r = await fetch(`${n.url}/schedules/${tag}`, {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ everyMs: PERIOD_MS, target, input: { tag } }),
    });
    if (r.status === 200) break;
    if (r.status !== 503 || i > 200) throw new Error(`PUT schedule: ${r.status} ${await r.text()}`);
    await sleep(50);
  }
  await sleep(PERIOD_MS * 2);
  const before = fired.get(tag)!;
  const l0 = await lag(n);
  await sleep(WINDOW_S * 1000);
  const l1 = await lag(n);
  const fires = l1.count - l0.count;
  return {
    units: fired.get(tag)! - before,
    extra: { lagMs: fires > 0 ? ((l1.sum - l0.sum) / fires) * 1000 : 0, fires },
  };
}

/** Work before the measurement window (not counted), by scenario. */
/** 300 existing `echo` calls for the two query scenarios. */
async function seedEcho(n: Node): Promise<void> {
  for (let i = 0; i < 300; i++) await (await post(`${n.url}/invoke/Echo/echo`, { i })).arrayBuffer();
}

/** `QUERIES` times `GET /invocations?limit=1000&<q>`, each checked to see ≥ 300 rows. */
async function queries(n: Node, q: string): Promise<{ units: number }> {
  const QUERIES = 20;
  for (let i = 0; i < QUERIES; i++) {
    const r = await fetch(`${n.url}/invocations?limit=1000&${q}`);
    const rows = (await r.json()) as unknown[];
    if (r.status !== 200 || rows.length < 300) throw new Error(`${q}: ${r.status} ${rows.length} rows`);
  }
  return { units: QUERIES };
}

const prepares: Record<string, (n: Node) => Promise<void>> = {
  "list-service": seedEcho,
  "list-time": seedEcho,
  "attach-wf": async (n) => {
    const r = await post(`${n.url}/invoke/Sleeper/run/send?key=k`, null);
    if (r.status !== 202) throw new Error(`start workflow: ${r.status} ${await r.text()}`);
    // The first turn (suspended at `sleep`) is durable before the window.
    await sleep(2000);
  },
};

const bodies: Record<string, (n: Node) => Promise<{ units: number; extra?: Record<string, number> }>> = {
  "echo-c1": (n) => closedLoop(n, "/invoke/Echo/echo", 1),
  "echo-c1-idem": (n) => closedLoop(n, "/invoke/Echo/echo", 1, idemHeader),
  "echo-c16": (n) => closedLoop(n, "/invoke/Echo/echo", 16),
  "echo-c16-idem": (n) => closedLoop(n, "/invoke/Echo/echo", 16, idemHeader),
  call: (n) => closedLoop(n, "/invoke/Caller/once", 1),
  race: (n) => closedLoop(n, "/invoke/Caller/race", 1),
  "race-loser": async (n) => {
    // The `race` calls run before the window (not counted), the window is the 8 s after:
    // only the losing timers firing on finished invocations remain.
    throw new Error(`race-loser is measured in main — ${n.url}`);
  },
  "cron-lib": (n) => chainScenario(n, "fire", "cron-lib"),
  chain: (n) => chainScenario(n, "tick", "chain"),
  "sched-local": (n) => schedScenario(n, "sched-local", { service: "Echo", handler: "target" }),
  "sched-remote": (n) =>
    schedScenario(n, "sched-remote", { service: "SchedTarget", handler: "hit", key: "k1" }),
  "xcall-c1": (n) => xcallLoop(n, 1),
  "xcall-c64": (n) => xcallLoop(n, 64),
  "list-service": (n) => queries(n, "service=Echo"),
  "list-time": (n) => queries(n, "created_after=0"),
  "attach-wf": async (n) => {
    let turns = 0;
    const end = Date.now() + WINDOW_S * 1000;
    while (Date.now() < end) {
      const a = await post(`${n.url}/workflow/Sleeper/k/attach`, null);
      if (a.status !== 202) throw new Error(`attach: ${a.status} ${await a.text()}`);
      await a.arrayBuffer();
      turns++;
    }
    return { units: turns };
  },
};

/**
 * `race-loser`: `N` `race` calls (the call wins), then measure the next 8 s
 * separately — the losing 4 s timer fires on a finished invocation. There is no
 * regular `body` because the measurement window does not overlap the load.
 */
async function raceLoser(sdk: Endpoint): Promise<Result> {
  const n = await spawnNode();
  try {
    const reg = await post(`${n.url}/deployments`, { uri: sdk.url });
    if (!reg.ok) throw new Error(`register: ${reg.status}`);
    for (let i = 0; i < 20; i++) await post(`${n.url}/invoke/Echo/echo`, { warm: i });
    await sleep(1000);
    const idle = await idleRate(n);
    // Fast `race` calls (< 4 s for the whole batch, level 16): every losing timer is still pending.
    const t0 = Date.now();
    let left = N;
    await Promise.all(
      Array.from({ length: 16 }, async () => {
        while (left-- > 0) {
          const r = await post(`${n.url}/invoke/Caller/race`, {});
          await r.arrayBuffer();
        }
      }),
    );
    const spent = Date.now() - t0;
    if (spent > 3500) console.warn(`race-loser: the race batch took ${spent} ms — some timers fired during the load`);
    const a = await scrape(n);
    const w0 = Date.now();
    await sleep(8000);
    const d = minus(await scrape(n), a);
    const seconds = (Date.now() - w0) / 1000;
    const net = (k: keyof Counts) => Math.max(0, d[k] - idle[k] * seconds) / N;
    const r: Result = {
      scenario: "race-loser",
      units: N,
      seconds,
      perUnit: Object.fromEntries(KEYS.map((k) => [k, net(k)])) as Counts,
      idlePerS: idle,
    };
    if (OUT) appendFileSync(OUT, JSON.stringify(r) + "\n");
    return r;
  } finally {
    await stop(n);
  }
}

async function main() {
  if (!existsSync(binary)) {
    console.error(`missing ${binary} — cargo build --release --bin cairnflow --features testkit --target-dir target/tk`);
    process.exit(2);
  }
  const sdk = await serve({ services: [echo, caller, cron, xcallA, xcallB, schedTarget, sleeper], port: 0 });
  const rows: Result[] = [];
  try {
    for (const name of SCENARIOS) {
      if (!(name in bodies)) throw new Error(`unknown scenario: ${name} (available: ${ALL.join(", ")})`);
      process.stderr.write(`· ${name}…\n`);
      rows.push(name === "race-loser" ? await raceLoser(sdk) : await measure(name, sdk, bodies[name]!));
    }
  } finally {
    await sdk.close?.();
  }
  const f = (x: number, d = 2) => x.toFixed(d).replace(".", ",");
  console.log(`\nbinary ${binary}, ${NODES} node × ${PARTITIONS} ranges, ${STORE}, count-only, BIDI\n`);
  console.log("| Scenario | Unit | Commit | PUT WAL | GET hot | GET store | Bytes written | PUT all kinds | LIST pages | Idle PUT WAL/s |");
  console.log("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
  for (const r of rows) {
    const p = r.perUnit;
    console.log(
      `| \`${r.scenario}\` | ${r.units} | ${f(p.commits)} | ${f(p.putWal)} | ${f(p.getHot)} | ${f(p.getAll)} | ${Math.round(p.putBytes)} | ${f(p.putAll)} | ${f(p.listAll)} | ${f(r.idlePerS.putWal)} |`,
    );
  }
  for (const r of rows) {
    if (r.extra?.["lagMs"] !== undefined) console.log(`${r.scenario}: mean firing lag ${f(r.extra["lagMs"], 0)} ms`);
  }
  process.exit(0);
}

await main();
