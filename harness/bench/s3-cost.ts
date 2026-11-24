// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

/**
 * Object-store cost model: counts the requests by kind that a **real** cairn
 * cluster issues under four sample workloads, plus the stored bytes over time. No
 * prices are applied here — `cost.ts` does that from the result file and a
 * parametric price table.
 *
 * ```sh
 * # A binary with the testkit feature — its own target directory, so check.sh's binary is not overwritten:
 * cargo build --bin cairnflow --features testkit --target-dir target/tk
 * # SeaweedFS (scripts/dev-store/up.sh), a separate bucket per run:
 * node harness/bench/s3-cost.ts --out bench/s3-model/results/run.jsonl
 * node harness/bench/cost.ts bench/s3-model/results/run.jsonl
 * ```
 *
 * **How it counts** (and why it matches the in-process tests): the binary reads
 * `CAIRNFLOW_STORE_PROFILE` pointing at `bench/s3-model/profiles/count-only.json`,
 * wraps the store in the **same** `FaultyStore` the Rust tests use (no rules, only
 * counting), and exposes the counters on `/metrics` (`cairnflow_testkit_store_*`).
 * The script sums the counters of every node at the start and the end of the
 * measurement window. The counters sit **above** `object_store`'s HTTP client: the
 * client's internal retries (5xx, network) are not counted — nearly absent on a
 * local SeaweedFS, present on real S3 (cross-check against the bill).
 *
 * **Steps** = metering's durable steps (SDK commands written to the journal,
 * `GET /metering`) — exactly the unit cairn's bill charges. Metering flushes every
 * 60 s, so after the measurement window the script waits one flush tick before
 * reading (the requests of that wait are not in the counts).
 *
 * **Stored bytes**: `aws s3 ls --recursive --summarize` over the workload's prefix,
 * sampled every `COST_SAMPLE_S` seconds (the counters cannot see what already sits
 * on the bucket or what another node deletes). The local SeaweedFS key comes from
 * `scripts/dev-store/seaweed-s3.json` — never an AWS key.
 *
 * Four workloads (`--scenarios` selects a subset, default all):
 *
 * | name | shape |
 * |---|---|
 * | `tenants` | `COST_TENANTS` small tenants, each with one API key and one deployment; each tenant calls a 3-step handler on average once every `COST_TENANT_GAP_S` seconds |
 * | `workflows` | long workflows: 4 × (`ctx.sleep` 2 s + `ctx.run`), then wait on a promise the driver resolves after ~10 s; started at `COST_WF_RATE`/s |
 * | `dense` | one tenant, `COST_DENSE_CONC` back-to-back loops calling a handler with 3 `ctx.run` + one state row |
 * | `idle` | a cluster without load; runs two sizes (`COST_IDLE_PARTITIONS`, default 8 and 32 ranges) to separate the per-node part from the per-range part |
 * | `console-overview` | like `idle` (one deployment), plus a simulated browser parked on the console's Overview screen: the 4 `/query` statements of `ui/src/data/queries.ts` with adaptive refresh (10 × the statement's duration, clamped to [10 s, 60 s] — `OVERVIEW_REFRESH` of `ui/src/data/hooks.ts`) and the hourly `history` statement clamped to [60 s, 300 s] (`HISTORY_REFRESH`), `GET /deployments` every 30 s, the top bar's health (`/cluster`, `/cluster/nodes`) every 30 s |
 * | `console-cluster` | the Cluster screen: `/cluster`, `/cluster/nodes`, `/ranges/ops`, `/ranges/suggestions` every 5 s (`routes/_app/cluster.tsx`) |
 * | `console-usage` | the Usage screen: `history` + `historyByHandler` by `HISTORY_REFRESH`, `meteringNow` by `OVERVIEW_REFRESH`, `/metering/hours` every 60 s, the top bar's health every 30 s |
 *
 * The three `console-*` workloads do not count as `idle` in `cost.ts` (different
 * names): their marginal part (with the same-size `idle` subtracted) is the requests
 * **caused by the console** per hour. The simulated browser asks only one node, like the real console.
 *
 * `COST_QUIET_LEASE=off|solo|delegation`: the mode of the quiet-renewal gate on
 * each workload's bucket. A binary with the node flag `--quiet-lease` (new buckets
 * enable delegation by default) receives it through `CAIRNFLOW_QUIET_LEASE`; with an
 * older binary, `solo`/`delegation` go through
 * `cairn-ctl cluster enable-quiet-lease --apply [--delegation]` before the nodes
 * start (needs `cairn-ctl` next to the binary, `COST_CTL`), `off` is its default.
 * Absent ⇒ the binary's default.
 *
 * Five agent-primitive workloads — one node, `COST_AGENT_PARTITIONS` ranges,
 * `COST_AGENT_CONC` (64) closed `/invoke` loops, each call one `ctx.run` lasting
 * `COST_AGENT_RUN_MS` (10 s):
 *
 * | name | shape | compared with |
 * |---|---|---|
 * | `agent-base` | `ctx.run` only sleeps | — |
 * | `agent-stream` | inside `ctx.run`: 50 tokens/s × 20 B into the transient stream `ui` | `agent-base`: 0 extra PUTs, 0 Db bytes |
 * | `agent-log` | like `agent-stream` + 5 `ctx.log` lines/s + 1 span around `ctx.run` | `agent-stream`: WAL PUT/s ±5 % |
 * | `agent-artifact` | 10 `ctx.run`, each returning the ref of an artifact of `COST_AGENT_ARTIFACT_KB` (2048) KiB | `agent-inline`: 1 PUT per artifact, WAL bytes down ≥ 90 % |
 * | `agent-inline` | 10 `ctx.run`, each returning that many bytes directly (a string) | — |
 *
 * The large-value workload `chat-long-<H>k` — one node, a tenant with
 * `max-payload=1 MiB` (a typical shared-cell limit, `seal_bytes` 256 KiB),
 * `COST_CHAT_KEYS` (4) `CostChat` objects, each with a JSON string history growing
 * to H KiB (`COST_CHAT_SIZES_KIB`, default 64,256,1024,4096,8192) by 256 KiB append
 * turns, then within the measurement window each `send` turn appends
 * `COST_CHAT_NEW` (2048) random bytes — a closed loop per object. The read path is a
 * node flag (`COST_NODE_ARGS="--value-read-path inline|proxy"`); the SDK's segment
 * cache is disabled with `COST_CHAT_SDK_CACHE=0` (cold serverless). Dollar figures
 * come from a separate price model.
 */

import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { randomBytes } from "node:crypto";
import { appendFileSync, existsSync, mkdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { type Context, serve, service, type ServiceContext, workflow } from "../../sdk/typescript/src/index.ts";
import { HISTORY_REFRESH_MS, OVERVIEW_REFRESH_MS, queries as consoleQueries } from "./console-queries.ts";

const here = fileURLToPath(new URL(".", import.meta.url));
const repoRoot = join(here, "..", "..");
const binary = process.env.COST_BINARY ?? join(repoRoot, "target", "tk", "debug", "cairnflow");
const profile = process.env.COST_PROFILE ?? join(repoRoot, "bench", "s3-model", "profiles", "count-only.json");
const BASE = process.env.COST_STORE_URI ?? `s3://walbench/run-${Date.now()}`;
const NODES = Number(process.env.COST_NODES ?? "3");
const PARTITIONS = Number(process.env.COST_PARTITIONS ?? "8");
const WARMUP_S = Number(process.env.COST_WARMUP_S ?? "15");
const SAMPLE_S = Number(process.env.COST_SAMPLE_S ?? "30");
const TENANTS = Number(process.env.COST_TENANTS ?? "40");
const TENANT_GAP_S = Number(process.env.COST_TENANT_GAP_S ?? "20");
const TENANTS_S = Number(process.env.COST_TENANTS_S ?? "300");
const WF_RATE = Number(process.env.COST_WF_RATE ?? "2");
const WF_S = Number(process.env.COST_WF_S ?? "300");
const DENSE_CONC = Number(process.env.COST_DENSE_CONC ?? "16");
const DENSE_S = Number(process.env.COST_DENSE_S ?? "120");
const IDLE_S = Number(process.env.COST_IDLE_S ?? "600");
const IDLE_PARTITIONS = (process.env.COST_IDLE_PARTITIONS ?? "8,32").split(",").map(Number);
const AGENT_S = Number(process.env.COST_AGENT_S ?? "120");
const AGENT_CONC = Number(process.env.COST_AGENT_CONC ?? "64");
const AGENT_RUN_MS = Number(process.env.COST_AGENT_RUN_MS ?? "10000");
const AGENT_PARTITIONS = Number(process.env.COST_AGENT_PARTITIONS ?? "4");
const AGENT_ARTIFACT_KB = Number(process.env.COST_AGENT_ARTIFACT_KB ?? "2048");
/// `agent-log`: one `ctx.log` line every `COST_AGENT_LOG_EVERY` 20 ms ticks (10 ⇒ 5 lines/s),
/// each line carrying `COST_AGENT_LOG_PAD` random bytes (0). Heavy logging (`EVERY=1`, `PAD=1500`
/// ≈ 750 KiB per `ctx.run`) measures the flush-when-full path: ≤ 1 PUT per 256 KiB of log.
const CHAT_S = Number(process.env.COST_CHAT_S ?? "60");
const CHAT_KEYS = Number(process.env.COST_CHAT_KEYS ?? "4");
const CHAT_SIZES_KIB = (process.env.COST_CHAT_SIZES_KIB ?? "64,256,1024,4096,8192").split(",").map(Number);
const CHAT_NEW = Number(process.env.COST_CHAT_NEW ?? "2048");
const CHAT_GROW = 256 * 1024;
/// `0` = the SDK does not cache segments (cold serverless); absent = the default 64 MiB.
const CHAT_SDK_CACHE = process.env.COST_CHAT_SDK_CACHE;
const AGENT_LOG_EVERY = Number(process.env.COST_AGENT_LOG_EVERY ?? "10");
const AGENT_LOG_PAD = Number(process.env.COST_AGENT_LOG_PAD ?? "0");
/** Counter slices (seconds): each slice records the cumulative count since the start of the
 * window, to separate the start-up tail from the steady state. 0 = off. */
const SLICE_S = Number(process.env.COST_SLICE_S ?? "300");
const QUIET_LEASE = process.env.COST_QUIET_LEASE ?? "";
if (!["", "off", "solo", "delegation"].includes(QUIET_LEASE)) {
  console.error(`COST_QUIET_LEASE only accepts off|solo|delegation, not ${QUIET_LEASE}`);
  process.exit(2);
}
const IDLE_REGISTER = process.env.COST_IDLE_REGISTER === "1";
/// Extra node flags, whitespace-separated (e.g. `--disk-cache-dir <dir>`
/// for the SST disk cache — each node gets a subdirectory `<dir>/<i>`).
const NODE_ARGS = (process.env.COST_NODE_ARGS ?? "").split(/\s+/).filter(Boolean);
/// The binary has the `--quiet-lease` node flag (default for new buckets).
const NODE_QUIET_FLAG =
  existsSync(binary) && spawnSync(binary, ["--help"], { encoding: "utf8" }).stdout?.includes("--quiet-lease");
const CTL = process.env.COST_CTL ?? join(dirname(binary), "cairn-ctl");
const ADMIN_KEY = `cf_cost_${randomBytes(16).toString("hex")}`;

const argv = process.argv.slice(2);
const flag = (name: string) => {
  const i = argv.indexOf(name);
  return i >= 0 ? argv[i + 1] : undefined;
};
const OUT = flag("--out") ?? join(repoRoot, "bench", "s3-model", "results", `run-${Date.now()}.jsonl`);
const SCENARIOS = (flag("--scenarios") ?? "tenants,workflows,dense,idle").split(",");

// Local SeaweedFS when no endpoint is set — like chaos.ts.
if (!process.env.AWS_ENDPOINT) {
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

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// ───────────────────────────────────────────── services of the sample workloads

const small = service("CostSmall", {
  async work(ctx: ServiceContext, input: { n: number }) {
    const a = await ctx.run("a", () => input.n + 1);
    const b = await ctx.run("b", () => a * 2);
    return { b };
  },
});

const dense = service("CostDense", {
  async work(ctx: ServiceContext, input: { n: number }) {
    const a = await ctx.run("a", () => input.n);
    const b = await ctx.run("b", () => a + 1);
    const c = await ctx.run("c", () => b + 1);
    await ctx.call("CostCounter", "add", { c }, { key: `k${input.n % 1024}` });
    return { c };
  },
});

const counter = service(
  "CostCounter",
  {
    async add(ctx: Context, input: { c: number }) {
      const v = ((await ctx.get<number>("v")) ?? 0) + input.c;
      ctx.set("v", v);
      return { v };
    },
  },
  { type: "VIRTUAL_OBJECT", handlerTypes: { add: "EXCLUSIVE" } },
);

const longFlow = workflow("CostFlow", {
  async run(ctx: Context, _input: unknown) {
    for (let i = 0; i < 4; i++) {
      await ctx.sleep(2000);
      await ctx.run(`step-${i}`, () => i);
    }
    const ok = await ctx.promise<string>("approve").get();
    return { ok };
  },
  async approve(ctx: Context, input: { v: string }) {
    await ctx.promise<string>("approve").resolve(input.v);
    return { ok: true };
  },
});

/** Sleeps `ms`, calling `tick` every `every` ms while sleeping. */
async function ticking(ms: number, every: number, tick: (i: number) => void): Promise<void> {
  const end = Date.now() + ms;
  for (let i = 0; Date.now() < end; i++) {
    tick(i);
    await sleep(Math.min(every, Math.max(0, end - Date.now())));
  }
}

const TOKEN = "x".repeat(12);
const logPad = () => (AGENT_LOG_PAD > 0 ? { pad: randomBytes((AGENT_LOG_PAD * 3) / 4).toString("base64") } : {});
/// Random bytes — incompressible (SlateDB's WAL is compressed, so a repetitive string would
/// measure the wrong WAL bytes for `agent-inline`). Base64 ¾ ⇒ a string of exactly `AGENT_ARTIFACT_KB` KiB.
const agentBlob = () => randomBytes((AGENT_ARTIFACT_KB * 1024 * 3) / 4).toString("base64");

const agent = service("CostAgent", {
  async base(ctx: ServiceContext, input: { n: number }) {
    return ctx.run("model", async () => (await sleep(AGENT_RUN_MS), input.n));
  },
  async stream(ctx: ServiceContext, input: { n: number }) {
    const ui = ctx.stream("ui");
    // 50 tokens/s × ~20 B per JSON record.
    return ctx.run("model", async () => (await ticking(AGENT_RUN_MS, 20, (i) => ui.append({ t: TOKEN, i })), input.n));
  },
  async log(ctx: ServiceContext, input: { n: number }) {
    const ui = ctx.stream("ui");
    return ctx.trace.span(
      "llm",
      () =>
        ctx.run("model", async () => {
          await ticking(AGENT_RUN_MS, 20, (i) => {
            ui.append({ t: TOKEN, i });
            if (i % AGENT_LOG_EVERY === 0) ctx.log.info("progress", { i, n: input.n, ...logPad() });
          });
          return input.n;
        }),
      { model: "bench" },
    );
  },
  async artifact(ctx: ServiceContext, input: { n: number }) {
    const refs: string[] = [];
    for (let i = 0; i < 10; i++) {
      const r = await ctx.run(`a${i}`, () => ctx.artifact.put(agentBlob()));
      refs.push(r.sha256);
    }
    return { refs: refs.length };
  },
  async inline(ctx: ServiceContext, input: { n: number }) {
    let total = 0;
    for (let i = 0; i < 10; i++) {
      const v = await ctx.run(`a${i}`, () => agentBlob());
      total += v.length;
    }
    return { total };
  },
});

/// Chat history in one state key (the shape of `chatObject`): each turn reads and
/// writes back the whole history plus the new part. The new part comes from the
/// input — replayed turns are deterministic.
const chat = service(
  "CostChat",
  {
    async send(ctx: Context, input: { pad: string }) {
      const h = (await ctx.get<string>("messages")) ?? "";
      const v = h + input.pad;
      ctx.set("messages", v);
      return { len: v.length };
    },
  },
  { type: "VIRTUAL_OBJECT", handlerTypes: { send: "EXCLUSIVE" } },
);

/// A random string of exactly `n` base64 characters (incompressible).
const chatPad = (n: number) => randomBytes(Math.ceil((n * 3) / 4) + 3).toString("base64").slice(0, n);

// ───────────────────────────────────────────── cluster

type Node = { proc: ChildProcess; url: string; log: string[] };

async function spawnNode(store: string, i: number, partitions: number): Promise<Node> {
  const args = [
    "--main-store", store,
    "--cluster",
    "--partitions", String(partitions),
    "--port", "0",
    "--node-id", `cost-${i}`,
    ...NODE_ARGS.map((a, j) => (NODE_ARGS[j - 1] === "--disk-cache-dir" ? join(a, String(i)) : a)),
  ];
  const proc = spawn(binary, args, {
    stdio: "pipe",
    env: {
      ...process.env,
      CAIRNFLOW_STORE_PROFILE: profile,
      CAIRNFLOW_ADMIN_KEY: ADMIN_KEY,
      ...(QUIET_LEASE && NODE_QUIET_FLAG ? { CAIRNFLOW_QUIET_LEASE: QUIET_LEASE } : {}),
      // `/invoke` waits until done (default 5 s, then `202`): the agent workloads'
      // closed loop must wait out the full 10 s `ctx.run`, otherwise it turns into open load.
      CAIRNFLOW_INVOKE_WAIT_MS: "120000",
      // The node's turn cap (default 32) must not throttle `COST_AGENT_CONC`.
      CAIRNFLOW_MAX_CONCURRENT_TURNS: String(Math.max(32, 2 * AGENT_CONC)),
    },
  });
  const log: string[] = [];
  const url = await new Promise<string>((resolve, reject) => {
    let buf = "";
    const t = setTimeout(() => reject(new Error(`node ${i} did not report its port:\n${buf}`)), 30_000);
    const take = (c: Buffer) => {
      buf += c.toString();
      if (log.length < 2000) log.push(c.toString());
      const m = /listening on (http:\/\/[\d.]+:\d+)/.exec(buf);
      if (m) {
        clearTimeout(t);
        resolve(m[1]!);
      }
    };
    proc.stdout!.on("data", take);
    proc.stderr!.on("data", (c: Buffer) => {
      if (log.length < 2000) log.push(c.toString());
    });
    proc.on("exit", (code) => reject(new Error(`node ${i} exited early (${code}):\n${buf}`)));
  });
  return { proc, url, log };
}

function auth(key: string = ADMIN_KEY): Record<string, string> {
  return { authorization: `Bearer ${key}` };
}

/** Leader-following fetch like chaos.ts: a 503 with `leader` is followed, otherwise back off. */
async function cfetch(nodes: Node[], path: string, init: RequestInit = {}, timeoutMs = 30_000): Promise<Response> {
  const deadline = Date.now() + timeoutMs;
  let base = nodes[Math.floor(Math.random() * nodes.length)]!.url;
  for (;;) {
    try {
      const r = await fetch(base + path, init);
      if ((r.status === 503 || (r.status === 404 && path.startsWith("/invoke/"))) && Date.now() < deadline) {
        const peek = (await r.clone().json().catch(() => undefined)) as { leader?: string } | undefined;
        if (peek?.leader) base = peek.leader.replace(/\/$/, "");
        else await sleep(200);
        continue;
      }
      return r;
    } catch (e) {
      if (Date.now() > deadline) throw e;
      await sleep(200);
    }
  }
}

async function waitLeaders(nodes: Node[]) {
  const deadline = Date.now() + 120_000;
  for (;;) {
    const c = (await cfetch(nodes, "/cluster", { headers: auth() }).then((r) => r.json())) as any;
    if (c.partitions?.every((p: any) => p.leader)) return;
    if (Date.now() > deadline) throw new Error(`cluster lacks leaders: ${JSON.stringify(c)}`);
    await sleep(500);
  }
}

/// Enables the quiet-renewal gate on `store` before any node exists (the command waits
/// `ctl::BOOT_WAIT` after setting `min_binary`).
async function enableQuietLease(store: string) {
  const args = ["--main-store", store, "--json", "cluster", "enable-quiet-lease", "--apply"];
  if (QUIET_LEASE === "delegation") {
    args.push("--delegation");
    // The flag that bypasses the evidence gate only exists in a cairn-ctl that has that gate.
    const help = spawnSync(CTL, ["cluster", "enable-quiet-lease", "--help"], { encoding: "utf8" });
    if (`${help.stdout}${help.stderr}`.includes("--accept-missing-evidence")) args.push("--accept-missing-evidence");
  }
  const r = spawnSync(CTL, args, { encoding: "utf8", timeout: 600_000, env: process.env });
  if (r.status !== 0) throw new Error(`enable-quiet-lease failed (${r.status}): ${r.stderr}${r.stdout}`);
  console.log(`   quiet-renewal gate (${QUIET_LEASE}) enabled`);
}

// ───────────────────────────────────────────── counters

type Counts = Record<string, number>;

/** Sums every `cairnflow_testkit_store_*` line of every node, by verbatim labels. */
async function scrape(nodes: Node[]): Promise<Counts> {
  const out: Counts = {};
  for (const n of nodes) {
    const text = await fetch(`${n.url}/metrics`, { headers: auth() }).then((r) => r.text());
    for (const line of text.split("\n")) {
      // Durable waits and stream-flush-only commits: each durable wait may cut a
      // WAL object, so WAL PUTs follow it. Stream payload and flush commits by label
      // (`durable`, `reason`): compares Db bytes with the durable payload, and
      // flush-when-full PUTs with the 256 KiB of log.
      if (/^cairnflow_stream_(bytes|records|flush_commits)_total\{/.test(line) || /^cairnflow_stream_row(s_carried|_commits|s_extra)_total /.test(line)) {
        const sp = line.lastIndexOf(" ");
        const key = line.slice(0, sp);
        out[key] = (out[key] ?? 0) + Number(line.slice(sp + 1));
        continue;
      }
      // Bytes to/from the SDK and the segmented-value counters.
      if (/^cairnflow_sdk_bytes_total\{/.test(line) || /^cairnflow_value_[a-z_]+_total /.test(line)) {
        const sp = line.lastIndexOf(" ");
        const key = line.slice(0, sp);
        out[key] = (out[key] ?? 0) + Number(line.slice(sp + 1));
        continue;
      }
      if (/^cairnflow_(durable_waits(_total)?|commit_wait_seconds_count)[{ ]/.test(line)) {
        const sp = line.lastIndexOf(" ");
        const key = line.slice(0, sp).replace(/\{.*\}/, "");
        out[key] = (out[key] ?? 0) + Number(line.slice(sp + 1));
        continue;
      }
      if (!line.startsWith("cairnflow_testkit_store_")) continue;
      const sp = line.lastIndexOf(" ");
      const key = line.slice(0, sp).replace(/tier="[^"]*",?/, "").replace("{}", "");
      out[key] = (out[key] ?? 0) + Number(line.slice(sp + 1));
    }
  }
  if (Object.keys(out).length === 0) {
    throw new Error(`no cairnflow_testkit_store_* on /metrics — was ${binary} built with --features testkit?`);
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

/** Bytes and object count under `uri` per `aws s3 ls --summarize`. */
function storage(uri: string): { bytes: number; objects: number } {
  const r = spawnSync(
    "aws",
    ["s3", "ls", `${uri.replace(/\/$/, "")}/`, "--recursive", "--summarize", "--endpoint-url", process.env.AWS_ENDPOINT!],
    { encoding: "utf8", env: process.env },
  );
  const objects = Number(/Total Objects: (\d+)/.exec(r.stdout)?.[1] ?? "NaN");
  const bytes = Number(/Total Size: (\d+)/.exec(r.stdout)?.[1] ?? "NaN");
  return { bytes, objects };
}

function hourOf(ms: number): string {
  return new Date(ms).toISOString().slice(0, 13);
}

/** Steps + invocations of the tenants, summed over every hour in `[from, to]`. */
async function metering(nodes: Node[], tenants: string[], from: number, to: number) {
  const hours = new Set<string>();
  for (let t = from - 3_600_000; t <= to + 3_600_000; t += 1_800_000) hours.add(hourOf(t));
  let steps = 0;
  let invocations = 0;
  for (const tenant of tenants) {
    for (const hour of hours) {
      const r = await cfetch(nodes, `/metering?tenant=${encodeURIComponent(tenant)}&hour=${hour}`, { headers: auth() });
      if (!r.ok) throw new Error(`/metering ${tenant} ${hour}: ${r.status} ${await r.text()}`);
      const m = (await r.json()) as { steps: number; invocations: number };
      steps += m.steps;
      invocations += m.invocations;
    }
  }
  return { steps, invocations };
}

// ───────────────────────────────────────────── one measurement run

type Ctx = { nodes: Node[]; sdkUrl: string };
type Scenario = {
  name: string;
  partitions: number;
  nodes?: number;
  seconds: number;
  /** Preparation (keys, deployment). Returns the list of tenants to read metering for. */
  setup: (c: Ctx) => Promise<{ tenants: string[]; keys: Map<string, string> }>;
  /** Generates load until `end`. */
  load: (c: Ctx, keys: Map<string, string>, end: number) => Promise<Record<string, number>>;
};

async function register(nodes: Node[], sdkUrl: string, key: string) {
  const r = await cfetch(nodes, "/deployments", {
    method: "POST",
    headers: { ...auth(key), "content-type": "application/json" },
    body: JSON.stringify({ uri: sdkUrl }),
  });
  if (!r.ok) throw new Error(`register deployment: ${r.status} ${await r.text()}`);
}

async function createKey(nodes: Node[], tenant: string): Promise<string> {
  const r = await cfetch(nodes, "/admin/keys", {
    method: "POST",
    headers: { ...auth(), "content-type": "application/json" },
    body: JSON.stringify({ tenant, role: "admin" }),
  });
  if (!r.ok) throw new Error(`create key ${tenant}: ${r.status} ${await r.text()}`);
  const body = (await r.json()) as { secret?: string; key?: string };
  const secret = body.secret ?? body.key;
  if (!secret) throw new Error(`key ${tenant} has no secret: ${JSON.stringify(body)}`);
  return secret;
}

async function measure(sc: Scenario, sdkUrl: string) {
  const store = `${BASE}/${sc.name}-${sc.partitions}p`;
  const nNodes = sc.nodes ?? NODES;
  console.log(`\n== ${sc.name}: ${nNodes} node × ${sc.partitions} ranges, ${sc.seconds}s, store ${store}`);
  const nodes: Node[] = [];
  try {
    if (QUIET_LEASE && QUIET_LEASE !== "off" && !NODE_QUIET_FLAG) await enableQuietLease(store);
    for (let i = 0; i < nNodes; i++) nodes.push(await spawnNode(store, i, sc.partitions));
    await waitLeaders(nodes);
    const ctx = { nodes, sdkUrl };
    const { tenants, keys } = await sc.setup(ctx);
    await sleep(WARMUP_S * 1000);

    const t0 = Date.now();
    const c0 = await scrape(nodes);
    const s0 = storage(store);
    const samples = [{ t: 0, ...s0 }];
    const sampler = setInterval(() => {
      samples.push({ t: Math.round((Date.now() - t0) / 1000), ...storage(store) });
    }, SAMPLE_S * 1000);
    const slices: { t: number; counts: Counts }[] = [];
    const slicer =
      SLICE_S > 0
        ? setInterval(() => {
            const t = Math.round((Date.now() - t0) / 1000);
            scrape(nodes)
              .then((c) => slices.push({ t, counts: diff(c0, c) }))
              .catch(() => {});
          }, SLICE_S * 1000)
        : undefined;
    const end = t0 + sc.seconds * 1000;
    const outcome = await sc.load(ctx, keys, end);
    clearInterval(sampler);
    if (slicer) clearInterval(slicer);
    const t1 = Date.now();
    const c1 = await scrape(nodes);
    samples.push({ t: Math.round((t1 - t0) / 1000), ...storage(store) });

    // One metering flush tick (60 s) before reading the steps.
    await sleep(70_000);
    const m = await metering(nodes, tenants, t0, t1);
    const row = {
      scenario: sc.name,
      nodes: nNodes,
      partitions: sc.partitions,
      seconds: (t1 - t0) / 1000,
      steps: m.steps,
      invocations: m.invocations,
      tenants: tenants.length,
      // The artifact / inline string size of the agent workloads — the price model
      // separates runs of different sizes (64 KiB, 256 KiB, 1 MiB, 2 MiB).
      ...(sc.name.startsWith("agent-") ? { artifact_kb: AGENT_ARTIFACT_KB } : {}),
      ...(sc.name === "agent-log" ? { log_every: AGENT_LOG_EVERY, log_pad: AGENT_LOG_PAD } : {}),
      ...(sc.name.startsWith("chat-long-")
        ? {
            chat_kib: Number(/chat-long-(\d+)k/.exec(sc.name)![1]),
            chat_new: CHAT_NEW,
            chat_keys: CHAT_KEYS,
            read_path: /--value-read-path\s+(\w+)/.exec(NODE_ARGS.join(" "))?.[1] ?? "auto",
            sdk_cache: CHAT_SDK_CACHE === "0" ? "cold" : "warm",
          }
        : {}),
      outcome,
      counts: diff(c0, c1),
      slices,
      storage: samples,
      store,
      binary,
      profile,
      quietLease: QUIET_LEASE || undefined,
      nodeArgs: NODE_ARGS.length > 0 ? NODE_ARGS.join(" ") : undefined,
      idleRegister: (sc.name === "idle" && IDLE_REGISTER) || undefined,
      at: new Date(t0).toISOString(),
    };
    mkdirSync(dirname(OUT), { recursive: true });
    appendFileSync(OUT, JSON.stringify(row) + "\n");
    const total = Object.entries(row.counts)
      .filter(([k]) => k.startsWith("cairnflow_testkit_store_requests_total"))
      .reduce((a, [, v]) => a + v, 0);
    console.log(
      `   ${row.seconds.toFixed(0)}s · ${m.steps} steps · ${m.invocations} invocation · ${total} request ` +
        `(${(total / row.seconds).toFixed(1)}/s) · ${JSON.stringify(outcome)}`,
    );
  } finally {
    for (const n of nodes) n.proc.kill("SIGTERM");
    await sleep(1000);
    for (const n of nodes) if (n.proc.exitCode === null) n.proc.kill("SIGKILL");
  }
}

// ───────────────────────────────────────────── the four workloads

const DEFAULT_TENANT = "default";

/// Other nodes learn a new key after one registry refresh tick (`--deploy-refresh-ms`, default 5 s).
const KEY_PROPAGATION_MS = 7_000;

async function oneTenant(c: Ctx) {
  const key = await createKey(c.nodes, "cost-dense");
  await sleep(KEY_PROPAGATION_MS);
  await register(c.nodes, c.sdkUrl, key);
  return { tenants: ["cost-dense"], keys: new Map([["cost-dense", key]]) };
}

const scenarios: Scenario[] = [
  {
    name: "tenants",
    partitions: PARTITIONS,
    seconds: TENANTS_S,
    async setup(c) {
      const keys = new Map<string, string>();
      for (let i = 0; i < TENANTS; i++) {
        const t = `cost-t${String(i).padStart(3, "0")}`;
        keys.set(t, await createKey(c.nodes, t));
      }
      await sleep(KEY_PROPAGATION_MS);
      for (const k of keys.values()) await register(c.nodes, c.sdkUrl, k);
      return { tenants: [...keys.keys()], keys };
    },
    async load(c, keys, end) {
      let ok = 0;
      let bad = 0;
      // One loop per tenant: wait an exponential random time (mean TENANT_GAP_S), then call.
      await Promise.all(
        [...keys.values()].map(async (key, i) => {
          let n = 0;
          for (;;) {
            await sleep(-Math.log(1 - Math.random()) * TENANT_GAP_S * 1000);
            if (Date.now() >= end) return;
            const r = await cfetch(c.nodes, "/invoke/CostSmall/work", {
              method: "POST",
              headers: auth(key),
              body: JSON.stringify({ n: i * 1_000_000 + n++ }),
            }).catch(() => undefined);
            if (r?.ok) ok++;
            else bad++;
            await r?.body?.cancel().catch(() => {});
          }
        }),
      );
      return { ok, bad };
    },
  },
  {
    name: "workflows",
    partitions: PARTITIONS,
    seconds: WF_S,
    setup: oneTenant,
    async load(c, keys, end) {
      const key = keys.get("cost-dense")!;
      let started = 0;
      let approved = 0;
      let bad = 0;
      const pending: Promise<void>[] = [];
      while (Date.now() < end) {
        const wfKey = `wf-${started++}-${Date.now()}`;
        const r = await cfetch(c.nodes, `/invoke/CostFlow/run/send?key=${wfKey}`, {
          method: "POST",
          headers: auth(key),
          body: "{}",
        }).catch(() => undefined);
        await r?.body?.cancel().catch(() => {});
        if (!r?.ok) {
          bad++;
        } else {
          // Resolve the promise after the four sleeps (~8 s) plus a little delay.
          pending.push(
            (async () => {
              await sleep(10_000);
              const a = await cfetch(c.nodes, `/invoke/CostFlow/approve?key=${wfKey}`, {
                method: "POST",
                headers: auth(key),
                body: JSON.stringify({ v: "ok" }),
              }).catch(() => undefined);
              if (a?.ok) approved++;
              else bad++;
              await a?.body?.cancel().catch(() => {});
            })(),
          );
        }
        await sleep(1000 / WF_RATE);
      }
      // Stop starting at `end`, but the measurement window extends until the last
      // workflow finishes: their steps are in metering, so their requests must be
      // in the counts too. The idle part of the tail is subtracted in `cost.ts`.
      await Promise.all(pending);
      await sleep(3_000);
      return { started, approved, bad };
    },
  },
  {
    name: "dense",
    partitions: PARTITIONS,
    seconds: DENSE_S,
    setup: oneTenant,
    async load(c, keys, end) {
      const key = keys.get("cost-dense")!;
      let ok = 0;
      let bad = 0;
      let n = 0;
      await Promise.all(
        Array.from({ length: DENSE_CONC }, async () => {
          while (Date.now() < end) {
            const r = await cfetch(c.nodes, "/invoke/CostDense/work", {
              method: "POST",
              headers: auth(key),
              body: JSON.stringify({ n: n++ }),
            }).catch(() => undefined);
            if (r?.ok) ok++;
            else bad++;
            await r?.body?.cancel().catch(() => {});
          }
        }),
      );
      return { ok, bad };
    },
  },
  ...(["base", "stream", "log", "artifact", "inline"] as const).map(
    (h): Scenario => ({
      name: `agent-${h}`,
      partitions: AGENT_PARTITIONS,
      nodes: 1,
      seconds: AGENT_S,
      setup: oneTenant,
      async load(c, keys, end) {
        const key = keys.get("cost-dense")!;
        let ok = 0;
        let waiting = 0;
        let bad = 0;
        // `bad_<code>` by the code of the failed call (`bad_err` = no response) —
        // separates overload from real errors.
        const badBy: Record<string, number> = {};
        let n = 0;
        await Promise.all(
          Array.from({ length: AGENT_CONC }, async () => {
            while (Date.now() < end) {
              const r = await cfetch(
                c.nodes,
                `/invoke/CostAgent/${h}`,
                { method: "POST", headers: auth(key), body: JSON.stringify({ n: n++ }) },
                120_000,
              ).catch(() => undefined);
              // Only `200` is done; `202` (wait cap reached) is counted separately.
              if (r?.status === 200) ok++;
              else if (r?.status === 202) waiting++;
              else {
                bad++;
                const k = `bad_${r === undefined ? "err" : r.status}`;
                badBy[k] = (badBy[k] ?? 0) + 1;
              }
              await r?.body?.cancel().catch(() => {});
            }
          }),
        );
        return { ok, waiting, bad, ...badBy };
      },
    }),
  ),
  ...IDLE_PARTITIONS.map(
    (p): Scenario => ({
      name: "idle",
      partitions: p,
      seconds: IDLE_S,
      async setup(c) {
        // `COST_IDLE_REGISTER=1`: one deployment before the measurement window, like a real cluster.
        // The first registry write creates `control/registry-gen`; a bucket that never wrote
        // the registry lacks that object and every registry/isolation poll tick is a full round.
        if (IDLE_REGISTER) await register(c.nodes, c.sdkUrl, ADMIN_KEY);
        return { tenants: [DEFAULT_TENANT], keys: new Map() };
      },
      async load(_c, _k, end) {
        await sleep(Math.max(0, end - Date.now()));
        return {};
      },
    }),
  ),
  ...(["overview", "cluster", "usage"] as const).map(
    (v): Scenario => ({
      name: `console-${v}`,
      partitions: PARTITIONS,
      seconds: IDLE_S,
      async setup(c) {
        // No deployment ⇒ Overview only shows the welcome screen and asks nothing.
        await register(c.nodes, c.sdkUrl, ADMIN_KEY);
        return { tenants: [DEFAULT_TENANT], keys: new Map() };
      },
      load: (c, _k, end) => consolePoll(c.nodes[0]!, v, end),
    }),
  ),
];

/**
 * A simulated browser parked on one console screen until `end`, with exactly the
 * refresh cadence of `ui/src` (TanStack Query: one loop per key, the next run
 * scheduled from when the previous one finished). Returns request counts by kind.
 */
async function consolePoll(node: Node, view: "overview" | "cluster" | "usage", end: number) {
  const out: Record<string, number> = { sql: 0, rest: 0, bad: 0 };
  const get = (path: string) => async () => {
    const r = await fetch(node.url + path, { headers: auth() });
    out[r.ok ? "rest" : "bad"]!++;
    await r.arrayBuffer();
    return undefined;
  };
  const sql = (q: { sql: string; params: unknown[] }) => async () => {
    const r = await fetch(node.url + "/query", {
      method: "POST",
      headers: { ...auth(), "content-type": "application/json" },
      body: JSON.stringify({ sql: q.sql, params: q.params, allow_partial: true }),
    });
    out[r.ok ? "sql" : "bad"]!++;
    const body = (await r.json().catch(() => ({}))) as { ms?: number };
    return body.ms;
  };
  /** One key: run, then wait `every(ms)` until the next one. */
  const loop = async (run: () => Promise<number | undefined>, every: (ms: number | undefined) => number) => {
    while (Date.now() < end) {
      const ms = await run().catch(() => ((out.bad! += 1), undefined));
      await sleep(Math.min(every(ms), Math.max(0, end - Date.now())));
    }
  };
  const fixed = (n: number) => () => n;
  const adaptive = (ms: number | undefined) =>
    Math.min(OVERVIEW_REFRESH_MS.max, Math.max(OVERVIEW_REFRESH_MS.min, 10 * (ms ?? 0)));
  const hourly = (ms: number | undefined) =>
    Math.min(HISTORY_REFRESH_MS.max, Math.max(HISTORY_REFRESH_MS.min, 10 * (ms ?? 0)));
  const since = Date.now() - 24 * 3_600_000;
  const hour = (t: number) => new Date(t).toISOString().slice(0, 13);
  // The top bar's health (`components/shell/shell.tsx`, `HEALTH_POLL`): every screen.
  const shell = [
    loop(get("/cluster"), fixed(30_000)),
    loop(get("/cluster/nodes?scope=cluster"), fixed(30_000)),
  ];
  const loops =
    view === "overview"
      ? [
          ...shell,
          loop(get("/deployments"), fixed(30_000)),
          ...[
            consoleQueries.invocationCounts(),
            consoleQueries.handlerStats(),
            consoleQueries.serviceHealth(),
            consoleQueries.deploymentsPinned(),
          ].map((q) => loop(sql(q), adaptive)),
          loop(sql(consoleQueries.history(since)), hourly),
        ]
      : view === "cluster"
        ? // The Cluster screen asks the same `cluster`/`cluster-nodes` keys as the top bar (one
          // loop per key, the fastest cadence wins) — 5 s.
          ["/cluster", "/cluster/nodes?scope=cluster", "/ranges/ops?scope=cluster", "/ranges/suggestions?scope=cluster"].map(
            (p) => loop(get(p), fixed(5_000)),
          )
        : [
            ...shell,
            loop(get(`/metering/hours?from=${hour(since)}&to=${hour(Date.now())}`), fixed(60_000)),
            ...[consoleQueries.history(since), consoleQueries.historyByHandler(since)].map((q) => loop(sql(q), hourly)),
            loop(sql(consoleQueries.meteringNow()), adaptive),
          ];
  await Promise.all(loops);
  return out;
}

for (const kib of CHAT_SIZES_KIB) {
  scenarios.push({
    name: `chat-long-${kib}k`,
    partitions: 4,
    nodes: 1,
    seconds: CHAT_S,
    async setup(c) {
      const key = await createKey(c.nodes, "cost-chat");
      await sleep(KEY_PROPAGATION_MS);
      const r = await cfetch(c.nodes, "/admin/tenants/cost-chat", {
        method: "PUT",
        headers: { ...auth(), "content-type": "application/json" },
        body: JSON.stringify({ maxPayload: 1 << 20 }),
      });
      if (!r.ok) throw new Error(`max-payload: ${r.status} ${await r.text()}`);
      await register(c.nodes, c.sdkUrl, key);
      // History up to H by 256 KiB append turns (like a conversation growing longer:
      // segments layered as in the product, not one H-sized segment).
      await Promise.all(
        Array.from({ length: CHAT_KEYS }, async (_, k) => {
          for (let len = 0; len < kib * 1024; len += CHAT_GROW) {
            const n = Math.min(CHAT_GROW, kib * 1024 - len);
            const g = await cfetch(c.nodes, `/invoke/CostChat/send?key=c${k}`, {
              method: "POST",
              headers: { ...auth(key), "content-type": "application/json" },
              body: JSON.stringify({ pad: chatPad(n) }),
            });
            if (!g.ok) throw new Error(`grow c${k}: ${g.status} ${await g.text()}`);
            await g.arrayBuffer();
          }
        }),
      );
      return { tenants: ["cost-chat"], keys: new Map([["cost-chat", key]]) };
    },
    async load(c, keys, end) {
      const key = keys.get("cost-chat")!;
      let ok = 0;
      let bad = 0;
      await Promise.all(
        Array.from({ length: CHAT_KEYS }, async (_, k) => {
          while (Date.now() < end) {
            const r = await cfetch(c.nodes, `/invoke/CostChat/send?key=c${k}`, {
              method: "POST",
              headers: { ...auth(key), "content-type": "application/json" },
              body: JSON.stringify({ pad: chatPad(CHAT_NEW) }),
            }).catch(() => undefined);
            if (r?.ok) ok++;
            else bad++;
            await r?.arrayBuffer().catch(() => {});
          }
        }),
      );
      return { ok, bad };
    },
  });
}

async function main() {
  if (!existsSync(binary)) {
    console.error(`missing ${binary} — cargo build --bin cairnflow --features testkit --target-dir target/tk`);
    process.exit(2);
  }
  console.log(`s3-cost: ${binary}\n  profile ${profile}\n  store ${BASE} (${process.env.AWS_ENDPOINT})\n  output ${OUT}`);
  const sdk = await serve({
    services: [small, dense, counter, longFlow, agent, chat],
    port: 0,
    ...(CHAT_SDK_CACHE === undefined ? {} : { valueCacheBytes: Number(CHAT_SDK_CACHE) }),
  });
  try {
    for (const sc of scenarios.filter((s) => SCENARIOS.includes(s.name))) {
      await measure(sc, sdk.url);
    }
  } finally {
    await sdk.close?.();
  }
  console.log(`\ndone — ${OUT}`);
  process.exit(0);
}

await main();
