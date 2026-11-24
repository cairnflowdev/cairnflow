// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

/**
 * Cost matrix of `REQUEST_RESPONSE` vs `BIDI_STREAM`: {steps per invocation} ×
 * {result size per step} × {mode}. Each cell gets a fresh `cairnflow` cluster;
 * counts object-store requests by op × class (the `count-only` profile, like
 * `s3-cost.ts`), PUT/GET bytes, and **HTTP body bytes to and from the SDK**
 * (`cairnflow_sdk_bytes_total{mode,dir}`). No prices applied — `cost.ts` does that.
 *
 * ```sh
 * cargo build --release --bin cairnflow --features testkit --target-dir target/tk
 * node harness/bench/cost-matrix.ts --out bench/s3-model/results/cost-matrix.jsonl
 * # A subset on a 3-node SeaweedFS (scripts/dev-store/up.sh):
 * CM_STORE=s3://walbench/cm-$RANDOM CM_NODES=3 CM_CELLS=bidi-10x10,rr-10x10,rr-50x100 \
 *   node harness/bench/cost-matrix.ts --out …
 * ```
 *
 * **One cell** (`<mode>-<steps>x<KiB>`): the `Matrix/run` handler runs `ctx.run`
 * `steps` times, each returning a random `KiB` KiB string (base64 of random bytes —
 * SlateDB's WAL is compressed, a repetitive string would measure the wrong bytes).
 * `CM_INV` invocations, `CM_CONC` closed `/invoke` loops (waiting until done).
 * Before the measurement window: warm-up, then the wait for metering to flush
 * (≥ `CM_IDLE_S`, ≥ 70 s) serves as the idle background (written to the row's
 * `idle`, `cost.ts` subtracts it in proportion to time). After the load:
 * `CM_TAIL_S` seconds of tail still count toward the cell (WAL flush, compaction
 * caused by the cell itself). **Steps** are read from `GET /metering` after one
 * flush tick (60 s), exactly the billing unit.
 *
 * `CM_PARALLEL` cells run at once (one process per cell, its own `memory://`): the
 * request counts do not depend on that; on SeaweedFS use 1.
 *
 * One JSON line per cell (`--out`), the same row shape as `s3-cost.ts` plus `mode`,
 * `steps_per_inv`, `kb`, `idle`, `payload`, `compression`.
 *
 * **RR body compression**: `CM_PAYLOAD=json` replaces the random string with
 * agent-step-like JSON (repeated vocabulary, numbers, ids — compresses like real
 * text); the cell name gets `-json`. `CM_COMPRESSION=0` for an SDK that does not
 * declare `requestCompression` (the "before compression" baseline on the same
 * binary); the cell name gets `-plain`.
 *
 * **Incremental replay** (wire v5): the RR SDK enables `replayCache` (the
 * default). `CM_DELTA=0` disables it (the "before v5" baseline on the same
 * binary); the cell name gets `-nodelta`. `CM_REPLICAS=2` puts two RR SDK replicas
 * behind an in-process h1 proxy: `CM_AFFINITY=none` (default) round-robins,
 * `cookie` sticks by the `cm_lb` cookie the proxy sets on the first response
 * (like ALB); the cell name gets `-r2`/`-r2c`.
 *
 * **Use-case cells** (`USECASES` below): selected by name in `CM_CELLS` like grid
 * cells, or `CM_CELLS=usecases` for the whole set. Without `CM_CELLS` only the grid
 * runs, as before. Each cell has its own handler simulating the shape (payload
 * always `json()`, sizes uniformly random within a range), and one kind of SDK
 * endpoint:
 *
 * | kind | endpoint |
 * |---|---|
 * | `bidi` | `serve()` `BIDI_STREAM` |
 * | `rr` | `serve()` `REQUEST_RESPONSE` with `identityKeys`, `replayCache` on (the default of `serve()` with identity) |
 * | `rr-ttl` | like `rr`, `replayCache: { ttlMs: 1500 }` — the cache entry expires during A3's 4 s wait |
 * | `fetch` | the real `fetchHandler()` (cache off, its default) behind an in-process `node:http` h1 server |
 *
 * `CM_NODE_ARGS` (space-separated) adds flags to every node (for example
 * `--wal-linger-us 20000`); `CM_TAG` is appended to the cell name (`-<tag>`) to tell runs apart.
 *
 * Use-case cell rows add `usecase`, `sdk`, `units` (from `/metering`) and `roots`
 * (root invocations — W4 counts parents, `invocations` includes the children).
 * Dollar figures are computed from these rows by a separate price model.
 */

import { spawn, type ChildProcess } from "node:child_process";
import { randomBytes } from "node:crypto";
import { appendFileSync, existsSync, mkdirSync, readFileSync } from "node:fs";
import { createServer } from "node:http";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { type Context, type Endpoint, fetchHandler, serve, service, type ServiceContext } from "../../sdk/typescript/src/index.ts";

const here = fileURLToPath(new URL(".", import.meta.url));
const repoRoot = join(here, "..", "..");
const env = process.env;
const binary = env.CM_BINARY ?? join(repoRoot, "target", "tk", "release", "cairnflow");
const profile = env.CM_PROFILE ?? join(repoRoot, "bench", "s3-model", "profiles", "count-only.json");
const STORE = env.CM_STORE ?? "memory://";
const NODES = Number(env.CM_NODES ?? "1");
const PARTITIONS = Number(env.CM_PARTITIONS ?? "4");
const INV = Number(env.CM_INV ?? "32");
const CONC = Number(env.CM_CONC ?? "8");
const IDLE_S = Number(env.CM_IDLE_S ?? "70");
const TAIL_S = Number(env.CM_TAIL_S ?? "5");
const PARALLEL = Number(env.CM_PARALLEL ?? (STORE.startsWith("memory") ? "3" : "1"));
const STEPS = (env.CM_STEPS ?? "1,10,50").split(",").map(Number);
const KBS = (env.CM_KB ?? "1,10,100").split(",").map(Number);
const MODES = (env.CM_MODES ?? "BIDI_STREAM,REQUEST_RESPONSE").split(",") as Mode[];
const PAYLOAD = env.CM_PAYLOAD === "json" ? "json" : "random";
const COMPRESSION = env.CM_COMPRESSION !== "0";
const DELTA = env.CM_DELTA !== "0";
const REPLICAS = Number(env.CM_REPLICAS ?? "1");
const AFFINITY = env.CM_AFFINITY === "cookie" ? "cookie" : "none";
const NODE_ARGS = (env.CM_NODE_ARGS ?? "").split(" ").filter(Boolean);
const TAG = env.CM_TAG ? `-${env.CM_TAG}` : "";
const suffix =
  `${PAYLOAD === "json" ? "-json" : ""}${COMPRESSION ? "" : "-plain"}${DELTA ? "" : "-nodelta"}` +
  `${REPLICAS > 1 ? (AFFINITY === "cookie" ? "-r2c" : "-r2") : ""}${TAG}`;
type Mode = "BIDI_STREAM" | "REQUEST_RESPONSE";
const short = (m: Mode) => (m === "BIDI_STREAM" ? "bidi" : "rr");

const argv = process.argv.slice(2);
const flag = (name: string) => {
  const i = argv.indexOf(name);
  return i >= 0 ? argv[i + 1] : undefined;
};
const OUT = flag("--out") ?? join(repoRoot, "bench", "s3-model", "results", `cost-matrix-${Date.now()}.jsonl`);

/** Kind of SDK endpoint of a cell (table in the header doc). */
type SdkKind = "bidi" | "rr" | "rr-ttl" | "fetch";
type Request_ = { path: string; body: unknown };
type Cell = {
  mode: Mode;
  /** Nominal steps per invocation (the real steps are read from `/metering`). */
  steps: number;
  kb: number;
  name: string;
  sdk: SdkKind;
  inv: number;
  conc: number;
  /** The `i`-th call of the closed loop. */
  request: (i: number) => Request_;
  /** Use-case name (`a1-chat`…); absent for grid cells. */
  usecase?: string;
};
const GRID: Cell[] = MODES.flatMap((mode) =>
  STEPS.flatMap((steps) =>
    KBS.map((kb) => ({
      mode,
      steps,
      kb,
      name: `${short(mode)}-${steps}x${kb}${suffix}`,
      sdk: mode === "BIDI_STREAM" ? "bidi" : "rr",
      inv: INV,
      conc: CONC,
      request: () => ({ path: "/invoke/Matrix/run", body: { steps, kb } }),
    })),
  ),
);

/** Parameters of the use-case handlers (`flow`, `chat`…): the result size per step is uniform in `[minKb, maxKb]`. */
type Steps = { steps: number; minKb: number; maxKb: number; sleepAt?: number; sleepMs?: number };

/**
 * Use-case cells. `steps`/`kb` are nominal (`ctx.run` steps ⇒ mean size), only
 * written to the row.
 */
type UseCase = Omit<Cell, "inv" | "conc" | "mode"> & { inv?: number };
const USECASES: UseCase[] = [
  // A1 — chat agent: one conversation turn = 20 LLM calls of 2–8 KiB, each result
  // `ctx.set` as a new key of the conversation virtual object (8 conversations,
  // 4 turns each ⇒ 80 messages at the end of the cell); the returned result carries no history.
  ...(["rr", "bidi"] as const).map((sdk) => ({
    name: `a1-chat-${sdk}`,
    usecase: "a1-chat",
    sdk,
    steps: 20,
    kb: 5,
    request: (i: number) => ({ path: `/invoke/Chat/turn?key=c${i % 8}`, body: { steps: 20, minKb: 2, maxKb: 8 } }),
  })),
  // A2 — tool-using agent: 100 steps, tool results of 5–50 KiB, every 20th step a
  // 200 KiB document through `ctx.artifact.put` (the journal keeps only the ref).
  ...(["rr", "bidi"] as const).map((sdk) => ({
    name: `a2-tools-${sdk}`,
    usecase: "a2-tools",
    sdk,
    steps: 100,
    kb: 27.5,
    request: () => ({
      path: "/invoke/Tools/run",
      body: { steps: 100, minKb: 5, maxKb: 50, docEvery: 20, docKb: 200 },
    }),
  })),
  // A3 — waiting for a human approval: 5 steps, `ctx.sleep(4 s)` (standing in for a
  // long wait / signal), 5 steps. `rr-ttl`: the SDK cache expires during the wait ⇒ the turn after the wait is a full replay.
  ...(["rr-ttl", "rr", "bidi"] as const).map((sdk) => ({
    name: `a3-approval-${sdk === "rr-ttl" ? "rr-expired" : sdk}`,
    usecase: "a3-approval",
    sdk,
    steps: 10,
    kb: 5,
    request: () => ({ path: "/invoke/Approval/run", body: { before: 5, after: 5, minKb: 2, maxKb: 8, waitMs: 4000 } }),
  })),
  // W1 — webhook: 4 steps × 1 KiB.
  {
    name: "w1-webhook-fetch",
    usecase: "w1-webhook",
    sdk: "fetch",
    steps: 4,
    kb: 1,
    request: () => ({ path: "/invoke/Flow/run", body: { steps: 4, minKb: 1, maxKb: 1 } satisfies Steps }),
  },
  // W2 — order saga: 7 steps × 2 KiB, `ctx.sleep(500 ms)` before step 4.
  {
    name: "w2-saga-fetch",
    usecase: "w2-saga",
    sdk: "fetch",
    steps: 7,
    kb: 2,
    request: () => ({
      path: "/invoke/Flow/run",
      body: { steps: 7, minKb: 2, maxKb: 2, sleepAt: 3, sleepMs: 500 } satisfies Steps,
    }),
  },
  // W3 — sync job: 30 pages × 10–20 KiB; an `rr` variant for comparison (serverless with affinity + cache).
  ...(["fetch", "rr"] as const).map((sdk) => ({
    name: `w3-sync-${sdk}`,
    usecase: "w3-sync",
    sdk,
    steps: 30,
    kb: 15,
    request: () => ({ path: "/invoke/Flow/run", body: { steps: 30, minKb: 10, maxKb: 20 } satisfies Steps }),
  })),
  // W4 — fan-out/fan-in: the parent `ctx.call`s 100 children then `ctx.all`, each child 3 steps
  // × 1 KiB. 8 parents per cell (800 children).
  ...(["fetch", "rr"] as const).map((sdk) => ({
    name: `w4-fanout-${sdk}`,
    usecase: "w4-fanout",
    sdk,
    inv: 8,
    steps: 100,
    kb: 1,
    request: () => ({ path: "/invoke/Fan/parent", body: { children: 100, childSteps: 3, kb: 1 } }),
  })),
];
const ucSuffix = suffix.replace("-json", "");
const USECASE_CELLS: Cell[] = USECASES.map((u) => ({
  ...u,
  name: `${u.name}${ucSuffix}`,
  mode: u.sdk === "bidi" ? "BIDI_STREAM" : "REQUEST_RESPONSE",
  inv: u.inv ?? INV,
  conc: CONC,
}));
const wanted = env.CM_CELLS?.split(",");
const CELLS = wanted
  ? [...GRID, ...USECASE_CELLS].filter((c) => wanted.includes(c.name) || (c.usecase && wanted.includes("usecases")))
  : GRID;

// Local SeaweedFS when measuring on `s3://` without an endpoint set — like `s3-cost.ts`.
if (STORE.startsWith("s3://") && !env.AWS_ENDPOINT) {
  const cfg = JSON.parse(
    readFileSync(join(repoRoot, "scripts", "dev-store", "seaweed-s3.json"), "utf8"),
  ) as { identities: { credentials: { accessKey: string; secretKey: string }[] }[] };
  const cred = cfg.identities[0]!.credentials[0]!;
  Object.assign(env, {
    AWS_ENDPOINT: "http://localhost:18333",
    AWS_ACCESS_KEY_ID: cred.accessKey,
    AWS_SECRET_ACCESS_KEY: cred.secretKey,
    AWS_REGION: "us-east-1",
    AWS_ALLOW_HTTP: "true",
    AWS_VIRTUAL_HOSTED_STYLE_REQUEST: "false",
  });
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// ───────────────────────────────────────────── deployment

/** A string of exactly `kb` KiB, incompressible. */
const random = (kb: number) => randomBytes((kb * 1024 * 3) / 4).toString("base64");

const WORDS = (
  "the a to of and in is for that on with as it be this are by from at or an was we can will not " +
  "order invoice customer payment total amount status pending shipped delivered refund tax price " +
  "quantity product item cart user account email address city country tool call result search " +
  "document summary answer question reason step plan observation error retry token model"
).split(" ");

/** Agent-step-like JSON, ≈ `kb` KiB: a message array with repeated vocabulary, random numbers and ids. */
function json(kb: number): string {
  const out: unknown[] = [];
  let len = 2;
  while (len < kb * 1024) {
    const n = 8 + Math.floor(Math.random() * 40);
    const words = Array.from({ length: n }, () => WORDS[Math.floor(Math.random() * WORDS.length)]);
    const m = {
      id: randomBytes(8).toString("hex"),
      role: Math.random() < 0.5 ? "assistant" : "tool",
      content: words.join(" "),
      tokens: n * 3 + Math.floor(Math.random() * 50),
      score: Math.round(Math.random() * 1e6) / 1e6,
    };
    const t = JSON.stringify(m);
    out.push(m);
    len += t.length + 1;
  }
  return JSON.stringify(out);
}

const blob = (kb: number) => (PAYLOAD === "json" ? json(kb) : random(kb));

const matrix = service("Matrix", {
  async run(ctx: ServiceContext, input: { steps: number; kb: number }) {
    let total = 0;
    for (let i = 0; i < input.steps; i++) {
      const v = await ctx.run(`s${i}`, () => blob(input.kb));
      total += v.length;
    }
    return { total };
  },
});

// ───────────────────────────────────────────── use-case handlers

/** A uniform size in `[min, max]` KiB — only call inside a `ctx.run` closure (replay does not rerun it). */
const between = (min: number, max: number) => min + Math.random() * (max - min);

/** W1–W3: `steps` times `ctx.run`, one `ctx.sleep(sleepMs)` before step `sleepAt` if set. */
const flow = service("Flow", {
  async run(ctx: ServiceContext, input: Steps) {
    let total = 0;
    for (let i = 0; i < input.steps; i++) {
      if (i === input.sleepAt) await ctx.sleep(input.sleepMs ?? 0, "wait");
      const v = await ctx.run(`s${i}`, () => json(between(input.minKb, input.maxKb)));
      total += v.length;
    }
    return { total };
  },
});

/**
 * A1: one conversation turn. The history lives in the object's state (one key per
 * message `m<n>`, counted in `count` — the name sorts before `m…` so it is always in the eager state).
 */
const chat = service(
  "Chat",
  {
    async turn(ctx: Context, input: Steps) {
      const seen = (await ctx.get<number>("count")) ?? 0;
      for (let i = 0; i < input.steps; i++) {
        const m = await ctx.run(`llm${i}`, () => json(between(input.minKb, input.maxKb)));
        ctx.set(`m${seen + i}`, m);
      }
      ctx.set("count", seen + input.steps);
      return { messages: seen + input.steps };
    },
  },
  { type: "VIRTUAL_OBJECT", handlerTypes: { turn: "EXCLUSIVE" } },
);

/** A2: `steps` tool steps; every `docEvery`-th step a `docKb` KiB document through an artifact (the journal keeps the ref). */
const tools = service("Tools", {
  async run(
    ctx: ServiceContext,
    input: { steps: number; minKb: number; maxKb: number; docEvery: number; docKb: number },
  ) {
    let docs = 0;
    let total = 0;
    for (let i = 0; i < input.steps; i++) {
      if (input.docEvery > 0 && (i + 1) % input.docEvery === 0) {
        await ctx.run(`doc${i}`, () => ctx.artifact.put(json(input.docKb), { contentType: "application/json" }));
        docs++;
      } else {
        const v = await ctx.run(`tool${i}`, () => json(between(input.minKb, input.maxKb)));
        total += v.length;
      }
    }
    return { docs, total };
  },
});

/** A3: `before` steps, wait for approval (`ctx.sleep(waitMs)`), `after` steps. */
const approval = service("Approval", {
  async run(ctx: ServiceContext, input: { before: number; after: number; minKb: number; maxKb: number; waitMs: number }) {
    for (let i = 0; i < input.before; i++) await ctx.run(`a${i}`, () => json(between(input.minKb, input.maxKb)));
    await ctx.sleep(input.waitMs, "approval");
    for (let i = 0; i < input.after; i++) await ctx.run(`b${i}`, () => json(between(input.minKb, input.maxKb)));
    return { ok: true };
  },
});

/** W4: the parent calls `children` children and waits for all; each child `childSteps` steps × `kb` KiB. */
const fan = service("Fan", {
  async parent(ctx: ServiceContext, input: { children: number; childSteps: number; kb: number }) {
    const calls = Array.from({ length: input.children }, (_, i) =>
      ctx.call<number>("Fan", "child", { steps: input.childSteps, kb: input.kb, i }),
    );
    const done = await ctx.all(calls);
    return { children: done.length };
  },
  async child(ctx: ServiceContext, input: { steps: number; kb: number; i: number }) {
    for (let s = 0; s < input.steps; s++) await ctx.run(`c${s}`, () => json(input.kb));
    return input.i;
  },
});

const SERVICES = [matrix, flow, chat, tools, approval, fan];

// ───────────────────────────────────────────── cluster

type Node = { proc: ChildProcess; url: string };

/** The cells' identity master: every node derives the same keys, so one `kid` serves every cell. */
const IDENTITY_MASTER = "cost-matrix-identity-master-longer-than-32-bytes";

/** The runtime's active `kid` (`GET /identity` of a throwaway node), set in `main()` before the SDKs start. */
let identityKeys: string[] = [];

async function runtimeKids(): Promise<string[]> {
  const node = await spawnNode("memory://", 99);
  try {
    const id = (await (await fetch(`${node.url}/identity`)).json()) as { keys: { kid: string; active: boolean }[] };
    return id.keys.filter((k) => k.active).map((k) => k.kid);
  } finally {
    await stop([node]);
  }
}

async function spawnNode(store: string, i: number): Promise<Node> {
  const proc = spawn(
    binary,
    [
      "--no-auth",
      "--main-store", store,
      "--cluster",
      "--partitions", String(PARTITIONS),
      "--port", "0",
      "--node-id", `cm-${i}`,
      ...NODE_ARGS,
    ],
    {
      stdio: "pipe",
      env: {
        ...env,
        CAIRNFLOW_STORE_PROFILE: profile,
        // `/invoke` waits until done: the closed loop does not turn into open load.
        CAIRNFLOW_INVOKE_WAIT_MS: "300000",
        // The nodes sign requests: the SDKs serve the replay cache and wire v6 only with `identityKeys`.
        CAIRNFLOW_IDENTITY_MASTER: IDENTITY_MASTER,
      },
    },
  );
  const url = await new Promise<string>((resolve, reject) => {
    let buf = "";
    const t = setTimeout(() => reject(new Error(`node ${i} did not report its port:\n${buf.slice(-2000)}`)), 60_000);
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
    proc.on("exit", (code) => reject(new Error(`node ${i} exited early (${code}):\n${buf.slice(-2000)}`)));
  });
  return { proc, url };
}

async function stop(nodes: Node[]) {
  for (const n of nodes) if (n.proc.exitCode === null) n.proc.kill("SIGKILL");
  await sleep(200);
}

/** Leader-following fetch: a `503` with `leader` switches node, otherwise back off. */
async function cfetch(nodes: Node[], path: string, init: RequestInit = {}, timeoutMs = 300_000): Promise<Response> {
  const deadline = Date.now() + timeoutMs;
  let base = nodes[Math.floor(Math.random() * nodes.length)]!.url;
  for (;;) {
    try {
      const r = await fetch(base + path, init);
      // A `404` from `/invoke`: another node has not yet learned the just-registered
      // deployment (≤ one registry refresh tick) — like `s3-cost.ts`.
      if ((r.status === 503 || (r.status === 404 && path.startsWith("/invoke/"))) && Date.now() < deadline) {
        const peek = (await r.clone().json().catch(() => undefined)) as { leader?: string } | undefined;
        await r.arrayBuffer().catch(() => {});
        if (peek?.leader) base = peek.leader.replace(/\/$/, "");
        else await sleep(100);
        continue;
      }
      return r;
    } catch (e) {
      if (Date.now() > deadline) throw e;
      await sleep(200);
    }
  }
}

// ───────────────────────────────────────────── counters

type Counts = Record<string, number>;

/** Sums `cairnflow_testkit_store_*`, `cairnflow_sdk_*` and the commit waits of every node. */
async function scrape(nodes: Node[]): Promise<Counts> {
  const out: Counts = {};
  for (const n of nodes) {
    const text = await fetch(`${n.url}/metrics`).then((r) => r.text());
    for (const line of text.split("\n")) {
      if (line.startsWith("#")) continue;
      const sp = line.lastIndexOf(" ");
      let key = line.slice(0, sp);
      if (key.startsWith("cairnflow_testkit_store_")) key = key.replace(/tier="[^"]*",?/, "").replace("{}", "");
      else if (
        !key.startsWith("cairnflow_sdk_bytes_total") &&
        !/^cairnflow_sdk_(rr_turns|bidi_streams|bidi_resumes)_total$/.test(key) &&
        key !== "cairnflow_commit_wait_seconds_count" &&
        !key.startsWith("cairnflow_rr_replay_") &&
        !key.startsWith("cairnflow_read_fence_total")
      )
        continue;
      out[key] = (out[key] ?? 0) + Number(line.slice(sp + 1));
    }
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

const hourOf = (ms: number) => new Date(ms).toISOString().slice(0, 13);

async function metering(nodes: Node[], from: number, to: number) {
  const hours = new Set<string>();
  for (let t = from - 3_600_000; t <= to + 3_600_000; t += 1_800_000) hours.add(hourOf(t));
  let steps = 0;
  let invocations = 0;
  let units = 0;
  for (const hour of hours) {
    const r = await cfetch(nodes, `/metering?tenant=default&hour=${hour}`);
    if (!r.ok) throw new Error(`/metering ${hour}: ${r.status} ${await r.text()}`);
    const m = (await r.json()) as { steps: number; invocations: number; units?: number };
    steps += m.steps;
    invocations += m.invocations;
    units += m.units ?? 0;
  }
  return { steps, invocations, units };
}

// ───────────────────────────────────────────── one cell

async function invokeLoop(nodes: Node[], n: number, conc: number, request: (i: number) => Request_) {
  let next = 0;
  let ok = 0;
  const bad: Record<string, number> = {};
  await Promise.all(
    Array.from({ length: conc }, async () => {
      while (next < n) {
        const { path, body } = request(next++);
        const r = await cfetch(nodes, path, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(body),
        }).catch(() => undefined);
        if (r?.status === 200) ok++;
        else {
          const k = `bad_${r?.status ?? "err"}`;
          bad[k] = (bad[k] ?? 0) + 1;
          if (r && Object.keys(bad).length === 1 && bad[k] === 1) console.error(`   ${k}: ${await r.clone().text()}`);
        }
        await r?.arrayBuffer().catch(() => {});
      }
    }),
  );
  return { ok, ...bad };
}

async function measure(cell: Cell, sdk: Endpoint): Promise<void> {
  const store = STORE.startsWith("memory") ? STORE : `${STORE.replace(/\/$/, "")}/${cell.name}`;
  const nodes: Node[] = [];
  try {
    for (let i = 0; i < NODES; i++) nodes.push(await spawnNode(store, i));
    const reg = await cfetch(nodes, "/deployments", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ uri: sdk.url }),
    });
    if (!reg.ok) throw new Error(`${cell.name}: register ${reg.status} ${await reg.text()}`);
    // Warm-up: every range has a leader, the Db is open, the SDK connection exists.
    await invokeLoop(nodes, 8, 4, () => ({ path: "/invoke/Matrix/run", body: { steps: 1, kb: 1 } }));
    await sleep(1000);

    // Idle background: the wait for metering to flush the warm-up calls (≥ 60 s) — the
    // steps read after it are the baseline to subtract, so the cell's steps are exactly the window's share.
    const i0 = await scrape(nodes);
    const it = Date.now();
    await sleep(Math.max(IDLE_S, 70) * 1000);
    const c0 = await scrape(nodes);
    const idle = { seconds: (Date.now() - it) / 1000, counts: diff(i0, c0) };
    const m0 = await metering(nodes, it, it);

    const t0 = Date.now();
    const outcome = await invokeLoop(nodes, cell.inv, cell.conc, cell.request);
    const loadS = (Date.now() - t0) / 1000;
    await sleep(TAIL_S * 1000);
    const c1 = await scrape(nodes);
    const t1 = Date.now();

    // One metering flush tick (60 s) before reading the steps.
    await sleep(70_000);
    const m1 = await metering(nodes, it, t1);
    const row = {
      scenario: cell.name,
      mode: cell.mode,
      steps_per_inv: cell.steps,
      kb: cell.kb,
      payload: cell.usecase ? "json" : PAYLOAD,
      compression: COMPRESSION,
      delta: DELTA,
      replicas: REPLICAS,
      affinity: AFFINITY,
      ...(NODE_ARGS.length > 0 ? { node_args: NODE_ARGS } : {}),
      nodes: NODES,
      partitions: PARTITIONS,
      seconds: (t1 - t0) / 1000,
      load_seconds: loadS,
      steps: m1.steps - m0.steps,
      invocations: m1.invocations - m0.invocations,
      units: m1.units - m0.units,
      ...(cell.usecase ? { usecase: cell.usecase, roots: outcome.ok } : {}),
      sdk: cell.sdk,
      tenants: 1,
      conc: cell.conc,
      outcome,
      counts: diff(c0, c1),
      idle,
      storage: [],
      store: STORE.startsWith("memory") ? STORE : store,
      binary,
      profile,
      at: new Date(t0).toISOString(),
    };
    mkdirSync(dirname(OUT), { recursive: true });
    appendFileSync(OUT, JSON.stringify(row) + "\n");
    const req = Object.entries(row.counts)
      .filter(([k]) => k.startsWith("cairnflow_testkit_store_requests_total"))
      .reduce((a, [, v]) => a + v, 0);
    const toSdk = row.counts[`cairnflow_sdk_bytes_total{mode="${cell.mode}",dir="to_sdk"}`] ?? 0;
    console.log(
      `${cell.name}: ${row.seconds.toFixed(0)}s · ${row.steps} steps · ${row.invocations} invocation · ` +
        `${req} request · ${(toSdk / 1e6).toFixed(1)} MB to the SDK · ${JSON.stringify(outcome)}`,
    );
  } finally {
    await stop(nodes);
  }
}

/** One RR SDK replica; `hosts` = the public host when a proxy in front rewrites `Host`. */
const rrReplica = (hosts?: string[]) =>
  serve({
    services: SERVICES,
    port: 0,
    protocolMode: "REQUEST_RESPONSE",
    compression: COMPRESSION,
    replayCache: DELTA,
    identityKeys,
    ...(hosts === undefined ? {} : { identityHosts: hosts }),
  });

/**
 * The real `fetchHandler()` (cache off — its default, as on Lambda/Workers/Vercel)
 * behind an in-process `node:http` h1 server: builds a `Request` from the incoming
 * request, writes back the `Response` byte for byte (a compressed body keeps `content-encoding`).
 */
async function fetchEndpoint(): Promise<Endpoint> {
  const handler = fetchHandler({ services: SERVICES, compression: COMPRESSION });
  const server = createServer((req, res) => {
    void (async () => {
      const chunks: Buffer[] = [];
      for await (const c of req) chunks.push(c as Buffer);
      const headers = new Headers();
      for (const [k, v] of Object.entries(req.headers)) {
        if (typeof v === "string" && !["host", "connection", "content-length", "transfer-encoding"].includes(k)) {
          headers.set(k, v);
        }
      }
      const method = req.method ?? "GET";
      const r = await handler(
        new Request(`http://127.0.0.1${req.url ?? "/"}`, {
          method,
          headers,
          ...(method === "GET" || method === "HEAD" ? {} : { body: Buffer.concat(chunks) }),
        }),
      );
      const out: Record<string, string> = {};
      r.headers.forEach((v, k) => {
        if (k !== "content-length") out[k] = v;
      });
      const body = Buffer.from(await r.arrayBuffer());
      res.writeHead(r.status, { ...out, "content-length": String(body.length) });
      res.end(body);
    })().catch((e: unknown) => {
      if (!res.headersSent) res.writeHead(502);
      res.end(String(e));
    });
  });
  await new Promise<void>((ok) => server.listen(0, "127.0.0.1", ok));
  const addr = server.address() as { port: number };
  return {
    port: addr.port,
    url: `http://127.0.0.1:${addr.port}`,
    close: async () => {
      server.closeAllConnections();
      server.close();
    },
  };
}

/** SDK endpoint by kind (table in the header doc). */
async function endpointOf(kind: SdkKind): Promise<Endpoint> {
  switch (kind) {
    case "bidi":
      return serve({ services: SERVICES, port: 0, protocolMode: "BIDI_STREAM", compression: COMPRESSION, identityKeys });
    case "rr":
      return rrEndpoint();
    case "rr-ttl":
      return serve({
        services: SERVICES,
        port: 0,
        protocolMode: "REQUEST_RESPONSE",
        compression: COMPRESSION,
        replayCache: { ttlMs: 1500 },
        identityKeys,
      });
    case "fetch":
      return fetchEndpoint();
  }
}

/**
 * `CM_REPLICAS` replicas behind an in-process h1 proxy (playing the customer
 * platform's load balancer): round-robin, or sticky by the `cm_lb` cookie.
 */
async function rrEndpoint(): Promise<Endpoint> {
  if (REPLICAS <= 1) return rrReplica();
  const { createServer } = await import("node:http");
  // Replicas are created once the proxy listens: the runtime signs the proxy's host.
  const replicas: Endpoint[] = [];
  let rr = 0;
  const proxy = createServer((req, res) => {
    void (async () => {
      const chunks: Buffer[] = [];
      for await (const c of req) chunks.push(c as Buffer);
      const cookie = String(req.headers.cookie ?? "");
      const m = /(?:^|;\s*)cm_lb=r(\d+)/.exec(cookie);
      const sticky = AFFINITY === "cookie" && m !== null ? Number(m[1]) % REPLICAS : undefined;
      const i = sticky ?? rr++ % REPLICAS;
      const headers: Record<string, string> = {};
      // `accept-encoding` is not forwarded: fetch decompresses the response itself, the proxy sends
      // the body back plain (only the SDK → runtime direction loses compression; the bytes to the SDK — what the cell measures — are unchanged).
      for (const [k, v] of Object.entries(req.headers)) {
        if (typeof v === "string" && k !== "host" && k !== "accept-encoding") headers[k] = v;
      }
      const method = req.method ?? "GET";
      const r = await fetch(replicas[i]!.url + (req.url ?? "/"), {
        method,
        headers,
        ...(method === "GET" ? {} : { body: Buffer.concat(chunks) }),
      });
      const out: Record<string, string> = {};
      r.headers.forEach((v, k) => {
        if (!["content-length", "transfer-encoding", "connection", "content-encoding"].includes(k)) out[k] = v;
      });
      if (AFFINITY === "cookie" && sticky === undefined) out["set-cookie"] = `cm_lb=r${i}; Path=/; HttpOnly`;
      const body = Buffer.from(await r.arrayBuffer());
      res.writeHead(r.status, { ...out, "content-length": String(body.length) });
      res.end(body);
    })().catch((e: unknown) => {
      if (!res.headersSent) res.writeHead(502);
      res.end(String(e));
    });
  });
  await new Promise<void>((ok) => proxy.listen(0, "127.0.0.1", ok));
  const addr = proxy.address() as { port: number };
  replicas.push(...(await Promise.all(Array.from({ length: REPLICAS }, () => rrReplica([`127.0.0.1:${addr.port}`])))));
  return {
    port: addr.port,
    url: `http://127.0.0.1:${addr.port}`,
    close: async () => {
      proxy.closeAllConnections();
      proxy.close();
      for (const r of replicas) await r.close();
    },
  } as Endpoint;
}

async function main() {
  if (!existsSync(binary)) {
    console.error(`missing ${binary} — cargo build --release --bin cairnflow --features testkit --target-dir target/tk`);
    process.exit(2);
  }
  console.log(`cost-matrix: ${binary}\n  store ${STORE}, ${NODES} node × ${PARTITIONS} ranges, ${INV} invocation, level ${CONC}\n  output ${OUT}`);
  identityKeys = await runtimeKids();
  const sdks = {} as Record<SdkKind, Endpoint>;
  for (const kind of new Set(CELLS.map((c) => c.sdk))) sdks[kind] = await endpointOf(kind);
  try {
    const queue = [...CELLS];
    await Promise.all(
      Array.from({ length: Math.max(1, PARALLEL) }, async () => {
        for (let c = queue.shift(); c; c = queue.shift()) {
          await measure(c, sdks[c.sdk]).catch((e) => console.error(`${c.name}: ${e}`));
        }
      }),
    );
  } finally {
    for (const s of Object.values(sdks)) await s.close?.();
  }
  console.log(`\ndone — ${OUT}`);
  process.exit(0);
}

await main();
