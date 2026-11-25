// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

/**
 * Load test — **one partition**, one node, one deployment process.
 *
 * Two questions, one run:
 *
 * 1. **How much can it do?** Throughput and latency percentiles for each shape of
 *    work — from the shortest path (`echo`) to the most expensive one (`sleep`,
 *    going through a timer). One partition is the **lower bound** of a cluster, not
 *    the upper bound: the partition is cairn's unit of serialization, so the number
 *    here multiplies (nearly) linearly with the number of partitions when keys spread evenly.
 *
 * 2. **Is it implemented correctly?** Load is a sieve: bugs that a sequential test
 *    never sees — lost updates on a hot key, side effects running twice, calls
 *    vanishing, holes in the journal — only show up when many turns run at once.
 *    Each scenario carries its own check, and the end of the run has four global checks.
 *
 * **Not an automated test** (no `.test.ts` suffix): it occupies the machine for a
 * few minutes, and the numbers it prints only mean something together with the
 * machine that printed them.
 *
 * ```sh
 * cargo build --release --bin cairnflow
 * node harness/bench/load.ts
 *
 * # Heavier, longer:
 * LOAD_CONCURRENCY=128 LOAD_DURATION_S=30 node harness/bench/load.ts
 *
 * # Only a few scenarios, and on the old transport:
 * LOAD_SCENARIOS=echo,run1,hot LOAD_SDK_MODE=REQUEST_RESPONSE \
 *   node harness/bench/load.ts
 *
 * # Cross-partition `ctx.call` in both directions (needs ≥ 2 partitions):
 * LOAD_ARGS="--main-store memory:// --cluster --partitions 4" \
 *   LOAD_SCENARIOS=xcall LOAD_SWEEP=0 node harness/bench/load.ts
 *
 * # Agent and workflow call shapes (tree, fan-out, agent, saga):
 * LOAD_ARGS="--main-store memory:// --cluster --partitions 4" \
 *   LOAD_SCENARIOS=tree,fanout,agent,saga LOAD_SWEEP=0 node harness/bench/load.ts
 * ```
 *
 * Scenarios with a `ctx.call` print an extra **cross-range** line read from the
 * `/metrics` of the measurement window: the number of calls and the
 * `placement=remote` share by lock level (`cairnflow_calls_total`), the number of
 * cross-range messages and broken messages (`cairnflow_interrange_messages_total`),
 * and the mean time of the drainer's two phases, `sync` and `send`
 * (`cairnflow_interrange_{sync,send}_seconds`).
 *
 * The default is **one partition** — the shape every scenario except `xcall` was
 * written to measure. `LOAD_ARGS` changes the runtime's shape; `xcall` is the only
 * scenario that needs it.
 *
 * Exits `1` if any check is red. See `harness/bench/README.md`.
 */

import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { Agent, request as httpRequest } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

// ───────────────────────────────────────────── knobs

const here = fileURLToPath(new URL(".", import.meta.url));
const repoRoot = join(here, "..", "..");

/** **Release**, not debug: a number measured on a debug build says nothing. */
const BINARY = process.env.LOAD_BINARY ?? join(repoRoot, "target", "release", "cairnflow");
/**
 * Number of calls in flight from the client side. This is the "load", in the closed-loop sense.
 *
 * The default is low to keep the run fast, **not** because the knee is there: with
 * group commit, the latency of one call stays nearly constant until the system
 * saturates, so throughput ≈ concurrency ÷ latency. To find the real ceiling,
 * push this up — see the sweep at the start of the run.
 */
const CONCURRENCY = Number(process.env.LOAD_CONCURRENCY ?? "8");
/** Each scenario runs this many seconds (not counting warm-up). */
const DURATION_S = Number(process.env.LOAD_DURATION_S ?? "20");
/**
 * Cap on the number of calls of the `send` scenario.
 *
 * `/send` accepts work **a hundred times** faster than the engine does it, so
 * firing by time would build a queue tens of minutes long and make the checks wait
 * for it. Capping by count is the only way to keep the test bounded.
 */
const SEND_COUNT = Number(process.env.LOAD_SEND_COUNT ?? "100");
/**
 * Sweep of `echo` throughput by concurrency, **before** the scenarios, on a clean
 * store. `0` disables it.
 *
 * It used to run at the end of the run and reported 2–5× less than a standalone
 * sweep (64 → 303/684 against ~1 460). Not a counting error: the number of calls
 * completed on the client side matched the server's
 * `cairnflow_invoke_completed_total` over the same window exactly. The system was
 * **really** slower: `call`/`send`/`xcall` leave thousands of deleted
 * outbox/inbound rows behind, and `drain_outbox`/`drain_inbound` scanned the index
 * from `seq` 0 on every drain turn, walking every tombstone until compaction
 * cleared them — every later `echo` call cost 4–8× the CPU. The sweep asks "does
 * adding waiters add work done", so it measures on a clean state; the residual
 * state is measured separately at the end (the "residual state" line).
 */
const SWEEP_LEVELS = (process.env.LOAD_SWEEP ?? "1,4,16,64")
  .split(",")
  .map(Number)
  .filter((x) => x > 0);
/** Skip this many initial seconds — SlateDB, Node's JIT and the TCP cache all need to warm up. */
const WARMUP_S = Number(process.env.LOAD_WARMUP_S ?? "3");
/**
 * Cap on the partition's concurrent turns.
 *
 * Defaults to the **highest** concurrency this run will use — the sweep's
 * included. Taking it from `CONCURRENCY` alone is a trap that has already sprung
 * once: the server keeps its cap for the whole run, so a sweep at a higher level
 * measures exactly that cap and reports a "plateau" that looks just like the
 * system's ceiling. Throughput levels off exactly at
 * `max_concurrent_turns / latency` and nothing in the table says so.
 *
 * Setting it lower *on purpose* measures the flow control itself — 429s go up, and
 * that is information, not a failure (`cairnflow_invoke_throttled_total`).
 */
const MAX_TURNS = Number(
  process.env.LOAD_MAX_TURNS ?? String(Math.max(CONCURRENCY, ...SWEEP_LEVELS)),
);
const QUEUE_WAIT_MS = Number(process.env.LOAD_QUEUE_WAIT_MS ?? "30000");
const INVOKE_WAIT_MS = Number(process.env.LOAD_INVOKE_WAIT_MS ?? "30000");
const NAP_MS = Number(process.env.LOAD_NAP_MS ?? "200");
/**
 * The server's WAL flush interval. Empty ⇒ `cairnflow`'s default.
 *
 * This is the knob with the most leverage in the whole file: the latency floor of
 * a commit, and an invocation costs a few commits. To remeasure its price, run this
 * test twice with `LOAD_FLUSH_MS=100` and `LOAD_FLUSH_MS=10`.
 */
const FLUSH_MS = process.env.LOAD_FLUSH_MS;
/**
 * The store to measure. Empty ⇒ a temporary directory (`file://`).
 *
 * **A number on `file://` is not cairn's number.** The project's thesis is object
 * storage, where a WAL flush is a PUT over the network rather than a local disk
 * write — and everything in this run is governed by the price of one flush. So
 * `file://` is an *upper bound*, and the number worth comparing with others has to
 * be measured here:
 *
 * ```sh
 * scripts/dev-store/up.sh                 # SeaweedFS on localhost:18333
 * AWS_ENDPOINT=http://localhost:18333 AWS_ACCESS_KEY_ID=dev-only-access-key \
 * AWS_SECRET_ACCESS_KEY=dev-only-not-a-secret AWS_REGION=us-east-1 AWS_ALLOW_HTTP=true \
 * AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false \
 * LOAD_STORE_URI=s3://walbench/load-$RANDOM node harness/bench/load.ts
 * ```
 */
const STORE_URI = process.env.LOAD_STORE_URI;
/**
 * Query-layer interference (`bench/query/README.md`): `N` loops of `POST /query`
 * in parallel **within the measurement window** of each scenario — alternating the
 * newest page and a full-table scan by class. Compare the p99 of the same scenario
 * with `LOAD_QUERY_LOOP=0`. Prints the number of statements run and the response codes.
 */
const QUERY_LOOP = Number(process.env.LOAD_QUERY_LOOP ?? "0");
const ONLY = (process.env.LOAD_SCENARIOS ?? "").split(",").map((s) => s.trim()).filter(Boolean);
/**
 * Extra arguments for the `cairnflow` binary, split on whitespace, appended
 * **after** the run's own arguments — the same rule as `CAIRNFLOW_E2E_ARGS` of the e2e suite.
 *
 * With `--main-store` or `--data-dir` in there, the run does not provide its own
 * store (`LOAD_STORE_URI` is ignored). With `--cluster`, the run waits for every
 * partition to have a leader before firing — a node that has not yet won its lease
 * answers 503, and the run would measure the start-up window instead of what it meant to measure.
 */
const EXTRA_ARGS = (process.env.LOAD_ARGS ?? "").split(/\s+/).filter(Boolean);
/**
 * The `xcall` scenario: no call completes for this many seconds while calls are
 * still in flight ⇒ **WEDGED**. The run reports the number of calls completed so
 * far and stops, instead of hanging until whoever runs it presses Ctrl-C.
 *
 * Must be longer than `LOAD_INVOKE_WAIT_MS` so as not to mistake "slow" for
 * "stuck" on a single call — but a slow call still returns 202 and gets polled,
 * and the other workers still get work done, so this clock measures the *whole partition* standing still.
 */
const STALL_S = Number(process.env.LOAD_STALL_S ?? "20");
/** Keys per side for `xcall`. The default is wide enough that `ping` rarely contends for a lock. */
const XCALL_KEYS = Number(process.env.LOAD_XCALL_KEYS ?? String(Math.max(8, CONCURRENCY)));
/**
 * `xcall` fires only **one** direction (A → B). Control: if one direction runs at
 * the same concurrency while two directions stall, the stall needs both partitions
 * sending to each other at once — the signature of a wait cycle, not of load.
 */
const XCALL_ONEWAY = process.env.LOAD_XCALL_ONEWAY === "1";
/** `tree`: depth and width of the unkeyed call tree. */
const TREE_DEPTH = Number(process.env.LOAD_TREE_DEPTH ?? "3");
const TREE_WIDTH = Number(process.env.LOAD_TREE_WIDTH ?? "10");
/**
 * Trees in flight at once. Separate, and low: a default tree is 1 111
 * invocations, so `LOAD_CONCURRENCY` trees at once means thousands of cross-range
 * calls waiting — the run would measure the drainer's queue rather than one tree.
 */
const TREE_CONCURRENCY = Number(process.env.LOAD_TREE_CONCURRENCY ?? "2");
/** `fanout`: children of one parent (a 1 000-child fan-out is the target size). */
const FANOUT = Number(process.env.LOAD_FANOUT ?? "100");
/** `fanout` parents in flight at once — same reason as `LOAD_TREE_CONCURRENCY`. */
const FANOUT_CONCURRENCY = Number(process.env.LOAD_FANOUT_CONCURRENCY ?? "2");
/**
 * `fanout`: deliberately spread children — `placement` (`placement: "spread"`), or
 * the earlier workaround `scope` / `idem` (one scope / one idempotency key per
 * child). Absent ⇒ unkeyed children, colocated.
 */
const FANOUT_SPREAD = process.env.LOAD_FANOUT_SPREAD || undefined;
/** `agent`: sleep time of the `ctx.run` simulating one model call. */
const STEP_MS = Number(process.env.LOAD_STEP_MS ?? "1000");
/**
 * Cap on waiting for an invocation that received `202` to come to rest. A large
 * tree or fan-out on a slow store taking over two minutes is normal today.
 */
const SETTLE_S = Number(process.env.LOAD_SETTLE_S ?? "120");

if (!existsSync(BINARY)) {
  console.error(
    `missing binary ${BINARY} — run \`cargo build --release --bin cairnflow\` first\n` +
      `(or point LOAD_BINARY somewhere else)`,
  );
  process.exit(2);
}

// ───────────────────────────────────────────── client HTTP

/**
 * `node:http`, not `fetch`.
 *
 * Node's `fetch` goes through undici, and undici decides the number of connections
 * per origin on its own. For a load generator, "on its own" means losing control of
 * exactly the variable being measured: if the pool is smaller than `CONCURRENCY`,
 * the measured number is the pool size, not the server's capacity. Here the pool is one line of declaration.
 */
const agent = new Agent({
  keepAlive: true,
  maxSockets: Math.max(CONCURRENCY * 2, 64),
  maxFreeSockets: Math.max(CONCURRENCY * 2, 64),
});

interface Res {
  status: number;
  body: string;
}

function http(
  method: string,
  url: string,
  body?: string,
  headers: Record<string, string> = {},
): Promise<Res> {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const req = httpRequest(
      {
        agent,
        method,
        hostname: u.hostname,
        port: u.port,
        path: u.pathname + u.search,
        headers: body === undefined ? headers : { ...headers, "content-length": Buffer.byteLength(body) },
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (c: Buffer) => chunks.push(c));
        res.on("end", () =>
          resolve({ status: res.statusCode ?? 0, body: Buffer.concat(chunks).toString() }),
        );
      },
    );
    req.on("error", reject);
    req.end(body);
  });
}

const post = (url: string, body?: string, h?: Record<string, string>) => http("POST", url, body, h);
const get = (url: string) => http("GET", url);

// ───────────────────────────────────────────── setting up

let runtime: ChildProcess | undefined;
let deployment: ChildProcess | undefined;
let dataDir = "";
let server = "";
let statsUrl = "";

/** Reads the port from the first log line of a child process. */
function firstMatch(proc: ChildProcess, re: RegExp, what: string): Promise<string> {
  return new Promise((resolve, reject) => {
    let buf = "";
    const t = setTimeout(() => reject(new Error(`${what} did not report its port:\n${buf}`)), 30_000);
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

async function setUp(): Promise<void> {
  dataDir = mkdtempSync(join(tmpdir(), "cairnflow-load-"));

  // No `--cluster` ⇒ **one partition**, by definition: `main.rs` rejects
  // `--partitions != 1` without `--cluster`. That is the shape being measured —
  // unless `LOAD_ARGS` says otherwise.
  const ownStore = !EXTRA_ARGS.some((a) => a === "--main-store" || a === "--data-dir");
  runtime = spawn(
    BINARY,
    [
      ...(!ownStore ? [] : STORE_URI ? ["--main-store", STORE_URI] : ["--data-dir", dataDir]),
      "--port", "0",
      "--node-id", "load-node",
      // The bench measures the commit path, not API keys.
      "--no-auth",
      "--max-concurrent-turns", String(MAX_TURNS),
      "--queue-wait-ms", String(QUEUE_WAIT_MS),
      // High, on purpose: we want `/invoke` to **wait until done** so the measured
      // latency is the real end-to-end latency, not "time until the 202" plus one
      // client polling round.
      "--invoke-wait-ms", String(INVOKE_WAIT_MS),
      ...(FLUSH_MS ? ["--flush-interval-ms", FLUSH_MS] : []),
      ...EXTRA_ARGS,
    ],
    { stdio: ["ignore", "pipe", "inherit"] },
  );
  server = await firstMatch(runtime, /http:\/\/[\d.]+:\d+/, "runtime");

  deployment = spawn(process.execPath, [join(here, "deployment.ts")], {
    stdio: ["ignore", "pipe", "inherit"],
    env: { ...process.env, LOAD_NAP_MS: String(NAP_MS) },
  });
  const line = await firstMatch(deployment, /\{"sdk".*\}/, "deployment");
  const urls = JSON.parse(line) as { sdk: string; stats: string };
  statsUrl = urls.stats;

  for (let i = 0; ; i++) {
    const r = await get(`${server}/health`).catch(() => undefined);
    if (r?.status === 200) break;
    if (i > 100) throw new Error("runtime did not come up");
    await sleep(100);
  }

  // `/health` only says "HTTP is up", not which partitions have a leader.
  if (EXTRA_ARGS.includes("--cluster")) {
    const deadline = Date.now() + 30_000;
    for (;;) {
      const r = JSON.parse((await get(`${server}/cluster`)).body) as {
        partitions: { leader?: unknown }[];
      };
      if (r.partitions.every((p) => p.leader)) break;
      if (Date.now() > deadline) throw new Error(`partition has no leader: ${JSON.stringify(r)}`);
      await sleep(100);
    }
  }

  const reg = await post(
    `${server}/deployments`,
    JSON.stringify({ uri: urls.sdk }),
    { "content-type": "application/json" },
  );
  if (reg.status !== 200) throw new Error(`deployment registration failed: ${reg.status} ${reg.body}`);
}

function tearDown(): void {
  runtime?.kill("SIGKILL");
  deployment?.kill("SIGTERM");
  if (dataDir) rmSync(dataDir, { recursive: true, force: true });
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// ───────────────────────────────────────────── one call

interface Shot {
  ok: boolean;
  /** End-to-end latency, ms — from sending until the invocation comes to rest. */
  ms: number;
  id?: string | undefined;
  /** The parsed body of the finished call, if any. */
  out?: Record<string, unknown> | undefined;
  err?: string | undefined;
  /** Got a `429` (and had to wait for the rescheduled run) — not a failure, but worth counting. */
  throttled?: boolean;
  /** Which worker fired this one. Filled in by `drive`, not `fire`. */
  worker?: number | undefined;
}

/** `/invocations/{id}` until it comes to rest. */
async function settle(id: string, timeoutMs = SETTLE_S * 1000): Promise<Record<string, unknown>> {
  const deadline = Date.now() + timeoutMs;
  for (let every = 5; ; every = Math.min(every * 2, 100)) {
    const r = await get(`${server}/invocations/${id}`);
    if (r.status === 200) {
      const j = JSON.parse(r.body) as Record<string, unknown>;
      if (j.status !== "running" && j.status !== "suspended") return j;
    }
    if (Date.now() > deadline) throw new Error(`invocation ${id} did not come to rest within ${timeoutMs}ms`);
    await sleep(every);
  }
}

/**
 * Sends one call and **waits for it to come to rest**, then inspects the result.
 *
 * `429` is not a failure: ingress says explicitly that the invocation is durable
 * and has been rescheduled (see `InvokeResponse::Throttled`). Dropping it would
 * break the count — every `429` is still an invocation that will run, and the
 * correctness checks below rely on exactly that number.
 */
async function shoot(
  path: string,
  input: unknown,
  check?: (out: Record<string, unknown>) => string | null,
  headers?: Record<string, string>,
): Promise<Shot> {
  const t0 = performance.now();
  try {
    const r = await post(`${server}${path}`, JSON.stringify(input), headers);
    let j = JSON.parse(r.body) as Record<string, unknown>;
    const throttled = r.status === 429;
    if (r.status === 202 || throttled) {
      j = await settle(String(j.id));
    }
    const ms = performance.now() - t0;
    const id = j.id === undefined ? undefined : String(j.id);
    if (j.status !== "completed") {
      return { ok: false, ms, id, err: `status=${j.status} ${r.body.slice(0, 160)}` };
    }
    const out = JSON.parse(String(j.output ?? "null")) as Record<string, unknown>;
    const bad = check?.(out);
    if (bad) return { ok: false, ms, id, out, err: bad };
    return { ok: true, ms, id, out, ...(throttled ? { throttled } : {}) };
  } catch (e) {
    return { ok: false, ms: performance.now() - t0, err: String(e) };
  }
}

/** Checks the content: the output must belong to **this very** call, nobody else's. */
const echoes = (tag: string, n: number) => (out: Record<string, unknown>) =>
  out.tag === tag && out.n === n ? null : `misrouted output: expected ${tag}/${n}, got ${JSON.stringify(out)}`;

// ───────────────────────────────────────────── scenarios

interface Run {
  shots: Shot[];
  elapsedS: number;
}

interface Scenario {
  name: string;
  desc: string;
  /** The tag branch to query the deployment's counters with. */
  prefix: string;
  fire(n: number): Promise<Shot>;
  /** Number of times `ctx.run` must run per invocation. `0` = no run at all. */
  runsPerInvocation: number;
  /** Cap on the number of calls, if this scenario cannot be bounded by time. */
  maxShots?: number;
  /**
   * Called **after** the warm-up phase, before the measurement phase.
   *
   * A scenario that leaves durable traces (an object's state) must reset the table
   * here — otherwise the warm-up leaks into the check and a green test reports red
   * for exactly the wrong reason.
   */
  reset?(): void;
  /** The scenario's own check. Returns a list of failures (empty = green). */
  verify?(run: Run): Promise<string[]>;
  /**
   * Called **before** warm-up. Returns the reason to skip the scenario (it cannot run
   * in this shape), or `null` to run.
   */
  prepare?(): Promise<string | null>;
  /**
   * Watchdog for a partition standing still (`LOAD_STALL_S`). Scenarios **written to
   * find stalls** turn it on — a measurement that hangs forever reports nothing.
   */
  watch?: boolean;
  /** Calls in flight for this scenario; absent ⇒ `LOAD_CONCURRENCY`. */
  concurrency?: number;
}

/**
 * The hot key of the `hot` scenario.
 *
 * Changes between warm-up and measurement: a virtual object's state is **durable**,
 * so reusing the old key would carry the warm-up's count into the check.
 */
let hotKey = "hot-warmup";
let hotEpoch = 0;

/** Number of partitions of the runtime under measurement, read from `/metrics` after setup. */
let partitions = 1;

/**
 * The partition of an invocation: the first 8 bytes of the id are the **partition
 * key** (big-endian), and the partition is the range holding it in the default
 * equal-split table (`PartitionTable::equal`: range `i` is
 * `[i·2⁶⁴/N, (i+1)·2⁶⁴/N − 1]`). True as long as the cluster uses the default
 * table — i.e. every cluster today.
 */
function partitionOfId(id: string): number {
  const key = BigInt("0x" + id.slice(0, 16));
  const space = 1n << 64n;
  const n = BigInt(partitions);
  for (let i = 0n; i < n; i++) {
    if (key <= ((i + 1n) * space) / n - 1n) return Number(i);
  }
  return partitions - 1;
}

/**
 * `count` keys of `service` that land on `partition`.
 *
 * **Asks** the runtime rather than rehashing on the client side: the key →
 * partition hash is an implementation detail of `cluster.rs` (and may change),
 * while an empty `where` call is right for any hash function. Costs a few dozen
 * calls before the warm-up phase, not counted in the measurement.
 */
async function keysOn(service: string, partition: number, count: number): Promise<string[]> {
  const found: string[] = [];
  const BATCH = 16;
  for (let i = 0; found.length < count; i += BATCH) {
    if (i > count * partitions * 20) {
      throw new Error(`asked ${i} keys but only ${found.length}/${count} land on partition ${partition}`);
    }
    const batch = Array.from({ length: BATCH }, (_unused, j) => `${service.toLowerCase()}-${i + j}`);
    const where = await Promise.all(
      batch.map(async (k) => {
        const r = await post(`${server}/invoke/${service}/where?key=${k}`, "{}");
        if (r.status !== 200) throw new Error(`${service}/where?key=${k}: ${r.status} ${r.body}`);
        return partitionOfId(String((JSON.parse(r.body) as { id: unknown }).id));
      }),
    );
    batch.forEach((k, j) => {
      if (where[j] === partition && found.length < count) found.push(k);
    });
  }
  return found;
}

/**
 * The two partitions of `xcall`, and the keys of each side — filled in by `prepare`.
 *
 * The keys receiving `ping` and the keys receiving `pong` are two **disjoint** sets.
 * `pong` is `SHARED` and so does not contend for the lock with `ping` anyway
 * (SHARED handlers enter a queue not keyed by key — `turn/mod.rs`, `queue_name`),
 * but with disjoint key sets the conclusion "the stall is the engine's" does not rest on that.
 */
const XCALL_PA = 0;
const XCALL_PB = 1;
const xcallKeys = { aPing: [] as string[], aPong: [] as string[], bPing: [] as string[], bPong: [] as string[] };

const SCENARIOS: Scenario[] = [
  {
    name: "echo",
    desc: "empty handler — the floor of every measurement (ingress + engine + one SDK turn)",
    prefix: "echo/",
    runsPerInvocation: 0,
    fire: (n) => {
      const tag = `echo/${n}`;
      return shoot("/invoke/LoadService/echo", { tag, n }, echoes(tag, n));
    },
  },
  {
    name: "run1",
    desc: "one ctx.run — the gap to echo is exactly the price of one durable journal entry",
    prefix: "run1/",
    runsPerInvocation: 1,
    fire: (n) => {
      const tag = `run1/${n}`;
      return shoot("/invoke/LoadService/run1", { tag, n }, echoes(tag, n));
    },
  },
  {
    name: "run5",
    desc: "five sequential ctx.run — cost by journal length, not by number of calls",
    prefix: "run5/",
    runsPerInvocation: 5,
    fire: (n) => {
      const tag = `run5/${n}`;
      return shoot("/invoke/LoadService/run5", { tag, n }, echoes(tag, n));
    },
  },
  {
    name: "state",
    desc: "virtual object, one key per call — the state write path, no lock contention",
    prefix: "state/",
    runsPerInvocation: 0,
    fire: (n) => {
      const tag = `state/${n}`;
      return shoot(
        `/invoke/LoadObject/touch?key=k-${n}`,
        { tag, n },
        (out) => (out.count === 1 ? echoes(tag, n)(out) : `new key but count=${out.count}`),
      );
    },
  },
  {
    name: "hot",
    desc: "EVERY call on ONE key — the exclusive lock must serialize them",
    prefix: "hot/",
    runsPerInvocation: 0,
    fire: (n) => {
      const tag = `hot/${n}`;
      return shoot(`/invoke/LoadObject/touch?key=${hotKey}`, { tag, n }, echoes(tag, n));
    },
    reset: () => {
      hotKey = `hot-run-${++hotEpoch}`;
    },
    // This is the most valuable check of the whole run.
    //
    // `touch` reads `count` then writes `count+1`. If two turns on the same key
    // overlapped, both would read the same number and write the same number — the
    // classic lost update. Two things must hold at once, and each catches a different failure:
    //
    //  * every returned `count` must be **pairwise distinct** ⇒ no two turns read
    //    the same value;
    //  * the final `count` must **equal** the number of calls ⇒ no turn lost,
    //    and no extra turn either.
    async verify(run) {
      const errs: string[] = [];
      const seen = new Set<number>();
      let dup = 0;
      for (const s of run.shots) {
        if (!s.ok) continue;
        const c = Number(s.out?.count);
        if (seen.has(c)) dup++;
        seen.add(c);
      }
      if (dup > 0) errs.push(`LOST UPDATE: ${dup} calls read a duplicate count value`);
      const r = await post(`${server}/invoke/LoadObject/read?key=${hotKey}`, "{}");
      const final = Number(JSON.parse(JSON.parse(r.body).output).count);
      const acked = run.shots.filter((s) => s.ok).length;
      if (final !== acked) {
        errs.push(`final count ${final} ≠ ${acked} successful calls`);
      }

      // ── FIFO order, under load.
      //
      // This is the only ordering guarantee cairn gives: *same object key,
      // `EXCLUSIVE` handler ⇒ runs exactly in the order received*. The wait queue
      // inserts by `seq` (assigned when the invocation is created), not by arrival
      // order, so it must hold even when a turn is deferred and comes back after newcomers.
      //
      // A worker sends **sequentially** — it fires only after its previous call
      // finished — so its own sequence has a known order, and the `count` the server
      // returns is exactly the *execution position*. So one worker's counts must
      // increase monotonically. Other workers interleaving is fine; interleaving that
      // reorders **one** worker's calls is not.
      //
      // **This check pins the contract, not the mechanism** — written down so nobody
      // tries again: change the wait queue from `insert` by seq to `push_back` and
      // run this scenario at 24 workers, and it stays green. The reason: the gap
      // between assigning `seq` and calling `acquire` is too short and too even, so
      // arrival order always matches seq order.
      //
      // The mechanism is pinned one layer down, where an arrival order opposite to
      // seq order can be built: `flow::tests::queue_orders_by_seq_not_arrival_order`.
      const lastOf = new Map<number, { count: number; n: number }>();
      let inversions = 0;
      const examples: string[] = [];
      for (const s of run.shots) {
        if (!s.ok || s.worker === undefined) continue;
        const count = Number(s.out?.count);
        const n = Number(s.out?.n);
        const prev = lastOf.get(s.worker);
        if (prev && count < prev.count) {
          inversions++;
          if (examples.length < 5) {
            examples.push(`worker ${s.worker}: #${prev.n}→position ${prev.count}, then #${n}→${count}`);
          }
        }
        lastOf.set(s.worker, { count, n });
      }
      if (inversions > 0) {
        errs.push(`WRONG ORDER: ${inversions} inversions on the same key (${examples.join(" · ")})`);
      }
      return errs;
    },
  },
  {
    name: "call",
    desc: "ctx.call — one invocation spawns another and waits for it",
    prefix: "call/",
    runsPerInvocation: 0,
    fire: (n) => {
      const tag = `call/${n}`;
      return shoot("/invoke/LoadService/caller", { tag, n }, echoes(tag, n));
    },
  },
  {
    name: "sleep",
    desc: `ctx.sleep(${NAP_MS}ms) — suspend, timer, wake: the longest path through the engine`,
    prefix: "sleep/",
    runsPerInvocation: 0,
    fire: (n) => {
      const tag = `sleep/${n}`;
      return shoot("/invoke/LoadService/nap", { tag, n }, echoes(tag, n));
    },
    // Latency must be **≥** the sleep time. Less means the timer fired early, and an
    // early timer is a broken promise — `ctx.sleep` is a contract, not a hint. The
    // excess above it is the engine's suspend-and-wake cost.
    async verify(run) {
      const early = run.shots.filter((s) => s.ok && s.ms < NAP_MS - 5).length;
      return early > 0 ? [`${early} calls woke BEFORE ${NAP_MS}ms — timer fired early`] : [];
    },
  },
  {
    name: "send",
    desc: `fire-and-forget × ${SEND_COUNT}: *accept* throughput, then wait for the queue to drain`,
    prefix: "send/",
    runsPerInvocation: 1,
    maxShots: SEND_COUNT,
    // `/send` answers as soon as the invocation is durable — without waiting for it
    // to run. So the latency on this line is "how long to accept a job", not "how
    // long to finish a job". Two numbers measuring two things, and the `p50` column will differ by an order of magnitude.
    fire: async (n) => {
      const tag = `send/${n}`;
      const t0 = performance.now();
      try {
        const r = await post(`${server}/invoke/LoadService/run1/send`, JSON.stringify({ tag, n }));
        const ms = performance.now() - t0;
        const j = JSON.parse(r.body) as Record<string, unknown>;
        if (r.status !== 200 && r.status !== 202 && r.status !== 429) {
          return { ok: false, ms, err: `${r.status} ${r.body.slice(0, 160)}` };
        }
        return { ok: true, ms, id: String(j.id) };
      } catch (e) {
        return { ok: false, ms: performance.now() - t0, err: String(e) };
      }
    },
    // Accepted work must get done — even when nobody is waiting for it anymore. That
    // is the promise of `/send`, and also where a broken outbox would show.
    async verify(run) {
      const want = run.shots.filter((s) => s.ok).length;
      const deadline = Date.now() + 180_000;
      for (;;) {
        const st = await stats("send/");
        if (st.runTags >= want) return [];
        if (Date.now() > deadline) {
          return [`only ${st.runTags}/${want} /send calls finished after 180s — some work was dropped`];
        }
        await sleep(250);
      }
    },
  },
  {
    // This scenario does **not** measure one partition: it needs ≥ 2, and it exists
    // to find stalls, not to produce nice numbers. It is the baseline for a suspected
    // cross-partition deadlock (the engine held across `transport.send`).
    //
    // Every even call: `XcallA.ping` (key on p0) → `XcallB.pong` (key on p1).
    // Every odd call: the opposite direction. Both directions fly at once, so
    // partition 0 sends to 1 exactly while 1 sends to 0.
    name: "xcall",
    desc: XCALL_ONEWAY
      ? `keyed ctx.call, p${XCALL_PA} → p${XCALL_PB}, ONE direction (control, LOAD_XCALL_ONEWAY=1)`
      : `keyed ctx.call between two virtual objects on p${XCALL_PA} ⇄ p${XCALL_PB}, both directions at once`,
    prefix: "xcall/",
    runsPerInvocation: 0,
    watch: true,
    async prepare() {
      if (partitions < 2) {
        return (
          `needs ≥ 2 partitions, has ${partitions} — ` +
          `LOAD_ARGS="--main-store memory:// --cluster --partitions 4"`
        );
      }
      const a = await keysOn("XcallA", XCALL_PA, 2 * XCALL_KEYS);
      const b = await keysOn("XcallB", XCALL_PB, 2 * XCALL_KEYS);
      xcallKeys.aPing = a.slice(0, XCALL_KEYS);
      xcallKeys.aPong = a.slice(XCALL_KEYS);
      xcallKeys.bPing = b.slice(0, XCALL_KEYS);
      xcallKeys.bPong = b.slice(XCALL_KEYS);
      return null;
    },
    fire: (n) => {
      const tag = `xcall/${n}`;
      const forward = XCALL_ONEWAY || n % 2 === 0;
      const i = (XCALL_ONEWAY ? n : Math.floor(n / 2)) % XCALL_KEYS;
      const [svc, key, peerKey] = forward
        ? ["XcallA", xcallKeys.aPing[i]!, xcallKeys.bPong[i]!]
        : ["XcallB", xcallKeys.bPing[i]!, xcallKeys.aPong[i]!];
      return shoot(`/invoke/${svc}/ping?key=${key}`, { tag, n, peerKey }, (out) =>
        out.via === peerKey ? echoes(tag, n)(out) : `pong returned from key ${out.via}, expected ${peerKey}`,
      );
    },
  },
  // ── Agent and workflow call shapes. They run on one partition (every call
  // local); on a multi-range cluster, each scenario's "cross-range" line gives
  // the share of calls that go through the transport.
  {
    name: "tree",
    desc:
      `unkeyed call tree depth ${TREE_DEPTH} width ${TREE_WIDTH} (${treeNodes()} invocations per tree), ` +
      `${TREE_CONCURRENCY} trees at once — workflow / agent tree`,
    prefix: "tree/",
    runsPerInvocation: TREE_WIDTH ** TREE_DEPTH,
    concurrency: TREE_CONCURRENCY,
    fire: (n) => {
      const tag = `tree/${n}`;
      return shoot(
        "/invoke/InterService/tree",
        { tag, n, depth: TREE_DEPTH, width: TREE_WIDTH },
        (out) => (out.nodes === treeNodes() ? echoes(tag, n)(out) : `tree has ${out.nodes} nodes, expected ${treeNodes()}`),
      );
    },
  },
  {
    name: "fanout",
    desc: `parent calls ${FANOUT} unkeyed children at once, ${FANOUT_CONCURRENCY} parents at once — ETL / batch`,
    prefix: "fanout/",
    runsPerInvocation: FANOUT,
    concurrency: FANOUT_CONCURRENCY,
    fire: (n) => {
      const tag = `fanout/${n}`;
      return shoot("/invoke/InterService/fanout", { tag, n, fanout: FANOUT, spread: FANOUT_SPREAD }, (out) =>
        out.done === FANOUT ? echoes(tag, n)(out) : `${out.done} children done, expected ${FANOUT}`,
      );
    },
  },
  {
    name: "agent",
    desc: `ctx.run sleeps ${STEP_MS}ms then 3 sequential unkeyed ctx.call — agent workflow`,
    prefix: "agent/",
    // `think` plus three tools.
    runsPerInvocation: 4,
    fire: (n) => {
      const tag = `agent/${n}`;
      return shoot("/invoke/InterService/agent", { tag, n, stepMs: STEP_MS }, (out) =>
        out.tools === 3 ? echoes(tag, n)(out) : `agent returned ${JSON.stringify(out)}`,
      );
    },
  },
  {
    name: "saga",
    desc: "3 virtual objects with different keys called in sequence — a saga across entities",
    prefix: "saga/",
    runsPerInvocation: 0,
    // The key of each step belongs to this call alone (`saga-<n>-<i>`), so each step
    // must see an empty state: a `count` other than 1 means a step ran twice or a
    // call strayed into someone else's object.
    fire: (n) => {
      const tag = `saga/${n}`;
      return shoot("/invoke/InterService/saga", { tag, n }, (out) => {
        const counts = out.counts as unknown;
        return Array.isArray(counts) && counts.length === 3 && counts.every((c) => c === 1)
          ? echoes(tag, n)(out)
          : `saga steps returned count ${JSON.stringify(counts)}, expected [1,1,1]`;
      });
    },
  },
];

/** Number of invocations in a `tree`: `Σ width^i, i = 0..depth`. */
function treeNodes(): number {
  let total = 0;
  for (let i = 0; i <= TREE_DEPTH; i++) total += TREE_WIDTH ** i;
  return total;
}

// ───────────────────────────────────────────── the deployment's counters

interface Stats {
  runExecutions: number;
  runTags: number;
  runDuplicates: number;
  dupSamples: string[];
  turnExecutions: number;
  turnTags: number;
  maxTurns: number;
}

async function stats(prefix: string): Promise<Stats> {
  const r = await get(`${statsUrl}/stats?prefix=${encodeURIComponent(prefix)}`);
  return JSON.parse(r.body) as Stats;
}

// ───────────────────────────────────────────── load loop

/**
 * Closed loop: `CONCURRENCY` runners, each sends one call and waits for it to
 * finish before sending the next.
 *
 * Closed loop rather than open loop on purpose: cairn has flow control, so firing
 * at a fixed rate into a capped system only measures the cap. Closing the loop
 * lets the load shrink to the capacity, and the number read out is "how many
 * concurrent users this system bears", the question an operator actually asks.
 */
/** Progress of one `drive`, visible from outside while it runs. */
interface Live {
  shots: Shot[];
  issued: number;
}

async function drive(
  sc: Scenario,
  seconds: number,
  seq: { n: number },
  conc = sc.concurrency ?? CONCURRENCY,
  cap = sc.maxShots ?? Infinity,
  live: Live = { shots: [], issued: 0 },
): Promise<Run> {
  const until = performance.now() + seconds * 1000;
  const t0 = performance.now();
  await Promise.all(
    Array.from({ length: conc }, async (_unused, worker) => {
      while (performance.now() < until && live.issued < cap) {
        live.issued++;
        // `worker` travels with every shot: a worker sends **sequentially** (awaiting
        // each one), so its own sequence of shots is the only thing in this run with
        // a known order. The ordering check relies on exactly that — see `hot`.
        live.shots.push({ ...(await sc.fire(seq.n++)), worker });
      }
    }),
  );
  return { shots: live.shots, elapsedS: (performance.now() - t0) / 1000 };
}

/**
 * What does the node still answer once the partition stands still? Each probe is capped at 3 s.
 *
 * Tells apart two very different kinds of stall: a stuck *invocation* (the engine
 * is still idle, GETs answer, nobody just wakes the work) and a stuck *engine*
 * (GETs needing the engine hang too — someone holds it and does not let go).
 */
async function diagnose(): Promise<string> {
  const probe = async (path: string): Promise<string> => {
    const t0 = performance.now();
    const r = await Promise.race([
      get(`${server}${path}`).then(
        (x) => `${x.status}${x.status === 200 && path.startsWith("/invocations") ? ` (${(JSON.parse(x.body) as unknown[]).length} items)` : ""}`,
        (e: unknown) => `error ${String(e)}`,
      ),
      sleep(3000).then(() => undefined),
    ]);
    return `${path} → ${r ?? "NO answer within 3s"}${r ? ` after ${f(performance.now() - t0, 0)}ms` : ""}`;
  };
  const out: string[] = [];
  for (const p of ["/health", "/cluster", "/invocations?status=running&limit=1000", "/invocations?status=suspended&limit=1000"]) {
    out.push(await probe(p));
  }
  return out.join("\n      ");
}

/**
 * `drive`, with a watchdog: no call completes within `LOAD_STALL_S` seconds while
 * calls are still in flight ⇒ return immediately with `wedged`, abandoning the stuck calls.
 *
 * Abandoning is correct, not sloppy: once a partition is stuck none of its calls
 * come back, and the only thing still worth knowing is **after how much work it
 * stalled**. The hanging promises die with the process at `process.exit`.
 */
async function driveWatched(
  sc: Scenario,
  seconds: number,
  seq: { n: number },
): Promise<Run & { wedged?: string }> {
  const live: Live = { shots: [], issued: 0 };
  const t0 = performance.now();
  const run = drive(sc, seconds, seq, sc.concurrency ?? CONCURRENCY, sc.maxShots ?? Infinity, live);
  let seen = 0;
  let lastAt = t0;
  for (;;) {
    const done = await Promise.race([run, sleep(250).then(() => undefined)]);
    if (done) return done;
    const now = performance.now();
    if (live.shots.length !== seen) {
      seen = live.shots.length;
      lastAt = now;
    }
    const inFlight = live.issued - live.shots.length;
    if (inFlight > 0 && now - lastAt > STALL_S * 1000) {
      const ok = live.shots.filter((s) => s.ok).length;
      return {
        shots: [...live.shots],
        elapsedS: (now - t0) / 1000,
        wedged:
          `WEDGED after ${ok} completions — ${inFlight} calls in flight, none ` +
          `finished within ${STALL_S}s (last completion at second ${f((lastAt - t0) / 1000)})`,
      };
    }
  }
}

/**
 * Waits until the partition has no work running or suspended.
 *
 * Needed because `/send` **accepts** a hundred times faster than the engine
 * **does**: the warm-up phase finished answering long ago while its queue is still
 * draining, and those `ctx.run`s would land in the reset counters — the
 * exactly-once check would go red for miscounting the previous phase's work, not because the engine did wrong.
 */
async function quiesce(timeoutMs = 300_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    let busy = 0;
    for (const s of ["running", "suspended"]) {
      const r = await get(`${server}/invocations?status=${s}&limit=1000`);
      busy += (JSON.parse(r.body) as unknown[]).length;
    }
    if (busy === 0 || Date.now() > deadline) return;
    await sleep(200);
  }
}

// ───────────────────────────────────────────── statistics

function pct(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const i = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
  return sorted[i]!;
}

const f = (x: number, d = 1) => x.toFixed(d);

interface Row {
  name: string;
  n: number;
  fail: number;
  throttled: number;
  rps: number;
  p50: number;
  p90: number;
  p99: number;
  max: number;
  turnsPerInv: number;
}

// ───────────────────────────────────────────── global checks

/** N calls with the same `Idempotency-Key` ⇒ exactly **one** invocation. */
async function checkIdempotency(): Promise<string[]> {
  const K = 32;
  const key = `load-idem-${process.pid}`;
  const tag = "idem/1";
  const rs = await Promise.all(
    Array.from({ length: K }, () =>
      shoot("/invoke/LoadService/run1", { tag, n: 1 }, undefined, { "idempotency-key": key }),
    ),
  );
  const errs: string[] = [];
  const bad = rs.filter((r) => !r.ok);
  if (bad.length > 0) return [`${bad.length}/${K} idempotent calls failed: ${bad[0]!.err}`];
  const ids = new Set(rs.map((r) => r.id));
  if (ids.size !== 1) errs.push(`${K} calls with the same key produced ${ids.size} invocations`);
  const st = await stats(tag);
  if (st.runExecutions !== 1) {
    errs.push(`side effect ran ${st.runExecutions} times for ${K} calls with the same key (must be 1)`);
  }
  return errs;
}

/** The journal must be contiguous from 0 — a hole at one spot is a replay off by one spot. */
async function checkJournals(ids: string[]): Promise<string[]> {
  const errs: string[] = [];
  for (const id of ids) {
    const r = await get(`${server}/invocations/${id}/journal`);
    if (r.status !== 200) {
      errs.push(`journal ${id}: HTTP ${r.status}`);
      continue;
    }
    const entries = JSON.parse(r.body) as { index: number }[];
    if (entries.length === 0) {
      errs.push(`journal ${id} is empty`);
      continue;
    }
    for (let i = 0; i < entries.length; i++) {
      if (entries[i]!.index !== i) {
        errs.push(`journal ${id} has a hole at position ${i} (index=${entries[i]!.index})`);
        break;
      }
    }
  }
  return errs;
}

/** Nobody left running, suspended, or waiting for a human — the load is over, so the work is over. */
async function checkNothingStuck(): Promise<string[]> {
  const errs: string[] = [];
  for (const status of ["running", "suspended", "paused"]) {
    // Background ticks are still running; give them a few seconds to drain before passing judgement.
    let left: { id: string; service?: string }[] = [];
    for (let i = 0; i < 40; i++) {
      const r = await get(`${server}/invocations?status=${status}&limit=1000`);
      left = JSON.parse(r.body) as { id: string; service?: string }[];
      if (left.length === 0) break;
      await sleep(500);
    }
    if (left.length > 0) {
      errs.push(
        `${left.length} invocations still ${status} after the load stopped ` +
          `(e.g. ${left[0]!.id})`,
      );
    }
  }
  return errs;
}

/** Reads `/metrics` as `name → number` (skipping labelled lines). */
async function metrics(): Promise<Map<string, number>> {
  const r = await get(`${server}/metrics`);
  const m = new Map<string, number>();
  for (const line of r.body.split("\n")) {
    if (line.startsWith("#") || !line.trim()) continue;
    const sp = line.lastIndexOf(" ");
    const name = line.slice(0, sp);
    if (name.includes("{")) continue;
    m.set(name, Number(line.slice(sp + 1)));
  }
  return m;
}

/**
 * Directory to write the `/metrics` difference of each measurement window to,
 * captured **right** after the window (before the checks) — for counting store
 * requests per call. Empty ⇒ not written.
 */
const METRICS_DUMP = process.env.LOAD_METRICS_DUMP;

async function dumpWindow(name: string, before: Map<string, number>, run: Run): Promise<void> {
  const after = await samples();
  const delta: Record<string, number> = {};
  for (const [k, v] of after) {
    const d = v - (before.get(k) ?? 0);
    if (d !== 0) delta[k] = d;
  }
  const ok = run.shots.filter((s) => s.ok).length;
  const { mkdirSync, writeFileSync } = await import("node:fs");
  mkdirSync(METRICS_DUMP!, { recursive: true });
  writeFileSync(
    join(METRICS_DUMP!, `${name}-c${CONCURRENCY}.json`),
    JSON.stringify({ name, concurrency: CONCURRENCY, elapsedS: run.elapsedS, shots: run.shots.length, ok, delta }, null, 1),
  );
}

/** Reads `/metrics` as `verbatim line before the value → number`, **including** labelled lines. */
async function samples(): Promise<Map<string, number>> {
  const r = await get(`${server}/metrics`);
  const m = new Map<string, number>();
  for (const line of r.body.split("\n")) {
    if (line.startsWith("#") || !line.trim()) continue;
    const sp = line.lastIndexOf(" ");
    m.set(line.slice(0, sp), Number(line.slice(sp + 1)));
  }
  return m;
}

/** Lock levels of `cairnflow_calls_total`, in the order the node prints them. */
const KEY_CLASSES = ["scoped", "object", "idempotent", "workflow", "unkeyed"] as const;
const MESSAGE_KINDS = ["start_call", "completion", "attach", "peek", "signal", "cancel"] as const;
const OUTCOMES = ["ok", "rejected", "wrong_range", "unreachable"] as const;

/** Calls and cross-range messages of one measurement window. */
interface Interrange {
  calls: number;
  remote: number;
  /** `lock level → [local, remote]`, only non-zero levels. */
  byClass: Map<string, [number, number]>;
  messages: number;
  /** Broken messages by outcome (`rejected`, `wrong_range`, `unreachable`), only non-zero outcomes. */
  failed: Map<string, number>;
  syncCount: number;
  syncMs: number;
  sendCount: number;
  sendMs: number;
  /** Upper bound of the bucket holding p50/p99 (ms), from the histogram — coarse, by bucket. */
  syncP50: number;
  syncP99: number;
  sendP50: number;
  sendP99: number;
  /** The outbox drainer counters; `undefined` when the binary does not have them yet. */
  drain: { passes: number; read: number; busy: number; handled: number } | undefined;
  elapsedS: number;
}

/** Upper bound (ms) of the bucket holding quantile `q` of a histogram difference. */
function bucketQuantile(a: Map<string, number>, b: Map<string, number>, name: string, q: number): number {
  const rows: [number, number][] = [];
  for (const [k, v] of b) {
    const m = new RegExp(`^${name}_bucket\\{le="([^"]+)"\\}$`).exec(k);
    if (!m) continue;
    const le = m[1] === "+Inf" ? Infinity : Number(m[1]) * 1000;
    rows.push([le, v - (a.get(k) ?? 0)]);
  }
  rows.sort((x, y) => x[0] - y[0]);
  const total = rows.length > 0 ? rows[rows.length - 1]![1] : 0;
  if (total === 0) return 0;
  return rows.find(([, acc]) => acc >= q * total)?.[0] ?? Infinity;
}

function interrangeOf(a: Map<string, number>, b: Map<string, number>, elapsedS: number): Interrange {
  const d = (k: string) => (b.get(k) ?? 0) - (a.get(k) ?? 0);
  const byClass = new Map<string, [number, number]>();
  let calls = 0;
  let remote = 0;
  for (const k of KEY_CLASSES) {
    const l = d(`cairnflow_calls_total{placement="local",key_class="${k}"}`);
    const r = d(`cairnflow_calls_total{placement="remote",key_class="${k}"}`);
    if (l + r > 0) byClass.set(k, [l, r]);
    calls += l + r;
    remote += r;
  }
  let messages = 0;
  const failed = new Map<string, number>();
  for (const kind of MESSAGE_KINDS) {
    for (const o of OUTCOMES) {
      const v = d(`cairnflow_interrange_messages_total{kind="${kind}",outcome="${o}"}`);
      messages += v;
      if (o !== "ok" && v > 0) failed.set(o, (failed.get(o) ?? 0) + v);
    }
  }
  const sync = "cairnflow_interrange_sync_seconds";
  const send = "cairnflow_interrange_send_seconds";
  return {
    calls,
    remote,
    byClass,
    messages,
    failed,
    syncCount: d(`${sync}_count`),
    syncMs: d(`${sync}_sum`) * 1000,
    sendCount: d(`${send}_count`),
    sendMs: d(`${send}_sum`) * 1000,
    syncP50: bucketQuantile(a, b, sync, 0.5),
    syncP99: bucketQuantile(a, b, sync, 0.99),
    sendP50: bucketQuantile(a, b, send, 0.5),
    sendP99: bucketQuantile(a, b, send, 0.99),
    drain: b.has("cairnflow_outbox_rows_read_total")
      ? {
          passes: d("cairnflow_outbox_drain_passes_total"),
          read: d("cairnflow_outbox_rows_read_total"),
          busy: d("cairnflow_outbox_lock_busy_total"),
          handled: d("cairnflow_outbox_rows_handled_total"),
        }
      : undefined,
    elapsedS,
  };
}

const pctOf = (part: number, whole: number) => (whole > 0 ? `${f((100 * part) / whole)}%` : "–");

/** One "cross-range" line under a scenario's result. No calls ⇒ empty. */
function interrangeLine(x: Interrange): string {
  if (x.calls === 0 && x.messages === 0) return "";
  const classes = [...x.byClass]
    .map(([k, [l, r]]) => `${k} ${pctOf(r, l + r)}`)
    .join(", ");
  const avg = (ms: number, n: number) => (n > 0 ? f(ms / n, 2) : "–");
  const failed = [...x.failed].map(([o, v]) => `${o} ${v}`).join(", ");
  return (
    `  cross-range: ${x.calls} calls (${f(x.calls / x.elapsedS)}/s), remote ${pctOf(x.remote, x.calls)}` +
    `${classes ? ` [${classes}]` : ""} · ${x.messages} msgs (${f(x.messages / x.elapsedS)}/s)` +
    `${failed ? `, broken: ${failed}` : ""}\n` +
    `            sync mean ${avg(x.syncMs, x.syncCount)} ms (p50 ≤${x.syncP50}, p99 ≤${x.syncP99}) · ` +
    `send mean ${avg(x.sendMs, x.sendCount)} ms (p50 ≤${x.sendP50}, p99 ≤${x.sendP99}) · ` +
    `${x.sendCount} drainer sends\n` +
    (x.drain
      ? `            drainer: ${x.drain.passes} passes · ${x.drain.read} rows read · ` +
        `${x.drain.busy} lock busy · ${x.drain.handled} rows done\n`
      : "")
  );
}

// ───────────────────────────────────────────── concurrency sweep

interface SweepPoint {
  n: number;
  rps: number;
  p50: number;
  p99: number;
  /** Calls completed according to the server (`cairnflow_invoke_completed_total`) in the same window. */
  srv: number;
  srvRps: number;
  mismatch: boolean;
}

/**
 * One level of the sweep: a short warm-up at exactly that level (not counted), then
 * a closed-loop `echo` measurement. The server's count is an independent cross-check:
 * the system is quiet before the window (`quiesce`) and `drive` waits for every call
 * to return, so the two numbers must be equal — a mismatch is the load generator's bug, not cairn's.
 */
async function sweepLevel(c: number, seq: { n: number }): Promise<SweepPoint> {
  const echo = SCENARIOS.find((s) => s.name === "echo")!;
  await drive(echo, Math.min(WARMUP_S, 2), seq, c);
  await quiesce();
  const ma = await metrics();
  const r = await drive(echo, Math.max(5, Math.round(DURATION_S / 2)), seq, c);
  const mb = await metrics();
  const lat = r.shots.filter((s) => s.ok).map((s) => s.ms).sort((a, b) => a - b);
  const done = "cairnflow_invoke_completed_total";
  const srv = (mb.get(done) ?? 0) - (ma.get(done) ?? 0);
  return {
    n: lat.length,
    rps: r.shots.length / r.elapsedS,
    p50: pct(lat, 50),
    p99: pct(lat, 99),
    srv,
    srvRps: srv / r.elapsedS,
    mismatch: srv !== lat.length,
  };
}

// ───────────────────────────────────────────── run

const rows: Row[] = [];
const failures: string[] = [];
const sampleIds: string[] = [];
/** The cross-range line of every measured scenario — the end-of-run table. */
const interranges: (Interrange & { name: string })[] = [];
let totalShots = 0;
/** The scenario that made the partition stand still, if any. Nothing is measured after it. */
let wedgedIn: string | undefined;
/** The highest level of the opening sweep, to compare with the rerun at the end. */
let sweepTop: { c: number; rps: number; p50: number } | undefined;

try {
  await setUp();

  const m0 = await metrics();
  partitions = m0.get("cairnflow_partitions") ?? 0;
  // The simulated store profile (a `--features testkit` binary) must actually be applied: a
  // binary without that feature ignores the environment variable **silently**, and a run
  // measured "on s3-standard" that actually ran on bare `memory://` is worse than no measurement.
  const storeProfile = process.env.CAIRNFLOW_STORE_PROFILE;
  if (storeProfile && ![...(await samples()).keys()].some((k) => k.startsWith("cairnflow_testkit_store_"))) {
    throw new Error(
      `CAIRNFLOW_STORE_PROFILE=${storeProfile} but /metrics has no cairnflow_testkit_store_* — ` +
        `was binary ${BINARY} built with --features testkit?`,
    );
  }
  if (EXTRA_ARGS.length === 0 && partitions !== 1) {
    throw new Error(`not one partition: cairnflow_partitions=${partitions}`);
  }

  console.log(
    `cairnflow load test — ${partitions} partition · ${CONCURRENCY} concurrent calls · ` +
      `${DURATION_S}s/scenario (+${WARMUP_S}s warm-up)\n` +
      `binary ${BINARY}${EXTRA_ARGS.length > 0 ? ` ${EXTRA_ARGS.join(" ")}` : ""}` +
      `${storeProfile ? `\nstore profile ${storeProfile} (applied: cairnflow_testkit_store_* present)` : ""}\n`,
  );

  const seq = { n: 0 };
  if (SWEEP_LEVELS.length > 0) {
    // The question the scenario table cannot answer: **does adding waiters add work
    // done?** With one partition, the answer decides how people deploy cairn — if
    // throughput is flat across concurrency, the only way to go faster is more
    // partitions, not more clients.
    console.log("concurrency sweep (echo, clean store):");
    console.log("  concurrency  inv/s     p50 ms    p99 ms    server inv/s");
    for (const c of SWEEP_LEVELS) {
      const r = await sweepLevel(c, seq);
      console.log(
        `  ${String(c).padEnd(11)}${f(r.rps).padEnd(10)}${f(r.p50).padEnd(10)}${f(r.p99).padEnd(10)}` +
          `${f(r.srvRps)}${r.mismatch ? `  ✗ client ${r.n} ≠ server ${r.srv}` : ""}`,
      );
      if (r.mismatch) failures.push(`[sweep ${c}] client counted ${r.n} completed calls, server ${r.srv}`);
      if (c === Math.max(...SWEEP_LEVELS)) sweepTop = { c, rps: r.rps, p50: r.p50 };
    }
    console.log();
  }
  for (const sc of SCENARIOS) {
    if (ONLY.length > 0 && !ONLY.includes(sc.name)) continue;
    process.stdout.write(`▸ ${sc.name.padEnd(6)} ${sc.desc}\n`);

    const skipWhy = await sc.prepare?.();
    if (skipWhy) {
      process.stdout.write(`  – skipped: ${skipWhy}\n\n`);
      continue;
    }
    const go = (seconds: number): Promise<Run & { wedged?: string }> =>
      sc.watch ? driveWatched(sc, seconds, seq) : drive(sc, seconds, seq);

    let wedged: string | undefined;
    if (WARMUP_S > 0) {
      const w = await go(WARMUP_S);
      if (w.wedged) wedged = `${w.wedged} — already during the ${WARMUP_S}s warm-up`;
    }
    // The counters are reset **after** warm-up, and after the warm-up has fully
    // drained: the warm-up is not part of the correctness checks, and mixing it in
    // only makes the numbers harder to read.
    let run: Run & { wedged?: string } = { shots: [], elapsedS: 0 };
    let before = new Map<string, number>();
    if (!wedged) {
      await quiesce();
      await post(`${statsUrl}/reset`);
      sc.reset?.();
      before = await samples();
      let stopQueries = false;
      const queryCodes = new Map<number, number>();
      const queryLoops = Array.from({ length: QUERY_LOOP }, async (_, k) => {
        const sqls = [
          "SELECT id, service, created_at FROM sys_invocation ORDER BY created_at DESC, id LIMIT 50",
          "SELECT class, count(*) FROM sys_invocation GROUP BY class",
        ];
        for (let i = k; !stopQueries; i++) {
          const r = await post(
            `${server}/query`,
            JSON.stringify({ sql: sqls[i % sqls.length] }),
            { "content-type": "application/json" },
          ).catch(() => ({ status: 0, body: "" }));
          queryCodes.set(r.status, (queryCodes.get(r.status) ?? 0) + 1);
          // Refused (429: slots, the tenant's token bucket): back off like a real
          // client — a bare 429 spin loop would measure the run's own noise, not the queries'.
          if (r.status !== 200) await new Promise((res) => setTimeout(res, 100));
        }
      });
      run = await go(DURATION_S);
      stopQueries = true;
      await Promise.all(queryLoops);
      if (QUERY_LOOP > 0) {
        const total = [...queryCodes.values()].reduce((a, b) => a + b, 0);
        process.stdout.write(
          `  parallel queries: ${QUERY_LOOP} loops, ${total} statements (${(total / run.elapsedS).toFixed(1)}/s) — codes ${[...queryCodes].map(([c, n]) => `${c}×${n}`).join(", ")}\n`,
        );
      }
      wedged = run.wedged;
      if (METRICS_DUMP) await dumpWindow(sc.name, before, run);
    }
    totalShots += run.shots.length;

    if (wedged) {
      // The partition is stuck: every check after this (`quiesce` included) would only
      // wait for an answer that never comes. Record where it stalled and stop.
      wedgedIn = sc.name;
      process.stdout.write(`  what the node still answers:\n      ${await diagnose()}\n`);
      failures.push(`[${sc.name}] ${wedged}`);
      const lat = run.shots.filter((s) => s.ok).map((s) => s.ms).sort((a, b) => a - b);
      rows.push({
        name: sc.name,
        n: run.shots.length,
        fail: run.shots.filter((s) => !s.ok).length,
        throttled: run.shots.filter((s) => s.throttled).length,
        rps: run.elapsedS > 0 ? run.shots.length / run.elapsedS : 0,
        p50: pct(lat, 50),
        p90: pct(lat, 90),
        p99: pct(lat, 99),
        max: lat.length > 0 ? lat[lat.length - 1]! : 0,
        turnsPerInv: 0,
      });
      process.stdout.write(`  ✗ ${wedged}\n\n`);
      break;
    }

    const errs: string[] = [];
    const failed = run.shots.filter((s) => !s.ok);
    if (failed.length > 0) {
      errs.push(`${failed.length}/${run.shots.length} calls failed — e.g.: ${failed[0]!.err}`);
    }
    for (const s of run.shots) {
      if (s.ok && s.id && sampleIds.length < 20 && Math.random() < 0.01) sampleIds.push(s.id);
    }

    if (sc.verify) errs.push(...(await sc.verify(run)));
    const ir = interrangeOf(before, await samples(), run.elapsedS);
    interranges.push({ name: sc.name, ...ir });
    // There is no `kill -9` in this run: a broken cross-range message is a bug of
    // the send path (destination unreachable, rejected), not noise.
    if (ir.failed.size > 0) {
      errs.push(`${[...ir.failed.values()].reduce((a, b) => a + b, 0)} broken cross-range messages (${[...ir.failed].map(([o, v]) => `${o} ${v}`).join(", ")})`);
    }

    // Exactly-once. There is no `kill -9` in this run, so this is an absolute
    // promise: every tag exactly once, no more. A 2 means the engine re-ran a
    // side effect that was already durable.
    const st = await stats(sc.prefix);
    if (sc.runsPerInvocation > 0) {
      if (st.runDuplicates > 0) {
        errs.push(
          `EXACTLY-ONCE RED: ${st.runDuplicates} side effects re-ran ` +
            `(${st.dupSamples.join(", ")})`,
        );
      }
      const wantTags = run.shots.filter((s) => s.ok).length * sc.runsPerInvocation;
      if (st.runTags !== wantTags) {
        errs.push(`${st.runTags} side effects ran, expected ${wantTags}`);
      }
    }

    const lat = run.shots.filter((s) => s.ok).map((s) => s.ms).sort((a, b) => a - b);
    rows.push({
      name: sc.name,
      n: run.shots.length,
      fail: failed.length,
      throttled: run.shots.filter((s) => s.throttled).length,
      rps: run.shots.length / run.elapsedS,
      p50: pct(lat, 50),
      p90: pct(lat, 90),
      p99: pct(lat, 99),
      max: lat.length > 0 ? lat[lat.length - 1]! : 0,
      turnsPerInv: st.turnTags > 0 ? st.turnExecutions / st.turnTags : 0,
    });

    for (const e of errs) failures.push(`[${sc.name}] ${e}`);
    process.stdout.write(
      `  ${errs.length === 0 ? "✓" : "✗"} ${run.shots.length} calls · ` +
        `${f(run.shots.length / run.elapsedS)} inv/s · p50 ${f(pct(lat, 50))}ms\n` +
        `${interrangeLine(ir)}\n`,
    );
  }

  // ── Global checks
  //
  // Skipped entirely if stalled: all four need the partition to answer.
  let requests = Number.NaN;
  if (wedgedIn) {
    process.stdout.write(`▸ global checks — skipped, the partition stalled in \`${wedgedIn}\`\n\n`);
  } else {
    process.stdout.write("▸ global checks\n");
    for (const [what, fn] of [
      ["idempotency", checkIdempotency],
      ["contiguous journal", () => checkJournals(sampleIds)],
      ["nothing left hanging", checkNothingStuck],
    ] as const) {
      const errs = await fn();
      process.stdout.write(`  ${errs.length === 0 ? "✓" : "✗"} ${what}\n`);
      for (const e of errs) failures.push(`[global] ${what}: ${e}`);
    }

    const m1 = await metrics();
    requests = (m1.get("cairnflow_invoke_requests_total") ?? 0) - (m0.get("cairnflow_invoke_requests_total") ?? 0);
    const timeouts = (m1.get("cairnflow_sdk_timeouts_total") ?? 0) - (m0.get("cairnflow_sdk_timeouts_total") ?? 0);
    if (timeouts > 0) {
      failures.push(`[global] ${timeouts} turns dropped because the SDK did not make progress within the deadline`);
    }
    process.stdout.write(`  ${timeouts === 0 ? "✓" : "✗"} no turn past the SDK deadline\n\n`);
  }

  // ── Report
  const w = [8, 9, 6, 10, 10, 9, 9, 9, 9];
  const head = ["scenario", "calls", "failed", "inv/s", "p50 ms", "p90 ms", "p99 ms", "max ms", "turns/inv"];
  console.log(head.map((h, i) => h.padEnd(w[i]!)).join(""));
  console.log("─".repeat(w.reduce((a, b) => a + b, 0)));
  for (const r of rows) {
    console.log(
      [
        r.name,
        String(r.n),
        String(r.fail),
        f(r.rps),
        f(r.p50, 2),
        f(r.p90, 2),
        f(r.p99, 2),
        f(r.max, 1),
        f(r.turnsPerInv, 2),
      ]
        .map((c, i) => c.padEnd(w[i]!))
        .join(""),
    );
  }

  // ── Cross-range: only scenarios with calls.
  const withCalls = interranges.filter((x) => x.calls > 0 || x.messages > 0);
  if (withCalls.length > 0) {
    console.log("\ncross-range (each scenario's measurement window; p50/p99 are bucket upper bounds):");
    const wi = [9, 11, 9, 9, 10, 9, 14, 9, 14, 10];
    const hi = ["scenario", "calls/s", "remote", "msg/s", "broken msg", "sync mean", "sync p50/p99", "send mean", "send p50/p99", "sync+send"];
    console.log(hi.map((h, i) => h.padEnd(wi[i]!)).join(""));
    console.log("─".repeat(wi.reduce((a, b) => a + b, 0)));
    for (const x of withCalls) {
      const avg = (ms: number, n: number) => (n > 0 ? f(ms / n, 2) : "–");
      console.log(
        [
          x.name,
          f(x.calls / x.elapsedS),
          pctOf(x.remote, x.calls),
          f(x.messages / x.elapsedS),
          String([...x.failed.values()].reduce((a, b) => a + b, 0)),
          avg(x.syncMs, x.syncCount),
          `${x.syncP50}/${x.syncP99}`,
          avg(x.sendMs, x.sendCount),
          `${x.sendP50}/${x.sendP99}`,
          // Drainer time in both phases, per wall-clock second: ≈ the number of
          // continuously busy drainers. Far from the number of sending ranges ⇒ the time is spent elsewhere.
          `${f((x.syncMs + x.sendMs) / 1000 / x.elapsedS, 2)} s/s`,
        ]
          .map((c, i) => c.padEnd(wi[i]!))
          .join(""),
      );
    }
  }

  // ── Residual state
  //
  // The highest sweep level, rerun after every scenario. Clearly slower than at
  // the start means the earlier scenarios left behind something that makes *every*
  // later turn more expensive — a warning flag, not a correctness check.
  if (sweepTop && !wedgedIn) {
    const again = await sweepLevel(sweepTop.c, seq);
    const ratio = again.rps / sweepTop.rps;
    console.log(
      `\nresidual state: echo at ${sweepTop.c} concurrency ${f(sweepTop.rps)} → ${f(again.rps)} inv/s ` +
        `after the scenarios (×${f(ratio, 2)}; p50 ${f(sweepTop.p50)} → ${f(again.p50)} ms)` +
        (ratio < 0.7 ? "\n  ⚠ clearly slower: earlier scenarios left work on every turn (see LOAD_SWEEP)" : ""),
    );
  }

  const throttled = rows.reduce((a, r) => a + r.throttled, 0);
  console.log(
    `\n${totalShots} calls from the generator · ${Number.isNaN(requests) ? "?" : requests} requests through ingress · ` +
      `${throttled} times got 429 (durable, runs later)`,
  );
  console.log(
    "column turns/inv = number of times the handler body ran per invocation; >1 is replay, " +
      "not a failure.",
  );
} finally {
  tearDown();
}

if (failures.length > 0) {
  console.error(`\n✗ ${failures.length} checks RED:`);
  for (const e of failures) console.error(`  · ${e}`);
  process.exit(1);
}
console.log("\n✓ every check green");
process.exit(0);
