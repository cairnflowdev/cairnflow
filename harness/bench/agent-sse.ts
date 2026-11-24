// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

/**
 * SSE streams under load on a **real** cairn cluster, `count-only` profile,
 * SeaweedFS.
 *
 * ```sh
 * cargo build --release -p cairn-server --bin cairnflow --bin cairn-ctl \
 *   --features failpoints,testkit --target-dir target/chaos
 * node harness/bench/agent-sse.ts --out bench/s3-model/results/sse.jsonl
 * ```
 *
 * Two phases on the **same** cluster of `SSE_NODES` (4) nodes × `SSE_PARTITIONS` (8) ranges:
 *
 * 1. **`latency`**: `SSE_LAT_INV` (16) invocations, each one `ctx.run` writing
 *    `{ts}` to the transient stream `ui` every 20 ms for `SSE_LAT_S` seconds. Each
 *    invocation gets one SSE client on the leader of its range. Latency = receive
 *    time − `ts`: SDK, node and client share the process/machine and therefore the
 *    clock. Includes the SDK's 50 ms batching.
 * 2. **`fanout`**: `SSE_CLIENTS` (256) loops, each loop `/send`s one invocation
 *    lasting `SSE_INV_S` (60 s, 1 record/s) and then waits exactly that long — no
 *    status polling, so a loop issues no other request. The `base` window
 *    (`SSE_BASE_S`, 600 s) has only the loops; the `sse` window (`SSE_S`, 1 800 s)
 *    adds one SSE client per loop, opened on a **random** node (spread evenly over
 *    the 4 nodes), following the `leader` of a `503`, reconnecting with
 *    `Last-Event-ID` when the connection closes (10-minute cap) or on `moved`.
 *    Measured: store requests per second in both windows
 *    (`cairnflow_testkit_store_requests_total`), and
 *    `cairnflow_follower_readers_open` sampled every 5 s on every node — must
 *    always be 0 (an SSE reader never opens a follower reader).
 *
 * `--phases latency,fanout` selects the phases. Output: one JSON line per phase.
 */

import { spawn, type ChildProcess } from "node:child_process";
import { randomBytes } from "node:crypto";
import { appendFileSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { serve, service, type ServiceContext } from "../../sdk/typescript/src/index.ts";

const here = fileURLToPath(new URL(".", import.meta.url));
const repoRoot = join(here, "..", "..");
const binary = process.env.SSE_BINARY ?? join(repoRoot, "target", "chaos", "release", "cairnflow");
const profile = process.env.SSE_PROFILE ?? join(repoRoot, "bench", "s3-model", "profiles", "count-only.json");
const STORE = process.env.SSE_STORE_URI ?? `s3://walbench/sse-${Date.now()}`;
const NODES = Number(process.env.SSE_NODES ?? "4");
const PARTITIONS = Number(process.env.SSE_PARTITIONS ?? "8");
const LAT_INV = Number(process.env.SSE_LAT_INV ?? "16");
const LAT_S = Number(process.env.SSE_LAT_S ?? "60");
const CLIENTS = Number(process.env.SSE_CLIENTS ?? "256");
const INV_S = Number(process.env.SSE_INV_S ?? "60");
const BASE_S = Number(process.env.SSE_BASE_S ?? "600");
const SSE_S = Number(process.env.SSE_S ?? "1800");
const ADMIN_KEY = `cf_sse_${randomBytes(16).toString("hex")}`;

const argv = process.argv.slice(2);
const flag = (name: string) => {
  const i = argv.indexOf(name);
  return i >= 0 ? argv[i + 1] : undefined;
};
const OUT = flag("--out") ?? join(repoRoot, "bench", "s3-model", "results", `sse-${Date.now()}.jsonl`);
const PHASES = (flag("--phases") ?? "latency,fanout").split(",");

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

// ───────────────────────────────────────────── handler

const sse = service("SseAgent", {
  /** `{seconds, every}`: writes `{ts, i}` to `ui` every `every` ms for `seconds` seconds. */
  async tick(ctx: ServiceContext, input: { seconds: number; every: number }) {
    const ui = ctx.stream("ui");
    return ctx.run("model", async () => {
      const end = Date.now() + input.seconds * 1000;
      let i = 0;
      while (Date.now() < end) {
        ui.append({ ts: Date.now(), i: i++ });
        await sleep(input.every);
      }
      return i;
    });
  },
});

// ───────────────────────────────────────────── cluster

type Node = { proc: ChildProcess; url: string };

async function spawnNode(i: number): Promise<Node> {
  const proc = spawn(
    binary,
    ["--main-store", STORE, "--cluster", "--partitions", String(PARTITIONS), "--port", "0", "--node-id", `sse-${i}`],
    {
      stdio: "pipe",
      env: {
        ...process.env,
        CAIRNFLOW_STORE_PROFILE: profile,
        CAIRNFLOW_ADMIN_KEY: ADMIN_KEY,
        CAIRNFLOW_MAX_CONCURRENT_TURNS: String(Math.max(32, 2 * CLIENTS)),
      },
    },
  );
  const url = await new Promise<string>((resolve, reject) => {
    let buf = "";
    const t = setTimeout(() => reject(new Error(`node ${i} did not report its port:\n${buf}`)), 30_000);
    proc.stdout!.on("data", (c: Buffer) => {
      buf += c.toString();
      const m = /listening on (http:\/\/[\d.]+:\d+)/.exec(buf);
      if (m) {
        clearTimeout(t);
        resolve(m[1]!);
      }
      if (buf.length > 1 << 16) buf = buf.slice(-4096);
    });
    proc.stderr!.on("data", () => {});
    proc.on("exit", (code) => reject(new Error(`node ${i} exited early (${code}):\n${buf}`)));
  });
  return { proc, url };
}

const auth = (key = ADMIN_KEY): Record<string, string> => ({ authorization: `Bearer ${key}` });
const pick = <T>(xs: T[]) => xs[Math.floor(Math.random() * xs.length)]!;

/** Leader-following fetch: a 503 with `leader` is followed, otherwise back off. */
async function cfetch(nodes: Node[], path: string, init: RequestInit = {}, timeoutMs = 30_000): Promise<Response> {
  const deadline = Date.now() + timeoutMs;
  let base = pick(nodes).url;
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
    const c = (await cfetch(nodes, "/cluster", { headers: auth() }).then((r) => r.json())) as {
      partitions?: { leader?: string }[];
    };
    if (c.partitions?.every((p) => p.leader)) return;
    if (Date.now() > deadline) throw new Error(`cluster lacks leaders: ${JSON.stringify(c)}`);
    await sleep(500);
  }
}

async function tenantKey(nodes: Node[], sdkUrl: string): Promise<string> {
  const r = await cfetch(nodes, "/admin/keys", {
    method: "POST",
    headers: { ...auth(), "content-type": "application/json" },
    body: JSON.stringify({ tenant: "sse", role: "admin" }),
  });
  if (!r.ok) throw new Error(`create key: ${r.status} ${await r.text()}`);
  const body = (await r.json()) as { secret?: string; key?: string };
  const key = body.secret ?? body.key!;
  await sleep(7_000); // the key propagates to every node
  const d = await cfetch(nodes, "/deployments", {
    method: "POST",
    headers: { ...auth(key), "content-type": "application/json" },
    body: JSON.stringify({ uri: sdkUrl }),
  });
  if (!d.ok) throw new Error(`register deployment: ${d.status} ${await d.text()}`);
  return key;
}

async function send(nodes: Node[], key: string, input: { seconds: number; every: number }): Promise<string> {
  const r = await cfetch(nodes, "/invoke/SseAgent/tick/send", {
    method: "POST",
    headers: auth(key),
    body: JSON.stringify(input),
  });
  if (r.status !== 202) throw new Error(`send: ${r.status} ${await r.text()}`);
  const body = (await r.json()) as { id?: string; invocationId?: string };
  return body.id ?? body.invocationId!;
}

// ───────────────────────────────────────────── counters

type Counts = Record<string, number>;

/** Store requests by op summed over every node, and the gauges of open follower readers / SSE. */
async function scrape(nodes: Node[]): Promise<{ req: Counts; bySource: Counts; followers: number; sseOpen: number }> {
  const req: Counts = {};
  const bySource: Counts = {};
  let followers = 0;
  let sseOpen = 0;
  for (const n of nodes) {
    const text = await fetch(`${n.url}/metrics`, { headers: auth() }).then((r) => r.text());
    for (const line of text.split("\n")) {
      const sp = line.lastIndexOf(" ");
      const v = Number(line.slice(sp + 1));
      if (line.startsWith("cairnflow_follower_readers_open{")) followers += v;
      else if (line.startsWith("cairnflow_stream_sse_open")) sseOpen += v;
      else if (line.startsWith("cairnflow_testkit_store_requests_total{")) {
        const op = /op="([a-z]+)"/.exec(line)?.[1] ?? "?";
        req[op] = (req[op] ?? 0) + v;
      } else if (line.startsWith("cairnflow_testkit_store_requests_by_source_total{")) {
        // By source × op — separates the requests of SSE opens from background requests.
        const src = /source="([^"]+)"/.exec(line)?.[1] ?? "?";
        const op = /op="([a-z]+)"/.exec(line)?.[1] ?? "?";
        bySource[`${src}/${op}`] = (bySource[`${src}/${op}`] ?? 0) + v;
      }
    }
  }
  return { req, bySource, followers, sseOpen };
}

const sub = (a: Counts, b: Counts) =>
  Object.fromEntries([...new Set([...Object.keys(a), ...Object.keys(b)])].map((k) => [k, (b[k] ?? 0) - (a[k] ?? 0)]));
const total = (c: Counts) => Object.values(c).reduce((x, y) => x + y, 0);

// ───────────────────────────────────────────── client SSE

type SseStats = { records: number; reconnects: number; moved: number; gaps: number; opens: number; lat: number[] };

/**
 * Reads `/streams/ui` of `id` up to `end` (or until `until`): a random node,
 * following the `leader` of a `503` (exactly once, per the contract),
 * reconnecting with `Last-Event-ID` when the connection closes or on `moved`.
 */
async function follow(nodes: Node[], key: string, id: string, until: number, st: SseStats, keepLat: boolean) {
  let last = -1;
  let base = pick(nodes).url;
  while (Date.now() < until) {
    const headers: Record<string, string> = { ...auth(key), accept: "text/event-stream" };
    if (last >= 0) headers["last-event-id"] = String(last);
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), Math.max(0, until - Date.now()));
    let r: Response;
    try {
      st.opens++;
      r = await fetch(`${base}/invocations/${id}/streams/ui`, { headers, signal: ac.signal });
    } catch {
      clearTimeout(timer);
      if (Date.now() >= until) return;
      await sleep(200);
      base = pick(nodes).url;
      continue;
    }
    if (r.status === 503) {
      const peek = (await r.json().catch(() => undefined)) as { leader?: string } | undefined;
      clearTimeout(timer);
      base = peek?.leader?.replace(/\/$/, "") ?? pick(nodes).url;
      if (!peek?.leader) await sleep(200);
      continue;
    }
    if (r.status === 404 || r.status === 410) {
      // Not yet at the leader (new id) or expired after sealing.
      clearTimeout(timer);
      await r.body?.cancel().catch(() => {});
      if (r.status === 410) return;
      await sleep(200);
      continue;
    }
    if (!r.ok || !r.body) {
      clearTimeout(timer);
      throw new Error(`SSE ${id}: ${r.status} ${await r.text()}`);
    }
    let ended = false;
    let moved: string | undefined;
    let buf = "";
    const dec = new TextDecoder();
    try {
      for await (const chunk of r.body as unknown as AsyncIterable<Uint8Array>) {
        buf += dec.decode(chunk, { stream: true });
        let cut: number;
        while ((cut = buf.indexOf("\n\n")) >= 0) {
          const raw = buf.slice(0, cut);
          buf = buf.slice(cut + 2);
          let ev = "message";
          let data = "";
          let eid: string | undefined;
          for (const l of raw.split("\n")) {
            if (l.startsWith("event:")) ev = l.slice(6).trim();
            else if (l.startsWith("data:")) data += l.slice(5).trim();
            else if (l.startsWith("id:")) eid = l.slice(3).trim();
          }
          if (ev === "record") {
            const now = Date.now();
            const rec = JSON.parse(data) as { offset: number; data: { ts: number } };
            last = eid !== undefined ? Number(eid) : rec.offset;
            st.records++;
            if (keepLat) st.lat.push(now - rec.data.ts);
          } else if (ev === "gap") st.gaps++;
          else if (ev === "end") ended = true;
          else if (ev === "moved") moved = (JSON.parse(data) as { leader?: string }).leader;
        }
        if (ended || moved) break;
      }
    } catch {
      // Closed by the connection's time cap or by `until`: reconnect below.
    }
    clearTimeout(timer);
    await r.body.cancel().catch(() => {});
    if (ended) return;
    if (moved) {
      st.moved++;
      base = moved.replace(/\/$/, "");
    }
    st.reconnects++;
  }
}

function pct(xs: number[], p: number): number {
  if (xs.length === 0) return NaN;
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))]!;
}

// ───────────────────────────────────────────── pha

async function latencyPhase(nodes: Node[], key: string) {
  console.log(`\n== latency: ${LAT_INV} invocation × ${LAT_S}s, 1 record / 20 ms`);
  const st: SseStats = { records: 0, reconnects: 0, moved: 0, gaps: 0, opens: 0, lat: [] };
  const until = Date.now() + (LAT_S + 60) * 1000;
  await Promise.all(
    Array.from({ length: LAT_INV }, async () => {
      const id = await send(nodes, key, { seconds: LAT_S, every: 20 });
      await follow(nodes, key, id, until, st, true);
    }),
  );
  const row = {
    phase: "latency",
    invocations: LAT_INV,
    records: st.records,
    p50_ms: pct(st.lat, 50),
    p90_ms: pct(st.lat, 90),
    p99_ms: pct(st.lat, 99),
    max_ms: pct(st.lat, 100),
    reconnects: st.reconnects,
    gaps: st.gaps,
  };
  console.log(`   ${JSON.stringify(row)}`);
  appendFileSync(OUT, JSON.stringify(row) + "\n");
}

/** `CLIENTS` loops of `/send` + wait `INV_S`, for `seconds`; `withSse` adds a client per loop. */
async function loops(nodes: Node[], key: string, seconds: number, withSse: boolean, st: SseStats) {
  const end = Date.now() + seconds * 1000;
  let sent = 0;
  await Promise.all(
    Array.from({ length: CLIENTS }, async (_, k) => {
      // Stagger the start points over one cycle, so the loops do not move as a wave.
      await sleep((k / CLIENTS) * INV_S * 1000);
      while (Date.now() < end) {
        const t0 = Date.now();
        const id = await send(nodes, key, { seconds: INV_S, every: 1000 }).catch(() => undefined);
        if (id) sent++;
        const cycleEnd = t0 + (INV_S + 2) * 1000;
        if (id && withSse) await follow(nodes, key, id, Math.min(end, cycleEnd + 30_000), st, false).catch(() => {});
        await sleep(Math.max(0, Math.min(end, cycleEnd) - Date.now()));
      }
    }),
  );
  return sent;
}

async function fanoutPhase(nodes: Node[], key: string) {
  const window = async (name: string, seconds: number, withSse: boolean) => {
    console.log(`\n== fanout/${name}: ${CLIENTS} loops × ${seconds}s${withSse ? `, ${CLIENTS} SSE clients` : ""}`);
    const st: SseStats = { records: 0, reconnects: 0, moved: 0, gaps: 0, opens: 0, lat: [] };
    let maxFollowers = 0;
    let maxSse = 0;
    let samples = 0;
    let stop = false;
    const sampler = (async () => {
      while (!stop) {
        const s = await scrape(nodes).catch(() => undefined);
        if (s) {
          samples++;
          maxFollowers = Math.max(maxFollowers, s.followers);
          maxSse = Math.max(maxSse, s.sseOpen);
        }
        await sleep(5_000);
      }
    })();
    // Warm up for one cycle before measuring, so the number of running loops has settled.
    const warm = INV_S + 5;
    const run = loops(nodes, key, seconds + warm, withSse, st);
    await sleep(warm * 1000);
    const s0 = await scrape(nodes);
    const t0 = Date.now();
    const sent = await run;
    const s1 = await scrape(nodes);
    const [c0, c1] = [s0.req, s1.req];
    const bySource = sub(s0.bySource, s1.bySource);
    const dt = (Date.now() - t0) / 1000;
    stop = true;
    await sampler;
    const d = sub(c0, c1);
    const row = {
      phase: `fanout-${name}`,
      clients: CLIENTS,
      seconds: Math.round(dt),
      sent,
      req_per_s: total(d) / dt,
      req_per_s_by_op: Object.fromEntries(Object.entries(d).map(([k, v]) => [k, v / dt])),
      req_per_s_by_source: Object.fromEntries(
        Object.entries(bySource)
          .filter(([, v]) => v !== 0)
          .map(([k, v]) => [k, v / dt]),
      ),
      sse_opens: st.opens,
      follower_readers_max: maxFollowers,
      sse_open_max: maxSse,
      samples,
      records: st.records,
      reconnects: st.reconnects,
      moved: st.moved,
      gaps: st.gaps,
    };
    console.log(`   ${JSON.stringify(row)}`);
    appendFileSync(OUT, JSON.stringify(row) + "\n");
  };
  await window("base", BASE_S, false);
  await window("sse", SSE_S, true);
}

async function main() {
  console.log(`agent-sse: ${binary}\n  profile ${profile}\n  store ${STORE}\n  output ${OUT}`);
  const sdk = await serve({ services: [sse], port: 0 });
  const nodes: Node[] = [];
  try {
    for (let i = 0; i < NODES; i++) nodes.push(await spawnNode(i));
    await waitLeaders(nodes);
    const key = await tenantKey(nodes, sdk.url);
    if (PHASES.includes("latency")) await latencyPhase(nodes, key);
    if (PHASES.includes("fanout")) await fanoutPhase(nodes, key);
  } finally {
    for (const n of nodes) n.proc.kill("SIGTERM");
    await sdk.close?.();
  }
  console.log(`\ndone — ${OUT}`);
}

await main();
