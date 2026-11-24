// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * `connect()` on the real binary. No hand-written `fetch` calls the data API:
 * every call goes through the client, and **every** response the client receives
 * is checked against `docs/api/openapi.json` (`checkResponse`).
 *
 * Two default shapes, each its own `cairnflow` process (auth on, the client uses
 * an `invoke` key): a `--data-dir` node, and a `memory:// --cluster --partitions 4`
 * cluster. Extra (`CAIRNFLOW_CLIENT_E2E_S3=1`, SeaweedFS at `localhost:18333`):
 * three nodes on a shared bucket, **killing the leader** in the middle of a call.
 *
 * Needs `cargo build --bin cairnflow`.
 */

import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { randomBytes } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, describe, test } from "node:test";
import { fileURLToPath } from "node:url";
import {
  AlreadyInvoked,
  AttachTimeout,
  CairnHttpError,
  type Client,
  connect,
  InvocationFailed,
} from "../src/client.ts";
import { type Context, type Endpoint, serve, service, TerminalError, workflow } from "../src/index.ts";
import { ApiSchema, checkResponse } from "./api-schema.ts";

const here = fileURLToPath(new URL(".", import.meta.url));
const binary = process.env.CAIRNFLOW_BINARY ?? join(here, "..", "..", "..", "target", "debug", "cairnflow");
const ADMIN_KEY = `cf_client_${randomBytes(16).toString("hex")}`;
const schema = new ApiSchema();

/** The client's `fetch` in the test: checks every response against the schema. */
const checkedFetch: typeof fetch = async (input, init) => {
  const r = await fetch(input, init);
  await checkResponse(schema, init?.method ?? "GET", String(input), r);
  return r;
};

// ───────────────────────────────────────────── service

/** State read by the test: the id of each run, awakeable ids by key. */
const seen = {
  runs: new Map<string, Set<string>>(),
  awakeables: new Map<string, string>(),
};

const orders = service("Orders", {
  async submit(_ctx, input: { amount: number }) {
    if (input.amount < 0) throw new TerminalError("negative amount", 422);
    return { ok: true, amount: input.amount };
  },
  async slow(ctx, ms: number) {
    await ctx.sleep(ms);
    return "late";
  },
});

const approval = service(
  "Approval",
  {
    async wait(ctx: Context) {
      const v = await ctx.race([ctx.signal<{ by: string }>("approve"), ctx.sleep(60_000)]);
      return v === undefined ? "expired" : v.by;
    },
    async awk(ctx: Context) {
      const { id, promise } = ctx.awakeable<{ ok: boolean }>();
      await ctx.run("report id", () => {
        seen.awakeables.set(ctx.key, id);
        return 1;
      });
      try {
        return await promise;
      } catch (e) {
        return { rejected: (e as Error).message };
      }
    },
  },
  { type: "VIRTUAL_OBJECT" },
);

/** Each run records the invocation id by key — counts the invocations of one logical call. */
const counter = service(
  "Counter",
  {
    async hit(ctx: Context, ms: number) {
      const ids = seen.runs.get(ctx.key) ?? new Set<string>();
      ids.add(ctx.invocationId);
      seen.runs.set(ctx.key, ids);
      if (ms > 0) await ctx.sleep(ms);
      const n = ((await ctx.get<number>("n")) ?? 0) + 1;
      ctx.set("n", n);
      return n;
    },
  },
  { type: "VIRTUAL_OBJECT" },
);

const signup = workflow("Signup", {
  async run(ctx, user: { email: string }) {
    const code = await ctx.promise<string>("email").get();
    return `${user.email}:${code}`;
  },
  async click(ctx, code: string) {
    await ctx.promise<string>("email").resolve(code);
    return "ok";
  },
});

/** The schedule's target: returns the mark the run belongs to. */
const cron = service("Cron", {
  async tick(ctx) {
    return ctx.schedule ?? null;
  },
});

const cronObject = service(
  "CronObject",
  {
    async tick(ctx: Context) {
      return { key: ctx.key, schedule: ctx.schedule ?? null };
    },
  },
  { type: "VIRTUAL_OBJECT" },
);

// ───────────────────────────────────────────── node

interface Node {
  proc: ChildProcess;
  url: string;
}

const spawned: ChildProcess[] = [];

async function startNode(args: string[], env: Record<string, string> = {}): Promise<Node> {
  const proc = spawn(binary, [...args, "--port", "0"], {
    stdio: "pipe",
    env: { ...process.env, ...env, CAIRNFLOW_ADMIN_KEY: ADMIN_KEY },
  });
  spawned.push(proc);
  proc.stderr!.resume();
  const url = await new Promise<string>((resolve, reject) => {
    let buf = "";
    const t = setTimeout(() => reject(new Error(`node did not report its port:\n${buf}`)), 30_000);
    proc.stdout!.on("data", (c: Buffer) => {
      buf += c.toString();
      const m = /http:\/\/[\d.]+:\d+/.exec(buf);
      if (m) {
        clearTimeout(t);
        resolve(m[0]);
      }
    });
    proc.on("exit", (code) => reject(new Error(`node exited early (${code}):\n${buf}`)));
  });
  return { proc, url };
}

/** Admin requests (admin key): register, create keys, read `/cluster`, `/metrics`. */
async function admin(url: string, path: string, init: RequestInit = {}): Promise<Response> {
  const headers = new Headers(init.headers);
  headers.set("authorization", `Bearer ${ADMIN_KEY}`);
  return fetch(`${url}${path}`, { ...init, headers });
}

async function until<T>(what: string, f: () => Promise<T | undefined>, ms = 30_000): Promise<T> {
  const deadline = Date.now() + ms;
  for (;;) {
    try {
      const v = await f();
      if (v !== undefined) return v;
    } catch {
      /* not yet */
    }
    if (Date.now() > deadline) throw new Error(`over ${ms} ms: ${what}`);
    await new Promise((r) => setTimeout(r, 50));
  }
}

/** Waits for a leader on every range of the cluster, registers the SDK, creates an `invoke` key. */
async function prepare(url: string, sdkUrl: string, cluster: boolean): Promise<string> {
  await until("health", async () => ((await fetch(`${url}/health`)).ok ? true : undefined));
  if (cluster) {
    await until("every range has a leader", async () => {
      const r = (await (await admin(url, "/cluster")).json()) as { partitions?: Array<{ leader?: unknown }> };
      return r.partitions?.every((p) => p.leader) ? true : undefined;
    });
  }
  await until("deployment registration", async () => {
    const r = await admin(url, "/deployments", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ uri: sdkUrl }),
    });
    return r.status === 200 ? true : undefined;
  });
  const k = await admin(url, "/admin/keys", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ tenant: "default", role: "invoke" }),
  });
  assert.equal(k.status, 200, await k.clone().text());
  return ((await k.json()) as { secret: string }).secret;
}

let sdk: Endpoint | undefined;
before(async () => {
  sdk = await serve({ services: [orders, approval, counter, signup, cron, cronObject], port: 0 });
});
after(async () => {
  for (const p of spawned) p.kill("SIGKILL");
  await sdk?.close();
});

// ───────────────────────────────────────────── the surface, two shapes

const shapes: Array<{ name: string; args: (dir: string) => string[]; cluster: boolean }> = [
  { name: "1 node", args: (dir) => ["--data-dir", dir], cluster: false },
  {
    name: "4p cluster",
    args: () => ["--main-store", "memory://", "--cluster", "--partitions", "4"],
    cluster: true,
  },
];

for (const shape of shapes) {
  describe(`client — ${shape.name}`, () => {
    let node: Node;
    let dir = "";
    let c: Client;
    let key = "";
    before(async () => {
      dir = mkdtempSync(join(tmpdir(), "cf-client-"));
      // `--invoke-wait-ms 300`: calls longer than that get a `202` — the client must
      // attach by id on its own without the caller seeing it.
      node = await startNode([...shape.args(dir), "--tick-interval-ms", "50", "--invoke-wait-ms", "300"]);
      key = await prepare(node.url, sdk!.url, shape.cluster);
      c = connect({ url: node.url, apiKey: key, fetch: checkedFetch, attachWaitMs: 2_000 });
    });
    after(() => {
      node?.proc.kill("SIGKILL");
      rmSync(dir, { recursive: true, force: true });
    });

    test("typed invoke; a 202 is not exposed to the caller; failed ⇒ TerminalError", async () => {
      const o = c.service<typeof orders>("Orders");
      assert.deepEqual(await o.submit({ amount: 5 }), { ok: true, amount: 5 });
      assert.equal(await o.slow(1_000), "late");
      await assert.rejects(o.submit({ amount: -1 }), (e: unknown) => {
        assert.ok(e instanceof InvocationFailed && e instanceof TerminalError);
        assert.equal(e.code, 422);
        return true;
      });
    });

    test("send ⇒ handle; attach, output, status by id", async () => {
      const o = c.service<typeof orders>("Orders");
      const h = await o.send.slow(800);
      assert.match(h.id, /^[0-9a-f]{48}$/);
      assert.deepEqual(await h.output(), { ready: false });
      await assert.rejects(h.attach({ timeoutMs: 100 }), (e: unknown) => e instanceof AttachTimeout);
      assert.equal(await h.attach(), "late");
      assert.deepEqual(await h.output(), { ready: true, value: "late" });
      const st = await h.status();
      assert.equal(st.status, "completed");
      assert.ok(typeof st.createdAt === "number" && typeof st.endedAt === "number");
      assert.equal(await c.invocation<string>(h.id).attach(), "late");
    });

    test("a signal with an invoke key wins the race; awakeable resolve/reject", async () => {
      const a = c.object<typeof approval>("Approval", `sig-${shape.cluster}`);
      const h = await a.send.wait();
      await until("suspended waiting for the signal", async () => ((await h.status()).status === "suspended" ? true : undefined));
      await h.signal("approve", { by: "boss" });
      assert.equal(await h.attach(), "boss");

      for (const [k, act] of [
        ["awk-ok", "resolve"],
        ["awk-no", "reject"],
      ] as const) {
        const key2 = `${k}-${shape.cluster}`;
        const w = await c.object<typeof approval>("Approval", key2).send.awk();
        const awk = await until("id awakeable", async () => seen.awakeables.get(key2));
        await until("suspended", async () => ((await w.status()).status === "suspended" ? true : undefined));
        if (act === "resolve") await c.awakeable(awk).resolve({ ok: true });
        else await c.awakeable(awk).reject("denied", 403);
        assert.deepEqual(await w.attach(), act === "resolve" ? { ok: true } : { rejected: "denied" });
      }
    });

    test("cooperative cancel with an invoke key", async () => {
      const h = await c.object<typeof approval>("Approval", `cancel-${shape.cluster}`).send.wait();
      await until("suspended", async () => ((await h.status()).status === "suspended" ? true : undefined));
      const ack = await h.cancel();
      assert.equal(ack.id, h.id);
      await assert.rejects(h.attach({ timeoutMs: 10_000 }), (e: unknown) => e instanceof TerminalError);
    });

    test("workflow: run, attach, output, promise; a second run ⇒ AlreadyInvoked", async () => {
      const key2 = `wf-${shape.cluster}`;
      const wf = c.workflow<typeof signup>("Signup", key2);
      const running = wf.run({ email: "a@b" });
      await until("the run is waiting on the promise", async () => ((await wf.output()).ready ? undefined : true));
      const p = c.promise("Signup", key2, "email");
      assert.equal((await p.peek()).completed, false);
      assert.equal(await wf.click("123"), "ok");
      assert.equal(await running, "a@b:123");
      assert.equal(await wf.attach(), "a@b:123");
      assert.deepEqual(await wf.output(), { ready: true, value: "a@b:123" });
      assert.equal((await p.peek()).completed, true);
      assert.equal((await p.resolve("456")).completed, false, "a second resolve is a no-op");
      await assert.rejects(wf.run({ email: "x@y" }), (e: unknown) => e instanceof AlreadyInvoked);
      const p2 = c.promise("Signup", `${key2}-r`, "email");
      assert.equal((await p2.reject("broken", 400)).completed, true);
      assert.ok((await p2.peek()).failure?.includes("broken"));
    });

    test("schedules: put no-op, trigger by key ⇒ the same run carries ctx.schedule, scope + VO variant, list, delete", async () => {
      const name = `nightly-${shape.cluster ? "c" : "s"}`;
      const def = { everyMs: 60_000, target: { service: "Cron", handler: "tick" }, paused: true } as const;
      const first = await c.schedules.put(name, def);
      assert.equal(first.created, true);
      assert.equal(first.nextAt, null, "paused ⇒ no next mark");
      const again = await c.schedules.put(name, def);
      assert.equal(again.changed, false, "same definition ⇒ no-op");
      const h = await c.schedules.trigger(name, { idempotencyKey: "r1" });
      const v = (await h.attach()) as { name: string; at: number };
      assert.equal(v.name, name);
      assert.ok(typeof v.at === "number" && v.at > 0);
      assert.equal((await c.schedules.trigger(name, { idempotencyKey: "r1" })).id, h.id);
      const got = await c.schedules.get(name);
      assert.equal(got?.fired, 1);
      assert.equal(got?.upcoming?.length, 10);

      const scoped = await c.schedules.put(
        `vo-${name}`,
        { everyMs: 60_000, target: { service: "CronObject", handler: "tick", key: "k1" }, paused: true },
        { scope: "acme" },
      );
      assert.equal(scoped.overlap, "allow");
      const o = await c.schedules.trigger(`vo-${name}`, { scope: "acme" });
      const ov = (await o.attach()) as { key: string; schedule: { name: string } };
      assert.equal(ov.key, "k1");
      assert.equal(ov.schedule.name, `vo-${name}`);
      const l = await c.schedules.list();
      assert.ok(l.schedules.some((x) => x.name === name));
      assert.equal(l.partial, false);
      assert.ok((await c.schedules.list({ scope: "acme" })).schedules.every((x) => x.scope === "acme"));
      await assert.rejects(
        c.schedules.put("bad", { everyMs: 5, target: { service: "Cron", handler: "tick" } }),
        (e: unknown) => e instanceof CairnHttpError && e.status === 400,
      );
      assert.equal(await c.schedules.delete(name), true);
      assert.equal(await c.schedules.get(name), undefined);
    });

    test("a lost /invoke response ⇒ the client resends with the same key, one invocation", async () => {
      // `fetch` drops the **first** `/invoke` response: the request reached the node
      // and the invocation is durable, the client only sees a network error and resends.
      let dropped = 0;
      const lossy: typeof fetch = async (input, init) => {
        const r = await checkedFetch(input, init);
        if (dropped === 0 && String(input).includes("/invoke/Counter/hit")) {
          dropped++;
          await r.body?.cancel();
          throw new TypeError("fetch failed (fake: response lost)");
        }
        return r;
      };
      const lc = connect({ url: node.url, apiKey: key, fetch: lossy, attachWaitMs: 2_000 });
      const k = `cg1-${shape.cluster}`;
      assert.equal(await lc.object<typeof counter>("Counter", k).hit(200), 1);
      assert.equal(dropped, 1);
      assert.equal(seen.runs.get(k)?.size, 1, `a lost response created ${seen.runs.get(k)?.size} invocations`);
      // With `idempotencyKey: null` the same situation creates a second call — the check
      // above has teeth.
      dropped = 0;
      const k2 = `cg1-null-${shape.cluster}`;
      const nc = connect({ url: node.url, apiKey: key, fetch: lossy });
      await nc.object<typeof counter>("Counter", k2).hit(0, { idempotencyKey: null });
      assert.equal(seen.runs.get(k2)?.size, 2);
    });
  });
}

// ───────────────────────────────────────────── the cost of one waiter

/**
 * A client waiting on one long invocation costs ≤ 150 GETs/hour (the read fence at
 * the leader), 0 PUTs. Measured over `CAIRNFLOW_CLIENT_E2E_ATTACH_S` seconds (default
 * 30; `300` is the 5-minute measurement) with the default `attachWaitMs` (25 s).
 */
test("long attach: read-fence GETs ≤ 150 per hour, no WAL PUT", async () => {
  const windowS = Number(process.env.CAIRNFLOW_CLIENT_E2E_ATTACH_S ?? "30");
  const dir = mkdtempSync(join(tmpdir(), "cf-client-att-"));
  const node = await startNode(["--main-store", "memory://", "--cluster", "--partitions", "4"]);
  try {
    const key = await prepare(node.url, sdk!.url, true);
    const c = connect({ url: node.url, apiKey: key, fetch: checkedFetch });
    const h = await c.object<typeof approval>("Approval", "long").send.wait();
    await until("suspended", async () => ((await h.status()).status === "suspended" ? true : undefined));
    const metric = async () => {
      const text = await (await admin(node.url, "/metrics")).text();
      const sum = (prefix: string) =>
        text
          .split("\n")
          .filter((l) => l.startsWith(prefix))
          .reduce((a, l) => a + Number(l.slice(l.lastIndexOf(" ") + 1)), 0);
      return { fence: sum("cairnflow_read_fence_total"), commits: sum("cairnflow_commit_wait_seconds_count") };
    };
    const m0 = await metric();
    await assert.rejects(h.attach({ timeoutMs: windowS * 1000 }), (e: unknown) => e instanceof AttachTimeout);
    const m1 = await metric();
    const fences = m1.fence - m0.fence;
    // Reading `status` before the window is already counted in m0; within the window there is only attach.
    const bound = Math.ceil((150 * windowS) / 3600) + 1;
    assert.ok(fences <= bound, `${fences} read-fence GETs in ${windowS} s (cap ${bound})`);
    assert.equal(m1.commits - m0.commits, 0, "attach must not commit anything");
    await h.signal("approve", { by: "x" });
    assert.equal(await h.attach(), "x");
  } finally {
    node.proc.kill("SIGKILL");
    rmSync(dir, { recursive: true, force: true });
  }
});

// ───────────────────────────────────────────── killing the leader (S3, opt-in)

/**
 * Three nodes on a shared SeaweedFS bucket, the client points at node A.
 *
 * 1. The client calls `Counter/hit` (sleeps 1.5 s) for a key whose range A does
 *    **not** hold: A returns `503` + `leader`, the transport follows to leader L;
 * 2. the SDK sees the first run ⇒ the test kills L with `-9` while the client is waiting;
 * 3. the client loses its connection to L, goes back to A, A points at the new leader
 *    (after the lease TTL); the resent call carries the **same** idempotency key.
 *
 * Must see: the call returns its result; that key has exactly **one** invocation id.
 */
test("killing the leader mid-call: the client follows the new leader, one invocation", { skip: process.env.CAIRNFLOW_CLIENT_E2E_S3 !== "1" && "CAIRNFLOW_CLIENT_E2E_S3=1 (SeaweedFS localhost:18333)" }, async () => {
  const env = {
    AWS_ENDPOINT: process.env.AWS_ENDPOINT ?? "http://localhost:18333",
    AWS_ACCESS_KEY_ID: process.env.AWS_ACCESS_KEY_ID ?? "dev-only-access-key",
    AWS_SECRET_ACCESS_KEY: process.env.AWS_SECRET_ACCESS_KEY ?? "dev-only-not-a-secret",
    AWS_REGION: process.env.AWS_REGION ?? "us-east-1",
    AWS_ALLOW_HTTP: "true",
    AWS_VIRTUAL_HOSTED_STYLE_REQUEST: "false",
  };
  const base = process.env.CAIRNFLOW_CLIENT_E2E_S3_URI ?? "s3://walbench";
  const store = `${base}/client-${Date.now()}-${randomBytes(3).toString("hex")}`;
  const args = (id: string) => [
    "--main-store", store, "--cluster", "--partitions", "4", "--tick-interval-ms", "50",
    "--lease-ttl-ms", "1500", "--node-id", id,
  ];
  const nodes = [await startNode(args("a"), env), await startNode(args("b"), env), await startNode(args("c"), env)];
  try {
    const a = nodes[0]!;
    const key = await prepare(a.url, sdk!.url, true);
    await until("every node knows the deployment", async () => {
      for (const n of nodes) {
        const r = await admin(n.url, "/deployments");
        if (((await r.json()) as unknown[]).length === 0) return undefined;
      }
      return true;
    });
    const c = connect({ url: a.url, apiKey: key, fetch: checkedFetch, maxElapsedMs: 60_000, attachWaitMs: 2_000 });
    let killed = 0;
    for (let i = 0; i < 20 && killed === 0; i++) {
      const k = `kill-${i}`;
      const call = c.object<typeof counter>("Counter", k).hit(1_500);
      const id = await until("the first run", async () => [...(seen.runs.get(k) ?? [])][0]);
      const ranges = (await (await admin(a.url, "/cluster")).json()) as {
        ranges: Array<{ start: string; end: string; leader?: string | null }>;
      };
      const pk = id.slice(0, 16);
      // `leader` is a node id (`a`, `b`, `c` — the order of `nodes`).
      const leader = ranges.ranges.find((r) => r.start <= pk && pk <= r.end)?.leader ?? "";
      const victim = { b: nodes[1], c: nodes[2] }[leader as "b" | "c"];
      if (victim !== undefined) {
        victim.proc.kill("SIGKILL");
        killed++;
      }
      assert.equal(await call, 1, `key ${k}`);
      assert.equal(seen.runs.get(k)?.size, 1, `key ${k}: ${seen.runs.get(k)?.size} invocations for one call`);
    }
    assert.equal(killed, 1, "found no key whose leader is not node A");
  } finally {
    for (const n of nodes) n.proc.kill("SIGKILL");
  }
});
