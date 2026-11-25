// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

/**
 * The deployment under load — runs in a **separate process**, and that is a
 * condition of the measurement, not a layout preference.
 *
 * SDK handlers live on Node's event loop. If the load generator shared the
 * process, every time it was busy `JSON.parse`-ing a response would be a time the
 * handler does *not* run — and the measured number would be "how much Node can
 * do", not "how much cairn can do". Two processes, two loops; it costs one more
 * start-up and buys a number that is about what it says it is about.
 *
 * This process prints **one JSON line** on stdout once it is ready:
 *
 * ```json
 * {"sdk":"http://127.0.0.1:51234","stats":"http://127.0.0.1:51235"}
 * ```
 *
 * `stats` is a side server, not part of the SDK: it exposes the counters that
 * only the *inside* of a handler can count — namely **the number of times a
 * `ctx.run` closure actually ran**. That is the evidence for the "is it correct"
 * half of the test; the load generator measures the "how fast" half itself.
 *
 * Not run by hand: `load.ts` spawns it. See `harness/bench/README.md`.
 */

import { createServer } from "node:http";
import { type Context, type ProtocolMode, serve, service, type ServiceContext } from "../../sdk/typescript/src/index.ts";

const sdkMode: ProtocolMode =
  (process.env.LOAD_SDK_MODE as ProtocolMode | undefined) ?? "BIDI_STREAM";
/** Sleep time of the `sleep` scenario — the suspend-then-wake path goes through a timer. */
const NAP_MS = Number(process.env.LOAD_NAP_MS ?? "200");

/** Every handler gets the same input shape: a unique tag, and a sequence number. */
interface In {
  tag: string;
  n: number;
}

// ───────────────────────────────────────────── counters

/**
 * `tag → number of times the ctx.run closure ran`.
 *
 * The most important counter of the whole file. There is no `kill -9` anywhere in
 * this run, so the exactly-once promise is **absolute**: every tag must be exactly
 * 1. A 2 means the engine re-ran a side effect that was already durable — the
 * worst bug a durable-execution engine can have, and a silent one unless someone counts.
 */
const runs = new Map<string, number>();
/**
 * `tag → number of times the handler body ran`.
 *
 * Unlike `runs`: the handler body runs again on **every turn** (replay is normal,
 * not a failure). Divided by the number of invocations it gives "how many turns
 * one invocation costs" — it proves nothing, but it explains the latency numbers.
 */
const turns = new Map<string, number>();

function bump(m: Map<string, number>, tag: string): void {
  m.set(tag, (m.get(tag) ?? 0) + 1);
}

// ───────────────────────────────────────────── the service under load

const load = service("LoadService", {
  /** The shortest possible path: in, out. Measures the part that is *not* the journal. */
  async echo(_ctx: ServiceContext, input: In) {
    bump(turns, input.tag);
    return { tag: input.tag, n: input.n };
  },

  /** One durable side effect. The gap to `echo` is the price of one journal entry. */
  async run1(ctx: ServiceContext, input: In) {
    bump(turns, input.tag);
    const v = await ctx.run("side", () => {
      bump(runs, input.tag);
      return `s-${input.n}`;
    });
    return { tag: input.tag, n: input.n, v };
  },

  /**
   * Five sequential side effects. The same invocation with a journal five times
   * longer — to see the cost by *journal length* rather than by *number of invocations*.
   */
  async run5(ctx: ServiceContext, input: In) {
    bump(turns, input.tag);
    let last = "";
    for (let i = 0; i < 5; i++) {
      last = await ctx.run(`side-${i}`, () => {
        bump(runs, `${input.tag}#${i}`);
        return `s-${input.n}-${i}`;
      });
    }
    return { tag: input.tag, n: input.n, v: last };
  },

  /** Called by `caller` through `ctx.call`. */
  async callee(_ctx: ServiceContext, input: In) {
    bump(turns, `${input.tag}!callee`);
    return { tag: input.tag, n: input.n };
  },

  /** One invocation spawns another and waits for its result. */
  async caller(ctx: ServiceContext, input: In) {
    bump(turns, input.tag);
    const r = await ctx.call<In>("LoadService", "callee", input);
    return { tag: input.tag, n: r.n };
  },

  /** Suspends on a timer, then wakes. The most expensive path: the turn ends, the engine schedules, wakes again. */
  async nap(ctx: ServiceContext, input: In) {
    bump(turns, input.tag);
    await ctx.sleep(NAP_MS, "nap");
    return { tag: input.tag, n: input.n };
  },
});

/**
 * Virtual object — state, and **an exclusive lock per key**.
 *
 * `touch` is deliberately written as read-then-write. On a hot key that is exactly
 * the shape of every lost-update bug: if the partition's exclusive lock did not
 * really serialize two turns on the same key, both would read `c`, both would write
 * `c+1`, and the final number would be smaller than the number of invocations.
 * The load generator checks exactly that.
 *
 * **`touch` must declare `EXCLUSIVE` explicitly.** `protocol-v1.md` §3: a virtual
 * object handler without `ty` means *no type*, i.e. no lock — not "exclusive by
 * default". Drop that declaration and `touch` runs in parallel on the same key and
 * this test turns red immediately, as it should.
 */
const obj = service(
  "LoadObject",
  {
    async touch(ctx: Context, input: In) {
      bump(turns, input.tag);
      const c = (await ctx.get<number>("count")) ?? 0;
      ctx.set("count", c + 1);
      return { tag: input.tag, n: input.n, count: c + 1, key: ctx.key };
    },

    /** Read only — `SHARED`, so it does not contend for the lock with `touch`. */
    async read(ctx: Context) {
      return { count: (await ctx.get<number>("count")) ?? 0, key: ctx.key };
    },
  },
  { type: "VIRTUAL_OBJECT", handlerTypes: { touch: "EXCLUSIVE", read: "SHARED" } },
);

/**
 * Two virtual objects calling each other — the `xcall` scenario of `load.ts`.
 *
 * `ping` (EXCLUSIVE) calls `pong` on **the other side** with a keyed `ctx.call`,
 * then waits. `load.ts` picks keys so that every `XcallA` key lands on one
 * partition and every `XcallB` key on another, then fires `ping` at **both**
 * sides at once: A→B and B→A cross the partition boundary in opposite directions.
 * That is the shape of a suspected deadlock (the engine held across a
 * cross-partition `transport.send`).
 *
 * `pong` is `SHARED`, on purpose: if it were `EXCLUSIVE`, `ping` A(k₁) → B(k₂)
 * running in parallel with `ping` B(k₂) → A(k₁) would be a lock cycle **of the
 * application** — a real deadlock, but the handler author's bug, not the
 * engine's. This test measures the engine, so the reverse direction must not contend for any lock.
 */
function xcallSide(name: string, peer: string) {
  return service(
    name,
    {
      async ping(ctx: Context, input: In & { peerKey: string }) {
        bump(turns, input.tag);
        const r = await ctx.call<{ key: string }>(
          peer,
          "pong",
          { tag: input.tag, n: input.n },
          { key: input.peerKey },
        );
        return { tag: input.tag, n: input.n, key: ctx.key, via: r.key };
      },

      async pong(ctx: Context, input: In) {
        bump(turns, `${input.tag}!pong`);
        return { tag: input.tag, n: input.n, key: ctx.key };
      },

      /** Empty — `load.ts` calls it to learn which partition a key lands on. */
      async where(ctx: Context) {
        return { key: ctx.key };
      },
    },
    {
      type: "VIRTUAL_OBJECT",
      handlerTypes: { ping: "EXCLUSIVE", pong: "SHARED", where: "SHARED" },
    },
  );
}

const xcallA = xcallSide("XcallA", "XcallB");
const xcallB = xcallSide("XcallB", "XcallA");

// ───────────────────────────────────────────── cross-range calls

/** Parameters of the cross-range scenarios travel **in the input**, not through environment variables. */
interface TreeIn extends In {
  depth: number;
  width: number;
}
interface FanoutIn extends In {
  fanout: number;
  /**
   * Deliberately spread children: `placement` ⇒ unkeyed children with `placement: "spread"`
   * (the `unkeyed` label); `scope` ⇒ one scope per child,
   * `idem` ⇒ one idempotency key per child (a workaround that changes the
   * child's identity). Absent ⇒ unkeyed (with the `calls/prefix` gate on, colocated on the range of the
   * parent).
   */
  spread?: "placement" | "scope" | "idem";
}
interface AgentIn extends In {
  stepMs: number;
}

/**
 * Four call shapes of agent and workflow workloads. Every `ctx.call` here is
 * **unkeyed and unscoped** — except `saga`, which deliberately calls keyed virtual
 * objects — so the callee's placement is the `unkeyed` level of the derived table: spread at random today.
 */
const inter = service("InterService", {
  /**
   * One node of the call tree. A leaf runs one `ctx.run` (counted exactly-once); an
   * inner node calls `width` children **in parallel** and waits for all. Returns
   * the number of nodes of the subtree, itself included — the root must get exactly `Σ width^i, i = 0..depth`.
   */
  async tree(ctx: ServiceContext, input: TreeIn) {
    bump(turns, input.tag);
    if (input.depth <= 0) {
      await ctx.run("leaf", () => {
        bump(runs, input.tag);
        return 1;
      });
      return { tag: input.tag, n: input.n, nodes: 1 };
    }
    const kids = await ctx.all(
      Array.from({ length: input.width }, (_unused, i) =>
        ctx.call<{ nodes: number }>("InterService", "tree", {
          tag: `${input.tag}.${i}`,
          n: input.n,
          depth: input.depth - 1,
          width: input.width,
        }),
      ),
    );
    return { tag: input.tag, n: input.n, nodes: 1 + kids.reduce((a, k) => a + k.nodes, 0) };
  },

  /** The parent calls `fanout` unkeyed children at once — ETL / batch. */
  async fanout(ctx: ServiceContext, input: FanoutIn) {
    bump(turns, input.tag);
    const done = await ctx.all(
      Array.from({ length: input.fanout }, (_unused, i) =>
        ctx.call<In>(
          "InterService",
          "leaf",
          { tag: `${input.tag}.${i}`, n: input.n },
          input.spread === "scope"
            ? // The tag has a `/`, invalid in a scope (`[A-Za-z0-9_.-]`): the server
              // would drop the broken scope and the call would fall back to the unkeyed level.
              { scope: `${input.tag}.${i}`.replace(/[^A-Za-z0-9_.-]/g, "-") }
            : input.spread === "idem"
              ? { idempotencyKey: `${input.tag}.${i}` }
              : input.spread === "placement"
                ? { placement: "spread" }
                : {},
        ),
      ),
    );
    return { tag: input.tag, n: input.n, done: done.length };
  },

  /** One `ctx.run` sleeping `stepMs` (a model call), then three sequential tools — an agent workflow. */
  async agent(ctx: ServiceContext, input: AgentIn) {
    bump(turns, input.tag);
    await ctx.run("think", async () => {
      bump(runs, `${input.tag}#think`);
      await new Promise((r) => setTimeout(r, input.stepMs));
      return 1;
    });
    for (let i = 0; i < 3; i++) {
      await ctx.call<In>("InterService", "leaf", { tag: `${input.tag}#tool${i}`, n: input.n });
    }
    return { tag: input.tag, n: input.n, tools: 3 };
  },

  /** Three virtual objects with different keys, called in sequence — a saga across entities. */
  async saga(ctx: ServiceContext, input: In) {
    bump(turns, input.tag);
    const counts: number[] = [];
    for (let i = 0; i < 3; i++) {
      const key = `saga-${input.n}-${i}`;
      const r = await ctx.call<{ count: number; key: string }>(
        "SagaObject",
        "apply",
        { tag: `${input.tag}#${i}`, n: input.n },
        { key },
      );
      if (r.key !== key) throw new Error(`SagaObject returned key ${r.key}, expected ${key}`);
      counts.push(r.count);
    }
    return { tag: input.tag, n: input.n, counts };
  },

  /** Leaf: one durable side effect. */
  async leaf(ctx: ServiceContext, input: In) {
    bump(turns, input.tag);
    await ctx.run("side", () => {
      bump(runs, input.tag);
      return 1;
    });
    return { tag: input.tag, n: input.n };
  },
});

/** One step of `saga`: read-then-write on the object's state, like `LoadObject.touch`. */
const sagaObject = service(
  "SagaObject",
  {
    async apply(ctx: Context, input: In) {
      bump(turns, input.tag);
      const c = (await ctx.get<number>("count")) ?? 0;
      ctx.set("count", c + 1);
      return { count: c + 1, key: ctx.key };
    },
  },
  { type: "VIRTUAL_OBJECT", handlerTypes: { apply: "EXCLUSIVE" } },
);

// ───────────────────────────────────────────── side server: counters

/** Aggregates one tag branch (`"<scenario>/…"`) into checkable numbers. */
function summarize(prefix: string) {
  let runExecutions = 0;
  let runTags = 0;
  let runDuplicates = 0;
  const dupSamples: string[] = [];
  for (const [tag, n] of runs) {
    if (!tag.startsWith(prefix)) continue;
    runTags++;
    runExecutions += n;
    if (n > 1) {
      runDuplicates++;
      if (dupSamples.length < 10) dupSamples.push(`${tag}×${n}`);
    }
  }
  let turnExecutions = 0;
  let turnTags = 0;
  let maxTurns = 0;
  for (const [tag, n] of turns) {
    if (!tag.startsWith(prefix)) continue;
    turnTags++;
    turnExecutions += n;
    if (n > maxTurns) maxTurns = n;
  }
  return {
    runExecutions,
    runTags,
    runDuplicates,
    dupSamples,
    turnExecutions,
    turnTags,
    maxTurns,
  };
}

const stats = createServer((req, res) => {
  const u = new URL(req.url ?? "/", "http://x");
  if (u.pathname === "/reset") {
    runs.clear();
    turns.clear();
    res.writeHead(200).end("ok");
    return;
  }
  if (u.pathname === "/stats") {
    const body = JSON.stringify(summarize(u.searchParams.get("prefix") ?? ""));
    res.writeHead(200, { "content-type": "application/json" }).end(body);
    return;
  }
  res.writeHead(404).end("not found");
});

const sdk = await serve({ services: [load, obj, xcallA, xcallB, inter, sagaObject], port: 0, protocolMode: sdkMode });
await new Promise<void>((r) => stats.listen(0, "127.0.0.1", r));
const addr = stats.address();
if (typeof addr === "string" || addr === null) throw new Error("stats has no port");

process.stdout.write(
  `${JSON.stringify({ sdk: sdk.url, stats: `http://127.0.0.1:${addr.port}` })}\n`,
);

const shutdown = () => {
  stats.close();
  void sdk.close().then(() => process.exit(0));
};
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
