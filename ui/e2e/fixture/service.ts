// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Sample service of the console e2e. `sdk/typescript/example`
// does not deterministically produce the classes the screens need to show; this service does — each handler
// puts its invocation into exactly one class and it stays there for the whole test suite:
//
// | Handler            | Class / status                                         |
// |--------------------|--------------------------------------------------------|
// | `Shop/ok`          | completed — one `run`, logs                             |
// | `Shop/decline`     | failed terminal (500 "card declined")                   |
// | `Shop/flaky`       | paused (out of 3 attempts, `PAUSE`)                     |
// | `Shop/retrying`    | retrying — next attempt in 1 hour                       |
// | `Shop/nap`         | suspended — sleeping 1 hour                             |
// | `Shop/napLog`      | suspended — one log line then sleeps 1 hour (live log)  |
// | `Shop/parent`      | completed, with a child (`Shop/ok`)                     |
// | `Shop/waitAwk`     | suspended — waiting on an awakeable                     |
// | `Shop/traced`      | completed after one failed attempt (run `llm` fails the first time) — |
// |                    | span `plan` (tokens) nested in turn/run spans           |
// | `Shop/streamed`    | completed — stream `tokens` three records, `meta` one (created by its case) |
// | `Shop/busy`        | running ~20 s in `ctx.run` (created by its case, flow control) |
// | `Approval/run`     | suspended — workflow waiting on promise `approved`      |
// | `Review/run`       | suspended — workflow waiting on signal `approve` (created by its case) |
// | `Cart/hold`        | suspended holding the object lock; a second call queues behind it |
import {
  type Context,
  type ServiceContext,
  serve,
  service,
  TerminalError,
  workflow,
} from "../../../sdk/typescript/src/index.ts";

const HOUR = 3_600_000;

/** Invocation of `Shop/traced` that failed its first attempt (sample service process). */
const tracedOnce = new Set<string>();

const shop = service(
  "Shop",
  {
    async ok(ctx: ServiceContext, input: { amount?: number } | null) {
      ctx.log.info("reserving", { amount: input?.amount ?? 0 });
      const receipt = await ctx.run("reserve", async () => `r-${input?.amount ?? 0}`);
      return { receipt };
    },
    async decline(ctx: ServiceContext) {
      await ctx.run("charge", async () => "attempted");
      throw new TerminalError("card declined", 500);
    },
    async flaky() {
      throw new Error("payment gateway timeout");
    },
    async retrying() {
      throw new Error("upstream 503");
    },
    async nap(ctx: ServiceContext) {
      await ctx.sleep(HOUR, "nap");
      return "rested";
    },
    // Live log: one log line then sleep — the `$log` stream is open,
    // the invocation is still alive, the Log tab reads SSE.
    async napLog(ctx: ServiceContext) {
      ctx.log.info("napping", { for: "1h" });
      await ctx.sleep(HOUR, "nap");
      return "rested";
    },
    async parent(ctx: ServiceContext) {
      return await ctx.call("Shop", "ok", { amount: 7 });
    },
    async waitAwk(ctx: ServiceContext) {
      const { promise } = ctx.awakeable<string>();
      return await promise;
    },
    // Durable stream `tokens`: three records then close. Ephemeral streams live only in the
    // leader's RAM — not in `sys_stream`.
    async streamed(ctx: ServiceContext) {
      const s = ctx.stream("tokens", { durable: true });
      s.append("hello");
      s.append({ n: 2 });
      s.append("bye");
      s.close();
      // A second stream ⇒ the Streams tab has a stream picker.
      const meta = ctx.stream("meta", { durable: true });
      meta.append({ model: "demo" });
      meta.close();
      return await ctx.run("done", async () => "streamed");
    },
    // Keeps a turn running for `ms` (default 20 s) — a suspended turn does not
    // hold a concurrency slot, a running turn does (the flow-control case).
    async busy(ctx: ServiceContext, input: { ms?: number } | null) {
      await ctx.run(
        "work",
        () => new Promise<string>((r) => setTimeout(() => r("ok"), input?.ms ?? 20_000)),
      );
      return "done";
    },
    async traced(ctx: ServiceContext) {
      // First attempt: run `llm` throws a transient error ⇒ the attempt fails, waits for retry, the next
      // one succeeds (per-attempt timeline of the Trace tab).
      const id = ctx.invocationId;
      return await ctx.trace.span(
        "plan",
        async (span) => {
          const out = await ctx.run("llm", async () => {
            if (!tracedOnce.has(id)) {
              tracedOnce.add(id);
              throw new Error("llm timeout");
            }
            return "ok";
          });
          span.set({ input_tokens: 120, output_tokens: 8 });
          return out;
        },
        { model: "m-1" },
      );
    },
  },
  {
    handlerOptions: {
      flaky: {
        retryPolicy: {
          initialInterval: 10,
          maxInterval: 10,
          maxAttempts: 3,
          onMaxAttempts: "PAUSE",
        },
      },
      retrying: { retryPolicy: { initialInterval: HOUR, maxInterval: HOUR, maxAttempts: 100 } },
      traced: {
        trace: "runtime",
        retryPolicy: { initialInterval: 300, maxInterval: 300, maxAttempts: 5 },
      },
    },
  },
);

const approval = workflow("Approval", {
  async run(ctx: Context) {
    return await ctx.promise<string>("approved").get();
  },
  async approve(ctx: Context, by: string) {
    await ctx.promise<string>("approved").resolve(by);
  },
});

// Named signal: `ctx.signal` writes no command into the journal — it waits
// until the signal arrives, then returns its value.
const review = workflow("Review", {
  async run(ctx: Context) {
    const v = await ctx.signal<unknown>("approve");
    return { approved: v };
  },
});

const cart = service(
  "Cart",
  {
    async hold(ctx: Context) {
      ctx.set("items", ["sku-1"]);
      await ctx.sleep(HOUR, "hold");
      return "released";
    },
  },
  { type: "VIRTUAL_OBJECT" },
);

export async function startFixtureService() {
  return serve({ services: [shop, approval, review, cart], port: 0, hostname: "127.0.0.1" });
}
