// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Demo services for the console: a small shop, enough for every screen to have real
// data — multi-level call trees, objects with state and lock queues, workflows waiting on
// promises, every status (done, failed, paused, retrying, stuck waiting on an awakeable/
// promise/sleep), slow turns. Behaviour is deterministic in the input (safe to replay): randomness
// lives only in the seed script (`seed.ts`), not in the handlers.
//
// | Service         | Kind     | Handler: behaviour                                            |
// |-----------------|----------|---------------------------------------------------------------|
// | `Checkout`      | service  | `placeOrder`: price → reserve each SKU → charge → send email  |
// | `Payments`      | service  | `charge`: card `declined` fails terminally, `3ds` waits on an |
// |                 |          | awakeable; `capture`: card `flaky` out of 3 attempts ⇒ paused;|
// |                 |          | `refund`: bank `slow-bank` fails ⇒ retry every 20 minutes     |
// | `Inventory`     | object   | `reserve` (out of stock ⇒ error), `restock`, `level` (shared),|
// |                 |          | `audit`: holds the lock 2 hours ⇒ later calls queue           |
// | `Cart`          | object   | `add`, `checkout` (calls `Checkout.placeOrder`)                |
// | `Onboarding`    | workflow | `run`: create the account, wait for promise `email-verified`, |
// |                 |          | 14-day trial; `verify`: resolves the promise                  |
// | `Fulfillment`   | workflow | `run`: ≈ 14 steps in 4 phases (spans), a log per step; `-hold`|
// |                 |          | waits for promise `delivered`, `-damaged` fails midway        |
// | `Reports`       | service  | `nightly`: a few slow steps (≈ 2–3 s) + an "LLM" span with    |
// |                 |          | tokens; `export`                                              |
// | `Notifications` | service  | `send`: address `@bounce.test` fails ⇒ retry every 15 minutes |
// | `Assistant`     | service  | `answer`: durable stream `answer` (token by token, ≈ 4 s) and |
// |                 |          | `sources`; `draft`: types a draft into `draft`, then waits on |
// |                 |          | an approval awakeable ⇒ stuck, stream still open (Streams tab, live) |
import {
  type Context,
  type ServiceContext,
  type ServiceDefinition,
  service,
  TerminalError,
  workflow,
} from "../../sdk/typescript/src/index.ts";

const MIN = 60_000;
const HOUR = 60 * MIN;
const DAY = 24 * HOUR;

export type Item = { sku: string; qty: number; price: number };
export type Order = { id: string; user: string; items: Item[]; card: string };

/** A simulated "external" job: really waits `ms` (to have run time, slow turns). */
const work = (ms: number) => new Promise((r) => setTimeout(r, ms));

const checkout = service(
  "Checkout",
  {
    async placeOrder(ctx: ServiceContext, o: Order) {
      ctx.log.info("placing order", { order: o.id, items: o.items.length });
      const total = await ctx.run("price", async () => {
        await work(20);
        return o.items.reduce((a, i) => a + i.qty * i.price, 0);
      });
      for (const it of o.items) {
        await ctx.call("Inventory", "reserve", { qty: it.qty, order: o.id }, { key: it.sku });
      }
      const charge = await ctx.call<{ id: string }>("Payments", "charge", {
        order: o.id,
        amount: total,
        card: o.card,
      });
      ctx.send("Notifications", "send", { to: o.user, template: "order-confirmed", order: o.id });
      return { order: o.id, total, charge: charge.id };
    },
    async quote(ctx: ServiceContext, items: Item[]) {
      return await ctx.run("price", () => items.reduce((a, i) => a + i.qty * i.price, 0));
    },
  },
  { handlerOptions: { placeOrder: { trace: "runtime" } } },
);

const payments = service(
  "Payments",
  {
    async charge(ctx: ServiceContext, p: { order: string; amount: number; card: string }) {
      await ctx.run("authorize", () => work(40 + (p.amount % 120)));
      if (p.card === "declined") throw new TerminalError("card declined by issuer", 402);
      if (p.card === "3ds") {
        // 3-D Secure: waits for the customer to confirm — resolved via `POST /awakeables/{id}/resolve`.
        const { id, promise } = ctx.awakeable<string>();
        ctx.log.info("waiting for 3-D Secure challenge", { awakeable: id });
        await promise;
      }
      return { id: await ctx.run("capture-id", () => `ch_${p.order}`), amount: p.amount };
    },
    async capture(ctx: ServiceContext, p: { charge: string; card: string }) {
      await ctx.run("capture", () => work(30));
      if (p.card === "flaky") throw new Error("payment gateway timeout");
      return { captured: p.charge };
    },
    async refund(ctx: ServiceContext, p: { charge: string; bank: string }) {
      if (p.bank === "slow-bank") throw new Error("bank API returned 503");
      return await ctx.run("refund", () => ({ refunded: p.charge }));
    },
  },
  {
    handlerOptions: {
      capture: {
        retryPolicy: {
          initialInterval: 50,
          maxInterval: 50,
          maxAttempts: 3,
          onMaxAttempts: "PAUSE",
        },
      },
      refund: {
        retryPolicy: { initialInterval: 20 * MIN, maxInterval: 20 * MIN, maxAttempts: 50 },
      },
    },
  },
);

const inventory = service(
  "Inventory",
  {
    async reserve(ctx: Context, r: { qty: number; order: string }) {
      const stock = (await ctx.get<number>("stock")) ?? 50;
      if (stock < r.qty) throw new TerminalError(`out of stock (${stock} left)`, 409);
      ctx.set("stock", stock - r.qty);
      const held = ((await ctx.get<string[]>("reserved")) ?? []).slice(-9);
      ctx.set("reserved", [...held, r.order]);
      return { left: stock - r.qty };
    },
    async restock(ctx: Context, qty: number) {
      const stock = (await ctx.get<number>("stock")) ?? 50;
      ctx.set("stock", stock + qty);
      ctx.set("restockedAt", await ctx.run("now", () => new Date().toISOString()));
      return { stock: stock + qty };
    },
    async level(ctx: Context) {
      return (await ctx.get<number>("stock")) ?? 50;
    },
    async audit(ctx: Context) {
      ctx.log.info("stock count started");
      await ctx.sleep(2 * HOUR, "count");
      return "counted";
    },
  },
  { type: "VIRTUAL_OBJECT", handlerTypes: { level: "SHARED" } },
);

const cart = service(
  "Cart",
  {
    async add(ctx: Context, item: Item) {
      const items = (await ctx.get<Item[]>("items")) ?? [];
      ctx.set("items", [...items, item]);
      ctx.set("updatedAt", await ctx.run("now", () => new Date().toISOString()));
      return items.length + 1;
    },
    async checkout(ctx: Context, p: { card: string }) {
      const items = (await ctx.get<Item[]>("items")) ?? [];
      if (items.length === 0) throw new TerminalError("cart is empty", 400);
      const id = await ctx.run("order-id", () => `o-${ctx.key}-${Date.now().toString(36)}`);
      const out = await ctx.call("Checkout", "placeOrder", {
        id,
        user: `${ctx.key}@shop.test`,
        items,
        card: p.card,
      });
      ctx.clear("items");
      return out;
    },
  },
  { type: "VIRTUAL_OBJECT" },
);

const onboarding = workflow("Onboarding", {
  async run(ctx: Context, p: { plan: "trial" | "pro" }) {
    await ctx.run("create-account", () => work(30));
    ctx.send("Notifications", "send", { to: ctx.key, template: "verify-email" });
    const code = await ctx.promise<string>("email-verified").get();
    ctx.log.info("email verified", { code });
    await ctx.run("provision", () => work(60));
    if (p.plan === "trial") await ctx.sleep(14 * DAY, "trial");
    return { user: ctx.key, plan: p.plan };
  },
  async verify(ctx: Context, code: string) {
    await ctx.promise<string>("email-verified").resolve(code);
  },
});

const reports = service("Reports", {
  async nightly(ctx: ServiceContext, p: { day: string }) {
    for (const step of ["extract", "aggregate", "render"]) {
      ctx.log.info(`${step} started`, { day: p.day });
      await ctx.run(step, () => work(700 + ((step.length * 37) % 300)));
    }
    // Summary by a fake "LLM": a span with a model and tokens (Trace tab, token column).
    const summary = await ctx.trace.span(
      "summarize",
      async (span) => {
        const text = await ctx.run("llm", async () => {
          await work(180);
          return `Sales for ${p.day} were steady.`;
        });
        span.set({ input_tokens: 1840, output_tokens: 212 });
        return text;
      },
      { model: "demo-small" },
    );
    ctx.log.info("report ready", { day: p.day, summary });
    return { url: `s3://reports/${p.day}.pdf`, summary };
  },
  async export(ctx: ServiceContext, p: { rows: number }) {
    for (let i = 0; i < Math.min(5, Math.ceil(p.rows / 1000)); i++) {
      await ctx.run(`chunk-${i}`, () => work(25));
    }
    return { rows: p.rows };
  },
});

/**
 * Delivers an order: a multi-step workflow (≈ 14 journal entries), a span per phase, a log line per
 * step — to look at the Journal, Log and Trace of a long run. An order
 * `…-hold` stops at the delivery step waiting for the carrier (promise `delivered`); an order
 * `…-damaged` fails at the inspection step (terminal error midway).
 */
const fulfillment = workflow(
  "Fulfillment",
  {
    async run(ctx: Context, o: { items: number; carrier: string }) {
      const phase = <T>(name: string, fn: () => Promise<T>) =>
        ctx.trace.span(name, fn, { order: ctx.key });
      await phase("prepare", async () => {
        ctx.log.info("validating order", { items: o.items });
        await ctx.run("validate", () => work(40));
        await ctx.run("allocate-warehouse", () => work(60));
        ctx.log.info("warehouse allocated", { warehouse: "HCM-1" });
      });
      await phase("pick-and-pack", async () => {
        for (let i = 0; i < Math.min(o.items, 4); i++) {
          await ctx.run(`pick-item-${i + 1}`, () => work(35));
          ctx.log.debug(`picked item ${i + 1}`);
        }
        await ctx.run("quality-check", async () => {
          await work(50);
          if (ctx.key.endsWith("-damaged")) throw new TerminalError("item damaged in QC", 422);
        });
        await ctx.run("pack", () => work(45));
        ctx.log.info("packed", { boxes: Math.ceil(o.items / 3) });
      });
      const label = await phase("ship", async () => {
        const l = await ctx.run("buy-label", async () => {
          await work(80);
          return `${o.carrier.toUpperCase()}-${ctx.key.slice(-6)}`;
        });
        await ctx.run("handover", () => work(30));
        ctx.log.info("handed to carrier", { carrier: o.carrier, label: l });
        return l;
      });
      ctx.send("Notifications", "send", { to: `${ctx.key}@shop.test`, template: "shipped" });
      if (ctx.key.endsWith("-hold")) {
        ctx.log.warn("waiting for delivery confirmation", { label });
        await ctx.promise<string>("delivered").get();
      }
      await phase("close", async () => {
        await ctx.run("invoice", () => work(40));
        await ctx.run("archive", () => work(20));
      });
      ctx.log.info("order fulfilled", { label });
      return { label };
    },
    async delivered(ctx: Context, by: string) {
      await ctx.promise<string>("delivered").resolve(by);
    },
  },
  { handlerOptions: { run: { trace: "runtime" } } },
);

const notifications = service(
  "Notifications",
  {
    async send(ctx: ServiceContext, msg: { to: string; template: string }) {
      if (msg.to.endsWith("@bounce.test")) throw new Error("SMTP 421: try again later");
      return await ctx.run("deliver", async () => {
        await work(15);
        return { delivered: msg.to, template: msg.template };
      });
    },
  },
  {
    handlerOptions: {
      send: { retryPolicy: { initialInterval: 15 * MIN, maxInterval: 15 * MIN, maxAttempts: 20 } },
    },
  },
);

/** Text of the fake answer — deterministic in the question (safe to replay). */
function words(question: string, n: number): string[] {
  const vocab = [
    "orders",
    "ship",
    "within",
    "two",
    "days",
    "from",
    "the",
    "nearest",
    "warehouse",
    "and",
    "refunds",
    "settle",
    "in",
    "five",
    "business",
    "days",
    "after",
    "return",
  ];
  let h = 0;
  for (const c of question) h = (h * 31 + c.charCodeAt(0)) >>> 0;
  return Array.from({ length: n }, (_, i) => vocab[(h + i * 7) % vocab.length] as string);
}

/**
 * Fake "LLM" assistant for the Streams tab: tokens pour into a durable stream
 * one by one while `ctx.run` is still running — open the Streams tab while the invocation is
 * running and the text appears gradually. `draft` finishes typing the draft and then waits for a reviewer
 * (awakeable), so the `draft` stream stays open forever.
 */
const assistant = service(
  "Assistant",
  {
    async answer(ctx: ServiceContext, p: { question: string }) {
      ctx.log.info("question received", { question: p.question });
      const sources = ctx.stream("sources", { durable: true });
      const docs = await ctx.run("retrieve", async () => {
        await work(300);
        const found = ["faq/shipping.md", "policy/refunds.md", "kb/warehouses.md"];
        for (const d of found) sources.append({ doc: d, score: 0.9 - found.indexOf(d) * 0.1 });
        sources.close();
        return found;
      });
      const out = ctx.stream("answer", { durable: true });
      const text = await ctx.trace.span(
        "generate",
        async (span) => {
          const t = await ctx.run("llm", async () => {
            const toks = words(p.question, 40);
            for (const tok of toks) {
              await work(100);
              out.append(`${tok} `);
            }
            out.close();
            return toks.join(" ");
          });
          span.set({ input_tokens: 620 + docs.length * 400, output_tokens: 40 });
          return t;
        },
        { model: "demo-small" },
      );
      ctx.log.info("answer streamed", { tokens: 40 });
      return { answer: text, sources: docs };
    },
    async draft(ctx: ServiceContext, p: { topic: string }) {
      const out = ctx.stream("draft", { durable: true });
      await ctx.run("write", async () => {
        out.append({ section: "title", text: `Notes on ${p.topic}` });
        for (const tok of words(p.topic, 24)) {
          await work(80);
          out.append(`${tok} `);
        }
      });
      const { id, promise } = ctx.awakeable<string>();
      ctx.log.info("draft ready, waiting for review", { awakeable: id });
      const verdict = await promise;
      out.append({ section: "review", text: verdict });
      out.close();
      return { topic: p.topic, verdict };
    },
  },
  { handlerOptions: { answer: { trace: "runtime" }, draft: { trace: "runtime" } } },
);

/** Every service of the current version. */
export const SERVICES: ServiceDefinition[] = [
  checkout,
  payments,
  inventory,
  cart,
  onboarding,
  fulfillment,
  reports,
  notifications,
  assistant,
];
