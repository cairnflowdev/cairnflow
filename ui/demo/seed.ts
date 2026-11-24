// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Demo data for the console: `npm run demo` (a node running at
// `CAIRN_URL`, default http://127.0.0.1:8080, with auth off or with
// `CAIRN_API_KEY`). Builds a small shop (`services.ts`) and then seeds enough for every screen
// to have something to show, then keeps steady traffic running (Usage, handler stats, schedules).
//
// - Three deployments: v0 (Notifications only) drained, no invocation pinned any more
//   ⇒ "safe to remove"; v1 drained but still has stuck invocations pinned to it; v2
//   current.
// - Fulfillment: a workflow of ≈ 14 steps, a span per phase, a log per step (Journal, Log,
//   Trace); Checkout/placeOrder and Fulfillment/run enable `trace: "runtime"`.
// - Cart ⇒ Checkout ⇒ Inventory + Payments + Notifications (a three-level tree);
//   declined card, out of stock (the error propagates up to the root), 3-D Secure waiting on an awakeable.
// - Paused (capture out of attempts), retrying (refund, bounced email), an object holding
//   its lock with a queue behind it, workflows waiting on a promise / on trial.
// - Slow turns (Reports ≈ 2–3 s, tenant threshold 1 s), schedules (one paused),
//   flow-control rules (one disabled), invocation capture, a few purged invocations
//   (Archive), tags and idempotency keys.
// - Assistant: durable streams (Streams tab) — `answer` types tokens for ≈ 4 s, `draft` stuck waiting
//   for approval with its stream still open; background traffic asks steadily so something is always running.
//
// Can be rerun on the same data: schedules/rules are overwritten, a workflow with the same key is the same one.
import { type Endpoint, serve } from "../../sdk/typescript/src/index.ts";
import { type Item, SERVICES } from "./services.ts";

const BASE = (process.env.CAIRN_URL ?? "http://127.0.0.1:8080").replace(/\/$/, "");
const AUTH: Record<string, string> = process.env.CAIRN_API_KEY
  ? { authorization: `Bearer ${process.env.CAIRN_API_KEY}` }
  : {};
/** Background traffic interval, ms; 0 ⇒ exit once seeded (services still kept alive). */
const TICK_MS = Number(process.env.DEMO_TICK_MS ?? 3000);

const USERS = ["ana", "bao", "chi", "dung", "em", "giang", "hoa", "khanh", "linh", "minh"];
const CATALOG: Item[] = [
  { sku: "sku-tee", qty: 1, price: 19 },
  { sku: "sku-mug", qty: 1, price: 12 },
  { sku: "sku-cap", qty: 1, price: 15 },
  { sku: "sku-sock", qty: 2, price: 6 },
];
const CHANNELS = ["web", "mobile", "partner"];

// ——— HTTP ———

async function http(method: string, path: string, body?: unknown, headers = {}) {
  const r = await fetch(`${BASE}${path}`, {
    method,
    headers: { "content-type": "application/json", ...AUTH, ...headers },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (r.status >= 300) throw new Error(`${method} ${path}: ${r.status} ${await r.text()}`);
  return r;
}

/** Send without waiting; returns the invocation id. */
async function send(path: string, body: unknown = null, headers: Record<string, string> = {}) {
  const r = await http("POST", path, body, headers);
  return r.headers.get("x-cairn-invocation-id") ?? "";
}

/** An error in a side step does not fail the whole seeding — print it and go on. */
async function soft(what: string, f: () => Promise<unknown>) {
  try {
    await f();
  } catch (e) {
    console.warn(`demo: skipping ${what}: ${(e as Error).message}`);
  }
}

async function status(id: string): Promise<{ status: string; lastFailure?: unknown }> {
  return (await (await http("GET", `/invocations/${id}`)).json()) as {
    status: string;
    lastFailure?: unknown;
  };
}

async function until(what: string, ok: () => Promise<boolean>, ms = 30_000) {
  const end = Date.now() + ms;
  for (;;) {
    if (await ok().catch(() => false)) return;
    if (Date.now() > end) throw new Error(`demo: timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 150));
  }
}

const reach = (id: string, want: (s: { status: string; lastFailure?: unknown }) => boolean) =>
  until(`${id}`, async () => want(await status(id)));

const pick = <T>(xs: readonly T[], i: number): T => xs[i % xs.length] as T;

// ——— Deployment ———

async function register(ep: Endpoint): Promise<string> {
  const r = await http("POST", "/deployments", { uri: ep.url });
  return ((await r.json()) as { id: string }).id;
}

async function deployments(): Promise<Endpoint[]> {
  // v0: Notifications only — soon replaced by v1, nothing pinned ⇒ removable.
  const v0 = await serve({
    services: SERVICES.filter((s) => s.name === "Notifications"),
    port: 0,
    hostname: "127.0.0.1",
  });
  await register(v0);
  for (const to of ["ops@shop.test", "ana@shop.test"]) {
    await reach(await send("/invoke/Notifications/send/send", { to, template: "welcome" }), (s) =>
      ["completed", "failed"].includes(s.status),
    );
  }

  // v1: all services; a few long-stuck invocations pinned to it.
  const v1 = await serve({ services: SERVICES, port: 0, hostname: "127.0.0.1" });
  await register(v1);
  const pinned = [
    await send("/invoke/Payments/charge/send", { order: "o-legacy-1", amount: 54, card: "3ds" }),
    await send("/invoke/Onboarding/run/send?key=legacy@shop.test", { plan: "trial" }),
  ];
  for (const id of pinned) await reach(id, (s) => s.status === "suspended");

  // v2: the current version.
  const v2 = await serve({ services: SERVICES, port: 0, hostname: "127.0.0.1" });
  await register(v2);
  return [v0, v1, v2];
}

// ——— Traffic ———

function headers(i: number): Record<string, string> {
  return { "cairnflow-tag": `channel=${pick(CHANNELS, i)}` };
}

/** One customer: adds a few items to the cart and then (usually) checks out. */
async function shopper(user: string, i: number, card = "ok", checkout = true) {
  for (let k = 0; k <= i % 3; k++) {
    await send(`/invoke/Cart/add/send?key=${user}`, pick(CATALOG, i + k), headers(i));
  }
  if (!checkout) return "";
  return await send(`/invoke/Cart/checkout/send?key=${user}`, { card }, headers(i));
}

async function seedData() {
  // Stock: one item nearly sold out so there are "out of stock" failed orders.
  for (const it of CATALOG) await send(`/invoke/Inventory/restock/send?key=${it.sku}`, 40);
  await send("/invoke/Inventory/restock/send?key=sku-limited", 1);

  // Cart ⇒ order: most complete; one card declined; one waits for 3-D Secure; a few carts
  // left open (state visible in Objects & State).
  const orders: string[] = [];
  for (let i = 0; i < USERS.length; i++) {
    const user = USERS[i] as string;
    const card = i === 2 ? "declined" : i === 5 ? "3ds" : "ok";
    orders.push(await shopper(user, i, card, i < 8));
  }
  // Out of stock: the error from Inventory propagates up to Checkout and then Cart.
  await send("/invoke/Cart/add/send?key=quang", { sku: "sku-limited", qty: 5, price: 99 });
  await send("/invoke/Cart/checkout/send?key=quang", { card: "ok" });

  // An object holding its lock: a 2-hour stock check of `sku-bag`, three holds queued behind it.
  await reach(
    await send("/invoke/Inventory/audit/send?key=sku-bag"),
    (s) => s.status === "suspended",
  );
  for (let i = 0; i < 3; i++) {
    await send("/invoke/Inventory/reserve/send?key=sku-bag", { qty: 1, order: `o-bag-${i}` });
  }

  // Payments: capture out of attempts ⇒ paused; refund/bounced email ⇒ retrying.
  for (let i = 0; i < 6; i++) {
    const card = i < 2 ? "flaky" : "ok";
    await send(
      "/invoke/Payments/capture/send",
      { charge: `ch_${i}`, card },
      {
        "Idempotency-Key": `capture-ch_${i}`,
      },
    );
  }
  for (let i = 0; i < 4; i++) {
    const bank = i < 2 ? "slow-bank" : "ok";
    await send("/invoke/Payments/refund/send", { charge: `ch_r${i}`, bank });
  }
  await send("/invoke/Notifications/send/send", { to: "lost@bounce.test", template: "receipt" });

  // Onboarding: half verified (pro ⇒ done, trial ⇒ on a 14-day trial),
  // half still waiting for the email.
  for (let i = 0; i < 6; i++) {
    const key = `${pick(USERS, i)}@shop.test`;
    await send(`/invoke/Onboarding/run/send?key=${encodeURIComponent(key)}`, {
      plan: i % 2 === 0 ? "trial" : "pro",
    });
  }
  await new Promise((r) => setTimeout(r, 500));
  for (let i = 0; i < 3; i++) {
    const key = encodeURIComponent(`${pick(USERS, i)}@shop.test`);
    await soft(`verify ${key}`, () =>
      send(`/invoke/Onboarding/verify/send?key=${key}`, `code-${i}`),
    );
  }

  // Delivery: a long workflow (Journal/Log/Trace) — done, waiting for the carrier, failed at QC.
  const shipments: [string, number, string][] = [
    ["o-1001", 3, "ghn"],
    ["o-1002", 6, "dhl"],
    ["o-1003-hold", 2, "ghn"],
    ["o-1004-damaged", 4, "ups"],
    ["o-1005-hold", 5, "dhl"],
  ];
  for (const [key, items, carrier] of shipments) {
    await send(`/invoke/Fulfillment/run/send?key=${key}`, { items, carrier }, headers(items));
  }

  // Reports: slow turns for "Slow runs"; small exports, a few purged ⇒ Archive.
  await soft("slow-run threshold", () => http("PUT", "/admin/tenants/default", { slowMs: 1000 }));
  for (let d = 1; d <= 3; d++) {
    await send("/invoke/Reports/nightly/send", {
      day: `2026-10-${String(26 + d).padStart(2, "0")}`,
    });
  }
  const exports: string[] = [];
  for (let i = 0; i < 4; i++)
    exports.push(await send("/invoke/Reports/export/send", { rows: 1000 * (i + 1) }));
  for (const id of exports.slice(0, 3)) {
    await reach(id, (s) => s.status === "completed");
    await soft(`purge ${id}`, () => http("POST", `/invocations/${id}/purge`));
  }

  // Streams: a few finished answers, one draft stuck waiting for approval (stream open).
  for (const question of ["when will my order ship?", "how do refunds work?"])
    await send("/invoke/Assistant/answer/send", { question });
  await send("/invoke/Assistant/draft/send", { topic: "holiday shipping" });

  // A list long enough (scrolling, "Load more"): quick quotes in the `checkout` scope.
  for (let i = 0; i < 120; i += 20) {
    await Promise.all(
      Array.from({ length: 20 }, (_, j) =>
        send("/scope/checkout/invoke/Checkout/quote/send", [pick(CATALOG, i + j)], headers(i + j)),
      ),
    );
  }

  // Schedules: one paused; trigger one once so there is a "last run".
  const schedules: [string, unknown][] = [
    [
      "nightly-report",
      {
        cron: "0 2 * * *",
        timeZone: "Europe/Berlin",
        target: { service: "Reports", handler: "nightly" },
        input: { day: "yesterday" },
      },
    ],
    [
      "ops-digest",
      {
        everyMs: 5 * 60_000,
        target: { service: "Notifications", handler: "send" },
        input: { to: "ops@shop.test", template: "digest" },
      },
    ],
    [
      "restock-tees",
      {
        cron: "0 * * * *",
        target: { service: "Inventory", handler: "restock", key: "sku-tee" },
        input: 20,
      },
    ],
    [
      "weekly-export",
      {
        cron: "0 9 * * 1",
        timeZone: "Europe/Berlin",
        target: { service: "Reports", handler: "export" },
        input: { rows: 50_000 },
      },
    ],
    [
      "price-heartbeat",
      { everyMs: 60_000, target: { service: "Checkout", handler: "quote" }, input: [] },
    ],
  ];
  for (const [name, spec] of schedules)
    await soft(`schedule ${name}`, () => http("PUT", `/schedules/${name}`, spec));
  await soft("pause weekly-export", () => http("POST", "/schedules/weekly-export/pause"));
  await soft("trigger nightly-report", () => http("POST", "/schedules/nightly-report/trigger"));

  // Flow control: a limit for the checkout scope, one disabled rule, a tenant limit.
  await soft("rule checkout/*", () =>
    http("POST", "/rules", {
      pattern: "checkout/*",
      concurrency: 20,
      rate: 100,
      description: "checkout traffic",
    }),
  );
  await soft("rule reports", () =>
    http("POST", "/rules", {
      pattern: "reports",
      concurrency: 1,
      description: "one report at a time",
    }),
  );
  await soft("rule partner/*", () =>
    http("POST", "/rules", {
      pattern: "partner/*",
      rate: 50,
      description: "partner API (off for now)",
      disabled: true,
    }),
  );
  await soft("tenant limit", () =>
    http("POST", "/rules", { tenant: "default", rate: 5000, burst: 10_000 }),
  );

  // Capture a failed invocation to review it in Settings.
  await soft("capture Payments/charge", () =>
    http("POST", "/admin/capture", { service: "Payments", handler: "charge", count: 3 }),
  );
  return orders;
}

/** Background traffic: customers buying steadily, occasional errors, new users signing up. */
function traffic() {
  let i = 0;
  return setInterval(() => {
    i++;
    const user = `${pick(USERS, i)}-${Math.floor(i / USERS.length)}`;
    const card = i % 17 === 0 ? "declined" : "ok";
    void soft("traffic", async () => {
      await shopper(user, i, card);
      await send("/scope/checkout/invoke/Checkout/quote/send", [pick(CATALOG, i)], headers(i));
      await send(`/invoke/Inventory/level/send?key=${pick(CATALOG, i).sku}`);
      if (i % 5 === 0) {
        const key = encodeURIComponent(`new-${i}@shop.test`);
        await send(`/invoke/Onboarding/run/send?key=${key}`, {
          plan: i % 10 === 0 ? "trial" : "pro",
        });
        if (i % 3 !== 0) {
          setTimeout(
            () =>
              void soft("verify", () => send(`/invoke/Onboarding/verify/send?key=${key}`, "ok")),
            2000,
          );
        }
      }
      if (i % 7 === 0)
        await send("/invoke/Payments/capture/send", { charge: `ch_t${i}`, card: "ok" });
      if (i % 11 === 0) await send("/invoke/Reports/export/send", { rows: 2000 });
      if (i % 2 === 0)
        await send("/invoke/Assistant/answer/send", { question: `question ${i} about ${user}` });
      if (i % 4 === 0) {
        await send(`/invoke/Fulfillment/run/send?key=o-t${i}`, {
          items: 1 + (i % 5),
          carrier: "ghn",
        });
      }
    });
  }, TICK_MS);
}

await until(`node at ${BASE}`, async () => (await fetch(`${BASE}/health`)).ok, 60_000);
const endpoints = await deployments();
await seedData();
console.log(`demo: seeded data into ${BASE}. Console: ${BASE}/ui/`);
if (TICK_MS > 0) {
  traffic();
  console.log(`demo: background traffic every ${TICK_MS} ms — Ctrl+C to stop.`);
}
// Keep the three deployments alive (stuck invocations need the SDK to continue).
process.on("SIGINT", async () => {
  await Promise.all(endpoints.map((e) => e.close()));
  process.exit(0);
});
