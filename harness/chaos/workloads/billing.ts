// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

/**
 * The `billing` workload for `chaos.ts --workload billing`.
 *
 * A condensed **scenario** of a sample billing service (run against cairn the way a
 * real user would) — only the scenario is reproduced, not its code. Why it exists
 * here: a whole series of lifecycle bugs only showed up in that billing scenario after
 * 60-minute runs, because the load of `chaos.ts` (one `ctx.run` + one `ctx.call`) has
 * no retries, `maxAttempts`, rules, hot objects or long-suspended workflows.
 *
 * ## Scenario
 *
 * `BILLING_MERCHANTS` merchants (`m0`, `m1`, …), each with `BILLING_SUBS` ordinary customers plus
 * one **hot** customer (`<m>:hot`). `m0` calls through the scope `/scope/m0/…` (carrying a
 * `concurrency` limit, the whole merchant on one range); the other merchants use no scope, their
 * keys spread across ranges so that auto-split has something to split.
 *
 * - **`Customer`** (virtual object, key `<m>:<c>`, every state-writing handler declares
 *   `EXCLUSIVE` explicitly — a missing `ty` means no VO lock): `recordUsage` per event
 *   (sent to `/send` with `Idempotency-Key` = the event id, resent with the same key until
 *   acked); the hot customer receives `BILLING_HOT_SHARE` of the events, sent in **batches** of
 *   `BILLING_HOT_BATCH` through `recordUsageBatch` (idempotency key per batch), each batch
 *   validated against an outside system with a `ctx.run` between reading and writing state.
 *   `closePeriod(p)` returns the usage of period `p` and opens the next one; calling again for the
 *   same period returns the old number. `usage` (shared) reads the total.
 * - **`Subscription`** (virtual object, same key): `start` schedules `cycle(0)`;
 *   `cycle(p)` calls `Customer.closePeriod(p)`, sends the `Invoice` workflow, then **sends itself
 *   with a delay** `cycle(p + 1)` after `BILLING_PERIOD_MS` (idempotency key per
 *   period). `cancel` closes the open period and stops the cycle; `suspend` (called by `Invoice`
 *   when payment cannot be collected) behaves like `cancel`.
 * - **`Invoice`** (workflow, key `<m>:<c>:<p>`): writes the ledger (`ctx.run`), then
 *   up to 4 charge attempts, each a `ctx.run` calling the **fake gateway** with the
 *   idempotency key `<invoice>:<k>`:
 *   - `succeeded` ⇒ record `paid`;
 *   - `requires_action` (3DS) ⇒ `ctx.race([ctx.promise("3ds-k").get(),
 *     ctx.sleep(1 day)])`; the gateway completes 3DS after 0–1.5 days and resolves the
 *     promise over HTTP (20% sent twice); on expiry ⇒ cancel the charge at the gateway before the
 *     next attempt (if already charged, it counts as paid);
 *   - `declined` ⇒ dunning `ctx.sleep` 1 → 3 → 7 days; after 4 attempts ⇒
 *     `uncollectible`, and `Subscription.suspend` is sent.
 *
 *   Handler contract: exponential retry `BILLING_RETRY_INITIAL_MS` → `…_MAX_MS`,
 *   `maxAttempts` `BILLING_MAX_ATTEMPTS`, `onMaxAttempts: PAUSE`.
 * - **Fake gateway** (inside the chaos process, no sqlite): dedupes by key like Stripe, records
 *   **every** raw call (key, invocation, `ctx.attempt`, result). Built-in faults: 500 (no charge),
 *   timeout **after charging**, `requires_action`, `declined`, and an **outage window** of
 *   `BILLING_OUTAGE_S` seconds every `BILLING_OUTAGE_EVERY_S` (503 for every call) — long enough
 *   for a few invoices to really run out of `maxAttempts`.
 * - **Rules:** a `rate` limit of `BILLING_RATE` CU/s for the tenant (`default`, chaos runs
 *   `--no-auth`) and a `concurrency` limit of `BILLING_CONCURRENCY` for the scope of the
 *   first merchant — background turns really do get throttled and wait for slots.
 * - Compressed time: `BILLING_DAY_MS` (1 000) per "day", a period of `BILLING_PERIOD_MS`
 *   (20 000).
 *
 * ## Correctness verdict (at the end of the run, after draining)
 *
 * - **I1** no double effective charge: each invoice has ≤ 1 `succeeded` charge at the gateway.
 * - **I2** no missing invoice: each subscription has invoices for exactly the periods
 *   `0 … last period` (the period `cancel`/`suspend` closed).
 * - **I3** the ledger matches the gateway: `paid` invoice ↔ `succeeded` charge, same amount —
 *   except invoices whose invocation is `paused` (charged at the gateway but not yet recorded is
 *   valid when the attempts ran out; counted separately).
 * - **I4** correct usage: the `Customer` total = the total of acked events (plus at most
 *   the part that could not be acked), and = the total usage on the invoices — a lost state
 *   update makes these numbers diverge.
 * - **I5** correct price; **I6** no silent hang: an invoice still `open` must have its invocation
 *   `paused`.
 *
 * The liveness checks (draining, real attempts, queues, deadlocks) live in
 * `chaos.ts`/`history.ts`; this workload supplies the data for (b): every turn the SDK
 * received for `Invoice.run` (`turns`).
 */

import { type Context, type ServiceDefinition, service, workflow } from "../../../sdk/typescript/src/index.ts";
import type { TurnSeen } from "../history.ts";

const env = process.env;
const num = (name: string, def: number) => Number(env[name] ?? String(def));

export const BILLING = {
  merchants: num("BILLING_MERCHANTS", 3),
  subs: num("BILLING_SUBS", 30),
  dayMs: num("BILLING_DAY_MS", 1_000),
  periodMs: num("BILLING_PERIOD_MS", 20_000),
  eventsPerS: num("BILLING_EVENTS_PER_S", 40),
  hotShare: num("BILLING_HOT_SHARE", 0.3),
  hotBatch: num("BILLING_HOT_BATCH", 10),
  /** Maximum concurrent event sends. */
  inflight: num("BILLING_INFLIGHT", 64),
  fail500: num("BILLING_FAIL_500", 0.1),
  timeoutAfterCharge: num("BILLING_TIMEOUT_AFTER_CHARGE", 0.05),
  requiresAction: num("BILLING_REQUIRES_ACTION", 0.05),
  declined: num("BILLING_DECLINED", 0.08),
  outageEveryS: num("BILLING_OUTAGE_EVERY_S", 90),
  outageS: num("BILLING_OUTAGE_S", 20),
  retryInitialMs: num("BILLING_RETRY_INITIAL_MS", 250),
  retryMaxMs: num("BILLING_RETRY_MAX_MS", 4_000),
  maxAttempts: num("BILLING_MAX_ATTEMPTS", 8),
  /** The tenant's `rate` limit, CU/s (`0` = not set). */
  rate: num("BILLING_RATE", 600),
  /** The `concurrency` limit of the first merchant's scope (`0` = not set). */
  concurrency: num("BILLING_CONCURRENCY", 4),
  /** The tenant carrying the `rate` limit — chaos runs `--no-auth`, so all work belongs to `default`. */
  tenant: env.BILLING_TENANT ?? "default",
};

/**
 * Ingress path prefix of merchant `m`. Only the first merchant (`m0`) is a scope
 * — it carries the `concurrency` limit. A scope is **one** owner partition key, so
 * the whole merchant sits on one range that auto-split cannot split ("hot owner", a matter
 * for flow control); the other merchants call without a scope, their keys spread across ranges.
 */
const route = (m: string) => (m === "m0" ? "/scope/m0" : "");

/** Maximum charge attempts (dunning) and the sleep after attempt k (days). */
const CHARGE_TRIES = 4;
const DUNNING_DAYS = [1, 3, 7];

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
/** `invokeSync` ran out of time; `ids` = the invocation ids received (durable). */
class StuckCall extends Error {
  readonly ids: string[];
  constructor(message: string, ids: string[]) {
    super(message);
    this.ids = ids;
  }
}
const terminal = (status: string | undefined) => status === "completed" || status === "failed" || status === "paused";

// ─────────────────────────────────────────────── domain: prices, keys

type Plan = "basic" | "pro";
const PRICES: Record<Plan, { fee: number; free: number; overage: number }> = {
  basic: { fee: 1_000, free: 100, overage: 2 },
  pro: { fee: 5_000, free: 1_000, overage: 1 },
};
export const priceOf = (plan: Plan, apiCalls: number) =>
  PRICES[plan].fee + Math.max(0, apiCalls - PRICES[plan].free) * PRICES[plan].overage;

const merchantOf = (key: string) => key.slice(0, key.indexOf(":"));

// ─────────────────────────────────────────────── ledger and fake gateway

type InvoiceRow = {
  invoiceId: string;
  merchant: string;
  sub: string;
  period: number;
  plan: Plan;
  apiCalls: number;
  amount: number;
  status: "open" | "paid" | "uncollectible";
  chargeKey?: string;
};

type ChargeStatus = "succeeded" | "declined" | "requires_action" | "canceled" | "failed_3ds";
type Charge = { key: string; invoiceId: string; amount: number; status: ChargeStatus };
type RawCall = {
  key: string;
  invoiceId: string;
  invocation: string;
  attempt: number;
  t: number;
  result: ChargeStatus | "500" | "503" | "timeout";
};

/** User-side state: ledger, gateway, turn log. Lives in the chaos process. */
class World {
  readonly invoices = new Map<string, InvoiceRow>();
  readonly charges = new Map<string, Charge>();
  readonly rawCalls: RawCall[] = [];
  readonly turns: TurnSeen[] = [];
  /** Invocation id → `Invoice` key (from the turns the SDK saw). */
  readonly invoiceIds = new Map<string, string>();
  outage = false;
  outages = 0;
  threeDs = { started: 0, succeeded: 0, failed: 0, webhooks: 0, webhookDup: 0, webhookFailed: 0 };
  /** Sends a 3DS promise resolve — the harness plugs it in once the cluster exists. */
  resolve3ds: (scope: string, invoiceId: string, attempt: number, ok: boolean) => Promise<boolean> = async () => false;

  ledgerOpen(row: InvoiceRow) {
    if (!this.invoices.has(row.invoiceId)) this.invoices.set(row.invoiceId, { ...row });
  }

  ledgerSettle(invoiceId: string, status: "paid" | "uncollectible", chargeKey?: string) {
    const row = this.invoices.get(invoiceId);
    if (!row || row.status !== "open") return;
    row.status = status;
    if (chargeKey !== undefined) row.chargeKey = chargeKey;
  }

  /**
   * `POST /charges` of the fake gateway. Throws when the gateway is down (503), on a 500 (no
   * charge) and on a timeout after charging — `ctx.run` retries per the policy.
   */
  async charge(ctx: Context, key: string, invoiceId: string, amount: number): Promise<{ status: ChargeStatus }> {
    const call = { key, invoiceId, invocation: ctx.invocationId, attempt: ctx.attempt, t: Date.now() };
    if (this.outage) {
      this.rawCalls.push({ ...call, result: "503" });
      throw new Error("payment gateway 503 (outage)");
    }
    const known = this.charges.get(key);
    if (known) {
      this.rawCalls.push({ ...call, result: known.status });
      return { status: known.status };
    }
    const r = Math.random();
    const f = BILLING;
    if (r < f.fail500) {
      this.rawCalls.push({ ...call, result: "500" });
      throw new Error("payment gateway 500");
    }
    if (r < f.fail500 + f.timeoutAfterCharge) {
      // The hardest fault: the money moved, the client does not know.
      this.charges.set(key, { key, invoiceId, amount, status: "succeeded" });
      this.rawCalls.push({ ...call, result: "timeout" });
      await sleep(1_500);
      throw new Error("payment gateway timeout (charged)");
    }
    let status: ChargeStatus = "succeeded";
    if (r < f.fail500 + f.timeoutAfterCharge + f.requiresAction) status = "requires_action";
    else if (r < f.fail500 + f.timeoutAfterCharge + f.requiresAction + f.declined) status = "declined";
    this.charges.set(key, { key, invoiceId, amount, status });
    this.rawCalls.push({ ...call, result: status });
    if (status === "requires_action") this.schedule3ds(key, invoiceId);
    return { status };
  }

  /** Cancels a charge waiting on 3DS; if already charged, returns `succeeded` (the client treats it as paid). */
  cancel(key: string): { status: ChargeStatus } {
    const c = this.charges.get(key);
    if (!c) return { status: "canceled" };
    if (c.status === "requires_action") c.status = "canceled";
    return { status: c.status };
  }

  /** Completes 3DS after 0–1.5 days: 80% success; 20% duplicate webhooks, retried until 2xx. */
  private schedule3ds(key: string, invoiceId: string) {
    this.threeDs.started++;
    const attempt = Number(key.slice(key.lastIndexOf(":") + 1));
    const delay = Math.random() * 1.5 * BILLING.dayMs;
    setTimeout(() => {
      const c = this.charges.get(key);
      if (!c || c.status !== "requires_action") return;
      const ok = Math.random() < 0.8;
      c.status = ok ? "succeeded" : "failed_3ds";
      if (ok) this.threeDs.succeeded++;
      else this.threeDs.failed++;
      const deliveries = Math.random() < 0.2 ? 2 : 1;
      if (deliveries === 2) this.threeDs.webhookDup++;
      for (let d = 0; d < deliveries; d++) {
        void (async () => {
          await sleep(d * Math.random() * 2_000);
          const deadline = Date.now() + 120_000;
          while (Date.now() < deadline) {
            if (await this.resolve3ds(merchantOf(invoiceId), invoiceId, attempt, ok).catch(() => false)) {
              this.threeDs.webhooks++;
              return;
            }
            await sleep(500);
          }
          this.threeDs.webhookFailed++;
        })();
      }
    }, delay);
  }
}

// ─────────────────────────────────────────────── service

type CustomerState = { period: number; apiCalls: number };
type Closed = { period: number; apiCalls: number };
type UsageEvent = { eventId: string; qty: number };
type SubState = { status: "active" | "canceled" | "suspended"; period: number; plan: Plan };

function services(w: World): ServiceDefinition[] {
  /**
   * Adds `events` to the open period. `validate`: validates the batch against an outside system
   * with a `ctx.run` **between** reading and writing state — the turn commits incrementally (read
   * state + `Run`, wait for the durable ack, then write), so a `kill -9` exactly in that gap leaves
   * a `Running` turn that has read state: exactly the shape of a lost-update bug (a turn that
   * became leader and released its VO lock let a later call write state, then the old turn replayed
   * and overwrote it).
   */
  async function record(ctx: Context, events: UsageEvent[], validate = false) {
    const cur = (await ctx.get<CustomerState>("cur")) ?? { period: 0, apiCalls: 0 };
    const total = (await ctx.get<number>("total")) ?? 0;
    const add = events.reduce((a, e) => a + e.qty, 0);
    if (validate) await ctx.run("validate", () => ({ events: events.length, add }));
    ctx.set("cur", { period: cur.period, apiCalls: cur.apiCalls + add });
    ctx.set("total", total + add);
  }

  const customer = service(
    "Customer",
    {
      async recordUsage(ctx: Context, e: UsageEvent) {
        await record(ctx, [e]);
      },
      async recordUsageBatch(ctx: Context, b: { batchId: string; events: UsageEvent[] }) {
        await record(ctx, b.events, true);
      },
      closePeriod,
      async usage(ctx: Context) {
        return {
          cur: (await ctx.get<CustomerState>("cur")) ?? { period: 0, apiCalls: 0 },
          total: (await ctx.get<number>("total")) ?? 0,
        };
      },
    },
    // **Declared explicitly.** A virtual-object handler that does not declare `ty` is "untyped" —
    // no VO lock (`docs/protocol-v1.md`, manifest), so two state-writing turns run
    // in parallel and lose updates. Restate defaults to EXCLUSIVE; cairn does not.
    {
      type: "VIRTUAL_OBJECT",
      handlerTypes: { recordUsage: "EXCLUSIVE", recordUsageBatch: "EXCLUSIVE", closePeriod: "EXCLUSIVE", usage: "SHARED" },
    },
  );

  /** Closes period `period`: returns its usage and opens the next one. Calling again for the same period returns the old number. */
  async function closePeriod(ctx: Context, period: number): Promise<number> {
    const cur = (await ctx.get<CustomerState>("cur")) ?? { period: 0, apiCalls: 0 };
    const last = await ctx.get<Closed>("closed");
    if (last && last.period === period) return last.apiCalls;
    if (cur.period !== period) {
      // The object is the clock of truth: closing a period other than the open one is
      // the caller's mistake (or state was overwritten).
      throw new Error(`closePeriod(${period}) but the open period is ${cur.period}`);
    }
    ctx.set("closed", { period, apiCalls: cur.apiCalls });
    ctx.set("cur", { period: period + 1, apiCalls: 0 });
    return cur.apiCalls;
  }

  /** Closes the open period and sends its invoice. */
  async function closeAndBill(ctx: Context, st: SubState) {
    const apiCalls = await ctx.call<number>("Customer", "closePeriod", st.period, { key: ctx.key });
    ctx.send(
      "Invoice",
      "run",
      { sub: ctx.key, period: st.period, plan: st.plan, apiCalls },
      { key: `${ctx.key}:${st.period}` },
    );
  }

  const subscription = service(
    "Subscription",
    {
      start,
      cycle,
      cancel: (ctx: Context) => stop(ctx, "canceled"),
      suspend: (ctx: Context) => stop(ctx, "suspended"),
    },
    {
      type: "VIRTUAL_OBJECT",
      handlerTypes: { start: "EXCLUSIVE", cycle: "EXCLUSIVE", cancel: "EXCLUSIVE", suspend: "EXCLUSIVE" },
    },
  );

  async function start(ctx: Context, input: { plan: Plan; offsetMs: number }): Promise<string> {
    if (await ctx.get<SubState>("sub")) return "already running";
    ctx.set("sub", { status: "active", period: 0, plan: input.plan } satisfies SubState);
    ctx.send("Subscription", "cycle", 0, {
      key: ctx.key,
      delay: BILLING.periodMs + input.offsetMs,
      idempotencyKey: `${ctx.key}:0`,
    });
    return "started";
  }

  async function cycle(ctx: Context, period: number): Promise<string> {
    const st = await ctx.get<SubState>("sub");
    // The second dedupe layer: a delayed send firing late after `cancel`, or an old period.
    if (!st || st.status !== "active" || st.period !== period) return "skipped";
    await closeAndBill(ctx, st);
    ctx.set("sub", { ...st, period: period + 1 });
    ctx.send("Subscription", "cycle", period + 1, {
      key: ctx.key,
      delay: BILLING.periodMs,
      idempotencyKey: `${ctx.key}:${period + 1}`,
    });
    return "period closed";
  }

  /** `cancel`/`suspend`: closes the open period (with an invoice), stops the cycle. Returns the next open period. */
  async function stop(ctx: Context, why: "canceled" | "suspended"): Promise<SubState> {
    const st = await ctx.get<SubState>("sub");
    if (!st) throw new Error("subscription not started");
    if (st.status !== "active") return st;
    await closeAndBill(ctx, st);
    const next: SubState = { ...st, status: why, period: st.period + 1 };
    ctx.set("sub", next);
    return next;
  }

  const invoice = workflow(
    "Invoice",
    {
      async run(ctx: Context, input: { sub: string; period: number; plan: Plan; apiCalls: number }) {
        // Data for check (b): every turn the SDK really receives.
        w.turns.push({ id: ctx.invocationId, attempt: ctx.attempt, t: Date.now() });
        w.invoiceIds.set(ctx.invocationId, ctx.key);
        const invoiceId = ctx.key;
        const amount = priceOf(input.plan, input.apiCalls);
        await ctx.run("open", () => {
          w.ledgerOpen({
            invoiceId,
            merchant: merchantOf(invoiceId),
            sub: input.sub,
            period: input.period,
            plan: input.plan,
            apiCalls: input.apiCalls,
            amount,
            status: "open",
          });
          return true;
        });
        for (let k = 1; k <= CHARGE_TRIES; k++) {
          const key = `${invoiceId}:${k}`;
          const r = await ctx.run(`charge-${k}`, () => w.charge(ctx, key, invoiceId, amount));
          let paid = r.status === "succeeded";
          if (r.status === "requires_action") {
            const won = await ctx.race([
              ctx.promise<{ ok: boolean }>(`3ds-${k}`).get(),
              ctx.sleep(BILLING.dayMs, `3ds-timeout-${k}`),
            ]);
            if (won) paid = won.ok;
            else {
              // Expired: cancel at the gateway **before** the next attempt — a late 3DS completion
              // plus a new attempt would charge twice.
              const c = await ctx.run(`cancel-${k}`, () => w.cancel(key));
              paid = c.status === "succeeded";
            }
          }
          if (paid) {
            await ctx.run("paid", () => {
              w.ledgerSettle(invoiceId, "paid", key);
              return true;
            });
            return { paid: true, tries: k };
          }
          if (k < CHARGE_TRIES) await ctx.sleep(DUNNING_DAYS[k - 1]! * BILLING.dayMs, `dunning-${k}`);
        }
        await ctx.run("uncollectible", () => {
          w.ledgerSettle(invoiceId, "uncollectible");
          return true;
        });
        ctx.send("Subscription", "suspend", null, { key: input.sub });
        return { paid: false, tries: CHARGE_TRIES };
      },
    },
    {
      options: {
        inactivityTimeout: 30_000,
        retryPolicy: {
          initialInterval: BILLING.retryInitialMs,
          maxInterval: BILLING.retryMaxMs,
          exponentiationFactor: 2,
          maxAttempts: BILLING.maxAttempts,
          onMaxAttempts: "PAUSE",
        },
      },
    },
  );

  return [customer, subscription, invoice];
}

// ─────────────────────────────────────────────── harness

/** What `chaos.ts` provides to the workload. */
export interface Harness {
  /** A cluster-aware fetch (follows the `leader` of a 503, backs off when it has no route). */
  cfetch(path: string, init?: RequestInit, timeoutMs?: number): Promise<Response>;
  log(line: string): void;
  /**
   * Snapshots the cluster state when a synchronous read does not finish (I4): `/invocations/{id}`
   * asked of each node directly, `/scheduler`, `/vqueues`, `/limits`, `/cluster`. Returns a
   * one-line summary to print.
   */
  diagnose?(what: string, ids: string[], trail: string[]): Promise<string>;
  /**
   * `GET /invocations/{id}` that **only accepts the leader's copy**: attempt number `attempt` asks
   * the `attempt`-th live node (round-robin), following the `leader` of a `503`. `undefined` = this
   * attempt could not read the leader's copy.
   */
  readLeader?(id: string, attempt: number): Promise<InvokeBody | undefined>;
}

export type InvokeBody = { id?: string; status?: string; output?: string; code?: string | number; message?: string; retry_after_ms?: number };

export type BillingVerdict = {
  /** Invariant → violation lines. */
  problems: Map<string, string[]>;
  /** Report lines (printed verbatim). */
  lines: string[];
};

export class BillingWorkload {
  readonly name = "billing";
  private readonly w = new World();
  readonly services: ServiceDefinition[] = services(this.w);
  private h!: Harness;
  private readonly merchants = Array.from({ length: BILLING.merchants }, (_, i) => `m${i}`);
  /** Subscriptions: key `<m>:<c>`, plan. */
  private readonly subs: Array<{ key: string; plan: Plan }> = [];
  /** Acked events: per customer key, total quantity. */
  private readonly ackedQty = new Map<string, number>();
  /** Events given up without an ack (they may still have gone in): per key, total quantity. */
  private readonly unknownQty = new Map<string, number>();
  /** Invocation ids the client received (acked) — for the drain check. */
  readonly ackedIds = new Set<string>();
  private readonly stats = { events: 0, batches: 0, sends: 0, resends: 0, acked: 0, unacked: 0, overloaded: 0 };
  /** Synchronous calls (`invokeSync`): answered directly, `429`, `202`; times a half-done follower copy had to ask the leader (I4). */
  private readonly sync = { direct: 0, throttled: 0, accepted: 0, followerStale: 0 };
  /** The `cancel` result of each subscription: the next open period = the expected invoice count. */
  private readonly finalSub = new Map<string, SubState>();
  /** Durable cancels that ran past the limit in `quiesce`: subscription key → invocation id. */
  private readonly lateCancels = new Map<string, string>();
  private seq = 0;

  get turns(): readonly TurnSeen[] {
    return this.w.turns;
  }
  get maxAttempts(): number {
    return BILLING.maxAttempts;
  }
  describe(): string {
    const b = BILLING;
    return (
      `billing: ${b.merchants} merchants × (${b.subs} + 1 hot) subscriptions, period ${b.periodMs} ms, day ${b.dayMs} ms; ` +
      `${b.eventsPerS} events/s (${Math.round(b.hotShare * 100)}% to hot customers, batch ${b.hotBatch}); ` +
      `gateway: 500 ${b.fail500}, timeout-after-charge ${b.timeoutAfterCharge}, 3DS ${b.requiresAction}, declined ${b.declined}, ` +
      `outage ${b.outageS}s every ${b.outageEveryS}s; Invoice maxAttempts ${b.maxAttempts} (exp ${b.retryInitialMs}→${b.retryMaxMs} ms, PAUSE); ` +
      `rules: rate ${b.rate || "none"} CU/s tenant ${b.tenant}, concurrency ${b.concurrency || "none"} scope m0`
    );
  }

  /**
   * A synchronous call that waits for the result (asks `/invocations/{id}` again on `202`).
   * `resume` = the id of a call already durable earlier (the previous wait ran out of time): only
   * ask again, do not make a new call. Out of time ⇒ [`StuckCall`] carrying the ids received.
   */
  private async invokeSync(path: string, body: unknown, deadlineMs = 120_000, resume?: string): Promise<unknown> {
    const start = Date.now();
    const deadline = start + deadlineMs;
    let id: string | undefined = resume;
    let leaderReads = 0;
    // Trail of the read (I4): one line each time the answer differs from the previous one —
    // timestamp, code, source, status.
    const ids: string[] = resume ? [resume] : [];
    const trail: string[] = [];
    let last = "";
    const note = (what: string, r: Response | undefined, b: InvokeBody | undefined, err?: unknown) => {
      const code = r?.status ?? (err ? `error ${(err as Error).message}` : "");
      const line = `${what} ${code} ${r?.headers.get("x-cairn-read") ?? ""} ${b?.status ?? ""} ${b?.code ?? ""}`.replace(/ +/g, " ").trim();
      if (line !== last) trail.push(`+${Date.now() - start}ms ${line}`);
      last = line;
    };
    while (Date.now() < deadline) {
      if (id === undefined) {
        let err: unknown;
        const r = await this.h
          .cfetch(path, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) }, 20_000)
          .catch((e) => {
            err = e;
            return undefined;
          });
        const b = (await r?.json().catch(() => ({}))) as InvokeBody | undefined;
        note("invoke", r, b, err);
        if (r?.ok && b?.status === "completed") {
          this.sync.direct++;
          return JSON.parse(b.output ?? "null");
        }
        if (b?.id && (r?.ok || r?.status === 202 || r?.status === 429)) {
          id = b.id;
          ids.push(id);
          if (r?.status === 429) this.sync.throttled++;
          else this.sync.accepted++;
        } else await sleep(Math.min(5_000, b?.retry_after_ms ?? 500));
        continue;
      }
      let err: unknown;
      const r = await this.h.cfetch(`/invocations/${id}`, undefined, 10_000).catch((e) => {
        err = e;
        return undefined;
      });
      let b = (await r?.json().catch(() => undefined)) as InvokeBody | undefined;
      note("status", r, b, err);
      // A follower's copy of a **half-done** status can be up to a minute stale (I4): the last
      // object of the shared WAL log is only exposed to followers when the writer writes a later
      // object, or when the range flushes to L0 — `frame_age_flush` 60 s (+ the check interval, +
      // the reader's manifest reload interval) on a node that has gone quiet. A final outcome
      // cannot be stale any more: accept it. A half-done one asks for the leader's copy.
      if (r?.headers.get("x-cairn-read") === "follower" && !terminal(b?.status) && this.h.readLeader) {
        this.sync.followerStale++;
        const l = await this.h.readLeader(id, leaderReads++).catch(() => undefined);
        note("leader", undefined, l, l ? undefined : new Error("unreadable"));
        if (l) b = l;
      }
      if (b?.status === "completed") return JSON.parse(b.output ?? "null");
      if (b?.status === "failed" || b?.status === "paused") {
        throw new Error(`${path}: ${b.status} ${b.code} ${b.message}`);
      }
      await sleep(250);
    }
    const diag = this.h.diagnose ? await this.h.diagnose(path, ids, trail).catch((e) => `snapshot failed: ${(e as Error).message}`) : "";
    throw new StuckCall(
      `${path}: not done within ${deadlineMs} ms (ids ${ids.join(",") || "—"}; trail ${trail.join(" | ")})${diag ? `; ${diag}` : ""}`,
      ids,
    );
  }

  async setup(h: Harness): Promise<void> {
    this.h = h;
    this.w.resolve3ds = async (scope, invoiceId, attempt, ok) => {
      const r = await h.cfetch(
        `${route(scope)}/promises/Invoice/${encodeURIComponent(invoiceId)}/3ds-${attempt}/resolve`,
        { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ ok }) },
        10_000,
      );
      await r.arrayBuffer().catch(() => undefined);
      return r.ok;
    };
    const rule = async (body: unknown) => {
      const r = await h.cfetch("/rules", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
      if (!r.ok) throw new Error(`rule ${JSON.stringify(body)}: ${r.status} ${await r.text()}`);
    };
    if (BILLING.rate > 0) await rule({ tenant: BILLING.tenant, rate: BILLING.rate, burst: BILLING.rate });
    if (BILLING.concurrency > 0) await rule({ pattern: this.merchants[0], concurrency: BILLING.concurrency });

    for (const m of this.merchants) {
      for (let c = 0; c < BILLING.subs; c++) this.subs.push({ key: `${m}:c${c}`, plan: c % 3 === 0 ? "pro" : "basic" });
      this.subs.push({ key: `${m}:hot`, plan: "pro" });
    }
    // Spread the period-closing points over half a cycle: mostly staggered, still with some
    // bunching.
    await pool(this.subs, 16, async (s, i) => {
      await this.invokeSync(`${route(merchantOf(s.key))}/invoke/Subscription/start?key=${encodeURIComponent(s.key)}`, {
        plan: s.plan,
        offsetMs: Math.round(((i % 10) / 10) * (BILLING.periodMs / 2)),
      });
    });
    h.log(`  billing: ${this.subs.length} subscriptions started; rules set`);
  }

  /** Sends one event (or one batch) until acked, with the same idempotency key; gives up after `giveUpAt`. */
  private async sendUsage(key: string, handler: string, body: unknown, idemKey: string, qty: number, giveUpAt: number) {
    const path = `${route(merchantOf(key))}/invoke/Customer/${handler}/send?key=${encodeURIComponent(key)}`;
    for (let n = 0; ; n++) {
      if (n > 0) this.stats.resends++;
      this.stats.sends++;
      const r = await this.h
        .cfetch(path, { method: "POST", headers: { "content-type": "application/json", "idempotency-key": idemKey }, body: JSON.stringify(body) }, 15_000)
        .catch(() => undefined);
      const b = (await r?.json().catch(() => ({}))) as InvokeBody | undefined;
      if (r && b?.id && (r.ok || r.status === 202 || r.status === 429)) {
        this.stats.acked++;
        this.ackedIds.add(b.id);
        this.ackedQty.set(key, (this.ackedQty.get(key) ?? 0) + qty);
        return;
      }
      if (r?.status === 429 && b?.code === "range_overloaded") this.stats.overloaded++;
      if (Date.now() > giveUpAt) {
        this.stats.unacked++;
        this.unknownQty.set(key, (this.unknownQty.get(key) ?? 0) + qty);
        return;
      }
      await sleep(Math.min(5_000, b?.retry_after_ms ?? 300));
    }
  }

  /** Load until `endAt`: usage events, the gateway's outage windows. */
  async run(endAt: number): Promise<void> {
    const giveUpAt = endAt + 120_000;
    const inflight = new Set<Promise<void>>();
    const launch = (p: Promise<void>) => {
      inflight.add(p);
      void p.finally(() => inflight.delete(p));
    };
    const hot = new Map<string, UsageEvent[]>(this.merchants.map((m) => [m, []]));
    const flushHot = (m: string) => {
      const evs = hot.get(m)!;
      if (evs.length === 0) return;
      hot.set(m, []);
      this.stats.batches++;
      const batchId = `b-${this.seq++}`;
      const key = `${m}:hot`;
      launch(this.sendUsage(key, "recordUsageBatch", { batchId, events: evs }, batchId, evs.reduce((a, e) => a + e.qty, 0), giveUpAt));
    };
    const outages = (async () => {
      if (BILLING.outageEveryS <= 0) return;
      for (;;) {
        await sleep(Math.max(0, Math.min(BILLING.outageEveryS * 1000, endAt - Date.now())));
        if (Date.now() >= endAt) return;
        this.w.outage = true;
        this.w.outages++;
        this.h.log(`  [${new Date().toISOString()}] billing: payment gateway down for ${BILLING.outageS}s`);
        await sleep(BILLING.outageS * 1000);
        this.w.outage = false;
      }
    })();
    const tick = 1000 / BILLING.eventsPerS;
    let lastFlush = Date.now();
    while (Date.now() < endAt) {
      const m = this.merchants[Math.floor(Math.random() * this.merchants.length)]!;
      const ev: UsageEvent = { eventId: `ev-${this.seq++}`, qty: 1 + Math.floor(Math.random() * 5) };
      this.stats.events++;
      if (Math.random() < BILLING.hotShare) {
        hot.get(m)!.push(ev);
        if (hot.get(m)!.length >= BILLING.hotBatch) flushHot(m);
      } else {
        const c = Math.floor(Math.random() * BILLING.subs);
        launch(this.sendUsage(`${m}:c${c}`, "recordUsage", ev, ev.eventId, ev.qty, giveUpAt));
      }
      if (Date.now() - lastFlush > 1_000) {
        for (const mm of this.merchants) flushHot(mm);
        lastFlush = Date.now();
      }
      // Limit on concurrent sends: wait for some to finish instead of piling up without bound when
      // the cluster is slow.
      while (inflight.size >= BILLING.inflight) await Promise.race(inflight);
      await sleep(tick);
    }
    for (const mm of this.merchants) flushHot(mm);
    await Promise.all(inflight);
    await outages;
    this.w.outage = false;
  }

  /**
   * After the load and chaos stop: cancel every subscription (closing the last period) so the
   * system can drain. Each cancel is limited by `BILLING_CANCEL_MS` (15 minutes by default): a
   * cancel is a turn of the scope, queued behind all the scope's pending work — the heavy 60-minute
   * variant (`concurrency` 2) piles up ~4 000 waiting turns in `m0`, more than the old 300 s limit
   * even though the system does drain.
   */
  async quiesce(): Promise<void> {
    const failed: string[] = [];
    await pool(this.subs, 16, async (s) => {
      try {
        const st = (await this.invokeSync(
          this.cancelPath(s.key),
          null,
          Number(process.env.BILLING_CANCEL_MS ?? "900000"),
        )) as SubState;
        this.finalSub.set(s.key, st);
      } catch (e) {
        // Out of time but the cancel is durable (has an id): it still runs after the scope's
        // backlog — read its outcome after the drain phase (`settleCancels`), do not judge I2 here.
        const ids = e instanceof StuckCall ? e.ids : [];
        if (ids.length > 0) this.lateCancels.set(s.key, ids[ids.length - 1]!);
        failed.push(`${s.key}: ${(e as Error).message}`);
      }
    });
    this.h.log(
      `  billing: cancelled ${this.finalSub.size}/${this.subs.length} subscriptions` +
        (failed.length ? ` (${failed.length} not done, ${this.lateCancels.size} durable — read again after draining: ${failed.slice(0, 3).join("; ")})` : ""),
    );
  }

  private cancelPath(key: string): string {
    return `${route(merchantOf(key))}/invoke/Subscription/cancel?key=${encodeURIComponent(key)}`;
  }

  /**
   * After the drain phase: the outcome of durable cancels that ran past `BILLING_CANCEL_MS` in
   * `quiesce` (scope `m0`, `concurrency` limit 2, queued behind thousands of waiting turns — the
   * heavy 60-minute variant drains a few dozen seconds **after** the 15-minute limit). Once the
   * system has drained the cancel has reached its outcome; if it still hangs, I2 still fails as
   * before.
   */
  private async settleCancels(): Promise<void> {
    if (this.lateCancels.size === 0) return;
    const late: string[] = [];
    await pool([...this.lateCancels], 16, async ([key, id]) => {
      try {
        const st = (await this.invokeSync(this.cancelPath(key), null, 60_000, id)) as SubState;
        this.finalSub.set(key, st);
        late.push(key);
      } catch {
        // I2 reports "could not cancel" as before.
      }
    });
    this.h.log(`  billing: ${late.length}/${this.lateCancels.size} over-limit cancels reached an outcome after the drain phase`);
  }

  /** The `Invoice` key of an invocation id the SDK has seen. */
  invoiceKeyOf(id: string): string | undefined {
    return this.w.invoiceIds.get(id);
  }

  /**
   * Verdict I1–I6 after draining. `pausedInvoices` = the `Invoice` keys whose
   * invocation is `paused` (from chaos's cluster-wide list).
   */
  async verdict(pausedInvoices: ReadonlySet<string>): Promise<BillingVerdict> {
    const problems = new Map<string, string[]>();
    const fail = (what: string, line: string) => problems.set(what, [...(problems.get(what) ?? []), line]);
    const lines: string[] = [];
    const w = this.w;

    // I1: ≤ 1 succeeded charge per invoice.
    const succeededBy = new Map<string, Charge[]>();
    for (const c of w.charges.values()) {
      if (c.status !== "succeeded") continue;
      succeededBy.set(c.invoiceId, [...(succeededBy.get(c.invoiceId) ?? []), c]);
    }
    for (const [inv, cs] of succeededBy) {
      if (cs.length > 1) fail("billing I1 double charge", `${inv}: ${cs.length} succeeded charges (${cs.map((c) => c.key).join(", ")})`);
    }

    // I2: contiguous invoices 0 … last period.
    await this.settleCancels();
    const bySub = new Map<string, Set<number>>();
    for (const r of w.invoices.values()) {
      const s = bySub.get(r.sub) ?? new Set<number>();
      s.add(r.period);
      bySub.set(r.sub, s);
    }
    for (const s of this.subs) {
      const fin = this.finalSub.get(s.key);
      if (!fin) {
        fail("billing I2 missing invoice", `${s.key}: could not cancel the subscription — last period unknown`);
        continue;
      }
      const have = bySub.get(s.key) ?? new Set<number>();
      const missing: number[] = [];
      for (let p = 0; p < fin.period; p++) if (!have.has(p)) missing.push(p);
      const extra = [...have].filter((p) => p >= fin.period);
      if (missing.length > 0) fail("billing I2 missing invoice", `${s.key}: missing periods ${missing.slice(0, 10).join(",")}${missing.length > 10 ? "…" : ""} (last period ${fin.period - 1})`);
      if (extra.length > 0) fail("billing I2 missing invoice", `${s.key}: extra invoices for periods ${extra.join(",")} (last period ${fin.period - 1})`);
    }

    // I3 + I5 + I6.
    let paid = 0;
    let uncollectible = 0;
    let open = 0;
    let openPaused = 0;
    let chargedNotPaid = 0;
    for (const r of w.invoices.values()) {
      if (r.amount !== priceOf(r.plan, r.apiCalls)) fail("billing I5 price", `${r.invoiceId}: ${r.amount} ≠ ${priceOf(r.plan, r.apiCalls)}`);
      const charged = succeededBy.get(r.invoiceId)?.[0];
      if (r.status === "paid") paid++;
      else if (r.status === "uncollectible") uncollectible++;
      else {
        open++;
        if (pausedInvoices.has(r.invoiceId)) openPaused++;
        else fail("billing I6 silent hang", `${r.invoiceId}: open but the invocation is not paused`);
      }
      if (pausedInvoices.has(r.invoiceId) && r.status === "open") {
        if (charged) chargedNotPaid++;
        continue;
      }
      if (r.status === "paid" && !charged) fail("billing I3 ledger differs from gateway", `${r.invoiceId}: paid in the ledger, no succeeded charge`);
      if (r.status !== "paid" && charged) fail("billing I3 ledger differs from gateway", `${r.invoiceId}: ${r.status} in the ledger but the gateway charged (${charged.key})`);
      if (r.status === "paid" && charged && charged.amount !== r.amount) {
        fail("billing I3 ledger differs from gateway", `${r.invoiceId}: ledger ${r.amount} ≠ gateway ${charged.amount}`);
      }
    }
    for (const inv of succeededBy.keys()) {
      if (!w.invoices.has(inv)) fail("billing I3 ledger differs from gateway", `${inv}: the gateway charged, the ledger has no invoice`);
    }

    // I4: usage.
    let usageRead = 0;
    const syncBefore = { ...this.sync };
    await pool(this.subs, 16, async (s) => {
      let u: { cur: CustomerState; total: number } | undefined;
      try {
        u = (await this.invokeSync(`${route(merchantOf(s.key))}/invoke/Customer/usage?key=${encodeURIComponent(s.key)}`, null, 60_000)) as {
          cur: CustomerState;
          total: number;
        };
      } catch (e) {
        fail("billing I4 usage", `${s.key}: could not read usage: ${(e as Error).message}`);
        return;
      }
      usageRead++;
      const acked = this.ackedQty.get(s.key) ?? 0;
      const unknown = this.unknownQty.get(s.key) ?? 0;
      const billed = [...w.invoices.values()].filter((r) => r.sub === s.key).reduce((a, r) => a + r.apiCalls, 0);
      if (u.total < acked || u.total > acked + unknown) {
        fail("billing I4 usage", `${s.key}: Customer.total ${u.total}, acked events ${acked} (+ ${unknown} unknown) — lost/double-counted state update`);
      }
      if (billed + u.cur.apiCalls !== u.total) {
        fail("billing I4 usage", `${s.key}: invoices ${billed} + open period ${u.cur.apiCalls} ≠ total ${u.total}`);
      }
    });

    const keyCalls = new Map<string, number>();
    for (const c of w.rawCalls) keyCalls.set(c.key, (keyCalls.get(c.key) ?? 0) + 1);
    const repeated = [...keyCalls.values()].filter((n) => n > 1).length;
    // The `charge-k` closure ran twice with the **same** `ctx.attempt`: two engine turns where the
    // later one was not counted as an attempt — a duplicated delivery (the FL-20 shape), or a turn
    // redelivered after the `kill -9` of an owner that does not count attempts. Report only.
    const sameAttempt = new Map<string, number>();
    for (const c of w.rawCalls) {
      const k = `${c.key}#${c.attempt}`;
      sameAttempt.set(k, (sameAttempt.get(k) ?? 0) + 1);
    }
    const dupTurns = [...sameAttempt.values()].filter((n) => n > 1).length;
    const outcome = (x: RawCall["result"]) => w.rawCalls.filter((c) => c.result === x).length;
    const s = this.stats;
    lines.push(
      `billing load: ${s.events} events (${s.batches} hot batches), ${s.sends} sends (${s.resends} resent with the same key), ` +
        `${s.acked} acked, ${s.unacked} given up without an ack, ${s.overloaded} times 429 range_overloaded`,
    );
    lines.push(
      `billing invoices: ${w.invoices.size} (paid ${paid}, uncollectible ${uncollectible}, open ${open} — ${openPaused} paused, ` +
        `${chargedNotPaid} charged at the gateway but not recorded because paused); subscriptions with usage read ${usageRead}/${this.subs.length}`,
    );
    const y = Object.fromEntries(Object.entries(this.sync).map(([k, v]) => [k, v - syncBefore[k as keyof typeof syncBefore]]));
    lines.push(
      `billing usage reads: answered directly ${y.direct}, 429 ${y.throttled}, 202 ${y.accepted}; ` +
        `half-done follower copy ⇒ asked the leader ${y.followerStale} times`,
    );
    lines.push(
      `billing gateway: ${w.rawCalls.length} raw calls (${repeated} keys called > 1 time), succeeded ${outcome("succeeded")}, ` +
        `500 ${outcome("500")}, 503 ${outcome("503")}, timeout-after-charge ${outcome("timeout")}, declined ${outcome("declined")}, ` +
        `3DS ${w.threeDs.started} (done ${w.threeDs.succeeded}/${w.threeDs.failed}, webhook ${w.threeDs.webhooks}, ` +
        `duplicate ${w.threeDs.webhookDup}, failed ${w.threeDs.webhookFailed}); ${w.outages} outage windows`,
    );
    lines.push(`billing duplicate turns (report only): ${dupTurns} times a charge closure ran again with the same ctx.attempt`);
    // Trail for post-run analysis: every `Invoice.run` turn the SDK received and every raw call at
    // the gateway.
    if (env.CHAOS_LOG_DIR) {
      const { mkdirSync, writeFileSync } = await import("node:fs");
      mkdirSync(env.CHAOS_LOG_DIR, { recursive: true });
      const jsonl = (xs: readonly unknown[]) => xs.map((x) => JSON.stringify(x)).join("\n") + "\n";
      writeFileSync(`${env.CHAOS_LOG_DIR}/billing-turns.jsonl`, jsonl(w.turns.map((t) => ({ ...t, key: w.invoiceIds.get(t.id) }))));
      writeFileSync(`${env.CHAOS_LOG_DIR}/billing-gateway.jsonl`, jsonl(w.rawCalls));
      lines.push(`billing trail: ${env.CHAOS_LOG_DIR}/billing-{turns,gateway}.jsonl`);
    }
    return { problems, lines };
  }
}

/** Runs `f` on every element, at most `n` at a time. */
async function pool<T>(xs: readonly T[], n: number, f: (x: T, i: number) => Promise<void>): Promise<void> {
  let next = 0;
  await Promise.all(
    Array.from({ length: Math.min(n, xs.length) }, async () => {
      while (next < xs.length) {
        const i = next++;
        await f(xs[i]!, i);
      }
    }),
  );
}
