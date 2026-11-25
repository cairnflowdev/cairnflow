// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * A full example: a model-driven research pipeline, multi-tenant.
 *
 * It exists to answer one question: *what can cairn do that a task queue cannot?*
 * Six things, and each has exactly one place in this file:
 *
 * | Capability | Where |
 * |---|---|
 * | A per-tenant concurrency cap — a valve that closes **before** money leaves the account | `ctx.scoped(tenant, "llm")` in `run` |
 * | Durable fan-out: N model calls in parallel, wait for all | `ctx.all([...ctx.call])` |
 * | A human gate: approval by one HTTP request, wait as long as needed | `ctx.promise("approval")` racing `ctx.sleep` |
 * | Saga: a failure at the last step compensates backwards, and the compensation is durable too | `undo[]` in a `try/catch` block |
 * | Giving up deliberately instead of retrying forever | `ctx.attempt` + `RunFailure` in `Researcher.dig` |
 * | A spending ledger serialized per tenant with no locks | virtual object `Ledger`, `EXCLUSIVE` handlers |
 *
 * Run:
 *
 * ```sh
 * cairnflow --data-dir ./data --port 8080 --no-auth   # runtime (dev: no API key)
 * node example/ai-pipeline/app.ts                     # SDK on :9080
 * node example/ai-pipeline/demo.ts                    # run the whole walkthrough
 * ```
 */

import {
  type Context,
  RunFailure,
  serve,
  service,
  type SharedContext,
  TerminalError,
  workflow,
  type ServiceContext,
} from "../../src/index.ts";
import {
  indexForSearch,
  type Note,
  notifySubscribers,
  publishToCdn,
  removeFromCdn,
  removeFromIndex,
  researchSection,
  writeDraft,
} from "./stubs.ts";

/** A failure scenario chosen by the input — see `stubs.notifySubscribers`. */
type Chaos = "notify-down" | "vendor-flaky";

interface ResearchRequest {
  readonly tenant: string;
  readonly topic: string;
  readonly sections: readonly string[];
  readonly chaos?: Chaos;
}

interface DigRequest {
  readonly tenant: string;
  /** Job id for the ledger — the same key as every other cost of this job. */
  readonly job: string;
  readonly topic: string;
  readonly section: string;
  readonly chaos?: Chaos | undefined;
}

interface Decision {
  readonly by: string;
  readonly ok: boolean;
  readonly note?: string;
}

/**
 * How long to wait for an approver before giving up.
 *
 * Thirty seconds because this is a demo. In real life this number is days — and that is
 * exactly the point: a `ctx.sleep(7 * 86400_000)` holds no process, no connection, not a
 * single line of RAM. The timer lives in the store.
 */
const APPROVAL_TIMEOUT_MS = Number(process.env.DEMO_APPROVAL_TIMEOUT_MS ?? 30_000);

/** How many attempts before concluding the vendor is permanently broken. */
const MAX_VENDOR_ATTEMPTS = 5;

const round = (usd: number): number => Math.round(usd * 10_000) / 10_000;

// ─────────────────────────────────────────────────────── ResearchWorkflow

const research = workflow("ResearchWorkflow", {
  /**
   * `ctx.key` **is** the job id, and it runs exactly once per value. No idempotency
   * key needed: the workflow key already is the deduplication key.
   */
  async run(ctx: Context, req: ResearchRequest) {
    if (
      typeof req?.tenant !== "string" ||
      !Array.isArray(req.sections) ||
      req.sections.length === 0
    ) {
      // Bad data stays bad no matter how many times it is retried.
      throw new TerminalError("need `tenant` and at least one `sections[]`", 400);
    }

    ctx.set("request", req);
    ctx.set("stage", "researching");

    // Saga: an array of closures, no API at all. It works because cairn guarantees
    // *this code* runs to the end — the `catch` block is exactly as durable as the `try`.
    //
    // Each compensation step is registered **before** its forward step. Dying right in
    // the middle of a forward step can happen, and then what we need is an undo command
    // already in the list — not a value nobody got to return yet. That is also why the
    // undo functions take `ctx.key` rather than a `url`/`handle`.
    //
    // And each compensation step must be a command that **waits for no one**: `ctx.run`
    // (runs in place, needs no completion) or `ctx.send`. The reason is in the `catch`
    // block below — a cancel comes in through the same door as a permanent failure.
    const undo: Array<() => PromiseLike<unknown> | void> = [];
    try {
      // The first compensation, registered before the first cent is spent: every cost of
      // this job is recorded in the ledger under one key, so one undo command covers them all.
      //
      // `ctx.send` rather than `ctx.call`, and that is exactly the condition for the money
      // to be returned when the job is cancelled cooperatively (`cancel?mode=signal`): the
      // CANCEL signal turns every *wait* point into a `TerminalError`, so a `ctx.call` in
      // the `catch` block throws at its first `await` — the compensation stops right before
      // the refund step, and the tenant pays for work whose result nobody reads.
      // `ctx.send` waits for no one so it is still emitted; the ledger is not on the path
      // to the result anyway.
      undo.push(() => ctx.send("Ledger", "refund", { job: ctx.key }, { key: req.tenant }));

      // ── 1. Fan-out. Each section is its own invocation, and each invocation is one
      // model call — that is, money.
      //
      // `ctx.scoped(tenant, "llm")` hangs the whole batch on the counter `tenant/llm`, so
      // the rule book (`POST /rules {"pattern": "acme/llm", "concurrency": 2}`) decides how
      // many *actually* run at once. Without it, six sections are six simultaneous model
      // calls — and a load spike does not slow the system down, it drives the bill up.
      //
      // The scope is **inherited** from the caller anyway (this workflow already runs in
      // the tenant's scope), so `ctx.scoped` here is not there to set the scope — it sets
      // the *limit key*, and a limit key must come with a scope.
      const digs = ctx.scoped(req.tenant, "llm");
      const notes = await ctx.all(
        req.sections.map((section: string) =>
          digs.call<Note>(
            "Researcher",
            "dig",
            {
              tenant: req.tenant,
              job: ctx.key,
              topic: req.topic,
              section,
              chaos: req.chaos,
            } satisfies DigRequest,
            { name: `dig/${section}` },
          ),
        ),
      );

      ctx.set("notes", notes);
      ctx.set("stage", "drafting");

      // ── 2. A costly step right inside the workflow. `ctx.run` ⇒ runs exactly once per
      // durable result, even if the process dies midway.
      const draft = await ctx.run("draft", () => writeDraft(req.topic, notes));
      ctx.send(
        "Ledger",
        "charge",
        { job: ctx.key, usd: draft.usd, what: "draft" } satisfies Charge,
        { key: req.tenant },
      );

      // ── 3. The human gate.
      //
      // Two futures, create both *then* wait — that is what makes combinators possible:
      // the command goes into the journal at creation, and waiting is what happens in
      // `race`.
      ctx.set("stage", "awaiting-approval");
      const approval = ctx.promise<Decision>("approval").get();
      const deadline = ctx.sleep(APPROVAL_TIMEOUT_MS, "approval deadline");
      await ctx.race([approval, deadline]);

      // Who arrives first is decided by **the order in the journal**, so every replay
      // picks the same one again.
      //
      // Do not write `approval.ready()` here: an approver clicking one second late still
      // puts a completion in the journal, and from then on `ready()` says "yes" on every
      // later replay — the same code giving two different answers, exactly the kind of
      // failure hardest to trace. `order()` has no such door: a recorded position does
      // not change.
      if (approval.order() > deadline.order()) {
        ctx.set("stage", "timed-out");
        throw new TerminalError(`nobody approved within ${APPROVAL_TIMEOUT_MS / 1000}s`, 408);
      }
      const decision = await approval;
      if (!decision.ok) {
        ctx.set("stage", "rejected");
        throw new TerminalError(
          `rejected by ${decision.by}: ${decision.note ?? "no reason given"}`,
          409,
        );
      }

      // ── 4. Three outbound steps, each with a compensation step.
      ctx.set("stage", "publishing");

      undo.push(() => ctx.run("unpublish", () => removeFromCdn(ctx.key)));
      const url = await ctx.run("publish", () => publishToCdn(ctx.key, draft.html));

      undo.push(() => ctx.run("deindex", () => removeFromIndex(ctx.key)));
      await ctx.run("index", () => indexForSearch(ctx.key, req.topic));

      const reached = await ctx.run("notify", async () => {
        try {
          return await notifySubscribers(req.topic, url, req.chaos === "notify-down");
        } catch (e) {
          // **Which errors are permanent is your code's decision**, not the remote
          // client library's. `410 Gone` means the topic no longer exists: retrying only
          // reproduces that same error, while the article stays on the CDN. `RunFailure`
          // records the failure in the journal as a result, so this step does not run
          // again, and `ctx.run` throws a `TerminalError` outwards — exactly what the
          // `catch` block below is waiting for to compensate.
          throw new RunFailure(e instanceof Error ? e.message : String(e), 410);
        }
      });

      ctx.set("stage", "published");
      return {
        job: ctx.key,
        url,
        reached,
        approvedBy: decision.by,
        sections: notes.length,
        usd: round(notes.reduce((n, x) => n + x.usd, 0) + draft.usd),
      };
    } catch (e) {
      // Compensate only when the outcome **has been decided**. A transient error is not
      // an outcome: the runtime will retry, and taking the article down while waiting for
      // the retry would be wrecking with our own hands what is about to finish.
      //
      // **A cooperative cancel also lands here**, through exactly this door: the CANCEL
      // signal turns the pending wait point into a `TerminalError`. That is right — a
      // cancelled job needs cleaning up too — but it places a constraint on the steps in
      // `undo[]`: from this moment, every *waiting* command (`ctx.call`,
      // `ctx.promise().get()`, `ctx.sleep`) throws again immediately. Only `ctx.run` and
      // `ctx.send` get through, so `undo[]` only holds those two.
      //
      // A **hard** cancel (`POST /invocations/{id}/cancel` without `?mode=`) never lands
      // here: the runtime sets a terminal state without giving the handler one more turn.
      // The job stops, the saga does not run. That is a deliberate trade-off of the admin
      // API — it does not need the SDK to be alive — not something this code can patch.
      if (e instanceof TerminalError) {
        ctx.set("stage", "rolling-back");
        for (const step of undo.reverse()) {
          await step();
        }
        ctx.set("stage", "rolled-back");
      }
      throw e;
    }
  },

  /**
   * A `SHARED` handler: runs **in parallel** with `run`, even while `run` is suspended at
   * the approval gate. That is how to look into a waiting workflow without waking it.
   *
   * The `SharedContext` type lacks `set`/`clear`/`clearAll` — `tsc` blocks it here, and
   * the runtime blocks it again with PROTOCOL_VIOLATION. Two fences for one rule, because
   * what it prevents is a lost update, and a lost update has no symptoms.
   */
  async status(ctx: SharedContext) {
    const notes = (await ctx.get<Note[]>("notes")) ?? [];
    return {
      job: ctx.key,
      stage: (await ctx.get<string>("stage")) ?? "not started",
      sectionsDone: notes.length,
      usdSoFar: round(notes.reduce((n, x) => n + x.usd, 0)),
    };
  },
});

// ─────────────────────────────────────────────────────────────── Researcher

const researcher = service("Researcher", {
  /**
   * One research section = one model call.
   *
   * This is the only place in the example that touches an external vendor, so it is also
   * the only place that has to answer "what if it breaks".
   */
  async dig(ctx: ServiceContext, req: DigRequest) {
    if (typeof req?.section !== "string" || req.section.length === 0) {
      throw new TerminalError("missing `section`", 400);
    }

    const note = await ctx.run(`dig/${req.section}`, async () => {
      // The runtime retries forever with backoff, and that is **the right default** for a
      // transient failure. It is wrong for a permanently broken API: the invocation sits
      // in the queue forever and nobody waiting for it ever gets an answer.
      //
      // `ctx.attempt` resets to 0 each time the handler makes progress, so it answers
      // exactly the question we need — "is *this* spot stuck" — not "how many times has
      // this invocation failed in total".
      if (ctx.attempt >= MAX_VENDOR_ATTEMPTS) {
        // `RunFailure` differs from `TerminalError` in one point: it records the *failure*
        // in the journal as a result. This step will never run again — even if the
        // invocation is recovered on another node.
        throw new RunFailure(
          `vendor not responding after ${ctx.attempt} attempts for ${req.section}`,
          503,
        );
      }
      if (req.chaos === "vendor-flaky" && ctx.attempt < 2) {
        // An ordinary error (not Terminal/RunFailure) ⇒ the runtime schedules a rerun with
        // backoff 100ms, 200ms, 400ms… The journal stays intact: the steps before it do
        // not run again.
        throw new Error(`vendor returned 503 (attempt ${ctx.attempt})`);
      }
      return researchSection(req.topic, req.section);
    });

    // One-way, no waiting: the ledger is not on the path to the result, so making the
    // pipeline stop and wait for it to finish writing is paying for nothing.
    ctx.send(
      "Ledger",
      "charge",
      { job: req.job, usd: note.usd, what: req.section } satisfies Charge,
      { key: req.tenant },
    );
    return note;
  },
});

// ─────────────────────────────────────────────────────────────────── Ledger

interface Charge {
  readonly job: string;
  readonly usd: number;
  readonly what: string;
}

/**
 * A tenant's spending ledger. Virtual object ⇒ **one instance per key**, and the
 * `EXCLUSIVE` handlers of the same key queue up.
 *
 * So `charge` reads-adds-writes with no lock, no transaction, no
 * `SELECT … FOR UPDATE`. Six `Researcher.dig` invocations running in parallel all fire
 * `charge` at it; the runtime serializes them by key, and the total is right.
 *
 * That is also why `refund` can **close the ledger** of a job: the decision "this job
 * takes no more money" sits in the same place, in the same queue as the spending —
 * so it does not race with it. See `charge`.
 */
const ledger = service(
  "Ledger",
  {
    async charge(ctx: Context, e: Charge) {
      const total = (await ctx.get<number>("usd")) ?? 0;
      const byJob = (await ctx.get<Record<string, number>>("byJob")) ?? {};
      const closed = (await ctx.get<Record<string, true>>("closed")) ?? {};

      // A refund **closes the job's ledger**, and any spending arriving late after that is dropped.
      //
      // Without this line the compensation is right but useless: `charge` flies here via
      // `ctx.send`, so it can arrive *after* the refund command — exactly what happens when
      // cancelling a job mid-fan-out, because the cancel runs at once while the children in
      // flight still record their spending. The result is a ledger line for a job that has
      // had every other trace wiped.
      //
      // The right place to block it is here, not at the caller: `EXCLUSIVE` serializes per
      // tenant, so read-decide-write here needs no lock and has no race window. (The
      // `closed` ledger grows forever — in real life it should expire with the job
      // retention time; in the example it does not.)
      if (closed[e.job]) {
        return { tenant: ctx.key, usd: total, skipped: e.usd };
      }

      byJob[e.job] = round((byJob[e.job] ?? 0) + e.usd);
      ctx.set("usd", round(total + e.usd));
      ctx.set("byJob", byJob);
      return { tenant: ctx.key, usd: round(total + e.usd) };
    },

    async refund(ctx: Context, e: { job: string }) {
      const total = (await ctx.get<number>("usd")) ?? 0;
      const byJob = (await ctx.get<Record<string, number>>("byJob")) ?? {};
      const closed = (await ctx.get<Record<string, true>>("closed")) ?? {};
      const back = byJob[e.job] ?? 0;
      delete byJob[e.job];
      closed[e.job] = true;
      ctx.set("usd", round(total - back));
      ctx.set("byJob", byJob);
      ctx.set("closed", closed);
      return { tenant: ctx.key, refunded: back, usd: round(total - back) };
    },

    /** `SHARED`: reads do not need to queue behind the writer. */
    async spend(ctx: SharedContext) {
      return {
        tenant: ctx.key,
        usd: (await ctx.get<number>("usd")) ?? 0,
        byJob: (await ctx.get<Record<string, number>>("byJob")) ?? {},
      };
    },
  },
  {
    type: "VIRTUAL_OBJECT",
    handlerTypes: { charge: "EXCLUSIVE", refund: "EXCLUSIVE", spend: "SHARED" },
  },
);

// ────────────────────────────────────────────────────────────────── serving

const port = Number(process.env.PORT ?? 9080);
const endpoint = await serve({ services: [research, researcher, ledger], port });
console.log(`ai-pipeline listening on ${endpoint.url}`);
console.log(`register:  cairn register ${endpoint.url}`);
console.log(`run the walkthrough: node example/ai-pipeline/demo.ts`);
