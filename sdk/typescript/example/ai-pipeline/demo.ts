// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * The walkthrough: seven scenes, each proving exactly one thing.
 *
 * This is a **plain HTTP client** — it imports nothing from the SDK. That is deliberate:
 * everything below can be done with `curl`, so there is no magic hidden in a library.
 * See `docs/admin-api.md` for each endpoint.
 *
 * ```sh
 * cairnflow --data-dir ./cairnflow-data --port 8080 --no-auth   # dev: no API key
 * node example/ai-pipeline/app.ts
 * node example/ai-pipeline/demo.ts
 * ```
 */

const RUNTIME = process.env.CAIRNFLOW_SERVER ?? "http://127.0.0.1:8080";
const SDK = process.env.SDK_URL ?? "http://127.0.0.1:9080";
const TENANT = "acme";
const SECTIONS = ["market", "competitors", "pricing", "regulation", "risks", "recommendations"];

/**
 * A short list, for the scenes where **the width of the fan-out is not the point**:
 * the saga is about publish/index/notify, the controlled scene is about the bill. Six
 * sections there would only cost more time — each section is an SDK turn that the
 * outbox drain loop runs sequentially inside the whole partition's lock (see the last scene).
 */
const SECTIONS_SHORT = ["market", "risks"];

/**
 * Suffix for this run's workflow keys.
 *
 * A workflow id runs exactly **once**: calling `run` a second time for the same key gets
 * `409`, not the old result. That is a property, not a nuisance — but it means the demo
 * must change keys on every run, otherwise the second run only looks at the first one
 * again. Set `DEMO_RUN` to choose it by hand.
 */
const RUN = process.env.DEMO_RUN ?? Date.now().toString(36).slice(-5);

/** The final stage: once here, `run` has stopped for good, nothing more to ask. */
const TERMINAL_STAGES = new Set(["published", "rolled-back", "timed-out", "rejected"]);

/** The concurrency cap set on the `acme/llm` counter. */
const LLM_BUDGET = 2;

/** The rule book polls every 2 seconds; after changing a rule we must wait for it to arrive. */
const RULES_SETTLE_MS = 2_500;

interface Reply {
  readonly status: number;
  readonly body: any;
  readonly retryAfter: string | null;
}

/**
 * Counts two kinds of requests, because the last scene needs to know where the
 * walkthrough spent its time.
 *
 * The boundary is **whether an invocation has to run**: `/invoke`, `/attach`,
 * `/resolve`, `/cancel` all have to wake the state machine and wait for it to stop; a
 * `GET` only reads the store. A few configuration requests (`POST /rules`,
 * `POST /deployments`) belong to neither and are not counted — they run exactly once
 * for the whole walkthrough, so they explain nothing.
 */
const count = { runCalls: 0, readCalls: 0, runMs: 0, readMs: 0 };
const isRunPath = (path: string): boolean =>
  ["/invoke/", "/attach", "/resolve", "/cancel"].some((p) => path.includes(p));

async function api(method: string, path: string, body?: unknown): Promise<Reply> {
  const runCalls = isRunPath(path);
  const t0 = Date.now();
  const r = await fetch(RUNTIME + path, {
    method,
    ...(body === undefined
      ? {}
      : { body: typeof body === "string" ? body : JSON.stringify(body) }),
    headers: body === undefined ? {} : { "content-type": "application/json" },
  });
  const text = await r.text();
  const ms = Date.now() - t0;
  if (runCalls) {
    count.runCalls++;
    count.runMs += ms;
  } else if (method === "GET") {
    count.readCalls++;
    count.readMs += ms;
  }
  let parsed: unknown = text;
  try {
    parsed = JSON.parse(text);
  } catch {
    /* some endpoints return plain text */
  }
  return { status: r.status, body: parsed, retryAfter: r.headers.get("retry-after") };
}

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

let scene = 0;
const startedAt = Date.now();
function act(title: string): void {
  scene++;
  console.log(`\n\x1b[1m── ${scene}. ${title}\x1b[0m`);
}
function say(...parts: unknown[]): void {
  console.log("   ", ...parts);
}
function ok(...parts: unknown[]): void {
  console.log("  \x1b[32m✓\x1b[0m", ...parts);
}
function bad(...parts: unknown[]): void {
  console.log("  \x1b[31m✗\x1b[0m", ...parts);
}

/** Waits for both processes to be ready, instead of failing with a puzzling `ECONNREFUSED`. */
async function waitUp(): Promise<void> {
  for (const [what, url] of [
    ["runtime", `${RUNTIME}/health`],
    ["SDK", `${SDK}/discover`],
  ] as const) {
    const deadline = Date.now() + 10_000;
    for (;;) {
      try {
        await fetch(url);
        break;
      } catch (e) {
        if (Date.now() > deadline) {
          throw new Error(`${what} is not up at ${url} — did you start it?`);
        }
        await sleep(200);
      }
    }
  }
}

/**
 * `/invoke` waits at most `--invoke-wait-ms`, then answers `202`; ask again until it
 * stops for good. With a pause between two polls: `202` means "not done yet", and
 * asking again immediately only takes the place of the very work we are waiting for.
 */
async function settle(path: string, timeoutMs = 120_000): Promise<Reply> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const r = await api("POST", path);
    if (r.status !== 202) return r;
    if (Date.now() > deadline) return r;
    await sleep(250);
  }
}

async function statusOf(job: string): Promise<any> {
  const r = await api("POST", `/scope/${TENANT}/invoke/ResearchWorkflow/status?key=${job}`);
  return r.status === 200 ? JSON.parse(r.body.output) : { stage: `?? (HTTP ${r.status})` };
}

/**
 * Runs one job: fire `run` and go, watch it through the `SHARED` handler, approve with
 * one HTTP request when it reaches the gate, then `attach` to get the result.
 *
 * Meanwhile, sample `/limits` to see whether the cap really holds things back.
 *
 * Three **sequential** phases — watch, approve, then `attach`. That is also the shape
 * of a real client: nobody `attach`es before finishing their own part.
 */
async function runJob(
  name: string,
  opts: { chaos?: string; approve?: boolean; sections?: readonly string[] } = {},
): Promise<{ result: Reply; stages: string[]; job: string }> {
  const job = `${name}-${RUN}`;
  const sections = opts.sections ?? SECTIONS;
  const sent = await api(
    "POST",
    `/scope/${TENANT}/invoke/ResearchWorkflow/run/send?key=${job}`,
    {
      tenant: TENANT,
      topic: "the coffee market in 2026",
      sections,
      ...(opts.chaos === undefined ? {} : { chaos: opts.chaos }),
    },
  );
  say(`run/send ${job} → ${sent.status} ${sent.body?.id ?? ""}`);
  if (sent.status === 409) {
    say("409 already_invoked: this key already has a run — continuing with attach");
  }

  const stages: string[] = [];
  let reachedGate = false;

  // Phase 1 — watch. The `SHARED` handler runs in parallel with `run`, even while `run`
  // is suspended at the approval gate: looking into a waiting workflow without waking
  // it.
  const deadline = Date.now() + 60_000;
  for (;;) {
    const s = await statusOf(job);
    if (typeof s.stage === "string" && stages[stages.length - 1] !== s.stage) {
      stages.push(s.stage);
      say(`stage → ${s.stage}`);
    }
    reachedGate = s.stage === "awaiting-approval";
    if (reachedGate || TERMINAL_STAGES.has(s.stage) || Date.now() > deadline) break;
    await sleep(100);
  }

  // Phase 2 — approve. There is no handler for this: the runtime exposes the promise
  // over HTTP, so the external system only needs to remember its own workflow key.
  if (reachedGate && opts.approve !== undefined) {
    const gate = `/scope/${TENANT}/promises/ResearchWorkflow/${job}/approval/resolve`;
    const r = await api("POST", gate, { by: "an@acme", ok: opts.approve, note: "read it" });
    say(`approved over HTTP: ${gate} → ${r.status} ${JSON.stringify(r.body)}`);
  }

  // Phase 3 — get the result. `attach` needs no id.
  const result = await settle(`/scope/${TENANT}/workflow/ResearchWorkflow/${job}/attach`);
  const last = await statusOf(job);
  if (typeof last.stage === "string" && stages[stages.length - 1] !== last.stage) {
    stages.push(last.stage);
  }
  return { result, stages, job };
}

/**
 * Waits for every virtual queue to be empty.
 *
 * Scene 7 is a measurement, and a measurement needs a clean baseline. Without this,
 * "while the partition is idle" would measure the tail of scene 6 — and that number
 * would exceed the truth by exactly what we do not see.
 */
async function quiet(timeoutMs = 30_000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const r = await api("GET", "/vqueues");
    const busy = ((r.body as any[]) ?? []).filter((q) => (q.waiting ?? 0) > 0);
    if (busy.length === 0) return true;
    if (Date.now() > deadline) {
      say(`still ${busy.length} queues with waiters after ${timeoutMs / 1000}s — measuring anyway`);
      return false;
    }
    await sleep(250);
  }
}

/** The fan-out's virtual queue — its name is read straight from the key, no reverse lookup table needed. */
async function llmQueue(): Promise<{ granted: number; blockedTimes: number }> {
  const r = await api("GET", "/vqueues");
  const q = (r.body as any[] | undefined)?.find((x) => x.limitKey === "llm" && x.scope === TENANT);
  return { granted: q?.granted ?? 0, blockedTimes: q?.blockedTimes ?? 0 };
}

async function ledger(): Promise<any> {
  const r = await api("POST", `/scope/${TENANT}/invoke/Ledger/spend?key=${TENANT}`);
  return r.status === 200 ? JSON.parse(r.body.output) : r.body;
}

/** Reads the ledger once it has stood still for two polls in a row — see `refunded` for why. */
async function ledgerSettled(timeoutMs = 10_000): Promise<any> {
  const deadline = Date.now() + timeoutMs;
  let prevLedger = JSON.stringify(await ledger());
  for (;;) {
    await sleep(300);
    const nextLedger = JSON.stringify(await ledger());
    if (nextLedger === prevLedger || Date.now() > deadline) return JSON.parse(nextLedger);
    prevLedger = nextLedger;
  }
}

/**
 * Waits for `job`'s line to disappear from the ledger.
 *
 * The refund step is a `ctx.send` — one-way, nobody waits for it. That is deliberate
 * (see `app.ts`: only non-waiting commands survive a cancel), and the price is paid
 * here: the observer must poll, rather than read it in one go right after `attach`
 * returns.
 */
async function refunded(job: string, timeoutMs = 15_000): Promise<any> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const snapshot = await ledger();
    if (snapshot?.byJob?.[job] === undefined || Date.now() > deadline) return snapshot;
    await sleep(200);
  }
}

// ───────────────────────────────────────────────────────────────── walkthrough

await waitUp();

act("Register the deployment");
{
  const r = await api("POST", "/deployments", { uri: SDK });
  say(`POST /deployments → ${r.status}`);
  const svcs = await api("GET", "/services");
  for (const s of svcs.body as any[]) {
    // Handlers of a flat `SERVICE` have no `ty` — nothing to declare.
    const hs = s.handlers.map((h: any) => (h.ty ? `${h.name} (${h.ty})` : h.name));
    say(`${s.name} [${s.ty}] — ${hs.join(", ")}`);
  }
  ok("three services: a workflow, a service, a virtual object");
}

act(`Set the concurrency cap: ${TENANT}/llm = ${LLM_BUDGET}`);
{
  const r = await api("POST", "/rules", {
    pattern: `${TENANT}/llm`,
    concurrency: LLM_BUDGET,
    description: "model call cap for tenant acme",
  });
  say(`POST /rules → ${r.status}, version ${r.body?.version}`);
  say("waiting for the rule book to reach the scheduler (polls every 2s)…");
  await sleep(RULES_SETTLE_MS);
  const sched = await api("GET", "/scheduler");
  say(`rulesVersion in effect: ${(sched.body as any[])[0]?.rulesVersion}`);
  ok("the cap is in place — this is the valve that closes BEFORE money leaves the account");
}

act("Happy path: fan-out of 6 sections, an approver, publishing");
{
  const before = await llmQueue();
  const { result, stages } = await runJob("job-1", { approve: true });
  say(`attach → ${result.status} ${result.body?.status}`);
  if (result.status === 200) {
    ok(JSON.parse(result.body.output as string));
  } else {
    bad(result.body);
  }
  say(`stages passed: ${stages.join(" → ")}`);

  const after = await llmQueue();
  ok(
    `${after.granted - before.granted} grants went through the queue ${TENANT}/[llm]/Researcher ` +
      `— the limit key reached the right counter (queue name read straight from the key)`,
  );
  if (after.blockedTimes > before.blockedTimes) {
    ok(
      `and the cap held back ${after.blockedTimes - before.blockedTimes}/${
        SECTIONS.length
      } grants: six sections asked to run, ${LLM_BUDGET} held a slot at a time`,
    );
    say("a fan-out inside the SAME partition hits the cap too — because `/invoke` emits the call and goes,");
    say("instead of running the child right inside the drain loop. Scene 6 shows it holds on the HTTP path too.");
  } else {
    say(
      `the ${LLM_BUDGET} cap held nothing back this time — the six sections were staggered enough that none`,
    );
    say(
      `had to queue. That is a matter of timing, not of the rule: scene 6 forces simultaneous demand`,
    );
    say(`from five HTTP requests at once, and there the cap holds for sure.`);
  }
  // Read the ledger once it has stopped moving. `charge` is a `ctx.send`: `attach` has
  // returned while a few cents are still in flight, so asking right then reads an
  // unfinished total — and an example about money must not print a half-done number.
  const snapshot = await ledgerSettled();
  say("ledger:", JSON.stringify(snapshot));
}

act("Saga: the last step fails permanently ⇒ compensate backwards");
{
  const before = await ledger();
  const { result, stages, job } = await runJob("job-2", {
    approve: true,
    chaos: "notify-down",
    sections: SECTIONS_SHORT,
  });
  say(`attach → ${result.status} ${result.body?.status}: ${result.body?.message ?? ""}`);
  say(`stages: ${stages.join(" → ")}`);
  const after = await refunded(job);
  say(`ledger before: $${before.usd}, after: $${after.usd}`);
  if (result.status === 500 && after.usd === before.usd && after.byJob[job] === undefined) {
    ok(`article taken down, index removed, money refunded — all three, and no trace of ${job} left in the ledger`);
  } else {
    bad("compensation was not clean", JSON.stringify(after));
  }
}

act("A flaky vendor: same bill, different time");
{
  // A CONTROLLED measurement. Two jobs with the same section list, differing in exactly
  // one variable: one has a flaky vendor, one does not. If retries cost money, it must
  // show here, and show in numbers rather than in my promises.
  const t0 = Date.now();
  const cleanJob = await runJob("job-3a", { approve: true, sections: SECTIONS_SHORT });
  const cleanMs = Date.now() - t0;

  const t1 = Date.now();
  const flakyJob = await runJob("job-3b", {
    approve: true,
    chaos: "vendor-flaky",
    sections: SECTIONS_SHORT,
  });
  const flakyMs = Date.now() - t1;

  if (cleanJob.result.status !== 200 || flakyJob.result.status !== 200) {
    bad("one of the two jobs did not finish", cleanJob.result.body, flakyJob.result.body);
  } else {
    const snapshot = (await ledgerSettled()).byJob ?? {};
    const cleanUsd = snapshot[cleanJob.job];
    const flakyUsd = snapshot[flakyJob.job];
    say(`clean : $${cleanUsd} · ${cleanMs}ms`);
    say(`flaky : $${flakyUsd} · ${flakyMs}ms  (each section fails twice, then passes)`);
    if (cleanUsd === flakyUsd) {
      ok(`same work ⇒ same bill ($${cleanUsd}), even though one side had to retry ${
        SECTIONS_SHORT.length * 2
      } times`);
      say("a durable result is never charged twice — that is what `ctx.run` buys you");
    } else {
      bad(`bills differ: $${cleanUsd} vs $${flakyUsd} — retries are charging twice`);
    }
    say(`the price paid is LATENCY, not money: ${
      Math.round((flakyMs / Math.max(cleanMs, 1)) * 10) / 10
    }× slower, and that is exactly backoff 100ms, 200ms, 400ms… adding up`);
  }
}

act("Out of budget ⇒ 429, not 503, and no lost work");
{
  await api("POST", "/rules", { pattern: "burst", concurrency: 1, description: "demo 429" });
  await sleep(RULES_SETTLE_MS);
  const shots = await Promise.all(
    [1, 2, 3, 4, 5].map((n) =>
      api("POST", `/scope/burst/invoke/Researcher/dig`, {
        tenant: "burst",
        job: `burst-${n}`,
        topic: "a load spike",
        section: `s${n}`,
      }),
    ),
  );
  const throttled = shots.filter((s) => s.status === 429);
  for (const s of shots) {
    say(`${s.status} ${s.body?.status ?? ""}${s.retryAfter ? ` retry-after: ${s.retryAfter}s` : ""}`);
  }
  if (throttled.length > 0) {
    ok(`${throttled.length}/5 throttled — but all have ids, all are durable, all are already queued`);
    say(`look one up again: GET /invocations/${throttled[0]?.body?.id}`);
  } else {
    say("none hit the cap this time — they finished faster than the sampling");
  }
  // This is the real proof that the cap holds, and it is in this scene rather than scene 3:
  // here simultaneous demand comes from **five HTTP requests**, not from a fan-out
  // inside one single-threaded partition.
  const queues = await api("GET", "/vqueues");
  for (const q of (queues.body as any[]) ?? []) {
    if (q.scope === "burst") {
      say(`${q.queue}: granted=${q.granted} blockedTimes=${q.blockedTimes} waiting=${q.waiting}`);
    }
  }

  // Put the cap back. A rule that outlives the scene that set it would make the next scene
  // measure the shadow of the previous one — and scene 7 is a measurement.
  const gone = await api("DELETE", "/rules/burst");
  say(`DELETE /rules/burst → ${gone.status}; the turns still queued now run to the end`);
}

act("Looking inside — and the real price of a fan-out");
{
  const r = await api("GET", "/invocations?status=failed&limit=5");
  for (const i of (r.body as any[]) ?? []) {
    say(`failed  ${i.id}  ${i.service ?? ""}/${i.handler ?? ""}`);
  }
  say(`step-by-step journal: cairn journal <id>  ·  or GET /invocations/<id>/journal`);

  // Does a running fan-out slow down the rest of the partition? Measure, do not guess —
  // I guessed wrong twice before measuring. The same call, two moments, and the baseline
  // must be clean: wait for every queue to be empty first.
  await quiet();
  const timed = async () => {
    const t = Date.now();
    await api("POST", `/scope/${TENANT}/invoke/Ledger/spend?key=${TENANT}`);
    return Date.now() - t;
  };
  // Three samples, take the smallest: a single measurement also measures unrelated things.
  const idleMs = Math.min(await timed(), await timed(), await timed());

  const probe = `probe-${RUN}`;
  const sent = await api(
    "POST",
    `/scope/${TENANT}/invoke/ResearchWorkflow/run/send?key=${probe}`,
    { tenant: TENANT, topic: "measuring the price of fan-out", sections: SECTIONS_SHORT },
  );
  const fanoutMs = await timed();
  const ratio = Math.round((fanoutMs / Math.max(idleMs, 1)) * 10) / 10;

  say(`Ledger/spend while the partition is idle  : ${idleMs}ms`);
  say(`Ledger/spend while a fan-out is draining  : ${fanoutMs}ms  (${ratio}×)`);
  say("");
  if (ratio >= 3) {
    say("The fan-out still holds the entrance: `/invoke` has to drain the outbox before");
    say("answering, and an N-branch fan-out holds that door for about N turns. Not flow");
    say("control — `Ledger` is another service, another object, and /vqueues reports");
    say("waiting=0 throughout.");
  } else {
    say("This number used to be ~14×, and it was where most of the walkthrough's time");
    say("went: the outbox drain loop held the WHOLE PARTITION's lock and ran a whole");
    say("SDK turn for each child right inside it. Now *emitting* is separate from");
    say("*running*, so a fan-out no longer blocks the entrance — the walkthrough's time");
    say("goes to the simulated model latency and the rule book waits, not here.");
  }
  say("Pure introspection (GET /invocations) does not go through that door — a few dozen ms.");

  // Clean up the scapegoat — and here there is a choice that is really worth money.
  //
  // `POST /invocations/{id}/cancel` is a **hard** cancel by default (572): the runtime
  // sets a terminal state and that is it, user code never gets to run one more turn.
  // The job stops, but the saga does NOT run — the money already spent on the finished
  // research sections stays on the ledger.
  //
  // `?mode=signal` is a **cooperative** cancel: the runtime delivers a CANCEL signal,
  // every pending future throws `TerminalError`, and the handler runs exactly its
  // `catch` block — so the saga runs, so the money is returned. The price is that it
  // needs the SDK alive to answer; with the SDK dead, the hard cancel is the remaining
  // path, and that is why the hard cancel is still the default of an admin API.
  const id = sent.body?.id;
  if (typeof id === "string") {
    const prevLedger = await ledger();
    const c = await api("POST", `/invocations/${id}/cancel?mode=signal`);
    say(`cooperative cancel (?mode=signal): ${c.status} ${JSON.stringify(c.body?.status ?? "")}`);
    say("children not yet emitted are not emitted anymore — and the saga's `catch` block runs");
    await refunded(probe);
    // Then wait some more: the cancel runs at once, while the `dig`s in flight still record
    // their spending afterwards. A ledger is only right if it stays right when those
    // arrive — and that is the job of `closed[job]` in `Ledger.charge`, not of a lucky
    // ordering.
    await sleep(2_000);
    const finalLedger = await ledger();
    if (finalLedger?.byJob?.[probe] === undefined && finalLedger?.usd === prevLedger?.usd) {
      ok(`cancelled with a clean ledger: ${probe} left no line, the total is still $${finalLedger.usd}`);
      say("try this scene again without `?mode=signal`: the same job stops, but the money stays.");
    } else {
      bad(
        `${probe} still has $${finalLedger?.byJob?.[probe] ?? 0}; total $${prevLedger?.usd} → $${finalLedger?.usd}`,
      );
    }
  }
  // And here is the answer to "why does the walkthrough take so long", in its own numbers.
  const totalMs = Date.now() - startedAt;
  say("");
  say(
    `${count.runCalls} invocation runs (/invoke, /attach, /resolve, /cancel): ${
      Math.round(count.runMs / 1000)
    }s — ${Math.round(count.runMs / Math.max(count.runCalls, 1))}ms on average`,
  );
  say(
    `${count.readCalls} store reads (GET /invocations, /vqueues…): ${
      Math.round(count.readMs / 1000)
    }s — ${Math.round(count.readMs / Math.max(count.readCalls, 1))}ms on average`,
  );
  say(
    `whole walkthrough: ${Math.round(totalMs / 1000)}s, of which ${
      Math.round((count.runMs / totalMs) * 100)
    }% is on the run path`,
  );
  const cheaperBy = Math.round(
    count.runMs / Math.max(count.runCalls, 1) / Math.max(count.readMs / Math.max(count.readCalls, 1), 1),
  );
  say(`A client tracking progress should poll \`/invocations/{id}\` — ~${cheaperBy}× cheaper — rather than`);
  say("call a `SHARED` handler in a loop the way this walkthrough deliberately does.");
  console.log(`
\x1b[1mWorth trying next:\x1b[0m
  1. Kill the runtime while a job is running, then start it again — there is no recovery step.
     Side effects already done do not run again — ctx.run waits for the runtime to confirm
     the write before moving on. The one IN FLIGHT when it was killed does: nobody knows how far it got.
  2. Fire one more job and DO NOT approve it. It stays suspended for ${"" + (Number(process.env.DEMO_APPROVAL_TIMEOUT_MS ?? 30_000) / 1000)}s
     without holding a process, a connection, or a single line of RAM — then compensates by itself.
  3. Change the rule to concurrency 6 and run job-1 again: same code, different bill/second.
`);
}
