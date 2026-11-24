// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

/**
 * **Mixed-binary cluster:** the previous binary (N−1) and this
 * binary (N) run in one cluster during a rolling upgrade, through split/merge.
 *
 * Not a test file: it runs through `scripts/mixed-binary.sh` (which builds the old binary with
 * `git worktree` + `cargo build`), on SeaweedFS (`localhost:18333`), in the
 * nightly run — the most expensive test, not part of `check.sh`.
 *
 * ```sh
 * MIXED_OLD_BINARY=target/mixed-old/debug/cairnflow MIXED_NEW_BINARY=target/debug/cairnflow \
 *   MIXED_STORE_URI=s3://walbench/mixed-$RANDOM node harness/chaos/mixed.ts
 * ```
 *
 * Scenario (every phase under continuous load, `MIXED_RATE`/s cross-partition calls):
 *
 * 1. **Two old nodes.** Set a rule with the old binary (rule book `/1`), register a
 *    deployment, one split + one merge.
 * 2. **Add a new node.** It is the first new node, so it runs the cluster-level migration:
 *    `cluster-version` rises to its `knows`, the rule book to `/2`. Must see:
 *    - `GET /admin/cluster-version` (new node): `version` = the last data step of the
 *      registry (trailing gate steps, `tier: "gate"`, do not run at startup), not locked,
 *      three sessions with `knows` {old, old, new};
 *    - the old node **stops loudly**, it does not overwrite: `POST /rules` on the old node
 *      returns an error saying the header is newer and to upgrade the binary; the rule book
 *      stays at `/2`, the rule is still there;
 *    - an old-binary process **freshly started** on that bucket refuses to start
 *      (non-zero exit, a `NewerFormat` error) instead of running;
 *    - one split + one merge in the mixed cluster.
 * 3. **Gradual upgrade:** `SIGTERM` each old node, start the new binary in its place, wait until
 *    every range has a leader. Then one more split + merge, and every session declares the same
 *    `knows`.
 * 3b. **Enable the quiet-renewal gate** (when
 *    `MIXED_CTL_BINARY` is set): `cairn-ctl cluster enable-quiet-lease --apply` on the
 *    all-new-binary cluster, after which a freshly started old-binary process must refuse
 *    (`min_binary`) — if the old binary does not know that gate step.
 * 3c. **Enable the batching gate** (when `MIXED_CTL_BINARY` is set): in phase 2
 *    (old nodes still present) `cairn-ctl cluster enable-call-prefix --apply` must **refuse**
 *    and write nothing; here (all new binaries) it turns on, and a freshly started old binary
 *    must refuse — an old binary never opens a Db with v6 outbox rows.
 *    Then one split + merge (the reopened range picks up the gate) under load with unkeyed
 *    `ctx.call`s.
 *    The same command also turns on the `calls/placement` gate (migration step 5,
 *    `min_binary=5`). With an old binary that knows `calls/prefix` but not
 *    `placement`: the old binary formed the cluster, so the batching gate is on from
 *    phase 1; in phase 2 the command must refuse and must **not** turn on
 *    `calls/placement`; here a freshly started old binary must refuse. The load carries
 *    a `placement: "spread"` call through every phase: its callee counts **distinct**
 *    invocations per `seq` — two ids for one call is a duplicated callee (old and new
 *    binaries processing the same journal under two rules), which is what the gate
 *    prevents.
 *
 * An old binary that only lacks **gate steps** (no missing data step — for example
 * `scripts/mixed-binary.base` right before `calls/prefix`) makes phase 2 skip the two
 * data-step checks (old node stops loudly before the new rule book, freshly started old binary
 * is refused): there is nothing new for it to stop on.
 * 3d. **Call lineage:** `MixedHop` calls carry tags through every phase. The old binary
 *    (before call lineage existed) reads outbox rows / `StartCall` messages with the lineage
 *    trailer written by the new binary — the 0-lost-0-wrong verdict covers it. At the end
 *    (all new binaries): `POST /query` sees the `MixedHop` callee with a `root_id`
 *    and the tag `mixed=hop`.
 * 4. **Verdict:** every acked invocation ends `completed` with the right content
 *    (**0 lost, 0 wrong**); the range table returns to its resting state; no split/merge
 *    response outside the contract.
 */

import { spawn, type ChildProcess } from "node:child_process";
import { createWriteStream, existsSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { type Context, serve, service, type ServiceContext } from "../../sdk/typescript/src/index.ts";
import { type RangeRow, rangesOf, tableProblem } from "../../sdk/typescript/test/ranges.ts";

const OLD = process.env.MIXED_OLD_BINARY ?? "";
const NEW = process.env.MIXED_NEW_BINARY ?? "";
/// The new binary's `cairn-ctl` — phase 3b is skipped when absent.
const CTL = process.env.MIXED_CTL_BINARY ?? "";
const STORE = process.env.MIXED_STORE_URI;
const PARTITIONS = process.env.MIXED_PARTITIONS ?? "4";
const RATE = Number(process.env.MIXED_RATE ?? "10");
const PHASE_S = Number(process.env.MIXED_PHASE_S ?? "20");
const LEASE_TTL_MS = process.env.MIXED_LEASE_TTL_MS ?? "1500";
/// Log of each node process: `<MIXED_LOG_DIR>/<name>.log`.
const LOG_DIR = process.env.MIXED_LOG_DIR ?? "target/mixed";
mkdirSync(LOG_DIR, { recursive: true });

if (STORE?.startsWith("s3://") && !process.env.AWS_ENDPOINT) {
  Object.assign(process.env, {
    AWS_ENDPOINT: "http://localhost:18333",
    AWS_ACCESS_KEY_ID: "dev-only-access-key",
    AWS_SECRET_ACCESS_KEY: "dev-only-not-a-secret",
    AWS_REGION: "us-east-1",
    AWS_ALLOW_HTTP: "true",
    AWS_VIRTUAL_HOSTED_STYLE_REQUEST: "false",
  });
}
for (const [what, v] of [["MIXED_STORE_URI", STORE], ["MIXED_OLD_BINARY", OLD], ["MIXED_NEW_BINARY", NEW]] as const) {
  if (!v || (what !== "MIXED_STORE_URI" && !existsSync(v))) {
    console.error(`missing ${what} (${v ?? ""}) — run through scripts/mixed-binary.sh`);
    process.exit(2);
  }
}

const ARGS = [
  "--main-store", STORE!,
  "--cluster",
  "--partitions", PARTITIONS,
  "--tick-interval-ms", "100",
  "--lease-ttl-ms", LEASE_TTL_MS,
  "--reader-poll-ms", "200",
  "--deploy-refresh-ms", "500",
  "--port", "0",
  "--no-auth",
];

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const now = () => new Date().toISOString();
const problems: string[] = [];
const fail = (p: string) => {
  console.error(`  ✗ ${p}`);
  problems.push(p);
};

// ───────────────────────────────────────────── service under load

const runCounts = new Map<string, number>();
const mixedService = service("MixedService", {
  async work(ctx: ServiceContext, input: { tag: string; seq: number }) {
    const stamped = await ctx.run("stamp", async () => {
      runCounts.set(input.tag, (runCounts.get(input.tag) ?? 0) + 1);
      return `stamp-${input.seq}`;
    });
    // Cross-partition call: the target is a virtual object keyed by `seq`.
    const echoed = await ctx.call<{ echo: number }>("MixedEcho", "ping", { seq: input.seq }, { key: `k-${input.seq}` });
    // An unkeyed call: after the batching gate (phase 3c) its outbox row is v6. Carries tags (field
    // 14): an old runtime ignores them; a new runtime writes the tags and the call-tree root into
    // the trailer of the outbox row / `StartCall` message — an old binary must read both (stopping
    // at the last field it knows).
    const hopped = await ctx.call<{ hop: number }>("MixedHop", "hop", { seq: input.seq }, { tags: { mixed: "hop" } });
    // A deliberately spread call: an old binary ignores the field, a new binary
    // only spreads when the caller carries the `calls/placement` mark — the two rules must never
    // meet on the same journal.
    const spread = await ctx.call<{ spread: number }>("MixedSpread", "spread", { seq: input.seq }, { placement: "spread" });
    return { seq: input.seq, stamped, echo: echoed.echo, hop: hopped.hop, spread: spread.spread };
  },
});
/** Invocation ids of the `spread` callee per `seq` — more than one is a duplicated callee. */
const spreadIds = new Map<number, Set<string>>();
const mixedSpread = service("MixedSpread", {
  async spread(ctx: ServiceContext, input: { seq: number }) {
    const ids = spreadIds.get(input.seq) ?? new Set<string>();
    ids.add(ctx.invocationId);
    spreadIds.set(input.seq, ids);
    return { spread: input.seq };
  },
});
const mixedHop = service("MixedHop", {
  async hop(_ctx: ServiceContext, input: { seq: number }) {
    return { hop: input.seq };
  },
});
const mixedEcho = service(
  "MixedEcho",
  {
    async ping(_ctx: Context, input: { seq: number }) {
      return { echo: input.seq };
    },
  },
  { type: "VIRTUAL_OBJECT", handlerTypes: { ping: "EXCLUSIVE" } },
);

// ───────────────────────────────────────────── node

type Kind = "old" | "new";
type Node = { proc: ChildProcess; url: string; alive: boolean; kind: Kind; name: string; log: string[] };
const nodes: Node[] = [];

function spawnProc(kind: Kind, name: string): { proc: ChildProcess; log: string[]; url: Promise<string> } {
  const proc = spawn(kind === "old" ? OLD : NEW, ARGS.concat(["--node-id", name]), { stdio: "pipe" });
  const file = createWriteStream(join(LOG_DIR, `${name}.log`));
  const log: string[] = [];
  const url = new Promise<string>((resolve, reject) => {
    let buf = "";
    const t = setTimeout(() => reject(new Error(`${name} did not report a port:\n${buf.slice(-2000)}`)), 30_000);
    const take = (c: Buffer) => {
      const s = c.toString();
      buf += s;
      file.write(s);
      log.push(s);
      if (log.length > 400) log.splice(0, log.length - 400);
      const m = /http:\/\/[\d.]+:(\d+)/.exec(buf);
      if (m) {
        clearTimeout(t);
        resolve(m[0]!);
      }
    };
    proc.stdout!.on("data", take);
    proc.stderr!.on("data", take);
    proc.on("exit", (code) => {
      clearTimeout(t);
      reject(new Error(`${name} exited (${code}):\n${buf.slice(-2000)}`));
    });
  });
  return { proc, log, url };
}

async function startNode(kind: Kind, name: string): Promise<Node> {
  const { proc, log, url } = spawnProc(kind, name);
  const node: Node = { proc, url: await url, alive: true, kind, name, log };
  proc.on("exit", (code, signal) => {
    if (node.alive && !stopping) {
      fail(`${name} (${kind}) exited on its own (${code ?? signal}):\n${log.join("").slice(-1500)}`);
    }
    node.alive = false;
  });
  console.log(`  [${now()}] ${name} (${kind}) up: ${node.url}`);
  return node;
}

async function stopGracefully(n: Node) {
  n.alive = false;
  const exited = new Promise((r) => n.proc.once("exit", r));
  n.proc.kill("SIGTERM");
  await Promise.race([exited, sleep(30_000)]);
  n.proc.kill("SIGKILL");
  console.log(`  [${now()}] ${n.name} (${n.kind}) shut down cleanly`);
}

let stopping = false;

/** A cluster-aware fetch — like `chaos.ts`: follows the `leader` of a 503. */
async function cfetch(path: string, init?: RequestInit, timeoutMs = 30_000, prefer?: Kind): Promise<Response> {
  const deadline = Date.now() + timeoutMs;
  const pick = () =>
    (prefer && nodes.find((n) => n.alive && n.kind === prefer)?.url) ?? nodes.find((n) => n.alive)?.url ?? nodes[0]!.url;
  let base = pick();
  for (;;) {
    try {
      const r = await fetch(base + path, init);
      if (r.status === 503) {
        const peek = (await r.clone().json().catch(() => undefined)) as { leader?: string } | undefined;
        if (Date.now() > deadline) return r;
        if (peek?.leader) base = peek.leader.replace(/\/$/, "");
        else {
          await sleep(200);
          base = pick();
        }
        continue;
      }
      if (r.status === 404 && path.startsWith("/invoke/") && Date.now() < deadline) {
        await sleep(200);
        continue;
      }
      return r;
    } catch (e) {
      if (Date.now() > deadline) throw e;
      base = pick();
      await sleep(200);
    }
  }
}

async function allLed(what: string, timeoutMs = 60_000) {
  const deadline = Date.now() + timeoutMs;
  let last = "";
  while (Date.now() < deadline) {
    const rows = await rangesOf((p, i) => cfetch(p, i)).catch(() => [] as RangeRow[]);
    const p = rows.length ? tableProblem(rows) : "could not read the table";
    if (p === null) return;
    last = p;
    await sleep(250);
  }
  fail(`${what}: the range table did not return to its resting state within ${timeoutMs / 1000}s — ${last}`);
}

/** One split, then merge the child pair back. Errors other than 409/503 are responses outside the contract. */
async function splitThenMerge(what: string) {
  for (let attempt = 0; attempt < 20; attempt++) {
    const rows = await rangesOf((p, i) => cfetch(p, i));
    const parent = rows.find((r) => !r.intent && r.leader);
    if (!parent) {
      await sleep(500);
      continue;
    }
    const r = await cfetch(`/ranges/${parent.range}/split`, { method: "POST" });
    const text = await r.text();
    if (r.status === 409 || r.status === 503) {
      console.log(`  ${what}: split ${parent.range} deferred (${r.status} ${text.slice(0, 120)})`);
      await sleep(1000);
      continue;
    }
    if (!r.ok) return fail(`${what}: split ${parent.range} → ${r.status} ${text.slice(0, 300)}`);
    const after = ((JSON.parse(text) as { ranges?: RangeRow[] }).ranges ?? []).filter(
      (x) => x.start >= parent.start && x.end <= parent.end && x.range !== parent.range,
    );
    if (after.length !== 2) return fail(`${what}: split ${parent.range} did not yield exactly two children: ${text.slice(0, 300)}`);
    console.log(`  [${now()}] ${what}: split ${parent.range} → ${after[0]!.range}, ${after[1]!.range} ✓`);
    for (let m = 0; m < 40; m++) {
      await allLed(`${what} (before merge)`, 30_000);
      const g = await cfetch(`/ranges/merge?left=${after[0]!.range}&right=${after[1]!.range}`, { method: "POST" });
      const gt = await g.text();
      if (g.ok) {
        console.log(`  [${now()}] ${what}: merge ${after[0]!.range}+${after[1]!.range} ✓`);
        return;
      }
      // A `404` from a node whose routing table has not yet seen the two fresh children:
      // retry, but log it — still the same after 40 attempts is an error.
      if (g.status !== 409 && g.status !== 503 && g.status !== 404) {
        return fail(`${what}: merge → ${g.status} ${gt.slice(0, 300)}`);
      }
      console.log(`  ${what}: merge deferred (${g.status} ${gt.slice(0, 120)})`);
      await sleep(1000);
    }
    return fail(`${what}: merge not done after 40 attempts`);
  }
  fail(`${what}: could not split any range after 20 attempts`);
}

/** The two checks of a new **data** step (phase 2): the old node stops loudly before
 *  the new rule book, and a freshly started old binary is refused. */
async function dataUpgradeChecks() {
  // The old node meets rule book `/2`: stops loudly, does not overwrite.
  const oldNode = nodes.find((n) => n.kind === "old" && n.alive)!;
  const put = await fetch(`${oldNode.url}/rules`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ pattern: "mixed", concurrency: 1 }),
  });
  const putText = await put.text();
  if (put.ok || !putText.includes("cairnflow-rules/") || !putText.includes("upgrade this binary")) {
    fail(`an old node writing a rule to rule book /2 must stop loudly: ${put.status} ${putText.slice(0, 300)}`);
  } else {
    console.log(`  the old node refused to write the rule: ${put.status} ${putText.slice(0, 160)}`);
  }
  const rules = (await (await cfetch("/rules", undefined, 30_000, "new")).json()) as any;
  const mixedRule = (rules.rules ?? []).find((r: any) => r.pattern === "mixed");
  if (mixedRule?.concurrency !== "64") fail(`the old binary's rule changed or vanished after the migration: ${JSON.stringify(rules)}`);
  // A freshly started old-binary process: refused.
  {
    const late = spawnProc("old", "old-late");
    const outcome = await late.url.then(
      (u) => `started (${u})`,
      (e: Error) => e.message,
    );
    late.proc.kill("SIGKILL");
    if (!/NewerFormat|upgrade this binary|cluster-version/.test(outcome)) {
      fail(`an old binary starting on an upgraded cluster must refuse: ${outcome.slice(0, 600)}`);
    } else {
      console.log(`  freshly started old binary refused ✓`);
    }
  }
}

/** Gate steps of `lease/quiet` and `calls/placement` (`cairn_store::migrate`). */
const QUIET_LEASE = 3;
const CALL_PLACEMENT = 5;

async function runCtl(args: string[]): Promise<{ code: number | null; out: string }> {
  const ctl = spawn(CTL, ["--main-store", STORE!, ...args], { env: process.env, stdio: ["ignore", "pipe", "pipe"] });
  let out = "";
  ctl.stdout?.on("data", (d) => (out += d));
  ctl.stderr?.on("data", (d) => (out += d));
  const code = await new Promise<number | null>((res) => ctl.on("exit", res));
  return { code, out };
}

/** A freshly started old-binary process must refuse (`min_binary`). */
async function oldBinaryRefused(name: string, what: string) {
  const late = spawnProc("old", name);
  const outcome = await late.url.then(
    (u) => `started (${u})`,
    (e: Error) => e.message,
  );
  late.proc.kill("SIGKILL");
  if (!/min_binary|NewerFormat|upgrade this binary/.test(outcome)) {
    fail(`an old binary starting ${what} must refuse: ${outcome.slice(0, 600)}`);
  } else {
    console.log(`  old binary refused ${what} ✓`);
  }
}

async function clusterVersion(): Promise<any> {
  const r = await cfetch("/admin/cluster-version", undefined, 30_000, "new");
  if (!r.ok) throw new Error(`/admin/cluster-version → ${r.status} ${await r.text()}`);
  return r.json();
}

// ───────────────────────────────────────────── scenario

async function main() {
  console.log(`mixed cluster: old=${OLD}\n               new=${NEW}\n               store=${STORE} (${process.env.AWS_ENDPOINT})`);
  const sdk = await serve({ services: [mixedService, mixedEcho, mixedHop, mixedSpread], port: 0 });

  // ── Phase 1: two old nodes.
  console.log(`\n── phase 1: two old nodes`);
  nodes.push(await startNode("old", "old-0"), await startNode("old", "old-1"));
  await allLed("phase 1 startup");
  const rule = await cfetch("/rules", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ pattern: "mixed", concurrency: 64, description: "set by the old binary" }),
  });
  if (!rule.ok) fail(`old binary setting a rule: ${rule.status} ${await rule.text()}`);
  const reg = await cfetch("/deployments", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ uri: sdk.url }),
  });
  if (!reg.ok) throw new Error(`deployment registration failed: ${reg.status} ${await reg.text()}`);

  const acked = new Map<string, { seq: number; tag: string }>();
  let sent = 0;
  let ackFailures = 0;
  let loadOn = true;
  const load = (async () => {
    while (loadOn) {
      const seq = sent++;
      const tag = `mixed-${seq}`;
      try {
        const r = await cfetch(`/invoke/MixedService/work`, { method: "POST", body: JSON.stringify({ tag, seq }) }, 20_000);
        if (r.ok || r.status === 202) {
          const body = (await r.json()) as { id?: string };
          if (body.id) acked.set(body.id, { seq, tag });
        } else ackFailures++;
      } catch {
        ackFailures++;
      }
      await sleep(1000 / RATE);
    }
  })();
  await sleep(PHASE_S * 1000);
  await splitThenMerge("phase 1 (old)");

  // ── Phase 2: add a new node — the one that runs the migration.
  console.log(`\n── phase 2: add a new node (runs the migration)`);
  nodes.push(await startNode("new", "new-2"));
  const cv = await clusterVersion();
  const knows = cv.binary?.knows;
  console.log(`  cluster-version: ${JSON.stringify(cv)}`);
  if (!(knows >= 1)) fail(`the new binary must carry at least one migration step: ${JSON.stringify(cv)}`);
  const dataSteps = ((cv.binary?.ledger ?? []) as Array<{ to: number; tier: string }>).filter((m) => m.tier !== "gate");
  const version = dataSteps.length ? Math.max(...dataSteps.map((m) => m.to)) : 0;
  if (cv.version !== version || cv.upgrading !== null) fail(`the cluster has not reached ${version}: ${JSON.stringify(cv)}`);
  const kn = (cv.nodes ?? []).map((n: any) => n.knows).sort();
  // The old binary has the same migration registry as the new one (only the code differs — e.g. the
  // lenient-read layout of call lineage): there is no step to differ on, so the `knows`-based
  // checks below do not apply; the 0-lost-0-wrong verdict still does.
  const sameLedger = kn.length === 3 && kn.every((k: number) => k === knows);
  if (sameLedger) {
    console.log(`  old binary has the same migration registry (knows=${knows}) — skipping the knows-based checks`);
  } else if (kn.length !== 3 || kn.filter((k: number) => k === knows).length !== 1) {
    fail(`must see three sessions, exactly one declaring knows=${knows}: ${JSON.stringify(cv.nodes)}`);
  }
  const oldKnows = Math.min(...kn.filter((k: number | null) => k !== knows).map((k: number | null) => k ?? 0));
  // The old binary lacks a **data** step (not only gate steps) ⇒ there is something new for it to
  // stop loudly on.
  const dataUpgrade = version > oldKnows;
  console.log(`  old binary knows=${oldKnows}, new knows=${knows}: ${dataUpgrade ? "new data step" : "gate steps differ only"}`);
  if (CTL && existsSync(CTL) && !sameLedger) {
    // The call gate cannot be turned on while a session of a binary that does not know the last
    // gate step remains. An old binary that knows `calls/prefix` formed a cluster with the batching
    // gate already on: compare with the state before the command, and `calls/placement` must still
    // be off.
    const { code, out } = await runCtl(["cluster", "enable-call-prefix", "--apply"]);
    const after = await clusterVersion();
    if (
      code !== 3 ||
      after.callPrefix !== cv.callPrefix ||
      after.callPlacement === true ||
      after.minBinary !== cv.minBinary
    ) {
      fail(`enable-call-prefix while old nodes remain must refuse and write nothing: exit ${code}, ${JSON.stringify(after)}\n${out.slice(0, 600)}`);
    } else {
      console.log(`  enable-call-prefix refused while old nodes remain ✓`);
    }
  }
  if (dataUpgrade) await dataUpgradeChecks();
  await sleep(PHASE_S * 1000);
  await splitThenMerge("phase 2 (mixed)");

  // ── Phase 3: upgrade the old nodes one by one.
  console.log(`\n── phase 3: gradual upgrade`);
  for (const [i, n] of nodes.entries()) {
    if (n.kind !== "old") continue;
    await stopGracefully(n);
    nodes[i] = await startNode("new", `new-${i}`);
    await allLed(`after upgrading ${n.name}`);
    await sleep((PHASE_S * 1000) / 2);
  }
  await splitThenMerge("phase 3 (new)");
  const cv3 = await clusterVersion();
  const live = (cv3.nodes ?? []) as Array<{ node: string; knows: number | null }>;
  const stale = live.filter((n) => n.knows !== knows);
  // Sessions of old nodes that shut down cleanly are `closed`, no longer in the live list.
  if (stale.length) fail(`sessions still declare a knows other than ${knows}: ${JSON.stringify(live)}`);

  // ── Phase 3b: enable the quiet-renewal gate, the old binary no longer starts.
  if (CTL && existsSync(CTL)) {
    console.log(`\n── phase 3b: cairn-ctl cluster enable-quiet-lease`);
    const { code, out } = await runCtl(["cluster", "enable-quiet-lease", "--apply"]);
    if (code !== 0) fail(`enable-quiet-lease exited ${code}: ${out.slice(0, 600)}`);
    else console.log(`  ${out.trim().split("\n")[0]}`);
    if (oldKnows < QUIET_LEASE) await oldBinaryRefused("old-after-gate", "after enabling the quiet-renewal gate");
    await sleep((PHASE_S * 1000) / 2);
    await allLed("after enabling the gate");

    // ── Phase 3c: enable the batching gate; the old binary no longer starts; reopened ranges write
    // v6 rows.
    console.log(`\n── phase 3c: cairn-ctl cluster enable-call-prefix`);
    const on = await runCtl(["cluster", "enable-call-prefix", "--apply"]);
    if (on.code !== 0) fail(`enable-call-prefix exited ${on.code}: ${on.out.slice(0, 600)}`);
    else console.log(`  ${on.out.trim().split("\n")[0]}`);
    const cvp = await clusterVersion();
    if (cvp.callPrefix !== true || cvp.callPlacement !== true || cvp.minBinary < CALL_PLACEMENT) {
      fail(`batching / spread gate not on: ${JSON.stringify(cvp)}`);
    }
    if (oldKnows < CALL_PLACEMENT) await oldBinaryRefused("old-after-call-prefix", "after enabling the call gate");
    await splitThenMerge("phase 3c (batching gate)");
    await sleep((PHASE_S * 1000) / 2);
    await allLed("after enabling the batching gate");
  }

  // ── Verdict.
  loadOn = false;
  await load;
  console.log(`\nload done: ${sent} sent, ${acked.size} acked, ${ackFailures} not acked`);
  let lost = 0;
  let wrong = 0;
  for (const [id, want] of acked) {
    const deadline = Date.now() + 120_000;
    let verdict: "ok" | "wrong" | "lost" = "lost";
    let last = "(not read yet)";
    do {
      const r = await cfetch(`/invocations/${id}`).catch(() => undefined);
      if (r?.ok) {
        const body = (await r.json()) as { status: string; output?: string };
        last = JSON.stringify(body);
        if (body.status === "completed") {
          const out = JSON.parse(body.output ?? "{}") as { seq?: number; echo?: number; hop?: number; spread?: number };
          verdict =
            out.seq === want.seq &&
            out.echo === want.seq &&
            out.hop === want.seq &&
            out.spread === want.seq &&
            (runCounts.get(want.tag) ?? 0) >= 1
              ? "ok"
              : "wrong";
          break;
        }
        if (body.status === "failed" || body.status === "paused") {
          verdict = "wrong";
          break;
        }
      }
      await sleep(250);
    } while (Date.now() < deadline);
    if (verdict === "lost") {
      lost++;
      console.error(`  LOST: ${id} (seq=${want.seq}) — ${last}`);
      const j = await cfetch(`/invocations/${id}/journal`).then((r) => r.text()).catch((e) => `${e}`);
      console.error(`    journal: ${j.slice(0, 3000)}`);
    } else if (verdict === "wrong") {
      wrong++;
      console.error(`  WRONG: ${id} (seq=${want.seq}) — ${last}`);
    }
  }
  console.log(`verdict: ${acked.size} acked → ${lost} lost, ${wrong} wrong`);
  if (lost || wrong) fail(`${lost} lost, ${wrong} wrong`);
  const dup = [...spreadIds].filter(([, ids]) => ids.size > 1);
  if (dup.length) {
    fail(`duplicated spread callee (${dup.length} seq): ${JSON.stringify(dup.slice(0, 5).map(([seq, ids]) => [seq, [...ids]]))}`);
  } else {
    console.log(`  spread callee: ${spreadIds.size} seq, exactly one invocation per seq ✓`);
  }
  if (acked.size === 0) fail("no invocation was acked — green for the wrong reason");
  await allLed("end of run");
  // Lineage on the all-new-binary cluster: callees have a root and tags. A new binary
  // that does not have `root_id` yet is skipped.
  {
    const sql =
      "SELECT count(*) AS n FROM sys_invocation WHERE service = 'MixedHop' " +
      "AND root_id IS NOT NULL AND tags LIKE '%\"mixed\":\"hop\"%'";
    const r = await cfetch("/query", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ sql }),
    }).catch(() => undefined);
    const body = r ? await r.text() : "(no answer)";
    if (r?.ok) {
      const n = Number((JSON.parse(body) as { rows: number[][] }).rows[0]?.[0] ?? 0);
      if (n > 0) console.log(`  lineage: ${n} MixedHop callees with root_id and tag mixed=hop ✓`);
      else fail(`lineage: no MixedHop callee has root_id + tag (${body.slice(0, 300)})`);
    } else if (body.includes("root_id")) {
      console.log("  lineage: the new binary does not have root_id yet — skipping");
    } else {
      fail(`lineage: /query error ${r?.status}: ${body.slice(0, 300)}`);
    }
  }

  stopping = true;
  for (const n of nodes) n.proc.kill("SIGKILL");
  await sdk.close();
  if (problems.length) {
    console.error(`\n❌ MIXED CLUSTER FAILED (${problems.length}):\n${problems.map((p) => ` - ${p.split("\n")[0]}`).join("\n")}`);
    process.exit(1);
  }
  console.log("\n✅ mixed cluster: 0 lost, 0 wrong; the old binary stops loudly on the upgraded bucket");
  process.exit(0);
}

main().catch((e) => {
  console.error(e);
  stopping = true;
  for (const n of nodes) n.proc.kill("SIGKILL");
  process.exit(1);
});
