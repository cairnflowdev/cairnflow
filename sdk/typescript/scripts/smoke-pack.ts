// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Smoke test of the npm package.
 *
 * 1. `npm pack` (runs `prepack` ⇒ `dist/`); the tarball holds only `dist/`,
 *    `README.md`, `LICENSE`, `package.json`.
 * 2. Installs the tarball into a directory **outside the repo**; a plain JS app
 *    (`app.mjs`) `serve()`s services, starts a `cairnflow` (`memory://`, a 4-range
 *    cluster, auth on, the client holding an `invoke` key) and calls everything through
 *    `@cairnflow/sdk/client` — no hand-written `fetch` for the data API.
 * 3. Runs the app on each Node in `SMOKE_NODES` (paths, separated by `:`;
 *    default: `mise where node@22`, `node@24`, and the Node running the script).
 * 4. `tsc` on the package user's code: types come from `typeof` of the definitions
 *    (wrong input is an error), and a file importing only `./client` compiles
 *    **without** `@types/node` (browsers, edge).
 *
 * `node scripts/smoke-pack.ts` (from `sdk/typescript`). Needs `cargo build --bin
 * cairnflow` (or `SMOKE_BINARY`).
 */

import { execFileSync, spawnSync } from "node:child_process";
import { mkdtempSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const sdkDir = resolve(fileURLToPath(new URL(".", import.meta.url)), "..");
const repo = resolve(sdkDir, "..", "..");
const binary = process.env.SMOKE_BINARY ?? join(repo, "target", "debug", "cairnflow");
const tsc = join(sdkDir, "node_modules", ".bin", "tsc");

function fail(msg: string): never {
  console.error(`SMOKE FAIL: ${msg}`);
  process.exit(1);
}

function mise(version: string): string | undefined {
  const r = spawnSync("mise", ["where", `node@${version}`], { encoding: "utf8" });
  return r.status === 0 ? join(r.stdout.trim(), "bin", "node") : undefined;
}

const nodes = (process.env.SMOKE_NODES?.split(":").filter(Boolean) ?? [mise("22"), mise("24"), process.execPath]).filter(
  (n): n is string => n !== undefined,
);

function main(): void {
  const work = mkdtempSync(join(tmpdir(), "cairnflow-smoke-"));
  try {
    // 1. Pack.
    // npm ≤ 11 prints an array, npm 12 an object keyed by package name.
    const out = JSON.parse(
      execFileSync("npm", ["pack", "--json", "--pack-destination", work], { cwd: sdkDir, encoding: "utf8" }),
    ) as unknown;
    const packed = (Array.isArray(out) ? out : Object.values(out as object)) as Array<{
      filename: string;
      files: Array<{ path: string }>;
    }>;
    const tarball = join(work, packed[0]!.filename);
    const files = packed[0]!.files.map((f) => f.path).sort();
    const stray = files.filter((f) => !(f.startsWith("dist/") || ["README.md", "LICENSE", "package.json"].includes(f)));
    if (stray.length > 0) fail(`tarball has unexpected files: ${stray.join(", ")}`);
    for (const must of [
      "LICENSE",
      "dist/index.js",
      "dist/index.d.ts",
      "dist/client.js",
      "dist/client.d.ts",
      "dist/fetch.js",
      "dist/fetch.d.ts",
    ]) {
      if (!files.includes(must)) fail(`tarball is missing ${must}`);
    }
    console.log(`tarball: ${packed[0]!.filename}, ${files.length} file`);

    // 2. The app outside the repo.
    const app = join(work, "app");
    execFileSync("mkdir", ["-p", app]);
    writeFileSync(
      join(app, "package.json"),
      JSON.stringify({ name: "smoke-app", private: true, type: "module", dependencies: { "@cairnflow/sdk": `file:${tarball}` } }),
    );
    execFileSync("npm", ["install", "--no-audit", "--no-fund", "--offline"], { cwd: app, stdio: "inherit" });
    writeFileSync(join(app, "app.mjs"), APP);
    writeFileSync(join(app, "services.ts"), SERVICES_TS);
    writeFileSync(join(app, "caller.ts"), CALLER_TS);
    writeFileSync(join(app, "browser.ts"), BROWSER_TS);

    // 3. Run on each Node.
    if (nodes.length === 0) fail("no Node to run on");
    for (const node of nodes) {
      const version = execFileSync(node, ["--version"], { encoding: "utf8" }).trim();
      const r = spawnSync(node, ["app.mjs"], {
        cwd: app,
        encoding: "utf8",
        env: { ...process.env, SMOKE_BINARY: binary },
        timeout: 120_000,
      });
      if (r.status !== 0 || !r.stdout.includes("SMOKE OK")) {
        fail(`Node ${version}: exited ${r.status}\n${r.stdout}\n${r.stderr}`);
      }
      console.log(`Node ${version}: SMOKE OK`);
    }

    // 4. Types, on the package user's side.
    const typeRoots = join(sdkDir, "node_modules", "@types");
    const base = {
      target: "ES2023",
      module: "nodenext",
      moduleResolution: "nodenext",
      strict: true,
      noEmit: true,
      skipLibCheck: false,
      typeRoots: [typeRoots],
    };
    writeFileSync(
      join(app, "tsconfig.json"),
      JSON.stringify({ compilerOptions: { ...base, lib: ["ES2023"], types: ["node"] }, files: ["services.ts", "caller.ts"] }),
    );
    writeFileSync(
      join(app, "tsconfig.browser.json"),
      JSON.stringify({ compilerOptions: { ...base, lib: ["ES2023", "DOM"], types: [] }, files: ["browser.ts"] }),
    );
    for (const cfg of ["tsconfig.json", "tsconfig.browser.json"]) {
      const r = spawnSync(tsc, ["-p", cfg], { cwd: app, encoding: "utf8" });
      if (r.status !== 0) fail(`tsc -p ${cfg}:\n${r.stdout}${r.stderr}`);
      console.log(`tsc -p ${cfg}: OK`);
    }
    console.log(`SMOKE PASS (${readdirSync(join(app, "node_modules", "@cairnflow", "sdk", "dist")).length} file dist)`);
  } finally {
    if (process.env.SMOKE_KEEP !== "1") rmSync(work, { recursive: true, force: true });
    else console.log(`kept ${work}`);
  }
}

/** The package user's plain JS app — run with bare Node, no type stripping. */
const APP = String.raw`
import { spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { serve, service, workflow, TerminalError } from "@cairnflow/sdk";
import { connect, InvocationFailed, AlreadyInvoked } from "@cairnflow/sdk/client";
import { fetchHandler, service as fetchService } from "@cairnflow/sdk/fetch";

const assert = (ok, msg) => { if (!ok) { console.error("wrong: " + msg); process.exit(1); } };
// The Fetch adapter loads from the built package and routes on the path suffix.
const fh = fetchHandler({ services: [fetchService("FSmoke", { async ping() { return 1; } })] });
const fd = await fh(new Request("https://app.example/api/cairn/discover"));
assert(fd.status === 200 && (await fd.json()).protocolMode === "REQUEST_RESPONSE", "fetchHandler /discover");
const awks = new Map();
const svc = service("Smoke", {
  async echo(_ctx, x) { if (x < 0) throw new TerminalError("negative", 422); return { x }; },
  async nap(ctx, ms) { await ctx.sleep(ms); return "awake"; },
});
const obj = service("Gate", {
  async wait(ctx) {
    const v = await ctx.race([ctx.signal("ok"), ctx.sleep(60000)]);
    return v === undefined ? "expired" : v;
  },
  async awk(ctx) {
    const { id, promise } = ctx.awakeable();
    await ctx.run("id", () => { awks.set(ctx.key, id); return 1; });
    try { return await promise; } catch (e) { return "rejected: " + e.message; }
  },
}, { type: "VIRTUAL_OBJECT" });
const wf = workflow("Flow", {
  async run(ctx, who) { return who + ":" + (await ctx.promise("code").get()); },
  async click(ctx, code) { await ctx.promise("code").resolve(code); return "ok"; },
});

const sdk = await serve({ services: [svc, obj, wf] });
const adminKey = "cf_smoke_" + randomBytes(24).toString("hex"); // ≥ 32 bytes
const proc = spawn(process.env.SMOKE_BINARY, ["--main-store", "memory://", "--cluster", "--partitions", "4",
  "--port", "0", "--tick-interval-ms", "50", "--invoke-wait-ms", "300"],
  { stdio: ["ignore", "pipe", "ignore"], env: { ...process.env, CAIRNFLOW_ADMIN_KEY: adminKey } });
const url = await new Promise((res, rej) => {
  let buf = "";
  const t = setTimeout(() => rej(new Error("node did not come up")), 30000);
  proc.stdout.on("data", (c) => { buf += c; const m = /http:\/\/[\d.]+:\d+/.exec(buf); if (m) { clearTimeout(t); res(m[0]); } });
});
const until = async (f) => { for (let i = 0; i < 600; i++) { try { if (await f()) return; } catch {} await new Promise((r) => setTimeout(r, 50)); } throw new Error("timed out"); };
const adm = (p, init = {}) => fetch(url + p, { ...init, headers: { ...init.headers, authorization: "Bearer " + adminKey, "content-type": "application/json" } });
try {
  // Administration (not the client's API): wait for ranges to have leaders, register, create an invoke key.
  await until(async () => (await (await adm("/cluster")).json()).partitions.every((p) => p.leader));
  await until(async () => (await adm("/deployments", { method: "POST", body: JSON.stringify({ uri: sdk.url }) })).ok);
  const key = (await (await adm("/admin/keys", { method: "POST", body: JSON.stringify({ tenant: "default", role: "invoke" }) })).json()).secret;

  const c = connect({ url, apiKey: key, attachWaitMs: 2000 });
  assert((await c.service("Smoke").echo(2)).x === 2, "invoke");
  assert((await c.service("Smoke").nap(800)) === "awake", "invoke via 202");
  try { await c.service("Smoke").echo(-1); assert(false, "must throw"); } catch (e) { assert(e instanceof InvocationFailed && e.code === 422, "failed"); }
  const h = await c.service("Smoke").send.nap(500);
  assert((await h.output()).ready === false, "output not done");
  assert((await h.attach()) === "awake", "attach");
  assert((await h.output()).value === "awake", "output done");
  const g = await c.object("Gate", "s").send.wait();
  await until(async () => (await g.status()).status === "suspended");
  await g.signal("ok", "approved");
  assert((await g.attach()) === "approved", "signal");
  for (const [k, act] of [["a", "resolve"], ["b", "reject"]]) {
    const w = await c.object("Gate", k).send.awk();
    await until(async () => awks.has(k) && (await w.status()).status === "suspended");
    if (act === "resolve") await c.awakeable(awks.get(k)).resolve("yes");
    else await c.awakeable(awks.get(k)).reject("no");
    assert((await w.attach()) === (act === "resolve" ? "yes" : "rejected: no"), "awakeable " + act);
  }
  const f = c.workflow("Flow", "u1");
  const run = f.run("an");
  await until(async () => !(await f.output()).ready && (await c.promise("Flow", "u1", "code").peek()).completed === false);
  await c.promise("Flow", "u1", "code").resolve("42");
  assert((await run) === "an:42", "workflow run");
  assert((await f.attach()) === "an:42", "workflow attach");
  assert((await f.output()).value === "an:42", "workflow output");
  try { await f.run("again"); assert(false, "must throw"); } catch (e) { assert(e instanceof AlreadyInvoked, "already_invoked"); }
  assert((await c.promise("Flow", "u2", "code").reject("broken")).completed === true, "promise reject");
  assert((await c.promise("Flow", "u2", "code").peek()).failure?.includes("broken"), "promise peek");
  console.log("SMOKE OK");
} finally {
  proc.kill("SIGKILL");
  await sdk.close();
}
`;

/** The package user's service definitions, for type checking. */
const SERVICES_TS = `
import { type Context, service, workflow } from "@cairnflow/sdk";
export const orders = service("Orders", {
  async submit(_ctx, input: { amount: number }) { return { ok: input.amount > 0 }; },
});
export const cart = service("Cart", {
  async add(ctx: Context, item: string) { ctx.set("last", item); return 1; },
}, { type: "VIRTUAL_OBJECT" });
export const signup = workflow("Signup", {
  async run(_ctx, email: string) { return email.length; },
});
`;

/** The caller only \`import type\`s the definitions — a wrong handler or wrong input is a \`tsc\` error. */
const CALLER_TS = `
import { connect } from "@cairnflow/sdk/client";
import type { cart, orders, signup } from "./services.js";
export async function main(): Promise<void> {
  const c = connect({ url: "http://localhost:8080" });
  const r: { ok: boolean } = await c.service<typeof orders>("Orders").submit({ amount: 1 });
  // @ts-expect-error wrong input type
  await c.service<typeof orders>("Orders").submit({ amount: "1" });
  // @ts-expect-error no such handler
  await c.service<typeof orders>("Orders").refund();
  const n: number = await c.object<typeof cart>("Cart", "u").add("apple");
  const len: number = await c.workflow<typeof signup>("Signup", "u").run("a@b");
  void [r, n, len];
}
`;

/** Only the \`./client\` subpath, no \`@types/node\`: compiles for browsers. */
const BROWSER_TS = `
import { connect, readStream } from "@cairnflow/sdk/client";
export async function main(): Promise<void> {
  const c = connect({ url: "https://cairn.example.com", apiKey: "cf_x" });
  const h = await c.service("Any").send.go!(1);
  for await (const e of readStream("https://cairn.example.com", h.id, "ui")) void e;
}
`;

main();
