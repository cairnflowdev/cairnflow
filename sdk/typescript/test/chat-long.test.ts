// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * A long chat on the real binary: 200 turns, a `UIMessage[]` history growing to
 * ≈ 8 MiB, a tenant with `max-payload=1 MiB`, both BIDI_STREAM and
 * REQUEST_RESPONSE, both the `proxy` and the `direct` read path.
 *
 * Only runs with `CAIRNFLOW_CHAT_LONG=1` (`scripts/chat-long.sh`, its own row in Ready —
 * not part of the `test/*.test.ts` batch).
 *
 * - `proxy`: a `memory://` node, the SDK downloads segments through `/values/v1/<token>`.
 * - `direct`: only with `CAIRNFLOW_CHAT_LONG_S3_URI` (`s3://bucket/prefix`, a
 *   prefix **of this run alone**) and the store's `AWS_*` in the environment:
 *   node `--main-store <uri> --value-read-path direct --value-public-endpoint
 *   $AWS_ENDPOINT`, the SDK downloads segments with the store's presigned GET. The test
 *   deletes nothing on the store: `scripts/chat-long.sh` provides a fresh prefix per run and deletes it afterwards.
 *
 * Each turn: `send(text)` = `ctx.get("messages")` (lazy) + append a ≈ 41 KiB
 * message + `ctx.set` (SDK v6 reuses the prefix's ref). `messages` (SHARED) returns
 * the whole history — output > cap ⇒ sealed, assembled by ingress. Reloads at turns
 * 50, 100, 150, 200.
 *
 * Must see: every turn `completed` with the right number of messages; every reload
 * returns every message exactly. Prints the measurements as one JSON line per path
 * (`chat-long:`).
 */

import assert from "node:assert/strict";
import { type ChildProcess, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { join } from "node:path";
import { after, test } from "node:test";
import { fileURLToPath } from "node:url";
import { serve } from "../src/serve.ts";
import { type Context, service } from "../src/index.ts";

const here = fileURLToPath(new URL(".", import.meta.url));
const binary = process.env.CAIRNFLOW_BINARY ?? join(here, "..", "..", "..", "target", "debug", "cairnflow");
const ON = process.env.CAIRNFLOW_CHAT_LONG === "1";
const S3_URI = process.env.CAIRNFLOW_CHAT_LONG_S3_URI;
const TURNS = Number(process.env.CAIRNFLOW_CHAT_LONG_TURNS ?? "200");
/** Characters per message: 200 messages ⇒ a JSON history of ≈ 8.02 MiB (> 8 MiB). */
const TEXT = Number(process.env.CAIRNFLOW_CHAT_LONG_TEXT ?? "42000");
const RELOAD_EVERY = 50;

const spawned: ChildProcess[] = [];
const closers: (() => Promise<void>)[] = [];
after(async () => {
  for (const p of spawned) p.kill("SIGKILL");
  for (const c of closers) await c();
});

async function runtime(args: readonly string[]): Promise<{ url: string; stop: () => Promise<void> }> {
  const proc = spawn(
    binary,
    ["--host", "127.0.0.1", "--port", "0", "--no-auth", "--tick-interval-ms", "50", ...args],
    // The node signs requests: wire v6 is only served by an SDK with `identityKeys` (or `segmentOrigins`).
    { stdio: "pipe", env: { ...process.env, CAIRNFLOW_IDENTITY_MASTER: "master-of-chat-long-test-ts-longer-than-32-bytes" } },
  );
  spawned.push(proc);
  let err = "";
  proc.stderr!.on("data", (c: Buffer) => {
    err = (err + c.toString()).slice(-4000);
  });
  const url = await new Promise<string>((resolve, reject) => {
    let buf = "";
    const t = setTimeout(() => reject(new Error(`node did not report its port:\n${buf}\n${err}`)), 60_000);
    proc.stdout!.on("data", (c: Buffer) => {
      buf += c.toString();
      const m = /http:\/\/[\d.]+:\d+/.exec(buf);
      if (m) {
        clearTimeout(t);
        resolve(m[0]);
      }
    });
    proc.on("exit", (code) => reject(new Error(`node exited early (${code}):\n${buf}\n${err}`)));
  });
  const stop = () =>
    new Promise<void>((ok) => {
      // Killed by a signal (`after` already SIGKILLed) leaves `exitCode` at `null`.
      if (proc.exitCode !== null || proc.signalCode !== null) return ok();
      proc.once("exit", () => ok());
      proc.kill("SIGTERM");
    });
  return { url, stop };
}

/** A deterministic pseudo-random string — no repetition, so nothing "compresses on our behalf". */
function noise(n: number, seed: number): string {
  let x = seed >>> 0;
  let s = "";
  for (let i = 0; i < n; i++) {
    x = (Math.imul(x, 1103515245) + 12345) >>> 0;
    s += String.fromCharCode(97 + ((x >>> 16) % 26));
  }
  return s;
}

type Msg = { id: string; role: "user"; text: string };

const chat = (name: string) =>
  service(
    name,
    {
      async send(ctx: Context, m: Msg) {
        const msgs = (await ctx.get<Msg[]>("messages")) ?? [];
        msgs.push(m);
        ctx.set("messages", msgs);
        return msgs.length;
      },
      async messages(ctx: Context) {
        return (await ctx.get<Msg[]>("messages")) ?? [];
      },
    },
    { type: "VIRTUAL_OBJECT", handlerTypes: { messages: "SHARED" } },
  );

/** Sum of the `/metrics` lines of `name` carrying every label in `labels`. */
async function metric(url: string, name: string, ...labels: string[]): Promise<number> {
  const text = await (await fetch(`${url}/metrics`)).text();
  let sum = 0;
  for (const line of text.split("\n")) {
    if ((line.startsWith(`${name}{`) || line.startsWith(`${name} `)) && labels.every((l) => line.includes(l))) {
      sum += Number(line.slice(line.lastIndexOf(" ") + 1)) || 0;
    }
  }
  return sum;
}

async function sql(url: string, q: string): Promise<unknown[][]> {
  const r = await fetch(`${url}/query`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ sql: q }),
  });
  const v = (await r.json()) as { rows?: unknown[][] };
  assert.equal(r.status, 200, JSON.stringify(v).slice(0, 400));
  return v.rows ?? [];
}

function quantile(xs: number[], q: number): number {
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor(q * s.length))] ?? 0;
}

async function run(path: "proxy" | "direct", nodeArgs: readonly string[]) {
  const { url, stop } = await runtime(nodeArgs);
  closers.push(stop);
  const limit = await fetch(`${url}/admin/tenants/default`, {
    method: "PUT",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ maxPayload: 1 << 20 }),
  });
  assert.equal(limit.status, 200, await limit.text());
  const id = (await (await fetch(`${url}/identity`)).json()) as { keys: { kid: string; active: boolean }[] };
  const identityKeys = id.keys.filter((k) => k.active).map((k) => k.kid);
  const report: Record<string, unknown>[] = [];
  for (const mode of ["BIDI_STREAM", "REQUEST_RESPONSE"] as const) {
    const name = `Chat${path === "proxy" ? "P" : "D"}${mode === "BIDI_STREAM" ? "B" : "R"}`;
    const ep = await serve({ services: [chat(name)], port: 0, protocolMode: mode, identityKeys });
    closers.push(() => ep.close());
    const reg = await fetch(`${url}/deployments`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ uri: ep.url }),
    });
    assert.equal(reg.status, 200, `${path} ${mode}: ${await reg.text()}`);
    const call = async (handler: string, body: unknown): Promise<unknown> => {
      const r = await fetch(`${url}/invoke/${name}/${handler}?key=c1`, { method: "POST", body: JSON.stringify(body) });
      let v = (await r.json()) as { status: string; output?: string; id?: string };
      for (let i = 0; i < 600 && v.status !== "completed" && v.status !== "failed" && v.id !== undefined; i++) {
        await new Promise((ok) => setTimeout(ok, 100));
        v = (await (await fetch(`${url}/invocations/${v.id}`)).json()) as typeof v;
      }
      assert.equal(v.status, "completed", `${path} ${mode} ${handler}: ${JSON.stringify(v).slice(0, 400)}`);
      return JSON.parse(v.output!) as unknown;
    };
    const sdkBytes = (dir: string) => metric(url, "cairnflow_sdk_bytes_total", `mode="${mode}"`, `dir="${dir}"`);
    const proxied = () => metric(url, "cairnflow_value_proxy_bytes_total");
    const proxied0 = await proxied();
    const toSdk0 = await sdkBytes("to_sdk");
    const fromSdk0 = await sdkBytes("from_sdk");
    const want: Msg[] = [];
    const lat: number[] = [];
    const reloads: number[] = [];
    const t0 = performance.now();
    for (let i = 0; i < TURNS; i++) {
      const m: Msg = { id: `m${i}`, role: "user", text: noise(TEXT, i + 1) };
      want.push(m);
      const s = performance.now();
      assert.equal(await call("send", m), i + 1, `${path} ${mode}: turn ${i}`);
      lat.push(performance.now() - s);
      if ((i + 1) % RELOAD_EVERY === 0 || i + 1 === TURNS) {
        const r0 = performance.now();
        const got = (await call("messages", null)) as Msg[];
        reloads.push(performance.now() - r0);
        assert.equal(got.length, want.length, `${path} ${mode}: reload at turn ${i + 1}`);
        assert.deepEqual(got, want, `${path} ${mode}: reload at turn ${i + 1}`);
      }
    }
    const secs = (performance.now() - t0) / 1000;
    const history = Buffer.byteLength(JSON.stringify(want));
    const segs = await sql(
      url,
      `SELECT count(*) FROM sys_value WHERE service = '${name}' AND key = 'c1' AND holder_kind = 'state'`,
    );
    const row = {
      path,
      mode,
      turns: TURNS,
      historyBytes: history,
      historySha: createHash("sha256").update(JSON.stringify(want)).digest("hex").slice(0, 16),
      seconds: Number(secs.toFixed(1)),
      turnMsP50: Math.round(quantile(lat, 0.5)),
      turnMsP95: Math.round(quantile(lat, 0.95)),
      turnMsMax: Math.round(Math.max(...lat)),
      reloadMs: reloads.map(Math.round),
      stateSegments: segs[0]?.[0],
      toSdkKiBPerTurn: Math.round(((await sdkBytes("to_sdk")) - toSdk0) / TURNS / 1024),
      fromSdkKiBPerTurn: Math.round(((await sdkBytes("from_sdk")) - fromSdk0) / TURNS / 1024),
      proxyMiB: Number((((await proxied()) - proxied0) / (1 << 20)).toFixed(1)),
    };
    console.log(`chat-long: ${JSON.stringify(row)}`);
    report.push(row);
    assert.ok(history >= 8 << 20 || TURNS < 200, `history of ${history} bytes did not reach 8 MiB`);
    if (path === "direct") assert.equal(row.proxyMiB, 0, "direct: no byte through the proxy route");
  }
  return report;
}

test(
  "long chat 200 turns × 8 MiB history, max-payload 1 MiB: proxy, BIDI and RR",
  {
    skip: !ON ? "CAIRNFLOW_CHAT_LONG=1 to run" : existsSync(binary) ? false : "cairnflow binary not built",
    timeout: 1_800_000,
  },
  async () => {
    await run("proxy", ["--main-store", "memory://", "--value-read-path", "proxy"]);
  },
);

test(
  "long chat 200 turns × 8 MiB history, max-payload 1 MiB: direct (store presign), BIDI and RR",
  {
    skip: !ON
      ? "CAIRNFLOW_CHAT_LONG=1 to run"
      : !existsSync(binary)
        ? "cairnflow binary not built"
        : S3_URI === undefined || process.env.AWS_ENDPOINT === undefined
          ? "direct needs CAIRNFLOW_CHAT_LONG_S3_URI and AWS_ENDPOINT (scripts/chat-long.sh)"
          : false,
    timeout: 1_800_000,
  },
  async () => {
    await run("direct", [
      "--main-store",
      S3_URI!,
      "--value-read-path",
      "direct",
      "--value-public-endpoint",
      process.env.AWS_ENDPOINT!,
    ]);
  },
);
