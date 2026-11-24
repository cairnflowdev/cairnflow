// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * The SDK's Fetch API and Lambda adapters: a `REQUEST_RESPONSE` endpoint in
 * the form `(Request) => Promise<Response>`.
 *
 * - In process: routing by path suffix and `basePath`, one turn with real
 *   frames, request identity (`aud` is the **full** path the runtime sends),
 *   configuration errors, Lambda payload v2 with base64 bodies both ways.
 * - Against the real binary: the runtime has an identity master, the SDK is
 *   mounted under the `/api/cairn` prefix behind a `node:http` bridge (playing
 *   the serverless platform) — via Fetch and via Lambda — registers and calls a
 *   handler with `ctx.run` + `ctx.sleep` (several turns, the journal resent
 *   each turn).
 *
 * The `REQUEST_RESPONSE` conformance traces run through the adapter in
 * `conformance.test.ts`.
 */

import assert from "node:assert/strict";
import { type ChildProcess, spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { join } from "node:path";
import { gunzipSync } from "node:zlib";
import { after, test } from "node:test";
import { fileURLToPath } from "node:url";
import { fetchHandler, type LambdaHttpEvent, lambdaHandler, MAX_PROTOCOL_VERSION, service, type ServiceContext } from "../src/fetch.ts";
import { JWT_HEADER, SCHEME_HEADER } from "../src/identity.ts";
import { Writer } from "../src/protobuf.ts";
import { decodeFrames, encodeFrame, MT } from "../src/wire.ts";
import { type Bridge, bridge } from "./fetch-bridge.ts";

const enc = new TextEncoder();

let runs = 0;
const svc = service("S", {
  async echo(_ctx: ServiceContext, input: { v: number }) {
    runs++;
    return { v: input.v * 2 };
  },
  async big(_ctx: ServiceContext, input: { n: number }) {
    return { text: "cairnflow ".repeat(input.n) };
  },
  async slow(ctx: ServiceContext, input: { v: number }) {
    const a = await ctx.run("add", () => input.v + 1);
    await ctx.sleep(30);
    return { v: a * 2 };
  },
});

const ID_A = new Uint8Array(24).fill(0xa1);
const ID_B = new Uint8Array(24).fill(0xb2);
const hex = (b: Uint8Array) => Buffer.from(b).toString("hex");

function startFrame(id: Uint8Array, retry: number): Uint8Array {
  return encodeFrame(MT.START, 0, new Writer().bytes(1, id).uint32(3, 1).uint32(7, retry).uint64(9, 1n).finish());
}

function inputFrame(value: unknown): Uint8Array {
  return encodeFrame(MT.INPUT, 0, new Writer().message(14, new Writer().bytes(1, enc.encode(JSON.stringify(value)))).finish());
}

function turnBody(id: Uint8Array = ID_A, retry = 0): Buffer {
  return Buffer.concat([startFrame(id, retry), inputFrame({ v: 21 })]);
}

const CT = "application/vnd.cairnflow.invocation.v1";

function invokeRequest(url: string, headers: Record<string, string> = {}, body: Buffer = turnBody()): Request {
  return new Request(url, { method: "POST", headers: { "content-type": CT, ...headers }, body });
}

test("discover: REQUEST_RESPONSE manifest; routing by path suffix, basePath only accepts the exact prefix", async () => {
  const h = fetchHandler({ services: [svc] });
  for (const url of ["https://app.example/discover", "https://app.example/api/cairn/discover"]) {
    const r = await h(new Request(url));
    assert.equal(r.status, 200, url);
    assert.equal(r.headers.get("content-type"), "application/vnd.cairnflow.endpointmanifest.v1+json");
    const m = (await r.json()) as { protocolMode: string; maxProtocolVersion: number; services: { name: string }[] };
    assert.equal(m.protocolMode, "REQUEST_RESPONSE");
    // No identityKeys, no segmentOrigins ⇒ wire v6 (segmented values) is not served.
    assert.equal(m.maxProtocolVersion, Math.min(MAX_PROTOCOL_VERSION, 5));
    assert.deepEqual(m.services.map((s) => s.name), ["S"]);
  }
  assert.equal((await h(new Request("https://app.example/other"))).status, 404);
  assert.equal((await h(new Request("https://app.example/invoke/S/echo"))).status, 404, "GET /invoke");
  assert.equal((await h(invokeRequest("https://app.example/invoke/S/missing"))).status, 404);

  const based = fetchHandler({ services: [svc], basePath: "/api/cairn/" });
  assert.equal((await based(new Request("https://app.example/api/cairn/discover"))).status, 200);
  assert.equal((await based(new Request("https://app.example/other/discover"))).status, 404);
  assert.equal((await based(new Request("https://app.example/api/cairnx/discover"))).status, 404);
  assert.throws(() => fetchHandler({ services: [svc], maxProtocolVersion: 9 }), RangeError);
});

test("one turn through the adapter: Start + Input ⇒ Output + End, invocation content-type", async () => {
  const h = fetchHandler({ services: [svc] });
  const before = runs;
  const r = await h(invokeRequest("https://app.example/api/cairn/invoke/S/echo"));
  assert.equal(r.status, 200);
  assert.equal(r.headers.get("content-type"), "application/vnd.cairnflow.invocation.v1");
  const frames = decodeFrames(new Uint8Array(await r.arrayBuffer()));
  assert.deepEqual(frames.map((f) => f.type), [MT.OUTPUT, MT.END]);
  assert.equal(runs, before + 1);
});

// ─────────────────────────────────────────── request identity

const ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

function base58Encode(bytes: Uint8Array): string {
  let zeros = 0;
  while (zeros < bytes.length && bytes[zeros] === 0) zeros++;
  const digits: number[] = [];
  for (const b of bytes.subarray(zeros)) {
    let carry = b;
    for (let i = 0; i < digits.length; i++) {
      carry += digits[i]! << 8;
      digits[i] = carry % 58;
      carry = Math.floor(carry / 58);
    }
    while (carry > 0) {
      digits.push(carry % 58);
      carry = Math.floor(carry / 58);
    }
  }
  return "1".repeat(zeros) + digits.reverse().map((d) => ALPHABET[d]).join("");
}

const b64url = (b: Uint8Array) => Buffer.from(b).toString("base64url");

/** Signing key standing in for the runtime. */
async function signer(): Promise<{ kid: string; sign: (aud: string, jti: string) => Promise<string> }> {
  const pair = (await crypto.subtle.generateKey({ name: "Ed25519" }, true, ["sign", "verify"])) as unknown as {
    publicKey: CryptoKey;
    privateKey: CryptoKey;
  };
  const kid = `publickeyv1_${base58Encode(new Uint8Array(await crypto.subtle.exportKey("raw", pair.publicKey)))}`;
  return {
    kid,
    sign: async (aud, jti) => {
      const now = Math.floor(Date.now() / 1000);
      const head = b64url(enc.encode(JSON.stringify({ alg: "EdDSA", typ: "JWT", kid })));
      const body = b64url(enc.encode(JSON.stringify({ aud, sub: "acme", iat: now, nbf: now - 60, exp: now + 60, jti })));
      const sig = new Uint8Array(await crypto.subtle.sign({ name: "Ed25519" }, pair.privateKey, enc.encode(`${head}.${body}`)));
      return `${head}.${body}.${b64url(sig)}`;
    },
  };
}

test("identityKeys: aud is the full path the runtime sends; missing signature, aud with the prefix stripped, other jti ⇒ 401, handler does not run", async () => {
  const rt = await signer();
  const h = fetchHandler({ services: [svc], identityKeys: [rt.kid] });
  const url = "https://app.example/api/cairn/invoke/S/echo";
  const full = "/api/cairn/invoke/S/echo";
  const jtiA = `${hex(ID_A)}:0`;
  const signed = async (aud: string, jti: string) => ({ [SCHEME_HEADER]: "v1", [JWT_HEADER]: await rt.sign(aud, jti) });

  const before = runs;
  assert.equal((await h(invokeRequest(url, await signed(full, jtiA)))).status, 200);
  assert.equal(runs, before + 1);
  for (const [why, headers, body] of [
    ["missing signature", {}, turnBody()],
    ["aud with the prefix stripped", await signed("/invoke/S/echo", jtiA), turnBody()],
    ["jti of another invocation", await signed(full, jtiA), turnBody(ID_B)],
    ["jti of another attempt", await signed(full, jtiA), turnBody(ID_A, 1)],
  ] as const) {
    assert.equal((await h(invokeRequest(url, headers, body))).status, 401, why);
  }
  assert.equal(runs, before + 1, "the handler must not run for a rejected request");

  assert.equal((await h(new Request("https://app.example/api/cairn/discover"))).status, 401);
  const d = await h(new Request("https://app.example/api/cairn/discover", { headers: await signed("/api/cairn/discover", "discover") }));
  assert.equal(d.status, 200);
  assert.equal(((await d.json()) as { requestIdentity?: string }).requestIdentity, "v1");
});

test("lambdaHandler: payload v2, base64 bodies both ways", async () => {
  const lambda = lambdaHandler(fetchHandler({ services: [svc] }));
  const event = (method: string, rawPath: string, body?: Buffer): LambdaHttpEvent => ({
    rawPath,
    rawQueryString: "",
    headers: { host: "abc.lambda-url.us-east-1.on.aws", "content-type": CT },
    ...(body ? { body: body.toString("base64"), isBase64Encoded: true } : {}),
    requestContext: { http: { method } },
  });
  const d = await lambda(event("GET", "/discover"));
  assert.equal(d.statusCode, 200);
  assert.equal(d.isBase64Encoded, true);
  assert.equal((JSON.parse(Buffer.from(d.body, "base64").toString()) as { protocolMode: string }).protocolMode, "REQUEST_RESPONSE");
  const r = await lambda(event("POST", "/invoke/S/echo", turnBody()));
  assert.equal(r.statusCode, 200);
  assert.equal(r.headers["content-type"], CT);
  assert.deepEqual(
    decodeFrames(new Uint8Array(Buffer.from(r.body, "base64"))).map((f) => f.type),
    [MT.OUTPUT, MT.END],
  );
  // A body larger than one encoding chunk (32 KiB) goes through whole.
  const big = await lambda(event("POST", "/invoke/S/echo", Buffer.concat([startFrame(ID_A, 0), inputFrame({ v: 1, pad: "x".repeat(100_000) })])));
  assert.equal(big.statusCode, 200);
});

/**
 * The adapter builds the request URL from the client's `Host` header. A host that
 * carries `/`, `?`, `#`, `@`, a backslash or white space would change the path the
 * endpoint routes on (and compares with the JWT's `aud`) away from `rawPath`. The Go
 * adapter refuses such a host; this one must too.
 */
test(
  "lambdaHandler: a Host header that would change the path is refused",
  async () => {
    const lambda = lambdaHandler(fetchHandler({ services: [svc] }));
    for (const host of ["x.example/invoke/S/echo?", "x.example?y", "x.example#y", "user@x.example", "x.example\\y", "x .example"]) {
      const r = await lambda({
        rawPath: "/discover",
        rawQueryString: "",
        headers: { host },
        requestContext: { http: { method: "GET" } },
      });
      assert.equal(r.statusCode, 400, host);
      assert.match(Buffer.from(r.body, "base64").toString(), /invalid host header/, host);
    }
  },
);

/**
 * The runtime sends `accept-encoding: gzip` (the manifest declares
 * `requestCompression`), and `fetchHandler` compresses a compressible response
 * of ≥ 8 KiB. `lambdaHandler` copies response headers from a list — without
 * `content-encoding` the runtime reads gzip as a bare frame, the turn fails
 * deterministically, and the invocation is `paused` after `maxAttempts`.
 *
 * Must see: the Lambda response carries `content-encoding: gzip`, and the body
 * decompresses to exactly `[Output, End]`.
 */
test("lambdaHandler: a compressed response keeps content-encoding", async () => {
  const lambda = lambdaHandler(fetchHandler({ services: [svc] }));
  const body = Buffer.concat([startFrame(ID_A, 0), inputFrame({ n: 2000 })]);
  const r = await lambda({
    rawPath: "/invoke/S/big",
    rawQueryString: "",
    headers: { host: "abc.lambda-url.us-east-1.on.aws", "content-type": CT, "accept-encoding": "gzip" },
    body: body.toString("base64"),
    isBase64Encoded: true,
    requestContext: { http: { method: "POST" } },
  });
  assert.equal(r.statusCode, 200);
  assert.equal(r.headers["content-encoding"], "gzip", "compressed body without content-encoding");
  const raw = gunzipSync(Buffer.from(r.body, "base64"));
  assert.deepEqual(
    decodeFrames(new Uint8Array(raw)).map((f) => f.type),
    [MT.OUTPUT, MT.END],
  );
});

// ─────────────────────────────────────────── against the real binary

const here = fileURLToPath(new URL(".", import.meta.url));
const binary = process.env.CAIRNFLOW_BINARY ?? join(here, "..", "..", "..", "target", "debug", "cairnflow");
const spawned: ChildProcess[] = [];
const bridges: Bridge[] = [];
after(async () => {
  for (const p of spawned) p.kill("SIGKILL");
  for (const b of bridges) await b.close();
});

async function runtime(): Promise<string> {
  const proc = spawn(binary, ["--main-store", "memory://", "--port", "0", "--no-auth", "--tick-interval-ms", "50"], {
    stdio: "pipe",
    env: { ...process.env, CAIRNFLOW_IDENTITY_MASTER: "master-of-fetch-test-ts-longer-than-32-bytes" },
  });
  spawned.push(proc);
  proc.stderr!.resume();
  return new Promise((resolve, reject) => {
    let buf = "";
    const t = setTimeout(() => reject(new Error(`node did not report a port:\n${buf}`)), 30_000);
    proc.stdout!.on("data", (c: Buffer) => {
      buf += c.toString();
      const m = /http:\/\/[\d.]+:\d+/.exec(buf);
      if (m) {
        clearTimeout(t);
        resolve(m[0]);
      }
    });
    proc.on("exit", (code) => reject(new Error(`node exited early (${code}):\n${buf}`)));
  });
}

test(
  "real binary: Fetch and Lambda endpoints under /api/cairn, identity on, a multi-turn handler runs to a result",
  { skip: existsSync(binary) ? false : "cairnflow binary not built" },
  async () => {
    const url = await runtime();
    const id = (await (await fetch(`${url}/identity`)).json()) as { keys: { kid: string; active: boolean }[] };
    const kid = id.keys.find((k) => k.active)!.kid;
    for (const via of ["fetch", "lambda"] as const) {
      const name = via === "fetch" ? "F" : "L";
      const s = service(name, svc.handlers);
      const h = fetchHandler({ services: [s], identityKeys: [kid] });
      const b = await bridge(via, h, lambdaHandler(h));
      bridges.push(b);
      const reg = await fetch(`${url}/deployments`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ uri: `${b.origin}/api/cairn` }),
      });
      const regBody = (await reg.json()) as { protocolMode?: string; requestIdentity?: string };
      assert.equal(reg.status, 200, `${via}: ${JSON.stringify(regBody)}`);
      assert.equal(regBody.protocolMode, "REQUEST_RESPONSE");
      for (const [handler, want] of [
        ["echo", { v: 8 }],
        ["slow", { v: 10 }],
      ] as const) {
        const r = await fetch(`${url}/invoke/${name}/${handler}`, { method: "POST", body: JSON.stringify({ v: 4 }) });
        const v = (await r.json()) as { status: string; output?: string };
        assert.equal(v.status, "completed", `${via} ${handler}: ${JSON.stringify(v)}`);
        assert.deepEqual(JSON.parse(v.output!), want);
      }
    }
  },
);
