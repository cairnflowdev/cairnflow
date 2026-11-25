// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * The runtime's request identity — SDK side.
 *
 * - Golden vector: a JWT signed by `cairn-server/src/identity.rs` (test
 *   `golden_vector_is_stable`) verifies here with Web Crypto — same
 *   base58, same JWT layout, same Ed25519.
 * - `serve({identityKeys})`: a request with a missing/wrong signature, wrong `aud`, expired, or
 *   carrying the `jti` of another invocation ⇒ `401`, the handler does not run; under both
 *   REQUEST_RESPONSE (h1) and BIDI_STREAM (h2c).
 */

import assert from "node:assert/strict";
import { type ChildProcess, spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { createServer, request as httpRequest } from "node:http";
import type { AddressInfo } from "node:net";
import * as http2 from "node:http2";
import { join } from "node:path";
import { after, test } from "node:test";
import { fileURLToPath } from "node:url";
import {
  base58Decode,
  type Endpoint,
  type Frame,
  FrameSplitter,
  IdentityError,
  IdentityVerifier,
  JWT_HEADER,
  MT,
  type ProtocolMode,
  SCHEME_HEADER,
  serve,
  service,
  type ServiceContext,
} from "../src/index.ts";
import { checkIdentity } from "../src/endpoint.ts";
import { bodyHash } from "../src/identity.ts";
import { Writer } from "../src/protobuf.ts";
import { encodeFrame, writeValue } from "../src/wire.ts";

/**
 * From `identity.rs::tests::golden_vector_is_stable` (master = `cf_test_golden_vector_master_key`,
 * tenant `acme`, host `sdk.example:8080`, body `golden-body`, nonce `golden-nonce-16b`). The SDK
 * ignores the `nonce` claim: it only makes each signed request's JWT distinct.
 */
const GOLDEN_KID = "publickeyv1_3CPYZuphHAqFNsix9y91xhW8L6Cn9sTuL2tfTVDhZJdr";
const GOLDEN_JWT =
  "eyJhbGciOiJFZERTQSIsImtpZCI6InB1YmxpY2tleXYxXzNDUFladXBoSEFxRk5zaXg5eTkxeGhXOEw2Q245c1R1TDJ0ZlRWRGhaSmRyIiwidHlwIjoiSldUIn0.eyJhdWQiOiIvaW52b2tlL1MvaCIsImJzaCI6IlpUUDBpVUVFWDhCT1otdzdycVh0RzFwZHVMSDJWdXFWUGtpQ1o0OXR1ZzAiLCJleHAiOjE3MDAwMDAwNjAsImhvc3QiOiJzZGsuZXhhbXBsZTo4MDgwIiwiaWF0IjoxNzAwMDAwMDAwLCJqdGkiOiJhYjowIiwibmJmIjoxNjk5OTk5OTQwLCJub25jZSI6IloyOXNaR1Z1TFc1dmJtTmxMVEUyWWciLCJzdWIiOiJhY21lIn0.ye5aaOY8bgpblEegb7oSCNtv_Ue0Lx56LK40CuSZC9XaXOb9IwBKHxiLhzf2y3UQkNyjf2rHDErF4DBul47yAg";
const GOLDEN_NOW = 1_700_000_000_123;

const headersOf =
  (h: Record<string, string>) =>
  (name: string): string | undefined =>
    h[name];

test("base58 decodes the Bitcoin alphabet correctly", () => {
  assert.deepEqual(base58Decode("StV1DL6CwTryKyV"), new TextEncoder().encode("hello world"));
  assert.deepEqual(base58Decode("112"), new Uint8Array([0, 0, 1]));
  assert.equal(base58Decode("0OIl"), null);
  assert.equal(base58Decode(GOLDEN_KID.slice("publickeyv1_".length))?.length, 32);
});

test("the runtime's golden vector verifies, with the right claims", async () => {
  const v = new IdentityVerifier([GOLDEN_KID]);
  const c = await v.verify(
    headersOf({ [SCHEME_HEADER]: "v1", [JWT_HEADER]: GOLDEN_JWT }),
    "/invoke/S/h",
    GOLDEN_NOW,
  );
  assert.deepEqual(
    { aud: c.aud, sub: c.sub, jti: c.jti, iat: c.iat, nbf: c.nbf, exp: c.exp, kid: c.kid, host: c.host, bsh: c.bsh },
    {
      aud: "/invoke/S/h",
      sub: "acme",
      jti: "ab:0",
      iat: 1_700_000_000,
      nbf: 1_699_999_940,
      exp: 1_700_000_060,
      kid: GOLDEN_KID,
      host: "sdk.example:8080",
      bsh: await bodyHash(new TextEncoder().encode("golden-body")),
    },
  );
});

test("the golden vector is rejected with a wrong path, expired, one byte changed, or an unknown kid", async () => {
  const v = new IdentityVerifier([GOLDEN_KID]);
  const ok = { [SCHEME_HEADER]: "v1", [JWT_HEADER]: GOLDEN_JWT };
  const reject = async (h: Record<string, string>, path: string, now: number) =>
    assert.rejects(v.verify(headersOf(h), path, now), IdentityError);
  await reject(ok, "/invoke/S/other", GOLDEN_NOW);
  await reject(ok, "/invoke/S/h", GOLDEN_NOW + 61_000);
  await reject(ok, "/invoke/S/h", GOLDEN_NOW - 61_000);
  const sig = GOLDEN_JWT.slice(-4);
  await reject({ ...ok, [JWT_HEADER]: GOLDEN_JWT.slice(0, -4) + (sig === "AAAA" ? "BBBB" : "AAAA") }, "/invoke/S/h", GOLDEN_NOW);
  await reject({ [JWT_HEADER]: GOLDEN_JWT }, "/invoke/S/h", GOLDEN_NOW);
  await reject({ [SCHEME_HEADER]: "v1" }, "/invoke/S/h", GOLDEN_NOW);
  // The key of another tenant (same shape, different public key).
  const other = new IdentityVerifier(["publickeyv1_2rAxB2eNFycA6hgt7trqW5UXrbn2yQvaK6TYfsZq3aBV"]);
  await assert.rejects(other.verify(headersOf(ok), "/invoke/S/h", GOLDEN_NOW), IdentityError);
  assert.throws(() => new IdentityVerifier(["not-a-kid"]), RangeError);
});

// ─────────────────────────────────────────── serve({identityKeys})


const enc = new TextEncoder();
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

function b64url(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString("base64url");
}

/** A signing key standing in for the runtime: `kid` and a function that signs a JWT with optional claims. */
async function signer(): Promise<{ kid: string; sign: (claims: Record<string, unknown>) => Promise<string> }> {
  const pair = (await crypto.subtle.generateKey({ name: "Ed25519" }, true, ["sign", "verify"])) as unknown as {
    publicKey: CryptoKey;
    privateKey: CryptoKey;
  };
  const raw = new Uint8Array(await crypto.subtle.exportKey("raw", pair.publicKey));
  const kid = `publickeyv1_${base58Encode(raw)}`;
  return {
    kid,
    sign: async (claims) => {
      const head = b64url(enc.encode(JSON.stringify({ alg: "EdDSA", typ: "JWT", kid })));
      const body = b64url(enc.encode(JSON.stringify(claims)));
      const sig = new Uint8Array(await crypto.subtle.sign({ name: "Ed25519" }, pair.privateKey, enc.encode(`${head}.${body}`)));
      return `${head}.${body}.${b64url(sig)}`;
    },
  };
}

function claimsFor(aud: string, jti: string, nowS = Math.floor(Date.now() / 1000)): Record<string, unknown> {
  return { aud, sub: "acme", iat: nowS, nbf: nowS - 60, exp: nowS + 60, jti };
}

const ID_A = new Uint8Array(24).fill(0xa1);
const ID_B = new Uint8Array(24).fill(0xb2);
const hex = (b: Uint8Array) => Buffer.from(b).toString("hex");

function startFrame(id: Uint8Array, retry: number): Uint8Array {
  return encodeFrame(MT.START, 0, new Writer().bytes(1, id).uint32(3, 1).uint32(7, retry).uint64(9, 1n).finish());
}

function inputFrame(value: unknown): Uint8Array {
  return encodeFrame(MT.INPUT, 0, new Writer().message(14, new Writer().bytes(1, enc.encode(JSON.stringify(value)))).finish());
}

let runs = 0;
const echo = service("S", {
  async echo(_ctx: ServiceContext, input: { v: number }) {
    runs++;
    return { v: input.v * 2 };
  },
});

const opened: Endpoint[] = [];
after(async () => {
  for (const ep of opened) await ep.close().catch(() => undefined);
});

async function up(mode: ProtocolMode, identityKeys?: string[]): Promise<Endpoint> {
  const ep = await serve({ services: [echo], port: 0, protocolMode: mode, ...(identityKeys ? { identityKeys } : {}) });
  opened.push(ep);
  return ep;
}

/** `POST /invoke/S/echo` over h1 with `jwt` (or without), body Start(`id`, `retry`) + Input. */
async function invokeH1(ep: Endpoint, jwt: string | undefined, id: Uint8Array, retry = 0): Promise<number> {
  const headers: Record<string, string> = { "content-type": "application/vnd.cairnflow.invocation.v1" };
  if (jwt !== undefined) {
    headers[SCHEME_HEADER] = "v1";
    headers[JWT_HEADER] = jwt;
  }
  const body = Buffer.concat([startFrame(id, retry), inputFrame({ v: 21 })]);
  const r = await fetch(`${ep.url}/invoke/S/echo`, { method: "POST", headers, body });
  await r.arrayBuffer();
  return r.status;
}

test("REQUEST_RESPONSE: a correct signature runs; missing, unknown key, wrong aud, expired, jti of another invocation ⇒ 401", async () => {
  const rt = await signer();
  const stranger = await signer();
  const ep = await up("REQUEST_RESPONSE", [rt.kid]);
  const path = "/invoke/S/echo";
  const jtiA = `${hex(ID_A)}:0`;

  const before = runs;
  assert.equal(await invokeH1(ep, await rt.sign(claimsFor(path, jtiA)), ID_A), 200);
  assert.equal(runs, before + 1);

  const now = Math.floor(Date.now() / 1000);
  for (const [why, jwt, id] of [
    ["missing header", undefined, ID_A],
    ["key not in identityKeys", await stranger.sign(claimsFor(path, jtiA)), ID_A],
    ["wrong aud", await rt.sign(claimsFor("/invoke/S/other", jtiA)), ID_A],
    ["expired", await rt.sign(claimsFor(path, jtiA, now - 3600)), ID_A],
    // A valid JWT of invocation A copied onto a request of invocation B.
    ["jti of another invocation", await rt.sign(claimsFor(path, jtiA)), ID_B],
  ] as const) {
    assert.equal(await invokeH1(ep, jwt, id), 401, why);
  }
  // Same invocation, another attempt: the jti carries the attempt number.
  assert.equal(await invokeH1(ep, await rt.sign(claimsFor(path, jtiA)), ID_A, 1), 401, "jti of another attempt");
  assert.equal(runs, before + 1, "the handler must not run for a rejected request");
});

test("/discover: with identityKeys ⇒ requires a signature (aud /discover, jti discover) and declares requestIdentity", async () => {
  const rt = await signer();
  const ep = await up("REQUEST_RESPONSE", [rt.kid]);
  assert.equal((await fetch(`${ep.url}/discover`)).status, 401);
  const r = await fetch(`${ep.url}/discover`, {
    headers: { [SCHEME_HEADER]: "v1", [JWT_HEADER]: await rt.sign(claimsFor("/discover", "discover")) },
  });
  assert.equal(r.status, 200);
  const m = (await r.json()) as { requestIdentity?: string };
  assert.equal(m.requestIdentity, "v1");
  // Without identityKeys: as before, nothing declared.
  const open = await up("REQUEST_RESPONSE");
  const plain = (await (await fetch(`${open.url}/discover`)).json()) as { requestIdentity?: string };
  assert.equal(plain.requestIdentity, undefined);
});

/** One h2c stream to `/invoke/S/echo`: (`:status`, the frames the SDK returns). */
async function invokeH2(ep: Endpoint, jwt: string | undefined, id: Uint8Array): Promise<{ status: number; frames: Frame[] }> {
  const client = http2.connect(ep.url);
  try {
    return await new Promise((resolve) => {
      const headers: Record<string, string> = {
        ":method": "POST",
        ":path": "/invoke/S/echo",
        "content-type": "application/vnd.cairnflow.invocation.v1",
      };
      if (jwt !== undefined) {
        headers[SCHEME_HEADER] = "v1";
        headers[JWT_HEADER] = jwt;
      }
      const stream = client.request(headers);
      const split = new FrameSplitter();
      const got: Frame[] = [];
      let status = 0;
      stream.on("response", (h) => {
        status = Number(h[":status"]);
      });
      // The body of a `401` is JSON, not frames.
      stream.on("data", (c: Buffer) => {
        if (status === 200) got.push(...split.push(new Uint8Array(c)));
      });
      const done = () => resolve({ status, frames: got });
      stream.on("end", done);
      stream.on("close", done);
      stream.on("error", done);
      stream.write(Buffer.from(startFrame(id, 0)));
      stream.end(Buffer.from(inputFrame({ v: 21 })));
    });
  } finally {
    client.close();
  }
}

test("BIDI_STREAM: a correct signature runs; missing signature ⇒ 401; jti of another invocation ⇒ stream closed, handler does not run", async () => {
  const rt = await signer();
  const ep = await up("BIDI_STREAM", [rt.kid]);
  const jtiA = `${hex(ID_A)}:0`;
  const before = runs;
  const ok = await invokeH2(ep, await rt.sign(claimsFor("/invoke/S/echo", jtiA)), ID_A);
  assert.equal(ok.status, 200);
  assert.deepEqual(ok.frames.map((f) => f.type), [MT.OUTPUT, MT.END]);
  assert.equal(runs, before + 1);

  assert.equal((await invokeH2(ep, undefined, ID_A)).status, 401);
  const copied = await invokeH2(ep, await rt.sign(claimsFor("/invoke/S/echo", jtiA)), ID_B);
  assert.ok(!copied.frames.some((f) => f.type === MT.OUTPUT), "a JWT copied onto another invocation must not run the handler");
  assert.equal(runs, before + 1);
});

// ─────────────────────────────────────────── body and host in the signature

const sha256b64 = async (bytes: Uint8Array): Promise<string> =>
  b64url(new Uint8Array(await crypto.subtle.digest("SHA-256", bytes as Uint8Array<ArrayBuffer>)));

/** The h1 body of `invokeH1`, with an arbitrary `input`. */
function bodyFor(id: Uint8Array, input: unknown): Buffer {
  return Buffer.concat([startFrame(id, 0), inputFrame(input)]);
}

async function postH1(ep: Endpoint, jwt: string, body: Buffer, host?: string): Promise<number> {
  const headers: Record<string, string> = {
    "content-type": "application/vnd.cairnflow.invocation.v1",
    [SCHEME_HEADER]: "v1",
    [JWT_HEADER]: jwt,
  };
  // `fetch` does not allow setting Host; `node:http` does.
  const u = new URL(`${ep.url}/invoke/S/echo`);
  return new Promise((resolve, reject) => {
    const req = httpRequest(
      { host: u.hostname, port: u.port, path: u.pathname, method: "POST", headers: { ...headers, host: host ?? u.host } },
      (res) => {
        res.resume();
        res.on("end", () => resolve(res.statusCode ?? 0));
      },
    );
    req.on("error", reject);
    req.end(body);
  });
}

/**
 * The identity JWT signs `aud` = path and `jti` = `<id>:<attempt>` — the body is bound only through
 * those two fields of `Start`. Anyone holding an unexpired JWT (another endpoint of the same tenant
 * with the same path, an intermediary's log) could send the real endpoint a **forged** body with
 * the same `id`/`retryCount`: the handler runs real side effects with a fake journal, input and
 * state.
 *
 * Must see: with a `bsh` claim (sha256 of the body before compression, base64url) ⇒ a
 * different body ⇒ 401; with a `host` claim ⇒ a request to another host ⇒ 401; a match ⇒ runs.
 */
test("JWT carries a body hash and host — a different body or host ⇒ 401 (REQUEST_RESPONSE)", async () => {
  const rt = await signer();
  const ep = await up("REQUEST_RESPONSE", [rt.kid]);
  const path = "/invoke/S/echo";
  const jti = `${hex(ID_A)}:0`;
  const real = bodyFor(ID_A, { v: 21 });
  const forged = bodyFor(ID_A, { v: 999 });
  const host = new URL(ep.url).host;
  const jwt = await rt.sign({ ...claimsFor(path, jti), bsh: await sha256b64(real), host });

  const before = runs;
  assert.equal(await postH1(ep, jwt, forged), 401, "forged body with the same id/retryCount");
  assert.equal(await postH1(ep, jwt, real, "prod.example:443"), 401, "JWT of another host");
  assert.equal(runs, before, "the handler must not run for a rejected request");
  assert.equal(await postH1(ep, jwt, real), 200);
  assert.equal(runs, before + 1);
});

test("BIDI_STREAM — a turn-opening body other than the one hashed in the JWT ⇒ stream closed, handler does not run", async () => {
  const rt = await signer();
  const ep = await up("BIDI_STREAM", [rt.kid]);
  const jti = `${hex(ID_A)}:0`;
  // The turn-opening body the runtime signed: Start + Input {v: 1}; the stream sends Input {v: 21}.
  const signed = bodyFor(ID_A, { v: 1 });
  const host = new URL(ep.url).host;
  const before = runs;
  const r = await invokeH2(
    ep,
    await rt.sign({ ...claimsFor("/invoke/S/echo", jti), bsh: await sha256b64(signed), host }),
    ID_A,
  );
  assert.ok(!r.frames.some((f) => f.type === MT.OUTPUT), "a turn-opening body that differs from the signature must not run the handler");
  assert.equal(runs, before);
  // The hash of exactly the body the stream sends ⇒ runs.
  const ok = await invokeH2(
    ep,
    await rt.sign({ ...claimsFor("/invoke/S/echo", jti), bsh: await sha256b64(bodyFor(ID_A, { v: 21 })), host }),
    ID_A,
  );
  assert.deepEqual(ok.frames.map((f) => f.type), [MT.OUTPUT, MT.END]);
});

// ─────────────────────────────────────────── no segment fetch before the body check

/**
 * Under BIDI, `JtiGate` used to release `Start` and each replayed frame to `runBidi` **before**
 * comparing `bsh` — only the last frame of the turn-opening part waited for the check. `runBidi`
 * loads `Start.segment_urls` and assembles replayed entries as soon as they arrive ⇒ the SDK would
 * `fetch` URLs written by the sender before knowing whether the body matches the signature (anyone
 * with an unexpired JWT of that very turn, within ≤ 120 s).
 *
 * Must see: `identityKeys` set, `segmentOrigins` not; a valid JWT (right jti)
 * whose `bsh` does not match the body; body = `Start`(known 3, `segment_urls` → the test
 * server) + Input + a segmented completion ⇒ the test server receives no GET,
 * the handler does not run.
 */
test("BIDI — no segment of the turn-opening body is fetched before the bsh check", async () => {
  let gets = 0;
  const srv = createServer((_req, res) => {
    gets++;
    res.writeHead(404);
    res.end();
  });
  await new Promise<void>((r) => srv.listen(0, "127.0.0.1", () => r()));
  const port = (srv.address() as AddressInfo).port;
  const rt = await signer();
  const ep = await up("BIDI_STREAM", [rt.kid]);
  const sha = new Uint8Array(32).fill(9);
  const gen = 5n;
  const startW = new Writer().bytes(1, ID_A).uint32(3, 3).uint32(7, 0).uint64(9, 1n);
  startW.message(20, new Writer().bytes(1, sha).fixed64(2, gen).string(3, `http://127.0.0.1:${port}/seg`));
  const opening = [
    encodeFrame(MT.START, 0, startW.finish()),
    inputFrame({ v: 1 }),
    encodeFrame(
      MT.GET_STATE_COMPLETION,
      0,
      new Writer().uint32(1, 1).message(5, writeValue(new Uint8Array(0), [{ sha256: sha, len: 100, gen }], 100)).finish(),
    ),
  ];
  const host = new URL(ep.url).host;
  const jwt = await rt.sign({
    ...claimsFor("/invoke/S/echo", `${hex(ID_A)}:0`),
    bsh: await sha256b64(new TextEncoder().encode("different body")),
    host,
  });
  const before = runs;
  const client = http2.connect(ep.url);
  try {
    await new Promise<void>((resolve) => {
      const stream = client.request({
        ":method": "POST",
        ":path": "/invoke/S/echo",
        "content-type": "application/vnd.cairnflow.invocation.v1",
        [SCHEME_HEADER]: "v1",
        [JWT_HEADER]: jwt,
      });
      stream.resume();
      stream.on("close", () => resolve());
      stream.on("error", () => resolve());
      for (const f of opening) stream.write(Buffer.from(f));
      // No third replayed frame is sent: the gate must hold everything until the body is complete.
      setTimeout(() => stream.close(), 300);
    });
    await new Promise((r) => setTimeout(r, 100));
  } finally {
    client.close();
    srv.closeAllConnections();
    srv.close();
  }
  assert.equal(gets, 0, "the SDK fetched a URL of the turn-opening body before the bsh check");
  assert.equal(runs, before);
});

// ─────────────────────────────────────────── one JWT, one BIDI stream

/**
 * Under BIDI only the turn-opening part is bound to the JWT (`bsh`); frames after it pass straight
 * through. An eavesdropper holding both the JWT and the turn-opening body (≤ 120 s) opens a second
 * stream, replays the turn-opening part verbatim and then sends forged frames (`SegmentUrls`, a
 * fake completion) — the handler runs again on fake data.
 *
 * Must see: the same JWT opening a BIDI stream a second time (identical body) ⇒ the stream is
 * rejected, the handler does not run a second time.
 */
test("one JWT opens only one BIDI stream", async () => {
  const rt = await signer();
  const ep = await up("BIDI_STREAM", [rt.kid]);
  const host = new URL(ep.url).host;
  const jwt = await rt.sign({
    ...claimsFor("/invoke/S/echo", `${hex(ID_A)}:0`),
    bsh: await sha256b64(bodyFor(ID_A, { v: 21 })),
    host,
  });
  const before = runs;
  const first = await invokeH2(ep, jwt, ID_A);
  assert.deepEqual(first.frames.map((f) => f.type), [MT.OUTPUT, MT.END]);
  const again = await invokeH2(ep, jwt, ID_A);
  assert.ok(!again.frames.some((f) => f.type === MT.OUTPUT), "a used JWT opened a second stream");
  assert.equal(runs, before + 1);
});

test("used-JWT ledger — rejected the second time; when full, expired entries are cleaned, still full ⇒ rejected", async () => {
  const { UsedTokens } = await import("../src/serve.ts");
  const u = new UsedTokens(2);
  assert.equal(u.claim("a", 100, 0), true);
  assert.equal(u.claim("a", 100, 0), false, "signature already used");
  assert.equal(u.claim("b", 100, 0), true);
  assert.equal(u.claim("c", 100, 50_000), false, "ledger full, no entry expired yet");
  assert.equal(u.claim("c", 200, 100_001), true, "an expired entry is cleaned");
});

/**
 * The used-JWT ledger was keyed by the signature **string**, while `atob`
 * ignores spare trailing bits and whitespace — one JWT had several spellings decoding to
 * the same signature, each opening a new stream.
 *
 * Must see: a JWT has opened a stream; the same JWT with the signature's last character changed to
 * a variant differing only in trailing bits, or with whitespace inserted into the signature ⇒ the
 * stream is rejected.
 */
test("base64url variants of the signature do not open another stream", async () => {
  const rt = await signer();
  const ep = await up("BIDI_STREAM", [rt.kid]);
  const host = new URL(ep.url).host;
  const jwt = await rt.sign({
    ...claimsFor("/invoke/S/echo", `${hex(ID_A)}:0`),
    bsh: await sha256b64(bodyFor(ID_A, { v: 21 })),
    host,
  });
  const before = runs;
  const first = await invokeH2(ep, jwt, ID_A);
  assert.deepEqual(first.frames.map((f) => f.type), [MT.OUTPUT, MT.END]);
  const ALPHA = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  const sig = jwt.split(".")[2]!;
  // 64 bytes = 512 bits; 86 chars × 6 = 516 bits ⇒ the last char's 4 trailing bits are spare.
  const last = ALPHA.indexOf(sig.at(-1)!);
  const twin = `${jwt.slice(0, -1)}${ALPHA[last ^ 0b0011]}`;
  const spaced = `${jwt.slice(0, -10)}    ${jwt.slice(-10)}`;
  for (const variant of [twin, spaced]) {
    const r = await invokeH2(ep, variant, ID_A);
    assert.ok(!r.frames.some((f) => f.type === MT.OUTPUT), `variant ${JSON.stringify(variant.slice(-12))} opened another stream`);
  }
  assert.equal(runs, before + 1);
});

// ─────────────────────────────────────────── host claim without a request host

/**
 * Without `identityHosts` the `host` claim was compared with the `Host`/`:authority`
 * header **of the request itself** — text written by the sender. A request carrying
 * no host (h2 without `:authority`, HTTP/1.0) skipped the comparison entirely:
 * endpoint A's JWT worked at B.
 *
 * Must see: a `host` claim present while the request has no host and there are no
 * `identityHosts` ⇒ rejected; with `identityHosts` ⇒ compared with that list.
 */
test("host claim present while the request has no host ⇒ rejected", async () => {
  const rt = await signer();
  const verifier = new IdentityVerifier([rt.kid]);
  const jti = `${hex(ID_A)}:0`;
  const jwt = await rt.sign({ ...claimsFor("/invoke/S/echo", jti), host: "a.example" });
  const header = headersOf({ [SCHEME_HEADER]: "v1", [JWT_HEADER]: jwt });
  const bare = await checkIdentity(verifier, header, "/invoke/S/echo", jti, {});
  assert.equal(bare.ok, false, "a request without a host passed the host check");
  const pinned = await checkIdentity(verifier, header, "/invoke/S/echo", jti, { hosts: ["b.example"] });
  assert.equal(pinned.ok, false, "identityHosts does not contain the JWT's host");
  const ok = await checkIdentity(verifier, header, "/invoke/S/echo", jti, { host: "a.example" });
  assert.equal(ok.ok, true);
});

// ─────────────────────────────────────────── against the real binary

const here = fileURLToPath(new URL(".", import.meta.url));
const binary = process.env.CAIRNFLOW_BINARY ?? join(here, "..", "..", "..", "target", "debug", "cairnflow");
const spawned: ChildProcess[] = [];
after(() => {
  for (const p of spawned) p.kill("SIGKILL");
});

async function runtime(): Promise<string> {
  const proc = spawn(binary, ["--main-store", "memory://", "--port", "0", "--no-auth", "--tick-interval-ms", "50"], {
    stdio: "pipe",
    env: { ...process.env, CAIRNFLOW_IDENTITY_MASTER: "master-of-identity-test-ts-longer-than-32-bytes" },
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
  "real binary: an SDK with identityKeys = the kid from GET /identity runs in both modes; a wrong kid ⇒ registration fails",
  { skip: existsSync(binary) ? false : "cairnflow binary not built" },
  async () => {
    const url = await runtime();
    const id = (await (await fetch(`${url}/identity`)).json()) as { keys: { kid: string; active: boolean }[] };
    const kid = id.keys.find((k) => k.active)!.kid;
    for (const mode of ["REQUEST_RESPONSE", "BIDI_STREAM"] as const) {
      const ep = await up(mode, [kid]);
      const reg = await fetch(`${url}/deployments`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ uri: ep.url }),
      });
      assert.equal(reg.status, 200, `${mode}: ${await reg.text()}`);
      const r = await fetch(`${url}/invoke/S/echo`, { method: "POST", body: JSON.stringify({ v: 4 }) });
      const v = (await r.json()) as { status: string; output?: string };
      assert.equal(v.status, "completed", `${mode}: ${JSON.stringify(v)}`);
      assert.deepEqual(JSON.parse(v.output!), { v: 8 });
    }
    const wrong = await signer();
    const ep = await up("REQUEST_RESPONSE", [wrong.kid]);
    const reg = await fetch(`${url}/deployments`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ uri: ep.url }),
    });
    assert.equal(reg.status, 400, "the SDK does not trust the runtime's key ⇒ /discover 401 ⇒ registration fails");
  },
);
