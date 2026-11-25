// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * gzip compression of REQUEST_RESPONSE bodies (modelled on Restate's
 * `lambdaCompression`), SDK side:
 *
 * - The manifest declares `requestCompression: "gzip"` by default; with
 *   `compression: false` it does not.
 * - An `/invoke` body with `content-encoding: gzip` is decompressed (the shared core,
 *   through `fetchHandler`, through `serve()` h1, and through the h2 branch of
 *   `serve()` — an edge forwarding h2 to the app); responses ≥ 8 KiB are compressed
 *   when the runtime invites it.
 * - An SDK that did not declare it but receives gzip ⇒ `415`, the handler does not run.
 * - Decompression has a cap on the decompressed size.
 */

import assert from "node:assert/strict";
import { request as h1request } from "node:http";
import { connect } from "node:http2";
import { test } from "node:test";
import { gunzipSync, gzipSync } from "node:zlib";
import { acceptsGzip, gunzip, gzip, gzipFrames } from "../src/endpoint.ts";
import { fetchHandler, service, type ServiceContext } from "../src/fetch.ts";
import { Writer } from "../src/protobuf.ts";
import { serve } from "../src/serve.ts";
import { decodeFrames, encodeFrame, MT } from "../src/wire.ts";

const enc = new TextEncoder();
const CT = "application/vnd.cairnflow.invocation.v1";

/** An endpoint not declaring compression (a variable, not a literal: runs even before the option exists). */
const OFF = { services: [] as ReturnType<typeof service>[], compression: false };

let runs = 0;
const svc = service("Z", {
  async echo(_ctx: ServiceContext, input: { v: number }) {
    runs++;
    return { v: input.v * 2 };
  },
  async big(_ctx: ServiceContext, _input: { v: number }) {
    runs++;
    return { s: "y".repeat(20_000) };
  },
});

OFF.services.push(svc);

function turnBody(pad = 16 << 10): Buffer {
  const id = new Uint8Array(24).fill(0xc3);
  const start = encodeFrame(MT.START, 0, new Writer().bytes(1, id).uint32(3, 1).uint32(7, 0).uint64(9, 1n).finish());
  // Input larger than the compression threshold: one padding JSON field.
  const input = encodeFrame(
    MT.INPUT,
    0,
    new Writer().message(14, new Writer().bytes(1, enc.encode(JSON.stringify({ v: 21, pad: "x".repeat(pad) })))).finish(),
  );
  return Buffer.concat([start, input]);
}

function outputOf(body: Uint8Array): string {
  const frames = decodeFrames(body);
  assert.deepEqual(
    frames.map((f) => f.type),
    [MT.OUTPUT, MT.END],
  );
  return Buffer.from(frames[0]!.payload).toString("latin1");
}

test("the manifest declares requestCompression: gzip by default, not with compression:false", async () => {
  const on = (await (await fetchHandler({ services: [svc] })(new Request("https://a.example/discover"))).json()) as Record<
    string,
    unknown
  >;
  assert.equal(on.requestCompression, "gzip");
  const off = (await (
    await fetchHandler(OFF)(new Request("https://a.example/discover"))
  ).json()) as Record<string, unknown>;
  assert.equal(off.requestCompression, undefined);
});

test("fetchHandler: a gzip body decodes correctly; a large response is compressed when invited, a small one goes plain", async () => {
  const h = fetchHandler({ services: [svc] });
  const before = runs;
  const r = await h(
    new Request("https://a.example/invoke/Z/echo", {
      method: "POST",
      headers: { "content-type": CT, "content-encoding": "gzip", "accept-encoding": "gzip" },
      body: gzipSync(turnBody()),
    }),
  );
  assert.equal(r.status, 200);
  assert.equal(r.headers.get("content-encoding"), null, "a small response goes plain");
  assert.match(outputOf(new Uint8Array(await r.arrayBuffer())), /"v":42/);
  assert.equal(runs, before + 1);

  const big = await h(
    new Request("https://a.example/invoke/Z/big", {
      method: "POST",
      headers: { "content-type": CT, "accept-encoding": "gzip" },
      body: turnBody(0),
    }),
  );
  assert.equal(big.status, 200);
  assert.equal(big.headers.get("content-encoding"), "gzip");
  const raw = gunzipSync(Buffer.from(await big.arrayBuffer()));
  assert.match(outputOf(raw), /y{20000}/);

  const plain = await h(
    new Request("https://a.example/invoke/Z/big", { method: "POST", headers: { "content-type": CT }, body: turnBody(0) }),
  );
  assert.equal(plain.headers.get("content-encoding"), null, "not invited ⇒ plain");
});

test("an SDK that did not declare it but receives gzip ⇒ 415, the handler does not run", async () => {
  const h = fetchHandler(OFF);
  const before = runs;
  const r = await h(
    new Request("https://a.example/invoke/Z/echo", {
      method: "POST",
      headers: { "content-type": CT, "content-encoding": "gzip" },
      body: gzipSync(turnBody()),
    }),
  );
  assert.equal(r.status, 415);
  assert.equal(runs, before);
});

test("serve(): a gzip body over h1 and over the h2 branch (an edge forwarding h2 to the app)", async () => {
  const ep = await serve({ services: [svc], port: 0 });
  try {
    const port = new URL(ep.url).port;
    const body = gzipSync(turnBody());
    // h1.
    const h1 = await new Promise<{ status: number; body: Buffer }>((resolve, reject) => {
      const req = h1request(
        {
          host: "127.0.0.1",
          port,
          method: "POST",
          path: "/invoke/Z/echo",
          headers: { "content-type": CT, "content-encoding": "gzip", "content-length": body.length },
        },
        (res) => {
          const chunks: Buffer[] = [];
          res.on("data", (c: Buffer) => chunks.push(c));
          res.on("end", () => resolve({ status: res.statusCode ?? 0, body: Buffer.concat(chunks) }));
        },
      );
      req.on("error", reject);
      req.end(body);
    });
    assert.equal(h1.status, 200);
    assert.match(outputOf(h1.body), /"v":42/);
    // h2 (prior knowledge, like an h2 → app edge).
    const session = connect(`http://127.0.0.1:${port}`);
    try {
      const h2 = await new Promise<{ status: number; body: Buffer }>((resolve, reject) => {
        const s = session.request({
          ":method": "POST",
          ":path": "/invoke/Z/echo",
          "content-type": CT,
          "content-encoding": "gzip",
        });
        let status = 0;
        const chunks: Buffer[] = [];
        s.on("response", (hd) => {
          status = Number(hd[":status"]);
        });
        s.on("data", (c: Buffer) => chunks.push(c));
        s.on("end", () => resolve({ status, body: Buffer.concat(chunks) }));
        s.on("error", reject);
        s.end(body);
      });
      assert.equal(h2.status, 200);
      assert.match(outputOf(h2.body), /"v":42/);
    } finally {
      session.close();
    }
  } finally {
    await ep.close();
  }
});

test("gunzip has a cap after decompression; gzip/gunzip round trip; accept-encoding", async () => {
  const z = gzipSync(Buffer.alloc(1 << 20));
  assert.equal(await gunzip(new Uint8Array(z), (1 << 20) - 1), null, "over the cap ⇒ null");
  assert.equal((await gunzip(new Uint8Array(z), 1 << 20))!.length, 1 << 20);
  const data = enc.encode(JSON.stringify({ a: "b".repeat(10_000) }));
  assert.deepEqual(await gunzip(await gzip(data), data.length), data);
  await assert.rejects(gunzip(enc.encode("not gzip"), 1 << 20));
  assert.equal(acceptsGzip("br, gzip;q=0.8"), true);
  assert.equal(acceptsGzip("identity"), false);
  assert.equal(acceptsGzip(undefined), false);
});

/**
 * No cross-frame compression in responses (against the BREACH class, the same rule
 * as on the runtime side): each frame is its own gzip member — `CompressionStream`
 * has no full flush. Two frames containing the same 6 KiB of random bytes ⇒ neither
 * frame references the other (≥ 2 × 6 KiB compressed); decompressing every member
 * yields exactly the bytes.
 */
test("gzipFrames: one member per frame, no cross references", async () => {
  const secret = new Uint8Array(6 << 10);
  let x = 0x9e3779b9;
  for (let i = 0; i < secret.length; i++) {
    x ^= x << 13;
    x ^= x >>> 17;
    x ^= x << 5;
    secret[i] = x & 0xff;
  }
  const body = Buffer.concat([encodeFrame(MT.OUTPUT, 0, secret), encodeFrame(MT.END, 0, secret)]);
  const z = (await gzipFrames(body))!;
  assert.ok(z.length >= 2 * secret.length, `cross-frame compression: ${z.length} bytes against ${secret.length}`);
  assert.deepEqual(new Uint8Array(gunzipSync(z)), new Uint8Array(body));
  const json = enc.encode(JSON.stringify({ step: 1, result: "ok".repeat(4_000) }));
  const two = Buffer.concat([encodeFrame(MT.OUTPUT, 0, json), encodeFrame(MT.END, 0, json)]);
  const zj = (await gzipFrames(two))!;
  assert.ok(zj.length * 5 < two.length, `${zj.length} against ${two.length}`);
  assert.deepEqual(new Uint8Array(gunzipSync(zj)), new Uint8Array(two));
  assert.equal(await gzipFrames(enc.encode("not a frame")), null);
});
