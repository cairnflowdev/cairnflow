// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * `ctx.artifact.put/get`: `put` sends an `ArtifactPut` frame (wire v4, side
 * channel) and returns a `{sha256, size, contentType}` ref computed locally; it is
 * **replay-aware** — a `put` at a position already in the journal is not sent
 * again (the previous run already PUT it and committed the row before the next
 * entry). `get` reads from the process's memory, otherwise from the configured source
 * (`GET /invocations/{id}/artifacts/{sha}`).
 */
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import * as http from "node:http";
import { after, test } from "node:test";
import { type Context, type Handler, runTurn } from "../src/index.ts";
import { eachField, Writer } from "../src/protobuf.ts";
import { decodeFrames, encodeFrame, type Frame, MT } from "../src/wire.ts";

const enc = new TextEncoder();

// The artifact API through a type cast: the test compiles against code that does not have it yet.
interface Ref {
  sha256: string;
  size: number;
  contentType: string;
}
interface Artifacts {
  put(bytes: Uint8Array | string, opts?: { contentType?: string }): Promise<Ref>;
  get(ref: Ref | string): Promise<Uint8Array>;
}
const art = (ctx: Context): Artifacts => (ctx as unknown as { artifact: Artifacts }).artifact;
async function configure(o: { baseUrl: string; headers?: Record<string, string> }): Promise<void> {
  const m = (await import("../src/index.ts")) as unknown as {
    configureArtifacts(o: { baseUrl: string; headers?: Record<string, string> }): void;
  };
  m.configureArtifacts(o);
}

const ID_HEX = "09".repeat(24);

function start(knownEntries: number): Uint8Array {
  return encodeFrame(
    MT.START,
    0,
    new Writer().bytes(1, new Uint8Array(24).fill(9)).uint32(3, knownEntries).uint64(9, 42n).finish(),
  );
}
function inputEntry(v: unknown): Uint8Array {
  return encodeFrame(
    MT.INPUT,
    0,
    new Writer().message(14, new Writer().bytes(1, enc.encode(JSON.stringify(v)))).finish(),
  );
}
function runEntry(name: string, cid: number): Uint8Array {
  return encodeFrame(MT.RUN, 0, new Writer().uint32(11, cid).string(12, name).finish());
}
function runCompletion(cid: number, v: unknown): Uint8Array {
  return encodeFrame(
    MT.RUN_COMPLETION,
    0,
    new Writer().uint32(1, cid).message(5, new Writer().bytes(1, enc.encode(JSON.stringify(v)))).finish(),
  );
}
function cat(...p: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(p.reduce((n, x) => n + x.length, 0));
  let at = 0;
  for (const x of p) {
    out.set(x, at);
    at += x.length;
  }
  return out;
}

interface Put {
  sha: string;
  content: string;
  ct: string;
}
function puts(frames: readonly Frame[]): Put[] {
  return frames
    .filter((f) => f.type === 0x000c)
    .map((f) => {
      const p: Put = { sha: "", content: "", ct: "" };
      eachField(f.payload, (field, _w, r) => {
        if (field === 1) p.sha = Buffer.from(r.bytes()).toString("hex");
        else if (field === 2) p.content = Buffer.from(r.bytes()).toString();
        else if (field === 3) p.ct = Buffer.from(r.bytes()).toString();
        else return false;
        return true;
      });
      return p;
    });
}

const sha = (s: string) => createHash("sha256").update(s).digest("hex");

test("put sends ArtifactPut before Output, returns a locally computed ref; get from memory", async () => {
  let ref: Ref | undefined;
  let back = "";
  const h: Handler = async (ctx) => {
    ref = await art(ctx).put("summary", { contentType: "text/plain" });
    back = Buffer.from(await art(ctx).get(ref)).toString();
    return ref.sha256;
  };
  const out = decodeFrames(await runTurn(h, cat(start(1), inputEntry(null)), 4));
  assert.deepEqual(ref, { sha256: sha("summary"), size: 7, contentType: "text/plain" });
  assert.equal(back, "summary");
  assert.deepEqual(puts(out), [{ sha: sha("summary"), content: "summary", ct: "text/plain" }]);
  const at = out.findIndex((f) => f.type === 0x000c);
  const output = out.findIndex((f) => f.type === MT.OUTPUT);
  assert.ok(at >= 0 && at < output, "ArtifactPut comes before Output");
});

test("replay-aware: a put before a command already in the journal is not sent again", async () => {
  const h: Handler = async (ctx) => {
    const r = await art(ctx).put(new Uint8Array([1, 2, 3]));
    await ctx.run("a", () => 1);
    const r2 = await art(ctx).put("after");
    return [r.sha256, r2.sha256];
  };
  const out = decodeFrames(
    await runTurn(h, cat(start(3), inputEntry(null), runEntry("a", 1), runCompletion(1, 1)), 4),
  );
  assert.deepEqual(
    puts(out).map((p) => p.content),
    ["after"],
    "only the put at the head of the journal is sent",
  );
});

test("wire < 4: put throws a clear error", async () => {
  let err: unknown;
  const h: Handler = async (ctx) => {
    try {
      await art(ctx).put("x");
    } catch (e) {
      err = e;
    }
    return 1;
  };
  await runTurn(h, cat(start(1), inputEntry(null)), 3);
  assert.match(String(err), /v4/);
});

const servers: http.Server[] = [];
after(() => {
  for (const s of servers) s.close();
});

test("get of a ref not in memory: GET /invocations/{id}/artifacts/{sha} at the configured source", async () => {
  const want = sha("from runtime");
  const seen: string[] = [];
  const s = http.createServer((req, res) => {
    seen.push(`${req.url}|${req.headers.authorization ?? ""}`);
    if (req.url === `/invocations/${ID_HEX}/artifacts/${want}`) {
      res.writeHead(200, { "content-type": "text/plain" });
      res.end("from runtime");
    } else {
      res.writeHead(404).end();
    }
  });
  servers.push(s);
  await new Promise<void>((r) => s.listen(0, "127.0.0.1", () => r()));
  await configure({
    baseUrl: `http://127.0.0.1:${(s.address() as { port: number }).port}`,
    headers: { authorization: "Bearer k" },
  });
  let got = "";
  let missing: unknown;
  const h: Handler = async (ctx) => {
    got = Buffer.from(await art(ctx).get(want)).toString();
    try {
      await art(ctx).get("00".repeat(32));
    } catch (e) {
      missing = e;
    }
    return 1;
  };
  await runTurn(h, cat(start(1), inputEntry(null)), 4);
  assert.equal(got, "from runtime");
  assert.ok(missing instanceof Error, "404 ⇒ error");
  assert.equal(seen[0], `/invocations/${ID_HEX}/artifacts/${want}|Bearer k`);
});

test("an artifact of exactly ARTIFACT_MAX_BYTES still fits in one frame", async () => {
  // Old bug: the content cap (16 MiB) equalled the frame cap (`MAX_FRAME_LEN`), but
  // `ArtifactPut` also carries the sha, the length and the content-type — an artifact
  // exactly at the cap (users chunk by the exported `ARTIFACT_MAX_BYTES`) passed
  // `put`'s check and then failed when encoding the frame (`FrameError`), retrying the
  // turn forever; on the server side, the 571 "over the cap" branch was never reached.
  const { ARTIFACT_MAX_BYTES, MAX_FRAME_LEN } = await import("../src/index.ts");
  const h: Handler = async (ctx) => {
    await art(ctx).put(new Uint8Array(ARTIFACT_MAX_BYTES), { contentType: "application/octet-stream" });
    return "ok";
  };
  const body = await runTurn(h, cat(start(1), inputEntry(null)), 4);
  const out = decodeFrames(body);
  const put = out.find((f) => f.type === 0x000c);
  assert.ok(put !== undefined, "no ArtifactPut");
  assert.ok(put.payload.length <= MAX_FRAME_LEN, `${put.payload.length} > ${MAX_FRAME_LEN}`);
  assert.ok(out.some((f) => f.type === MT.OUTPUT), "the turn did not reach Output");
});

/** A local server for `configureArtifacts`; `route` answers every request. */
async function source(route: http.RequestListener): Promise<{ url: string; seen: string[] }> {
  const seen: string[] = [];
  const s = http.createServer((req, res) => {
    seen.push(`${req.url}|${req.headers.authorization ?? ""}`);
    route(req, res);
  });
  servers.push(s);
  await new Promise<void>((r) => s.listen(0, "127.0.0.1", () => r()));
  return { url: `http://127.0.0.1:${(s.address() as { port: number }).port}`, seen };
}

/** What `get` gives for each ref, inside one turn: the bytes, or the error. */
async function getAll(refs: readonly (Ref | string)[]): Promise<(string | Error)[]> {
  const out: (string | Error)[] = [];
  const h: Handler = async (ctx) => {
    for (const r of refs) {
      try {
        out.push(Buffer.from(await art(ctx).get(r)).toString());
      } catch (e) {
        out.push(e instanceof Error ? e : new Error(String(e)));
      }
    }
    return 1;
  };
  await runTurn(h, cat(start(1), inputEntry(null)), 4);
  return out;
}

/**
 * `get` trusted whatever the source answered: other bytes than the ref's sha256, another
 * size, or an endless body (read whole into memory, with no time limit). The bytes were
 * also remembered under the sha, so every later `get` in the process returned them.
 * Must see: a mismatched or oversized body is an error, and nothing is remembered.
 */
test(
  "get checks the download against the ref: sha256, size, the artifact limit; a bad body is not remembered",
  async () => {
    const real = "the real artifact";
    const want = sha(real);
    let endless = 0;
    const src = await source((req, res) => {
      if (req.url?.endsWith(`/${want}`)) {
        res.writeHead(200).end("tampered bytes!!!");
      } else if (req.url?.endsWith(`/${sha("sized")}`)) {
        res.writeHead(200).end("sized");
      } else if (req.url?.endsWith(`/${sha("endless")}`)) {
        res.writeHead(200);
        const chunk = Buffer.alloc(1 << 20);
        const pump = (): void => {
          while (endless < 256 << 20 && res.write(chunk)) endless += chunk.length;
          if (endless < 256 << 20) res.once("drain", pump);
          else res.end();
        };
        pump();
      } else {
        res.writeHead(404).end();
      }
    });
    await configure({ baseUrl: src.url, headers: { authorization: "Bearer k" } });
    const got = await getAll([
      want,
      { sha256: want, size: real.length, contentType: "text/plain" },
      { sha256: sha("sized"), size: 99, contentType: "text/plain" },
      sha("endless"),
    ]);
    assert.ok(got[0] instanceof Error, `sha256 mismatch accepted: ${String(got[0])}`);
    assert.ok(got[1] instanceof Error, "a mismatched body was remembered under the sha");
    assert.ok(got[2] instanceof Error, "a size other than the ref's was accepted");
    assert.ok(got[3] instanceof Error, "a body over the artifact limit was accepted");
    assert.ok(endless < 64 << 20, `the SDK read ${endless} bytes of an endless body`);
  },
);

/**
 * The configured headers carry the tenant's key. Following a redirect sends them on to
 * wherever the source points; a ref that is not a sha256 builds another path at the source.
 * Must see: no redirect followed, no request for a malformed sha, a time limit on the request.
 */
test(
  "get follows no redirect, refuses a sha that is not 64 hex digits, and bounds the request in time",
  async () => {
    const elsewhere = await source((_req, res) => res.writeHead(200).end("elsewhere"));
    const want = sha("redirected");
    const src = await source((_req, res) => res.writeHead(302, { location: `${elsewhere.url}/x` }).end());
    await configure({ baseUrl: src.url, headers: { authorization: "Bearer k" } });
    const got = await getAll([want, "../../admin/keys", "AB".repeat(32)]);
    assert.ok(got[0] instanceof Error, "a redirect was followed");
    assert.deepEqual(elsewhere.seen, [], "the key reached another origin");
    assert.ok(got[1] instanceof Error && got[2] instanceof Error, "a malformed sha was requested");
    assert.equal(src.seen.length, 1, `requests for malformed shas: ${src.seen.join(", ")}`);
    // The request carries a time limit.
    const realFetch = globalThis.fetch;
    let signal: AbortSignal | undefined;
    globalThis.fetch = (async (_u: unknown, init?: RequestInit) => {
      signal = init?.signal ?? undefined;
      return new Response("timed", { status: 200 });
    }) as typeof fetch;
    try {
      await getAll([sha("timed")]);
    } finally {
      globalThis.fetch = realFetch;
    }
    assert.ok(signal instanceof AbortSignal, "no time limit on the artifact request");
  },
);
