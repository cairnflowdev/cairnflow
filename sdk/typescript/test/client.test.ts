// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * `readStream`: reads an invocation's stream over SSE, following `503` +
 * `leader`, `moved` (leader change), `reset` (a transient stream lost together with
 * the term: read again from the start), `410 stream_expired`, and a cut connection
 * (the SSE maximum lifetime): reconnect from the next offset.
 */
import assert from "node:assert/strict";
import * as http from "node:http";
import { after, test } from "node:test";

interface Ev {
  kind: string;
  offset?: number;
  durable?: boolean;
  data?: unknown;
  leader?: string;
  next?: number;
}

type ReadStream = (
  base: string,
  id: string,
  name: string,
  opts?: { from?: number },
) => AsyncGenerator<Ev>;

// The stream-reading API through a type cast: the test compiles against code that does not have it yet.
async function readStreamFn(): Promise<ReadStream> {
  const m = (await import("../src/index.ts")) as unknown as { readStream: ReadStream };
  return m.readStream;
}

type Script = (url: URL, req: http.IncomingMessage, res: http.ServerResponse) => void;

const servers: http.Server[] = [];
after(() => {
  for (const s of servers) s.close();
});

async function serve(script: Script): Promise<{ url: string; seen: string[] }> {
  const seen: string[] = [];
  const s = http.createServer((req, res) => {
    const url = new URL(req.url ?? "/", "http://x");
    seen.push(`${url.pathname}${url.search}|${req.headers["last-event-id"] ?? ""}`);
    script(url, req, res);
  });
  servers.push(s);
  await new Promise<void>((r) => s.listen(0, "127.0.0.1", () => r()));
  const a = s.address() as { port: number };
  return { url: `http://127.0.0.1:${a.port}`, seen };
}

function sse(res: http.ServerResponse, events: [string, unknown, number?][], end = true): void {
  res.writeHead(200, { "content-type": "text/event-stream" });
  res.write(": keepalive\n\n");
  for (const [event, data, id] of events) {
    res.write(`event: ${event}\n${id === undefined ? "" : `id: ${id}\n`}data: ${JSON.stringify(data)}\n\n`);
  }
  if (end) res.end();
}

async function collect(gen: AsyncGenerator<Ev>, max = 50): Promise<Ev[]> {
  const out: Ev[] = [];
  for await (const e of gen) {
    out.push(e);
    if (out.length >= max) break;
  }
  return out;
}

const ID = "ab".repeat(24);

test(
  "503 + leader ⇒ ask the leader; moved + reset ⇒ the transient stream is read again from 0 at the new leader",
  async () => {
    let calls = 0;
    const b = await serve((_url, _req, res) => {
      calls += 1;
      if (calls === 1) {
        sse(res, [
          ["record", { offset: 0, durable: false, data: { tok: 0 } }, 0],
          ["record", { offset: 1, durable: false, data: { tok: 1 } }, 1],
          ["moved", { leader: b.url }],
          ["reset", {}],
        ]);
      } else {
        sse(res, [
          ["record", { offset: 0, durable: false, data: { tok: "a" } }, 0],
          ["end", { next: 1 }],
        ]);
      }
    });
    const a = await serve((_url, _req, res) => {
      res.writeHead(503, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "not the leader", leader: b.url.replace("http://", "") }));
    });
    const readStream = await readStreamFn();
    const got = await collect(readStream(a.url, ID, "ui"));
    assert.deepEqual(
      got.map((e) => e.kind),
      ["record", "record", "moved", "reset", "record", "end"],
    );
    assert.deepEqual(got[4]!.data, { tok: "a" });
    assert.equal(a.seen.length, 1);
    assert.match(b.seen[1]!, /from=0/, `after reset read again from 0: ${b.seen}`);
  },
);

test(
  "durable stream: moved without reset ⇒ reconnect from the next offset; dropped connection ⇒ reconnect with Last-Event-ID",
  async () => {
    let calls = 0;
    const s = await serve((_url, _req, res) => {
      calls += 1;
      if (calls === 1) {
        sse(res, [["record", { offset: 0, durable: true, data: 1 }, 0], ["moved", { leader: s.url }]]);
      } else if (calls === 2) {
        // The connection's maximum lifetime: the server closes without `end`.
        sse(res, [["record", { offset: 1, durable: true, data: 2 }, 1]]);
      } else {
        sse(res, [["record", { offset: 2, durable: true, data: 3 }, 2], ["end", { next: 3 }]]);
      }
    });
    const readStream = await readStreamFn();
    const got = await collect(readStream(s.url, ID, "$log"));
    assert.deepEqual(
      got.filter((e) => e.kind === "record").map((e) => e.offset),
      [0, 1, 2],
    );
    assert.equal(got.at(-1)!.kind, "end");
    assert.match(s.seen[1]!, /from=1/, `${s.seen}`);
    assert.ok(/from=2/.test(s.seen[2]!) || s.seen[2]!.endsWith("|1"), `${s.seen}`);
  },
);

test("410 stream_expired ⇒ an error with a code", async () => {
  const s = await serve((_url, _req, res) => {
    res.writeHead(410, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "expired", code: "stream_expired" }));
  });
  const readStream = await readStreamFn();
  await assert.rejects(collect(readStream(s.url, ID, "ui")), (e: unknown) => {
    assert.equal((e as { code?: string }).code, "stream_expired");
    return true;
  });
});

/** Breaks the connection like a node under `kill -9`: no terminating chunk. */
function die(res: http.ServerResponse, events: [string, unknown, number?][]): void {
  sse(res, events, false);
  setTimeout(() => res.socket?.destroy(), 20);
}


test(
  "connection broken midway (node died) ⇒ reconnect, no throw; same stream lifetime ⇒ no duplicate records",
  async () => {
    let calls = 0;
    const s = await serve((url, _req, res) => {
      calls += 1;
      if (calls === 1) {
        die(res, [
          ["record", { offset: 0, durable: false, step: 3, attempt: 0, k: 0, data: "a" }, 0],
          ["record", { offset: 1, durable: false, step: 3, attempt: 0, k: 1, data: "b" }, 1],
        ]);
      } else {
        const from = Number(url.searchParams.get("from"));
        const all: [string, unknown, number?][] = [
          ["record", { offset: 0, durable: false, step: 3, attempt: 0, k: 0, data: "a" }, 0],
          ["record", { offset: 1, durable: false, step: 3, attempt: 0, k: 1, data: "b" }, 1],
          ["record", { offset: 2, durable: false, step: 3, attempt: 0, k: 2, data: "c" }, 2],
        ];
        sse(res, [...all.slice(from), ["end", { next: 3 }]]);
      }
    });
    const readStream = await readStreamFn();
    const got = await collect(readStream(s.url, ID, "ui"));
    assert.deepEqual(got.map((e) => e.kind), ["record", "record", "record", "end"]);
    assert.deepEqual(got.filter((e) => e.kind === "record").map((e) => e.data), ["a", "b", "c"]);
  },
);

test(
  "runtime restarted midway (new hub, new transient stream lifetime) ⇒ reset, then read again from 0",
  async () => {
    let calls = 0;
    const s = await serve((url, _req, res) => {
      calls += 1;
      // A new lifetime: the rerun regenerates the step from the start, different records at the same offset.
      const fresh: [string, unknown, number?][] = [
        ["record", { offset: 0, durable: false, step: 3, attempt: 1, k: 0, data: "x" }, 0],
        ["record", { offset: 1, durable: false, step: 3, attempt: 1, k: 1, data: "y" }, 1],
      ];
      if (calls === 1) {
        die(res, [
          ["record", { offset: 0, durable: false, step: 3, attempt: 0, k: 0, data: "a" }, 0],
          ["record", { offset: 1, durable: false, step: 3, attempt: 0, k: 1, data: "b" }, 1],
        ]);
      } else {
        sse(res, [...fresh.slice(Number(url.searchParams.get("from"))), ["end", { next: 2 }]]);
      }
    });
    const readStream = await readStreamFn();
    const got = await collect(readStream(s.url, ID, "ui"));
    assert.deepEqual(got.map((e) => e.kind), ["record", "record", "reset", "record", "record", "end"]);
    assert.deepEqual(got.slice(3, 5).map((e) => e.data), ["x", "y"]);
  },
);

test(
  "leader dead for good ⇒ go back to the origin node to ask for the new leader, never keep knocking on a dead address",
  async () => {
    const rec = (offset: number, data: string): [string, unknown, number] => [
      "record",
      { offset, durable: true, step: 1, attempt: 0, k: offset, data },
      offset,
    ];
    const c = await serve((url, _req, res) => {
      sse(res, [...[rec(0, "a"), rec(1, "b")].slice(Number(url.searchParams.get("from"))), ["end", { next: 2 }]]);
    });
    let dead: http.Server | undefined;
    const b = await serve((_url, _req, res) => {
      die(res, [rec(0, "a")]);
      setTimeout(() => dead?.close(), 25);
    });
    dead = servers.at(-1);
    let asked = 0;
    const a = await serve((_url, _req, res) => {
      asked += 1;
      res.writeHead(503, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "not the leader", leader: (asked === 1 ? b.url : c.url).replace("http://", "") }));
    });
    const readStream = await readStreamFn();
    const got = await collect(readStream(a.url, ID, "$log"));
    assert.deepEqual(got.map((e) => e.kind), ["record", "record", "end"]);
    assert.equal(asked, 2, "back to the origin node once more");
  },
);

/**
 * `readStream` following `moved` did not check the scheme. The leader advertises
 * `http://<internal address>`; a client speaking `https://…` would then send the next
 * request with `authorization: Bearer cf_…` in clear text to that address.
 * `Transport` already rejected a `503 + leader` with another scheme; the `moved` branch did not.
 *
 * Must see: a `moved` to another scheme is dropped — no connection to it, reading
 * continues at the origin node. (The test goes http → https because the test server
 * is http; the rule is the same.)
 */
test("moved to another scheme is not followed — reading continues at the origin node", async () => {
  const net = await import("node:net");
  let foreign = 0;
  const other = net.createServer((sock) => {
    foreign += 1;
    sock.destroy();
  });
  await new Promise<void>((r) => other.listen(0, "127.0.0.1", () => r()));
  const port = (other.address() as { port: number }).port;
  let calls = 0;
  try {
    const home = await serve((_url, _req, res) => {
      calls += 1;
      if (calls === 1) sse(res, [["moved", { leader: `https://127.0.0.1:${port}` }]]);
      else sse(res, [["end", { next: 0 }]]);
    });
    const readStream = await readStreamFn();
    const got = await collect(readStream(home.url, ID, "ui"));
    assert.deepEqual(got.map((e) => e.kind), ["moved", "end"]);
    assert.equal(foreign, 0, "opened a connection to a leader with another scheme");
  } finally {
    other.close();
  }
});

/**
 * A broken connection ⇒ reconnect with `from=last.offset` to compare the stream's
 * lifetime. If the last record has already left the transient stream's buffer, the
 * server returns a `gap` (from `last.offset`) and then a record with a larger offset —
 * the client took that as "a new lifetime", returned `reset` and read again from 0:
 * the UI cleared what it had shown. A `gap` covering the last record itself does not
 * prove a new lifetime.
 *
 * Must see: no `reset`; the `gap` reaches the reader; reading continues after the `gap`.
 */
test("reconnect after a break when the last record has left the buffer (gap) ⇒ no false reset", async () => {
  let calls = 0;
  const s = await serve((_url, _req, res) => {
    calls += 1;
    if (calls === 1) {
      die(res, [
        ["record", { offset: 0, durable: false, step: 1, attempt: 0, k: 0, data: "a" }, 0],
        ["record", { offset: 1, durable: false, step: 1, attempt: 0, k: 1, data: "b" }, 1],
      ]);
    } else {
      sse(res, [
        ["gap", { offset: 1, dropped: 3 }],
        ["record", { offset: 4, durable: false, step: 1, attempt: 0, k: 4, data: "e" }, 4],
        ["end", { next: 5 }],
      ]);
    }
  });
  const readStream = await readStreamFn();
  const got = await collect(readStream(s.url, ID, "ui"));
  assert.deepEqual(
    got.map((e) => e.kind),
    ["record", "record", "gap", "record", "end"],
  );
  assert.equal(got[3]!.offset, 4);
});
