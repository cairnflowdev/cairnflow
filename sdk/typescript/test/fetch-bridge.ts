// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * A `node:http` → Fetch/Lambda adapter bridge for tests: it stands where the
 * serverless platform would (Vercel, Workers, Function URL) — takes the
 * runtime's real HTTP request, builds a `Request` (or a Lambda payload v2
 * event), calls the adapter, and returns the `Response` byte for byte. Not a
 * test file (`*.test.ts`).
 */

import { createServer, type IncomingMessage } from "node:http";
import { type FetchHandler, type LambdaHttpEvent, type LambdaHttpResult } from "../src/fetch.ts";

export interface Bridge {
  /** Origin of the bridge server (`http://127.0.0.1:<port>`), without a prefix. */
  readonly origin: string;
  close(): Promise<void>;
}

async function bodyOf(req: IncomingMessage): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const c of req) chunks.push(c as Buffer);
  return Buffer.concat(chunks);
}

/**
 * `via: "fetch"` calls `handler` directly; `via: "lambda"` goes through `lambda`
 * (usually `lambdaHandler(handler)`) with a base64 body, as a Function URL does.
 */
export async function bridge(
  via: "fetch" | "lambda",
  handler: FetchHandler,
  lambda?: (event: LambdaHttpEvent) => Promise<LambdaHttpResult>,
): Promise<Bridge> {
  const server = createServer((req, res) => {
    void (async () => {
      const method = req.method ?? "GET";
      const body = await bodyOf(req);
      const url = new URL(req.url ?? "/", `http://${req.headers.host ?? "127.0.0.1"}`);
      let status: number;
      let contentType: string;
      let out: Buffer;
      // Wire v5 control headers (`docs/protocol-v1.md` §9.3) pass through the bridge unchanged.
      const extra: Record<string, string> = {};
      const pass = (get: (h: string) => string | null | undefined) => {
        for (const h of ["cairn-replay", "cairn-replay-cache", "cairn-replay-held", "cairn-affinity"]) {
          const v = get(h);
          if (typeof v === "string") extra[h] = v;
        }
      };
      if (via === "fetch") {
        const headers = new Headers();
        for (const [k, v] of Object.entries(req.headers)) {
          if (typeof v === "string") headers.set(k, v);
        }
        const r = await handler(
          new Request(url, { method, headers, ...(method === "GET" || method === "HEAD" ? {} : { body }) }),
        );
        status = r.status;
        contentType = r.headers.get("content-type") ?? "application/octet-stream";
        pass((h) => r.headers.get(h));
        out = Buffer.from(await r.arrayBuffer());
      } else {
        const headers: Record<string, string> = {};
        for (const [k, v] of Object.entries(req.headers)) {
          if (typeof v === "string") headers[k] = v;
        }
        const r = await lambda!({
          rawPath: url.pathname,
          rawQueryString: url.search.replace(/^\?/, ""),
          headers,
          ...(body.length > 0 ? { body: body.toString("base64"), isBase64Encoded: true } : {}),
          requestContext: { http: { method } },
        });
        status = r.statusCode;
        contentType = r.headers["content-type"] ?? "application/octet-stream";
        pass((h) => r.headers[h]);
        out = Buffer.from(r.body, "base64");
      }
      res.writeHead(status, { "content-type": contentType, "content-length": String(out.length), ...extra });
      res.end(out);
    })().catch((e: unknown) => {
      if (!res.headersSent) res.writeHead(502, { "content-type": "text/plain" });
      res.end(`bridge failed: ${e instanceof Error ? e.message : String(e)}`);
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const addr = server.address();
  const port = typeof addr === "object" && addr !== null ? addr.port : 0;
  return {
    origin: `http://127.0.0.1:${port}`,
    close: () =>
      new Promise<void>((resolve) => {
        server.closeAllConnections();
        server.close(() => resolve());
      }),
  };
}
