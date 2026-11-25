// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { queries } from "./queries";
import { resetForTest, run } from "./run";

const body = (rows: unknown[][], extra: object = {}) =>
  JSON.stringify({
    columns: [
      { name: "service", type: "Utf8" },
      { name: "n", type: "Int64" },
    ],
    rows,
    partial: false,
    warnings: [],
    read: 0,
    ms: 1,
    ...extra,
  });

describe("run", () => {
  beforeEach(() => resetForTest());
  afterEach(() => vi.unstubAllGlobals());

  it("sends sql + params, returns objects by column, keeps partial/warnings", async () => {
    const fetch = vi.fn(
      async (_req: Request) =>
        new Response(
          body([["Order", 3]], { partial: true, warnings: [{ range: 2, reason: "indexing" }] }),
          {
            headers: { "content-type": "application/json" },
          },
        ),
    );
    vi.stubGlobal("fetch", fetch);
    const out = await run<{ service: string; n: number }>(queries.topFailing(5));
    expect(out.rows).toEqual([{ service: "Order", n: 3 }]);
    expect(out.partial).toBe(true);
    expect(out.warnings).toEqual([{ range: 2, reason: "indexing" }]);
    const req = fetch.mock.calls[0]?.[0] as Request;
    expect(new URL(req.url).pathname).toBe("/query");
    const sent = await req.json();
    expect(sent.params).toEqual([5]);
    expect(sent.sql).toContain("LIMIT $1");
  });

  it("429 ⇒ backs off by Retry-After then retries", async () => {
    vi.useFakeTimers();
    let calls = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => {
        calls++;
        return calls === 1
          ? new Response(JSON.stringify({ error: "busy" }), {
              status: 429,
              headers: { "retry-after": "2" },
            })
          : new Response(body([]), { headers: { "content-type": "application/json" } });
      }),
    );
    const p = run(queries.invocationCounts());
    await vi.advanceTimersByTimeAsync(1999);
    expect(calls).toBe(1);
    await vi.advanceTimersByTimeAsync(1);
    await expect(p).resolves.toMatchObject({ rows: [] });
    expect(calls).toBe(2);
    vi.useRealTimers();
  });

  it("no more than 2 statements in flight", async () => {
    let live = 0;
    let peak = 0;
    const gates: (() => void)[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => {
        live++;
        peak = Math.max(peak, live);
        await new Promise<void>((r) => gates.push(r));
        live--;
        return new Response(body([]), { headers: { "content-type": "application/json" } });
      }),
    );
    const all = Promise.all([1, 2, 3, 4].map(() => run(queries.invocationCounts())));
    for (let i = 0; i < 4; i++) {
      await vi.waitFor(() => expect(gates.length).toBeGreaterThan(0));
      gates.shift()?.();
    }
    await all;
    expect(peak).toBe(2);
  });
});

describe("ok", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("an error keeps the server's message (body `{error}`), not just statusText", async () => {
    const { ApiError, ok, rest } = await import("./api");
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(JSON.stringify({ error: 'pointer of "Shop" is at dp_b' }), {
            status: 409,
            statusText: "Conflict",
            headers: { "content-type": "application/json" },
          }),
      ),
    );
    const e = await ok(rest.GET("/services")).catch((x: unknown) => x);
    expect(e).toBeInstanceOf(ApiError);
    expect((e as InstanceType<typeof ApiError>).status).toBe(409);
    expect((e as Error).message).toBe('pointer of "Shop" is at dp_b');
  });

  it("`503` carries the server's `leader`", async () => {
    const { ApiError, ok, rest } = await import("./api");
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(
            JSON.stringify({
              error: "this node is not the leader of partition 3; the leader is at 10.0.0.2:8080",
              leader: "10.0.0.2:8080",
              id: "ab",
            }),
            { status: 503, headers: { "content-type": "application/json" } },
          ),
      ),
    );
    const e = await ok(rest.GET("/services")).catch((x: unknown) => x);
    expect(e).toBeInstanceOf(ApiError);
    expect((e as InstanceType<typeof ApiError>).leader).toBe("10.0.0.2:8080");
  });

  it("every REST request asks for forwarding to the leader", async () => {
    const { ok, rest } = await import("./api");
    const seen: string[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (req: Request) => {
        seen.push(req.headers.get("x-cairn-forward") ?? "");
        return new Response("[]", { status: 200, headers: { "content-type": "application/json" } });
      }),
    );
    await ok(rest.GET("/services"));
    await ok(rest.POST("/invocations/{id}/resume", { params: { path: { id: "ab" } } })).catch(
      () => undefined,
    );
    expect(seen).toEqual(["leader", "leader"]);
  });
});
