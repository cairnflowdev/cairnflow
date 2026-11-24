// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { describe, expect, it, vi } from "vitest";
import { BulkRun, errorsCsv, fixedPages, type ItemResult, KEEP_ERRORS, PageError } from "./bulk";

const okAll = async (ids: string[]): Promise<ItemResult[]> =>
  ids.map((id) => ({ id, ok: true, status: 200 }));

describe("BulkRun", () => {
  it("runs every page, counts ok/failed, keeps ≤ 100 last errors", async () => {
    const ids = Array.from({ length: 250 }, (_, i) => `id${i}`);
    const run = new BulkRun({
      next: fixedPages(ids, 100),
      send: async (page) =>
        page.map((id, i) => ({ id, ok: i % 2 === 0, status: i % 2 === 0 ? 200 : 409, error: "x" })),
      onProgress: () => {},
    });
    const p = await run.run();
    expect(p.state).toBe("done");
    expect(p.done).toBe(250);
    expect(p.ok + p.failed).toBe(250);
    expect(p.errors.length).toBe(KEEP_ERRORS);
    expect(p.errors.at(-1)?.id).toBe("id249");
  });

  it("429 on a whole page ⇒ backs off by Retry-After then resends that same page", async () => {
    const sleep = vi.fn(async () => {});
    let calls = 0;
    const run = new BulkRun({
      next: fixedPages(["a", "b"]),
      send: async (page) => {
        calls++;
        if (calls === 1) throw new PageError("busy", 429, 2000);
        return okAll(page);
      },
      onProgress: () => {},
      sleep,
    });
    const p = await run.run();
    expect(sleep).toHaveBeenCalledWith(2000);
    expect(p).toMatchObject({ state: "done", done: 2, ok: 2 });
  });

  it("pause between pages, resume, cancel", async () => {
    const pages: string[][] = [];
    let gate: () => void = () => {};
    const run = new BulkRun({
      next: fixedPages(["a", "b", "c"], 1),
      send: async (page) => {
        pages.push(page);
        if (pages.length === 1) await new Promise<void>((r) => (gate = r));
        return okAll(page);
      },
      onProgress: () => {},
    });
    const done = run.run();
    await vi.waitFor(() => expect(pages.length).toBe(1));
    run.pause();
    gate();
    await vi.waitFor(() => expect(run.current.done).toBe(1));
    expect(pages.length).toBe(1);
    run.resume();
    await vi.waitFor(() => expect(pages.length).toBeGreaterThan(1));
    run.cancel();
    const p = await done;
    expect(["cancelled", "done"]).toContain(p.state);
  });

  it("cancel while paused ⇒ no further page is sent", async () => {
    const send = vi.fn(okAll);
    const run = new BulkRun({ next: fixedPages(["a", "b"], 1), send, onProgress: () => {} });
    run.pause();
    const done = run.run();
    run.cancel();
    expect((await done).state).toBe("cancelled");
    expect(send).not.toHaveBeenCalled();
  });

  it("an error other than 429 ⇒ stops in the error state, keeps the message", async () => {
    const run = new BulkRun({
      next: fixedPages(["a"]),
      send: async () => {
        throw new PageError("forbidden", 403, null);
      },
      onProgress: () => {},
    });
    const p = await run.run();
    expect(p.state).toBe("error");
    expect(p.errors[0]?.error).toBe("forbidden");
  });

  it("error CSV escapes quotes", () => {
    expect(errorsCsv([{ id: "a", status: 409, error: 'say "hi"' }])).toBe(
      'id,status,error\na,409,"say ""hi"""',
    );
  });
});
