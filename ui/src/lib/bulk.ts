// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Bulk runner: the client coordinates in pages of ≤ 1 000 ids, each page **one**
// `POST /invocations/batch/{op}` call (the server runs 5 ids in parallel). Pause /
// resume / cancel between pages; `429` backs off by `Retry-After` and then resends
// that same page; keeps the last 100 errors. Pure logic — the UI and tests supply
// `pages` and `send`.
import { toCsv } from "./csv";

export const BULK_PAGE = 1_000;
export const KEEP_ERRORS = 100;
/** Above this threshold the confirm dialog states the estimated time. */
export const BULK_WARN_OVER = 5_000;

export type BulkOp = "cancel" | "purge" | "resume" | "restart";
export type ItemResult = { id: string; ok: boolean; status: number; error?: string };
export type BulkError = { id: string; status: number; error: string };

export type Progress = {
  done: number;
  ok: number;
  failed: number;
  errors: BulkError[];
  state: BulkState;
};
export type BulkState = "running" | "paused" | "cancelled" | "done" | "error";

/** Error sending a whole page (not a per-id error). `retryAfterMs` on `429`. */
export class PageError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly retryAfterMs: number | null,
  ) {
    super(message);
  }
}

export type BulkDeps = {
  /** Next page of ids (≤ `BULK_PAGE`); empty = done. */
  next: () => Promise<string[]>;
  send: (ids: string[]) => Promise<ItemResult[]>;
  onProgress: (p: Progress) => void;
  sleep?: (ms: number) => Promise<void>;
  maxRetries?: number;
};

export class BulkRun {
  private state: BulkState = "running";
  private wake: (() => void) | null = null;
  private progress: Progress = { done: 0, ok: 0, failed: 0, errors: [], state: "running" };

  constructor(private readonly deps: BulkDeps) {}

  pause() {
    if (this.state === "running") this.set("paused");
  }

  resume() {
    if (this.state === "paused") {
      this.set("running");
      this.wake?.();
    }
  }

  cancel() {
    if (this.state === "running" || this.state === "paused") {
      this.set("cancelled");
      this.wake?.();
    }
  }

  get current(): Progress {
    return this.progress;
  }

  private set(state: BulkState) {
    this.state = state;
    this.emit();
  }

  private emit() {
    this.progress = { ...this.progress, state: this.state };
    this.deps.onProgress(this.progress);
  }

  /** Waits while paused; `false` = cancelled. */
  private async gate(): Promise<boolean> {
    while (this.state === "paused") await new Promise<void>((r) => (this.wake = r));
    return this.state === "running";
  }

  async run(): Promise<Progress> {
    const sleep = this.deps.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
    const maxRetries = this.deps.maxRetries ?? 5;
    try {
      for (;;) {
        if (!(await this.gate())) return this.progress;
        const ids = await this.deps.next();
        if (ids.length === 0) break;
        let results: ItemResult[] | null = null;
        for (let attempt = 0; results === null; attempt++) {
          try {
            results = await this.deps.send(ids);
          } catch (e) {
            if (e instanceof PageError && e.status === 429 && attempt < maxRetries) {
              await sleep(e.retryAfterMs ?? 1000 * 2 ** attempt);
              if (!(await this.gate())) return this.progress;
              continue;
            }
            throw e;
          }
        }
        const failed = results.filter((r) => !r.ok);
        this.progress = {
          ...this.progress,
          done: this.progress.done + results.length,
          ok: this.progress.ok + results.length - failed.length,
          failed: this.progress.failed + failed.length,
          errors: [
            ...this.progress.errors,
            ...failed.map((r) => ({ id: r.id, status: r.status, error: r.error ?? "" })),
          ].slice(-KEEP_ERRORS),
        };
        this.emit();
      }
      this.set("done");
    } catch (e) {
      this.progress = {
        ...this.progress,
        errors: [
          ...this.progress.errors,
          { id: "", status: 0, error: e instanceof Error ? e.message : String(e) },
        ].slice(-KEEP_ERRORS),
      };
      this.set("error");
    }
    return this.progress;
  }
}

/** Pages of ids from a fixed id list (the selected rows). */
export function fixedPages(ids: string[], size = BULK_PAGE): () => Promise<string[]> {
  let at = 0;
  return async () => {
    const page = ids.slice(at, at + size);
    at += page.length;
    return page;
  };
}

/** Estimated time (seconds) from the number of items and the measured rate (items/second). */
export function estimateSeconds(total: number, perSecond = 50): number {
  return Math.ceil(total / perSecond);
}

/** CSV of the retained errors (download). */
export function errorsCsv(errors: BulkError[]): string {
  return toCsv(
    ["id", "status", "error"],
    errors.map((e) => [e.id, e.status, e.error]),
  );
}
