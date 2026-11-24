// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Bulk action bar of the list. Select rows, or "every invocation matching the
// filters" (counted first, question 33); each page of ≤ 1 000 ids is one `batch`
// call. Cancel and purge are type-to-confirm; above 5 000 items the estimated time
// is stated; while running, leaving the page warns.
import { useQueryClient } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";
import { actionError } from "@/components/cairn/confirm";
import { shorten } from "@/components/cairn/id";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { ApiError, ok, rest } from "@/data/api";
import { type Cursor, type InvocationRow, nextCursor, page } from "@/data/pages";
import { type InvocationFilter, invocationList } from "@/data/queries";
import { run } from "@/data/run";
import {
  BULK_PAGE,
  BULK_WARN_OVER,
  type BulkOp,
  BulkRun,
  errorsCsv,
  estimateSeconds,
  fixedPages,
  type ItemResult,
  PageError,
  type Progress,
} from "@/lib/bulk";
import { m } from "@/paraglide/messages";
import { getLocale } from "@/paraglide/runtime";

const OPS: BulkOp[] = ["cancel", "resume", "restart", "purge"];
const DESTRUCTIVE: BulkOp[] = ["cancel", "purge"];

const LABEL: Record<BulkOp, () => string> = {
  cancel: m.bulk_cancel,
  resume: m.action_resume,
  restart: m.bulk_restart,
  purge: m.action_purge,
};

/** Pages of ids for "every invocation matching the filters" — paged by timestamp like the list. */
function matchingPages(filter: InvocationFilter): () => Promise<string[]> {
  let cursor: Cursor | null = null;
  let done = false;
  return async () => {
    if (done) return [];
    const extra = cursor?.seen.length ?? 0;
    const out = await run<InvocationRow>(
      invocationList(filter, "created", BULK_PAGE + extra, cursor?.atMs),
    );
    const rows = page(out.rows, "created", cursor, BULK_PAGE);
    const next = nextCursor(rows, "created", cursor, rows.length === BULK_PAGE);
    if (!next) done = true;
    cursor = next;
    return rows.map((r) => r.id);
  };
}

/** How to cancel: cooperative (`mode=signal`, the handler cleans up) or immediate (572). */
type CancelMode = "signal" | "hard";

async function send(op: BulkOp, ids: string[], mode: CancelMode): Promise<ItemResult[]> {
  try {
    const query = op === "cancel" && mode === "signal" ? { mode: "signal" as const } : {};
    const out = await ok(
      rest.POST("/invocations/batch/{op}", { params: { path: { op }, query }, body: { ids } }),
    );
    return out.results.map((r) => ({
      id: r.id,
      ok: r.ok,
      status: r.status,
      // A per-id `503`: the node serving the console cannot reach the leader of that
      // id's range (the batch asked for forwarding); `504`: it may have run.
      error:
        r.status === 503 ? m.bulk_not_leader() : r.status === 504 ? m.action_ambiguous() : r.error,
    }));
  } catch (e) {
    if (e instanceof ApiError) throw new PageError(actionError(e), e.status, e.retryAfterMs);
    throw e;
  }
}

export function BulkBar({
  selected,
  total,
  filter,
  onClear,
}: {
  selected: string[];
  /** Number of invocations matching the filters (≤ 10 000), if counted. */
  total: number | undefined;
  filter: InvocationFilter;
  onClear: () => void;
}) {
  const [all, setAll] = useState(false);
  const [op, setOp] = useState<BulkOp | null>(null);
  const count = all ? (total ?? 0) : selected.length;
  if (selected.length === 0) return null;
  const fmt = new Intl.NumberFormat(getLocale());
  return (
    <section
      aria-label={m.bulk_title()}
      className="flex flex-wrap items-center gap-2 rounded-md border border-border bg-muted px-3 py-2"
    >
      <span className="font-medium">{m.bulk_selected({ n: fmt.format(count) })}</span>
      {!all && total !== undefined && total > selected.length ? (
        <Button variant="link" size="sm" onClick={() => setAll(true)}>
          {m.bulk_select_all({ n: fmt.format(total) })}
        </Button>
      ) : null}
      <div className="ml-auto flex flex-wrap gap-2">
        {OPS.map((o) => (
          <Button
            key={o}
            size="sm"
            variant={DESTRUCTIVE.includes(o) ? "destructive" : "outline"}
            onClick={() => setOp(o)}
          >
            {LABEL[o]()}
          </Button>
        ))}
        <Button
          size="sm"
          variant="ghost"
          onClick={() => {
            setAll(false);
            onClear();
          }}
        >
          {m.bulk_clear()}
        </Button>
      </div>
      {op ? (
        <BulkDialog
          op={op}
          count={count}
          pages={() => (all ? matchingPages(filter) : fixedPages(selected))}
          onClose={(finished) => {
            setOp(null);
            if (finished) {
              setAll(false);
              onClear();
            }
          }}
        />
      ) : null}
    </section>
  );
}

function BulkDialog({
  op,
  count: initialCount,
  pages,
  onClose,
}: {
  op: BulkOp;
  count: number;
  pages: () => () => Promise<string[]>;
  onClose: (finished: boolean) => void;
}) {
  const qc = useQueryClient();
  // Pin the item count when opening: after the run the list reloads and the filter's total changes.
  const [count] = useState(initialCount);
  const [typed, setTyped] = useState("");
  // The safer option comes first and is preselected.
  const [mode, setMode] = useState<CancelMode>("signal");
  const [progress, setProgress] = useState<Progress | null>(null);
  const runRef = useRef<BulkRun | null>(null);
  const running = progress?.state === "running" || progress?.state === "paused";
  const confirmWord = String(count);
  const needType = op === "cancel" ? mode === "hard" : DESTRUCTIVE.includes(op);
  const action =
    op !== "cancel"
      ? LABEL[op]()
      : mode === "signal"
        ? m.action_cancel_soft()
        : m.action_cancel_hard();
  const ready = !needType || typed.trim() === confirmWord;

  useEffect(() => {
    if (!running) return;
    const warn = (e: BeforeUnloadEvent) => e.preventDefault();
    window.addEventListener("beforeunload", warn);
    return () => window.removeEventListener("beforeunload", warn);
  }, [running]);

  const start = async () => {
    const r = new BulkRun({
      next: pages(),
      send: (ids) => send(op, ids, mode),
      onProgress: setProgress,
    });
    runRef.current = r;
    await r.run();
    await qc.invalidateQueries();
  };
  const fmt = new Intl.NumberFormat(getLocale());
  const csv =
    progress && progress.errors.length > 0
      ? URL.createObjectURL(new Blob([errorsCsv(progress.errors)]))
      : null;
  useEffect(() => () => void (csv && URL.revokeObjectURL(csv)), [csv]);

  return (
    <Dialog open onOpenChange={(o) => !o && !running && onClose(progress !== null)}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{m.bulk_dialog_title({ op: action, n: fmt.format(count) })}</DialogTitle>
          <DialogDescription>{m.bulk_dialog_body()}</DialogDescription>
        </DialogHeader>
        {progress === null ? (
          <form
            className="flex flex-col gap-3"
            onSubmit={(e) => {
              e.preventDefault();
              if (ready) void start();
            }}
          >
            {op === "cancel" ? (
              <fieldset className="flex flex-col gap-2">
                <legend className="sr-only">{m.bulk_cancel_how()}</legend>
                {(["signal", "hard"] as const).map((v) => (
                  <label key={v} className="flex items-start gap-2">
                    <input
                      type="radio"
                      name="bulk-cancel-mode"
                      className="mt-1 accent-primary"
                      checked={mode === v}
                      onChange={() => {
                        setMode(v);
                        setTyped("");
                      }}
                    />
                    <span className="flex flex-col">
                      <span className="font-medium">
                        {v === "signal" ? m.action_cancel_soft() : m.action_cancel_hard()}
                      </span>
                      <span className="text-sm text-muted-foreground">
                        {v === "signal" ? m.action_cancel_soft_body() : m.action_cancel_hard_body()}
                      </span>
                    </span>
                  </label>
                ))}
              </fieldset>
            ) : null}
            {count > BULK_WARN_OVER ? (
              <p role="note">{m.bulk_estimate({ s: fmt.format(estimateSeconds(count)) })}</p>
            ) : null}
            {needType ? (
              <>
                <Label htmlFor="bulk-confirm">{m.confirm_type({ text: confirmWord })}</Label>
                <Input
                  id="bulk-confirm"
                  autoComplete="off"
                  value={typed}
                  onChange={(e) => setTyped(e.target.value)}
                  className="font-mono"
                />
              </>
            ) : null}
            <DialogFooter>
              <Button type="button" variant="outline" onClick={() => onClose(false)}>
                {m.confirm_cancel()}
              </Button>
              <Button
                type="submit"
                variant={needType ? "destructive" : "default"}
                disabled={!ready}
              >
                {action}
              </Button>
            </DialogFooter>
          </form>
        ) : (
          <div className="flex flex-col gap-3">
            <progress
              className="w-full"
              max={Math.max(count, progress.done)}
              value={progress.done}
              aria-label={m.bulk_progress()}
            />
            <p role="status" aria-live="polite">
              {m.bulk_status({
                done: fmt.format(progress.done),
                n: fmt.format(count),
                ok: fmt.format(progress.ok),
                failed: fmt.format(progress.failed),
              })}{" "}
              · {m[`bulk_state_${progress.state}`]()}
            </p>
            {progress.errors.length > 0 ? (
              <details>
                <summary>{m.bulk_errors({ n: String(progress.errors.length) })}</summary>
                <ul className="max-h-40 overflow-auto font-mono text-xs">
                  {progress.errors.map((e) => (
                    <li key={`${e.id}-${e.status}-${e.error}`}>
                      {shorten(e.id)} {e.status}: {e.error}
                    </li>
                  ))}
                </ul>
                {csv ? (
                  <a
                    href={csv}
                    download="bulk-errors.csv"
                    className="text-primary underline-offset-4 hover:underline"
                  >
                    {m.bulk_download_errors()}
                  </a>
                ) : null}
              </details>
            ) : null}
            {progress.state === "error" ? (
              <p role="alert" className="text-destructive">
                {actionError(new Error(progress.errors.at(-1)?.error))}
              </p>
            ) : null}
            <DialogFooter>
              {progress.state === "running" ? (
                <Button variant="outline" onClick={() => runRef.current?.pause()}>
                  {m.bulk_pause()}
                </Button>
              ) : null}
              {progress.state === "paused" ? (
                <Button variant="outline" onClick={() => runRef.current?.resume()}>
                  {m.bulk_resume()}
                </Button>
              ) : null}
              {running ? (
                <Button variant="destructive" onClick={() => runRef.current?.cancel()}>
                  {m.bulk_stop()}
                </Button>
              ) : (
                <Button onClick={() => onClose(true)}>{m.bulk_close()}</Button>
              )}
            </DialogFooter>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}
