// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Tabs of the invocation detail: Journal (two tiers), Log (durable records,
// polled through `/query`), Tree, State/Promise, Artifact.
import { useQuery } from "@tanstack/react-query";
import { Link, useNavigate } from "@tanstack/react-router";
import { ChevronDown, ChevronRight } from "lucide-react";
import { useState } from "react";
import { Checkbox } from "@/components/cairn/checkbox";
import { EmptyState } from "@/components/cairn/empty-state";
import { Id } from "@/components/cairn/id";
import { QueryError, QueryState } from "@/components/cairn/query-state";
import { Segmented } from "@/components/cairn/segmented";
import { Skeleton } from "@/components/cairn/skeleton";
import { StatusBadge } from "@/components/cairn/status-badge";
import { absolute, clock } from "@/components/cairn/time";
import { shownStatus } from "@/components/invocations/peek";
import { Button } from "@/components/ui/button";
import { ok, rest } from "@/data/api";
import { useRows } from "@/data/hooks";
import { queries } from "@/data/queries";
import { type SseEvent, useSse } from "@/data/sse";
import { cn } from "@/lib/utils";
import { m } from "@/paraglide/messages";
import {
  type Body,
  type Entry,
  isRestartPoint,
  pairEntries,
  showBody,
  splitError,
} from "./journal";

export const TABS = ["journal", "log", "trace", "streams", "tree", "state", "artifacts"] as const;
export type Tab = (typeof TABS)[number];

export function JournalTab({
  id,
  entries,
  open,
  onRestartFrom,
}: {
  id: string;
  entries: Entry[];
  open?: number;
  /** Set ⇒ every command row has a "Restart from here" button — keeps the journal before that command. */
  onRestartFrom?: (from: number) => void;
}) {
  const navigate = useNavigate({ from: "/invocations/$id" });
  if (entries.length === 0) return <EmptyState title={m.journal_empty()} />;
  return (
    <ol className="flex flex-col">
      {pairEntries(entries).map(({ entry: e, completion: c }) => {
        // A deep link to a completion (old `?entry=`) opens the row of the command that contains it.
        const isOpen = open === e.index || (c !== undefined && open === c.index);
        const l = e.lite;
        const failure = l.failure ?? c?.lite.failure;
        const hasBody = l.hasBody || (c?.lite.hasBody ?? false);
        // Opening/closing an entry replaces the current history item — Back does not step back
        // through every open.
        const toggle = () =>
          navigate({
            search: (s: { tab?: Tab; entry?: number }) => ({
              ...s,
              entry: isOpen ? undefined : e.index,
            }),
            replace: true,
          });
        return (
          <li
            key={e.index}
            className={cn("border-t border-border", l.completed === false && "bg-muted")}
          >
            {/* Row: the open-entry button takes most of it, "Restart from here" at the end of the
                row — shown on hover/focus (touch devices: always shown), instead of a separate
                line under every entry. */}
            <div className="group flex items-center gap-2">
              <button
                type="button"
                aria-expanded={isOpen}
                disabled={!hasBody}
                onClick={toggle}
                className="flex min-w-0 flex-1 flex-wrap items-center gap-x-2 py-1.5 text-left disabled:cursor-default"
              >
                <span className="w-4 shrink-0" aria-hidden>
                  {hasBody ? (
                    isOpen ? (
                      <ChevronDown className="size-4" />
                    ) : (
                      <ChevronRight className="size-4" />
                    )
                  ) : null}
                </span>
                <span className="w-8 shrink-0 text-right text-muted-foreground tabular">
                  {e.index}
                </span>
                <span className="font-medium">{l.kind ?? e.name ?? "?"}</span>
                {l.name ? <span className="text-muted-foreground">“{l.name}”</span> : null}
                {l.service ? (
                  <span className="text-muted-foreground">
                    {l.service}/{l.handler}
                    {l.key ? ` · ${l.key}` : ""}
                  </span>
                ) : null}
                {l.promise ? (
                  <span className="text-muted-foreground">promise {l.promise}</span>
                ) : null}
                {l.stateKey ? (
                  <span className="font-mono text-muted-foreground">{l.stateKey}</span>
                ) : null}
                {failure ? (
                  <span className="text-destructive">
                    {failure.code}: {splitError(failure.message).head}
                  </span>
                ) : c ? (
                  <span className="text-xs text-muted-foreground">
                    {m.journal_done({ index: String(c.index) })}
                  </span>
                ) : null}
                {l.completed === false ? (
                  <span className="ml-auto text-xs">{m.journal_waiting()}</span>
                ) : null}
                {e.unfinished ? (
                  <span className="ml-auto text-xs text-destructive">{m.journal_unfinished()}</span>
                ) : null}
              </button>
              {onRestartFrom && isRestartPoint(e) ? (
                <button
                  type="button"
                  aria-label={`${m.journal_restart_here()} (${e.index})`}
                  // Touch devices (no hover): only on the open row — no repeated blue link on every
                  // row; the menu's "Restart from step…" dialog can pick any step.
                  className={cn(
                    "inline-flex min-h-6 shrink-0 items-center rounded-sm px-1 text-xs text-primary underline-offset-4 opacity-0 group-hover:opacity-100 hover:underline focus-visible:opacity-100 focus-visible:outline-2 focus-visible:outline-ring",
                    isOpen ? "[@media(hover:none)]:opacity-100" : "[@media(hover:none)]:hidden",
                  )}
                  onClick={() => onRestartFrom(e.index)}
                >
                  {m.journal_restart_here()}
                </button>
              ) : null}
            </div>
            {isOpen ? (
              <div className="flex flex-col gap-2 pb-2 pl-14">
                {l.hasBody ? (
                  <EntryBody id={id} index={e.index} label={c ? m.journal_body_command() : null} />
                ) : null}
                {c?.lite.hasBody ? (
                  <EntryBody
                    id={id}
                    index={c.index}
                    label={m.journal_body_result({ index: String(c.index) })}
                  />
                ) : null}
              </div>
            ) : null}
          </li>
        );
      })}
    </ol>
  );
}

function EntryBody({ id, index, label }: { id: string; index: number; label: string | null }) {
  const q = useRows<{ body: string | null }>(queries.journalBody(id, index));
  return (
    <div>
      {label ? <p className="text-xs font-medium">{label}</p> : null}
      <QueryState q={q}>
        {({ rows: [r] }) => {
          if (!r?.body) return <EmptyState variant="inline" title={m.journal_no_body()} />;
          const b = JSON.parse(r.body) as Body;
          const shown = showBody(b);
          return (
            <figure className="flex flex-col gap-1">
              <figcaption className="text-xs text-muted-foreground">
                {b.encoding} · {m.bytes({ n: String(b.size) })}
                {b.truncated ? ` · ${m.journal_truncated()}` : ""}
              </figcaption>
              <pre
                className={cn(
                  "max-h-96 overflow-auto rounded-md bg-muted p-2 text-xs whitespace-pre-wrap break-words",
                  shown.mono && "font-mono",
                )}
              >
                {shown.text}
              </pre>
            </figure>
          );
        }}
      </QueryState>
    </div>
  );
}

type LogRow = { offset: number; kind: string; body: string; durable?: boolean };

/**
 * SSE events of `$log` ⇒ rows like those of `sys_log` (`record` carries `data`, the
 * log record; `gap`/`superseded` show up as their type). Pure — testable.
 */
export function sseLogRows(events: SseEvent[]): LogRow[] {
  const out: LogRow[] = [];
  for (const e of events) {
    if (e.event !== "record" && e.event !== "gap" && e.event !== "superseded") continue;
    let v: { offset?: number; durable?: boolean; data?: unknown } = {};
    try {
      v = JSON.parse(e.data);
    } catch {
      continue;
    }
    if (typeof v.offset !== "number") continue;
    out.push({
      offset: v.offset,
      kind: e.event === "record" ? "data" : e.event,
      body: e.event === "record" ? JSON.stringify(v.data ?? null) : e.data,
      durable: v.durable,
    });
  }
  return out;
}

/**
 * Log tab: the invocation is still running ⇒ SSE `logs?follow=1` (records arrive
 * at once, even before they are durable; the console asks for forwarding, so it
 * works on any node). SSE fails or the invocation has finished ⇒ poll the durable
 * records through `/query`.
 */
export function LogTab({ id, live }: { id: string; live: boolean }) {
  const sse = useSse(live ? `/invocations/${id}/logs?follow=1` : null);
  const streaming = live && sse.state !== "failed";
  const q = useRows<LogRow>(queries.logsAfter(id, -1), {
    refresh: live && !streaming ? { minMs: 2000, maxMs: 2000 } : false,
    enabled: !streaming,
  });
  const [level, setLevel] = useState<MinLevel>("all");
  const [wrap, setWrap] = useState(true);
  // Pause: keep a snapshot of the rows at the moment of the click; the stream keeps running behind.
  const [frozen, setFrozen] = useState<LogRow[] | null>(null);
  const rows = streaming ? sseLogRows(sse.events) : (q.data?.rows ?? []);
  const shown = frozen ?? rows;
  const mode = !live
    ? m.log_durable_ended()
    : !streaming
      ? m.log_polling()
      : sse.state === "open"
        ? m.log_live()
        : sse.state === "ended"
          ? m.log_ended()
          : m.log_connecting();
  return (
    <div className="flex flex-col gap-2">
      <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
        <p role="status" className="flex items-center gap-1.5 text-xs text-muted-foreground">
          {streaming ? (
            <span aria-hidden className="size-1.5 rounded-full bg-status-running-foreground" />
          ) : null}
          {mode}
        </p>
        <div className="ml-auto flex flex-wrap items-center gap-3">
          <Segmented
            label={m.log_level()}
            value={level}
            onChange={setLevel}
            options={[
              { value: "all", label: m.log_level_all() },
              { value: "warn", label: m.log_level_warn() },
              { value: "error", label: m.log_level_error() },
            ]}
          />
          <Checkbox checked={wrap} onCheckedChange={setWrap}>
            {m.log_wrap()}
          </Checkbox>
          {live ? (
            <Button
              variant="outline"
              size="sm"
              aria-pressed={frozen !== null}
              onClick={() => setFrozen(frozen ? null : rows)}
            >
              {frozen
                ? m.log_resume({ n: String(Math.max(0, rows.length - frozen.length)) })
                : m.log_pause()}
            </Button>
          ) : null}
        </div>
      </div>
      {!streaming && q.isPending ? (
        <Skeleton className="h-16 w-full" />
      ) : !streaming && q.isError ? (
        <QueryError error={q.error} />
      ) : shown.length === 0 ? (
        streaming && sse.state === "connecting" ? (
          <Skeleton className="h-16 w-full" />
        ) : (
          <EmptyState title={m.log_empty()} />
        )
      ) : (
        <LogLines rows={shown} level={level} wrap={wrap} />
      )}
    </div>
  );
}

/** Minimum level shown: every row, from warnings up, errors only. */
type MinLevel = "all" | "warn" | "error";
const RANK: Record<string, number> = { trace: 0, debug: 1, info: 2, warn: 3, warning: 3, error: 4 };

/** One decoded `$log` row: the SDK writes `{ts, level, msg, attrs}`; other rows stay raw. */
export function parseLogLine(body: string): {
  ts?: number;
  level?: string;
  msg?: string;
  attrs?: unknown;
} | null {
  try {
    const v = JSON.parse(body) as unknown;
    return v && typeof v === "object" ? (v as { ts?: number; level?: string; msg?: string }) : null;
  } catch {
    return null;
  }
}

/** Rows at or above the minimum level; rows with no level only show under "every row". */
export function atLeast(level: string | undefined, min: MinLevel): boolean {
  if (min === "all") return true;
  const r = level === undefined ? undefined : RANK[level.toLowerCase()];
  return r !== undefined && r >= (min === "warn" ? 3 : 4);
}

function LogLines({ rows, level, wrap }: { rows: LogRow[]; level: MinLevel; wrap: boolean }) {
  const lines = rows
    .map((r) => ({ r, line: parseLogLine(r.body) }))
    .filter(({ line }) => atLeast(line?.level, level));
  if (lines.length === 0) return <EmptyState title={m.log_none_at_level()} />;
  return (
    <ol className={cn("font-mono text-xs", !wrap && "overflow-x-auto")}>
      {lines.map(({ r, line }) => {
        const lv = line?.level?.toLowerCase();
        return (
          <li
            key={r.offset}
            className={cn(
              "flex gap-3 border-t border-border py-1",
              wrap ? "whitespace-pre-wrap break-words" : "w-max min-w-full whitespace-pre",
            )}
          >
            <span className="w-10 shrink-0 text-right text-muted-foreground">{r.offset}</span>
            {r.kind !== "data" ? <span className="text-muted-foreground">[{r.kind}]</span> : null}
            {line ? (
              <>
                {line.ts ? (
                  <time
                    dateTime={new Date(line.ts).toISOString()}
                    title={absolute(line.ts)}
                    className="shrink-0 tabular text-muted-foreground"
                  >
                    {clock(line.ts)}
                  </time>
                ) : null}
                {line.level ? (
                  <span
                    className={cn(
                      "w-12 shrink-0 uppercase",
                      lv === "error" && "font-medium text-destructive",
                      (lv === "warn" || lv === "warning") && "text-status-paused-foreground",
                    )}
                  >
                    {line.level}
                  </span>
                ) : null}
                <span>{line.msg}</span>
                {line.attrs ? (
                  <span className="text-muted-foreground">{JSON.stringify(line.attrs)}</span>
                ) : null}
              </>
            ) : (
              <span>{r.body}</span>
            )}
            {r.durable === false ? (
              <span className="text-muted-foreground">{m.log_not_durable()}</span>
            ) : null}
          </li>
        );
      })}
    </ol>
  );
}

type TreeRow = {
  id: string;
  parent_id: string | null;
  service: string;
  handler: string;
  class: string;
  status: string | null;
};

export function TreeTab({ id, rootId }: { id: string; rootId: string }) {
  const tree = useRows<TreeRow>(queries.tree(rootId));
  const kids = useRows<TreeRow>(queries.children(id));
  return (
    <QueryState q={tree}>
      {({ rows }) => {
        const all = new Map<string, TreeRow>();
        // Direct children of this invocation (question 24) — for a keyed tree `root_id` may
        // not cover everything yet; the tree query's row (with `parent_id`) wins on duplicates.
        for (const r of kids.data?.rows ?? []) all.set(r.id, { ...r, parent_id: id });
        for (const r of rows) all.set(r.id, r);
        all.delete(rootId);
        if (all.size === 0) return <EmptyState title={m.tree_empty()} />;
        return (
          <ul>
            <li className="flex items-center gap-2 py-1">
              <Id
                value={rootId}
                wrap={(t) => (
                  <Link to="/invocations/$id" params={{ id: rootId }} className="hover:underline">
                    {t}
                  </Link>
                )}
              />
              <span className="text-xs text-muted-foreground">
                ({rootId === id ? m.tree_this() : m.tree_root()})
              </span>
            </li>
            <TreeNodes all={all} at={rootId} depth={1} current={id} />
          </ul>
        );
      }}
    </QueryState>
  );
}

function TreeNodes({
  all,
  at,
  depth,
  current,
}: {
  all: Map<string, TreeRow>;
  at: string;
  depth: number;
  current: string;
}) {
  return (
    <>
      {[...all.values()]
        .filter((r) => r.parent_id === at)
        .map((r) => (
          <li
            key={r.id}
            style={{ paddingLeft: `${depth * 1.25}rem` }}
            className="flex flex-col py-1"
          >
            <div className="flex items-center gap-2">
              <Id
                value={r.id}
                wrap={(t) => (
                  <Link to="/invocations/$id" params={{ id: r.id }} className="hover:underline">
                    {t}
                  </Link>
                )}
              />
              <span>
                {r.service}/{r.handler}
              </span>
              <StatusBadge status={shownStatus(r)} />
              {r.id === current ? (
                <span className="text-xs text-muted-foreground">({m.tree_this()})</span>
              ) : null}
            </div>
            <ul>
              <TreeNodes all={all} at={r.id} depth={depth + 1} current={current} />
            </ul>
          </li>
        ))}
    </>
  );
}

type StateRow = { state_key: string; value: string };
type PromiseRow = { name: string; ok: boolean; value: string | null; failure_code: number | null };

export function StateTab({ service, objectKey }: { service: string; objectKey: string }) {
  const state = useRows<StateRow>(queries.state(service, objectKey));
  const promises = useRows<PromiseRow>(queries.promises(service, objectKey));
  return (
    <div className="flex flex-col gap-6">
      <section aria-labelledby="state-h">
        <h3 id="state-h" className="mb-2 font-semibold">
          {m.state_title()}
        </h3>
        <QueryState q={state}>
          {({ rows }) =>
            rows.length === 0 ? (
              <EmptyState variant="inline" title={m.state_empty()} />
            ) : (
              <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1">
                {rows.map((r) => (
                  <div key={r.state_key} className="contents">
                    <dt className="font-mono">{r.state_key}</dt>
                    <dd className="font-mono break-all">{r.value}</dd>
                  </div>
                ))}
              </dl>
            )
          }
        </QueryState>
      </section>
      <section aria-labelledby="promise-h">
        <h3 id="promise-h" className="mb-2 font-semibold">
          {m.promises_title()}
        </h3>
        <QueryState q={promises}>
          {({ rows }) =>
            rows.length === 0 ? (
              <EmptyState variant="inline" title={m.promises_empty()} />
            ) : (
              <ul>
                {rows.map((r) => (
                  <li key={r.name} className="font-mono">
                    {r.name}: {r.ok ? r.value : `${m.promise_failed()} ${r.failure_code ?? ""}`}
                  </li>
                ))}
              </ul>
            )
          }
        </QueryState>
      </section>
    </div>
  );
}

/** Types previewable inline in the page (the server only returns these types inline). */
const IMAGE = /^image\/(png|jpeg|gif|webp|avif)$/;

export function ArtifactsTab({ id }: { id: string }) {
  const q = useQuery({
    queryKey: ["artifacts", id],
    queryFn: () => ok(rest.GET("/invocations/{id}/artifacts", { params: { path: { id } } })),
  });
  if (q.isPending) return null;
  if (q.isError) return <p role="alert">{q.error.message}</p>;
  const list = q.data.artifacts;
  if (list.length === 0) return <EmptyState title={m.artifacts_empty()} />;
  return (
    <ul className="flex flex-col gap-3">
      {list.map((a) => {
        const href = `/invocations/${id}/artifacts/${a.sha256}`;
        return (
          <li key={a.sha256} className="flex flex-col gap-1">
            <div className="flex items-center gap-2">
              <Id value={a.sha256} />
              <span className="text-muted-foreground">
                {a.contentType} · {m.bytes({ n: String(a.size) })}
              </span>
              <a href={href} download className="text-primary underline-offset-4 hover:underline">
                {m.artifact_download()}
              </a>
            </div>
            {IMAGE.test(a.contentType) ? (
              <img
                src={href}
                alt={m.artifact_preview({ sha: a.sha256.slice(0, 8) })}
                className="max-h-64 max-w-full rounded-md border"
              />
            ) : null}
          </li>
        );
      })}
    </ul>
  );
}
