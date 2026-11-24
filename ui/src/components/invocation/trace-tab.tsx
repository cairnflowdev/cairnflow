// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Trace tab of the invocation detail: spans of `$trace` (`ctx.trace.span`, and
// `trace: "runtime"` ⇒ one span per turn and per `ctx.run`) drawn as a waterfall:
// one row per span, bars placed by time, nested parent–child. Clicking a row ⇒
// attributes, step, attempt. Durable chunks only (`sys_span`, question 37) — spans
// still open in the leader's RAM are not visible. Above the waterfall is the
// per-attempt timeline (`timeline.tsx`); for a handler without `trace: "runtime"`
// it is the invocation's raw milestone strip.
import { ChevronDown, ChevronRight, TriangleAlert } from "lucide-react";
import { useState } from "react";
import { EmptyState } from "@/components/cairn/empty-state";
import { QueryState } from "@/components/cairn/query-state";
import type { InvocationDetail } from "@/components/invocations/peek";
import { useRows } from "@/data/hooks";
import { queries } from "@/data/queries";
import { buildLanes, buildSpans, duration, extent, type Span, type SpanRow } from "@/lib/spans";
import { cn } from "@/lib/utils";
import { m } from "@/paraglide/messages";
import { getLocale } from "@/paraglide/runtime";
import type { Entry } from "./journal";
import { Milestones, Timeline } from "./timeline";

const TICKS = [0, 0.25, 0.5, 0.75, 1];

export function TraceTab({
  id,
  live,
  r,
  entries,
}: {
  id: string;
  live: boolean;
  r: InvocationDetail;
  /** Journal headers (already loaded by the page) — joins `ctx.run` spans to entries. */
  entries: Entry[];
}) {
  const q = useRows<SpanRow>(queries.spans(id), {
    refresh: live ? { minMs: 2000, maxMs: 2000 } : false,
  });
  return (
    <QueryState q={q}>
      {({ rows }) => {
        const spans = buildSpans(rows);
        return (
          <div className="flex flex-col gap-6">
            {buildLanes(spans).length > 0 ? (
              <Timeline
                spans={spans}
                entries={entries}
                live={live}
                finalFailed={r.class === "failed" || r.class === "paused"}
              />
            ) : (
              <Milestones r={r} />
            )}
            {spans.length === 0 ? (
              <EmptyState title={m.trace_empty()}>{m.trace_empty_body()}</EmptyState>
            ) : (
              <Waterfall spans={spans} live={live} />
            )}
          </div>
        );
      }}
    </QueryState>
  );
}

function Waterfall({ spans, live }: { spans: Span[]; live: boolean }) {
  const locale = getLocale();
  const { t0, t1 } = extent(spans, live ? Date.now() : 0);
  const total = t1 - t0;
  const [open, setOpen] = useState<string | null>(null);
  const tokensIn = spans.reduce((a, s) => a + (s.inputTokens ?? 0), 0);
  const tokensOut = spans.reduce((a, s) => a + (s.outputTokens ?? 0), 0);
  const errors = spans.filter((s) => s.status === "error").length;
  const fmt = new Intl.NumberFormat(locale);
  const pct = (t: number) => ((t - t0) / total) * 100;
  return (
    <div className="flex flex-col gap-2">
      <p className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
        <span>
          {m.trace_summary({ n: fmt.format(spans.length), took: duration(total, locale) })}
        </span>
        {errors > 0 ? (
          <span className="text-destructive">{m.trace_errors({ n: fmt.format(errors) })}</span>
        ) : null}
        {tokensIn + tokensOut > 0 ? (
          <span>
            {m.trace_tokens({ input: fmt.format(tokensIn), output: fmt.format(tokensOut) })}
          </span>
        ) : null}
      </p>
      <div className="relative overflow-x-auto">
        <div className="min-w-[40rem] text-xs">
          {/* Axis: labels at 0, ¼, ½, ¾, end. */}
          <div className="grid grid-cols-[minmax(10rem,28%)_1fr_5rem] items-end gap-x-3 border-b border-border pb-1 text-muted-foreground">
            <span>{m.trace_span()}</span>
            <div className="relative h-4">
              {TICKS.map((f) => (
                <span
                  key={f}
                  className={cn(
                    // The label at the right edge has no room to spare ⇒ no wrapping.
                    "absolute bottom-0 whitespace-nowrap tabular",
                    f === 0 ? "" : f === 1 ? "-translate-x-full" : "-translate-x-1/2",
                  )}
                  style={{ left: `${f * 100}%` }}
                >
                  {duration(total * f, locale)}
                </span>
              ))}
            </div>
            <span className="text-right">{m.trace_took()}</span>
          </div>
          <ol>
            {spans.map((s) => {
              const end = s.end ?? t1;
              const expanded = open === s.id;
              return (
                <li key={s.id} className="border-b border-border">
                  <button
                    type="button"
                    aria-expanded={expanded}
                    onClick={() => setOpen(expanded ? null : s.id)}
                    className="grid w-full grid-cols-[minmax(10rem,28%)_1fr_5rem] items-center gap-x-3 py-1 text-left hover:bg-accent"
                  >
                    <span
                      className="flex min-w-0 items-center gap-1"
                      style={{ paddingLeft: `${s.depth * 0.875}rem` }}
                    >
                      {expanded ? (
                        <ChevronDown aria-hidden className="size-3 shrink-0" />
                      ) : (
                        <ChevronRight aria-hidden className="size-3 shrink-0" />
                      )}
                      {s.status === "error" ? (
                        <TriangleAlert
                          aria-label={m.trace_error()}
                          className="size-3 shrink-0 text-destructive"
                        />
                      ) : null}
                      <span className="truncate font-mono" title={s.name}>
                        {s.name}
                      </span>
                    </span>
                    {/* Bar: at least 2 px wide so instantaneous spans are still visible. */}
                    <span className="relative h-4">
                      <span
                        aria-hidden
                        className={cn(
                          "absolute top-0.5 bottom-0.5 min-w-0.5 rounded-sm",
                          s.status === "error"
                            ? "bg-destructive"
                            : s.status === "open"
                              ? "bg-primary/40 bg-[repeating-linear-gradient(45deg,transparent_0_4px,var(--background)_4px_6px)]"
                              : "bg-primary/70",
                        )}
                        style={{ left: `${pct(s.start)}%`, width: `${pct(end) - pct(s.start)}%` }}
                      />
                    </span>
                    <span className="text-right whitespace-nowrap tabular">
                      {s.end === null ? m.trace_open() : duration(s.end - s.start, locale)}
                    </span>
                  </button>
                  {expanded ? <Details s={s} /> : null}
                </li>
              );
            })}
          </ol>
        </div>
      </div>
    </div>
  );
}

function Details({ s }: { s: Span }) {
  const fmt = new Intl.NumberFormat(getLocale());
  const facts: [string, string][] = [
    [m.trace_started(), new Date(s.start).toISOString()],
    ...(s.step !== null ? ([[m.trace_step(), String(s.step)]] as [string, string][]) : []),
    ...(s.attempt !== null ? ([[m.trace_attempt(), String(s.attempt)]] as [string, string][]) : []),
    ...(s.inputTokens !== null || s.outputTokens !== null
      ? ([
          [
            m.trace_tokens_label(),
            `${fmt.format(s.inputTokens ?? 0)} / ${fmt.format(s.outputTokens ?? 0)}`,
          ],
        ] as [string, string][])
      : []),
  ];
  const attrs = Object.keys(s.attrs).length > 0 ? JSON.stringify(s.attrs, null, 2) : null;
  return (
    <div className="flex flex-col gap-1 pb-2 pl-6">
      <dl className="flex flex-wrap gap-x-4 gap-y-0.5">
        {facts.map(([k, v]) => (
          <div key={k} className="flex gap-1">
            <dt className="text-muted-foreground">{k}</dt>
            <dd className="font-mono">{v}</dd>
          </div>
        ))}
      </dl>
      {s.startLost ? <p className="text-muted-foreground">{m.trace_start_lost()}</p> : null}
      {attrs ? (
        <pre className="max-h-48 overflow-auto rounded-md bg-muted p-2 font-mono whitespace-pre-wrap break-words">
          {attrs}
        </pre>
      ) : null}
    </div>
  );
}
