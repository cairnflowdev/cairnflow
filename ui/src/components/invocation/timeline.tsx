// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Timeline in the Trace tab: built from the runtime's spans (`trace: "runtime"`),
// no milestone is written per journal entry.
//
// - One lane per attempt (the `attempt` column), failed lanes are red; between two
//   lanes is the retry wait, hatched.
// - One row per `ctx.run` in the lane, joined to the journal's `Run` entry
//   (`linkRuns`: name + order); selecting a row ⇒ highlights the entry, selecting an
//   entry ⇒ highlights the row. Entries being waited on (no completion yet) stand out.
// - A handler without `trace: "runtime"` ⇒ the invocation's raw milestone strip and
//   a line suggesting to enable it.
//
// Every clickable row is a full-width button (hit area ≥ 24 px); bars are only
// visual (`aria-hidden`), the figures are in the button's text.
import { useState } from "react";
import { Time } from "@/components/cairn/time";
import type { InvocationDetail } from "@/components/invocations/peek";
import {
  attemptNumber,
  buildLanes,
  duration,
  type Lane,
  linkRuns,
  type Milestone,
  milestones,
  retryGaps,
  type Span,
} from "@/lib/spans";
import { cn } from "@/lib/utils";
import { m } from "@/paraglide/messages";
import { getLocale } from "@/paraglide/runtime";
import type { Entry } from "./journal";

const HATCH =
  "bg-[repeating-linear-gradient(45deg,var(--muted-foreground)_0_2px,transparent_2px_6px)] opacity-60";
const GRID = "grid grid-cols-[minmax(11rem,30%)_1fr_5rem] items-center gap-x-3";

export function Timeline({
  spans,
  entries,
  live,
  finalFailed,
}: {
  spans: Span[];
  entries: Entry[];
  live: boolean;
  /** Invocation is `failed`/`paused` ⇒ the last attempt failed too. */
  finalFailed: boolean;
}) {
  const locale = getLocale();
  const lanes = buildLanes(spans, { finalFailed });
  const gaps = retryGaps(lanes);
  const link = linkRuns(
    spans,
    entries.map((e) => ({ index: e.index, kind: e.lite.kind, name: e.lite.name })),
  );
  const [selected, setSelected] = useState<number | null>(null);
  const now = live ? Date.now() : 0;
  const t0 = Math.min(...lanes.map((l) => l.start));
  const t1 = Math.max(t0 + 1, ...lanes.map((l) => l.end ?? Math.max(now, l.start)));
  const pct = (t: number) => ((t - t0) / (t1 - t0)) * 100;
  const bar = (from: number, to: number) => ({
    left: `${pct(from)}%`,
    width: `${Math.max(0, pct(to) - pct(from))}%`,
  });
  const runEntries = entries.filter(
    (e) => e.lite.kind === "Run" || e.lite.completed === false || e.unfinished,
  );
  const failedLanes = lanes.filter((l) => l.failed).length;

  return (
    <section aria-labelledby="timeline-h" className="flex flex-col gap-2">
      <h3 id="timeline-h" className="text-sm font-semibold">
        {m.timeline_title()}
      </h3>
      <p className="text-xs text-muted-foreground">
        {m.timeline_summary({
          n: String(lanes.length),
          failed: String(failedLanes),
          took: duration(t1 - t0, locale),
        })}
      </p>
      <div className="relative overflow-x-auto">
        <ol className="min-w-[40rem] text-xs">
          {lanes.map((l) => {
            const gap = gaps.find((g) => g.after === l.attempt);
            return (
              <li key={l.attempt} className="border-b border-border">
                <LaneHead l={l} bar={bar} t1={t1} locale={locale} />
                <ol>
                  {l.runs.map((s) => {
                    const entry = link.get(s.id);
                    const on = entry !== undefined && entry === selected;
                    // A run unfinished when the turn ended (transient error inside the closure —
                    // no `ProposeRunCompletion`): the runtime closes it `abandoned`; spans from
                    // older servers stay open inside a closed attempt — same meaning.
                    const cut = s.attrs.abandoned === true || (s.end === null && l.end !== null);
                    // A run re-run in a later attempt: the runtime does not see when it
                    // started, `start` is the start of the turn ⇒ the duration is an upper bound.
                    const estimated = s.attrs.start_estimated === true;
                    const bad = s.status === "error" || cut;
                    const end = s.end ?? l.end ?? t1;
                    return (
                      <li key={s.id}>
                        <button
                          type="button"
                          aria-pressed={on}
                          onClick={() => setSelected(on || entry === undefined ? null : entry)}
                          className={cn(GRID, "min-h-6 w-full py-0.5 text-left hover:bg-accent")}
                        >
                          <span className="flex min-w-0 items-center gap-1 pl-4">
                            <span className="truncate font-mono" title={s.name}>
                              {s.name}
                            </span>
                            {bad ? (
                              <span className="text-destructive">
                                · {cut ? m.timeline_run_cut() : m.timeline_run_failed()}
                              </span>
                            ) : null}
                            {entry !== undefined ? (
                              <span className="text-muted-foreground">
                                · {m.timeline_entry({ n: String(entry) })}
                              </span>
                            ) : null}
                          </span>
                          <span className="relative h-3">
                            <span
                              aria-hidden
                              className={cn(
                                "absolute top-0 bottom-0 min-w-0.5 rounded-sm",
                                bad
                                  ? "bg-destructive"
                                  : s.status === "open"
                                    ? HATCH
                                    : "bg-primary/70",
                                on && "outline-2 outline-offset-1 outline-ring",
                              )}
                              style={bar(s.start, end)}
                            />
                          </span>
                          <span
                            className="text-right whitespace-nowrap tabular"
                            title={estimated ? m.timeline_estimated_title() : undefined}
                          >
                            {s.end === null && !cut
                              ? m.trace_open()
                              : estimated
                                ? m.timeline_estimated({ d: duration(end - s.start, locale) })
                                : duration(end - s.start, locale)}
                          </span>
                        </button>
                      </li>
                    );
                  })}
                </ol>
                {gap ? (
                  <div className={cn(GRID, "py-0.5 text-muted-foreground")}>
                    <span className="pl-4">{m.timeline_retry_wait()}</span>
                    <span className="relative h-3">
                      <span
                        aria-hidden
                        className={cn("absolute top-0 bottom-0 rounded-sm", HATCH)}
                        style={bar(gap.from, gap.to)}
                      />
                    </span>
                    <span className="text-right whitespace-nowrap tabular">
                      {duration(gap.to - gap.from, locale)}
                    </span>
                  </div>
                ) : null}
              </li>
            );
          })}
        </ol>
      </div>
      {runEntries.length > 0 ? (
        <div className="flex flex-col gap-1">
          <h4 className="text-xs font-medium text-muted-foreground">{m.timeline_entries()}</h4>
          <ol aria-label={m.timeline_entries()} className="flex flex-wrap gap-1.5 text-xs">
            {runEntries.map((e) => {
              const on = e.index === selected;
              const awaited = e.lite.completed === false;
              const unfinished = e.unfinished === true;
              const n = [...link.values()].filter((v) => v === e.index).length;
              return (
                <li key={e.index}>
                  <button
                    type="button"
                    aria-pressed={on}
                    onClick={() => setSelected(on ? null : e.index)}
                    className={cn(
                      "inline-flex min-h-6 items-center gap-1 rounded-md border px-2 hover:bg-accent",
                      on ? "border-ring bg-accent font-medium" : "border-input",
                      awaited && "border-2 border-primary font-medium",
                    )}
                  >
                    <span className="tabular text-muted-foreground">#{e.index}</span>
                    <span>{e.lite.kind}</span>
                    {e.lite.name ? <span className="font-mono">“{e.lite.name}”</span> : null}
                    {awaited ? <span>· {m.journal_waiting()}</span> : null}
                    {unfinished ? (
                      <span className="text-destructive">· {m.journal_unfinished()}</span>
                    ) : null}
                    {n > 0 ? (
                      <span className="text-muted-foreground">
                        · {m.timeline_runs({ n: String(n) })}
                      </span>
                    ) : null}
                  </button>
                </li>
              );
            })}
          </ol>
        </div>
      ) : null}
    </section>
  );
}

function LaneHead({
  l,
  bar,
  t1,
  locale,
}: {
  l: Lane;
  bar: (from: number, to: number) => { left: string; width: string };
  t1: number;
  locale: string;
}) {
  return (
    <div className={cn(GRID, "py-1")}>
      <span className="flex items-center gap-1 font-medium">
        {m.timeline_attempt({ n: String(attemptNumber(l.attempt)) })}
        <span className={l.failed ? "text-destructive" : "text-muted-foreground"}>
          ·{" "}
          {l.end === null ? m.timeline_running() : l.failed ? m.timeline_failed() : m.timeline_ok()}
        </span>
      </span>
      <span className="relative h-4">
        <span
          aria-hidden
          className={cn(
            "absolute top-0 bottom-0 rounded-sm border",
            l.failed ? "border-destructive bg-destructive/15" : "border-primary/50 bg-primary/10",
          )}
          style={bar(l.start, l.end ?? t1)}
        />
      </span>
      <span className="text-right whitespace-nowrap tabular">
        {l.end === null ? m.trace_open() : duration(l.end - l.start, locale)}
      </span>
    </div>
  );
}

const MILESTONE: Record<Milestone["kind"], () => string> = {
  created: m.timeline_m_created,
  transitioned: m.timeline_m_transitioned,
  lastFailure: m.timeline_m_last_failure,
  retryAt: m.timeline_m_retry_at,
  ended: m.timeline_m_ended,
};

/** Handler without `trace: "runtime"`: raw milestone strip + hint. */
export function Milestones({ r }: { r: InvocationDetail }) {
  const list = milestones(r);
  const t0 = list[0]?.at ?? 0;
  const t1 = Math.max(t0 + 1, ...list.map((x) => x.at));
  return (
    <section aria-labelledby="timeline-h" className="flex flex-col gap-2">
      <h3 id="timeline-h" className="text-sm font-semibold">
        {m.timeline_title()}
      </h3>
      <p className="text-sm">{m.timeline_enable_runtime()}</p>
      <div aria-hidden className="relative mx-2 h-3">
        <span className="absolute top-1/2 right-0 left-0 h-px bg-border" />
        {list.map((x) => (
          <span
            key={x.kind}
            className={cn(
              "absolute top-0 size-3 -translate-x-1/2 rounded-full border-2 border-background",
              x.kind === "lastFailure" ? "bg-destructive" : "bg-primary",
            )}
            style={{ left: `${((x.at - t0) / (t1 - t0)) * 100}%` }}
          />
        ))}
      </div>
      <ol aria-label={m.timeline_milestones()} className="flex flex-wrap gap-x-5 gap-y-1 text-xs">
        {list.map((x) => (
          <li key={x.kind} className="flex gap-1">
            <span className="text-muted-foreground">{MILESTONE[x.kind]()}</span>
            <Time ms={x.at} />
          </li>
        ))}
      </ol>
    </section>
  );
}
