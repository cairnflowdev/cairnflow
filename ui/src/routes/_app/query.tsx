// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// SQL console: CodeMirror editor, three-tab sidebar — sample queries
// (click to run), recently run queries (this browser), table catalog (click a
// column ⇒ insert) — with a filter box; results as a table or a chart, CSV. `?sql=` keeps the query for
// sharing. Queries go to `POST /query` via `run` (≤ 2 in flight, backs off on `429`).
import { useMutation } from "@tanstack/react-query";
import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import { Loader2, Play, Search, Terminal, X } from "lucide-react";
import { useId, useRef, useState } from "react";
import { EmptyState } from "@/components/cairn/empty-state";
import { QueryError } from "@/components/cairn/query-state";
import { TabPanel, Tabs } from "@/components/cairn/tabs";
import { type Result, Results } from "@/components/query/results";
import { SqlEditor, type SqlEditorHandle } from "@/components/query/sql-editor";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ApiError } from "@/data/api";
import { run } from "@/data/run";
import { SAMPLES, sampleTitle } from "@/data/samples";
import { TABLES } from "@/data/tables.gen";
import { cn } from "@/lib/utils";
import { m } from "@/paraglide/messages";

export const Route = createFileRoute("/_app/query")({
  validateSearch: (s: Record<string, unknown>): { sql?: string } => ({
    sql: typeof s.sql === "string" && s.sql ? s.sql : undefined,
  }),
  component: Query,
});

type Pane = "samples" | "recent" | "tables";

/** Recently run queries — a per-viewer convenience (localStorage, may be empty). */
const RECENT_KEY = "cairn.query.recent";
const RECENT_MAX = 20;

function loadRecent(): string[] {
  try {
    const v = JSON.parse(localStorage.getItem(RECENT_KEY) ?? "[]");
    return Array.isArray(v) ? v.filter((x): x is string => typeof x === "string") : [];
  } catch {
    return [];
  }
}

function saveRecent(list: string[]) {
  try {
    localStorage.setItem(RECENT_KEY, JSON.stringify(list));
  } catch {}
}

const oneLine = (sql: string) => sql.replace(/\s+/g, " ").trim();

function Query() {
  const { sql: shared } = Route.useSearch();
  const navigate = useNavigate({ from: "/query" });
  const [sql, setSql] = useState(shared ?? SAMPLES[0]?.sql ?? "");
  const [pane, setPane] = useState<Pane>("samples");
  const tabs = useId();
  const [filter, setFilter] = useState("");
  const [recent, setRecent] = useState(loadRecent);
  const editor = useRef<SqlEditorHandle>(null);
  const exec = useMutation({
    mutationFn: async (text: string): Promise<Result> => {
      const out = await run<Record<string, unknown>>({ questions: [], sql: text, params: [] });
      return { ...out, rows: out.raw };
    },
  });
  const go = (text = sql) => {
    if (!text.trim()) return;
    void navigate({ search: { sql: text }, replace: true });
    exec.mutate(text);
    const next = [text, ...recent.filter((r) => oneLine(r) !== oneLine(text))].slice(0, RECENT_MAX);
    setRecent(next);
    saveRecent(next);
  };
  const needle = filter.trim().toLowerCase();
  const samples = SAMPLES.filter(
    (s) =>
      !needle ||
      sampleTitle(s.pq).toLowerCase().includes(needle) ||
      s.sql.toLowerCase().includes(needle),
  );
  const tables = TABLES.filter(
    (t) =>
      !needle ||
      t.name.includes(needle) ||
      t.columns.some((c) => c.name.toLowerCase().includes(needle)),
  );
  const recents = recent.filter((r) => !needle || r.toLowerCase().includes(needle));
  return (
    <div className="grid grid-cols-1 gap-6 p-6 lg:grid-cols-[18rem_minmax(0,1fr)]">
      <aside
        aria-label={m.query_sidebar()}
        // Sticks while scrolling long results (wide screen); narrow screen: below the editor.
        className="flex min-h-0 flex-col gap-2 text-sm max-lg:order-last lg:sticky lg:top-6 lg:max-h-[calc(100vh-8rem)] lg:self-start"
      >
        <Tabs
          id={tabs}
          label={m.query_sidebar()}
          value={pane}
          onChange={setPane}
          options={[
            { value: "samples", label: m.query_samples() },
            { value: "recent", label: m.query_recent() },
            { value: "tables", label: m.query_schema() },
          ]}
        />
        <div className="relative">
          <Search
            aria-hidden
            className="pointer-events-none absolute top-2 left-2 size-4 text-muted-foreground"
          />
          <Input
            aria-label={m.query_filter()}
            placeholder={m.query_filter()}
            className="h-8 pr-7 pl-7"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
          />
          {filter ? (
            <button
              type="button"
              aria-label={m.query_filter_clear()}
              className="absolute top-1.5 right-1.5 rounded-sm p-0.5 text-muted-foreground hover:text-foreground"
              onClick={() => setFilter("")}
            >
              <X aria-hidden className="size-4" />
            </button>
          ) : null}
        </div>
        <TabPanel id={tabs} value={pane} className="min-h-0 overflow-auto max-lg:max-h-96">
          {pane === "samples" ? (
            <List empty={samples.length === 0}>
              {samples.map((s) => {
                const active = oneLine(s.sql) === oneLine(sql);
                return (
                  <li key={s.pq}>
                    <button
                      type="button"
                      aria-current={active || undefined}
                      title={s.sql}
                      className={cn(
                        "flex w-full gap-1.5 rounded-md px-2 py-1 text-left hover:bg-accent",
                        active && "bg-secondary font-medium text-secondary-foreground",
                      )}
                      onClick={() => {
                        setSql(s.sql);
                        go(s.sql);
                      }}
                    >
                      <span className="w-6 shrink-0 text-right text-tertiary-foreground tabular">
                        {s.pq}
                      </span>
                      <span>{sampleTitle(s.pq)}</span>
                    </button>
                  </li>
                );
              })}
            </List>
          ) : pane === "recent" ? (
            recent.length === 0 ? (
              <div className="px-2">
                <EmptyState variant="inline" title={m.query_recent_empty()} />
              </div>
            ) : (
              <List empty={recents.length === 0}>
                {recents.map((r) => (
                  <li key={r}>
                    <button
                      type="button"
                      title={r}
                      className="w-full truncate rounded-md px-2 py-1 text-left font-mono text-xs hover:bg-accent"
                      onClick={() => {
                        setSql(r);
                        go(r);
                      }}
                    >
                      {oneLine(r)}
                    </button>
                  </li>
                ))}
              </List>
            )
          ) : (
            <List empty={tables.length === 0}>
              {tables.map((t) => (
                <li key={t.name}>
                  <details open={!!needle || undefined}>
                    <summary
                      className="min-h-6 cursor-pointer rounded-md px-2 py-0.5 font-mono hover:bg-accent"
                      title={t.doc}
                    >
                      {t.name}
                    </summary>
                    <ul className="ml-4">
                      {t.columns.map((c) => (
                        <li key={c.name}>
                          <button
                            type="button"
                            className={cn(
                              "flex min-h-6 w-full justify-between gap-2 rounded-sm px-1 text-left font-mono text-xs hover:bg-accent",
                              needle && c.name.toLowerCase().includes(needle) && "bg-accent",
                            )}
                            title={c.doc}
                            onClick={() => editor.current?.insert(c.name)}
                          >
                            {c.name} <span className="text-tertiary-foreground">{c.type}</span>
                          </button>
                        </li>
                      ))}
                    </ul>
                  </details>
                </li>
              ))}
            </List>
          )}
        </TabPanel>
      </aside>
      <div className="flex min-w-0 flex-col gap-3">
        <div className="flex items-baseline justify-between gap-4">
          <h1 className="text-xl font-semibold">{m.nav_query()}</h1>
          <Link
            to="/query-stats"
            className="text-sm text-primary underline-offset-4 hover:underline"
          >
            {m.query_stats_link()}
          </Link>
        </div>
        <SqlEditor
          ref={editor}
          value={sql}
          onChange={setSql}
          onRun={() => go()}
          label={m.query_editor()}
        />
        <div className="flex items-center gap-3">
          <Button onClick={() => go()} disabled={exec.isPending || !sql.trim()}>
            {exec.isPending ? (
              <Loader2 aria-hidden className="animate-spin" />
            ) : (
              <Play aria-hidden />
            )}
            {m.query_run()}
          </Button>
          <span className="text-xs text-muted-foreground">{m.query_run_hint()}</span>
        </div>
        {exec.isError ? (
          exec.error instanceof ApiError &&
          exec.error.status === 422 &&
          exec.error.message.includes("query_budget") ? (
            <div role="alert" className="rounded-md border border-destructive/40 px-3 py-2">
              <p className="font-medium">{m.query_budget_title()}</p>
              <p className="text-muted-foreground">{m.query_budget_hint()}</p>
            </div>
          ) : (
            <QueryError error={exec.error} />
          )
        ) : null}
        {exec.data ? (
          // A new query is running: old results dim (still readable), no blank flash.
          <div aria-busy={exec.isPending} className={cn(exec.isPending && "opacity-60")}>
            <Results r={exec.data} />
          </div>
        ) : !exec.isError && !exec.isPending ? (
          <EmptyState icon={<Terminal aria-hidden />} title={m.query_start_title()}>
            {m.query_start_body()}
          </EmptyState>
        ) : null}
      </div>
    </div>
  );
}

function List({ empty, children }: { empty: boolean; children: React.ReactNode }) {
  if (empty)
    return (
      <div className="px-2">
        <EmptyState variant="inline" title={m.query_filter_none()} />
      </div>
    );
  return <ul className="flex flex-col gap-0.5">{children}</ul>;
}
