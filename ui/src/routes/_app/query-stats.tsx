// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// "Query stats": every `/query` the console sent in this
// session (this tab), grouped by query — count, server `ms`/`read`, wall-clock
// time on the browser side, `partial`, errors, PQ (product question) numbers — and the 50 most recent queries.
import { createFileRoute, Link } from "@tanstack/react-router";
import { BarChart3 } from "lucide-react";
import { EmptyState } from "@/components/cairn/empty-state";
import { Table, TBody, TD, TH, THead, TRow } from "@/components/cairn/table";
import { Time } from "@/components/cairn/time";
import { Button } from "@/components/ui/button";
import { clearStats, groupBySql, STATS_MAX, useQueryEvents } from "@/data/stats";
import { m } from "@/paraglide/messages";
import { getLocale } from "@/paraglide/runtime";

export const Route = createFileRoute("/_app/query-stats")({ component: QueryStats });

const RECENT = 50;

function QueryStats() {
  const events = useQueryEvents();
  const groups = groupBySql(events);
  const fmt = new Intl.NumberFormat(getLocale());
  const read = events.reduce((s, e) => s + (e.read ?? 0), 0);
  const errors = events.filter((e) => e.status !== 200).length;
  return (
    <div className="flex flex-col gap-4 p-6">
      <div className="flex items-baseline justify-between gap-4">
        <h1 className="text-xl font-semibold">{m.stats_title()}</h1>
        <Button size="sm" variant="outline" onClick={clearStats} disabled={events.length === 0}>
          {m.stats_clear()}
        </Button>
      </div>
      <p className="text-muted-foreground">{m.stats_body({ n: fmt.format(STATS_MAX) })}</p>
      {events.length === 0 ? (
        <EmptyState icon={<BarChart3 />} title={m.stats_empty()} />
      ) : (
        <>
          <p role="status">
            {m.stats_summary({
              n: fmt.format(events.length),
              shapes: fmt.format(groups.length),
              read: fmt.format(read),
              errors: fmt.format(errors),
            })}
          </p>
          <section aria-labelledby="stats-by-sql">
            <h2 id="stats-by-sql" className="mb-2 text-base font-semibold">
              {m.stats_by_sql()}
            </h2>
            <Table>
              <THead>
                <TH>{m.stats_col_sql()}</TH>
                <TH>PQ</TH>
                <TH numeric>{m.stats_col_count()}</TH>
                <TH numeric>{m.stats_col_read()}</TH>
                <TH numeric>{m.stats_col_ms()}</TH>
                <TH numeric>{m.stats_col_wall()}</TH>
                <TH numeric>{m.stats_col_partial()}</TH>
                <TH numeric>{m.stats_col_errors()}</TH>
              </THead>
              <TBody>
                {groups.map((g) => (
                  <TRow key={g.sql} className="align-top">
                    <TD priority="primary" className="max-w-xl">
                      <Link
                        to="/query"
                        search={{ sql: g.sql }}
                        className="line-clamp-2 font-mono text-xs break-all hover:underline"
                        title={g.sql}
                      >
                        {g.sql}
                      </Link>
                    </TD>
                    <TD priority="hidden" className="tabular">
                      {g.questions.join(", ") || "—"}
                    </TD>
                    <TD numeric>{fmt.format(g.count)}</TD>
                    <TD numeric>{fmt.format(g.read)}</TD>
                    <TD priority="hidden" numeric>
                      {fmt.format(g.avgMs)} / {fmt.format(g.maxMs)}
                    </TD>
                    <TD numeric>{fmt.format(g.avgWallMs)}</TD>
                    <TD numeric>{fmt.format(g.partial)}</TD>
                    <TD numeric>{fmt.format(g.errors)}</TD>
                  </TRow>
                ))}
              </TBody>
            </Table>
          </section>
          <section aria-labelledby="stats-recent">
            <h2 id="stats-recent" className="mb-2 text-base font-semibold">
              {m.stats_recent({ n: String(RECENT) })}
            </h2>
            <ol className="flex flex-col text-sm">
              {events
                .slice(-RECENT)
                .reverse()
                .map((e, i) => (
                  <li
                    // biome-ignore lint/suspicious/noArrayIndexKey: events have no id; the order is fixed
                    key={`${e.at}-${i}`}
                    className="flex flex-wrap items-baseline gap-x-3 border-t border-border py-1"
                  >
                    <Time ms={e.at} />
                    <span className={e.status === 200 ? "tabular" : "tabular text-destructive"}>
                      {e.status}
                    </span>
                    <span className="tabular text-muted-foreground">
                      {e.status === 200
                        ? m.stats_event({
                            ms: String(e.ms ?? 0),
                            read: String(e.read ?? 0),
                            rows: String(e.rows ?? 0),
                          })
                        : e.error}
                    </span>
                    {e.partial ? <span className="text-xs">{m.stats_partial_tag()}</span> : null}
                    <span className="min-w-0 flex-1 truncate font-mono text-xs" title={e.sql}>
                      {e.sql}
                    </span>
                  </li>
                ))}
            </ol>
          </section>
        </>
      )}
    </div>
  );
}
