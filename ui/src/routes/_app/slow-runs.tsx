// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Slow runs: runs over the tenant's `slowMs` threshold, still in
// each node's ring buffer (`GET /stats/slow-runs?scope=cluster`, newest first).
// Each row says where the time went: code running, SDK, waiting for durability, waiting for the engine, waiting for quota.
import { useQuery } from "@tanstack/react-query";
import { createFileRoute, Link } from "@tanstack/react-router";
import { Snail } from "lucide-react";
import { EmptyState } from "@/components/cairn/empty-state";
import { Id } from "@/components/cairn/id";
import { QueryError } from "@/components/cairn/query-state";
import { Skeleton } from "@/components/cairn/skeleton";
import { Table, TBody, TD, TH, THead, TRow } from "@/components/cairn/table";
import { Time } from "@/components/cairn/time";
import { ok, rest } from "@/data/api";
import { mostly } from "@/lib/slow";
import { m } from "@/paraglide/messages";
import { getLocale } from "@/paraglide/runtime";

export const Route = createFileRoute("/_app/slow-runs")({ component: SlowRuns });

function SlowRuns() {
  const q = useQuery({
    queryKey: ["slow-runs"],
    queryFn: () => ok(rest.GET("/stats/slow-runs", { params: { query: { scope: "cluster" } } })),
    refetchInterval: 10_000,
  });
  const fmt = new Intl.NumberFormat(getLocale());
  return (
    <div className="flex flex-col gap-4 p-6">
      <h1 className="text-xl font-semibold">{m.nav_slow_runs()}</h1>
      <p className="text-muted-foreground">{m.slow_body()}</p>
      {q.isPending ? (
        <Skeleton className="h-32 w-full" />
      ) : q.isError ? (
        <QueryError error={q.error} />
      ) : (
        <>
          {q.data.partial ? (
            <p role="status" className="rounded-md border border-input bg-muted px-3 py-2">
              {m.slow_partial({ nodes: q.data.missing.join(", ") })}
            </p>
          ) : null}
          {q.data.runs.length === 0 ? (
            <EmptyState icon={<Snail />} title={m.slow_empty()} />
          ) : (
            <Table>
              <THead>
                <TH>{m.slow_col_at()}</TH>
                <TH>{m.col_target()}</TH>
                <TH>{m.col_id()}</TH>
                <TH>{m.slow_col_outcome()}</TH>
                <TH numeric>{m.slow_col_total()}</TH>
                <TH>{m.slow_col_mostly()}</TH>
              </THead>
              <TBody>
                {q.data.runs.map((r) => {
                  const most = mostly(r);
                  return (
                    <TRow key={`${r.invocation}-${r.atMs}`} className="align-top">
                      <TD>
                        <Time ms={r.atMs} />
                      </TD>
                      <TD priority="primary">
                        {r.service}/{r.handler}
                      </TD>
                      <TD>
                        <Id
                          value={r.invocation}
                          wrap={(t) => (
                            <Link to="/invocations/$id" params={{ id: r.invocation }}>
                              {t}
                            </Link>
                          )}
                        />
                      </TD>
                      <TD priority="status">{r.outcome}</TD>
                      <TD numeric>
                        {m.slow_total({
                          ms: fmt.format(r.totalMs),
                          threshold: fmt.format(r.thresholdMs),
                        })}
                      </TD>
                      <TD priority="hidden">
                        {most.label}{" "}
                        <span className="tabular text-muted-foreground">
                          ({fmt.format(most.ms)} ms)
                        </span>
                      </TD>
                    </TRow>
                  );
                })}
              </TBody>
            </Table>
          )}
        </>
      )}
    </div>
  );
}
