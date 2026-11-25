// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Overview. No deployment yet ⇒ the onboarding flow. Adaptive refresh
// 10–60 s.
import { useQuery } from "@tanstack/react-query";
import { createFileRoute, Link } from "@tanstack/react-router";
import { CircleCheck, TriangleAlert } from "lucide-react";
import { lazy, Suspense, useMemo } from "react";
import { EmptyState } from "@/components/cairn/empty-state";
import { QueryError, QueryState, Updated } from "@/components/cairn/query-state";
import { Skeleton } from "@/components/cairn/skeleton";
import { Table, TBody, TD, TH, THead, TRow } from "@/components/cairn/table";
import { Onboarding } from "@/components/overview/onboarding";
import { ok, rest } from "@/data/api";
import { HISTORY_REFRESH, OVERVIEW_REFRESH, useRows } from "@/data/hooks";
import { queries } from "@/data/queries";
import { fillHours, HOUR_MS, hourFloor } from "@/lib/hours";
import { useThresholds } from "@/lib/thresholds";
import { formatMs } from "@/lib/usage";
import { cn } from "@/lib/utils";
import { m } from "@/paraglide/messages";
import { getLocale } from "@/paraglide/runtime";

export const Route = createFileRoute("/_app/")({ component: Overview });

type Count = { service: string; class: string; n: number };
type Stat = {
  service: string;
  handler: string;
  turns: number;
  completed: number;
  failed: number;
  p99_ms: number | null;
};
type Health = { service: string; failed: number; total: number };
type Pinned = { id: string; pinned: number | null };

const fmt = (n: number) => new Intl.NumberFormat(getLocale()).format(n);
const pct = (x: number) =>
  new Intl.NumberFormat(getLocale(), { style: "percent", maximumFractionDigits: 1 }).format(x);
const ms = (x: number) => formatMs(x);

function Overview() {
  const deployments = useQuery({
    queryKey: ["deployments"],
    queryFn: () => ok(rest.GET("/deployments")),
    refetchInterval: 30_000,
  });
  if (deployments.isPending) {
    return (
      <Page>
        <Skeleton className="h-24 w-full" />
      </Page>
    );
  }
  if (deployments.isError) {
    return (
      <Page>
        <QueryError error={deployments.error} />
      </Page>
    );
  }
  if (deployments.data.length === 0) {
    return (
      <Page>
        <Onboarding />
      </Page>
    );
  }
  return (
    <Dashboard
      drained={deployments.data.filter((d) => d.currentFor.length === 0).map((d) => d.id)}
    />
  );
}

function Page({ children, updated }: { children: React.ReactNode; updated?: number }) {
  return (
    <div className="flex flex-col gap-6 p-6">
      <div className="flex items-baseline justify-between gap-4">
        <h1 className="text-xl font-semibold">{m.nav_overview()}</h1>
        {updated ? <Updated at={updated} /> : null}
      </div>
      {children}
    </div>
  );
}

function Dashboard({ drained }: { drained: string[] }) {
  const counts = useRows<Count>(queries.invocationCounts(), { refresh: OVERVIEW_REFRESH });
  const stats = useRows<Stat>(queries.handlerStats(), { refresh: OVERVIEW_REFRESH });
  const health = useRows<Health>(queries.serviceHealth(), { refresh: OVERVIEW_REFRESH });
  const pinned = useRows<Pinned>(queries.deploymentsPinned(), { refresh: OVERVIEW_REFRESH });
  const services = useQuery({ queryKey: ["services"], queryFn: () => ok(rest.GET("/services")) });
  const limits = useThresholds();

  const by = (cls: string, svc?: string) =>
    (counts.data?.rows ?? [])
      .filter((r) => r.class === cls && (svc === undefined || r.service === svc))
      .reduce((a, r) => a + Number(r.n), 0);
  const statRows = stats.data?.rows ?? [];
  const total = sum(statRows);
  const slowest = statRows.reduce<Stat | null>(
    (a, r) => (r.p99_ms !== null && (a === null || Number(r.p99_ms) > Number(a.p99_ms)) ? r : a),
    null,
  );

  const unhealthy = (health.data?.rows ?? []).filter(
    (h) =>
      Number(h.total) >= limits.minRuns && Number(h.failed) / Number(h.total) > limits.failedRate,
  );
  const drainedReady = drained.filter((id) =>
    (pinned.data?.rows ?? []).some((p) => p.id === id && p.pinned === 0),
  );
  const paused = by("paused");
  const failed = by("failed");
  const serviceNames = [
    ...new Set([
      ...(services.data ?? []).map((s) => s.name),
      ...(counts.data?.rows ?? []).map((r) => r.service),
    ]),
  ].sort();
  const attention = paused > 0 || unhealthy.length > 0 || drainedReady.length > 0;
  // "Nothing needs attention" only once every source has arrived — while loading we do not know yet.
  const known = counts.isSuccess && health.isSuccess && pinned.isSuccess;

  return (
    <Page updated={counts.dataUpdatedAt || undefined}>
      <QueryState q={counts} skeleton={<Skeleton className="h-24 w-full" />}>
        {() => (
          <dl className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-5">
            <Kpi
              label={m.kpi_running()}
              value={fmt(by("active"))}
              caption={m.kpi_running_caption()}
              to={{ view: "running", root: 0 }}
            />
            <Kpi
              label={m.kpi_paused()}
              value={fmt(paused)}
              caption={m.kpi_paused_caption()}
              to={{ view: "paused", root: 0 }}
              tone={paused > 0 ? "warn" : undefined}
            />
            <Kpi
              label={m.kpi_failed()}
              value={fmt(failed)}
              caption={m.kpi_failed_caption()}
              to={{ view: "failed", root: 0 }}
              tone={failed > 0 ? "bad" : undefined}
            />
            <Kpi
              label={m.kpi_success()}
              value={total.ended > 0 ? pct(total.ok / total.ended) : "—"}
              caption={
                total.ended > 0
                  ? m.kpi_success_caption({ ok: fmt(total.ok), failed: fmt(total.failed) })
                  : m.kpi_no_turns()
              }
            />
            <Kpi
              label={m.kpi_p99()}
              value={slowest ? ms(Number(slowest.p99_ms)) : "—"}
              caption={
                slowest
                  ? m.kpi_p99_caption({ handler: `${slowest.service}/${slowest.handler}` })
                  : m.kpi_no_turns()
              }
            />
          </dl>
        )}
      </QueryState>
      {/* Takes space only when there is work; otherwise one small line. */}
      {attention ? (
        <section
          aria-labelledby="attention"
          className="rounded-lg border border-status-paused-foreground/30 bg-status-paused/40 px-4 py-3"
        >
          <h2 id="attention" className="mb-1.5 text-sm font-semibold">
            {m.overview_attention()}
          </h2>
          <ul className="flex flex-col gap-1.5 text-sm">
            {paused > 0 ? (
              <Attention>
                <Link
                  to="/invocations"
                  search={{ view: "paused", root: 0 }}
                  className="hover:underline"
                >
                  {m.attention_paused({ n: fmt(paused) })}
                </Link>
              </Attention>
            ) : null}
            {unhealthy.map((h) => (
              <Attention key={h.service}>
                <Link
                  to="/invocations"
                  search={{ service: h.service, view: "failed", root: 0 }}
                  className="hover:underline"
                >
                  {m.attention_failing({
                    service: h.service,
                    rate: pct(Number(h.failed) / Number(h.total)),
                  })}
                </Link>
              </Attention>
            ))}
            {drainedReady.map((id) => (
              <Attention key={id}>
                <Link to="/deployments/$id" params={{ id }} className="hover:underline">
                  {m.attention_drained({ id })}
                </Link>
              </Attention>
            ))}
          </ul>
        </section>
      ) : known ? (
        <p className="-mt-2 flex items-center gap-1.5 text-sm text-muted-foreground">
          <CircleCheck aria-hidden className="size-4" />
          {m.attention_none()}
        </p>
      ) : null}
      <Throughput />
      <div className="grid grid-cols-1 gap-6 xl:grid-cols-[minmax(0,3fr)_minmax(0,2fr)]">
        <section aria-labelledby="by-service">
          <h2 id="by-service" className="mb-2 text-base font-semibold">
            {m.overview_by_service()}
          </h2>
          <QueryState q={counts}>
            {() => (
              <Table>
                <THead>
                  <TH>{m.col_service()}</TH>
                  <TH numeric>{m.kpi_running()}</TH>
                  <TH numeric>{m.kpi_paused()}</TH>
                  <TH numeric>{m.kpi_failed()}</TH>
                  <TH numeric>{m.stat_turns()}</TH>
                  <TH numeric>{m.col_success()}</TH>
                  <TH numeric>{m.col_p99()}</TH>
                </THead>
                <TBody className="tabular">
                  {serviceNames.map((svc) => {
                    const info = services.data?.find((s) => s.name === svc);
                    const t = sum(statRows.filter((r) => r.service === svc));
                    return (
                      <TRow key={svc}>
                        {/* The name column takes the rest of the table (`w-full max-w-0`) and cuts long names
                            with "…" (full name in `title`): it does not push the numeric columns out of the frame. */}
                        <TD priority="primary" className="w-full max-w-0">
                          <span className="flex min-w-0 items-baseline gap-2">
                            <Link
                              to="/invocations"
                              search={{ service: svc }}
                              title={svc}
                              className="truncate font-medium hover:underline"
                            >
                              {svc}
                            </Link>
                            {info ? (
                              <span className="shrink-0 text-xs text-muted-foreground">
                                {TY[info.ty]()}
                              </span>
                            ) : null}
                          </span>
                        </TD>
                        <Num n={by("active", svc)} />
                        <Num n={by("paused", svc)} />
                        <Num
                          n={by("failed", svc)}
                          bad
                          to={{ service: svc, view: "failed", root: 0 }}
                        />
                        <Num n={t.turns} />
                        <TD numeric>{t.ended > 0 ? pct(t.ok / t.ended) : <Dim>—</Dim>}</TD>
                        <TD numeric>{t.p99 === null ? <Dim>—</Dim> : ms(t.p99)}</TD>
                      </TRow>
                    );
                  })}
                </TBody>
              </Table>
            )}
          </QueryState>
        </section>
        <section aria-labelledby="busiest">
          <h2 id="busiest" className="mb-2 text-base font-semibold">
            {m.overview_busiest()}
          </h2>
          <QueryState q={stats}>
            {({ rows }) =>
              rows.length === 0 ? (
                <EmptyState variant="inline" title={m.kpi_no_turns()} />
              ) : (
                <Table>
                  <THead>
                    <TH>{m.col_target()}</TH>
                    <TH numeric>{m.stat_turns()}</TH>
                    <TH numeric>{m.col_success()}</TH>
                    <TH numeric>{m.col_p99()}</TH>
                  </THead>
                  <TBody className="tabular">
                    {[...rows]
                      .sort(
                        (a, b) =>
                          Number(b.turns) - Number(a.turns) ||
                          `${a.service}/${a.handler}`.localeCompare(`${b.service}/${b.handler}`),
                      )
                      .slice(0, BUSIEST)
                      .map((r) => {
                        const t = sum([r]);
                        return (
                          <TRow key={`${r.service}/${r.handler}`}>
                            <TD priority="primary" className="w-full max-w-0">
                              <Link
                                to="/invocations"
                                search={{ service: r.service, handler: r.handler }}
                                title={`${r.service}/${r.handler}`}
                                className="block truncate hover:underline"
                              >
                                {r.service}/{r.handler}
                              </Link>
                            </TD>
                            <Num n={t.turns} />
                            <TD
                              numeric
                              className={cn(t.failed > 0 && "font-medium text-destructive")}
                            >
                              {t.ended > 0 ? pct(t.ok / t.ended) : <Dim>—</Dim>}
                            </TD>
                            <TD numeric>{t.p99 === null ? <Dim>—</Dim> : ms(t.p99)}</TD>
                          </TRow>
                        );
                      })}
                  </TBody>
                </Table>
              )
            }
          </QueryState>
        </section>
      </div>
    </Page>
  );
}

// uPlot loads lazily: the Overview draws its frame first, the chart comes later.
const LineChart = lazy(() =>
  import("@/components/query/line-chart").then((x) => ({ default: x.LineChart })),
);

type Hour = { hour_ms: number; completed: number; failed: number; cu: number };

/**
 * Done/failed per hour, last 24 h ("Throughput & errors" —
 * `sys_handler_history`, the same query as Usage). Empty hours filled with 0.
 */
function Throughput() {
  const now = Date.now();
  const since = hourFloor(now) - 23 * HOUR_MS;
  const hours = useRows<Hour>(queries.history(since), { refresh: HISTORY_REFRESH });
  const filled = useMemo(
    () => fillHours(hours.data?.rows ?? [], ["completed", "failed", "cu"], since, now),
    [hours.data, since, now],
  );
  return (
    <section aria-labelledby="throughput" className="flex flex-col gap-2">
      <h2 id="throughput" className="text-base font-semibold">
        {m.overview_throughput()}
      </h2>
      <QueryState q={hours} skeleton={<Skeleton className="h-64 w-full" />}>
        {({ rows }) =>
          rows.length === 0 ? (
            <EmptyState variant="inline" title={m.overview_throughput_empty()} />
          ) : (
            <Suspense fallback={<Skeleton className="h-64 w-full" />}>
              <LineChart
                x={filled.map((r) => r.hour_ms)}
                series={[
                  {
                    name: m.usage_completed(),
                    values: filled.map((r) => Number(r.completed)),
                    color: "--chart-ok",
                  },
                  {
                    name: m.usage_failed(),
                    values: filled.map((r) => Number(r.failed)),
                    color: "--chart-fail",
                  },
                ]}
                label={m.usage_chart_status_label()}
              />
            </Suspense>
          )
        }
      </QueryState>
    </section>
  );
}

/** Number of handlers in the "busiest" table. */
const BUSIEST = 8;

const TY: Record<"service" | "virtualObject" | "workflow", () => string> = {
  service: m.service_ty_service,
  virtualObject: m.service_ty_object,
  workflow: m.service_ty_workflow,
};

/**
 * Sums the stats rows. `turns` is every turn (including stuck turns, retry turns);
 * the success rate divides over *ended* turns (`ended` = done + error). Merged p99 takes the
 * max (percentiles cannot be summed).
 */
function sum(rows: Stat[]) {
  let turns = 0;
  let ok = 0;
  let failed = 0;
  let p99: number | null = null;
  for (const r of rows) {
    turns += Number(r.turns);
    ok += Number(r.completed);
    failed += Number(r.failed);
    if (r.p99_ms !== null) p99 = Math.max(p99 ?? 0, Number(r.p99_ms));
  }
  return { turns, ok, failed, ended: ok + failed, p99 };
}

/** Number cell: 0 printed dimmed (see at a glance which row has trouble); `bad` > 0 in red, with a link. */
function Num({
  n,
  bad,
  to,
}: {
  n: number;
  bad?: boolean;
  to?: { service: string; view: "failed"; root: 0 };
}) {
  if (n === 0)
    return (
      <TD numeric>
        <Dim>0</Dim>
      </TD>
    );
  const text = fmt(n);
  return (
    <TD numeric className={cn(bad && "font-medium text-destructive")}>
      {to ? (
        <Link to="/invocations" search={to} className="hover:underline">
          {text}
        </Link>
      ) : (
        text
      )}
    </TD>
  );
}

function Dim({ children }: { children: React.ReactNode }) {
  return <span className="text-muted-foreground">{children}</span>;
}

function Attention({ children }: { children: React.ReactNode }) {
  return (
    <li className="flex gap-2">
      <TriangleAlert aria-hidden className="mt-0.5 size-4 shrink-0" />
      <span>{children}</span>
    </li>
  );
}

function Kpi({
  label,
  value,
  caption,
  to,
  tone,
}: {
  label: string;
  value: string;
  caption?: string;
  /**
   * List the card opens. The KPI counts **every** invocation (`sys_invocation_counts`),
   * so the link turns "Roots only" off — the number on the card and the list's number are the same.
   */
  to?: { view: "running" | "paused" | "failed"; root: 0 };
  /** `warn`: needs a person (paused); `bad`: has errors. Always with an icon, not colour alone. */
  tone?: "warn" | "bad";
}) {
  // Clickable card: the link sits inside `dd` (valid `dl` structure) and covers the whole card.
  return (
    <div
      className={cn(
        "relative flex flex-col gap-1 rounded-lg border border-border p-4 has-[a:hover]:bg-accent",
        tone === "bad" && "border-destructive/40",
        tone === "warn" && "border-status-paused-foreground/40",
      )}
    >
      <dt className="flex items-center gap-1.5 text-xs text-muted-foreground">
        {label}
        {tone ? (
          <TriangleAlert
            aria-label={m.needs_attention()}
            className={cn(
              "size-3.5",
              tone === "bad" ? "text-destructive" : "text-status-paused-foreground",
            )}
          />
        ) : null}
      </dt>
      <dd className="text-2xl leading-tight font-semibold tabular">
        {to ? (
          <Link
            to="/invocations"
            search={to}
            aria-label={`${label}: ${value}`}
            className="after:absolute after:inset-0"
          >
            {value}
          </Link>
        ) : (
          value
        )}
      </dd>
      {caption ? (
        <dd className="truncate text-xs text-muted-foreground" title={caption}>
          {caption}
        </dd>
      ) : null}
    </div>
  );
}
