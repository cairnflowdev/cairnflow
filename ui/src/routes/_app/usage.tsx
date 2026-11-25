// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Usage: number tiles for the current hour (`sys_metering` — has only
// this hour), hourly charts for 24 h / 7 days / 30 days from `sys_handler_history`
// (done/failed in status colours; CU as one series), and a per-handler table.
// Tokens and cost per hour, and per model, from `GET /metering/hours`
// (`sys_metering` has only the current hour). Tenant limits from
// `GET /tenant`.
import { useQuery } from "@tanstack/react-query";
import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import { ChartLine } from "lucide-react";
import { useMemo } from "react";
import { EmptyState } from "@/components/cairn/empty-state";
import { QueryError, QueryState } from "@/components/cairn/query-state";
import { Segmented } from "@/components/cairn/segmented";
import { Skeleton } from "@/components/cairn/skeleton";
import { Table, TBody, TD, TH, THead, TRow } from "@/components/cairn/table";
import { LineChart } from "@/components/query/line-chart";
import { TenantLimits } from "@/components/tenant/limits";
import { ok, rest } from "@/data/api";
import { HISTORY_REFRESH, OVERVIEW_REFRESH, useRows } from "@/data/hooks";
import { queries } from "@/data/queries";
import { useCapabilities } from "@/data/session";
import { fillHours, HOUR_MS, hourFloor, hourKey, hourKeyMs } from "@/lib/hours";
import { can } from "@/lib/roles";
import { byModel, costLabel, formatMs, type MeterHour } from "@/lib/usage";
import { m } from "@/paraglide/messages";
import { getLocale } from "@/paraglide/runtime";

const RANGES = { "24h": 24, "7d": 7 * 24, "30d": 30 * 24 } as const;
type Range = keyof typeof RANGES;

export const Route = createFileRoute("/_app/usage")({
  validateSearch: (s: Record<string, unknown>): { range?: Range } => ({
    range: typeof s.range === "string" && s.range in RANGES ? (s.range as Range) : undefined,
  }),
  component: Usage,
});

type Hour = { hour_ms: number; completed: number | null; failed: number | null; cu: number | null };
type ByHandler = {
  service: string;
  handler: string;
  turns: number;
  completed: number;
  failed: number;
  cu: number;
  p99_ms: number | null;
};
type Meter = {
  invocations: number | null;
  steps: number | null;
  run_ms: number | null;
  input_tokens: number | null;
  output_tokens: number | null;
  cost_micros: number | null;
  /** Billing unit weighted by payload size. */
  units: number | null;
};

const RANGE_LABEL: Record<Range, () => string> = {
  "24h": m.usage_range_24h,
  "7d": m.usage_range_7d,
  "30d": m.usage_range_30d,
};

function Usage() {
  const { range = "24h" } = Route.useSearch();
  const navigate = useNavigate({ from: "/usage" });
  // Anchored at the start of the hour ⇒ the query key is stable within an hour.
  const now = hourFloor(Date.now());
  const since = now - RANGES[range] * HOUR_MS;
  const hours = useRows<Hour>(queries.history(since), { refresh: HISTORY_REFRESH });
  const byHandler = useRows<ByHandler>(queries.historyByHandler(since), {
    refresh: HISTORY_REFRESH,
  });
  const meter = useRows<Meter>(queries.meteringNow(), { refresh: OVERVIEW_REFRESH });
  const metered = useQuery({
    queryKey: ["metering-hours", since, now],
    queryFn: () =>
      ok(
        rest.GET("/metering/hours", {
          params: { query: { from: hourKey(since), to: hourKey(now) } },
        }),
      ),
    // The summary flushes every minute; refresh slowly like the "this hour" tile.
    refetchInterval: 60_000,
  });
  // No price table set ⇒ cost cannot be computed, do not print $0.00.
  // Price table unreadable (role) ⇒ treat it as present, as before.
  // The price table belongs to the cluster (`cluster` capability): other roles do not ask — a `403` every
  // time the screen opens is only noise — and treat it as priced.
  const cluster = can(useCapabilities(), "cluster");
  const pricing = useQuery({
    queryKey: ["pricing"],
    queryFn: () => ok(rest.GET("/metering/pricing")),
    enabled: cluster,
  });
  const priced = !cluster || (pricing.data ? pricing.data.models.length > 0 : !pricing.isPending);
  const fmt = new Intl.NumberFormat(getLocale());
  return (
    <div className="flex flex-col gap-6 p-6">
      <h1 className="text-xl font-semibold">{m.nav_usage()}</h1>
      <section aria-labelledby="usage-now">
        <h2 id="usage-now" className="mb-2 text-base font-semibold">
          {m.usage_this_hour()}
        </h2>
        <QueryState q={meter}>
          {({ rows: [r] }) => <Tiles r={r} fmt={fmt} priced={priced} />}
        </QueryState>
      </section>
      <TenantLimits />
      <Segmented
        className="self-start"
        label={m.usage_range()}
        value={range}
        onChange={(r) => navigate({ search: { range: r === "24h" ? undefined : r } })}
        options={(Object.keys(RANGES) as Range[]).map((r) => ({
          value: r,
          label: RANGE_LABEL[r](),
        }))}
      />
      <QueryState q={hours}>
        {({ rows }) =>
          rows.length === 0 ? (
            <EmptyState icon={<ChartLine />} title={m.usage_empty()}>
              {m.usage_empty_body()}
            </EmptyState>
          ) : (
            <Charts rows={rows} since={since} now={now} />
          )
        }
      </QueryState>
      <section aria-labelledby="usage-tokens" className="flex flex-col gap-4">
        <h2 id="usage-tokens" className="text-base font-semibold">
          {m.usage_tokens_title({ range: RANGE_LABEL[range]() })}
        </h2>
        {metered.isError ? (
          <QueryError error={metered.error} />
        ) : metered.data === undefined ? (
          <Skeleton className="h-40" />
        ) : (
          <Tokens hours={metered.data.hours} since={since} now={now} fmt={fmt} priced={priced} />
        )}
      </section>
      <section aria-labelledby="usage-by-handler">
        <h2 id="usage-by-handler" className="mb-2 text-base font-semibold">
          {m.usage_by_handler({ range: RANGE_LABEL[range]() })}
        </h2>
        <QueryState q={byHandler}>
          {({ rows }) =>
            rows.length === 0 ? (
              <EmptyState variant="inline" title={m.usage_empty()} />
            ) : (
              <Table>
                <THead>
                  <TH>{m.col_target()}</TH>
                  <TH numeric>{m.usage_turns()}</TH>
                  <TH numeric>{m.usage_completed()}</TH>
                  <TH numeric>{m.usage_failed()}</TH>
                  <TH numeric>{m.usage_cu()}</TH>
                  <TH numeric>{m.kpi_p99()}</TH>
                </THead>
                <TBody>
                  {rows.map((r) => (
                    <TRow key={`${r.service}/${r.handler}`}>
                      <TD priority="primary">
                        <Link
                          to="/invocations"
                          search={{ service: r.service, handler: r.handler }}
                          className="hover:underline"
                        >
                          {r.service}/{r.handler}
                        </Link>
                      </TD>
                      <TD numeric>{fmt.format(r.turns)}</TD>
                      <TD numeric>{fmt.format(r.completed)}</TD>
                      <TD numeric>{fmt.format(r.failed)}</TD>
                      <TD numeric>{fmt.format(r.cu)}</TD>
                      <TD numeric>{r.p99_ms != null ? formatMs(r.p99_ms) : "—"}</TD>
                    </TRow>
                  ))}
                </TBody>
              </Table>
            )
          }
        </QueryState>
      </section>
    </div>
  );
}

function Tiles({
  r,
  fmt,
  priced,
}: {
  r: Meter | undefined;
  fmt: Intl.NumberFormat;
  priced: boolean;
}) {
  const tiles: [string, string][] = [
    [m.usage_invocations(), fmt.format(r?.invocations ?? 0)],
    [m.usage_steps(), fmt.format(r?.steps ?? 0)],
    [m.usage_units(), fmt.format(r?.units ?? 0)],
    [m.usage_run_time(), `${fmt.format(Math.round((r?.run_ms ?? 0) / 1000))} s`],
    [m.usage_tokens_in(), fmt.format(r?.input_tokens ?? 0)],
    [m.usage_tokens_out(), fmt.format(r?.output_tokens ?? 0)],
    [m.usage_cost(), costLabel(r?.cost_micros ?? 0, priced) ?? m.usage_no_price()],
  ];
  return (
    <dl className="grid grid-cols-2 gap-3 sm:grid-cols-4 lg:grid-cols-7">
      {tiles.map(([label, value]) => (
        <div key={label} className="rounded-md border border-border p-3">
          <dt className="text-xs text-muted-foreground">{label}</dt>
          <dd className="text-lg font-semibold tabular">{value}</dd>
        </div>
      ))}
    </dl>
  );
}

function Charts({ rows, since, now }: { rows: Hour[]; since: number; now: number }) {
  const filled = useMemo(
    () => fillHours(rows, ["completed", "failed", "cu"], since, now),
    [rows, since, now],
  );
  const x = useMemo(() => filled.map((r) => r.hour_ms), [filled]);
  const status = useMemo(
    () => [
      { name: m.usage_completed(), values: filled.map((r) => r.completed), color: "--chart-ok" },
      { name: m.usage_failed(), values: filled.map((r) => r.failed), color: "--chart-fail" },
    ],
    [filled],
  );
  const cu = useMemo(() => [{ name: m.usage_cu(), values: filled.map((r) => r.cu) }], [filled]);
  return (
    <div className="grid grid-cols-1 gap-6 xl:grid-cols-2">
      <figure className="flex flex-col gap-1">
        <figcaption className="text-sm font-medium">{m.usage_chart_status()}</figcaption>
        <LineChart x={x} series={status} label={m.usage_chart_status_label()} />
      </figure>
      <figure className="flex flex-col gap-1">
        <figcaption className="text-sm font-medium">{m.usage_chart_cu()}</figcaption>
        <LineChart x={x} series={cu} label={m.usage_chart_cu_label()} />
      </figure>
    </div>
  );
}

/** Tokens in/out, cost per hour, and a per-model table for the selected range. */
function Tokens({
  hours,
  since,
  now,
  fmt,
  priced,
}: {
  hours: MeterHour[];
  since: number;
  now: number;
  fmt: Intl.NumberFormat;
  priced: boolean;
}) {
  const filled = useMemo(
    () =>
      fillHours(
        hours.map((h) => ({
          hour_ms: hourKeyMs(h.hour),
          input: h.inputTokens,
          output: h.outputTokens,
          cost: h.costMicros / 1_000_000,
        })),
        ["input", "output", "cost"],
        since,
        now,
      ),
    [hours, since, now],
  );
  const models = useMemo(() => byModel(hours), [hours]);
  const usd = new Intl.NumberFormat(getLocale(), {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 4,
  });
  if (hours.every((h) => h.inputTokens === 0 && h.outputTokens === 0)) {
    return <EmptyState variant="inline" title={m.usage_tokens_empty()} />;
  }
  const x = filled.map((r) => r.hour_ms);
  return (
    <>
      <div className="grid grid-cols-1 gap-6 xl:grid-cols-2">
        <figure className="flex flex-col gap-1">
          <figcaption className="text-sm font-medium">{m.usage_chart_tokens()}</figcaption>
          <LineChart
            x={x}
            series={[
              { name: m.usage_tokens_in(), values: filled.map((r) => r.input) },
              { name: m.usage_tokens_out(), values: filled.map((r) => r.output) },
            ]}
            label={m.usage_chart_tokens_label()}
          />
        </figure>
        {priced ? (
          <figure className="flex flex-col gap-1">
            <figcaption className="text-sm font-medium">{m.usage_chart_cost()}</figcaption>
            <LineChart
              x={x}
              series={[{ name: m.usage_cost(), values: filled.map((r) => r.cost) }]}
              label={m.usage_chart_cost_label()}
            />
          </figure>
        ) : (
          <p className="text-sm text-muted-foreground">{m.usage_no_price_note()}</p>
        )}
      </div>
      <Table>
        <caption className="mb-1 text-left text-sm font-medium">{m.usage_by_model()}</caption>
        <THead>
          <TH>{m.usage_model()}</TH>
          <TH numeric>{m.usage_tokens_in()}</TH>
          <TH numeric>{m.usage_tokens_out()}</TH>
          {priced ? <TH numeric>{m.usage_cost()}</TH> : null}
        </THead>
        <TBody>
          {models.map((r) => (
            <TRow key={r.model ?? ""}>
              <TD priority="primary">
                {r.model ?? (
                  <span className="text-muted-foreground">{m.usage_model_unknown()}</span>
                )}
              </TD>
              <TD numeric>{fmt.format(r.input)}</TD>
              <TD numeric>{fmt.format(r.output)}</TD>
              {priced ? <TD numeric>{usd.format(r.cost / 1_000_000)}</TD> : null}
            </TRow>
          ))}
        </TBody>
      </Table>
    </>
  );
}
