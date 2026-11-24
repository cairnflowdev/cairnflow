// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Results of the SQL console: `read`/`ms`/`partial` are always shown; a table or a
// chart (shape picked from the data, `result-chart.ts`); CSV download.
import { Link } from "@tanstack/react-router";
import { Download } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { EmptyState } from "@/components/cairn/empty-state";
import { PartialBanner } from "@/components/cairn/partial-banner";
import { Segmented } from "@/components/cairn/segmented";
import { Table, TBody, TD, TH, THead, TRow } from "@/components/cairn/table";
import { absolute } from "@/components/cairn/time";
import { buttonVariants } from "@/components/ui/button";
import { NativeSelect } from "@/components/ui/native-select";
import type { Warning } from "@/data/run";
import { toCsv } from "@/lib/csv";
import { type Col, chartShape, isNumeric, isTime } from "@/lib/result-chart";
import { m } from "@/paraglide/messages";
import { getLocale } from "@/paraglide/runtime";
import { BarChart } from "./bar-chart";
import { LineChart } from "./line-chart";

export type Result = {
  columns: Col[];
  rows: unknown[][];
  partial: boolean;
  warnings: Warning[];
  read: number;
  ms: number;
};

/** The table renders at most this many rows — the CSV has them all. */
export const RENDER_MAX = 1_000;

/** Number formatter for the current locale (follows a language change). */
const numFormats = new Map<string, Intl.NumberFormat>();
function num(v: number): string {
  const loc = getLocale();
  let f = numFormats.get(loc);
  if (!f) {
    f = new Intl.NumberFormat(loc, { maximumFractionDigits: 2 });
    numFormats.set(loc, f);
  }
  return f.format(v);
}

function Cell({ v, epoch }: { v: unknown; epoch?: boolean }) {
  if (v === null || v === undefined) return <span className="text-tertiary-foreground">—</span>;
  // Epoch ms columns (`hour_ms`, `at_ms`): print a readable time, the raw number in the tooltip (CSV keeps the number).
  if (epoch && typeof v === "number") {
    return (
      <time dateTime={new Date(v).toISOString()} title={String(v)} className="whitespace-nowrap">
        {absolute(v)}
      </time>
    );
  }
  if (typeof v === "string" && /^[0-9a-f]{48}$/.test(v)) {
    return (
      <Link
        to="/invocations/$id"
        params={{ id: v }}
        className="font-mono text-primary hover:underline"
      >
        {v}
      </Link>
    );
  }
  // Numbers: thousands grouping, at most 2 decimals (CSV keeps the exact value).
  if (typeof v === "number") return <span className="tabular">{num(v)}</span>;
  const s = typeof v === "object" ? JSON.stringify(v) : String(v);
  return (
    <span
      className={typeof v === "number" ? "tabular" : undefined}
      title={s.length > 80 ? s : undefined}
    >
      {s.length > 200 ? `${s.slice(0, 200)}…` : s}
    </span>
  );
}

export function Results({ r }: { r: Result }) {
  const shape = useMemo(() => chartShape(r.columns, r.rows), [r]);
  // Numeric columns right-aligned (comparable by eye); numeric columns holding epoch ms ⇒ date and time.
  const kinds = useMemo(
    () =>
      r.columns.map((c, i) => {
        if (!isNumeric(c, r.rows, i)) return "text" as const;
        return isTime(c, r.rows, i) ? ("epoch" as const) : ("number" as const);
      }),
    [r],
  );
  const [view, setView] = useState<"table" | "chart">("table");
  const [valueCol, setValueCol] = useState(0);
  const fmt = new Intl.NumberFormat(getLocale());
  const csv = useMemo(
    () =>
      URL.createObjectURL(
        new Blob(
          [
            toCsv(
              r.columns.map((c) => c.name),
              r.rows,
            ),
          ],
          { type: "text/csv" },
        ),
      ),
    [r],
  );
  useEffect(() => () => URL.revokeObjectURL(csv), [csv]);
  useEffect(() => {
    setValueCol(0);
    if (!shape) setView("table");
  }, [shape]);

  const series = useMemo(
    () =>
      shape?.kind === "line"
        ? shape.ys.map((i) => ({
            name: r.columns[i]?.name ?? "",
            values: r.rows.map((row) => row[i] as number | null),
          }))
        : [],
    [shape, r],
  );

  return (
    <section aria-label={m.query_results()} className="flex flex-col gap-2">
      <div className="flex flex-wrap items-center gap-3 text-sm">
        <p role="status" className="text-muted-foreground">
          {m.query_meta({
            rows: fmt.format(r.rows.length),
            read: fmt.format(r.read),
            ms: fmt.format(r.ms),
          })}
        </p>
        <Segmented
          className="ml-auto"
          label={m.query_view()}
          value={view}
          onChange={setView}
          options={[
            { value: "table", label: m.query_view_table() },
            {
              value: "chart",
              label: m.query_view_chart(),
              disabled: !shape,
              title: shape ? undefined : m.query_no_chart(),
            },
          ]}
        />
        <a
          href={csv}
          download="query.csv"
          className={buttonVariants({ variant: "ghost", size: "sm" })}
        >
          <Download aria-hidden />
          {m.query_csv()}
        </a>
      </div>
      {r.partial ? <PartialBanner warnings={r.warnings} /> : null}
      {view === "chart" && shape?.kind === "line" ? (
        <>
          <LineChart
            x={r.rows.map((row) => row[shape.x])}
            series={series}
            label={m.query_chart_label({
              y: series.map((s) => s.name).join(", "),
              x: r.columns[shape.x]?.name ?? "",
            })}
          />
          {shape.dropped.length > 0 ? (
            <p className="text-xs text-muted-foreground">
              {m.query_chart_dropped({
                cols: shape.dropped.map((i) => r.columns[i]?.name).join(", "),
              })}
            </p>
          ) : null}
        </>
      ) : view === "chart" && shape?.kind === "bar" ? (
        <>
          {shape.values.length > 1 ? (
            <div className="flex items-center gap-2 text-sm">
              <label htmlFor="chart-value">{m.query_chart_value()}</label>
              <NativeSelect
                id="chart-value"
                className="h-7"
                value={valueCol}
                onChange={(e) => setValueCol(Number(e.target.value))}
              >
                {shape.values.map((ci, k) => (
                  <option key={ci} value={k}>
                    {r.columns[ci]?.name}
                  </option>
                ))}
              </NativeSelect>
            </div>
          ) : null}
          <BarChart
            title={`${r.columns[shape.values[valueCol] ?? 0]?.name ?? ""} · ${shape.label.map((i) => r.columns[i]?.name).join(" / ")}`}
            rows={r.rows.map((row) => ({
              label: shape.label.map((i) => String(row[i] ?? "—")).join(" / "),
              value: Number(row[shape.values[valueCol] ?? 0] ?? 0),
            }))}
          />
          {r.rows.length > 30 ? (
            <p className="text-xs text-muted-foreground">{m.query_chart_top({ n: "30" })}</p>
          ) : null}
        </>
      ) : r.rows.length === 0 ? (
        <EmptyState variant="inline" title={m.query_no_rows()} />
      ) : (
        <>
          <Table stickyHeader dense narrow="scroll">
            <THead>
              {r.columns.map((c, j) => (
                <TH key={c.name} numeric={kinds[j] === "number"} title={c.type}>
                  {c.name}
                </TH>
              ))}
            </THead>
            <TBody>
              {r.rows.slice(0, RENDER_MAX).map((row, i) => (
                // biome-ignore lint/suspicious/noArrayIndexKey: result rows have no key
                <TRow key={i} className="align-top">
                  {row.map((v, j) => (
                    <TD key={r.columns[j]?.name ?? j} numeric={kinds[j] === "number"}>
                      <Cell v={v} epoch={kinds[j] === "epoch"} />
                    </TD>
                  ))}
                </TRow>
              ))}
            </TBody>
          </Table>
          {r.rows.length > RENDER_MAX ? (
            <p className="text-xs text-muted-foreground">
              {m.query_render_cap({ n: fmt.format(RENDER_MAX) })}
            </p>
          ) : null}
        </>
      )}
    </section>
  );
}
