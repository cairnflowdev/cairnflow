// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Line chart for results with a time axis (uPlot). Following the dataviz rules: one
// y axis, ≤ 4 series coloured `--chart-1..4` in a fixed order, 2 px lines, 8 px
// points when there are few points, the legend always shown (doubling as the
// readout on hover — crosshair). Colours are read from tokens at draw time;
// changing theme ⇒ redraw.
import { useEffect, useRef, useState } from "react";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import "./line-chart.css";
import { toSeconds } from "@/lib/result-chart";
import { getLocale } from "@/paraglide/runtime";

function token(name: string): string {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

/** Bumped every time `<html class>` changes (light/dark theme). */
function useThemeKey(): number {
  const [n, setN] = useState(0);
  useEffect(() => {
    const o = new MutationObserver(() => setN((x) => x + 1));
    o.observe(document.documentElement, { attributes: true, attributeFilter: ["class"] });
    return () => o.disconnect();
  }, []);
  return n;
}

export function LineChart({
  x,
  series,
  label,
}: {
  x: unknown[];
  /** `color`: token name (`--chart-ok`…); absent ⇒ `--chart-<index>` (categorical). */
  series: { name: string; values: (number | null)[]; color?: string }[];
  /** Summary for screen readers — the table is the full version. */
  label: string;
}) {
  const host = useRef<HTMLDivElement>(null);
  const theme = useThemeKey();
  // biome-ignore lint/correctness/useExhaustiveDependencies: `theme` only redraws on theme change (colours are read from tokens)
  useEffect(() => {
    const el = host.current;
    if (!el) return;
    // uPlot needs increasing x: sort by time, keeping the value pairs.
    const order = x
      .map((v, i) => [toSeconds(v), i] as const)
      .filter((p): p is [number, number] => p[0] !== null);
    order.sort((a, b) => a[0] - b[0]);
    const data: uPlot.AlignedData = [
      order.map(([t]) => t),
      ...series.map((s) => order.map(([, i]) => s.values[i] ?? null)),
    ];
    const ink = token("--muted-foreground");
    const grid = token("--border");
    // Point markers only when sparse: 24 hourly points each with a marker become a
    // string of beads hiding the line; the cursor still shows the hovered point.
    const few = order.length <= 12;
    const colors = series.map((s, i) => token(s.color ?? `--chart-${i + 1}`));
    const num = new Intl.NumberFormat(getLocale());
    const when = new Intl.DateTimeFormat(getLocale(), { dateStyle: "medium", timeStyle: "short" });
    const u = new uPlot(
      {
        width: el.clientWidth || 600,
        height: 260,
        cursor: { points: { size: 8 } },
        // The legend doubles as the hover readout; when not hovering leave it empty
        // (uPlot's default prints a bold "--" next to each name).
        legend: {
          show: true,
          live: true,
          markers: { width: 2, fill: (_u, i) => colors[i - 1] ?? "transparent" },
        },
        scales: { x: { time: true } },
        axes: [
          { stroke: ink, grid: { stroke: grid, width: 1 }, ticks: { stroke: grid } },
          { stroke: ink, grid: { stroke: grid, width: 1 }, ticks: { stroke: grid }, size: 60 },
        ],
        series: [
          {
            label: "",
            value: (_u, v) => (v == null ? "" : when.format(v * 1000)),
          },
          ...series.map((s, i) => {
            const color = colors[i] as string;
            return {
              label: s.name,
              stroke: color,
              width: 2,
              value: (_u: uPlot, v: number | null) => (v == null ? "" : num.format(v)),
              points: { show: few, size: 8, stroke: color, fill: token("--card") },
            };
          }),
        ],
      },
      data,
      el,
    );
    const ro = new ResizeObserver(() => u.setSize({ width: el.clientWidth, height: 260 }));
    ro.observe(el);
    return () => {
      ro.disconnect();
      u.destroy();
    };
  }, [x, series, theme]);
  return <div ref={host} role="img" aria-label={label} className="w-full text-xs" />;
}
