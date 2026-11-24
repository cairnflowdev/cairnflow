// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Horizontal bars for "label + number" results. Following the dataviz rules: one
// series ⇒ one colour (`--chart-1`), no legend (the title names the column); bar ends
// rounded 4 px anchored at the baseline, 2 px gaps; labels and numbers use text
// tokens, not the series colour; hover or focus ⇒ a "label: value" tooltip.
import { useState } from "react";
import { MAX_BARS } from "@/lib/result-chart";
import { getLocale } from "@/paraglide/runtime";

export function BarChart({
  rows,
  title,
}: {
  rows: { label: string; value: number }[];
  title: string;
}) {
  const shown = rows.slice(0, MAX_BARS);
  const max = Math.max(0, ...shown.map((r) => r.value));
  const min = Math.min(0, ...shown.map((r) => r.value));
  const span = max - min || 1;
  const fmt = new Intl.NumberFormat(getLocale());
  const [hover, setHover] = useState<number | null>(null);
  return (
    <figure className="flex flex-col gap-1">
      <figcaption className="text-xs text-muted-foreground">{title}</figcaption>
      <ul className="flex flex-col gap-0.5">
        {shown.map((r, i) => {
          const w = (Math.abs(r.value) / span) * 100;
          return (
            <li
              // biome-ignore lint/suspicious/noArrayIndexKey: labels may repeat; the order is fixed by the result
              key={i}
              className="relative grid grid-cols-[minmax(6rem,14rem)_1fr] items-center gap-2 text-xs"
            >
              <span className="truncate text-right text-muted-foreground" title={r.label}>
                {r.label}
              </span>
              <span className="relative h-6">
                {/* A button covering the whole row (hit area ≥ 24 px, focusable) — the tooltip opens
                    on hover or focus; the coloured bar is the thin part inside. */}
                <button
                  type="button"
                  aria-label={`${r.label}: ${fmt.format(r.value)}`}
                  onMouseEnter={() => setHover(i)}
                  onMouseLeave={() => setHover(null)}
                  onFocus={() => setHover(i)}
                  onBlur={() => setHover(null)}
                  className="absolute inset-0 cursor-default rounded-sm outline-offset-1 focus-visible:outline-2 focus-visible:outline-ring"
                >
                  <span
                    aria-hidden
                    className="absolute top-1 bottom-1 rounded-r-[4px] bg-[var(--chart-1)]"
                    style={{
                      left: `${((Math.min(r.value, 0) - min) / span) * 100}%`,
                      width: `max(2px, ${w}%)`,
                    }}
                  />
                </button>
                {hover === i ? (
                  <span
                    role="tooltip"
                    className="pointer-events-none absolute -top-7 z-10 rounded-md border border-border bg-popover px-2 py-0.5 whitespace-nowrap text-popover-foreground shadow-sm"
                    style={{
                      left: `${Math.min(80, ((Math.max(r.value, 0) - min) / span) * 100)}%`,
                    }}
                  >
                    {r.label}: <span className="tabular">{fmt.format(r.value)}</span>
                  </span>
                ) : null}
              </span>
            </li>
          );
        })}
      </ul>
    </figure>
  );
}
