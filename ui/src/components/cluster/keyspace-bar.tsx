// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Keyspace bar: each range is a segment on [0, 2⁶⁴), coloured by leader (fixed
// order, with a legend); ranges with a split/merge intent in progress get a dashed
// border. Each segment is a button: selects the range, tooltip on hover/focus. No
// text on the segments — some colours are below 3:1 against text; labels are in the
// tooltip, the `aria-label` and the range table below.
import { useState } from "react";
import type { components } from "@/data/api.gen";
import { leaderColors, segments, shortNode } from "@/lib/keyspace";
import { cn } from "@/lib/utils";
import { m } from "@/paraglide/messages";

type Range = components["schemas"]["RangeView"];

export function KeyspaceBar({
  ranges,
  selected,
  onSelect,
}: {
  ranges: Range[];
  selected: number | null;
  onSelect: (range: number) => void;
}) {
  const segs = segments(ranges);
  const colors = leaderColors(segs.map((s) => s.leader));
  const byId = new Map(ranges.map((r) => [r.range, r]));
  const [hover, setHover] = useState<number | null>(null);
  return (
    <div className="flex flex-col gap-2">
      {/* `div role=group`, not `<fieldset>`: fieldset's own layout (legend,
          min-content) sometimes collapsed the absolutely positioned segments to 0 — e2e
          saw the buttons as "not visible". */}
      {/* biome-ignore lint/a11y/useSemanticElements: see the comment above */}
      <div
        role="group"
        aria-label={m.keyspace_caption()}
        className="relative h-8 w-full rounded-md bg-muted"
      >
        {segs.map((s) => {
          const r = byId.get(s.range);
          const intent = r?.intent != null;
          const label = m.keyspace_range_label({
            range: String(s.range),
            start: r?.start ?? "",
            end: r?.end ?? "",
            leader: s.leader ? shortNode(s.leader) : m.keyspace_no_leader(),
          });
          return (
            <button
              key={s.range}
              type="button"
              aria-label={intent ? `${label} · ${m.keyspace_intent()}` : label}
              aria-pressed={selected === s.range}
              onClick={() => onSelect(s.range)}
              onMouseEnter={() => setHover(s.range)}
              onMouseLeave={() => setHover(null)}
              onFocus={() => setHover(s.range)}
              onBlur={() => setHover(null)}
              className={cn(
                // 2 px gap between segments (a spacer in the background colour).
                "absolute top-0 bottom-0 border-r-2 border-background text-xs outline-offset-2 hover:opacity-85 focus-visible:outline-2 focus-visible:outline-ring",
                intent && "border-2 border-dashed border-foreground",
                selected === s.range && "ring-2 ring-foreground ring-inset",
              )}
              style={{
                left: `${s.left}%`,
                width: `max(24px, ${s.width}%)`,
                background: (s.leader && colors.get(s.leader)) || "var(--muted-foreground)",
              }}
            >
              {hover === s.range ? (
                <span
                  role="tooltip"
                  className="pointer-events-none absolute -top-8 left-0 z-10 rounded-md border border-border bg-popover px-2 py-0.5 whitespace-nowrap text-popover-foreground shadow-sm"
                >
                  {label}
                </span>
              ) : null}
            </button>
          );
        })}
      </div>
      {colors.size > 1 ? (
        <ul aria-label={m.keyspace_legend()} className="flex flex-wrap gap-x-4 gap-y-1 text-xs">
          {[...colors].map(([node, color]) => (
            <li key={node} className="flex items-center gap-1.5">
              <span
                aria-hidden
                className="inline-block size-3 rounded-sm"
                style={{ background: color }}
              />
              <span className="font-mono">{shortNode(node)}</span>
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  );
}
