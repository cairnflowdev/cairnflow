// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// An answer missing ranges: shows the range names and the reason, never hides it.
// `warnings` has exactly the shape of `/query`.
import { TriangleAlert } from "lucide-react";
import { m } from "@/paraglide/messages";

export type Warning = { range: number | null; reason: string };

export function PartialBanner({ warnings }: { warnings: Warning[] }) {
  if (warnings.length === 0) return null;
  return (
    <div role="status" className="flex gap-2 rounded-md border border-input bg-muted px-3 py-2">
      <TriangleAlert aria-hidden className="mt-0.5 size-4 shrink-0" />
      <div>
        <p className="font-medium">{m.partial_title()}</p>
        <ul className="text-muted-foreground">
          {warnings.map((w) => (
            <li key={`${w.range}-${w.reason}`}>
              {w.range === null
                ? m.partial_node({ reason: w.reason })
                : m.partial_range({ range: String(w.range), reason: w.reason })}
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}
