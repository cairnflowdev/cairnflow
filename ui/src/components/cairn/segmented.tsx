// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// A switch choosing one of a few views: Table/Chart, time range. Same look as the
// view strip of Invocations (`segmentedItem` is shared with link strips). Each item
// is an `aria-pressed` button, and the group has a name.
import { cn } from "@/lib/utils";

// `w-fit`: inside a flex column (`flex-col`), `inline-flex` is still stretched to the
// full width (align-items: stretch) — the button group must fit its content wherever it sits.
export const segmentedGroup =
  "inline-flex h-8 w-fit max-w-full items-center gap-0.5 overflow-x-auto rounded-lg bg-muted p-0.5 [scrollbar-width:none]";

export function segmentedItem(active: boolean) {
  return cn(
    "inline-flex h-7 items-center rounded-md px-2.5 text-sm whitespace-nowrap hover:text-foreground disabled:pointer-events-none disabled:text-muted-foreground",
    active
      ? "bg-background font-medium text-foreground shadow-sm dark:bg-input/50"
      : "text-muted-foreground",
  );
}

export function Segmented<T extends string>({
  label,
  value,
  options,
  onChange,
  className,
}: {
  label: string;
  value: T;
  options: { value: T; label: string; disabled?: boolean; title?: string }[];
  onChange: (value: T) => void;
  className?: string;
}) {
  return (
    // biome-ignore lint/a11y/useSemanticElements: a group of toggle buttons; `fieldset` brings its own layout (legend)
    <div role="group" aria-label={label} className={cn(segmentedGroup, className)}>
      {options.map((o) => (
        <button
          key={o.value}
          type="button"
          aria-pressed={o.value === value}
          disabled={o.disabled}
          title={o.title}
          className={segmentedItem(o.value === value)}
          onClick={() => onChange(o.value)}
        >
          {o.label}
        </button>
      ))}
    </div>
  );
}
