// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Underlined tab strip: switches content between the sections of one screen — the
// sections of the invocation detail (links, one URL per tab, `tabList`/`tabItem`), the
// sidebar of the Query screen (in-page buttons, `Tabs` + `TabPanel`). Unlike
// `Segmented`: a switch between views of the same content (Table/Chart, time range).
// The hosted web console copies this file verbatim (its quickstart uses tabs with step
// numbers — `step`) and checks the copy for drift.
import { type KeyboardEvent, type ReactNode, useRef } from "react";
import { cn } from "@/lib/utils";

// Phones: the strip scrolls horizontally on its own instead of widening the page. `shrink-0`:
// `overflow-x-auto` drops the content-based minimum height, so inside a height-capped flex
// column (the sticky sidebar of the Query screen) the strip would shrink, clipping the top
// half of the text.
export const tabList =
  "flex shrink-0 gap-1 overflow-x-auto border-b border-border [scrollbar-width:none]";

export function tabItem(active: boolean) {
  return cn(
    "-mb-px inline-flex shrink-0 items-center gap-1.5 border-b-2 border-transparent px-3 py-1.5 text-sm whitespace-nowrap hover:text-foreground [&_svg]:size-4 [&_svg]:shrink-0",
    active ? "border-primary font-medium" : "text-muted-foreground",
  );
}

const tabId = (id: string, value: string) => `${id}-tab-${value}`;
const panelId = (id: string, value: string) => `${id}-panel-${value}`;

/**
 * In-page tabs following the ARIA pattern: only the selected tab takes Tab, left/right
 * arrows (wrapping) and Home/End move and select at once. Content goes in a
 * [`TabPanel`] with the same `id`.
 */
export function Tabs<T extends string>({
  id,
  label,
  value,
  options,
  onChange,
  className,
}: {
  /** Id prefix of the tabs and panels — unique within the page. */
  id: string;
  label: string;
  value: T;
  /**
   * `icon`: icon before the text (decorative). `step`: step number in a circle before
   * the text — the tab strip is a sequence of steps (quickstart); hidden on narrow
   * screens, the text still carries the meaning.
   */
  options: { value: T; label: string; icon?: ReactNode; step?: number }[];
  onChange: (value: T) => void;
  className?: string;
}) {
  const refs = useRef(new Map<T, HTMLButtonElement>());
  const onKeyDown = (e: KeyboardEvent, i: number) => {
    const n = options.length;
    const to =
      e.key === "ArrowRight"
        ? (i + 1) % n
        : e.key === "ArrowLeft"
          ? (i - 1 + n) % n
          : e.key === "Home"
            ? 0
            : e.key === "End"
              ? n - 1
              : -1;
    const next = options[to];
    if (!next) return;
    e.preventDefault();
    onChange(next.value);
    refs.current.get(next.value)?.focus();
  };
  return (
    <div role="tablist" aria-label={label} className={cn(tabList, className)}>
      {options.map((o, i) => {
        const active = o.value === value;
        return (
          <button
            key={o.value}
            ref={(el) => {
              if (el) refs.current.set(o.value, el);
              else refs.current.delete(o.value);
            }}
            type="button"
            role="tab"
            id={tabId(id, o.value)}
            aria-selected={active}
            aria-controls={panelId(id, o.value)}
            tabIndex={active ? 0 : -1}
            className={tabItem(active)}
            onClick={() => onChange(o.value)}
            onKeyDown={(e) => onKeyDown(e, i)}
          >
            {o.step !== undefined ? (
              <span
                aria-hidden
                className="flex size-5 items-center justify-center rounded-full bg-secondary text-2xs font-semibold text-secondary-foreground max-sm:hidden"
              >
                {o.step}
              </span>
            ) : null}
            {o.icon ? <span aria-hidden>{o.icon}</span> : null}
            {o.label}
          </button>
        );
      })}
    </div>
  );
}

/** Content of tab `value` in the [`Tabs`] strip with the same `id`. */
export function TabPanel({
  id,
  value,
  className,
  children,
}: {
  id: string;
  value: string;
  className?: string;
  children: ReactNode;
}) {
  return (
    <div
      role="tabpanel"
      id={panelId(id, value)}
      aria-labelledby={tabId(id, value)}
      className={className}
    >
      {children}
    </div>
  );
}
