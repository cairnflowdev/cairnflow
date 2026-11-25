// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Data table: every table of the console goes through here — one header style, one row
// rhythm, numeric columns right-aligned. A wide table scrolls horizontally inside its frame
// instead of widening the page. `<table` outside this file is a lint error (`lint.test.ts`).
//
// Narrow screens (below 640 px): each row becomes a two-line card according to the `priority`
// of each cell — line 1 holds the `primary` cells (checkbox, name) and the `status` cell aligned
// right, line 2 holds the `secondary` cells (short id, secondary numbers) in small text; `hidden`
// cells only show on wide screens (the details are on their own page). Free-form data tables
// (SQL results) keep horizontal scrolling: `narrow="scroll"`.
import {
  type ComponentProps,
  createContext,
  useContext,
  useLayoutEffect,
  useRef,
  useState,
} from "react";
import { cn } from "@/lib/utils";
import { m } from "@/paraglide/messages";

type Shape = { dense: boolean; sticky: boolean; cards: boolean };
const ShapeContext = createContext<Shape>({ dense: false, sticky: false, cards: true });

/** Where a column goes when the table becomes cards on a narrow screen (see top of file). */
export type Priority = "primary" | "status" | "secondary" | "hidden";

// Lines 1 and 2 of the card: the row's `::before` is a 100%-wide flex item with `order` 1 — it
// forces a line break between the `order` 0 cells (primary, status) and the `order` 2 cells (secondary).
const CARD_ROW =
  "max-sm:flex max-sm:flex-wrap max-sm:items-center max-sm:gap-x-2 max-sm:gap-y-0.5 max-sm:py-2 max-sm:before:order-1 max-sm:before:basis-full max-sm:before:content-['']";
const CARD_CELL: Record<Priority, string> = {
  // `basis-0 grow`: a long name shrinks and truncates with "…" instead of pushing the status cell
  // to the next line; a cell holding only a checkbox keeps exactly its own width.
  primary:
    "max-sm:order-none max-sm:min-w-0 max-sm:grow max-sm:basis-0 max-sm:truncate max-sm:has-[[role=checkbox]]:grow-0 max-sm:has-[[role=checkbox]]:basis-auto",
  status: "max-sm:order-none max-sm:ml-auto max-sm:max-w-[50%] max-sm:shrink-0 max-sm:truncate",
  secondary: "max-sm:order-2 max-sm:text-xs max-sm:text-muted-foreground max-sm:empty:hidden",
  hidden: "max-sm:hidden",
};

/**
 * - `dense`: shorter rows (secondary tables inside a card, long lists).
 * - `stickyHeader`: bordered frame, height capped at 60vh, header sticky on a `muted`
 *   background — for long result tables (the Query screen).
 * - `fixed`: `table-fixed` — columns follow `<colgroup>`, so stacked tables line up.
 * - `frameClassName`: class for the scroll frame (e.g. `min-w` goes on the table, `mt-2` on the frame).
 * - `label`: name of the frame when it scrolls (default "Table, scrolls").
 *
 * The frame only takes focus when the content really overflows (phones, wide tables): keyboard
 * users can scroll it with the arrows (axe `scrollable-region-focusable`), while a table that fits
 * adds no extra Tab stop.
 */
export function Table({
  dense = false,
  stickyHeader = false,
  fixed = false,
  className,
  frameClassName,
  label,
  narrow = "cards",
  ...props
}: ComponentProps<"table"> & {
  dense?: boolean;
  stickyHeader?: boolean;
  fixed?: boolean;
  frameClassName?: string;
  label?: string;
  /** Narrow screens: `cards` (default) — one card per row by `priority`; `scroll` — horizontal scroll. */
  narrow?: "cards" | "scroll";
}) {
  const cards = narrow === "cards";
  const frame = useRef<HTMLDivElement>(null);
  const [scrolls, setScrolls] = useState(false);
  useLayoutEffect(() => {
    const el = frame.current;
    if (!el) return;
    const check = () =>
      setScrolls(el.scrollWidth > el.clientWidth + 1 || el.scrollHeight > el.clientHeight + 1);
    check();
    if (typeof ResizeObserver === "undefined") return;
    const ro = new ResizeObserver(check);
    ro.observe(el);
    if (el.firstElementChild) ro.observe(el.firstElementChild);
    return () => ro.disconnect();
  }, []);
  return (
    <ShapeContext.Provider value={{ dense, sticky: stickyHeader, cards }}>
      <div
        ref={frame}
        {...(scrolls
          ? { tabIndex: 0, role: "region", "aria-label": label ?? m.table_scrolls() }
          : {})}
        className={cn(
          "relative overflow-x-auto focus-visible:outline-2 focus-visible:outline-ring",
          stickyHeader && "max-h-[60vh] overflow-auto rounded-md border border-border",
          frameClassName,
        )}
      >
        <table
          className={cn(
            "w-full text-left text-sm",
            fixed && "table-fixed",
            cards && "max-sm:block max-sm:min-w-0",
            className,
          )}
          {...props}
        />
      </div>
    </ShapeContext.Provider>
  );
}

/** Header row: children are [`TH`]s (one row — console tables have no multi-level headers). */
export function THead({ className, children, ...props }: ComponentProps<"thead">) {
  const { sticky, cards } = useContext(ShapeContext);
  return (
    <thead
      className={cn(
        "text-xs text-muted-foreground",
        sticky && "sticky top-0 z-[1] bg-muted",
        // Cards have no visible header row; screen readers still read the column names.
        cards && "max-sm:sr-only",
        className,
      )}
      {...props}
    >
      <tr>{children}</tr>
    </thead>
  );
}

export function TBody({ className, ...props }: ComponentProps<"tbody">) {
  const { cards } = useContext(ShapeContext);
  return <tbody className={cn(cards && "max-sm:block", className)} {...props} />;
}

/** Data row: a thin top border separates rows. */
export function TRow({ className, ...props }: ComponentProps<"tr">) {
  const { cards } = useContext(ShapeContext);
  return <tr className={cn("border-t border-border", cards && CARD_ROW, className)} {...props} />;
}

const pad = (s: Shape, head: boolean) =>
  cn(
    s.sticky ? "px-2" : "pr-3 last:pr-0",
    head ? (s.dense ? "py-0.5" : "py-1") : s.dense ? "py-1" : "py-1.5",
  );

/** Header cell. `numeric`: numeric column — right-aligned, matching [`TD`] `numeric`. */
export function TH({
  numeric = false,
  className,
  ...props
}: ComponentProps<"th"> & { numeric?: boolean }) {
  const shape = useContext(ShapeContext);
  return (
    <th
      className={cn(
        "font-medium whitespace-nowrap",
        pad(shape, true),
        numeric && "text-right",
        className,
      )}
      {...props}
    />
  );
}

/**
 * Data cell. `numeric`: right-aligned, tabular digits. `priority`: where the column goes when
 * the table becomes cards on a narrow screen (default `secondary`; every cell of a column has
 * the same value). The wide-screen truncation class (`w-full max-w-0`) is removed in cards.
 */
export function TD({
  numeric = false,
  priority = "secondary",
  className,
  ...props
}: ComponentProps<"td"> & { numeric?: boolean; priority?: Priority }) {
  const shape = useContext(ShapeContext);
  return (
    <td
      className={cn(
        pad(shape, false),
        numeric && "text-right tabular",
        className,
        shape.cards && "max-sm:block max-sm:w-auto max-sm:max-w-none max-sm:p-0 max-sm:text-left",
        shape.cards && CARD_CELL[priority],
      )}
      {...props}
    />
  );
}
