// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Buttons on a table row: one style for every screen — small ghost, right-aligned, no
// wrapping. Delete is a red trash icon (full name in `aria-label`); other destructive
// actions (revoke) are red text. Outlined/solid red buttons on every row would weigh
// more than the row's own data.
import { Trash2 } from "lucide-react";
import type { ComponentProps, ReactNode } from "react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

/**
 * Class for the `<td>` holding `RowActions`: on medium screens (640–768 px) the table
 * scrolls horizontally ⇒ the actions cell sticks to the right edge (solid background,
 * faint left border) — no need to scroll the whole table to reach the buttons. Below
 * 640 px the table becomes cards: this cell declares `priority="status"`, sitting on
 * line 1 at the right.
 */
export const rowActionsCell =
  "py-1.5 sm:max-md:sticky sm:max-md:right-0 sm:max-md:bg-background sm:max-md:pl-2 sm:max-md:shadow-[-1px_0_0_var(--border)]";

export function RowActions({ children }: { children: ReactNode }) {
  return <div className="flex justify-end gap-0.5 whitespace-nowrap">{children}</div>;
}

/** A regular row action; `destructive` ⇒ red text (revoke, stop). */
export function RowButton({
  destructive,
  className,
  ...props
}: Omit<ComponentProps<typeof Button>, "variant" | "size"> & { destructive?: boolean }) {
  return (
    <Button
      variant="ghost"
      size="sm"
      className={cn(
        destructive && "text-destructive hover:bg-destructive/10 hover:text-destructive",
        className,
      )}
      {...props}
    />
  );
}

/** Delete a row: an icon, `label` is the full name ("Delete <name>") for screen readers. */
export function RowDelete({
  label,
  title,
  onClick,
}: {
  label: string;
  /** Short tooltip ("Delete"); defaults to `label`. */
  title?: string;
  onClick: () => void;
}) {
  return (
    <Button
      size="icon-sm"
      variant="ghost"
      className="text-destructive hover:bg-destructive/10 hover:text-destructive"
      aria-label={label}
      title={title ?? label}
      onClick={onClick}
    >
      <Trash2 aria-hidden />
    </Button>
  );
}
