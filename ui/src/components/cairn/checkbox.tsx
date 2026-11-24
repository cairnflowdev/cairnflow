// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Checkbox on top of Base UI's checkbox: same border, checked colour and focus ring as
// inputs; `indeterminate` for the "select the whole page" box when only some are
// selected. `type="checkbox"` outside this file is a lint error (`lint.test.ts`). The
// empty box's dark background (`dark:`) must not override the checked background:
// without `dark:data-checked:` a checked box in dark mode is left with only a teal border.
import { Checkbox as Base } from "@base-ui/react/checkbox";
import { Check, Minus } from "lucide-react";
import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

/**
 * With `children` ⇒ box and text sit in one `<label>` (clicking the text toggles the
 * box too). Without ⇒ an `aria-label` is required (row checkboxes in tables).
 */
export function Checkbox({
  checked,
  onCheckedChange,
  indeterminate = false,
  disabled,
  className,
  children,
  ...aria
}: {
  checked: boolean;
  onCheckedChange: (checked: boolean) => void;
  indeterminate?: boolean;
  disabled?: boolean;
  className?: string;
  children?: ReactNode;
  "aria-label"?: string;
}) {
  const box = (
    <Base.Root
      checked={checked}
      indeterminate={indeterminate}
      disabled={disabled}
      onCheckedChange={(c) => onCheckedChange(c)}
      className="flex size-4 shrink-0 items-center justify-center rounded-[4px] border border-input bg-background text-primary-foreground outline-none focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ring data-checked:border-primary data-checked:bg-primary data-disabled:cursor-not-allowed data-disabled:border-border data-disabled:bg-muted data-indeterminate:border-primary data-indeterminate:bg-primary dark:bg-input/30 dark:data-checked:bg-primary dark:data-indeterminate:bg-primary data-disabled:data-checked:border-muted-foreground data-disabled:data-checked:bg-muted-foreground data-disabled:data-indeterminate:border-muted-foreground data-disabled:data-indeterminate:bg-muted-foreground dark:data-disabled:bg-muted dark:data-disabled:data-checked:bg-muted-foreground dark:data-disabled:data-indeterminate:bg-muted-foreground"
      {...aria}
    >
      <Base.Indicator keepMounted={false} className="flex">
        {indeterminate ? (
          <Minus aria-hidden className="size-3" strokeWidth={3} />
        ) : (
          <Check aria-hidden className="size-3" strokeWidth={3} />
        )}
      </Base.Indicator>
    </Base.Root>
  );
  if (!children) return <span className={cn("inline-flex", className)}>{box}</span>;
  return (
    // biome-ignore lint/a11y/noLabelWithoutControl: the control is `Base.Root` (role checkbox) right inside the label
    <label
      className={cn(
        "inline-flex items-center gap-1.5 text-sm whitespace-nowrap has-data-disabled:text-muted-foreground",
        className,
      )}
    >
      {box}
      {children}
    </label>
  );
}
