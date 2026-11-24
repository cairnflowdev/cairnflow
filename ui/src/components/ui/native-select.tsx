// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// The browser's native `<select>`, styled like `Input` (height, border, rounded corners,
// themed background) — the arrow is drawn with an icon instead of the OS arrow, which
// differs between Chromium/WebKit/Firefox and is off-colour in the dark theme. The
// native `<select>` is kept: keyboard, screen readers and mobile pickers work out of the box.
import { ChevronDown } from "lucide-react";
import type * as React from "react";
import { cn } from "@/lib/utils";

export function NativeSelect({
  className,
  wrapperClassName,
  ...props
}: React.ComponentProps<"select"> & { wrapperClassName?: string }) {
  return (
    <span className={cn("relative inline-flex", wrapperClassName)}>
      <select
        data-slot="native-select"
        className={cn(
          "h-8 w-full min-w-0 appearance-none rounded-lg border border-input bg-transparent py-1 pr-8 pl-2.5 text-sm outline-none transition-colors focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50 disabled:cursor-not-allowed disabled:bg-muted disabled:text-muted-foreground dark:bg-input/30 dark:disabled:bg-muted [&>option]:bg-popover [&>option]:text-popover-foreground",
          className,
        )}
        {...props}
      />
      <ChevronDown
        aria-hidden
        className="pointer-events-none absolute top-1/2 right-2.5 size-4 -translate-y-1/2 text-muted-foreground"
      />
    </span>
  );
}
