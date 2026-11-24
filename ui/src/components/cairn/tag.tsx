// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Small tag: handler type, "key of this session", environment badge, counts next to
// buttons, active filters. Unlike `StatusBadge`, whose colours carry meaning: tags here
// are neutral, colour is only emphasis (`danger` for `prod`), the text is always there.
import { X } from "lucide-react";
import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

const TONE = {
  muted: "border-border text-muted-foreground",
  default: "border-border",
  strong: "border-foreground/40 font-semibold",
  danger: "border-destructive font-semibold text-destructive",
  /** No border, `muted` background: counts next to buttons. */
  filled: "border-transparent bg-muted text-muted-foreground tabular",
} as const;

const SIZE = {
  sm: "min-h-5 items-center gap-1 rounded-sm px-1.5 text-xs",
  /** As tall as an `sm` button: filter chips, each clickable part separated by a left border. */
  md: "h-7 items-stretch overflow-hidden rounded-lg text-sm",
} as const;

type Remove =
  | { onRemove?: undefined; removeLabel?: undefined }
  | { onRemove: () => void; removeLabel: string };

export function Tag({
  tone = "muted",
  size = "sm",
  mono = false,
  as: As = "span",
  title,
  className,
  children,
  onRemove,
  removeLabel,
}: {
  tone?: keyof typeof TONE;
  size?: keyof typeof SIZE;
  mono?: boolean;
  /** `li` when the tag is an item of a list (filter chips). */
  as?: "span" | "li";
  title?: string;
  className?: string;
  children: ReactNode;
} & Remove) {
  return (
    <As
      title={title}
      className={cn(
        "inline-flex max-w-full shrink-0 border whitespace-nowrap",
        TONE[tone],
        SIZE[size],
        mono && "font-mono",
        className,
      )}
    >
      {children}
      {onRemove ? (
        <button
          type="button"
          aria-label={removeLabel}
          className={cn(
            "flex shrink-0 items-center justify-center text-muted-foreground hover:bg-accent hover:text-foreground focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-ring",
            size === "md" ? "w-7 border-l border-border" : "-mr-1 size-4 rounded-sm",
          )}
          onClick={onRemove}
        >
          <X aria-hidden className={size === "md" ? "size-3.5" : "size-3"} />
        </button>
      ) : null}
    </As>
  );
}
