// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { cn } from "@/lib/utils";

/** Placeholder block while loading; `aria-hidden` — the parent region announces `aria-busy`. */
export function Skeleton({ className }: { className?: string }) {
  return (
    <div
      aria-hidden
      className={cn("rounded-md bg-skeleton motion-safe:animate-pulse", className)}
    />
  );
}
