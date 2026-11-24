// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Id truncated in the middle, mono, copyable. The full id is always in `title` and
// in a region read by screen readers.
import type { ReactNode } from "react";
import { cn } from "@/lib/utils";
import { m } from "@/paraglide/messages";
import { CopyButton } from "./copy-button";

/**
 * `e068…a1f5ed`: keeps the first `head` characters and the last `tail`. The
 * distinguishing part is the **tail**: the first 16 hex of an invocation id are the
 * partition key, shared by every invocation of one key or one call tree; deployment
 * ids share a time prefix. The head is kept so two ids with the same key are visible
 * as such.
 */
export function shorten(id: string, head = 4, tail = 6): string {
  return id.length <= head + tail + 1 ? id : `${id.slice(0, head)}…${id.slice(-tail)}`;
}

/** Type-to-confirm string of an id: the last 8 characters — the id's distinct part (see `shorten`). */
export const confirmText = (id: string) => id.slice(-8);

/**
 * `wrap`: wraps **only the text** (usually a `Link`) — the copy button sits beside it,
 * not nested in the link (a button inside `<a>` is invalid HTML and the link's hit
 * area covers the button).
 */
export function Id({
  value,
  full = false,
  className,
  wrap,
}: {
  value: string;
  full?: boolean;
  className?: string;
  wrap?: (text: ReactNode) => ReactNode;
}) {
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 font-mono whitespace-nowrap tabular",
        className,
      )}
    >
      {(wrap ?? ((t: ReactNode) => t))(
        <span title={value}>
          <span aria-hidden="true">{full ? value : shorten(value)}</span>
          <span className="sr-only">{value}</span>
        </span>,
      )}
      <CopyButton value={value} label={m.id_what()} size="xs" />
    </span>
  );
}
