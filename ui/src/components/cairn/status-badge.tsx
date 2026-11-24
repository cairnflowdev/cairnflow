// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Status badge: **always icon + label**, never colour alone. The value is exactly
// the string the server returns — the UI never derives a status itself.
import {
  Check,
  CircleDashed,
  Clock,
  ListOrdered,
  type LucideIcon,
  Pause,
  RotateCw,
  X,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { m } from "@/paraglide/messages";

export type Status =
  | "running"
  | "queued"
  | "suspended"
  | "paused"
  | "completed"
  | "failed"
  | "scheduled"
  | "retrying";

const LOOK: Record<Status, { icon: LucideIcon | "dot"; cls: string; label: () => string }> = {
  running: {
    icon: "dot",
    cls: "bg-status-running text-status-running-foreground",
    label: m.status_running,
  },
  // Not a server status: a `running` call whose queue entry is still `inbox` — it has
  // not run a single turn yet, it is waiting for the lock or a slot.
  queued: {
    icon: ListOrdered,
    cls: "bg-status-suspended text-status-suspended-foreground",
    label: m.status_queued,
  },
  suspended: {
    icon: CircleDashed,
    cls: "bg-status-suspended text-status-suspended-foreground",
    label: m.status_suspended,
  },
  paused: {
    icon: Pause,
    cls: "bg-status-paused text-status-paused-foreground",
    label: m.status_paused,
  },
  completed: {
    icon: Check,
    cls: "bg-status-completed text-status-completed-foreground",
    label: m.status_completed,
  },
  failed: {
    icon: X,
    cls: "bg-status-failed text-status-failed-foreground",
    label: m.status_failed,
  },
  scheduled: {
    icon: Clock,
    cls: "bg-status-scheduled text-status-scheduled-foreground",
    label: m.status_scheduled,
  },
  retrying: {
    icon: RotateCw,
    cls: "bg-status-retrying text-status-retrying-foreground",
    label: m.status_retrying,
  },
};

export function isStatus(s: string): s is Status {
  return s in LOOK;
}

/** Unknown `status` ⇒ the label is that string itself, neutral style — no guessing. */
export function StatusBadge({ status, attempts }: { status: string; attempts?: number }) {
  const look = isStatus(status) ? LOOK[status] : null;
  const Icon = look?.icon;
  return (
    <span
      className={cn(
        "inline-flex h-5 items-center gap-1 rounded-sm px-1.5 text-xs font-medium whitespace-nowrap",
        "forced-colors:border forced-colors:border-[CanvasText]",
        look?.cls ?? "bg-muted text-muted-foreground",
      )}
      data-status={status}
    >
      {Icon === "dot" ? (
        <span aria-hidden className="size-2 rounded-full bg-current motion-safe:animate-pulse" />
      ) : Icon ? (
        <Icon aria-hidden className="size-3" />
      ) : null}
      <span>{look ? look.label() : status}</span>
      {status === "retrying" && attempts !== undefined ? (
        <span className="tabular">×{attempts}</span>
      ) : null}
    </span>
  );
}
