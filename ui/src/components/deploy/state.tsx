// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Status badge of a deployment: current / draining — N pins left / removable. The
// text is always there, colour is only emphasis.
import type { deploymentState } from "@/lib/manifest-diff";
import { cn } from "@/lib/utils";
import { m } from "@/paraglide/messages";

export function DeploymentState({ state }: { state: ReturnType<typeof deploymentState> }) {
  const text =
    state.kind === "current"
      ? m.deployment_state_current()
      : state.kind === "draining"
        ? m.deployment_state_draining({ n: String(state.pinned) })
        : state.kind === "removable"
          ? m.deployment_state_removable()
          : m.deployment_pinned_unknown();
  return (
    <span
      className={cn(
        "inline-flex h-5 items-center rounded-sm border px-1.5 text-xs whitespace-nowrap",
        state.kind === "current"
          ? "border-primary/40 font-medium"
          : state.kind === "removable"
            ? "border-status-paused-foreground/40 text-status-paused-foreground"
            : "border-border text-muted-foreground",
      )}
    >
      {text}
    </span>
  );
}
