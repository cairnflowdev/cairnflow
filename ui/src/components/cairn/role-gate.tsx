// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Permissions in the UI: items that can never be used are hidden; actions needing a
// capability the key lacks are disabled with a reason. `have` is the capability set
// the server sends in `whoami` — the server still decides.
import type { ReactNode } from "react";
import { EmptyState } from "@/components/cairn/empty-state";
import { type Capability, can } from "@/lib/roles";
import { m } from "@/paraglide/messages";

export function RoleGate({
  have,
  need,
  mode = "disable",
  children,
}: {
  /** Capability set of the key in use (`whoami.capabilities`). */
  have: readonly Capability[] | undefined;
  need: Capability;
  /** `hide`: render nothing; `disable`: call `children` with the reason for disabling. */
  mode?: "hide" | "disable";
  children: (blocked: string | null) => ReactNode;
}) {
  if (can(have, need)) return <>{children(null)}</>;
  if (mode === "hide") return null;
  return <>{children(m.cap_needs({ cap: need }))}</>;
}

/** A whole screen needs `need` (reached directly by URL when the sidebar hid the item): lacking it ⇒ state
 *  the reason instead of a page full of `403` errors. */
export function RequireCapability({
  have,
  need,
  children,
}: {
  have: readonly Capability[] | undefined;
  need: Capability;
  children: ReactNode;
}) {
  if (can(have, need)) return <>{children}</>;
  return <EmptyState title={m.cap_needs({ cap: need })} />;
}
