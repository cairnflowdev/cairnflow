// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Navigation items of the shell. Each item declares the capabilities it needs (a
// list = having any one of them is enough) — the sidebar hides items the key can
// never use — and a `g`+letter shortcut.
import {
  Activity,
  Archive,
  Boxes,
  CalendarClock,
  ChartLine,
  Database,
  Gauge,
  LayoutDashboard,
  type LucideIcon,
  Network,
  Rocket,
  ScrollText,
  Settings,
  Snail,
  SquareTerminal,
  Workflow,
} from "lucide-react";
import { type Capability, canAny } from "@/lib/roles";
import { m } from "@/paraglide/messages";

export type NavItem = {
  to:
    | "/"
    | "/invocations"
    | "/workflows"
    | "/objects"
    | "/schedules"
    | "/services"
    | "/deployments"
    | "/query"
    | "/usage"
    | "/slow-runs"
    | "/archive"
    | "/audit"
    | "/flow"
    | "/settings"
    | "/cluster";
  label: () => string;
  shortcut: () => string;
  need: Capability | readonly Capability[];
  icon: LucideIcon;
  keys: string;
  group?: () => string;
};

export const NAV: NavItem[] = [
  {
    to: "/",
    label: m.nav_overview,
    shortcut: m.shortcut_go_overview,
    need: "read",
    icon: LayoutDashboard,
    keys: "g o",
  },
  {
    to: "/invocations",
    label: m.nav_invocations,
    shortcut: m.shortcut_go_invocations,
    need: "read",
    icon: Activity,
    keys: "g i",
    group: m.nav_group_run,
  },
  {
    to: "/workflows",
    label: m.nav_workflows,
    shortcut: m.shortcut_go_workflows,
    need: "read",
    icon: Workflow,
    keys: "g w",
    group: m.nav_group_run,
  },
  {
    to: "/objects",
    label: m.nav_objects,
    shortcut: m.shortcut_go_objects,
    need: "read",
    icon: Database,
    keys: "g b",
    group: m.nav_group_run,
  },
  {
    to: "/schedules",
    label: m.nav_schedules,
    shortcut: m.shortcut_go_schedules,
    // Schedules are the app's business inside the tenant (an `invoke` key calls PUT at
    // startup); a `view` key only looks.
    need: ["read", "invoke"],
    icon: CalendarClock,
    keys: "g c",
    group: m.nav_group_run,
  },
  {
    to: "/services",
    label: m.nav_services,
    shortcut: m.shortcut_go_services,
    need: "read",
    icon: Boxes,
    keys: "g s",
    group: m.nav_group_define,
  },
  {
    to: "/deployments",
    label: m.nav_deployments,
    shortcut: m.shortcut_go_deployments,
    need: "read",
    icon: Rocket,
    keys: "g d",
    group: m.nav_group_define,
  },
  {
    to: "/query",
    label: m.nav_query,
    shortcut: m.shortcut_go_query,
    need: "read",
    icon: SquareTerminal,
    keys: "g q",
    group: m.nav_group_operate,
  },
  {
    to: "/usage",
    label: m.nav_usage,
    shortcut: m.shortcut_go_usage,
    need: "read",
    icon: ChartLine,
    keys: "g u",
    group: m.nav_group_operate,
  },
  {
    to: "/slow-runs",
    label: m.nav_slow_runs,
    shortcut: m.shortcut_go_slow_runs,
    need: "read",
    icon: Snail,
    keys: "g l",
    group: m.nav_group_operate,
  },
  {
    to: "/archive",
    label: m.nav_archive,
    shortcut: m.shortcut_go_archive,
    need: "read",
    icon: Archive,
    keys: "g a",
    group: m.nav_group_operate,
  },
  {
    to: "/audit",
    label: m.nav_audit,
    shortcut: m.shortcut_go_audit,
    // Audit of the key's tenant: every key can read it; another `?tenant=` only for
    // `provision`/`cluster` — the screen guards itself.
    need: "read",
    icon: ScrollText,
    keys: "g t",
    group: m.nav_group_operate,
  },
  {
    to: "/flow",
    label: m.nav_flow,
    shortcut: m.shortcut_go_flow,
    // The rule book is shared by the whole cluster: `/rules`, `/limits`, `/vqueues` need the `cluster` capability.
    need: "cluster",
    icon: Gauge,
    keys: "g f",
    group: m.nav_group_operate,
  },
  {
    to: "/settings",
    label: m.nav_settings,
    shortcut: m.shortcut_go_settings,
    // UI thresholds are readable by every key; keys, tenants, capture, pricing are guarded
    // by the screen itself on `provision`/`cluster`.
    need: "read",
    icon: Settings,
    keys: "g e",
    group: m.nav_group_operate,
  },
  {
    to: "/cluster",
    label: m.nav_cluster,
    shortcut: m.shortcut_go_cluster,
    need: "cluster",
    icon: Network,
    keys: "g k",
    group: m.nav_group_operate,
  },
];

/** Sidebar items usable by a key with capability set `have`. */
export function navFor(have: readonly Capability[] | undefined): NavItem[] {
  return NAV.filter((n) => canAny(have, n.need));
}
