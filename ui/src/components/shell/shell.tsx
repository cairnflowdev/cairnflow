// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// The console shell: an inverted L — top bar, sidebar, content.
import { useQuery } from "@tanstack/react-query";
import { Link, Outlet, useNavigate, useRouterState } from "@tanstack/react-router";
import { Menu, PanelLeftClose, PanelLeftOpen, Search, X } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { Tag } from "@/components/cairn/tag";
import { Button } from "@/components/ui/button";
import { ok, rest } from "@/data/api";
import type { WhoAmI } from "@/data/session";
import { can } from "@/lib/roles";
import { install, pageShortcuts, type Shortcut } from "@/lib/shortcuts";
import { cn } from "@/lib/utils";
import { m } from "@/paraglide/messages";
import { CommandPalette } from "./command-palette";
import { Keys } from "./kbd";
import { type NavItem, navFor } from "./nav";
import { ShortcutsDialog } from "./shortcuts-dialog";
import { UserMenu } from "./user-menu";

const COLLAPSED = "cairn-sidebar-collapsed";

function storedCollapsed(): boolean {
  try {
    return localStorage.getItem(COLLAPSED) === "1";
  } catch {
    return false;
  }
}

export function Shell({ me, now = Date.now() }: { me: WhoAmI; now?: number }) {
  const navigate = useNavigate();
  const [palette, setPalette] = useState(false);
  const [help, setHelp] = useState(false);
  const [collapsed, setCollapsed] = useState(storedCollapsed);
  // Narrow screens (< md): the sidebar is a drawer over the content, opened by the menu
  // button — kept fixed, its 208 px would squeeze the content to ~180 px and every table overflows.
  const [drawer, setDrawer] = useState(false);
  const path = useRouterState({ select: (s) => s.location.pathname });
  // biome-ignore lint/correctness/useExhaustiveDependencies: close the drawer on every page change
  useEffect(() => setDrawer(false), [path]);
  useEffect(() => {
    if (!drawer) return;
    const close = (e: KeyboardEvent) => {
      if (e.key === "Escape") setDrawer(false);
    };
    document.addEventListener("keydown", close);
    return () => document.removeEventListener("keydown", close);
  }, [drawer]);
  const items = useMemo(() => navFor(me.capabilities), [me.capabilities]);
  const shortcuts: Shortcut[] = useMemo(
    () => [
      { keys: "mod+k", label: m.shortcut_palette, run: () => setPalette(true) },
      { keys: "/", label: m.shortcut_search, run: () => setPalette(true) },
      { keys: "?", label: m.shortcut_help, run: () => setHelp(true) },
      ...items.map((n) => ({ keys: n.keys, label: n.shortcut, run: () => navigate({ to: n.to }) })),
    ],
    [items, navigate],
  );
  useEffect(() => install(shortcuts), [shortcuts]);
  useEffect(() => {
    try {
      localStorage.setItem(COLLAPSED, collapsed ? "1" : "0");
    } catch {}
  }, [collapsed]);
  const expiresIn =
    me.sessionExpiresAt === null ? null : Math.floor((me.sessionExpiresAt - now) / 60_000);

  return (
    <div className="flex h-dvh flex-col">
      <a
        href="#main"
        className="sr-only z-50 rounded-md bg-background px-3 py-2 focus:not-sr-only focus:fixed focus:top-2 focus:left-2"
      >
        {m.shell_skip()}
      </a>
      {me.noAuth ? (
        <p className="border-b border-dashed border-input px-4 py-1 text-center text-xs">
          {m.shell_auth_off()}
        </p>
      ) : null}
      <header className="flex h-12 shrink-0 items-center gap-3 border-b border-border px-3">
        <Button
          variant="ghost"
          size="icon"
          className="md:hidden"
          aria-label={drawer ? m.shell_menu_close() : m.shell_menu_open()}
          aria-expanded={drawer}
          aria-controls="sidebar"
          onClick={() => setDrawer((d) => !d)}
        >
          {drawer ? <X aria-hidden /> : <Menu aria-hidden />}
        </Button>
        <Link to="/" className="font-semibold">
          {m.app_name()}
        </Link>
        <Tag className="min-w-0 shrink max-sm:hidden">
          <span className="truncate">
            {can(me.capabilities, "cluster")
              ? m.shell_all_tenants()
              : m.shell_tenant({ tenant: me.tenant })}
          </span>
        </Tag>
        {me.envLabel ? <EnvBadge label={me.envLabel} /> : null}
        <Button
          variant="outline"
          className="ml-auto w-64 justify-start text-muted-foreground max-md:w-auto"
          onClick={() => setPalette(true)}
        >
          <Search aria-hidden />
          <span className="max-md:sr-only">{m.shell_search()}</span>
          <span className="ml-auto max-md:hidden">
            <Keys keys="mod+k" />
          </span>
        </Button>
        <span className="text-xs text-tertiary-foreground max-lg:hidden">
          {m.shell_version({ version: me.version })}
          {me.clusterVersion !== null && me.clusterVersion !== undefined
            ? ` · ${m.shell_cluster_version({ n: String(me.clusterVersion) })}`
            : null}
        </span>
        {can(me.capabilities, "cluster") ? <ClusterHealth /> : null}
        <UserMenu me={me} />
      </header>
      <div className="relative flex min-h-0 flex-1">
        {drawer ? (
          <button
            type="button"
            tabIndex={-1}
            aria-hidden
            className="absolute inset-0 z-30 bg-foreground/20 md:hidden"
            onClick={() => setDrawer(false)}
          />
        ) : null}
        <nav
          id="sidebar"
          aria-label={m.app_name()}
          className={cn(
            "flex shrink-0 flex-col gap-0.5 overflow-y-auto border-r border-sidebar-border bg-sidebar p-2 text-sidebar-foreground",
            "max-md:absolute max-md:inset-y-0 max-md:left-0 max-md:z-40 max-md:w-60 max-md:shadow-lg",
            !drawer && "max-md:hidden",
            collapsed ? "md:w-12" : "md:w-52",
          )}
        >
          {groups(items).map(([group, list], gi) => (
            <div key={group ?? gi} className="flex flex-col gap-0.5">
              {group ? (
                <p
                  className={cn(
                    "px-2 pt-3 pb-1 text-xs font-medium text-tertiary-foreground",
                    collapsed && "md:sr-only",
                  )}
                >
                  {group}
                </p>
              ) : null}
              {group && collapsed ? (
                <hr aria-hidden className="mx-2 my-1 border-sidebar-border max-md:hidden" />
              ) : null}
              {list.map((it) => (
                <Link
                  key={it.to}
                  to={it.to}
                  activeOptions={{ exact: it.to === "/" }}
                  className="flex h-8 shrink-0 items-center gap-2 rounded-md px-2 hover:bg-sidebar-accent data-[status=active]:bg-sidebar-accent data-[status=active]:font-medium data-[status=active]:text-sidebar-accent-foreground"
                  title={collapsed ? it.label() : undefined}
                >
                  <it.icon aria-hidden className="size-4 shrink-0" />
                  <span className={cn(collapsed && "md:sr-only")}>{it.label()}</span>
                </Link>
              ))}
            </div>
          ))}
          {items.length === 0 ? (
            <p className={cn("px-2 text-xs", collapsed && "md:sr-only")}>{m.shell_no_sections()}</p>
          ) : null}
          <Button
            variant="ghost"
            size="icon"
            className="mt-auto shrink-0 max-md:hidden"
            aria-label={collapsed ? m.shell_expand() : m.shell_collapse()}
            aria-pressed={collapsed}
            onClick={() => setCollapsed((c) => !c)}
          >
            {collapsed ? <PanelLeftOpen aria-hidden /> : <PanelLeftClose aria-hidden />}
          </Button>
        </nav>
        <main
          id="main"
          tabIndex={-1}
          className="relative min-w-0 flex-1 overflow-auto outline-none"
        >
          {expiresIn !== null && expiresIn < 10 ? (
            <p role="status" className="border-b border-input bg-muted px-4 py-2">
              {m.shell_expires({ minutes: String(Math.max(expiresIn, 0)) })}
            </p>
          ) : null}
          <Outlet />
        </main>
      </div>
      <CommandPalette open={palette} onOpenChange={setPalette} items={items} />
      <ShortcutsDialog
        open={help}
        onOpenChange={setHelp}
        shortcuts={help ? [...shortcuts, ...pageShortcuts()] : shortcuts}
      />
    </div>
  );
}

/** Groups adjacent items of the same group, keeping the order of `NAV`. */
function groups(items: NavItem[]): [string | undefined, NavItem[]][] {
  const out: [string | undefined, NavItem[]][] = [];
  for (const it of items) {
    const g = it.group?.();
    const last = out.at(-1);
    if (last && last[0] === g) last[1].push(it);
    else out.push([g, [it]]);
  }
  return out;
}

/**
 * Environment badge (`--ui-env-label`): whoever is on `prod` sees it before clicking
 * anything. Labels containing `prod` use the danger colour, other labels a strong
 * neutral — colour is only emphasis, the text is always there.
 */
export function EnvBadge({ label }: { label: string }) {
  const prod = /prod/i.test(label);
  return (
    <Tag tone={prod ? "danger" : "strong"} title={m.shell_env({ label })}>
      <span className="sr-only">{m.shell_env({ label })}</span>
      <span aria-hidden>{label}</span>
    </Tag>
  );
}

/**
 * Cluster health in the top bar — only cluster-admin can read `/cluster`: number of
 * nodes, and ranges with no leader are called out at once, with a link to the Cluster
 * screen.
 */
function ClusterHealth() {
  const nodes = useQuery({
    queryKey: ["cluster-nodes"],
    queryFn: () => ok(rest.GET("/cluster/nodes", { params: { query: { scope: "cluster" } } })),
    refetchInterval: HEALTH_POLL,
  });
  const view = useQuery({
    queryKey: ["cluster"],
    queryFn: () => ok(rest.GET("/cluster")),
    refetchInterval: HEALTH_POLL,
  });
  if (!nodes.data || !view.data) return null;
  const n = nodes.data.rows.length;
  const leaderless = view.data.ranges.filter((r) => !r.leader).length;
  return (
    <Link
      to="/cluster"
      className="text-xs whitespace-nowrap text-tertiary-foreground hover:underline max-lg:hidden"
    >
      {n === 1 ? m.shell_node_one() : m.shell_nodes({ n: String(n) })}
      {leaderless > 0 ? (
        <span className="font-medium text-destructive">
          {" · "}
          {m.shell_leaderless({ n: String(leaderless) })}
        </span>
      ) : null}
    </Link>
  );
}

const HEALTH_POLL = 30_000;
