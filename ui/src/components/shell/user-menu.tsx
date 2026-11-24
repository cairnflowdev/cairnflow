// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// User menu: the person (SSO) or key, role, switch environment (portal link of the
// OIDC client), theme, language, time zone, sign out.
import { useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "@tanstack/react-router";
import { ArrowLeftRight, CircleUser } from "lucide-react";
import { setTimeZonePref, type TimeZonePref, useTimeZonePref } from "@/components/cairn/time";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuGroup,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { logout, type WhoAmI } from "@/data/session";
import { initials } from "@/lib/sso";
import { type Theme, useTheme } from "@/lib/theme";
import { m } from "@/paraglide/messages";
import { getLocale, locales, setLocale } from "@/paraglide/runtime";

const LANG: Record<string, string> = { en: "English", vi: "Tiếng Việt" };

export function UserMenu({ me }: { me: WhoAmI }) {
  const { theme, setTheme } = useTheme();
  const zone = useTimeZonePref();
  const qc = useQueryClient();
  const navigate = useNavigate();
  const user = me.user ?? null;
  return (
    <DropdownMenu>
      <DropdownMenuTrigger
        render={<Button variant="ghost" size="icon" aria-label={m.shell_user()} />}
      >
        {user ? (
          <span
            aria-hidden
            className="flex size-7 items-center justify-center rounded-full bg-secondary text-xs font-semibold text-secondary-foreground"
          >
            {initials(user.name, user.email) || <CircleUser />}
          </span>
        ) : (
          <CircleUser aria-hidden />
        )}
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="min-w-64">
        <DropdownMenuGroup>
          <DropdownMenuLabel className="flex flex-col gap-0.5">
            {user ? (
              <>
                <span className="truncate font-medium text-foreground">
                  {user.name ?? user.email ?? user.sub}
                </span>
                {user.email && user.name ? (
                  <span className="truncate text-xs text-muted-foreground">{user.email}</span>
                ) : null}
              </>
            ) : null}
            <span>{m.shell_role({ role: me.role })}</span>
            <span
              className={
                user ? "text-2xs text-muted-foreground" : "font-mono text-2xs text-muted-foreground"
              }
            >
              {user
                ? m.shell_via_sso()
                : me.keyId
                  ? m.shell_key({ key: me.keyId })
                  : me.bootstrap
                    ? m.shell_bootstrap()
                    : null}
            </span>
          </DropdownMenuLabel>
          {me.portalUrl ? (
            <DropdownMenuItem render={<a href={me.portalUrl} />}>
              <ArrowLeftRight aria-hidden />
              {m.shell_switch_env()}
            </DropdownMenuItem>
          ) : null}
        </DropdownMenuGroup>
        <DropdownMenuSeparator />
        <DropdownMenuGroup>
          <DropdownMenuLabel>{m.shell_theme()}</DropdownMenuLabel>
          <DropdownMenuRadioGroup value={theme} onValueChange={(v) => setTheme(v as Theme)}>
            <DropdownMenuRadioItem value="light">{m.shell_theme_light()}</DropdownMenuRadioItem>
            <DropdownMenuRadioItem value="dark">{m.shell_theme_dark()}</DropdownMenuRadioItem>
            <DropdownMenuRadioItem value="system">{m.shell_theme_system()}</DropdownMenuRadioItem>
          </DropdownMenuRadioGroup>
        </DropdownMenuGroup>
        <DropdownMenuSeparator />
        <DropdownMenuGroup>
          <DropdownMenuLabel>{m.shell_language()}</DropdownMenuLabel>
          <DropdownMenuRadioGroup
            value={getLocale()}
            onValueChange={(v) => setLocale(v as (typeof locales)[number])}
          >
            {locales.map((l) => (
              <DropdownMenuRadioItem key={l} value={l}>
                {LANG[l] ?? l}
              </DropdownMenuRadioItem>
            ))}
          </DropdownMenuRadioGroup>
        </DropdownMenuGroup>
        <DropdownMenuSeparator />
        <DropdownMenuGroup>
          <DropdownMenuLabel>{m.shell_time_zone()}</DropdownMenuLabel>
          <DropdownMenuRadioGroup
            value={zone}
            onValueChange={(v) => setTimeZonePref(v as TimeZonePref)}
          >
            <DropdownMenuRadioItem value="local">{m.shell_time_zone_local()}</DropdownMenuRadioItem>
            <DropdownMenuRadioItem value="utc">UTC</DropdownMenuRadioItem>
          </DropdownMenuRadioGroup>
        </DropdownMenuGroup>
        {me.noAuth ? null : (
          <>
            <DropdownMenuSeparator />
            <DropdownMenuItem
              onClick={async () => {
                await logout();
                qc.clear();
                await navigate({ to: "/login" });
              }}
            >
              {m.shell_logout()}
            </DropdownMenuItem>
          </>
        )}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
