// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Identity and session: `GET /auth/whoami`, signing in with an API key once ⇒ an
// `HttpOnly` cookie, signing out clears the cookie.
import { queryOptions, useSuspenseQuery } from "@tanstack/react-query";
import { basePath } from "@/lib/base";
import type { Capability } from "@/lib/roles";
import { api, ok, rest } from "./api";
import type { components } from "./api.gen";

export type WhoAmI = components["schemas"]["WhoAmI"];

export const whoamiQuery = queryOptions({
  queryKey: ["whoami"],
  queryFn: () => ok(rest.GET("/auth/whoami")),
  retry: false,
  staleTime: 60_000,
});

/** Capability set of the key in use — only for hiding/disabling, the server decides. */
export function useCapabilities(): readonly Capability[] {
  return useSuspenseQuery(whoamiQuery).data.capabilities;
}

/** `<base>/session` — the dev server (no base meta) uses `/ui`, like Vite's proxy. */
export function sessionPath(): string {
  return `${uiBase()}/session`;
}

/** Base path of the server-side sign-in routes (`<base>/session`, `<base>/login/*`). */
function uiBase(): string {
  return basePath() || "/ui";
}

/**
 * `GET <base>/login/options` (public): whether this hostname has SSO sign-in, the
 * IdP name on the button, the portal link. Reveals neither tenant nor issuer.
 */
export type LoginOptions = { sso: boolean; displayName: string | null; portalUrl: string | null };

export const loginOptionsQuery = queryOptions({
  queryKey: ["login-options"],
  queryFn: () => api<LoginOptions>(`${uiBase()}/login/options`),
  retry: false,
  staleTime: 60_000,
});

/**
 * Link that starts SSO sign-in: a full-page navigation (not `fetch`) — the server
 * redirects to the IdP and back to `return_to`. `redirect` is a router path
 * (`/deployments`); the server only accepts same-origin relative paths.
 */
export function ssoHref(redirect?: string): string {
  const back = `${uiBase()}${redirect && redirect !== "/" ? redirect : "/"}`;
  return `${uiBase()}/login/oidc?return_to=${encodeURIComponent(back)}`;
}

export function login(key: string) {
  // The `200` body carries `role`/`capabilities` like `whoami`; the shell re-reads `whoami`.
  return api<{ noAuth?: boolean; capabilities?: Capability[] }>(sessionPath(), {
    method: "POST",
    body: JSON.stringify({ key }),
  });
}

export function logout() {
  return api<void>(sessionPath(), { method: "DELETE" });
}
