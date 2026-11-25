// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { mkdirSync } from "node:fs";
import { join } from "node:path";
import type { BrowserContext, Page } from "@playwright/test";
import { setNextUser } from "./fixture/oidc";
import { axe, clean, expect, test } from "./fixtures";
import { E2E_ADMIN_KEY, E2E_PORTAL_URL, E2E_SSO_TENANT } from "./ports";

// Console SSO sign-in on the `sso` node: OIDC client
// `localhost` → tenant `acme`, the fake issuer `fixture/oidc.ts` (global setup) auto-
// consents for the next user the test sets. The browser leaves the console's origin exactly
// twice per sign-in (issuer: `/authorize`); every other request must stay same-
// origin.
//
// `CAIRN_E2E_SCREENS=<directory>` ⇒ capture each new screen in light and dark themes.

const issuer = () => {
  const v = process.env.CAIRN_E2E_ISSUER;
  if (!v) throw new Error("global setup has not set up the fake issuer");
  return v;
};

async function shots(page: Page, name: string) {
  const dir = process.env.CAIRN_E2E_SCREENS;
  if (!dir) return;
  mkdirSync(dir, { recursive: true });
  for (const scheme of ["light", "dark"] as const) {
    await page.emulateMedia({ colorScheme: scheme });
    // Switching theme runs a colour transition: capture after every finite animation is done.
    await page.waitForFunction(() =>
      document
        .getAnimations()
        .every(
          (a) =>
            a.playState !== "running" ||
            a.effect?.getTiming().iterations === Number.POSITIVE_INFINITY,
        ),
    );
    // Screenshots only: an open menu repaints in the new theme colours one tick after the
    // `prefers-color-scheme` change (the computed style is already right — not a colour bug).
    await page.waitForTimeout(400);
    await page.screenshot({ path: join(dir, `${name}-${scheme}.png`) });
  }
  await page.emulateMedia({ colorScheme: null });
}

/** Cross-origin requests may only go to the issuer (sign-in navigation). */
function onlyIssuer(foreign: string[]): string[] {
  return foreign.filter((u) => !u.startsWith(issuer()));
}

async function sessionCookie(context: BrowserContext) {
  return (await context.cookies()).find((c) => c.name === "__Host-cairn_ui");
}

test("SSO: sign-in button, IdP, __Host- session, user menu, limits, sign-out", async ({
  page,
  context,
  watch,
}) => {
  await setNextUser(issuer(), {
    sub: "user-1",
    name: "Ada Lovelace",
    email: "ada@example.com",
    role: "admin",
  });
  await page.goto("/ui/usage");
  await expect(page).toHaveURL(/\/ui\/login\?redirect=%2Fusage$/);
  const sso = page.getByRole("link", { name: "Sign in with Cairnflow Cloud" });
  await expect(sso).toBeVisible();
  // The API key always remains — a secondary button when SSO exists.
  await expect(page.getByLabel("API key")).toBeVisible();
  await expect(page.getByText("or use an API key")).toBeVisible();
  await axe(page);
  await shots(page, "login-sso");

  await sso.click();
  await expect(page).toHaveURL(/\/ui\/usage$/);
  const cookie = await sessionCookie(context);
  expect(cookie?.httpOnly).toBe(true);
  expect(cookie?.secure).toBe(true);
  expect(cookie?.sameSite).toBe("Strict");
  expect(cookie?.path).toBe("/");
  // The flow's temporary cookie has been deleted.
  expect((await context.cookies()).some((c) => c.name === "__Host-cairn_oidc")).toBe(false);
  expect(await page.evaluate(() => document.cookie)).not.toContain("cairn_ui");

  // The tenant comes from the client's configuration, not from a claim.
  await expect(page.getByText(`Tenant ${E2E_SSO_TENANT}`, { exact: true })).toBeVisible();
  await expect(
    page.getByRole("heading", { name: `Limits of tenant ${E2E_SSO_TENANT}` }),
  ).toBeVisible();
  await expect(page.getByText("No limit").first()).toBeVisible();
  await axe(page);
  await shots(page, "usage-limits");

  await page.getByRole("button", { name: "Account" }).click();
  const menu = page.getByRole("menu");
  await expect(menu.getByText("Ada Lovelace")).toBeVisible();
  await expect(menu.getByText("ada@example.com")).toBeVisible();
  await expect(menu.getByText("Role: admin")).toBeVisible();
  await expect(menu.getByText("Signed in with single sign-on")).toBeVisible();
  await expect(menu.getByRole("menuitem", { name: "Switch environment" })).toHaveAttribute(
    "href",
    E2E_PORTAL_URL,
  );
  await axe(page);
  await shots(page, "user-menu");
  await menu.getByRole("menuitem", { name: "Log out" }).click();
  await expect(page).toHaveURL(/\/ui\/login/);
  expect(await sessionCookie(context)).toBeUndefined();
  await page.goto("/ui/");
  await expect(page).toHaveURL(/\/ui\/login/);

  watch.foreign = onlyIssuer(watch.foreign);
  // 401: `whoami` before there is a session — on purpose.
  await clean(page, watch, [/status of 401/]);
});

test("SSO: the view role is read-only, write actions hidden; IdP refuses ⇒ reported on the sign-in page", async ({
  page,
  watch,
}) => {
  await setNextUser(issuer(), { sub: "user-2", name: "Grace Hopper", role: "view" });
  await page.goto("/ui/login?redirect=%2Fdeployments");
  await page.getByRole("link", { name: "Sign in with Cairnflow Cloud" }).click();
  await expect(page).toHaveURL(/\/ui\/deployments$/);
  // Role `view`: can read every tenant screen, no write button; cluster screens hidden.
  await expect(page.getByRole("button", { name: "Deploy safely…" })).toBeDisabled();
  await expect(page.getByRole("link", { name: "Deploy safely…" })).toHaveCount(0);
  const nav = page.getByRole("navigation", { name: "Cairnflow" });
  await expect(nav.getByRole("link", { name: "Deployments" })).toBeVisible();
  await expect(nav.getByRole("link", { name: "Flow control" })).toHaveCount(0);
  await expect(nav.getByRole("link", { name: "Cluster" })).toHaveCount(0);
  await page.getByRole("button", { name: "Account" }).click();
  await expect(page.getByRole("menu").getByText("Role: view")).toBeVisible();
  await page.keyboard.press("Escape");
  await axe(page);
  await shots(page, "view-role-deployments");

  await page.getByRole("button", { name: "Account" }).click();
  await page.getByRole("menuitem", { name: "Log out" }).click();
  await expect(page).toHaveURL(/\/ui\/login/);

  // A person with no access to the environment (billing role in the control plane).
  await setNextUser(issuer(), { sub: "user-3", deny: true });
  await page.getByRole("link", { name: "Sign in with Cairnflow Cloud" }).click();
  await expect(page).toHaveURL(/\/ui\/login\?sso_error=access_denied$/);
  await expect(page.getByRole("alert")).toHaveText(
    "Your account does not have access to this environment. Ask an owner of the organization to grant it.",
  );
  await axe(page);
  await shots(page, "login-sso-error");

  watch.foreign = onlyIssuer(watch.foreign);
  await clean(page, watch);
});

test("Audit: SSO sign-in in the tenant audit, reported when truncated; cluster key picks a tenant", async ({
  page,
  watch,
}) => {
  await setNextUser(issuer(), {
    sub: "user-4",
    name: "Ada Lovelace",
    email: "ada@example.com",
    role: "admin",
  });
  await page.goto("/ui/login?redirect=%2Faudit");
  await page.getByRole("link", { name: "Sign in with Cairnflow Cloud" }).click();
  await expect(page).toHaveURL(/\/ui\/audit$/);
  await expect(page.getByRole("heading", { name: "Audit log", level: 1 })).toBeVisible();
  await expect(
    page.getByRole("heading", { name: `Records of tenant ${E2E_SSO_TENANT}` }),
  ).toBeVisible();
  // The sign-in that just happened: an SSO user, outcome `ok`.
  const login = page
    .getByRole("row")
    .filter({ hasText: "session.login" })
    .filter({ hasText: "ada@example.com" })
    .first();
  await expect(login).toBeVisible();
  await expect(login.getByText("ok", { exact: true })).toBeVisible();
  await expect(login.getByText("via=sso")).toBeVisible();
  // A tenant's `admin` role cannot pick another tenant.
  await expect(page.locator("#audit-tenant")).toHaveCount(0);
  await login.getByRole("button", { name: "Details" }).click();
  await expect(page.getByText("Intent before, outcome after")).toBeVisible();
  await axe(page);
  await shots(page, "audit");

  await page.getByLabel("Action").fill("key.create");
  await page.getByRole("button", { name: "Apply filters" }).click();
  await expect(page).toHaveURL(/action=key\.create/);
  await expect(page.getByText("No audit records in this range")).toBeVisible();

  // The server truncates at 1000 objects (`truncated`): the page must say so.
  const audit = (u: URL) => u.pathname === "/audit";
  await page.route(audit, (r) =>
    r.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        tenant: E2E_SSO_TENANT,
        from: 0,
        to: 1,
        records: [],
        truncated: true,
      }),
    }),
  );
  await page.getByRole("button", { name: "7 days" }).click();
  await expect(
    page.getByRole("status").filter({ hasText: "Showing only part of this range" }),
  ).toBeVisible();
  await axe(page);
  await shots(page, "audit-truncated");
  await page.unroute(audit);

  await page.getByRole("button", { name: "Account" }).click();
  await page.getByRole("menuitem", { name: "Log out" }).click();
  await expect(page).toHaveURL(/\/ui\/login/);

  // A cluster key (bootstrap) can pick a tenant: it sees the sign-out that just happened too.
  await page.getByLabel("API key").fill(E2E_ADMIN_KEY);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page).not.toHaveURL(/\/ui\/login/);
  await page.goto(`/ui/audit?tenant=${E2E_SSO_TENANT}`);
  await expect(page.locator("#audit-tenant")).toHaveValue(E2E_SSO_TENANT);
  await expect(
    page.getByRole("heading", { name: `Records of tenant ${E2E_SSO_TENANT}` }),
  ).toBeVisible();
  await expect(
    page
      .getByRole("row")
      .filter({ hasText: "session.logout" })
      .filter({ hasText: "ada@example.com" })
      .first(),
  ).toBeVisible();
  await axe(page);

  watch.foreign = onlyIssuer(watch.foreign);
  await clean(page, watch);
});
