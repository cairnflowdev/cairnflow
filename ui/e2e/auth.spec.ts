// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { axe, clean, expect, test } from "./fixtures";
import { E2E_ADMIN_KEY } from "./ports";

// Sign-in on a node with auth: no session ⇒ the sign-in page (remembering
// where you were going); wrong key ⇒ error; right key ⇒ session cookie, `whoami` builds the shell;
// a new tab is still signed in (cookie, not sessionStorage); sign-out ⇒ back to the
// sign-in page.
test("sign-in, new tab, sign-out", async ({ page, context, watch }) => {
  await page.goto("/ui/deployments");
  await expect(page).toHaveURL(/\/ui\/login\?redirect=%2Fdeployments$/);
  await axe(page);

  await page.getByLabel("API key").fill("cf_not_a_real_key");
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByRole("alert")).toHaveText("That key was not accepted.");

  await page.getByLabel("API key").fill(E2E_ADMIN_KEY);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page).toHaveURL(/\/ui\/deployments$/);
  await expect(page.getByText("All tenants")).toBeVisible();
  // The auth node's `--ui-env-label prod-e2e`: a badge in the top bar.
  await expect(page.getByTitle("Environment: prod-e2e")).toBeVisible();
  // The auth node is empty ⇒ the Overview is onboarding.
  await page.goto("/ui/");
  await expect(page.getByRole("heading", { name: "Register your first deployment" })).toBeVisible();
  await axe(page);
  await expect(page.getByText("Auth is off")).toHaveCount(0);

  const cookies = await context.cookies();
  const session = cookies.find((c) => c.name === "cairn_ui");
  expect(session?.httpOnly).toBe(true);
  expect(session?.sameSite).toBe("Strict");
  expect(await page.evaluate(() => document.cookie)).not.toContain("cairn_ui");

  const tab = await context.newPage();
  await tab.goto("/ui/invocations");
  await expect(tab.getByRole("heading", { name: "Invocations", level: 1 })).toBeVisible();
  await tab.close();

  await page.getByRole("button", { name: "Account" }).click();
  await page.getByRole("menuitem", { name: "Log out" }).click();
  await expect(page).toHaveURL(/\/ui\/login/);
  await page.goto("/ui/");
  await expect(page).toHaveURL(/\/ui\/login/);
  // 401: `whoami` before there is a session and the wrong key at `<base>/session` — on purpose.
  await clean(page, watch, [/status of 401/]);
});

// Empty node, no filter at all: the Invocations list says "none yet" and shows how to
// send the first call, it does not say "remove a filter" when there is no filter.
test("empty node: Invocations says none yet, no 'remove a filter'", async ({ page, watch }) => {
  await page.goto("/ui/login");
  await page.getByLabel("API key").fill(E2E_ADMIN_KEY);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByRole("heading", { level: 1 })).toBeVisible();
  // Let the post-sign-in page go quiet before moving on: Firefox cancels `goto` while the previous page still has queries in flight.
  await page.waitForLoadState("networkidle");
  await page.goto("/ui/invocations");
  await expect(page.getByRole("heading", { name: "No invocations yet" })).toBeVisible();
  await expect(page.getByText("remove a filter")).toHaveCount(0);
  await clean(page, watch);
});
