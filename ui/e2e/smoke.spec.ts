// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { clean, expect, test } from "./fixtures";

// Smoke test: the binary serves the real build under /ui/, deep links return to the
// app, the theme applies before the first paint, the strict CSP blocks nothing of the app itself.
test("console loads under /ui with a strict CSP", async ({ page, watch }) => {
  const res = await page.goto("/ui/");
  expect(res?.status()).toBe(200);
  expect(res?.headers()["content-security-policy"]).toContain("script-src 'self'");
  await expect(page.getByRole("heading", { name: "Overview", level: 1 })).toBeVisible();
  const base = await page.locator('meta[name="cairn-base"]').getAttribute("content");
  expect(base).toBe("/ui");
  await clean(page, watch);
});

test("GET / redirects to the console, deep links are still the app", async ({ page, watch }) => {
  await page.goto("/");
  await expect(page).toHaveURL(/\/ui\/$/);
  // Wait for the Overview to finish loading: leaving the page while a `fetch` is in flight makes WebKit print
  // the console error "access control checks" for the cancelled request.
  await expect(page.getByRole("heading", { name: "Overview", level: 1 })).toBeVisible();
  await expect(page.locator("main [aria-busy]")).toHaveCount(0);
  await page.waitForLoadState("networkidle");
  const deep = await page.goto("/ui/invocations/abc");
  expect(deep?.status()).toBe(200);
  await expect(page.locator("#root")).toBeAttached();
  await clean(page, watch);
});

// Remote Firefox (container) does not pass the context's `colorScheme` to the console page
// (`matchMedia` stays light — probed); WebKit and Chromium do.
const NO_SCHEME = "remote Firefox does not emulate prefers-color-scheme for the page";

test("dark theme follows the system, no flash", async ({ browser, browserName }) => {
  test.skip(browserName === "firefox", NO_SCHEME);
  const ctx = await browser.newContext({ colorScheme: "dark" });
  const page = await ctx.newPage();
  await page.goto("/ui/");
  await expect(page.locator("html")).toHaveClass(/dark/);
  await ctx.close();
});

test("light theme follows the system", async ({ browser, browserName }) => {
  test.skip(browserName === "firefox", NO_SCHEME);
  const ctx = await browser.newContext({ colorScheme: "light" });
  const page = await ctx.newPage();
  await page.goto("/ui/");
  await expect(page.getByRole("heading", { name: "Overview", level: 1 })).toBeVisible();
  await expect(page.locator("html")).not.toHaveClass(/dark/);
  await ctx.close();
});
