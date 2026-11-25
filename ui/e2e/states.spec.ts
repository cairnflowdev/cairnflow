// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { clean, expect, test } from "./fixtures";

// Error and loading states (screenshot review of 2026-11-02).

// A missing deployment: `404` is the answer, not a transient error — no retry (one request),
// and the screen says clearly "not found" with a way back to the list, it does not sit on a skeleton.
test("missing deployment ⇒ one request, says not found, has a way back to the list", async ({
  page,
  watch,
}) => {
  const id = `dp_${"0".repeat(26)}`;
  let gets = 0;
  page.on("request", (r) => {
    if (new URL(r.url()).pathname === `/deployments/${id}`) gets++;
  });
  await page.goto(`/ui/deployments/${id}`);
  await expect(page.getByRole("heading", { name: "No such deployment" })).toBeVisible();
  await expect(page.getByRole("link", { name: "Back to deployments" })).toBeVisible();
  await page.waitForTimeout(1500);
  expect(gets).toBe(1);
  await clean(page, watch, [/status of 404/]);
});

// The Overview while numbers have not arrived: it must not assert "nothing needs attention".
test("the loading Overview does not say 'Nothing needs attention'", async ({ page }) => {
  await page.route("**/query", () => {});
  await page.goto("/ui/");
  await expect(page.getByRole("heading", { name: "Overview", level: 1 })).toBeVisible();
  await page.waitForTimeout(500);
  await expect(page.getByText("Nothing needs attention")).toHaveCount(0);
});
