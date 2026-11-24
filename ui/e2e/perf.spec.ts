// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { expect, test } from "./fixtures";

// Performance budget: a 250-row list, INP ≤ 200 ms. Measured with Event
// Timing (`PerformanceObserver` of type `event`, Chromium): each interaction (click a
// row ⇒ quick-look panel, change view, type in the search box) takes the largest `duration` of
// that interactionId; INP is approximated by the largest value (under 50 interactions).
const INP_BUDGET_MS = 200;

test("INP of a 250-row list ≤ 200 ms", async ({ page }) => {
  await page.addInitScript(() => {
    const w = window as unknown as { __inp: Map<number, number> };
    w.__inp = new Map();
    new PerformanceObserver((list) => {
      for (const e of list.getEntries() as PerformanceEventTiming[]) {
        if (!e.interactionId) continue;
        w.__inp.set(e.interactionId, Math.max(w.__inp.get(e.interactionId) ?? 0, e.duration));
      }
    }).observe({ type: "event", durationThreshold: 16, buffered: true } as PerformanceObserverInit);
  });
  await page.goto("/ui/invocations");
  const rows = page.locator("tbody tr");
  await expect(rows.first()).toBeVisible();
  while ((await rows.count()) < 250) {
    const before = await rows.count();
    await page.getByRole("button", { name: "Load more" }).click();
    await expect.poll(() => rows.count()).toBeGreaterThan(before);
  }
  expect(await rows.count()).toBeGreaterThanOrEqual(250);

  // Interactions on a page that already has ≥ 250 rows.
  for (const i of [3, 120, 240]) {
    await rows.nth(i).locator("td").nth(2).click();
    await expect(page.getByRole("complementary", { name: "Invocation" })).toBeVisible();
  }
  await page.getByRole("link", { name: "Failed" }).first().click();
  await page.getByRole("link", { name: "All", exact: true }).click();
  await page.getByLabel("Invocation ID or prefix").pressSequentially("abc");
  await page.waitForTimeout(300);

  const worst = await page.evaluate(() =>
    Math.max(0, ...(window as unknown as { __inp: Map<number, number> }).__inp.values()),
  );
  console.log(`approximate INP: ${worst} ms`);
  expect(worst).toBeLessThanOrEqual(INP_BUDGET_MS);
});
