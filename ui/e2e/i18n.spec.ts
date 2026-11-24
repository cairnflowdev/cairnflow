// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { axe, clean, expect, test } from "./fixtures";
import { seed } from "./seed";

// Vietnamese: picked from the user menu (stored in localStorage), the page
// changes `lang`, the phase-1 screens show the translation and are axe clean.
test("switch to Vietnamese", async ({ page, watch }) => {
  const s = seed();
  await page.goto("/ui/");
  await expect(page.getByRole("heading", { name: "Overview", level: 1 })).toBeVisible();
  await page.getByRole("button", { name: "Account" }).click();
  await page.getByRole("menuitemradio", { name: "Tiếng Việt" }).click();
  await expect(page.getByRole("heading", { name: "Tổng quan", level: 1 })).toBeVisible();
  await expect(page.locator("html")).toHaveAttribute("lang", "vi");
  await axe(page);

  await page.goto(`/ui/invocations/${s.failed}`);
  await expect(page.getByRole("region", { name: "Vì sao ở trạng thái này" })).toContainText(
    "card declined",
  );
  await axe(page);
  await page.goto("/ui/invocations?view=paused");
  await expect(page.getByRole("link", { name: "Đang thử lại" })).toBeVisible();
  await axe(page);
  await clean(page, watch);
});
