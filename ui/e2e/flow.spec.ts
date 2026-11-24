// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { axe, clean, expect, test } from "./fixtures";

// Flow control on the `noauth` node (the key is cluster-admin). The rules
// use a scope nobody runs (`e2eflow`) — they do not throttle the other cases; the tenant's
// rate limit is removed at the end of the case.

test("rule: add (* warning), applied to every range, disable, merge-style edit, bad pattern ⇒ 400, delete by typing the pattern", async ({
  page,
  watch,
}) => {
  const pattern = `e2eflow${Date.now() % 100000}/*`;
  await page.goto("/ui/flow");
  await expect(page.getByRole("heading", { name: "Scheduler per range" })).toBeVisible();
  await page.getByRole("button", { name: "New rule", exact: true }).click();
  const dialog = page.getByRole("dialog");
  await dialog.getByLabel("Pattern").fill(pattern);
  await expect(dialog.getByRole("note")).toContainText("is not a shared pool");
  await dialog.getByLabel("Concurrency").fill("2");
  await dialog.getByLabel("Description").fill("e2e");
  await dialog.getByRole("button", { name: "Save" }).click();
  await expect(
    page.getByText(new RegExp(`Saved ${pattern.replace("*", "\\*")} \\(version \\d+\\)`)),
  ).toBeVisible();
  const row = page.getByRole("row", { name: new RegExp(pattern.replace("*", "\\*")) });
  await expect(row.getByRole("cell", { name: "2", exact: true })).toBeVisible();
  // The server's warning when the rule book has `*`; the rule propagates to every range in ~2 s.
  await expect(page.getByRole("note")).toContainText("*");
  await expect(page.getByRole("status").filter({ hasText: "Rule book version" })).toContainText(
    /Applied on every range \(\d+\)/,
    {
      timeout: 15_000,
    },
  );
  await axe(page);

  await row.getByRole("button", { name: "Disable" }).click();
  await expect(row).toContainText("(off)");

  // Merge: setting only the rate ⇒ concurrency kept as is.
  await row.getByRole("button", { name: "Edit" }).click();
  await dialog.getByLabel("Rate (CU/s)").fill("10");
  await dialog.getByRole("button", { name: "Save" }).click();
  await expect(row.getByRole("cell", { name: "10", exact: true })).toBeVisible();
  await expect(row.getByRole("cell", { name: "2", exact: true })).toBeVisible();

  // Clearing the concurrency box that was set ⇒ back to unset; rate kept.
  await row.getByRole("button", { name: "Edit" }).click();
  await expect(dialog.getByLabel("Concurrency")).toHaveValue("2");
  await dialog.getByLabel("Concurrency").fill("");
  await expect(dialog.getByLabel("Concurrency")).toHaveAttribute(
    "placeholder",
    "empty removes this limit",
  );
  await dialog.getByRole("button", { name: "Save" }).click();
  await expect(row.getByRole("cell", { name: "2", exact: true })).toHaveCount(0);
  await expect(row.getByRole("cell", { name: "10", exact: true })).toBeVisible();

  await page.getByRole("button", { name: "New rule", exact: true }).click();
  await dialog.getByLabel("Pattern").fill("a/b/c/d");
  await dialog.getByLabel("Concurrency").fill("1");
  await dialog.getByRole("button", { name: "Save" }).click();
  await expect(dialog.getByRole("alert")).toBeVisible();
  await dialog.getByRole("button", { name: "Go back" }).click();

  await row.getByRole("button", { name: "Delete" }).click();
  const del = page.getByRole("dialog");
  const go = del.getByRole("button", { name: "Delete" });
  await expect(go).toBeDisabled();
  await del.getByLabel(`Type ${pattern} to confirm`).fill(pattern);
  await go.click();
  await expect(
    page.getByRole("row", { name: new RegExp(pattern.replace("*", "\\*")) }),
  ).toHaveCount(0);
  await clean(page, watch, [/status of 400/]);
});

test("tenant rate limit: add (concurrency not accepted), delete by typing the tenant name", async ({
  page,
  watch,
}) => {
  await page.goto("/ui/flow");
  await page.getByRole("button", { name: "New tenant rate cap" }).click();
  const dialog = page.getByRole("dialog");
  await expect(dialog.getByLabel("Concurrency")).toHaveCount(0);
  await dialog.getByLabel("Tenant").fill("e2e-tenant");
  await dialog.getByLabel("Rate (CU/s)").fill("abc");
  await dialog.getByRole("button", { name: "Save" }).click();
  await expect(dialog.getByText("Must be a whole number")).toBeVisible();
  await dialog.getByLabel("Rate (CU/s)").fill("5000");
  await dialog.getByRole("button", { name: "Save" }).click();
  const tenants = page.getByRole("region", { name: "Tenant rate caps" });
  const row = tenants.getByRole("row", { name: /e2e-tenant/ });
  await expect(row).toContainText("5000");
  await row.getByRole("button", { name: "Delete" }).click();
  const del = page.getByRole("dialog");
  await del.getByLabel("Type e2e-tenant to confirm").fill("e2e-tenant");
  await del.getByRole("button", { name: "Delete" }).click();
  await expect(tenants.getByRole("row", { name: /e2e-tenant/ })).toHaveCount(0);
  await clean(page, watch);
});
