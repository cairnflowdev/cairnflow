// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import type { APIRequestContext } from "@playwright/test";
import { clean, expect, test } from "./fixtures";

// Bulk actions on the real binary. Each case tags the
// invocations it creates and filters by that tag — it does not touch the sample data.

async function make(
  request: APIRequestContext,
  path: string,
  tag: string,
  n: number,
): Promise<string[]> {
  const ids: string[] = [];
  for (let i = 0; i < n; i++) {
    const r = await request.post(path, { data: null, headers: { "cairnflow-tag": tag } });
    expect(r.status()).toBeLessThan(300);
    ids.push(r.headers()["x-cairn-invocation-id"] as string);
  }
  return ids;
}

async function statusOf(request: APIRequestContext, id: string): Promise<number | string> {
  const r = await request.get(`/invocations/${id}`);
  return r.status() === 404 ? 404 : ((await r.json()) as { status: string }).status;
}

test("select rows ⇒ bulk cancel, type the count to confirm", async ({ page, request, watch }) => {
  const tag = `bulk=c${Date.now()}`;
  const ids = await make(request, "/invoke/Shop/nap/send", tag, 3);
  for (const id of ids) await expect.poll(() => statusOf(request, id)).toBe("suspended");
  await page.goto(`/ui/invocations?tag=${encodeURIComponent(tag)}`);
  await expect(page.locator("tbody tr")).toHaveCount(3);
  await page.getByLabel("Select all rows on this page").check();
  const bar = page.getByRole("region", { name: "Bulk actions" });
  await expect(bar).toContainText("3 selected");
  await bar.getByRole("button", { name: "Cancel…" }).click();
  const dialog = page.getByRole("dialog");
  await dialog.getByRole("radio", { name: /Cancel now/ }).check();
  const go = dialog.getByRole("button", { name: "Cancel now" });
  await expect(go).toBeDisabled();
  await dialog.getByLabel("Type 3 to confirm").fill("3");
  await go.click();
  await expect(dialog.getByRole("status")).toContainText("3 of 3 done — 3 ok, 0 failed");
  await dialog.getByRole("button", { name: "Done" }).click();
  for (const id of ids) expect(await statusOf(request, id)).toBe("failed");
  await clean(page, watch);
});

test("select every invocation matching the filter ⇒ purge; per-id errors do not stop the batch", async ({
  page,
  request,
  watch,
}) => {
  const tag = `bulk=p${Date.now()}`;
  const done = await make(request, "/invoke/Shop/ok/send", tag, 2);
  const [running] = await make(request, "/invoke/Shop/nap/send", tag, 1);
  for (const id of done) await expect.poll(() => statusOf(request, id)).toBe("completed");
  await page.goto(`/ui/invocations?tag=${encodeURIComponent(tag)}`);
  await expect(page.locator("tbody tr")).toHaveCount(3);
  await page.locator("tbody tr").first().getByRole("checkbox").check();
  const bar = page.getByRole("region", { name: "Bulk actions" });
  await bar.getByRole("button", { name: "Select all 3 matching" }).click();
  await expect(bar).toContainText("3 selected");
  await bar.getByRole("button", { name: "Purge" }).click();
  const dialog = page.getByRole("dialog");
  await dialog.getByLabel("Type 3 to confirm").fill("3");
  await dialog.getByRole("button", { name: "Purge" }).click();
  // A running invocation cannot be purged (409) — its own error.
  await expect(dialog.getByRole("status")).toContainText("3 of 3 done — 2 ok, 1 failed");
  await dialog.getByText("Last 1 errors").click();
  await expect(dialog.getByText(/409/)).toBeVisible();
  for (const id of done) expect(await statusOf(request, id)).toBe(404);
  expect(await statusOf(request, running as string)).toBe("suspended");
  await clean(page, watch);
});

// A selection belongs to one filter. Changing the view or filter while keeping
// the selection would let the bulk bar act on rows no longer visible.
test("change view ⇒ selection cleared, hidden rows untouched", async ({ page, request, watch }) => {
  const tag = `bulk=v${Date.now()}`;
  const ids = await make(request, "/invoke/Shop/nap/send", tag, 2);
  for (const id of ids) await expect.poll(() => statusOf(request, id)).toBe("suspended");
  await page.goto(`/ui/invocations?tag=${encodeURIComponent(tag)}`);
  await expect(page.locator("tbody tr")).toHaveCount(2);
  await page.getByLabel("Select all rows on this page").check();
  const bar = page.getByRole("region", { name: "Bulk actions" });
  await expect(bar).toContainText("2 selected");
  await page
    .getByRole("navigation", { name: "Views" })
    .getByRole("link", { name: "Failed" })
    .click();
  await expect(bar).toHaveCount(0);
  await page.getByRole("navigation", { name: "Views" }).getByRole("link", { name: "All" }).click();
  await expect(page.locator("tbody tr")).toHaveCount(2);
  await expect(bar).toHaveCount(0);
  await clean(page, watch);
});

test("bulk cancel defaults to cooperative cancel, no typing", async ({ page, request, watch }) => {
  const tag = `bulk=s${Date.now()}`;
  const ids = await make(request, "/invoke/Shop/nap/send", tag, 2);
  for (const id of ids) await expect.poll(() => statusOf(request, id)).toBe("suspended");
  await page.goto(`/ui/invocations?tag=${encodeURIComponent(tag)}`);
  await page.getByLabel("Select all rows on this page").check();
  const bar = page.getByRole("region", { name: "Bulk actions" });
  await bar.getByRole("button", { name: "Cancel…" }).click();
  const dialog = page.getByRole("dialog");
  await expect(dialog.getByRole("radio", { name: /Cancel \(cooperative\)/ })).toBeChecked();
  // The dialog's close button must not have the same text as the cancel-invocation action.
  await expect(dialog.getByRole("button", { name: "Go back" })).toBeVisible();
  await dialog.getByRole("button", { name: "Cancel (cooperative)" }).click();
  await expect(dialog.getByRole("status")).toContainText("2 of 2 done — 2 ok, 0 failed", {
    timeout: 30_000,
  });
  await dialog.getByRole("button", { name: "Done" }).click();
  for (const id of ids) await expect.poll(() => statusOf(request, id)).toBe("failed");
  await clean(page, watch);
});
