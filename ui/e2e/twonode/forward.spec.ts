// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { clean, expect, test } from "../fixtures";
import { BULK_TAG, type TwoNodeSeed } from "./setup";

// Console on a two-node cluster: the console is opened on the node that does **not**
// lead the invocation's range (chosen by the global setup), every action must succeed
// thanks to forwarding — no `503 leader`, no "the range is led by another node" message.

const seed = (): TwoNodeSeed => JSON.parse(process.env.CAIRN_E2E_TWONODE_SEED ?? "{}");

// biome-ignore lint/correctness/noEmptyPattern: Playwright requires the first argument to be a destructuring pattern.
test.beforeEach(({}, info) => {
  test.skip(info.project.name !== `console-${seed().console}`, "the console is on the other node");
});

test("kill now an invocation led by the other node", async ({ page, request, watch }) => {
  const { id } = seed();
  await page.goto(`/ui/invocations/${id}`);
  await expect(page.getByRole("button", { name: "Actions" })).toBeVisible();
  await page.getByRole("button", { name: "Actions" }).click();
  await page.getByRole("menuitem", { name: "Cancel now" }).click();
  const dialog = page.getByRole("dialog", { name: "Cancel now" });
  await dialog.getByLabel(/^Type .* to confirm$/).fill(id.slice(-8));
  const res = page.waitForResponse((r) => r.url().endsWith(`/invocations/${id}/cancel`));
  await dialog.getByRole("button", { name: "Cancel now" }).click();
  const r = await res;
  expect(r.status()).toBe(200);
  expect(r.headers()["x-cairn-forwarded-by"]).toBeTruthy();
  await expect(page.getByText("Cancelled", { exact: true })).toBeVisible();
  await expect
    .poll(
      async () =>
        ((await (await request.get(`/invocations/${id}`)).json()) as { status: string }).status,
    )
    .toBe("failed");
  await clean(page, watch);
});

test("bulk: ids led by the other node run there", async ({ page, request, watch }) => {
  const { cancelId } = seed();
  await page.goto(`/ui/invocations?tag=${encodeURIComponent(BULK_TAG)}`);
  await expect(page.locator("tbody tr")).toHaveCount(1);
  await page.getByLabel("Select all rows on this page").check();
  const bar = page.getByRole("region", { name: "Bulk actions" });
  await bar.getByRole("button", { name: "Cancel…" }).click();
  const dialog = page.getByRole("dialog");
  await expect(dialog.getByRole("radio", { name: /Cancel \(cooperative\)/ })).toBeChecked();
  await dialog.getByRole("button", { name: "Cancel (cooperative)" }).click();
  await expect(dialog.getByRole("status")).toContainText("1 of 1 done — 1 ok, 0 failed", {
    timeout: 30_000,
  });
  await dialog.getByRole("button", { name: "Done" }).click();
  await expect
    .poll(
      async () =>
        ((await (await request.get(`/invocations/${cancelId}`)).json()) as { status: string })
          .status,
    )
    .toBe("failed");
  await clean(page, watch);
});

test("live log of an invocation led by the other node (SSE through forwarding)", async ({
  page,
  watch,
}) => {
  const { logId } = seed();
  const res = page.waitForResponse((r) => r.url().includes(`/invocations/${logId}/logs?follow=1`));
  await page.goto(`/ui/invocations/${logId}?tab=log`);
  const r = await res;
  expect(r.status()).toBe(200);
  expect(r.headers()["x-cairn-forwarded-by"]).toBeTruthy();
  await expect(page.getByRole("status").filter({ hasText: /^Live/ })).toBeVisible();
  await expect(page.getByText("napping")).toBeVisible();
  await clean(page, watch);
});
