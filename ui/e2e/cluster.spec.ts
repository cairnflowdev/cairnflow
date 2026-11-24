// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { axe, clean, expect, test } from "./fixtures";

// Cluster screen on the `noauth` node (cluster-admin) — one node, or a cluster
// of 4 ranges in the `check.sh` cluster row. Split the leftmost range, then merge the two halves
// back: the range count returns to what it was for later cases.

test("nodes, key-space bar, split then merge (type to confirm), operation log", async ({
  page,
  request,
  watch,
}) => {
  test.setTimeout(90_000);
  const before = ((await (await request.get("/cluster")).json()) as { ranges: unknown[] }).ranges
    .length;
  await page.goto("/ui/cluster");
  await expect(page.getByRole("region", { name: "Nodes" }).getByRole("row")).not.toHaveCount(0);
  await expect(page.getByText(/^Cluster version \d+/)).toBeVisible();
  const bar = page.getByRole("group", { name: /Ranges across the key space/ });
  await expect(bar.getByRole("button")).toHaveCount(before);
  await axe(page);

  // Split the leftmost range (starting at key 0).
  const left = bar.getByRole("button", { name: /: 0000000000000000–/ });
  const id = (await left.getAttribute("aria-label"))?.match(/^Range (\d+):/)?.[1] as string;
  await left.click();
  await page.getByRole("button", { name: "Split", exact: true }).click();
  let dialog = page.getByRole("dialog");
  await expect(dialog.getByRole("button", { name: "Split" })).toBeDisabled();
  await dialog.getByLabel(`Type split ${id} to confirm`).fill(`split ${id}`);
  await dialog.getByRole("button", { name: "Split" }).click();
  await expect(
    page.getByText(new RegExp(`Split range ${id} — ${before + 1} ranges now`)),
  ).toBeVisible({ timeout: 60_000 });
  await expect(bar.getByRole("button")).toHaveCount(before + 1);
  await expect(
    page
      .getByRole("region", { name: "Split / merge log" })
      .getByRole("cell", { name: "split" })
      .first(),
  ).toBeVisible();
  // The operation log records parent ⇒ children, not just the parent number.
  await expect(
    page
      .getByRole("region", { name: "Split / merge log" })
      .getByText(new RegExp(`^#${id} → #\\d+, #\\d+$`))
      .first(),
  ).toBeVisible();

  // Merge the two halves just split.
  const a = bar.getByRole("button", { name: /: 0000000000000000–/ });
  const la = (await a.getAttribute("aria-label"))?.match(/^Range (\d+):/)?.[1] as string;
  await a.click();
  const merge = page.getByRole("button", { name: /^Merge with \d+$/ });
  const lb = (await merge.textContent())?.match(/(\d+)$/)?.[1] as string;
  await merge.click();
  dialog = page.getByRole("dialog");
  await dialog.getByLabel(`Type merge ${la} ${lb} to confirm`).fill(`merge ${la} ${lb}`);
  await dialog.getByRole("button", { name: "Merge" }).click();
  await expect(page.getByText(`Merged ranges ${la} and ${lb}`)).toBeVisible({ timeout: 60_000 });
  await expect(bar.getByRole("button")).toHaveCount(before);
  await axe(page);
  await clean(page, watch);
});

// Split at a chosen key. The leftmost range starts at 0 and
// extends to at least 3fff… (one range, or 4 ranges in the cluster row) ⇒ 1000… is inside it.
// A bad key is blocked right in the dialog; after the split the new boundary is exactly that key, the operation
// log records `at`, then merge back so the range count returns to what it was.
test("split at a chosen key: bad key blocked, new boundary at the key, operation log records at", async ({
  page,
  request,
  watch,
}) => {
  test.setTimeout(90_000);
  const AT = "1000000000000000";
  const before = ((await (await request.get("/cluster")).json()) as { ranges: unknown[] }).ranges
    .length;
  await page.goto("/ui/cluster");
  const bar = page.getByRole("group", { name: /Ranges across the key space/ });
  await expect(bar.getByRole("button")).toHaveCount(before);
  const left = bar.getByRole("button", { name: /: 0000000000000000–/ });
  const id = (await left.getAttribute("aria-label"))?.match(/^Range (\d+):/)?.[1] as string;
  await left.click();
  await page.getByRole("button", { name: "Split", exact: true }).click();
  const dialog = page.getByRole("dialog");
  const field = dialog.getByLabel("Split at key (hex, 16 digits)");
  await expect(field).toHaveAttribute("placeholder", /^[0-9a-f]{16}$/);
  await dialog.getByLabel(`Type split ${id} to confirm`).fill(`split ${id}`);
  // The range's own first key: not allowed (the server also returns 400).
  // Submit with Enter in the box (the keyboard path) — the pointer is not resting on the red button.
  await field.fill("0000000000000000");
  await field.press("Enter");
  await expect(dialog.getByRole("alert")).toContainText("must be after 0000000000000000");
  await field.fill("12345");
  await field.press("Enter");
  await expect(dialog.getByRole("alert")).toContainText("exactly 16 hex digits");
  await axe(page);
  await field.fill(AT);
  await expect(dialog.getByRole("alert")).toHaveCount(0);
  await dialog.getByRole("button", { name: "Split" }).click();
  await expect(
    page.getByText(new RegExp(`Split range ${id} — ${before + 1} ranges now`)),
  ).toBeVisible({ timeout: 60_000 });
  await expect(bar.getByRole("button")).toHaveCount(before + 1);
  // New boundary: the left half ends right before AT, the right half starts at AT.
  await expect(
    bar.getByRole("button", { name: /: 0000000000000000–0fffffffffffffff,/ }),
  ).toHaveCount(1);
  await expect(bar.getByRole("button", { name: new RegExp(`: ${AT}–`) })).toHaveCount(1);
  const ops = (await (await request.get("/ranges/ops")).json()) as {
    ops: { kind: string; ranges: number[]; at?: string; state: string }[];
  };
  expect(
    ops.ops.some(
      (o) =>
        o.kind === "split" && o.ranges[0] === Number(id) && o.at === AT && o.state === "committed",
    ),
  ).toBe(true);

  // Merge the two halves back.
  const a = bar.getByRole("button", { name: /: 0000000000000000–/ });
  const la = (await a.getAttribute("aria-label"))?.match(/^Range (\d+):/)?.[1] as string;
  await a.click();
  const merge = page.getByRole("button", { name: /^Merge with \d+$/ });
  const lb = (await merge.textContent())?.match(/(\d+)$/)?.[1] as string;
  await merge.click();
  const md = page.getByRole("dialog");
  await md.getByLabel(`Type merge ${la} ${lb} to confirm`).fill(`merge ${la} ${lb}`);
  await md.getByRole("button", { name: "Merge" }).click();
  await expect(page.getByText(`Merged ranges ${la} and ${lb}`)).toBeVisible({ timeout: 60_000 });
  await expect(bar.getByRole("button")).toHaveCount(before);
  await clean(page, watch);
});
