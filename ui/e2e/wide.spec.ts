// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { serve, service, TerminalError } from "../../sdk/typescript/src/index.ts";
import { clean, expect, test } from "./fixtures";
import { E2E_PORT } from "./ports";

// Runs LAST in the `noauth` project (the file name sorts after every other spec, like `deploy.auth.spec.ts`): the
// long-named service registered here stays on the shared node, and the filter's fuzzy suggestion box would match it with text
// typed in another test ("Nope").
//
// Long text: long service/handler names must not push columns out of the table at
// 1280 px — the name cell is cut with "…" (full name in `title`), later columns stay inside the frame. A table
// overflowing its frame gets the `region` role "Table, scrolls" (scroll frame) from `Table` — at 1280 px there must be none.
const SVC = "InvoiceReconciliationForEnterpriseCustomers";
const HANDLER = "reconcileOutstandingInvoicesAcrossAllRegions";
const BASE = `http://127.0.0.1:${E2E_PORT}`;

test.beforeAll(async () => {
  // A single call failing with a very long error: the "Last error" column must not be pushed out of the frame either.
  const long = service(SVC, {
    async [HANDLER](_ctx, n: number) {
      if (n % 2)
        throw new TerminalError(
          "upstream ledger rejected the batch: invoice INV-2026-000184213 references a closed fiscal period (FY2025-Q4) and cannot be reconciled without a manual adjustment entry",
          422,
        );
      return "ok";
    },
  });
  const ep = await serve({ services: [long], port: 0, hostname: "127.0.0.1" });
  const r = await fetch(`${BASE}/deployments`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ uri: ep.url }),
  });
  expect(r.ok).toBe(true);
  for (let i = 0; i < 3; i++) {
    await fetch(`${BASE}/invoke/${SVC}/${HANDLER}/send`, { method: "POST", body: String(i) });
  }
});

test("long names at 1280 px: Invocations list, Overview, Services do not overflow", async ({
  page,
  watch,
}) => {
  await page.setViewportSize({ width: 1280, height: 900 });
  await page.goto(`/ui/invocations?service=${SVC}`);
  await expect(page.getByRole("row", { name: new RegExp(HANDLER) }).first()).toBeVisible();
  await expect(page.getByText(/upstream ledger rejected/).first()).toBeVisible();
  await expect(page.getByRole("region", { name: "Table, scrolls" })).toHaveCount(0);
  await expect(page.getByRole("columnheader", { name: "Last error" })).toBeInViewport();

  await page.goto("/ui/");
  // A 4-range cluster (the cluster row of `check.sh`): per-handler stats arrive later — wait up to 30 s.
  await expect(
    page.getByRole("link", { name: new RegExp(`${SVC}/${HANDLER}`) }).first(),
  ).toBeVisible({ timeout: 30_000 });
  await page.waitForLoadState("networkidle");
  await expect(page.getByRole("region", { name: "Table, scrolls" })).toHaveCount(0);

  await page.goto("/ui/services");
  const card = page.getByRole("region", { name: SVC });
  await expect(card).toBeVisible();
  const spill = await card.evaluate((el) => el.scrollWidth - el.clientWidth);
  expect(spill, "content overflowing the service card").toBeLessThanOrEqual(0);
  await clean(page, watch);
});
