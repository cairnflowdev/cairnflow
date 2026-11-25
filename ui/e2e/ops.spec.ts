// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import type { APIRequestContext } from "@playwright/test";
import { axe, clean, expect, test } from "./fixtures";

// Phase 3 — operations: Usage, Slow runs, Archive on the
// `noauth` node with sample data. Each case creates its own data.

async function create(request: APIRequestContext, path: string): Promise<string> {
  const r = await request.post(path, { data: null });
  expect(r.status(), await r.text()).toBeLessThan(300);
  return r.headers()["x-cairn-invocation-id"] as string;
}

const statusOf = async (request: APIRequestContext, id: string) =>
  ((await (await request.get(`/invocations/${id}`)).json()) as { status: string }).status;

test("Usage: this-hour tile, time range, chart or no history yet, per-handler table", async ({
  page,
  watch,
}) => {
  await page.goto("/ui/usage");
  const now = page.getByRole("region", { name: "This hour" });
  for (const t of [
    "Invocations",
    "Durable steps",
    "Run time",
    "Input tokens",
    "Output tokens",
    "Cost",
  ])
    await expect(now.getByText(t, { exact: true })).toBeVisible();
  // Tokens and cost per hour: `GET /metering/hours` through the fixture's strict
  // schema checker; the numbers flush every minute so they may not be there yet.
  const tokens = page.getByRole("region", { name: "Tokens and cost, last 24 hours" });
  await expect(
    tokens
      .getByRole("img", { name: /Line chart of input and output tokens/ })
      .or(tokens.getByText(/^No tokens recorded/)),
  ).toBeVisible();
  // The hourly summary flushes every minute — it may not be there yet when this case runs.
  await expect(
    page
      .getByRole("img", { name: /Line chart of completed and failed/ })
      .or(page.getByText("No history yet").first()),
  ).toBeVisible();
  await axe(page);
  await page.getByRole("button", { name: "30 days" }).click();
  await expect(page).toHaveURL(/range=30d/);
  await expect(page.getByRole("heading", { name: "By handler, last 30 days" })).toBeVisible();
  await clean(page, watch);
});

test("Slow runs: threshold 1 ms ⇒ a Shop/ok turn shows with 'mostly because of'", async ({
  page,
  request,
  watch,
}) => {
  const put = (slowMs: number) => request.put("/admin/tenants/default", { data: { slowMs } });
  expect((await put(1)).ok()).toBe(true);
  try {
    // The node loads the threshold on an interval — call until a turn is recorded as slow.
    await expect
      .poll(
        async () => {
          await request.post("/invoke/Shop/ok", { data: { amount: 1 } });
          const r = await request.get("/stats/slow-runs?scope=cluster");
          const v = (await r.json()) as { runs: { service: string; handler: string }[] };
          return v.runs.some((x) => x.service === "Shop" && x.handler === "ok");
        },
        { timeout: 30_000 },
      )
      .toBe(true);
  } finally {
    await put(0);
  }
  await page.goto("/ui/slow-runs");
  const row = page.getByRole("row", { name: /Shop\/ok/ }).first();
  await expect(row).toContainText(/ms \(limit 1\)/);
  await expect(row).toContainText(/your code|SDK|durable waits|engine wait|quota wait/);
  await axe(page);
  await clean(page, watch);
});

test("Archive: purge ⇒ read back by id (status, output, journal), hash-chain check; listed under the day", async ({
  page,
  request,
  watch,
}) => {
  const id = await create(request, "/invoke/Shop/ok/send");
  await expect.poll(() => statusOf(request, id)).toBe("completed");
  expect((await request.post(`/invocations/${id}/purge`)).ok()).toBe(true);
  await expect.poll(async () => (await request.get(`/archive/${id}`)).status()).toBe(200);

  await page.goto("/ui/archive");
  await page.getByLabel("Invocation ID").fill(id);
  await page.getByRole("button", { name: "Open" }).click();
  const one = page.getByRole("region", { name: /Archived invocation/ });
  await expect(one).toContainText("completed");
  await expect(one.getByText("Output", { exact: true })).toBeVisible();
  await one.getByText(/^Journal \(\d+ entries\)$/).click();
  await expect(one.getByText(/^0 Input/)).toBeVisible();
  await one.getByRole("button", { name: "Verify hash chain" }).click();
  await expect(one.getByRole("status")).toHaveText(
    /matches its hash chain|No hash chain on this tenant/,
  );
  const day = page.getByRole("region", { name: /^Archived on / });
  await expect(day.getByRole("row", { name: new RegExp(id.slice(0, 4)) }).first()).toBeVisible();
  await axe(page);
  await clean(page, watch);
});
