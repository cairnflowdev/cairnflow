// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import type { APIRequestContext } from "@playwright/test";
import { axe, clean, expect, test } from "./fixtures";
import { seed } from "./seed";

// The "why" banner, items 4–5, on the `noauth` node
// (cluster-admin — can read the flow-control RAM tables).

async function create(request: APIRequestContext, path: string, data: unknown = null) {
  const r = await request.post(path, {
    data,
    headers: { "content-type": "application/json" },
  });
  expect(r.status(), await r.text()).toBeLessThan(300);
  const id = r.headers()["x-cairn-invocation-id"];
  expect(id).toBeTruthy();
  return id as string;
}

test("queued behind whom: the second call of Cart · cart-1 names the right lock holder", async ({
  page,
  watch,
}) => {
  const s = seed();
  await page.goto(`/ui/invocations/${s.queued}`);
  const why = page.getByRole("region", { name: "Why it is in this state" });
  await expect(why).toContainText("Waiting for the lock of Cart · cart-1 — 1 of 1 in line");
  await expect(why).toContainText("in line since");
  const holder = why.getByRole("link", { name: `${s.holder.slice(0, 4)}…${s.holder.slice(-6)}` });
  await expect(holder).toBeVisible();
  await axe(page);
  await holder.click();
  await expect(page).toHaveURL(new RegExp(`/invocations/${s.holder}`));
  // The holder's side: says one call is waiting behind it.
  await expect(page.getByRole("region", { name: "Why it is in this state" })).toContainText(
    "Holds the lock of Cart · cart-1; waiting behind it: 1",
  );
  await clean(page, watch);
});

// Flow control: a concurrency-limit-1 rule for a dedicated scope; the first call
// sleeps holding the slot, the second call is blocked — the banner names the counter, usage and rule.
// Same scope ⇒ same range (the partition key is derived from the scope), so there is one counter.
test("flow control: a call held by the concurrency limit names the counter and rule", async ({
  page,
  request,
  watch,
}) => {
  test.setTimeout(90_000);
  const scope = `e2ewhy${Date.now() % 100000}`;
  const put = await request.post("/rules", {
    data: { pattern: scope, concurrency: 1, description: "e2e why" },
  });
  expect(put.status(), await put.text()).toBe(200);
  const { version } = (await put.json()) as { version: number };
  await expect
    .poll(async () => {
      const r = (await (await request.get("/scheduler")).json()) as { rulesVersion: number }[];
      return r.every((x) => x.rulesVersion >= version);
    })
    .toBe(true);
  const ids: string[] = [];
  try {
    ids.push(await create(request, `/scope/${scope}/invoke/Shop/busy/send`, { ms: 12_000 }));
    await expect
      .poll(
        async () =>
          ((await (await request.get(`/invocations/${ids[0]}`)).json()) as { status: string })
            .status,
      )
      .toBe("running");
    ids.push(await create(request, `/scope/${scope}/invoke/Shop/busy/send`, { ms: 12_000 }));
    await page.goto(`/ui/invocations/${ids[1]}`);
    const why = page.getByRole("region", { name: "Why it is in this state" });
    await expect(why).toContainText(
      `Held back by flow control: concurrency cap — counter ${scope} at 1 of 1 (rule ${scope})`,
      { timeout: 15_000 },
    );
    await axe(page);
    await clean(page, watch);
  } finally {
    for (const id of ids) await request.post(`/invocations/${id}/cancel`);
    await request.delete(`/rules/${encodeURIComponent(scope)}`);
  }
});

// As above, but the call carries a limit key (`?limit-key=`): a two-level rule
// `scope/*` with concurrency limit 1 per limit key. The console finds the queue entry through the
// `limit_key` column of `sys_invocation` — without that column the banner
// has no flow-control item.
test("flow control by limit key: the banner names the scope/limit key counter", async ({
  page,
  request,
  watch,
}) => {
  test.setTimeout(90_000);
  const scope = `e2elk${Date.now() % 100000}`;
  const pattern = `${scope}/*`;
  const put = await request.post("/rules", {
    data: { pattern, concurrency: 1, description: "e2e why limit key" },
  });
  expect(put.status(), await put.text()).toBe(200);
  const { version } = (await put.json()) as { version: number };
  await expect
    .poll(async () => {
      const r = (await (await request.get("/scheduler")).json()) as { rulesVersion: number }[];
      return r.every((x) => x.rulesVersion >= version);
    })
    .toBe(true);
  const ids: string[] = [];
  const path = `/scope/${scope}/invoke/Shop/busy/send?limit-key=alice`;
  try {
    ids.push(await create(request, path, { ms: 12_000 }));
    await expect
      .poll(
        async () =>
          ((await (await request.get(`/invocations/${ids[0]}`)).json()) as { status: string })
            .status,
      )
      .toBe("running");
    ids.push(await create(request, path, { ms: 12_000 }));
    await page.goto(`/ui/invocations/${ids[1]}`);
    const why = page.getByRole("region", { name: "Why it is in this state" });
    await expect(why).toContainText(
      `Held back by flow control: concurrency cap — counter ${scope}/alice at 1 of 1 (rule ${pattern})`,
      { timeout: 15_000 },
    );
    await axe(page);
    await clean(page, watch);
  } finally {
    for (const id of ids) await request.post(`/invocations/${id}/cancel`);
    await request.delete(`/rules/${encodeURIComponent(pattern)}`);
  }
});
