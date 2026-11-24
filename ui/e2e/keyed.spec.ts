// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import type { APIRequestContext } from "@playwright/test";
import { axe, clean, expect, test } from "./fixtures";

// Schedules, Workflows/promises, Objects & State, awakeables — on the
// real binary. Each case creates its own data (it does not touch the pending sample data).

async function create(request: APIRequestContext, path: string): Promise<string> {
  const r = await request.post(path, { data: null });
  expect(r.status(), await r.text()).toBeLessThan(300);
  return r.headers()["x-cairn-invocation-id"] as string;
}

const statusOf = async (request: APIRequestContext, id: string) =>
  ((await (await request.get(`/invocations/${id}`)).json()) as { status: string }).status;

test("schedule: create (cron described in words), pause, resume, trigger now, delete by typing the name", async ({
  page,
  watch,
}) => {
  const name = `e2e-${Date.now()}`;
  await page.goto("/ui/schedules");
  await page.getByRole("button", { name: "New schedule" }).click();
  const form = page.getByRole("dialog");
  await form.getByLabel("Name").fill(name);
  await form.getByLabel("Service").selectOption("Shop");
  await form.getByLabel("Handler").selectOption("ok");
  await form.getByRole("textbox", { name: "Cron" }).fill("30 2 * * *");
  await expect(form.getByText("Every day at 02:30")).toBeVisible();
  await form.getByLabel("Input (JSON, optional)").fill('{"amount": 9}');
  await form.getByRole("button", { name: "Save" }).click();
  const row = page.getByRole("row", { name: new RegExp(name) });
  await expect(row).toContainText("Shop/ok");
  await axe(page);

  await row.getByRole("button", { name: "Pause" }).click();
  await expect(page.getByText(`Paused ${name}`)).toBeVisible();
  await row.getByRole("button", { name: "Resume" }).click();
  await expect(page.getByText(`Resumed ${name}`)).toBeVisible();

  await row.getByRole("button", { name: "Run now" }).click();
  await page.getByRole("dialog").getByRole("button", { name: "Run now" }).click();
  await expect(page.getByRole("heading", { name: /Shop\/ok/ })).toBeVisible();

  await page.goto("/ui/schedules");
  await page
    .getByRole("row", { name: new RegExp(name) })
    .getByRole("button", { name: "Delete" })
    .click();
  const del = page.getByRole("dialog");
  await del.getByLabel(`Type ${name} to confirm`).fill(name);
  await del.getByRole("button", { name: "Delete" }).click();
  await expect(page.getByRole("row", { name: new RegExp(name) })).toHaveCount(0);
  await clean(page, watch);
});

test("schedule: bad cron ⇒ the server's 400 error shows in the dialog", async ({ page, watch }) => {
  await page.goto("/ui/schedules");
  await page.getByRole("button", { name: "New schedule" }).click();
  const form = page.getByRole("dialog");
  await form.getByLabel("Name").fill(`bad-${Date.now()}`);
  await form.getByLabel("Service").selectOption("Shop");
  await form.getByLabel("Handler").selectOption("ok");
  await form.getByRole("textbox", { name: "Cron" }).fill("99 * * * *");
  await form.getByRole("button", { name: "Save" }).click();
  await expect(form.getByRole("alert")).toBeVisible();
  await clean(page, watch, [/status of 400/]);
});

test("workflow: sees the pending promise, resolve ⇒ the run turn finishes", async ({
  page,
  request,
  watch,
}) => {
  const key = `wf-${Date.now()}`;
  const id = await create(request, `/invoke/Approval/run/send?key=${key}`);
  await expect.poll(() => statusOf(request, id)).toBe("suspended");
  // The list of recent run turns of the workflow service (default: the first service) ⇒ click the key.
  await page.goto("/ui/workflows");
  await page.getByRole("link", { name: key, exact: true }).click();
  await expect(page).toHaveURL(new RegExp(`service=Approval&key=${key}`));
  await expect(page.getByText("Promises the run is waiting on")).toBeVisible();
  const form = page.getByRole("form", { name: "Complete a promise" });
  await expect(form.getByLabel("Promise name")).toHaveValue("approved");
  await form.getByLabel("Value (JSON)").fill('"alice"');
  await form.getByRole("button", { name: "Resolve" }).click();
  await expect(page.getByText("Completed promise approved")).toBeVisible();
  await expect.poll(() => statusOf(request, id)).toBe("completed");
  await axe(page);
  await clean(page, watch);
});

test("objects: state of a virtual object", async ({ page, watch }) => {
  // A key the service just used ⇒ click it; the "Open a key…" box opens a known key directly.
  await page.goto("/ui/objects");
  await page.getByLabel("Service").selectOption("Cart");
  await page.getByRole("link", { name: "cart-1", exact: true }).click();
  await expect(page).toHaveURL(/service=Cart&key=cart-1/);
  await expect(page.getByText("items")).toBeVisible();
  await page.getByRole("link", { name: "Cart keys" }).click();
  await page.getByRole("textbox", { name: "Key", exact: true }).fill("cart-1");
  await page.getByRole("textbox", { name: "Key", exact: true }).press("Enter");
  await expect(page).toHaveURL(/service=Cart&key=cart-1/);
  await expect(page.getByText("items")).toBeVisible();
  await axe(page);
  await clean(page, watch);
});

test("awakeable: paste awk_… into Cmd+K, resolve ⇒ the invocation finishes", async ({
  page,
  request,
  watch,
}) => {
  const id = await create(request, "/invoke/Shop/waitAwk/send");
  await expect.poll(() => statusOf(request, id)).toBe("suspended");
  const awk = `awk_${id}00000011`;
  await page.goto("/ui/");
  await expect(page.getByRole("heading", { name: "Overview", level: 1 })).toBeVisible();
  await page.keyboard.press("ControlOrMeta+k");
  await page.getByRole("combobox").fill(awk);
  await page.keyboard.press("Enter");
  await expect(page).toHaveURL(new RegExp(`/ui/awakeables/${awk}$`));
  await page.getByLabel("Value (JSON)").fill('"done"');
  await page.getByRole("button", { name: "Resolve" }).click();
  await expect(page.getByText("Resolved")).toBeVisible();
  await expect.poll(() => statusOf(request, id)).toBe("completed");
  await axe(page);
  await clean(page, watch);
});

test("try a handler from Services ⇒ lands on the new invocation, with a tag", async ({
  page,
  request,
  watch,
}) => {
  await page.goto("/ui/services");
  await page.getByRole("button", { name: "Run ok…" }).click();
  const dialog = page.getByRole("dialog", { name: "Run Shop/ok" });
  await dialog.getByLabel("Input (JSON)").fill('{"amount": 3}');
  await dialog.getByLabel("Tag (k=v, optional)").fill("try=ui");
  await dialog.getByRole("button", { name: "Run" }).click();
  await expect(page).toHaveURL(/\/ui\/invocations\/[0-9a-f]{48}$/);
  const id = page.url().split("/").at(-1) as string;
  await expect.poll(() => statusOf(request, id)).toBe("completed");
  await expect(page.getByText("try=ui")).toBeVisible();
  await clean(page, watch);
});
