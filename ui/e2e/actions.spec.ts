// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import type { APIRequestContext, Page } from "@playwright/test";
import { axe, clean, expect, test } from "./fixtures";

// Actions on an invocation on the real binary. Each case
// creates its own invocation (it does not touch the shared sample data). The 409 branch is
// real (a race with an API command); 403, 429 and 503 "not the leader" are faked with
// `page.route` — a key with full rights on one node cannot produce those codes by itself.

async function create(request: APIRequestContext, path: string): Promise<string> {
  const r = await request.post(path, {
    data: null,
    headers: { "content-type": "application/json" },
  });
  expect(r.status(), await r.text()).toBeLessThan(300);
  const id = r.headers()["x-cairn-invocation-id"];
  expect(id).toBeTruthy();
  return id as string;
}

async function status(request: APIRequestContext, id: string, want: (s: string) => boolean) {
  await expect
    .poll(
      async () =>
        want(
          ((await (await request.get(`/invocations/${id}`)).json()) as { status: string }).status,
        ),
      { timeout: 15_000 },
    )
    .toBe(true);
}

async function open(page: Page, id: string) {
  await page.goto(`/ui/invocations/${id}`);
  await expect(page.getByRole("button", { name: "Actions" })).toBeVisible();
}

async function menu(page: Page, item: string) {
  await page.getByRole("button", { name: "Actions" }).click();
  await page.getByRole("menuitem", { name: item }).click();
}

test("resume: toast when the server confirms; a real 409 when the invocation was already cancelled", async ({
  page,
  request,
  watch,
}) => {
  const id = await create(request, "/invoke/Shop/flaky/send");
  await status(request, id, (s) => s === "paused");
  await open(page, id);
  await page.getByRole("button", { name: "Resume" }).click();
  await expect(page.getByText("Resumed")).toBeVisible();

  const other = await create(request, "/invoke/Shop/flaky/send");
  await status(request, other, (s) => s === "paused");
  await open(page, other);
  await request.post(`/invocations/${other}/cancel`);
  await page.getByRole("button", { name: "Resume" }).click();
  await expect(page.getByText(/^Not possible right now/)).toBeVisible();
  await clean(page, watch, [/status of 409/]);
});

test("kill now: type to confirm", async ({ page, request, watch }) => {
  const id = await create(request, "/invoke/Shop/nap/send");
  await status(request, id, (s) => s === "suspended");
  await open(page, id);
  await menu(page, "Cancel now");
  const dialog = page.getByRole("dialog", { name: "Cancel now" });
  const confirm = dialog.getByRole("button", { name: "Cancel now" });
  await expect(confirm).toBeDisabled();
  await dialog.getByLabel(/^Type .* to confirm$/).fill(id.slice(-7));
  await expect(confirm).toBeDisabled();
  await dialog.getByLabel(/^Type .* to confirm$/).fill(id.slice(-8));
  await confirm.click();
  await expect(page.getByText("Cancelled", { exact: true })).toBeVisible();
  await status(request, id, (s) => s === "failed");
  await clean(page, watch);
});

test("cooperative cancel: confirm, SDK cleans up then stops", async ({ page, request, watch }) => {
  const id = await create(request, "/invoke/Shop/nap/send");
  await status(request, id, (s) => s === "suspended");
  await open(page, id);
  await menu(page, "Cancel (cooperative)");
  await page
    .getByRole("dialog", { name: "Cancel (cooperative)" })
    .getByRole("button", { name: "Cancel (cooperative)" })
    .click();
  // The server replies once the invocation has really stopped: the SDK wakes up, receives CANCEL, cleans up.
  await expect(page.getByRole("dialog")).toHaveCount(0, { timeout: 30_000 });
  await status(request, id, (s) => s === "failed");
  await clean(page, watch);
});

test("purge: type to confirm, back to the list", async ({ page, request, watch }) => {
  const id = await create(request, "/invoke/Shop/ok/send");
  await status(request, id, (s) => s === "completed");
  await open(page, id);
  await menu(page, "Purge");
  const dialog = page.getByRole("dialog", { name: "Purge" });
  await dialog.getByLabel(/^Type .* to confirm$/).fill(id.slice(-8));
  await dialog.getByRole("button", { name: "Purge" }).click();
  await expect(page).toHaveURL(/\/ui\/invocations$/);
  expect((await request.get(`/invocations/${id}`)).status()).toBe(404);
  await clean(page, watch);
});

test("restart from a journal entry ⇒ new invocation; trial replay matches", async ({
  page,
  request,
  watch,
}) => {
  const id = await create(request, "/invoke/Shop/ok/send");
  await status(request, id, (s) => s === "completed");
  await open(page, id);
  await menu(page, "Replay check");
  const replay = page.getByRole("dialog", { name: "Replay check" });
  await replay.getByRole("button", { name: "Replay check" }).click();
  await expect(replay.getByText(/^Matches/)).toBeVisible();
  await page.keyboard.press("Escape");

  await page.getByRole("button", { name: "Restart from here (1)" }).click();
  const dialog = page.getByRole("dialog", { name: "Restart from a step" });
  // Pick by step, not by entry number; entry 1 is the first command ⇒ "from the start".
  await expect(dialog.getByLabel("Run again from")).toHaveValue("1");
  await dialog.getByRole("button", { name: "Restart", exact: true }).click();
  await expect(page).not.toHaveURL(new RegExp(id));
  await expect(page.getByRole("heading", { name: /Shop\/ok/ })).toBeVisible();
  await clean(page, watch);
});

test("migrate to a deployment (trial replay matches first)", async ({ page, request, watch }) => {
  const id = await create(request, "/invoke/Shop/nap/send");
  await status(request, id, (s) => s === "suspended");
  const [dep] = (await (await request.get("/deployments")).json()) as { id: string }[];
  await open(page, id);
  await menu(page, "Migrate…");
  const dialog = page.getByRole("dialog", { name: "Migrate…" });
  await dialog.getByLabel("Deployment").selectOption(dep?.id ?? "");
  await dialog.getByRole("button", { name: "Migrate…" }).click();
  await expect(page.getByText("Migrated")).toBeVisible();
  await clean(page, watch);
});

test("403 and 429: show the reason, do not report success", async ({ page, request, watch }) => {
  const id = await create(request, "/invoke/Shop/nap/send");
  await status(request, id, (s) => s === "suspended");
  await page.route(/\/invocations\/[0-9a-f]+\/cancel/, (r) =>
    r.fulfill({
      status: 403,
      contentType: "application/json",
      body: JSON.stringify({ error: "role" }),
    }),
  );
  await open(page, id);
  await menu(page, "Cancel (cooperative)");
  const dialog = page.getByRole("dialog", { name: "Cancel (cooperative)" });
  await dialog.getByRole("button", { name: "Cancel (cooperative)" }).click();
  await expect(dialog.getByRole("alert")).toHaveText("Your key's role cannot do this.");
  await page.unroute(/\/invocations\/[0-9a-f]+\/cancel/);
  await page.route(/\/invocations\/[0-9a-f]+\/cancel/, (r) =>
    r.fulfill({
      status: 429,
      contentType: "application/json",
      body: JSON.stringify({ error: "busy" }),
    }),
  );
  await dialog.getByRole("button", { name: "Cancel (cooperative)" }).click();
  await expect(dialog.getByRole("alert")).toHaveText(/busy \(429\)/);
  await expect(page.getByText("Cancelled", { exact: true })).toHaveCount(0);
  await clean(page, watch, [/status of 403/, /status of 429/]);
});

test("503 not the leader: the message names the node that leads the range", async ({
  page,
  request,
  watch,
}) => {
  const id = await create(request, "/invoke/Shop/nap/send");
  await status(request, id, (s) => s === "suspended");
  await page.route(/\/invocations\/[0-9a-f]+\/cancel/, (r) =>
    r.fulfill({
      status: 503,
      contentType: "application/json",
      body: JSON.stringify({
        error: "this node is not the leader of partition 2; the leader is at 10.9.8.7:8080",
        leader: "10.9.8.7:8080",
        id,
      }),
    }),
  );
  await open(page, id);
  await menu(page, "Cancel (cooperative)");
  const dialog = page.getByRole("dialog", { name: "Cancel (cooperative)" });
  await dialog.getByRole("button", { name: "Cancel (cooperative)" }).click();
  const alert = dialog.getByRole("alert");
  await expect(alert).toContainText("led by another node (10.9.8.7:8080)");
  await expect(alert).not.toContainText("partition");
  await expect(page.getByText("Cancelled", { exact: true })).toHaveCount(0);
  await clean(page, watch, [/status of 503/]);
});

// Workflow `Review/run` waits for signal `approve` ⇒ send it from the
// "Actions" menu (name + JSON value) ⇒ the invocation continues and returns that exact value.
test("named signal: workflow waits for a signal ⇒ send from the menu ⇒ continues to completed", async ({
  page,
  request,
  watch,
}) => {
  const key = `rv-${Date.now()}`;
  const id = await create(request, `/invoke/Review/run/send?key=${key}`);
  await status(request, id, (s) => s === "suspended");
  await open(page, id);
  await menu(page, "Send signal…");
  const dialog = page.getByRole("dialog", { name: "Send signal…" });
  const send = dialog.getByRole("button", { name: "Send" });
  // The name is required; a value that is not JSON cannot be sent yet.
  await expect(send).toBeDisabled();
  await dialog.getByLabel("Signal name").fill("approve");
  await dialog.getByLabel("Value (JSON, optional)").fill("{oops");
  await expect(dialog.getByText("Not valid JSON.")).toBeVisible();
  await expect(send).toBeDisabled();
  await dialog.getByLabel("Value (JSON, optional)").fill('{"by": "e2e"}');
  await axe(page);
  await send.click();
  await expect(page.getByText(/^Sent signal approve — now /)).toBeVisible();
  await expect(dialog).toHaveCount(0);
  await status(request, id, (s) => s === "completed");
  const out = await request.get(`/workflow/Review/${key}/output`);
  const done = (await out.json()) as { output: string };
  expect(JSON.parse(done.output)).toEqual({ approved: { by: "e2e" } });
  // The page refreshes itself: the header reaches completed.
  await expect(page.getByText("completed").first()).toBeVisible();
  await clean(page, watch);
});

test("live log: invocation still alive ⇒ SSE, log lines appear, no polling", async ({
  page,
  request,
  watch,
}) => {
  const id = await create(request, "/invoke/Shop/napLog/send");
  await status(request, id, (s) => s === "suspended");
  await page.goto(`/ui/invocations/${id}?tab=log`);
  await expect(page.getByRole("status").filter({ hasText: /^Live/ })).toBeVisible();
  await expect(page.getByText("napping")).toBeVisible();
  await expect(page.getByText(/^Polling mode/)).toHaveCount(0);
  await request.post(`/invocations/${id}/cancel`);
  await clean(page, watch);
});
