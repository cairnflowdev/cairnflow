// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import type { APIRequestContext, Page } from "@playwright/test";
import { axe, clean, expect, test } from "./fixtures";
import { E2E_ADMIN_KEY } from "./ports";
import { seed } from "./seed";

// "Safe deploy" on the `auth` node — an empty node, so
// migrating and removing deployments does not touch the `noauth` node's sample data. Runs after
// `auth.spec.ts` (its name sorts later), that case needs a node with no deployment yet.

const auth = { authorization: `Bearer ${E2E_ADMIN_KEY}` };

async function login(page: Page) {
  await page.goto("/ui/login");
  await page.getByLabel("API key").fill(E2E_ADMIN_KEY);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByRole("heading", { level: 1 })).toBeVisible();
  // The Overview still has queries in flight ⇒ the next `goto` cancels them; WebKit reports a fetch
  // cancelled on unload as a `pageerror` "access control checks" (like smoke).
  await page.waitForLoadState("networkidle");
}

async function api<T>(
  request: APIRequestContext,
  method: "GET" | "POST" | "PUT",
  path: string,
  data?: unknown,
) {
  const r = await request.fetch(path, { method, headers: auth, data });
  expect(r.ok(), `${method} ${path}: ${r.status()} ${await r.text()}`).toBe(true);
  return (await r.json()) as T;
}

const currentOf = async (request: APIRequestContext, service: string) =>
  (await api<{ name: string; deploymentId: string }[]>(request, "GET", "/services")).find(
    (s) => s.name === service,
  )?.deploymentId;

async function stage(page: Page): Promise<string> {
  await page.goto("/ui/deployments");
  await page.getByRole("link", { name: "Deploy safely…" }).click();
  await page.getByLabel("Endpoint URI of the new version").fill(seed().sdk);
  await page.getByRole("button", { name: "Register without moving" }).click();
  await expect(page).toHaveURL(/[?&]d=dp_/);
  return new URL(page.url()).searchParams.get("d") as string;
}

test("register without moving ⇒ diff, trial replay, move, migrate, remove the old version (type to confirm)", async ({
  page,
  request,
  watch,
}) => {
  const old = (await api<{ id: string }>(request, "POST", "/deployments", { uri: seed().sdk })).id;
  const r = await request.post("/invoke/Shop/nap/send", { headers: auth, data: null });
  const napping = r.headers()["x-cairn-invocation-id"] as string;
  await expect
    .poll(
      async () => (await api<{ status: string }>(request, "GET", `/invocations/${napping}`)).status,
    )
    .toBe("suspended");

  await login(page);
  const next = await stage(page);
  // Not moved yet: new invocations still go to the old version.
  expect(await currentOf(request, "Shop")).toBe(old);
  const diff = page.getByRole("region", { name: "1. What changes" });
  await expect(diff.getByRole("heading", { name: /^Shop/ })).toContainText(
    "no change to handlers or contract",
  );
  await axe(page);

  const replay = page.getByRole("region", { name: "2. Replay check" });
  await replay.getByRole("button", { name: "Run replay check" }).click();
  await expect(replay.getByText(/^Shop: 1 of 1 match/)).toBeVisible();

  const move = page.getByRole("region", { name: "3. Move the revision" });
  await move.getByRole("button", { name: /^Move \d+ services$/ }).click();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: /^Move \d+ services$/ })
    .click();
  await expect(move.getByText("Shop: moved")).toBeVisible();
  expect(await currentOf(request, "Shop")).toBe(next);

  const drain = page.getByRole("region", { name: "4. Drain the old version" });
  await expect(drain.getByText("1 unfinished invocations still run on it")).toBeVisible();
  await drain.getByRole("button", { name: "Migrate the ones that replay cleanly" }).click();
  await expect(drain.getByText("Migrated 1; 0 stay (replay differs or busy)")).toBeVisible();
  await expect(drain.getByText(/^Drained/)).toBeVisible({ timeout: 15_000 });

  await drain.getByRole("button", { name: "Remove old version" }).click();
  const dialog = page.getByRole("dialog");
  const go = dialog.getByRole("button", { name: "Remove old version" });
  await expect(go).toBeDisabled();
  await dialog.getByLabel(`Type ${old.slice(-8)} to confirm`).fill(old.slice(-8));
  await go.click();
  await expect(drain.getByText(`Removed ${old}`)).toBeVisible();
  expect((await request.get(`/deployments/${old}`, { headers: auth })).status()).toBe(404);
  await axe(page);
  // The Deployments list sees the new version, and no longer the removed one.
  await page.goto("/ui/deployments");
  await expect(page.getByRole("row", { name: new RegExp(next) })).toBeVisible();
  await expect(page.getByRole("row", { name: new RegExp(old) })).toHaveCount(0);
  await clean(page, watch);
});

test("someone else moved first ⇒ 409 shows the server's message; one-button rollback", async ({
  page,
  request,
  watch,
}) => {
  await login(page);
  const before = await currentOf(request, "Shop");
  const next = await stage(page);
  // Someone else moves Shop to a third version while we are looking at the page.
  const other = (
    await api<{ id: string }>(request, "POST", "/deployments?activate=false", { uri: seed().sdk })
  ).id;
  await api(request, "PUT", "/services/Shop/revision", { deployment: other, expect: before });

  const move = page.getByRole("region", { name: "3. Move the revision" });
  await move.getByRole("button", { name: /^Move \d+ services$/ }).click();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: /^Move \d+ services$/ })
    .click();
  await expect(move.getByText(/^Shop: Not possible right now: /)).toBeVisible();
  expect(await currentOf(request, "Shop")).toBe(other);
  // Other services already moved ⇒ roll them back to the old version, conditionally.
  expect(await currentOf(request, "Cart")).toBe(next);
  const drain = page.getByRole("region", { name: "4. Drain the old version" });
  await drain.getByRole("button", { name: "Roll back" }).click();
  await page.getByRole("dialog").getByRole("button", { name: "Roll back" }).click();
  await expect(page.getByText("Rolled back")).toBeVisible();
  expect(await currentOf(request, "Cart")).toBe(before);
  await clean(page, watch, [/status of 409/]);
});
