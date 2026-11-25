// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import type { Page } from "@playwright/test";
import { axe, clean, expect, test } from "./fixtures";
import { E2E_ADMIN_KEY } from "./ports";

// Settings on the `auth` node: the key secret lifecycle is real —
// a new secret can call the API, does not stay in the URL/storage/dialog, and once revoked
// it is refused. A separate tenant (`e2e-set`) so as not to touch the other cases.

async function login(page: Page) {
  await page.goto("/ui/login");
  await page.getByLabel("API key").fill(E2E_ADMIN_KEY);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByRole("heading", { level: 1 })).toBeVisible();
  await page.waitForLoadState("networkidle");
}

test("API key: the secret is shown once and works; revoke by typing the id ⇒ refused", async ({
  page,
  request,
  watch,
}) => {
  await login(page);
  await page.goto("/ui/settings");
  const keys = page.getByRole("region", { name: "API keys" });
  await keys.getByRole("button", { name: "New key" }).click();
  const dialog = page.getByRole("dialog");
  await dialog.getByLabel("Tenant").fill("e2e-set");
  await dialog.getByLabel("Role").selectOption("invoke");
  await dialog.getByRole("button", { name: "Create" }).click();
  await expect(dialog.getByRole("alert")).toContainText("shown only this once");
  const secret = await dialog.getByLabel("Secret").inputValue();
  expect(secret).toMatch(/^cf_/);
  // The new secret can call the API with the right role.
  const who = await request.get("/auth/whoami", { headers: { authorization: `Bearer ${secret}` } });
  expect(who.status()).toBe(200);
  const body = (await who.json()) as { role: string; tenant: string; capabilities: string[] };
  expect(body.role).toBe("invoke");
  // Capability sets: `invoke` only calls, it does not read tenant data.
  expect(body.capabilities).toEqual(["invoke"]);
  expect(page.url()).not.toContain(secret);
  expect(
    await page.evaluate(() => JSON.stringify({ ...localStorage, ...sessionStorage })),
  ).not.toContain(secret);
  await axe(page);
  await dialog.getByRole("button", { name: "I have stored it" }).click();
  // Reopen: a fresh form, the old secret is gone.
  await keys.getByRole("button", { name: "New key" }).click();
  await expect(page.getByRole("dialog").getByLabel("Secret")).toHaveCount(0);
  await page.getByRole("dialog").getByRole("button", { name: "Go back" }).click();

  const row = keys.getByRole("row", { name: /e2e-set/ }).first();
  const id = (await row.getByRole("cell").first().textContent())?.trim() ?? "";
  await row.getByRole("button", { name: "Revoke" }).click();
  const del = page.getByRole("dialog");
  await expect(del.getByRole("button", { name: "Revoke" })).toBeDisabled();
  await del.getByLabel(`Type ${id} to confirm`).fill(id);
  await del.getByRole("button", { name: "Revoke" }).click();
  await expect(page.getByText(new RegExp(`Revoked ${id}`))).toBeVisible();
  const after = await request.get("/auth/whoami", {
    headers: { authorization: `Bearer ${secret}` },
  });
  expect(after.status()).toBe(401);
  await clean(page, watch);
});

test("tenant: hash chain, slow-run threshold; invocation capture; price table; UI thresholds stored in the browser", async ({
  page,
  request,
  watch,
}) => {
  // A key for `e2e-set` so the tenant shows up in the list.
  const k = await request.post("/admin/keys", {
    headers: { authorization: `Bearer ${E2E_ADMIN_KEY}` },
    data: { tenant: "e2e-set", role: "admin" },
  });
  expect(k.ok()).toBe(true);
  await login(page);
  await page.goto("/ui/settings");
  const tenants = page.getByRole("region", { name: "Tenants" });
  // The box changes when the server confirms (not optimistic) — click and wait.
  const chain = tenants.getByLabel("Hash chain for e2e-set");
  await chain.click();
  await expect(chain).toBeChecked();
  await expect(page.getByText("Saved settings for e2e-set").first()).toBeVisible();
  await tenants.getByLabel("Slow run threshold for e2e-set (ms)").fill("2500");
  await tenants
    .getByRole("row", { name: /e2e-set/ })
    .getByRole("button", { name: "Save", exact: true })
    .click();
  await expect
    .poll(async () => {
      const r = await request.get("/admin/tenants", {
        headers: { authorization: `Bearer ${E2E_ADMIN_KEY}` },
      });
      const v = (await r.json()) as { hashChain: string[]; slowMs: Record<string, number> };
      return [v.hashChain.includes("e2e-set"), v.slowMs["e2e-set"]];
    })
    .toEqual([true, 2500]);

  const cap = page.getByRole("region", { name: "Capture an invocation" });
  await cap.getByLabel("Tenant").fill("e2e-set");
  await cap.getByLabel("Service").fill("Shop");
  await cap.getByLabel("Handler").fill("ok");
  await cap.getByRole("button", { name: "Capture next" }).click();
  await expect(cap.getByText(/Shop\/ok · 1 left/)).toBeVisible();

  await expect(page.getByRole("region", { name: "Token pricing" })).toContainText("No prices set");

  const thr = page.getByRole("region", { name: "Needs-attention thresholds" });
  await thr.getByLabel("Failed rate above (%)").fill("7.5");
  await thr.getByLabel("Once it has at least (runs)").fill("3");
  await thr.getByRole("button", { name: "Save" }).click();
  await page.reload();
  await expect(thr.getByLabel("Failed rate above (%)")).toHaveValue("7.5");
  await expect(thr.getByLabel("Once it has at least (runs)")).toHaveValue("3");
  await axe(page);
  await clean(page, watch);
});

type TenantList = {
  tenants: { tenant: string; quarantineMode?: string | null; quarantineRule?: string | null }[];
  quarantine: Record<string, unknown>;
};

// The tenant's quarantine mode and rule can be changed in Settings. A separate tenant
// (`e2e-q`, with one key so it shows up in the list).
test("tenant: change mode + quarantine rule and read back; bad rule ⇒ 400 shown verbatim", async ({
  page,
  request,
  watch,
}) => {
  const auth = { authorization: `Bearer ${E2E_ADMIN_KEY}` };
  const list = async () =>
    (await (await request.get("/admin/tenants", { headers: auth })).json()) as TenantList;
  const k = await request.post("/admin/keys", {
    headers: auth,
    data: { tenant: "e2e-q", role: "admin" },
  });
  expect(k.ok()).toBe(true);
  await login(page);
  await page.goto("/ui/settings");
  const tenants = page.getByRole("region", { name: "Tenants" });
  const mode = tenants.getByLabel("Quarantine mode for e2e-q");
  const rule = tenants.getByLabel("Quarantine rule for e2e-q (attempts/invocations/seconds)");
  const save = tenants.getByRole("button", { name: "Save quarantine for e2e-q" });
  await expect(mode).toHaveValue("");
  await mode.selectOption("hold");
  await rule.fill("10/3/120");
  await save.click();
  await expect(page.getByText("Saved settings for e2e-q").first()).toBeVisible();
  await expect
    .poll(async () => {
      const t = (await list()).tenants.find((t) => t.tenant === "e2e-q");
      return [t?.quarantineMode, t?.quarantineRule];
    })
    .toEqual(["hold", "10/3/120"]);
  // Reload: the box reads the value the server holds.
  await page.reload();
  await expect(mode).toHaveValue("hold");
  await expect(rule).toHaveValue("10/3/120");

  // A rule with bad syntax: the server returns 400, the message is shown verbatim under the box, the old value stays.
  await rule.fill("abc");
  await save.click();
  await expect(tenants.getByRole("alert")).toContainText('400: quarantineRule "abc"');
  expect((await list()).quarantine["e2e-q"]).toEqual({ mode: "hold", rule: "10/3/120" });
  await axe(page);

  // Back to the node default: pick "node default" again, clear the rule.
  await mode.selectOption("");
  await rule.fill("");
  await save.click();
  await expect.poll(async () => (await list()).quarantine["e2e-q"]).toBeUndefined();
  await clean(page, watch, [/400/]);
});

// The capture form has every field — deployment and duration. A separate tenant
// (`e2e-cap`) so as not to mix with the request of the case above.
test("invocation capture with deployment and duration: shows in the pending list, expires at the right ttl", async ({
  page,
  request,
  watch,
}) => {
  await login(page);
  await page.goto("/ui/settings");
  const cap = page.getByRole("region", { name: "Capture an invocation" });
  await cap.getByLabel("Tenant").fill("e2e-cap");
  await cap.getByLabel("Service").fill("Shop");
  await cap.getByLabel("Handler").fill("decline");
  await cap.getByLabel("Deployment (optional)").fill("dp_e2e_cap");
  await cap.getByLabel("Wait up to").selectOption({ label: "6 hours" });
  await cap.getByLabel("How many").fill("2");
  const sent = Date.now();
  await cap.getByRole("button", { name: "Capture next" }).click();
  await expect(cap.getByText(/Shop\/decline @ dp_e2e_cap · 2 left · expires/)).toBeVisible();
  const r = await request.get("/admin/capture?tenant=e2e-cap", {
    headers: { authorization: `Bearer ${E2E_ADMIN_KEY}` },
  });
  const { captures } = (await r.json()) as {
    captures: { handler: string; deployment?: string; remaining: number; expiresMs: number }[];
  };
  const c = captures.find((c) => c.handler === "decline");
  expect(c?.deployment).toBe("dp_e2e_cap");
  expect(c?.remaining).toBe(2);
  const SIX_H = 6 * 3_600_000;
  expect(c?.expiresMs).toBeGreaterThanOrEqual(sent + SIX_H - 60_000);
  expect(c?.expiresMs).toBeLessThanOrEqual(Date.now() + SIX_H + 60_000);
  await axe(page);
  await clean(page, watch);
});

// The key in use is marked; revoking that very key says clearly that you will be signed out.
test("the signed-in key is marked; revoking it warns about sign-out", async ({ page, watch }) => {
  await login(page);
  await page.goto("/ui/settings");
  const keys = page.getByRole("region", { name: "API keys" });
  await keys.getByRole("button", { name: "New key" }).click();
  const dialog = page.getByRole("dialog");
  await dialog.getByLabel("Tenant").fill("e2e-me");
  await dialog.getByLabel("Role").selectOption("cluster-admin");
  await dialog.getByRole("button", { name: "Create" }).click();
  const secret = await dialog.getByLabel("Secret").inputValue();
  await dialog.getByRole("button", { name: "I have stored it" }).click();
  await page.getByRole("button", { name: "Account" }).click();
  await page.getByRole("menuitem", { name: "Log out" }).click();
  await expect(page).toHaveURL(/\/ui\/login/);
  await page.getByLabel("API key", { exact: true }).fill(secret);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByRole("heading", { level: 1 })).toBeVisible();
  await page.goto("/ui/settings");
  const mine = keys.getByRole("row", { name: /This session/ });
  await expect(mine).toHaveCount(1);
  await mine.getByRole("button", { name: "Revoke" }).click();
  await expect(page.getByRole("dialog")).toContainText("you will be signed out");
  await page.getByRole("dialog").getByRole("button", { name: "Go back" }).click();
  // Signed out mid-case: whoami of the old session is 401.
  await clean(page, watch, [/status of 401/]);
});
