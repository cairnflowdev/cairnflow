// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { axe, clean, expect, test } from "./fixtures";

// Shell on a `--no-auth` node: the "auth off" banner, keyboard navigation, Cmd+K
// pasting an id, the `?` sheet. Every route axe clean in both themes, no CSP violations — including
// while the palette (cmdk) is open.
const ID = "3fa9c21b0e6d4a7f9be1d2c3a4b5c6d7e8f9a0b1c2d3ec21";

test("the shell is keyboard navigable", async ({ page, watch }) => {
  await page.goto("/ui/");
  await expect(page.getByText("Auth is off — everyone is cluster-admin")).toBeVisible();
  await expect(page.getByRole("heading", { name: "Overview", level: 1 })).toBeVisible();

  await page.keyboard.press("/");
  const palette = page.getByRole("dialog");
  await expect(palette).toBeVisible();
  await page.keyboard.type("Invocations");
  await page.keyboard.press("Enter");
  await expect(page).toHaveURL(/\/ui\/invocations$/);

  await page.locator("body").click();
  await page.keyboard.press("g");
  await page.keyboard.press("d");
  await expect(page).toHaveURL(/\/ui\/deployments$/);

  await page.keyboard.press("?");
  await expect(page.getByRole("dialog", { name: "Keyboard shortcuts" })).toBeVisible();
  await page.keyboard.press("Escape");

  await page.keyboard.press("ControlOrMeta+k");
  // Paste: `fill` sets the whole string at once like the clipboard.
  await page.getByRole("combobox").fill(ID);
  await expect(page.getByRole("option", { name: `Open invocation ${ID}` })).toBeVisible();
  await page.keyboard.press("Enter");
  await expect(page).toHaveURL(new RegExp(`/ui/invocations/${ID}$`));
  await clean(page, watch);
});

test("unknown route ⇒ in-app 404 page", async ({ page, watch }) => {
  await page.goto("/ui/no-such-page");
  await expect(page.getByRole("heading", { name: "Page not found" })).toBeVisible();
  await clean(page, watch);
});

for (const scheme of ["light", "dark"] as const) {
  test(`axe clean on every route — ${scheme}`, async ({ browser }) => {
    const ctx = await browser.newContext({ colorScheme: scheme });
    const page = await ctx.newPage();
    for (const path of ["/ui/", "/ui/invocations", "/ui/services", "/ui/deployments", "/ui/nope"]) {
      await page.goto(path);
      await expect(page.locator("main")).toBeVisible();
      await axe(page);
    }
    // The 404 sits outside the shell (no shortcut sheet) — the palette opens from the home page.
    await page.goto("/ui/");
    // The key listener is attached once the shell has been built (after `whoami`).
    await expect(page.getByRole("heading", { name: "Overview", level: 1 })).toBeVisible();
    await page.keyboard.press("ControlOrMeta+k");
    await expect(page.getByRole("dialog")).toBeVisible();
    await axe(page);
    await ctx.close();
  });
}

// Command palette.
test("Cmd+K: search service/handler, id prefix, actions on the invocation page", async ({
  page,
  watch,
}) => {
  await page.goto("/ui/");
  await expect(page.getByRole("heading", { name: "Overview", level: 1 })).toBeVisible();
  await page.keyboard.press("ControlOrMeta+k");
  await page.getByRole("combobox").fill("Shop/de");
  await page.getByRole("option", { name: /Shop\/decline/ }).click();
  await expect(page).toHaveURL(/service=Shop&handler=decline|handler=decline&service=Shop/);

  await page.keyboard.press("ControlOrMeta+k");
  await page.getByRole("combobox").fill("3fa9c21b0e6d");
  await page.getByRole("option", { name: /Invocations starting with 3fa9c21b0e6d/ }).click();
  await expect(page).toHaveURL(/id=3fa9c21b0e6d/);

  const s = (await import("./seed")).seed();
  await page.goto(`/ui/invocations/${s.paused}`);
  await expect(page.getByRole("heading", { name: /Shop\// })).toBeVisible();
  await page.keyboard.press("ControlOrMeta+k");
  await expect(page.getByRole("option", { name: "Resume" })).toBeVisible();
  await expect(page.getByRole("option", { name: "Copy ID" })).toBeVisible();
  await page.keyboard.press("Escape");
  await clean(page, watch);
});

test("top bar: cluster version, node count; UTC time zone in the user menu", async ({
  page,
  watch,
}) => {
  await page.goto("/ui/");
  const banner = page.getByRole("banner");
  await expect(banner.getByText(/cluster v\d+/)).toBeVisible();
  await expect(banner.getByRole("link", { name: /\d+ nodes?/ })).toBeVisible();
  await page.getByRole("button", { name: "Account" }).click();
  await page.getByRole("menuitemradio", { name: "UTC" }).click();
  await page.goto("/ui/invocations");
  await expect(page.locator("tbody time").first()).toHaveAttribute("title", /UTC/);
  await page.getByRole("button", { name: "Account" }).click();
  await page.getByRole("menuitemradio", { name: "Browser time zone" }).click();
  await clean(page, watch);
});
