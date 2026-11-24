// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { axe, clean, expect, mod, test } from "./fixtures";

// SQL console on the `noauth` node with sample data: sample queries, editor
// (typing, autocompletion, Mod-Enter), table, bar and line charts, CSV, query errors, and
// `422 query_budget` (faked with `page.route` — the default read cap cannot be reached
// with the sample data). CodeMirror injects styles at runtime: the fixture catches CSP violations.

test("sample query ⇒ table; bar chart; CSV", async ({ page, watch }) => {
  await page.goto("/ui/query");
  await page.getByRole("button", { name: /Handlers that fail most/ }).click();
  await expect(page.getByLabel("SQL")).toContainText("FROM sys_invocation_counts");
  await page.getByRole("button", { name: "Run", exact: true }).click();
  const results = page.getByRole("region", { name: "Results" });
  await expect(results.getByRole("status")).toContainText(/Rows: \d+ · read \d+ · \d+ ms/);
  await expect(results.getByRole("columnheader", { name: "handler" })).toBeVisible();
  await expect(results.getByRole("cell", { name: "decline" })).toBeVisible();
  await axe(page);

  await results.getByRole("button", { name: "Chart" }).click();
  const bar = results.getByRole("button", { name: /^Shop \/ decline: \d/ }).first();
  await bar.focus();
  await expect(results.getByRole("tooltip")).toContainText(/: \d/);
  await axe(page);

  const [download] = await Promise.all([
    page.waitForEvent("download"),
    results.getByRole("link", { name: "Download CSV" }).click(),
  ]);
  expect(download.suggestedFilename()).toBe("query.csv");
  const csv = await download.createReadStream().then(
    (s) =>
      new Promise<string>((res) => {
        let b = "";
        s.on("data", (c: Buffer) => {
          b += c.toString();
        });
        s.on("end", () => res(b));
      }),
  );
  expect(csv.split("\n")[0]).toBe("service,handler,n");
  await clean(page, watch);
});

test("type a query, autocompletion, Mod-Enter; time column ⇒ line chart", async ({
  page,
  watch,
  browserName,
}) => {
  // Known open issue: on Firefox (container)
  // Mod+A right after closing the autocompletion box does not select everything — the text inserted afterwards lands before the old text.
  test.fixme(browserName === "firefox", "Mod+A after Esc of the autocompletion box on Firefox");
  await page.goto("/ui/query");
  const key = await mod(page);
  const editor = page.getByLabel("SQL");
  await editor.click();
  await page.keyboard.press(`${key}+a`);
  await page.keyboard.press("Delete");
  // Type at a human pace: typing in one burst lets the autocompletion box open midway and swallow keys
  // (clearly seen on Firefox: "created_at" became "created_t").
  await page.keyboard.type("SELECT created_at, duration_ms FROM sys_invoc", { delay: 25 });
  await expect(page.getByRole("option", { name: /sys_invocation\b/ }).first()).toBeVisible();
  await page.keyboard.press("Escape");
  // Wait for the autocompletion box to close fully before selecting all: Firefox swallows Ctrl+A while the box is still closing.
  await expect(page.getByRole("listbox")).toHaveCount(0);
  // Insert the rest at once (not key by key): an autocompletion box opening late
  // under load could swallow keys and garble the query.
  await page.keyboard.press(`${key}+a`);
  await page.keyboard.insertText(
    "SELECT created_at, duration_ms FROM sys_invocation WHERE class = 'completed' ORDER BY created_at DESC LIMIT 50",
  );
  await expect(editor).toContainText("LIMIT 50");
  await page.keyboard.press(`${key}+Enter`);
  const results = page.getByRole("region", { name: "Results" });
  await expect(results.getByRole("columnheader", { name: "duration_ms" })).toBeVisible();
  await expect(page).toHaveURL(/[?&]sql=/);
  await results.getByRole("button", { name: "Chart" }).click();
  await expect(
    results.getByRole("img", { name: /Line chart of duration_ms over created_at/ }),
  ).toBeVisible();
  await expect(results.locator("canvas")).toBeVisible();
  await axe(page);
  await clean(page, watch);
});

// The sidebar switches between Samples / Recent / Tables: the console's shared tab strip
// (`components/cairn/tabs.tsx`, like the sections of the invocation detail), not the
// `Segmented` switch (meant for view modes: Table/Chart, time ranges).
test("sidebar: shared tab strip — tab role, arrow keys, named panels", async ({ page, watch }) => {
  await page.goto("/ui/query");
  const list = page.getByRole("tablist", { name: "Samples and tables" });
  const samples = list.getByRole("tab", { name: "Samples" });
  await expect(samples).toHaveAttribute("aria-selected", "true");
  await expect(page.getByRole("tabpanel", { name: "Samples" })).toBeVisible();
  // The long sample list in the sidebar has a height cap: the tab strip is not shrunk and clipped
  // (when shrunk: off by 13 px). 1 px is the tab's `-mb-px` — the underline overlaps the bottom border.
  expect(await list.evaluate((e) => e.scrollHeight - e.clientHeight)).toBeLessThanOrEqual(1);
  await samples.focus();
  await page.keyboard.press("ArrowRight");
  const recent = list.getByRole("tab", { name: "Recent" });
  await expect(recent).toBeFocused();
  await expect(recent).toHaveAttribute("aria-selected", "true");
  await expect(page.getByRole("tabpanel", { name: "Recent" })).toBeVisible();
  await page.keyboard.press("End");
  const tables = list.getByRole("tab", { name: "Tables" });
  await expect(tables).toBeFocused();
  await expect(tables).toHaveAttribute("aria-selected", "true");
  await expect(page.getByRole("tabpanel", { name: "Tables" })).toBeVisible();
  await page.keyboard.press("ArrowRight");
  await expect(samples).toBeFocused();
  await axe(page);
  await clean(page, watch);
});

test("table in the sidebar inserts column names; bad query ⇒ server error; 422 ⇒ LIMIT hint", async ({
  page,
  watch,
}) => {
  await page.goto("/ui/query?sql=SELECT%20nope_column%20FROM%20sys_invocation");
  await page.getByRole("button", { name: "Run", exact: true }).click();
  await expect(page.getByRole("alert")).toBeVisible();

  await page.getByRole("tab", { name: "Tables", exact: true }).click();
  await page.getByLabel("Filter…").fill("p99");
  await page.getByText("sys_handler_stats", { exact: true }).click();
  await page.getByRole("button", { name: /^p99_ms float/ }).click();
  await expect(page.getByLabel("SQL")).toContainText("p99_ms");

  await page.route("**/query", (r) =>
    r.fulfill({
      status: 422,
      contentType: "application/json",
      body: JSON.stringify({ error: "query_budget: query reads over the cap" }),
    }),
  );
  await page.getByRole("button", { name: "Run", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("This query reads too much.");
  await expect(page.getByRole("alert")).toContainText("Add a LIMIT");
  await clean(page, watch, [/status of 400/, /status of 422/]);
});

test("Query stats: Overview and console queries, grouped by query, with PQ numbers", async ({
  page,
  watch,
}) => {
  await page.goto("/ui/");
  await expect(page.getByRole("heading", { name: "Overview", level: 1 })).toBeVisible();
  // Navigate inside the app (no page reload) — the ring buffer lives in the tab.
  await page.getByRole("link", { name: "Query", exact: true }).click();
  await page.getByRole("button", { name: "Run", exact: true }).click();
  await expect(page.getByRole("region", { name: "Results" })).toBeVisible();
  await page.getByRole("link", { name: "Query stats" }).click();
  await expect(page.getByRole("status")).toContainText(
    /\d+ queries · \d+ distinct · read \d+ units · 0 errors/,
  );
  const byQuery = page.getByRole("region", { name: "By query" });
  await expect(
    byQuery.getByRole("link", { name: /FROM sys_invocation_counts/ }).first(),
  ).toBeVisible();
  await expect(byQuery.getByRole("cell", { name: /^1(, |$)/ }).first()).toBeVisible();
  await axe(page);
  await page.getByRole("button", { name: "Clear" }).click();
  await expect(page.getByText("No queries yet in this tab")).toBeVisible();
  await clean(page, watch);
});
