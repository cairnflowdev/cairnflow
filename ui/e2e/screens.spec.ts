// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { axe, clean, expect, test } from "./fixtures";
import { seed } from "./seed";

// Phase-1 screens on the `noauth` node with sample data (global setup).

/**
 * The `journey_find_failure` journey: from the Overview, find a
 * `failed` invocation and read its last error in ≤ 3 clicks.
 */
test("journey_find_failure", async ({ page, watch }) => {
  await page.goto("/ui/");
  await expect(page.getByRole("heading", { name: "Overview", level: 1 })).toBeVisible();
  let clicks = 0;
  await page
    .getByRole("link", { name: /^Failed/ })
    .first()
    .click();
  clicks++;
  await expect(page).toHaveURL(/view=failed/);
  await page
    .getByRole("row", { name: /Shop\/decline/ })
    .getByRole("link")
    .first()
    .click();
  clicks++;
  await expect(page.getByRole("region", { name: "Why it is in this state" })).toContainText(
    "card declined",
  );
  expect(clicks).toBeLessThanOrEqual(3);
  await clean(page, watch);
});

test("Overview: KPIs, by service, needs attention", async ({ page, watch }) => {
  await page.goto("/ui/");
  // The action cases (actions.spec) create more invocations — do not count exact numbers.
  await expect(page.getByRole("link", { name: /^Paused: [1-9]/ })).toBeVisible();
  const byService = page.getByRole("region", { name: "By service" });
  await expect(byService.getByRole("link", { name: "Shop", exact: true })).toBeVisible();
  await expect(page.getByRole("region", { name: "Busiest handlers" })).toBeVisible();
  await expect(page.getByRole("link", { name: /Paused, waiting for a resume/ })).toBeVisible();
  await clean(page, watch);
});

test("Invocations: views, filter chips, quick look, search by id", async ({ page, watch }) => {
  const s = seed();
  await page.goto("/ui/invocations?view=paused");
  await expect(page.getByRole("row", { name: new RegExp(s.paused) })).toBeVisible();
  await expect(page.getByRole("row", { name: /Shop\/ok/ })).toHaveCount(0);

  await page.goto("/ui/invocations");
  // Linear-style filter: field ⇒ values suggested from /services; type to filter the list.
  await page.getByRole("button", { name: "Filter", exact: true }).click();
  await page.getByRole("option", { name: "Service" }).click();
  await page.getByLabel("Filter value").fill("ca");
  await page.getByRole("option", { name: "Cart" }).click();
  await expect(page).toHaveURL(/service=Cart/);
  await expect(page.getByRole("row", { name: /Cart\/hold/ })).toHaveCount(2);
  // Chip: click the value to change it, type a value that is not among the suggestions.
  await page.getByRole("button", { name: "Service is Cart, change" }).click();
  await page.getByLabel("Filter value").fill("Nope");
  await page.keyboard.press("Enter");
  await expect(page).toHaveURL(/service=Nope/);
  await page.getByRole("button", { name: "Service is Nope, change" }).click();
  await page.getByLabel("Filter value").fill("Cart");
  await page.getByRole("option", { name: "Cart" }).click();
  await expect(page).toHaveURL(/service=Cart/);
  await page.getByRole("button", { name: "Remove filter Service" }).click();
  await expect(page).not.toHaveURL(/service=/);

  // Long list (data for measuring INP): filter down to the exact handler first.
  await page.goto("/ui/invocations?service=Shop&handler=nap");
  await page
    .getByRole("row", { name: new RegExp(s.sleeping) })
    .getByRole("cell")
    // Column 0 is the checkbox, 1 is the id (link + copy button) — click the "Target" cell.
    .nth(2)
    .click();
  const peek = page.getByRole("complementary", { name: "Invocation" });
  await expect(peek).toContainText("Shop/nap");

  await page.goto("/ui/invocations");
  await page.getByLabel("Invocation ID or prefix").fill(s.failed.slice(0, 12));
  await page.getByLabel("Invocation ID or prefix").press("Enter");
  await expect(page.getByRole("row")).toHaveCount(2);
  await clean(page, watch);
});

test("Detail: error, paused, what it waits on, two-level journal, log, tree, state", async ({
  page,
  watch,
}) => {
  const s = seed();
  await page.goto(`/ui/invocations/${s.failed}`);
  const why = page.getByRole("region", { name: "Why it is in this state" });
  await expect(why).toContainText("card declined");
  // "failed at entry N (name)" when there is an unfinished command, otherwise "after entry N" — never
  // the Output entry carrying that same error. The link opens the right entry.
  await why.getByRole("link", { name: /^(at|after) entry \d+/ }).click();
  await expect(page).toHaveURL(/[?&]entry=\d+/);

  await page.goto(`/ui/invocations/${s.paused}`);
  await expect(page.getByText(/Paused after running out of attempts/)).toBeVisible();
  // The first line of the error in the banner; the stack (which also contains the message) is collapsed below.
  await expect(page.getByRole("region", { name: "Why it is in this state" })).toContainText(
    "payment gateway timeout",
  );

  await page.goto(`/ui/invocations/${s.sleeping}`);
  await expect(page.getByText(/Sleeping until/)).toBeVisible();

  await page.goto(`/ui/invocations/${s.promise}`);
  await expect(page.getByText("Waiting for promise approved")).toBeVisible();

  await page.goto(`/ui/invocations/${s.completed}`);
  await page.getByRole("button", { name: /^\s*0\s*Input/ }).click();
  await expect(page.getByText('"amount": 42')).toBeVisible();
  await page.getByRole("link", { name: "Log", exact: true }).click();
  await expect(page.getByText("reserving")).toBeVisible();

  await page.goto(`/ui/invocations/${s.parent}?tab=tree`);
  await expect(page.getByText("Shop/ok")).toBeVisible();

  await page.goto(`/ui/invocations/${s.holder}?tab=state`);
  await expect(page.getByText("items")).toBeVisible();
  await clean(page, watch);
});

test("Services and Deployments", async ({ page, watch }) => {
  await page.goto("/ui/services");
  await expect(page.getByRole("heading", { name: "Shop" })).toBeVisible();
  await expect(page.getByRole("link", { name: "flaky" })).toBeVisible();
  await page.goto("/ui/deployments");
  const link = page.getByRole("link", { name: /dp_/ }).first();
  await link.click();
  await expect(page.getByText("Pinned invocations")).toBeVisible();
  await clean(page, watch);
});

for (const scheme of ["light", "dark"] as const) {
  test(`axe clean on the phase-1 screens — ${scheme}`, async ({ browser, browserName }) => {
    // 15 screens × axe: Firefox in a container runs axe slower than Chromium (20–35 s) — triple the limit.
    test.slow(browserName === "firefox", "axe on Firefox in a container is slower");
    const s = seed();
    const ctx = await browser.newContext({ colorScheme: scheme });
    const page = await ctx.newPage();
    for (const path of [
      "/ui/",
      "/ui/invocations",
      `/ui/invocations?view=failed&peek=${s.failed}`,
      `/ui/invocations/${s.failed}`,
      `/ui/invocations/${s.completed}?entry=0`,
      `/ui/invocations/${s.completed}?tab=log`,
      `/ui/invocations/${s.parent}?tab=tree`,
      `/ui/invocations/${s.holder}?tab=state`,
      `/ui/invocations/${s.completed}?tab=artifacts`,
      // Queue banner, timeline, raw timestamps, streams.
      `/ui/invocations/${s.queued}`,
      `/ui/invocations/${s.traced}?tab=trace`,
      `/ui/invocations/${s.completed}?tab=trace`,
      `/ui/invocations/${s.streamed}?tab=streams`,
      "/ui/services",
      "/ui/deployments",
    ]) {
      await page.goto(path);
      await expect(page.locator("main [aria-busy]")).toHaveCount(0);
      await axe(page);
    }
    await ctx.close();
  });
}

test("Trace: span waterfall of an invocation (custom spans + turn/run spans), click ⇒ attributes", async ({
  page,
  request,
  watch,
}) => {
  const r = await request.post("/invoke/Shop/traced/send", { data: null });
  const id = r.headers()["x-cairn-invocation-id"] as string;
  await expect
    .poll(
      async () =>
        ((await (await request.get(`/invocations/${id}`)).json()) as { status: string }).status,
    )
    .toBe("completed");
  await page.goto(`/ui/invocations/${id}?tab=trace`);
  const trace = page.getByRole("region", { name: "Trace" });
  await expect(trace.getByText(/^Spans: \d+ · /)).toBeVisible();
  await expect(trace.getByText("Tokens: 120 in · 8 out")).toBeVisible();
  await trace.getByRole("button", { name: /^plan/ }).click();
  await expect(trace.getByText(/"model": "m-1"/)).toBeVisible();
  await axe(page);
  await clean(page, watch);
});

// The timeline in the Trace tab. `Shop/traced` (`trace: "runtime"`)
// fails its first attempt (run `llm` throws a transient error) then succeeds on the next: two lanes, the first
// failed, the retry wait in between; run `llm` links to the journal's Run entry, selecting
// one highlights the other. A handler without runtime trace (`Shop/ok`) ⇒ raw timestamps + a hint.
test("Trace — timeline by attempt: failed lane, retry wait, run ↔ entry; no runtime trace ⇒ hint", async ({
  page,
  request,
  watch,
}) => {
  const r = await request.post("/invoke/Shop/traced/send", { data: null });
  const id = r.headers()["x-cairn-invocation-id"] as string;
  await expect
    .poll(
      async () =>
        ((await (await request.get(`/invocations/${id}`)).json()) as { status: string }).status,
      { timeout: 20_000 },
    )
    .toBe("completed");
  await page.goto(`/ui/invocations/${id}?tab=trace`);
  const tl = page.getByRole("region", { name: "Timeline by attempt" });
  await expect(tl.getByText(/^Attempts: 2 · failed: 1 · /)).toBeVisible();
  await expect(tl.getByText(/^Attempt 1/)).toContainText("failed");
  await expect(tl.getByText(/^Attempt 2/)).toContainText("ok");
  await expect(tl.getByText("Waiting to retry")).toBeVisible();
  const run = tl.getByRole("button", { name: /^llm · did not finish · entry 1/ });
  const step = tl.getByRole("button", { name: /^#1 Run “llm”/ });
  await expect(step).toHaveAttribute("aria-pressed", "false");
  await run.click();
  await expect(run).toHaveAttribute("aria-pressed", "true");
  await expect(step).toHaveAttribute("aria-pressed", "true");
  await step.click();
  await expect(run).toHaveAttribute("aria-pressed", "false");
  await axe(page);

  await page.goto(`/ui/invocations/${seed().completed}?tab=trace`);
  const plain = page.getByRole("region", { name: "Timeline by attempt" });
  await expect(plain).toContainText('Turn on trace: "runtime" for this handler');
  const marks = plain.getByRole("list", { name: "Milestones" });
  await expect(marks).toContainText("created");
  await expect(marks).toContainText("ended");
  await axe(page);
  await clean(page, watch);
});

// Streams tab — the user streams `tokens` (and `meta`) of `Shop/streamed` (durable
// records, by offset); `$log`/`$trace` do not mix in. An invocation without streams
// ⇒ empty state.
test("Streams: durable records of ctx.stream, by offset; no streams ⇒ empty", async ({
  page,
  request,
  watch,
}) => {
  const r = await request.post("/invoke/Shop/streamed/send", { data: null });
  const id = r.headers()["x-cairn-invocation-id"] as string;
  await expect
    .poll(
      async () =>
        ((await (await request.get(`/invocations/${id}`)).json()) as { status: string }).status,
    )
    .toBe("completed");
  await page.goto(`/ui/invocations/${id}`);
  await page.getByRole("link", { name: "Streams" }).click();
  const tab = page.getByRole("region", { name: "Streams" });
  await expect(tab.getByText(/^Durable streams only/)).toBeVisible();
  await tab.getByRole("group", { name: "Stream" }).getByRole("button", { name: "tokens" }).click();
  const records = tab.getByRole("list", { name: "Records of tokens" });
  await expect(records.getByRole("listitem")).toHaveCount(3);
  await expect(records.getByRole("listitem").nth(0)).toContainText('"hello"');
  await expect(records.getByRole("listitem").nth(1)).toContainText('{"n":2}');
  await expect(tab.getByText("Records: 3")).toBeVisible();
  await expect(tab.getByText("$log")).toHaveCount(0);
  await axe(page);

  await page.goto(`/ui/invocations/${seed().completed}?tab=streams`);
  await expect(page.getByRole("region", { name: "Streams" }).getByText("No streams")).toBeVisible();
  await clean(page, watch);
});

// The stream picker buttons fit their content, they do not stretch across the tab (a user saw it in
// the demo: the `answer`/`sources` strip spanning the whole screen). Picking the other stream ⇒ its records.
test("Streams: picker buttons fit their content; picking a stream ⇒ that stream's records", async ({
  page,
  watch,
}) => {
  await page.goto(`/ui/invocations/${seed().streamed}?tab=streams`);
  const tab = page.getByRole("region", { name: "Streams" });
  const picker = tab.getByRole("group", { name: "Stream" });
  await expect(picker.getByRole("button")).toHaveCount(2);
  const [p, t] = [await picker.boundingBox(), await tab.boundingBox()];
  expect(p && t && p.width < t.width / 2, `picker width ${p?.width} / tab ${t?.width}`).toBe(true);
  await picker.getByRole("button", { name: "meta" }).click();
  await expect(
    tab.getByRole("list", { name: "Records of meta" }).getByRole("listitem"),
  ).toHaveCount(1);
  await clean(page, watch);
});

// Logs read in order need absolute times; the detail's tab strip
// must not widen the page on a phone.
test("Log: absolute time, level filter, correct mode text once ended", async ({ page, watch }) => {
  const s = seed();
  await page.goto(`/ui/invocations/${s.completed}?tab=log`);
  const log = page.getByRole("region", { name: "Log" });
  await expect(log.getByText("reserving")).toBeVisible();
  await expect(log.locator("time").first()).toHaveText(/^\d{2}:\d{2}:\d{2}\.\d{3}$/);
  await expect(log.getByText(/refreshed every 2 s/)).toHaveCount(0);
  await log.getByRole("group", { name: "Level" }).getByRole("button", { name: "Errors" }).click();
  await expect(log.getByText("reserving")).toHaveCount(0);
  await log.getByRole("checkbox", { name: "Wrap lines" }).uncheck();
  await clean(page, watch);
});

test("phone 375 px: invocation detail does not overflow horizontally", async ({ page, watch }) => {
  await page.setViewportSize({ width: 375, height: 812 });
  const s = seed();
  await page.goto(`/ui/invocations/${s.failed}`);
  await expect(page.getByRole("navigation", { name: "Invocation sections" })).toBeVisible();
  const overflow = await page.locator("main").evaluate((m) => m.scrollWidth - m.clientWidth);
  expect(overflow).toBeLessThanOrEqual(0);
  await clean(page, watch);
});

// Invocations list.
test("Overview KPI and the list it opens count the same set", async ({ page, watch }) => {
  await page.goto("/ui/");
  const kpi = page.getByRole("link", { name: /^Failed: \d/ }).first();
  const n = Number(((await kpi.getAttribute("aria-label")) ?? "").replace(/\D/g, ""));
  await kpi.click();
  await expect(page.getByText(new RegExp(`^${n} results?$|^1 result$`))).toBeVisible();
  // The last-error column of a failed row is not empty.
  await expect(page.getByRole("row", { name: /Shop\/decline/ }).first()).toContainText(
    "card declined",
  );
  await clean(page, watch);
});

test("peek below 1024 px is a sheet; Esc closes; opening/closing peek adds no history", async ({
  page,
  watch,
}) => {
  const s = seed();
  await page.setViewportSize({ width: 900, height: 800 });
  await page.goto("/ui/");
  // Wait for the Overview to finish loading before moving on: Firefox cancels the next `goto` (NS_BINDING_ABORTED) when
  // the previous page still has queries in flight.
  await expect(page.getByRole("heading", { name: "Overview", level: 1 })).toBeVisible();
  await page.waitForLoadState("networkidle");
  await page.goto("/ui/invocations?service=Shop&handler=nap");
  await page
    .getByRole("row", { name: new RegExp(s.sleeping) })
    .getByRole("cell")
    .nth(2)
    .click();
  const sheet = page.getByRole("dialog", { name: "Invocation" });
  await expect(sheet).toContainText("Shop/nap");
  await page.keyboard.press("Escape");
  await expect(sheet).toHaveCount(0);
  await expect(page).not.toHaveURL(/peek=/);
  await page.goBack();
  await expect(page).toHaveURL(/\/ui\/?$/);
  await clean(page, watch);
});

test("J/K/Enter keys on the list; Esc closes the panel on wide screens", async ({
  page,
  request,
  watch,
}) => {
  // Needs ≥ 2 rows even when this file runs alone.
  const tag = `keys=${Date.now()}`;
  for (let i = 0; i < 2; i++)
    await request.post("/invoke/Shop/ok/send", { data: null, headers: { "cairnflow-tag": tag } });
  await page.setViewportSize({ width: 1400, height: 900 });
  await page.goto(`/ui/invocations?tag=${encodeURIComponent(tag)}`);
  await expect(page.locator("tbody tr")).toHaveCount(2);
  await page.locator("main").click({ position: { x: 5, y: 5 } });
  await page.keyboard.press("j");
  const first = page.locator("tbody tr").first();
  await expect(first).toBeFocused();
  await page.keyboard.press("j");
  await expect(page.locator("tbody tr").nth(1)).toBeFocused();
  await page.keyboard.press("k");
  await expect(first).toBeFocused();
  await page.keyboard.press(" ");
  await expect(page.getByRole("complementary", { name: "Invocation" })).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(page.getByRole("complementary", { name: "Invocation" })).toHaveCount(0);
  await first.focus();
  await page.keyboard.press("Enter");
  await expect(page).toHaveURL(/\/ui\/invocations\/[0-9a-f]{48}/);
  await clean(page, watch);
});

test("the list does not jump by itself: new calls ⇒ banner, click reloads", async ({
  page,
  request,
  watch,
}) => {
  const tag = `fresh=${Date.now()}`;
  await request.post("/invoke/Shop/ok/send", { data: null, headers: { "cairnflow-tag": tag } });
  await page.goto(`/ui/invocations?tag=${encodeURIComponent(tag)}`);
  await expect(page.locator("tbody tr")).toHaveCount(1);
  await expect(page.getByText(/^Updated/)).toBeVisible();
  await request.post("/invoke/Shop/ok/send", { data: null, headers: { "cairnflow-tag": tag } });
  const banner = page.getByRole("button", { name: /1 new — show/ });
  await expect(banner).toBeVisible({ timeout: 20_000 });
  await expect(page.locator("tbody tr")).toHaveCount(1);
  await banner.click();
  await expect(page.locator("tbody tr")).toHaveCount(2);
  await clean(page, watch);
});

test("the 'Created after' filter", async ({ page, watch }) => {
  await page.goto("/ui/invocations");
  await page.getByRole("button", { name: "Filter", exact: true }).click();
  await page.getByRole("option", { name: "Created after" }).click();
  await page.getByRole("option", { name: "Last hour" }).click();
  await expect(page).toHaveURL(/from=\d+/);
  await expect(page.getByRole("button", { name: /^Created after is .*, change$/ })).toBeVisible();
  await page.getByRole("button", { name: "Remove filter Created" }).click();
  await expect(page).not.toHaveURL(/from=/);
  await clean(page, watch);
});

// Overview chart, Deployments status, Usage pricing.
test("Overview has a 24 h chart; Deployments has a status column; Usage does not print $0.00 when no price is set", async ({
  page,
  watch,
}) => {
  await page.goto("/ui/");
  await expect(
    page.getByRole("region", { name: "Completed and failed, last 24 hours" }),
  ).toBeVisible();
  await page.goto("/ui/deployments");
  await expect(page.getByRole("columnheader", { name: "Status" })).toBeVisible();
  await expect(page.getByRole("cell", { name: "Current" }).first()).toBeVisible();
  await page.goto("/ui/usage");
  await expect(page.getByText("No price set")).toBeVisible();
  await expect(page.getByText("$0.00")).toHaveCount(0);
  await clean(page, watch);
});
