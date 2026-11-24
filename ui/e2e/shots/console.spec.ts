// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Screenshots for the console's design review: every screen ×
// variant, plus the states the sample data does not produce by itself — empty (the empty `auth` node), loading
// (holding `/query` requests), errors (missing ids), long text (long service/handler/error names), Vietnamese.
// Images at `SCREENS_DIR/<variant>/<group>-<screen>.png`; no pixel comparison.
import { type Page, test } from "@playwright/test";
import { serve, service, TerminalError } from "../../../sdk/typescript/src/index.ts";
import { E2E_ADMIN_KEY, E2E_AUTH_PORT, E2E_PORT } from "../ports";
import { concrete, ROUTES, settled } from "../routes";

const OUT = process.env.SCREENS_DIR ?? "../target/screens/console";
const BASE = `http://127.0.0.1:${E2E_PORT}`;

const VARIANTS = [
  { name: "375-light", width: 375, height: 812, colorScheme: "light", locale: "en" },
  { name: "375-dark", width: 375, height: 812, colorScheme: "dark", locale: "en" },
  { name: "1280-light", width: 1280, height: 800, colorScheme: "light", locale: "en" },
  { name: "1280-dark", width: 1280, height: 800, colorScheme: "dark", locale: "en" },
  { name: "375-light-vi", width: 375, height: 812, colorScheme: "light", locale: "vi" },
] as const;

const LONG_SERVICE = "InvoiceReconciliationForEnterpriseCustomers";
const LONG_HANDLER = "reconcileOutstandingInvoicesAcrossAllRegions";
let longFailed = "";

const slug = (route: string) => (route === "/" ? "overview" : route.slice(1).replace(/[/$]/g, "_"));

async function send(path: string, body: unknown = null): Promise<string> {
  const r = await fetch(`${BASE}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  if (r.status >= 300) throw new Error(`POST ${path}: ${r.status}`);
  return r.headers.get("x-cairn-invocation-id") ?? "";
}

test.beforeAll(async () => {
  // A service with a long name: columns, chips and titles must cope (the "long text" checklist item).
  const long = service(LONG_SERVICE, {
    async [LONG_HANDLER](_ctx, input: { n: number }) {
      if (input.n % 2) {
        throw new TerminalError(
          "upstream ledger rejected the batch: invoice INV-2026-000184213 references a closed fiscal period (FY2025-Q4) and cannot be reconciled without a manual adjustment entry",
          422,
        );
      }
      return { reconciled: 1_234_567, amount: 98_765_432.1 };
    },
  });
  const ep = await serve({ services: [long], port: 0, hostname: "127.0.0.1" });
  const reg = await fetch(`${BASE}/deployments`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ uri: ep.url }),
  });
  if (!reg.ok) throw new Error(`registering the long-named service: ${reg.status}`);
  for (let n = 0; n < 6; n++) {
    const id = await send(`/invoke/${LONG_SERVICE}/${LONG_HANDLER}/send`, { n });
    if (n % 2) longFailed = id;
  }
  await new Promise((r) => setTimeout(r, 2000));
});

/**
 * The whole screen, not just the viewport: the console shell scrolls inside `main` (the page does not scroll), so `fullPage`
 * sees only the top. Stretch the viewport to the height of `main`'s content, capture, then restore it.
 */
async function shot(page: Page, dir: string, name: string) {
  await page.waitForTimeout(250);
  const size = page.viewportSize();
  const need = await page.evaluate(() => {
    const main = document.querySelector("main");
    return main ? main.getBoundingClientRect().top + main.scrollHeight : 0;
  });
  if (size && need > size.height) {
    await page.setViewportSize({ width: size.width, height: Math.min(Math.ceil(need), 8000) });
    await page.waitForTimeout(150);
  }
  await page.screenshot({ path: `${OUT}/${dir}/${name}.png`, fullPage: true });
  if (size) await page.setViewportSize(size);
}

for (const v of VARIANTS) {
  test(v.name, async ({ browser }) => {
    const ctx = await browser.newContext({
      viewport: { width: v.width, height: v.height },
      colorScheme: v.colorScheme,
      baseURL: BASE,
    });
    if (v.locale === "vi") {
      await ctx.addInitScript(() => localStorage.setItem("PARAGLIDE_LOCALE", "vi"));
    }
    const page = await ctx.newPage();

    for (const route of ROUTES) {
      await page.goto(`/ui${await concrete(page, route)}`);
      await settled(page);
      await shot(page, v.name, `screen-${slug(route)}`);
    }

    // Long text: list filtered by the long service, detail with a long error, Services.
    await page.goto(`/ui/invocations?service=${LONG_SERVICE}`);
    await settled(page);
    await shot(page, v.name, "long-invocations");
    await page.goto(`/ui/invocations/${longFailed}`);
    await settled(page);
    await shot(page, v.name, "long-invocation-failed");

    // Errors: malformed id, well-formed id that does not exist, missing deployment.
    await page.goto("/ui/invocations/not-an-id");
    await settled(page);
    await shot(page, v.name, "error-invocation-bad-id");
    await page.goto(`/ui/invocations/${"0".repeat(48)}`);
    await settled(page);
    await shot(page, v.name, "error-invocation-missing");
    await page.goto("/ui/deployments/dp_0000000000000000000000000000");
    await settled(page);
    await shot(page, v.name, "error-deployment-missing");

    // Loading: hold every `/query` of the Overview.
    await page.route("**/query", () => {});
    await page.goto("/ui/");
    await page.getByRole("heading", { level: 1 }).first().waitFor({ timeout: 15_000 });
    await shot(page, v.name, "loading-overview");
    await page.unroute("**/query");
    await ctx.close();

    // Empty: the `auth` node has no deployment (signed in with the bootstrap key).
    const empty = await browser.newContext({
      viewport: { width: v.width, height: v.height },
      colorScheme: v.colorScheme,
      baseURL: `http://127.0.0.1:${E2E_AUTH_PORT}`,
    });
    if (v.locale === "vi") {
      await empty.addInitScript(() => localStorage.setItem("PARAGLIDE_LOCALE", "vi"));
    }
    const p = await empty.newPage();
    await p.goto("/ui/login");
    await shot(p, v.name, "empty-login");
    await p.getByLabel(/API key|Khoá API/).fill(E2E_ADMIN_KEY);
    await p.getByRole("button", { name: /Sign in|Đăng nhập/ }).click();
    // Capture only after sign-in: an earlier run captured 18 identical shots of the sign-in screen.
    await p.waitForURL((u) => !u.pathname.endsWith("/login"), { timeout: 15_000 });
    await settled(p);
    for (const route of ROUTES.filter((r) => !r.includes("$"))) {
      await p.goto(`/ui${route}`);
      await settled(p);
      await shot(p, v.name, `empty-${slug(route)}`);
    }
    await empty.close();
  });
}
