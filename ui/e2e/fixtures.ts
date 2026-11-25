// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Shared e2e fixture: every page records CSP violations, cross-origin
// requests, console errors, and **every API request of the console** — it must have a path
// in `docs/api/openapi.json` and a response matching the schema in strict mode
// (the same checker as the TS SDK's e2e). Every test checks them at the end (`clean`).
import AxeBuilder from "@axe-core/playwright";
import { test as base, expect, type Page } from "@playwright/test";
import { ApiSchema } from "../../sdk/typescript/test/api-schema.ts";

type Watch = {
  csp: string[];
  foreign: string[];
  errors: string[];
  api: string[];
  pending: Promise<void>[];
};

const schema = new ApiSchema();

/** UI paths, not API: assets under the base path and the `<base>/session` session. */
const UI_PATH = /^\/ui(\/|$)/;

export const test = base.extend<{ watch: Watch }>({
  watch: async ({ page, baseURL }, use) => {
    const w: Watch = { csp: [], foreign: [], errors: [], api: [], pending: [] };
    const origin = new URL(baseURL ?? "http://127.0.0.1").origin;
    page.on("response", (r) => {
      const u = new URL(r.url());
      const method = r.request().method();
      if (
        u.origin !== origin ||
        UI_PATH.test(u.pathname) ||
        r.request().resourceType() === "document"
      )
        return;
      if (!schema.route(method, u.pathname)) {
        w.api.push(`${method} ${u.pathname}: not in openapi.json`);
        return;
      }
      w.pending.push(
        (async () => {
          // The page navigated away before the body arrived: Playwright no longer holds the body
          // (error, or hangs never returning) — nothing to check, not a
          // schema mismatch. A 5 s limit for both headers and body.
          const got = await Promise.race([
            Promise.all([r.allHeaders(), r.text()]).catch(() => null),
            new Promise<null>((done) => setTimeout(() => done(null), 5000)),
          ]);
          if (got === null) return;
          const [h, text] = got;
          const errs = schema.check(
            method,
            r.url(),
            r.status(),
            { get: (n) => h[n.toLowerCase()] ?? null },
            text,
          );
          if (errs?.length) w.api.push(...errs);
        })(),
      );
    });
    await page.addInitScript(() => {
      document.addEventListener("securitypolicyviolation", (e) => {
        (window as unknown as { __csp: string[] }).__csp ??= [];
        (window as unknown as { __csp: string[] }).__csp.push(
          `${e.violatedDirective} ${e.blockedURI}`,
        );
      });
    });
    page.on("request", (r) => {
      const u = new URL(r.url());
      if (u.protocol.startsWith("http") && u.origin !== origin) w.foreign.push(r.url());
    });
    page.on("console", (m) => {
      if (m.type() === "error") w.errors.push(m.text());
    });
    page.on("pageerror", (e) => w.errors.push(e.message));
    await use(w);
  },
});

/**
 * Clean CSP, no outgoing requests, no console errors. `expected`: console errors
 * the test causes on purpose (e.g. the `401` of `whoami` before sign-in — the browser
 * always prints a line for each 4xx response); each pattern must match at least one line.
 */
export async function clean(page: Page, w: Watch, expected: RegExp[] = []) {
  const csp = await page.evaluate(() => (window as unknown as { __csp?: string[] }).__csp ?? []);
  expect(csp, "securitypolicyviolation").toEqual([]);
  expect(w.foreign, "cross-origin request").toEqual([]);
  await Promise.all(w.pending);
  expect(w.api, "API request diverges from openapi.json").toEqual([]);
  // Firefox prints no console line for a 4xx response of `fetch` (Chromium and WebKit do) — there
  // expected errors are only filtered, not required.
  const logsHttpErrors = page.context().browser()?.browserType().name() !== "firefox";
  for (const re of logsHttpErrors ? expected : [])
    expect(
      w.errors.some((e) => re.test(e)),
      `expected error ${re}`,
    ).toBe(true);
  expect(
    w.errors.filter((e) => !expected.some((re) => re.test(e))),
    "console errors",
  ).toEqual([]);
}

export { expect };

/**
 * The Mod key by the **browser's** platform (Meta on macOS, Control elsewhere). Playwright's `ControlOrMeta`
 * picks by the machine running the test — wrong when the browser runs in a Linux container
 * (WebKit, Firefox: `scripts/e2e-container.sh`).
 */
export async function mod(page: Page): Promise<"Meta" | "Control"> {
  return (await page.evaluate(() => /Mac/.test(navigator.platform))) ? "Meta" : "Control";
}

/** axe on the current page (WCAG 2.0–2.2 A/AA tags). */
export async function axe(page: Page) {
  // Measuring colours while a dialog is fading gives wrong contrast — wait for finite
  // animations to finish. Infinitely repeating animations ("running" dot, skeleton) never finish.
  await page.waitForFunction(() =>
    document
      .getAnimations()
      .every(
        (a) =>
          a.playState !== "running" ||
          a.effect?.getTiming().iterations === Number.POSITIVE_INFINITY,
      ),
  );
  const res = await new AxeBuilder({ page })
    .withTags(["wcag2a", "wcag2aa", "wcag21aa", "wcag22aa"])
    .analyze();
  expect(
    res.violations.map(
      (v) =>
        `${v.id}: ${v.help} (${v.nodes.length}) — ${v.nodes
          .slice(0, 3)
          .map((n) => n.target.join(" "))
          .join(", ")}`,
    ),
  ).toEqual([]);
}
