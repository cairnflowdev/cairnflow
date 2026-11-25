// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Every screen × phone/desktop size × two themes: the page does not
// overflow horizontally, axe clean (with `color-contrast` on the real layout), no console errors,
// clean CSP. The route list is read from the generated route tree (`src/routeTree.gen.ts`) —
// new screens join automatically; screens with parameters take data from `routes.ts` (missing ⇒ red, says so).
import { axe, clean, expect, test } from "./fixtures";
import { concrete, ROUTES } from "./routes";

const VARIANTS = [
  { name: "375-light", width: 375, height: 812, colorScheme: "light" },
  { name: "375-dark", width: 375, height: 812, colorScheme: "dark" },
  { name: "1280-light", width: 1280, height: 800, colorScheme: "light" },
  { name: "1280-dark", width: 1280, height: 800, colorScheme: "dark" },
] as const;

test("the route tree has the console's screens", () => {
  expect(ROUTES).toContain("/");
  expect(ROUTES).toContain("/invocations/$id");
  expect(ROUTES.length).toBeGreaterThan(15);
});

for (const v of VARIANTS) {
  test.describe(v.name, () => {
    test.use({ colorScheme: v.colorScheme, viewport: { width: v.width, height: v.height } });
    for (const route of ROUTES) {
      test(route, async ({ page, watch }) => {
        const path = await concrete(page, route);
        await page.goto(`/ui${path}`);
        await expect(page.getByRole("heading", { level: 1 }).first()).toBeVisible();
        await expect(page.locator("main [aria-busy='true']")).toHaveCount(0);
        await page.waitForLoadState("networkidle");
        // The console shell scrolls inside `main` (the page does not scroll): horizontal overflow is measured on both `main` and the page —
        // overflow inside `main` is text/cells sticking out past the right edge with no page scrollbar.
        // Dedicated horizontal scroll frames (tables, tab strips) are fine: elements inside them do not count.
        const spill = await page.evaluate(() => {
          const main = document.querySelector("main");
          const right = Math.min(
            window.innerWidth,
            main?.getBoundingClientRect().right ?? Infinity,
          );
          const inFrame = (el: Element) => {
            for (let p = el.parentElement; p && p !== main; p = p.parentElement) {
              const o = getComputedStyle(p).overflowX;
              if (o === "auto" || o === "scroll" || o === "hidden") return true;
            }
            return false;
          };
          const page = document.documentElement.scrollWidth - document.documentElement.clientWidth;
          const inMain = main ? main.scrollWidth - main.clientWidth : 0;
          const culprits = [...document.querySelectorAll("main *")]
            .filter((el) => el.getBoundingClientRect().right > right + 1 && !inFrame(el))
            .filter(
              (el) => ![...el.children].some((c) => c.getBoundingClientRect().right > right + 1),
            )
            .slice(0, 5)
            .map(
              (el) =>
                `<${el.tagName.toLowerCase()} class="${el.getAttribute("class")}">${(el.textContent ?? "").slice(0, 30)}`,
            );
          return { px: Math.max(page, inMain), culprits };
        });
        expect(
          spill.px,
          `horizontal overflow ${spill.px} px — ${spill.culprits.join(" ")}`,
        ).toBeLessThanOrEqual(0);
        expect(spill.culprits, "elements sticking out past the right edge").toEqual([]);
        await axe(page);
        await clean(page, watch);
      });
    }
  });
}

// On narrow screens each table row becomes a two-row card — the status column
// (header "Status", "State", "Outcome") is in the viewport right away, without scrolling the
// table frame horizontally to see it (item A of the 2026-11-02 screenshot review).
const STATUS_HEAD = /^(Status|State|Outcome)$/;
/** Screens certain to have a table with a status column and rows (sample data of the global setup). */
const HAS_STATUS_ROWS = new Set(["/invocations/", "/deployments/"]);

test.describe("375 px: the status column of every table is in the viewport", () => {
  test.use({ viewport: { width: 375, height: 812 } });
  for (const route of ROUTES) {
    test(route, async ({ page }) => {
      const path = await concrete(page, route);
      await page.goto(`/ui${path}`);
      await expect(page.getByRole("heading", { level: 1 }).first()).toBeVisible();
      await expect(page.locator("main [aria-busy='true']")).toHaveCount(0);
      await page.waitForLoadState("networkidle");
      const out = await page.evaluate((src) => {
        const head = new RegExp(src);
        const main = document.querySelector("main")?.getBoundingClientRect();
        const bad: string[] = [];
        let seen = 0;
        for (const table of document.querySelectorAll("main table")) {
          const cols = [...table.querySelectorAll("thead th")]
            .map((h, i) => (head.test((h.textContent ?? "").trim()) ? i : -1))
            .filter((i) => i >= 0);
          if (cols.length === 0) continue;
          const frame = (table.parentElement ?? table).getBoundingClientRect();
          const left = Math.max(0, frame.left, main?.left ?? 0);
          const right = Math.min(window.innerWidth, frame.right, main?.right ?? Infinity);
          for (const tr of [...table.querySelectorAll("tbody tr")].slice(0, 10)) {
            for (const i of cols) {
              const r = tr.children[i]?.getBoundingClientRect();
              if (!r || r.width === 0) continue;
              seen++;
              if (r.left < left - 1 || r.right > right + 1) {
                bad.push(
                  `column ${i}: ${Math.round(r.left)}–${Math.round(r.right)} outside ${Math.round(left)}–${Math.round(right)}`,
                );
              }
            }
          }
        }
        return { seen, bad: bad.slice(0, 5) };
      }, STATUS_HEAD.source);
      expect(out.bad, "status cells outside the viewport at 375 px").toEqual([]);
      if (HAS_STATUS_ROWS.has(route)) expect(out.seen, "no status cell seen").toBeGreaterThan(0);
    });
  }
});
