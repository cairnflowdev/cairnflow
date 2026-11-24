// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// The console's screen list for the every-screen suites (`responsive.spec.ts`, screenshots
// `shots/`): read from the generated route tree (`src/routeTree.gen.ts`), so
// new screens join automatically. Screens with parameters are built from the global setup's sample data (`routeParams`).
import { readFileSync } from "node:fs";
import type { Page } from "@playwright/test";
import { seed } from "./seed";

const tree = readFileSync(new URL("../src/routeTree.gen.ts", import.meta.url), "utf8");

/** `fullPath` of every child route of the `_app` layout (the `FileRoutesByPath` block). */
export const ROUTES = [
  ...new Set(
    [
      ...tree.matchAll(/'\/_app\/[^']*': \{\s*id: '[^']*'\s*path: '[^']*'\s*fullPath: '([^']*)'/g),
    ].map((m) => m[1] as string),
  ),
].sort();

/** Parameter values of dynamic routes — built from the global setup's sample data. */
export async function routeParams(page: Page): Promise<Record<string, string>> {
  const s = seed();
  const deps = await page.request.get("/deployments");
  const dep = ((await deps.json()) as { id: string }[])[0]?.id;
  if (!dep) throw new Error("no sample deployment");
  return {
    "/invocations/$id": s.failed,
    "/deployments/$id": dep,
    // The first awakeable of `Shop/waitAwk` (same index as `keyed.spec.ts`).
    "/awakeables/$id": `awk_${s.awakeable}00000011`,
  };
}

/** The real path of `route` (parameters replaced with sample data); missing data ⇒ an error that says so. */
export async function concrete(page: Page, route: string): Promise<string> {
  if (!route.includes("$")) return route;
  const p = (await routeParams(page))[route];
  if (!p)
    throw new Error(`route ${route} has parameters but routeParams has no sample data for it`);
  return route.replace(/\$\w+/, p);
}

/**
 * The screen has finished drawing: it has a title (`h1`; error/not-found pages have only the empty state's `h2`), no
 * loading regions left, network quiet. Each step has a time limit — a page that never goes quiet (live streams)
 * still gets captured.
 */
export async function settled(page: Page) {
  const timeout = 15_000;
  await page.locator("main h1, main h2").first().waitFor({ timeout });
  await page
    .locator("main [aria-busy='true']")
    .first()
    .waitFor({ state: "detached", timeout })
    .catch(() => {});
  await page.waitForLoadState("networkidle", { timeout }).catch(() => {});
}
