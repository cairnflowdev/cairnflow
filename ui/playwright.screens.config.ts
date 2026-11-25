// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Screenshots for design review: not part of Ready, no pixel comparison,
// images do not go into git. Same three nodes and sample data as e2e (`playwright.config.ts`); each
// variant (375/1280 × light/dark, plus 375 in Vietnamese) captures every screen, empty states (the empty `auth`
// node), loading, errors, and long text/numbers into `SCREENS_DIR` (default `../target/screens/console`).
//
//   npm run build && cargo build -p cairn-server && npx playwright test -c playwright.screens.config.ts
import { defineConfig, devices } from "@playwright/test";
import { E2E_PORT } from "./e2e/ports";
import base from "./playwright.config";

export default defineConfig({
  ...base,
  testDir: "./e2e/shots",
  outputDir: `test-results/shots-${E2E_PORT}`,
  timeout: 900_000,
  projects: [
    {
      name: "shots",
      use: { ...devices["Desktop Chrome"], baseURL: `http://127.0.0.1:${E2E_PORT}` },
    },
  ],
});
