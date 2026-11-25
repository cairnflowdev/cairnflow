// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Console E2E on the real binary. The build (`npm run build`)
// must exist before building `cairnflow` — the `cairn-ui` crate embeds `ui/dist` at compile time.
//
// `CAIRN_E2E_NOAUTH_ARGS`: extra arguments for the `noauth` node — the cluster row of
// `check.sh` runs the whole suite with `--cluster --partitions 4`.
//
// Three nodes: `noauth` (`--no-auth`, with the sample data of `e2e/global-setup.ts`),
// `auth` (bootstrap key `E2E_ADMIN_KEY`, environment label `prod-e2e`, empty — onboarding)
// and `sso` (bootstrap key, OIDC client `localhost` → tenant `acme` with the fake issuer
// `e2e/fixture/oidc.ts`). The browser reaches the `sso` node via
// `localhost`: the `__Host-` cookie carries `Secure`, and Chromium accepts `Secure` over
// http only on `localhost`.
// `CAIRN_BIN` (default `../target/debug/cairnflow`), `CAIRN_E2E_PORT`
// (default 18480; the auth node on port + 2; the internal port is port + 1).
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { defineConfig, devices } from "@playwright/test";

import { E2E_ADMIN_KEY, E2E_AUTH_PORT, E2E_PORT, E2E_SSO_PORT } from "./e2e/ports";

const port = E2E_PORT;
const authPort = E2E_AUTH_PORT;
const bin = process.env.CAIRN_BIN ?? "../target/debug/cairnflow";
const dir = (name: string) => mkdtempSync(join(tmpdir(), `cairn-ui-e2e-${name}-`));

const server = (p: number, args: string, env: Record<string, string> = {}) => ({
  command: `${bin} --data-dir ${dir(String(p))} --port ${p} --internal-port ${p + 1} ${args}`,
  url: `http://127.0.0.1:${p}/health`,
  timeout: 60_000,
  reuseExistingServer: false,
  stdout: "ignore" as const,
  stderr: "pipe" as const,
  env,
});

// WebKit: only when `CAIRN_E2E_WEBKIT_WS` is set — the browser runs
// in a container (`scripts/e2e-webkit.sh`), because Playwright's WebKit build does not
// run on every distro. Small scope: CSP and the session cookie.
const ws = process.env.CAIRN_E2E_WEBKIT_WS;
const webkit = ws
  ? [
      {
        name: "webkit-noauth",
        // SQL console: CodeMirror in a shadow root (constructed stylesheets) must pass CSP
        // on WebKit too.
        testMatch: /(smoke|query)\.spec\.ts/,
        use: {
          ...devices["Desktop Safari"],
          baseURL: `http://127.0.0.1:${port}`,
          connectOptions: { wsEndpoint: ws },
        },
      },
      {
        name: "webkit-auth",
        testMatch: /auth\.spec\.ts/,
        use: {
          ...devices["Desktop Safari"],
          baseURL: `http://127.0.0.1:${authPort}`,
          connectOptions: { wsEndpoint: ws },
        },
      },
    ]
  : [];

// Firefox: only when `CAIRN_E2E_FIREFOX_WS` is set — the browser runs in a
// container like WebKit (`scripts/e2e-container.sh firefox`; Playwright's Firefox build does not
// start on macOS 27). Scope like WebKit plus the main journey: smoke (CSP), the main
// screens (list, detail, axe), SQL console, sign-in with a key.
const ffWs = process.env.CAIRN_E2E_FIREFOX_WS;
const firefox = ffWs
  ? [
      {
        name: "firefox-noauth",
        testMatch: /(smoke|screens|query)\.spec\.ts/,
        use: {
          ...devices["Desktop Firefox"],
          baseURL: `http://127.0.0.1:${port}`,
          connectOptions: { wsEndpoint: ffWs },
        },
      },
      {
        name: "firefox-auth",
        testMatch: /(^|\/)auth\.spec\.ts/,
        use: {
          ...devices["Desktop Firefox"],
          baseURL: `http://127.0.0.1:${authPort}`,
          connectOptions: { wsEndpoint: ffWs },
        },
      },
    ]
  : [];

export default defineConfig({
  testDir: "./e2e",
  // One results directory per run, by port: `check.sh` runs three runs in parallel
  // (Chromium, cluster, WebKit) in the same `ui/`, and Playwright deletes `outputDir` at
  // startup — sharing it would let one run delete the other's traces (ENOENT).
  outputDir: `test-results/${port}`,
  globalSetup: "./e2e/global-setup.ts",
  timeout: 30_000,
  fullyParallel: false,
  // One worker: every test shares the two nodes, and each node accepts only 4 concurrent
  // `/query` requests (429 after that). One console tab keeps ≤ 2 in flight;
  // three parallel workers exceed the cap — that is test load, not user
  // load.
  workers: 1,
  reporter: [["list"]],
  use: { trace: "retain-on-failure" },
  projects: [
    ...webkit,
    ...firefox,
    {
      name: "noauth",
      // `twonode/`: the separate two-node row (`playwright.twonode.config.ts`);
      // `shots/`: screenshots for review (`playwright.screens.config.ts`).
      testIgnore: [/auth\.spec\.ts/, /sso\.spec\.ts/, /twonode\//, /shots\//],
      use: { ...devices["Desktop Chrome"], baseURL: `http://127.0.0.1:${port}` },
    },
    {
      name: "auth",
      testMatch: /auth\.spec\.ts/,
      use: { ...devices["Desktop Chrome"], baseURL: `http://127.0.0.1:${authPort}` },
    },
    {
      name: "sso",
      testMatch: /sso\.spec\.ts/,
      use: { ...devices["Desktop Chrome"], baseURL: `http://localhost:${E2E_SSO_PORT}` },
    },
  ],
  webServer: [
    server(port, `--no-auth ${process.env.CAIRN_E2E_NOAUTH_ARGS ?? ""}`.trim()),
    // The auth node carries an environment label — `auth.spec.ts` checks the badge.
    server(authPort, "--ui-env-label prod-e2e", { CAIRNFLOW_ADMIN_KEY: E2E_ADMIN_KEY }),
    server(E2E_SSO_PORT, "--ui-env-label staging-e2e", { CAIRNFLOW_ADMIN_KEY: E2E_ADMIN_KEY }),
  ],
});
