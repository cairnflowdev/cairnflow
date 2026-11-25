// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Console on a **two-node cluster sharing one bucket**: open the console on
// one node, act on an invocation the other node leads — it must succeed thanks to
// forwarding (`x-cairn-forward: leader`), not `503 leader`.
//
// The two processes need a shared bucket: `CAIRN_E2E_S3_URI` (e.g. `s3://walbench`,
// the SeaweedFS of `scripts/dev-store/up.sh`) plus `AWS_*` in the environment —
// `check.sh` runs this row under the same condition as the "real S3" row. Each run gets its own prefix
// under that URI. `CAIRN_E2E_PORT` (default 18560): node a on that port, node b on
// port + 2, the internal port is port + 1.
import { defineConfig, devices } from "@playwright/test";

import { TWONODE_A, TWONODE_B } from "./e2e/twonode/ports";

const bin = process.env.CAIRN_BIN ?? "../target/debug/cairnflow";
const s3 = (process.env.CAIRN_E2E_S3_URI ?? "").replace(/\/$/, "");
if (!s3)
  throw new Error("playwright.twonode.config.ts needs CAIRN_E2E_S3_URI (a shared S3 bucket)");
// One prefix per run — the config is reloaded in each worker, so take it from the environment once set.
process.env.CAIRN_E2E_TWONODE_RUN ??= String(Date.now());
const prefix = `${s3}/ui-twonode-${process.env.CAIRN_E2E_TWONODE_RUN}`;
const env = Object.fromEntries(
  Object.entries(process.env).filter((e): e is [string, string] => e[1] !== undefined),
);

const node = (name: string, p: number) => ({
  command:
    `${bin} --main-store ${prefix} --cluster --partitions 2 --node-id ui-${name} ` +
    `--port ${p} --internal-port ${p + 1} --no-auth --tick-interval-ms 100`,
  url: `http://127.0.0.1:${p}/health`,
  timeout: 60_000,
  reuseExistingServer: false,
  stdout: "ignore" as const,
  stderr: "pipe" as const,
  env,
});

export default defineConfig({
  testDir: "./e2e/twonode",
  outputDir: `test-results/twonode-${TWONODE_A}`,
  globalSetup: "./e2e/twonode/setup.ts",
  timeout: 60_000,
  workers: 1,
  reporter: [["list"]],
  use: { trace: "retain-on-failure" },
  // Which node the console is on is up to the global setup (the node that does **not** lead the
  // sample invocation's range): each project skips itself when it is not the one.
  projects: [
    {
      name: "console-a",
      use: { ...devices["Desktop Chrome"], baseURL: `http://127.0.0.1:${TWONODE_A}` },
    },
    {
      name: "console-b",
      use: { ...devices["Desktop Chrome"], baseURL: `http://127.0.0.1:${TWONODE_B}` },
    },
  ],
  webServer: [node("a", TWONODE_A), node("b", TWONODE_B)],
});
