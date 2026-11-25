// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Publishes `@cairnflow/sdk` to npm.
 *
 * **Dry run** by default (`npm publish --dry-run`): prints exactly the tarball that
 * would be published, sends nothing. Only `--yes` publishes for real. Checks before
 * publishing:
 *
 * 1. `version` of the form `X.Y.Z-beta.N`, `publishConfig` is `{access: "public", tag:
 *    "beta"}` — a beta release never becomes `latest`.
 * 2. The `sdk/typescript` tree is clean in git (the release matches a commit).
 * 3. `tsc --noEmit` and the tarball smoke test (`scripts/smoke-pack.ts`, needs the
 *    `cairnflow` binary; `--skip-smoke` skips it when it already ran in the full check).
 * 4. Provenance: can only be produced in CI with OIDC (GitHub Actions, public repo).
 *    Outside CI you must say `--no-provenance` explicitly; otherwise it stops.
 *
 * `node scripts/publish.ts [--yes] [--skip-smoke] [--no-provenance]` (from `sdk/typescript`).
 * The person running it has done `npm login` to the `cairnflow` org (2FA).
 */

import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const sdkDir = resolve(fileURLToPath(new URL(".", import.meta.url)), "..");
const args = new Set(process.argv.slice(2));
const real = args.has("--yes");
const inCi = process.env.GITHUB_ACTIONS === "true";

function fail(msg: string): never {
  console.error(`publish: ${msg}`);
  process.exit(1);
}

const pkg = JSON.parse(readFileSync(join(sdkDir, "package.json"), "utf8")) as {
  name: string;
  version: string;
  license: string;
  publishConfig?: { access?: string; tag?: string };
};
if (!/^\d+\.\d+\.\d+-beta\.\d+$/.test(pkg.version)) {
  fail(`version ${pkg.version} is not X.Y.Z-beta.N — beta releases only for now`);
}
if (pkg.publishConfig?.access !== "public" || pkg.publishConfig?.tag !== "beta") {
  fail(`publishConfig must be {"access": "public", "tag": "beta"}, found ${JSON.stringify(pkg.publishConfig)}`);
}
if (pkg.license !== "Apache-2.0") fail(`license must be Apache-2.0, found ${pkg.license}`);

const dirty = execFileSync("git", ["status", "--porcelain", "--", "."], { cwd: sdkDir, encoding: "utf8" }).trim();
if (dirty && real) fail(`sdk/typescript has uncommitted changes:\n${dirty}`);

if (!inCi && !args.has("--no-provenance")) {
  fail("provenance needs CI with OIDC (GitHub Actions on a public repo); pass --no-provenance to publish without it");
}

const run = (cmd: string, argv: string[]) => {
  console.log(`$ ${cmd} ${argv.join(" ")}`);
  execFileSync(cmd, argv, { cwd: sdkDir, stdio: "inherit" });
};
run(join(sdkDir, "node_modules", ".bin", "tsc"), ["--noEmit"]);
if (!args.has("--skip-smoke")) run(process.execPath, [join(sdkDir, "scripts", "smoke-pack.ts")]);

const publish = ["publish", "--access", "public", "--tag", "beta"];
if (inCi) publish.push("--provenance");
if (!real) publish.push("--dry-run");
run("npm", publish);
console.log(
  real
    ? `published ${pkg.name}@${pkg.version} with dist-tag beta`
    : `dry run of ${pkg.name}@${pkg.version} done — rerun with --yes to publish`,
);
