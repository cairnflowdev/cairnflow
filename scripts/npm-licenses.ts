#!/usr/bin/env node
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// License gate for npm dependencies, read from a package-lock.json (v3).
//
//   node scripts/npm-licenses.ts ui/package-lock.json sdk/typescript/package-lock.json harness/package-lock.json
//
// Every package in the lockfile (dependencies and devDependencies alike: dev
// tools run in this repo's own checks) must carry a license from the allow list
// below. Some licenses are allowed only for dev packages (they never ship in the
// console bundle, the binary or the npm SDK). Packages whose lockfile entry has
// no license field are allowed only through EXCEPTIONS, with the license read
// from the package's own files and the reason written down.
//
// `--prod-list` prints the production packages (name, version, license) of each
// lockfile instead; scripts/third-party-licenses.sh uses it to collect the
// license texts of what goes into the console bundle.

import { readFileSync } from "node:fs";

/** Allowed for any package. */
const ALLOW = new Set([
  "MIT",
  "ISC",
  "Apache-2.0",
  "Apache-2.0 WITH LLVM-exception",
  "BSD-2-Clause",
  "BSD-3-Clause",
  "0BSD",
  "Unlicense",
  "CC0-1.0",
  "BlueOak-1.0.0",
  // Fonts embedded in the console bundle (and so in the binary). OFL allows
  // bundling with software; it requires shipping the license text (done by
  // THIRD_PARTY_LICENSES) and forbids selling the fonts on their own.
  "OFL-1.1",
]);

/** Allowed only for dev packages. */
const ALLOW_DEV = new Set([
  // File-level copyleft; used unmodified as build/test tools (lightningcss, axe-core).
  "MPL-2.0",
  // Browser-usage data (caniuse-lite), used by build tooling only.
  "CC-BY-4.0",
]);

/** Packages without a license field in the lockfile: license read from the package. */
const EXCEPTIONS: Record<string, string> = {
  "@inlang/plugin-message-format": "MIT (LICENSE file in the package, Copyright (c) 2024 Opral US Inc.)",
};

/** `A OR B` passes when any branch passes; `(A)` is unwrapped. */
function allowed(expr: string, dev: boolean): boolean {
  const e = expr.trim().replace(/^\((.*)\)$/, "$1");
  if (/ OR /.test(e)) return e.split(/ OR /).some((b) => allowed(b, dev));
  if (/ AND /.test(e)) return e.split(/ AND /).every((b) => allowed(b, dev));
  return ALLOW.has(e) || (dev && ALLOW_DEV.has(e));
}

type Entry = { version?: string; license?: string; dev?: boolean; devOptional?: boolean; link?: boolean };

function packages(lockPath: string): Array<[string, Entry]> {
  const lock = JSON.parse(readFileSync(lockPath, "utf8")) as { packages?: Record<string, Entry> };
  if (!lock.packages) throw new Error(`${lockPath}: not a lockfile v2/v3 (no "packages")`);
  return Object.entries(lock.packages)
    .filter(([path, e]) => path !== "" && !e.link)
    .map(([path, e]) => [path.replace(/^.*node_modules\//, ""), e]);
}

function main(): void {
  const args = process.argv.slice(2);
  const prodList = args[0] === "--prod-list";
  const files = prodList ? args.slice(1) : args;
  if (files.length === 0) {
    console.error("usage: npm-licenses.ts [--prod-list] <package-lock.json>...");
    process.exit(2);
  }
  let bad = 0;
  for (const file of files) {
    let checked = 0;
    for (const [name, e] of packages(file)) {
      const dev = e.dev === true || e.devOptional === true;
      if (prodList) {
        if (!dev) console.log(`${name}\t${e.version ?? "?"}\t${e.license ?? EXCEPTIONS[name] ?? "UNKNOWN"}`);
        continue;
      }
      checked++;
      if (e.license === undefined) {
        if (EXCEPTIONS[name] === undefined) {
          console.error(`${file}: ${name}@${e.version}: no license field and no entry in EXCEPTIONS`);
          bad++;
        }
        continue;
      }
      if (!allowed(e.license, dev)) {
        console.error(`${file}: ${name}@${e.version}: license "${e.license}" not allowed${dev ? " (dev)" : " (prod)"}`);
        bad++;
      }
    }
    if (!prodList) console.log(`${file}: ${checked} packages checked`);
  }
  if (bad > 0) {
    console.error(`npm license gate: ${bad} problem(s)`);
    process.exit(1);
  }
}

main();
