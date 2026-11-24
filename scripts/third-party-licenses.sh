#!/usr/bin/env bash
# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: BUSL-1.1
# Generates THIRD_PARTY_LICENSES: the license texts of everything shipped in the
# cairnflow binary, the container image and the console bundle.
#
#   scripts/third-party-licenses.sh [OUT]      (default: target/THIRD_PARTY_LICENSES)
#
# Two parts:
#   1. Rust crates linked into the binaries — `cargo about` (about.toml, about.hbs).
#   2. npm packages in the console bundle (`ui/`, production dependencies only,
#      including the OFL-1.1 fonts) — license files read from ui/node_modules,
#      so `npm ci` must have run in ui/ first.
# The project's own NOTICE is prepended. The SDK (`sdk/typescript`) has no runtime
# dependencies, so nothing of it is listed.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${1:-$ROOT/target/THIRD_PARTY_LICENSES}"
cd "$ROOT"

command -v cargo-about >/dev/null 2>&1 || {
  echo "cargo-about not found — brew install cargo-about, or cargo install cargo-about --locked" >&2
  exit 1
}
[ -d ui/node_modules ] || {
  echo "ui/node_modules missing — run 'npm ci' in ui/ first" >&2
  exit 1
}

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

cargo about generate --fail about.hbs -o "$tmp/rust.txt"

{
  echo "THIRD-PARTY SOFTWARE LICENSES"
  echo
  echo "This file lists the third-party software included in Cairnflow binaries,"
  echo "container images and the web console, with their licenses."
  echo
  cat NOTICE
  echo
  echo "################################################################################"
  echo "# Part 1 — Rust crates"
  echo "################################################################################"
  cat "$tmp/rust.txt"
  echo
  echo "################################################################################"
  echo "# Part 2 — npm packages in the web console bundle"
  echo "################################################################################"
  node scripts/npm-licenses.ts --prod-list ui/package-lock.json | sort -u |
    while IFS=$'\t' read -r name version license; do
      dir="ui/node_modules/$name"
      echo
      echo "================================================================================"
      echo "$name $version ($license)"
      echo
      found=0
      for f in "$dir"/LICENSE* "$dir"/LICENCE* "$dir"/license* "$dir"/COPYING* "$dir"/OFL*; do
        [ -f "$f" ] || continue
        cat "$f"
        echo
        found=1
        break
      done
      if [ "$found" = 0 ]; then
        author="$(node -e 'const p=require(process.argv[1]);const a=p.author;console.log(typeof a==="string"?a:(a&&a.name)||"the "+p.name+" authors")' "$ROOT/$dir/package.json")"
        echo "The package ships no license file; package.json declares: $license"
        if [ "$license" = MIT ]; then
          echo
          echo "Copyright (c) $author"
          echo
          sed -n '/^Permission is hereby granted/,$p' "$ROOT/ui/node_modules/@base-ui/react/LICENSE"
        fi
      fi
    done
} >"$OUT.tmp"
mv "$OUT.tmp" "$OUT"
echo "wrote $OUT ($(wc -l <"$OUT" | tr -d ' ') lines)"
