#!/usr/bin/env bash
# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: BUSL-1.1
# A mixed-binary cluster N−1 ↔ N — nightly only, not part of check.sh (the most
# expensive test of the upgrade checks).
#
# Builds the binary at an earlier commit (default `scripts/mixed-binary.base`: the
# revision right before the newest migration step — change it whenever the
# MIGRATIONS registry gets a new entry) with `git worktree` + `cargo build`, builds
# the current binary, then runs `harness/chaos/mixed.ts` on SeaweedFS
# (`localhost:18333`).
#
#   scripts/mixed-binary.sh                 # rev in mixed-binary.base
#   MIXED_OLD_REV=<rev> scripts/mixed-binary.sh
#   MIXED_OLD_REV=previous-tag scripts/mixed-binary.sh   # the `v*` tag before HEAD
#
# `previous-tag` is the default when `scripts/mixed-binary.base` does not exist (the
# public repository): N−1 is the previous release.
# Without any release tag it prints SKIP with the reason and exits 0 (the first
# release has nothing to upgrade from).
#
# Log: target/mixed/mixed.log. Exits non-zero when the scenario fails — a red run
# must be investigated as a real bug, not rerun until green.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
if [ -z "${MIXED_OLD_REV:-}" ] && [ -f scripts/mixed-binary.base ]; then
  OLD_REV=$(tr -d '[:space:]' < scripts/mixed-binary.base)
else
  OLD_REV=${MIXED_OLD_REV:-previous-tag}
fi
if [ "$OLD_REV" = previous-tag ]; then
  if ! OLD_REV=$(git describe --tags --abbrev=0 --match 'v*' HEAD^ 2>/dev/null); then
    echo "SKIP: no release tag before HEAD — the mixed-binary upgrade test starts with the second release"
    exit 0
  fi
fi
OUT=$ROOT/target/mixed
mkdir -p "$OUT"
OLD_SHA=$(git rev-parse --verify "$OLD_REV^{commit}")
WT=$OUT/old-src
OLD_TARGET=$ROOT/target/mixed-old

echo "── old binary: $OLD_REV ($OLD_SHA)"
if [ -d "$WT" ]; then
  git worktree remove --force "$WT" 2>/dev/null || rm -rf "$WT"
fi
git worktree prune
git worktree add --detach "$WT" "$OLD_SHA" >/dev/null
trap 'git worktree remove --force "$WT" >/dev/null 2>&1 || true' EXIT
(cd "$WT" && git -c protocol.file.allow=always submodule update --init --quiet)
(cd "$WT" && CARGO_TARGET_DIR=$OLD_TARGET cargo build --quiet --bin cairnflow)

echo "── new binary: $(git rev-parse --short HEAD)$(git diff --quiet || echo '+dirty')"
cargo build --quiet --bin cairnflow --bin cairn-ctl

if [ -z "${AWS_ENDPOINT:-}" ] && ! curl -s -o /dev/null --max-time 2 http://localhost:18333/; then
  echo "SeaweedFS is not running on localhost:18333 — scripts/dev-store/up.sh" >&2
  exit 2
fi

STORE=${MIXED_STORE_URI:-s3://walbench/mixed-$(date +%s)-$RANDOM}
echo "── store: $STORE"
MIXED_OLD_BINARY=$OLD_TARGET/debug/cairnflow \
MIXED_NEW_BINARY=$ROOT/target/debug/cairnflow \
MIXED_CTL_BINARY=$ROOT/target/debug/cairn-ctl \
MIXED_STORE_URI=$STORE \
  node harness/chaos/mixed.ts 2>&1 | tee "$OUT/mixed.log"
exit "${PIPESTATUS[0]}"
