#!/usr/bin/env bash
# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: BUSL-1.1
# Cross-range baseline: every cross-range scenario of `load.ts`, measured on
# a one-node, four-range cluster.
#
# Each scenario gets its own `load.ts` run — a fresh runtime, a fresh
# `memory://` store — so the deleted outbox/inbound rows of one scenario do not
# slow down the next (`harness/bench/README.md`, "residual state"). The
# raw log of each run is in `$OUT/<name>.log`, the machine load line before
# each run in `$OUT/machine.log`.
#
#   BINARY=…/cairnflow PROFILE=memory OUT=/tmp/ir bench/interrange/run.sh
#   BINARY=…/cairnflow-testkit PROFILE=s3-standard OUT=/tmp/ir-s3 bench/interrange/run.sh
#
# `PROFILE=s3-standard` needs a `--features testkit` binary (it sets
# `CAIRNFLOW_STORE_PROFILE=bench/s3-model/profiles/s3-standard.json`).
# `ONLY="xcall-64 tree"` runs a subset.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
: "${BINARY:?BINARY=path to a release cairnflow binary}"
: "${OUT:?OUT=directory for the logs}"
PROFILE="${PROFILE:-memory}"
DURATION_S="${DURATION_S:-20}"
mkdir -p "$OUT"

case "$PROFILE" in
  memory) STORE_PROFILE="" ;;
  s3-standard) STORE_PROFILE="$ROOT/bench/s3-model/profiles/s3-standard.json" ;;
  *) echo "PROFILE must be memory or s3-standard" >&2; exit 2 ;;
esac

# name | environment variables for load.ts
RUNS=(
  "xcall-1|LOAD_SCENARIOS=xcall LOAD_CONCURRENCY=1"
  "xcall-64|LOAD_SCENARIOS=xcall LOAD_CONCURRENCY=64"
  "xcall-oneway-1|LOAD_SCENARIOS=xcall LOAD_CONCURRENCY=1 LOAD_XCALL_ONEWAY=1"
  "xcall-oneway-64|LOAD_SCENARIOS=xcall LOAD_CONCURRENCY=64 LOAD_XCALL_ONEWAY=1"
  "tree|LOAD_SCENARIOS=tree"
  "fanout|LOAD_SCENARIOS=fanout"
  "fanout-1000|LOAD_SCENARIOS=fanout LOAD_FANOUT=1000 LOAD_FANOUT_CONCURRENCY=1"
  "agent|LOAD_SCENARIOS=agent"
  "saga|LOAD_SCENARIOS=saga"
)

status=0
for entry in "${RUNS[@]}"; do
  name="${entry%%|*}"
  vars="${entry#*|}"
  if [ -n "${ONLY:-}" ] && [[ " $ONLY " != *" $name "* ]]; then
    continue
  fi
  {
    echo "== $name $(date '+%F %T') · $PROFILE · $vars"
    uptime
  } >>"$OUT/machine.log"
  echo "▸ $name ($PROFILE)"
  # shellcheck disable=SC2086 # `vars` is a list of assignments, split on whitespace on purpose.
  if ! env $vars \
    LOAD_BINARY="$BINARY" \
    LOAD_ARGS="--main-store memory:// --cluster --partitions 4" \
    LOAD_SWEEP=0 LOAD_DURATION_S="$DURATION_S" LOAD_SETTLE_S=900 \
    CAIRNFLOW_STORE_PROFILE="$STORE_PROFILE" \
    node "$ROOT/harness/bench/load.ts" >"$OUT/$name.log" 2>&1; then
    echo "  ✗ $name failed — see $OUT/$name.log"
    status=1
  fi
  # The result line of the scenario and the cross-range lines under it (the
  # first carries "remote <pct>", the rest are indented by 12 spaces).
  grep -E "^  (✓|✗)| remote [0-9]|^            [^ ]" "$OUT/$name.log" || true
done
exit "$status"
