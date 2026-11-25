#!/usr/bin/env bash
# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: BUSL-1.1
# Measures the latency floor of `xcall` (one cross-range call and its reply).
#
# Each run is its own `load.ts` (fresh runtime, fresh `memory://` store), a
# one-node, four-range cluster as in bench/interrange/run.sh. The binary must be
# built with `--features testkit,probe` for the store profile and the stage
# marks:
#
#   cargo build --release -p cairn-server --bin cairnflow --features testkit,probe
#   BINARY=target/release/cairnflow OUT=/tmp/lf PROFILE=s3-standard FLUSH=10 \
#     CONC=1 bench/latency-floor/run.sh
#
# - `PROFILE`: count-only (no latency, only counts requests — "memory"),
#   s3-express, s3-standard (bench/s3-model/profiles/).
# - `FLUSH`: `--flush-interval-ms` (also the shared WAL window when
#   `--wal-window-ms` is not set). `EXTRA`: extra flags for the node.
# - `CONC`: concurrency (1 or 64). Level 1 writes stage marks to
#   `$OUT/<tag>.probe` (read by `timeline.py`); level 64 writes none (the probe
#   has a global mutex).
# - The `/metrics` delta of the measurement window is in
#   `$OUT/<tag>/xcall-c<CONC>.json`.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
: "${BINARY:?BINARY=binary cairnflow --features testkit,probe}"
: "${OUT:?OUT=output directory}"
PROFILE="${PROFILE:-count-only}"
FLUSH="${FLUSH:-10}"
CONC="${CONC:-1}"
DURATION_S="${DURATION_S:-20}"
EXTRA="${EXTRA:-}"
TAG="${TAG:-$PROFILE-f$FLUSH-c$CONC}"
mkdir -p "$OUT"

probe=""
if [ "$CONC" = 1 ]; then
  probe="$OUT/$TAG.probe"
fi
{
  echo "== $TAG $(date '+%F %T') · EXTRA=$EXTRA"
  uptime
  pgrep -fl "chaos.ts|bench/interrange|load.ts" | grep -v pgrep || true
} >>"$OUT/machine.log"

# shellcheck disable=SC2086 # EXTRA is a list of flags, split on purpose.
env -u CAIRNFLOW_PROBE_OUT LOAD_SCENARIOS=xcall LOAD_CONCURRENCY="$CONC" \
  LOAD_BINARY="$BINARY" \
  LOAD_ARGS="--main-store memory:// --cluster --partitions 4 $EXTRA" \
  LOAD_FLUSH_MS="$FLUSH" \
  LOAD_SWEEP=0 LOAD_DURATION_S="$DURATION_S" LOAD_SETTLE_S=900 \
  LOAD_METRICS_DUMP="$OUT/$TAG" \
  CAIRNFLOW_STORE_PROFILE="$ROOT/bench/s3-model/profiles/$PROFILE.json" \
  ${probe:+CAIRNFLOW_PROBE_OUT="$probe"} \
  node "$ROOT/harness/bench/load.ts" >"$OUT/$TAG.log" 2>&1 || {
  echo "✗ $TAG failed — $OUT/$TAG.log"
  exit 1
}
grep -E "^  (✓|✗)|xcall " "$OUT/$TAG.log" | head -5 || true
