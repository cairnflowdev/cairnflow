#!/usr/bin/env bash
# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: BUSL-1.1
# One command that checks the whole repository: Rust, TypeScript and the e2e matrix.
#
#   scripts/check.sh              # the full run
#   scripts/check.sh --quick      # Rust only: fmt, clippy, test, build
#   scripts/check.sh --no-cluster # skip the two 4-partition cluster e2e rows
#
# Runs from anywhere (it `cd`s to the repository root). Each step's log is in
# `target/check/<step>.log`; the end of the run prints a PASS/FAIL/WARN table.
# Exits non-zero if any step FAILs — WARN does not.
#
# e2e matrix = {the default single node (`--data-dir`, no `--cluster` — a
# one-node cluster on the shared WAL), `--main-store memory:// --cluster
# --partitions 4 --tick-interval-ms 100`} × {BIDI_STREAM, REQUEST_RESPONSE}, every
# row on the shared WAL — the only WAL (the per-range WAL rows of Rust and e2e
# were dropped together with it). Plus two single-node rows with the SDK pinned to
# wire 1 (REQUEST_RESPONSE) and wire 2 (BIDI_STREAM) — `CAIRNFLOW_E2E_SDK_MAX_WIRE`.
# Every test has `--test-timeout=120000` (the `node --test` default is infinite —
# one hung test hangs the whole run), and every row has an overall cap as well
# (`CHECK_E2E_ROW_S`, default 900 s): once the server is stuck the following tests
# are stuck too, and 35 tests × 120 s is over an hour of waiting for a known answer.
#
# The **`ui`** row: the console's (`ui/`) `npm ci && npm run check` **before** any
# cargo step — the binary and `cargo test` embed the real build — plus a 3 MB cap
# on the embedded size, and Playwright on the real binary in the TypeScript part
# (Chromium, a 4-range cluster; WebKit and Firefox in Docker). No Node ⇒ FAIL;
# `CHECK_SKIP_UI=1` skips it deliberately.
#
# A separate Rust row for **failpoints** (`--features failpoints`): only the two
# `tests/failpoints.rs` binaries of cairn-store and cairn-server.
# Another row for the **network harness** (`--features testkit`):
# cairn-server's `tests/net_partition.rs` and `tests/dead_destination.rs` binaries
# (a dead destination through the network filter), together with
# `tests/store_profile.rs` (simulated S3 store profiles on the `testkit` binary),
# and cairn-server's `--lib` unit tests (modules that exist only under `testkit`:
# `netfilter`, `store_profile`, …).
#
# An **"ambiguous once per op"** row: cairn-server's cluster test files with
# `CAIRNFLOW_TEST_FAULTS=each-once` — the first write of each object cairn manages
# itself reaches the store and then receives the answer of object_store's own
# retry (`Action::EachOnce`). Every conditional write site must resolve it (rule 7
# of AGENTS.md). The sibling row **"ambiguous once per op, shared WAL log too"**
# (`CAIRNFLOW_TEST_FAULTS=each-once+nodewal`) runs the same files with the
# `nodewal` layer ambiguous as well. Ambiguous log PUTs are read back on an
# `atomic_put` store (before that, every log PUT was a fail-stop and 83 of 114 of
# these tests failed), so the row is on by default and `fatal`
# (`CHECK_EACH_ONCE_NODEWAL=0` turns it off).
#
# A **real S3** row: `graceful_shutdown` (real binary, two processes — `#[ignore]`
# in the `--workspace` row) and the real-store branch of `failover`. Runs when
# `CAIRN_TEST_S3_URI` is set, or when a local SeaweedFS
# (`scripts/dev-store/up.sh`, `localhost:18333`) answers; otherwise `SKIP`
# with the reason. `CHECK_S3=0` turns it off entirely.
#
# A **billing workload** row: `harness/chaos/chaos.ts --workload billing` for `CHECK_BILLING_S`
# seconds (default 180) on a temporary `file://` directory — one node that
# `kill -9`s itself every 20 s and is rebuilt from the directory, liveness checks
# (a)–(d) and I1–I6. Needs no SeaweedFS (`memory://` cannot be killed; a
# multi-node cluster in one process does not have that). It runs in the background
# **outside** the `CHECK_E2E_JOBS` batch, starting right after `cargo test
# --workspace` on a copy of the binary, so it overlaps the remaining Rust rows and
# the TS part instead of running after them. `CHECK_BILLING=0` turns it off. The
# 60-minute variant on SeaweedFS is a nightly job.
#
# Plus a **"split mid-run"** row: the 4-range cluster as above, plus a saboteur
# that splits a range and then merges the child pair back every `CHECK_SPLIT_MS`
# (default 3000) throughout the suite — see `sdk/typescript/test/ranges.ts`. On by
# default; turn it off with `CHECK_E2E_SPLIT=0`.
#
# `cargo fmt --check` is a FAIL step: the tree was formatted once in a `style:`
# commit (hash in `.git-blame-ignore-revs`).
#
# Clippy is a FAIL step (`-D warnings`) on three configurations: default,
# `--features cairn-server/testkit`, `--features
# cairn-server/failpoints,cairn-store/failpoints` — code behind a feature is only
# checked when the feature is on. `clippy.toml` blocks new sources of
# nondeterminism (wall clock, `std::time::Instant`, std's `HashMap`, `getrandom`,
# `reqwest` requests sent directly); `scripts/lint-sources.sh` (the FAIL step
# "source lints") covers what clippy cannot: new bare `sleep(…).await` and the
# `// #[hot-path]` mark of child files.
#
# **Speed**: with `cargo-nextest` the Rust rows run under `cargo nextest run`
# (one process per test, all binaries in parallel — `.config/nextest.toml`), plus
# doctests through `cargo test --doc` (nextest does not run doctests). Without it,
# it falls back to `cargo test` and says so; `CHECK_NEXTEST=0` forces `cargo test`.
# The Rust rows still run **one after another** (throughput tests must not carry
# another row's load). The TS unit tests and the e2e rows run **in parallel** with
# each other (each row has its own server: `memory://`/a temporary directory, port
# 0); `CHECK_E2E_JOBS` (default 9) is the number of rows at once, `1` runs them one
# by one. Every step also writes the list of tests that actually ran to
# `target/check/<step>.tests` (`scripts/check-tests.sh`) to compare coverage
# between two runs.
#
# Written for bash 3.2 (macOS's default bash): no associative arrays, no
# `timeout` (macOS has none) — time limits are a background job plus a watchdog.

set -euo pipefail

QUICK=0
CLUSTER=1
for arg in "$@"; do
  case "$arg" in
    --quick) QUICK=1 ;;
    --no-cluster) CLUSTER=0 ;;
    -h | --help)
      sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "unknown argument: $arg (see --help)" >&2
      exit 2
      ;;
  esac
done

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# The SlateDB fork is a submodule (`vendor/slatedb`); a fresh worktree does not
# have it on its own.
git -c protocol.file.allow=always submodule update --init --quiet

STEP_S="${CHECK_STEP_S:-1800}"
E2E_ROW_S="${CHECK_E2E_ROW_S:-900}"
UNIT_TEST_TIMEOUT_MS="${CHECK_UNIT_TEST_TIMEOUT_MS:-300000}"
SPLIT_ROW="${CHECK_E2E_SPLIT:-1}"
SPLIT_MS="${CHECK_SPLIT_MS:-3000}"
BILLING_ROW="${CHECK_BILLING:-1}"
BILLING_S="${CHECK_BILLING_S:-180}"
CLUSTER_ARGS="--main-store memory:// --cluster --partitions 4 --tick-interval-ms 100"
S3_ROW="${CHECK_S3:-1}"
EACH_ONCE_NODEWAL="${CHECK_EACH_ONCE_NODEWAL:-1}"
SEAWEED_ENDPOINT="${CHECK_SEAWEED_ENDPOINT:-http://localhost:18333}"
E2E_JOBS="${CHECK_E2E_JOBS:-9}"
if [ "${CAIRNFLOW_E2E_NODES:-1}" -gt 1 ] 2>/dev/null; then E2E_JOBS=1; fi

LOG_DIR="$ROOT/target/check"
mkdir -p "$LOG_DIR"
rm -f "$LOG_DIR"/*.result "$LOG_DIR"/*.tests

NEXTEST=0
if [ "${CHECK_NEXTEST:-1}" = 1 ] && cargo nextest --version >/dev/null 2>&1; then
  NEXTEST=1
  echo "Rust: $(cargo nextest --version 2>/dev/null | head -n 1) (.config/nextest.toml) + cargo test --doc"
elif [ "${CHECK_NEXTEST:-1}" = 1 ]; then
  echo "Rust: NO cargo-nextest — falling back to cargo test (much slower; install:"
  echo "      brew install cargo-nextest, or cargo install cargo-nextest --locked)"
else
  echo "Rust: cargo test (CHECK_NEXTEST=${CHECK_NEXTEST})"
fi

# ───────────────────────────────────────────── running one step with a time limit

NAMES=()
STATUS=()
SECS=()
NOTES=()
CURRENT_PGID=""
# Process group of the running watchdog: an interrupt kills it too, so it does not
# wake up after `secs` seconds and kill a pgid that has been reused.
CURRENT_DOG=""

# Ctrl-C mid-run: kill the whole process group of the running step (node --test
# spawns child processes, and the children spawn `cairnflow`).
on_interrupt() {
  if [ -n "$CURRENT_PGID" ]; then kill -TERM -- "-$CURRENT_PGID" 2>/dev/null || true; fi
  if [ -n "$CURRENT_DOG" ]; then kill -TERM -- "-$CURRENT_DOG" 2>/dev/null || true; fi
  # Background steps (`step_async`): each kills its own process group when it
  # receives TERM.
  local p
  for p in ${ASYNC_PIDS[@]+"${ASYNC_PIDS[@]}"}; do kill -TERM "$p" 2>/dev/null || true; done
  # The billing row runs in the background outside `ASYNC_PIDS`.
  if [ -n "${billing_pid:-}" ]; then kill -TERM "$billing_pid" 2>/dev/null || true; fi
  echo
  echo "interrupted" >&2
  exit 130
}
trap on_interrupt INT TERM

# run_with_deadline <seconds> <log> <command...>
#
# Runs the command in the background in **its own process group** (`set -m`), so
# that past the limit the whole tree can be killed, not just the first process.
# Returns the command's exit code, or 124 past the limit (the same convention as
# coreutils' `timeout`).
run_with_deadline() {
  local secs="$1" log="$2"
  shift 2
  local marker="$log.timedout"
  rm -f "$marker"
  set -m
  "$@" >"$log" 2>&1 &
  local pid=$!
  (
    sleep "$secs"
    : >"$marker"
    kill -TERM -- "-$pid" 2>/dev/null || true
    sleep 5
    kill -KILL -- "-$pid" 2>/dev/null || true
  ) &
  local dog=$!
  set +m
  CURRENT_PGID="$pid"
  CURRENT_DOG="$dog"
  local rc=0
  wait "$pid" || rc=$?
  CURRENT_PGID=""
  CURRENT_DOG=""
  kill -TERM -- "-$dog" 2>/dev/null || true
  wait "$dog" 2>/dev/null || true
  if [ -f "$marker" ]; then
    rm -f "$marker"
    return 124
  fi
  return "$rc"
}

record() {
  NAMES+=("$1")
  STATUS+=("$2")
  SECS+=("$3")
  NOTES+=("$4")
  printf '  %-4s %4ss  %s\n' "$2" "$3" "$4"
}

# Names of the failed tests: node's `spec` reporter (lines starting with ✖), Rust's
# libtest (`---- name stdout ----`) and nextest (`FAIL [ 1.2s] (3/9) crate::bin name`).
failed_tests() {
  {
    grep -E '^[[:space:]]*✖ ' "$1" 2>/dev/null | sed -E 's/^[[:space:]]*✖ //; s/ \([0-9.]+m?s\)$//'
    grep -E '^---- .* stdout ----$' "$1" 2>/dev/null | sed -E 's/^---- (.*) stdout ----$/\1/'
    grep -E '^ +(FAIL|TIMEOUT|SIG[A-Z]+|LEAK-FAIL) \[' "$1" 2>/dev/null |
      sed -E 's/^ +[A-Z-]+ \[[^]]*\] +(\([^)]*\) +)?//'
  } |
    grep -v '^failing tests:$' |
    awk '!seen[$0]++' |
    head -8 |
    paste -sd '|' - |
    sed 's/|/ · /g' || true
}

slug_of() {
  printf '%s' "$1" | tr -c 'A-Za-z0-9._-' '_'
}

# Pending background steps (`step_async` → `wait_async`): subshell pid and name.
ASYNC_PIDS=()
ASYNC_NAMES=()
# When set, `step` writes its result to this file instead of the table (background steps).
RESULT_FILE=""

# step <name> <mode> <seconds> <command...>
#
# mode: `fatal` — a failure is a FAIL; `warn` — a failure is a WARN (does not fail the run).
step() {
  local name="$1" mode="$2" secs="$3"
  shift 3
  local log
  log="$LOG_DIR/$(slug_of "$name").log"
  [ -n "$RESULT_FILE" ] || printf '▸ %s\n' "$name"
  local t0=$SECONDS rc=0
  run_with_deadline "$secs" "$log" "$@" || rc=$?
  local dt=$((SECONDS - t0))
  local rel="${log#"$ROOT"/}"
  local note="$rel"
  # The list of tests that actually ran, to compare coverage between two runs (`scripts/check-tests.sh`).
  "$ROOT/scripts/check-tests.sh" "$log" >"${log%.log}.tests" 2>/dev/null || true
  if [ "$rc" -eq 124 ]; then
    local fails
    fails="$(failed_tests "$log")"
    note="TIMED OUT ${secs}s${fails:+ — failed: $fails} ($rel)"
  elif [ "$rc" -ne 0 ]; then
    local fails
    fails="$(failed_tests "$log")"
    note="exit $rc${fails:+ — failed: $fails} ($rel)"
  fi
  local status
  if [ "$rc" -eq 0 ]; then
    status=PASS
    note="$(awk '{n[$1]++} END{if (NR) printf "%d ok%s", n["ok"], (n["ignored"] ? ", " n["ignored"] " ignored" : "")}' "${log%.log}.tests")"
  elif [ "$mode" = warn ]; then
    status=WARN
  else
    status=FAIL
  fi
  if [ -n "$RESULT_FILE" ]; then
    printf '%s\n%s\n%s\n' "$status" "$dt" "$note" >"$RESULT_FILE"
    return 0
  fi
  record "$name" "$status" "$dt" "$note"
  [ "$status" != FAIL ] || tail -n 25 "$log" | sed 's/^/      │ /'
  return 0
}

# step_async <name> <mode> <seconds> <command...> — like `step`, but in the
# background; the result goes into the table at `wait_async`, in start order. When
# `CHECK_E2E_JOBS` background steps are already running, waits for that batch first.
step_async() {
  local name="$1"
  if [ "${#ASYNC_PIDS[@]}" -ge "$E2E_JOBS" ]; then wait_async; fi
  local res
  res="$LOG_DIR/$(slug_of "$name").result"
  rm -f "$res"
  printf '▸ %s (background)\n' "$name"
  set -m
  (
    # Ctrl-C in the main script arrives here as TERM (`on_interrupt`): kill the
    # command's process group and its watchdog, leaving no orphans.
    CURRENT_PGID=""
    CURRENT_DOG=""
    ASYNC_PIDS=()
    trap 'on_interrupt >/dev/null 2>&1' INT TERM
    RESULT_FILE="$res"
    step "$@"
  ) &
  ASYNC_PIDS+=($!)
  set +m
  ASYNC_NAMES+=("$name")
}

wait_async() {
  local i
  for i in ${ASYNC_PIDS[@]+"${!ASYNC_PIDS[@]}"}; do
    wait "${ASYNC_PIDS[$i]}" 2>/dev/null || true
    local name="${ASYNC_NAMES[$i]}"
    local res
    res="$LOG_DIR/$(slug_of "$name").result"
    if [ -f "$res" ]; then
      local status dt note
      status="$(sed -n 1p "$res")"
      dt="$(sed -n 2p "$res")"
      note="$(sed -n 3p "$res")"
      rm -f "$res"
      printf '▸ %s\n' "$name"
      record "$name" "$status" "$dt" "$note"
      [ "$status" != FAIL ] || tail -n 25 "$LOG_DIR/$(slug_of "$name").log" | sed 's/^/      │ /'
    else
      printf '▸ %s\n' "$name"
      record "$name" FAIL 0 "background step died without leaving a result"
    fi
  done
  ASYNC_PIDS=()
  ASYNC_NAMES=()
}

skip() {
  if [ "${#ASYNC_PIDS[@]}" -gt 0 ]; then
    # Background steps still pending: queue behind them so the table keeps start order.
    printf 'SKIP\n0\n%s\n' "$2" >"$LOG_DIR/$(slug_of "$1").result"
    : &
    ASYNC_PIDS+=($!)
    ASYNC_NAMES+=("$1")
    return
  fi
  record "$1" SKIP 0 "$2"
}

# ───────────────────────────────────────────── billing row
#
# Its own background job, **outside** the `step_async` batches (a full batch waits
# for the whole batch — one 3-minute row in a batch would stretch the batch).
# Started as early as possible — right after `cargo test --workspace`, once the
# binary exists — to overlap the remaining Rust rows and the TS part instead of
# running after them; runs on a **copy** of the binary in a temporary directory,
# because the later `--features` rows rebuild `target/debug/cairnflow`. The result
# is collected after the last `wait_async` (`billing_collect`).
billing_name="chaos billing ${BILLING_S}s (file://, kill -9)"
billing_pid=""
billing_dir=""

billing_start() {
  billing_dir="$(mktemp -d "${TMPDIR:-/tmp}/cairn-check-billing.XXXXXX")"
  mkdir -p "$billing_dir/bin" "$billing_dir/store"
  cp target/debug/cairnflow target/debug/cairn-query-worker target/debug/cairn-ctl "$billing_dir/bin/"
  printf '▸ %s (background)\n' "$billing_name"
  set -m
  (
    CURRENT_PGID=""
    CURRENT_DOG=""
    ASYNC_PIDS=()
    trap 'on_interrupt >/dev/null 2>&1' INT TERM
    RESULT_FILE="$LOG_DIR/$(slug_of "$billing_name").result"
    step "$billing_name" fatal "$((BILLING_S + 420))" env -u AWS_ENDPOINT \
      CHAOS_BINARY="$billing_dir/bin/cairnflow" CHAOS_CTL="$billing_dir/bin/cairn-ctl" \
      CHAOS_STORE_URI="file://$billing_dir/store" CHAOS_DURATION_S="$BILLING_S" \
      CHAOS_KILL_EVERY_S=20 CHAOS_RESTART_AFTER_S=2 CHAOS_DRAIN_S=120 \
      bash -c 'cd harness && exec node chaos/chaos.ts --workload billing'
  ) &
  billing_pid=$!
  set +m
}

billing_collect() {
  [ -n "$billing_pid" ] || return 0
  wait "$billing_pid" 2>/dev/null || true
  local res
  res="$LOG_DIR/$(slug_of "$billing_name").result"
  printf '▸ %s\n' "$billing_name"
  if [ -f "$res" ]; then
    record "$billing_name" "$(sed -n 1p "$res")" "$(sed -n 2p "$res")" "$(sed -n 3p "$res")"
    [ "$(sed -n 1p "$res")" != FAIL ] || tail -n 25 "$LOG_DIR/$(slug_of "$billing_name").log" | sed 's/^/      │ /'
    rm -f "$res"
  else
    record "$billing_name" FAIL 0 "background step died without leaving a result"
  fi
  rm -rf "$billing_dir"
  billing_pid=""
}

status_of() {
  local i
  for i in "${!NAMES[@]}"; do
    if [ "${NAMES[$i]}" = "$1" ]; then
      echo "${STATUS[$i]}"
      return
    fi
  done
  echo NONE
}

# ───────────────────────────────────────────── Rust

# ───────────────────────────────────────────── web console
#
# **Before** any cargo step: the `cairn-ui` crate embeds `ui/dist` at compile time,
# so building the console first gives `cargo test` and the e2e binary the real
# console (absent ⇒ a placeholder page). No Node ⇒ an explicit FAIL, never a silent
# skip; `CHECK_SKIP_UI=1` skips it deliberately (shown in the table). Embedded size
# = total bytes of `ui/dist` (rust-embed copies the bytes into the binary as is),
# capped at 3 MB. Playwright e2e runs in the background in the TypeScript part,
# after the binary build step.
UI_SKIP="${CHECK_SKIP_UI:-0}"
UI_E2E_PORT="${CHECK_UI_E2E_PORT:-18480}"
UI_EMBED_CAP=$((3 * 1024 * 1024))
ui() {
  (cd "$ROOT/ui" && "$@")
}
ui_embed_size() {
  # Total bytes of file contents (exactly what `cairn-ui`'s `rust-embed` embeds). Not
  # `du -sb`: `-b` is GNU, macOS's `du` does not have it.
  local bytes
  bytes="$(find "$ROOT/ui/dist" -type f -exec cat {} + | wc -c | tr -d ' ')"
  echo "ui/dist embedded in the binary: $bytes bytes (cap $UI_EMBED_CAP)"
  [ "$bytes" -le "$UI_EMBED_CAP" ]
}
ui_ok=0
if [ "$QUICK" = 1 ]; then
  skip "ui npm run check" "--quick: Rust only"
elif [ "$UI_SKIP" = 1 ]; then
  skip "ui npm run check" "CHECK_SKIP_UI=1 — console not checked, the binary embeds the existing ui/dist or a placeholder page"
elif ! command -v node >/dev/null 2>&1 || ! command -v npm >/dev/null 2>&1; then
  record "ui npm run check" FAIL 0 "no node/npm — install Node ≥ 22.6, or CHECK_SKIP_UI=1 to skip deliberately"
else
  step "ui npm ci" fatal "$STEP_S" ui npm ci --no-audit --no-fund
  step "ui npm run check" fatal "$STEP_S" ui npm run check
  if [ "$(status_of "ui npm run check")" = PASS ]; then
    ui_ok=1
    step "ui embedded size ≤ 3 MB" fatal 60 ui_embed_size
  fi
fi

step "cargo fmt --check" fatal "$STEP_S" cargo fmt --all --check

step "source lints" fatal "$STEP_S" scripts/lint-sources.sh

# Optional publication row (`scripts/oss/`, private repository only): secrets
# and personal/SaaS traces (`oss_scrub.py`, needs gitleaks) and SPDX headers
# (`oss_license.py`), both in report mode, never FAIL. Runs with CHECK_OSS=1.
if [ "${CHECK_OSS:-0}" = 1 ] && [ -f scripts/oss/oss_scrub.py ]; then
  if command -v gitleaks >/dev/null 2>&1; then
    step "oss-scrub (report)" warn 600 python3 scripts/oss/oss_scrub.py
  else
    skip "oss-scrub (report)" "gitleaks not installed (brew install gitleaks)"
  fi
  step "oss-license (report)" warn 120 python3 scripts/oss/oss_license.py
fi

# **Dependency audit** row: `cargo audit` on `Cargo.lock` (downloads the
# `~/.cargo/advisory-db` DB the way `npm ci` downloads packages) with the acceptance
# list `.cargo/audit.toml` — `deny = ["warnings"]`, so a new advisory without a
# reason line is a FAIL. Plus `npm audit --audit-level=high` for `ui/`,
# `sdk/typescript/` and `harness/`, **devDependencies included**: the SDK and the
# harness have no runtime dependency, and devDependencies really run in this very
# check (`zod` 4.6.5, GHSA-mrhx-c7xc-m3w5, once slipped through `--omit=dev`). The
# SDK and the harness keep their lockfiles (`sdk/typescript/package-lock.json`,
# `harness/package-lock.json`).
#
# Dependency **license** rows: `cargo deny check licenses bans sources` (`deny.toml`)
# and `scripts/npm-licenses.ts` on the lockfiles of `ui/`, the SDK and the harness,
# devDependencies included — a license outside the allow list is a FAIL.
cargo_audit() {
  if ! command -v cargo-audit >/dev/null 2>&1; then
    echo "cargo-audit not found — brew install cargo-audit, or cargo install cargo-audit --locked"
    return 1
  fi
  cargo audit
}
sdk_npm_audit() {
  (cd "$ROOT/sdk/typescript" && npm audit --audit-level=high)
}
harness_npm_audit() {
  (cd "$ROOT/harness" && npm audit --audit-level=high)
}
cargo_deny() {
  if ! command -v cargo-deny >/dev/null 2>&1; then
    echo "cargo-deny not found — brew install cargo-deny, or cargo install cargo-deny --locked"
    return 1
  fi
  cargo deny check licenses bans sources
}
step "cargo audit" fatal 600 cargo_audit
step "cargo deny" fatal 600 cargo_deny
if [ "$QUICK" = 0 ] && command -v npm >/dev/null 2>&1; then
  step "npm audit ui" fatal 600 ui npm audit --audit-level=high
  step "npm audit sdk" fatal 600 sdk_npm_audit
  step "npm audit harness" fatal 600 harness_npm_audit
  step "npm licenses" fatal 600 node "$ROOT/scripts/npm-licenses.ts" \
    "$ROOT/ui/package-lock.json" "$ROOT/sdk/typescript/package-lock.json" \
    "$ROOT/harness/package-lock.json"
fi

step "cargo clippy" fatal "$STEP_S" cargo clippy --workspace --all-targets -- -D warnings
step "cargo clippy --features testkit" fatal "$STEP_S" \
  cargo clippy --workspace --all-targets --features cairn-server/testkit -- -D warnings
step "cargo clippy --features failpoints" fatal "$STEP_S" \
  cargo clippy --workspace --all-targets --features cairn-server/failpoints,cairn-store/failpoints -- -D warnings

# with_env <NAME=value>... <command...>: like `env`, but the command may be a function
# of this script (`step` runs the command in its own subshell, so `export` does not leak).
with_env() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      [A-Za-z_]*=*) export "${1?}" ;;
      *) break ;;
    esac
    shift
  done
  "$@"
}
# rust_tests <`cargo test` selection arguments...>: `cargo nextest run` when nextest is
# installed, otherwise `cargo test` — the same package/feature/binary selection syntax.
rust_tests() {
  if [ "$NEXTEST" = 1 ]; then cargo nextest run "$@"; else cargo test "$@"; fi
}
# Nextest does not run doctests: `cargo test --doc` runs afterwards, and both run even
# when the first one fails.
workspace_tests() {
  if [ "$NEXTEST" = 1 ]; then
    local rc=0
    cargo nextest run --workspace --no-fail-fast || rc=$?
    cargo test --workspace --doc --no-fail-fast || rc=$?
    return "$rc"
  fi
  cargo test --workspace --no-fail-fast
}
# `--no-fail-fast`: without it cargo stops at the first failing test binary, and the
# table could only say "at least one failed".
step "cargo test --workspace" fatal "$STEP_S" workspace_tests
# The billing row starts here if the binary exists (the `cairn-server` integration
# tests build it) — see `billing_start`.
if [ "$QUICK" = 0 ] && [ "$BILLING_ROW" = 1 ] && [ "$(status_of "cargo test --workspace")" = PASS ] &&
  [ -x target/debug/cairnflow ] && [ -x target/debug/cairn-ctl ]; then
  billing_start
fi
# Ambiguous store results (rule 7 of AGENTS.md): each (write or delete, path) pair
# of an object cairn manages itself — the lease layer and `cluster.conf`, rule book,
# catalog, deployments, keys, … — first reaches the store and then receives the
# answer of object_store's own retry (`Action::EachOnce`,
# `common::install_env_faults`). The set = every test file that builds nodes through
# `common` and touches such objects; WAL/manifest/SST and the shared WAL log are not
# touched (there `AlreadyExists` is a fence).
step "cargo test ambiguous once per op" fatal "$STEP_S" with_env CAIRNFLOW_TEST_FAULTS=each-once \
  rust_tests -p cairn-server --no-fail-fast --test cluster_lease --test split --test gc \
  --test deployments --test auth --test tenants --test quarantine --test range_table \
  --test range_window --test upgrade --test archive --test shared_wal_cluster --test node_smoke \
  --test slow_shed --test wrong_range --test auto_split --test retry_attempts --test invoke_transient \
  --test invoke_wait
# The shared WAL log too: green since ambiguous log PUTs are read back on an
# `atomic_put` store.
each_once_nodewal="cargo test ambiguous once per op + nodewal"
if [ "$EACH_ONCE_NODEWAL" = 1 ]; then
  step "$each_once_nodewal" fatal "$STEP_S" with_env CAIRNFLOW_TEST_FAULTS=each-once+nodewal \
    rust_tests -p cairn-server --no-fail-fast --test cluster_lease --test split --test gc \
    --test deployments --test auth --test tenants --test quarantine --test range_table \
    --test range_window --test upgrade --test archive --test shared_wal_cluster --test node_smoke \
    --test slow_shed --test wrong_range --test auto_split --test retry_attempts --test invoke_transient \
    --test invoke_wait
else
  skip "$each_once_nodewal" "CHECK_EACH_ONCE_NODEWAL=$EACH_ONCE_NODEWAL"
fi
# Failpoints: the `failpoints` feature is off by default, so the rows above do not
# build their code. Tests that use failpoints live in one `tests/failpoints.rs`
# binary per crate (the `fail` registry is global — they run sequentially in that
# binary; under nextest each test is its own process, so the registry is not
# shared).
step "cargo test --features failpoints" fatal "$STEP_S" \
  rust_tests -p cairn-store -p cairn-server --features failpoints --no-fail-fast --test failpoints
# Network harness and the deterministic simulation smoke test (the determinism
# criterion needs `--cfg tokio_unstable` and runs separately —
# `docs/testing.md` §4): cairn-server's `testkit` feature sets `NodeConfig::net_filter`; the
# `--workspace` row above builds without it (and the doctest of `NodeConfig` checks
# that the non-testkit build lacks that field).
step "cargo test --features testkit" fatal "$STEP_S" \
  rust_tests -p cairn-server --features testkit --no-fail-fast --lib --test net_partition --test store_profile \
  --test sim_cluster --test dead_destination --test registry_gen --test forward --test transport_tls \
  --test values
# OTLP (the `otel` feature): the default release build does not pull in the OTLP
# tree, so the `--workspace` row does not build the exporting part of `otel.rs`;
# this row builds the lib + binary with the feature and runs its tests (an
# in-process fake collector: `--no-user-otlp` sends nothing, enabled it arrives).
step "cargo test --features otel" fatal "$STEP_S" \
  rust_tests -p cairn-server --features otel --no-fail-fast --lib --bin cairnflow otel
# Real S3: `graceful_shutdown` needs two processes on one bucket (`memory://` cannot
# be shared, `file://` does not pass the CAS gate), `failover` has a real-store
# branch. `backup_credentials`: a backup with its own credential set
# `CAIRN_BACKUP_AWS_*` and real SigV4 signatures — a wrong target secret is rejected
# by the store.
s3_uri="${CAIRN_TEST_S3_URI:-}"
s3_why=""
if [ "$S3_ROW" != 1 ]; then
  s3_why="CHECK_S3=$S3_ROW"
elif [ -z "$s3_uri" ]; then
  if curl -s -m 2 -o /dev/null "$SEAWEED_ENDPOINT/" 2>/dev/null; then
    s3_uri="s3://walbench/check"
    export CAIRN_TEST_S3_ENDPOINT="$SEAWEED_ENDPOINT" CAIRN_TEST_S3_KEY=dev-only-access-key \
      CAIRN_TEST_S3_SECRET=dev-only-not-a-secret CAIRN_TEST_S3_REGION=us-east-1
  else
    s3_why="no CAIRN_TEST_S3_URI and SeaweedFS at $SEAWEED_ENDPOINT does not answer"
  fi
fi
# One test at a time: the two binaries share one bucket. Plus S3-5 of
# `s3_store_contract` (contract G18 — a conditional GET never returns a stale 304)
# on the same bucket; the `CAIRN_TEST_S3_*` credentials are mapped to the `AWS_*`
# that `StoreLocation::parse` reads (when absent, the existing `AWS_*` are kept).
s3_contract_env() {
  env AWS_ENDPOINT="${CAIRN_TEST_S3_ENDPOINT:-${AWS_ENDPOINT:-}}" \
    AWS_ACCESS_KEY_ID="${CAIRN_TEST_S3_KEY:-${AWS_ACCESS_KEY_ID:-}}" \
    AWS_SECRET_ACCESS_KEY="${CAIRN_TEST_S3_SECRET:-${AWS_SECRET_ACCESS_KEY:-}}" \
    AWS_REGION="${CAIRN_TEST_S3_REGION:-${AWS_REGION:-us-east-1}}" \
    AWS_ALLOW_HTTP=true AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false \
    CAIRNFLOW_STORE_CONTRACT_URI="${CAIRN_TEST_S3_URI%/}/g18" "$@"
}
s3_tests() {
  local rc=0
  if [ "$NEXTEST" = 1 ]; then
    cargo nextest run -p cairn-server --no-fail-fast --test graceful_shutdown --test failover \
      --test backup_credentials --run-ignored all --test-threads 1 || rc=1
    s3_contract_env cargo nextest run -p cairn-store --test s3_store_contract \
      --run-ignored all s3_5_conditional_get_never_serves_a_stale_304 || rc=1
  else
    cargo test -p cairn-server --no-fail-fast --test graceful_shutdown --test failover \
      --test backup_credentials -- --include-ignored --test-threads=1 || rc=1
    s3_contract_env cargo test -p cairn-store --test s3_store_contract \
      s3_5_conditional_get_never_serves_a_stale_304 -- --include-ignored || rc=1
  fi
  return "$rc"
}
if [ -z "$s3_why" ]; then
  step "cargo test real S3" fatal "$STEP_S" with_env CAIRN_TEST_S3_URI="$s3_uri" s3_tests
else
  skip "cargo test real S3" "graceful_shutdown, failover, backup_credentials, s3_store_contract S3-5 (S3): $s3_why"
fi
# `cairn` in the same step: `durability.test.ts` runs `cairn replay --bundle`.
step "cargo build --bin cairnflow --bin cairn-query-worker --bin cairn --bin cairn-ctl" fatal "$STEP_S" cargo build --bin cairnflow --bin cairn-query-worker --bin cairn --bin cairn-ctl

# ───────────────────────────────────────────── Python SDK
#
# Unit tests, the conformance runner (every trace, also through the Lambda adapter) and
# the end-to-end test against the binary just built. With `uv`, on the oldest supported
# Python and with the `h2` extra, so the BIDI_STREAM traces run too, plus `hypercorn`
# (test only) so every trace and the end-to-end suite also run through the ASGI app;
# without `uv`, the system `python3` (≥ 3.11) runs everything except BIDI_STREAM and the
# hypercorn passes (skipped with their reason).
#
# With `uv`, two more rows: `pyright` (pinned) on the SDK, its example and its tests with
# every optional dependency installed, and the wheel smoke test (`scripts/smoke_wheel.py`:
# build, install alone outside the repo, serve + register + call through the client, and
# the shipped types as a user sees them).
PY_PYRIGHT='pyright@1.1.407'
if [ "$QUICK" = 1 ]; then
  skip "python sdk unittest" "--quick: Rust only"
elif ! command -v uv >/dev/null 2>&1 && ! command -v python3 >/dev/null 2>&1; then
  record "python sdk unittest" FAIL 0 "no python3 — install Python ≥ 3.11 (or uv)"
elif ! command -v uv >/dev/null 2>&1 && ! python3 -c 'import sys; sys.exit(sys.version_info < (3, 11))'; then
  record "python sdk unittest" FAIL 0 "python3 is $(python3 -c 'import platform; print(platform.python_version())') — install Python ≥ 3.11 (or uv)"
else
  py_sdk() {
    if command -v uv >/dev/null 2>&1; then
      (cd "$ROOT/sdk/python" && uv run --no-project --python 3.11 --with 'h2>=4.1,<5' \
        --with 'hypercorn>=0.17,<0.19' python -m unittest discover -s tests -v)
    else
      (cd "$ROOT/sdk/python" && python3 -m unittest discover -s tests -v)
    fi
  }
  step "python sdk unittest" fatal "$STEP_S" py_sdk
  if command -v uv >/dev/null 2>&1; then
    py_types() {
      local env
      env=$(mktemp -d "${TMPDIR:-/tmp}/cairnflow-py-types.XXXXXX")
      (cd "$ROOT/sdk/python" &&
        uv venv --quiet --python 3.11 "$env/venv" &&
        uv pip install --quiet --python "$env/venv/bin/python" 'h2>=4.1,<5' \
          'hypercorn>=0.17,<0.19' 'cryptography>=42' &&
        uvx --python 3.11 "$PY_PYRIGHT" --pythonpath "$env/venv/bin/python" src example tests)
      local rc=$?
      rm -rf "$env"
      return $rc
    }
    step "python sdk typecheck" fatal "$STEP_S" py_types
    step "python sdk wheel smoke" fatal "$STEP_S" \
      env SMOKE_PYRIGHT="$PY_PYRIGHT" bash -c 'cd "$1" && uv run --no-project --python 3.11 python scripts/smoke_wheel.py' _ "$ROOT/sdk/python"
  else
    skip "python sdk typecheck" "needs uv"
    skip "python sdk wheel smoke" "needs uv"
  fi
fi

# ───────────────────────────────────────────── Go SDK
#
# The module at sdk/go (no dependency): gofmt, `go vet` (also with the e2e tag) and
# staticcheck (pinned); `go test -race` (unit, the conformance runner on every trace, the
# suspension vectors, cairntest, the pinned public API of scripts/smoke/app); a short fuzz
# budget per fuzz target; the end-to-end suite against the binary just built, on one node
# and on a 4-partition cluster (a missing binary is a FAIL, not a skip); the smoke test
# of a user's module outside the repository (`scripts/smoke.sh`). Every Go test runs with
# a memory limit and one package at a time. No govulncheck: it needs the vulnerability
# database from the network.
GO_STATICCHECK='honnef.co/go/tools/cmd/staticcheck@2026.2.2'
GO_FUZZ_S="${CHECK_GO_FUZZ_S:-5}"
GO_FUZZ_TARGETS="internal/wire:FuzzSplitter internal/wire:FuzzDecode internal/wire:FuzzDecodeValue internal/values:FuzzAdmit"
go_sdk_names=("go sdk vet + staticcheck" "go sdk test -race" "go sdk fuzz" "go sdk e2e" "go sdk e2e 4p cluster" "go sdk smoke")
if [ "$QUICK" = 1 ]; then
  for n in "${go_sdk_names[@]}"; do skip "$n" "--quick: Rust only"; done
elif ! command -v go >/dev/null 2>&1; then
  for n in "${go_sdk_names[@]}"; do record "$n" FAIL 0 "no go — install Go ≥ 1.25"; done
else
  go_sdk() {
    (cd "$ROOT/sdk/go" && env GOMEMLIMIT=1GiB GOWORK=off "$@")
  }
  go_sdk_static() {
    local unformatted
    unformatted="$(go_sdk gofmt -l .)" || return 1
    if [ -n "$unformatted" ]; then
      echo "gofmt: not formatted: $unformatted"
      return 1
    fi
    go_sdk go vet ./... && go_sdk go vet -tags e2e ./e2e/... &&
      go_sdk go run "$GO_STATICCHECK" ./... && go_sdk go run "$GO_STATICCHECK" -tags e2e ./e2e/...
  }
  go_sdk_fuzz() {
    local t
    for t in $GO_FUZZ_TARGETS; do
      go_sdk go test -run '^$' -fuzz "^${t#*:}\$" -fuzztime "${GO_FUZZ_S}s" -parallel 4 "./${t%%:*}" || return 1
    done
  }
  step "go sdk vet + staticcheck" fatal "$STEP_S" go_sdk_static
  step "go sdk test -race" fatal "$STEP_S" go_sdk go test -race -p 1 -count=1 ./...
  step "go sdk fuzz" fatal "$STEP_S" go_sdk_fuzz
  if [ "$(status_of "cargo build --bin cairnflow --bin cairn-query-worker --bin cairn --bin cairn-ctl")" = PASS ]; then
    step "go sdk e2e" fatal "$STEP_S" \
      go_sdk env CAIRNFLOW_E2E_REQUIRE=1 go test -tags e2e -p 1 -count=1 -v ./e2e/...
    step "go sdk e2e 4p cluster" fatal "$STEP_S" \
      go_sdk env CAIRNFLOW_E2E_REQUIRE=1 CAIRNFLOW_E2E_ARGS="$CLUSTER_ARGS" go test -tags e2e -p 1 -count=1 -v ./e2e/...
    step "go sdk smoke" fatal "$STEP_S" env GOMEMLIMIT=1GiB bash "$ROOT/sdk/go/scripts/smoke.sh"
  else
    for n in "go sdk e2e" "go sdk e2e 4p cluster" "go sdk smoke"; do
      record "$n" FAIL 0 "the binaries did not build"
    done
  fi
fi

# ───────────────────────────────────────────── TypeScript

if [ "$QUICK" = 1 ]; then
  echo "(--quick: skipping the TypeScript part)"
else
  ts() {
    (cd "$ROOT/sdk/typescript" && "$@")
  }
  # The engine harness (`harness/`, BUSL-1.1): chaos, mixed-binary and benchmark
  # drivers. It imports the SDK by relative source path, so it needs no SDK build.
  harness() {
    (cd "$ROOT/harness" && "$@")
  }

  # devDependencies (`tsc`, `ai`/`@ai-sdk/*`/`zod` of `test/ai-sdk.test.ts`) are
  # installed when missing or when the lockfile is newer than the last install (the
  # hidden lockfile `node_modules/.package-lock.json` npm writes on every install) —
  # looking only at `tsc`, a devDependency added later would never reach an old
  # `node_modules`. `npm ci` on the committed `package-lock.json`: a reproducible
  # install.
  sdk_installed="$ROOT/sdk/typescript/node_modules/.package-lock.json"
  if [ ! -x "$ROOT/sdk/typescript/node_modules/.bin/tsc" ] ||
    [ ! -f "$sdk_installed" ] || [ "$ROOT/sdk/typescript/package-lock.json" -nt "$sdk_installed" ]; then
    step "npm ci" fatal "$STEP_S" ts npm ci --no-audit --no-fund
  fi
  step "tsc --noEmit" fatal "$STEP_S" ts node_modules/.bin/tsc --noEmit
  # The same install rule for the harness's own devDependencies (`tsc`, `@types/node`).
  harness_installed="$ROOT/harness/node_modules/.package-lock.json"
  if [ ! -x "$ROOT/harness/node_modules/.bin/tsc" ] ||
    [ ! -f "$harness_installed" ] || [ "$ROOT/harness/package-lock.json" -nt "$harness_installed" ]; then
    step "harness npm ci" fatal "$STEP_S" harness npm ci --no-audit --no-fund
  fi
  step "harness tsc --noEmit" fatal "$STEP_S" harness node_modules/.bin/tsc --noEmit

  have_binary=1
  [ "$(status_of "cargo build --bin cairnflow --bin cairn-query-worker --bin cairn --bin cairn-ctl")" = PASS ] || have_binary=0
  # The npm package: `npm pack`, installed outside the repository, a plain JS app on
  # Node 22, 24 (through `mise`, when available) and the current Node, the user's `tsc`.
  if [ "$have_binary" = 1 ]; then
    step "ts tarball smoke" fatal "$STEP_S" ts node scripts/smoke-pack.ts
  else
    skip "ts tarball smoke" "the binary did not build"
  fi

  # Billing workload: usually already started after `cargo test --workspace`; if not
  # (the binary did not exist then), it starts here.
  if [ -z "$billing_pid" ]; then
    if [ "$BILLING_ROW" != 1 ]; then
      skip "$billing_name" "CHECK_BILLING=$BILLING_ROW"
    elif [ "$have_binary" = 0 ]; then
      skip "$billing_name" "the binary did not build"
    else
      billing_start
    fi
  fi

  # Web console on the real binary: Playwright + Chromium; its own ports
  # `CHECK_UI_E2E_PORT` .. +3 (a `--no-auth` node and a node with a bootstrap key,
  # each with an internal port).
  if [ "$ui_ok" = 1 ] && [ "$have_binary" = 1 ]; then
    step_async "ui e2e (Playwright)" fatal "$STEP_S" env CAIRN_BIN="$ROOT/target/debug/cairnflow" \
      CAIRN_E2E_PORT="$UI_E2E_PORT" bash -c 'cd ui && exec npx playwright test'
  elif [ "$ui_ok" = 1 ]; then
    skip "ui e2e (Playwright)" "the binary did not build"
  else
    skip "ui e2e (Playwright)" "the ui row did not run or is not green"
  fi
  # The same Chromium suite with the `noauth` node running a 4-range cluster: every
  # `/query` statement fans out over the ranges. Its own port range +40 .. +43.
  if [ "$ui_ok" = 1 ] && [ "$have_binary" = 1 ]; then
    step_async "ui e2e cluster-4p (Playwright)" fatal "$STEP_S" env CAIRN_BIN="$ROOT/target/debug/cairnflow" \
      CAIRN_E2E_PORT="$((UI_E2E_PORT + 40))" \
      CAIRN_E2E_NOAUTH_ARGS="--cluster --partitions 4 --tick-interval-ms 100" \
      bash -c 'cd ui && exec npx playwright test --project=noauth'
  fi
  # The console on a cluster of **two processes sharing an S3 bucket**: actions on a
  # node that does not lead the range must succeed through forwarding. Same condition
  # as the "real S3" row; its own port range +50 .. +53.
  if [ "$ui_ok" = 1 ] && [ "$have_binary" = 1 ] && [ -z "$s3_why" ]; then
    step_async "ui e2e two nodes (S3)" fatal "$STEP_S" \
      with_env CAIRN_TEST_S3_URI="$s3_uri" s3_contract_env env CAIRN_BIN="$ROOT/target/debug/cairnflow" \
      CAIRN_E2E_PORT="$((UI_E2E_PORT + 50))" CAIRN_E2E_S3_URI="${s3_uri%/}" \
      bash -c 'cd ui && exec npx playwright test -c playwright.twonode.config.ts'
  elif [ "$ui_ok" = 1 ] && [ "$have_binary" = 1 ]; then
    skip "ui e2e two nodes (S3)" "$s3_why"
  fi
  # A small WebKit project (CSP, session cookie): the browser runs in Playwright's
  # image (`ui/scripts/e2e-webkit.sh`). Its own port range (+20 .. +23, run-server at
  # +30) to run in parallel with the Chromium rows. No Docker ⇒ SKIP with a reason.
  if [ "$ui_ok" = 1 ] && [ "$have_binary" = 1 ] && command -v docker >/dev/null 2>&1; then
    step_async "ui e2e WebKit (Docker)" fatal "$STEP_S" env CAIRN_BIN="$ROOT/target/debug/cairnflow" \
      CAIRN_E2E_PORT="$((UI_E2E_PORT + 20))" CAIRN_E2E_WEBKIT_PORT="$((UI_E2E_PORT + 30))" \
      bash -c 'cd ui && exec scripts/e2e-webkit.sh'
  elif [ "$ui_ok" = 1 ] && [ "$have_binary" = 1 ]; then
    skip "ui e2e WebKit (Docker)" "no docker — Playwright's WebKit build needs an Ubuntu image"
  fi
  # Firefox: smoke, the main screens, the SQL console, key login — the browser in a
  # container like WebKit (`ui/scripts/e2e-container.sh`; Playwright's native Firefox does
  # not start on macOS 27). Its own port range (+60 .. +65, run-server at +70).
  if [ "$ui_ok" = 1 ] && [ "$have_binary" = 1 ] && command -v docker >/dev/null 2>&1; then
    step_async "ui e2e Firefox (Docker)" fatal "$STEP_S" env CAIRN_BIN="$ROOT/target/debug/cairnflow" \
      CAIRN_E2E_PORT="$((UI_E2E_PORT + 60))" CAIRN_E2E_BROWSER_PORT="$((UI_E2E_PORT + 70))" \
      bash -c 'cd ui && exec scripts/e2e-container.sh firefox'
  elif [ "$ui_ok" = 1 ] && [ "$have_binary" = 1 ]; then
    skip "ui e2e Firefox (Docker)" "no docker — Playwright's Firefox runs in an Ubuntu image"
  fi

  # Unit tests = every `test/*.test.ts`, nothing excluded — including the two suites
  # that need the binary (`e2e`, `durability`) in their default shape.
  if [ "$have_binary" = 1 ]; then
    step_async "ts unit (test/*.test.ts)" fatal "$STEP_S" \
      ts node --test --test-reporter=spec --test-timeout="$UNIT_TEST_TIMEOUT_MS" "test/*.test.ts"
  else
    skip "ts unit (test/*.test.ts)" "the binary did not build"
  fi
  # The harness's own unit tests (the chaos case table, the history and ledger
  # checkers): pure TypeScript, no binary.
  step_async "harness unit (*/*.test.ts)" fatal "$STEP_S" \
    harness node --test --test-reporter=spec --test-timeout="$UNIT_TEST_TIMEOUT_MS" "*/*.test.ts"

  # The client with the leader killed mid-call: three nodes sharing an S3 bucket —
  # same condition as the "real S3" row above.
  if [ "$have_binary" = 1 ] && [ -z "$s3_why" ]; then
    step_async "ts client killing the leader (S3)" fatal "$STEP_S" \
      with_env CAIRN_TEST_S3_URI="$s3_uri" s3_contract_env env CAIRNFLOW_CLIENT_E2E_S3=1 CAIRNFLOW_CLIENT_E2E_S3_URI="${s3_uri%/}" \
      bash -c 'cd sdk/typescript && exec node --test --test-reporter=spec --test-timeout=300000 --test-name-pattern="killing the leader" test/client.e2e.test.ts'
  else
    skip "ts client killing the leader (S3)" "${s3_why:-the binary did not build}"
  fi

  # A long chat on the real binary: 200 turns × 8 MiB history, tenant
  # max-payload=1 MiB, BIDI and RR. `direct` (store presign) only on a local
  # SeaweedFS, with `aws` to delete the run's own prefix; otherwise only `proxy`
  # (memory://).
  if [ "$have_binary" = 1 ]; then
    if [ "$s3_uri" = "s3://walbench/check" ] && command -v aws >/dev/null; then
      step_async "ts long chat 200 turns × 8 MiB (proxy + direct S3)" fatal "$STEP_S" \
        env CHAT_LONG_S3_ENDPOINT="$SEAWEED_ENDPOINT" CAIRNFLOW_BINARY="$ROOT/target/debug/cairnflow" scripts/chat-long.sh
    else
      step_async "ts long chat 200 turns × 8 MiB (proxy)" fatal "$STEP_S" \
        env CHAT_LONG_NO_S3=1 CAIRNFLOW_BINARY="$ROOT/target/debug/cairnflow" scripts/chat-long.sh
    fi
  else
    skip "ts long chat 200 turns × 8 MiB" "the binary did not build"
  fi

  # e2e_row <label> <cairnflow arguments> <SDK mode> [ms between two splits/merges] [SDK wire cap]
  #
  # The rows (and the TS unit tests above) run in the background, at most
  # `CHECK_E2E_JOBS` at once: each row has its own `cairnflow` process (`memory://` or
  # a temporary directory, `--port 0`), no shared bucket. Except when
  # `CAIRNFLOW_E2E_NODES` > 1 — then the store must be shared (AGENTS.md §5), so the
  # rows run one by one.
  e2e_row() {
    local label="$1" args="$2" mode="$3" split_ms="${4:-0}" max_wire="${5:-}"
    local name="e2e $label $mode"
    if [ "$have_binary" = 0 ]; then
      skip "$name" "the binary did not build"
      return
    fi
    if [ -n "$args" ]; then
      step_async "$name" fatal "$E2E_ROW_S" env CAIRNFLOW_E2E_ARGS="$args" CAIRNFLOW_E2E_SDK_MODE="$mode" \
        CAIRNFLOW_E2E_SPLIT_MS="$split_ms" ${max_wire:+CAIRNFLOW_E2E_SDK_MAX_WIRE=$max_wire} \
        bash -c 'cd sdk/typescript && exec node --test --test-reporter=spec --test-timeout=120000 test/e2e.test.ts'
    else
      step_async "$name" fatal "$E2E_ROW_S" env -u CAIRNFLOW_E2E_ARGS CAIRNFLOW_E2E_SDK_MODE="$mode" \
        CAIRNFLOW_E2E_SPLIT_MS=0 ${max_wire:+CAIRNFLOW_E2E_SDK_MAX_WIRE=$max_wire} \
        bash -c 'cd sdk/typescript && exec node --test --test-reporter=spec --test-timeout=120000 test/e2e.test.ts'
    fi
  }

  for mode in BIDI_STREAM REQUEST_RESPONSE; do
    e2e_row "1-node" "" "$mode"
  done
  # The real SDK pinned to an old wire version (`maxProtocolVersion`): the same e2e
  # suite, the runtime settles on v1/v2. One transport per version — the conformance
  # traces (`ts unit`) already cover both transports at every version.
  e2e_row "1-node-wire1" "" REQUEST_RESPONSE 0 1
  e2e_row "1-node-wire2" "" BIDI_STREAM 0 2
  for mode in BIDI_STREAM REQUEST_RESPONSE; do
    if [ "$CLUSTER" = 1 ]; then
      e2e_row "cluster-4p" "$CLUSTER_ARGS" "$mode"
    else
      skip "e2e cluster-4p $mode" "--no-cluster"
    fi
  done
  if [ "$CLUSTER" = 0 ]; then
    skip "e2e cluster-4p+split BIDI_STREAM" "--no-cluster"
  elif [ "$SPLIT_ROW" = 1 ]; then
    e2e_row "cluster-4p+split" "$CLUSTER_ARGS" BIDI_STREAM "$SPLIT_MS"
    # SST disk cache on (Ready must be green with the flag on in one cluster e2e
    # row): splits/merges reread SSTs, so the cache is really used.
    disk_cache_dir="$ROOT/target/check/e2e-disk-cache"
    rm -rf "$disk_cache_dir"
    e2e_row "cluster-4p+split+disk-cache" "$CLUSTER_ARGS --disk-cache-dir $disk_cache_dir" BIDI_STREAM "$SPLIT_MS"
  else
    skip "e2e cluster-4p+split BIDI_STREAM" "CHECK_E2E_SPLIT=0"
  fi
  wait_async
  billing_collect
fi

# ───────────────────────────────────────────── table

echo
echo "─── scripts/check.sh ────────────────────────────────────────────"
failed=0
for i in "${!NAMES[@]}"; do
  printf '%-4s %5ss  %-30s %s\n' "${STATUS[$i]}" "${SECS[$i]}" "${NAMES[$i]}" "${NOTES[$i]}"
  [ "${STATUS[$i]}" != FAIL ] || failed=1
done
echo "──────────────────────────────────────────────────────────────────"
if [ "$failed" = 1 ]; then
  echo "✗ some steps FAILed"
  exit 1
fi
echo "✓ no step FAILed"
