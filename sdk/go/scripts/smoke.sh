#!/usr/bin/env bash
# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
#
# Smoke test of the Go SDK as a user gets it: a module outside the repository that
# requires github.com/cairnflowdev/cairnflow/sdk/go through a replace directive, built
# with the module proxy off (the SDK has no dependency to download).
#
# 1. Copies scripts/smoke/app (a service, a client and api.go, which pins the public API
#    signatures a user's code sees) into a temporary directory, `go mod init`,
#    `go mod edit -replace`, `go build`.
# 2. Each file of scripts/smoke/bad must NOT compile, with the error its `// want:` line
#    names (a shared handler writing state, state in a plain service, an internal
#    package, ...).
# 3. Starts the runtime binary with auth and request identity on, serves the app with
#    IdentityKeys set to the runtime's kid, `cairn register`, calls it through the Go
#    client and through `cairn invoke`, and checks the totals.
#
#   bash sdk/go/scripts/smoke.sh
#
# Needs `go` and the binaries: cargo build --bin cairnflow --bin cairn
# (or CAIRNFLOW_BINARY=<path to cairnflow>; cairn is looked up next to it).
set -euo pipefail

SDK="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$SDK/../.." && pwd)"
BIN="${CAIRNFLOW_BINARY:-$REPO/target/debug/cairnflow}"
CLI="$(dirname "$BIN")/cairn"
fail() {
  echo "SMOKE FAIL: $*" >&2
  exit 1
}
[ -x "$BIN" ] || fail "missing $BIN: cargo build --bin cairnflow --bin cairn"
[ -x "$CLI" ] || fail "missing $CLI: cargo build --bin cairn"
command -v go >/dev/null 2>&1 || fail "no go on PATH"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/cairn-go-smoke.XXXXXX")"
PIDS=()
cleanup() {
  for p in "${PIDS[@]:-}"; do
    [ -n "$p" ] && { kill -9 "$p"; wait "$p"; } 2>/dev/null || true
  done
  rm -rf "$WORK"
}
trap cleanup EXIT

export GOWORK=off GOPROXY=off GOFLAGS=-mod=mod GOMEMLIMIT="${GOMEMLIMIT:-1GiB}"

# ── 1. a user's module, outside the repository
mkdir -p "$WORK/app"
cp "$SDK"/scripts/smoke/app/*.go "$WORK/app/"
(
  cd "$WORK/app"
  go mod init example.com/smoke >/dev/null 2>&1
  go mod edit -require github.com/cairnflowdev/cairnflow/sdk/go@v0.0.0 \
    -replace "github.com/cairnflowdev/cairnflow/sdk/go=$SDK"
  go build -o "$WORK/app-bin" . || exit 1
  go vet . || exit 1
) || fail "the app does not build outside the repository (a changed public API? see scripts/smoke/app/api.go)"
grep -q '^require' "$WORK/app/go.mod" || fail "go.mod has no require"
if grep -v cairnflowdev "$WORK/app/go.sum" 2>/dev/null | grep -q .; then
  fail "building the app pulled in a dependency: $(cat "$WORK/app/go.sum")"
fi
echo "ok   builds outside the repository, no dependency"

# ── 2. user mistakes the types catch
for bad in "$SDK"/scripts/smoke/bad/*.go.txt; do
  name="$(basename "$bad" .go.txt)"
  want="$(sed -n 's|^// want: ||p' "$bad")"
  mkdir -p "$WORK/app/bad/$name"
  cp "$bad" "$WORK/app/bad/$name/main.go"
  if out="$(cd "$WORK/app" && go build -o /dev/null "./bad/$name" 2>&1)"; then
    fail "$name compiles; it must not"
  fi
  echo "$out" | grep -qF "$want" || fail "$name: the error does not say \"$want\":
$out"
  echo "ok   $name does not compile ($want)"
done

# ── 3. serve, register, call
ADMIN_KEY="cf_smoke_$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
# The keys go to the runtime only, through its environment.
CAIRNFLOW_ADMIN_KEY="$ADMIN_KEY" CAIRNFLOW_IDENTITY_MASTER="go-smoke-identity-master-$ADMIN_KEY" \
  "$BIN" --data-dir "$WORK/data" --port 0 --node-id go-smoke --tick-interval-ms 50 \
  >"$WORK/runtime.log" 2>&1 &
PIDS+=($!)
URL=""
for _ in $(seq 1 300); do
  URL="$(sed -n 's|.*listening on \(http://[0-9.]*:[0-9]*\).*|\1|p' "$WORK/runtime.log" | head -1)"
  [ -n "$URL" ] && break
  sleep 0.1
done
[ -n "$URL" ] || fail "the runtime did not report its address:
$(tail -20 "$WORK/runtime.log")"
for _ in $(seq 1 300); do
  curl -sf -o /dev/null "$URL/health" && break
  sleep 0.1
done
KID="$(curl -sf -H "Authorization: Bearer $ADMIN_KEY" "$URL/identity" | grep -o 'publickeyv1_[1-9A-HJ-NP-Za-km-z]*' | head -1)"
[ -n "$KID" ] || fail "GET /identity lists no key"

PORT="$("$WORK/app-bin" freeport)"
"$WORK/app-bin" serve "127.0.0.1:$PORT" "$KID" "$URL" >"$WORK/app.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 300); do
  curl -s -o /dev/null "http://127.0.0.1:$PORT/discover" && break
  sleep 0.1
done

cli() { "$CLI" --server "$URL" --token "$ADMIN_KEY" "$@"; }
reg="$(cli register "http://127.0.0.1:$PORT")" || fail "cairn register: $reg"
echo "$reg" | grep -q "SmokeCounter" || fail "cairn register did not list SmokeCounter: $reg"
echo "ok   cairn register"

total="$("$WORK/app-bin" call "$URL" "$ADMIN_KEY" k1)" || fail "the Go client call failed: $total"
[ "$total" = 5 ] || fail "the Go client got total $total, want 5"
echo "ok   Go client call: total $total"

out="$(cli invoke SmokeCounter add --key k1 --input 7)" || fail "cairn invoke: $out"
echo "$out" | grep -q '^completed' || fail "cairn invoke did not complete: $out"
echo "$out" | tail -1 | grep -qx '12' || fail "cairn invoke: total is not 12: $out"
echo "ok   cairn invoke: total 12"

if grep -q "CAIRNFLOW_UNVERIFIED" "$WORK/app.log"; then
  fail "the app warns it is unverified although IdentityKeys is set"
fi
echo "smoke: PASS"
