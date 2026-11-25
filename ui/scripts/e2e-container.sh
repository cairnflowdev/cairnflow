#!/usr/bin/env bash
# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: BUSL-1.1
# A browser in the Playwright container: `webkit` or
# `firefox`. Playwright's WebKit build runs only on the distros it supports (Ubuntu/Debian), the
# Firefox 157 build does not start on macOS 27 ("Could not find profile folder") — so the
# **browser** runs in Playwright's official image (`run-server`, host network), while the tests and the three
# `cairnflow` nodes run on this machine.
#
#   scripts/e2e-container.sh webkit|firefox [playwright arguments…]
#
# Needs Docker. `CAIRN_E2E_BROWSER_PORT` (default 18490) is the run-server port.
set -euo pipefail
cd "$(dirname "$0")/.."
BROWSER=${1:?webkit hay firefox}
shift
VERSION=$(node -p 'require("@playwright/test/package.json").version')
PORT=${CAIRN_E2E_BROWSER_PORT:-${CAIRN_E2E_WEBKIT_PORT:-18490}}
NAME="cairn-ui-${BROWSER}-$$"
# Do not `exec` the test command below: `exec` replaces the shell so the EXIT trap does not run and the
# run-server container lives forever, holding the port of the next run (seen in a combined Ready run).
trap 'docker rm -f "$NAME" >/dev/null 2>&1 || true' EXIT
docker run -d --rm --name "$NAME" --network host --init \
  "mcr.microsoft.com/playwright:v${VERSION}-noble" \
  /bin/sh -c "cd /tmp && npx -y playwright@${VERSION} run-server --port ${PORT} --host 127.0.0.1" >/dev/null
for _ in $(seq 1 120); do
  if (exec 3<>"/dev/tcp/127.0.0.1/${PORT}") 2>/dev/null; then break; fi
  sleep 0.5
done
case "$BROWSER" in
  webkit) CAIRN_E2E_WEBKIT_WS="ws://127.0.0.1:${PORT}/" npx playwright test --project=webkit-noauth --project=webkit-auth "$@" ;;
  firefox) CAIRN_E2E_FIREFOX_WS="ws://127.0.0.1:${PORT}/" npx playwright test --project=firefox-noauth --project=firefox-auth "$@" ;;
  *) echo "unknown browser: $BROWSER" >&2; exit 2 ;;
esac
