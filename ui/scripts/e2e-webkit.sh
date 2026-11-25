#!/usr/bin/env bash
# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: BUSL-1.1
# The small WebKit project of the console e2e: CSP and the session cookie on WebKit, the
# browser in the Playwright container — see `scripts/e2e-container.sh`.
# Needs Docker. `CAIRN_E2E_WEBKIT_PORT` (default 18490) is the run-server port.
exec "$(dirname "$0")/e2e-container.sh" webkit "$@"
