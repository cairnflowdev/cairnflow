#!/usr/bin/env bash
# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: BUSL-1.1
#
# Stops the local S3 test store of `up.sh`. `down.sh -v` also deletes its data.
set -euo pipefail
cd "$(dirname "$0")"
docker compose --profile setup down "$@"
