#!/usr/bin/env bash
# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: BUSL-1.1
#
# Starts the local S3 test store: one SeaweedFS container on 127.0.0.1:18333
# with the buckets `walbench` and `walbench-restore`. Safe to run again: it
# keeps the data and only creates what is missing. Needs Docker.
#
#   scripts/dev-store/up.sh
#   scripts/dev-store/down.sh        # stop; `down.sh -v` also deletes the data
#
# The S3 key is the fixed sample pair of `seaweed-s3.json`
# (`dev-only-access-key` / `dev-only-not-a-secret`): local test use only. The
# scripts and tests of this repository use it by default when `AWS_ENDPOINT` is
# not set, so nothing has to be exported.
#
# Variables: DEV_STORE_PORT (default 18333), DEV_STORE_BUCKETS (default
# "walbench walbench-restore"), SEAWEEDFS_IMAGE (default the `latest` tag; CI
# pins a digest).
set -euo pipefail
cd "$(dirname "$0")"

PORT="${DEV_STORE_PORT:-18333}"

command -v docker >/dev/null || { echo "dev-store: docker is not installed" >&2; exit 2; }

docker compose up -d seaweedfs

# The S3 port answers HTTP (a 403 for the missing signature counts): probe it
# from the host, because that is the address everything else uses.
printf 'dev-store: waiting for http://localhost:%s ' "$PORT"
n=0
until curl -s -o /dev/null --max-time 3 "http://localhost:$PORT/"; do
  n=$((n + 1))
  if [ "$n" -gt 60 ]; then
    echo " no answer after 2 minutes" >&2
    docker compose logs --tail 30 seaweedfs >&2 || true
    exit 1
  fi
  printf '.'
  sleep 2
done
echo " ok"

docker compose --profile setup run --rm create-buckets

echo "dev-store: SeaweedFS S3 on http://localhost:$PORT (key: dev-only-access-key / dev-only-not-a-secret)"
