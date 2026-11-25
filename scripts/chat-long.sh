#!/usr/bin/env bash
# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: BUSL-1.1
# Long-chat e2e on the real binary: 200 turns × 8 MiB history, tenant
# max-payload=1 MiB, BIDI and RR, over the proxy and direct read paths.
#
# Ready (`scripts/check.sh`) runs it: proxy + direct when a local SeaweedFS and
# `aws` are available, otherwise proxy only (≈ 30 s per path). `direct` needs an
# S3 store that can sign URLs — by default a local SeaweedFS
# (scripts/dev-store/up.sh, localhost:18333, bucket walbench). Each run
# uses its own prefix `lv-chat-long-<seconds>-<pid>`, deleted on exit (even when
# the test fails). Do not run it while a chaos run is using SeaweedFS: deleting a
# large prefix triggers a vacuum.
#
#   cargo build -p cairn-server --bin cairnflow && scripts/chat-long.sh
#
# Variables: CHAT_LONG_S3_ENDPOINT, CHAT_LONG_S3_BUCKET, AWS_ACCESS_KEY_ID,
# AWS_SECRET_ACCESS_KEY, AWS_REGION; CHAT_LONG_NO_S3=1 runs proxy only.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENDPOINT="${CHAT_LONG_S3_ENDPOINT:-http://localhost:18333}"
BUCKET="${CHAT_LONG_S3_BUCKET:-walbench}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-dev-only-access-key}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-dev-only-not-a-secret}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export CAIRNFLOW_CHAT_LONG=1

if [ "${CHAT_LONG_NO_S3:-0}" != 1 ]; then
  command -v aws >/dev/null || { echo "chat-long: the aws CLI is needed to delete the prefix (CHAT_LONG_NO_S3=1 skips direct)" >&2; exit 2; }
  curl -s -m 2 -o /dev/null "$ENDPOINT/" || { echo "chat-long: $ENDPOINT does not answer" >&2; exit 2; }
  PREFIX="lv-chat-long-$(date +%s)-$$"
  cleanup() {
    aws --endpoint-url "$ENDPOINT" s3 rm --recursive --only-show-errors "s3://$BUCKET/$PREFIX/" || true
    echo "chat-long: deleted s3://$BUCKET/$PREFIX/"
  }
  trap cleanup EXIT
  export AWS_ENDPOINT="$ENDPOINT" AWS_ALLOW_HTTP=true AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false
  export CAIRNFLOW_CHAT_LONG_S3_URI="s3://$BUCKET/$PREFIX"
  echo "chat-long: direct on $CAIRNFLOW_CHAT_LONG_S3_URI"
fi
cd "$ROOT/sdk/typescript"
node --test --test-reporter=spec --test-timeout=3600000 test/chat-long.test.ts
