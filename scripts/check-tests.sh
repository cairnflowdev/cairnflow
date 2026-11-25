#!/usr/bin/env bash
# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: BUSL-1.1
# The list of tests that actually ran in a `scripts/check.sh` log (or any
# `cargo test`/`cargo nextest run`/`node --test` log), one line each
# `<status> <binary> <test name>`, sorted — to compare coverage between two runs:
#
#   scripts/check-tests.sh target/check/cargo_test_--workspace.log > a.txt
#   diff a.txt b.txt
#
# status: `ok` | `FAILED` | `ignored`. `<binary>` is the test binary's name
# (`split`, `cairn_server` for `--lib`, `doc:<crate>` for doctests), so libtest
# and nextest output compare with each other. `node --test` tests carry binary `ts`.
set -euo pipefail

for log in "$@"; do
  awk '
    # libtest: "     Running tests/split.rs (target/debug/deps/split-0123abcd)"
    /^ *Running (unittests )?[^ ]+ \(.*\/deps\/[^)]*\)$/ {
      bin = $0
      sub(/.*\/deps\//, "", bin); sub(/\)$/, "", bin); sub(/-[0-9a-f]+$/, "", bin)
      next
    }
    # doctest: "   Doc-tests cairn_server"
    /^ *Doc-tests / { bin = "doc:" $2; next }
    # libtest: "test name ... ok" (doctest: "test crates/x/src/y.rs - z (line 1) ... ok")
    /^test .* \.\.\. (ok|FAILED|ignored)/ {
      st = $NF
      if ($0 ~ /\.\.\. ignored/) st = "ignored"
      name = $0
      sub(/^test /, "", name); sub(/ \.\.\. .*$/, "", name)
      if (bin ~ /^doc:/) sub(/ - compile( fail)?/, "", name)
      sub(/ - should panic$/, "", name)
      print st, bin, name
      next
    }
    # nextest: "        PASS [   0.012s] ( 12/764) cairn-server::split name",
    # "        SKIP [         ] (───────) cairn-server::split name"
    /^ +(PASS|FAIL|SKIP|SIGSEGV|SIGABRT|SIGKILL|TIMEOUT|LEAK|LEAK-FAIL|FLAKY|TRY [0-9]+ PASS) +\[[^]]*\] +/ {
      line = $0
      sub(/^ +/, "", line)
      st = line; sub(/ .*/, "", st)
      if (st == "PASS" || st == "TRY" || st == "FLAKY") st = "ok"
      else if (st == "SKIP") st = "ignored"
      else st = "FAILED"
      sub(/^[^]]*\] +/, "", line)              # drop status + duration
      sub(/^\([^)]*\) +/, "", line)            # drop "( 12/764)"
      id = line; sub(/ .*/, "", id)             # cairn-server::split
      name = line; sub(/^[^ ]+ /, "", name)
      crate = id; sub(/::.*/, "", crate)
      b = id; sub(/^[^:]*(::|$)/, "", b)        # split | bin/cairnflow | empty (--lib)
      if (b == "") b = crate
      sub(/^bin\//, "", b)
      gsub(/-/, "_", b)
      print st, b, name
      next
    }
    # node --test --test-reporter=spec: "✔ name (12.3ms)" / "✖ name" / "﹣ name (skipped)"
    /^[[:space:]]*(✔|✖|﹣) / {
      line = $0
      sub(/^[[:space:]]*/, "", line)
      mark = substr(line, 1, index(line, " ") - 1)
      name = substr(line, index(line, " ") + 1)
      st = (mark == "✔") ? "ok" : (mark == "✖") ? "FAILED" : "ignored"
      if (name ~ / # /) st = "ignored"
      sub(/ # .*$/, "", name)
      sub(/ \([0-9.]+m?s\)$/, "", name)
      print st, "ts", name
    }
  ' "$log"
done | LC_ALL=C sort
