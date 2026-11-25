#!/usr/bin/env python3
# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: BUSL-1.1

"""Table from the `$OUT/<tag>/xcall-c<N>.json` directories written by run.sh.

    python3 bench/latency-floor/summary.py $OUT

One line per run: tag, inv/s, p50 (from the load.ts log), WAL PUT/s, WAL PUT
per call, other PUT/s, read-fence GET per call.
"""
import json
import os
import re
import sys

out = sys.argv[1]
rows = []
for tag in sorted(os.listdir(out)):
    d = os.path.join(out, tag)
    if not os.path.isdir(d):
        continue
    for f in os.listdir(d):
        j = json.load(open(os.path.join(d, f)))
        delta, s, ok = j["delta"], j["elapsedS"], max(j["ok"], 1)
        wal = sum(v for k, v in delta.items() if k.startswith("cairnflow_testkit_store_requests_by_source_total") and 'source="nodewal.stripe"' in k and 'op="put"' in k)
        puts = sum(v for k, v in delta.items() if k.startswith("cairnflow_testkit_store_requests_total{") and 'op="put"' in k)
        fence = sum(v for k, v in delta.items() if 'source="session.read_fence"' in k)
        log = open(os.path.join(out, tag + ".log")).read()
        m = re.search(r"✓ (\d+) [^·]*· ([\d.]+) inv/s · p50 ([\d.]+)ms", log)
        inv, p50 = (float(m.group(2)), float(m.group(3))) if m else (j["ok"] / s, float("nan"))
        rows.append((tag, inv, p50, wal / s, wal / ok, (puts - wal) / s, fence / ok))
print(f"{'tag':34s} {'inv/s':>7s} {'p50 ms':>8s} {'WAL PUT/s':>10s} {'PUT/inv':>8s} {'other PUT/s':>11s} {'fence GET/inv':>13s}")
for r in rows:
    print(f"{r[0]:34s} {r[1]:7.1f} {r[2]:8.1f} {r[3]:10.1f} {r[4]:8.2f} {r[5]:11.1f} {r[6]:13.2f}")
