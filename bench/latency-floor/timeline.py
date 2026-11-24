#!/usr/bin/env python3
# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: BUSL-1.1

"""Cuts a `cairn_store::probe` mark file into `/invoke` round trips.

Use it with a scenario that has only ONE call in flight at a time (`xcall` at
concurrency 1): every mark between `invoke.in` and `invoke.out` belongs to that
round trip (plus rare background work — the tick).

    python3 bench/latency-floor/timeline.py probe.txt            # summary
    python3 bench/latency-floor/timeline.py probe.txt --show 3   # print 3 round trips near the median

Summary: per-round-trip median of
- the whole round trip (`invoke.in` → `invoke.out`);
- the number of durability waits (`sync.begin`…`sync.end`) and their total time;
- the number of WAL objects (`wal.put`), the PUT time, and the time the queue head waits on the window grid;
- the SDK wait (`sdk.read` → `sdk.frame`) and stream opening;
- the cross-range send time (`drain.send` → `drain.send_done`);
- the time `/invoke` spends polling after the invocation has finished (last poll − last `turn.sdk_done`).
"""

import statistics
import sys
from collections import defaultdict


def load(path):
    ev = []
    with open(path) as f:
        for line in f:
            p = line.split()
            if len(p) != 3:
                continue
            ev.append((int(p[0]) / 1e6, p[1], int(p[2])))
    ev.sort()
    return ev


def rounds(ev):
    out, cur = [], None
    for t, n, a in ev:
        if n == "invoke.in":
            cur = [(t, n, a)]
        elif cur is not None:
            cur.append((t, n, a))
            if n == "invoke.out":
                out.append(cur)
                cur = None
    return out


def pairs(r, begin, end):
    """Pairs begin/end in order (FIFO) — good enough while pairs rarely overlap."""
    open_, spans = [], []
    for t, n, a in r:
        if n == begin:
            open_.append(t)
        elif n == end and open_:
            spans.append(t - open_.pop(0))
    return spans


def summarize(r):
    t0 = r[0][0]
    total = r[-1][0] - t0
    syncs = pairs(r, "sync.begin", "sync.end")
    puts = pairs(r, "wal.put", "wal.put_done")
    cutw = [a / 1000 for _, n, a in r if n == "wal.cut_wait_us"]
    sdk = pairs(r, "sdk.read", "sdk.frame")
    opens = pairs(r, "sdk.open", "sdk.open_done")
    sends = pairs(r, "drain.send", "drain.send_done")
    reacq = [a / 1000 for _, n, a in r if n in ("engine.reacq_us", "engine.acq_us")]
    done = [t for t, n, _ in r if n == "turn.sdk_done"]
    polls = [t for t, n, _ in r if n == "invoke.poll"]
    # The last durable write of the round trip: the last `sync.end` before the last poll.
    last_sync = max((t for t, n, _ in r if n == "sync.end"), default=t0)
    poll_tail = (polls[-1] - last_sync) if polls else 0.0
    return {
        "total": total,
        "n_sync": len(syncs),
        "sync_ms": sum(syncs),
        "n_put": len(puts),
        "put_ms": sum(puts),
        "cutwait_ms": sum(cutw),
        "n_sdk_read": len(sdk),
        "sdk_ms": sum(sdk),
        "n_open": len(opens),
        "open_ms": sum(opens),
        "n_send": len(sends),
        "send_ms": sum(sends),
        "engine_wait_ms": sum(reacq),
        "n_turn": len(done),
        "n_poll": len(polls),
        "poll_tail_ms": poll_tail,
    }


# The chain of marks of one `xcall` round trip (A calls B, waits, returns):
# (name of the stage that ends at this mark, mark name, range: "A" | "B" | None).
# Matched greedily in order; `sync.end` is filtered by range so that it does not
# pick up a wait of the other range running in parallel.
CHAIN = [
    ("ingress → turn A", "drive.begin", "A"),
    ("A: open turn", "turn.running", None),
    ("A: wait for Running commit", "sync.end", "A"),
    ("A: SDK (open stream → frame)", "sdk.frame", None),
    ("A: wait for turn commit (outbox Call)", "sync.end", "A"),
    ("A: end turn (commit, no wait)", "drive.end", None),
    ("A: drainer wakes", "drain.sync", "A"),
    ("A: drainer sync (wait for turn-end commit)", "drain.sync_done", "A"),
    ("A→B: HTTP to /internal", "inbound.in", None),
    ("B: commit received message + durable wait", "inbound.out", None),
    ("B: inbound drainer → turn B", "drive.begin", "B"),
    ("B: open turn", "turn.running", None),
    ("B: wait for Running commit", "sync.end", "B"),
    ("B: SDK", "sdk.frame", None),
    ("B: wait for turn commit (output + outbox Reply)", "sync.end", "B"),
    ("B: end turn", "drive.end", None),
    ("B: drainer wakes + sync", "drain.sync_done", "B"),
    ("B→A: HTTP to /internal", "inbound.in", None),
    ("A: commit received completion + durable wait", "inbound.out", None),
    ("A: inbound drainer → turn A", "drive.begin", "A"),
    ("A: open turn", "turn.running", None),
    ("A: wait for Running commit", "sync.end", "A"),
    ("A: SDK", "sdk.frame", None),
    ("A: wait for last turn commit (output)", "sync.end", "A"),
    ("A: end turn", "drive.end", None),
    ("/invoke: poll + read fence → reply", "invoke.out", None),
]


# With the early-nudge experiment (`CAIRNFLOW_RESEARCH_EARLY_NUDGE=1`) the turn
# on the receiving side starts before `inbound.out` (200) returns — drop that
# mark from the chain (`--early`).
CHAIN_EARLY = [c for c in CHAIN if c[1] != "inbound.out"]


def chain(r, spec=None):
    """Time of each CHAIN stage, or None when the round trip does not match the chain."""
    ranges = {}
    first = next((a for _, n, a in r if n == "drive.begin"), None)
    if first is None:
        return None
    ranges["A"] = first
    i, prev, out = 0, r[0][0], []
    for label, name, who in spec or CHAIN:
        while i < len(r):
            t, n, a = r[i]
            i += 1
            if n != name:
                continue
            if who == "B" and "B" not in ranges and n == "drive.begin" and a != ranges["A"]:
                ranges["B"] = a
            if who is not None and who in ranges and a != ranges[who]:
                continue
            if who == "B" and "B" not in ranges:
                continue
            out.append((label, t - prev))
            prev = t
            break
        else:
            return None
    return out


def show(r):
    t0 = r[0][0]
    prev = t0
    for t, n, a in r:
        if n in ("engine.reacq_us", "engine.acq_us") and a < 200:
            continue
        print(f"  {t - t0:8.2f}  +{t - prev:6.2f}  {n:18s} {a}")
        prev = t


def main():
    path = sys.argv[1]
    ev = load(path)
    rs = rounds(ev)
    # Drop the first 20% of round trips (warm-up).
    rs = rs[len(rs) // 5:]
    if not rs:
        print("no round trips")
        return
    sums = [summarize(r) for r in rs]
    keys = list(sums[0].keys())
    print(f"{len(rs)} round trips (first 20% dropped)")
    for k in keys:
        vals = [s[k] for s in sums]
        print(f"  {k:16s} median {statistics.median(vals):8.2f}   p10 {sorted(vals)[len(vals)//10]:8.2f}   p90 {sorted(vals)[len(vals)*9//10]:8.2f}")
    spec = CHAIN_EARLY if "--early" in sys.argv else CHAIN
    chains = [c for c in (chain(r, spec) for r in rs) if c]
    print(f"\nconsecutive stages (median over {len(chains)} round trips matching the chain; sum of medians ≠ median of sums):")
    tot = 0.0
    if not chains:
        print("  no round trip matches the chain (try --early)")
        return
    for j, (label, _, _) in enumerate(spec):
        v = statistics.median(c[j][1] for c in chains)
        tot += v
        print(f"  {j+1:2d}. {label:48s} {v:8.2f} ms")
    print(f"      {'total':48s} {tot:8.2f} ms")
    if "--show" in sys.argv:
        k = int(sys.argv[sys.argv.index("--show") + 1])
        order = sorted(range(len(rs)), key=lambda i: sums[i]["total"])
        mid = len(order) // 2
        for i in order[max(0, mid - k // 2): mid - k // 2 + k]:
            print(f"\n── round trip {i}: {sums[i]['total']:.2f} ms")
            show(rs[i])


if __name__ == "__main__":
    main()
