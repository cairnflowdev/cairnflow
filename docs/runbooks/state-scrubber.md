# Runbook — the state scrubber reports findings

**Alerts:** `CairnflowScrubFindings` — `cairnflow_scrub_findings{check}` > 0 for more than
15 minutes. `CairnflowScrubStale` — `min(cairnflow_scrub_last_complete_age_seconds)` > 6 h for more
than 15 minutes (section [Last complete round too old](#last-complete-round-too-old)).

## Mechanism

Code: `crates/cairn-server/src/scrub.rs`, the checks in `crates/cairn-server/src/ctl.rs`
(`Ctl::verify`).

- The scrubber is `cairn-ctl verify` running **in the background** on the D-d runner (the leader of
  the range with the smallest id), every `--scrub-every-s` (600 s; `0` = off). It is a **dry run**:
  only metrics and logs, it deletes and repairs nothing.
- **The interval follows a request budget:** `--scrub-every-s` is the fastest interval. A round
  costs ~8 + 14 requests per range (manifests, checkpoints, owner records — tombstones included); the
  next round waits `max(--scrub-every-s, requests of the round / --scrub-requests-per-hour)`. The
  default budget is the cost of one round over 64 ranges every `--scrub-every-s` (6 000/h at 600 s):
  up to ~64 ranges the interval is unchanged, 128 ranges ~18 minutes, 1 000 ranges ~2.3 h, ~2 500
  ranges reach 6 h. Every round is still complete (reads every range).
  `--scrub-requests-per-hour 0` = no cap (a fixed interval).
- It does not open DBs (unlike `cairn-ctl verify` without flags). It adds two checks that a manual
  `verify` only runs when asked: (g) shared WAL logs nobody holds, (h) retired DBs that D-d should
  have deleted (only when `--gc-retired-dbs` is on).
- A finding only counts when it is **still present in two rounds in a row**: a live cluster always
  has half-done state (a parent that just left the table without its tombstone yet, the log of a node
  that just died and has not been swept yet). Each finding carries `heals` — the cluster round that
  should have healed it. Still present after two rounds means that round has had a full cycle and did
  not manage it.

| `check` | Meaning | Round that should heal it (`heals`) | Next runbook |
|---|---|---|---|
| `table` | `cluster.conf` cannot be read / is damaged | — | stop every range operation; [cluster-conf-lost.md](./cluster-conf-lost.md), [restore-from-fork.md](./restore-from-fork.md) |
| `db` | a range in the table whose DB has no manifest | the first open (no owner yet); — (owner exists: **data may be lost**) | [restore-from-fork.md](./restore-from-fork.md) |
| `external_db` | a live range stands on a DB/checkpoint that is gone | — (**damage**) | [gc-and-barriers.md](./gc-and-barriers.md), [restore-from-fork.md](./restore-from-fork.md) |
| `orphan_db` | `ranges/<id>` outside the table, no tombstone | SM-11 (owner record exists), SM-14 (no owner record) | [stuck-intent.md](./stuck-intent.md) |
| `stuck_intent` | a split/merge intent in the table for more than 100 s | — (the intent reconciliation round of SM-15 rolls it back when the coordinator is dead; not when the coordinator hangs) | [stuck-intent.md](./stuck-intent.md) |
| `orphan_checkpoint` | an orphan child's checkpoint pins SSTs of a live range | SM-14 (the child's DB still exists); — (the child's DB is gone) | [gc-and-barriers.md](./gc-and-barriers.md) |
| `unheld_wal_log` | a shared WAL log of a dead incarnation, listed by no owner record, still has objects | the dead-log sweep (NW-8) | [shared-wal-floor-stalled.md](./shared-wal-floor-stalled.md) |
| `retired_unreferenced` | a retired DB old enough, referenced by nobody, no barrier — D-d has not deleted it | D-d | [gc-and-barriers.md](./gc-and-barriers.md) |

## Diagnosis

```sh
# The same checks as the scrubber, with `heals` — reads the bucket directly, no node needed.
# (--retired-after-s = --gc-min-age-s + --scrub-every-s, only when D-d is on.)
cairn-ctl --main-store <uri> [--wal-store <uri>] --json verify --no-open --wal-logs [--retired-after-s N]
```

The D-d runner's log, each finding **once** until it disappears:

```
scrubber: <detail> — still present after two rounds (dry run, nothing repaired) check=… range=… healed_by=…
```

- `healed_by` is a background round (`SM-11`, `SM-14`, `NW-8`, `D-d`): check whether that round runs
  (`--retire-orphans-every-s`, `--gc-retired-dbs`), whether its runner is up
  (`CairnflowRangesWithoutLeader`), and what its log says about this range/log (lines with
  "keeping").
- `healed_by` is `none`: real damage or a real leak — no round will clean it up by itself. Do not
  repair by hand before reading the runbook of the `check` in the table above.
- The metric `cairnflow_scrub_rounds_total{result="failed"}` grows steadily: the round cannot read
  the bucket — look for "state scrubber failed, retrying on the next tick" in the log. Only the round's
  shared reads (`cluster.conf`, LIST of `owners/`, `ranges/`, `nodewal/`) failing past the retry limit
  fail the round.
- **Skipped entries:** when a read of one entry (an owner record, a range's manifest, a
  `ranges/<id>`, a session record or an incarnation's log) fails past the retry limit, that entry is
  skipped and counted in `cairnflow_scrub_skipped_total`; the round still finishes. Findings of the
  previous round on a skipped entry are carried over to the next round. Each round with skips logs:
  `scrubber: skipped N entries whose reads failed past the retry limit — e.g. …` (entry, check,
  error).
- `cairnflow_scrub_rounds_total{result="incomplete"}` grows: the round skipped more than 2% of the
  entries (at least 1) — **not complete**, counted neither as clean nor as having findings, and the
  gauge `cairnflow_scrub_findings` keeps the number of the last complete round. An occasional one
  under a fault profile is normal; a steady increase means the store is failing widely — look at
  `cairnflow_retry_exhausted_total{op="scan.read"}` and the store's latency/errors before reading
  anything into the findings.

## Last complete round too old

`cairnflow_scrub_last_complete_age_seconds` is the age of the latest **complete** round on the node;
only a node that has been the scrubber runner prints it (with no complete round yet, it counts from
when it became the runner). The alert takes the `min` over the cluster — the current runner.

- `cairnflow_scrub_wait_seconds` close to that age: the interval has stretched to fit the budget —
  `cairnflow_scrub_round_requests` × 3600 / `--scrub-requests-per-hour`. The cluster is too large for
  the default budget: raise `--scrub-requests-per-hour` (the cost: that many class A/B requests per
  hour on the runner), or accept findings arriving later (they only count after two rounds in a row).
  More ranges because of tombstones in `owners/` also make a round more expensive.
- `cairnflow_scrub_rounds_total{result="failed"}` or `{result="incomplete"}` grows: rounds are not
  complete — see the diagnosis section above.
- No node prints the metric: no node has been the runner yet (the smallest range has no leader —
  `CairnflowRangesWithoutLeader`), or `--scrub-every-s 0`.

## Handling

The scrubber never repairs. Each `check` has its own repair path in the matching runbook; for the two
classes no round heals (`db` with an owner, `external_db`), stop split/merge and GC (`--gc-retired-dbs`
off) before doing anything else.
