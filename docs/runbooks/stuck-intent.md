# Runbook — a hung split/merge intent

**Alert:** no direct rule yet. The code does not export the intent's age (see
[Missing metrics](./README.md#missing-metrics)). The two closest rules:

- `CairnflowRangesWithoutLeader`: the child ranges of a half-done split/merge have no leader, or any
  range has had no leader for more than 2 minutes;
- `CairnflowAutoSplitFailed`: an automatic split wrote its intent and then failed (and was rolled
  back).

Until the metric exists, check periodically with `cairn-ctl verify` (section below).

## Mechanism

Code: `crates/cairn-server/src/split/`, `crates/cairn-server/src/ctl.rs`; the D24 protocol and the
SM-* invariants are in [`docs/guides/split-merge.md`](../guides/split-merge.md).

- Split and merge start with a CAS that writes an **intent** on the range table (`cluster.conf`):
  `splitting(at=…,left=…,right=…)` on the parent range, or `merging(with=…,into=…)` on both source
  ranges. The coordinator then flushes, quiesces, clones, takes ownership of the child ranges, and
  CASes the commit. A live split holds its intent for a few seconds: the measured write pause is
  0.3–0.7 s, and quiescing is capped at 10 s.
- **Automatic roll-back:** if the coordinator dies midway, whichever node takes the parent range back
  sees the intent and rolls it back: it removes the intent first, then cleans up the child ranges' DBs
  and owner records. The log line is `rolled back intent — cleaning up`.
- An intent **hangs** when nobody takes the parent back to roll it back: the coordinator is alive but
  stuck, or the whole cluster went down. While it hangs:
  - splitting/merging the range that carries the intent again returns `409`, and auto split reports
    `blockedBy: "intent"` for every range (the whole cluster may only have one half-done split/merge);
  - the `/admin/tick` tick waits up to 10 s and then returns `503`.

## Diagnosis

While the cluster is still running:

```sh
cairn ranges        # the range table: a range carrying an intent shows it in the last column
cairn cluster       # who holds which range; child ranges without a leader yet
```

With no node running:

```sh
cairn-ctl --main-store <uri> [--wal-store <uri>] table show   # the intent on each range
cairn-ctl --main-store <uri> owners                           # owners, whether their session is still live
cairn-ctl --main-store <uri> verify                           # (e) intent hung for more than 100 s
```

`verify` exits with code 1 when it finds problems, so cron can run it: `--json` for machines, and
`--no-open` skips opening each DB (faster). The "hung" threshold is 100 s, i.e. 10× the quiesce cap.

Read `owners`: does the owner of the parent range (and of the child ranges) still have a `live`
session? If so, the coordinator may still be running. Look at that node's log before doing anything.

## Handling

1. **The owner is alive but stuck:** restart that node with `SIGTERM` (graceful shutdown). Another
   node takes the parent back and rolls the intent back by itself. Confirm with `cairn ranges`.
2. **Nobody takes it back** (the whole cluster is down, or the node holding the parent died and
   nobody has marked its session `expired` yet): roll back with `cairn-ctl`. The default is a **dry
   run**:

   ```sh
   # 1. Preview: prints the table before/after and what would be cleaned up. Writes nothing.
   cairn-ctl --main-store <uri> intent clear <range>
   # 2. Write, only if the table has not changed since the preview:
   cairn-ctl --main-store <uri> intent clear <range> --expect-version <N> --apply
   ```

   - For a merge, `<range>` is either of the two source ranges; the intent is removed from both.
   - The command does exactly what a node's automatic roll-back does: one CAS that drops the intent,
     then cleaning up the DB and `owners/range-<id>` of the child ranges. It leaves `audit/ctl/<ts>`
     on the bucket (who — `--who`, default `$USER@$HOST` —, the command, before, after).
   - The command **refuses** (exit code 3) while the owner of the parent or a child range still has a
     `live` session. Only add `--force` when you are sure that node is dead: `--force` removes the only
     safety net between the tool and a live node, and two writers of the table corrupt the table.
   - If `--expect-version` does not match, the command refuses: the table just changed, so run step 1
     again.
3. After the roll-back: `cairn-ctl verify` must say `CLEAN`, and `cairn cluster` must show a leader
   for every range. To split again, call `cairn ranges split …` again.

**Do not:** edit `cluster.conf` by hand. The table is CASed by every node; editing it by hand without
`cairn-ctl` loses both the checks and the audit.

## Related

- [gc-and-barriers.md](./gc-and-barriers.md): `range force-retire` for a range that left the table
  without a tombstone.
- [`docs/self-hosting.md` §7 "Incident recovery: `cairn-ctl`"](../self-hosting.md#incident-recovery-cairn-ctl)
  (exit codes, command table).
