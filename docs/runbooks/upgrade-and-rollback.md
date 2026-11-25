# Runbook — upgrading the binary and rolling back

**Alert:** none yet. The code does not export metrics for `cluster-version` or the `upgrading` lock
(see [Missing metrics](./README.md#missing-metrics)), so the rule "`upgrading` for more than N
minutes" cannot be written yet. Until then, poll the endpoint (section *Watching the `upgrading`
lock* below).

**Dashboard:** the *cluster-version migration* panel only points at the endpoint.

The full procedure is in [`docs/self-hosting.md`](../self-hosting.md#upgrading) §7 "Upgrading". This
runbook is the short version to follow, plus what to do when it gets stuck.

## Mechanism

Code: `crates/cairn-store/src/migrate.rs`.

- The bucket carries a **cluster version**, the object `cluster-version` (absent means 0). A binary
  carries an ordered migration registry, and each step brings the cluster up by exactly one number.
  `DATA_FORMAT` is frozen at 10 (D27).
- **The cluster tier** (objects such as `rules`, `catalog/`, `tenants`): the **first** new node to
  start takes the lock `upgrading=<from>-><to>@<node>/<inc>` on `cluster-version`, runs the missing
  steps, writes `version` after each step, then removes the lock.
- **The range tier** (rows in a DB): the range owner runs it lazily, when the range opens on a new
  node. The mark is `data=` on `owners/range-<id>`.
- **While the lock is held:** split/merge returns `503`, the retired-DB GC round skips its turn
  (`blocked` with reason `upgrading`), and auto split reports `blockedBy: "upgrading"`.
- Only clusters mixing **two consecutive versions** (N−1 ↔ N) are supported. After `version` is
  bumped, old nodes cannot open new ranges, and when they meet a new object they **stop loudly**
  instead of overwriting it.

## Rolling upgrade

```sh
export CAIRNFLOW_SERVER=http://<node>:8080 CAIRNFLOW_TOKEN=<cluster-admin key>
cv() { curl -s -H "Authorization: Bearer $CAIRNFLOW_TOKEN" "$CAIRNFLOW_SERVER/admin/cluster-version"; }
```

1. **Back up the bucket**, or at least `rules`, `catalog/`, `tenants`, `cluster.conf`,
   `cluster-version`. There are no reverse migrations: once `version` is bumped, the only way back is
   this backup.
2. Record the state: `cv | jq '{version, requiresKnows, upgrading, binary, nodes}'`.
3. Upgrade **one node at a time**. Send `SIGTERM` (graceful shutdown, peers take the ranges after
   ~0.4 s), replace the binary, then start it. The first new node logs `cluster-tier migration
   starting`, `running cluster migration step` (one line per step), then `cluster-tier migration
   done`.
4. Wait until `cairn cluster` shows a leader for every range (`CairnflowRangesWithoutLeader` does not
   fire) before shutting down the next node.
5. After the last node, `cv` must show `upgrading: null` and `version` = `binary.knows`, and every
   `nodes[].knows` must be equal.

## Watching the `upgrading` lock (in place of the missing alert)

`upgrading` carries no start time, so it can only be measured by polling repeatedly:

```sh
# Prints one line per minute; a lock still there after N polls in a row is stuck.
N=10; n=0
while sleep 60; do
  u=$(cv | jq -c '.upgrading')
  if [ "$u" = "null" ]; then n=0; else n=$((n+1)); fi
  echo "$(date -u +%FT%TZ) upgrading=$u polls=$n"
  [ "$n" -ge "$N" ] && echo "STUCK: upgrading still held after $N minutes" >&2
done
```

The only cluster-tier step that runs at startup (`rules/2`, `version` 1) touches one object; the
other steps are gates turned on by `cairn-ctl` or range-tier steps. So the lock lives a few seconds.
A lock still held after a few minutes is already abnormal.

## When the lock is stuck

`upgrading.by` is the `<node>/<inc>` of the runner.

1. **The runner is alive and running:** its log has `running cluster migration step` but no
   `cluster-tier migration done` yet. Wait, and read the log for the step's error.
2. **The runner is dead** (the process is gone, or its session has expired): the lock is **not
   removed automatically**. Only a **newly starting** node takes the lock over. Start (or restart) a
   node running the new binary: it sees the holder is dead, logs `the previous migration runner died
   — taking over`, then runs **one** step again (every step is idempotent).
3. **A step returns an error:** the node does not start and the lock stays. Read the error in the
   node's log. Fix the cause (usually the store: permissions, network), then restart the node.
4. A starting node reports `… newer than this binary understands … upgrade this binary`
   (`NewerFormat`): the bucket is at a higher version than this binary knows (`requiresKnows` >
   `knows`). Use a newer binary.

**Do not:** edit `cluster-version` by hand (removing `upgrading`, lowering `version`). A cluster-tier
step may have run halfway; the next runner relies on the lock and `version` to know which step to run
again.

## Rolling back

- **Before `version` is bumped**, i.e. while `requiresKnows` is still ≤ the old binary's `knows`: put
  the old binary back, rolling as for the upgrade.
- **After `version` is bumped:** the old binary refuses to start. Roll back with the backup from
  step 1. Every invocation written after the backup is lost. Before restoring, consider
  [restore-from-fork.md](./restore-from-fork.md) to investigate the current bucket without touching
  it.
- While the cluster mixes binaries, old nodes answer `500` with a `NewerFormat` message for operations
  touching a new object (for example `POST /rules`). That is the correct behaviour; finish the upgrade
  instead of leaving the cluster mixed for long.
