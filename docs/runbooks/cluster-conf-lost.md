# Runbook — `cluster.conf` (the range table) is lost

**Alert:** none. The node **refuses to start** and prints:

```
cluster.conf is missing at <prefix>/cluster.conf but the bucket already holds range data …
rebuild it from cluster-history/ with `cairn-ctl table restore` (dry run first, then --apply), or
restore the last version of cluster.conf from bucket versioning. See docs/runbooks/cluster-conf-lost.md
```

Code: `ClusterConfig::open` (`crates/cairn-store/src/cluster.rs`), `StoreError::MissingClusterConfig`,
the history in `crates/cairn-store/src/cluster_history.rs`, the command in
`crates/cairn-server/src/ctl/table_restore.rs`; invariants SM-21, SM-22, SM-23
(`docs/guides/split-merge.md`).

## Symptoms

- Every node starting on that prefix exits at once with the error above, including a node that was
  running normally before it restarted. Running nodes see nothing yet: they keep the table in RAM
  and only reload it periodically. Their next split/merge fails ("cluster.conf vanished from the
  bucket").
- `cairn-ctl verify` reports a `table` finding "no cluster.conf"; other `cairn-ctl` commands report
  "no cluster.conf at … — wrong --main-store/--prefix?".

## Cause

`cluster.conf` is the range table: which range covers which key span, `next_range_id`, the version.
It is the only cluster object that **cannot be rebuilt** from the rest of the bucket — owner records
(`owners/range-<id>`) do not carry the key span. The object disappears when it is deleted from
outside cairn: by an operator, a bucket lifecycle rule (expiry by age, storage class transition), a
store bug.

A node that finds a bucket without a table creates a new table only when the bucket has no
`ranges/`, `partition-*` or `owners/` (SM-21); otherwise it fails at startup as above. Creating a new
table over old data would hand out range ids again that collide with live `ranges/<id>` directories
and leave holes where ranges were split — state and acknowledged invocations that nobody can find
any more.

## Recovery

**Do not start any node with a new table** — do not delete `ranges/`/`owners/` so the node "comes
up", do not point the node at another prefix and copy the data over. Both lose customer data. Do not
edit the table's content by hand.

### 1. Stop every node

Including nodes still running: they may CAS the table (split, merge) or write table history while
you recover.

### 2. Dry-run `table restore`

Every time the table changes (cluster creation, every won CAS of a split/merge/roll-back), the node
writes the exact bytes of the new table to `cluster-history/<version>` (SM-22; the 16 newest
versions, older ones go to the trash). `table restore` takes the highest version and **proves it is
the last one** from the rest of the bucket before writing anything:

```sh
cairn-ctl --main-store s3://<bucket>/<prefix> table restore
```

The dry run prints the version it would rebuild (`cluster.conf from cluster-history/<V>`), every
proof line that passed (`proof: …`), the table, and a warning if sessions have not ended. It writes
nothing.

The proofs (SM-23):

| `proof:` line | Meaning |
|---|---|
| `N owner records: none with id >= next_range_id X` | no owner record carries an id that only a table after V could have handed out |
| `N live ranges of version V: each has an owner record without a tombstone` | no live range of V was retired by a split/merge after V |
| `N tombstones: none in version V` | no tombstoned range is still in V or promised by an intent of V |
| `N ranges/ directories: none with id >= next_range_id X` | no DB was cloned after V |
| `N split/merge ledgers: consistent with version V` | no `ops/<key>` ledger is `committed` while its sources are still in V, or `cleaned` while V still carries its intent |
| `ops/<key> is prepared and every child has a Db and an owner record, every source is sealed — … restoring the committed table (roll forward), not the intent` | V carries the intent of a split/merge whose preparation finished: the commit may have won after V (the coordinator died before writing the history) and the children may have taken writes. The command writes **the committed table** (version V + 1) instead of V, together with its history copy and a `committed` ledger — rebuilding V with the intent would let a failed ledger read or a later `intent clear` roll the op back and move the children's DB (with acknowledged writes) to the trash |
| `ops/<key> is prepared but a child or a source seal is missing — … keeping the intent` | the op cannot have committed: keep the intent; nodes roll it back at startup like any half-done op |

After the rebuild, **do not** `cairn-ctl intent clear` an intent without reading carefully: without
`--force`, the command refuses an op whose preparation finished and whose commit may have won
(`ops/<key> is prepared … the commit may have won`).

### 3. Write

```sh
cairn-ctl --main-store s3://<bucket>/<prefix> table restore --apply
```

Writes `cluster.conf` with `Create` (the exact bytes of the history copy, overwriting nothing) and
leaves `audit/ctl/<ts>`. `--apply` refuses while sessions have not ended (`sessions/`): a node killed
by `kill -9` or a crash leaves a session `live` forever (no node is left to mark it expired). Check
with `cairn-ctl cluster status` and with your infrastructure that **every** node is stopped, then add
`--force`.

### 4. Check, then start

```sh
cairn-ctl --main-store s3://<bucket>/<prefix> verify --deep
```

No `table` finding, no new `orphan_db`, no `sst_missing`. Then start the nodes again.

### Reading a refusal

Every refusal exits with code 3 (`refused: …`) and writes nothing.

| Error | Meaning | What to do |
|---|---|---|
| `cluster.conf exists at … — nothing to restore` | the table is still there (or someone just put it back) | `table show`, `verify`; no restore needed |
| `no range table history under …/cluster-history` | the cluster has not changed its table since running a binary that writes the history, or the history is lost too | take the versioning path below |
| `cannot prove that cluster-history/V is the last range table` + a list | the history lags the lost table: a node died between the table CAS and the history write (`cairnflow_cluster_history_failures_total{step="write"}` grew earlier), or the history belongs to another timeline (`step="conflict"`) | **do not** rebuild V; take the versioning path. Each line names one mismatch: `owners/range-<id> exists … next_range_id` (the table handed out an id after V), `range <id> is live … tombstone` (the range was retired after V), `ops/<key> is committed but its source ranges are still live` (committed after V), `… has no owner record` (a range of V was cleaned up for good, or never opened) |
| `sessions not ended: …` | a node may still be running | stop it; once sure, `--force` |
| `cluster-history/V cannot be decoded` / `holds table version …` | the history copy is damaged or not cairn's | take the versioning path; keep the object as is for investigation |
| `writing cluster.conf failed (it may or may not have been written …)` | a store error on the `Create` | `table show`: if a table is there, check it and you are done; otherwise run again |

### When `table restore` refuses: bucket versioning

1. **Find the last version of `cluster.conf` with bucket versioning** (S3: `aws s3api
   list-object-versions --bucket <b> --prefix <prefix>/cluster.conf`; if there is a delete marker,
   the version right before it is the last one). The last version is the one with the largest
   `table_version`. The mismatch lines that `table restore` prints are a hint: the last version must
   have a `next_range_id` larger than every id they mention.
2. **Put back the exact bytes of that version** at `<prefix>/cluster.conf` (remove the delete
   marker, or copy the old version over). Do not edit the content by hand.
3. Run `cairn-ctl … table restore`: it now answers `cluster.conf exists`. Run `verify --deep`: no
   `table` finding, no new `orphan_db`. If `ranges/` has ranges the table does not know
   (`orphan_db`), the version you put back **is not the last one** — stop and find a newer one.
4. Start the nodes again. The table history continues from the next table change; history copies
   with the same version but different bytes (the old timeline) are kept as is and counted under
   `step="conflict"` — delete them from `cluster-history/` by hand once you are sure the table is
   right, so that a later `table restore` does not pick the wrong one.

**The bucket has no versioning and `table restore` refuses:** there is no self-service recovery path.
Do not start any node; keep the bucket as it is and contact the developers. A backup
(`restore-from-backup.md`) can rebuild the cluster as of the snapshot time (a backup does not carry
`cluster-history/`: the restored cluster writes its own history).

## Prevention

- Turn on versioning for the cluster's bucket; it covers **every** single object, not only
  `cluster.conf` — `cluster-history/` only covers the range table
  ([self-hosting §7](../self-hosting.md#incident-recovery-cairn-ctl)).
- Bucket lifecycle rules must not touch the cluster's prefix (except cleaning up expired noncurrent
  versions of overwritten objects, if you accept that window).
- Watch `cairnflow_cluster_history_failures_total`: a growing `step="write"` means the history is
  missing a version — `table restore` will refuse until the next table change.
- Run `cairn-ctl verify --deep` periodically (it is not in the background scrubber: one LIST per
  DB): it finds missing SSTs (`sst_missing`) before a read hits them.
