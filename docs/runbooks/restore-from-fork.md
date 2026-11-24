# Runbook — investigating and recovering with `cairn-ctl fork`

**Alert:** none. This is a tool for after an incident, when you need to look at real data without
touching it.

## What `fork` is, and what it is **not**

Code: `Ctl::fork` in `crates/cairn-server/src/ctl.rs`.

`cairn-ctl fork --to <prefix>` builds **a copy of the cluster under another prefix in the same
bucket**, without stopping the cluster:

- every range in the table is cloned from a checkpoint. The copy **reads the source's SSTs** through
  `external_dbs`, so no data is copied;
- `cluster.conf`, `cluster-version`, `rules`, `tenants`, `range-windows`, `catalog/`,
  `deployments/`, `auth/` are copied verbatim — `cluster.conf` is exactly the bytes of the table the
  fork cloned from (read once at the start), not the live version at copy time;
- the range table changes during the run (a split/merge commits while cloning) ⇒ `fork` **reports an
  error**: the copy under the target prefix is half-done. Delete that prefix and run again when auto
  split is quiet. Do not rerun in place, since the target already has objects
  (`tests/failpoints.rs::fork_during_a_split_never_loses_the_split_range`);
- owner records have their **lease scrubbed**: session `cairn-ctl-fork/0`, `released=1`, no WAL
  segments.

**Not copied:** sessions, `nodewal/` (shared WAL logs), archives, metering, capture bundles, GC
barriers.

**Written on the source:** a checkpoint on each range's DB, an audit record, and the **GC barrier**
`gc-barriers/fork-<prefix>` (RD6).

Therefore:

- **A range on the shared WAL only carries what reached L0 at snapshot time.** Frames still in the
  shared WAL log (not flushed to L0 yet; a cold range is forced to flush by frame age
  `--wal-age-flush-ms`, default 60 s) are **not** in the copy. Invocations acknowledged within that
  window may be missing or in an older state.
- **`fork` is not a backup** — the self-contained backup is `cairn-ctl backup`
  ([restore-from-backup.md](./restore-from-backup.md)). A fork lives off the source's SSTs: lose the
  source and you lose the copy.

Use `fork` to:

1. **investigate an incident** on real data: start a node on the copy, call the read APIs;
2. **rehearse a repair command** (`intent clear`, `range force-retire`, a new binary) on the copy
   before doing it on the real cluster;
3. **get information back** (journals, invocation status) when the real cluster cannot serve but the
   bucket is intact.

## Steps

```sh
export SRC=s3://<bucket>/<prefix>          # --main-store of the real cluster
export FORK=incident-$(date -u +%Y%m%d%H%M)   # a new prefix, must be empty
```

1. **GC barrier: fork sets it, you renew it.** The copy stands on the source ranges' SSTs — both the
   ranges live at fork time and the **retired ancestors** they inherit from. The real cluster's D-d
   round only considers references from live ranges **in its own table**: when a source range splits
   again and the child's compaction moves away from the parent, D-d deletes the DB the copy is
   reading. So `fork` sets the barrier `fork-<prefix>` on the source: `all` while cloning, then
   narrowed to exactly the ranges the copy's manifests list (if they cannot all be read, it keeps
   `all`, with a warning). The barrier **expires after 24 hours** (the TTL cap of every barrier); the
   output of `fork` prints its id, `protects` and the renewal command. To use the copy longer, renew
   it before it expires, with **exactly** the `protects` that was printed:

   ```sh
   curl -s -X PUT -H "Authorization: Bearer $CAIRNFLOW_TOKEN" -H 'content-type: application/json' \
     "$CAIRNFLOW_SERVER/admin/gc/barriers/fork-$FORK" \
     -d "{\"holder\": \"$USER\", \"protects\": \"<protects printed by fork>\", \"ttlMs\": 86400000, \"reason\": \"fork $FORK\"}"
   ```

   If the barrier expires while the copy is still in use, the copy can lose any SST after the next
   D-d round. A fork that failed midway leaves an `all` barrier until it expires — remove it by hand
   if you do not fork again.

2. **Fork.** Needs write access to the bucket: fork writes checkpoints on the source DBs and leaves
   an audit record.

   ```sh
   cairn-ctl --main-store "$SRC" [--wal-store <uri>] fork --to "$FORK"
   ```

   The output lists the ranges cloned, the owner records, the objects copied, the GC barrier set on
   the source, and warnings (a range in the table that has no DB yet is skipped). If the target prefix
   already has objects, the command refuses (code 3).
3. **Check the copy**, no node needed:

   ```sh
   cairn-ctl --main-store "s3://<bucket>/$FORK" verify
   cairn-ctl --main-store "s3://<bucket>/$FORK" table show
   ```

4. **Start a node on the copy.** Use another port, another `--node-id`, and **never** point it at
   the real cluster's prefix:

   ```sh
   cairnflow --main-store "s3://<bucket>/$FORK" [--wal-store <uri as for the real cluster>] \
     --cluster --port 18080 --node-id fork-1
   ```

   API keys are copied with `auth/`, so the real cluster's keys work (including the bootstrap key
   `CAIRNFLOW_ADMIN_KEY`). A node on the copy **really runs**: due timers fire, retries run and **call
   the registered deployments**. `--tick-interval-ms 0` turns off the background loops (timers,
   retries), but a range that opens can still continue invocations in progress. To be sure it only
   reads, block the network from this machine to the deployments.
5. **Investigate or rehearse:** `cairn --server http://localhost:18080 invocations`, `status`,
   `journal`; or run a repair command `cairn-ctl … --apply` on `s3://<bucket>/$FORK`, then `verify`.
6. **Clean up.** Stop the copy's node, delete the `$FORK` prefix, **then** remove the barrier
   (removing it before the delete has finished lets D-d delete DBs the copy's node may still read):

   ```sh
   curl -s -X PUT -H "Authorization: Bearer $CAIRNFLOW_TOKEN" -H 'content-type: application/json' \
     "$CAIRNFLOW_SERVER/admin/gc/barriers/fork-$FORK" -d '{"ttlMs": 0}'
   ```

   Checkpoints the fork left on the source DBs are **not** removed when the copy is deleted: the
   temporary checkpoint expires by itself after 1 hour, while the checkpoints SlateDB pins for the
   copy do not expire. It leaks space, loses nothing.

## What else

**Promoting the copy to the main cluster:** not recommended, and there is no tool for it. The copy
lacks the unflushed shared WAL part, depends on the source's SSTs, and has no sessions or barriers.
To recover the real cluster, use a bucket backup ([restore-from-backup.md](./restore-from-backup.md),
or [upgrade-and-rollback.md](./upgrade-and-rollback.md) section "Rolling back"), or repair in place
with `cairn-ctl` ([stuck-intent.md](./stuck-intent.md), [gc-and-barriers.md](./gc-and-barriers.md)).
