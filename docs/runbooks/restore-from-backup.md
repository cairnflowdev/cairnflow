# Runbook — self-contained backup and rebuilding the cluster (`cairn-ctl backup` / `restore`)

**Alert:** none. This is a periodic operation (a daily backup) and an operation for when the source
bucket is lost (a lost account, a bucket deleted by mistake, a failed provider).

Code: `crates/cairn-server/src/ctl/backup.rs`, `crates/cairn-store/src/backup.rs`.

## What a backup holds

`cairn-ctl --main-store <source> backup --to <target>`:

1. sets the GC barrier `gc-barriers/backup-<id>` (`all`) on the source, then checkpoints the DB of
   every range in the table. The barrier lasts 10 minutes and the command renews it every 2 minutes
   while it runs: a backup that dies (killed, a cron timeout, a crash) stops holding the GC within
   10 minutes. If renewals keep failing until the barrier may have expired, the command stops and
   fails before writing `BACKUP` — GC may have deleted what it still had to copy;
2. copies to `<target>/<id>/data/` **every object the checkpoints reference**: manifests, SSTs
   (including SSTs of retired ancestors that child ranges stand on through `external_dbs`), manifests
   of final checkpoints (the pins a clone leaves on its parent). Checkpoints that expire — the
   compactor's, a reader's, another backup's — belong to the source cluster: their objects are not
   copied, and they are removed from the copy's manifest (each removal writes the copy's next
   manifest). It also copies every cluster object except the skip list below, and owner records with their
   **lease scrubbed** (`session=cairn-ctl-backup/0`, `released=1`, no WAL segments). Objects are copied 16 at a time; `restore` copies back 16 at a time too, `format`
   last;
3. writes the backup manifest `<target>/<id>/BACKUP` **last**: every object, size, SHA-256;

   **One table snapshot for the whole run:** `cluster.conf` is read once in step 1 (table V); the
   checkpoints cover exactly the ranges of V, and the backup's `cluster.conf` is written from those
   same bytes. Right before the manifest, the command re-reads the table version: a split/merge that
   committed in between (owner records and other objects may already belong to the later table) ⇒
   the run is abandoned — the half-done copy has no `BACKUP` — and run again with a new id, at most 3
   times, with one warning line each time. If the table still changes after 3 runs ⇒ the command
   fails (non-zero exit code): run it again when auto split is quiet. Without this check, a backup
   taken during a split could pair the checkpoints of table V with the live `cluster.conf` V+1: after
   a restore, the two child ranges would open as empty ranges
   (`tests/failpoints.rs::backup_during_a_split_never_loses_the_split_range`);
4. removes the checkpoints and the barrier. If removal fails, the command still finishes and prints a
   warning: checkpoints expire by themselves after 6 hours, the barrier within 10 minutes.

Nothing on the source is deleted. The command writes an `audit/ctl/<ts>` record on the source.

**A backup without `BACKUP` is partial.** `restore` does not use it, `backup list` shows it as
`PARTIAL`, and `backup prune` cleans it up once it is older than 24 hours.

**Not in the backup:**

- `auth/cluster-secret`, the bootstrap key, the identity master — they live wherever you keep
  secrets;
- `sessions/`, `incarnations/`, `nodewal/` (shared WAL logs), `gc-barriers/`,
  `control/coordinator`, `control/probe/`, `shedding` — state of the source nodes.

**Limits — read before restoring:**

- **Each range only has what reached L0.** Commits still in the shared WAL log at snapshot time are
  not in the backup. A cold range is forced to flush by frame age (`--wal-age-flush-ms`, default
  60 s), so each range lags the snapshot time by at most that much. The backup manifest records
  `as_of=<range>:<ms>,…` — when each range's checkpoint was taken.
- **RPO** = the interval between two backups + at most `--wal-age-flush-ms` per range. With a daily
  backup you lose at most ~1 day + 60 s.
- **No common cut across ranges.** Each range reaches L0 at a different time, so the ranges' cuts
  differ by up to ~60 s. A loaded cluster can lose inter-range messages in flight within that window:
  - calls: range A deleted its outbox row because B acknowledged it, but B's copy was taken before B
    wrote it ⇒ the caller waits for a callee that does not exist;
  - results: the `reply` row in B's outbox is lost before A recorded the result ⇒ the caller waits
    for a callee that has already finished.

  In both cases the caller stays `suspended` forever. `cairn-ctl backup check-restore` (step 3)
  finds both; handle them in step 5.
- Clusters with a separate WAL store (`--wal-store`) are not supported yet.

## Periodic backups

```sh
cairn-ctl --main-store s3://my-bucket/cluster-1 backup --to s3://my-backups/cluster-1
cairn-ctl --main-store s3://my-bucket/cluster-1 backup list --to s3://my-backups/cluster-1
```

Deleting old backups: keep the newest backup of each day (7 days) and of each week (4 weeks). The
command dry-runs first; only `--apply` deletes, and it writes `audit/ctl` on the cluster:

```sh
cairn-ctl --main-store s3://my-bucket/cluster-1 backup prune --to s3://my-backups/cluster-1 --keep-daily 7 --keep-weekly 4
cairn-ctl --main-store s3://my-bucket/cluster-1 backup prune --to s3://my-backups/cluster-1 --apply
```

The target should be at **another provider** — losing the source account must not take the backups
with it. Two providers means two key sets and two endpoints: `--backup-env-prefix P` makes the backup
store (`--to` of `backup`/`backup list`/`backup prune`, `--from` of `restore`) use **only** the
variables `P` + `AWS_*`; `--main-store` keeps using `AWS_*`. For example, the source at one S3
provider and the target on Cloudflare R2:

```sh
# source — as the nodes use it
export AWS_ENDPOINT_URL_S3=https://s3.provider-a.example AWS_REGION=auto \
       AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=…
# target (R2) — its own set
export CAIRN_BACKUP_AWS_ENDPOINT=https://<account>.r2.cloudflarestorage.com \
       CAIRN_BACKUP_AWS_REGION=auto \
       CAIRN_BACKUP_AWS_ACCESS_KEY_ID=… CAIRN_BACKUP_AWS_SECRET_ACCESS_KEY=…
cairn-ctl --main-store s3://my-bucket/cluster-1 --backup-env-prefix CAIRN_BACKUP_ \
  backup --to s3://my-backups/cluster-1
```

- A missing `P…AWS_ACCESS_KEY_ID` or `P…AWS_SECRET_ACCESS_KEY` (absent or empty), a `P…AWS_<X>`
  variable whose `<X>` is not an S3 key (a typo), or a URI that is not `s3://` ⇒ exit code 2 before
  any request, and the error names the **variable**. It never borrows the source's `AWS_*`.
- Wrong target keys ⇒ the target refuses at once on the first LIST (code 4,
  `SignatureDoesNotMatch` / `InvalidAccessKeyId`), **before** `backup` sets a barrier or a checkpoint
  on the source: nothing changes on the source.
- Secrets travel only through the environment, never in the URI or on the command line (`ps`, and
  `audit/ctl` records the whole command line).
- Without the flag: as before, the target uses the same `AWS_*` as the source — only use this when
  both stores share one key set.
- A target with the same bucket name + prefix as the source (or nested in it) is refused even with a
  different endpoint: the command cannot tell whether two endpoints are one store — name the target
  bucket differently.

## Rebuilding the cluster from a backup

1. **A new store, with the source's prefix.** Create a new bucket. The new cluster's `--main-store`
   must carry **the same prefix** as the source cluster (`cluster-1` in the example above), because
   SlateDB manifests record the absolute paths of ancestor DBs. With a wrong prefix, `restore`
   refuses and says why.
2. **Copy back:**

   ```sh
   cairn-ctl --main-store s3://new-bucket/cluster-1 restore --from s3://my-backups/cluster-1
   # target at another provider (its own set, as above): add --backup-env-prefix CAIRN_BACKUP_
   ```

   Without `--id` the command takes the newest **complete** backup; for another one use `--id <id>`
   (take the id from `backup list`). The command checks the size and SHA-256 of every object, and
   writes `format` last. The command **refuses** (before writing anything) a backup whose table lists
   a range that has an owner record but whose DB is not in the backup — such a backup was taken while
   a split/merge committed, by a binary without the single-snapshot check; restoring it would open
   that range empty. Pick another backup
   (`tests/backup.rs::restore_refuses_a_backup_whose_table_lists_a_range_without_db`). A target with a
   half-done copy has no `format` but has `ranges/`, so a node refuses to open it. If it fails midway,
   wipe the target and run again.
3. **Check** (before starting nodes):

   ```sh
   cairn-ctl --main-store s3://new-bucket/cluster-1 backup check-restore
   ```

   This is `verify` opening every DB plus the check for lost inter-range messages. Only go on with
   exit code 0. The findings `restore.lost_call` / `restore.lost_reply` name the exact caller, callee
   and completion — handle them in step 5. Any other finding (a DB that cannot open, a broken table):
   stop and pick another backup.
4. **Start the nodes** on `s3://new-bucket/cluster-1`. The cluster secret
   (`CAIRNFLOW_CLUSTER_SECRET`, ≥ 32 bytes) and the bootstrap key come from where you keep secrets,
   **not** from the backup.
5. **Handle calls that lost messages** reported by step 3 (see the limits above). `check-restore`
   finds them automatically (`crates/cairn-server/src/ctl/restore_check.rs`; tests
   `tests/failpoints.rs::restore_finds_a_lost_call`, `::restore_finds_a_lost_reply`):
   - `restore.lost_call`: the caller waits for the result of a callee that is not in the backup;
   - `restore.lost_reply`: the callee finished but its result is nowhere.

   A finding with `(healed by: not checked)` is a range too large to scan completely: inspect that
   range by hand with the five queries below. Every query is a `POST /query` (`{"sql": "…"}`) with a
   cluster-admin key; the test `tests/backup.rs::runbook_queries_find_a_waiting_call` runs exactly
   these queries on a cluster with a waiting call.

   1. Hanging invocations:
      `SELECT id FROM sys_invocation WHERE status = 'suspended'`
   2. For each id there, calls without a result yet (`entry_lite` is JSON; `invocationId` is the
      callee):
      `SELECT "index", entry_lite FROM sys_journal WHERE id = '<caller>' AND name = 'Call' AND entry_lite LIKE '%"completed":false%'`
   3. Does the callee exist, and in which status:
      `SELECT status FROM sys_invocation WHERE id = '<callee>'`
   4. Is the call still waiting to be delivered:
      `SELECT kind, completion_id FROM sys_outbox WHERE id = '<caller>'`
   5. Is the result still waiting to be delivered:
      `SELECT id, completion_id FROM sys_outbox WHERE kind = 'reply'`

   The caller lost a message when query 4 has no row for that call, and:
   - query 3 returns no row — the call never reached the callee; or
   - query 3 returns a finished status (`completed`, `failed`) and query 5 has no row for the
     caller — the result never came back.

   Handle each caller:
   - **restart-as-new from before the call**: `POST /invocations/<id>/restart?from=<index of the
     Call entry>`. The new copy's journal stops before `Call`, so the call is made again. Earlier
     `ctx.run` results are reused. In the "callee already finished" case, the callee **runs again**
     under a new id: only use this when the callee's handler tolerates running again (idempotent);
   - **cancel** (`POST /invocations/<id>/cancel`) when it cannot run again, then fix things by hand on
     the business side.
6. **Purge deleted tenants again:** a backup is not rewritten when a tenant is purged, so a backup
   older than the purge brings that tenant **back to life**. **Before** pointing DNS, for every tenant
   you have deleted (your own records, or `state=purged` in the old cluster's `tenants` registry):

   ```sh
   curl -X PUT -H "Authorization: Bearer $PROVISION_KEY" -d '{"state":"deleting"}' \
     https://<new cluster>/admin/tenants/<t>/state
   ```

   The tenant's keys and sessions are blocked at once (`403 tenant_deleting`); the data is really
   deleted after `--tenant-purge-keep-s` (default 7 days) — choose your backup retention and this
   delay so that together they stay within any deletion commitment you have. Progress:
   `GET /admin/tenants/<t>/purge`.
7. Point DNS/the endpoint at the new cluster. The old cluster (if it still exists) must be stopped for
   good: both taking work means running everything twice.

## Rehearsal

Rehearse regularly (for example every quarter): run steps 1–5 into a temporary bucket, run a sample
invocation, then delete the temporary bucket.
