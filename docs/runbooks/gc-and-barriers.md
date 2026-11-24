# Runbook — retired-DB GC and GC barriers

**Alert:** `CairnflowGcClockSkew` (the clock-skew gate blocks a deletion round — see *Mechanism*).
Apart from it, the D-d round only exports `cairnflow_gc_skipped_total{stage}` (ranges kept because a
read or a delete failed, `plan`/`delete`; see [Missing metrics](./README.md#missing-metrics)), so the
rule "GC blocked for too long" cannot be written yet. Poll with `GET /admin/gc/plan` (see
*Diagnosis*).

**Dashboard:** the *Retired-DB GC* panel only points at the endpoint.

## Mechanism

Code: `crates/cairn-server/src/gc.rs`, `crates/cairn-store/src/gc_barrier.rs`. The GC invariants
are in [`docs/guides/split-merge.md`](../guides/split-merge.md) (SM-9).

- After a split/merge, the parent range **retires**: it carries a tombstone, but its DB stays on the
  bucket, because the child ranges read the parent's SSTs through `external_dbs`.
- `--gc-retired-dbs` turns the D-d round on (default **off**). `--gc-retired-dbs=plan` only logs the
  plan and deletes nothing. The interval is `--gc-every-s` (600); the first round runs a few seconds
  (5 s) after the node comes up or just becomes the runner, without waiting a whole interval. The
  minimum tombstone age is `--gc-min-age-s` (1800). The runner is the leader of the range with the
  smallest id.
- Each round **makes a plan**: every retired range that still has a DB is either eligible for
  deletion or blocked, with `by` one of:

  | `by` | Meaning | Clears by itself? |
  |---|---|---|
  | `age` | the tombstone is younger than `--gc-min-age-s` | yes, with time |
  | `reference` | a live range still lists this DB in `external_dbs`, or it cannot be proven that none does | when the child range's compaction stops reading the parent's SSTs |
  | `barrier` | an unexpired barrier covers this range, or a barrier **cannot be read** | when the barrier expires or is removed |
  | `upgrading` | the cluster is migrating: the whole round skips its turn | when the lock is removed ([upgrade-and-rollback.md](./upgrade-and-rollback.md)) |
  | `unreadable` | reading the tombstone, manifest or DB size of **this range** failed: kept, other ranges are still considered | when the store reads again; a `cairnflow_gc_skipped_total{stage="plan"}` that grows forever is a really damaged object |

- Before deleting each range the runner reloads the table and the barriers. If the table changed it
  drops the whole pass; a newly set barrier keeps that range. Every ambiguous case chooses **keep**
  (AGENTS.md rule 5). A failed deletion step of one range keeps that range, counts
  `cairnflow_gc_skipped_total{stage="delete"}`, and the round moves on; only a failed shared read
  (the table, the LIST of `owners/`, the barriers, the reference set) fails the whole round.
- **The clock-skew gate:** every age (tombstones, barriers, trash entries) is measured by the
  runner's clock. Before the first delete of a round the runner overwrites the probe object
  `gc-clock-probe` (not data), HEADs it, and compares `last_modified` with its own clock. Skew larger
  than `--gc-min-age-s`/4 (the trash round: `keep`/4; floor 2 s), or no measurement ⇒ the round
  **deletes nothing**, log `warn` "GC: dropping every deletion of the round (clock-skew gate)",
  `cairnflow_gc_clock_blocked_total{round}` grows (`dbs`, `orphans`, `trash`), alert
  `CairnflowGcClockSkew`. `cairnflow_gc_clock_skew_ms` is the latest measurement. Fix: synchronise
  the node's time (NTP/chrony: `chronyc tracking`, `timedatectl`); the next round opens again by
  itself. Do not lower `--gc-min-age-s` to get past the gate — the threshold drops with it, and so
  does the tombstone-age safety margin.
- **Barriers** (`gc-barriers/<id>`): something outside the round (backup, export, `fork`, an
  operator) holds the DBs of some or all ranges for a bounded time. The TTL is mandatory, at most
  24 hours; to hold longer, renew periodically. An expired barrier is ignored at once, and the round
  runner deletes its object once it has been expired for over an hour (`BARRIER_REAP_GRACE`: the
  DELETE is unconditional, so the grace keeps a renewal right at the deadline from being deleted). A
  barrier that cannot be read blocks every range, and **nobody deletes it automatically**.

## Diagnosis

```sh
export CAIRNFLOW_SERVER=http://<node>:8080 CAIRNFLOW_TOKEN=<cluster-admin key>
api() { curl -s -H "Authorization: Bearer $CAIRNFLOW_TOKEN" "$CAIRNFLOW_SERVER$@"; }

api /admin/gc/plan | jq '{tableVersion, bytes, eligible, blocked, expiredBarriers, upgrading}'
api /admin/gc/barriers | jq '.'
```

- `eligible`: ranges the next round will delete, with `bytes` and `ageMs`.
- `blocked[]`: `{range, by, detail}`. For `barrier`, `detail` names the barrier and its holder.
- No running node needed: `cairn-ctl --main-store <uri> ranges retired` lists tombstoned ranges,
  whether their DB still exists, and which live ranges still hold that DB.

A plan that stays the same over many rounds means **blocked for too long**. Read `by`:

- `reference` lasting for hours: a live range's manifest still lists the parent DB in
  `external_dbs`, because its compaction has not rewritten all inherited SSTs yet. `detail` names
  that range. It leaks space, loses nothing, and needs no urgent action.
- `barrier`: see who holds that barrier (`holder`, `reason`, `expiresMs`). A barrier of a finished
  job can be removed (section below).
- `upgrading`: see [upgrade-and-rollback.md](./upgrade-and-rollback.md).

`cairnflow_gc_unproven_candidates` (gauge, only on the round runner): the number of ranges in
`eligible` for which the latest executing round **could not rebuild** a `DeleteProof` right before
deleting — the DB is kept, not deleted. A non-zero value in a single round is normal (a barrier just
set, a tombstone just changed). Non-zero over many rounds in a row means the plan and the proof gate
disagree: the log "D-d: no longer provable after the check — keeping the Db" gives the reason for
each range.

## Setting, renewing and removing a barrier

```sh
# Hold the DBs of every range for 6 hours (for example during a bucket backup):
curl -s -X PUT -H "Authorization: Bearer $CAIRNFLOW_TOKEN" -H 'content-type: application/json' \
  "$CAIRNFLOW_SERVER/admin/gc/barriers/backup-2026-10-12" \
  -d '{"holder": "ops@host", "protects": "all", "ttlMs": 21600000, "reason": "bucket backup"}'

# Only some ranges: "protects": "ranges:4,7" or [4, 7]
# Renew: PUT the same id again with a new ttlMs (at most 86400000).
# Remove: PUT the same id with ttlMs 0 — this also removes a barrier that cannot be read.
curl -s -X PUT -H "Authorization: Bearer $CAIRNFLOW_TOKEN" -H 'content-type: application/json' \
  "$CAIRNFLOW_SERVER/admin/gc/barriers/backup-2026-10-12" -d '{"ttlMs": 0}'
```

The response has `previous` (`absent` | `active` | `expired` | `unreadable`). A late renewer that
sees `expired` knows the barrier expired in between, and GC may have run in that gap. The `missing`
array lists covered ranges whose DB no longer exists.

**Set a barrier before** any job that reads a range's DB directly from the bucket without the GC
round knowing: backups, exports. `cairn-ctl backup` sets its own `backup-<id>` barrier (`all`) for 10
minutes and renews it every 2 minutes while it runs, so a killed backup stops holding the GC within
10 minutes ([restore-from-backup.md](./restore-from-backup.md)). `cairn-ctl fork` sets a `fork-<prefix>` barrier (24 hours) for every
range the copy stands on; renew it when you use the copy for longer
([restore-from-fork.md](./restore-from-fork.md)).

## Orphan ranges, DBs nobody references

`cairn-ctl verify` reports (d) "`ranges/<id>` orphan" when a range has left the table without a
tombstone. GC never deletes a range without a tombstone.

The most common case — the parent range's leader died between the table commit and writing the
tombstone — the nodes fix by themselves: the tombstone-on-behalf round (run on the leader of the
smallest range, as soon as the tracker sees the old owner's session expire; safety net
`--retire-orphans-every-s`, default 3600 s) writes the tombstone. Wait one lease TTL plus a few
seconds, then run `verify` again. The node logs `tombstone on behalf: the range left the table…`
when it does this; a range it keeps is logged at debug level as `tombstone on behalf: keeping` with
`by` (`OwnerLive`, `Promised`, `Unreadable`, `UnknownWalKind`, …). If it is still reported after that
(no owner record, or the owner is still alive), write the tombstone by hand:

```sh
cairn-ctl --main-store <uri> range force-retire <id>          # dry run: prints before/after
cairn-ctl --main-store <uri> range force-retire <id> --apply  # writes, leaves audit/ctl/<ts>
```

The command refuses if the range is still in the table. From then on the D-d round deletes the DB
once nobody references it.

**Do not:**

- Delete `ranges/<id>/` by hand on the bucket. A child range may still stand on its checkpoint; the
  result is a permanent `500 checkpoint missing` on that range (`split-merge.md` §5).
- Set a barrier with the maximum TTL and forget it. Set a TTL just long enough for the job, and renew
  if needed.
