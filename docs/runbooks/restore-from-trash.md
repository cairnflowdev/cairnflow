# Runbook — restoring from the trash (`cairn-ctl trash`)

**Alert:** no dedicated metric yet (see the trash row of [Missing metrics](./README.md#missing-metrics)).
Current signs:

- the D-d runner's `error` log: *"trash: an expired entry is still needed — the original deletion was
  wrong"*. This is the tripwire `trash::purge::still_needed`, and it is **always** a bug to
  investigate;
- `cairn-ctl verify` reports a live DB referencing a DB that is gone (`external_db`), or a range in
  the table that no longer has a DB (`db`);
- a client reports a lost acknowledged invocation.

## What the trash is

Code: `crates/cairn-store/src/trash.rs`; invariants SM-16, NW-12.

Three of cairn's data deletion paths **move** objects into the trash instead of deleting them:

| Path | Entry | Reason recorded in the entry |
|---|---|---|
| D-d (`--gc-retired-dbs`) | `range-db <id>` | DB of a retired range nobody references any more |
| split/merge roll-back, the orphan-child cleanup round, `cairn-ctl intent clear --apply` | `range-db <id>` + `owner <id>` | DB and owner record of an orphan child |
| shared WAL: forgetting the log of a dead incarnation for good | `wal-log <node>/<inc>:<stripe>` | seal, floor, index |

Layout: `<prefix>/trash/<yyyy-mm-dd>/<id>/entry` (the description) and
`<prefix>/trash/<yyyy-mm-dd>/<id>/obj/<original path>` (a verbatim copy). A separate WAL store has
its own trash, with an entry of the same id for the DB's `wal/` part.

The D-d runner (the leader of the range with the smallest id) really deletes an entry once it is
older than `--trash-keep-s` (default 604800 s = 7 days) **and** a fresh check shows nobody needs it:
the range is not in the table, no live DB lists it in `external_dbs`, no owner record lists the log.
When in doubt it keeps the entry. `--trash-keep-s 0` turns real deletion off entirely.

**The trash does not cover:**

- shared WAL frames that GC deletes below the floor (as much volume as everything written);
- the checkpoint a split roll-back removes from an ancestor DB, and every internal SlateDB delete
  (SSTs, old manifests). Restoring a child DB **after** the ancestor's GC deleted the SSTs that
  checkpoint pinned leaves the child with broken reads.

These two can only be recovered if the bucket has **versioning** on. Hence: **restore as early as
possible**, within the `min_age` of SlateDB's GC.

## Steps

```sh
export CTL="cairn-ctl --main-store s3://my-bucket/prod"   # add --wal-store if separate
```

1. **Look at the trash.**

   ```sh
   $CTL trash list
   ```

   Each line has the id, the store (`main`/`wal`), what was deleted, the object count, bytes, the age
   in hours, and the reason. An `UNREADABLE` entry usually comes from a newer binary
   (`cairnflow-trash/2`), or from a move that died midway. The purge round never deletes such
   entries.

2. **Stop the purge round and the self-repair rounds before restoring.** The orphan-child cleanup
   round (SM-14) treats a DB outside the table as an orphan and cleans it up again; the trash purge
   round may really delete another entry while you work. The surest way: stop the cluster. If you
   cannot, restart the nodes with
   `--retire-orphans-every-s 0 --reconcile-intents-every-s 0 --trash-keep-s 0`, and turn
   `--gc-retired-dbs` off.

3. **Dry run, then restore.**

   ```sh
   # The DB and owner record of a range (the owner record comes back first):
   $CTL trash restore --range 7
   $CTL trash restore --range 7 --apply
   # one entry by id (e.g. a shared WAL log):
   $CTL trash restore 1791986234088-057ba90af7dcadbd --apply
   ```

   Restoring copies each object back to its original place with `Create`, so it **never overwrites**
   what is there. If the original place already holds another object, that object is reported as a
   conflict, and the entry is kept. The command refuses (exit code 3) when the range's DB exists at
   `ranges/<id>`. `--force` copies into that DB anyway, but that is rarely right: old manifests copied
   into a live DB do not bring data back, they only muddle its history. Each `--apply` leaves an
   `audit/ctl/<ts>` record.

4. **Check.**

   ```sh
   $CTL verify
   $CTL owners
   $CTL range dump 7 --table journal --limit 20
   ```

5. **Put the range back in its place in the protocol.** Restoring only brings objects back; it does
   not change the range table. For a range that left the table and that you want to read again: build
   a copy with `cairn-ctl fork` and investigate there ([restore-from-fork.md](./restore-from-fork.md)).
   Do not edit `cluster.conf` yourself. A range **in the table** that lost its DB is a data-loss
   incident: stop the cluster, restore, check that `verify` is clean, and only then restart.

6. **Report the bug.** An entry that had to be restored means a deletion was wrong. Open an issue
   with `trash list --json` and the D-d runner's logs.

## Cost

The trash keeps deleted data for an extra `--trash-keep-s` seconds. A range DB removed by D-d takes as
much space as itself for 7 days. Each moved object costs one more HEAD (for the whole entry), one COPY
and one DELETE, against a single DELETE before.
