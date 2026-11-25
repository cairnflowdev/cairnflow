# Runbook — a session stands still but cannot be marked expired

**Alert:** `CairnflowSessionUnexpirable` (`cairnflow_session_rearm_total{outcome="failed"}` grows
within 30 minutes). Often together with `CairnflowRangesWithoutLeader` — nobody takes that node's
ranges.

## Mechanism, in five lines

Code: `crates/cairn-store/src/session/tracker.rs` (`SessionTracker::observe`, `rearm`). Invariant
LS-1 in [`docs/guides/lease-session.md`](../guides/lease-session.md); the proof is at the top of
`crates/cairn-store/src/session/mod.rs`.

1. Other nodes (observers) consider a session dead **only** when the bucket says so: they read
   `expired`/`closed`, or their own `expired` CAS wins after one silent TTL.
2. A session that stands still while the `expired` CAS keeps losing (`Precondition` ≥ 2 times, silent
   for more than 3 TTLs) is a torn object (erasure-coded MinIO after a half-done PUT: GETs alternate
   between etags) or a stale read layer in front of the store.
3. The observer then **overwrites** the record (an unconditional PUT, a `live` copy carrying the
   largest beat it has seen): if the owner is alive, its next renewal fails and it drops the session
   by itself. `cairnflow_session_rearm_total{outcome="written"}` grows.
4. One TTL after the overwrite, the `expired` CAS runs as usual. If it wins ⇒ that node's ranges are
   taken again.
5. The overwrite fails ⇒ `outcome="failed"`, log `warn` "session stood still and could not be marked,
   the overwrite failed too — NOT considered dead". The session stays alive for everyone, and its
   ranges are taken by **nobody** until the store accepts writes. Treating the node as dead by the
   clock alone here could produce two leaders if the node is in fact still alive.

## Diagnosis

1. **Which session?** The observer's `warn` log carries `session=<node>/<inc>`. Read the bucket
   directly with the store's tools (e.g. `aws s3api head-object` on `sessions/<node>/<inc>` twice in a
   row): different etags with identical content mean a torn object.
2. **`written` grows, `failed` does not:** the store just had a torn object and the on-bucket way out
   worked. Check the store (erasure coding / quorum settings of MinIO: a client killed mid-CAS can
   leave an object split between two versions); nothing to do on the cairn side.
3. **`failed` grows:** PUTs to `sessions/` fail — write permission, throttling, a stuck store. Look at
   `cairnflow_object_put_seconds` and the store's logs.

## Fix

- Fix the store so PUTs get through: the next observation round (after 3 more silent TTLs) overwrites
  again by itself.
- You know **for sure** that node is dead (the machine is off) but the store still cannot write: stop
  its process for good first, then write `state=closed` into that session record by hand once the
  store accepts writes again.

## Do not

- Delete a `live` session record to "unstick" it: others see "absent" ⇒ consider it dead without any
  time-based reasoning, even when the node is alive.
