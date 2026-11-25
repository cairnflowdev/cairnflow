# Runbook — a node drops its incarnation (fail-stop, lost session)

**Alert:** `CairnflowNodeFailStops` (the same `cause` more than 2 times in 30 minutes). Often
together with `CairnflowRangesWithoutLeader` for a few seconds after each one.

## Mechanism, in five lines

Code: `crates/cairn-store/src/failstop.rs` (labels), `crates/cairn-store/src/session/renew.rs`
(`lose`, the only place that counts), `crates/cairn-store/src/nodewal/stripe.rs` (`put`).
Invariants NW-2, NW-5 in [`docs/guides/nodewal.md`](../guides/nodewal.md).

1. An **incarnation** is one lifetime of a node's session. Dropping it revokes every grant, every
   range of the node must be taken again (about 5 writes each), turns in flight fail; the node opens a
   new incarnation with a new WAL log.
2. `cairnflow_failstop_total{cause}` grows **once** per dropped incarnation, by the **first** cause.
   It counts per process: a node restart resets it to 0.
3. Shared WAL labels: `wal_fenced` (a log PUT met a seal on its first attempt — the log was sealed),
   `wal_ambiguous` (met a seal after a failed or timed-out attempt), `wal_stall` (a log PUT did not
   finish, or its outcome is unknown, within `stall_max` **and** the stream could not be rotated — a
   stuck PUT normally only rotates that stream: the stream's ranges are taken again and the node
   lives; counted in `cairnflow_nodewal_stripe_rotations_total{cause}`). On stores that declare
   `atomic_put` (default for `s3://`, `memory://`, `file://`), an `AlreadyExists` is read back: the
   node's own write ⇒ ack, no fail-stop — so the first two labels only mean a real seal. Outcomes of
   the read-back: `cairnflow_nodewal_put_resolved_total{outcome}` (`ours`, `theirs`, `absent`,
   `unknown`). A store declared with `--wal-atomic-put false` keeps the old behaviour: `wal_fenced`
   also covers object_store retrying by itself a 5xx that had in fact been written.
4. Range acquisition labels: `owner_cas_lost` (the owner-record CAS lost while taking a range), `c1`
   (the owner record points at the node's own live log). Label `dirty_yield`: a
   `wal_fenced`/`wal_ambiguous`/`c1` that follows a **dirty yield** of the same incarnation — a range
   stepped down with its seal failing or over the limit, or with the clean owner-record CAS after the
   seal failing, so the owner record still points at a live log. Number of dirty yields:
   `cairnflow_dirty_yields_total` (each one is now a stream rotation, not a node going down).
5. Session labels: `session_expired` (could not renew within ⅔·TTL), `session_lost` (the session
   record was changed, ended or vanished). `other` should always be 0 — non-zero is a path without a
   label, report it as a bug.

## Diagnosis

1. **Which label?** `sum by (instance, cause) (increase(cairnflow_failstop_total[30m]))`.
2. **`wal_stall` on a node whose ranges nobody took:** the object store is slow (PUT > 2 s) or flaky
   (constant 5xx) to the point that the stream cannot claim a new id to rotate (log `cannot rotate
   the shared WAL stream and no other node can take the ranges — fail-stop the incarnation`). Frequent
   stream rotations without fail-stop (`rate(cairnflow_nodewal_stripe_rotations_total[5m])`) are the
   same group, milder: each rotation only makes one stream's ranges be taken again. Look at
   `cairnflow_object_put_seconds` and `cairnflow_slow_score`. On an `atomic_put = false` store,
   `wal_ambiguous` and most `wal_fenced` on a node whose ranges nobody took belong to this group too
   (object_store retried a PUT that had landed). `wal_fenced`/`wal_ambiguous` on an `atomic_put` store
   with no node taking the ranges is odd — keep the logs (`seal by …` in `reason` says who sealed).
3. **`wal_fenced` together with another node taking the ranges:** this node was already considered
   dead (its session expired on the bucket) — look for `session_expired` at the same time, and at the
   node's clock/network.
4. **`session_expired`:** the session renewal (a conditional PUT every TTL/5) did not come back in
   time. A sleeping machine, a GC pause, CPU steal, the network to the store. Log: "session lost —
   dropping every partition" with `reason`.
5. **`owner_cas_lost`, `c1`:** a race while taking a range. A single one is normal under chaos; if it
   repeats, keep the logs of both nodes around that time and open a bug.
6. **`dirty_yield`:** the store was slow or failing exactly when a range stepped down (logs `seal
   exceeded`, `cannot CAS the clean owner record after seal`). Neither path takes the node down any
   more: a dirty yield rotates the stream before writing the owner record
   (`cairnflow_nodewal_stripe_rotations_total{cause="dirty_yield"}`, log `rotated the shared WAL
   stream` with `cause=dirty_yield`), and a self-retake after a failed clean CAS applies the pending
   yield. If this label still grows, some dirty-yield path does not go through
   `LeaseKeeper::rotate_live_logs` — keep the node's logs around that time and open a bug.

## Do not

- Raise the TTL or `put_timeout` to silence the alert: it only makes acks wait longer without
  shrinking the blast radius.
