# Runbook — invocations stuck on infrastructure (`paused` 577) and broken message rows

**Alerts:** `CairnflowInfraStuck`, `CairnflowPoisonRows`.

## Mechanism

Code: `crates/cairn-server/src/turn/inbound.rs` (`poison_row`, `drain_inbound`, `drain_signals`),
`crates/cairn-server/src/drainer.rs`; invariant FL-15 in
[`docs/guides/flow.md`](../guides/flow.md).

- Code **577 `INFRA_STUCK`** (`cairn_engine::INFRA_STUCK`) marks an invocation that **cairn's
  infrastructure** cannot move forward — not a handler error. The invocation is `paused`, keeps its
  journal and its `EXCLUSIVE` lock; it never becomes `failed`.
- **The rescue net stops spinning** (`drainer::Rescue`, `actor::rescue_round`): when the turns of an
  invocation fail at a point that leaves no durable appointment behind (reading the status, asking
  for a queue slot, writing the appointment all fail), the term's rescue net hands it over again,
  backing off **per id**: 0.5 s, 1 s, … up to 30 s. After 50 attempts in a row, or once the streak
  has lasted 1 hour, the net moves it to `paused` 577 instead of handing it over, logs `error`
  "invocation stuck on infrastructure — paused 577" and increments
  `cairnflow_rescue_infra_stuck_total`. The streak is counted in the term's RAM: a leader change
  starts it from zero. **Durable** retries (a retry row, a handler error or an SDK error) are not
  covered by this rule — they retry with backoff as usual, visible through `attempt`.
- **Broken message rows:** the range's drainer meets an `inbound` row (a message from another range)
  or a `signal` row that cannot be decoded. The row **is kept**, not deleted: it may have been written
  by a newer binary (a rolling upgrade), or be damaged on the store. A kept row does not block any
  other row. The first time it is seen in a term, the node logs `error` "undecodable row — kept, not
  deleted" and increments `cairnflow_poison_rows_total`. When the row is a returned result
  (`Completion`), the invocation waiting for it (in the same range) goes to `paused` 577 with a
  message naming the completion and its `seq`. Other kinds carry the waiter inside the broken part or
  in the sending range: only the log and the metric.

## Diagnosis

```sh
cairn invocations --status paused            # find paused invocations
curl -s -H "authorization: Bearer $CAIRNFLOW_TOKEN" \
  $CAIRNFLOW_SERVER/invocations/<id>          # `code` 577 and `message` say why
```

For a 577 from the rescue net, look at the node log around the line "invocation stuck on
infrastructure" and the lines "could NOT reschedule after a failure"/"scheduler-run turn failed" of
the same invocation: they tell where the turn failed. Usually it is a prolonged failure of that
range's store (`cairnflow_rescue_round_failed_total`, `cairnflow_rescue_marked_total` grow steadily).
Marks that come from a turn ending without acknowledging its wake-up duty (nack by default) are
counted separately in `cairnflow_rescue_nacked_total` — if that grows without any store error,
suspect a turn path that panics or a `?` that leaves no appointment behind.

Look in the node log for the line "undecodable row" (fields `row`, `seq`, `error`, `waiter`). An
`UnknownVersion` error means the row was written by a newer binary: a node in the cluster runs a
newer version than the node leading this range.

## Handling

1. **Binary version skew:** upgrade the node leading the range to the same version (see
   [upgrade-and-rollback.md](./upgrade-and-rollback.md)). The new version can read the row and
   delivers it.
2. **A really damaged row** (not version skew): keep the bucket as it is for investigation, open an
   issue. Do not delete the row by hand.
3. Once the cause is fixed: `cairn resume <id>` (or `POST /invocations/<id>/resume`) for each
   `paused` 577 invocation. The row is still there, so the completion is delivered and the invocation
   continues. If the cause is not fixed, it pauses again.
