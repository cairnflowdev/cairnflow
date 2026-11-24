# Runbook — a range is L0-overloaded

**Alerts:** `CairnflowRangeL0Throttling` (the range has been throttling on L0 for 10 minutes
straight), `CairnflowRangeL0Stalling` (SlateDB reports `l0_stall` throughout 10 minutes: writes
stopped completely).

**Dashboard:** the rows *L0 by range* and *429 by reason*.

## Mechanism

Code: `crates/cairn-server/src/l0.rs`; the contract with clients is in
[`docs/admin-api.md`](../admin-api.md) ("`429 range_overloaded`").

- Each range is one SlateDB DB. The memtable flushes to L0 as SSTs, compaction merges L0 into the
  lower levels. The L0 cap is 8 views (`l0_max_ssts`). At the cap the flusher refuses to upload the
  memtable, and every write of the range waits, including the work of running invocations. That is
  **the wall**.
- **A soft throttle before the wall.** From `--l0-soft` views (default 5), ingress refuses **new
  work** for the range with probability 0.3 at 5 views, 0.6 at 6 views, and 0.9 from 7 views up
  (`--l0-max-reject`). The refusal is a `429` with `code: "range_overloaded"` and `Retry-After`. **No
  invocation is created**, so resending is safe.
- Work of running invocations is not throttled: completions, timers, retries, signals. Draining the
  backlog is what brings L0 down. A `StartCall` from another range and local child calls are
  **deferred**, not failed.
- The throttle level only returns to 0 once L0 has stayed below `--l0-soft` for 5 s.

Metrics:

| Metric | Meaning |
|---|---|
| `cairnflow_range_l0_views{range}` | number of L0 views (cap 8) |
| `cairnflow_range_l0_reject_ratio{range}` | current throttle level; `> 0` ⇔ throttling |
| `cairnflow_range_l0_shed_total{range,kind}` | work throttled: `ingress` (429), `inbound`, `dispatch` |
| `cairnflow_range_slatedb_l0_ssts{range}`, `cairnflow_range_slatedb_l0_stalls_total{range}`, `cairnflow_range_slatedb_backpressure_total{range}` | SlateDB's numbers |
| `cairnflow_range_write_stalls_total{range}`, `cairnflow_range_write_stall_seconds_total{range}` | `db.write` waits of ≥ 10 ms |

Per-range metrics are printed by the range's **leader**. When the range changes leader, the series
moves to another `instance`.

## Diagnosis

1. **Which range, since when?** The panels *L0 views by range* and *L0 throttle level by range*.
   Filter with the `range` variable.
2. **Load or store?**
   - A high PUT p99 (`cairnflow_object_put_seconds`) on the range's leader ⇒ a slow store slows
     compaction down. See also [slow-node-self-yield.md](./slow-node-self-yield.md).
   - Normal PUTs while the range writes a lot ⇒ load. Look at the load per range:

     ```sh
     curl -s -H "Authorization: Bearer $CAIRNFLOW_TOKEN" \
       "$CAIRNFLOW_SERVER/ranges/stats?scope=cluster" | jq '.'
     ```

     Fields to read: `opsPerSec`, `bytesPerSec`, `turnShare`, and `topOwner` (the hottest partition
     key with its share).
3. **Is there a split suggestion yet?**

   ```sh
   curl -s -H "Authorization: Bearer $CAIRNFLOW_TOKEN" \
     "$CAIRNFLOW_SERVER/ranges/suggestions?scope=cluster" | jq '.'
   ```

   `reason` is `load`, `size` or `hot_owner`. `at` is the split key. `blockedBy` says why it has not
   split yet: `l0` means the child range would hit the L0 cap.

## Handling

1. **Load spread over many keys** (`reason: load` or `size`): split the range.

   ```sh
   cairn ranges split <id> --at <at from suggestions>
   ```

   A split needs L0 room for the child range. Without it, it returns `503`, and
   `/ranges/suggestions` reports `blockedBy: "l0"`. Then lower the load first (step 2), wait for L0
   to go down, then split. With `--auto-split=on` the cluster splits by itself once the conditions
   hold (at most one half-done split/merge in the whole cluster, a 10-minute cooldown per range).
2. **One hot owner** (`reason: hot_owner`): **cannot be split**, because one owner never spans two
   ranges. Lower the load with flow control for that scope or tenant:

   ```sh
   cairn rules set <pattern> --concurrency N
   # rate cap (CU/s) per pattern or for a whole tenant:
   curl -s -X POST -H "Authorization: Bearer $CAIRNFLOW_TOKEN" -H 'content-type: application/json' \
     "$CAIRNFLOW_SERVER/rules" -d '{"tenant": "<tenant>", "rate": 50, "burst": 100}'
   ```

   A new rule takes effect after about 2 s: compare `version` at `/rules` with `rulesVersion` at
   `/scheduler`.
3. **A slow store:** fix the store. The throttle comes down by itself when compaction keeps up.
4. **On the client side:** honour `Retry-After`. Resending with the same `Idempotency-Key` yields
   exactly one invocation.

**Do not:**

- Turn the throttle off (`--l0-soft 0`) to "get rid of the 429s". The range will go straight to the
  wall: every write stops, including those of running invocations. `CairnflowRangeL0Throttling` then
  goes silent, only `CairnflowRangeL0Stalling` is left.
- Raise `--l0-soft` close to 8 hoping for more capacity. That only moves the throttle closer to the
  wall; it does not make compaction faster.

## Related

- [shared-wal-floor-stalled.md](./shared-wal-floor-stalled.md): a range that cannot flush to L0 pins
  the shared WAL floor.
- [`docs/guides/flow.md`](../guides/flow.md) (flow control), [`docs/guides/split-merge.md`](../guides/split-merge.md)
  (auto split).
