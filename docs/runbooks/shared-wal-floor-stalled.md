# Runbook — the shared WAL floor does not move

**Alert:** `CairnflowNodeWalFloorStuck`. The GC floor of a node's shared WAL log has been held at a
pinned object for more than 10 minutes.

**Dashboard:** the *Shared WAL* row.

## Mechanism

Code: `crates/cairn-store/src/nodewal/`; invariants NW-* in
[`docs/guides/nodewal.md`](../guides/nodewal.md), NW-7 above all.

- Each node writes one shared WAL for every range it leads (every range uses the shared WAL). A
  range's frames can only be dropped once the range has **flushed its memtable to L0**.
- A log's **GC floor** is the lowest object still kept. The floor only moves past an object whose
  every frame is beyond every reference (NW-7). A range that has not flushed **pins** the floor.
- A cold range (no writes) is forced to flush when its frames are older than `--wal-age-flush-ms`
  (default 60 s), so in a healthy state the floor never stands still for long.
- After 600 s standing still, the node logs `warn`: `shared WAL floor stuck — range {range} has not
  flushed frame {wal_id} to L0`. The line repeats every 600 s while the floor stays put.

Metrics:

- `cairnflow_nodewal_floor_age_seconds{node}`: the floor age of the longest-pinned log on the node.
  The line is present even when it is 0.
- `cairnflow_nodewal_floor_pinned{range}` = 1: the range is pinning a floor. Only present while some
  range pins.

## Diagnosis

1. **Which range pins?** The panel *Ranges pinning the floor*, the `warn` log line above, or, with no
   node needed:

   ```sh
   cairn-ctl --main-store <uri> [--wal-store <uri>] wal floors
   ```

   Each log prints its floor, its object count, and `pinned by range R (wal id W)`.
2. **Why does that range not flush?**
   - **L0 is full:** the flusher refuses to upload the memtable when L0 hits the cap. Look at
     `cairnflow_range_l0_views{range="R"}` and `cairnflow_range_slatedb_l0_stalls_total`. If so,
     follow [range-l0-overload.md](./range-l0-overload.md); the floor moves by itself once the range
     can flush.
   - **The store is slow or failing:** a flush is an SST PUT. Look at `cairnflow_object_put_seconds`
     and the node logs. See also [slow-node-self-yield.md](./slow-node-self-yield.md).
   - **The range has no leader**, or its leader is stuck: `cairn cluster`. A range's frames are only
     flushed when someone leads the range to replay and flush them.

## Handling

- Fix the cause from step 2. The floor moves at the next GC round after the range flushes, and
  `cairnflow_nodewal_floor_age_seconds` goes back to 0.
- A range stuck on a healthy node: restart that node with `SIGTERM` (graceful shutdown). The new
  leader replays the range's frames from the old log and flushes.

A floor standing still **loses no data**. The cost is that WAL objects below the floor are not cleaned
up: the bucket grows, and a node takes longer to replay when it takes the range.

**Do not:** delete objects in `nodewal/` by hand. The pinning range's unflushed frames are there, and
that is acknowledged data.
