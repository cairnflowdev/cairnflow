# Runbook — a slow node gives up leadership by itself

**Alerts:** `CairnflowNodeSlowScoreMax` (slow score at 100 for more than 1 minute),
`CairnflowNodeDrainingLong` (the node has been draining for more than 20 minutes). Often together
with `CairnflowRangesWithoutLeader`.

**Dashboard:** the row *Slow node and self-yield* (`deploy/grafana/cairnflow.json`).

## Mechanism, in five lines

Code: `crates/cairn-server/src/slow.rs`, `crates/cairn-store/src/health.rs`; invariant LS-11 in
[`docs/guides/lease-session.md`](../guides/lease-session.md).

1. **Measure.** Every 10 s round, the node takes the latency of its own PUTs — session renewals and
   shared WAL PUTs, with no probe requests — and computes an AIMD score from 1 (healthy) to 100
   (slow). A renewal PUT over 1 s is a "timeout". A round in which the node is **busy** (out of turn
   slots for more than half the time) is skipped: busy is not slow.
2. **Publish.** The score and the PUT p99 go into the session record at the next renewal. Prometheus
   sees `cairnflow_slow_score` and the histogram `cairnflow_object_put_seconds{kind}`.
3. **Yield.** When the score is at 100 for two rounds in a row **and** the PUT p99 is more than 2× the
   median p99 of the other live nodes, the node takes the `shedding` marker on the bucket. Only one
   node holds the marker at a time. Once it has it, the node sets `cairnflow_draining` = 1, refuses
   new ranges, then hands over 2 ranges per second.
4. **Recover.** Score below 10 for 5 minutes straight ⇒ the node clears `draining`, gives the marker
   back and takes back its fair share of ranges. Going from 100 back to 1 takes about 5 minutes, so a
   normal yield-and-recover cycle lasts about 10 minutes.
5. **Safety net.** A node never yields when it is the only live node. While draining, if every other
   node is slow too (score ≥ 50), or no other node is left, the node clears `draining` at once. The
   comparison is against the median, not an absolute threshold, so **a uniformly slow bucket makes
   nobody yield**.

`--slow-shed off` (`CAIRNFLOW_SLOW_SHED=off`) turns step 3 off on that node; steps 1–2 still run.

## Diagnosis

1. **One node or the whole cluster?** Open the panels *Slow score by node* and *Session PUT p99 by
   node*.
   - **Every node** high ⇒ the object store, or the network to it, is slow. The cluster deliberately
     does not yield (safety net 5). This is a store incident: check the provider's status page, the
     network latency, request limits. Nothing to fix on the node.
   - **One node** high ⇒ that machine has a problem: CPU steal, the network to the store, DNS, or
     another process eating the bandwidth.
2. **Is the node yielding?** `cairnflow_draining{instance=…}`. Node log (default target, level
   `warn`/`info`):
   - `node clearly slower than the cluster — shedding leadership`: it took the marker and is
     yielding.
   - `node slow, but another node is shedding — waiting`: another node holds the marker.
   - `slow node — range handed over` (one line per range), or `resign missed, retrying next round`.
   - `cannot read/write the shedding marker — retrying next round`: the bucket has a problem.
3. **Where did the ranges go?** `cairn cluster` (or `GET /cluster`): the slow node's ranges must get
   a new leader within a few seconds. A range without a leader for more than 2 minutes fires
   `CairnflowRangesWithoutLeader`.

## Handling

- **A slow machine that can be fixed** (network limits, noisy neighbours): fix the machine. The node
  recovers by itself after about 10 minutes, nothing else to do.
- **A slow machine that cannot be fixed right away:** replace the node. Send `SIGTERM` (graceful
  shutdown: seal each range, close the session, notify peers; peers take the ranges after ~0.4 s),
  then start a new node pointing at the same bucket. A shutdown is capped at `--shutdown-timeout-ms`
  (default 20 s).
- **`CairnflowNodeDrainingLong`:** the node is still slow after 20 minutes, so the cluster is running
  one node short. Handle it as "cannot be fixed right away". Restarting the node clears the
  measurements (they live in RAM), but if the machine is still slow the node yields again after about
  20 s. **Do not restart it in a loop:** each restart is two owner changes for every one of its
  ranges.
- **Wrong yields, cascading owner changes** (rare: the three safety nets above exist to prevent it):
  restart the nodes with `--slow-shed off` while you investigate. The score is still measured and
  published, so the dashboard stays usable.

**Do not:**

- Delete the `shedding` object on the bucket by hand. A marker that stands still for 120 s is
  considered abandoned by the other nodes anyway.
- `kill -9` several nodes at once to "force" ranges to move. Peers only take ranges after one session
  TTL (`--lease-ttl-ms`, default 10 s), and each owner change hands over every invocation in progress
  again.

## Related

- [range-l0-overload.md](./range-l0-overload.md): a slow store makes compaction fall behind, L0 rises.
- [`docs/self-hosting.md`](../self-hosting.md) §7 (shutting a node down).
