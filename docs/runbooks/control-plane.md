# Runbook — the control plane: grids, forcing a refresh, the quiet-renewal gate

**Alert:** none yet. The code does not export metrics for the coordinator, the age of the `view`,
the applied registry generation or the session mode (see the control-plane row of
[Missing metrics](./README.md#missing-metrics)). Until it does, poll with
`cairn-ctl cluster status` and `GET /cluster`.

Invariants: [`docs/guides/lease-session.md`](../guides/lease-session.md) LS-3, LS-5, LS-6, LS-16.

## Mechanism

**Signals travel over the network, the truth lives on the bucket.** Notifications between nodes
("this range just changed owner", "the registry just changed", heartbeats to the coordinator and
the `view` it returns) are only the fast path. Each fast path has a **grid** that reads the bucket
on a timer, in case a notification gets lost. A grid is only about **liveness and freshness**, not
safety:

- the old owner of a range can no longer write (`writer_epoch`, log sealing — LS-4);
- the old owner can no longer answer reads (the read fence — LS-5);
- a range is taken only by a CAS on the bucket, after the owner's session has ended **on the
  bucket** (LS-3).

So a slow grid only makes something **late**. It does not lose or corrupt data. The one exception
is the authentication registry (API keys, tenants): a revoked key is still accepted on a node that
missed the notification until that node re-reads the registry. The acknowledged write with
`pending` closes this window, see [202 `pending`](#202-pending-when-revoking-a-key-or-disabling-a-tenant).

## Worst-case delay left for each grid

The default TTL is 10 s (`--lease-ttl-ms`), so `renew_interval` = TTL/5 = 2 s.

| What | Fast path | Second path | Grid — worst-case delay | What being late means |
|---|---|---|---|---|
| **The old owner learns it lost a range** (LS-6 layer 3) | the taker's notification, `POST /internal/owner/changed` (~ms) | the coordinator's `view` records someone else holding the range: ~`renew_interval` + one GET | **1 minute** (`DEFAULT_OWNER_WATCH`): one `If-None-Match` GET per held range, almost always `304` | The old owner thinks it still leads. Clients calling it get `NotLeader` (read fence) and go to another node. It cannot write, it does not serve stale reads |
| **The registry** (deployments, API keys, tenants, rules, isolation, range windows) | the notification `POST /internal/registry/changed` | GET `control/registry-gen` every `--deploy-refresh-ms` (30 s), or the generation in the `view` (a node listening to the `view` does not GET by itself) | The writer's generation bump failed, or it died between the main write and the bump: **~2 minutes + one tick** (the bump is retried in the background; odd/even generations mark a write in flight) | A node that missed it runs the old registry: a new deployment not visible yet, an old rule, a revoked key still accepted (see 202 `pending` below) |
| **Full reconciliation** (the last layer of the registry) | — | — | **10 minutes** (`registry_gen::DEFAULT_RECONCILE`), whatever the generation says | Catches every case the generation misses |
| **Membership** (which peers are alive, what they hold, health) | heartbeat to the coordinator every `renew_interval`, the `view` in the answer | a node the `view` marks as suspect (silent > 2·TTL) is then observed directly | A `view` older than TTL/3 (coordinator lost) ⇒ back to the bucket tracker within ≤ TTL/3 + one tick. The sparse tracker runs every 6·TTL. A node listening to the `view` LISTs `sessions/` every 10 minutes, the coordinator every TTL | Routing and balancing use a stale hint. "Dead" only ever comes from the bucket (LS-1, LS-3) |
| **The quiet-renewal gate** (`control/quiet-lease`) | — | — | A solo node reads the gate at the start of each incarnation and every 60 s (`DEFAULT_QUIET_LEASE_POLL`) | Turning the gate on or off takes effect within ≤ 60 s |

## Forcing a refresh

None of the steps below is needed for safety. Use them only when you want a change to take effect
at once, without waiting for a grid.

```sh
export CAIRNFLOW_SERVER=http://<node>:8080 CAIRNFLOW_TOKEN=<cluster-admin key>
```

- **The old owner has not stepped down.** Nothing to do: it cannot write and cannot answer reads,
  and the grid catches it within ≤ 1 minute. If you want it now, shut that node down gracefully
  (`SIGTERM`). The node hands over every range and closes its session as `closed`; peers take the
  ranges at once. On restart the node reads everything from the bucket. Who holds what:
  `cairn-ctl … owners`.
- **The registry on one node does not have a change yet.** Three ways, in order of preference:
  1. Wait ≤ one `--deploy-refresh-ms` tick if the generation bump succeeded.
  2. Write the change again through the API on any node. Every registry write bumps
     `control/registry-gen` and notifies peers.
  3. Shut down the lagging node gracefully and start it again. At startup, `RefreshLoop` runs a
     full round.

  Check by comparing `GET /deployments`, `GET /admin/keys`, `GET /admin/tenants` across nodes.
- **Membership.** `GET /cluster` on each node shows whom that node sees. A `view` older than TTL/3
  makes the node fall back to reading the bucket by itself. There is no command to force it.

## 202 `pending` when revoking a key or disabling a tenant

- **200:** the write is durable on the bucket, and every live session in `Membership` has
  confirmed it applied the new generation. From then on no node running normally accepts that key.
- **202 with `pending: [node…]`:** the write **is durable** (do not write it again; writing it again
  is just one more write). The nodes in the list did not confirm within the time limit. Each such
  node is in one of two cases:
  - it can reach the bucket: it sees the change within ≤ one tick, then as for 200;
  - it cannot reach the bucket: after `2·deploy_refresh` it **refuses** every request that needs
    authentication with `503` (closed when unsure) until it can read the registry again.

  What to do: check the nodes in `pending` (`GET /health`, network to the peer, logs). A node that is
  cut off for good can be treated as dead. It does not serve authenticated requests, and it cannot
  write to the bucket.

## New bucket: gate on, with delegation

A node that **creates** an empty bucket writes the gate together with `cluster-version`
(`crates/cairn-store/src/migrate.rs::bootstrap_fresh`):

- `cluster-version` carries `min_binary ≥ 3` from creation (in practice 5, with the call gates);
- `control/quiet-lease` = `state=on`, `knows=3`, `delegation=on`, `by=bootstrap@<node>/<inc>`, and
  the marker `delegation_unproven=<at>,bootstrap,abcde` — delegation is on **without any evidence
  yet**. `cluster status` shows that marker on its `WARNING:` line, as with the override flag;
- `control/registry-gen` at generation 0 (registry readers get `304` instead of a full round every
  30 s).

There is no `BOOT_WAIT` and no `knows` check as in `enable-quiet-lease`: an empty bucket has no
binary yet. On `s3://` the node runs the G18 test (the store test of the next section) before writing the gate: if
it fails, the gate is **not turned on** — log `warn` "store does not keep the conditional-GET
contract (G18) — new bucket does NOT turn on the quiet-renewal gate, cluster runs direct", and the
cluster runs `direct`; a transient I/O error makes the boot path retry. `memory://` and `file://` are
not tested.

To choose otherwise at creation: `--quiet-lease off` (a `direct` cluster, no gate written) or `solo`
(solo mode only). The flag only matters at creation; on a bucket that already holds a cluster it is
ignored (log `debug`). To turn it off later: [Turning the gate off and downgrading](#turning-the-gate-off-and-downgrading).
Two nodes creating the same empty bucket: the winner of the `upgrading` lock decides, the other
waits — one outcome.

## Turning the quiet-renewal gate on

The gate enables solo mode (LS-16) and delegated renewal. A new bucket has it on already (section
above); this section is for buckets created by an older binary or with `--quiet-lease off`. Code:
`crates/cairn-server/src/ctl.rs::enable_quiet_lease`.

1. Upgrade every node to a binary that knows the gate step (`knows ≥ 3`). Check with
   `cairn-ctl … cluster status`: every session that has not ended must show `knows=3` or more.
2. Dry run: `cairn-ctl … cluster enable-quiet-lease`. The command prints `cluster-version` and the
   gate before and after, plus the line "`--apply will first run the conditional GET test (G18) …`".
3. For real: `cairn-ctl … cluster enable-quiet-lease --apply`. In order:
   1. **Test the store.** The conditional GET test, contract G18 of
      [`docs/reliability/irreversible.md`](../reliability/irreversible.md) §0.3. The command runs 32
      rounds on `control/probe/<nonce>`, reading with the writing client and with a second client:
      after each won write, a GET with the **old** etag must return `200` with the new version,
      **never `304`**. If it passes, it prints the line `cleanup: control/probe/<nonce>: conditional
      GET test (G18) passed 32 rounds …`, and the probe object has been deleted.
   2. Check `knows` of every session.
   3. CAS `min_binary = 3`. From here old binaries cannot start.
   4. **Wait `BOOT_WAIT`** (`ctl.rs`, 190 s: the node's 120 s startup limit + one S3 client retry
      + one request + a margin). The command prints "min_binary=… set; waiting … s" on stderr. An old
      binary that passed the `min_binary` check before step 3 and is still starting has, within this
      window, either opened its session or given up. If the gate was already `on` (only adding
      `--delegation`), there is no wait.
   5. Check the sessions again. A session with `knows` < 3 left ⇒ refused, the gate is not on — stop
      that old binary and run again (`min_binary` stays as it is).
   6. Write `control/quiet-lease` = `on`.

   Step 4 closes the window of an old binary that is still starting. An old-binary process that **is
   already running** but whose session had ended at both checks (a long pause) can still wake up and
   open a new incarnation; the coordinator sees the low `knows` and puts the whole cluster back to
   `direct`.
4. A solo node enters `mode=solo` within ≤ 60 s. `cluster status` shows `mode=solo`.

**The command refuses because of the test** (exit code 3, the reason starts with "the store does not
keep the conditional GET contract"). Something between the node and the bucket returns a stale
`304` or a stale read: a caching proxy, a CDN, an S3 gateway with a cache, or a weakly consistent
store. Solo mode is **not safe** on that path. A solo node could renew from a stale `304` while a
newcomer has finished its challenge and taken the ranges. What to do:

- point `--main-store` straight at the bucket, remove the caching layer, then run again;
- there is no flag to skip the test, and you should not look for a way to skip it;
- the object `control/probe/<nonce>` may be left behind: when the store reads wrong, the command
  does not trust its last read and so does not delete it. The object is not customer data and
  nothing references it; delete it by hand.

**The command refuses because of a transient I/O error** ("no conclusion yet"): run again.

## Turning on delegated renewal: the evidence gate

Delegated renewal is turned on separately with `enable-quiet-lease --delegation`. The command
**refuses** (exit code 3, writes nothing, even for a dry run) while `control/quiet-lease` does not
record all five evidence items of the done gate:

| Item | Content |
|---|---|
| `a` | wider model: M1 running split/merge with every delegation bug, two delegating nodes with two terms |
| `b` | the tripwire runs in release builds: `cairnflow_lease_delegated_revoked_while_valid_total` exists in the binary about to run |
| `c` | chaos ≥ 30 minutes with `--kill-coordinator-every-s` and delegation on, tripwire at zero |
| `d` | a clean 24-hour soak |
| `e` | the store contract suite green on **the target store** of this cluster |

1. See what is recorded and what is missing: `cairn-ctl … cluster status` (the line "done-gate
   evidence"; each item is listed with its title, a missing one as `MISSING`).
2. Record each item once you have the evidence, with a short note enough for someone later to check
   it: commit or binary, command or log path, result.

   ```sh
   cairn-ctl … cluster record-evidence c --note "chaos 30 min, binary 1a2b3c4d, log /data/chaos-1.log, tripwire 0"            # dry run
   cairn-ctl … cluster record-evidence c --note "chaos 30 min, binary 1a2b3c4d, log /data/chaos-1.log, tripwire 0" --apply
   ```

   The command records who ran it (`--who`, default `$USER@$HOST`) and when into the item, plus an
   audit record. Recording an item again overwrites the old one (the command prints the old one).
   Recording evidence **turns nothing on**: on a bucket without a gate the command creates the object
   with `state=off`. `disable-quiet-lease` keeps the evidence.
3. All five items present: `cairn-ctl … cluster enable-quiet-lease --delegation --apply`.

**The override flag `--accept-missing-evidence`** (only together with `--delegation`) turns
delegation on despite missing items. It is meant for test clusters, chaos and soak runs: the chaos run (c)
and the soak (d) themselves must run with delegation on before the evidence exists. **Do not use it in
production.** When used, the command prints a warning and writes the marker `delegation_unproven=`
(missing items, who, when) into `control/quiet-lease`. `cluster status` shows the marker on its
`WARNING:` line. The marker is removed by running `--delegation` again once all five items are
recorded, or by `disable-quiet-lease`.

A `delegation_unproven=` marker on a production cluster means delegation has not passed its done
gate: `disable-quiet-lease --apply` (nodes go back to `direct`, ≤ 60 s), record all the evidence,
then turn it on again.

## Turning the gate off and downgrading

1. `cairn-ctl … cluster disable-quiet-lease --apply` writes `control/quiet-lease` = `off`. Nodes
   see it at their next gate read (≤ 60 s) and CAS their session record back to `direct`.
2. Wait until `cairn-ctl … cluster status` shows every session that has not ended at `mode=direct`,
   with no `challenged=` left.
3. `cairn-ctl … cluster lower-min-binary --apply` lowers `min_binary` to `version`. The command
   refuses while the gate is still on or a session is not at `direct`, because an old binary would
   apply the LS-1 silence rule to a `solo` record while its owner still holds a grant.
4. From here old binaries can start. Roll back the binary as in
   [upgrade-and-rollback.md](./upgrade-and-rollback.md).

The fall-back path during network trouble needs no action: losing the coordinator, a partition, or
an old binary slipping in all put nodes back on the `direct` protocol (renewal by PUT, observation by
the tracker). That only costs S3 requests.
