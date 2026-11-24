# Runbook — the key registry may not be fresh (the node answers 503 to authenticated requests)

**Alert:** `CairnflowRegistryStale` (`cairnflow_registry_stale` = 1 for more than 2 minutes). Often
together with `CairnflowNodeFailStops`, `CairnflowNodeSlowScoreMax` (a slow store).

## Mechanism

Code: `crates/cairn-server/src/registry_gen.rs` (`RegistryGen::fresh`), `auth.rs`
(`authenticate`).

- Revoking a key (`DELETE /admin/keys/{id}`, `cairn keys rm`) answers `204` only once every live
  node has confirmed it applied the change. Nodes that did not confirm in time are named in `pending`
  of a `202` answer.
- Each node **confirms by itself** that its key registry is still fresh at every registry refresh
  tick (`--deploy-refresh-ms`, default 30 s): the registry generation `control/registry-gen` has not
  changed since the one it applied (by its own GET, or through the coordinator's `view`), or it
  re-read `auth/keys` successfully.
- Not confirmed for more than **2 ticks** ⇒ the node answers `503` to requests authenticated by a
  key in the registry (`cairnflow_registry_stale_rejected_total` grows), because that key may have
  been revoked on another node. The bootstrap key (`CAIRNFLOW_ADMIN_KEY`) is not in the registry, so
  it is not closed — `/metrics` and `/cluster` can still be read with it. Confirmed again ⇒ reopened
  at once.
- The node logs at level `warn`, once per transition: "cannot confirm the key registry is still
  fresh — closing requests authenticated by registry keys (503) …" and "key registry confirmed fresh
  again — reopening authenticated requests".

## Diagnosis

1. **One node or the whole cluster?** The whole cluster ⇒ the bucket cannot be read (the provider's
   status page, credentials, request limits). One node ⇒ that machine's network path to the bucket.
2. The node log has `cannot re-read auth/keys` or `cannot read the registry generation` (level
   `debug`) — set `RUST_LOG=cairn_server=debug` on that node if needed.
3. Does that node still hear the coordinator? `GET /cluster` with the bootstrap key.

## Handling

- Fix the path to the bucket; the node reopens by itself at the next registry refresh tick.
- If it cannot be fixed quickly: take the node out of the load balancer (clients see `503` there
  anyway), or shut it down gracefully (`SIGTERM`). A node cut off from the bucket cannot write
  anything either.
- **Do not** defeat this mechanism by raising `--deploy-refresh-ms` very high: the freshness limit
  follows it, and a revoked key would be accepted longer on a cut-off node.
