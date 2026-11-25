# Key design: which range an invocation lives in, and when a call has to travel

This page is for people writing services that run on a **cluster** (`--partitions N`, several
ranges). On one node with one range every call is local, and this page is only something to know in
advance.

**Summary.** Every invocation has a 64-bit **partition key** (pk), carried inside its id. The range
table splits the pk space into **contiguous ranges**; each range has one writer, one thread, and each
of its commits goes through the object store. Two invocations in the same range call each other
**locally**: the callee is created right inside that range, without the network. Across ranges the
call goes outbox → HTTP → inbound, and each side must write durably before telling the other
anything. The `xcall` benchmark (two-way inter-range calls, a 4-range cluster on `memory://`)
measured a p50 of ~160 ms at concurrency 1 and a ceiling of ~42 inv/s at concurrency 64, while `echo`
within one range reached ~1 300 inv/s; on real S3 inter-range calls are slower still.

The pk is decided by **the call's key**, through the four tiers below. Choosing a key is choosing a
place.

---

## 1. The four tiers of pk derivation

Checked in order, the first tier present wins (`partition_key_for_target`, `callee_partition_key` in
`cairn-store`):

| Tier | The call has | pk | Consequence |
|---|---|---|---|
| 0 | a **scope** | `H(scope)` | Every target in one scope, **every service, every key**, shares **one** pk ⇒ always the same range. The scope's concurrency counter is a variable in that range's RAM |
| 1 | an **object key** (non-empty) | `H(service, key)` | Every call to the same object meets in the same queue — the condition for `EXCLUSIVE` to mean anything |
| 2 | an **idempotency key** | `H(service, idempotency key)` | Every call with the same key goes to the same deduplication place |
| 3 | no key at all | see below | |

Tier 3 depends on the entry path:

- **HTTP** `POST /invoke/<Service>/<handler>` with no key, no scope, no `Idempotency-Key`: a
  **random** pk.
- **`ctx.call` / `ctx.send`** with no key, no scope, no `idempotencyKey` — **grouped by prefix** (the
  `calls/prefix` gate on, section 7):
  `pk = high 32 bits of the caller's pk | low 32 bits of H(caller id, completion id)`.
  The callee lands **next to the caller**: a whole unkeyed call tree starting from one HTTP root lies
  in one 2³² window of the key space, i.e. almost always within **one range** (the probability that a
  range boundary cuts through the window is ≈ R/2³²). It is not **one** pk: auto split can still cut
  inside the window when the tree gets too hot (section 4). Each HTTP root has a random pk, so each tree has
  its own prefix and load across trees stays spread evenly.
- With the gate **off** (an old cluster that has not turned it on, section 7): `pk = H(caller id, completion
  id)` — deterministic but **unrelated to the caller's pk**: the callee lands in a random range.
- **`placement: "spread"`** on the call (section 8): deliberate spreading — the old rule
  `H(caller id, completion id)` even with the grouping gate on. For CPU-heavy child fan-out.

Both rules are deterministic: processing the same call again (after failover, after a retry) yields
the same id. The rule that applies is the **caller's**, fixed once when the caller is created: an
invocation created after the gate is on groups every unkeyed call it makes by prefix (except `spread`
calls, section 8); an invocation that existed before keeps the old rule for all its calls, including calls
made after the gate was turned on. That way turning the gate on midway never gives one call two ids.

Details people often ask about:

- **The tenant** goes into the hash (a tenant other than `default` hashes under its own tag), but it
  is not a tier: `Cart/x` of two tenants are two objects, two pks. There are no cross-tenant calls.
- **An empty key is not a key.** `key: ""` falls through to the next tier.
- **Workflow `run`:** the pk follows tiers 0–1 with the workflow key as the object key — `H(scope)`
  with a scope, otherwise `H(service, workflow key)`. The idempotency key is not part of the
  derivation (a workflow is already idempotent by key; HTTP refuses `Idempotency-Key` with `run`). An
  empty workflow key ⇒ refused. A workflow's `SHARED` handlers called with the same key get the same
  pk as `run`, so they are always in the same range.
- **The limit key is not part of the identity**, so not of the pk: it only affects queueing.
- **The pk is permanent.** An object's state, promises and idempotency table live under that pk.
  Changing the scope or the key means moving to **another object** (`/invoke/Cart/add?key=u1` and
  `/scope/bob/invoke/Cart/add?key=u1` do not see each other's state), not moving the old object.

## 2. Scope is inherited — the whole subtree stays in it

`ctx.call`/`ctx.send` **without a declared scope inherit the caller's scope**. An invocation that
starts with `POST /scope/acme/invoke/...` (or is called through `ctx.scoped("acme")`) puts everything
it calls, and everything those call, in scope `acme` — that is, **the same pk, the same range**,
whatever service and key they are.

- Switching to another scope is possible: `ctx.scoped("other")` or `{ scope: "other" }` on the call.
- **Leaving the scope is not**: there is no way to call "without a scope" from inside a scope (an empty
  scope is refused). The subtree of an invocation in a scope always has a scope.

## 3. When a call is inter-range

A call is **inter-range** when the callee's pk lies outside the range holding the caller's pk. The
answers travel with it: the callee's result (`Reply`), `attach`, `cancel`, and signals to an invocation
in another range take the same path. Two ranges on the same node are still inter-range — the messages
still go through the transport and two durable writes.

With `R` evenly split ranges:

| Caller → callee | Inter-range? |
|---|---|
| same scope (including by inheritance), whatever the service/key | **never** |
| same object (equal `H(service, key)`) | never |
| different object/idempotency keys, no scope | probability ≈ (R−1)/R |
| unkeyed `ctx.call` / `ctx.send`, no scope (gate on) | almost **never** (≈ R/2³²) — the call tree is grouped in the root's range |
| unkeyed `ctx.call` / `ctx.send`, no scope (gate off, old cluster) | probability ≈ (R−1)/R — spread |
| unkeyed `ctx.call` / `ctx.send` with `placement: "spread"` (section 8) | probability ≈ (R−1)/R — spread on purpose |
| no scope → into a scope, or one scope → another scope | probability ≈ (R−1)/R |
| a workflow `run`, no scope | like an object key: ≈ (R−1)/R |

The fourth row deserves a word: a naturally written workflow / agent tree (a parent `ctx.call`ing
tools, sub-agents with no key) is grouped in the root's range without declaring anything —
ScyllaDB or Cosmos DB make you choose a partition key to get this. On an **old** cluster without the
gate (section 7), the same tree on 4 ranges has ~75% inter-range calls; the way to group it there is to turn
the gate on, or to **set a scope on the root call** (one scope per user or tenant — section 5).

## 4. The price of grouping: a scope is capped at one range

Grouping is right, but it has a ceiling, and that ceiling is hard:

- **One scope is one pk, so one range.** A cairn range is one thread whose every commit goes through
  the object store. A scope's effective throughput and concurrency ceiling is the capacity of **one
  range on one node**, however high a rule sets `concurrency`
  ([`admin-api.md`](./admin-api.md) "Limits that must be stated").
- **A split cannot divide a pk.** Auto split recognises a range that is hot because of **one** owner
  and reports `hot_owner` instead of splitting — one owner never spans two ranges
  ([`admin-api.md`](./admin-api.md) "Auto split"). A tenant too large for one scope can only be divided
  into several scopes (for example `acme-0` … `acme-7`), accepting that each scope has its own counter
  and that objects of one scope do not see objects of another.
- **Auto split defaults to `suggest`:** it only proposes through `GET /ranges/suggestions`, it **does
  not cut by itself**. A range overloaded by many scopes / many objects stays one range until an
  operator splits it by hand (`POST /ranges/{id}/split?at=`) or turns on `--auto-split=on`.
- **An unkeyed call tree differs from a scope exactly here:** the tree is a **window** of pks, not one
  pk. A hot tree (large fan-out, CPU-heavy children) piles into one range, but auto split (`on`) or a
  manual split can cut **inside** its window: the two halves of the tree end up in two ranges. With
  `suggest`, the tree stays in one range until someone cuts it.

Numbers today: `echo` on **one** `file://` range reaches ~1 300 inv/s at concurrency 64 on a dev
machine, and that is also the ceiling of the single-process load generator. The ceiling of one range
on S3 with real latency is **not measured yet**; it is lower, because each WAL flush is a PUT.

**The cardinality of scopes is something to think about up front.** One scope per tenant / user is
fine; one scope per request is not ([`admin-api.md`](./admin-api.md)); one scope for the whole system
funnels everything into one thread.

## 5. Choosing keys by workload

| Workload | How to call | Why |
|---|---|---|
| Workflow / agent tree (a parent calling tools, sub-agents) | no key (gate on) — or a scope per user / tenant / session on the root call if you need a concurrency cap per scope | no key: the whole tree is grouped in the root's range, and auto split can cut it when hot; scope: the whole tree is local, one pk |
| Multi-tenant SaaS grouped by tenant | scope = tenant | grouped from the start, with a concurrency cap per tenant; split a tenant that is too large into a few scopes (section 4) |
| Saga across entities (`Order/o1` → `Payment/p9` → `Stock/s3`) | a separate object key per entity | inter-range calls are unavoidable — each entity must be its own object for `EXCLUSIVE` to be correct |
| ETL / batch fan-out, CPU-heavy children | no key with `placement: "spread"` (section 8), or no key with `--auto-split=on` | spreading is the goal: children run on many ranges, many nodes. `spread` spreads from the call itself, without changing any identity; the unkeyed default groups into one range until auto split cuts the tree (`on`). Do not use a separate key or idempotency key per child just to spread: they are identities (object, deduplication), not placement |
| ETL / batch fan-out, children waiting on I/O (LLM, external APIs) | no key, or a scope per job | a turn waiting on the SDK does not hold the engine thread; grouping is enough |
| One node / one range | anything | there are no inter-range calls |

**A scope is not only a placement.** It is part of the identity of every object, idempotency key and
promise inside it, and it turns on the per-scope concurrency counter
([`admin-api.md`](./admin-api.md) "Flow control"). Setting a scope on a tree that uses unscoped
objects moves that tree to **other objects** (section 1): do it at the start, or when you accept that the old
state stays where it was.

## 6. Reading the `cairnflow_calls_total` metric

`cairnflow_calls_total{placement, key_class}` counts `ctx.call` / `ctx.send` calls when the runtime
decides where to place the callee:

- `placement`: `local` (the callee is in the caller's range, created in place) or `remote` (the callee
  is in another range, goes through the outbox → HTTP).
- `key_class`: the tier that decided the callee's pk — `scoped` (tier 0), `object` (tier 1),
  `idempotent` (tier 2), `workflow` (a workflow `run`), `unkeyed` (tier 3).

The labels only take a bounded set of values (no service names, no range ids), so read them as
ratios:

```promql
# Share of inter-range calls by key class, last 5 minutes
sum by (key_class) (rate(cairnflow_calls_total{placement="remote"}[5m]))
  /
sum by (key_class) (rate(cairnflow_calls_total[5m]))
```

| `key_class` | A high `remote` share means | What to do |
|---|---|---|
| `unkeyed` | the `calls/prefix` gate is not on (≈ (R−1)/R: call trees are spreading), `placement: "spread"` calls (spread on purpose, section 8), or a tree just cut by a split (a small part) | check `cairn-ctl cluster status` (`call-prefix`); on an old cluster, turn the gate on (section 7) |
| `scoped` | calls that **change** scope (no scope → a scope, one scope → another); calls within one scope are always `local` | check whether one tree really needs several scopes |
| `object`, `idempotent` | calls between different objects — sagas | usually unavoidable; if the objects really belong to one user/tenant, put them in a common scope |
| `workflow` | starting a workflow from an invocation in another range | normal; only worth a look at high volume |

Compare with the number of ranges: with `R` evenly split ranges, ungrouped calls have a `remote` share
≈ (R−1)/R (0.75 with 4 ranges). A `key_class` close to that number is spreading completely; close to
0 it is grouping.

## 7. The `calls/prefix` and `calls/placement` gates: turning them on for an old cluster

Grouping by prefix (tier 3 of `ctx.call`/`ctx.send`, section 1) is a **migration gate** (step 4 of the
migration registry, `Run::Gate`), because calls written under the new rule carry a new field in the
outbox row that old binaries cannot read. The spreading option `placement: "spread"` (section 8) is a
**second** gate (step 5, `calls/placement`): a step-4 binary does not know the `placement` field of the
call command — it ignores the field, and when it processes a `spread` call again (after failover) it
places the callee by the grouping rule, i.e. creates **a second callee**. The gate guarantees no such
binary still runs on the cluster.

- **A new cluster** (empty bucket) created by this binary: both gates are **on from the start**
  (`min_binary=5`).
- **An old cluster**, including one that turned `calls/prefix` on with a step-4 binary: upgrade every
  node to the new binary, then `cairn-ctl cluster enable-call-prefix --apply` — **one command turns
  both gates on**. The command refuses while sessions of binaries that do not know step 5 remain; it
  sets `min_binary=5` (old binaries stop starting), waits for the node startup limit, checks again,
  then writes `call_prefix=on` and `call_placement=on` into `cluster-version` in one write. If it dies
  midway, run it again. Each range picks up the gate **at its next open** (leader change, split, node
  restart), and only invocations created after that carry the new rule; invocations that existed
  before (a long-running workflow) keep the old rule for all their calls.
- **One way.** There is no command to turn it off, and `cairn-ctl cluster lower-min-binary` does not
  lower `min_binary` below a gate step that is on (4 or 5): DBs may already hold calls written under
  the new rule.
- Status: `cairn-ctl cluster status` (`call-prefix ON/off`, `call-placement ON/off`),
  `GET /admin/cluster-version` (`callPrefix`, `callPlacement`).

Only the unkeyed tier changes. Scope, object key, idempotency key and workflows keep the rules of section 1 —
their state, promises and idempotency tables live under the pk given by those rules.

## 8. Spreading on purpose: `placement: "spread"`

Grouping is the right default for workflow / agent trees. A fan-out whose children are **CPU-heavy**
wants the opposite: children running on many ranges, many nodes. Declare it on each call:

```ts
// each child spreads by the old rule: ≈ (R−1)/R in another range
await Promise.all(items.map((x) => ctx.call("Resize", "run", x, { placement: "spread" })));
ctx.send("Report", "build", job, { placement: "spread" });
```

- **Only the placement changes, nothing else.** The callee gets `pk = H(caller id, completion id)` as
  with the gate off; the id is still deterministic (same id after failover, after a retry). No identity
  changes, no deduplication changes, no counter is turned on — unlike using a separate key or
  idempotency key per child just to spread.
- **Only the unkeyed tier.** With a scope (including a scope **inherited** from the caller), an object
  key or an idempotency key, `spread` is **ignored**: the key tier decides (section 1), because its
  state/promises/deduplication live under the pk given by that tier's rule. There is no error — the
  call runs as if nothing was declared. Within a scope every call is in the same range; to spread you
  have to leave the scope through a different scope per child (sections 2 and 4).
- **Ignored (the callee is grouped as by default) when**: the cluster has not turned the
  `calls/placement` gate on (section 7); or the caller was created **before** its range picked up the gate —
  the rule is the caller's, fixed when it was created, as with the grouping gate. An old runtime that
  does not know the field ignores it too. That never gives one call two ids: a binary that ignores
  `spread` is not allowed to run on a cluster with the gate on.
- **Reading the metric:** `spread` calls are counted under `key_class="unkeyed"` (section 6); a `remote` share
  ≈ (R−1)/R in that cell on a cluster with the grouping gate on means calls are spreading.
- Going inter-range pays the inter-range price (outbox → HTTP → inbound, section 3): worth it when a child
  runs much longer than one send, not worth it for children that take a few milliseconds.
