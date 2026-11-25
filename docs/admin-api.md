# Cairnflow — Admin & Introspection API

**Status:** follows the code, Phase 3.

**Machine-readable contract:** [`api/openapi.json`](./api/openapi.json) (OpenAPI 3.1, JSON
Schema 2020-12 schemas) for the response bodies of `/invoke`, `/send`, `/workflow/…`,
`GET /invocations`, `GET /invocations/{id}` and the error body. The TS e2e suite checks every
response of those paths against that file in strict mode; when that file and the prose below
disagree, the file wins.
Evolution rule: only **optional** fields are added — clients ignore unknown fields. Hand-written
clients (and the SDK's ingress client) should generate from that file or validate against it
instead of probing by hand.
That file **deliberately does not cover** the operational and diagnostic paths — `GET /health`
(the text `ok`), `GET /metrics` (Prometheus format), `/limits`, `/vqueues`, `/scheduler`,
`/stats/handlers`, `/metering`, `/invocations/{id}/spans`, `/ranges/stats`: their shape may change
between releases and is described only here; do not generate clients from them.

Every endpoint is HTTP + JSON. The `cairn` CLI is only a thin shell over these
endpoints — anything the CLI can do, `curl` can do (`cairn --token` / `CAIRNFLOW_TOKEN`
sends the key).

## Auth

Every endpoint below except `GET /health` requires `Authorization: Bearer <key>`: missing
or wrong ⇒ `401`, insufficient role ⇒ `403`. Each route declares a **capability set** (`read`,
`invoke`, `operate`, `provision`, `cluster`); a key's role is a fixed capability set, and the
key may call the route when the two sets intersect — the full table is in
[`self-hosting.md`](./self-hosting.md) §8. A node running `--no-auth` treats every request as
cluster-admin of tenant `default`. An `invoke` key may send signals (`POST
/invocations/{id}/signal`) and request a **cooperative** cancel (`cancel?mode=signal`) for its
own tenant's invocations; a hard cancel needs `operate`.

| Endpoint (capability `provision`; cluster-admin has it) | Meaning |
|---|---|
| `POST /admin/keys` `{"tenant": "acme", "role": "invoke"}` | Create a key. Returns `{id, tenant, role, createdAtMs, secret}` — `secret` (`cf_…`) appears in **this response only**; the bucket keeps its SHA-256. Roles: `invoke`, `view`, `admin`, `provision`, `cluster-admin`; `view`/`provision` before the `auth/keys-v2` gate (`cairn-ctl cluster enable-key-roles`) ⇒ `409` `code: key_roles_disabled`. A key without the `cluster` capability can only create/revoke the roles `invoke`/`view`/`admin` (otherwise ⇒ `403`) |
| `GET /admin/keys` | Every stored key, no secrets. Bootstrap keys (`CAIRNFLOW_ADMIN_KEY`, `CAIRNFLOW_ADMIN_KEY_SHA256`) and the hash-configured `provision` key (`CAIRNFLOW_PROVISION_KEY_SHA256`) are not in the list — they are not stored and cannot be revoked through the API (rotating = set a new hash, restart) |
| `DELETE /admin/keys/{id}` | Revoke. `204` **only when every live session in the cluster has confirmed it applied the change** (an acknowledged notice, 2 s limit per node) — from the moment you receive `204`, no node accepts the key any more. Nodes that do not confirm in time ⇒ `202` `{"pending": ["<node>", …]}`: the change is written, those nodes see it through the registry generation within one `--deploy-refresh-ms` tick, and a node that cannot confirm a registry fresher than 2 ticks answers `503` to every request authenticated with a registry key. `404` if absent. One node / `--data-dir`: `204` at once |
| `GET /admin/ops-reports` | Reports written by `cairn-ctl … --report-to-bucket <id>`, newest first: `{reports: [{id, lastModifiedMs, sizeBytes}], truncated}` (limit 1000; `cairn-ctl` keeps the newest 256 reports, ≤ 30 days) |
| `GET /admin/ops-reports/{id}` | One report: `{id, header, report}`; `report` = `{id, command, apply, binary, dataFormat, knows, who, startedMs, finishedMs, exitCode, ok, error, result}`, `result` is the command's `--json` output. `404` = not there yet (the command has not finished or not run — a control plane polls until it appears, or until the machine running it exits and then reads one last time); `400` = malformed id (`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`). Carries no secrets: only `backup`, `backup list`, `backup prune`, `restore`, `cluster status`, `cluster enable-*` accept the flag |

`/internal/*` is **not** part of this API: those routes exist only on the internal port
(`--internal-port`), accept only requests HMAC-signed with the cluster secret, and the ingress
port answers `404` for them.

**Web console**: the static files under `--ui-base-path` (default `/ui`) require no key —
they carry no data and only reveal the version; all UI data goes through this same API. Only
`GET`/`HEAD` under the base path belong to the UI; `GET /` with `Accept: text/html` ⇒
`302 /ui/`, every other request to `/` keeps its old behaviour. A deep link without `text/html`
and a missing asset ⇒ `404`. `--no-ui` turns it off entirely. The base path comes only from the
flag, `X-Forwarded-Prefix` is not read; a base path equal to the first segment of an API route
(`/invocations`, `/admin`, …) is a startup error.

**Console session**. The browser does not hold the API key: the console exchanges the key for a
cookie.

| Endpoint | Meaning |
|---|---|
| `POST <base>/session` `{"key": "cf_…"}` | Checks the key as Bearer would. `200` `{tenant, role, keyId, bootstrap, sessionExpiresAt}` + `Set-Cookie: cairn_ui=…; HttpOnly; SameSite=Strict; Path=/; Max-Age=43200`; when `Secure` (per `--ui-cookie-secure`, on by default when `--host` is not loopback) the name is `__Host-cairn_key` and only that name opens a session. A request carrying two valid sessions with different identities ⇒ `401`. Wrong key ⇒ `401`. `--no-auth` ⇒ `{"noAuth": true}`, no cookie |
| `DELETE <base>/session` | `204` + an expired cookie. Revokes this session on the server (its random session id goes into `auth/revoked-sessions`); other sessions of the same key live on. Over the tenant's 200 unexpired logged-out sessions, or over the per-node logout rate ⇒ `429` (the cookie is still removed). See `docs/self-hosting.md` §8 |
| `GET /identity` (admin; `?tenant=` for cluster-admin) | `{tenant, signing, keys: [{generation, kid, active}]}` — the tenant's public request-identity keys: the `kid` (`publickeyv1_…`) goes into the SDK's `serve({identityKeys})`. `signing: false` when the node has no `CAIRNFLOW_IDENTITY_MASTER`. Between `cairn-ctl identity publish` and `retire` there are two keys |
| `GET /audit?from&to&actor&action` (read; `?tenant=` for provision/cluster-admin) | `{tenant, from, to, records: [...], truncated}` — the data plane's audit log: every human action — `kind: row` (cancel, purge, unstick, restart, migrate, signal, schedule, bulk — written in the same commit as the action), `intent`/`done` (deployment, key, tenant setting/state, rule, capture, quarantine — the intent before, the outcome after), `denied` (commands refused with `403`, merged per minute, `count`), `noop` (actions inside a range that changed nothing — outcome `noop`/`not_found` — merged per minute by actor, action and outcome, `count`). Each record: `ts`, `actor {type, id, role}`, `action`, `target`, `outcome`, `ip`, `userAgent`, `requestId`. Default 24 h; at most 31 days and 1000 objects per request. The `sys_audit` table has the same data (24 h) |
| `GET /auth/whoami` (every key) | `{tenant, role, keyId, bootstrap, noAuth, via: "cookie"\|"bearer"\|"none", node, version, clusterVersion, envLabel, sessionExpiresAt}` — no secrets; `envLabel` = `--ui-env-label` or `null` |
| `GET /tenant` (admin, the key's tenant) | `{tenant, state, limits: {retentionMs, maxPayload, maxJournal, deployRate, maxLive, maxDeployments, maxSchedules, maxTimers, maxStreams, maxStateKeys, maxArtifactBytes, maxJournalBytes, maxValueBytes}, usage: {live, artifactBytes}}` — the state (`active`/`limited`/`suspended`) and the per-tenant limits of the `tenants` registry; `null` = no limit; `usage` is counted by a background loop (only when `maxLive`/`maxArtifactBytes` is set, up to one 5 s tick late). Only the `provision` capability can change it (`PUT /admin/tenants/{tenant}[/state]`) |

The cookie does not contain the key: it carries the key id (`kid`; for a bootstrap key,
`bootstrap:<first 8 bytes of the sha256>`), tenant, role, a random session id and expiry — HMAC-signed with a key
derived from the cluster secret, so any node in the cluster can check it. **Every request looks
the `kid` up in the key registry again**: revoking a key kills its sessions at the same pace as
Bearer; changing `CAIRNFLOW_ADMIN_KEY` kills every old bootstrap session. Logging out revokes only that session. Lifetime 12 hours, no
sliding renewal. When `Authorization` is present the cookie is ignored (a wrong Bearer is `401`,
it does not fall back to the cookie).
A **cookie** request with an unsafe method (`POST`/`PUT`/`DELETE`, `/query` included), and
`POST/DELETE <base>/session` too, must carry `Sec-Fetch-Site: same-origin` or (without that
header) an `Origin` equal to `Host` — otherwise `403`. The CLI and SDK use Bearer and are not
checked.

## Tenant

Every request acts in the **key's tenant** (`--no-auth`: `default`). No path parameter or
header selects another tenant — except `?tenant=` for a `cluster-admin` key on
`/deployments*`, `/services*`, `/deployments/{id}/replay-check` and `GET /invocations`, and for
a `provision` key on `GET /identity`, `GET /metering*` (any other role sending `?tenant=` ⇒
`403`).

- `/invoke*`, `/workflow/*`, `/promises/*`: service, state, promise, idempotency key and
  workflow id all live in the key's tenant — two tenants' `Cart/x` are two objects.
- Endpoints by id (`/invocations/{id}`, `/journal`, `/logs`, `/spans`, `/streams/*`, `/cancel`,
  `/purge`, `/resume`, `/signal`, `/replay`, `/migrate`, `/restart`): another tenant's
  invocation ⇒ `404`, exactly as if it did not exist. `cluster-admin` sees every tenant.
  `migrate`/`restart`/`replay` only accept a deployment of the invocation's own tenant.
- `GET /invocations`: the tenant filter runs in the engine **before** `limit`, like every other
  filter. The tenant filter reads entry 0 of every row, so every listing is a bounded scan
  (10 000 rows and 2 s per range); hitting the bound ⇒ `x-cairn-partial: true`.
- The rule book (`/rules`) is cluster-wide; counters (`/limits`) are per tenant, shown as
  `tenant:scope/...` for tenants other than `default`.

## Error contract

Errors are JSON:

```json
{ "error": "a sentence for humans", "leader": "http://10.0.0.7:8080" }
```

`leader` is the **ingress** port of the leading node — never the internal port.

- `error` — for humans. Do not parse it. Always present.
- `leader` — **for machines**, present only on a `503` when the node asked is not the
  partition's leader and the routing table knows who is. The client retries exactly once at
  that address. A `503` without `leader` = the cluster is electing; back off and ask again.
- **Forwarding instead of `503`**: for a request carrying the header
  `x-cairn-forward: leader`, a node that does not lead the range **runs the request at the
  leader itself** over the internal port (one hop; the leader authenticates again from the
  original headers) and returns the leader's response, with `x-cairn-forwarded-by: <node>`.
  This applies to every path that answers `503` + `leader` (by id, by key, split/merge) and to
  each id of `POST /invocations/batch/{op}`. Leader unreachable ⇒ still the original `503`
  (nothing ran yet). Sent but no response ⇒ `504` with `id` when known: the operation **may**
  have run — ask for the state again (or resend with the same `Idempotency-Key`) before doing it
  again. Without the header ⇒ the `503` behaviour above, unchanged. The console always sends
  this header; the SDK does not (it follows `leader` itself). Metric
  `cairnflow_forward_total{result="relayed|refused|ambiguous"}`.
- `id` — the invocation the error is about, when it already had an identity when the error
  happened (e.g. a `502`/`503` from `/invoke` when the turn lost leadership midway or an error at
  admission — see §Calling services, "Infrastructure errors of a turn"). An `id` does not mean
  the invocation surely exists: look up `/invocations/{id}`, or resend with the same
  `Idempotency-Key`.
- `code` — a machine-readable code, absent on most errors: `range_overloaded` (`429`, with
  `retry_after_ms`, see §Flow control), `wrong_range` (`409`, with `table`),
  `egress_denied` (`400`: the node's egress policy — flags `--egress-*`,
  [`self-hosting.md`](./self-hosting.md) §6 — refused the deployment's endpoint at `POST
  /deployments` or in a replay check; the message says why, without the address a name resolved to
  or the blocked range, and does not echo content from the destination side).
  `stream_expired` (`410`: reading a temporary stream that was sealed more than 60 s ago, see
  §Streams), `sse_limit` / `too_many_open_streams` (`429` + `Retry-After`: the node or the tenant
  already has enough long-lived connections), `store_unavailable` (`503`: the cell's store has a
  transient error — the body carries no store details, resend later; commands that create work
  resend with the same idempotency key), `keys_busy` (`503` + `Retry-After` on
  `POST /admin/keys`/`DELETE /admin/keys/{id}`: the key registry keeps changing because of
  concurrent key commands — nothing written yet, resend), plus the tenant, key and limit codes —
  the complete list is the `ErrorCode` enum of [`api/openapi.json`](./api/openapi.json).
  `POST /query` sends no `code`: read limits are reported by an `error` starting with
  `query_budget:` (`422` a query over the limit, `429` the tenant ran out of read units).

Every error of the paths in [`api/openapi.json`](./api/openapi.json) — including the `401`/`403`
of the key layer — has this JSON body.

## Deployments & services

| Endpoint | Meaning |
|---|---|
| `POST /deployments` `{"uri": "http://sdk:9080"}` | Discovery + version negotiation, creates a **new, immutable** deployment with id `dp_…`, then moves the revision of every service in the manifest to it (one CAS on the tenant's catalog). Registering the same endpoint again yields a new id. No overlapping version ⇒ refused **at registration**. No partition write rights needed — a follower can register too. `?activate=false`: only creates the deployment and moves **no** revision — compare manifests, run a replay check, then move each service with `PUT /services/{service}/revision` with `expect` |
| `GET /deployments` | Every known deployment, from the operator's point of view: `id`, `endpoint`, `createdAtMs`, `protocolMode` (`BIDI_STREAM` h2c or `REQUEST_RESPONSE` h1 — the first thing to look at when every invocation of one deployment fails at the transport), `currentFor` (the services it **is the revision of**) |
| `GET /deployments/{id}` | As above, plus `pinnedInvocations`: the number of **unfinished** invocations in the whole cluster still pinned to it. Absent = not every range could be asked. And `manifest`: services, handlers with their **effective** type, and the retry/timeout contract captured at registration — to compare a deployment not yet promoted with the current one |
| `DELETE /deployments/{id}` | Remove a deployment. `409` while it is still the revision of a service, or invocations are still pinned to it; `503` when not every range could be counted |
| `PUT /services/{service}/revision` `{"deployment": "dp_…", "expect": "dp_…"?}` | Move a service's revision to an existing deployment — rollback or pinning. `expect` = move only if the pointer is **currently** there (`409` otherwise): when two people roll back at once, exactly one wins |
| `GET /services` | Callable services + handlers, from the client author's point of view — **per the current revision** (`deployment`, `deploymentId`). A service's `ty` is `service` · `virtualObject` · `workflow`; a handler's `ty` is the **effective type** (a handler that declares nothing shows as `shared` in a workflow and `exclusive` in a virtual object). `exclusive` handlers queue per object key |

**Handler contract** — declarable in the manifest at the service or handler level (handler
overrides service), field names equal to Restate's endpoint manifest (compatible with Restate's
service protocol): `inactivityTimeout` (ms, overrides `--inactivity-timeout-ms`),
`retryPolicyInitialInterval` / `MaxInterval` / `ExponentiationFactor` (backoff),
`retryPolicyMaxAttempts` (total attempts including the first; absent = unlimited),
`retryPolicyOnMaxAttempts` (`PAUSE` by default | `KILL`), `journalRetention`, `trace`
(`runtime` | `off`), `logFlushMs` (ms: flush durable stream records that no commit has carried
yet on the node's clock, to keep logs when the node dies inside `ctx.run` — one range commit per
tick while records are waiting; absent = they only ride along with existing commits). Attempts
exhausted: the invocation is `paused` (or `failed`) with code `575` — the message includes the
last attempt's error; `resume` grants one more round. Values are captured in the deployment —
changing the policy means registering a new deployment.

**An invocation is pinned to its deployment**: entry 0 records the deployment id at creation,
and every later turn calls exactly that endpoint — even after the revision has moved to a new
deployment. New invocations follow the revision. Deploying a new version therefore does not drag
in-flight invocations onto new code; moving them is a deliberate act (replay check, `restart`).

## Calling services

**Two doors, and the response shape is a property of the door — not of the cluster.**

| Endpoint | Meaning |
|---|---|
| `POST /invoke/{service}/{handler}?key=K` | **Wait for the result.** The body is the input, raw bytes. Returns `200 completed` / `500 failed` / `409 paused`; when the `--invoke-wait-ms` limit (default 5 s) runs out, `202 waiting` with the id. Waits through retries after transient turn errors — a handler reporting a retryable error, or a failing node → SDK hop (see "Infrastructure errors of a turn" below) |
| `POST /invoke/{service}/{handler}/send?key=K` | **Fire and forget.** Returns `202` as soon as the invocation is **durable** — does not wait for it to run. The returned id can be looked up at once at `/invocations/{id}` |
| `POST /scope/{scope}/invoke/...` | As above, but **inside a scope** — see §Flow control |
| `POST /workflow/{service}/{key}/attach` | Waits for **a workflow's run** to stop for good, without needing the id. Same response shape as `/invoke`; `404` if nobody called `run` yet. Accepts `?wait_ms=` like attach by id |
| `POST /invocations/{id}/attach[?wait_ms=N]` | Waits for **one invocation by id** to stop for good — the way to continue after a `202` from `/invoke`. Role `invoke` (the key that called); another tenant or absent ⇒ `404`. Same body as `/invoke` (`200` done, `500` failed, `409` paused, `202` still running). `wait_ms`: long-poll up to `min(wait_ms, --attach-max-wait-ms)` (default 60 000; `0` = return at once; invalid ⇒ `400`); absent ⇒ `--invoke-wait-ms`. Clients should wait 25 000 — under the 30 s limit of the GCP LB and the 60 s of ALB/nginx: one waiting client costs ~144 read-fence GETs per hour (at 5 s: 720). `GET /invocations/{id}` does **not** long-poll |
| `GET /workflow/{service}/{key}/output` | As above but **without waiting**: `200` if `completed`, `500` if `failed`, `409` if `paused`, `202` if running, `404` if it never ran — body as `/invoke` |
| `GET /workflow/{service}/{key}/id` | **Only computes** the run id of `(tenant, scope, service, key)` — the same function as the two doors above, reads nothing and does not say whether the workflow has run: `200` `{id, tenant, service, key}`. Empty key ⇒ `400`; `cluster-admin` picks the tenant with `?tenant=`. The console uses it and then does a point read `sys_invocation WHERE id = $1` |

Every response carrying an id from the doors above (and attach by id) has the header
`x-cairn-invocation-id` — clients read the id without parsing the body.

**The response body** of all five doors above is an object discriminated by `status`
(schemas `Invoke*` in [`api/openapi.json`](./api/openapi.json)):

| Code | `status` | Other fields |
|---|---|---|
| `200` | `completed` | `id`, `output` |
| `202` | `waiting` | `id` |
| `409` | `paused` | `id`, `code`, `message` (e.g. `575` `maxAttempts` exhausted) |
| `409` | `already_invoked` | `id` (the `run` holding the workflow key) |
| `429` | `throttled` | `id`, `retry_after_ms`, header `Retry-After` |
| `500` | `failed` | `id`, `code`, `message` |

- **`output` is a string**, not a JSON value: the handler's result bytes decoded as UTF-8
  (invalid bytes become U+FFFD). The TS SDK writes results with `JSON.stringify`, so for it
  `output` is a **string containing JSON** — the client does one more `JSON.parse(body.output)`.
  `GET /invocations/{id}` uses the same convention.
- **`/workflow/…/output` and `/attach` return `500` for a `run` that `failed`**, `409` for
  `paused`, and `200` only for `completed` ("finished" does not include failure). A scoring
  client should still read `status` in the body: a `500` can also be a lower-layer error with the
  generic error body (`error` present, no `status`).
- **`waiting` is only an answer of the calling door**, meaning "not stopped for good yet, ask
  again later" (waiting on a timer, on a VO lock, or `/send`). It is **not** a durable state:
  `GET /invocations` and `GET /invocations/{id}` only return
  `running|suspended|completed|failed|paused`, and `?status=waiting` is `400`. An invocation that
  is "accepted, not finished" is `running` or `suspended` there.
- Errors that are not an invocation's outcome (`400`, `401`, `403`, `404`, `503`, `429
  range_overloaded`, lower-layer `500`/`502`) carry the generic error body of §Error contract.
- **Infrastructure errors of a turn** (pinned in `tests/invoke_transient.rs`). A turn that fails
  on the node → SDK hop — the SDK cannot be reached, the connection resets mid-turn
  (`error sending request`, `error decoding response body`), the SDK returns a 5xx — **after** the
  invocation was accepted: the turn leaves a retry appointment (100 ms, backing off) and `/invoke`
  (and `/scope/…`) **waits through** it as it does for a retryable handler error — the client gets
  the final result (`200`/`500`/`409`), or `202 waiting` with the id when the `--invoke-wait-ms`
  limit runs out (the SDK is dead for good, the next attempts fail too). The last attempt's error
  is in `lastFailure` of `GET /invocations/{id}` (code `500`). `waiting` goes out only **after**
  the admission is durable (the durable ticket of the answer); waiting at the door adds no SDK
  call — the retry is the one `/send` would also get. Restate and Temporal do the same: the client
  does not see transient errors. Before 2026-10-21 this branch returned `502` with the generic
  error body and the `id` at once.

  Still a **generic error body with `id`**, without waiting:
  - the turn **lost leadership** midway — `503` (`leader` if known), or `502 "engine stopped"`
    when the node steps down and cancels the turn: an old leader has nothing to promise; the
    invocation (if durable) continues at the new leader;
  - an error **at admission** (writing the new invocation failed — an ambiguous outcome for the
    caller), `502`: the invocation may not exist yet.

  A client that gets an error with an `id`: do not resend without an `Idempotency-Key` (the
  invocation may already be accepted — that runs it twice); look up the `id`, or resend with the
  same key.

`202` can still happen at the first door, and it must: a handler that sleeps an hour must not
hold an HTTP connection for an hour. What was fixed is **when** it happens.

Before Phase 4, the `/invoke` door returned right after the first turn, so the response shape
depended on **where the callee hashed to**: a `ctx.call` in the same partition finished inside
the caller's turn ⇒ `200`; across partitions it could not ⇒ `202`. Same code, same request, two
shapes — a single-partition dev saw `200`, an eight-partition prod saw `202`. Now the rule is the
same in every cluster shape.


The header `Idempotency-Key: <key>` enables idempotency for the HTTP door
([`protocol-v1.md`](./protocol-v1.md) §8.6): a client retrying with the same key points at the
same old invocation — if the result exists it is returned at once, if it is running, `202` with the **same id**.
Without it, a retry after a timeout is a side effect run twice. The dedupe window is the
retention (see `--retention-ms` below).

**Workflows differ from those two rules**: calling the `run` handler **must** carry `?key=`,
**must not** carry `Idempotency-Key` (`400` — the workflow key is already the dedupe key), and a
second call for the same key gets `409 already_invoked` with the id of the run holding that key.
From the `409`, continue with `/workflow/{service}/{key}/attach`. CLI:
`cairn workflow <svc> <key>` (add `--attach` to wait, `--scope S` if the workflow runs in a
scope).

The returned id is a contract: **48 hex characters, treat it as an opaque string**. You can
store it in your database and look it up later at any node. A call carrying `Idempotency-Key`
gets an id **deterministic in the key tuple** — calling again yields exactly that id.

Inside, the first 8 bytes are the *partition key* (a position in the `u64` key space), the next
16 bytes are the identifier; the partition holding the id is the key range containing it in the
cluster's table (`cluster.conf`). Do not derive the partition from the id on the client side —
ask `GET /invocations` (the `partition` field). The table may change later; the id does not.
(Old ids, before the 2026-10 refactor, were 32 hex characters whose first 2 bytes were the
partition *number* — a bucket carrying old ids is refused at startup.)

## Flow control: scopes, limit keys, concurrency limits

The concurrency limit is **the only valve that can close before money leaves the account**:
cairn's target workload is one where every running invocation costs money, so a load spike does
not make the system slower — it makes the model bill bigger.

### Two entry paths, and they differ in more than a prefix

| Path | Meaning |
|---|---|
| `POST /invoke/...` | **No limit at all** besides the node's turn limit. Kept as is, behaviour unchanged |
| `POST /scope/{scope}/invoke/...` | Everything this invocation touches **lives inside the scope** |

A scope is not a label for counting — it is **part of the identity**:

| Thing | No scope | Inside scope `bob` |
|---|---|---|
| Virtual object `Cart/a` | one instance | a **different** instance, its own state, its own queue |
| `Idempotency-Key: k` | deduplicated globally | deduplicated **within `bob`** |
| Promise `(svc, key, name)` | one promise | a separate promise |

A consequence to know before using it: **`/invoke/Cart/add?key=u1` and
`/scope/bob/invoke/Cart/add?key=u1` are two different objects.** They do not see each other's
state and do not lock each other.

Scope: `[a-zA-Z0-9_.-]`, non-empty, ≤ 36 bytes. Malformed ⇒ `400`.

### Limit key

`?limit-key=l1` or `?limit-key=l1/l2` (or the header `x-cairn-limit-key`). Subdivides the
limit **within** a scope.

- **Requires a scope.** A limit key on the unscoped path ⇒ `400`: without a scope there is no
  counter to hang it on, and silently ignoring it would let the operator believe they just set
  a limit.
- **Not part of the identity.** Two calls with the same target, same scope, same object key
  but different limit keys still reach **the same** virtual object. It only affects queueing.

### Rule book

| Endpoint | Meaning |
|---|---|
| `GET /rules` | The whole rule book with its `version`; `rules` (by pattern) and `tenants` (per-tenant rate limits) |
| `POST /rules` `{"pattern": "acme/growth", "concurrency": 5\|"unlimited", "rate": 50\|"unlimited", "burst": 100, "description": "...", "disabled": false}` | Add/change a rule by pattern. **Idempotent and merging** — an absent field means *leave it alone*, not *delete it*; `null` in `concurrency`/`rate`/`burst` **removes** that limit back to unset (the limit of a less specific rule applies — unlike `"unlimited"`) |
| `POST /rules` `{"tenant": "acme", "rate": 50, "burst": 100}` | Rate limit of the **whole** tenant `acme` (every scope, every range). Does not accept `concurrency` |
| `DELETE /rules/{pattern}` | Delete. `404` if absent |
| `DELETE /rules?tenant=<t>` | Delete the tenant's rate-limit row. `404` if absent |

`pattern := comp` \| `comp/comp` \| `comp/comp/comp`, each `comp` is a value or `*`.
**More specific wins**, ordered from the scope down: `checkout/premium` > `checkout/*` >
`*/premium` > `*/*`. A rule applies only to counters **at its own depth**: `scope/l1` limits the
L1 level, not the scope.

> **`*` is NOT a shared pool.** `{"pattern": "*", "concurrency": 1000}` gives **each** scope its
> own budget of 1000; two scopes run 2000 invocations at once under that rule. This is the number
> one misunderstanding, and it fails in the expensive direction — so the API prints this sentence
> in the `warning` of every response involving `*`.

**Each attribute has its own winner** (rule book `/2`): a counter's concurrency limit comes from
the most specific rule that *sets* `concurrency`; the rate limit from the most specific rule that
*sets* `rate`. Unset fields are **absent** from `GET /rules`. A rule that only sets `rate` does
not remove the concurrency limit a broader rule applies to that counter.

**Rate limits** (`rate`) are measured in **CU/s** — the same unit as `cu` in `GET /metering`:
every SDK turn costs `durable steps + ⌈run ms / 100⌉` CU. "Run ms" is the time the turn is
**open** (`turn/sdk.rs`, `metering::run_time`), including while a `ctx.run` closure waits on an
external API or model; a turn ends when the invocation suspends (`sleep`, waiting on a
promise/awakeable, waiting on a `ctx.call` result), so suspended time is not counted. A 10 s model
step is ≈ 100 CU. Before a turn 1 CU is taken, after the turn the rest (the bucket may go
negative: with a debt, the next turn waits). `burst` (default = `rate`) is the most CU that can
accumulate.

- Per **tenant** (`{"tenant": …}`): one bucket per tenant **on each node**, each node holding
  `rate / number of nodes leading at least one range` — the total never exceeds `rate`; when the
  load concentrates on one node the tenant gets less than `rate` (the deliberate price of an
  even split).
- Per **pattern**: like concurrency, every `(tenant, counter)` matching the rule has its own
  bucket (the scope decides the range, so the bucket is not split per node).
- Only **turns** are throttled: creating an invocation is not throttled (the 1 CU at creation
  still goes on the bill).

`disabled` is a **soft** flag: turns the rule off without losing its definition. A disabled rule
is **absent** from matching, so disabling a specific rule can make a broader `*` rule win — that
is, it **loosens** the limit rather than removing it.

The rule book is **one object** on the cluster's own bucket, written by CAS on the etag — there
is no second configuration store to operate. On a store that cannot CAS (`--data-dir` with
`LocalFileSystem`) it falls back to overwriting and logs one `warn!` line: safe with one node,
because the missing CAS already prevents forming a cluster.

**Rule changes have a delay** of about one poll cycle (2 seconds). Not a bug, a choice: the rule
book changes a few times a day while the permit path runs several thousand times a second.
Compare `version` at `/rules` with `rulesVersion` at `/scheduler` to see that delay as a number.

### `429` — and how it differs from `202`

| Situation | Code | Why |
|---|---|---|
| Waiting for the `EXCLUSIVE` lock of a virtual object | `202 waiting` | Normal serialisation, not a limit |
| Scope/limit key **out of budget** | `429 throttled` + `Retry-After` | The tenant hit its limit — this is the only signal that says the bill is hitting its limit |
| Range **overloaded**: compaction behind, L0 near its limit | `429` `code: "range_overloaded"` + `Retry-After`, **no `id`** | Soft throttle before the L0 wall. **Nothing created** — see below |
| Tenant/counter **out of tokens** (`rate` limit) | `429 throttled` + `Retry-After` = when the bucket turns positive again | As above, for the rate limit; background turns are scheduled for exactly that moment |
| Node is not the leader | `503` + `leader` | Change node, not wait |
| Range is splitting/merging, or a freshly created range has no leader yet | `503` without `leader` | Wait a tick and ask the same node again. Ingress waits first by itself (up to 2 s: the table changes, the new range gets a leader) before answering `503` |

`429` here is **not a "refusal"**: the invocation was accepted, is durable, is in the queue and
is scheduled to run again. The body still carries the `id`, so the client looks up
`/invocations/{id}` as usual. `Retry-After` (seconds) says when to come back.

`503` is not used for this because in cairn `503` already means "not the leader", and clients
handle those two situations differently.

#### `429 range_overloaded` — **no id, no invocation**

A completely different contract from `429 throttled` above, and clients must tell them apart by
`code`:

```json
HTTP/1.1 429 Too Many Requests
Retry-After: 1

{"error": "range is overloaded (L0 near its limit, compaction behind) — nothing created, retry later",
 "code": "range_overloaded", "retry_after_ms": 1000}
```

- **The invocation was not created.** No `id` in the body; `/invocations/{id}` of a
  self-computed id will be `404`. Resending after `Retry-After` is **safe** and is what the client
  should do.
- **The same idempotency key matches.** A resend carrying `Idempotency-Key` (or a workflow's
  `run`, whose id is deterministic in the key) yields exactly one invocation like any other
  resend. An invocation that **already exists** never gets `429 range_overloaded`: the resend
  returns its outcome (`200`/`202`/`409`) as usual, even while the range is throttling.
- **Only new work is throttled**, and only at `POST /invoke…` (`/send` included). Everything of
  running invocations — completions, timers, retries, signals, `attach`, results — goes on:
  draining the backlog is what brings L0 down. A `ctx.call`/`ctx.send` to a throttling range never
  fails: it is accepted durably and runs more slowly.
- **Probabilistic, never 100 %.** From `--l0-soft` (default 5) L0 views under a limit of 8, the
  rejection probability rises linearly: 5 → 0.3; 6 → 0.6; 7 and above → 0.9
  (`--l0-max-reject`). Lowering has hysteresis: it returns to 0 only after L0 has stayed below
  `--l0-soft` for 5 s.

Metrics: `cairnflow_range_l0_views{range}`, `cairnflow_range_l0_reject_ratio{range}`
(current throttle level), `cairnflow_range_l0_shed_total{range,kind=ingress|inbound|dispatch}`
(new work shed: `429` at ingress, `StartCall` from another range deferred, a local child call
deferred), plus SlateDB's numbers (`cairnflow_range_slatedb_backpressure_total`,
`cairnflow_range_slatedb_l0_stalls_total`, `cairnflow_range_slatedb_l0_ssts`) and write-stall
time (`cairnflow_range_write_stalls_total`, `cairnflow_range_write_stall_seconds_total`).

### Diagnostics

| Endpoint | Answers the question |
|---|---|
| `GET /limits` | "Which tenant is spending how much, what is the limit, how much is left" — with the `rule` currently winning at each counter |
| `GET /vqueues` | "Which queues have work, how much is waiting, where is it blocked" |
| `GET /scheduler` | Queue depths, permits in use, locks held, the `rulesVersion` actually applied; `tenants`: the bucket (`rate`, `burst`, `peers`, `balance` CU — negative is debt) and `vt` of every tenant that has a limit or is waiting on the node |

These three only speak about **the partitions this node leads** — the same rule as
`/invocations`.

**Two things to know so as not to misread them:**

- `GET /limits` **only lists non-zero counters.** A counter back at 0 is removed from the table;
  keeping every scope that ever ran forever would be a slow memory leak. To know the limit of a
  scope that has not run anything, ask `/rules`.
- **Per-queue statistics are not persisted.** `granted`/`blockedTimes` reset to 0 after every
  leader change. INV-3 requires *pending work* to live in the state tables, and statistics are not
  pending work — and in cairn every increment of a number is a commit through the object store.

Metrics (`/metrics`): `cairnflow_flow_granted_total`,
`cairnflow_flow_wait_ms_total` (divide by `granted` for the average),
`cairnflow_flow_blocked_total{resource=lock|concurrency|turns}`,
`cairnflow_flow_throttled_total`, `cairnflow_invoke_throttled_total`. Labels are by resource
**kind**, not by value: scopes are user-chosen strings, and a metric label set by outsiders is a
way to blow up Prometheus. Ask `/vqueues` who is blocking whom.

### Limits that must be stated

A scope is **one partition**, and a cairn partition is a single stream with every commit going
through the object store. So **a scope's effective concurrency is bounded above by what one
stream of one node can do**, whatever the rule says: setting `concurrency = 10000` on a scope
does not give you 10000 — it gives you what one partition can carry.

**Scope cardinality is the thing to think about first.** One scope per large tenant is fine; one
scope per request is not.

A scope also decides the **range** of everything it calls (the whole subtree inherits it), so it
is the tool for gathering calls onto one range: [`key-design.md`](./key-design.md).

## Doors for external systems (human-in-the-loop)

Wake a waiting workflow **with one HTTP request**, without writing any intermediate invocation.

| Endpoint | Meaning |
|---|---|
| `POST /invocations/{id}/signal` | Body `{"signal": "approve"\|4, "value": <JSON>}`. `signal` is a string (named) or a number (built-in). Wakes `ctx.signal` / `ctx.awaitSignal` (including signal leaves inside `ctx.race`/`ctx.all`). Arriving exactly while the id's range is splitting/merging, it waits for the table to change and is delivered at the new range (`503` if not settled after 2 s) |
| `POST /invocations/{id}/resume` | Takes it out of `paused` and continues **within the request**. `409` if not paused, `404` if absent |
| `POST /invocations/{id}/replay?deployment=<id>` | **Replay check, read-only**: runs the existing journal against the given deployment (default: the deployment it is pinned to) — no commit, no delivery, no closure of the new code runs. Returns `{matches, upto, mismatchAt?, expected?, got?, message?}`. A deployment below wire v3 ⇒ `409` |
| `POST /deployments/{id}/replay-check?service=&sample=20` | Replay-checks a sample of **unfinished** invocations of `service` against deployment `id`, on the ranges this node leads. Returns `{checked, matched, failures, ranges}` — whether to block or allow moving the revision is the caller's policy. `&scope=cluster`: runs the same request at every other node over the internal port (each node authenticates again, `sample` is the limit per node) and sums the reports, adding `partial`/`missing` |
| `POST /invocations/{id}/migrate?deployment=<id>[&force=true]` | Moves an **unfinished** invocation to another deployment — the only way to change the deployment it is pinned to. By default requires a matching replay check on the target first (`409` with the report on a mismatch); `force=true` skips the check |
| `POST /invocations/{id}/restart?from=<n>&deployment=<id>` | **`restart-as-new`**: a new invocation whose journal is the first `n` entries of the old one (default 1 — only `Input`), on the given deployment (default: the one the old one is pinned to). `ctx.run` results inside the prefix are reused. If the old one is unfinished it ends with `{"continuedAsNew": "<id>"}`; waiters and the idempotency key move to the new one. A workflow's `run` ⇒ `409`. The body `{"patch": {"entry": n, "input": …}}` (an input patch — "fix it and rerun from a step") is **not open yet**: present ⇒ `400`, no restart; any other body is ignored. SDK: `ctx.continueAsNew(input)` (wire v3) — the same path, new input, runs on the current revision |
| `POST /invocations/batch/{cancel\|purge\|resume\|restart}` | **Bulk**: `{"ids": [...]}` 1–1 000 ids (otherwise ⇒ `400`), runs **exactly the single operation** for each id — same role (admin; cancel's `?mode=signal` is admin here too), same tenant check — at most 5 ids at a time, order preserved. Query parameters (`mode`, `from`, `deployment`) apply to every id. Returns `{op, results: [{id, ok, status, result \| error}]}`; one failing id (`404` another tenant, `409`, `400` malformed id) does not fail the batch. Unknown operation ⇒ `404` |
| `POST /awakeables/{id}/resolve` (JSON body) · `POST /awakeables/{id}/reject` `{code?, message}` | **Awakeable**: completes a `ctx.awakeable()` wait with the `awk_…` id the handler sent to the external system. An `invoke` key of the same tenant; another tenant ⇒ `404` |
| `PUT /ranges/{id}/window` `{"ms": n\|null}` · `GET /ranges/windows` | **Per-range flush window** (D-h; cluster-admin): trades commit latency for PUT count — a plan tier. `1..=1000` ms; `null` = follow the node. The shared WAL applies it on every node once the writing node's notice arrives (at most one `--deploy-refresh-ms` tick later), and the stream is cut by the smallest window among the ranges with frames waiting; the native WAL applies it at the next DB open. Child ranges of a split/merge follow the node until set |
| `GET /metering?hour=yyyy-mm-ddThh[&tenant=]` | **Metering**: `{invocations, steps, runMs, cu}` of one `(tenant, UTC hour)`, summed over every node. `cu` = steps + ⌈run ms / 100⌉ per turn + 1 per invocation — the same unit as the rule book's `rate` limit (hours flushed before `cu` existed read as 0). Admin of the tenant; cluster-admin picks `?tenant=`. A dead node loses at most one flush tick (60 s). Plus `inputTokens`, `outputTokens`, `costMicros`: summed at receipt from the `end` records of `$trace` spans (`attrs.input_tokens`/`output_tokens`; the model from `attrs.model` of `end` or `start`), cost per the `pricing` table (micro-USD per 1 M tokens; `cairn-ctl pricing set <model> <in> <out> --apply`, nodes load it at the flush tick). An invocation's token totals: `GET /invocations/{id}/spans` |
| `POST /promises/{svc}/{key}/{name}/resolve` | The body is the value, raw bytes. Wakes `ctx.promise(name).get()`. With a scope: `/scope/{scope}/promises/...` |
| `POST /promises/{svc}/{key}/{name}/reject` | Body `{"code": 403, "message": "..."}`. The waiter gets a terminal error |
| `GET /promises/{svc}/{key}/{name}` | Whether the promise has a result yet — answers "is it stuck because nobody approved, or because the approval never arrived" |

Before these existed, human-in-the-loop had to go the long way round: write a `SHARED` handler
on the same object only to call `ctx.promise(name).resolve()`, then `/invoke` it. It worked, but
it is exactly a piece of boilerplate that *every* user has to rewrite — which means it belongs in
the runtime.

Three properties worth relying on:

- **Calling again is a no-op, not an error.** A promise completes exactly once; the second time
  returns `completed: false`. Signals are deduplicated by identity in the state machine. So
  **webhook retries are safe** — no separate idempotency key needed.
- **Resolving before anyone waits still arrives.** `woken: 0` is valid and common: the external
  system answers faster than the workflow runs to the wait point. Promises are durable, so a late
  waiter reads the result directly — there is no race to lose.
- **Start a human-waiting workflow with `/send`**, not `/invoke`: holding an HTTP connection until
  someone presses a button is exactly what `/send` exists to avoid. Then fetch the result with
  `/workflow/{service}/{key}/attach` — it needs no id, so the external system only has to remember
  its own workflow key.

`resume` returns the `status` **after** continuing, not an "ok": it may be `completed`,
`suspended`, or `paused` **again** because nobody fixed the cause. Not every `paused` can be
lifted — `JOURNAL_MISMATCH` also leads there, and rerunning only reproduces the same error unless
the deployment changed.

CLI: `cairn resume <id>`, `cairn signal <id> <name> --value <JSON>`, `cairn resolve <svc> <key>
<name> --value <JSON>`, `cairn reject <svc> <key> <name> <message>`, `cairn promise
<svc> <key> <name>`.

## Introspection

| Endpoint | Meaning |
|---|---|
| `GET /invocations` | Invocations of the partitions **this node leads** — an array; the API is honest about its scope instead of returning an answer that looks cluster-wide. Each row: `id`, `partition`, `status`, `service`/`handler`/`key` (the first two absent while entry 0 could not be read, `key` absent when empty), `deployment` (the one it is pinned to) and `attempt` (consecutive failed attempts of the pending retry appointment; 0 = none). `?status=running\|suspended\|completed\|failed\|paused` (an unknown name — `waiting` included, which is only an answer of the calling door — ⇒ `400`, not an empty list), `?limit=N` (default 100/partition, **clamped to 1000**; `?scope=cluster` returns the `limit` actually used) — **`limit` bounds the result after filtering**, not the scan. Filtered in the engine, before `limit`: `?service=`, `?handler=` (reads entry 0 of each row; stops after 10 000 rows read), `?trace_id=<32 hex>` (the OTLP trace id = the first 16 bytes of the id; malformed ⇒ `400`). **By time**: `?created_after=`, `?created_before=`, `?ended_after=`, `?ended_before=` (Unix ms, half-open `[after, before)`; a bad number ⇒ `400`) — `ended_*` only sees `completed`/`failed`; `?order=created_desc` (the default with a time filter: `createdAt` descending, `limit` cuts after sorting). Every listing is a bounded scan — 10 000 rows **and** 2 s per range (it does not hold the range's engine longer), and it waits at most 4 s for a range to answer (the range's engine is busy with other work: the range is left out of the answer): the key's tenant is filtered in the engine before `limit`, so the engine reads entry 0 of every row; hitting the bound ⇒ header `x-cairn-partial: true` (`partial` with `?scope=cluster`). Rows without timestamps are excluded and counted in the header `x-cairn-untimed` (`untimed`). Each row carries `createdAt`/`modifiedAt`/`endedAt` like `GET /invocations/{id}`. Every filter passes through `?scope=cluster` |
| `GET /invocations?cursor=` | **Listing on the query layer**: a `cursor` parameter (empty on the first page) takes this path — cluster-wide, keyset-paginated, ordered by `createdAt` descending then `id`. Returns `{invocations, nextCursor?, partial, warnings}`; each row `id`, `service`, `handler`, `key?`, `class` (`active`/`paused`/`completed`/`failed`), `status`, `root`, `createdAt`, `transitionedAt`, `tags`. Filters: `?class=a,b`, `?service=`, `?handler=`, `?key=`, `?root=true\|false`, `?tag=k=v`; `?limit=` (default 50, limit 1 000). Next page: `?cursor=<nextCursor>`; no `nextCursor` means the end. `partial: true` when a range could not be read or an index is being built (`warnings`). Without `cursor` it is the old listing in the row above. CLI: `cairn invocations --cursor '' [--tag k=v] [--root] [--class C] [--service S]` |
| `GET /invocations?scope=cluster` | **Cluster-wide**: the node asked collects the share of every node leading a range (internal port, HMAC-signed, 5 s limit per node). Returns an **object** `{scope, nodes, invocations, partial, missing, missingRanges}`; each row adds `node`. `partial: true` when a node did not answer (named in `missing`) or a range is covered by nobody (`missingRanges`) — it never guesses the missing part. CLI: `cairn invocations --all-nodes` |
| `GET /stats/handlers?window=5m&tenant=&scope=local\|cluster` | **Per-handler summary**, role `admin`. One row per `(tenant, service, handler, deployment)` per node: `node`, `turns` (SDK turns), `completed`/`failed`/`paused` (runs that stopped in that state), `failedAttempts` (turns that failed in a retryable way: retry, SDK wait limit exceeded, error mid-turn), `suspensions`, `steps` (durable steps), `runMs`, `p50Ms`/`p99Ms`/`maxMs` of turn duration (log histogram, error ≤ 25 %, never above max). `window`: `5m`…`60m` or `1h`, merges every 5-minute window touching that span (the current window included). A tenant key sees exactly its own tenant (`?tenant=` ⇒ `403`); cluster-admin sees every tenant, or one with `?tenant=`. `scope=cluster`: the rows of every node, not summed; `partial`/`missing` as in `/invocations`. Each node keeps 12 windows, limit 2 000 keys; evicted keys (LRU) are merged into the `(others)` row **of that tenant**. Numbers lag at most 1 s (background batching). No Prometheus label is per handler. CLI: `cairn stats [--window 15m] [--tenant T] [--all-nodes]` |
| `GET /invocations/{id}` | `{id, status, output?, code?, message?}` — `output` (a string, as in `/invoke`) if `completed`; `code` + `message` if `failed`/`paused` (e.g. `577` INFRA_STUCK). `lastFailure` (`{code, message, attempt, atMs, nextAttemptAtMs?}`): the error of the most recent failed attempt — a handler reporting a retryable error, or an infrastructure error of a turn (`500`: the SDK made no progress within the limit, the node → SDK hop failed, the SDK returned a 5xx) — present when `running`/`suspended`/`paused` and read from the leader; the `message` of a `paused` 575 includes it too (`exhausted N attempts … — last error (attempt k): <code> <message>`). `nextAttemptAtMs` (Unix ms) is when the scheduled retry is due — only while the invocation is backing off (`running` between two attempts); absent while an attempt runs and when `suspended`/`paused`. **Timestamps** (Unix ms, the clock of the node holding the range at commit, monotonic within one range): `createdAt` — when the admission committed, unchanged for the invocation's whole life (through suspension, pause, migrate, split, merge; `restart` gets a new id and new timestamps); `modifiedAt` — the most recent state change; `endedAt` = `modifiedAt` when `completed`/`failed`. `createdAt` is read from the invocation's metadata row (`created_ms` from the engine's clock at creation — may differ from entry 0's `create_ts` by a few ms); leader and follower return the same timestamps. `tags` (an object, absent when empty): user tags attached at call time with the header `cairnflow-tag: k=v` (≤ 8; key `[a-z0-9_.-]` ≤ 32 B, value ≤ 64 B; rule violated ⇒ `400`). An optional field **reserved**, not returned by today's runtime: `streams` (`[{name, durableOffset, sealed}]`, streams behind a gate). On a follower it is served from the reader when the data is there — **positive** results only; "not seen yet" answers `503` pointing at the leader, not a `404` from stale data. The header `x-cairn-read: leader\|follower` says where the state was read: a follower's copy is durable state but may be older than an earlier leader read |
| `GET /invocations/{id}/journal` | Each entry: `index`, `ty` (hex code), `name` (`Input`, `Sleep`, ... — absent for unknown codes), `payload` in hex, enough for a bit-exact comparison. `?decode=1` adds `entry` — the decoded header: `kind`, `name`, `service`/`handler`/`key`, `invocationId` (the target, or the callee id of a `Call`), `wakeUpAtMs`, `promise`, `stateKey`, `signal`, `failure`, `awaits`/`completes`, `completed` (for a command waiting on a result: `false` = **still waiting**), `hasBody` — and `body` `{encoding: json\|utf8\|base64, size, truncated, text\|base64}` (cut at 64 KiB). The same JSON as the `entry_lite`/`body` columns of `sys_journal`. **Paged**: `?from=` (first entry index, default 0), `?limit=` (1–10 000, default 10 000); a page also stops early when the payload reaches 4 MiB. More entries after the page ⇒ header `x-cairnflow-journal-next: <next index>`; absent = last page |
| `GET /invocations/{id}/streams/{name}` | The invocation's **streams** over SSE — see the section below |
| `GET /invocations/{id}/logs` | The invocation's **`$log`**: a JSON page, `?follow=1` is SSE — see Logs and traces |
| `GET /invocations/{id}/spans` | The **span tree** built from `$trace`, abandoned spans, token totals — see Logs and traces |
| `GET /invocations/{id}/artifacts` | The invocation's **artifacts**: `{artifacts: [{sha256, size, contentType}]}`. Readable by the tenant's `invoke` key (like user streams); leader only (`503` with `leader`); a purged invocation ⇒ `404` |
| `GET /invocations/{id}/artifacts/{sha}` | The bytes of artifact `sha` (64 hex), `content-type` as given to `ctx.artifact.put`, `cache-control: immutable` (content-addressed by sha). Content is always **inert**:
`x-content-type-options: nosniff`, `content-security-policy: sandbox; default-src 'none'`, and
`content-disposition: attachment` except for raster images, `text/plain`, `application/json` (HTML,
SVG, `*xml`, PDF are always downloaded) — the handler chooses the content type freely, and the API
also accepts the console's session cookie. No row ⇒ `404` — orphaned objects or ones from a
purged earlier life of the same id cannot be read through here |

### SQL queries (`POST /query`)

`POST /query` with `{"sql": "...", "allow_partial": true, "timeout_ms": 10000}` runs one
**read-only** query (DataFusion) over the `sys_*` tables — the catalogue of tables, columns and
required keys: [`docs/api/query-tables.md`](api/query-tables.md). An `admin` key sees exactly its
own tenant (the filter is part of the scan's structure, not an added `WHERE`); `cluster-admin`
sees every tenant.

- Returns `{columns: [{name, type}], rows: [[…]], partial, warnings: [{range, reason}], read, ms}`;
  times are ISO-8601 strings, numbers and bools keep their types. `read` is read units (index rows
  + point reads). The status is sent only after the query has finished — an error midway is never
  a truncated `200`.
- **Bind parameters**: `{"sql": "… WHERE service = $1 AND handler = $2", "params":
  ["Order", "charge"]}`. Values are strings, integers (i64), bools or `null`; only consecutive
  positional parameters `$1…$n`, exactly `n` values — missing, extra, skipped numbers, `$name`,
  floats, arrays, objects ⇒ `400`. Values are substituted into the logical plan before the
  optimiser: results and read cost are as for a query written with literals, and they never become
  SQL text. Clients (the console) never splice user-typed values into `sql`.
- `partial: true`: a range of the receiving node could not be read (the node holding it did not
  answer) or the range's index is being built (`indexing`); `allow_partial: false` turns it into
  `503`.
- Errors: `400` bad SQL, unknown table/column, a locked-out statement (DDL, DML, `COPY`, `SET`,
  table functions, recursive CTEs, file reads) or a keyed table missing its key; `422
  query_budget` over the limit (200 000 read units, 128 MiB, 10 s — `timeout_ms` ≤ 30 s — or
  10 000 rows; add a `LIMIT`); `429` more than 4 concurrent queries on the node, more than 2
  concurrent queries of one tenant after 10 s waiting for a slot, or the tenant ran out of read
  units (a token bucket per tenant — it charges read units **plus** run time: one second of
  running costs one second of the bucket's refill).
- Keyed tables (`sys_journal`, `sys_state`, `sys_promise`, `sys_artifact`, `sys_log`) require `=`
  on the key (`id = …` or `service = … AND key = …`) — never a scan of the whole range.
- RAM/bucket tables (`sys_deployment`, `sys_service`, `sys_handler`, `sys_scheduler`, …) run the
  `GET` of the corresponding REST endpoint with the identity of the asker (Bearer or the console's
  session cookie): same role, same data. Tables with a path parameter (`sys_archive`: `date`)
  require `=` on that column.
- Cluster tables (cluster-admin): `sys_node`, `sys_nodewal`, `sys_scrub`, `sys_trash`,
  `sys_pricing`, `sys_tenant`, `sys_range_load`, `sys_range_window`, `sys_inbound`, `sys_outbox`
  do not filter by `id`. Additional read endpoints for them (cluster-admin; `scope=cluster` merges
  every node leading a range, returns `{rows, partial, missing}`):

  | Endpoint | Meaning |
  |---|---|
  | `GET /cluster/nodes[?scope=cluster]` | nodes, the ranges they lead, published PUT health (`slow`, `p99PutMs`, `draining`), the SST disk cache (`diskCacheBytes`, `diskCacheHitRatio`; absent when off) |
  | `GET /admin/nodewal[?scope=cluster]` | live shared-WAL logs: `floor`, `pinnedByRange`, `pinnedWalId`, `pinnedForMs` |
  | `GET /admin/scrub[?scope=cluster]` | the most recent round of the background scrubber (the node running it): `atMs`, `items`, `complete`, `fresh`, `skipped`, `findings: [{check, range, detail, heals}]` |
  | `GET /admin/trash` | trash entries `{entries: [{entry, createdMs, subject, inPlace, reason, objects, bytes, unreadable}]}` |
  | `GET /metering/pricing` | the price table `{version, by, models: [{model, input, output}]}` (micro-USD / 1 000 000 tokens) |

  Added to existing endpoints (read leniently, the old shape is unchanged): `GET /admin/tenants`
  has `tenants: [{tenant, hashChain, slowMs, quarantineMode, quarantineRule}]`;
  `GET /deployments?pinned=true` has `pinnedInvocations` on each deployment (one cluster-wide
  count); `GET /ranges/windows?format=rows` returns `{rows: [{range, windowMs}]}`.
- Metrics: `cairnflow_query_{requests,client_errors,server_errors,rejected,throttled,partial,rows,read_units,duration_ms}_total`.
- CLI: `cairn sql "SELECT class, sum(count) FROM sys_invocation_counts GROUP BY class"`.

### Streams (`ctx.stream`, wire v4)

`GET /invocations/{id}/streams/{name}[?from=<offset>]`, `accept: text/event-stream`. Only the
range's **leader** serves it: other nodes answer `503` with `leader` (retry exactly once there)
and open no follower reader. There is no proxy between nodes.

| `event` | `data` |
|---|---|
| `record` | `{offset, durable, step, attempt, k, data}` — `id:` is the offset; `data` is the JSON the SDK sent |
| `gap` | `{offset, dropped}` — records dropped because of a limit, or overwritten when the reader fell too far behind |
| `superseded` | `{offset, step, attempt}` — that attempt of that step was replaced; the records before it from that attempt belong to an abandoned branch |
| `end` | `{next}` — the stream is sealed (the invocation ended, `paused`, `restart`) and fully read; the server closes |
| `moved` | `{leader}` — the range's leader term ended (step-down, split, deploy); ask again at `leader` |
| `reset` | `{}` — follows `moved`, only for temporary streams: what was shown exists nowhere any more, the client clears it; the new attempt will emit from the start again |

- **Temporary streams** (the default) live only in the leader's RAM: 1 MiB per stream (a ring),
  still readable 60 s after sealing, then `410 stream_expired`. `durable` of every `record` is
  `false`.
- Resuming: `?from=` or the header `Last-Event-ID` (continues from id + 1).
- Permissions: user names need an `invoke` key; `$log`/`$trace` need the `read` capability (a `view` or `admin` key). Another tenant:
  `404`.
- Limits: 16 streams per invocation; the node's stream RAM 256 MiB; over it, records are dropped
  and counted in `cairnflow_stream_dropped_total`, and readers see a `gap`. SSE connections per
  node 1 024 (`429 sse_limit` + `Retry-After`), `:keepalive` every 15 s, each connection lives at
  most 10 minutes. An invocation never fails because of a stream.

### Logs and traces (`ctx.log`, `ctx.trace.span`)

Two system streams, always **durable**: `$log` and `$trace`. Records are tied to
`(step, attempt, k)`, survive `kill -9` until the invocation is purged (`purge`); they do not go
into the archive. The permission is **Admin**, as for `/journal`: this is internal content that
may contain prompts. Only the **leader** answers (`503` + `leader` on other nodes, no follower
reader is opened — AP-13).

- **Limits** per invocation, across all attempts: `$log` 1 MiB, `$trace` 1 MiB, all of the user's
  durable streams together 8 MiB. Over it ⇒ dropped, one `gap {dropped}` record at the end,
  `cairnflow_log_dropped_total{stream}`.
- **When it becomes durable.** Records ride along with an existing commit of the range (any
  invocation's), the fence when the range is handed over, or a flush when the range's non-durable
  part exceeds 256 KiB — no commit is made by time. A node dying inside a `ctx.run` loses the
  non-durable records of the running attempt; a step that fails while the node is still alive
  loses nothing (AP-14).
- **Log level.** Default `info`; the SDK's `HandlerOptions.logLevel`; the header
  `cairnflow-log-level` in the invocation's entry 0 overrides it (the SDK reads it — ingress does
  not yet copy the request's headers into entry 0).

```
GET /invocations/{id}/logs[?level=<debug|info|warn|error>][&attempt=<n>][&from=<offset>][&limit=<n>]
```

`{"records": [{"offset", "durable", "step", "attempt", "k", "ts", "level", "msg", "attrs"}], "next"}`
— the fields the SDK writes (`ts`, `level`, `msg`, `attrs`) are flattened into the record; a `gap`
record carries `{"gap": {"dropped"}}`. `level` keeps that level and above; `limit` defaults to
1000, limit 10 000; the next page is asked with `from=<next>`. `durable` as for streams (AP-12).
`?follow=1` returns the SSE of `$log` — exactly `/streams/$log` (filters do not apply in this
mode).

```
GET /invocations/{id}/spans
```

`{"spans": [tree], "tokens": {"input", "output"}}`. Each node: `span_id`, `parent_id`, `name`,
`start_ts`, `end_ts`, `status` (`ok`/`error`), `error`, `attrs` (those of `start` merged with
`end`), `step`, `attempt`, `unfinished`, `start_lost`, `children`. `unfinished: true` = a `start`
without an `end` — the node died inside the span, or that attempt was replaced.
`start_lost: true` = an `end` without a `start` — the non-durable `start` was lost when the runtime
died and the rerun did not send it again; the name and parent come from `end`, `start_ts` is
`null`. A missing parent (not in the stream) ⇒ the node is a root.
`tokens` sums `input_tokens`/`output_tokens` in the `attrs` of the `end` record of every span.

## Cancellation

```
POST /invocations/{id}/cancel
```

- Not stopped yet ⇒ ends `failed` with code **572** (the 57x band: 570 journal mismatch, 571
  protocol violation, 572 cancelled, 574 no object key, 575 attempts exhausted, 576 quarantine,
  577 `INFRA_STUCK` — `paused` because cairn's infrastructure could not move the invocation
  forward; see `docs/runbooks/invocation-stuck-on-infra.md`). A waiter through `ctx.call` receives
  that failure; if it was holding an `EXCLUSIVE` lock, the invocation queued behind it runs at
  once. Returns `{"cancelled": true, "status": "failed"}`.
- Already stopped ⇒ no-op, `{"cancelled": false, "status": "completed"}` — the operator needs to
  know *how it finished*.
- Does not exist ⇒ `404`.

This is an **admin**-level cancel: an SDK turn in flight is not cut, but its result is not
written — the cancel holds the invocation lock while writing the terminal state, and
`Effect::Finish` refuses to overwrite a state that is already terminal. Pending timers/retries are
cleaned up when due.

What **still** happens, and must be known in advance: the turn in flight runs to its natural
stopping point. Side effects inside that turn's `ctx.run` **have run and have been paid for**, and
its `ctx.set` still goes into the object's state. Only the turn's *outcome* is dropped. To cut it
midway, use `?mode=signal` below.

### Cancellation propagates down the subtree

Cancelling an invocation also cancels the invocations it created with **`ctx.call`** and has not
received a result from — recursively down the whole tree, across partitions. Nobody reads their
results any more, so letting them continue is letting them spend money on a result that will be
thrown away. Two paths close together:

- A child that **was already created** ⇒ receives the cancel, ends `failed` with code 572.
- A `ctx.call` **still in the outbox** ⇒ is never emitted. This is the bulk of a fan-out cancelled
  midway: `ctx.all([...ctx.call])` emits N commands in one turn but they are drained one by one.

**`ctx.send` is left alone.** A `OneWayCall` declares no result completion, so the caller never
waited for it — it is *detached* work, not a branch of the running tree. Restate cuts at exactly
this point. To cancel it, cancel it by id.

One case cannot be reached, and it is a limit of the admin level rather than a gap: a child in the
**middle of an SDK turn** (inside a `ctx.run` closure) finishes that turn, because the cancel must
wait for the invocation lock. If the turn finishes and the handler has not, it is cancelled right
after; if the turn finishes and the handler finishes too, it is `completed`. The only way to cut
midway is `?mode=signal` below, and it needs the SDK still
alive.

```
POST /invocations/{id}/cancel?mode=signal
```

A **cooperative** cancel ([`protocol-v1.md`](./protocol-v1.md) §8.7): delivers the `CANCEL`
signal, the SDK turns every wait point into a terminal 409 error so that user code's
`try/catch/finally` can clean up, and then the invocation ends with its own failure. An invocation
**in the middle of a turn** gets the CANCEL in that turn at once, without waiting for the turn to
stop: `BIDI_STREAM` pushes the signal down the open stream — a cancellable `ctx.run` closure that
is running receives the interrupt; `REQUEST_RESPONSE` delivers it before the next turn. The answer
still arrives when the invocation stops. Returns `{"cancelled": true|false, "status": ...}` —
`cancelled` is *whether it really stopped*. It needs the SDK alive and answering; if the SDK is
dead the invocation keeps running, and the hard level above is the remaining path — which is why
the hard level stays the default.

## Purge

```
POST /invocations/{id}/purge
```

Deletes the journal + state + idempotency key of an invocation that has **stopped for good**.
Still running (including `paused` — it still holds its place in the EXCLUSIVE queue) ⇒ `409`;
cancel first, then purge. Does not exist ⇒ `404`. CLI: `cairn purge <id>`.

The automatic path is **retention**: `--retention-ms N` (default `86400000` = 24h, `0` = keep
forever). The purge appointment is written in the *same commit* as the terminal state, so it
survives failover. Two consequences to know:

1. **The bucket does not grow with history.** Without retention, every invocation that ever ran
   is a key living forever in compaction — the cost grows with the *past*, not with the load.
2. **The idempotency dedupe window = retention.** Once purged, the same key is a new invocation
   (with the same id — the id is deterministic in the key).

**Purging a workflow's `run` purges two more things**: all the state and all the promises of
that workflow, in the same commit. This is required, not a convenience — the `run` id is a
function of `(scope, service, key)`, so after the purge the same workflow key **can run again**,
and a new run seeing the old run's state would be a workflow starting from the middle. In other
words: *the "once per workflow id" window is also the retention.*

**Per-handler retention**: `journalRetention` (ms) in the service/handler manifest overrides
`--retention-ms` at the terminal commit of a run. Cancelling through the admin API and `restart`
end the invocation outside a turn — they follow the node's retention.

### Archive

Purging (by expiry or by hand) **archives first**: the raw journal + the final state become one
zstd-compressed object `archive/<tenant>/<yyyy-mm-dd>/<id>` in the main store, written with
`PutMode::Create`. If the object already exists it is read back and compared: if it is from this
very run (the purge running again after dying midway), it is done; if it is from **another run
with the same id** (a workflow or an `Idempotency-Key` called again after being purged, then
purged on the same day), this one is written to `archive/<tenant>/<yyyy-mm-dd>/<id>.1`, `.2`, …
If writing the archive fails ⇒ **no** purge; the appointment stays, the next tick retries. The
date is the UTC date of the **purge appointment** (manual purge: the day of the purge). Turned off
with `--no-archive`.

| Endpoint (admin) | Meaning |
|---|---|
| `GET /archive?date=yyyy-mm-dd[&limit=N]` | The tenant's archive for one UTC day, one LIST: `{date, entries: [{id, run, bytes, writtenAtMs}]}`, by id, at most `limit` (default and limit 10 000). SQL: `sys_archive` (requires `date = '…'`) |
| `GET /archive/{id}[?date=yyyy-mm-dd][&days=N][&verify=true]` | A purged invocation, in the same shape as `/invocations/{id}` + `/journal`: `{id, status, output?, failure?, journal, purgedAtMs, chain}`. Without `date` it walks back from today at most `days` days (default 31, limit 31), one GET per day, each charged to the tenant's read units (spent ⇒ `429`). When one day holds several runs of the id, the last one is returned (the earlier ones are at `<id>`, `<id>.1`, … on the bucket). `404` = not found in that span. The tenant follows the key (`?tenant=` for cluster-admin) |

### Tamper evidence

When a tenant enables the **hash chain**, every journal write updates, in the same commit, a
chain-head row `(len, hash)` with `hash_n = SHA-256(hash_{n-1} ‖ index ‖ raw entry)`
(`hash_{-1}` = 32 zero bytes). The journal entry layout does not change. The archive carries the
chain head; `?verify=true` recomputes it from the journal in the archive: `verified: true|false`,
`null` when the invocation was created before the tenant enabled it. An archive that cannot be
decoded ⇒ `verified: false` with `error`. Off by default.

| Endpoint (capability `provision`; cluster-admin has it) | Meaning |
|---|---|
| `PUT /admin/tenants/{tenant}` `{"hashChain"?: bool, "slowMs"?: n}` | Enable/disable the hash chain; set the tenant's own slow-log threshold (ms, `0` = back to the node's default — see *Slow log*). Absent fields are kept; a body with no field ⇒ `400`. The hash chain applies to invocations **created after** the command (other nodes: once they receive the notice, at most one `--deploy-refresh-ms` tick later); existing invocations are unchanged |
| `PUT /admin/tenants/{tenant}` `{"retentionMs"?, "maxPayload"?, "maxJournal"?, "deployRate"?}` | Per-tenant limits for a shared cell. Absent = kept, `null` = remove the limit, an unknown value ⇒ `400`. `retentionMs`: ms to keep a finished invocation, `0` = forever, overrides `--retention-ms`, loses to the handler's `journalRetention`. `maxPayload`: bytes ≥ 1 — a `/invoke`/`/send` body over it ⇒ `413` `code: "payload_too_large"`; an `Output`/`SetState` frame over it ⇒ the invocation `failed` with code `413`. `maxJournal`: entries ≥ 1 (counting `Input`) — over it ⇒ the invocation `failed` with code `578` (`journal_too_long`). `deployRate`: `"N/S"` — the N+1th `POST /deployments` within S seconds ⇒ `429` `code: "deploy_rate_limited"` + `Retry-After` (counted on each node). A `tenants` object with an unknown value (written by hand) ⇒ the node keeps the old registry, gauge `cairnflow_tenants_book_rejected` = 1 (runbook `tenants-registry-broken.md`) |
| `PUT /admin/tenants/{tenant}` `{"maxLive"?, "maxDeployments"?, "maxSchedules"?, "maxTimers"?, "maxStreams"?, "maxStateKeys"?, "maxArtifactBytes"?}` | Resource limits, each field a number ≥ 1, `null` = remove. `maxLive`: live invocations (running, sleeping, waiting, `paused`) of the tenant — a **soft limit**: counted with the query counters of every range every 5 s (a range rebuilding its counters is missing), overshoot at most ≈ creation rate × tick; reached ⇒ new work (`/invoke`, `/send`, workflows, `restart`, schedule fires) `429 live_invocations_limit`, no `id`; child calls are still created. `maxDeployments`: the N+1th `POST /deployments` ⇒ `409 deployments_limit` (before discovery; counted on the receiving node's registry). `maxSchedules`: `PUT` of a **new** schedule when N already exist ⇒ `409 schedules_limit` (editing an existing schedule passes). `maxTimers`: pending timers per invocation — over it ⇒ `failed` 579 `timers_limit`. `maxStreams`: user streams per invocation — a new stream over the limit is dropped (an invocation never fails because of a stream). `maxStateKeys`: state keys per object — a new key over it ⇒ `failed` 579 `state_keys_limit`. `maxArtifactBytes`: total bytes of `artifacts/<tenant>/` (LIST every 5 s) — once reached ⇒ an `ArtifactPut` makes the invocation `failed` 579 `artifacts_limit`. `maxJournalBytes`: total journal bytes per invocation (encoded bytes of every entry, counting `Input`) — the entry that pushes the total over is **not** written, the invocation is `paused` 580 `journal_bytes_limit`; raise the limit, then `POST /invocations/{id}/resume` to continue. `maxValueBytes`: the most **logical** bytes of one value the SDK writes into the journal (`ctx.set`, output, a `ctx.run` result) — over it ⇒ `failed` 579 `value_bytes_limit`; since segmented values exist, `maxPayload` only applies to the inline part of a frame; the SaaS profile defaults to 64 MiB |
| `PUT /admin/tenants/{tenant}/state` `{"state": "active"\|"limited"\|"suspended"}` | The tenant's state — see `docs/self-hosting.md` §8. `204` when every live node has applied it, `202 {pending, listed}` when not yet (resend to ask again); `409 tenant_state_disabled` before the `tenants/state` gate |
| `GET /admin/tenants` | `{hashChain: [tenant…], slowMs: {tenant: ms}}`; `limits: {tenant: {…}}` for tenants with limits; `states: {tenant: state}` for tenants that are not `active` |

A limit that must be stated: the chain head lives **inside** the archive, so whoever can modify
the bucket can rewrite both the chain and its head. The chain catches clumsy tampering (editing
one entry without recomputing) and corruption; to defend against someone with write access to the
bucket, the chain head must be anchored outside (export `chain.hash` to another system) — not done
yet.

## Slow log

A run (from acquiring a permit to releasing it, SDK included) longer than the tenant's threshold
leaves **one** log line at level `warn`, target `cairnflow::slow_log`, with fields:
`tenant`, `service`, `handler`, `deployment`, `invocation`, `outcome`
(`completed|failed|paused|retry|suspended|error`), `total_ms`, `threshold_ms`,
`turns`, `steps`, `run_ms`, `sdk_ms` (waiting on the SDK), `durable_waits` +
`durable_wait_ms` (waiting for the WAL flush), `engine_wait_ms` (waiting to get the engine back),
`permit_wait_ms` (waiting for a flow-control permit). With `CAIRNFLOW_LOG=json` it is one JSON
line. Default threshold `--slow-log-ms` (5000, env `CAIRNFLOW_SLOW_LOG_MS`); a tenant sets its
own with `PUT /admin/tenants/{tenant} {"slowMs": n}` (the `slow-ms=` flag in the `tenants`
object, read leniently). **Limit 10 lines/s per node**: over it, lines are not printed but counted
in `cairnflow_slow_log_dropped_total`; printed lines are counted in `cairnflow_slow_log_total`.

Printed lines also go into a **ring** of 1 000 lines per node: `GET
/stats/slow-runs?tenant=&scope=local|cluster` (role `admin`, tenant as in `/stats/handlers`)
returns `{runs, partial, missing}`, newest first, each row the fields above (camelCase) plus
`atMs`, `node`; SQL: `SELECT * FROM sys_slow_run WHERE …` (cluster-wide). Restarting the node
loses the ring.

## Capturing the next run

| Endpoint (cluster-admin) | Meaning |
|---|---|
| `POST /admin/capture` `{tenant?, service, handler, deployment?, count?, ttl?}` | The **next** matching invocations of `(tenant, service, handler[, deployment])` — at most `count` of them (default 1, limit 100), within `ttl` seconds (default 3600, limit 7 days) — leave a bundle `captures/<tenant>/<id>` when they end (completed/failed) or when they become `paused`. The request lives in the object `capture-requests/<tenant>` (CAS, read leniently, absent = nothing; limit 100 live requests per tenant). Returns `{id, tenant, service, handler, deployment?, remaining, expiresMs}`. The node receiving the command can capture at once, other nodes once they receive the writing node's notice (at most one `--deploy-refresh-ms` tick later). `tenant` absent = the key's tenant. `400` when a field is wrong, `409` when the object has an unknown header (a newer binary) |
| `GET /admin/capture?tenant=` | `{tenant, captures: [live requests], bundles: [invocation ids that have a bundle]}` |
| `GET /admin/capture/{tenant}/{id}` | The raw bytes of the bundle (`application/octet-stream`): header `cairnflow-capture/1`, JSON `{capture, capturedAtMs, status, deployment, contract}`, then the journal + state in exactly the archive layout |

A slot is claimed (CAS decrementing `remaining`) **before** writing the bundle: never more than
`count` bundles, even when several nodes match at once; dying between the two steps loses one
slot. The bundle is written after the journal is durable, outside the engine lock.

CLI: `cairn capture <service> <handler> [--tenant T] [--deployment D] [--count N]
[--ttl SECONDS]`, `cairn capture ls [--tenant T]`, `cairn capture get <tenant> <id>
[--out F]`, and **`cairn replay --bundle <file> [--endpoint URL]`** — replay-checks the bundle
(like `/invocations/{id}/replay`: read-only, the deployment must be wire v3) **without a
cluster**: the bundle carries the endpoint and the journal; `--endpoint` points at a local SDK
being fixed. Exits `3` on a mismatch.

## Auto split

Hot or large ranges are proposed for splitting automatically — and, when enabled, split
automatically. The mode is chosen with `--auto-split=off|suggest|on` (variable
`CAIRNFLOW_AUTO_SPLIT`), **default `suggest`**: only propose, never split. Switching to `on` is a
separate decision, taken after looking at the proposals under real load.

**What is measured.** Per range, on its engine thread: keyed commands, the time of finished SDK
turns, committed bytes, and a reservoir of 64 `owner_pk` samples (the invocation's partition key)
weighted by turn time. Cut into 1 s windows, 60 windows kept. A window's **turn-slot share** =
turn time / (`--max-concurrent-turns` × window length) — measured in turn slots because that is
the node's scarce resource, not QPS.

**When to propose.** On the range's own leader, every second:

| Reason | When | Split key (`at`) |
|---|---|---|
| `load` | turn-slot share ≥ `--auto-split-hot-share` (0.5) for `--auto-split-hot-secs` (60) **consecutive** windows — one cool window restarts the count | the weighted median of the samples: left and right keep nearly equal **turn time**, not key counts |
| `size` | visible size > `--max-range-bytes` (8 GiB). Measured with one manifest GET, only after the range has written another `--auto-split-size-check-bytes` (64 MiB) and at most every `--auto-split-size-check-ms` (5 minutes) | an SST view boundary near the size median; failing that, the midpoint |
| `hot_owner` | hot as for `load`, but one owner holds ≥ 80 % of the weight (or no gap balances better than 0.8) | **no split** — one owner never spans two ranges; this is a job for flow control (rules, limit keys). `owner` is its partition key. A range that is both hot on one owner and over the size limit still splits, reason `size` |

**Rows of `GET /ranges/suggestions`:**

| Field | Meaning |
|---|---|
| `range`, `node` | the range, and the node (leader) proposing |
| `reason` | `load` · `size` · `hot_owner` |
| `at` | the split key, 16 hex digits — usable directly for `POST /ranges/{id}/split?at=`. `null` with `hot_owner` |
| `balance` | \|left − right\| / total at `at` according to the samples (0 = perfectly balanced); with `hot_owner` it is that owner's share |
| `blockedBy` | what is blocking the split, `null` = can split: `max_ranges` (a split for load while the table already has `--auto-split-max-ranges` ranges, 128; splits for size are not capped), `cooldown` (the range, or its parent, split within `--auto-split-cooldown-ms`, 10 minutes), `node_rate` (the node auto-split within `--auto-split-node-interval-ms`, 1 minute), `intent` (the table still has an unfinished split/merge somewhere), `upgrading` (the cluster is migrating), `l0` (a child would hit the L0 limit), `busy`/`conflict`/`failed` (the previous attempt was refused transiently, or failed after the intent and rolled back) |
| `owner` | the hot partition key (hex) for `hot_owner` |
| `turnShare`, `hotSecs` | the average turn-slot share over the run of hot windows (time the turns spent in the handler, not waiting for the engine or for durability), and the run's length |
| `sizeBytes` | the most recently measured visible size |
| `sinceMs` | the wall-clock time this reason started holding continuously |

**Mode `on` — four nets.** (1) The whole range table has only **one** unfinished split/merge:
checked on exactly the table snapshot whose version the intent CAS compares, so another intent
slipping in makes the CAS miss — the CAS is the referee. (2) A per-range cooldown, applied to both
children too. (3) Each node auto-splits at most once per `node_interval` (a failure after the
intent counts too — it already cost one write stop). (4) The two children receive ½ of the
parent's statistics (samples are filtered by the child's key span): a child does not start from
zero, but does not carry the parent's full heat either. In addition: no split while the cluster is
upgrading or while a child's L0 does not have enough room. Auto split never merges.

**`GET /ranges/stats`** — each row: `range`, `node`, `windows`, `opsTotal`,
`turnMsTotal`, `opsPerSec`/`turnMsPerSec`/`bytesPerSec` (averages over the windows kept),
`turnShare` (the most recent window), `hotStreak` (when auto split is not off),
`topOwner: {ownerPk, share}`, `sizeBytes`, `sizeCheckedMsAgo`,
`bytesSinceSizeCheck`; `detail=1` adds `recent: [{atMs, ops, turnMs, bytes, samples}]`.

**Metrics:** `cairnflow_range_turn_ms_total{range}` (per-range form only),
`cairnflow_auto_split_total`, `cairnflow_auto_split_failed_total`.

## Cluster & health

| Endpoint | Meaning |
|---|---|
| `GET /health` | `ok` — the process is alive |
| `GET /cluster` | The range table and who holds which range. `version` + `nextRangeId` + `ranges` (each range: `range`, `start`/`end` 16 hex digits, both ends inclusive, `gen`, `intent` if any — `{"kind":"splitting","at","left","right"}` or `{"kind":"merging","with","into"}` —, `leader`/`address`/`term`); `partitionCount` + `partitions` is the old shape, kept for old clients (`partition` = the range id). Leaders are read from the lease objects themselves, so **every node gives the same answer** (the range table after at most one reload tick). The `readers` field (readers open on this node) is the only part that depends on the node asked. CLI: `cairn ranges`, `cairn cluster` |
| `POST /ranges/{id}/split[?at=HEX16]` | Splits range `id` into two child ranges at key `at` (16 hex digits as in `cluster.conf`, the first key of the right range; absent ⇒ the middle of the key span). The node asked **must be leading** that range — otherwise `503 {"error","leader"}` points the way. Protocol D24: check L0 room, CAS the table to record a `splitting` intent, flush + quiesce, clone both children, claim ownership of both children, commit CAS (drop the parent, add two children with `gen+1`), open both children, retire the parent. Dying midway ⇒ whoever reclaims the parent sees the intent and rolls back (removes the intent first, then cleans up the children's DBs and owners). Returns `/cluster` after the commit, plus `report` — per-phase measurements: `phases` (`[{phase, ms}]` in the order `precondition`, `intentCas`, `flush`, `quiesce`, `cloneLeft`, `cloneRight`, `ownerClaim`, `commitCas`, `open`, `retire`), `windowMs` (the write-stop window: from asking for quiesce until both children have a leader), `parents` (`drainMs`, `sealMs` measured on the parent's engine thread) and `children` (`claimMs`, `openDbMs`, `restoreMs`, `rebuild: {ms, hotRows, derivedRows, bulkRows, bytes, written, removed}`). `400` when `at` lies outside the range or at its start; `409` when the range already has an intent or the table changed underfoot; `503` (retry after a tick) for every **transient or legitimately racing** condition — not enough L0 room for a child range, the cluster is upgrading, the source range just changed owner or lost its lease midway, the intent was rolled back by another node, a transient store I/O error, a wait limit exceeded; the intent has been rolled back before answering. `500` remains only for abnormal errors (a failing step outside the categories above, an injected failpoint); the operations log records the matching `error.kind` (`busy` / `failed`). **The operation runs in a task of the node**: the call is still synchronous (it waits for the commit or the rollback), but a client disconnecting midway does **not** cancel it — the operation goes on to commit or finishes rolling back; look it up later at `GET /ranges/ops`. A successful response carries `opId`. Needs a store that supports CAS (`s3://`, `memory://`) — `file://` has no `PutMode::Update`. CLI: `cairn ranges split <id> [--at HEX16]` |
| `POST /ranges/merge?left=&right=` | Merges two **adjacent** ranges into one new range (new id, `gen` = max + 1). The node asked must lead one of the two; the owner of the other is asked to resign (`POST /internal/ranges/{id}/resign` to the address in the owner record) and then this node claims it (D-e), quiesces both, `union_ranges`, commits, opens the merged range, and the two old ranges retire. `report` as for split, the phases are `precondition`, `intentCas`, `gather`, `flush`, `quiesce`, `union`, `ownerClaim`, `commitCas`, `open`, `retire`. Error codes as for split; the other range not coming over to this node (its owner did not resign within the wait limit, or the node claiming it rolled the intent back — SM-3) is `503`. CLI: `cairn ranges merge <left> <right>` |
| `GET /ranges/ops` · `GET /ranges/ops/{id}` | **The split/merge operations log of the node asked** (cluster-admin): `{"node", "ops": [...]}` / one entry. Each entry: `id` (`<node>-<n>`, the prefix is the node that received the operation — ask that node), `kind` (`split`/`merge`), `ranges`, `at` (if any), `state` (`running`, `committed`, `failed`), `elapsedMs`, `error {kind, message}` on failure (`kind` like the error codes: `busy`, `conflict`, `failed`, …), `report` like the split/merge response on commit. RAM only: a restarted node has an empty log; the 64 most recent finished operations are kept. The source of truth is the intent in the table (`GET /cluster`): an unfinished intent nobody is coordinating is rolled back by whoever claims the source range or by the intent reconciliation loop. `404` when the id is not in the log. `GET /ranges/ops?scope=cluster`: the logs of every node, adding `partial`/`missing` |
| `GET /ranges/stats[?scope=cluster][&detail=1]` | **Load per range** (cluster-admin) — see [Auto split](#auto-split). A node returns the numbers of the ranges **it leads**; `scope=cluster` gathers every node (`partial`/`missing` as in `/stats/handlers`) |
| `GET /ranges/suggestions[?scope=cluster]` | **Split proposals**: an array `[{range, reason, at, balance, blockedBy, …}]` — see [Auto split](#auto-split) |
| `GET /internal/ranges/stats`, `GET /internal/ranges/suggestions` | One node's share of the two views above with `scope=cluster`. Internal port, HMAC-signed |
| `POST /internal/ranges/{id}/resign` | Merge's internal path: the node leading range `id` hands it over (reloads the table first, then `Resign`). Not a public API |
| `GET /internal/ranges/{id}/coordinator` | `{"held": bool}` — whether this node's split/merge coordinator is holding range `id`. The intent reconciliation loop of the D-d runner asks the node leading each source before rolling an intent back (SM-15). Internal port, HMAC-signed |
| `GET /metrics` | Prometheus text. Counters measure what **this node's HTTP layer** sees (invokes by outcome, cancels, admin ticks) plus the path down to the SDK (`cairnflow_sdk_rr_turns_total`, `cairnflow_sdk_bidi_streams_total`, `cairnflow_sdk_bidi_resumes_total` — see below; HTTP body bytes with the customer's endpoint `cairnflow_sdk_bytes_total{mode=BIDI_STREAM|REQUEST_RESPONSE,dir=to_sdk|from_sdk}`) and the slow log (`cairnflow_slow_log_total`, `cairnflow_slow_log_dropped_total`); gauges snapshot at scrape time (partitions with a leader, deployments, reader freshness + health — `cairnflow_reader_healthy` at 0 means a wedged reader) |
| `GET /internal/stats/handlers`, `GET /internal/invocations` | One node's share of `/stats/handlers?scope=cluster` and `/invocations?scope=cluster`. Internal port, HMAC-signed. Not a public API |
| `POST /admin/tick?now_ms=T` | Fires due timers/retries. The background loop calls it every `--tick-interval-ms`; the endpoint exists so tests can control time. The tick follows the **range table**: while the table has an intent, or a range in the table has no leader yet, it waits (up to 10 s) before ticking; a tick that fired something watches the table for another 300 ms and ticks again if a split/merge has just started (turns cancelled by quiesce only fire again by real time). Cannot settle ⇒ `503` — the tick has **not** run completely, retry |
| `POST /internal/inbound` | The cross-partition path between nodes. Not a public API. `?range=<id>&table=<v>`: the range and the table version the sender used; a mismatch (an older table, a wrong range, a range that is splitting or retired) ⇒ `409 {"error","code":"wrong_range","table":{version,nextRangeId,ranges}}` so the sender learns the table and resends (D23) |

The three SDK counters answer one question: *how many round-trips down to the SDK does one
invocation cost?* Each `REQUEST_RESPONSE` turn is one HTTP request **and** one resend of the whole
journal by the runtime. `BIDI_STREAM` changes that number: `bidi_streams` counts connections,
`bidi_resumes` counts the reruns **inside** an open connection — each one is exactly one
round-trip and one journal resend that did not have to be paid.

Each counter has **two series**, and they answer two different questions:

| Series | Labels | Used for |
|---|---|---|
| `cairnflow_sdk_*_total` | none | Alerting. Always present, even at 0 — `cairnflow_sdk_timeouts_total` sitting at 0 is the healthy state, so it must *exist* for an alert to work |
| `cairnflow_sdk_*_by_range_total` | `range` | Diagnosis. Only prints ranges that were ever bumped |

**Do not `sum()` both** — they have two different names precisely so that nobody does.
The sum of the split series equals the unlabelled series. The split part is a map by range id,
without a limit: range ids are not contiguous and never reused (D22), so any limit set in advance
would be exceeded after a few splits. A retired range keeps its row until the process dies —
counters must not go backwards.

Why the split series is needed: a process holding several partitions is several engines running
in parallel, so the difference between two readings of the **total** is not the cost of one
invocation — it is the cost of everything that ran in that window. The split series narrows
"everything" down to the invocations of the same range.
A high `bidi_resumes / bidi_streams` is a healthy sign; a non-zero `rr_turns` in a cluster that
should be all bidi means some deployment is declaring `REQUEST_RESPONSE`.
