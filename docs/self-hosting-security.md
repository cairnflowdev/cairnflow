# Self-hosting security: defaults versus `--profile saas`

Cairnflow is multi-tenant: one cluster can serve many tenants, each with its own API keys,
deployments, invocations and limits. The defaults of a self-hosted node are chosen for the common
case — **one organisation running its own services** — and several protections are therefore off
or loose by default. `--profile saas` turns on the settings meant for a cell shared by tenants who
do not trust each other, and refuses to start when a required secret is missing.

Read this page before you let anyone outside your organisation register a deployment or hold an
API key on your cluster. The full flag reference is `cairnflow --help` and
[`docs/self-hosting.md`](self-hosting.md).

## What `--profile saas` does

`--profile saas` (or `CAIRNFLOW_PROFILE=saas`):

- **requires** `CAIRNFLOW_CLUSTER_SECRET` (at least 32 bytes, the same on every node),
  `CAIRNFLOW_IDENTITY_MASTER` (at least 32 bytes) and at least one `--egress-deny-cidr`;
- **refuses** `--no-auth`, `--ui-cookie-secure off`, `--inactivity-timeout-ms 0` and
  `--query-isolation inline`;
- **turns on** `--egress-deny-private`, `--egress-require-https`, `--ui-cookie-secure on`,
  `--no-user-otlp`, `--deploy-require-identity`, `--deploy-https-request-response` and
  `--deploy-require-compression`, and runs every `/query` in a child process;
- **sets defaults** where none were given: `--max-turns-per-tenant` to a quarter of
  `--max-concurrent-turns` (at least 1), `--key-rate 1000/1`, `--inactivity-timeout-max-ms` at
  most 15 minutes, `--query-worker-memory-mb 256`, and per-tenant resource caps for every cap a
  tenant's entry leaves empty;
- marks the node process non-dumpable, so a query worker running as the same user cannot read the
  node's environment (cluster secret, store credentials).

A missing requirement stops the node with a message that lists everything missing.

## The table

"Self-host default" is what a node does with no flags. "To serve untrusted tenants" is what to
set on a self-hosted cluster that is shared by mutually untrusted tenants, whether or not you use
`--profile saas`.

| Protection | Self-host default | `--profile saas` | To serve untrusted tenants |
|---|---|---|---|
| **API key at ingress** | On. Every route except `/health` needs `Authorization: Bearer <key>`; with no key configured the node does not start. `--no-auth` turns it off (development only). | `--no-auth` refused. | Never use `--no-auth`. Give each tenant its own keys (`POST /admin/keys`) with the narrowest role. |
| **Bootstrap key strength** | `CAIRNFLOW_ADMIN_KEY` must be at least 32 bytes, otherwise the node does not start. | Same. | Prefer `CAIRNFLOW_ADMIN_KEY_SHA256` (the node keeps only the hash; the plaintext stays in your vault). |
| **Cluster secret** (HMAC between nodes) | `CAIRNFLOW_CLUSTER_SECRET` if set (at least 32 bytes); otherwise the first node generates one and **stores it in the bucket** (`auth/cluster-secret`). Anyone who can read the bucket can then sign internal requests. | Required from the environment, never stored in the bucket. | Set `CAIRNFLOW_CLUSTER_SECRET` (for example `openssl rand -hex 32`) from a secret manager on every node. |
| **Request identity towards customer endpoints** | Off: with no `CAIRNFLOW_IDENTITY_MASTER`, requests to deployments are not signed, so an endpoint accepts turns from anyone who can reach it. | Required master; `--deploy-require-identity` refuses SDKs that do not verify `x-cairn-jwt-v1`. | Set `CAIRNFLOW_IDENTITY_MASTER` (at least 32 bytes, outside the bucket) and `--deploy-require-identity`; tenants verify with the keys from `GET /identity`. |
| **Egress to customer endpoints** (discovery, SDK calls, test replay) | **Any destination**: loopback, private networks, link-local including `169.254.169.254` (cloud metadata), and plain `http://`. Redirects are never followed and `HTTP(S)_PROXY` is not read. | `--egress-deny-private` and `--egress-require-https` on; at least one `--egress-deny-cidr` required. | `--egress-deny-private`, `--egress-require-https`, and `--egress-deny-cidr` for the cluster's own ingress and load-balancer addresses. Use `--egress-allow-cidr` only for networks you mean to open. |
| **Console cookie `Secure` flag** | On when `--host` is not a loopback address (the container image binds `0.0.0.0`, so it is on); off on loopback. `--ui-cookie-secure on\|off` overrides. | Forced on; `off` refused. | Serve the console only over TLS and keep `--ui-cookie-secure on`. |
| **Trusted proxies** (`X-Forwarded-*`) | None: no forwarded header is trusted. | Not set by the profile. | Set `--trust-proxy` to your TLS proxy's addresses; otherwise the Origin check, per-IP sign-in limits and audit records see the proxy's address. |
| **Per-key request rate** | No cap. | `--key-rate 1000/1` unless set. | Set `--key-rate` (burst/seconds). |
| **Per-tenant rate and resource caps** (`deployRate`, `maxLive`, `maxDeployments`, `maxSchedules`, `maxTimers`, `maxStreams`, `maxStateKeys`, `maxArtifactBytes`, payload and journal limits) | **No cap** for a field a tenant's entry leaves empty. | A default for every empty field. | Set caps per tenant with `PUT /admin/tenants/{tenant}`; there is no flag for cluster-wide defaults outside the profile. |
| **Per-tenant concurrent turns** | `--max-turns-per-tenant 0` (no cap); the node-wide cap is `--max-concurrent-turns` (default 32). | A quarter of `--max-concurrent-turns`, at least 1. | Set `--max-turns-per-tenant`, so one tenant whose endpoint hangs cannot hold every turn slot. |
| **Other per-tenant shares** (on in both modes) | One tenant holds at most a quarter of `--replay-ram-budget-bytes` (default 512 MiB) and at most `--max-open-streams-per-tenant` (default 256) long-lived connections. A node also serves at most 1024 server-sent-event connections in total, so four tenants at their cap use all of them. | Same. | Size these for your nodes. |
| **SDK inactivity limit** (`inactivityTimeout`) | `--inactivity-timeout-ms 60000` per turn; handlers may declare up to `--inactivity-timeout-max-ms` (default one hour); `--inactivity-timeout-ms 0` turns the limit off. | At most 15 minutes for declared values; `0` refused. | Keep a non-zero `--inactivity-timeout-ms` and lower `--inactivity-timeout-max-ms`. |
| **`REQUEST_RESPONSE` compression and transport** | Compression is used when the SDK declares it (`--deploy-no-rr-compression` turns it off) and is not required; `https://` endpoints keep the transport the SDK declares. Compression is flushed at every frame boundary, so one frame never shares a compression context with another. | `--deploy-require-compression` (endpoints must declare gzip) and `--deploy-https-request-response` (`https://` endpoints default to `REQUEST_RESPONSE`). | Optional; these bound egress volume and avoid `BIDI_STREAM` through TLS edges that buffer bodies. |
| **Read path of segmented (large) values** | `--value-read-path auto`: `direct` when `--value-public-endpoint` is set on an `s3://` store, `proxy` when `--value-public-url` is set, otherwise `inline` (the node assembles the value; no URL leaves the node). | `auto` prefers `direct` on `s3://`: the SDK receives presigned store URLs, which reveal the bucket name and the access-key id. A presigned URL that was already issued keeps working until it expires (`--value-presign-ttl-s`, default 60 s), even if the key is revoked or the tenant suspended in the meantime. | Prefer `--value-read-path proxy` with `--value-public-url` (tokens are bound to tenant, range and object and expire after `--value-presign-ttl-s`), or `inline`. |
| **Export of tenants' `$trace` / `$log` to your OTLP collector** | On when the binary is built with the `otel` cargo feature and `CAIRNFLOW_OTEL_ENDPOINT` is set: spans and logs written by tenants' handlers reach your collector. | `--no-user-otlp`: system spans only. | Set `--no-user-otlp` unless your collector is meant to receive tenant data. |
| **TLS** | None in the node: both ports speak plain HTTP. | Same. | Terminate TLS in a proxy in front of the public port; keep the internal port on a private network. |
| **Internal port** (`/internal/*`: messages between ranges, handover) | `--internal-port` defaults to `--port` + 1 and binds the same `--host` (the image binds `0.0.0.0:8081`). Every request is HMAC-signed with the cluster secret; traffic is not encrypted. The signature carries a timestamp and no nonce: a captured request can be replayed to a node for 60 seconds, and a request forwarded to a range's leader contains the client's own request, its API key or session cookie included. A request without a body-hash header (an older node during a rolling upgrade) is buffered up to 4 MiB before its signature is checked. | Same. | Never expose the internal port outside the cluster network; firewall it to cluster nodes, and use a private network or TLS between nodes. |
| **Web console and `/query`** | The console is on (`--no-ui` turns it off); it loads without a key but every data call needs one. `/query` is on for keys with the right role and only sees the caller's tenant. `--query-isolation auto` runs SQL in a `cairn-query-worker` child process when that binary is next to `cairnflow`, otherwise inside the node with a warning. | Same surface; `--query-isolation inline` refused, workers limited to 256 MiB. In both modes the worker runs as the same user as the node; its system-call filter (no network, no `fork`/`execve`, no opening files) exists on Linux only, so on macOS a worker is held only by resource limits and the query deadline. | Ship `cairn-query-worker` next to the node binary and set `--query-isolation process` so a hostile query cannot stop the node; consider `--no-ui` if you do not need the console. |
| **Test-only surfaces** (failpoints, simulated stores) | Not compiled into release binaries (build features). | Same. | Use release builds without `failpoints` or `testkit` features. |

## Also worth knowing

- **The bucket is the trust boundary.** Whoever can write the bucket can do anything a node can.
  Give nodes their own store credentials, and do not share the bucket with other workloads.
- **The SST disk cache** (`--disk-cache-dir`, off by default) holds tenant data on the node's disk.
  Use an encrypted volume and wipe it when a node leaves the cluster.
- **Slow headers.** `--http-header-timeout-ms` (default 75 s) bounds how long a client may take to
  send headers on either port; set it above the idle timeout of your proxy.
- **Key roles.** Roles `view` and `provision` exist only after `cairn-ctl cluster
  enable-key-roles`; until then, prefer `invoke` keys for services and keep `admin` keys with
  tenant operators. A `provision` key reads no run data, but it can create `admin` keys for any
  tenant: guard it like an admin key of every tenant.
- **Console sessions and sign-in limits.** A logout is recorded in the bucket and reaches the
  other nodes within a few seconds; until then the cookie still works there. Each tenant keeps at
  most 200 logged-out sessions that have not expired yet; past that a logout answers `429` and the
  session lives until it expires (revoke the key to end it at once). The sign-in rate limiters
  track at most 10 000 client addresses per node: when the table is full, a new address is
  refused with `429` until older entries have refilled.
- **The CLI token.** `cairn --token <key>` is visible to other users of the machine in the process
  list; prefer the `CAIRNFLOW_TOKEN` environment variable.
- **Clients follow the leader.** When a node answers `503` with the address of a range's leader,
  the SDK clients resend the request there, API key included (same scheme only; `followLeader:
  false` or the equivalent option turns it off). Serve nodes over TLS, so that nothing on the
  path can answer in a node's place.
- **SDK-side checks.** What an SDK endpoint verifies about a request, and the limits of those
  checks, are described in each SDK's README.
- **Bucket versioning** is your last line of defence against deletes that cairnflow's own trash
  does not cover (SlateDB's internal garbage collection). See
  [`docs/architecture.md`](architecture.md) §7.
