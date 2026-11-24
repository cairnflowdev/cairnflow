# Cairnflow — self-hosting

Running cairnflow on your own: one node on a local disk, or a cluster on an S3-compatible bucket. No
database, no durable disk for cluster nodes — all state lives in the object store, and a node is only
its reader and writer.

**Status:** usable for development and testing. Per-tenant API keys with roles and an HMAC-signed
internal port (section 8), isolated tenants; no TLS yet. The data format is **frozen** at `DATA_FORMAT` 10
(D27): later releases open your bucket (section 7). The first ten minutes with the SDK:
[`quickstart.md`](./quickstart.md). Endpoints: [`admin-api.md`](./admin-api.md).

---

## 1. Choosing a shape

| Shape | Minimal command | Use when |
|---|---|---|
| One node, local disk | `cairnflow --data-dir /var/lib/cairnflow` | Dev, a single machine, no failover needed |
| One node, RAM | `cairnflow --main-store memory://` | Tests — **everything is lost when the process exits** |
| One node, S3 | `cairnflow --main-store s3://bucket/prefix` | One machine, state durable off the machine |
| Cluster, S3 | `cairnflow --main-store s3://bucket/prefix --partitions N --host <address>` on each node | Failover, several nodes sharing one bucket |

Two binaries: `cairnflow` (the runtime) and `cairn` (the CLI — each command is just one HTTP
request). No other side process: no coordinator service, no etcd, no Postgres.

## 2. One node

```sh
git submodule update --init    # fresh clone: the SlateDB fork in vendor/slatedb
cargo build --release          # needs protoc on PATH (cairn-protocol/build.rs)

export CAIRNFLOW_ADMIN_KEY="cf_$(openssl rand -hex 24)"   # the bootstrap key (section 8)
target/release/cairnflow --data-dir /var/lib/cairnflow --port 8080
```

- **Auth is on by default** (section 8): every route except `/health` requires `Authorization: Bearer
  <key>`. With no key at all (no `CAIRNFLOW_ADMIN_KEY`, no key created yet) the node refuses to
  start; on a dev machine, use `--no-auth`.
- Two ports: `--port` for clients (ingress + admin), `--internal-port` (default `--port` + 1) for
  other nodes. A single node still opens the internal port — inter-range messages go over HTTP even
  to the node itself.

- Without `--data-dir` or `--main-store`, data goes to `./cairnflow-data` (relative to the working
  directory at startup). `--main-store` wins over `--data-dir` when both are given.
- By default it listens on `127.0.0.1:8080`. For other machines to reach it: `--host 0.0.0.0` (fine
  for one node — see section 4 for why a cluster must **not**).
- Your SDK deployment must be reachable *from the node* — the runtime calls back into the registered
  URL (`cairn register http://…`).
- `file://` passes the conditional PUT check but has **no** CAS, so it cannot run a cluster (section 3).
- One node runs N partitions in the same process with `--partitions N`.

## 3. Object store

### URI

| URI | Meaning |
|---|---|
| `file:///some/path` | A local directory (created if missing) |
| `memory://` | The process's RAM — tests only |
| `s3://bucket/prefix` | An S3 or S3-compatible bucket; `prefix` is optional, several clusters can share a bucket under different prefixes |

Of the cloud stores only `s3://` is supported (the SlateDB fork turns on `object_store`'s `aws`
feature, not GCS/Azure). `--wal-store URI` moves the WAL tier to another store (D7); the prefix
follows `--main-store`.

### Configuring S3 through environment variables

There are no command-line flags for credentials. The store is built with
`object_store::parse_url_opts` from **every environment variable of the process, lower-cased**
(`Db::resolve_object_store` in the SlateDB fork):

```sh
export AWS_ENDPOINT=http://seaweedfs:8333      # omit for real AWS
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1
export AWS_ALLOW_HTTP=true                     # only when the endpoint is http://
export AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false  # path-style — SeaweedFS/MinIO need it
```

**Trap:** because *every* environment variable is passed in, plain variable names that match an
`object_store` alias also become S3 configuration — `ENDPOINT`, `REGION`, `BUCKET`, `TOKEN`,
`SESSION_TOKEN`, `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY`… A stray `TOKEN=` in a container environment
is sent as the session token. Keep the `cairnflow` process's environment clean.

Other `object_store` keys are read the same way (`AWS_SESSION_TOKEN`, `AWS_WEB_IDENTITY_TOKEN_FILE`
+ `AWS_ROLE_ARN`, `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, `AWS_CONDITIONAL_PUT`…), but in this
repository **only static keys have actually been run**.

**A backup store at another provider** (`cairn-ctl backup`/`restore`, section 7): `--backup-env-prefix P`
builds the backup store's client **only** from the variables `P` + `AWS_*`
(`CAIRN_BACKUP_AWS_ACCESS_KEY_ID`, `…_SECRET_ACCESS_KEY`, `…_ENDPOINT`, `…_REGION=auto` for R2…) — it
does not read bare `AWS_*`, and the plain-alias trap above does not apply. The access key and secret
are required; an unknown `P…AWS_<X>` variable is an error. See the
[backup runbook](./runbooks/restore-from-backup.md).

### Required capabilities — three checks at startup

A node checks the store **before** writing anything, and a failure means **it does not start** —
there is no flag to skip it (D12):

1. **Conditional PUT** (`PutMode::Create`, every shape, both tiers when the WAL is separate): write
   the same key twice; the second write must be refused with `AlreadyExists` **and** the content must
   not be overwritten. This is SlateDB's fence against a zombie leader — not `writer_epoch`.
2. **CAS on etags** (`PutMode::Update`, main store only; every node runs the lease tier, even a
   single node): an update with the right etag must be accepted, one with a stale etag must be
   refused with `Precondition`. The lease tier renews sessions with exactly this operation.
3. **Atomic PUT on the WAL tier** (assumption S3-2,
   [`reliability/irreversible.md`](./reliability/irreversible.md) §0.3). The shared WAL resolves an ambiguous log PUT by **reading it back** when the WAL tier is
   atomic (`atomic_put`). On a store that **tears objects** (erasure-coded MinIO) the read-back can
   ack a PUT that the sealer saw as empty — **data loss**. So with `s3://` and no `--wal-atomic-put`,
   a node only runs on a **verified** store, recognised at startup:
   - **AWS S3** — no custom endpoint (`AWS_ENDPOINT`, `AWS_ENDPOINT_URL`, `AWS_ENDPOINT_URL_S3` — read
     from the client itself), or an endpoint `*.amazonaws.com`;
   - **Tigris** — endpoint `t3.storage.dev` / `fly.storage.tigris.dev`;
   - **SeaweedFS** — the node GETs the endpoint's root (unsigned) and reads the `Server` header;
     SeaweedFS 4.40 answers `SeaweedFS 30GB 4.40`.

   Any other store (MinIO, Garage, Ceph, a gateway or proxy that rewrites the `Server` header, an
   endpoint that does not answer) ⇒ **it does not start**, and the error points here. Declare one of
   the two:
   - `--wal-atomic-put false` — safe on **every** store: an ambiguous log PUT ⇒ treated as sealed, the
     node fail-stops and recovers as usual (only less available). **MinIO: use this.**
   - `--wal-atomic-put true` — only once you have run
     `crates/cairn-store/tests/s3_store_contract.rs::s3_2_kill_mid_put_leaves_whole_or_nothing`
     against that very store (`CAIRNFLOW_STORE_CONTRACT_URI`) and it passed.

   The `WAL store:` line at startup prints the capability in use; the `info` log "atomic PUT
   capability of the WAL tier" prints the reason (declared, endpoint or header).

On S3, `object_store` uses etag-based `If-None-Match`/`If-Match` by default
(`aws_conditional_put = etag`) — nothing to set.

| Store | Check 1 | Check 2 | Check 3 (without `--wal-atomic-put`) | Notes |
|---|---|---|---|---|
| `file://` | ✓ | ✗ | ✓ atomic (temp file + `rename`) | One node only. On a **case-insensitive** file system (macOS's default APFS/HFS+, NTFS) tenants `Acme` and `acme` share one directory: tenant isolation breaks, and purging one tenant deletes the other's objects — use a case-sensitive file system, or only lower-case tenant names (a known limitation; S3 is not affected) |
| `memory://` | ✓ | ✓ | ✓ atomic | One process (including a multi-partition cluster in one process — how `scripts/check.sh` runs the cluster matrix) |
| SeaweedFS | ✓ | ✓ | ✓ by `Server` header (S3-2 measured: 0 torn) | **Recommended for self-hosting.** One-hour chaos runs with `kill -9` and live range split/merge: 0 lost, 0 wrong — on a **single-process** SeaweedFS; distributed mode (several filers/volumes) not measured yet |
| MinIO (local, erasure coding) | ✓ | ✓ | ✗ — declare `--wal-atomic-put false` | Passes checks 1–2 but **tears lease objects in two** when a client is `kill -9`ed in the middle of a CAS — that partition never gets a leader again. Not recommended for a cluster |
| Garage v1.0.1 | ✗ | — | — | Accepts a second conditional PUT and overwrites |
| AWS S3 | not run here | not run here | ✓ by endpoint (documented by S3: never adds a partial object; `s3_2_…` not run on real S3) | S3 has had conditional writes since 2024; the checks will tell the truth at startup. AWS price/latency numbers: not measured yet |
| Tigris | ✓ | ✓ | ✓ by endpoint (`s3_2_…` not run on Tigris) | **Only single-region buckets** (`iad`, …) or multi-region ones (`usa`/`eur`). A bucket created without a region is Global (Dual-region too): strongly consistent only within one region, eventually consistent across regions — a lease CAS reaching another region may be judged against an old version. Create: `aws s3api create-bucket --bucket B --create-bucket-configuration '{"LocationConstraint":"iad"}'`; check: `aws s3api get-bucket-location --bucket B` (Global answers `global`). The region cannot change after creation |

Any other store: checks 1–2 measure real behaviour — just run it, the checks will tell. Check 3
cannot be measured at startup (torn objects only show when a client dies mid-PUT), so it **is** an
allow list: stores outside the list need `--wal-atomic-put`.

## 4. Cluster

```sh
# every node: same bucket, same --partitions, same CAIRNFLOW_ADMIN_KEY (or created keys)
cairnflow --main-store s3://my-bucket/prod --partitions 8 \
          --node-id node-a --host 10.0.0.11 --port 8080   # internal port 8081
cairnflow --main-store s3://my-bucket/prod --partitions 8 \
          --node-id node-b --host 10.0.0.12 --port 8080
```

- **Two ports per node.** `--port` (ingress) for clients; `--internal-port` (default `--port` + 1)
  for peers — inter-range messages, asking for a range hand-over, deployment counts. The session
  lease advertises **both**: peers send to the internal port, clients are pointed at the ingress port
  (the `leader` field of a `503`, `address` in `/cluster`). Firewall: open the internal port only
  between nodes.

- **`--host` must be an address peers can reach, not `0.0.0.0`** — unless `--advertise-host` is set.
  Without that flag, a node advertises to peers (session lease, routing table, the `leader` field of a
  `503`) **exactly the address it binds** (`local_addr()` of the two listeners in `node/setup.rs`).
  Binding `0.0.0.0` ⇒ advertising `http://0.0.0.0:8080` ⇒ inter-partition messages reach nobody, and
  it fails silently: each node still looks healthy. A host name works: it is resolved to an IP at bind
  time (as `deploy/docker-compose.yml` does).
- **Bind on one address, advertise another: `--advertise-host`.** On a platform whose proxy can only
  reach the node when it listens on every address, while peers must call it on a private IP, bind
  `--host ::` (or `0.0.0.0`) and advertise the private IP:

  ```sh
  cairnflow --main-store s3://my-bucket/prod --partitions 8 --node-id "$NODE_ID" \
            --host :: --advertise-host "$PRIVATE_IP" --port 8080
  ```

  The advertised port is the **real** bound port (`--port 0` still works). The value is a host — a
  name or an IP, not resolved at startup (peers resolve it, so private DNS names work); `0.0.0.0`/`::`,
  a scheme, a port or a path are refused when the flag is read. The internal port's firewall (below)
  must still block everything outside the network between nodes: binding `::` means the internal port
  also listens on every address, with only the HMAC signature in between.
- **The same `--partitions` on every node.** The number is written into `cluster.conf` when the bucket
  is first initialised; while the range table has never been split or merged, a node configured with a
  different number refuses to start. After a split/merge, the table on the bucket wins. Over-provision
  from the start (8–64 for a small cluster); change it at runtime with `cairn ranges split|merge`
  (needs a store with CAS).
- `--node-id` should be fixed per machine (by default it is generated per process).
- Ranges balance themselves across nodes; a dead node's ranges get a new owner after about one lease
  TTL (`--lease-ttl-ms`, default 10 000) plus at most one session observation tick (TTL/3). In
  delegation mode (below), when a delegating node dies its ranges move in < 2·TTL; when the
  coordinator and a node die at the same time, in < 3·TTL.
- **Quiet renewal is on by default for new buckets.** A node that **creates** an empty bucket writes
  `control/quiet-lease` = on with delegation (`--quiet-lease delegation`, the default) and a
  `min_binary` high enough that binaries that do not know that rule cannot start: a single node renews
  with conditional GETs (no PUT), a multi-node cluster delegates renewal to the coordinator. On `s3://`
  the node first runs the conditional GET test (G18, 32 rounds on `control/probe/`); if the store
  returns a stale `304`, the gate is not turned on, the cluster renews with PUTs (`direct`) and logs a
  `warn`. Delegation turned on this way **has no done-gate evidence yet**: `cairn-ctl cluster status`
  prints the five missing items and the `delegation_unproven` marker (`by=bootstrap`). `--quiet-lease
  off` creates a `direct` cluster as before; `solo` only turns on solo mode. A bucket that already
  holds a cluster is never changed; turn it off later with `cairn-ctl cluster disable-quiet-lease
  --apply` ([runbook](./runbooks/control-plane.md)).
- Every range's WAL is the **per-node shared WAL**: the number of WAL PUTs follows the number of
  nodes, not of ranges. SlateDB's native WAL (one WAL per range) **is gone**: a native-WAL range of an
  old bucket is moved to the shared WAL when it opens — migration step `wal/shared`, see
  [Upgrading](#upgrading). `--wal-kind shared` is still accepted (changes nothing); `--wal-kind
  slatedb` is refused at startup, **before** the node touches the bucket.
- **Requests to a node that does not hold the target range.** By default the node answers `503` with
  `leader` (the advertised address of the holder). The `cairn` CLI and the TypeScript SDK's client
  (`connect()`, `readStream`, the transport layer `transport.ts`) follow it by themselves, keeping the
  `Idempotency-Key`; a hand-written client reads `leader` from the body and resends there. Following
  `leader` requires the client to reach every node's advertised address. Behind a load balancer where
  clients cannot see the nodes, send the header `x-cairn-forward: leader`: the receiving node
  **forwards** the request to the leader over the internal port (signed with the cluster secret, one
  hop, no retry) and returns the leader's response with `x-cairn-forwarded-by`
  (`ingress/forward.rs`). The web console always sends this header.
- The shared WAL's flush window: by default **one number for the whole node** (`--wal-window-ms`,
  equal to `--flush-interval-ms` when unset); set it per range with `PUT /ranges/{id}/window {"ms":
  n|null}` (cluster-admin, D-h — trades commit latency for cost per range). It applies on every node
  after one refresh tick. Child ranges after a split/merge follow the node default until set again.
  See [`admin-api.md`](./admin-api.md).

## 5. Docker

`Dockerfile` at the repository root: a multi-stage build, the image runs as user `cairnflow` (uid
10001), with `cairnflow`, `cairn` and `cairn-ctl` — three binaries from the same build, hence the
same version (`--version` prints `data=` and `knows=`, section 7). Default: one node,
`file:///var/lib/cairnflow`, listening on `0.0.0.0:8080`.

```sh
git submodule update --init        # the Dockerfile COPYs vendor/ — the build fails without it
docker build -t cairnflow:local .

# one node, data in a volume
docker run --rm -p 8080:8080 -v cairnflow-data:/var/lib/cairnflow cairnflow:local

# one node on S3
docker run --rm -p 8080:8080 --env-file s3.env cairnflow:local --main-store s3://bucket/prefix
```

- Every argument after the image name is passed straight to `cairnflow`
  (`ENTRYPOINT ["cairnflow"]`).
- `cairn-ctl` (backup, restore, `verify`, migration gates — section 7) runs by overriding the entrypoint,
  with the same S3 env as the node:
  `docker run --rm --env-file s3.env --entrypoint cairn-ctl cairnflow:local --main-store s3://bucket/prefix verify`.
  It reads the bucket directly: only hand the image/credentials to people allowed to change the bucket.
- An SDK deployment running on the host machine: the container calls back into it, so register it
  with an address the container can see — `cairn register http://host.docker.internal:9080` (Docker
  Desktop), or the host's IP on the bridge (Linux).
- The image sets `CAIRNFLOW_HOST=0.0.0.0` — right for one node, **wrong for a cluster** (section 4): cluster
  nodes must pass `--host` as the container's own host name or IP.
- Auth is on (section 8): pass `-e CAIRNFLOW_ADMIN_KEY=…`, or `--no-auth` for a quick try. The image
  `EXPOSE`s 8080 and 8081; only publish 8080 — 8081 is the internal port between nodes.

**Sample cluster:** [`deploy/docker-compose.yml`](../deploy/docker-compose.yml) — SeaweedFS, a
container that creates the bucket, two nodes sharing `s3://cairnflow/demo`:

```sh
docker compose -f deploy/docker-compose.yml up --build
docker compose -f deploy/docker-compose.yml exec cairn-1 cairn --server http://cairn-1:8080 cluster
```

Node 1 is at `127.0.0.1:8080`, node 2 at `127.0.0.1:8081` (each container's ingress port, **on the
host's loopback only**; the internal port 8081 inside the compose network is not published). The
file has no default keys: `CAIRNFLOW_ADMIN_KEY` (≥ 32 bytes) and `CAIRNFLOW_CLUSTER_SECRET` must be
in `docker compose`'s environment, otherwise it stops — `cairn --token …` or `CAIRNFLOW_TOKEN` inside
the container is that key. The advertised addresses are IPs inside the compose network, so from the
host machine (Docker Desktop) a `503` pointing at the other node cannot be followed — call from inside
the compose network, or run your SDK service as a compose service. The S3 keys in that file are
sample keys, for trying it out only.

**Kubernetes (Helm):** [`deploy/helm/cairnflow`](../deploy/helm/cairnflow) — a StatefulSet (only so
pods have stable names for `--node-id` and `--host`; no PersistentVolume), a headless service for the
internal port between nodes, an ingress service, an SST disk cache on a capped `emptyDir` (section 7 "Disk
cache"). Create the S3 secret (`AWS_*`) and the bootstrap key (`CAIRNFLOW_ADMIN_KEY`) first:

```sh
kubectl create secret generic cairnflow-s3 --from-env-file=s3.env
kubectl create secret generic cairnflow-admin --from-literal=CAIRNFLOW_ADMIN_KEY=cf_…
helm install demo deploy/helm/cairnflow --set mainStore=s3://bucket/prefix --set image.repository=…
```

`--host` is the pod's IP (`status.podIP`) — the address the node binds and advertises to peers. Do
not use the pod's DNS name through the headless service: that record appears after the container has
started, and a node that cannot resolve its own address exits (measured on kind: each pod restarted
once). `terminationGracePeriodSeconds` is longer than `--shutdown-timeout-ms` so SIGTERM has time to
shut down gracefully. The test `ops_pack.rs::deploy_templates_pass_only_real_flags` keeps the chart's
(and compose's) flags in line with the binary.

## 6. Flags and defaults

Every flag is first read from its environment variable; the command line wins. The full set:
`cairnflow --help`.

| Flag | Environment variable | Default | Meaning |
|---|---|---|---|
| `--data-dir DIR` | `CAIRNFLOW_DATA_DIR` | `./cairnflow-data` | Shortcut for `--main-store file://DIR` |
| `--main-store URI` | `CAIRNFLOW_MAIN_STORE` | — | The main store (SSTs + manifests + range table + leases) |
| `--wal-store URI` | `CAIRNFLOW_WAL_STORE` | shares the main store | A separate WAL tier |
| `--wal-atomic-put true\|false` | — | by store (section 3 check 3) | Declares the WAL tier's atomic PUT capability. An `s3://` other than AWS S3/Tigris/SeaweedFS **must** declare it, or the node does not start; MinIO: `false` |
| `--host ADDR` | `CAIRNFLOW_HOST` | `127.0.0.1` | Bind address; also the advertised address without `--advertise-host` (section 4) |
| `--advertise-host ADDR` | `CAIRNFLOW_ADVERTISE_HOST` | = `--host` | Host advertised to peers and clients (session lease, `leader` of a `503`, `/cluster`) instead of the bind address; the port is still the real bound port. A host name or IP (IPv6 with or without `[]`); `0.0.0.0`/`::`, a scheme, a port, a path ⇒ refuses to start (section 4) |
| `--port N` | `CAIRNFLOW_PORT` | `8080` | Ingress + admin port; `0` = system-chosen; the real port is printed on the first log line |
| `--internal-port N` | `CAIRNFLOW_INTERNAL_PORT` | `--port` + 1 (`0` if `--port 0`) | Port for other nodes (`/internal/*`, HMAC-signed) — the address advertised to peers |
| `--no-auth` | `CAIRNFLOW_NO_AUTH` | off | Turns API keys off — **dev only**. The internal port still requires signatures |
| `--egress-deny-private` | — | off | Requests on behalf of a tenant (discovery, SDK turns, replay checks) cannot reach loopback, private networks, link-local/metadata (`169.254/16`, Azure's `168.63.129.16`), CGNAT, special-purpose ranges, multicast, IPv4-embedded IPv6. Checked on the URL, on every IP the DNS returns, and on the socket's real peer (against DNS rebinding). Refused ⇒ `400 egress_denied`. Off because e2e tests and dev call `localhost`; turn it on when untrusted clients register deployments |
| `--egress-allow-cidr CIDR` | — | — | Repeatable. A deliberate internal network, wins over `--egress-deny-private` (e.g. an SDK running in the same VPC) |
| `--egress-deny-cidr CIDR` | — | — | Repeatable. Always blocked, wins over `--egress-allow-cidr`, effective even with `--egress-deny-private` off — the cluster's own ingress/load balancer addresses (by IP/CIDR, not by name: CNAMEs and bare IPs get around name suffixes) |
| `--egress-require-https` | — | off | Only accept `https://` endpoints |
| — | `CAIRNFLOW_ADMIN_KEY` | — | The bootstrap key: cluster-admin of tenant `default`, stored nowhere |
| — | `CAIRNFLOW_METRICS_KEY` | — | A token for `GET /metrics` only (role `metrics`, section 8) for Prometheus; ≥ 32 bytes, different from `CAIRNFLOW_ADMIN_KEY`, stored nowhere |
| — | `CAIRNFLOW_ADMIN_KEY_SHA256` | — | 64 hex digits = the `sha256` of the bootstrap cluster-admin key (section 8): like `CAIRNFLOW_ADMIN_KEY` but the node only knows the hash — the plaintext sits in a break-glass vault. Do not set it together with `CAIRNFLOW_ADMIN_KEY` |
| — | `CAIRNFLOW_PROVISION_KEY_SHA256` | — | 64 hex digits = the `sha256` of the provisioning key (section 8): role `provision`, tenant `default`, not in the registry, cannot log in to the console; counts as "has a key" — a cluster can run with no cluster-admin key at all |
| — | `CAIRNFLOW_CLUSTER_SECRET` | the `auth/cluster-secret` object in the bucket | The HMAC secret between nodes; when set it must be ≥ 32 bytes (shorter ⇒ the node refuses to start) and the same on every node |
| — | `CAIRNFLOW_IDENTITY_MASTER` | — (no signing) | The request identity master (≥ 32 bytes, the same on every node; shorter ⇒ does not start): every request to an SDK carries a JWT signed with the tenant's key derived from it (section 8). Not stored in the bucket |
| `--cluster` | `CAIRNFLOW_CLUSTER` | — | **No effect any more**, only accepted so old commands do not break: every node runs the lease tier, a single node is a one-node cluster |
| `--node-id ID` | `CAIRNFLOW_NODE_ID` | generated per process | The node's identity in the cluster |
| `--partitions N` | `CAIRNFLOW_PARTITIONS` | `1` | Number of ranges when the cluster is created (section 4) |
| `--lease-ttl-ms N` | `CAIRNFLOW_LEASE_TTL_MS` | `10000` | Session TTL; failover ≈ one TTL |
| `--quiet-lease off\|solo\|delegation` | `CAIRNFLOW_QUIET_LEASE` | `delegation` | The quiet-renewal gate (`control/quiet-lease`) the node writes **when it creates a new bucket** (section 4 "Quiet renewal"). A bucket that already holds a cluster: ignored — change it with `cairn-ctl cluster enable-/disable-quiet-lease` |
| `--tick-interval-ms N` | `CAIRNFLOW_TICK_MS` | `200` | Background loop interval (timers, retries); `0` = off |
| `--retention-ms N` | `CAIRNFLOW_RETENTION_MS` | `86400000` (24 h) | Keep finished invocations, then clean them up; also the dedupe window of idempotency keys. `0` = keep forever (the bucket grows with history). A handler's `journalRetention` in the manifest overrides it |
| `--no-archive` | `CAIRNFLOW_NO_ARCHIVE` | archiving on | Clean up without first saving `archive/<tenant>/<date>/<id>`. Archives never expire by themselves — set a bucket lifecycle rule on the `archive/` prefix |
| `--archive-artifacts` | — | off | The archive also copies artifact bytes to `archive/<tenant>/<date>/<id>.artifacts/<sha>` before the record. By default the record only carries refs (the sha in the journal): artifacts of a cleaned-up invocation are deleted after the trash retention |
| `--inactivity-timeout-ms N` | `CAIRNFLOW_INACTIVITY_TIMEOUT_MS` | `60000` | The SDK silence cap within one turn — in practice the longest `ctx.run` closure; `0` = off (`--profile saas` refuses `0`) |
| `--inactivity-timeout-max-ms N` | `CAIRNFLOW_INACTIVITY_TIMEOUT_MAX_MS` | `3600000` (1 h; `--profile saas`: at most `900000`) | Upper bound on the `inactivityTimeout` a handler contract declares — longer values are clamped. The TypeScript SDK declares 15 minutes when the user declares nothing |
| `--max-concurrent-turns N` | `CAIRNFLOW_MAX_CONCURRENT_TURNS` | `32` | Turns running at the same time on the whole node (shared by its ranges) — each turn is one connection to your deployment |
| `--key-rate N/S` | `CAIRNFLOW_KEY_RATE` | no cap (`--profile saas`: `1000/1`) | Request rate cap of **one** API key (or one SSO user in one environment) on the node receiving the request: a burst of `N`, then one request every `S/N` seconds (GCRA). Over ⇒ `429` `code: "key_rate_limited"` + `Retry-After`; requests refused with `403` (missing permission) also count; the bootstrap/env keys do not count; open routes (`/health`) do not count. If the system clock goes backwards, a key waits at most one `S` window |
| `--replay-ram-budget-bytes N` | `CAIRNFLOW_REPLAY_RAM_BUDGET_BYTES` | `536870912` (512 MiB), `0` = off | Total journal bytes that the node's running SDK turns hold in RAM (FL-27). One tenant holds at most ¼ (its first turn while it holds nothing is always admitted); a turn with a journal ≤ `min(1 MiB, N/64)` is always admitted. A turn that does not fit is rescheduled after 1 s without calling the deployment, and the attempt does not count; with an empty budget a turn always runs. Size it with the machine's RAM and the `max-journal-bytes=` of the `tenants` registry |
| `--max-turns-per-tenant N` | `CAIRNFLOW_MAX_TURNS_PER_TENANT` | `0` = no cap (`--profile saas`: ¼ of `--max-concurrent-turns`, floor 1) | **Running** turns of one tenant on the whole node: a hanging endpoint or a long model step of one tenant does not hold every slot (FL-26). A soft cap across engine threads, like the node's turn cap |
| `--queue-wait-ms N` | `CAIRNFLOW_QUEUE_WAIT_MS` | `10000` | Cap on waiting for a slot; past it `429`; `0` = wait forever |
| `--l0-soft N` | `CAIRNFLOW_L0_SOFT` | `5` | A soft throttle before the L0 wall (cap 8): from N views, **new** work is refused with a probability, as `429 range_overloaded` without an id ([admin-api](./admin-api.md)); `0` = off |
| `--l0-max-reject P` | `CAIRNFLOW_L0_MAX_REJECT` | `0.9` | Maximum refusal probability (at 7 views and above), in `[0, 1)` |
| `--invoke-wait-ms N` | `CAIRNFLOW_INVOKE_WAIT_MS` | `5000` | How long `POST /invoke` waits for the result — across retries after a turn's transient errors; past it, `202` with the id |
| `--http-header-timeout-ms N` | `CAIRNFLOW_HTTP_HEADER_TIMEOUT_MS` | `75000` | Cap on reading HTTP headers on both ports: a connection that sends half a header is closed after N ms. hyper's HTTP/1 has no separate idle timeout, so this is also the idle cap of a keep-alive connection — **set it above the idle timeout of the proxy in front** so the proxy closes idle connections before the node does (no reuse race). Common platform proxies close idle connections after ≈ 60 s, AWS ALB defaults to 60 s ⇒ 75 s. Both ports only speak HTTP/1 (no h2c) |
| `--max-open-streams-per-tenant N` | `CAIRNFLOW_MAX_OPEN_STREAMS_PER_TENANT` | `256` | Cap on long-lived connections of **one tenant** on the node: SSE streams (`GET /invocations/{id}/streams/{name}`) and pending attaches (`POST /invocations/{id}/attach`, `/workflow/.../attach`). Over ⇒ `429 too_many_open_streams` + `Retry-After`. `0` = no cap. There is no global connection cap: behind a proxy every connection comes from a few addresses, and a shared cap is a shared bucket. The node-wide SSE cap (1024, `429 sse_limit`) still applies |
| `--flush-interval-ms N` | `CAIRNFLOW_FLUSH_INTERVAL_MS` | `10` | The shared WAL's cut window (the minimum gap between two WAL object cuts of one stream; `--wal-window-ms` overrides it) — on `memory://`/`file://` it is the latency floor of sequential commits, on S3 it barely matters (a PUT takes longer than the window); smaller = more PUTs under dense load. It is not SlateDB's flush interval |
| `--engine-threads N` | `CAIRNFLOW_ENGINE_THREADS` | `0` (= core count) | Thread pool running every range's engine |
| `--reader-poll-ms N` | `CAIRNFLOW_READER_POLL_MS` | `500` | Refresh interval of follower readers |
| `--deploy-refresh-ms N` | `CAIRNFLOW_DEPLOY_REFRESH_MS` | `30000` | Interval for re-reading the cluster-level registries (deployments, keys, tenants, captures, range windows). Only a **safety net**: the node that takes a registry write notifies peers at once over the internal port (`/internal/registry/changed`); this interval is the delay when a notification is missed (a peer just joined, the network dropped packets). The quarantine list follows `min(N, 5000)` |
| `--wal-kind shared` | `CAIRNFLOW_WAL_KIND` | `shared` | Only `shared` is left (changes nothing); `slatedb` is refused at startup — the native WAL is gone |
| `--wal-stripes N` | — | `2` | Number of shared WAL logs per node |
| `--wal-inflight N` | — | `1` | PUTs in flight per log stream |
| `--wal-window-ms N` | — | = `--flush-interval-ms` | The shared WAL's flush window, node-wide — the default for ranges without their own window (`PUT /ranges/{id}/window`) |
| `--wal-linger-us N` | — | `1000` | Linger L: the first row after an idle period waits L µs more before the cut, so commits arriving close together (a batch of SDK frames) share one object — fewer PUTs under sparse load, never cutting faster than the window. `0` = cut at once; cap `100000` (100 ms), larger is refused at startup |
| `--wal-object-cap-bytes N` | — | `1048576` | Size cap of one shared WAL object |
| `--wal-age-flush-ms N` | — | `60000` | Age at which a range's memtable is forced to flush to L0 |
| `--wal-put-timeout-ms N` | — | `2000` | Cap on one WAL PUT; 3 failures in a row ⇒ the node fail-stops |
| `--disk-cache-dir DIR` | — | off | The node's SST disk cache: copies of every range's SSTs under `DIR`, one budget for the whole node. WAL, manifests, leases never go there. **Holds customer data** — see §7 "Disk cache" |
| `--disk-cache-bytes N` | — | `17179869184` (16 GiB) | Disk cache cap; the LRU evictor stays under it |
| `--gc-retired-dbs` | `CAIRNFLOW_GC_RETIRED_DBS=1` | off | D-d: every 10 minutes, delete the DBs of retired ranges that no live range references any more. **Deletes data** — turn it on once chaos runs are clean on your store. Minimum delay ≈ the compactor's checkpoint age (10 minutes) |
| — | `CAIRNFLOW_OTEL_ENDPOINT` (or `OTEL_EXPORTER_OTLP_ENDPOINT`) | — | Export OTLP/HTTP traces (`<endpoint>/v1/traces`) — only when the binary is built with `--features otel`. One trace per invocation (the trace id derived from the invocation id), a `cairnflow.run` span per run, `cairnflow.turn` per SDK turn. User spans (`ctx.trace.span`, the `$trace` stream) join that trace through a separate export stream, a channel capped at 4096 (full ⇒ dropped, `cairnflow_otel_user_spans_dropped_total`); the attributes `model`/`input_tokens`/`output_tokens`/… are mapped to `gen_ai.*` on export. `ctx.log` lines (the `$log` stream) are exported to `<endpoint>/v1/logs` under the same trace id, a channel capped at 4096 (full ⇒ dropped, `cairnflow_otel_user_logs_dropped_total`), with the same `gen_ai.*` mapping |
| `--query-isolation auto\|process\|inline` | — | `auto` (`--profile saas`: `process`) | `/query` SQL runs in a `cairn-query-worker` child process (`process`), in the node (`inline`), or in a child process when the binary sits next to `cairnflow` (`auto`) — §8 "Isolating `/query`" |
| `--query-worker PATH` | `CAIRNFLOW_QUERY_WORKER` | next to the binary | The `cairn-query-worker` binary |
| `--query-worker-memory-mb N` | — | `512` (`--profile saas`: `256`) | Memory cap of each `/query` child process; over ⇒ the query gets `422 query_budget` |
| `--profile saas` | `CAIRNFLOW_PROFILE=saas` | off | Safe defaults for a shared multi-tenant cluster (section 8 "The SaaS profile"): refuses to start when a condition is missing |
| `--deploy-require-identity` | — | off | `POST /deployments` refuses SDKs that do not declare `requestIdentity: "v1"` (`400 identity_required`) — section 8 |
| `--deploy-https-request-response` | — | off (on with `--profile saas`) | An `https://` endpoint declaring `BIDI_STREAM` is registered as `REQUEST_RESPONSE` when the request does not say `transport` (section 8 "The SaaS profile") |
| `--no-rr-delta-replay` | — | off (delta when the SDK declares `replayCache`) | Every `REQUEST_RESPONSE` turn replays in full as with wire v4, even an SDK declaring `replayCache` on wire v5 (section 8 "Incremental replay"). A runtime switch, applies to every deployment |
| `--deploy-no-rr-compression` | — | off (compress when the SDK declares it) | **New** deployments do not record `rrCompression` even when the SDK declares `requestCompression` — `REQUEST_RESPONSE` bodies go uncompressed (section 8 "Compressing REQUEST_RESPONSE bodies"). Existing deployments keep theirs (immutable): register again to turn it off |
| `--deploy-require-compression` | — | off (on with `--profile saas`) | `POST /deployments` refuses a `REQUEST_RESPONSE` endpoint whose SDK does not declare `requestCompression: "gzip"` (`400 compression_required`) — section 8. Not to be used with `--deploy-no-rr-compression` |
| `--deploy-transport declared\|auto` | — | `declared` | Choosing the transport mode at registration. `declared`: the mode the SDK declares. `auto`: when the SDK declares `BIDI_STREAM`, probe `/discover` over h2 once — if h2 does not reach the SDK (a proxy that only speaks h1: Render, Railway, Heroku, an edge buffering request bodies) ⇒ the deployment is fixed to `REQUEST_RESPONSE`, with the reason in `transport.bidiProbe`. `POST /deployments {"transport": "auto"}` chooses it for one registration. Every new deployment records `transport.discoveredHttp` |
| `--value-read-path auto\|direct\|proxy\|inline` | — | `auto` | The read path for segmented values for SDKs on wire v6. Large state goes down to the SDK as ref + tail, with a short-lived URL per ref; the SDK GETs the bytes itself, the node does not assemble. `direct`: a presigned GET on the main `s3://` store. `proxy`: `<--value-public-url>/values/v1/<token>`, an HMAC token of the cluster secret, served by any node of the cluster. `inline`: the node assembles, as for older SDKs. `auto`: `direct` when the store is `s3://` and `--value-public-endpoint` is set (or under `--profile saas`); `proxy` when `--value-public-url` is set; otherwise `inline`. An explicit choice that cannot be honoured ⇒ does not start |
| `--value-public-endpoint URL` | — | the store's endpoint | The S3 endpoint **your clients' apps** see, when it differs from the one the node uses (the node's private network). Only changes presigned URLs, not the node's client |
| `--value-public-url URL` | — | the bound ingress address (unless `0.0.0.0`) | The `http(s)://` base address of the node's ingress that your clients' apps can reach, for the `proxy` path. The route `GET /values/v1/{token}` needs no API key: the token is permission to read exactly one segment, short-lived; any token error ⇒ `403` |
| `--value-sweep-every-s N` | — | `3600` (`0` = off) | Interval of the sweep for segments nobody holds any more: they are held in place in the trash and really deleted after `--trash-keep-s` while the proof still holds |
| `--value-min-age-s N` | — | `3600` (minimum `600`) | Segments younger than N seconds are never touched by the sweep — must be ≥ 2 × the PUT→commit deadline (300 s) |
| `--value-presign-ttl-s N` | — | `60` (at most `3600`) | Lifetime of every segment read URL (`direct` and `proxy`). The SDK fetches the bytes when it receives the message, so a short lifetime is enough |
| `--no-user-otlp` | `CAIRNFLOW_NO_USER_OTLP=1` | off | Only export system spans (`cairnflow.run`, `cairnflow.turn`); user spans (`$trace`) and logs (`$log`) never leave the node — they can still be read in the console (`sys_span`, `GET /invocations/{id}/spans`, `/logs`). For shared clusters: the operator's collector must not receive data written by clients, mixing every tenant. On with `--profile saas` |

Logs: stderr, filtered by `RUST_LOG` (default `info`), `CAIRNFLOW_LOG=json` for line-delimited JSON.

## 7. Operations

- **Health:** `GET /health` answers `ok` once HTTP is up. On a cluster it does **not** say whether
  ranges have a leader — see `GET /cluster` (`cairn cluster`): every range having a `leader` means
  ready.
- **Dashboards, alerts, runbooks:** [`docs/runbooks/README.md`](./runbooks/README.md) (Grafana
  `deploy/grafana/cairnflow.json`, Prometheus rules `deploy/prometheus/alerts.yml`).
- **Metrics:** Prometheus at `GET /metrics` (cluster-admin) — counters, gauges, and the latency
  histograms `cairnflow_commit_wait_seconds` (waiting for a durable commit), `cairnflow_drive_seconds`
  (one run of an invocation), `cairnflow_timer_lateness_seconds` (late timers). Shared WAL:
  `cairnflow_nodewal_floor_age_seconds{node}` — how long the GC floor of a node's log has been held at
  a pinned object — and `cairnflow_nodewal_floor_pinned{range}` = 1 for a range that pins it (has not
  flushed its frames to L0). A floor standing still for more than 10 minutes makes the node log a
  `warn` naming the range. L0 per range: `cairnflow_range_l0_views{range}` (cap 8),
  `cairnflow_range_l0_reject_ratio` (throttle level), `cairnflow_range_l0_shed_total{kind}`,
  `cairnflow_range_write_stall_seconds_total` and SlateDB's numbers (`cairnflow_range_slatedb_*`). L0
  at 5 or more for a long time means compaction is not keeping up; the cluster is throttling new work
  with `429 range_overloaded`.
- **Per-tenant metering:** each node process flushes `metering/<tenant>/<hour>/<node>.<boot>` every
  60 s and a last time on graceful shutdown (`<boot>` is random per start, so a restart with a fixed
  `--node-id` does not overwrite the previous life's numbers); `GET /metering?hour=yyyy-mm-ddThh`
  merges every object of that hour (tenant admin). `kill -9` loses the part not flushed yet (up to
  60 s). `units` is a payload-weighted billing unit: each durable command **once durable** counts
  `max(1, ⌈payload / 64 KiB⌉)` (`ctx.run` by its result, `call`/`send` by their argument, a state `set`
  by its value, signals/promises by the resolved value, `sleep`/lazy `get`/`clear` 1; `get` from eager
  state 0), each invocation adds 2 (creation; completion — the part of the result over 64 KiB is
  added), each new artifact by its bytes. Counted at the range's commit, only once durable: replays,
  retried turns, leader changes, splits/merges do not count again. It is also the `units` column of
  `sys_metering` and the billing-units tile of the Usage screen.
- **Shutting a node down:** `SIGTERM`/`SIGINT` ⇒ graceful shutdown: seal each range, close the
  session, notify peers — peers take the ranges after ~0.4 s. The whole shutdown is capped by
  `--shutdown-timeout-ms` (default 20 s); past the cap, or on a second signal, it exits at once and
  peers take the ranges after one TTL.
- **Upgrading the binary and the format:** the bucket carries a `DATA_FORMAT` mark (today **10**,
  **frozen** — D27). From format 10 on, every later release must open your bucket: every bucket change
  goes through the migration framework — see [Upgrading](#upgrading) just below. Buckets at format ≤ 9
  are refused — create a new bucket.
- **Upgrading an SDK deployment** while invocations are running: safe — an invocation is bound to its
  deployment, the new version only takes new invocations. Moving an in-flight invocation to the new
  version is deliberate: `POST /deployments/{id}/replay-check` to try it, then
  `POST /invocations/{id}/migrate` (requires a matching replay) or `restart`.
- **The bucket grows with splits/merges:** retired parent DBs are kept forever unless
  `--gc-retired-dbs` is on. Shared WAL logs of dead nodes are removed for good once no owner record
  references them.

### Disk cache

By default a node **uses no disk**: all state lives in the object store, the block cache is RAM only
(512 MiB + 128 MiB of metadata per node), so a restart or a leader change means a cold cache and the
first turn reads SSTs from the store. `--disk-cache-dir DIR` turns on an SST cache on the node's disk
(SlateDB's `CachedObjectStore`):

- **One cache per node**, shared by every range, capped by `--disk-cache-bytes` (16 GiB). Freshly
  flushed or compacted SSTs go into the cache at once; when taking a range, the node loads the range's
  L0 SSTs in the background (256 MiB per range, capped at 60 s, skipping ranges that are splitting or
  merging). The cache survives restarts.
- **It only makes things faster, never wrong.** Only compacted SSTs go into the cache; WAL, manifests,
  leases, owner records go straight to the store. A damaged part ⇒ read again from the store; a full or
  broken disk ⇒ reads fall back to the store. Contracts DC1–DC5 in
  [`reliability/irreversible.md`](./reliability/irreversible.md) §0.3. Deleting the whole directory
  while the node is stopped is always safe.
- **Measuring:** `cairnflow_disk_cache_{hits,misses,evictions}_total`, `cairnflow_disk_cache_bytes`;
  `SELECT node, disk_cache_bytes, disk_cache_hit_ratio FROM sys_node` through `POST /query`. Runbook
  [`disk-cache.md`](./runbooks/disk-cache.md).
- **Defaults:** off in the binary (it runs without a disk); the sample cluster
  [`deploy/docker-compose.yml`](../deploy/docker-compose.yml) turns it on, 2 GiB per node on a separate
  volume; the Helm chart [`deploy/helm/cairnflow`](../deploy/helm/cairnflow) turns it on, 8 GiB on an
  `emptyDir` with `sizeLimit: 10Gi` (a lost pod means a lost cache — exactly what the "node leaves the
  cluster" item below asks for). With the cache on, the first turn after a leader change made 0 SST
  GETs instead of 21, echo p99 on SeaweedFS did not change, and DC1–DC5 pass.
- **Size:** the hot SST set of the ranges the node leads — the UI's "latest" pages and the hot path
  reading L0. Start at 10–20 % of the DB size per node; the alert `CairnflowDiskCacheThrashing` tells
  you when the cap is too small.

**Customer data on the node's disk.** The cache holds copies of SSTs — customers' journals, state,
output:

- put `DIR` on an **encrypted volume**;
- **purging does not delete the copies** already in the cache: they go when evicted (LRU) or when the
  directory is deleted, **with no time bound**. The same holds for old SSTs on the object store until
  GC deletes them;
- a node **leaving the cluster** (machine replaced, cluster shrunk): delete the whole `DIR`. Temporary
  volumes (`emptyDir`, instance disks) do that by themselves when the pod/machine goes away;
- do not copy the cache directory between nodes.

### Upgrading

The bucket carries a **cluster version** (the `cluster-version` object; absent = 0). Each release
carries an ordered migration registry; each step brings the cluster up by exactly one number. Two
tiers:

| Tier | Who runs it, when |
|---|---|
| cluster (objects on the bucket: `rules`, `catalog/`, …) | **the first new node** to start: takes the `upgrading` lock on `cluster-version`, runs the missing steps, writes `version` after each step, removes the lock |
| range DB (manifest, the WAL kind of the range's DB and its owner record) | the range owner, lazily, when the range opens on a new node, **before** the DB opens; the mark is `data=` on `owners/range-<id>` |
| range (rows in a range's DB) | the range owner, lazily, when the range opens on a new node; the mark is `data=` on `owners/range-<id>` |

| `version` | Step | Tier |
|---|---|---|
| 1 | `rules/2` — the rule book gains `rate`/`burst` (CU/s) and the `tenant` line | cluster |
| 2 | `wal/shared` — a range DB still on SlateDB's native WAL (a single-process bucket created by an older binary, a cluster that ran `--wal-kind slatedb`): drain the native WAL into SSTs, switch the `wal_name` mark to the shared WAL, then the owner record to `wal_kind=shared`. A range already on the shared WAL: nothing to do | range DB |

Later steps (3 and up) are gates that `cairn-ctl cluster enable-*` turns on; the gates are described
in the `GET /admin/cluster-version` fields below and in section 8.

**The `wal/shared` step** runs on each range's first open with the new binary (logs `draining the
native WAL to SSTs for the shared WAL` / `range moved to the shared WAL`). That open is a bit slower
(opening the DB with the native WAL, one memtable flush). If the node dies midway, the next open — on
any node — continues where it stopped. The old native WAL objects (`ranges/<id>/wal/`) stay until the
range retires (DB GC removes the whole DB directory through the trash); they are no longer read. After
this step old binaries (`knows` = 1) **do not start** on the bucket — no way back except a backup.

**Rolling upgrade sequence:**

1. **Back up the bucket** (or at least `rules`, `catalog/`, `tenants`, `cluster.conf`,
   `cluster-version`). There are no reverse migrations: going back after `version` is bumped means
   going back to the backup.
2. `GET /admin/cluster-version` (cluster-admin): record `version` and each node's `knows`.
3. Upgrade **one node at a time**: `SIGTERM` (graceful shutdown, peers take the ranges after
   ~0.4 s), replace the binary, start it. The first new node is the cluster-tier migration runner —
   logs `cluster-tier migration starting` / `cluster-tier migration done`. While the `upgrading` lock
   is held, split/merge returns `503` and the retired-DB GC round skips its turn. If the runner dies
   midway, the next new node takes the lock over (the dead runner's session expires) and runs **one**
   step again (steps are idempotent).
4. Wait until `/cluster` shows a leader for every range before shutting down the next node.
5. After the last node: `GET /admin/cluster-version` must show `upgrading: null`, `version` =
   `binary.knows`, and every `nodes[].knows` line equal.

**Reading `GET /admin/cluster-version`:**

```json
{
  "version": 1, "minBinary": 1, "requiresKnows": 1,
  "upgrading": null,
  "binary": { "knows": 1, "ledger": [{ "to": 1, "name": "rules/2", "tier": "cluster" }] },
  "nodes": [{ "node": "host-a", "inc": 17, "knows": 1 }, { "node": "host-b", "inc": 9, "knows": 0 }]
}
```

- `upgrading` not `null`: a runner holds the lock — `{from, to, by: "<node>/<inc>"}`.
- `requiresKnows` = `max(version, minBinary)`: any binary whose `knows` is lower **can no longer
  start** on the cluster (error `… newer than this binary understands … upgrade this binary`,
  `NewerFormat`).
- `nodes[].knows` `null`: a binary from before the migration framework.
- `callPrefix`: the `calls/prefix` gate (grouping unkeyed `ctx.call` trees in the caller's range,
  [`key-design.md`](./key-design.md) §7). `callPlacement`: the `calls/placement` gate (deliberate
  spreading with `placement: "spread"`, section 7 of the same page). A new cluster created by a binary with step 5:
  both on from the start (`requiresKnows` = 5). An old cluster — including one that turned
  `calls/prefix` on with a step-4 binary —: once **every** node runs the new binary,
  `cairn-ctl cluster enable-call-prefix --apply` (one command turns both on). Gates are one-way — once
  on, `lower-min-binary` does not lower `minBinary` below a gate step that is on (4, 5, 6 or 7), and
  binaries from before that step can no longer start on the cluster.
- `streams`: the `streams/table` gate (step 6, the `Streams` table of durable streams, `$log`,
  `$trace` — [`admin-api.md`](./admin-api.md) section Streams). A new cluster created by a binary with
  step 6: on from the start (`requiresKnows` = 6). An old cluster: once **every** node runs the new
  binary, `cairn-ctl cluster enable-streams --apply`. One-way like the two gates above: a step-5 binary
  ignores `Streams` rows and **leaks** them when purging an invocation. With the gate off, durable
  streams only live in RAM like temporary streams.
- `schedules`: the `schedules/table` gate (step 7, the `Schedules` table of server-side schedules —
  [`admin-api.md`](./admin-api.md) section Schedules). A new cluster created by a binary with step 7: on
  from the start (`requiresKnows` = 7). An old cluster: once **every** node runs the new binary,
  `cairn-ctl cluster enable-schedules --apply`. One-way: a step-6 binary leading a range that has
  schedules **fires nothing**. With the gate off, `PUT /schedules` returns `409` and the firing round
  does not run. `--schedule-min-period-ms` (default 10 000, env `CAIRNFLOW_SCHEDULE_MIN_PERIOD_MS`) is
  the floor of `everyMs`: each firing is at least one commit (estimated ≤ 3 PUTs within a range, ≤ 4
  across ranges), so sub-minute schedules are expensive. Metrics:
  `cairnflow_schedule_fires_total{outcome}`, `cairnflow_schedule_fire_lag_seconds`,
  `cairnflow_schedules` (the "Server-side schedules" row of the dashboard).

**While the cluster mixes binaries** (only two consecutive versions are supported, N−1 ↔ N): a running
old node keeps serving the ranges it leads, but once `version` is bumped it **cannot open new ranges**,
and when it meets a new object it **stops loudly** instead of overwriting — for example `POST /rules`
on an old node answers `500` `NewerFormat … cairnflow-rules/2` and the rule book does not change; the
node keeps running with the rule book it loaded earlier. So finish the upgrade; do not leave the
cluster mixed for long.

**Rolling back:** only possible **before** `version` is bumped — i.e. while `requiresKnows` is still ≤
the old binary's `knows`. After that the old binary refuses to start; roll back with the backup from
step 1 (invocations written after the backup are lost).

### Incident recovery: `cairn-ctl`

`cairn-ctl` reads **the bucket directly**, no running node needed. It needs the bucket's credentials,
which is why it is a separate binary — do not hand it to people who only need an API key (`cairn`).
Point it at the bucket exactly as a node:

```sh
cairn-ctl --main-store s3://my-bucket/prod [--wal-store s3://my-wal-bucket] verify
cairn-ctl --data-dir ./data table show
```

**Read-only** commands (GET/HEAD/LIST only; a missing `cluster.conf` is reported as an error, never
creates a cluster):

| Command | Tells |
|---|---|
| `table show` | the range table: version, key span, `gen`, intent |
| `owners` | every range's owner record: term, session (and whether it is still `live`), tombstone, WAL segments |
| `ranges retired` | tombstoned ranges, whether their DB still exists, which live ranges still hold that DB |
| `range dump <id> [--table T] [--limit N]` | raw rows of a range's DB, opened with the production loader |
| `wal floors` | every shared WAL log: GC floor, object count, ranges pinning the floor |
| `verify` | (a) the table covers the whole keyspace; (b) every range in the table has a DB that opens; (c) every `external_dbs` points at a DB and checkpoint that still exist; (d) orphan `ranges/<id>` directories; (e) intents hung for more than 100 s. Exit code 1 when it finds problems; `--json` for machines. `--deep` adds `sst_missing`: every SST that the current manifest and unexpired checkpoints of a live range mention is still on the bucket (one LIST per DB; not in the background scrubber) |

**Repair** commands **dry-run** by default: they print the table / record before and after and write
nothing. Add `--apply` to write — every check first, then **one** CAS, leaving `audit/ctl/<ts>` on the
bucket (who — `--who`, default `$USER@$HOST` —, the command, before, after).

- `intent clear <range> [--expect-version N]` — roll back an uncommitted split/merge that `verify`
  reports as hung: a CAS that drops the intent (exactly as a node rolls back when it takes a range),
  then cleaning up the DB and owner record of the orphan child ranges. `--expect-version` refuses if
  the table changed.
- `range force-retire <id>` — write the tombstone of a range that **has left the table** without one
  (an orphan DB reported by `verify` under (d)); from then on DB GC (`--gc-retired-dbs`) may delete it
  once nobody references it. A range still in the table is refused. When the leader died between the
  table commit and writing the tombstone, the nodes write the tombstone on its behalf as soon as the
  tracker sees the old owner's session expire (the round runs on a nudge; `--retire-orphans-every-s`
  is the safety net, default 3600 s, `0` off); this command is for the rest.
- `table restore [--force]` — rebuild a `cluster.conf` deleted from outside from the highest history
  copy `cluster-history/<V>` (the nodes write one after every won table change and keep 16), **only
  when** the rest of the bucket proves V is the last table (no owner record, `ranges/<id>` or `ops/`
  ledger carries an id ≥ the `next_range_id` of V; every live range of V has an owner record without a
  tombstone; the operation ledgers agree). Writes with `Create`; `--apply` refuses while sessions have
  not ended (`--force` when you are sure every node is stopped). Cannot be proven ⇒ refused, printing
  the mismatches ([runbook](./runbooks/cluster-conf-lost.md)).
- `fork --to <prefix>` — a copy of the cluster under another prefix **in the same bucket**: every
  range cloned from a checkpoint (no need to stop the cluster), with `cluster.conf`, the cluster
  configuration and owner records with their lease scrubbed. Start a node with `--main-store
  <bucket>/<prefix>` to investigate an incident on real data without touching real data. The copy
  reads the source's SSTs: fork writes checkpoints on the source DBs (they do not expire), deleting the
  copy does not remove them; and it sets a **GC barrier** `fork-<prefix>` on the source for every range
  the copy stands on (including retired ancestors), valid for 24 hours — renew the barrier to use the
  copy longer ([runbook](./runbooks/restore-from-fork.md)). A range on the shared WAL only carries what
  reached L0 at snapshot time.

**The owner is still alive.** Repair commands refuse while a range's owner still has a `live` session —
a split/merge coordinator may still be running, and repairing under its feet means two writers of the
table. Once you are sure that node is dead (the whole cluster went down, so nobody marks its session
`expired`), add `--force`. `--force` is only for incident recovery: it removes the only safety net
between the tool and a live node.

**Self-contained backup to another store:** `backup --to <uri>`, `backup list`, `backup prune`,
`restore --from <uri>` — [runbook](./runbooks/restore-from-backup.md). A split/merge that commits
during a backup run ⇒ the run is abandoned and retried (at most 3 times, then an error); `restore`
refuses a backup with a range that has an owner record but no DB. A target at another provider gets
its own credential set `P` + `AWS_*` with `--backup-env-prefix P` (section 3).

**Reports on the bucket:** `--report-to-bucket <id>` on `backup`, `backup list`, `backup prune`,
`restore`, `cluster status`, `cluster enable-*`, `table restore` also writes the result into the
cluster's `ops-reports/<id>` object (`--main-store`): exactly the JSON that `--json` prints (`result`),
plus `command` (the command name, not argv), `apply`, the `binary`/`knows`/`dataFormat` of
`cairn-ctl`, `who`, start/end times, `exitCode`, `ok`, `error`. It carries no secrets (`keys create`
does not take the flag). Read it with a `provision` key: `GET /admin/ops-reports/<id>` (`404` = not
there yet), `GET /admin/ops-reports` (the list). How a provisioning service uses it: run `cairn-ctl`
as a one-off job next to the cluster (the bucket and backup-target credentials are that job's
secrets), then read the report over HTTP — the provisioning service never holds bucket credentials.

- `<id>` is chosen by the caller, **new each time** (1–128 characters `A-Z a-z 0-9 - _ .`, starting
  with a letter or digit). An id that already has a report ⇒ code 3, the command **does not run**; a
  malformed id ⇒ code 2. Written once with `Create` (an ambiguous write result is resolved by
  reading back), never overwritten.
- The report is written **even when the command fails** (`ok: false`, `exitCode`, `error`, `result:
  null`). A command that finished but could not write its report ⇒ a non-zero exit code (read
  stderr).
- Cleanup: after each write, the 256 newest reports are kept and reports more than 30 days older than
  the one just written are dropped (age by the store's `last_modified`). Reports do not travel with
  backups, and `restore` treats a target holding only `ops-reports/` as empty — the report of a failed
  `restore` does not block a retry.

`cairn-ctl --version` (also `cairnflow --version`, `cairn --version`) prints
`<binary> <package version> (data=<data format>, knows=<number of migration steps the binary knows>)`.
Use a `cairn-ctl` with the same `knows=` as the cluster's binary — taking it from the same image is the
surest way.

Exit codes: 0 done (or `verify` clean), 1 `verify` found problems, 2 bad arguments, 3 refused, 4 store
error.

**Bucket versioning is the safety net for every single object.** cairn does not delete customer data
without a proof (the trash, the proof gate), but it cannot stop a delete **from outside**: an operator,
a bucket lifecycle rule, a store bug. Only the range table has its own layer (`cluster-history/` +
`table restore`); every other object — a range's SSTs and manifests, owner records, `auth/keys`,
`tenants`, the catalog — can only be brought back from bucket versioning once lost (or from a backup,
as of its snapshot time). Turn on versioning for the cluster's bucket, keep old versions at least as
long as you need to detect an incident (a periodic `cairn-ctl verify --deep` finds missing SSTs before
a read does), and keep every lifecycle rule away from the cluster's prefix. cairn does not check
versioning at startup (`object_store` does not expose bucket configuration).

## 8. Security

**Logging out revokes the session on the server.** Key session cookies
(`cairn_ui`/`__Host-cairn_key`) and the SSO cookie (`__Host-cairn_ui`) carry a random session id;
`DELETE <base>/session` writes that id into `auth/revoked-sessions` (each line lives until the cookie
would expire). Only that session dies — other sessions of the same person and the same key live on;
logging in again works at once. Other nodes see the revocation at their next key registry refresh (a
few seconds). The list is capped: each tenant keeps at most 200 logged-out sessions **still
unexpired** — over ⇒ logout answers `429` (the cookie is still removed in the browser, the session
lives until it expires; revoke the key to end it at once). There is no global cap: another tenant
filling the list cannot block your logouts. Logging out with an expired, revoked, or tenant-cap-refused
cookie writes nothing and does not bump the registry generation. Logouts that **do write** are
rate-limited per node: 30, then one every 2 s per client IP, and 60, then one per second per tenant —
over ⇒ `429` with `Retry-After`. If `auth/revoked-sessions` cannot be read, API keys keep working; only
console sessions get `503` after two refresh ticks, until it can be read again. A logout bumps the
registry generation and notifies other nodes like every cluster-level registry write, so other nodes
re-read the registry at once (without waiting for the 10-minute full reconciliation). Open SSE streams
re-check permissions every 30 seconds: a revoked key, a logged-out session, a tenant being deleted ⇒
the stream closes. **Upgrading:** cookies minted before this behaviour existed carry no session id and
are refused — console users log in again once.

**API keys on the ingress port.** Every route except `GET /health` requires `Authorization: Bearer
<key>` (`401` if missing/wrong, `403` if the role is not enough). A plaintext bootstrap key
`CAIRNFLOW_ADMIN_KEY` shorter than 32 bytes ⇒ the node refuses to start, and the message says how to
generate one; the hash form `CAIRNFLOW_ADMIN_KEY_SHA256` is not measured. **Wrong** Bearer attempts are
capped per client IP (behind `--trust-proxy`: the address in `X-Forwarded-For`): a burst of 100, then
one per second; a wrong attempt over the budget ⇒ `429 auth_failures_limited` with `Retry-After`. A
correct key is never capped — clients behind the same NAT as a prober are not locked out. Each key is
bound to one **tenant** and one **role**. A role is a **capability set**, not a nested string: each
route declares the capabilities it accepts (`auth::declared_caps`, one table in one place; an
undeclared route falls back to `cluster` — closed by default), and a key may call a route when its
role's set intersects the route's set.

| Capability | Routes |
|---|---|
| `read` | reading the tenant's data: `GET /invocations`, journals, `$log`/`$trace`, `/deployments*`, `/services*`, `/archive*`, `/stats/*`, `/tenant`, `GET /identity`, `GET /metering*`, `POST /query`; plus every `GET` of `invoke` below |
| `invoke` | calling: `/invoke/*`, `/scope/*/invoke/*`, `/workflow/*`, `/promises/*`, `/awakeables/*`, `GET /invocations/{id}`, `POST /invocations/{id}/attach`, `POST /invocations/{id}/signal`, `POST /invocations/{id}/cancel?mode=signal` (cooperative cancel), `/schedules*` and `/scope/*/schedules*` within the key's tenant (another tenant's schedules through `?tenant=` need `cluster`), streams (except `$…`) and artifacts of invocations |
| `operate` | changing the tenant: `POST/DELETE /deployments*` (including `replay-check`), `PUT /services/*/revision`, hard cancel (`cancel` without `mode=signal`), purge, resume, replay, migrate, restart, `/invocations/batch/*` |
| `provision` | the provisioning service's work: `/admin/keys*` (create, list, revoke, ask about a revocation again), `/admin/tenants*` (including `PUT /admin/tenants/{t}/state` — suspending/limiting a tenant), `GET /admin/cluster-version`, `/rules` (only the `tenant` line — pattern rules are neither visible nor editable), `GET /identity?tenant=`, `GET /metering*?tenant=`, `GET /audit?tenant=`, `GET`/`PUT /admin/oidc`, `PUT`/`DELETE /admin/oidc/clients/{id}`, `POST /admin/oidc/revoked` (SSO configuration, section 8), `GET /admin/ops-reports*`, `GET /auth/whoami` (`kind: "env"` with a hashed key in the env). `GET /admin/tenants` does not return quarantine rules (`quarantineRule`) to this role. It reads **no** run data: invocations, journals, state, `/query`/`sys_*`, archives, artifacts, streams |
| `cluster` | the cluster's work: `/rules/{pattern}`, pattern rules, `/limits`, `/vqueues`, `/scheduler`, `/metrics`, `/cluster*`, `/ranges/*`, every other `/admin/*`; and seeing across every tenant (`?tenant=` on tenant routes, `/query` over every tenant) |

| Role | Capabilities | Notes |
|---|---|---|
| `invoke` | `invoke` | |
| `view` | `read` | read-only on its own tenant — everything `admin` reads, without calling or changing anything |
| `admin` | `read`, `invoke`, `operate` | |
| `provision` | `provision` | the provisioning service's key: reads no run data; can only grant/revoke `invoke`/`view`/`admin` keys (not `provision`, not `cluster-admin` — no escalation); cannot log in to the console. In the registry, or as a hash through `CAIRNFLOW_PROVISION_KEY_SHA256` (below) |
| `cluster-admin` | all five | |
| `metrics` | no capability | **only** `GET /metrics` (not `HEAD`, not `/scope/*/metrics`): every other route `403`. Only available through `CAIRNFLOW_METRICS_KEY` (below), cannot be created with `POST /admin/keys`, cannot log in to the console |

`view`/`provision` keys need the key registry format `cairnflow-keys/2`, which binaries from before
key roles cannot read (they stop confirming the registry is fresh and answer `503` to every stored
key). So the registry keeps writing `/1` until the operator turns the gate on **once every node runs
the new binary**:

```sh
cairn-ctl cluster status                       # every session knows ≥ 8
cairn-ctl cluster enable-key-roles --apply     # min_binary=8, then key_roles=on (one-way)
cairn-ctl keys create default provision --apply   # a key for the provisioning service; the secret is printed once
```

Before that, creating a `view`/`provision` key (API or `cairn-ctl`) ⇒ `409 key_roles_disabled`. A new
cluster created by this binary has the gate on already.

**Tenant state:** `PUT /admin/tenants/{t}/state {"state": "active"|"limited"|"suspended"}`
(`provision` capability). `limited` refuses the tenant's new work (`429 tenant_limited`) and skips
schedule firings; work already running runs to its outcome. `suspended` adds: none of the tenant's
turns may run (the engine does not call out to its endpoint, `POST /deployments` `403`), invocations
stay where they are, due timers are deferred; back to `active`, they continue within at most 15
minutes. A turn that had its slot before the command also stops right before its next call; a turn that
is in the SDK when the command arrives goes on to the next step boundary (a `REQUEST_RESPONSE` request in
flight, the next ack batch of a BIDI stream) and then stops — after the `204` no new call goes out to
the endpoint. The command answers `204` once every live node has applied it, `202 {pending}` otherwise
— send the same command again to ask again. A node that misses the apply, or that died a moment
earlier, is asked again within the request for up to two lease TTLs (until it confirms or its session
ends), so one node failure does not turn the answer into `202`. A node whose session ended stops
accepting registry keys before its record says so: a node serves keys from its copy of the registry
only while its own lease grant is valid, and a node that lost its session and opened a new one
re-reads the registry before serving keys again (`503` meanwhile; the bootstrap key is not affected). Binaries before step 9 ignore the state, so the command
answers `409 tenant_state_disabled` until the gate is turned on once every node runs the new binary:

```sh
cairn-ctl cluster enable-tenant-state --apply  # min_binary=9, then tenant_state=on (one-way)
```

An unknown `state=` written by hand into the `tenants` registry (a typo) ⇒ the tenant is treated as
`suspended`.

**Deleting a tenant:** `PUT /admin/tenants/{t}/state {"state":"deleting"}` (`provision` capability;
tenant `default` ⇒ `400`). From then on every key, cookie and SSO session **of** the tenant ⇒ `403
tenant_deleting` (`cluster-admin` and `provision` keys can still read the state), no turn runs, no new
key is granted. The node taking the command measures its clock against the store before writing the
`deleting-ms` mark: skew over `keep/4` or no measurement ⇒ `503` (fix the node's clock or send it to
another node). Phase B also waits until phase A's in-place holds are `keep` old by the store's clock,
so a mark written by a node with a skewed clock cannot shorten the reversible window.

- **Phase A, 7 days, reversible** (`--tenant-purge-keep-s`, default 604800): the D-d runner holds in
  place in the trash every object prefix of the tenant (`archive/<t>/`, `artifacts/<t>/`,
  `audit/tenant/<t>/`, `deployments/<t>/`, `metering/<t>/`, `stats/<t>/`, `captures/<t>/`,
  `capture-artifacts/<t>/`, `catalog/<t>`, `quarantine/<t>`, `capture-requests/<t>`) — nothing is
  deleted yet. `PUT …/state {"state":"suspended"}` (or `active`, `limited`) reverses it: the next round
  releases the holds, the data is intact.
- **Phase B, irreversible:** at the end of phase A the runner writes the `purging-ms=` mark into the
  registry (from here a reversal ⇒ `409 tenant_purging`), then logically deletes every row of the
  tenant in every range, forces a compaction of each range (a memtable flush, then a full compaction
  into the last sorted run — the deleted rows' bytes leave the SSTs; the compactor sees this
  compaction at its next poll, ≤ 1 hour), really deletes the prefixes (the proof
  `DeleteProof::tenant_purged` is rebuilt before each delete), removes the tenant's API keys, OIDC
  clients and rule line.
- **Tombstone:** every live range has a durable clean proof (`tenant-purge/<t>/<range>`: a full
  compaction finished, or a child of a split/merge after the source's compaction mark — a split/merge
  midway does not force a rescan) and every prefix is empty ⇒ the registry becomes `state=purged
  purged-ms=`, with an `audit/ctl/<ts>` record (no customer data). Tenant ids are never reused: every
  `PUT …/state` and `POST /admin/keys` for it ⇒ `409 tenant_purged`.

Progress: `GET /admin/tenants/{t}/purge` (`provision` capability) — the state, the three marks, the
prefixes being held, the objects left per prefix. `--tenant-purge-keep-s 0` turns the purge round off
(the tenant stays `deleting` forever, nothing is deleted). **Backups are not rewritten**: they expire
by their own retention schedule; restoring a backup older than the purge brings the tenant back to life
— run `PUT …/state deleting` again for every tenant you have purged (runbook
`restore-from-backup.md`). Binaries before step 11 read `deleting` as `suspended`, so the command
answers `409 tenant_purge_disabled` until the gate is turned on:

```sh
cairn-ctl cluster enable-tenant-purge --apply  # min_binary=11, then tenant_purge=on (one-way)
```

**Stream RAM per tenant:** the streams (`ctx.stream`, `$log`, `$trace`) of every tenant on a node share
a 256 MiB cap, but each tenant can only hold **its share** — by default 1/8 of that cap (32 MiB); tenant
`default` (self-hosting with a single tenant) holds the whole cap. Changing a tenant's share:
`PUT /admin/tenants/{t} {"maxStreamRam": <bytes>}` (`provision` capability; `null` = back to the
default), registry line `max-stream-ram=`. Records over the share are dropped and counted
(`cairnflow_stream_dropped_total`, a `gap` for readers) — as with the node's cap, an invocation does
not fail because of a stream; other tenants are not affected.

**Data plane audit log:** each human action on a tenant becomes a small object under
`audit/tenant/<tenant>/<date>/` (read: `GET /audit`, `sys_audit`). Actions on invocations and
schedules are written **in the same commit** as the action — after the `audit/table` gate, turned on
once every node runs the new binary:

```sh
cairn-ctl cluster enable-audit --apply         # min_binary=10, then audit=on (one-way)
```

For tamper resistance (WORM): turn on S3 Object Lock on the bucket's `audit/` prefix — cairnflow only
writes with `Create`, never overwrites, never deletes.

Console logins/logouts (key or SSO) are audited too (`session.login`/`session.logout`, the session's
tenant). The intent is written **before** the cookie is granted: if the bucket refuses writes under
`audit/tenant/`, nobody can log in to the console (key `503`, SSO `sso_error=unavailable`); logging
out still works. The console has an "Audit log" screen (`/ui/audit`).

- The bootstrap key: the `CAIRNFLOW_ADMIN_KEY` variable — cluster-admin of tenant `default`, living in
  an environment variable, written down nowhere. Other keys: `cairn keys create <tenant> <role>`
  (`POST /admin/keys`) — the `cf_…` secret is shown **once**; the bucket only keeps its SHA-256 (the
  `auth/keys` object). `cairn keys rm <id>` revokes **synchronously**: the command only reports done
  once every live node has confirmed it applied the change; nodes that did not confirm in time are
  printed (`202` + `pending`) — they see it within one `--deploy-refresh-ms` tick. A node that does not
  confirm is asked again within the request for up to two lease TTLs, until it confirms or its session
  ends on the bucket (a node killed a moment earlier), so the command can take that long to answer. The set of "live
  nodes" is a LIST of `sessions/` at answer time, not a cached table: a failed LIST gives `202` with
  `listed: false`, never `204`. After a `202` (or a `DELETE` with an unknown outcome — a lost
  connection, a `5xx`) ask again on **any node**: `GET /admin/keys/{id}/revocation` re-reads the
  registry, asks every live session to re-read it as well, then answers `{revoked, confirmed, pending,
  listed}`. `confirmed: true` means every live node has certainly stopped accepting the key;
  `revoked: false` means the revocation was not written — call `DELETE` again; a failed registry read ⇒
  `503`. An id that is not in the registry (including a typo) is also `revoked: true`, so check the id
  with `GET /admin/keys` before trusting it. A node that cannot confirm its registry is still fresh for
  more than 2 ticks (cut off from the bucket and the coordinator) answers `503` to every request
  authenticated by a registry key until it confirms again (the bootstrap key is never closed).
- **The metrics token:** the `CAIRNFLOW_METRICS_KEY` variable — role `metrics`, so Prometheus can
  scrape without holding a cluster-admin key (metrics see across every tenant, so this role is not
  available to tenant keys). Like the bootstrap key: lives in an environment variable, never written to
  the bucket, the same on every node; rotating = changing the variable and restarting. Shorter than 32
  bytes or equal to `CAIRNFLOW_ADMIN_KEY` ⇒ the node refuses to start. It is deliberately not in the
  `auth/keys` registry: an unknown role in the registry would make old binaries fail to read the whole
  registry during a rolling upgrade. Scrape configuration: [runbooks/README.md](./runbooks/README.md)
  "Setup".
- **Hash-only keys:** `CAIRNFLOW_PROVISION_KEY_SHA256` and `CAIRNFLOW_ADMIN_KEY_SHA256` carry the
  64-hex-digit `sha256` of a key, never the plaintext — anyone who can read the node's env (platform
  secrets, `docker inspect`, `/proc/<pid>/environ`) does not get the key. Computing the hash:
  `printf %s "$KEY" | sha256sum` (no trailing newline). Generate the key like any secret
  (`cf_$(openssl rand -hex 24)`): hashing does not slow down guessing, so the key must be random
  enough.
  - `CAIRNFLOW_PROVISION_KEY_SHA256`: role `provision` of tenant `default` (table above), living in the
    env like the bootstrap key — not in the `auth/keys` registry (no `auth/keys-v2` gate needed), cannot
    log in to the console, cannot be revoked through the API, never closed because of a stale registry.
    Enough for the node to start: a cluster provisioned by a separate service can run with **no
    cluster-admin key at all** in that service's hands.
  - `CAIRNFLOW_ADMIN_KEY_SHA256`: a bootstrap cluster-admin key like `CAIRNFLOW_ADMIN_KEY` (the same
    console session `kid` as the plaintext form of the same key) — the **break-glass** path: the
    plaintext only sits in a break-glass vault, an operator takes it out when needed, the cluster only
    holds the hash.
  - Malformed (not 64 hex digits), both `CAIRNFLOW_ADMIN_KEY` and `…_SHA256` set, or two roles sharing
    one secret (cluster-admin, `provision`, the metrics token must be pairwise different) ⇒ the node
    **refuses to start**, with an error naming the variable, not the value. An empty variable ≡ absent.
  - **Rotating / revoking:** set the new hash (or remove the variable) on **every** node, then restart
    the nodes one by one; the old key stops being accepted on restarted nodes. During the rollout both
    keys live on two groups of nodes — switch the provisioning service to the new key once every node is
    up. Emergency revocation (a leaked key): the same, restarting every node at once. Console cookies of
    the old cluster-admin key die with it (the `kid` changes with the hash).
- No key at all and no `--no-auth` ⇒ the node **refuses to start** (the metrics token alone does not
  count).
- **The tenant** is the key's tenant — never taken from the path. Each tenant has its own service
  registry (the same service name in two tenants is two deployments), and the tenant is part of every
  user-named key: state, promises, idempotency keys, workflow ids, the queue and EXCLUSIVE lock of a
  virtual object, the input of the partition key hash. `Cart/x` of two tenants are two objects.
  `ctx.call` stays within the caller's tenant. Another tenant's invocation answers `404` as if it did
  not exist. A `cluster-admin` key sees every tenant, and administers other tenants with `?tenant=` on
  `/deployments*`, `/services*`, `GET /invocations`.
- **The flow control rule book belongs to the cluster, the counters to each tenant**: a rule `acme`
  with cap 10 gives the `acme` scope of *each* tenant 10 slots of its own, not 10 shared.

**The internal port between nodes.** `/internal/*` only exists on `--internal-port`, and every request
there must carry `x-cairn-timestamp` + `x-cairn-signature` = HMAC-SHA256(cluster secret, `method \n
path?query \n ts \n sha256(body)`), clock skew ≤ 60 s — otherwise `401`. The cluster secret: the
`CAIRNFLOW_CLUSTER_SECRET` variable (the same on every node), or the `auth/cluster-secret` object the
first node creates in the bucket. The ingress port does not serve `/internal/*` (`404`).

**Requests on behalf of a tenant.** Discovery, every SDK turn and replay checks go through a client of
their own, separate from the client of the internal path (forwarding to the leader, asking sibling
nodes — that path must reach internal IPs). The tenant client **does not follow redirects** and **does
not read `HTTP(S)_PROXY`** in any configuration; an SDK behind an HTTP proxy is registered with an
address the node can reach directly. The `/discover` body is capped at 1 MiB, the whole discovery
request at 30 s, connecting at 10 s. If someone other than you can register deployments (a tenant's
`admin` key), turn on `--egress-deny-private` and `--egress-deny-cidr <the cluster's load balancer
address>` (section 6): each `POST /deployments` is a GET to a host the caller chooses.

**Request identity towards the SDK.** With `CAIRNFLOW_IDENTITY_MASTER` set (≥ 32 bytes, the same on
every node, kept outside the bucket — like a secret in a secret manager), every `/discover` and
`/invoke` the node sends to an SDK carries `x-cairn-signature-scheme: v1` and `x-cairn-jwt-v1` (an
EdDSA JWT: `aud` = the path, `sub` = the tenant, valid ±60 s, `jti` = the invocation id + attempt,
`nonce` = fresh random bytes, so no two requests carry the same token). The
key is **per tenant**, derived from the master and a generation — backups and `cairn-ctl fork` carry no
private key. Get the `kid` with `GET /identity` (the tenant's `admin` key), then turn verification on in
the SDK:

```ts
await serve({ services, identityKeys: ["publickeyv1_…"] });
```

A request without a signature, signed with another key, for the wrong path, expired, or carrying the
`jti` of another invocation ⇒ `401`, and the handler does not run. An SDK whose clock is more than 60 s
off the node's answers `401` to every request. Rotating the key takes four steps, each a command with an
audit record: `cairn-ctl identity publish <tenant> --apply` (publishes a new generation, still signing
with the old one) → add the new `kid` to `identityKeys` and deploy the SDK → `cairn-ctl identity
activate <tenant> --apply` (sign with the new one) → `cairn-ctl identity retire <tenant> --apply`
(remove the old key; then remove the old `kid` from the SDK). Changing the master changes every key at
once — every verifying SDK answers `401` until `identityKeys` is updated.

**SSO login to the console (OIDC).** A cluster is a relying party of **one** OIDC issuer (Keycloak,
Authentik, Okta, the provisioning service's own `oidc-provider`…). There is no flag: the configuration
is the `auth/oidc` + `auth/revoked` objects, set through the API with a `provision` or `cluster-admin`
key. Absent ⇒ no SSO button, everything as before (logging in with an API key always remains).

```sh
# the issuer (compared verbatim with the discovery's `issuer` and the `iss` claim), session TTL
# (default 1 h, 60 s…24 h), the IdP name on the login button ("Sign in with …", default "SSO")
curl -X PUT -H "Authorization: Bearer $KEY" localhost:8080/admin/oidc \
  -d '{"issuer":"https://idp.example.com/realms/ops","sessionTtlMs":3600000,"displayName":"Keycloak"}'
# one client per hostname: request to a hostname ⇒ client ⇒ tenant
curl -X PUT -H "Authorization: Bearer $KEY" localhost:8080/admin/oidc/clients/cairn-prod \
  -d '{"hostname":"cairn.example.com","tenant":"default","maxRole":"admin",
       "portalUrl":"https://portal.example.com"}'
curl -H "Authorization: Bearer $KEY" localhost:8080/admin/oidc      # the registry just read from the bucket
```

- At the IdP: a **public** client (no secret), PKCE `S256`, the redirect exactly
  `https://<hostname><base>/login/oidc/callback` (base defaults to `/ui`), scopes `openid email
  profile`. The `id_token` is signed `RS256`/`PS256`/`ES256`/`EdDSA` (never `none`/`HS*`), and has
  `sub`, `cairn_role` (`admin`, `view`, or `invoke`; absent or unknown ⇒ refused), optionally `email`,
  `name`, `cairn_tenant` (when present it must equal the client's tenant). Allowed clock skew 60 s.
- **The tenant comes from the configuration, not from a claim**, and the role is **cut** to the
  client's `maxRole` — at login and on every request. With a shared IdP, a user attribute that users can
  edit themselves and that maps into a claim cannot pick a tenant or a higher role. `cluster-admin`
  never goes through SSO.
- The session cookie `__Host-cairn_ui` (`Secure`, `HttpOnly`, `SameSite=Strict`, `Path=/`), the flow's
  temporary cookie `__Host-cairn_oidc` (`Lax` — required when the IdP is on another site). The
  `__Host-` prefix forces `Secure`: browsers only accept it over **https or `localhost`**. Behind a TLS
  proxy set `--ui-cookie-secure on` (or `--trust-proxy` so the node knows the request came over https)
  — the node builds `redirect_uri` from `Host` and that scheme.
- Each request with an SSO cookie checks again: the client is still in the registry, the request's
  hostname matches the client's hostname, the tenant and issuer have not changed, the session has not
  been revoked. Removing a client (`DELETE /admin/oidc/clients/{id}`), changing the issuer, lowering
  `maxRole` take effect on the next request. Revoking one person: `POST /admin/oidc/revoked
  {"client","sub","beforeMs"}` — sessions with an `iat` before the mark die; the stored mark is at
  least the cluster's time + 60 s, so the person can log in again after about a minute (changing a
  role: revoke, then log in again to get the new role).
- Write commands answer `204` once **every** node has applied them, `202 {pending}` otherwise — as for
  key revocation. Repeating a write with the same content is idempotent.
- Anti-flooding: the flow cookie is **single-use** per node (a `state` that already went through the
  callback is refused before calling the issuer); `/login/oidc` 20 times per client, then once every
  3 s; **failed** callbacks 10 times per client, then once every 6 s; calls to the issuer's token
  endpoint 20 times per **OIDC client** (per tenant), then once per second, and 200 times from the
  whole node, then 10 per second (so flooding one tenant does not push another tenant's SSO into
  `429`). The per-client cap has two lanes: an IP holds 3 tickets for that lane (one more each
  minute); a ticket is taken only by a callback with a fresh, valid flow, right before the exchange,
  and given back when the login succeeds. An IP out of tickets uses a second lane of 10, then one
  every 2 s — junk codes from a few IPs do not lock the tenant's other users out. Out of budget ⇒ `429` + `Retry-After`. Every per-IP cap of the console (including key login)
  groups IPv6 by /64; the client IP is taken from `X-Forwarded-For` only when the peer is within
  `--trust-proxy`.
- Discovery, JWKS and token requests go through the node's egress policy (`--egress-*`): an issuer in
  a private network needs `--egress-allow-cidr` when `--egress-deny-private` is on. The last good copy
  stays in RAM: a dead issuer only blocks **new** logins; existing sessions and API keys keep working.
- Rolling upgrades: binaries from before SSO support do not read `auth/oidc` and refuse SSO cookies —
  SSO users log in again once when a request lands on an old node.

**The SaaS profile.** A cluster shared by many clients — or a self-hosted cluster that wants the same
level of lock-down — runs `--profile saas` (`CAIRNFLOW_PROFILE=saas`). The profile adds no new
behaviour; it turns on the whole set of safety flags and **refuses to start** (exit code 1, listing
every missing condition at once, before touching the bucket) when a condition is missing:

| | |
|---|---|
| Turns on | `--query-isolation process` (`/query` SQL runs in a `cairn-query-worker` child process; a missing binary ⇒ does not start), a non-dumpable node on Linux (`PR_SET_DUMPABLE 0`: processes of the same user — including `/query` children — cannot read `/proc/<node>/environ`, cannot `ptrace`, no core dumps), `--egress-deny-private`, `--egress-require-https` (only `https://` URIs accepted), `--ui-cookie-secure on`, `--no-user-otlp` (§6), `--deploy-require-identity`, `--deploy-https-request-response`, `--deploy-require-compression`; `--value-read-path auto` picks `direct` on an `s3://` store (segmented values read straight from the public store) |
| Sets defaults | `--query-worker-memory-mb 256` when unset (sized for small machines, ~1 GB); `--max-turns-per-tenant` = ¼ of `--max-concurrent-turns` (floor 1) when unset; `--inactivity-timeout-max-ms` clamped at `900000` (15 minutes). **Absent** fields of the `tenants` registry (and absent tenants) take a default limit instead of "no limit": `max-live=1000`, `max-deployments=20`, `max-schedules=50`, `max-timers=1000`, `max-streams=64`, `max-state-keys=10000`, `max-artifact-bytes=1 GiB`, `max-journal-bytes=32 MiB`, `max-payload=1 MiB`, `max-value-bytes=64 MiB`, `max-journal=10000`; a present field wins, `GET /tenant` returns the effective limits. `--key-rate 1000/1` when unset |
| Requires | `CAIRNFLOW_CLUSTER_SECRET` ≥ 32 bytes — the cluster secret never lives in the bucket (`auth/cluster-secret` is not created); `CAIRNFLOW_IDENTITY_MASTER` ≥ 32 bytes; at least one `--egress-deny-cidr` — the public IP/CIDR of the cluster's own ingress/load balancer, so clients cannot register endpoints pointing back into the cluster |
| Refuses | `--query-isolation inline` (a tenant's SQL does not run inside the node process), `--no-auth`, `--ui-cookie-secure off`, `--inactivity-timeout-ms 0` (including `CAIRNFLOW_INACTIVITY_TIMEOUT_MS=0`) — with no SDK wait cap, a turn hanging at an edge would hold a turn slot forever |

**Isolating `/query`.** The asker's SQL (`POST /query`, the console's SQL screen) runs in a
`cairn-query-worker` child process per query, not in the node process: DataFusion's planning and
optimisation run synchronously, without memory or time caps, so a cleverly written query ≤ 64 KiB could
kill a node with a view-only key. The child gets no cluster secret, environment variables, store
credentials or range handles — the node serves its scans and enforces the tenant, ranges and read
budget itself. The child is also locked down like a stranger: its own session (the node kills the whole
group), `RLIMIT_AS` = the memory cap + 1 GiB, `RLIMIT_FSIZE 0`, `RLIMIT_NOFILE 64`, no core, working
directory `/`, extra fds closed; on Linux also non-dumpable (a sibling child of another tenant cannot
open `/proc/<child>/fd`) and seccomp after initialisation — blocking the network
(`socket`/`connect`), `fork`/`execve`, signals to other processes (including the node),
`ptrace`/`process_vm_*`, opening files and changing files by path (`unlink`, `rename`, `truncate`,
`chmod`…), other processes' limits (`prlimit64`), the x32 syscall numbers on x86_64, `io_uring`,
`bpf`, namespaces. What remains: the child still
runs as the same user as the node (on macOS only rlimits and a separate session, no seccomp), can
read/write its own pipes and burn CPU until the query deadline. `--profile saas` also makes the node
non-dumpable on Linux so processes of the same user cannot read the node's environment variables (the
cluster secret, store keys); self-hosting keeps the usual debug/core dump behaviour. A child that
exceeds `--query-worker-memory-mb` (default 512) or the query deadline ⇒ the child dies, the query gets
`422 query_budget`, the node is unharmed; the node keeps 4 warm children (~9 MiB RSS each while idle,
measured on a macOS debug build). The `cairn-query-worker` binary must sit **next to** `cairnflow` (the
Docker image and the release tarball have it; building by hand: `cargo build --release --bin cairnflow
--bin cairn-query-worker`) or be pointed at with `--query-worker PATH` / `CAIRNFLOW_QUERY_WORKER`.
Self-hosting defaults to `--query-isolation auto`: a child process when the binary is found, otherwise
in the node with a warning on the `query:` line at startup; `inline` explicitly runs in the node (very
small machines, API keys only in trusted hands). On Linux the child dies with the node
(`PR_SET_PDEATHSIG`) and is the OOM killer's first candidate (`oom_score_adj` 1000). Fixed-shape
queries built by the server (`GET /invocations`) still run in the node.

`--deploy-require-identity` (usable on its own, without the profile): `POST /deployments` answers
`400` `code: "identity_required"` when the SDK's manifest does not declare `requestIdentity: "v1"` —
i.e. the SDK does not run `serve({identityKeys})`. Otherwise another tenant who knows the URL could
still call that endpoint through the cluster with a valid signature of **its own**.
`--egress-allow-cidr` is still accepted under the profile (a deliberate internal network, e.g. a
system cluster calling the provisioning service over a private network) and is printed on the startup
line; clusters serving clients should not have it.

`--deploy-https-request-response` (usable on its own): an `https://` endpoint whose SDK declares
`BIDI_STREAM` (the default of the TS `serve()`) is registered as `REQUEST_RESPONSE` when `POST
/deployments` does not say `transport` and the effective policy is `declared`. The reason: the TLS
edge of most platforms (Heroku, Railway, Render, Vercel, Netlify) speaks HTTP/1 to the app and buffers
request bodies — a BIDI turn there hangs until `inactivityTimeout` and then retries forever, with no
error saying why. The deployment is recorded with existing values (old binaries can read it):
`protocolMode: REQUEST_RESPONSE`, `transport.policy: declared`, `transport.declared: BIDI_STREAM`,
`transport.bidiProbe: "skipped: …"`. A tenant with HTTP/2 all the way to the app (Fly `h2_backend`,
Cloud Run `--use-http2`) chooses BIDI deliberately with `POST /deployments {"uri": …, "transport":
"declared"}` — the price of `REQUEST_RESPONSE` is replay bytes (the whole journal is sent again each
turn) and losing token streaming / cancellation through `AbortSignal`. `--deploy-transport auto` (or
`"transport": "auto"`) probes as before and does not apply this rule. A TS SDK declaring BIDI still
answers `REQUEST_RESPONSE` `/invoke`s: over h1 with `EndpointCore.answer`, over h2 (an edge that
forwards h2 to the app) with `openBidi` — the turn ends once the runtime has closed the request body.

`--deploy-require-compression` (usable on its own): `POST /deployments` answers `400` `code:
"compression_required"` when the deployment is fixed to `REQUEST_RESPONSE` (declared by the SDK, or
switched from BIDI by `--deploy-https-request-response` / `auto`) and the manifest does not declare
`requestCompression: "gzip"`. The reason: each RR turn sends the journal to the SDK again, that egress
is paid by the cluster operator and the payload-weighted billing unit does not count it; an
uncompressed body is the only case that leaks that cost. The TypeScript SDK declares compression by
default (`serve()` and `fetchHandler`, `compression` on by default); other SDKs must decode a request's
`Content-Encoding: gzip` and declare that field. `BIDI_STREAM` is not checked — an open stream does not
replay each turn. Only checked at registration: existing deployments are not checked again.

**Compressing REQUEST_RESPONSE bodies.** With `REQUEST_RESPONSE` each turn sends the whole journal
again, so the bytes to the SDK grow quadratically with the number of steps. When the SDK declares
`"requestCompression": "gzip"` in its manifest (TS SDK: the default, turned off with
`serve({compression: false})` / `fetchHandler(…, {compression: false})`), the deployment records
`rrCompression: "gzip"`; every RR turn sends `accept-encoding: gzip`, bodies from 8 KiB on are gzipped
at level 1 (`content-encoding: gzip`; if compression is not smaller the body is sent as is), and the
SDK may compress its response. The runtime only accepts a gzip response it invited, decompressing with
a 32 MiB cap **after decompression** (as for plain bodies). Each body gets a fresh compressor — no
compression state crosses requests or tenants. Incompressible payloads (images, random strings) gain
nothing. Security consideration: compressing before TLS a body that holds both bytes chosen by a third
party (invocation input) and a tenant's secrets (`ctx.run` results, state) opens a BREACH-style length
attack for someone who can observe the cluster → endpoint path. It is blocked at two layers: **no
compression across frames** — the runtime does a full flush at each frame boundary (window and hash
table cleared, still one standard gzip member), and the TS SDK compresses each response frame as its
own gzip member — so the bytes of one entry are never a back-reference to the bytes of another (the
context separation idea of HPACK; costs ≤ 5 % of the JSON compression ratio, 0 on delta bodies); and
**do not keep secrets in the same entry as outside input**: do not return secrets (tokens, API keys) as
a `ctx.run` result or as state — keep them in environment variables or a secret store on the endpoint
side (`ctx.run` results stay in the journal for a long time anyway). An entry that itself contains both
a stranger's input and a secret cannot be saved by separating compression. Self-hosters may turn
compression off in the SDK; the SaaS profile does not accept uncompressed RR
(`--deploy-require-compression`). Bytes on the wire (after compression) go into per-tenant metering:
`sdkBytesOut`/`sdkBytesIn` of `/metering`.

**Incremental REQUEST_RESPONSE replay** (wire v5, `protocol-v1.md` §9.3). Compression only reduces the
constant; the quadratic growth remains. When the SDK declares `"replayCache": true` (TS SDK: the
default of `serve()` with `identityKeys`, `fetchHandler(…, {replayCache: true, identityKeys})` when the
process is long-lived; the SDKs keep no cache without request identity), the
deployment records `replayCache`; from an invocation's second turn on, the runtime only sends the new
entries (`Start.replay_from = B`) together with a SHA-256 digest of the prefix `[0, B)` **recomputed on
the current journal**. The SDK compares it with its cache: on a match it splices and runs exactly as with
a full replay; on a mismatch/miss it answers `409` + `cairn-replay: miss` without running anything,
together with the part it still holds (`cairn-replay-held: k:h_k`); the runtime checks `h_k` against its
own journal and resends at once from `k`, or in full when it is wrong or absent (the attempt does not
count). The "how far does the SDK hold" hint lives only in the range term's RAM (a leader change, a
split/merge, a node reopening ⇒ the next turn is full); nothing on the bucket. Over 20 delta turns, if
the bytes sent in delta mode (resends included) are ≥ 95 % of the full bytes ⇒ the deployment stops
sending deltas for 10 minutes on that range (`cairnflow_rr_replay_auto_off_total`). Metrics:
`cairnflow_rr_replay_total{result="hit|miss|partial|full|disabled"}` (`partial` = a resend from `k`
that hit), `cairnflow_rr_replay_saved_bytes_total`, `cairnflow_rr_journal_bytes` (histogram; a journal
> 10 MiB ⇒ one `warn` per invocation — move large payloads to `ctx.artifact`). **Clients with several
replicas** only hit when an invocation's request lands on the replica that is still warm: the runtime
keeps the `Set-Cookie` (name=value pairs, capped at 4 KiB) of the responses of **that** invocation and
sends it back as `Cookie` on the next turn, to the same deployment — never to another invocation or
tenant — enough for cookie-sticky load balancers (AWS ALB, GCP, Heroku session affinity); on platforms
that route by machine id, the SDK answers `cairn-affinity: fly=<machine id>` and the runtime sends
`fly-force-instance-id`. Render and Railway have no sticky sessions at the time of writing: several
replicas there miss often, each miss costs one more HTTP round trip plus the part of the journal the old
replica lacks — still much cheaper than a full replay (measured: two alternating replicas ≈ 0.13× of
the full bytes); one replica, or affinity, costs no extra round trip. To turn it off on the node:
`--no-rr-delta-replay`.

A BIDI turn whose SDK sends no response headers within 15 s of opening the stream fails with a
retryable error starting with the code `bidi_no_response_headers` (in `lastFailure.message`), with a
hint to register again with `transport: auto` or to run REQUEST_RESPONSE — a protocol-conforming SDK
sends headers at once, so this is almost certainly an edge buffering the body. The runtime only
reports; it does not switch modes by itself.

Example (secrets from your secret manager):

```sh
CAIRNFLOW_CLUSTER_SECRET=… CAIRNFLOW_IDENTITY_MASTER=… CAIRNFLOW_ADMIN_KEY=… \
  cairnflow --profile saas --main-store s3://my-bucket/prod --host 0.0.0.0 \
    --egress-deny-cidr 203.0.113.0/24 --trust-proxy fd00::/8
```

Still missing, and what it means:

- **no TLS yet** — put TLS on a proxy in front for clients; API keys travel in clear on the wire;
- signatures block forged messages but do not encrypt — the network between nodes should still be
  private;
- a captured internal request can be replayed within 60 s (every internal request is idempotent);
- S3 credentials are the highest-level secret: whoever has them can read the cluster secret and alter
  journals.

## 9. Release binaries

`.github/workflows/release.yml` builds static (musl) binaries for linux x86_64 and aarch64 when a
`v*` tag is pushed, as a draft GitHub Release. **It has never run** — see the top of that file. Until
there is a release: build from source (section 2) or use Docker (section 5).
