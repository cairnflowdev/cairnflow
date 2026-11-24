# Compatibility

What Cairnflow promises across versions, and what it does not. Cairnflow is
pre-1.0: the rules below are the minimum we hold ourselves to; anything not
listed can change in a minor release, with a migration note.

## Version numbers

Releases are tagged `vX.Y.Z` (semver). Before 1.0:

- `0.Y.0` (minor) may change APIs, flags and defaults; every such change has a
  migration note in the release notes.
- `0.Y.Z` (patch) fixes bugs only and never changes data or the wire.

## Data on the bucket

- **The bucket format is frozen at `DATA_FORMAT` 10.** Every later release opens
  a format-10 bucket. Buckets with format 9 or older are refused (start a new
  bucket).
- **Upgrading one minor release migrates data automatically**, with no manual
  step. Changes to the bucket layout go through the migration framework
  (`crates/cairn-store/src/migrate.rs`): the bucket carries a **cluster version**
  (object `cluster-version`; absent means 0), and each release carries an ordered
  ledger of migration steps, each raising the cluster version by exactly one.
  The first upgraded node runs the cluster-level steps under a lock; range-level
  steps run lazily when a range is opened by a new binary. Every step is
  idempotent and resumes after a crash.
- **Some features are behind one-way gates** (for example durable streams,
  server-side schedules, tenant state, audit, segmented values). A new cluster
  has them on; an upgraded cluster turns each on with
  `cairn-ctl cluster enable-<gate> --apply` once every node runs the new binary.
  After a gate is on, binaries older than that gate refuse to start on the
  cluster.
- **Downgrade** is possible only until the cluster version is raised past what
  the older binary knows (`GET /admin/cluster-version` shows `requiresKnows` and
  each binary's `knows`). After that, roll back by restoring a backup taken
  before the upgrade. There are no reverse migrations.

## Rolling upgrades

A cluster may run two **adjacent** releases (N−1 and N) at the same time during a
rolling upgrade: replace nodes one at a time (`SIGTERM` for a graceful hand-off,
wait until every range has a leader again, continue). An old node keeps serving
the ranges it leads but refuses to overwrite objects written in a newer layout.
Finish the upgrade rather than leaving the cluster mixed. The step-by-step
procedure is in [`self-hosting.md`](self-hosting.md) ("Upgrading") and the
runbook for upgrades and rollbacks in [`runbooks/`](runbooks/).

Mixed-binary upgrade tests run against the previous release tag. The first
public release has no previous tag, so the guarantee applies from the second
release on.

## Wire protocol and SDKs

- The service protocol between the engine and SDK endpoints is versioned
  (currently versions 1–6; see [`protocol-v1.md`](protocol-v1.md)). An SDK
  advertises the range it speaks and the engine picks the highest common
  version per invocation, so **a newer engine works with older SDKs** and a newer
  SDK works with an older engine down to the oldest version both speak.
- The engine continues to support every wire version it has shipped within the
  same major release.
- Three SDKs ship from this repository: TypeScript (`sdk/typescript`, `@cairnflow/sdk`),
  Python (`sdk/python`, `cairnflow`, Python ≥ 3.11) and Go (`sdk/go`, module
  `github.com/cairnflowdev/cairnflow/sdk/go`, Go ≥ 1.25). All three speak versions 1–6, and each
  has a client for calling services through the HTTP ingress (`@cairnflow/sdk/client`,
  `cairnflow.connect`, the Go package `client`). Go releases of the module take tags
  `sdk/go/vX.Y.Z` (Go's rule for a module in a subdirectory); none has been tagged yet.
- The conformance suite (`conformance/`) pins the behaviour on the wire; every
  SDK runs it. A change to wire behaviour regenerates the traces and is called
  out in the release notes.
- Upgrading a deployment while invocations are in flight is safe: an invocation
  stays bound to the deployment it started on. Moving in-flight invocations to a
  new deployment is explicit (`replay-check`, then `migrate` or `restart`).

## HTTP API, CLI and metrics

- Admin and ingress HTTP routes are documented in [`admin-api.md`](admin-api.md)
  and `docs/api/openapi.json`. Removing or changing a route or a response field
  is a minor-release change with a migration note; adding is not breaking.
- CLI flags of `cairnflow`, `cairn` and `cairn-ctl`: same rule.
- Metric names and labels: same rule; the shipped dashboards and alerts in
  `deploy/` are kept in sync by tests.
