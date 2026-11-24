// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **The two-tier migration framework.**
//!
//! From `DATA_FORMAT` 9 on (frozen, D27; 9 → 10 was the query layer's one-time break,
//! and format 10 is frozen in turn) a bucket never has to be deleted to upgrade the
//! binary. Every bucket change after format 10 goes through here, by **cluster
//! version**: a number that only grows, each migration step taking the cluster up by
//! exactly one.
//!
//! # The `cluster-version` object
//!
//! A separate object, not part of `cluster.conf` (the range table is CAS-ed on every
//! split/merge; mixing the version into it would make a migration race the rebalancer
//! for the CAS). Read leniently:
//!
//! ```text
//! cairnflow-cluster-version/1
//! version=<N>                              last step completed
//! upgrading=<from>-><to>@<node>/<inc>      present ⇔ a runner holds the lock
//! min_binary=<N>                           binaries must know at least this step
//! call_prefix=on                           gate `calls/prefix` is on (absent ≡ off)
//! call_placement=on                        gate `calls/placement` is on (absent ≡ off)
//! streams=on                               gate `streams/table` is on (absent ≡ off)
//! schedules=on                             gate `schedules/table` is on (absent ≡ off)
//! key_roles=on                             gate `auth/keys-v2` is on (absent ≡ off)
//! tenant_state=on                          gate `tenants/state` is on (absent ≡ off)
//! audit=on                                 gate `audit/table` is on (absent ≡ off)
//! tenant_purge=on                          gate `tenants/purge` is on (absent ≡ off)
//! values=on                                gate `values/segmented` is on (absent ≡ off)
//! ```
//!
//! **Absent object ≡ `version=0`**: a bucket built before the framework needs no change
//! for a new binary to open it. A node that starts and sees a `version` (or
//! `min_binary`) larger than the number of steps it knows ([`KNOWS`]) **does not
//! start** ([`ensure_known`]). Each node writes the number of steps it knows into its
//! session record (`knows=`), so the operator can see which binaries the cluster is
//! mixing.
//!
//! # The migration registry
//!
//! [`MIGRATIONS`] is an ordered list of `{ to, name, run }`; `to` starts at 1, each entry
//! +1, no gaps, no duplicates (test `ledger_is_strictly_increasing_without_gaps`). Each
//! step belongs to one of the tiers — the tier lives right in [`Run`], so a step cannot
//! declare one tier and run the function of another:
//!
//! | Tier | Touches | Who runs it, when | Marked done by |
//! |---|---|---|---|
//! | [`Run::Cluster`] | objects on the bucket (`rules`, `catalog/`, `tenants`, …) | one node, taking the `upgrading` lock on `cluster-version`, at startup | `cluster-version.version` |
//! | [`Run::RangeDb`] | a range's Db **before it is opened** (manifest, WAL kind) and its owner record | the range owner, at open, before opening the Db | `data=` on `owners/range-<id>` |
//! | [`Run::Range`] | rows in a range's Db | the range owner, at open, before serving | `data=` on `owners/range-<id>` |
//! | [`Run::Gate`] | touches no data: a binary **capability** the operator turns on once the whole cluster is up | nobody runs it at startup; the enable command (`cairn-ctl cluster …`) sets `min_binary` | does not write `version` |
//!
//! Rules for every step (inspired by TiDB's upgrade steps):
//!
//! - **Idempotent.** The runner may die after the step finished but before the mark was
//!   written; next time the whole step runs again. A range-tier step must also cope with
//!   rows **already** in the new layout (a brand-new Db, a split child Db, a Db being
//!   rerun).
//! - **Never edit an old step.** If an old step has a bug, add a new step that makes up
//!   for it: a cluster that already ran the old step will not run it again.
//! - **No reverse migrations.** Going back after `version` was bumped means restoring a
//!   backup.
//! - A step receives `from` — the data version when the runner started — so it knows
//!   where it is coming from.
//! - A cluster-tier step **must not assume** the range-tier steps before it have run on
//!   every range: range-tier steps run lazily, as each range opens.
//! - Within one open, every missing [`Run::RangeDb`] step runs **before** every missing
//!   [`Run::Range`] step (the Db is opened only after the last Db-tier step). So a
//!   Db-tier step must not assume that a row-tier step **ahead of it in the registry**
//!   has run on this Db.

use {
    crate::{
        error::{Result, StoreError},
        owner::SessionId,
        writer::PartitionWriter,
    },
    futures::future::{BoxFuture, LocalBoxFuture},
    slatedb::object_store::{
        path::Path, Error as OsError, ObjectStore, ObjectStoreExt, PutMode, UpdateVersion,
    },
    std::sync::Arc,
};

/// What a cluster-tier step is handed: the cluster's object store and prefix.
#[derive(Clone)]
pub struct ClusterCtx {
    pub store: Arc<dyn ObjectStore>,
    pub prefix: Path,
}

/// A cluster-tier step: `from` is the `version` when the runner took the lock.
pub type ClusterStep = fn(from: u32, ctx: ClusterCtx) -> BoxFuture<'static, Result<()>>;

/// A range-tier step: `from` is the range's `data` at open. The step writes through the
/// range owner's writer (fenced like every write), committing once or several times —
/// every commit idempotent.
pub type RangeStep =
    for<'a> fn(from: u32, writer: &'a mut PartitionWriter) -> LocalBoxFuture<'a, Result<()>>;

/// What a range-Db-tier step is handed: the stores, the range id, and the range owner's
/// keeper — the range's Db is **not open yet** (changing the WAL kind cannot be done on
/// an open Db).
pub struct RangeDbCtx<'a> {
    pub stores: &'a crate::ObjectStores,
    pub range: u32,
    pub keeper: &'a mut crate::LeaseKeeper,
}

/// A range-Db-tier step: `from` is the range's `data` at open. The step runs under the
/// range owner's lease, before the Db is opened to serve; every owner-record write goes
/// through `keeper` (CAS, rule 7). Idempotent like every step.
pub type RangeDbStep = for<'a> fn(from: u32, ctx: RangeDbCtx<'a>) -> LocalBoxFuture<'a, Result<()>>;

/// The function of a step, with its tier.
#[derive(Clone, Copy)]
pub enum Run {
    Cluster(ClusterStep),
    /// Range tier, the part **before the Db is opened**: the Db's manifest and the owner record.
    RangeDb(RangeDbStep),
    Range(RangeStep),
    /// **Gate step**: a number in the registry for a binary capability that changes no
    /// data — a binary that carries it has `knows ≥ to`. The cluster-tier runner skips it:
    /// it does **not** raise `version` or `min_binary` at startup, so old binaries can
    /// still start during a rolling upgrade. The command that turns the capability on
    /// (`cairn-ctl cluster enable-quiet-lease`) checks that every session already knows
    /// this step before setting `min_binary = to`; it can be downgraded with the reverse
    /// command because `version` does not change.
    Gate,
}

/// The tier of a step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tier {
    Cluster,
    RangeDb,
    Range,
    Gate,
}

/// One entry of the migration registry.
#[derive(Clone, Copy)]
pub struct Migration {
    /// The cluster version after this step. The registry goes 1, 2, 3, …
    pub to: u32,
    /// Short name for logs and `GET /admin/cluster-version`.
    pub name: &'static str,
    pub run: Run,
}

impl Migration {
    pub const fn tier(&self) -> Tier {
        match self.run {
            Run::Cluster(_) => Tier::Cluster,
            Run::RangeDb(_) => Tier::RangeDb,
            Run::Range(_) => Tier::Range,
            Run::Gate => Tier::Gate,
        }
    }
}

impl std::fmt::Debug for Migration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Migration")
            .field("to", &self.to)
            .field("name", &self.name)
            .field("tier", &self.tier())
            .finish()
    }
}

/// **This binary's migration registry.** Append only; never edit or remove an entry that
/// has shipped (see the module head).
///
/// | `to` | Name | Tier | What it does |
/// |---|---|---|---|
/// | 1 | `rules/2` | cluster | rewrites `rules` from `cairnflow-rules/1` to `/2` (the `rate`, `burst` columns, the `tenant` line). A binary that only knows `/1` stops loudly when it meets `/2` |
/// | 2 | `wal/shared` | range Db | native-WAL Db to the shared WAL: drain the native WAL into SSTs, change the `wal_name` marker, then CAS `wal_kind=shared` onto the owner record. A range already on the shared WAL: nothing to do. A binary that only knows step 1 does not start on a cluster already at 2 (`min_binary`) |
/// | 3 | `lease/quiet` | gate | the binary knows passive renewal (`mode=solo`) and the `control/quiet-lease` gate ([`crate::quiet_lease`]). Does not run at startup; `cairn-ctl cluster enable-quiet-lease` sets `min_binary=3` |
/// | 4 | `calls/prefix` | gate | the binary reads outbox rows v6 (`callee_pk`) and knows this object's `call_prefix=` line. `cairn-ctl cluster enable-call-prefix` sets `min_binary=4` then `call_prefix=on`; a **newly** created cluster has it on already ([`run_cluster`]). One way: there is no off command, `lower-min-binary` does not go below 4 while the gate is on |
/// | 6 | `streams/table` | gate | the binary knows the `Streams` table (`0x18`, bulk region — durable streams, `$log`/`$trace`), the `Artifacts` table (`0x19`, hot region — rows `(id, sha) → epoch`) and this object's `streams=` line: purge deletes their rows together with the journal. A binary that knows up to 5 **skips** unknown table codes and leaks rows on purge (`cairn-engine/tests/agent_pins.rs`), so `min_binary=6` blocks it. `cairn-ctl cluster enable-streams` sets `min_binary=6` then `streams=on`; new clusters have it on already. One way: once rows are written, old binaries leak them |
/// | 5 | `calls/placement` | gate | the binary reads `placement` (field 13) of the `Call`/`OneWayCall` commands and the entry 0 marker `cairnflow-call-placement`: `spread` calls of a caller marked under the old rule. A binary that knows up to 4 **skips** that field and replays under the colocation rule ⇒ a second callee — so `min_binary=5` blocks it. The same `cairn-ctl cluster enable-call-prefix` command sets `min_binary=5` then `call_prefix=on`, `call_placement=on`; new clusters have it on already. One way, like step 4 |
/// | 7 | `schedules/table` | gate | the binary knows the `Schedules` table (`0x1A`, hot region — server-side schedules), the `ScheduleDue` index, outbox row v7 `Fire`, the `schedule` field of the `StartCall` message and this object's `schedules=` line. A binary that knows up to 6 and leads a range with schedules **fires nothing** (it does not know the table) — so `min_binary=7` blocks it (CG-13), and the API only accepts `PUT /schedules` once the gate is on. `cairn-ctl cluster enable-schedules` sets `min_binary=7` then `schedules=on`; new clusters have it on already. One way: once schedules are written, old binaries miss them |
/// | 8 | `auth/keys-v2` | gate | the binary reads the key registry `cairnflow-keys/2` (roles `view`, `provision`) and this object's `key_roles=` line. A binary that knows up to 7 decodes `/1` strictly: when it meets `/2` it stops confirming the registry is fresh and returns `503` for every stored key, cookies included — so the writer keeps `/1` until the gate is on, and `min_binary=8` blocks that binary. No `Run::Cluster`: rewriting the registry when the first new node starts would make the whole cluster `503` in the middle of a rolling upgrade. `cairn-ctl cluster enable-key-roles` sets `min_binary=8` then `key_roles=on`; new clusters have it on already. One way: once a `/2` registry is written, old binaries cannot read it |
/// | 9 | `tenants/state` | gate | the binary enforces the `state=` field of the `tenants` registry (`limited` refuses new work, `suspended` also grants no slots) and understands this object's `tenant_state=` line. A binary that knows up to 8 **skips** `state=` (an unknown flag of a leniently read registry): a suspended tenant keeps running on that node — so states are only promised after the gate, `PUT /admin/tenants/{t}/state` returns `409 tenant_state_disabled` until it is on, and `min_binary=9` blocks that binary. `cairn-ctl cluster enable-tenant-state` sets `min_binary=9` then `tenant_state=on`; new clusters have it on already. One way |
/// | 10 | `audit/table` | gate | the binary knows the `Audit` table (`0x1C`, hot region — audit rows waiting for export), the `Audit` index (`0x80`), the loop that exports rows as `audit/tenant/…` objects and this object's `audit=` line. A binary that knows up to 9 **skips** unknown table codes: audit rows stay behind with nobody exporting them while it leads — so durable audit together with the action is only promised after the gate, the engine writes no rows while the gate is off, and `min_binary=10` blocks that binary. `cairn-ctl cluster enable-audit` sets `min_binary=10` then `audit=on`; new clusters have it on already. One way |
/// | 11 | `tenants/purge` | gate | the binary understands `state=deleting|purged` of the `tenants` registry: it blocks every key, cookie, SSO and turn of a tenant being deleted, runs the logical deletion of the tenant's rows when it leads a range, and understands this object's `tenant_purge=` line. A binary that knows up to 10 reads `deleting` as `suspended` — it does not block reads, does not delete rows, and its `set_state` overwrites `state=` — so `PUT …/state {"state":"deleting"}` returns `409 tenant_purge_disabled` until it is on, and `min_binary=11` blocks that binary. `cairn-ctl cluster enable-tenant-purge` sets `min_binary=11` then `tenant_purge=on`; new clusters have it on already. One way |
/// | 12 | `values/segmented` | gate | the binary knows segmented values: the `SegmentedState` (`0x1D`) and `ValueRefs` (`0x1E`) tables, `Value.segments` in the journal, inline assembly for SDK ≤ v5, the `values/` sweep, and this object's `values=` line. A binary that knows up to 11 **skips** unknown `Value` fields — reading the tail as the whole value (silent corruption) — and unknown table codes, so the runtime only seals when the gate is on at range open, and `min_binary=12` blocks that binary. `cairn-ctl cluster enable-values` sets `min_binary=12` then `values=on`; new clusters have it on already. One way |
pub const MIGRATIONS: &[Migration] = &[
    Migration {
        to: 1,
        name: "rules/2",
        run: Run::Cluster(rules_v2),
    },
    Migration {
        to: 2,
        name: "wal/shared",
        run: Run::RangeDb(wal_shared),
    },
    Migration {
        to: QUIET_LEASE,
        name: QUIET_LEASE_NAME,
        run: Run::Gate,
    },
    Migration {
        to: CALL_PREFIX,
        name: CALL_PREFIX_NAME,
        run: Run::Gate,
    },
    Migration {
        to: CALL_PLACEMENT,
        name: CALL_PLACEMENT_NAME,
        run: Run::Gate,
    },
    Migration {
        to: STREAMS_TABLE,
        name: STREAMS_TABLE_NAME,
        run: Run::Gate,
    },
    Migration {
        to: SCHEDULES_TABLE,
        name: SCHEDULES_TABLE_NAME,
        run: Run::Gate,
    },
    Migration {
        to: KEY_ROLES,
        name: KEY_ROLES_NAME,
        run: Run::Gate,
    },
    Migration {
        to: TENANT_STATE,
        name: TENANT_STATE_NAME,
        run: Run::Gate,
    },
    Migration {
        to: AUDIT_TABLE,
        name: AUDIT_TABLE_NAME,
        run: Run::Gate,
    },
    Migration {
        to: TENANT_PURGE,
        name: TENANT_PURGE_NAME,
        run: Run::Gate,
    },
    Migration {
        to: VALUES,
        name: VALUES_NAME,
        run: Run::Gate,
    },
];

/// The gate step of quiet renewal: `knows ≥` this number ⇔ the binary knows `mode=solo`
/// and honors challenges instead of the LS-1 silence rule on that record.
pub const QUIET_LEASE: u32 = 3;

/// The name of step [`QUIET_LEASE`] in the registry.
pub const QUIET_LEASE_NAME: &str = "lease/quiet";

/// The gate step of **colocating call trees by pk prefix**: `knows ≥` this number ⇔ the
/// binary reads outbox rows v6 (the `callee_pk` field appended at the end) and
/// understands the `call_prefix=` line of `cluster-version`.
///
/// The gate is on ⇔ [`ClusterVersion::call_prefix_on`]: `call_prefix=on` **and**
/// `min_binary ≥` this number. The engine reads it at range open (a GET that happens
/// anyway — the spawner's `ensure_known`), never on the turn path. When it is on, the
/// engine marks entry 0 of the invocations it creates, and their unkeyed calls write
/// `Call` rows at v6 with a pk following the caller's prefix; binaries that know up to
/// step 3 read the outbox row version strictly, so they must not run on that cluster —
/// `min_binary` blocks them.
pub const CALL_PREFIX: u32 = 4;

/// The name of step [`CALL_PREFIX`] in the registry.
pub const CALL_PREFIX_NAME: &str = "calls/prefix";

/// The gate step of **deliberate spreading** `placement: "spread"`: `knows ≥` this number
/// ⇔ the binary reads `placement` (field 13 of the `Call`/`OneWayCall` commands) and the
/// entry 0 marker `cairnflow-call-placement`, and understands the `call_placement=` line
/// of `cluster-version`.
///
/// **Why it is a gate** even with no new rows: the journal keeps the SDK's command bytes
/// as they are, and a binary that knows up to step 4 decodes them with the old proto — it
/// **skips** `placement`. Each replay re-emits the `Task::Invoke` of a call still waiting
/// for its id and overwrites the outbox row; the old binary writes it under the
/// colocation rule, i.e. a different pk, i.e. a second callee if the `StartCall` under
/// the old rule has already gone out. Nothing in the old binary makes it stop instead of
/// going wrong, so it must not run on a cluster with marked callers: `min_binary` blocks
/// it.
///
/// The gate is on ⇔ [`ClusterVersion::call_placement_on`]. The engine reads it at range
/// open like [`CALL_PREFIX`] and marks entry 0 of the invocations it creates; the
/// `spread` rule belongs to the **caller** — an unmarked caller ignores `spread` forever.
pub const CALL_PLACEMENT: u32 = 5;

/// The name of step [`CALL_PLACEMENT`] in the registry.
pub const CALL_PLACEMENT_NAME: &str = "calls/placement";

/// The gate step of the **`Streams` table** (`0x18`): `knows ≥` this number ⇔ the binary
/// knows the chunk rows of durable streams (`$log`, `$trace`,
/// `ctx.stream(name, {durable: true})`) — reading them, purging them with the journal —
/// and the `streams=` line of `cluster-version`.
///
/// **Why a gate:** a binary that does not know the table code skips its rows, and its
/// purge **leaks** those rows (pinned by `cairn-engine/tests/agent_pins.rs`). The leak
/// loses no data, but it is customer data (logs may contain prompts) left behind after
/// the invocation was purged — `min_binary` blocks that binary.
///
/// The gate is on ⇔ [`ClusterVersion::streams_on`]. The engine reads it at range open
/// like [`CALL_PREFIX`]; when it is off, durable streams only live in RAM like temporary
/// streams (no record carries `durable: true`).
pub const STREAMS_TABLE: u32 = 6;

/// The name of step [`STREAMS_TABLE`] in the registry.
pub const STREAMS_TABLE_NAME: &str = "streams/table";

/// The gate step of **server-side schedules**: `knows ≥` this number ⇔ the binary knows
/// the `Schedules` table (`0x1A`), the `ScheduleDue` index, outbox row v7 `Fire`, the
/// `schedule` field of `StartCall` and the `schedules=` line of `cluster-version`.
///
/// **Why a gate:** a binary that does not know the table and leads a range with
/// schedules fires no due time — the schedules die silently, with no error to see
/// (CG-13). The rows themselves do not leak (schedules belong to no invocation), but a
/// missed due time is lost customer work — `min_binary` blocks that binary.
///
/// The gate is on ⇔ [`ClusterVersion::schedules_on`]. The engine reads it at range open
/// like [`STREAMS_TABLE`]; when it is off the API refuses `PUT /schedules` and the
/// firing loop does not run.
pub const SCHEDULES_TABLE: u32 = 7;

/// The name of step [`SCHEDULES_TABLE`] in the registry.
pub const SCHEDULES_TABLE_NAME: &str = "schedules/table";

/// The gate step of **new roles in the key registry**: `knows ≥` this number ⇔ the binary
/// decodes the `cairnflow-keys/2` registry (roles `view`, `provision`) and understands the
/// `key_roles=` line of `cluster-version`.
///
/// **Why a gate, not `Run::Cluster`:** the decoder of a binary that knows up to 7 reads
/// `cairnflow-keys/1` strictly — one unknown role breaks the whole registry. On meeting a
/// `/2` registry, that node stops confirming freshness and returns `503` for every stored
/// key (cookies included). Rewriting the registry when the first new node starts
/// (`Run::Cluster`, the `rules/2` precedent) would mean a cluster-wide `503` in the middle
/// of a rolling upgrade. So the writer keeps `/1` until the gate is on, and creating a
/// key with a new role before then returns `409`.
///
/// The gate is on ⇔ [`ClusterVersion::key_roles_on`]. The key-registry writer reads it
/// right before each write (an admin action, rare) — never on the request path.
pub const KEY_ROLES: u32 = 8;

/// The name of step [`KEY_ROLES`] in the registry.
pub const KEY_ROLES_NAME: &str = "auth/keys-v2";

/// The gate step of **tenant state**: `knows ≥` this number ⇔ the binary enforces
/// `state=` of the `tenants` registry and understands the `tenant_state=` line of
/// `cluster-version`.
///
/// **Why a gate:** the `tenants` registry is read leniently — a binary that knows up to
/// 8 treats `state=` as an unknown flag and skips it, so a suspended tenant keeps running
/// on that node for the whole rolling upgrade. There is nothing to run at startup
/// (`Run::Gate`): whoever changes the state reads the gate before each write, and before
/// the gate gets `409`.
///
/// The gate is on ⇔ [`ClusterVersion::tenant_state_on`].
pub const TENANT_STATE: u32 = 9;

/// The name of step [`TENANT_STATE`] in the registry.
pub const TENANT_STATE_NAME: &str = "tenants/state";

/// The gate step of the **in-range audit log**: `knows ≥` this number ⇔ the binary knows
/// the `Audit` table (`0x1C`), its index and the loop that exports rows as objects. An
/// old binary skips unknown table codes — the rows stay behind with nobody exporting
/// them — so the engine only writes rows when the gate is on at range open.
///
/// The gate is on ⇔ [`ClusterVersion::audit_on`].
pub const AUDIT_TABLE: u32 = 10;

/// The name of step [`AUDIT_TABLE`] in the registry.
pub const AUDIT_TABLE_NAME: &str = "audit/table";

/// The gate step of **tenant purge**: `knows ≥` this number ⇔ the binary understands
/// `state=deleting|purged` of the `tenants` registry (blocking every key, cookie, SSO and
/// turn of the tenant), runs the logical deletion of the tenant's rows when it leads a
/// range, and understands the `tenant_purge=` line of this object.
///
/// **Why a gate:** a binary that knows up to 10 reads `deleting` as `suspended` — it
/// does not block the tenant's keys or reads (the data is still readable through that
/// node during the 7 retention days), does not delete rows when it leads (the purge gets
/// stuck), and its `set_state` overwrites `state=` (silently removing `deleting`). Before
/// the gate `PUT /admin/tenants/{t}/state {"state":"deleting"}` returns `409
/// tenant_purge_disabled`.
///
/// The gate is on ⇔ [`ClusterVersion::tenant_purge_on`].
pub const TENANT_PURGE: u32 = 11;

/// The name of step [`TENANT_PURGE`] in the registry.
pub const TENANT_PURGE_NAME: &str = "tenants/purge";

/// The gate step of **segmented values**: `knows ≥` this number ⇔ the binary reads
/// `Value.segments` (assembling for SDK ≤ v5), knows the `SegmentedState`/`ValueRefs`
/// tables, runs the `values/` sweep.
///
/// **Why a gate:** a binary that knows up to 11 skips unknown `Value` fields — reading
/// the tail as the whole value, silent corruption — and skips unknown table codes
/// (segmented state becomes "absent"). The engine only seals when the gate is on at range
/// open.
///
/// The gate is on ⇔ [`ClusterVersion::values_on`].
pub const VALUES: u32 = 12;

/// The name of step [`VALUES`] in the registry.
pub const VALUES_NAME: &str = "values/segmented";

/// Step 1: the rule book to `/2` — see [`crate::RuleStore::rewrite_as_v2`].
/// Idempotent: no rule book, or already at `/2`, writes nothing.
fn rules_v2(_: u32, ctx: ClusterCtx) -> BoxFuture<'static, Result<()>> {
    Box::pin(async move {
        crate::RuleStore::new(ctx.store, &ctx.prefix)
            .rewrite_as_v2()
            .await
    })
}

/// Step 2: native-WAL Db to the shared WAL — the Db part in
/// [`crate::ObjectStores::native_wal_to_shared`], then the owner record
/// ([`crate::LeaseKeeper::set_wal_kind`]). This order is the safe order: dying after the
/// marker change but before the record change means the next run sees the shared-WAL
/// marker and only has the record left to write; nobody opens the Db with the shared WAL
/// while the record still says `slatedb` (the WAL kind of an open is taken from the
/// record), so the Db's shared WAL has no frames yet. Idempotent: record already
/// `shared` ⇒ nothing to do.
fn wal_shared(_: u32, ctx: RangeDbCtx<'_>) -> LocalBoxFuture<'_, Result<()>> {
    Box::pin(async move {
        let RangeDbCtx {
            stores,
            range,
            keeper,
        } = ctx;
        match &keeper.wal().kind {
            crate::WalKind::Shared => return Ok(()),
            crate::WalKind::SlateDb => {}
            crate::WalKind::Unknown(kind) => {
                return Err(StoreError::UnknownWalKind {
                    range,
                    kind: kind.clone(),
                })
            }
        }
        stores.native_wal_to_shared(range).await?;
        crate::failpoint!("migrate::wal_shared::marked", err);
        keeper.set_wal_kind(crate::WalKind::Shared).await?;
        tracing::info!(range, "range moved to the shared WAL");
        Ok(())
    })
}

/// The last step of a registry (0 if empty).
pub const fn latest(migrations: &[Migration]) -> u32 {
    match migrations.last() {
        Some(m) => m.to,
        None => 0,
    }
}

/// The last migration step this binary knows how to run — gate steps ([`Run::Gate`])
/// included. Written into the session record (`knows=`) and compared with `min_binary`.
pub const KNOWS: u32 = latest(MIGRATIONS);

/// The last **data** step of a registry (ignoring gate steps at the tail): the cluster
/// version the cluster-tier runner brings the bucket to. 0 if there is no data step.
pub const fn latest_data(migrations: &[Migration]) -> u32 {
    let mut i = migrations.len();
    while i > 0 {
        i -= 1;
        if !matches!(migrations[i].run, Run::Gate) {
            return migrations[i].to;
        }
    }
    0
}

/// The cluster version (`cluster-version.version`) this binary brings the bucket to at
/// startup — the last data step of [`MIGRATIONS`]. Smaller than [`KNOWS`] when the
/// registry has gate steps at its tail.
pub const VERSION: u32 = latest_data(MIGRATIONS);

/// A valid registry: `to` goes 1, 2, 3, … — growing by one, no gaps, no duplicates.
pub fn check_ledger(migrations: &[Migration]) -> std::result::Result<(), String> {
    for (i, m) in migrations.iter().enumerate() {
        let want = i as u32 + 1;
        if m.to != want {
            return Err(format!(
                "entry {i} ({}) has to={}, must be {want}",
                m.name, m.to
            ));
        }
    }
    Ok(())
}

pub(crate) const HEADER: &str = "cairnflow-cluster-version/1";

/// The lock of the cluster-tier migration runner: taking the cluster from `from` to `to`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Upgrading {
    pub from: u32,
    pub to: u32,
    /// The runner's session. If that session dies, someone else takes over.
    pub by: SessionId,
}

impl Upgrading {
    /// `<from>-><to>@<node>/<inc>`.
    fn parse(s: &str) -> Option<Upgrading> {
        let (span, by) = s.split_once('@')?;
        let (from, to) = span.split_once("->")?;
        Some(Upgrading {
            from: from.parse().ok()?,
            to: to.parse().ok()?,
            by: SessionId::parse(by)?,
        })
    }
}

impl std::fmt::Display for Upgrading {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}->{}@{}", self.from, self.to, self.by)
    }
}

/// The content of `cluster-version`. `Default` is a bucket that does not have this object yet.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClusterVersion {
    pub version: u32,
    pub upgrading: Option<Upgrading>,
    pub min_binary: u32,
    /// The `call_prefix=on` line (step [`CALL_PREFIX`]). Meaningful only together with
    /// `min_binary` — read it through [`Self::call_prefix_on`].
    pub call_prefix: bool,
    /// The `call_placement=on` line (step [`CALL_PLACEMENT`]). Read it through
    /// [`Self::call_placement_on`].
    pub call_placement: bool,
    /// The `streams=on` line (step [`STREAMS_TABLE`]). Read it through
    /// [`Self::streams_on`].
    pub streams: bool,
    /// The `schedules=on` line (step [`SCHEDULES_TABLE`]). Read it through
    /// [`Self::schedules_on`].
    pub schedules: bool,
    /// The `key_roles=on` line (step [`KEY_ROLES`]). Read it through
    /// [`Self::key_roles_on`].
    pub key_roles: bool,
    /// The `tenant_state=on` line (step [`TENANT_STATE`]). Read it through
    /// [`Self::tenant_state_on`].
    pub tenant_state: bool,
    /// The `audit=on` line (step [`AUDIT_TABLE`]). Read it through [`Self::audit_on`].
    pub audit: bool,
    /// The `tenant_purge=on` line (step [`TENANT_PURGE`]). Read it through
    /// [`Self::tenant_purge_on`].
    pub tenant_purge: bool,
    /// The `values=on` line (step [`VALUES`]). Read it through [`Self::values_on`].
    pub values: bool,
    /// Unknown lines (added by a newer binary) — written back verbatim, like
    /// `cluster.conf`.
    pub extra: Vec<String>,
}

impl ClusterVersion {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = format!("{HEADER}\nversion={}\n", self.version);
        if let Some(u) = &self.upgrading {
            out.push_str(&format!("upgrading={u}\n"));
        }
        if self.min_binary > 0 {
            out.push_str(&format!("min_binary={}\n", self.min_binary));
        }
        if self.call_prefix {
            out.push_str("call_prefix=on\n");
        }
        if self.call_placement {
            out.push_str("call_placement=on\n");
        }
        if self.streams {
            out.push_str("streams=on\n");
        }
        if self.schedules {
            out.push_str("schedules=on\n");
        }
        if self.key_roles {
            out.push_str("key_roles=on\n");
        }
        if self.tenant_state {
            out.push_str("tenant_state=on\n");
        }
        if self.audit {
            out.push_str("audit=on\n");
        }
        if self.tenant_purge {
            out.push_str("tenant_purge=on\n");
        }
        if self.values {
            out.push_str("values=on\n");
        }
        for l in &self.extra {
            out.push_str(l);
            out.push('\n');
        }
        out.into_bytes()
    }

    /// Lenient read: unknown lines are kept, but a **known** line that is corrupt refuses
    /// the whole object — a misread `version` or `upgrading` means rerunning or skipping
    /// a migration step.
    pub fn decode(raw: &[u8]) -> Result<ClusterVersion> {
        let bad = |reason: String| StoreError::BadClusterConfig {
            reason: format!("cluster-version: {reason}"),
        };
        let text = std::str::from_utf8(raw).map_err(|_| bad("not UTF-8".into()))?;
        let mut lines = text.lines();
        let header = lines.next().unwrap_or_default();
        if header != HEADER {
            if let Some(e) = crate::format::newer_header("cluster-version", HEADER, header) {
                return Err(e);
            }
            return Err(bad(format!("header {header:?}, expected {HEADER:?}")));
        }
        let mut out = ClusterVersion::default();
        let mut has_version = false;
        for line in lines.filter(|l| !l.is_empty()) {
            match line.split_once('=') {
                Some(("version", v)) => {
                    out.version = v.parse().map_err(|_| bad(format!("bad version: {v:?}")))?;
                    has_version = true;
                }
                Some(("upgrading", v)) => {
                    out.upgrading = Some(
                        Upgrading::parse(v).ok_or_else(|| bad(format!("bad upgrading: {v:?}")))?,
                    )
                }
                Some(("min_binary", v)) => {
                    out.min_binary = v
                        .parse()
                        .map_err(|_| bad(format!("bad min_binary: {v:?}")))?
                }
                Some(("call_prefix", v)) => {
                    out.call_prefix = match v {
                        "on" => true,
                        "off" => false,
                        _ => return Err(bad(format!("bad call_prefix: {v:?}"))),
                    }
                }
                Some(("call_placement", v)) => {
                    out.call_placement = match v {
                        "on" => true,
                        "off" => false,
                        _ => return Err(bad(format!("bad call_placement: {v:?}"))),
                    }
                }
                Some(("streams", v)) => {
                    out.streams = match v {
                        "on" => true,
                        "off" => false,
                        _ => return Err(bad(format!("bad streams: {v:?}"))),
                    }
                }
                Some(("schedules", v)) => {
                    out.schedules = match v {
                        "on" => true,
                        "off" => false,
                        _ => return Err(bad(format!("bad schedules: {v:?}"))),
                    }
                }
                Some(("key_roles", v)) => {
                    out.key_roles = match v {
                        "on" => true,
                        "off" => false,
                        _ => return Err(bad(format!("bad key_roles: {v:?}"))),
                    }
                }
                Some(("tenant_state", v)) => {
                    out.tenant_state = match v {
                        "on" => true,
                        "off" => false,
                        _ => return Err(bad(format!("bad tenant_state: {v:?}"))),
                    }
                }
                Some(("audit", v)) => {
                    out.audit = match v {
                        "on" => true,
                        "off" => false,
                        _ => return Err(bad(format!("bad audit: {v:?}"))),
                    }
                }
                Some(("tenant_purge", v)) => {
                    out.tenant_purge = match v {
                        "on" => true,
                        "off" => false,
                        _ => return Err(bad(format!("bad tenant_purge: {v:?}"))),
                    }
                }
                Some(("values", v)) => {
                    out.values = match v {
                        "on" => true,
                        "off" => false,
                        _ => return Err(bad(format!("bad values: {v:?}"))),
                    }
                }
                _ => out.extra.push(line.to_string()),
            }
        }
        if !has_version {
            return Err(bad("missing version".into()));
        }
        Ok(out)
    }

    /// The `calls/prefix` gate is on: the `call_prefix=on` line **and** `min_binary`
    /// already blocks every binary that does not know step [`CALL_PREFIX`].
    ///
    /// Two conditions, not one: with `call_prefix=on` but a low `min_binary` (an old
    /// `cairn-ctl` lowered it, or someone edited it by hand) old binaries can start —
    /// the engine must **not** write v6 rows that such a binary cannot read.
    pub fn call_prefix_on(&self) -> bool {
        self.call_prefix && self.min_binary >= CALL_PREFIX
    }

    /// The `calls/placement` gate is on: the `call_placement=on` line **and** `min_binary`
    /// already blocks every binary that does not know step [`CALL_PLACEMENT`] — the same
    /// two-condition reason as [`Self::call_prefix_on`]: a binary that knows up to 4 skips
    /// `placement`, so the engine must not mark any caller while such a binary can still
    /// start.
    pub fn call_placement_on(&self) -> bool {
        self.call_placement && self.min_binary >= CALL_PLACEMENT
    }

    /// The `streams/table` gate is on: the `streams=on` line **and** `min_binary` already
    /// blocks every binary that does not know step [`STREAMS_TABLE`] — such a binary
    /// leaks `Streams` rows on purge, so the engine must not write any row while it can
    /// still start.
    pub fn streams_on(&self) -> bool {
        self.streams && self.min_binary >= STREAMS_TABLE
    }

    /// The `schedules/table` gate is on: the `schedules=on` line **and** `min_binary`
    /// already blocks every binary that does not know step [`SCHEDULES_TABLE`] — such a
    /// binary leading a range with schedules fires nothing.
    pub fn schedules_on(&self) -> bool {
        self.schedules && self.min_binary >= SCHEDULES_TABLE
    }

    /// The `auth/keys-v2` gate is on: the `key_roles=on` line **and** `min_binary` already
    /// blocks every binary that does not know step [`KEY_ROLES`] — such a binary fails to
    /// read a `/2` registry and closes every stored key, so the writer must not write `/2`
    /// while it can still start.
    pub fn key_roles_on(&self) -> bool {
        self.key_roles && self.min_binary >= KEY_ROLES
    }

    /// The `tenants/state` gate is on: the `tenant_state=on` line **and** `min_binary`
    /// already blocks every binary that does not know step [`TENANT_STATE`] — such a
    /// binary skips `state=`, so states must not be promised while it can still start.
    pub fn tenant_state_on(&self) -> bool {
        self.tenant_state && self.min_binary >= TENANT_STATE
    }

    /// The `audit/table` gate is on: the `audit=on` line **and** `min_binary` already
    /// blocks every binary that does not know step [`AUDIT_TABLE`].
    pub fn audit_on(&self) -> bool {
        self.audit && self.min_binary >= AUDIT_TABLE
    }

    /// The `tenants/purge` gate is on: the `tenant_purge=on` line **and** `min_binary`
    /// already blocks every binary that does not know step [`TENANT_PURGE`] — such a
    /// binary reads `deleting` as `suspended`, does not block reads and does not delete rows.
    pub fn tenant_purge_on(&self) -> bool {
        self.tenant_purge && self.min_binary >= TENANT_PURGE
    }

    /// The `values/segmented` gate is on: the `values=on` line **and** `min_binary`
    /// already blocks every binary that does not know step [`VALUES`] — such a binary
    /// reads the tail as the whole value.
    pub fn values_on(&self) -> bool {
        self.values && self.min_binary >= VALUES
    }
}

pub(crate) fn version_path(prefix: &Path) -> Path {
    if prefix.as_ref().is_empty() {
        Path::from("cluster-version")
    } else {
        Path::from(format!("{}/cluster-version", prefix.as_ref()))
    }
}

fn io(e: OsError) -> StoreError {
    StoreError::Io(e.to_string())
}

/// Read `cluster-version` together with the object version to CAS against. `Ok(None)` =
/// not there yet (≡ `version=0`).
pub async fn read_cluster_version(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
) -> Result<Option<(ClusterVersion, UpdateVersion)>> {
    let got = match store.get(&version_path(prefix)).await {
        Ok(g) => g,
        Err(OsError::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(io(e)),
    };
    let version = UpdateVersion {
        e_tag: got.meta.e_tag.clone(),
        version: got.meta.version.clone(),
    };
    let raw = got.bytes().await.map_err(io)?;
    Ok(Some((ClusterVersion::decode(&raw)?, version)))
}

/// Write `cluster-version` with a CAS on `expected` (`None` ⇒ create).
/// `Ok(None)` = the CAS missed: someone changed the object before us.
///
/// An ambiguous result is resolved by reading back (rule 7): every version the runner
/// writes carries the lock `upgrading=…@<session>` or is the unlock right after its own
/// locked version, so "the object holds exactly what we meant to write" means it is our
/// write. A store that cannot do CAS (`LocalFileSystem`, the `--data-dir` path):
/// overwrite — missing CAS already blocks forming a cluster (D12), there is only one
/// process on the bucket.
pub async fn write_cluster_version(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    cv: &ClusterVersion,
    expected: Option<UpdateVersion>,
) -> Result<Option<UpdateVersion>> {
    let mode = match expected {
        Some(v) => PutMode::Update(v),
        None => PutMode::Create,
    };
    match crate::cas::put_resolving_or_overwrite(
        store,
        &version_path(prefix),
        cv.encode().into(),
        mode,
    )
    .await
    {
        Ok(crate::cas::Cas::Written(v)) => Ok(Some(v)),
        Ok(crate::cas::Cas::Lost) => Ok(None),
        Err(e) => Err(io(e)),
    }
}

/// The lock of the cluster-tier migration runner, if there is one. While the lock is
/// present, split/merge does not start and the retired-Db GC loop skips its turn — the
/// way TiDB pauses DDL during an upgrade.
pub async fn upgrading(store: &Arc<dyn ObjectStore>, prefix: &Path) -> Result<Option<Upgrading>> {
    Ok(read_cluster_version(store, prefix)
        .await?
        .and_then(|(cv, _)| cv.upgrading))
}

/// Whether the cluster is at a version that a binary knowing `knows` steps can run on.
/// `version` or `min_binary` larger than `knows` ⇒ [`StoreError::NewerFormat`]: a newer
/// binary has upgraded the cluster, this binary must not run on it.
pub fn check_known(cv: &ClusterVersion, knows: u32) -> Result<()> {
    if cv.version > knows || cv.min_binary > knows {
        return Err(StoreError::NewerFormat {
            object: "cluster-version".into(),
            header: format!(
                "version={} min_binary={} (binary knows up to {knows})",
                cv.version, cv.min_binary
            ),
        });
    }
    Ok(())
}

/// Read `cluster-version` then [`check_known`]. Returns the current version (absent ≡ 0).
pub async fn ensure_known(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    knows: u32,
) -> Result<ClusterVersion> {
    let cv = read_cluster_version(store, prefix)
        .await?
        .map(|(cv, _)| cv)
        .unwrap_or_default();
    check_known(&cv, knows)?;
    Ok(cv)
}

/// Every **live** session on the bucket with the number of migration steps its binary
/// knows (`knows=`; `None` = a binary from before the migration framework) — for `GET
/// /admin/cluster-version`: the operator sees which binaries the cluster is mixing
/// before upgrading further. One LIST + one GET per session; only for the admin API.
///
/// The third element is the record's age (ms since the last write, by `last_modified`):
/// a node that was `kill -9`ed leaves a `live` record until someone marks it expired —
/// an age larger than a few TTLs is the sign of that.
pub async fn live_binaries(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
) -> Result<Vec<(SessionId, Option<u32>, u64)>> {
    use futures::TryStreamExt;
    let dir = if prefix.as_ref().is_empty() {
        "sessions".to_string()
    } else {
        format!("{}/sessions", prefix.as_ref())
    };
    let listed: Vec<_> = store
        .list(Some(&Path::from(dir.clone())))
        .try_collect()
        .await
        .map_err(io)?;
    let mut out = Vec::new();
    for meta in listed {
        let Some(id) = meta
            .location
            .as_ref()
            .strip_prefix(&format!("{dir}/"))
            .and_then(SessionId::parse)
        else {
            continue;
        };
        let got = match store.get(&meta.location).await {
            Ok(g) => g,
            Err(OsError::NotFound { .. }) => continue,
            Err(e) => return Err(io(e)),
        };
        let modified = got.meta.last_modified.timestamp_millis().max(0) as u64;
        let raw = got.bytes().await.map_err(io)?;
        if let Ok(rec) = crate::SessionRecord::decode(&raw) {
            if rec.state == crate::SessionState::Live {
                let now = crate::entropy::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                out.push((id, rec.knows, now.saturating_sub(modified)));
            }
        }
    }
    out.sort_by(|a, b| (&a.0.node, a.0.inc).cmp(&(&b.0.node, b.0.inc)));
    Ok(out)
}

/// The cluster-tier migration runner: the identity written into `upgrading=`, and how to
/// tell whether the current lock holder has died.
pub enum Runner<'a> {
    /// A cluster node: the identity is the live session incarnation; the lock holder is
    /// dead when its session dies (`SessionTracker`, or a record that has ended / is
    /// gone).
    Session(&'a crate::Session),
    /// A runner with no session: anyone else's lock counts as belonging to an earlier run
    /// that died. No node runs like this any more (a node without `--cluster` is a
    /// one-node cluster, with a session) — it remains for the migration framework's tests,
    /// which build no session.
    Alone { node: String },
}

/// **The cluster-tier runner.** Brings `cluster-version` up to the last step of
/// `migrations`, running every missing [`Run::Cluster`] step; returns the final version.
///
/// Inspired by TiDB's upgrade runner: one runner, taken with a lock, **reading again
/// after taking it**, every step idempotent.
///
/// 1. Read `cluster-version` (absent ≡ 0). A version larger than the registry ⇒ refuse
///    ([`check_known`]). Already there and nobody holds the lock ⇒ done, write nothing.
/// 2. Someone else's `upgrading` lock whose session is **still alive** ⇒ wait (`poll`),
///    read again. Dead ⇒ take over.
/// 3. CAS `upgrading=<from>-><target>@<us>`, then **read again**: the lock must be ours.
///    Missed ⇒ back to 1. When this CAS **creates** the object on a bucket that has
///    never had a cluster ⇒ it also carries `call_prefix=on` and `min_binary ≥`
///    [`CALL_PREFIX`] (a new cluster has the colocation gate on from the start) — if the
///    registry knows that step; if the registry knows [`CALL_PLACEMENT`] then also
///    `call_placement=on`, `min_binary ≥` 5.
/// 4. Run each missing step in order. After each step — range-tier steps included, which
///    themselves run lazily as each range opens — CAS `version=<to>`, `min_binary=<to>`,
///    keeping the lock: dying midway means the next runner reruns only **one** step. The
///    CAS misses (someone took the lock thinking we died) ⇒ back to 1.
/// 5. CAS to remove `upgrading`.
///
/// A step returns an error ⇒ return the error at once, the lock stays: the node does not
/// start, and when its session dies someone else (or itself, on the next run) takes over.
pub async fn run_cluster(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    runner: Runner<'_>,
    migrations: &[Migration],
    poll: std::time::Duration,
) -> Result<ClusterVersion> {
    run_cluster_with(
        store,
        prefix,
        runner,
        migrations,
        poll,
        &NewCluster::default(),
    )
    .await
}

/// The choices of whoever builds a **new bucket** — only in effect when the cluster-tier
/// runner sees a truly empty bucket ([`fresh_bucket`]); a bucket that already has a
/// cluster ignores them.
#[derive(Clone, Default)]
pub struct NewCluster {
    /// The quiet-renewal gate written up front (`--quiet-lease`, delegation by default).
    pub quiet_lease: crate::quiet_lease::Bootstrap,
    /// The read client for the conditional-GET test (G18, [`crate::store_probe`])
    /// **before** writing the gate; `None` = no test (`memory://`, `file://`).
    /// A store that answers wrongly ⇒ the gate is not written.
    pub probe_reader: Option<Arc<dyn ObjectStore>>,
    /// Other objects the builder creates at the same time (one object per hook, `Create`
    /// — rule 7 on the hook's side; already there means leave it). `cairn-server` attaches
    /// `control/registry-gen` generation 0.
    pub on_fresh: Vec<FreshHook>,
}

/// One object the new-bucket builder creates ([`NewCluster::on_fresh`]). It runs under
/// the `upgrading` lock and may run again (the builder dies and the next one takes the
/// lock), so it must be idempotent.
pub type FreshHook = Arc<dyn Fn() -> BoxFuture<'static, Result<()>> + Send + Sync + 'static>;

/// Like [`run_cluster`], with the new-bucket builder's choices.
pub async fn run_cluster_with(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    runner: Runner<'_>,
    migrations: &[Migration],
    poll: std::time::Duration,
    new: &NewCluster,
) -> Result<ClusterVersion> {
    check_ledger(migrations).map_err(|reason| StoreError::BadClusterConfig {
        reason: format!("corrupt migration registry: {reason}"),
    })?;
    // Gate steps at the tail of the registry are not the runner's job: the target is the
    // last data step; `check_known`, though, compares against every step the binary
    // knows, gates included (`min_binary` set by a gate's enable command can be larger
    // than `version`).
    let target = latest_data(migrations);
    let knows = latest(migrations);
    let me = match &runner {
        Runner::Session(s) => s.current().ok_or(StoreError::LeaseLost)?,
        Runner::Alone { node } => SessionId {
            node: node.clone(),
            inc: 0,
        },
    };
    let ctx = ClusterCtx {
        store: Arc::clone(store),
        prefix: prefix.clone(),
    };
    let mut waiting_for = None;
    'outer: loop {
        let (cv, tag) = match read_cluster_version(store, prefix).await? {
            Some((cv, tag)) => (cv, Some(tag)),
            None => (ClusterVersion::default(), None),
        };
        check_known(&cv, knows)?;
        match &cv.upgrading {
            None if cv.version >= target => return Ok(cv),
            Some(u) if u.by != me && !holder_is_dead(&runner, &u.by).await? => {
                if waiting_for.as_ref() != Some(&u.by) {
                    tracing::info!(holder = %u, "another node is running the cluster-tier migration — waiting");
                    waiting_for = Some(u.by.clone());
                }
                crate::pace::poll_gap(poll).await;
                continue;
            }
            Some(u) if u.by != me => {
                tracing::warn!(holder = %u, "the previous migration runner died — taking over");
            }
            _ => {}
        }
        // ── 3. Take the lock, then read again.
        let mut locked = ClusterVersion {
            upgrading: Some(Upgrading {
                from: cv.version,
                to: target,
                by: me.clone(),
            }),
            ..cv.clone()
        };
        // A **new** cluster has the colocation gate on from the start: we are the one
        // creating `cluster-version` on a bucket that has never had a cluster. Write it in
        // the same CAS that takes the lock — any old binary that reads the object after
        // this meets `min_binary` and does not start; so does an old binary waiting for
        // the lock when it reads again.
        let call_gate = knows_gate(migrations, CALL_PREFIX, CALL_PREFIX_NAME);
        let quiet_gate = wants_quiet_gate(migrations, new);
        if tag.is_none() && (call_gate || quiet_gate) && fresh_bucket(store, prefix).await? {
            if call_gate {
                locked.call_prefix = true;
                locked.min_binary = locked.min_binary.max(CALL_PREFIX);
                // Deliberate spreading comes along: a new cluster has no old binary to
                // block.
                if knows_gate(migrations, CALL_PLACEMENT, CALL_PLACEMENT_NAME) {
                    locked.call_placement = true;
                    locked.min_binary = locked.min_binary.max(CALL_PLACEMENT);
                }
            }
            // The quiet-renewal gate (a deliberate decision): a binary that does not know
            // the `solo`/delegation rules must not start on a cluster whose gate is about
            // to go on — `min_binary` goes up in this same create, before the gate is
            // written (`bootstrap_fresh`).
            if quiet_gate {
                locked.min_binary = locked.min_binary.max(QUIET_LEASE);
            }
            // The durable-streams table: a new cluster has no binary that leaks its
            // rows.
            if knows_gate(migrations, STREAMS_TABLE, STREAMS_TABLE_NAME) {
                locked.streams = true;
                locked.min_binary = locked.min_binary.max(STREAMS_TABLE);
            }
            // Server-side schedules: a new cluster has no binary that misses their due
            // times.
            if knows_gate(migrations, SCHEDULES_TABLE, SCHEDULES_TABLE_NAME) {
                locked.schedules = true;
                locked.min_binary = locked.min_binary.max(SCHEDULES_TABLE);
            }
            // The `/2` key registry: a new cluster has no binary that misreads it.
            if knows_gate(migrations, KEY_ROLES, KEY_ROLES_NAME) {
                locked.key_roles = true;
                locked.min_binary = locked.min_binary.max(KEY_ROLES);
            }
            // Tenant state: a new cluster has no binary that skips `state=`.
            if knows_gate(migrations, TENANT_STATE, TENANT_STATE_NAME) {
                locked.tenant_state = true;
                locked.min_binary = locked.min_binary.max(TENANT_STATE);
            }
            // In-range audit: a new cluster has no binary that skips the `Audit` table.
            if knows_gate(migrations, AUDIT_TABLE, AUDIT_TABLE_NAME) {
                locked.audit = true;
                locked.min_binary = locked.min_binary.max(AUDIT_TABLE);
            }
            // Tenant purge: a new cluster has no binary that reads `deleting` as
            // `suspended`.
            if knows_gate(migrations, TENANT_PURGE, TENANT_PURGE_NAME) {
                locked.tenant_purge = true;
                locked.min_binary = locked.min_binary.max(TENANT_PURGE);
            }
            // Segmented values: a new cluster has no binary that reads the tail as the
            // whole value.
            if knows_gate(migrations, VALUES, VALUES_NAME) {
                locked.values = true;
                locked.min_binary = locked.min_binary.max(VALUES);
            }
        }
        if write_cluster_version(store, prefix, &locked, tag)
            .await?
            .is_none()
        {
            continue;
        }
        crate::failpoint!("migrate::cluster::locked", err);
        let Some((mut cur, mut tag)) = read_cluster_version(store, prefix).await? else {
            continue;
        };
        let Some(from) = cur
            .upgrading
            .as_ref()
            .filter(|u| u.by == me)
            .map(|_| cur.version)
        else {
            continue;
        };
        // ── 3b. New bucket: the quiet-renewal gate (and the builder's other objects),
        // under the lock — whoever takes over the lock from a builder that died while the
        // bucket was still empty finishes the job.
        bootstrap_fresh(store, prefix, &cur, migrations, new, &me, &runner).await?;
        tracing::info!(from = from, to = target, "cluster-tier migration starting");
        // ── 4. Each missing step, writing the mark after each one.
        for m in migrations.iter().filter(|m| m.to > from && m.to <= target) {
            // A gate step in the middle of the registry: run nothing, do not raise `version` to it.
            if matches!(m.run, Run::Gate) {
                continue;
            }
            if let Run::Cluster(step) = m.run {
                tracing::info!(step = m.to, name = m.name, "running cluster migration step");
                step(from, ctx.clone()).await?;
                crate::failpoint!("migrate::cluster::after_step", err);
            }
            cur.version = m.to;
            cur.min_binary = cur.min_binary.max(m.to);
            match write_cluster_version(store, prefix, &cur, Some(tag)).await? {
                Some(t) => tag = t,
                None => continue 'outer,
            }
        }
        // ── 5. Remove the lock.
        cur.upgrading = None;
        if write_cluster_version(store, prefix, &cur, Some(tag))
            .await?
            .is_none()
        {
            continue;
        }
        tracing::info!(version = cur.version, "cluster-tier migration done");
        return Ok(cur);
    }
}

/// The builder wants to write the quiet-renewal gate: the choice is not `off` and the
/// registry knows step [`QUIET_LEASE`].
fn wants_quiet_gate(migrations: &[Migration], new: &NewCluster) -> bool {
    new.quiet_lease != crate::quiet_lease::Bootstrap::Off
        && knows_gate(migrations, QUIET_LEASE, QUIET_LEASE_NAME)
}

/// **The new-bucket builder's part** (a deliberate decision), under our own `upgrading`
/// lock: the bucket is still truly empty ([`fresh_bucket`]) ⇒ run
/// [`NewCluster::on_fresh`], then write `control/quiet-lease` per
/// [`NewCluster::quiet_lease`] if it is not there yet. Conditions for the gate:
///
/// - `min_binary ≥` [`QUIET_LEASE`] is already on `cluster-version` (the create set it;
///   a successor of a builder with an older binary may not have it yet — skip);
/// - the gate is **absent**: if it is already there (a builder before us, `cairn-ctl`)
///   leave it alone;
/// - there is a second read client (`s3://`) ⇒ the G18 test passes first
///   ([`crate::store_probe::conditional_get`]): a store that answers wrongly ⇒ the gate
///   is not written, the cluster runs `direct` (log `warn`); a transient I/O error ⇒
///   return the error, the boot path retries.
///
/// Written with `Create` through [`crate::quiet_lease::write`] (rule 7: ambiguous ⇒ read
/// back; the content is unique thanks to `by=bootstrap@<session>`). Lost ⇒ someone else
/// has just written it — keep theirs. No evidence could be read, so delegation carries
/// the `delegation_unproven=` marker on all five entries: `cluster status` still prints
/// them.
async fn bootstrap_fresh(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    cur: &ClusterVersion,
    migrations: &[Migration],
    new: &NewCluster,
    me: &SessionId,
    runner: &Runner<'_>,
) -> Result<()> {
    let quiet = wants_quiet_gate(migrations, new) && cur.min_binary >= QUIET_LEASE;
    if (!quiet && new.on_fresh.is_empty()) || !fresh_bucket(store, prefix).await? {
        return Ok(());
    }
    // Other objects of a new bucket (`control/registry-gen` generation 0), even when the
    // gate is off. Each hook is idempotent (`Create`, already there means leave it).
    for hook in &new.on_fresh {
        hook().await?;
    }
    if !quiet {
        return Ok(());
    }
    if crate::quiet_lease::read(store, prefix).await?.is_some() {
        return Ok(());
    }
    if let Some(reader) = &new.probe_reader {
        let nonce = crate::store_probe::nonce();
        if let Err(why) = crate::store_probe::conditional_get(
            store,
            reader,
            prefix,
            &nonce,
            crate::store_probe::ROUNDS,
        )
        .await?
        {
            tracing::warn!(
                reason = %why,
                "store does not keep the conditional-GET contract (G18) — new bucket does NOT turn \
                 on the quiet-renewal gate, cluster runs direct"
            );
            return Ok(());
        }
    }
    let Some(q) = new
        .quiet_lease
        .gate(&me.to_string(), crate::entropy::now_ms())
    else {
        return Ok(());
    };
    if crate::quiet_lease::write(store, prefix, &q, None).await? {
        tracing::info!(
            mode = new.quiet_lease.as_str(),
            "new bucket: quiet-renewal gate on from the start (--quiet-lease)"
        );
        // Our session may have read the gate (absent) before this write.
        if let Runner::Session(s) = runner {
            s.recheck_quiet_lease();
        }
    } else {
        tracing::debug!("new bucket: someone else just wrote the quiet-renewal gate — keeping it");
    }
    Ok(())
}

/// Whether the registry has the gate step `to`/`name` — [`CALL_PREFIX`]: the binary reads
/// outbox rows v6; [`CALL_PLACEMENT`]: the binary reads the call command's `placement`.
fn knows_gate(migrations: &[Migration], to: u32, name: &str) -> bool {
    migrations
        .iter()
        .any(|m| m.to == to && m.name == name && matches!(m.run, Run::Gate))
}

/// A bucket that has never had a cluster: no `cluster.conf`, no range data (`ranges/` or
/// `partition-*` of earlier formats). A format 9 bucket from a binary before the
/// migration framework also lacks `cluster-version` — it is **not** new, and old
/// binaries may be running on it, so only a truly empty bucket gets the gates on up front.
async fn fresh_bucket(store: &Arc<dyn ObjectStore>, prefix: &Path) -> Result<bool> {
    match store.head(&crate::cluster::config_path(prefix)).await {
        Ok(_) => return Ok(false),
        Err(OsError::NotFound { .. }) => {}
        Err(e) => return Err(io(e)),
    }
    Ok(!crate::format::has_partition_data(store, prefix).await?)
}

/// **The range-Db-tier runner.** The range is at `data`, the cluster at `version`: run
/// every [`Run::RangeDb`] step with `data < to ≤ version`, in order, under `keeper`'s
/// lease, **before** the range's Db is opened. Does not write `data=`: the mark is written
/// once, after [`run_range`] (the part inside the Db) — dying anywhere before that makes
/// the next open rerun both parts (the steps are idempotent).
///
/// `data` larger than the registry's last step ⇒ [`StoreError::NewerFormat`], like
/// [`run_range`].
pub async fn run_range_db(
    stores: &crate::ObjectStores,
    range: u32,
    keeper: &mut crate::LeaseKeeper,
    data: u32,
    version: u32,
    migrations: &[Migration],
) -> Result<()> {
    check_range_data(range, data, latest(migrations))?;
    for m in migrations.iter().filter(|m| data < m.to && m.to <= version) {
        if let Run::RangeDb(step) = m.run {
            tracing::info!(
                range,
                step = m.to,
                name = m.name,
                "running a range-Db-tier migration step"
            );
            step(
                data,
                RangeDbCtx {
                    stores,
                    range,
                    keeper: &mut *keeper,
                },
            )
            .await?;
            crate::failpoint!("migrate::range_db::after_step", err);
        }
    }
    Ok(())
}

/// **The range-tier runner.** The range is at `data`, the cluster at `version`: run every
/// [`Run::Range`] step with `data < to ≤ version` through `writer`, in order, then **wait
/// until durable**. Returns the new `data`; the caller writes it onto the owner record
/// **after** this function returns — the mark is written last, like `rebuild_derived`:
/// dying anywhere before that makes the next open see the old `data` and rerun (the steps
/// are idempotent).
///
/// Runs on the range owner's writer, before `Engine::open_in_range`: no turn has run yet,
/// no engine is held yet.
///
/// `data` larger than the registry's last step ⇒ [`StoreError::NewerFormat`]: this Db
/// has been migrated by a newer binary, this binary must not write to it.
pub async fn run_range(
    writer: &mut PartitionWriter,
    data: u32,
    version: u32,
    migrations: &[Migration],
) -> Result<u32> {
    let range = writer.partition().0;
    check_range_data(range, data, latest(migrations))?;
    if data >= version {
        return Ok(data);
    }
    for m in migrations.iter().filter(|m| data < m.to && m.to <= version) {
        if let Run::Range(step) = m.run {
            tracing::info!(
                range,
                step = m.to,
                name = m.name,
                "running a range-tier migration step"
            );
            step(data, writer).await?;
            crate::failpoint!("migrate::range::after_step", err);
        }
    }
    writer.wait_durable().await?;
    crate::failpoint!("migrate::range::before_mark", err);
    Ok(version)
}

/// Range `range` is at data version `data` while the binary only knows up to `knows` ⇒
/// [`StoreError::NewerFormat`]: that Db has been migrated by a newer binary, this binary
/// must not open it for writing.
pub fn check_range_data(range: u32, data: u32, knows: u32) -> Result<()> {
    if data > knows {
        return Err(StoreError::NewerFormat {
            object: format!("owners/range-{range}"),
            header: format!("data={data} (binary knows up to {knows})"),
        });
    }
    Ok(())
}

/// Whether the holder `by` of the lock has died, in `runner`'s way.
async fn holder_is_dead(runner: &Runner<'_>, by: &SessionId) -> Result<bool> {
    match runner {
        Runner::Session(s) => s.is_session_dead(by).await,
        // No session: anyone else's lock belongs to an earlier run, which has died.
        Runner::Alone { .. } => Ok(true),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Rule 7: the `cluster-version` CAS was written but the reply says otherwise. Must
    /// see: `Some(version)` usable for the next CAS — not "CAS missed" (`None`) nor an
    /// error.
    #[tokio::test]
    async fn ambiguous_write_is_resolved_by_reread() {
        use crate::testkit::{Ambiguity, FaultyStore, Rule};
        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let faulty =
                FaultyStore::wrap(Arc::new(slatedb::object_store::memory::InMemory::new()));
            let store: Arc<dyn ObjectStore> = faulty.clone();
            let prefix = Path::from("c");
            let mut cv = ClusterVersion {
                version: 1,
                ..Default::default()
            };
            faulty.add_rule(Rule::ambiguous_put("cluster-version", reply));
            let tag = write_cluster_version(&store, &prefix, &cv, None)
                .await
                .unwrap_or_else(|e| panic!("{reply:?}: create written but error reported: {e}"))
                .unwrap_or_else(|| panic!("{reply:?}: create written but CAS miss reported"));
            cv.min_binary = 1;
            faulty.add_rule(Rule::ambiguous_put("cluster-version", reply));
            let tag = write_cluster_version(&store, &prefix, &cv, Some(tag))
                .await
                .unwrap_or_else(|e| panic!("{reply:?}: CAS written but error reported: {e}"))
                .unwrap_or_else(|| panic!("{reply:?}: CAS written but miss reported"));
            cv.version = 2;
            write_cluster_version(&store, &prefix, &cv, Some(tag))
                .await
                .unwrap()
                .expect("version after resolving the ambiguity must be usable");
            assert_eq!(
                read_cluster_version(&store, &prefix)
                    .await
                    .unwrap()
                    .unwrap()
                    .0,
                cv
            );
        }
    }

    #[test]
    fn cluster_version_roundtrips_and_keeps_unknown_lines() {
        let cv = ClusterVersion {
            version: 3,
            upgrading: Some(Upgrading {
                from: 3,
                to: 5,
                by: SessionId {
                    node: "host/1".into(),
                    inc: 42,
                },
            }),
            min_binary: 3,
            call_prefix: true,
            call_placement: true,
            streams: true,
            schedules: true,
            key_roles: true,
            tenant_state: true,
            audit: true,
            tenant_purge: true,
            values: true,
            extra: vec!["future=1".into()],
        };
        let text = String::from_utf8(cv.encode()).unwrap();
        assert!(text.contains("streams=on\n"), "{text}");
        assert!(text.contains("schedules=on\n"), "{text}");
        assert!(text.contains("key_roles=on\n"), "{text}");
        assert!(text.contains("tenant_state=on\n"), "{text}");
        assert!(text.contains("audit=on\n"), "{text}");
        assert!(text.contains("tenant_purge=on\n"), "{text}");
        assert!(text.contains("values=on\n"), "{text}");
        assert!(text.contains("upgrading=3->5@host/1/42\n"), "{text}");
        assert!(text.contains("call_prefix=on\n"), "{text}");
        assert!(text.contains("call_placement=on\n"), "{text}");
        assert_eq!(ClusterVersion::decode(&cv.encode()).unwrap(), cv);
        let bare = ClusterVersion::decode(b"cairnflow-cluster-version/1\nversion=0\n").unwrap();
        assert_eq!(bare, ClusterVersion::default());
    }

    #[test]
    fn broken_cluster_versions_are_rejected() {
        for raw in [
            &b"junk"[..],
            b"cairnflow-cluster-version/1\n",
            b"cairnflow-cluster-version/1\nversion=x\n",
            b"cairnflow-cluster-version/1\nversion=1\nupgrading=1-2@a/1\n",
            b"cairnflow-cluster-version/1\nversion=1\nupgrading=1->2@a\n",
            b"cairnflow-cluster-version/1\nversion=1\nmin_binary=-1\n",
            b"cairnflow-cluster-version/1\nversion=1\ncall_prefix=yes\n",
            b"cairnflow-cluster-version/1\nversion=1\nkey_roles=yes\n",
            b"cairnflow-cluster-version/1\nversion=1\ntenant_state=1\n",
            b"cairnflow-cluster-version/1\nversion=1\naudit=yes\n",
            b"cairnflow-cluster-version/1\nversion=1\ncall_placement=1\n",
        ] {
            assert!(ClusterVersion::decode(raw).is_err(), "{raw:?}");
        }
        assert!(matches!(
            ClusterVersion::decode(b"cairnflow-cluster-version/2\nversion=1\n"),
            Err(StoreError::NewerFormat { .. })
        ));
    }

    /// The `calls/prefix` gate is on only with **both** the `call_prefix=on` line and a
    /// `min_binary` that blocks binaries not knowing that step; `off` or absent ≡ off.
    #[test]
    fn call_prefix_needs_the_line_and_min_binary() {
        let on = |raw: &str| {
            ClusterVersion::decode(raw.as_bytes())
                .unwrap()
                .call_prefix_on()
        };
        assert!(on(
            "cairnflow-cluster-version/1\nversion=2\nmin_binary=4\ncall_prefix=on\n"
        ));
        assert!(!on(
            "cairnflow-cluster-version/1\nversion=2\nmin_binary=3\ncall_prefix=on\n"
        ));
        assert!(!on(
            "cairnflow-cluster-version/1\nversion=2\nmin_binary=4\ncall_prefix=off\n"
        ));
        assert!(!on(
            "cairnflow-cluster-version/1\nversion=2\nmin_binary=4\n"
        ));
        assert!(!ClusterVersion::default().call_prefix_on());
        // A binary that only knows up to step 3 keeps that line verbatim when writing back.
        let old = ClusterVersion {
            extra: vec!["call_prefix=on".into()],
            min_binary: 4,
            ..Default::default()
        };
        assert!(ClusterVersion::decode(&old.encode())
            .unwrap()
            .call_prefix_on());
    }

    /// The `calls/placement` gate: like `calls/prefix`, it needs **both** the
    /// `call_placement=on` line and `min_binary ≥ 5` — `min_binary=4` still lets a binary
    /// that skips `placement` start.
    #[test]
    fn call_placement_needs_the_line_and_min_binary() {
        let on = |raw: &str| {
            ClusterVersion::decode(raw.as_bytes())
                .unwrap()
                .call_placement_on()
        };
        assert!(on(
            "cairnflow-cluster-version/1\nversion=2\nmin_binary=5\ncall_prefix=on\ncall_placement=on\n"
        ));
        assert!(!on(
            "cairnflow-cluster-version/1\nversion=2\nmin_binary=4\ncall_prefix=on\ncall_placement=on\n"
        ));
        assert!(!on(
            "cairnflow-cluster-version/1\nversion=2\nmin_binary=5\ncall_placement=off\n"
        ));
        assert!(!on(
            "cairnflow-cluster-version/1\nversion=2\nmin_binary=5\ncall_prefix=on\n"
        ));
        // A binary that knows up to step 4 keeps that line verbatim when writing back.
        let old = ClusterVersion {
            extra: vec!["call_placement=on".into()],
            min_binary: 5,
            ..Default::default()
        };
        assert!(ClusterVersion::decode(&old.encode())
            .unwrap()
            .call_placement_on());
    }

    #[test]
    fn check_known_refuses_newer_cluster() {
        let cv = |version, min_binary| ClusterVersion {
            version,
            min_binary,
            ..Default::default()
        };
        assert!(check_known(&cv(0, 0), 0).is_ok());
        assert!(check_known(&cv(2, 2), 3).is_ok());
        assert!(matches!(
            check_known(&cv(4, 0), 3),
            Err(StoreError::NewerFormat { .. })
        ));
        assert!(matches!(
            check_known(&cv(3, 4), 3),
            Err(StoreError::NewerFormat { .. })
        ));
    }

    /// **The migration registry grows by one, no gaps, no duplicate `to`.**
    #[test]
    fn ledger_is_strictly_increasing_without_gaps() {
        check_ledger(MIGRATIONS).unwrap();
        assert_eq!(KNOWS, MIGRATIONS.len() as u32);
    }

    #[test]
    fn check_ledger_catches_gaps_duplicates_and_bad_start() {
        fn noop(_: u32, _: ClusterCtx) -> BoxFuture<'static, Result<()>> {
            Box::pin(async { Ok(()) })
        }
        let m = |to| Migration {
            to,
            name: "x",
            run: Run::Cluster(noop),
        };
        assert!(check_ledger(&[]).is_ok());
        assert!(check_ledger(&[m(1), m(2), m(3)]).is_ok());
        for bad in [
            vec![m(2)],
            vec![m(1), m(3)],
            vec![m(1), m(1)],
            vec![m(1), m(2), m(2)],
            vec![m(0)],
        ] {
            assert!(check_ledger(&bad).is_err(), "{bad:?}");
        }
        assert_eq!(latest(&[m(1), m(2)]), 2);
    }

    #[test]
    fn header_is_in_policy() {
        assert_eq!(
            crate::format::object_policy("cluster-version").map(|p| p.header),
            Some(HEADER)
        );
    }
}
