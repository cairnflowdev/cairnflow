// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `cairn-ctl` — checks and repairs the bucket **without a running node**. The
//! `cairn-ctl` binary only parses arguments and prints; every command lives here and
//! returns a `Serialize` structure so tests can call it directly in process.
//!
//! Inspired by TiKV's `tikv-ctl`:
//!
//! - **read-only is the default.** Read commands only GET/HEAD/LIST — including `verify`,
//!   which opens Dbs with a `DbReader` (`FollowLatest`, writing no checkpoint).
//!   `cluster.conf` is read with [`ClusterConfig::read_only`], never creating a cluster;
//! - **repair commands dry-run** unless given `--apply`: check everything, then write
//!   **one** CAS (the range table or an owner record), refuse while the owner is alive
//!   (session `live`) unless given `--force`, and every `--apply` leaves
//!   `audit/ctl/<ts>` in the bucket;
//! - `verify` opens each Db with the production loader itself.

use {
    crate::split::QUIESCE_GRACE,
    cairn_store::{
        gc_barrier::{self, GcBarrier, Protects},
        inspect::{self, AuditEntry, ExternalStatus},
        keys::{owner_of, table_of, Table},
        migrate,
        nodewal::inspect::log_floors,
        quiet_lease::{self, QuietLease},
        store_probe, ClusterConfig, NodeTailers, ObjectStores, OwnerRecord, PartitionId,
        PartitionTable, RangeEntry, RangeIntent, SessionId, SessionMode, SessionState, StoreError,
        TableUpdate, WalKind,
    },
    serde::Serialize,
    std::{
        collections::{BTreeMap, BTreeSet},
        time::Duration,
    },
};

// Self-contained backup and restore.
mod backup;
pub use backup::*;

// The `--report-to-bucket` reports.
mod audit_check;
mod purge_check;
mod report;
mod restore_check;
// `table restore`.
mod table_restore;
mod value_check;
pub use report::*;
pub use restore_check::{LOST_CALL, LOST_REPLY};
pub use table_restore::TableRestoreReport;

/// An intent sitting in the table longer than this is stuck (check (e)): a live
/// split/merge only holds an intent for a few seconds (quiesce at most `QUIESCE_GRACE`).
pub const INTENT_STUCK_AFTER: Duration = Duration::from_secs(QUIESCE_GRACE.as_secs() * 10);

/// The lifetime of the temporary checkpoint `fork` sets on each source Db — it only
/// needs to live until the clone pins its final checkpoint.
pub const FORK_CHECKPOINT_LIFETIME: Duration = Duration::from_secs(3600);

/// The expiry of the GC fence `fork` sets on the source cluster: the TTL cap of a fence —
/// there are no infinite fences. To use the copy longer, extend the fence (same
/// `protects`) before it expires; `fork` prints the command.
pub const FORK_BARRIER_TTL: Duration = crate::gc::MAX_BARRIER_TTL;

/// The GC fence id of the copy at prefix `to`: `fork-<to>` when that is a valid id,
/// otherwise the valid part truncated plus 8 hex of SHA-256(`to`) — two different
/// prefixes never overwrite each other's fence.
pub fn fork_barrier_id(to: &str) -> String {
    let plain = format!("fork-{to}");
    if gc_barrier::check_barrier_id(&plain).is_ok() {
        return plain;
    }
    use sha2::{Digest, Sha256};
    let clean: String = to
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '-'
            }
        })
        .take(40)
        .collect();
    let h = Sha256::digest(to.as_bytes());
    format!(
        "fork-{clean}-{:02x}{:02x}{:02x}{:02x}",
        h[0], h[1], h[2], h[3]
    )
}

/// The error of a command.
#[derive(Debug, thiserror::Error)]
pub enum CtlError {
    #[error(transparent)]
    Store(#[from] StoreError),
    /// A repair command refuses to act — nothing written.
    #[error("refused: {0}")]
    Refused(String),
    /// Bad arguments, or the bucket lacks what the command needs.
    #[error("{0}")]
    Bad(String),
}

pub type CtlResult<T> = Result<T, CtlError>;

/// One step of rotating a tenant's identity key.
pub use crate::identity::IdentityStep;

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn now_ms() -> u64 {
    cairn_store::entropy::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn intent_text(i: &RangeIntent) -> String {
    match i {
        RangeIntent::Splitting { at, left, right } => {
            format!("splitting(at={at:016x},left={},right={})", left.0, right.0)
        }
        RangeIntent::Merging { with, into } => format!("merging(with={},into={})", with.0, into.0),
    }
}

/// Ids the table's intent has assigned that have not become ranges yet.
fn promised(t: &PartitionTable) -> BTreeSet<u32> {
    t.ranges()
        .iter()
        .filter_map(|r| r.intent)
        .flat_map(|i| match i {
            RangeIntent::Splitting { left, right, .. } => vec![left.0, right.0],
            RangeIntent::Merging { into, .. } => vec![into.0],
        })
        .collect()
}

fn state_text(s: SessionState) -> String {
    format!("{s:?}").to_lowercase()
}

// ─────────────────────────────────────────────────────────────── results

#[derive(Clone, Debug, Serialize)]
pub struct RangeView {
    pub id: u32,
    pub start: String,
    pub end: String,
    pub gen: u64,
    pub intent: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TableView {
    pub version: u64,
    pub next_range_id: u32,
    /// The `last_modified` of `cluster.conf` (ms).
    pub modified_ms: Option<u64>,
    pub ranges: Vec<RangeView>,
    /// `k=v` lines this binary does not know, verbatim.
    pub extra: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OwnerView {
    pub range: u32,
    pub in_table: bool,
    pub term: Option<u64>,
    pub session: Option<String>,
    /// `live` / `expired` / `closed`, `missing` (record cleaned up), `corrupt`.
    pub session_state: Option<String>,
    /// The session record has not changed for this long (ms) — renewed every 2 s.
    pub session_idle_ms: Option<u64>,
    pub released: bool,
    pub retired: bool,
    pub wal_kind: Option<String>,
    pub wal_segments: Vec<String>,
    pub wal_floor_id: Option<u64>,
    pub data: Option<u32>,
    /// The owner record could not be read.
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RetiredView {
    pub range: u32,
    pub since_ms: u64,
    pub db_exists: bool,
    /// Why the Db cannot be deleted yet (which live range still references it), if any.
    pub referenced_by: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RowView {
    pub key: String,
    pub table: Option<String>,
    pub owner_pk: Option<String>,
    pub value_len: usize,
    /// The first 64 bytes of the value, hex.
    pub value: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WalLogView {
    pub log: String,
    pub session_state: Option<String>,
    pub floor: u64,
    pub objects: u64,
    pub sealed_at: Option<u64>,
    pub pinned_by_range: Option<u32>,
    pub pinned_wal_id: Option<u64>,
    /// The log has been fully cleaned up (`forgotten` marker): the seal stays as a fence, the floor
    /// reads 0.
    pub forgotten: bool,
}

/// An error `verify` found.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Finding {
    /// `table` (a), `db` (b), `external_db` (c), `orphan_db` (d), `stuck_intent` (e),
    /// `orphan_checkpoint` (f), `unheld_wal_log` (g), `retired_unreferenced` (h),
    /// `query_index` (i), `restore.lost_call`/`restore.lost_reply` (j), `audit` (k),
    /// `value_orphan`/`value_dangling` (m, n).
    pub check: &'static str,
    pub range: Option<u32>,
    pub detail: String,
    /// Which background loop of the cluster **heals** this state by itself when it runs
    /// (`SM-11`, `SM-14`, `NW-8`, `D-d`, the first open) — i.e. it may be the half-done
    /// state of a `kill -9` point rather than a leak. `None` = no loop heals it: a real
    /// corruption or leak.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heals: Option<&'static str>,
}

impl Finding {
    fn new(check: &'static str, range: Option<u32>, detail: impl Into<String>) -> Self {
        Finding {
            check,
            range,
            detail: detail.into(),
            heals: None,
        }
    }

    fn heals(mut self, by: &'static str) -> Self {
        self.heals = Some(by);
        self
    }
}

/// Every check name of `verify` — the `check` label of the scrubber's metric.
pub const CHECKS: [&str; 16] = [
    "table",
    "db",
    "external_db",
    "orphan_db",
    "stuck_intent",
    "orphan_checkpoint",
    "unheld_wal_log",
    "retired_unreferenced",
    "query_index",
    "restore.lost_call",
    "restore.lost_reply",
    "audit",
    purge_check::CHECK,
    value_check::ORPHAN,
    value_check::DANGLING,
    SST_MISSING,
];

/// The `verify --deep` check: objects a live range's Db needs (the SSTs of the current
/// manifest and of unexpired checkpoints, the checkpoints' manifests) are no longer in
/// the bucket. No loop heals it.
pub const SST_MISSING: &str = "sst_missing";

/// An item `verify` **skipped** because its read failed beyond the retry cap — only in
/// tolerant mode ([`VerifyOptions::skip_unreadable`], the background scrubber). Check
/// `check` did not consider this item in that round.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Skipped {
    pub check: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub range: Option<u32>,
    /// The item's incarnation (check (g)) — `node/inc`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session: Option<String>,
    /// The item, as an operator reads it: `ranges/12`, `owners/range-12`, …
    pub item: String,
    pub error: String,
}

impl Skipped {
    fn new(
        check: &'static str,
        range: Option<u32>,
        item: impl Into<String>,
        error: impl ToString,
    ) -> Self {
        Skipped {
            check,
            range,
            session: None,
            item: item.into(),
            error: error.to_string(),
        }
    }

    fn scan(check: &'static str, s: inspect::ScanSkip) -> Self {
        Skipped {
            check,
            range: s.range.map(|r| r.0),
            session: s.session.map(|id| id.to_string()),
            item: s.item,
            error: s.error,
        }
    }
}

/// The subject of an `unheld_wal_log` finding for incarnation `session` — the head of its
/// detail, and the scrubber's stable key (`scrub::key`).
pub(crate) fn wal_log_subject(session: &str) -> String {
    format!("shared WAL log of {session}")
}

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VerifyReport {
    pub table_version: Option<u64>,
    pub ranges_checked: usize,
    pub external_refs_checked: usize,
    pub range_dirs: usize,
    pub findings: Vec<Finding>,
    /// The number of items whose read was attempted (owner records, ranges, directories,
    /// incarnations, cluster-wide checks) — the denominator of the scrubber's
    /// "incomplete round" threshold.
    pub items: usize,
    /// Skipped items — empty unless [`VerifyOptions::skip_unreadable`].
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub skipped: Vec<Skipped>,
    /// **Information** lines: valid states worth knowing that do not make the report
    /// dirty (`clean()` does not consider them) — young audit intents without an outcome
    /// (check (k)). After a `kill -9` they are normal; counting them as findings would
    /// make `verify`/`backup check-restore` exit 1 after every crash.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<Finding>,
}

impl VerifyReport {
    pub fn clean(&self) -> bool {
        self.findings.is_empty()
    }
}

/// The options of `verify`.
#[derive(Clone, Copy, Debug)]
pub struct VerifyOptions {
    /// The clock used to compute intent age (ms) — tests can set it.
    pub now_ms: u64,
    pub stuck_after: Duration,
    /// Open each Db with a reader (check (b)); off, only check that the manifest exists.
    pub open_dbs: bool,
    /// (g) the shared WAL logs of finished incarnations that no owner record lists any
    /// more but that still have objects (the dead log sweep, NW-8, has not cleaned them).
    /// Off by default: right after a `kill -9` every log of the dead node looks like that.
    pub wal_logs: bool,
    /// (h) a retired Db no live range references, no fence covers, with a tombstone older
    /// than this — exactly the set D-d would delete with this `min_age`. `None` = not
    /// checked (with D-d off that is by design, not a leak).
    pub retired_after: Option<Duration>,
    /// **Tolerate per-item failures** (the background scrubber): a read of one item (an
    /// owner record, a range's manifest, a `ranges/<id>`, an incarnation) that fails
    /// beyond the retry cap ([`inspect::SCAN_RETRY`]) puts that item into
    /// [`VerifyReport::skipped`] and the round goes on. Off (the default,
    /// `cairn-ctl verify`): such an error is an error of the whole command — as before,
    /// after the retry cap.
    pub skip_unreadable: bool,
    /// (j) lost inter-range messages ([`restore_check`]): a call whose caller waits for
    /// the result of a callee that does not exist, or whose callee finished and the reply
    /// is nowhere. Only with `open_dbs`. On by default — checked after a restore and at
    /// every cluster test's cleanup; `--no-restore-checks` turns it off.
    pub restore_checks: bool,
    /// [`SST_MISSING`] (`--deep`): every SST the current manifest and the unexpired
    /// checkpoints of a live range mention is still in the bucket — one LIST per
    /// `compacted`/`manifest` directory of each Db. Off by default, not in the background
    /// scrubber.
    pub deep: bool,
}

impl Default for VerifyOptions {
    fn default() -> Self {
        Self {
            now_ms: now_ms(),
            stuck_after: INTENT_STUCK_AFTER,
            open_dbs: true,
            wal_logs: false,
            retired_after: None,
            skip_unreadable: false,
            restore_checks: true,
            deep: false,
        }
    }
}

/// Reads one `verify` item through [`inspect::scan_read`]. Still failing beyond the cap:
/// tolerant mode ⇒ record a `skip` in `rep.skipped`, return `None`; otherwise an error.
async fn read_item<T, F, Fut>(
    opts: &VerifyOptions,
    rep: &mut VerifyReport,
    skip: impl FnOnce(&StoreError) -> Skipped,
    f: F,
) -> CtlResult<Option<T>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, StoreError>>,
{
    rep.items += 1;
    match inspect::scan_read(f).await {
        Ok(v) => Ok(Some(v)),
        Err(e) if opts.skip_unreadable => {
            rep.skipped.push(skip(&e));
            Ok(None)
        }
        Err(e) => Err(e.into()),
    }
}

/// Options shared by repair commands.
#[derive(Clone, Debug)]
pub struct RepairOptions {
    /// `false` = dry run: print the change, write nothing.
    pub apply: bool,
    /// Act even while the range's owner is alive.
    pub force: bool,
    pub who: String,
    /// The command line verbatim, for the audit record.
    pub command: String,
    /// `intent clear`: the expected table version; anything else ⇒ refuse.
    pub expect_version: Option<u64>,
    pub now_ms: u64,
}

/// The result of a repair command (dry or real).
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RepairReport {
    pub applied: bool,
    /// The object the CAS changes.
    pub object: String,
    pub before: String,
    pub after: String,
    /// Cleanup after the CAS (dry run: would do; real: done).
    pub cleanup: Vec<String>,
    pub warnings: Vec<String>,
    /// The path of the audit record written.
    pub audit: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TrashEntryView {
    pub id: String,
    /// `main` or `wal` — the store holding the entry (a Db on a separate WAL store has
    /// one entry with the same id in each store).
    pub store: &'static str,
    /// `range-db <id>`, `owner <id>`, `wal-log <log>`; `None` = an entry that could not be read.
    pub subject: Option<String>,
    pub reason: String,
    pub objects: usize,
    pub bytes: u64,
    pub age_ms: u64,
    pub unreadable: Option<String>,
}

/// Which entries `trash restore` restores.
#[derive(Clone, Debug)]
pub enum TrashTarget {
    /// One entry by id.
    Entry(String),
    /// Every entry about the Db **and** the owner record of a range (SM-16: restored together).
    Range(u32),
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ForkReport {
    pub to: String,
    /// Artifact objects copied.
    pub artifacts: usize,
    /// Segments of segmented values copied.
    pub values: usize,
    pub ranges: Vec<u32>,
    /// The GC fence set on the source cluster. `None` = no range was cloned, the copy
    /// stands on nothing of the source.
    pub barrier: Option<ForkBarrier>,
    pub owners: Vec<u32>,
    pub copied: Vec<String>,
    pub warnings: Vec<String>,
    pub audit: Option<String>,
}

/// The GC fence `fork` leaves on the source cluster.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ForkBarrier {
    pub id: String,
    /// `ranges:<id>,…`, or `all` when the range set cannot be proven.
    pub protects: String,
    pub expires_ms: u64,
}

/// A session in the bucket, as `cluster status` sees it.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionView {
    pub session: String,
    /// `live`, `expired`, `closed`, or `corrupt` (cannot be decoded).
    pub state: String,
    /// The binary's `knows=`; `None` = a binary predating the migration framework (or a broken
    /// record).
    pub knows: Option<u32>,
    /// `direct`, `solo`, or an unknown value verbatim.
    pub mode: String,
    pub challenged: Option<String>,
    /// ms since the last write (`last_modified`).
    pub idle_ms: u64,
}

/// `cluster status`: the cluster version, the quiet renewal gate, and every session.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterStatus {
    pub version: u32,
    pub min_binary: u32,
    /// The migration runner lock, if any.
    pub upgrading: Option<String>,
    /// `control/quiet-lease` (absent or unreadable ≡ `false`).
    pub quiet_lease: bool,
    /// The gate step of this `cairn-ctl` binary ([`cairn_store::migrate::QUIET_LEASE`]).
    pub quiet_lease_knows: u32,
    /// The gate is on **and** `delegation=on` (delegated renewal).
    pub delegation: bool,
    /// The five evidence items (a)–(e) of the delegation gate, in order.
    pub evidence: Vec<EvidenceView>,
    /// Evidence items still missing — `--delegation` refuses when this is not empty.
    pub evidence_missing: Vec<char>,
    /// Delegation turned on with `--accept-missing-evidence`: the missing items, who, when.
    pub delegation_unproven: Option<String>,
    /// The gate grouping call trees by pk prefix (`call_prefix=on` and a sufficient
    /// `min_binary` — [`migrate::ClusterVersion::call_prefix_on`]).
    pub call_prefix: bool,
    /// That gate's step ([`cairn_store::migrate::CALL_PREFIX`]).
    pub call_prefix_knows: u32,
    /// The gate of the deliberate spread path `placement: "spread"` (`call_placement=on`
    /// and a sufficient `min_binary` — [`migrate::ClusterVersion::call_placement_on`]).
    pub call_placement: bool,
    /// That gate's step ([`cairn_store::migrate::CALL_PLACEMENT`]).
    pub call_placement_knows: u32,
    /// The durable stream gate (`streams=on` and a sufficient `min_binary` —
    /// [`migrate::ClusterVersion::streams_on`]).
    pub streams: bool,
    /// The durable stream gate's step — [`migrate::STREAMS_TABLE`].
    pub streams_knows: u32,
    /// The server-side schedule gate (`schedules=on` and a sufficient `min_binary` —
    /// [`migrate::ClusterVersion::schedules_on`]).
    pub schedules: bool,
    /// The schedule gate's step — [`migrate::SCHEDULES_TABLE`].
    pub schedules_knows: u32,
    /// The key registry `/2` gate (`key_roles=on` and a sufficient `min_binary` —
    /// [`migrate::ClusterVersion::key_roles_on`]).
    pub key_roles: bool,
    /// That gate's step — [`migrate::KEY_ROLES`].
    pub key_roles_knows: u32,
    /// The tenant state gate (`tenant_state=on` and a sufficient `min_binary` —
    /// [`migrate::ClusterVersion::tenant_state_on`]).
    pub tenant_state: bool,
    /// That gate's step — [`migrate::TENANT_STATE`].
    pub tenant_state_knows: u32,
    /// The in-range audit gate (`audit=on` and a sufficient `min_binary` —
    /// [`migrate::ClusterVersion::audit_on`]).
    pub audit: bool,
    /// That gate's step — [`migrate::AUDIT_TABLE`].
    pub audit_knows: u32,
    /// The tenant purge gate (`tenant_purge=on` and a sufficient `min_binary` —
    /// [`migrate::ClusterVersion::tenant_purge_on`]).
    pub tenant_purge: bool,
    /// That gate's step — [`migrate::TENANT_PURGE`].
    pub tenant_purge_knows: u32,
    /// The segmented values gate (`values=on` and a sufficient `min_binary` —
    /// [`migrate::ClusterVersion::values_on`]).
    pub values: bool,
    /// That gate's step — [`migrate::VALUES`].
    pub values_knows: u32,
    pub sessions: Vec<SessionView>,
}

/// One evidence item of the delegation gate as `cluster status` prints it.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EvidenceView {
    /// `a` … `e`.
    pub item: char,
    pub title: &'static str,
    /// `None` = missing.
    pub note: Option<String>,
    pub by: Option<String>,
    pub at_ms: Option<u64>,
}

/// `(c), (d), (e)` — evidence items for the printed line.
fn items_text(items: &[char]) -> String {
    items
        .iter()
        .map(|k| format!("({k})"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The override marker for `cluster status`.
fn unproven_text(u: &quiet_lease::Unproven) -> String {
    format!(
        "delegation enabled with --accept-missing-evidence while missing {} — by {} at at_ms={}",
        items_text(&u.missing),
        u.by,
        u.at_ms
    )
}

// ────────────────────────────────────────────────────────────────── commands

/// A bucket (main store + WAL store, prefix) that `cairn-ctl` works on.
pub struct Ctl {
    stores: ObjectStores,
    /// The clock of the waits ([`BOOT_WAIT`] of `enable-quiet-lease`) — injectable so
    /// tests do not wait in real time.
    clock: std::sync::Arc<dyn cairn_store::clock::Clock>,
    /// TTL of the GC fence `backup` holds on the source while it copies
    /// ([`backup::BACKUP_FENCE_TTL`]; tests shorten it).
    backup_fence_ttl: Duration,
}

/// `enable-quiet-lease` waits this long after setting `min_binary`, then checks every
/// session again: an old binary that read `cluster-version` before the CAS and is still
/// starting has, within this window, either opened its session (and the final check
/// sees it) or given up. The node's boot path (read `cluster-version` at the gate, then
/// open the session) has a shared cap [`crate::boot::BOOT_IO_DEADLINE`] measured from
/// before that read; a last attempt issued before the cap is not cut off and may run one
/// more S3 client self-retry ([`cairn_store::S3_CLIENT_RETRY_TIMEOUT`]) plus one request
/// (the client's per-request cap, the same 30 s); plus a 10 s margin.
pub const BOOT_WAIT: Duration = Duration::from_secs(
    crate::boot::BOOT_IO_DEADLINE.as_secs()
        + 2 * cairn_store::S3_CLIENT_RETRY_TIMEOUT.as_secs()
        + 10,
);

impl Ctl {
    pub fn new(stores: ObjectStores) -> Self {
        Self {
            stores,
            clock: std::sync::Arc::new(cairn_store::clock::SystemClock),
            backup_fence_ttl: backup::BACKUP_FENCE_TTL,
        }
    }

    /// Like [`Self::new`] with TTL `ttl` for the GC fence of `backup` (tests).
    pub fn with_backup_fence_ttl(mut self, ttl: Duration) -> Self {
        self.backup_fence_ttl = ttl;
        self
    }

    /// Like [`Self::new`] with clock `clock` for the waits (tests).
    pub fn with_clock(mut self, clock: std::sync::Arc<dyn cairn_store::clock::Clock>) -> Self {
        self.clock = clock;
        self
    }

    pub fn stores(&self) -> &ObjectStores {
        &self.stores
    }

    async fn cluster(&self) -> CtlResult<ClusterConfig> {
        let main = self.stores.main();
        ClusterConfig::read_only(main.store(), main.prefix())
            .await?
            .ok_or_else(|| {
                CtlError::Bad(format!(
                    "no cluster.conf at {} — wrong --main-store/--prefix?",
                    main.uri()
                ))
            })
    }

    /// `table show`.
    pub async fn table_show(&self) -> CtlResult<TableView> {
        let cluster = self.cluster().await?;
        let t = cluster.table();
        Ok(TableView {
            version: t.version(),
            next_range_id: t.next_range_id(),
            modified_ms: cluster.stored_modified_ms().await?,
            ranges: t
                .ranges()
                .iter()
                .map(|r| RangeView {
                    id: r.id.0,
                    start: format!("{:016x}", r.start),
                    end: format!("{:016x}", r.end),
                    gen: r.gen,
                    intent: r.intent.as_ref().map(intent_text),
                })
                .collect(),
            extra: t.extra().to_vec(),
        })
    }

    /// The state of session `id` and how long its record has been idle.
    async fn session_state(&self, id: &SessionId, now: u64) -> CtlResult<(String, Option<u64>)> {
        let main = self.stores.main();
        Ok(
            match inspect::session_record(main.store(), main.prefix(), id).await? {
                None => ("missing".into(), None),
                Some((Ok(rec), modified)) => {
                    (state_text(rec.state), Some(now.saturating_sub(modified)))
                }
                Some((Err(_), modified)) => ("corrupt".into(), Some(now.saturating_sub(modified))),
            },
        )
    }

    /// `owners`: every owner record, with the state of the owner's session.
    pub async fn owners(&self) -> CtlResult<Vec<OwnerView>> {
        let table = self.cluster().await.ok().map(|c| c.table());
        let main = self.stores.main();
        let now = now_ms();
        let mut out = Vec::new();
        for r in inspect::owner_ranges(main.store(), main.prefix()).await? {
            let in_table = table.as_ref().is_some_and(|t| t.contains(r));
            let mut view = OwnerView {
                range: r.0,
                in_table,
                term: None,
                session: None,
                session_state: None,
                session_idle_ms: None,
                released: false,
                retired: false,
                wal_kind: None,
                wal_segments: Vec::new(),
                wal_floor_id: None,
                data: None,
                error: None,
            };
            match inspect::owner_observed(main.store(), main.prefix(), r).await {
                Ok(Some(o)) => {
                    let rec = o.record;
                    let (state, idle) = self.session_state(&rec.session, now).await?;
                    view.term = Some(rec.term);
                    view.session = Some(rec.session.to_string());
                    view.session_state = Some(state);
                    view.session_idle_ms = idle;
                    view.released = rec.released;
                    view.retired = rec.retired;
                    view.wal_kind = Some(rec.wal_kind.as_str().to_string());
                    view.wal_segments = rec.wal_segments.iter().map(|s| s.to_string()).collect();
                    view.wal_floor_id = rec.wal_floor_id;
                    view.data = rec.data;
                }
                Ok(None) => continue,
                Err(e) => view.error = Some(e.to_string()),
            }
            out.push(view);
        }
        Ok(out)
    }

    /// `ranges retired`: ranges carrying a tombstone, whether the Db remains, who still holds the
    /// Db.
    pub async fn retired(&self) -> CtlResult<Vec<RetiredView>> {
        let cluster = self.cluster().await?;
        let live: Vec<PartitionId> = cluster.partitions();
        let main = self.stores.main();
        let refs = self.stores.referenced_ranges(&live).await?;
        let mut out = Vec::new();
        for r in cairn_store::list_retired(main.store(), main.prefix()).await? {
            out.push(RetiredView {
                range: r.range.0,
                since_ms: r.since_ms(),
                db_exists: self.stores.range_db_exists(r.range).await?,
                referenced_by: refs.blocker(r.range),
            });
        }
        out.sort_by_key(|v| v.range);
        Ok(out)
    }

    /// Opens a range's Db with the production loader, read-only (shared-WAL ranges read
    /// unflushed frames through the tailer, like a follower).
    async fn open_reader(
        &self,
        range: u32,
    ) -> CtlResult<(cairn_store::PartitionReader, NodeTailers)> {
        let poll = Duration::from_millis(200);
        let tailers = NodeTailers::new(&self.stores, poll);
        let reader = self
            .stores
            .open_reader_with(range, poll, Some(&tailers))
            .await?;
        Ok((reader, tailers))
    }

    /// `range dump <id> [--table T] [--limit N]`.
    pub async fn range_dump(
        &self,
        range: u32,
        table: Option<&str>,
        limit: usize,
    ) -> CtlResult<Vec<RowView>> {
        let want = match table {
            None => None,
            Some(name) => Some(parse_table(name)?),
        };
        let (reader, _tailers) = self.open_reader(range).await?;
        let rows = reader
            .scan_raw(&[], limit, |k| want.is_none_or(|t| table_of(k) == Some(t)))
            .await;
        let _ = reader.close().await;
        Ok(rows?
            .into_iter()
            .map(|(k, v)| RowView {
                key: hex(&k),
                table: table_of(&k).map(|t| format!("{t:?}")),
                owner_pk: owner_of(&k).map(|pk| format!("{:016x}", pk.0)),
                value_len: v.len(),
                // 128 bytes: enough for the scan mark row (`Floors`, 73 bytes) to be read whole.
                value: hex(&v[..v.len().min(128)]),
            })
            .collect())
    }

    /// `wal floors`: every shared WAL log in the bucket, its floor and pinning ranges.
    pub async fn wal_floors(&self) -> CtlResult<Vec<WalLogView>> {
        Ok(log_floors(&self.stores)
            .await?
            .into_iter()
            .map(|l| WalLogView {
                log: l.log.to_string(),
                session_state: l.session_state.map(state_text),
                floor: l.floor,
                objects: l.objects,
                sealed_at: l.sealed_at,
                pinned_by_range: l.pinned_by.map(|p| p.0),
                pinned_wal_id: l.pinned_by.map(|p| p.1),
                forgotten: l.forgotten,
            })
            .collect())
    }

    /// `verify`: (a) the table covers the whole keyspace, (b) every range in the table has
    /// a Db and it opens, (c) every `external_dbs` points to a Db (and checkpoint) that
    /// still exists, (d) orphan `ranges/<id>` directories, (e) stuck intents, (f) final
    /// checkpoints of orphan children on a live range.
    pub async fn verify(&self, opts: VerifyOptions) -> CtlResult<VerifyReport> {
        let mut rep = VerifyReport::default();
        let main = self.stores.main();
        // (a) — `PartitionTable::decode` enforces the coverage rule; with a broken table
        // there is nothing left to compare b–e against.
        let cluster = match inspect::scan_read(|| {
            ClusterConfig::read_only(main.store(), main.prefix())
        })
        .await
        {
            Ok(Some(c)) => c,
            Ok(None) => {
                rep.findings
                    .push(Finding::new("table", None, "no cluster.conf"));
                return Ok(rep);
            }
            // The scrubber: a transient read failure beyond the cap is not a broken table —
            // a failed round (no table means nothing to compare), not a finding.
            Err(e)
                if opts.skip_unreadable
                    && cairn_store::retry::store_verdict(&e)
                        == cairn_store::retry::Verdict::Transient =>
            {
                return Err(e.into());
            }
            Err(e) => {
                rep.findings.push(Finding::new(
                    "table",
                    None,
                    format!("invalid cluster.conf: {e}"),
                ));
                return Ok(rep);
            }
        };
        let t = cluster.table();
        rep.table_version = Some(t.version());

        // Owner records: the tombstone and owner of each range.
        let mut owners: BTreeMap<u32, Option<OwnerRecord>> = BTreeMap::new();
        // An owner record whose read failed **transiently** beyond the cap (tolerant
        // mode): nothing is known about that range — unlike a record with broken content
        // (`None` in `owners`).
        let mut unknown_owner: BTreeSet<u32> = BTreeSet::new();
        for r in inspect::scan_read(|| inspect::owner_ranges(main.store(), main.prefix())).await? {
            rep.items += 1;
            let rec = match inspect::scan_read(|| {
                inspect::owner_observed(main.store(), main.prefix(), r)
            })
            .await
            {
                Ok(o) => o.map(|o| o.record),
                Err(e)
                    if opts.skip_unreadable
                        && cairn_store::retry::store_verdict(&e)
                            == cairn_store::retry::Verdict::Transient =>
                {
                    rep.skipped.push(Skipped::new(
                        "orphan_db",
                        Some(r.0),
                        format!("owners/range-{}", r.0),
                        &e,
                    ));
                    unknown_owner.insert(r.0);
                    continue;
                }
                Err(_) => None,
            };
            owners.insert(r.0, rec);
        }
        let retired: BTreeSet<u32> = owners
            .iter()
            .filter(|(_, r)| r.as_ref().is_some_and(|r| r.retired))
            .map(|(id, _)| *id)
            .collect();

        // The facts of each range for (j) — cross-checked after the round.
        let mut restore_facts = BTreeMap::new();
        // (m), (n) — holder rows and refs of every range.
        let mut value_facts = value_check::Facts::default();
        // (l) — the tenants registry and purged tenants.
        let tenant_book = if opts.open_dbs {
            let main = self.stores.main();
            purge_check::book(main.store(), main.prefix(), &mut rep).await
        } else {
            BTreeMap::new()
        };
        let purged = purge_check::purged(&tenant_book);
        let deleting: BTreeSet<String> = tenant_book
            .iter()
            .filter(|(_, l)| l.state.as_deref() == Some("deleting"))
            .map(|(t, _)| t.as_str().to_string())
            .collect();
        // (b), (c).
        for entry in t.ranges() {
            let r = entry.id;
            rep.ranges_checked += 1;
            let Some(exists) = read_item(
                &opts,
                &mut rep,
                |e| Skipped::new("db", Some(r.0), format!("ranges/{} (manifest)", r.0), e),
                || self.stores.range_db_exists(r),
            )
            .await?
            else {
                continue;
            };
            // No manifest and not known whether the range has an owner: cannot be
            // classified (data loss or never opened) — skipped, already counted above.
            if !exists && unknown_owner.contains(&r.0) {
                continue;
            }
            if !exists {
                rep.findings.push(if owners.contains_key(&r.0) {
                    Finding::new(
                        "db",
                        Some(r.0),
                        "Db has no manifest although the range has an owner — the range data may \
                         be lost",
                    )
                } else {
                    // The first leader creates the Db when it opens the range: with no
                    // owner yet nobody has written anything that could be lost.
                    Finding::new("db", Some(r.0), "Db has no manifest (range never opened?)")
                        .heals("first open")
                });
                continue;
            }
            if opts.open_dbs {
                match self.open_reader(r.0).await {
                    Ok((reader, _tailers)) => {
                        // (i) — the query index matches the metadata. Both areas are read
                        // at one mark. If there are findings, still wait for the reader to
                        // settle and check again, reporting only the recheck: the
                        // `readable` marker is read separately, outside the scan.
                        let mut found = query_index_findings(&reader, r.0, &deleting).await;
                        if found.as_ref().is_ok_and(|f| !f.is_empty()) {
                            settle(&reader).await;
                            found = query_index_findings(&reader, r.0, &deleting).await;
                        }
                        match found {
                            Ok(f) => rep.findings.extend(f),
                            Err(e) => rep.findings.push(Finding::new(
                                "query_index",
                                Some(r.0),
                                format!("cannot read the query index: {e}"),
                            )),
                        }
                        // (j) — the first read; candidates are re-read after the round.
                        if opts.restore_checks {
                            match restore_check::collect(&reader).await {
                                Ok(f) => {
                                    restore_facts.insert(r.0, f);
                                }
                                Err(e) => rep.findings.push(Finding::new(
                                    "db",
                                    Some(r.0),
                                    format!(
                                        "cannot read the Db for the cross-range message check: {e}"
                                    ),
                                )),
                            }
                        }
                        // (l) — rows of purged tenants. A reader still replaying the
                        // shared WAL tail sees rows the delete batch has removed: with
                        // residue, wait for the reader to settle and count again, like (i).
                        let mut left = purge_check::rows(&reader, &purged).await;
                        if left.as_ref().is_ok_and(|l| !l.is_empty()) {
                            settle(&reader).await;
                            left = purge_check::rows(&reader, &purged).await;
                        }
                        match left {
                            Ok(left) => {
                                for (t, n) in left {
                                    rep.findings.push(Finding::new(
                                        purge_check::CHECK,
                                        Some(r.0),
                                        format!(
                                            "tenant {t} is purged but the range still has {n} \
                                                 of its rows"
                                        ),
                                    ));
                                }
                            }
                            Err(e) => rep.findings.push(Finding::new(
                                purge_check::CHECK,
                                Some(r.0),
                                format!("cannot read the Db for the tenant residue check: {e}"),
                            )),
                        }
                        // (m), (n) — compared with the objects after the round.
                        if let Err(e) = value_check::collect(&reader, r.0, &mut value_facts).await {
                            rep.findings.push(Finding::new(
                                value_check::DANGLING,
                                Some(r.0),
                                format!("cannot read the Db for the value check: {e}"),
                            ));
                        }
                        let _ = reader.close().await;
                    }
                    Err(e) => rep.findings.push(Finding::new(
                        "db",
                        Some(r.0),
                        format!("cannot open the Db with the production loader: {e}"),
                    )),
                }
            }
            let Some(refs) = read_item(
                &opts,
                &mut rep,
                |e| {
                    Skipped::new(
                        "external_db",
                        Some(r.0),
                        format!("ranges/{} (external_dbs)", r.0),
                        e,
                    )
                },
                || self.stores.external_db_refs(r),
            )
            .await?
            else {
                continue;
            };
            for e in refs.unwrap_or_default() {
                rep.external_refs_checked += 1;
                let what = match &e.status {
                    // The gap between the two steps of GC detach: stands on no SST.
                    ExternalStatus::Ok | ExternalStatus::Detaching => continue,
                    ExternalStatus::DbMissing => "source Db has no manifest anymore".to_string(),
                    ExternalStatus::CheckpointMissing(id) => {
                        format!("source Db lost checkpoint {id} that this range stands on")
                    }
                    ExternalStatus::Unreadable(err) => {
                        format!("cannot read the source manifest: {err}")
                    }
                };
                let src = e
                    .range
                    .map_or_else(|| e.path.clone(), |s| format!("range {} ({})", s.0, e.path));
                rep.findings.push(Finding::new(
                    "external_db",
                    Some(r.0),
                    format!("external_dbs → {src}: {what}"),
                ));
            }
        }

        // `--deep`: individual objects a live range's Db needs.
        if opts.deep {
            let mut listing = inspect::SstListing::default();
            for entry in t.ranges() {
                let r = entry.id;
                rep.items += 1;
                let missing = match self.stores.missing_objects(r, &mut listing).await {
                    Ok(m) => m,
                    Err(e) if opts.skip_unreadable => {
                        rep.skipped.push(Skipped::new(
                            SST_MISSING,
                            Some(r.0),
                            format!("ranges/{}", r.0),
                            &e,
                        ));
                        continue;
                    }
                    Err(e) => return Err(e.into()),
                };
                for p in missing.unwrap_or_default() {
                    rep.findings.push(Finding::new(
                        SST_MISSING,
                        Some(r.0),
                        format!(
                            "{p} is named by the range's manifest or a live checkpoint but is not \
                             on the bucket — reads that reach it will fail"
                        ),
                    ));
                }
            }
        }

        // (m), (n) — segments in the bucket against holders in the Dbs.
        if opts.open_dbs {
            let main = self.stores.main();
            value_check::findings(
                main.store(),
                main.prefix(),
                &value_facts,
                &deleting,
                cairn_store::entropy::now_ms(),
                &mut rep,
            )
            .await?;
        }

        // (j) — cross-checked between ranges.
        if !restore_facts.is_empty() {
            rep.findings
                .extend(self.restore_findings(&t, &restore_facts).await);
        }

        // (d) — in the table, carrying a tombstone, or a child promised by an intent ⇒ not an
        //     orphan.
        let (dirs, dir_skips) = self.stores.range_dirs_scan().await?;
        rep.items += dirs.len() + dir_skips.len();
        if let Some(s) = dir_skips.first().filter(|_| !opts.skip_unreadable) {
            return Err(StoreError::Io(s.error.clone()).into());
        }
        rep.skipped
            .extend(dir_skips.into_iter().map(|s| Skipped::scan("orphan_db", s)));
        rep.range_dirs = dirs.len();
        // For (h): ids of the swept-tombstone ledger whose `ranges/<id>` has no object
        // left (and was not skipped for a failed read) certainly have no Db — the D-d plan
        // of (h) drops them, changing no finding.
        let with_objects: BTreeSet<u32> = dirs
            .iter()
            .map(|d| d.0)
            .chain(rep.skipped.iter().filter_map(|s| s.range))
            .collect();
        let promised = promised(&t);
        for d in dirs {
            if t.contains(d) || retired.contains(&d.0) || promised.contains(&d.0) {
                continue;
            }
            // The owner record could not be read yet: already counted as skipped above.
            if unknown_owner.contains(&d.0) {
                continue;
            }
            // The loop writing tombstones on behalf of others (SM-11) writes a tombstone
            // when the owner died; the orphan child cleanup loop (SM-14) cleans Dbs that
            // never entered the table. An unreadable owner record makes both keep it — no
            // loop heals it.
            let (why, heals) = match owners.get(&d.0) {
                Some(Some(_)) => (
                    "has an owner record but is not in the table, no tombstone",
                    Some("SM-11"),
                ),
                Some(None) => ("owner record unreadable, not in the table", None),
                None => ("not in the table, no owner record", Some("SM-14")),
            };
            let f = Finding::new(
                "orphan_db",
                Some(d.0),
                format!("ranges/{} orphan: {why}", d.0),
            );
            rep.findings.push(match heals {
                Some(by) => f.heals(by),
                None => f,
            });
        }

        // (f) — a final checkpoint on a **live** range created by the clone of a child that
        // is permanently out of the table (not in the table, not promised, id already
        // assigned): that child never committed (the commit takes the source out of the
        // table), so the checkpoint pins the live range's SSTs with nobody reading them.
        // Recognised by the checkpoint name (the child Db's path); a checkpoint without a
        // name — created before the fork named them — cannot be judged.
        for entry in t.ranges() {
            let Some(checkpoints) = read_item(
                &opts,
                &mut rep,
                |e| {
                    Skipped::new(
                        "orphan_checkpoint",
                        Some(entry.id.0),
                        format!("ranges/{} (checkpoint)", entry.id.0),
                        e,
                    )
                },
                || self.stores.range_checkpoints(entry.id),
            )
            .await?
            else {
                continue;
            };
            for c in checkpoints {
                let Some(child) = c.created_by else { continue };
                if c.expires
                    || t.contains(child)
                    || promised.contains(&child.0)
                    || child.0 >= t.next_range_id()
                {
                    continue;
                }
                let Some(child_db) = read_item(
                    &opts,
                    &mut rep,
                    |e| {
                        Skipped::new(
                            "orphan_checkpoint",
                            Some(entry.id.0),
                            format!("ranges/{} (manifest)", child.0),
                            e,
                        )
                    },
                    || self.stores.range_db_exists(child),
                )
                .await?
                else {
                    continue;
                };
                let db = if child_db {
                    "the child Db still exists — the orphan child cleanup loop (SM-14) will remove \
                     it"
                } else {
                    "the child Db is gone — no loop will see this checkpoint anymore"
                };
                let f = Finding::new(
                    "orphan_checkpoint",
                    Some(entry.id.0),
                    format!(
                        "checkpoint {} created by the clone of orphan child {} pins SSTs of a live \
                         range ({db})",
                        c.id, child.0
                    ),
                );
                rep.findings
                    .push(if child_db { f.heals("SM-14") } else { f });
            }
        }

        // (e) — intents carry no timestamp: the `last_modified` of cluster.conf is a lower
        // bound of their age.
        let modified = read_item(
            &opts,
            &mut rep,
            |e| Skipped::new("stuck_intent", None, "cluster.conf (HEAD)", e),
            || cluster.stored_modified_ms(),
        )
        .await?
        .flatten();
        if let Some(modified) = modified {
            let age = Duration::from_millis(opts.now_ms.saturating_sub(modified));
            for entry in t.ranges() {
                let Some(i) = entry.intent else { continue };
                if age >= opts.stuck_after {
                    rep.findings.push(Finding::new(
                        "stuck_intent",
                        Some(entry.id.0),
                        format!(
                            "intent {} stuck for at least {} s (limit {} s) — `cairn-ctl intent \
                             clear {}`",
                            intent_text(&i),
                            age.as_secs(),
                            opts.stuck_after.as_secs(),
                            entry.id.0
                        ),
                    ));
                }
            }
        }

        // (g) — the log of a finished incarnation (or one whose session record was cleaned
        // up) that no owner record, tombstones included, still lists: the dead log sweep
        // (NW-8) must seal it and then clean it up for good. An unreadable owner record ⇒
        // no conclusion (it may be the one listing the log).
        if opts.wal_logs {
            let listed: BTreeSet<SessionId> = owners
                .values()
                .flatten()
                .flat_map(|o| o.wal_segments.iter().map(|s| s.log.session.clone()))
                .collect();
            let unreadable_owner = owners.values().any(Option::is_none);
            let scan = cairn_store::nodewal::inspect::incarnation_logs(&self.stores).await?;
            rep.items += scan.logs.len() + scan.skipped.len();
            if let Some(s) = scan.skipped.first().filter(|_| !opts.skip_unreadable) {
                return Err(StoreError::Io(s.error.clone()).into());
            }
            rep.skipped.extend(
                scan.skipped
                    .into_iter()
                    .map(|s| Skipped::scan("unheld_wal_log", s)),
            );
            // An owner record not read yet (transiently) may be exactly what lists the log:
            // no conclusion about any log is possible — the whole check skips this round.
            if !unknown_owner.is_empty() && opts.skip_unreadable {
                rep.skipped.push(Skipped::new(
                    "unheld_wal_log",
                    None,
                    format!("{} owner records not yet readable", unknown_owner.len()),
                    "depends on skipped owner records",
                ));
            }
            let logs = if unknown_owner.is_empty() {
                scan.logs
            } else {
                Vec::new()
            };
            for l in logs {
                let (Ok(state), Some(objects)) = (l.state, l.objects) else {
                    continue;
                };
                if objects == 0 || listed.contains(&l.session) || unreadable_owner {
                    continue;
                }
                let state = state.map_or("already cleaned up".to_string(), |s| {
                    state_text(s).to_string()
                });
                rep.findings.push(
                    Finding::new(
                        "unheld_wal_log",
                        None,
                        format!(
                            "{} (session {state}) still has {objects} objects not listed by any \
                             owner record",
                            wal_log_subject(&l.session.to_string())
                        ),
                    )
                    .heals("NW-8"),
                );
            }
        }

        // (h) — exactly the set D-d would delete with `min_age` = `retired_after`: a
        // tombstone old enough, nobody referencing it, no fence covering it. Still there
        // after a D-d round means D-d is stuck (or off while the operator thinks it is on).
        if let Some(after) = opts.retired_after {
            let plan = read_item(
                &opts,
                &mut rep,
                |e| Skipped::new("retired_unreferenced", None, "D-d plan", e),
                || async {
                    let swept = cairn_store::gc_swept::read(main.store(), main.prefix()).await;
                    let skip: BTreeSet<u32> = swept
                        .ids()
                        .iter()
                        .copied()
                        .filter(|id| !with_objects.contains(id))
                        .collect();
                    crate::gc::plan_skipping(&self.stores, &cluster, after, opts.now_ms, &skip)
                        .await
                },
            )
            .await?;
            // Ranges the plan could not read: skipped like every other failed verify item,
            // or the whole command fails without `skip_unreadable` — as before D-d
            // tolerated per-item failures.
            for b in plan.iter().flat_map(|p| &p.blocked) {
                if b.by != crate::gc::BlockedBy::Unreadable {
                    continue;
                }
                if !opts.skip_unreadable {
                    return Err(StoreError::Io(format!(
                        "D-d plan cannot read range {}: {}",
                        b.range, b.detail
                    ))
                    .into());
                }
                rep.skipped.push(Skipped::new(
                    "retired_unreferenced",
                    Some(b.range),
                    "D-d plan",
                    &b.detail,
                ));
            }
            for e in plan.map(|p| p.eligible).unwrap_or_default() {
                rep.findings.push(
                    Finding::new(
                        "retired_unreferenced",
                        Some(e.range),
                        format!(
                            "retired Db referenced by nobody, tombstone {} s, {} bytes — D-d has \
                             not deleted it",
                            e.age_ms / 1000,
                            e.bytes
                        ),
                    )
                    .heals("D-d"),
                );
            }
        }
        // (k) — audit objects; not in the background scrubber.
        if opts.open_dbs {
            audit_check::audit_findings(main.store(), main.prefix(), &opts, &mut rep).await?;
            // (l) — the tenant's objects and hold entries.
            purge_check::findings(main.store(), main.prefix(), &tenant_book, &opts, &mut rep)
                .await?;
        }
        Ok(rep)
    }

    /// Whether the owner of `r` is still alive — `Some(description)` when it is.
    async fn live_owner(&self, r: PartitionId, now: u64) -> CtlResult<Option<String>> {
        let main = self.stores.main();
        let Some(o) = inspect::owner_observed(main.store(), main.prefix(), r).await? else {
            return Ok(None);
        };
        if o.record.released {
            return Ok(None);
        }
        let (state, idle) = self.session_state(&o.record.session, now).await?;
        Ok((state == "live").then(|| {
            format!(
                "range {}: owner {} still has a live session (record idle {} ms)",
                r.0,
                o.record.session,
                idle.unwrap_or(0)
            )
        }))
    }

    /// `intent clear <range>` — rolls back an uncommitted split/merge like
    /// [`crate::split::roll_back`]: a CAS dropping the intent (once, on the expected
    /// version), then cleaning up the child ranges' Dbs and owner records.
    pub async fn intent_clear(&self, range: u32, opts: &RepairOptions) -> CtlResult<RepairReport> {
        let p = PartitionId(range);
        let cluster = self.cluster().await?;
        let t = cluster.table();
        if let Some(v) = opts.expect_version {
            if v != t.version() {
                return Err(CtlError::Refused(format!(
                    "range table is at version {}, expected {v}",
                    t.version()
                )));
            }
        }
        let entry = t
            .range(p)
            .copied()
            .ok_or_else(|| CtlError::Refused(format!("range {range} is not in the table")))?;
        let intent = entry
            .intent
            .ok_or_else(|| CtlError::Refused(format!("range {range} has no intent")))?;
        let (clear, children): (Vec<PartitionId>, Vec<PartitionId>) = match intent {
            RangeIntent::Splitting { left, right, .. } => (vec![p], vec![left, right]),
            RangeIntent::Merging { with, into } => (vec![p, with], vec![into]),
        };
        let mut warnings = Vec::new();
        let mut live = Vec::new();
        for r in clear.iter().chain(&children) {
            if let Some(l) = self.live_owner(*r, opts.now_ms).await? {
                live.push(l);
            }
        }
        // The core decides (`Found(Ctl)`): the owner still alive without `--force` ⇒
        // refused; a prepared op whose commit may have won ⇒ refused.
        let finder = cairn_split::Finder::Ctl {
            live_owner: !live.is_empty(),
            force: opts.force,
        };
        match crate::split::core::found_with(&self.stores, p, intent, finder).await {
            cairn_split::Plan::Refused if !live.is_empty() => {
                return Err(CtlError::Refused(format!(
                "{} — the coordinator may still be running; add --force if you are sure it is dead",
                live.join("; ")
            )))
            }
            cairn_split::Plan::Refused => {
                return Err(CtlError::Refused(format!(
                    "ops/{} is prepared and every child has a Db and an owner record, or the \
                     ledger could not be read — the commit may have won (the table may have been \
                     rebuilt from an older history version); clearing the intent would move the \
                     children's Db, with acknowledged writes, to the trash. Check `table show`, \
                     `verify` and the children first; add --force only if you are sure the commit \
                     did not win",
                    cairn_store::ledger::op_of(p, intent).key()
                )))
            }
            cairn_split::Plan::Abort { .. } => {
                warnings.extend(live.iter().map(|l| format!("--force: {l}")));
            }
            other => unreachable!("core returned {other:?} for Found(Ctl)"),
        }
        let next = |cur: &PartitionTable| {
            let ranges = cur
                .ranges()
                .iter()
                .map(|r| {
                    if clear.contains(&r.id) {
                        RangeEntry { intent: None, ..*r }
                    } else {
                        *r
                    }
                })
                .collect();
            PartitionTable::from_entries(cur.version() + 1, cur.next_range_id(), ranges)
        };
        let after = next(&t)?.with_extra(t.extra());
        let cleanup: Vec<String> = children
            .iter()
            .flat_map(|c| {
                [
                    format!(
                        "delete Db ranges/{} (and the checkpoint it left on the parent) if present",
                        c.0
                    ),
                    format!("delete owners/range-{} if present", c.0),
                ]
            })
            .collect();
        let mut rep = RepairReport {
            applied: false,
            object: "cluster.conf".into(),
            before: t.encode(),
            after: after.encode(),
            cleanup,
            warnings,
            audit: None,
        };
        if !opts.apply {
            return Ok(rep);
        }
        // The log goes to `Aborting` before the CAS, like the node's rollback path.
        let op = crate::split::core::op_of(p, intent);
        crate::split::mark_ledger(&self.stores, op, cairn_split::Phase::Aborting).await?;
        // The sources' close marks are read **before** the CAS: the marker of a later
        // intent (closing the source again after this CAS) has a higher epoch, and the
        // removal is fenced.
        let marks = crate::split::close_marks(&self.stores, &clear).await;
        let cleared = cluster.update(t.version(), |cur| next(cur)).await;
        let cas = crate::split::core::cas(&cleared);
        let won = cleared?;
        let (ledger, reopen) = match cairn_split::step(
            &cairn_split::View::default(),
            &cairn_split::Event::Cleared {
                cas,
                by: cairn_split::Who::Ctl,
            },
        ) {
            cairn_split::Plan::Cleanup { ledger, reopen } => (ledger, reopen),
            // The table just changed: ctl does not retry — the operator looks at the new state.
            cairn_split::Plan::Reread => {
                let TableUpdate::Conflict(won) = won else {
                    unreachable!("Reread only for a lost CAS")
                };
                return Err(CtlError::Refused(format!(
                    "table just changed (version {} → {}) — run again to see the new state",
                    t.version(),
                    won.version()
                )));
            }
            other => unreachable!("core returned {other:?} for Cleared(Ctl)"),
        };
        rep.applied = true;
        if let Some(to) = ledger {
            if let Err(e) = crate::split::mark_ledger(&self.stores, op, to).await {
                rep.warnings
                    .push(format!("cannot write ops/{} Cleaned: {e}", op.key()));
            }
        }
        // Remove the close markers from the sources (SM-17) — after the CAS, like the
        // node's rollback path: by the mark read before the CAS, as the core says
        // (`Reopen::AtMark`). A failure or a fence only logs (in `reopen_sources`): the
        // next Db open removes the old marker by itself.
        cairn_store::failpoint!("ctl::intent_clear::before_reopen");
        rep.warnings
            .extend(crate::split::reopen_sources(&self.stores, &marks, reopen).await);
        for c in &children {
            // A failed Db cleanup keeps the owner record: the Db is still evidence of its
            // checkpoint on the source range; the orphan child cleanup loop continues.
            if let Err(e) = crate::split::discard_abandoned_child(
                &self.stores,
                &cluster,
                *c,
                &clear,
                cairn_split::Remover::Ctl,
            )
            .await
            {
                rep.warnings.push(format!(
                    "child range {} not fully cleaned up (Db first, owner record only once the Db \
                     is gone) — the orphan child cleanup loop continues: {e}",
                    c.0
                ));
            }
        }
        rep.audit = Some(self.audit(opts, &rep).await?);
        Ok(rep)
    }

    /// `trash list`: every trash entry on every store, oldest first.
    pub async fn trash_list(&self, now_ms: u64) -> CtlResult<Vec<TrashEntryView>> {
        let mut out = Vec::new();
        for (store, bin) in self.trash_bins() {
            for e in bin.entries().await? {
                out.push(TrashEntryView {
                    id: e.id.to_string(),
                    store,
                    subject: e.subject.as_ref().map(ToString::to_string),
                    reason: e.reason,
                    objects: e.objects,
                    bytes: e.bytes,
                    age_ms: now_ms.saturating_sub(e.id.created_ms()),
                    unreadable: e.unreadable,
                });
            }
        }
        out.sort_by(|a, b| a.id.cmp(&b.id).then(a.store.cmp(b.store)));
        Ok(out)
    }

    fn trash_bins(&self) -> Vec<(&'static str, cairn_store::trash::Trash)> {
        let bins = self.stores.trash_bins();
        ["main", "wal"].into_iter().zip(bins).collect()
    }

    /// `trash restore <id> | --range <r>`: copies the entry's objects back to their
    /// original place with `Create` — overwriting nothing that exists. With `--range`, the
    /// owner record comes back **before** the Db, so that a Db never shows up without its
    /// owner record (SM-14 takes that as evidence of an orphan child). Within one id, the
    /// entry on the WAL store comes back before the entry on the main store — the entry
    /// carrying the manifest comes back last, the reverse of the order D-d moved them out
    /// (`delete_range_objects`), so that a Db does not "appear" before it has enough
    /// objects. Restoring **one** entry of a Db that still has other entries (a Db moved
    /// half-way in several passes) warns: the restored Db may be missing objects in the
    /// other entry.
    ///
    /// Refused while the range's Db exists at the original place (unless `--force`):
    /// copying an old manifest into a live Db restores nothing and muddles its history.
    pub async fn trash_restore(
        &self,
        target: &TrashTarget,
        opts: &RepairOptions,
    ) -> CtlResult<RepairReport> {
        use cairn_store::trash::Subject;
        let mut picked = Vec::new();
        for (store, bin) in self.trash_bins() {
            for e in bin.entries().await? {
                let hit = match (target, &e.subject) {
                    (TrashTarget::Entry(id), _) => e.id.as_str() == id,
                    (TrashTarget::Range(r), Some(Subject::RangeDb(x) | Subject::Owner(x))) => {
                        x == r
                    }
                    _ => false,
                };
                if hit {
                    picked.push((store, bin.clone(), e));
                }
            }
        }
        if picked.is_empty() {
            return Err(CtlError::Bad(format!("trash has no entry for {target:?}")));
        }
        // Owner records first, Dbs after; within each kind, older entries first; for the
        // same id, the WAL store before the main store (the manifest last).
        picked.sort_by_key(|(store, _, e)| {
            (
                !matches!(e.subject, Some(Subject::Owner(_))),
                e.id.clone(),
                *store == "main",
            )
        });
        let mut warnings = Vec::new();
        if matches!(target, TrashTarget::Entry(_)) {
            let dbs: std::collections::BTreeSet<u32> = picked
                .iter()
                .filter_map(|(_, _, e)| match e.subject {
                    Some(Subject::RangeDb(r)) => Some(r),
                    _ => None,
                })
                .collect();
            for r in dbs {
                let mut others = Vec::new();
                for (store, bin) in self.trash_bins() {
                    for e in bin.entries().await? {
                        let theirs = matches!(e.subject, Some(Subject::RangeDb(x)) if x == r);
                        if theirs && !picked.iter().any(|(_, _, p)| p.id == e.id) {
                            others.push(format!("{} [{store}]", e.id));
                        }
                    }
                }
                if !others.is_empty() {
                    warnings.push(format!(
                        "the Db of range {r} has other trash entries ({}): restoring this entry \
                         alone may bring the Db back without objects held there — use `trash \
                         restore --range {r}`",
                        others.join(", ")
                    ));
                }
            }
        }
        for (_, _, e) in &picked {
            if let Some(Subject::RangeDb(r)) = &e.subject {
                if self.stores.range_db_exists(PartitionId(*r)).await? {
                    let why = format!("the Db of range {r} exists at ranges/{r}");
                    if !opts.force {
                        return Err(CtlError::Refused(format!(
                            "{why}; add --force if you are sure you want to copy old objects into \
                             it"
                        )));
                    }
                    warnings.push(format!("--force: {why}"));
                }
            }
            if let Some(why) = &e.unreadable {
                warnings.push(format!(
                    "entry {} unreadable ({why}) — copying the objects back anyway",
                    e.id
                ));
            }
        }
        let describe = |(store, _, e): &(
            &str,
            cairn_store::trash::Trash,
            cairn_store::trash::EntryView,
        )| {
            format!(
                "{} [{store}] {} — {} object, {} B ({})",
                e.id,
                e.subject
                    .as_ref()
                    .map_or("?".to_string(), ToString::to_string),
                e.objects,
                e.bytes,
                e.reason
            )
        };
        let mut rep = RepairReport {
            applied: false,
            object: "trash/".into(),
            before: picked.iter().map(describe).collect::<Vec<_>>().join("\n"),
            after: String::new(),
            cleanup: Vec::new(),
            warnings,
            audit: None,
        };
        if !opts.apply {
            rep.after =
                "the objects above back to where they were (Create, no overwrite); the entry is \
                 removed from the trash"
                    .into();
            return Ok(rep);
        }
        let mut after = Vec::new();
        for (store, bin, e) in &picked {
            let r = bin.restore(&e.id).await?;
            after.push(format!(
                "{} [{store}]: {} objects back in place{}",
                e.id,
                r.restored,
                if r.conflicts.is_empty() {
                    String::new()
                } else {
                    format!(
                        ", {} places already hold another object (not overwritten, entry kept): {}",
                        r.conflicts.len(),
                        r.conflicts.join(", ")
                    )
                }
            ));
        }
        rep.after = after.join("\n");
        rep.applied = true;
        rep.audit = Some(self.audit(opts, &rep).await?);
        Ok(rep)
    }

    /// `range force-retire <id>`: writes a tombstone for a range that **has left the
    /// table** (a parent range whose split/merge did not get to write its tombstone, or an
    /// orphan Db `verify` reports) — from then on D-d may delete its Db when nobody
    /// references it, and nobody claims it again.
    pub async fn force_retire(&self, range: u32, opts: &RepairOptions) -> CtlResult<RepairReport> {
        let p = PartitionId(range);
        let t = self.cluster().await?.table();
        if t.contains(p) {
            return Err(CtlError::Refused(format!(
                "range {range} is still in the range table — only a range that left the table can \
                 be retired"
            )));
        }
        if promised(&t).contains(&range) {
            return Err(CtlError::Refused(format!(
                "range {range} is a child range promised by an intent in the table — `intent \
                 clear` first"
            )));
        }
        if range >= t.next_range_id() {
            return Err(CtlError::Refused(format!(
                "range {range} was never allocated (next_range_id={})",
                t.next_range_id()
            )));
        }
        let main = self.stores.main();
        let observed = inspect::owner_observed(main.store(), main.prefix(), p).await?;
        let mut warnings = Vec::new();
        if let Some(o) = &observed {
            if o.record.retired {
                return Err(CtlError::Refused(format!(
                    "range {range} is already a tombstone"
                )));
            }
        }
        if let Some(live) = self.live_owner(p, opts.now_ms).await? {
            if !opts.force {
                return Err(CtlError::Refused(format!(
                    "{live}; add --force if you are sure it is dead"
                )));
            }
            warnings.push(format!("--force: {live}"));
        }
        let record = match &observed {
            Some(o) => OwnerRecord {
                released: true,
                retired: true,
                retired_ms: Some(opts.now_ms),
                wal_segments: crate::gc::tomb_segments(),
                ..o.record.clone()
            },
            None => {
                let wal_kind = self
                    .stores
                    .range_wal_kind(p)
                    .await?
                    .unwrap_or(WalKind::Shared);
                OwnerRecord {
                    term: 0,
                    session: SessionId {
                        node: "cairn-ctl".into(),
                        inc: opts.now_ms,
                    },
                    address: None,
                    released: true,
                    wal_kind,
                    retired: true,
                    wal_segments: Vec::new(),
                    wal_floor_id: None,
                    data: None,
                    retired_ms: Some(opts.now_ms),
                }
            }
        };
        let mut rep = RepairReport {
            applied: false,
            object: format!("owners/range-{range}"),
            before: observed
                .as_ref()
                .map(|o| String::from_utf8_lossy(&o.record.encode()).into_owned())
                .unwrap_or_default(),
            after: String::from_utf8_lossy(&record.encode()).into_owned(),
            cleanup: Vec::new(),
            warnings,
            audit: None,
        };
        if !opts.apply {
            return Ok(rep);
        }
        let expected = observed.map(|o| o.version);
        if !inspect::cas_owner(main.store(), main.prefix(), p, &record, expected).await? {
            return Err(CtlError::Refused(format!(
                "owners/range-{range} just changed — run again to see the new state"
            )));
        }
        rep.applied = true;
        rep.audit = Some(self.audit(opts, &rep).await?);
        Ok(rep)
    }

    // ─────────────────────────────────────────── quiet renewal gate

    /// Every session in the bucket (one LIST + one GET per session).
    async fn sessions(&self, now: u64) -> CtlResult<Vec<SessionView>> {
        let main = self.stores.main();
        let mut out = Vec::new();
        for id in inspect::session_ids(main.store(), main.prefix()).await? {
            let Some((rec, modified)) =
                inspect::session_record(main.store(), main.prefix(), &id).await?
            else {
                continue;
            };
            let idle_ms = now.saturating_sub(modified);
            out.push(match rec {
                Ok(r) => SessionView {
                    session: id.to_string(),
                    state: state_text(r.state),
                    knows: r.knows,
                    mode: match &r.mode {
                        SessionMode::Direct => "direct".into(),
                        SessionMode::Solo => "solo".into(),
                        SessionMode::Delegated { coord, term } => {
                            format!("delegated coord={coord}:{term}")
                        }
                        SessionMode::Other(m) => m.clone(),
                    },
                    challenged: r.challenged,
                    idle_ms,
                },
                Err(_) => SessionView {
                    session: id.to_string(),
                    state: "corrupt".into(),
                    knows: None,
                    mode: "?".into(),
                    challenged: None,
                    idle_ms,
                },
            });
        }
        Ok(out)
    }

    /// `cluster status`.
    pub async fn cluster_status(&self, now: u64) -> CtlResult<ClusterStatus> {
        let main = self.stores.main();
        let cv = migrate::read_cluster_version(main.store(), main.prefix())
            .await?
            .map(|(cv, _)| cv)
            .unwrap_or_default();
        let gate = quiet_lease::read(main.store(), main.prefix())
            .await?
            .and_then(|(q, _)| q)
            .unwrap_or_default();
        let evidence = quiet_lease::EVIDENCE_ITEMS
            .into_iter()
            .map(|k| {
                let e = gate.evidence.get(k);
                EvidenceView {
                    item: k,
                    title: quiet_lease::evidence_title(k),
                    note: e.map(|e| e.note.clone()),
                    by: e.map(|e| e.by.clone()),
                    at_ms: e.map(|e| e.at_ms),
                }
            })
            .collect();
        Ok(ClusterStatus {
            version: cv.version,
            min_binary: cv.min_binary,
            upgrading: cv.upgrading.as_ref().map(|u| u.to_string()),
            quiet_lease: gate.on,
            quiet_lease_knows: migrate::QUIET_LEASE,
            delegation: gate.on && gate.delegation,
            evidence,
            evidence_missing: gate.evidence.missing(),
            delegation_unproven: gate.delegation_unproven.as_ref().map(unproven_text),
            call_prefix: cv.call_prefix_on(),
            call_prefix_knows: migrate::CALL_PREFIX,
            call_placement: cv.call_placement_on(),
            call_placement_knows: migrate::CALL_PLACEMENT,
            streams: cv.streams_on(),
            streams_knows: migrate::STREAMS_TABLE,
            schedules: cv.schedules_on(),
            schedules_knows: migrate::SCHEDULES_TABLE,
            key_roles: cv.key_roles_on(),
            key_roles_knows: migrate::KEY_ROLES,
            tenant_state: cv.tenant_state_on(),
            tenant_state_knows: migrate::TENANT_STATE,
            audit: cv.audit_on(),
            audit_knows: migrate::AUDIT_TABLE,
            tenant_purge: cv.tenant_purge_on(),
            tenant_purge_knows: migrate::TENANT_PURGE,
            values: cv.values_on(),
            values_knows: migrate::VALUES,
            sessions: self.sessions(now).await?,
        })
    }

    /// Unfinished sessions whose binary does not know gate step `gate` yet — the reason
    /// to refuse turning it on.
    async fn sessions_below_gate(&self, now: u64, gate: u32) -> CtlResult<Vec<String>> {
        Ok(self
            .sessions(now)
            .await?
            .into_iter()
            .filter(|s| s.state != "expired" && s.state != "closed")
            .filter(|s| s.knows.is_none_or(|k| k < gate))
            .map(|s| {
                format!(
                    "{} ({}, knows={}, idle {} ms)",
                    s.session,
                    s.state,
                    s.knows.map_or("-".into(), |k| k.to_string()),
                    s.idle_ms
                )
            })
            .collect())
    }

    /// `cluster enable-quiet-lease`. Refused while there is an unfinished session
    /// (including `corrupt`) of a binary that does not know [`migrate::QUIET_LEASE`] — a
    /// `live` session of a dead node blocks too, until someone marks it expired (≤ one TTL
    /// and one observation tick while the cluster has nodes). Then, with `--apply`:
    ///
    /// 1. CAS `min_binary = K` into `cluster-version` (from then on old binaries cannot
    ///    start — `migrate::ensure_known`); a migration lock being held ⇒ refused;
    /// 2. **wait [`BOOT_WAIT`]** — unless the gate was already on before (only adding
    ///    `--delegation`: that earlier enabling already waited): an old binary that passed
    ///    the `min_binary` check before step 1 and is still starting has, within this
    ///    window, either opened its session or given up;
    /// 3. **check** the sessions **again**: an old binary that passed the `min_binary`
    ///    check before step 1 and opened its session after the first check shows up here
    ///    — refused, the gate is not on (an already set `min_binary` stays as it is: lower
    ///    it with `lower-min-binary`);
    /// 4. write `control/quiet-lease` = `on`.
    ///
    /// **Before step 1**: a self-test of the store — the conditional GET probe
    /// ([`store_probe::conditional_get`], assumption G18) runs [`store_probe::ROUNDS`]
    /// rounds on `control/probe/<nonce>` of the bucket itself, reading with the writing
    /// client and a second client ([`Self::second_client`]). A store returning a stale
    /// `304` or a stale read (a caching proxy, a weakly consistent store) ⇒ refused,
    /// nothing written: solo mode (LS-16) and the read fence (LS-5) rely on that
    /// assumption. A transient I/O error ⇒ refused as "inconclusive, run again". A dry run
    /// does not probe (writes nothing, not even the probe object).
    ///
    /// The case of an old binary that is starting, has read `cluster-version` before step
    /// 1 and has not opened its session yet is closed by step 2. What remains is not the
    /// boot path: an old binary process **already running** whose session ended at both
    /// checks (a long pause, expired) may wake up and open a new incarnation within the
    /// process without re-reading `cluster-version` — the runtime rule (the coordinator
    /// sees `knows` < K ⇒ the whole cluster falls back) covers it, LS-4/LS-5 cover
    /// safety.
    pub async fn enable_quiet_lease(&self, opts: &RepairOptions) -> CtlResult<RepairReport> {
        self.enable_quiet_lease_with(opts, false, false).await
    }

    /// `cluster record-evidence <a|b|c|d|e> --note …`: writes (or overwrites) one
    /// evidence item of the delegation done-gate into `control/quiet-lease`, with who ran
    /// it and when. **Turns nothing on**: the gate state and the delegation flag stay as
    /// they are; a bucket without the object gets a `state=off` object carrying that item.
    /// Written with a CAS on the version just read ([`quiet_lease::write`]; an ambiguous
    /// outcome is resolved by reading back — the content carries the `by`, `at_ms` and
    /// note of this run).
    pub async fn record_evidence(
        &self,
        item: char,
        note: &str,
        opts: &RepairOptions,
    ) -> CtlResult<RepairReport> {
        if !quiet_lease::EVIDENCE_ITEMS.contains(&item) {
            return Err(CtlError::Bad(format!(
                "unknown evidence item {item:?} — only a, b, c, d, e"
            )));
        }
        let note = note.trim();
        if note.is_empty() {
            return Err(CtlError::Bad(
                "empty --note — record the commit, command or log, who, when".into(),
            ));
        }
        let main = self.stores.main();
        let gate = quiet_lease::read(main.store(), main.prefix()).await?;
        let had = gate.as_ref().and_then(|(q, _)| q.clone());
        let mut next = had.clone().unwrap_or_else(|| QuietLease {
            on: false,
            knows: migrate::QUIET_LEASE,
            by: opts.who.clone(),
            at_ms: opts.now_ms,
            ..Default::default()
        });
        next.evidence.record(
            item,
            quiet_lease::EvidenceEntry {
                at_ms: opts.now_ms,
                by: opts.who.clone(),
                note: note.to_string(),
            },
        );
        let mut rep = RepairReport {
            applied: false,
            object: quiet_lease::OBJECT.into(),
            before: match &gate {
                None => format!("({} absent)", quiet_lease::OBJECT),
                Some((Some(q), _)) => String::from_utf8_lossy(&q.encode()).into_owned(),
                Some((None, _)) => format!("({} unreadable)", quiet_lease::OBJECT),
            },
            after: String::from_utf8_lossy(&next.encode()).into_owned(),
            cleanup: Vec::new(),
            warnings: Vec::new(),
            audit: None,
        };
        if matches!(gate, Some((None, _))) {
            // An object that exists but cannot be read (unknown or newer header): overwriting
            // would erase something we do not understand — leave it to the operator.
            return Err(CtlError::Refused(format!(
                "{} exists but cannot be read (unknown or newer header) — not overwriting; use a \
                 newer cairn-ctl or inspect the object by hand",
                quiet_lease::OBJECT
            )));
        }
        if let Some(old) = had.as_ref().and_then(|q| q.evidence.get(item)) {
            rep.warnings.push(format!(
                "overwriting old item ({item}): {:?} — by {} at at_ms={}",
                old.note, old.by, old.at_ms
            ));
        }
        let missing = next.evidence.missing();
        rep.warnings.push(if missing.is_empty() {
            "all five items (a)–(e) present — enable-quiet-lease --delegation can enable delegation"
                .into()
        } else {
            format!(
                "still missing {} — enable-quiet-lease --delegation still refuses",
                items_text(&missing)
            )
        });
        if !opts.apply {
            return Ok(rep);
        }
        if !quiet_lease::write(main.store(), main.prefix(), &next, gate.map(|(_, v)| v)).await? {
            return Err(CtlError::Refused(format!(
                "{} just changed — run again to see the new state",
                quiet_lease::OBJECT
            )));
        }
        rep.applied = true;
        rep.audit = Some(self.audit(opts, &rep).await?);
        Ok(rep)
    }

    /// Like [`Self::enable_quiet_lease`]; `delegation` ⇒ also writes `delegation=on`
    /// (`--delegation`): the coordinator only grants renewal delegation when that flag is
    /// on — without it the gate only turns on solo mode. With delegation already on,
    /// running again without `--delegation` does not turn it off (turn it off with
    /// `disable-quiet-lease`). The done-gate evidence (`evidence=`) stays as it is.
    ///
    /// **The evidence gate**: `delegation` while `control/quiet-lease` does not yet record
    /// all five items (a)–(e) ([`Self::record_evidence`]) ⇒ refused, listing the missing
    /// items, nothing written (dry run included) — unless `accept_missing_evidence`
    /// (`--accept-missing-evidence`, for staging/chaos/tests): turned on, with a warning,
    /// and a `delegation_unproven=` marker (missing items, who, when) written into the
    /// object.
    pub async fn enable_quiet_lease_with(
        &self,
        opts: &RepairOptions,
        delegation: bool,
        accept_missing_evidence: bool,
    ) -> CtlResult<RepairReport> {
        let main = self.stores.main();
        let below = self
            .sessions_below_gate(opts.now_ms, migrate::QUIET_LEASE)
            .await?;
        if !below.is_empty() {
            return Err(CtlError::Refused(format!(
                "there are unfinished sessions of binaries that do not know step {} — upgrade the \
                 binary (or wait for the dead session to be marked) and run again: {}",
                migrate::QUIET_LEASE,
                below.join("; ")
            )));
        }
        let (cv, tag) = match migrate::read_cluster_version(main.store(), main.prefix()).await? {
            Some((cv, tag)) => (cv, Some(tag)),
            None => (migrate::ClusterVersion::default(), None),
        };
        if let Some(u) = &cv.upgrading {
            return Err(CtlError::Refused(format!(
                "a cluster-level migration is in progress ({u}) — wait for it to finish and run \
                 again"
            )));
        }
        if cv.version > migrate::KNOWS || cv.min_binary > migrate::KNOWS {
            return Err(CtlError::Refused(format!(
                "cluster is at version={} min_binary={}, this cairn-ctl only knows up to {} — use \
                 a newer cairn-ctl",
                cv.version,
                cv.min_binary,
                migrate::KNOWS
            )));
        }
        let gate = quiet_lease::read(main.store(), main.prefix()).await?;
        let raised = migrate::ClusterVersion {
            min_binary: cv.min_binary.max(migrate::QUIET_LEASE),
            ..cv.clone()
        };
        let had = gate.as_ref().and_then(|(q, _)| q.clone());
        let evidence = had.as_ref().map(|q| q.evidence.clone()).unwrap_or_default();
        let missing = evidence.missing();
        // The evidence gate: `--delegation` requires all five items (a)–(e) recorded in
        // `control/quiet-lease`. The override flag is for staging, chaos (c) and soak (d)
        // — they themselves must run with delegation on before the evidence exists; that
        // enabling leaves the `delegation_unproven=` marker on the object.
        let unproven = if delegation && !missing.is_empty() {
            if !accept_missing_evidence {
                return Err(CtlError::Refused(format!(
                    "--delegation needs all five done-gate evidence items of delegation \
                     in {}; still missing: {}. Record each item with \
                     `cairn-ctl cluster record-evidence <item> --note \"…\" --apply`, then run \
                     again. Staging, chaos, test: add --accept-missing-evidence (NOT for \
                     production). Nothing written.",
                    quiet_lease::OBJECT,
                    missing
                        .iter()
                        .map(|k| format!("({k}) {}", quiet_lease::evidence_title(*k)))
                        .collect::<Vec<_>>()
                        .join("; ")
                )));
            }
            Some(quiet_lease::Unproven {
                at_ms: opts.now_ms,
                by: opts.who.clone(),
                missing: missing.clone(),
            })
        } else if delegation {
            None
        } else {
            had.as_ref().and_then(|q| q.delegation_unproven.clone())
        };
        let on = QuietLease {
            on: true,
            knows: migrate::QUIET_LEASE,
            by: opts.who.clone(),
            at_ms: opts.now_ms,
            delegation: delegation || had.as_ref().is_some_and(|q| q.on && q.delegation),
            evidence,
            delegation_unproven: unproven.clone(),
        };
        let mut rep = RepairReport {
            applied: false,
            object: format!("cluster-version + {}", quiet_lease::OBJECT),
            before: format!(
                "{}\n{}",
                String::from_utf8_lossy(&cv.encode()).trim_end(),
                gate.as_ref()
                    .and_then(|(q, _)| q.as_ref())
                    .map(|q| String::from_utf8_lossy(&q.encode()).into_owned())
                    .unwrap_or_else(|| format!("({} absent)", quiet_lease::OBJECT))
            ),
            after: format!(
                "{}\n{}",
                String::from_utf8_lossy(&raised.encode()).trim_end(),
                String::from_utf8_lossy(&on.encode())
            ),
            cleanup: Vec::new(),
            warnings: Vec::new(),
            audit: None,
        };
        if gate
            .as_ref()
            .and_then(|(q, _)| q.as_ref())
            .is_some_and(|q| {
                q.on && (q.delegation || !delegation)
                    // Evidence complete now: running `--delegation` again removes the old override
                    // marker.
                    && !(delegation && unproven.is_none() && q.delegation_unproven.is_some())
            })
            && cv.min_binary >= migrate::QUIET_LEASE
        {
            rep.warnings
                .push("gate already on and min_binary already set — nothing to write".into());
            return Ok(rep);
        }
        if let Some(u) = &unproven {
            rep.warnings.push(format!(
                "DELEGATION ENABLED WITH MISSING EVIDENCE {} (--accept-missing-evidence) — only \
                 for staging/chaos/test, NOT for production; marker written to {} \
                 (delegation_unproven=), cluster status shows it",
                items_text(&u.missing),
                quiet_lease::OBJECT
            ));
        }
        if !opts.apply {
            rep.cleanup.push(format!(
                "--apply will first run the conditional GET test (G18) {} rounds on {}/<nonce>",
                store_probe::ROUNDS,
                store_probe::DIR
            ));
            return Ok(rep);
        }
        rep.cleanup.push(self.probe_conditional_get().await?);
        if raised != cv
            && migrate::write_cluster_version(main.store(), main.prefix(), &raised, tag)
                .await?
                .is_none()
        {
            return Err(CtlError::Refused(
                "cluster-version just changed — run again to see the new state".into(),
            ));
        }
        // The gate was already `on`: that earlier enabling waited after setting `min_binary`.
        if !had.as_ref().is_some_and(|q| q.on) {
            // The operator sees at once what the command is waiting for, not only in the final
            // report.
            eprintln!(
                "min_binary={} set; waiting {} s (node startup limit) so old binaries still \
                 starting get to open their session, then checking every session again…",
                raised.min_binary,
                BOOT_WAIT.as_secs()
            );
            self.wait(BOOT_WAIT).await;
            rep.cleanup.push(format!(
                "waited {} s after setting min_binary (node startup limit: old binaries still \
                 starting have opened their session or given up), then checked every session again",
                BOOT_WAIT.as_secs()
            ));
        }
        let below = self
            .sessions_below_gate(opts.now_ms, migrate::QUIET_LEASE)
            .await?;
        if !below.is_empty() {
            return Err(CtlError::Refused(format!(
                "min_binary={} set, but a session of an old binary just opened — gate NOT enabled; \
                 stop it and run again (downgrade: lower-min-binary): {}",
                raised.min_binary,
                below.join("; ")
            )));
        }
        if !quiet_lease::write(main.store(), main.prefix(), &on, gate.map(|(_, v)| v)).await? {
            return Err(CtlError::Refused(format!(
                "{} just changed — run again to see the new state",
                quiet_lease::OBJECT
            )));
        }
        rep.applied = true;
        rep.audit = Some(self.audit(opts, &rep).await?);
        Ok(rep)
    }

    /// Waits `d` by the `Ctl`'s clock (injectable — tests advance it by hand).
    async fn wait(&self, d: Duration) {
        let end = self.clock.mono() + d;
        loop {
            let now = self.clock.mono();
            if now >= end {
                return;
            }
            cairn_store::pace::poll_gap((end - now).min(Duration::from_millis(200))).await;
        }
    }

    /// A second client pointing at the same bucket for the G18 probe (read-after-write
    /// from another client): an S3 store rebuilt from the URI — its own connection pool;
    /// other stores (`memory://`, `file://`, test stores) use themselves.
    fn second_client(&self) -> std::sync::Arc<dyn slatedb::object_store::ObjectStore> {
        let main = self.stores.main();
        if main.uri().starts_with("s3://") {
            match cairn_store::StoreLocation::parse(main.uri()) {
                Ok(loc) => return std::sync::Arc::clone(loc.store()),
                Err(e) => {
                    tracing::warn!(error = %e, "cannot build a second client — the read test uses \
                                              the writing client")
                }
            }
        }
        std::sync::Arc::clone(main.store())
    }

    /// The G18 probe of `enable-quiet-lease`: `Ok(report line)` when it passes,
    /// [`CtlError::Refused`] when the store answers wrongly or fails transiently.
    async fn probe_conditional_get(&self) -> CtlResult<String> {
        let main = self.stores.main();
        let nonce = store_probe::nonce();
        let reader = self.second_client();
        match store_probe::conditional_get(
            main.store(),
            &reader,
            main.prefix(),
            &nonce,
            store_probe::ROUNDS,
        )
        .await
        {
            Ok(Ok(p)) => Ok(format!(
                "{}/{nonce}: conditional GET test (G18) passed {} rounds in {} ms, probe object \
                 deleted",
                store_probe::DIR,
                p.rounds,
                p.elapsed.as_millis()
            )),
            Ok(Err(why)) => Err(CtlError::Refused(format!(
                "the store does not keep the conditional GET contract (G18, \
                 docs/reliability/irreversible.md §0.3) — solo mode and delegation are NOT safe on \
                 this store (a caching proxy or a weakly consistent store in front of the \
                 bucket?); gate NOT enabled, nothing changed. The probe object {}/{nonce} may be \
                 left behind. Reason: {why}",
                store_probe::DIR
            ))),
            Err(e) => Err(CtlError::Refused(format!(
                "the conditional GET test (G18) hit a transient I/O error, no conclusion yet — \
                 gate NOT enabled; run again: {e}"
            ))),
        }
    }

    /// `cluster enable-call-prefix`: turns on **every call gate** this `cairn-ctl` knows —
    /// `calls/prefix` (from the next range open, the engine writes unkeyed `Call` rows at
    /// v6 with the pk following the caller's prefix) and `calls/placement` (calls with
    /// `placement: "spread"` from callers created after that follow the old rule). One
    /// command for both rather than two: one [`BOOT_WAIT`] instead of two, and no
    /// "grouping but cannot spread" state the operator has to remember to move on from.
    /// A cluster that turned on `calls/prefix` with an older binary runs exactly this
    /// command again to turn on `calls/placement` too.
    ///
    /// The same pattern as [`Self::enable_quiet_lease`], except that the gate lives right
    /// in `cluster-version` (the lines `call_prefix=on`, `call_placement=on`, no new
    /// object) and is **one-way**: once v6 rows or callers carrying the
    /// `cairnflow-call-placement` marker exist, a binary that does not know step
    /// [`migrate::CALL_PLACEMENT`] gets them wrong (cannot read v6 rows; ignores
    /// `placement` and replays by the grouping rule ⇒ a second callee), so there is no
    /// off command and `lower-min-binary` does not lower `min_binary` below the step of a
    /// gate that is on.
    ///
    /// Refused while there is an unfinished session (including `corrupt`) of a binary
    /// that does not know the last gate step. Then, with `--apply`:
    ///
    /// 1. CAS `min_binary = K` (old binaries stop starting); a migration lock being held
    ///    ⇒ refused;
    /// 2. **wait [`BOOT_WAIT`]** — unless `min_binary` was already ≥ K before this command
    ///    (no old binary can be starting any more): an old binary that passed the
    ///    `min_binary` check before step 1 and is still starting has, within this window,
    ///    either opened its session or given up;
    /// 3. **check** the sessions **again** — an old binary ⇒ refused, the gate not on;
    /// 4. CAS `call_prefix=on`, `call_placement=on` into `cluster-version` (one write).
    ///
    /// Dying between 1 and 4 (failpoint `ctl::enable_call_prefix::raised`): the cluster is
    /// at `min_binary = K`, the gate not on — running again skips the wait (step 2) and
    /// goes on.
    ///
    /// An open range does **not** change rules midway: the engine reads the gate when
    /// the range opens (a leader change, a split, a restart), and only invocations created
    /// **after** that open carry the new rule's marker in entry 0. The rule belongs to the
    /// caller, fixed when it is created, so whichever range turns it on whenever, no
    /// duplicate callee is born (IR-9).
    pub async fn enable_call_prefix(&self, opts: &RepairOptions) -> CtlResult<RepairReport> {
        self.enable_gate(
            opts,
            &OneWayGate {
                k: migrate::CALL_PLACEMENT,
                what: "call gate",
                failpoint: "ctl::enable_call_prefix::raised",
                on: |cv| cv.call_prefix_on() && cv.call_placement_on(),
                set: |cv| {
                    cv.call_prefix = true;
                    cv.call_placement = true;
                },
            },
        )
        .await
    }

    /// `cluster enable-streams`: turns on the `streams/table` gate — from the next range
    /// open, the engine writes the chunk rows of durable streams (`$log`, `$trace`,
    /// `ctx.stream(name, {durable: true})`) into the `Streams` table (`0x18`). The same
    /// pattern and the same four steps as [`Self::enable_call_prefix`], and the same
    /// **one-way** nature: a binary that does not know step [`migrate::STREAMS_TABLE`]
    /// leaks those rows on purge, so there is no off command.
    ///
    /// Dying between the `min_binary` CAS and the `streams=on` CAS (failpoint
    /// `ctl::enable_streams::raised`): the cluster is at `min_binary = 6`, the gate not on
    /// — running again skips the wait and goes on.
    pub async fn enable_streams(&self, opts: &RepairOptions) -> CtlResult<RepairReport> {
        self.enable_gate(
            opts,
            &OneWayGate {
                k: migrate::STREAMS_TABLE,
                what: "durable stream gate",
                failpoint: "ctl::enable_streams::raised",
                on: migrate::ClusterVersion::streams_on,
                set: |cv| cv.streams = true,
            },
        )
        .await
    }

    /// `cluster enable-schedules`: turns on the `schedules/table` gate — from the next
    /// range open, the engine runs the schedule firing loop and the API accepts `PUT
    /// /schedules`. The same pattern, the same four steps, the same **one-way** nature as
    /// [`Self::enable_streams`]: with a binary that does not know step
    /// [`migrate::SCHEDULES_TABLE`] leading a range with schedules, nothing fires.
    ///
    /// Dying between the `min_binary` CAS and the `schedules=on` CAS (failpoint
    /// `ctl::enable_schedules::raised`): the cluster is at `min_binary = 7`, the gate not
    /// on — running again skips the wait and goes on.
    pub async fn enable_schedules(&self, opts: &RepairOptions) -> CtlResult<RepairReport> {
        self.enable_gate(
            opts,
            &OneWayGate {
                k: migrate::SCHEDULES_TABLE,
                what: "schedule gate",
                failpoint: "ctl::enable_schedules::raised",
                on: migrate::ClusterVersion::schedules_on,
                set: |cv| cv.schedules = true,
            },
        )
        .await
    }

    /// `keys create <tenant> <role>`: creates a key directly in the `auth/keys` registry —
    /// the control plane's bootstrap path: the operator creates its `provision` key
    /// without taking a cluster-admin key out of the cell. The same writer as
    /// `POST /admin/keys` ([`crate::auth::Keys`]: CAS, ambiguity resolved by reading back,
    /// registry `/1` until the gate is on), plus the two phases of the registry generation
    /// like every registry write through the API: polling nodes see it on the next tick.
    ///
    /// `view`/`provision` roles only when the `auth/keys-v2` gate is on
    /// ([`Self::enable_key_roles`]); before that refused, nothing written. The secret only
    /// lives in [`CreatedKey::secret`] — the report, the audit record and `--dry-run` do
    /// not carry it. Without `--apply` ⇒ only reports what would be created.
    pub async fn create_key(
        &self,
        tenant: &str,
        role: &str,
        opts: &RepairOptions,
    ) -> CtlResult<CreatedKey> {
        use crate::auth::{Keys, Role};
        let t = cairn_store::TenantId::new(tenant).map_err(|e| CtlError::Bad(e.to_string()))?;
        let r = Role::parse(role).ok_or_else(|| {
            CtlError::Bad(format!(
                "no such role {role:?} — invoke | view | admin | provision | cluster-admin"
            ))
        })?;
        let main = self.stores.main();
        let keys = Keys::new(
            Some((std::sync::Arc::clone(main.store()), main.prefix())),
            None,
        );
        if !r.in_v1_book() && !keys.roles_enabled().await.map_err(CtlError::Refused)? {
            return Err(CtlError::Refused(format!(
                "role {role} needs the auth/keys-v2 gate — `cairn-ctl cluster enable-key-roles \
                 --apply` first"
            )));
        }
        let mut rep = RepairReport {
            applied: false,
            object: "auth/keys".into(),
            before: String::new(),
            after: format!("+ new key, tenant {t}, role {}", r.as_str()),
            cleanup: Vec::new(),
            warnings: Vec::new(),
            audit: None,
        };
        if !opts.apply {
            return Ok(CreatedKey {
                id: String::new(),
                tenant: t.to_string(),
                role: r.as_str().into(),
                secret: String::new(),
                report: rep,
            });
        }
        let gen = crate::registry_gen::RegistryGen::new(
            std::sync::Arc::clone(main.store()),
            main.prefix(),
            None,
        );
        let open = gen
            .begin()
            .await
            .map_err(|e| CtlError::Refused(format!("cannot open control/registry-gen: {e}")))?;
        let made = keys.create(t.clone(), r).await;
        if let Err(e) = gen.seal(open).await {
            rep.warnings.push(format!(
                "could not settle control/registry-gen ({e}) — nodes see it at the next registry \
                 reconcile"
            ));
        }
        let (record, secret) = made.map_err(|e| CtlError::Refused(e.to_string()))?;
        rep.after = format!(
            "+ {} tenant {} role {} (hash {}…)",
            record.id,
            record.tenant,
            record.role.as_str(),
            record.hash_prefix()
        );
        rep.cleanup.push(
            "nodes pick up the key at the next registry refresh tick (--deploy-refresh-ms); the \
             secret is printed only once"
                .into(),
        );
        rep.applied = true;
        rep.audit = Some(self.audit(opts, &rep).await?);
        Ok(CreatedKey {
            id: record.id,
            tenant: record.tenant.to_string(),
            role: record.role.as_str().into(),
            secret,
            report: rep,
        })
    }

    /// `cluster enable-key-roles`: turns on the `auth/keys-v2` gate — from then on the key
    /// registry writer writes `cairnflow-keys/2` and accepts the `view`/`provision`
    /// roles. The same pattern, the same four steps, the same **one-way** nature as
    /// [`Self::enable_schedules`]: a binary that does not know step [`migrate::KEY_ROLES`]
    /// fails to read a `/2` registry and closes every stored key.
    ///
    /// Dying between the `min_binary` CAS and the `key_roles=on` CAS (failpoint
    /// `ctl::enable_key_roles::raised`): the cluster is at `min_binary = 8`, the gate not
    /// on — the writer still writes `/1` (the gate needs both lines); running again skips
    /// the wait and goes on.
    pub async fn enable_key_roles(&self, opts: &RepairOptions) -> CtlResult<RepairReport> {
        let mut rep = self
            .enable_gate(
                opts,
                &OneWayGate {
                    k: migrate::KEY_ROLES,
                    what: "key registry /2 gate",
                    failpoint: "ctl::enable_key_roles::raised",
                    on: migrate::ClusterVersion::key_roles_on,
                    set: |cv| cv.key_roles = true,
                },
            )
            .await?;
        if rep.applied {
            // The gate is on no range: the key registry writer reads it before every write,
            // so the generic pattern's "at the next range open" line is wrong here.
            rep.cleanup.retain(|c| !c.contains("at their next open"));
            rep.cleanup.push(
                "the next key registry write (key create/revoke) writes auth/keys as \
                 cairnflow-keys/2; view/provision keys can be created from now on"
                    .into(),
            );
        }
        Ok(rep)
    }

    /// `cluster enable-tenant-state`: turns on the `tenants/state` gate — refused while
    /// there is a session of a binary not knowing step 9 (that binary ignores `state=`),
    /// CAS `min_binary=9`, wait, check again, CAS `tenant_state=on`. Dying between the two
    /// CASes (failpoint `ctl::enable_tenant_state::raised`): running again finishes it.
    pub async fn enable_tenant_state(&self, opts: &RepairOptions) -> CtlResult<RepairReport> {
        let mut rep = self
            .enable_gate(
                opts,
                &OneWayGate {
                    k: migrate::TENANT_STATE,
                    what: "tenant state gate",
                    failpoint: "ctl::enable_tenant_state::raised",
                    on: migrate::ClusterVersion::tenant_state_on,
                    set: |cv| cv.tenant_state = true,
                },
            )
            .await?;
        if rep.applied {
            // The gate is on no range: the state changer reads it before every write.
            rep.cleanup.retain(|c| !c.contains("at their next open"));
            rep.cleanup.push(
                "PUT /admin/tenants/{t}/state is accepted from now on; every running node already \
                 enforces state= of the tenants registry"
                    .into(),
            );
        }
        Ok(rep)
    }

    /// `cluster enable-audit`: turns on the `audit/table` gate — refused while there is a
    /// session of a binary not knowing step 10 (that binary ignores the `Audit` table),
    /// CAS `min_binary=10`, wait, check again, CAS `audit=on`. Ranges write audit rows
    /// from their next open. Dying between the two CASes (failpoint
    /// `ctl::enable_audit::raised`): running again finishes it.
    pub async fn enable_audit(&self, opts: &RepairOptions) -> CtlResult<RepairReport> {
        self.enable_gate(
            opts,
            &OneWayGate {
                k: migrate::AUDIT_TABLE,
                what: "in-range audit gate",
                failpoint: "ctl::enable_audit::raised",
                on: migrate::ClusterVersion::audit_on,
                set: |cv| cv.audit = true,
            },
        )
        .await
    }

    /// `cluster enable-values`: turns on the `values/segmented` gate — refused while there
    /// is a session of a binary not knowing step 12 (that binary would read the tail as the
    /// whole value), CAS `min_binary=12`, wait, check again, CAS `values=on`. Dying between
    /// the two CASes (failpoint `ctl::enable_values::raised`): running again finishes it.
    /// Ranges seal from their next open.
    pub async fn enable_values(&self, opts: &RepairOptions) -> CtlResult<RepairReport> {
        self.enable_gate(
            opts,
            &OneWayGate {
                k: migrate::VALUES,
                what: "segmented values gate",
                failpoint: "ctl::enable_values::raised",
                on: migrate::ClusterVersion::values_on,
                set: |cv| cv.values = true,
            },
        )
        .await
    }

    /// `cluster enable-tenant-purge`: turns on the `tenants/purge` gate — refused while
    /// there is a session of a binary not knowing step 11 (that binary reads `deleting` as
    /// `suspended`: it does not block reads and does not delete rows), CAS
    /// `min_binary=11`, wait, check again, CAS `tenant_purge=on`. Dying between the two
    /// CASes (failpoint `ctl::enable_tenant_purge::raised`): running again finishes it.
    pub async fn enable_tenant_purge(&self, opts: &RepairOptions) -> CtlResult<RepairReport> {
        let mut rep = self
            .enable_gate(
                opts,
                &OneWayGate {
                    k: migrate::TENANT_PURGE,
                    what: "tenant purge gate",
                    failpoint: "ctl::enable_tenant_purge::raised",
                    on: migrate::ClusterVersion::tenant_purge_on,
                    set: |cv| cv.tenant_purge = true,
                },
            )
            .await?;
        if rep.applied {
            // The gate is on no range: the state changer and the purge loop read it before
            // every step.
            rep.cleanup.retain(|c| !c.contains("at their next open"));
            rep.cleanup.push(
                "PUT /admin/tenants/{t}/state accepts \"deleting\" from now on; every running node \
                 already blocks keys and turns of a deleting tenant"
                    .into(),
            );
        }
        Ok(rep)
    }

    /// The shared body of commands turning on a **one-way** gate living in
    /// `cluster-version` (the four steps in [`Self::enable_call_prefix`]).
    async fn enable_gate(
        &self,
        opts: &RepairOptions,
        gate: &OneWayGate,
    ) -> CtlResult<RepairReport> {
        let k = gate.k;
        let main = self.stores.main();
        let below = self.sessions_below_gate(opts.now_ms, k).await?;
        if !below.is_empty() {
            return Err(CtlError::Refused(format!(
                "there are unfinished sessions of binaries that do not know step {k} — upgrade the \
                 binary (or wait for the dead session to be marked) and run again: {}",
                below.join("; ")
            )));
        }
        let (cv, tag) = match migrate::read_cluster_version(main.store(), main.prefix()).await? {
            Some((cv, tag)) => (cv, Some(tag)),
            None => (migrate::ClusterVersion::default(), None),
        };
        if let Some(u) = &cv.upgrading {
            return Err(CtlError::Refused(format!(
                "a cluster-level migration is in progress ({u}) — wait for it to finish and run \
                 again"
            )));
        }
        if cv.version > migrate::KNOWS || cv.min_binary > migrate::KNOWS {
            return Err(CtlError::Refused(format!(
                "cluster is at version={} min_binary={}, this cairn-ctl only knows up to {} — use \
                 a newer cairn-ctl",
                cv.version,
                cv.min_binary,
                migrate::KNOWS
            )));
        }
        let raised = migrate::ClusterVersion {
            min_binary: cv.min_binary.max(k),
            ..cv.clone()
        };
        let mut on = raised.clone();
        (gate.set)(&mut on);
        let mut rep = RepairReport {
            applied: false,
            object: "cluster-version".into(),
            before: String::from_utf8_lossy(&cv.encode()).into_owned(),
            after: String::from_utf8_lossy(&on.encode()).into_owned(),
            cleanup: Vec::new(),
            warnings: Vec::new(),
            audit: None,
        };
        if (gate.on)(&cv) {
            rep.warnings.push(format!(
                "{} already on and min_binary already set — nothing to write",
                gate.what
            ));
            return Ok(rep);
        }
        if !opts.apply {
            return Ok(rep);
        }
        let mut tag = tag;
        if raised != cv {
            match migrate::write_cluster_version(main.store(), main.prefix(), &raised, tag).await? {
                Some(t) => tag = Some(t),
                None => {
                    return Err(CtlError::Refused(
                        "cluster-version just changed — run again to see the new state".into(),
                    ))
                }
            }
        }
        cairn_store::failpoint!(gate.failpoint, err);
        // `min_binary` was already ≥ k before this command: no old binary can have passed
        // the boot path check since then, nothing to wait for.
        if cv.min_binary < k {
            eprintln!(
                "min_binary={} set; waiting {} s (node startup limit) so old binaries still \
                 starting get to open their session, then checking every session again…",
                raised.min_binary,
                BOOT_WAIT.as_secs()
            );
            self.wait(BOOT_WAIT).await;
            rep.cleanup.push(format!(
                "waited {} s after setting min_binary (node startup limit: old binaries still \
                 starting have opened their session or given up), then checked every session again",
                BOOT_WAIT.as_secs()
            ));
        }
        let below = self.sessions_below_gate(opts.now_ms, k).await?;
        if !below.is_empty() {
            return Err(CtlError::Refused(format!(
                "min_binary={} set, but a session of an old binary just opened — gate NOT enabled; \
                 stop it and run again: {}",
                raised.min_binary,
                below.join("; ")
            )));
        }
        if migrate::write_cluster_version(main.store(), main.prefix(), &on, tag)
            .await?
            .is_none()
        {
            return Err(CtlError::Refused(
                "cluster-version just changed — gate NOT enabled; run again to see the new state"
                    .into(),
            ));
        }
        rep.cleanup.push(
            "open ranges pick up the gate at their next open (leader change, split, node restart)"
                .into(),
        );
        rep.applied = true;
        rep.audit = Some(self.audit(opts, &rep).await?);
        Ok(rep)
    }

    /// `pricing list`: the current token price table.
    pub async fn pricing_show(&self) -> CtlResult<crate::pricing::Pricing> {
        let main = self.stores.main();
        Ok(crate::pricing::read(main.store(), main.prefix())
            .await
            .map_err(CtlError::Refused)?
            .map(|(p, _)| p)
            .unwrap_or_default())
    }

    /// `pricing set <model> <in> <out>`: sets the price (micro-USD per 1 M tokens) of a
    /// model. A CAS on the `pricing` object; `version` increases with every write, so the
    /// content is distinctive enough for the comparison when the outcome is ambiguous.
    /// A price of `0 0` removes the model from the table. Nodes load it at the next
    /// metering flush tick.
    pub async fn pricing_set(
        &self,
        model: &str,
        input: u64,
        output: u64,
        opts: &RepairOptions,
    ) -> CtlResult<RepairReport> {
        if model.is_empty() || model.contains(['\t', '\n']) {
            return Err(CtlError::Bad(format!("invalid model name: {model:?}")));
        }
        let main = self.stores.main();
        let (cur, tag) = match crate::pricing::read(main.store(), main.prefix())
            .await
            .map_err(CtlError::Refused)?
        {
            Some((p, tag)) => (p, Some(tag)),
            None => (crate::pricing::Pricing::default(), None),
        };
        let mut next = cur.clone();
        next.version = cur.version + 1;
        next.by = opts.who.clone();
        if input == 0 && output == 0 {
            next.models.remove(model);
        } else {
            next.models
                .insert(model.to_string(), crate::pricing::Price { input, output });
        }
        let mut rep = RepairReport {
            applied: false,
            object: "pricing".into(),
            before: String::from_utf8_lossy(&cur.encode()).into_owned(),
            after: String::from_utf8_lossy(&next.encode()).into_owned(),
            cleanup: Vec::new(),
            warnings: Vec::new(),
            audit: None,
        };
        if !opts.apply {
            return Ok(rep);
        }
        if !crate::pricing::write(main.store(), main.prefix(), &next, tag)
            .await
            .map_err(CtlError::Refused)?
        {
            return Err(CtlError::Refused(
                "pricing just changed — run again to see the new state".into(),
            ));
        }
        rep.cleanup.push(
            "nodes load the price table at the next metering flush tick (default 60 s)".into(),
        );
        rep.applied = true;
        rep.audit = Some(self.audit(opts, &rep).await?);
        Ok(rep)
    }

    /// `identity publish|activate|retire <tenant>`: one step of rotating `tenant`'s
    /// identity key — a CAS on the `tenants` registry, with an audit record. Nodes see it
    /// at the next registry re-read tick (`--deploy-refresh-ms`). Touches no private key:
    /// the keys are derived from the cell's master and the generation.
    pub async fn identity(
        &self,
        tenant: &str,
        step: IdentityStep,
        opts: &RepairOptions,
    ) -> CtlResult<RepairReport> {
        let t = cairn_store::TenantId::new(tenant).map_err(|e| CtlError::Bad(e.to_string()))?;
        let main = self.stores.main();
        let settings = crate::tenants::TenantSettings::new(Some((
            std::sync::Arc::clone(main.store()),
            main.prefix(),
        )));
        settings.refresh().await.map_err(CtlError::Refused)?;
        let cur = settings.identity_generations(&t);
        let next = cur.after(step).map_err(CtlError::Refused)?;
        let mut rep = RepairReport {
            applied: false,
            object: "tenants".into(),
            before: format!("{tenant} {}", cur.describe()),
            after: format!("{tenant} {}", next.describe()),
            cleanup: Vec::new(),
            warnings: Vec::new(),
            audit: None,
        };
        if step == IdentityStep::Activate {
            rep.warnings.push(format!(
                "the endpoint of {tenant} must already have the kid of generation {} in \
                 identityKeys — otherwise every request to it gets 401",
                next.active
            ));
        }
        if !opts.apply {
            return Ok(rep);
        }
        // The two phases of the registry generation, like every registry write through
        // the API: nodes polling `control/registry-gen` see the change on the next tick,
        // without waiting for reconciliation.
        let gen = crate::registry_gen::RegistryGen::new(
            std::sync::Arc::clone(main.store()),
            main.prefix(),
            None,
        );
        let open = gen
            .begin()
            .await
            .map_err(|e| CtlError::Refused(format!("cannot open control/registry-gen: {e}")))?;
        // Apply **the same** transformation to the copy read inside the CAS loop: someone
        // else writing other flags of the registry midway loses nothing; someone else
        // rotating at the same time has the transformation recomputed on the new copy.
        let lost = std::sync::Mutex::new(None);
        let wrote = settings
            .update(&t, |f| {
                let g = f.identity.clone().unwrap_or_default();
                match g.after(step) {
                    Ok(n) => {
                        f.identity = Some(n);
                        *lost.lock().expect("mutex") = None;
                    }
                    Err(e) => *lost.lock().expect("mutex") = Some(e),
                }
            })
            .await;
        if let Err(e) = gen.seal(open).await {
            rep.warnings.push(format!(
                "could not settle control/registry-gen ({e}) — nodes see it at the next registry \
                 reconcile"
            ));
        }
        wrote.map_err(CtlError::Refused)?;
        if let Some(e) = lost.into_inner().expect("mutex") {
            return Err(CtlError::Refused(format!(
                "tenants registry just changed: {e}"
            )));
        }
        rep.cleanup.push(
            "nodes load the new generation at the next tenants registry reread tick \
             (--deploy-refresh-ms)"
                .into(),
        );
        rep.applied = true;
        rep.audit = Some(self.audit(opts, &rep).await?);
        Ok(rep)
    }

    /// `cluster disable-quiet-lease`: writes `control/quiet-lease` = `off`. Nodes see it at
    /// the next gate read and go back to `direct` (CAS, `beat+1`). Does not lower
    /// `min_binary` — that is `lower-min-binary`, after every session is back at `direct`.
    /// The done-gate evidence (`evidence=`) stays as it is — it is about the release and
    /// the store, not about the enabling; the override marker is removed together with the
    /// delegation flag.
    pub async fn disable_quiet_lease(&self, opts: &RepairOptions) -> CtlResult<RepairReport> {
        let main = self.stores.main();
        let gate = quiet_lease::read(main.store(), main.prefix()).await?;
        let off = QuietLease {
            on: false,
            knows: migrate::QUIET_LEASE,
            by: opts.who.clone(),
            at_ms: opts.now_ms,
            evidence: gate
                .as_ref()
                .and_then(|(q, _)| q.as_ref())
                .map(|q| q.evidence.clone())
                .unwrap_or_default(),
            ..Default::default()
        };
        let mut rep = RepairReport {
            applied: false,
            object: quiet_lease::OBJECT.into(),
            before: gate
                .as_ref()
                .and_then(|(q, _)| q.as_ref())
                .map(|q| String::from_utf8_lossy(&q.encode()).into_owned())
                .unwrap_or_default(),
            after: String::from_utf8_lossy(&off.encode()).into_owned(),
            cleanup: Vec::new(),
            warnings: Vec::new(),
            audit: None,
        };
        if !gate
            .as_ref()
            .and_then(|(q, _)| q.as_ref())
            .is_some_and(|q| q.on)
        {
            rep.warnings
                .push("gate already off — nothing to write".into());
            return Ok(rep);
        }
        if !opts.apply {
            return Ok(rep);
        }
        if !quiet_lease::write(main.store(), main.prefix(), &off, gate.map(|(_, v)| v)).await? {
            return Err(CtlError::Refused(format!(
                "{} just changed — run again to see the new state",
                quiet_lease::OBJECT
            )));
        }
        rep.applied = true;
        rep.audit = Some(self.audit(opts, &rep).await?);
        Ok(rep)
    }

    /// `cluster lower-min-binary`: the last step of the downgrade runbook — lowers
    /// `min_binary` to `version` (the last data step that ran), so binaries from before the
    /// gate step can start again. Refused while the gate is still on or there is an
    /// unfinished session not at `direct` (an old binary would apply the silence rule
    /// LS-1 to it). With the grouping gate (`call_prefix=on`) on it does **not lower
    /// below** [`migrate::CALL_PREFIX`], with the spread gate (`call_placement=on`) on not
    /// below [`migrate::CALL_PLACEMENT`]: those two gates are one-way (see
    /// [`Self::enable_call_prefix`]).
    pub async fn lower_min_binary(&self, opts: &RepairOptions) -> CtlResult<RepairReport> {
        let main = self.stores.main();
        let gate_on = quiet_lease::read(main.store(), main.prefix())
            .await?
            .and_then(|(q, _)| q)
            .is_some_and(|q| q.on);
        if gate_on {
            return Err(CtlError::Refused(
                "quiet renewal gate is still on — disable-quiet-lease first".into(),
            ));
        }
        let quiet: Vec<String> = self
            .sessions(opts.now_ms)
            .await?
            .into_iter()
            .filter(|s| s.state != "expired" && s.state != "closed" && s.mode != "direct")
            .map(|s| format!("{} (mode={})", s.session, s.mode))
            .collect();
        if !quiet.is_empty() {
            return Err(CtlError::Refused(format!(
                "some sessions are not back to direct yet — wait (cluster status) and run again: \
                 {}",
                quiet.join("; ")
            )));
        }
        let Some((cv, tag)) = migrate::read_cluster_version(main.store(), main.prefix()).await?
        else {
            return Err(CtlError::Refused(
                "no cluster-version — min_binary is already 0".into(),
            ));
        };
        if cv.upgrading.is_some() {
            return Err(CtlError::Refused(
                "a cluster-level migration is in progress — wait for it to finish and run again"
                    .into(),
            ));
        }
        // One-way gates: v6 outbox rows, callers carrying the `cairnflow-call-placement`
        // marker, `Streams` rows or schedules may already be in the Db, the key registry
        // may already be `/2` — a binary not knowing that step must not start again.
        let floor = if cv.values {
            migrate::VALUES
        } else if cv.tenant_purge {
            migrate::TENANT_PURGE
        } else if cv.audit {
            migrate::AUDIT_TABLE
        } else if cv.tenant_state {
            migrate::TENANT_STATE
        } else if cv.key_roles {
            migrate::KEY_ROLES
        } else if cv.schedules {
            migrate::SCHEDULES_TABLE
        } else if cv.streams {
            migrate::STREAMS_TABLE
        } else if cv.call_placement {
            migrate::CALL_PLACEMENT
        } else if cv.call_prefix {
            migrate::CALL_PREFIX
        } else {
            0
        };
        let lowered = migrate::ClusterVersion {
            min_binary: cv.version.min(cv.min_binary).max(floor.min(cv.min_binary)),
            ..cv.clone()
        };
        let mut rep = RepairReport {
            applied: false,
            object: "cluster-version".into(),
            before: String::from_utf8_lossy(&cv.encode()).into_owned(),
            after: String::from_utf8_lossy(&lowered.encode()).into_owned(),
            cleanup: Vec::new(),
            warnings: Vec::new(),
            audit: None,
        };
        if floor > 0 && lowered.min_binary > cv.version.min(cv.min_binary) {
            rep.warnings.push(format!(
                "one-way gate ({}) is on — min_binary cannot go below {floor}: the bucket may \
                 already hold things an older binary gets wrong (outbox v6 rows, callers carrying \
                 a placement marker, Streams rows, schedules, key registry /2, tenant state, audit \
                 rows, deleting tenants, segmented values)",
                if cv.values {
                    "values"
                } else if cv.tenant_purge {
                    "tenant_purge"
                } else if cv.audit {
                    "audit"
                } else if cv.tenant_state {
                    "tenant_state"
                } else if cv.key_roles {
                    "key_roles"
                } else if cv.schedules {
                    "schedules"
                } else if cv.streams {
                    "streams"
                } else if cv.call_placement {
                    "call_placement"
                } else {
                    "call_prefix"
                }
            ));
        }
        if lowered == cv {
            rep.warnings
                .push("min_binary already equals version — nothing to write".into());
            return Ok(rep);
        }
        if !opts.apply {
            return Ok(rep);
        }
        if migrate::write_cluster_version(main.store(), main.prefix(), &lowered, Some(tag))
            .await?
            .is_none()
        {
            return Err(CtlError::Refused(
                "cluster-version just changed — run again to see the new state".into(),
            ));
        }
        rep.applied = true;
        rep.audit = Some(self.audit(opts, &rep).await?);
        Ok(rep)
    }

    async fn audit(&self, opts: &RepairOptions, rep: &RepairReport) -> CtlResult<String> {
        let main = self.stores.main();
        let entry = AuditEntry {
            who: opts.who.clone(),
            at_ms: opts.now_ms,
            command: opts.command.clone(),
            object: rep.object.clone(),
            before: rep.before.clone(),
            after: rep.after.clone(),
        };
        Ok(inspect::write_audit(main.store(), main.prefix(), &entry)
            .await
            .map_err(|e| {
                CtlError::Bad(format!(
                    "change APPLIED but the audit record could not be written: {e}\n{}",
                    entry.encode()
                ))
            })?
            .to_string())
    }

    /// `fork --to <prefix>`: a copy of the cluster under another prefix, in the same
    /// bucket — every range in the table cloned from a checkpoint, `cluster.conf` and the
    /// cluster's configuration (rules, tenants, catalog, deployments, auth, …) copied
    /// verbatim, and owner records **scrubbed of their lease** (released, no WAL segment),
    /// so the test harness can build nodes on the copy. Sessions, shared WAL logs,
    /// archive, metering and GC fences are not copied.
    ///
    /// **A GC fence on the source.** The copy reads the source's SSTs through
    /// `external_dbs` — of the ranges live at fork time **and** of every retired ancestor
    /// those ranges inherit from. The source's D-d round only considers references of live
    /// ranges *in its own table*, so when the source splits further and the children
    /// compact away from the parent, D-d would delete Dbs the copy stands on. Fork sets
    /// the fence [`fork_barrier_id`] on the source: `all` **before** reading the table and
    /// cloning (D-d rechecks fences right before deleting, so no Db the copy is about to
    /// stand on is deleted midway), then narrows it to exactly the ranges the copy's
    /// manifests list. If that set cannot be proven ⇒ keep `all`. Expiry
    /// [`FORK_BARRIER_TTL`]; a fork failing midway leaves the `all` fence until it
    /// expires — keeping too much, never deleting by mistake.
    pub async fn fork(&self, to: &str, who: &str, command: &str) -> CtlResult<ForkReport> {
        self.fork_with(to, who, command, ForkArtifacts::Live).await
    }

    /// [`Self::fork`] with artifact copy mode `artifacts`. Copied with server-side copy,
    /// after the ranges and the configuration.
    pub async fn fork_with(
        &self,
        to: &str,
        who: &str,
        command: &str,
        artifacts: ForkArtifacts,
    ) -> CtlResult<ForkReport> {
        let main = self.stores.main();
        let to = to.trim_matches('/');
        if to.is_empty() || to == main.prefix().as_ref() {
            return Err(CtlError::Bad(
                "--to must be a prefix other than the source prefix".into(),
            ));
        }
        let dst = self.stores.at_prefix(to)?;
        if !inspect::prefix_is_empty(dst.main()).await? {
            return Err(CtlError::Refused(format!(
                "target prefix {to:?} already has objects"
            )));
        }
        let barrier_id = fork_barrier_id(to);
        let expires_ms = now_ms() + FORK_BARRIER_TTL.as_millis() as u64;
        let barrier = |protects: Protects, reason: String| GcBarrier {
            holder: who.to_string(),
            protects,
            expires_ms,
            reason,
        };
        gc_barrier::put_barrier(
            main.store(),
            main.prefix(),
            &barrier_id,
            &barrier(Protects::All, format!("cairn-ctl fork --to {to}: cloning")),
        )
        .await?;
        // One table snapshot for the whole pass: clone exactly V's ranges, the copy's
        // `cluster.conf` written from V's very bytes.
        let (conf, t) = self.table_snapshot().await?;
        dst.ensure_format().await?;
        let mut rep = ForkReport {
            to: to.to_string(),
            artifacts: 0,
            values: 0,
            ranges: Vec::new(),
            barrier: None,
            owners: Vec::new(),
            copied: Vec::new(),
            warnings: Vec::new(),
            audit: None,
        };
        for r in t.partitions() {
            if !self.stores.range_db_exists(r).await? {
                rep.warnings.push(format!(
                    "range {}: no Db yet — the copy opens it as a new range",
                    r.0
                ));
                continue;
            }
            self.stores
                .fork_range(r, &dst, FORK_CHECKPOINT_LIFETIME)
                .await?;
            rep.ranges.push(r.0);
            // The gap between two ranges' clones: tests squeeze a split in here.
            cairn_store::failpoint!("ctl::fork::after_range", err);
        }
        // Narrow the fence to exactly the source ranges the copy stands on.
        let mut stands_on = BTreeSet::new();
        let mut unproven = None;
        for &r in &rep.ranges {
            stands_on.insert(r);
            match dst
                .source_ranges_under(PartitionId(r), main.prefix())
                .await?
            {
                Some(ids) => stands_on.extend(ids),
                None => unproven = Some(r),
            }
        }
        let protects = match unproven {
            Some(r) => {
                rep.warnings.push(format!(
                    "range {r}: cannot read the full external_dbs of the copy — the GC fence on \
                     the source keeps every range"
                ));
                Some(Protects::All)
            }
            None if stands_on.is_empty() => None,
            None => Some(Protects::Ranges(stands_on)),
        };
        match protects {
            Some(p) => {
                gc_barrier::put_barrier(
                    main.store(),
                    main.prefix(),
                    &barrier_id,
                    &barrier(
                        p.clone(),
                        format!("cairn-ctl fork --to {to}: the copy reads SSTs of these ranges"),
                    ),
                )
                .await?;
                rep.barrier = Some(ForkBarrier {
                    id: barrier_id,
                    protects: p.to_string(),
                    expires_ms,
                });
            }
            None => gc_barrier::delete_barrier(main.store(), main.prefix(), &barrier_id).await?,
        }
        {
            let path = inspect::object_path(dst.main(), "cluster.conf");
            match cairn_store::cas::put_resolving(
                dst.main().store(),
                &path,
                conf,
                slatedb::object_store::PutMode::Create,
            )
            .await
            {
                Ok(cairn_store::cas::Cas::Written(_)) => rep.copied.push("cluster.conf".into()),
                Ok(cairn_store::cas::Cas::Lost) => {
                    return Err(CtlError::Refused(format!(
                        "{path} already exists at the target with different content"
                    )))
                }
                Err(e) => return Err(CtlError::Store(StoreError::Io(e.to_string()))),
            }
        }
        let mut objects: Vec<String> = ["cluster-version", "rules", "tenants", "range-windows"]
            .into_iter()
            .map(str::to_string)
            .collect();
        for dir in ["catalog", "deployments", "auth"] {
            objects.extend(inspect::list_relative(main, dir).await?);
        }
        for rel in objects {
            if inspect::copy_object(main, dst.main(), &rel).await? {
                rep.copied.push(rel);
            }
        }
        rep.artifacts = self.fork_artifacts(&dst, artifacts).await?;
        rep.values = self.fork_values(&dst).await?;
        let scrubbed = SessionId {
            node: "cairn-ctl-fork".into(),
            inc: 0,
        };
        for r in inspect::owner_ranges(main.store(), main.prefix()).await? {
            let Some(o) = inspect::owner_observed(main.store(), main.prefix(), r).await? else {
                continue;
            };
            let rec = OwnerRecord {
                session: scrubbed.clone(),
                address: None,
                released: true,
                wal_segments: Vec::new(),
                wal_floor_id: None,
                ..o.record
            };
            inspect::cas_owner(dst.main().store(), dst.main().prefix(), r, &rec, None).await?;
            rep.owners.push(r.0);
        }
        // The table is still V after reading the (live) owner records: different ⇒ a
        // split/merge committed midway, and the owner records may belong to a later table.
        // The destination already has objects, so it is not re-run in place — report a
        // clear error.
        let (_, now) = self.table_snapshot().await?;
        if now.version() != t.version() {
            return Err(CtlError::Refused(format!(
                "the range table changed during fork (version {} → {}): a split or merge \
                 committed — the copy at {to:?} is incomplete; delete that prefix and run fork \
                 again when range operations are quiet",
                t.version(),
                now.version()
            )));
        }
        let entry = AuditEntry {
            who: who.to_string(),
            at_ms: now_ms(),
            command: command.to_string(),
            object: "ranges/*/manifest (checkpoint for fork)".into(),
            before: String::new(),
            after: format!("fork → {to}: ranges {:?}", rep.ranges),
        };
        rep.audit = Some(
            inspect::write_audit(main.store(), main.prefix(), &entry)
                .await?
                .to_string(),
        );
        Ok(rep)
    }
}

/// Copying artifacts during `fork`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForkArtifacts {
    /// Every `(tenant, id, epoch)` directory except those with a hold-in-place entry in the
    /// trash (a dead earlier life). Does not read the Db: orphans not swept yet are copied
    /// too — the copy's sweep keeps them as at the source.
    Live,
    /// Every object under `artifacts/`.
    All,
    /// No copy: the copy's `Artifacts` rows point to nothing (`404`).
    None,
}

impl std::str::FromStr for ForkArtifacts {
    type Err = CtlError;
    fn from_str(s: &str) -> CtlResult<Self> {
        match s {
            "live" => Ok(Self::Live),
            "all" => Ok(Self::All),
            "none" => Ok(Self::None),
            _ => Err(CtlError::Bad(format!(
                "--artifacts must be live, all or none, not {s:?}"
            ))),
        }
    }
}

impl Ctl {
    /// Copies the source's artifacts to `dst` per `mode` — the number of objects copied.
    async fn fork_artifacts(&self, dst: &ObjectStores, mode: ForkArtifacts) -> CtlResult<usize> {
        if mode == ForkArtifacts::None {
            return Ok(0);
        }
        let main = self.stores.main();
        let src = cairn_store::artifact::ArtifactStore::new(
            std::sync::Arc::clone(main.store()),
            main.prefix(),
        );
        let to = cairn_store::artifact::ArtifactStore::new(
            std::sync::Arc::clone(dst.main().store()),
            dst.main().prefix(),
        );
        let dead: BTreeSet<cairn_store::artifact::Owner> = if mode == ForkArtifacts::Live {
            cairn_store::trash::Trash::new(std::sync::Arc::clone(main.store()), main.prefix())
                .entries()
                .await?
                .into_iter()
                .filter_map(|e| match e.subject {
                    Some(cairn_store::trash::Subject::Artifacts(o)) => Some(o),
                    _ => None,
                })
                .collect()
        } else {
            BTreeSet::new()
        };
        let mut n = 0;
        let mut after = None;
        loop {
            let (dirs, next) = src.list_page(after.as_ref(), 1000).await?;
            for d in dirs.into_iter().filter(|d| !dead.contains(&d.owner)) {
                for m in src.objects(&d.owner).await? {
                    let Some(sha) = m
                        .location
                        .filename()
                        .and_then(cairn_store::artifact::sha_from_hex)
                    else {
                        continue;
                    };
                    slatedb::object_store::ObjectStoreExt::copy(
                        main.store().as_ref(),
                        &m.location,
                        &to.path(&d.owner, &sha),
                    )
                    .await
                    .map_err(|e| CtlError::Refused(e.to_string()))?;
                    n += 1;
                }
            }
            match next {
                Some(p) => after = Some(p),
                None => return Ok(n),
            }
        }
    }
}

impl Ctl {
    /// Copies the source's segments to `dst`: **always** copied, refs never shared — one
    /// cluster's GC does not see the other cluster's holders, so sharing objects would mean
    /// deleting by mistake. Segments with a hold-in-place entry are skipped (like
    /// `--artifacts live`): orphans not swept yet are still copied — the copy's sweep keeps
    /// them as at the source. There is no `none` mode: the copy's state needs them.
    /// Returns the number of objects copied.
    async fn fork_values(&self, dst: &ObjectStores) -> CtlResult<usize> {
        let main = self.stores.main();
        let src =
            cairn_store::value::ValueStore::new(std::sync::Arc::clone(main.store()), main.prefix());
        let to = cairn_store::value::ValueStore::new(
            std::sync::Arc::clone(dst.main().store()),
            dst.main().prefix(),
        );
        let dead: BTreeSet<cairn_store::value::Segment> =
            cairn_store::trash::Trash::new(std::sync::Arc::clone(main.store()), main.prefix())
                .entries()
                .await?
                .into_iter()
                .filter_map(|e| match e.subject {
                    Some(cairn_store::trash::Subject::Value(s)) => Some(s),
                    _ => None,
                })
                .collect();
        let mut n = 0;
        let mut after = None;
        loop {
            let (dirs, next) = src.list_page(after.as_ref(), 1000).await?;
            for d in dirs {
                for (sha, gen, _) in d.segments {
                    let seg = cairn_store::value::Segment {
                        tenant: d.tenant.clone(),
                        pk: d.pk,
                        owner: d.owner,
                        sha,
                        gen,
                    };
                    if dead.contains(&seg) {
                        continue;
                    }
                    slatedb::object_store::ObjectStoreExt::copy(
                        main.store().as_ref(),
                        &src.path(&seg),
                        &to.path(&seg),
                    )
                    .await
                    .map_err(|e| CtlError::Refused(e.to_string()))?;
                    n += 1;
                }
            }
            match next {
                Some(p) => after = Some(p),
                None => return Ok(n),
            }
        }
    }
}

/// The table name of `--table`: a [`Table`] name (`InvocationStatus`, `invocation_status`,
/// `invocation-status` all work) or a hex code (`0x01`).
pub fn parse_table(name: &str) -> CtlResult<Table> {
    let norm = |s: &str| {
        s.chars()
            .filter(|c| c.is_ascii_alphanumeric())
            .collect::<String>()
            .to_lowercase()
    };
    if let Some(code) = name
        .strip_prefix("0x")
        .and_then(|h| u8::from_str_radix(h, 16).ok())
    {
        if let Some(t) = Table::from_u8(code) {
            return Ok(t);
        }
    }
    Table::ALL
        .into_iter()
        .find(|t| norm(&format!("{t:?}")) == norm(name))
        .ok_or_else(|| {
            CtlError::Bad(format!(
                "unknown table {name:?}; available: {}",
                Table::ALL
                    .iter()
                    .map(|t| format!("{t:?}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })
}

/// A key just created with `cairn-ctl keys create`: the secret exists only here, once —
/// the report and the audit record do not carry it. Without `--apply` ⇒ `id`, `secret`
/// empty.
pub struct CreatedKey {
    pub id: String,
    pub tenant: String,
    pub role: String,
    pub secret: String,
    pub report: RepairReport,
}

impl std::fmt::Debug for CreatedKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreatedKey")
            .field("id", &self.id)
            .field("tenant", &self.tenant)
            .field("role", &self.role)
            .field("secret", &if self.secret.is_empty() { "" } else { "…" })
            .field("report", &self.report)
            .finish()
    }
}

/// A one-way gate in `cluster-version` that `cairn-ctl` turns on
/// ([`Ctl::enable_call_prefix`], [`Ctl::enable_streams`],
/// [`Ctl::enable_schedules`]).
struct OneWayGate {
    /// The last gate step the command turns on — the minimum `min_binary`.
    k: u32,
    /// A name for the report reader.
    what: &'static str,
    /// The failpoint in the gap between the `min_binary` CAS and the gate CAS (without the
    /// `failpoints` feature the macro is empty and the field is not read).
    #[cfg_attr(not(feature = "failpoints"), allow(dead_code))]
    failpoint: &'static str,
    /// The gate is on (including a sufficient `min_binary`).
    on: fn(&migrate::ClusterVersion) -> bool,
    /// Sets the line turning the gate on.
    set: fn(&mut migrate::ClusterVersion),
}

/// The cap on rows per kind check (i) loads into RAM; over it, that range is skipped (one
/// `query_index` line with `heals`, not an error).
const QUERY_VERIFY_MAX_ROWS: usize = 2_000_000;

/// Waits for the reader to replay the whole shared WAL tail: `durable_seq` stays still
/// across three tailer poll ticks (200 ms), capped at 10 s. If it still advances at the
/// cap (the bucket is still being written), check at the mark of that moment.
async fn settle(reader: &cairn_store::PartitionReader) {
    let start = tokio::time::Instant::now();
    let mut last = reader.freshness().durable_seq;
    let mut still = 0;
    while still < 3 && start.elapsed() < Duration::from_secs(10) {
        cairn_store::pace::poll_gap(Duration::from_millis(200)).await;
        let now = reader.freshness().durable_seq;
        if now == last {
            still += 1;
        } else {
            (still, last) = (0, now);
        }
    }
}

/// **(i) the query index**: every metadata row matches exactly the set of index rows
/// derived from it (through the descriptor table) in every **`readable`** descriptor per
/// the Db's marker; no index row points to an id without metadata.
///
/// A `building` descriptor may be missing rows (the background build has not got there)
/// — not an error; rows of a code removed from the table are garbage the background
/// build cleans. Extra or orphan rows, wrong rows of a `readable` descriptor, and
/// invocations without metadata are healed by no loop: real errors.
async fn query_index_findings(
    reader: &cairn_store::PartitionReader,
    range: u32,
    deleting: &BTreeSet<String>,
) -> cairn_store::Result<Vec<Finding>> {
    use cairn_store::query_index as qi;
    // Both areas (hot `0x00`: metadata + status; derived `0x01`: indexes) are read in
    // **one** scan: the reader is still replaying the shared WAL tail and each iterator
    // holds its own mark — two scans would be two marks, and an invocation created between
    // the two marks would show up as an "orphan" index row. Like (j), (l).
    let mut hot = Vec::new();
    let mut rows = Vec::new();
    let mut crossed = false;
    reader
        .scan_fold(&[], |k, v| {
            let Some(&lead) = k.first() else {
                return true;
            };
            if lead > 0x00 && !crossed {
                crossed = true;
                cairn_store::failpoint!("ctl::verify::between_query_scans");
            }
            match lead {
                0x00 if matches!(
                    table_of(k),
                    Some(Table::InvocationMeta | Table::InvocationStatus)
                ) =>
                {
                    hot.push((
                        bytes::Bytes::copy_from_slice(k),
                        bytes::Bytes::copy_from_slice(v),
                    ));
                }
                0x01 if k.len() >= 2 && qi::QUERY_CODES.contains(&k[1]) => {
                    rows.push((
                        bytes::Bytes::copy_from_slice(k),
                        bytes::Bytes::copy_from_slice(v),
                    ));
                }
                0x00 | 0x01 => {}
                _ => return false,
            }
            hot.len() < QUERY_VERIFY_MAX_ROWS && rows.len() < QUERY_VERIFY_MAX_ROWS
        })
        .await?;
    if hot.len() >= QUERY_VERIFY_MAX_ROWS || rows.len() >= QUERY_VERIFY_MAX_ROWS {
        return Ok(vec![Finding::new(
            "query_index",
            Some(range),
            "range too large for the in-memory check — skipped",
        )
        .heals("not checked")]);
    }
    let marker = reader
        .get_meta(&qi::ready_key())
        .await?
        .and_then(|(raw, _)| qi::parse_ready_marker(&raw));
    let readable: Vec<&qi::QueryIndex> = qi::QUERY_INDEXES
        .iter()
        .filter(|ix| {
            marker
                .as_ref()
                .and_then(|m| m.version_of(ix.name))
                .is_some_and(|v| v == ix.version)
        })
        .collect();
    Ok(query_index_compare(range, &hot, &rows, &readable, deleting))
}

/// The comparison of check (i) over the rows one scan read: `hot` (metadata and status
/// rows), `rows` (query index rows), the `readable` descriptors per the Db's marker.
/// `deleting`: tenants in state `deleting`. Their purge round deletes rows in passes
/// (derived rows first, then status rows before metadata rows, in key order), so a Db read
/// half way — a backup copied during the purge, `verify` during the purge — holds rows
/// missing their partner. Those are reported in one finding with `heals`: the round
/// deletes the rest when it resumes, and check (l) reports any row left after `purged`.
fn query_index_compare(
    range: u32,
    hot: &[(bytes::Bytes, bytes::Bytes)],
    rows: &[(bytes::Bytes, bytes::Bytes)],
    readable: &[&cairn_store::query_index::QueryIndex],
    deleting: &BTreeSet<String>,
) -> Vec<Finding> {
    use cairn_store::query_index::{self as qi, InvocationMeta, Placement};
    let mut metas = std::collections::BTreeMap::new();
    let mut statuses = std::collections::BTreeMap::new();
    for (k, v) in hot {
        if let Some(id) = qi::parse_meta_key(k) {
            metas.insert(id, v.clone());
        } else if let Some(id) = cairn_store::keys::parse_invocation_status(k) {
            statuses.insert(id, v.clone());
        }
    }

    // The tenant of each invocation (its metadata row): a row of a tenant being purged
    // that is missing its partner is the purge round's unfinished work, not an error.
    let tenant_of: std::collections::BTreeMap<_, String> = metas
        .iter()
        .filter_map(|(id, v)| InvocationMeta::decode(v).map(|m| (*id, m.tenant)))
        .collect();
    let purging = |id: &cairn_store::keys::InvocationId| {
        tenant_of
            .get(id)
            .is_some_and(|t| deleting.contains(t.as_str()))
    };

    let mut out = Vec::new();
    // `held`: findings of tenants being purged, reported with `heals`.
    let mut held: Vec<String> = Vec::new();
    let mut expected = std::collections::BTreeSet::new();
    let mut expected_purging = std::collections::BTreeSet::new();
    let mut no_meta = 0usize;
    for (id, raw) in &statuses {
        let Some(m) = metas.get(id).and_then(|m| InvocationMeta::decode(m)) else {
            no_meta += 1;
            continue;
        };
        let at = cairn_engine::InvocationStatus::decode_at(raw);
        let Some(status) = cairn_engine::InvocationStatus::decode(raw.clone()) else {
            continue;
        };
        let p = Placement::new(status.class(), at.unwrap_or(0), &m);
        let into = if deleting.contains(m.tenant.as_str()) {
            &mut expected_purging
        } else {
            &mut expected
        };
        for ix in readable {
            into.extend(qi::rows(ix, id, &m, p));
        }
    }
    if no_meta > 0 {
        out.push(Finding::new(
            "query_index",
            Some(range),
            format!("{no_meta} invocations have no metadata row"),
        ));
    }
    let (orphans_purging, orphans): (Vec<&cairn_store::keys::InvocationId>, Vec<_>) = metas
        .keys()
        .filter(|id| !statuses.contains_key(*id))
        .partition(|id| purging(id));
    if !orphans.is_empty() {
        out.push(Finding::new(
            "query_index",
            Some(range),
            format!("{} metadata rows have no status row", orphans.len()),
        ));
    }
    if !orphans_purging.is_empty() {
        held.push(format!(
            "{} metadata rows have no status row",
            orphans_purging.len()
        ));
    }

    // The tenant of an index row: the tenant in its key, else its invocation's metadata.
    let row_purging = |row: &qi::IndexRow| {
        row.tenant
            .as_deref()
            .map_or_else(|| purging(&row.id), |t| deleting.contains(t))
    };
    let mut actual = std::collections::BTreeSet::new();
    let mut dropped = 0usize;
    let mut dangling = 0usize;
    let mut dangling_purging = 0usize;
    for (k, v) in rows {
        let Some(ix) = qi::QUERY_INDEXES.iter().find(|ix| ix.code == k[1]) else {
            dropped += 1;
            continue;
        };
        let hint = ix.applies == qi::Applies::PerObject;
        match qi::decode_row(ix, k, v) {
            // Hint rows (`ObjectKeys`) carry no id: outliving the invocation is by design.
            Some(_) if hint => {}
            Some(row) if !metas.contains_key(&row.id) => {
                if row_purging(&row) {
                    dangling_purging += 1;
                } else {
                    dangling += 1;
                }
            }
            None => dangling += 1,
            Some(_) => {}
        }
        if readable.iter().any(|r| r.code == ix.code) {
            actual.insert((k.to_vec(), v.to_vec()));
        }
    }
    if dangling > 0 {
        out.push(Finding::new(
            "query_index",
            Some(range),
            format!("{dangling} query index rows orphaned or corrupt (no metadata)"),
        ));
    }
    if dangling_purging > 0 {
        held.push(format!(
            "{dangling_purging} query index rows have no metadata"
        ));
    }
    if dropped > 0 {
        out.push(
            Finding::new(
                "query_index",
                Some(range),
                format!("{dropped} rows of descriptors removed from the table"),
            )
            .heals("background rebuild"),
        );
    }
    let missing = expected.difference(&actual).count();
    let missing_purging = expected_purging.difference(&actual).count();
    // Extra hint rows (an object whose invocations are gone) are not a mismatch: readers
    // check, a background loop cleans.
    let hint = |k: &[u8]| {
        qi::QUERY_INDEXES
            .iter()
            .any(|ix| ix.code == k[1] && ix.applies == qi::Applies::PerObject)
    };
    let (extra_purging, extra): (Vec<_>, Vec<_>) = actual
        .iter()
        .filter(|row| !expected.contains(*row) && !expected_purging.contains(*row))
        .filter(|(k, _)| !hint(k))
        .partition(|(k, v)| {
            qi::QUERY_INDEXES
                .iter()
                .find(|ix| ix.code == k[1])
                .and_then(|ix| qi::decode_row(ix, k, v))
                .is_some_and(|row| row_purging(&row))
        });
    let names = || {
        readable
            .iter()
            .map(|ix| ix.name)
            .collect::<Vec<_>>()
            .join(",")
    };
    if missing + extra.len() > 0 {
        out.push(Finding::new(
            "query_index",
            Some(range),
            format!(
                "readable index out of step with metadata: missing {missing}, extra {} \
                 (descriptor: {})",
                extra.len(),
                names()
            ),
        ));
    }
    if missing_purging + extra_purging.len() > 0 {
        held.push(format!(
            "index out of step with metadata: missing {missing_purging}, extra {}",
            extra_purging.len()
        ));
    }
    if !held.is_empty() {
        out.push(
            Finding::new(
                "query_index",
                Some(range),
                format!(
                    "rows of a tenant being purged, not deleted yet: {}",
                    held.join("; ")
                ),
            )
            .heals("tenant purge round"),
        );
    }
    out
}

#[cfg(test)]
mod query_index_tests {
    use {
        super::*,
        cairn_store::query_index::{self as qi, InvocationMeta, Placement},
    };

    type Rows = Vec<(bytes::Bytes, bytes::Bytes)>;

    fn meta(tenant: &str) -> InvocationMeta {
        InvocationMeta {
            created_ms: 1_700_000_000_000,
            tenant: tenant.into(),
            service: "Greeter".into(),
            handler: "greet".into(),
            root: true,
            ..InvocationMeta::default()
        }
    }

    /// One running invocation `n` of `tenant`, with only the rows asked for.
    fn invocation(hot: &mut Rows, index: &mut Rows, n: u8, tenant: &str, has: (bool, bool, bool)) {
        let (status_row, meta_row, index_rows) = has;
        let id = [n; cairn_store::keys::ID_LEN];
        let m = meta(tenant);
        let status = cairn_engine::InvocationStatus::Running;
        if status_row {
            hot.push((
                cairn_store::keys::invocation_status(&id).into(),
                status.encode(0),
            ));
        }
        if meta_row {
            hot.push((qi::meta_key(&id).into(), m.encode().into()));
        }
        if index_rows {
            let p = Placement::new(status.class(), 0, &m);
            for ix in qi::QUERY_INDEXES {
                for (k, v) in qi::rows(ix, &id, &m, p) {
                    index.push((k.into(), v.into()));
                }
            }
        }
    }

    /// A Db copied while the purge of tenant `gone` is half way: purge pass 1 deleted the
    /// index rows of invocation 2 (status and metadata still there), pass 2 deleted the
    /// status row of invocation 1 (metadata still there). Both are the purge round's
    /// unfinished work, healed when it resumes. The same gap for the live tenant `acme`
    /// (invocation 3: metadata without status) is a real error.
    ///
    /// Must see: every finding about `gone` carries `heals`; the finding about `acme` does not.
    #[test]
    fn rows_of_a_tenant_being_purged_heal() {
        let (mut hot, mut index) = (Rows::new(), Rows::new());
        invocation(&mut hot, &mut index, 1, "gone", (false, true, false));
        invocation(&mut hot, &mut index, 2, "gone", (true, true, false));
        invocation(&mut hot, &mut index, 3, "acme", (false, true, false));
        invocation(&mut hot, &mut index, 4, "acme", (true, true, true));
        hot.sort();
        index.sort();
        let readable: Vec<&qi::QueryIndex> = qi::QUERY_INDEXES.iter().collect();
        let deleting = BTreeSet::from(["gone".to_string()]);
        let found = query_index_compare(9, &hot, &index, &readable, &deleting);
        let real: Vec<&Finding> = found.iter().filter(|f| f.heals.is_none()).collect();
        assert_eq!(real.len(), 1, "only acme's gap is an error: {found:?}");
        assert!(
            real[0]
                .detail
                .contains("1 metadata rows have no status row"),
            "{found:?}"
        );
        assert!(
            found
                .iter()
                .any(|f| f.heals.is_some() && f.detail.contains("missing")),
            "gone's missing index rows heal: {found:?}"
        );
    }

    /// Without a purge every gap is an error, as before.
    #[test]
    fn without_a_purge_every_gap_is_an_error() {
        let (mut hot, mut index) = (Rows::new(), Rows::new());
        invocation(&mut hot, &mut index, 1, "gone", (false, true, false));
        invocation(&mut hot, &mut index, 2, "gone", (true, true, false));
        hot.sort();
        index.sort();
        let readable: Vec<&qi::QueryIndex> = qi::QUERY_INDEXES.iter().collect();
        let found = query_index_compare(9, &hot, &index, &readable, &BTreeSet::new());
        assert_eq!(found.len(), 2, "{found:?}");
        assert!(found.iter().all(|f| f.heals.is_none()), "{found:?}");
    }
}
