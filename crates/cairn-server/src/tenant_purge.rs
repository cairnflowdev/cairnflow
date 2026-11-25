// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The runner's tenant purge round.
//!
//! The runner is the leader of the lowest range (`gc::is_runner`), like D-d and the trash
//! purge round. Each round reads the `tenants` book **fresh** from the bucket
//! (`cairn_store::tenant_purge::read_book`) and for each tenant:
//!
//! - **not** `deleting`/`purged` but still with `TenantPrefix` in-place hold entries
//!   (reversed during phase A): remove the entries — restore, delete nothing.
//! - `deleting`, phase A: each prefix of the tenant without a hold entry yet ⇒
//!   `hold_in_place` with `DeleteProof::tenant_deleting` (A1). Once `keep` has passed since
//!   `deleting-ms=` ⇒ through the clock skew gate, CAS the `purging-ms=` mark (B0) — the
//!   point of no return.
//! - `deleting` + `purging-ms=`, phase B: every range logically deletes the tenant's rows
//!   (B1, the leader of each range through `POST /internal/ranges/{id}/tenant-purge`); each
//!   prefix is really deleted with a `DeleteProof::tenant_purged` rebuilt right then
//!   (B2); the tenant's keys, OIDC clients and rule lines are removed (B3). Once every live
//!   range of the fresh table has a "clean" proof and every prefix is empty ⇒ delete the
//!   progress entries, then with no hold entries left ⇒ CAS `state=purged purged-ms=` and
//!   one `audit/ctl` record (B4, no customer data).
//!
//! **Per-range proofs**: the durable entry
//! `tenant-purge/<tenant>/<range>` ([`tp::RangeProof`]) is written once the range is clean
//! of the tenant's rows and the full compaction has finished, and once the table, read
//! **after** the compaction was seen finished, shows the range alive without an intent. The children of a
//! split/merge whose intent came after that mark of every source inherit "clean"
//! ([`tp::inherits`], the lineage from the `ops/<key>` ledger) — no scan, no new compaction.
//! The tombstone does not require the table to stand still for the whole round: under
//! continuous auto-split the round still closes as long as each lineage has a window long
//! enough to compact once. A progress entry absent or unreadable ⇒ the range is scanned
//! again (it only costs work). Right before the tombstone, **every** live range runs a
//! read-only check pass ([`final_check`]); tenant rows seen arriving after the proof ⇒ a
//! dirty mark ([`tp::mark_dirty`]) before anything is deleted, no tombstone written — a
//! window for late rows as before per-range proofs existed. Leftover progress entries of a
//! tenant already `purged` are cleaned up ([`Purger::sweep_progress_residue`]).
//!
//! Every step is idempotent; dying midway means the next round re-reads the book and
//! continues. Two runners at once during a leader change (`gc::is_runner` only asks at the
//! start of a round): safe like D-d — every delete builds its own proof. The two places the
//! other runner can slip in are handled separately: progress entries are re-read **after**
//! the final check, right before the tombstone (a dirty mark it writes within the round
//! blocks the tombstone); in-place hold entries it writes after the tombstone (A1 builds its
//! proof before the tombstone CAS, PUTs after) are removed in the next round — removing
//! entries, deleting nothing.
//!
//! Hold entries are read through [`Trash::held_entries_checked`]: entries another node has
//! removed are never reported as still there (stores keep empty directories — SeaweedFS
//! lists an entry directory for 2–3 minutes after its last DELETE), and a `GET …/purge` visit
//! is bounded ([`STATUS_HELD_BUDGET`], [`STATUS_BOUND`]) even with a cold cache over a large
//! trash.

use {
    crate::{actor::ActorError, partitions::Partitions},
    cairn_engine::{TenantPurgeCursor, TenantRowsStep, TenantState},
    cairn_store::{
        proof::{tenant_purge_grant, DeleteProof},
        retry::{self, Retryable, Verdict},
        tenant_purge::{self as tp, PrefixKind},
        trash::{EntryId, Subject, Trash},
        ClusterConfig, ObjectStores, PartitionId, StoreError, TenantId,
    },
    serde::{Deserialize, Serialize},
    std::{collections::BTreeMap, sync::Arc, time::Duration},
};

/// Maximum rows per delete lot in the engine.
pub const ROW_BUDGET: usize = 500;

/// Maximum lots per range per round — a large range continues in the next round. The default
/// of [`Purger::lots_per_range`].
pub const LOTS_PER_RANGE: u32 = 200;

/// Maximum objects per LIST/delete of one prefix.
const OBJECT_BATCH: usize = 1000;

/// Limit on waiting for one lot of a range on another node.
const REMOTE_LOT_TIMEOUT: Duration = Duration::from_secs(30);

/// Limit on classifying trash entries in one `GET /admin/tenants/{t}/purge` visit: past it,
/// return what is known, `heldComplete: false` and the number of entries not yet
/// classified — the progress is remembered for the next visit.
pub const STATUS_HELD_BUDGET: Duration = Duration::from_secs(5);

/// Limit on a whole `GET /admin/tenants/{t}/purge` visit on the server (reading the book,
/// LISTing the trash, bounded classification, LISTing the prefixes): below the chaos harness
/// client's 15 s limit. Past it ⇒ `503`.
pub const STATUS_BOUND: Duration = Duration::from_secs(12);

/// Internal route of the delete lot (the same route flushes the memtable / wakes the compactor).
const ROWS_ROUTE: &str = "tenant-purge";

/// Internal route of the **check** lot. A route of its own, not a flag in the body: an
/// older binary acting as leader answers `404` (the range cannot be checked yet ⇒ keep),
/// and never mistakes it for a delete lot.
const CHECK_ROUTE: &str = "tenant-purge-check";

/// Outcome of checking one range in the final check ([`check_keyed`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Check {
    /// A whole pass saw no row of the tenant.
    Clean,
    /// A row of the tenant was seen.
    Found,
    /// The round's lot limit ran out mid-pass — the next round continues from the cursor.
    Partial,
}

/// Body of `POST /internal/ranges/{id}/tenant-purge`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RowsRequest {
    pub tenant: String,
    pub segment: u8,
    /// Hex of the last key seen; empty = the start of the span.
    #[serde(default)]
    pub after: String,
    pub budget: usize,
    /// `true` = delete nothing: flush the range's memtable down to L0 (tombstones into SSTs
    /// before asking for a full compaction).
    #[serde(default)]
    pub flush: bool,
    /// `true` = delete nothing: wake the range's compactor so the full compaction just
    /// requested runs right away. Sent with `flush: true` — an older binary that does not
    /// know this field only flushes the memtable (harmless), and the compaction waits for
    /// the poll tick as before.
    #[serde(default)]
    pub wake: bool,
}

/// The answer of that route.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RowsReply {
    pub deleted: u64,
    pub scanned: u64,
    pub unattributed: u64,
    /// Check lot (`…/tenant-purge-check`): tenant rows seen (stops at the first
    /// one). Delete lot: 0.
    #[serde(default)]
    pub found: u64,
    /// `None` = a whole pass has been scanned.
    pub next: Option<(u8, String)>,
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

impl RowsRequest {
    pub fn cursor(&self) -> Option<Option<TenantPurgeCursor>> {
        if self.segment == 0 && self.after.is_empty() {
            return Some(None);
        }
        Some(Some(TenantPurgeCursor {
            segment: self.segment,
            after: unhex(&self.after)?,
        }))
    }
}

impl From<TenantRowsStep> for RowsReply {
    fn from(s: TenantRowsStep) -> Self {
        RowsReply {
            deleted: s.deleted,
            scanned: s.scanned,
            unattributed: s.unattributed,
            found: s.found,
            next: s.next.map(|c| (c.segment, hex(&c.after))),
        }
    }
}

/// Progress of scanning a tenant's rows on one range: the cursor of the unfinished whole
/// pass and its counts.
#[derive(Clone, Debug, Default)]
pub struct Sweep {
    cursor: Option<TenantPurgeCursor>,
    deleted: u64,
    unattributed: u64,
}

/// Scan progress by `(tenant, range)`.
pub type Sweeps = Arc<std::sync::Mutex<BTreeMap<(String, u32), Sweep>>>;

/// The final check of a `(tenant, range)` ([`check_keyed`]): the cursor of the unfinished
/// check pass, and whether the range has already had a whole clean check pass within this
/// check batch.
#[derive(Clone, Debug, Default)]
pub struct CheckState {
    cursor: Option<TenantPurgeCursor>,
    clean: bool,
}

/// The final check by `(tenant, range)`.
pub type Checks = Arc<std::sync::Mutex<BTreeMap<(String, u32), CheckState>>>;

/// Full compactions requested by `(tenant, range)`: the compaction's id, and the number of
/// rows whose owner could not be resolved in the clean pass that requested it.
pub type Compactions = Arc<std::sync::Mutex<BTreeMap<(String, u32), (String, u64)>>>;

/// The ledger watch of each full compaction id that [`Compactions`] names
/// ([`cairn_store::CompactionWatch`], G31).
pub type Watches = Arc<std::sync::Mutex<BTreeMap<String, cairn_store::CompactionWatch>>>;

/// How often the purge round runs while a full compaction it requested is not finished. Well
/// under `gc_min_age` (5 minutes by default): the round finds how a job ended in the stored
/// `.compactions` versions after the last one it saw, and GC deletes older versions (G31).
pub const COMPACTION_POLL: Duration = Duration::from_secs(30);

/// [`sweep`] on the progress of `key` in `sweeps`.
pub(crate) async fn sweep_keyed<F, Fut>(
    sweeps: &Sweeps,
    key: (String, u32),
    cap: u32,
    lot: F,
) -> Result<(u64, u64, bool), String>
where
    F: FnMut(Option<TenantPurgeCursor>) -> Fut,
    Fut: std::future::Future<Output = Result<TenantRowsStep, String>>,
{
    // No lock held across `.await`: take the progress out, run, put it back (even on error —
    // committed lots are still progress).
    let mut state = sweeps
        .lock()
        .ok()
        .and_then(|mut m| m.remove(&key))
        .unwrap_or_default();
    let r = sweep(&mut state, cap, lot).await;
    if let Ok(mut m) = sweeps.lock() {
        m.insert(key, state);
    }
    r
}

/// Run lots (`lot`) on a range until one **whole pass** (from the start of the range to the
/// end) deletes nothing — the range is clean — or `cap` lots are used up. `state` carries an
/// unfinished pass to the next call. Returns `(deleted in this call, unresolved in the last
/// whole pass, clean)`.
pub(crate) async fn sweep<F, Fut>(
    state: &mut Sweep,
    cap: u32,
    mut lot: F,
) -> Result<(u64, u64, bool), String>
where
    F: FnMut(Option<TenantPurgeCursor>) -> Fut,
    Fut: std::future::Future<Output = Result<TenantRowsStep, String>>,
{
    let mut deleted = 0;
    let mut lots = 0;
    loop {
        let step = lot(state.cursor.clone()).await?;
        lots += 1;
        deleted += step.deleted;
        state.deleted += step.deleted;
        state.unattributed += step.unattributed;
        cairn_store::failpoint!("tenant_purge::between_batches", |arg: Option<String>| Err(
            cairn_store::failpoint::error("tenant_purge::between_batches", arg).to_string()
        ));
        match step.next {
            Some(c) => state.cursor = Some(c),
            None => {
                let done = std::mem::take(state);
                if done.deleted == 0 {
                    return Ok((deleted, done.unattributed, true));
                }
            }
        }
        if lots >= cap {
            return Ok((deleted, state.unattributed, false));
        }
    }
}

/// How far a range has got on the scan–delete–compact path of one round ([`settle_rows`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Settle {
    /// Not done: an unfinished scan pass, or a compaction not finished yet.
    Pending,
    /// One whole clean pass, then the full compaction `id` has finished (`-` = the Db had
    /// nothing to compact).
    Compacted(String),
}

/// One round of [`settle_rows`] on one range.
#[derive(Clone, Debug)]
pub(crate) struct RowsRound {
    /// Rows deleted in this round.
    pub deleted: u64,
    /// Rows whose owner could not be resolved in the last whole pass.
    pub unattributed: u64,
    /// The outcome; `Err` = an error when requesting or asking about the compaction (the next round redoes it).
    pub settle: Result<Settle, String>,
}

/// The scan–delete–compact path of **one** range in one round: delete lots (`lot`) through
/// [`sweep_keyed`] up to the limit `cap`; one whole clean pass ⇒ request a full compaction
/// (`submit`: flush the memtable then request; `Some(id)` = requested, `None` = the Db has
/// nothing to compact); later rounds ask about the requested compaction (`done`: `Some(true)`
/// finished, `Some(false)` failed, `None` not yet). The requested compaction lives in
/// `compactions`. `Err` = a delete lot failed.
///
/// **The requested compaction is asked about before scanning again**: finished ⇒
/// `Compacted`, no scan. Not finished ⇒ keep scanning, and only drop the id when a delete
/// lot removes rows (late rows: compact again after the next clean pass) — not because a new
/// pass is unfinished. Before the fix, a range whose pass needed ≥ 2 rounds dropped the id
/// in every non-clean round in between ⇒ it never got a proof, and requested a new full
/// compaction every k rounds. Rows arriving after the clean pass but before the proof: the
/// final check catches them (a dirty mark).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn settle_rows<L, LF, S, SF, D, DF>(
    sweeps: &Sweeps,
    compactions: &Compactions,
    key: (String, u32),
    cap: u32,
    lot: L,
    mut submit: S,
    mut done: D,
) -> Result<RowsRound, String>
where
    L: FnMut(Option<TenantPurgeCursor>) -> LF,
    LF: std::future::Future<Output = Result<TenantRowsStep, String>>,
    S: FnMut() -> SF,
    SF: std::future::Future<Output = Result<Option<String>, String>>,
    D: FnMut(String) -> DF,
    DF: std::future::Future<Output = Result<Option<bool>, String>>,
{
    let forget = || {
        if let Ok(mut c) = compactions.lock() {
            c.remove(&key);
        }
    };
    let known = compactions.lock().ok().and_then(|c| c.get(&key).cloned());
    if let Some((id, unattributed)) = known {
        // Nothing deleted in this round; the unresolved count is that of the clean pass that
        // requested this compaction.
        let round = |settle| RowsRound {
            deleted: 0,
            unattributed,
            settle,
        };
        match done(id.clone()).await {
            Ok(Some(true)) => return Ok(round(Ok(Settle::Compacted(id)))),
            Ok(Some(false)) => forget(),
            Ok(None) => {}
            Err(e) => return Ok(round(Err(e))),
        }
    }
    let (deleted, unattributed, clean) = sweep_keyed(sweeps, key.clone(), cap, lot).await?;
    let round = |settle| RowsRound {
        deleted,
        unattributed,
        settle,
    };
    if deleted > 0 {
        // New rows deleted after the clean pass that requested the compaction: compact again.
        forget();
    }
    let waiting = compactions.lock().is_ok_and(|c| c.contains_key(&key));
    if waiting || !clean {
        return Ok(round(Ok(Settle::Pending)));
    }
    Ok(round(match submit().await {
        Ok(Some(id)) => {
            if let Ok(mut c) = compactions.lock() {
                c.insert(key.clone(), (id, unattributed));
            }
            Ok(Settle::Pending)
        }
        Ok(None) => Ok(Settle::Compacted("-".into())),
        Err(e) => Err(e),
    }))
}

/// Retries of one failed check lot within a round. Under chaos some lot fails in most
/// rounds (the remote leader was just killed, a GET of the purge grant hit a fault); left
/// unretried, that range sits in `checking` and blocks the whole round, so the batch
/// concludes only in a round where every range answers at the first try. A check lot
/// deletes nothing: trying it again is always safe. ~20 s covers a range getting a new
/// leader after a kill.
const CHECK_LOT_RETRY: retry::Policy =
    retry::Policy::new(Duration::from_millis(500), Duration::from_secs(4), 6)
        .with_total(Duration::from_secs(20));

/// The error of a check lot, for [`retry::run`]: always transient (a check lot deletes nothing).
struct LotFault(String);

impl std::fmt::Display for LotFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Retryable for LotFault {
    fn verdict(&self) -> Verdict {
        Verdict::Transient
    }
    fn timed_out(op: &'static str, after: Duration) -> Self {
        LotFault(format!("{op}: over {after:?}"))
    }
}

/// The **check** pass of `key` (the final check): check lots (`lot`) from the cursor of the
/// unfinished pass in `checks` until a whole pass completes, a tenant row is seen, or `cap`
/// lots are used up (an unfinished pass is left for the next round — like the scan pass).
/// A whole clean pass ⇒ mark the range clean within the batch; a row seen ⇒ drop the mark.
/// Returns the outcome of this call and the mark after it.
pub(crate) async fn check_keyed<F, Fut>(
    checks: &Checks,
    key: (String, u32),
    cap: u32,
    mut lot: F,
) -> (Result<Check, String>, bool)
where
    F: FnMut(Option<TenantPurgeCursor>) -> Fut,
    Fut: std::future::Future<Output = Result<TenantRowsStep, String>>,
{
    let mut state = checks
        .lock()
        .ok()
        .and_then(|mut m| m.remove(&key))
        .unwrap_or_default();
    let mut lots = 0;
    let out = loop {
        let cursor = state.cursor.clone();
        let step = match retry::run(
            &retry::ops::TENANT_PURGE_CHECK,
            &CHECK_LOT_RETRY,
            retry::Never,
            || {
                let attempt = lot(cursor.clone());
                async move { attempt.await.map_err(LotFault) }
            },
        )
        .await
        {
            Ok(s) => s,
            Err(e) => break Err(e.0),
        };
        lots += 1;
        if step.found > 0 {
            state = CheckState::default();
            break Ok(Check::Found);
        }
        match step.next {
            None => {
                state = CheckState {
                    cursor: None,
                    clean: true,
                };
                break Ok(Check::Clean);
            }
            Some(c) => state.cursor = Some(c),
        }
        if lots >= cap {
            break Ok(Check::Partial);
        }
    };
    let clean = state.clean;
    if state.cursor.is_some() || state.clean {
        if let Ok(mut m) = checks.lock() {
            m.insert(key, state);
        }
    }
    (out, clean)
}

/// Outcome of the final check of one round ([`final_check`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct FinalCheck {
    /// A range saw tenant rows arriving after the proof — the caller marks it dirty
    /// **before** deleting anything.
    pub late: Vec<u32>,
    /// A range without a whole clean check pass within the batch yet (the first pass still
    /// unfinished), or that could not be asked in this round.
    pub checking: Vec<u32>,
}

impl FinalCheck {
    /// Every live range has passed a whole clean check pass within the batch: the tombstone may be written.
    pub fn clean(&self) -> bool {
        self.late.is_empty() && self.checking.is_empty()
    }
}

/// The final check over every live range `live` of `tenant` ([`check_keyed`] per range,
/// `lot(range, cursor)` being one check lot).
///
/// **A check batch spans several rounds**: a range that completes a whole clean check pass
/// keeps a "clean within the batch" mark; the batch concludes in the round where **every**
/// live range carries the mark — the round of the slowest range, not the least common
/// multiple of the ranges' round counts as before. A range already carrying the mark **is
/// still checked again every round** (a new pass, the same cost as before): a row seen ⇒
/// `late`, the mark is lost. So a range's window for late rows runs from the start of its
/// last whole clean pass to the tombstone — a range taking one round per pass: as before
/// (within the tombstone round); a range taking k rounds: at most ≈ 2k rounds (before: k
/// rounds, but only closing at the common multiple). Marks of ranges no longer alive are
/// dropped; new ranges (children of a split/merge) start unmarked. A round that cannot check
/// a range (an error left after the retries of [`CHECK_LOT_RETRY`]) ⇒ the range goes into
/// `checking`, keeping its mark.
pub(crate) async fn final_check<F, Fut>(
    checks: &Checks,
    tenant: &str,
    live: &[u32],
    cap: u32,
    mut lot: F,
) -> FinalCheck
where
    F: FnMut(u32, Option<TenantPurgeCursor>) -> Fut,
    Fut: std::future::Future<Output = Result<TenantRowsStep, String>>,
{
    if let Ok(mut m) = checks.lock() {
        m.retain(|(t, r), _| t != tenant || live.contains(r));
    }
    let mut out = FinalCheck::default();
    for &range in live {
        let key = (tenant.to_string(), range);
        match check_keyed(checks, key, cap, |c| lot(range, c)).await {
            (Ok(Check::Found), _) => out.late.push(range),
            (Ok(_), true) => {}
            (Ok(_), false) => out.checking.push(range),
            (Err(e), _) => {
                tracing::debug!(tenant = %tenant, range, error = %e, "tenant purge: cannot re-check the range yet — next round");
                out.checking.push(range);
            }
        }
    }
    if out.clean() {
        // The batch concludes: the next check (if the tombstone misses) starts a new batch.
        if let Ok(mut m) = checks.lock() {
            m.retain(|(t, _), _| t != tenant);
        }
    }
    out
}

/// Result of one round for one tenant (logs, tests, the runner's `GET …/purge`).
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TenantRound {
    pub tenant: String,
    /// `deleting` | `purging` | `purged` | `restored`.
    pub phase: String,
    /// In-place hold entries just written (A1).
    pub held: usize,
    /// Hold entries just removed because the tenant was reversed.
    pub released: usize,
    /// Rows deleted in Dbs in this round.
    pub rows_deleted: u64,
    /// Rows whose owner could not be resolved in the last scan (kept).
    pub unattributed: u64,
    /// Ranges not clean yet (rows left, or could not be asked) in this round.
    pub ranges_pending: Vec<u32>,
    /// Objects deleted in this round.
    pub objects_deleted: usize,
    /// Live ranges with a durable "clean" proof because the memtable was flushed and a full
    /// compaction finished in phase B (G23) — including proofs from earlier rounds.
    pub compacted: Vec<u32>,
    /// Live ranges inheriting "clean" through their lineage (children of a split/merge after
    /// the compaction mark of every source).
    pub inherited: Vec<u32>,
    /// Why the tombstone has not been written (if not yet).
    pub blocked: Option<String>,
}

/// Everything the purge round needs — built in `node::start`.
#[derive(Clone)]
pub struct Purger {
    pub stores: ObjectStores,
    pub cluster: ClusterConfig,
    pub partitions: Partitions,
    pub routing: crate::routing::RoutingTable,
    pub secret: Option<crate::auth::ClusterSecret>,
    pub client: reqwest::Client,
    pub settings: crate::tenants::TenantSettings,
    pub keys: Option<crate::auth::Keys>,
    pub rules: Option<cairn_store::RuleStore>,
    /// The reversible retention time of phase A (default 7 days, [`tp::KEEP`]).
    pub keep: Duration,
    /// The latest round's result on this node.
    pub last: Arc<std::sync::Mutex<BTreeMap<String, TenantRound>>>,
    /// Full compactions requested for `(tenant, range)` on this node. Lost when the node
    /// restarts ⇒ requested once more — it only costs work.
    pub compactions: Compactions,
    /// The ledger watch of each id in `compactions`. Lost on restart with `compactions`.
    pub watches: Watches,
    /// Maximum lots per range per round, in the scan–delete pass and the check pass (default
    /// [`LOTS_PER_RANGE`]). Tests set it small to exercise the "one pass takes k rounds" case
    /// on a small range.
    pub lots_per_range: u32,
    /// The unfinished row scan round of `(tenant, range)` ([`Sweep`]).
    pub sweeps: Sweeps,
    /// The unfinished **check** pass of `(tenant, range)` in the final check (the cursor of
    /// [`check_keyed`]).
    pub checks: Checks,
    /// The progress entry of `(tenant, range)` read when the unfinished scan–delete–compact
    /// pass started: the pass's proof only overwrites exactly this entry. Lost when the node
    /// restarts ⇒ the pass starts again from scratch (the scan and compaction RAM is lost too).
    pub bases: Arc<std::sync::Mutex<BTreeMap<(String, u32), tp::Base>>>,
}

fn now_ms() -> u64 {
    crate::gc::wall_ms()
}

/// The prefix hold entry of `tenant`.
fn of_tenant(s: &Subject, tenant: &TenantId) -> bool {
    matches!(s, Subject::TenantPrefix { tenant: t, .. } if t == tenant)
}

/// A new proof for `range`; `by` carries a nonce (rule 7).
fn proof(range: u32, purging_ms: u64, basis: tp::RangeBasis) -> tp::RangeProof {
    tp::RangeProof {
        range,
        purging_ms,
        basis,
        by: cairn_store::ledger::writer_tag("tenant-purge"),
    }
}

impl Purger {
    /// The main store's trash, sharing the node's entry classification memory
    /// ([`Trash::held_entries`]).
    fn bin(&self) -> Trash {
        self.stores.main_trash()
    }

    /// The purge state of `tenant` for `GET /admin/tenants/{t}/purge`: the book line read
    /// fresh, in-place hold entries, objects left per prefix, the latest round on this
    /// node.
    pub async fn status(&self, tenant: &TenantId) -> Result<serde_json::Value, StoreError> {
        use tp::Mark;
        let main = self.stores.main();
        let line = tp::read_line(main.store(), main.prefix(), tenant)
            .await?
            .unwrap_or_default();
        let mark = |m: &Mark| match m {
            Mark::Absent => serde_json::Value::Null,
            Mark::At(ms) => serde_json::json!(ms),
            Mark::Bad(raw) => serde_json::json!(raw),
        };
        // In-place hold entries through the classification memory shared with the purge round,
        // checked again and bounded: entries another node removed are not reported; a cold
        // cache over a large trash (chaos: 8 000–9 000 entries, a node killed every
        // ~10 s) ⇒ the unclassified part is counted, not pretended absent.
        let scan = self
            .bin()
            .held_entries_checked(|s| of_tenant(s, tenant), Some(STATUS_HELD_BUDGET))
            .await?;
        let held: Vec<&'static str> = scan
            .held
            .into_iter()
            .filter_map(|(_, s)| match s {
                Subject::TenantPrefix { kind, .. } => Some(kind.name()),
                _ => None,
            })
            .collect();
        let mut objects = serde_json::Map::new();
        for kind in PrefixKind::ALL {
            let n = tp::objects(main.store(), main.prefix(), tenant, kind, 1000)
                .await?
                .len();
            if n > 0 {
                objects.insert(kind.name().into(), serde_json::json!(n.min(1000)));
            }
        }
        let last = self
            .last
            .lock()
            .ok()
            .and_then(|l| l.get(tenant.as_str()).cloned());
        Ok(serde_json::json!({
            "tenant": tenant.to_string(),
            "state": line.state.as_deref().unwrap_or("active"),
            "deletingMs": mark(&line.deleting),
            "purgingMs": mark(&line.purging),
            "purgedMs": mark(&line.purged),
            "keepMs": self.keep.as_millis() as u64,
            "held": held,
            "heldComplete": scan.unchecked == 0,
            "heldUnchecked": scan.unchecked,
            "objects": objects,
            "lastRound": last,
        }))
    }

    /// One round over every tenant of the book. The cluster is migrating ⇒ skip the round.
    pub async fn run_once(&self) -> Result<Vec<TenantRound>, StoreError> {
        let main = self.stores.main();
        if cairn_store::migrate::upgrading(main.store(), main.prefix())
            .await?
            .is_some()
        {
            return Ok(Vec::new());
        }
        let book = tp::read_book(main.store(), main.prefix()).await?;
        let bin = self.bin();
        // Only in-place hold entries, GETting `entry` only for unclassified entries: an idle
        // round does not pay by the size of the trash (≈ 8 100 GETs per round otherwise).
        // Known prefix hold entries are HEADed again: entries another node removed do not
        // enter the round.
        let entries = bin
            .held_entries_checked(|s| matches!(s, Subject::TenantPrefix { .. }), None)
            .await?
            .held;
        let mut holds: BTreeMap<TenantId, Vec<(EntryId, PrefixKind)>> = BTreeMap::new();
        for (id, subject) in entries {
            if let Subject::TenantPrefix { tenant, kind } = subject {
                holds.entry(tenant).or_default().push((id, kind));
            }
        }
        let mut out = Vec::new();
        // Hold entries of a tenant no longer `deleting`/`purged`: phase A was reversed.
        for (tenant, held) in &holds {
            let state = book.get(tenant).and_then(|l| l.state.as_deref());
            if !matches!(state, Some("deleting") | Some("purged")) {
                let mut r = TenantRound {
                    tenant: tenant.to_string(),
                    phase: "restored".into(),
                    ..Default::default()
                };
                for (id, _) in held {
                    bin.restore(id).await?;
                    r.released += 1;
                }
                tracing::info!(tenant = %tenant, entries = r.released, "tenant purge: tenant reversed — removing in-place hold entries");
                out.push(r);
            }
        }
        for (tenant, line) in &book {
            if line.state.as_deref() == Some("purged") {
                let (progress, stray): (Vec<_>, Vec<_>) = holds
                    .remove(tenant)
                    .unwrap_or_default()
                    .into_iter()
                    .partition(|(_, k)| *k == PrefixKind::Progress);
                // Data prefix hold entries of a tenant already `purged`: an old runner built
                // its A1 proof before the new runner's tombstone CAS and PUT the entry after
                // it. The tombstone has checked that the prefixes are empty; the entry is only
                // a promise to delete that nobody holds any more — remove it, delete nothing
                // (rule 5: there is no proof for deleting data after the tombstone; any object
                // arriving after the tombstone is `verify` (l)'s business).
                if !stray.is_empty() {
                    let mut r = TenantRound {
                        tenant: tenant.to_string(),
                        phase: "purged".into(),
                        ..Default::default()
                    };
                    for (id, _) in &stray {
                        bin.restore(id).await?;
                        r.released += 1;
                    }
                    tracing::warn!(tenant = %tenant, entries = r.released, "tenant purge: removing in-place hold entries written after the tombstone (old runner)");
                    out.push(r);
                }
                // Leftover progress entries: LIST only within `KEEP` after the tombstone, or
                // while its hold entries remain.
                let held: Vec<EntryId> = progress.into_iter().map(|(id, _)| id).collect();
                let recent = line
                    .purged
                    .at()
                    .is_some_and(|at| now_ms().saturating_sub(at) < tp::KEEP.as_millis() as u64);
                if !held.is_empty() || recent {
                    self.sweep_progress_residue(tenant, held).await?;
                }
                continue;
            }
            if line.state.as_deref() != Some("deleting") {
                continue;
            }
            let held = holds.remove(tenant).unwrap_or_default();
            let r = self.one(tenant, line, held).await?;
            out.push(r);
        }
        if let Ok(mut last) = self.last.lock() {
            for r in &out {
                last.insert(r.tenant.clone(), r.clone());
            }
        }
        Ok(out)
    }

    async fn one(
        &self,
        tenant: &TenantId,
        line: &tp::TenantLine,
        held: Vec<(EntryId, PrefixKind)>,
    ) -> Result<TenantRound, StoreError> {
        let now = now_ms();
        let mut r = TenantRound {
            tenant: tenant.to_string(),
            phase: "deleting".into(),
            ..Default::default()
        };
        // A1: one hold entry per prefix.
        let bin = self.bin();
        let mut entries: BTreeMap<PrefixKind, Vec<EntryId>> = BTreeMap::new();
        for (id, kind) in held {
            entries.entry(kind).or_default().push(id);
        }
        for kind in PrefixKind::ALL {
            if entries.contains_key(&kind) {
                continue;
            }
            match DeleteProof::tenant_deleting(&self.stores, tenant, kind).await? {
                Ok(proof) => {
                    let id = EntryId::new(now);
                    bin.hold_in_place(&id, &proof, "tenant deleting (phase A)")
                        .await?;
                    cairn_store::failpoint!("tenant_purge::after_hold", |arg: Option<String>| {
                        Err(cairn_store::failpoint::error(
                            "tenant_purge::after_hold",
                            arg,
                        ))
                    });
                    entries.entry(kind).or_default().push(id);
                    r.held += 1;
                }
                Err(why) => {
                    r.blocked = Some(why);
                    return Ok(r);
                }
            }
        }
        if r.held > 0 {
            tracing::info!(tenant = %tenant, entries = r.held, "tenant purge: holding prefixes in place (phase A)");
        }
        // B0: the point of no return.
        if !line.purging.is_present() {
            let Some(since) = line.deleting.at() else {
                r.blocked = Some(format!("deleting-ms {:?} unreadable", line.deleting));
                return Ok(r);
            };
            if now.saturating_sub(since) < self.keep.as_millis() as u64 {
                return Ok(r);
            }
            if let Err(why) = crate::gc::clock_gate(
                &self.stores,
                crate::gc::clock_skew_limit(self.keep),
                crate::gc::ClockRound::Trash,
            )
            .await
            {
                r.blocked = Some(why);
                return Ok(r);
            }
            // A second mark by the store's clock: `deleting-ms` carries the clock of the node
            // that took the command — Δ slow and the reversible window gets Δ shorter. Phase
            // A's in-place hold entries are written **after** that mark (the first round after
            // `deleting`), once (`Create`): the `last_modified` of the oldest entry (the store's
            // clock; the runner has just passed the clock skew gate) must also be at least
            // `keep` old. Unreadable ⇒ not yet (rule 5).
            let mut oldest: Option<u64> = None;
            for id in entries.values().flatten() {
                match bin.entry_stored_ms(id).await {
                    Ok(Some(ms)) => oldest = Some(oldest.map_or(ms, |o| o.min(ms))),
                    Ok(None) => {}
                    Err(e) => {
                        r.blocked = Some(format!("cannot read hold entry {id}: {e}"));
                        return Ok(r);
                    }
                }
            }
            let held_for = oldest.map(|o| now.saturating_sub(o));
            if held_for.is_none_or(|age| age < self.keep.as_millis() as u64) {
                r.blocked = Some(format!(
                    "phase A is {} ms old by the store's clock (oldest in-place hold entry), needs {} ms — \
                     deleting-ms may have been written by a node with a skewed clock",
                    held_for.unwrap_or(0),
                    self.keep.as_millis()
                ));
                return Ok(r);
            }
            let raw = line.deleting.clone();
            let marked = self
                .settings
                .try_update(tenant, |f| {
                    let same = match &raw {
                        tp::Mark::At(ms) => f.purge.deleting_ms() == Some(*ms),
                        _ => false,
                    };
                    if f.state() != TenantState::Deleting || f.purge.purging.is_some() || !same {
                        return Err("book changed underfoot".to_string());
                    }
                    f.purge.purging = Some(now.to_string());
                    Ok(())
                })
                .await
                .map_err(StoreError::Io)?;
            if let Err(why) = marked {
                r.blocked = Some(why);
                return Ok(r);
            }
            cairn_store::failpoint!("tenant_purge::after_purging_mark", |arg: Option<String>| {
                Err(cairn_store::failpoint::error(
                    "tenant_purge::after_purging_mark",
                    arg,
                ))
            });
            tracing::warn!(tenant = %tenant, "tenant purge: purging-ms written — from here it cannot be reversed (phase B)");
            r.phase = "purging".into();
            return Ok(r);
        }
        r.phase = "purging".into();
        // Phase B: every condition is re-read fresh at the gate.
        let grant_ok = tenant_purge_grant(&self.stores, tenant, self.keep, now).await?;
        if let Err(why) = grant_ok {
            r.blocked = Some(why);
            return Ok(r);
        }
        // B1: one durable "clean" proof per live range —
        // already there, inherited through the lineage, or scan + full compaction right here.
        let Some(purging) = line.purging.at() else {
            r.blocked = Some(format!("purging-ms {:?} unreadable", line.purging));
            return Ok(r);
        };
        let main = self.stores.main();
        let tp::Progress {
            mut proofs,
            mut dirty,
        } = tp::read_proofs(main.store(), main.prefix(), tenant, purging).await?;
        self.cluster.reload().await?;
        for range in self.cluster.partitions() {
            self.settle_range(tenant, purging, range, &mut proofs, &mut dirty, &mut r)
                .await?;
        }
        // B2: prefixes — except the progress entries, deleted last, right before the tombstone.
        let mut objects_left = false;
        for (kind, ids) in &entries {
            if *kind == PrefixKind::Progress {
                continue;
            }
            match self.purge_prefix(tenant, *kind, ids, &mut r).await? {
                Ok(left) => objects_left |= left,
                Err(why) => {
                    r.blocked = Some(why);
                    return Ok(r);
                }
            }
        }
        // B3: the tenant's configuration.
        self.forget_config(tenant).await?;
        // B4: bia.
        if !r.ranges_pending.is_empty() {
            r.blocked = Some(format!("ranges with rows left: {:?}", r.ranges_pending));
            return Ok(r);
        }
        if objects_left {
            r.blocked = Some("prefixes with objects left".into());
            return Ok(r);
        }
        cairn_store::failpoint!("tenant_purge::before_final_check");
        // Every live range of the **fresh** table has a proof: the table need not stand
        // still — ranges born from ranges that already have a proof (a split or merge after
        // the mark) inherit it, other ranges are scanned in the next round.
        self.cluster.reload().await?;
        let live = self.cluster.partitions();
        let (unproven, inherited) = self.coverage(tenant, &live, &proofs, &dirty).await;
        r.inherited.extend(inherited);
        if !unproven.is_empty() {
            r.blocked = Some(format!("ranges without a clean proof yet: {unproven:?}"));
            return Ok(r);
        }
        // Check **every** live range again, including ranges that already have a proof: tenant
        // rows arriving after the proof (a late inter-range message, a write from RAM) would
        // make the proof a lie. Check lots use the same scan and classifier as delete lots,
        // delete nothing, stop at the first row; each lot holds the engine once, bounded
        // (`8 × ROW_BUDGET` keys). The window left for late rows is as before per-range proofs
        // existed: after the range's check pass, before the tombstone, within this round.
        let ids: Vec<u32> = live.iter().map(|r| r.0).collect();
        let fc = final_check(
            &self.checks,
            tenant.as_str(),
            &ids,
            self.lots_per_range,
            |range, cursor| self.check_lot(tenant, PartitionId(range), cursor),
        )
        .await;
        for &range in &fc.late {
            // The dirty mark **before** deleting rows: dying between the delete and the new
            // compaction leaves no lying proof behind.
            tp::mark_dirty(
                main.store(),
                main.prefix(),
                tenant,
                range,
                purging,
                cairn_store::ledger::writer_tag("tenant-purge"),
            )
            .await?;
            self.forget_range(&(tenant.to_string(), range));
            tracing::warn!(tenant = %tenant, range, "tenant purge: rows arrived after the range's clean proof — marking dirty, scanning and compacting again");
        }
        if !fc.late.is_empty() {
            r.ranges_pending.extend(&fc.late);
            r.blocked = Some(format!(
                "rows arrived after the proof in range {:?} — scanning and compacting again",
                fc.late
            ));
            return Ok(r);
        }
        if !fc.clean() {
            r.ranges_pending.extend(&fc.checking);
            r.blocked = Some(format!("ranges being re-checked: {:?}", fc.checking));
            return Ok(r);
        }
        // Re-read the progress entries **after** the final check: `proofs`/`dirty` were read
        // at the start of the round. The other runner (the runner role just moved node) sees
        // late rows in a range ⇒ marks it dirty **before** deleting, then deletes without
        // compacting again yet; if it deleted before this round's check pass, the check pass
        // is clean, so the dirty mark — written before the check pass — is only visible in
        // this read. A dirty range or a lost proof ⇒ no tombstone; the next round scans and
        // compacts again. A mark written after this read goes with rows that arrived after the
        // range's check pass: the existing window for late rows.
        let fresh = tp::read_proofs(main.store(), main.prefix(), tenant, purging).await?;
        let (stale, _) = self
            .coverage(tenant, &live, &fresh.proofs, &fresh.dirty)
            .await;
        if !stale.is_empty() {
            for &range in &stale {
                self.forget_range(&(tenant.to_string(), range));
            }
            r.ranges_pending.extend(&stale);
            r.blocked = Some(format!(
                "progress entries of ranges {stale:?} changed within the round (another runner marked them dirty) — scanning and compacting again"
            ));
            return Ok(r);
        }
        // Progress entries: no work left, delete them like every prefix of the tenant. Dying
        // after this step ⇒ the next round scans and compacts again (it only costs work).
        cairn_store::failpoint!("tenant_purge::before_progress_delete", |arg: Option<
            String,
        >| {
            Err(cairn_store::failpoint::error(
                "tenant_purge::before_progress_delete",
                arg,
            ))
        });
        let progress = entries
            .get(&PrefixKind::Progress)
            .cloned()
            .unwrap_or_default();
        match self
            .purge_prefix(tenant, PrefixKind::Progress, &progress, &mut r)
            .await?
        {
            Ok(false) => {}
            Ok(true) => {
                r.blocked = Some("progress entries with objects left".into());
                return Ok(r);
            }
            Err(why) => {
                r.blocked = Some(why);
                return Ok(r);
            }
        }
        if !self
            .bin()
            .held_entries_checked(|s| of_tenant(s, tenant), None)
            .await?
            .held
            .is_empty()
        {
            r.blocked = Some("in-place hold entries left".into());
            return Ok(r);
        }
        // LIST every prefix again right before the tombstone: objects written after B2's last
        // LIST — metering/statistics/audit flushes for the `403 tenant_deleting` refusals, the
        // archive of a retention pass in flight — would be cleaned by no round after `purged`.
        // Seen ⇒ no tombstone yet; the next round holds the prefix in place again (A1) and
        // deletes it (B2).
        for kind in PrefixKind::ALL {
            if !tp::objects(main.store(), main.prefix(), tenant, kind, 0)
                .await?
                .is_empty()
            {
                r.blocked = Some(format!(
                    "prefix {} has objects written after the delete pass — the next round holds and deletes again",
                    kind.name()
                ));
                return Ok(r);
            }
        }
        let done = now_ms();
        let marked = self
            .settings
            .try_update(tenant, |f| {
                if f.state() != TenantState::Deleting || f.purge.purging.is_none() {
                    return Err("book changed underfoot".to_string());
                }
                f.state_raw = Some(TenantState::Purged.as_str().to_string());
                f.purge.purged = Some(done.to_string());
                Ok(())
            })
            .await
            .map_err(StoreError::Io)?;
        if let Err(why) = marked {
            r.blocked = Some(why);
            return Ok(r);
        }
        let entry = cairn_store::inspect::AuditEntry {
            who: "cairnflow/tenant-purge".into(),
            at_ms: done,
            command: format!("purge tenant {tenant}"),
            object: format!("tenants (line {tenant}), ranges/*, prefixes of {tenant}"),
            before: "state=deleting".into(),
            after: format!(
                "state=purged purged-ms={done}; rows with an unresolved owner still kept in the last round: {}",
                r.unattributed
            ),
        };
        if r.unattributed > 0 {
            // A deliberate deviation: rows that cannot be attributed to any tenant do not block
            // the tombstone — but they must be visible.
            tracing::warn!(tenant = %tenant, row = r.unattributed, "tenant purge: tombstone written with rows of unresolved owner left (kept, rule 5)");
        }
        if let Err(e) = cairn_store::inspect::write_audit(main.store(), main.prefix(), &entry).await
        {
            tracing::warn!(tenant = %tenant, error = %e, "tenant purge: cannot write audit/ctl");
        }
        tracing::warn!(tenant = %tenant, "tenant purge: done — state=purged");
        r.phase = "purged".into();
        Ok(r)
    }

    /// B2 for one prefix: LIST in lots, build `DeleteProof::tenant_purged` right before each
    /// lot, delete in place through the hold entry. `Ok(Ok(objects left))`;
    /// `Ok(Err(reason))` = cannot be proven (keep).
    async fn purge_prefix(
        &self,
        tenant: &TenantId,
        kind: PrefixKind,
        ids: &[EntryId],
        r: &mut TenantRound,
    ) -> Result<Result<bool, String>, StoreError> {
        let main = self.stores.main();
        let bin = self.bin();
        for id in ids {
            loop {
                let listed =
                    tp::objects(main.store(), main.prefix(), tenant, kind, OBJECT_BATCH).await?;
                let more = listed.len() > OBJECT_BATCH;
                let batch = &listed[..listed.len().min(OBJECT_BATCH)];
                let proof = match DeleteProof::tenant_purged(
                    &self.stores,
                    tenant,
                    kind,
                    self.keep,
                    now_ms(),
                )
                .await?
                {
                    Ok(p) => p,
                    Err(why) => return Ok(Err(why)),
                };
                cairn_store::failpoint!("tenant_purge::before_prefix_delete", |arg: Option<
                    String,
                >| {
                    Err(cairn_store::failpoint::error(
                        "tenant_purge::before_prefix_delete",
                        arg,
                    ))
                });
                if more {
                    // The entry disappeared midway (manual restore): stop — the next round
                    // holds the prefix again and continues deleting.
                    let Some(n) = bin.purge_objects_in_place(id, proof, batch).await? else {
                        break;
                    };
                    r.objects_deleted += n;
                    continue;
                }
                r.objects_deleted += bin.purge_in_place(id, proof, batch).await?;
                break;
            }
        }
        // Duplicate entries (died midway in A1) go in the same pass; objects written after the
        // last entry was removed ⇒ the next round holds them again and deletes.
        Ok(Ok(!tp::objects(
            main.store(),
            main.prefix(),
            tenant,
            kind,
            0,
        )
        .await?
        .is_empty()))
    }

    /// B1 for one range: an existing proof ⇒ done; inherited
    /// through the lineage ⇒ write an `inherited` proof; otherwise scan the tenant's rows,
    /// compact fully, then write a `compacted` proof once a fresh table shows the range still
    /// alive and carrying no intent. Ranges without a proof yet go into `ranges_pending`. A
    /// read/write error on one range does not fail the round for other ranges, except a
    /// bucket error when writing a proof.
    async fn settle_range(
        &self,
        tenant: &TenantId,
        purging: u64,
        range: PartitionId,
        proofs: &mut BTreeMap<u32, tp::RangeProof>,
        dirty: &mut std::collections::BTreeSet<u32>,
        r: &mut TenantRound,
    ) -> Result<(), StoreError> {
        if let Some(p) = proofs.get(&range.0) {
            match p.basis {
                tp::RangeBasis::Compacted(_) => r.compacted.push(range.0),
                tp::RangeBasis::Inherited(_) => r.inherited.push(range.0),
                // `read_proofs` never puts dirty marks into `proofs`.
                tp::RangeBasis::Dirty => r.ranges_pending.push(range.0),
            }
            return Ok(());
        }
        let main = self.stores.main();
        // A dirty range (rows after the proof) does not inherit: it must be scanned, deleted, compacted.
        let lineage = if dirty.contains(&range.0) {
            Ok(None)
        } else {
            self.inherited(range.0, proofs, dirty).await
        };
        match lineage {
            Ok(Some(op)) => {
                let proof = proof(range.0, purging, tp::RangeBasis::Inherited(op));
                // Only inherit when the entry is absent (range not dirty, no proof): if someone
                // marks it dirty meanwhile, the `Create` misses.
                if tp::write_proof(
                    main.store(),
                    main.prefix(),
                    tenant,
                    &proof,
                    &tp::Base::Absent,
                )
                .await?
                {
                    tracing::info!(tenant = %tenant, range = range.0, op, "tenant purge: range inherits clean through its lineage");
                    proofs.insert(range.0, proof);
                    r.inherited.push(range.0);
                } else {
                    r.ranges_pending.push(range.0);
                }
                return Ok(());
            }
            Ok(None) => {}
            Err(e) => {
                tracing::debug!(tenant = %tenant, range = range.0, error = %e, "tenant purge: cannot read the lineage — scanning directly");
            }
        }
        let key = (tenant.to_string(), range.0);
        // The range's entry is read **before** the scan–delete–compact pass: this pass's
        // proof only overwrites exactly what was read. Kept across rounds with the scan cursor
        // and the compaction (dropped together).
        let known = self.bases.lock().ok().and_then(|b| b.get(&key).cloned());
        let base = match known {
            Some(b) => b,
            None => match tp::read_entry(main.store(), main.prefix(), tenant, range.0).await? {
                tp::Entry::Clean(p) if p.purging_ms == purging => {
                    match p.basis {
                        tp::RangeBasis::Inherited(_) => r.inherited.push(range.0),
                        _ => r.compacted.push(range.0),
                    }
                    proofs.insert(range.0, p);
                    return Ok(());
                }
                tp::Entry::Clean(_) => {
                    tracing::warn!(tenant = %tenant, range = range.0, "tenant purge: proof of another purge — left as is, the range has no proof yet");
                    r.ranges_pending.push(range.0);
                    return Ok(());
                }
                tp::Entry::Start(b) => {
                    self.forget_range(&key);
                    // The dirty mark **before** the first delete lot: a range that has deleted
                    // rows without compacting again always leaves a durable mark, so the
                    // children of a split/merge of it before the compaction finished do not
                    // inherit "clean" through it from an ancestor with a proof
                    // ([`tp::inherits`]). The entry absent (the first round of phase B, or an
                    // unreadable lineage) or unreadable ⇒ mark it and take that very mark as the
                    // pass's entry.
                    let b = if dirty.contains(&range.0) {
                        b
                    } else {
                        tp::mark_dirty(
                            main.store(),
                            main.prefix(),
                            tenant,
                            range.0,
                            purging,
                            cairn_store::ledger::writer_tag("tenant-purge"),
                        )
                        .await?;
                        dirty.insert(range.0);
                        match tp::read_entry(main.store(), main.prefix(), tenant, range.0).await? {
                            tp::Entry::Start(b) => b,
                            // Another runner just wrote a proof over this mark:
                            // the next round reads again.
                            tp::Entry::Clean(_) => {
                                r.ranges_pending.push(range.0);
                                return Ok(());
                            }
                        }
                    };
                    if let Ok(mut m) = self.bases.lock() {
                        m.insert(key.clone(), b.clone());
                    }
                    b
                }
            },
        };
        let rows = settle_rows(
            &self.sweeps,
            &self.compactions,
            key.clone(),
            self.lots_per_range,
            |cursor| self.lot(tenant, range, cursor),
            || self.submit_compaction(tenant, range),
            |id| self.compaction_done(range, id),
        )
        .await;
        let rows = match rows {
            Ok(rows) => rows,
            Err(e) => {
                tracing::debug!(tenant = %tenant, range = range.0, error = %e, "tenant purge: cannot delete the range's rows yet — next round");
                r.ranges_pending.push(range.0);
                return Ok(());
            }
        };
        r.rows_deleted += rows.deleted;
        r.unattributed += rows.unattributed;
        // Flush and compact: tombstones into L0, then a full compaction into the last run (G23).
        let compaction = match rows.settle {
            Ok(Settle::Compacted(id)) => id,
            Ok(Settle::Pending) => {
                r.ranges_pending.push(range.0);
                return Ok(());
            }
            Err(e) => {
                tracing::debug!(tenant = %tenant, range = range.0, error = %e, "tenant purge: cannot force the range's compaction yet — next round");
                r.ranges_pending.push(range.0);
                return Ok(());
            }
        };
        // The mark: read the table **after** seeing the compaction finished. The range still
        // alive and without an intent ⇒ any op that splits/merges it later has its intent after
        // the mark, so that op's clone/union takes the compacted manifest (G28). An intent in
        // place ⇒ do not write yet: the op may have cloned before the compaction; reconsider next round.
        self.cluster.reload().await?;
        if !self
            .cluster
            .table()
            .range(range)
            .is_some_and(|e| e.intent.is_none())
        {
            r.ranges_pending.push(range.0);
            return Ok(());
        }
        let proof = proof(range.0, purging, tp::RangeBasis::Compacted(compaction));
        cairn_store::failpoint!("tenant_purge::before_range_proof");
        if !tp::write_proof(main.store(), main.prefix(), tenant, &proof, &base).await? {
            // The entry changed since the pass started (another runner marked it dirty again):
            // this proof was built before the new rows — drop it, the next round reads the new entry and scans again.
            tracing::warn!(tenant = %tenant, range = range.0, "tenant purge: progress entry changed during the scan — proof dropped, scanning again");
            self.forget_range(&key);
            r.ranges_pending.push(range.0);
            return Ok(());
        }
        cairn_store::failpoint!("tenant_purge::after_range_proof", |arg: Option<String>| {
            Err(cairn_store::failpoint::error(
                "tenant_purge::after_range_proof",
                arg,
            ))
        });
        proofs.insert(range.0, proof);
        dirty.remove(&range.0);
        r.compacted.push(range.0);
        self.forget_range(&key);
        Ok(())
    }

    /// Drop the RAM state of `key`'s scan–delete–compact pass: the entry read at the start,
    /// the scan cursor, the requested compaction — always together, so a pass never carries
    /// another pass's entry.
    fn forget_range(&self, key: &(String, u32)) {
        if let Ok(mut b) = self.bases.lock() {
            b.remove(key);
        }
        if let Ok(mut c) = self.compactions.lock() {
            c.remove(key);
        }
        if let Ok(mut s) = self.sweeps.lock() {
            s.remove(key);
        }
    }

    /// Which ranges of `live` have no clean proof yet by `proofs`/`dirty`: directly, or
    /// inherited through the lineage. Returns `(missing, inherited)`. An unreadable lineage
    /// ⇒ missing.
    async fn coverage(
        &self,
        tenant: &TenantId,
        live: &[PartitionId],
        proofs: &BTreeMap<u32, tp::RangeProof>,
        dirty: &std::collections::BTreeSet<u32>,
    ) -> (Vec<u32>, Vec<u32>) {
        let (mut unproven, mut inherited) = (Vec::new(), Vec::new());
        for &range in live {
            if proofs.contains_key(&range.0) {
                continue;
            }
            if dirty.contains(&range.0) {
                unproven.push(range.0);
                continue;
            }
            match self.inherited(range.0, proofs, dirty).await {
                Ok(Some(_)) => inherited.push(range.0),
                Ok(None) => unproven.push(range.0),
                Err(e) => {
                    tracing::debug!(tenant = %tenant, range = range.0, error = %e, "tenant purge: cannot read the lineage — next round");
                    unproven.push(range.0);
                }
            }
        }
        (unproven, inherited)
    }

    /// The op that produced range `c`, if `c` inherits "clean" ([`tp::inherits`]).
    async fn inherited(
        &self,
        c: u32,
        proofs: &BTreeMap<u32, tp::RangeProof>,
        dirty: &std::collections::BTreeSet<u32>,
    ) -> Result<Option<u32>, StoreError> {
        let main = self.stores.main();
        let proven = |s: u32| proofs.contains_key(&s);
        let marked = |s: u32| dirty.contains(&s);
        let ops = tp::lineage(
            main.store(),
            main.prefix(),
            c,
            &proven,
            &marked,
            tp::LINEAGE_DEPTH,
        )
        .await?;
        Ok(tp::inherits(c, &proven, &marked, &ops, tp::LINEAGE_DEPTH))
    }

    /// Request a full compaction of range `range` once it is clean of `tenant`'s rows: flush
    /// the memtable (tombstones into L0), then request, then wake the compactor.
    /// `Some(id)` = requested; `None` = the Db has nothing to compact yet.
    async fn submit_compaction(
        &self,
        tenant: &TenantId,
        range: PartitionId,
    ) -> Result<Option<String>, String> {
        self.flush(tenant, range).await?;
        let watch = match self
            .stores
            .submit_full_compaction(range.0)
            .await
            .map_err(|e| e.to_string())?
        {
            cairn_store::FullCompaction::Nothing => return Ok(None),
            // Not `Ok(None)`: that would prove the range clean. The clean pass runs again.
            cairn_store::FullCompaction::SnapshotOpen => {
                // Info: a soak or an operator counts these to see if query snapshots hold a
                // purge back (G32). At most one per range per round.
                tracing::info!(tenant = %tenant, range = range.0, "tenant purge: a snapshot older than the flushed rows is open — full compaction not requested this round");
                return Err(
                    "a snapshot older than the flushed rows is open — the full compaction waits for a later round".into(),
                );
            }
            cairn_store::FullCompaction::Requested(w) => w,
        };
        let id = watch.id.clone();
        let named: std::collections::BTreeSet<String> = self
            .compactions
            .lock()
            .map(|c| c.values().map(|(id, _)| id.clone()).collect())
            .unwrap_or_default();
        if let Ok(mut w) = self.watches.lock() {
            w.retain(|id, _| named.contains(id));
            w.insert(id.clone(), watch);
        }
        tracing::info!(tenant = %tenant, range = range.0, compaction = %id, "tenant purge: full compaction of the range requested");
        // Without a wake-up the compaction stays `Submitted` until the coordinator's next
        // poll (1 h). A failure only delays, it is not wrong: the poll tick is the net.
        if let Err(e) = self.wake(tenant, range).await {
            tracing::warn!(tenant = %tenant, range = range.0, error = %e, "tenant purge: cannot wake the compactor — the compaction waits for the poll tick");
        }
        Ok(Some(id))
    }

    /// Whether compaction `id` of range `range` has finished (`Some(false)` = failed).
    async fn compaction_done(
        &self,
        range: PartitionId,
        id: String,
    ) -> Result<Option<bool>, String> {
        // No watch (lost, or never stored) ⇒ unknown: request again (G31).
        let Some(mut watch) = self.watches.lock().ok().and_then(|w| w.get(&id).cloned()) else {
            return Ok(Some(false));
        };
        let done = self
            .stores
            .full_compaction_done(range.0, &mut watch)
            .await
            .map_err(|e| e.to_string())?;
        if let Ok(mut w) = self.watches.lock() {
            match done {
                Some(_) => w.remove(&id),
                None => w.insert(id, watch),
            };
        }
        Ok(done)
    }

    /// Whether a full compaction this node requested is not finished yet: the round then
    /// runs every [`COMPACTION_POLL`] at most.
    pub fn awaiting_compaction(&self) -> bool {
        self.compactions.lock().is_ok_and(|c| !c.is_empty())
    }

    /// Flush the memtable of range `range` on the node leading it.
    async fn flush(&self, tenant: &TenantId, range: PartitionId) -> Result<(), String> {
        if let Some(h) = self.partitions.get(range) {
            match h.flush_memtable_fresh().await {
                Ok(()) => return Ok(()),
                Err(ActorError::NotLeader | ActorError::WrongRange) => {}
                Err(e) => return Err(e.to_string()),
            }
        }
        let body = RowsRequest {
            tenant: tenant.to_string(),
            segment: 0,
            after: String::new(),
            budget: 1,
            flush: true,
            wake: false,
        };
        self.remote(range, ROWS_ROUTE, &body).await.map(|_| ())
    }

    /// Wake the compactor of range `range` on the node leading it (after the full compaction
    /// request of [`Self::compact_range`]).
    async fn wake(&self, tenant: &TenantId, range: PartitionId) -> Result<(), String> {
        if let Some(h) = self.partitions.get(range) {
            match h.wake_compactor().await {
                Ok(()) => return Ok(()),
                Err(ActorError::NotLeader | ActorError::WrongRange) => {}
                Err(e) => return Err(e.to_string()),
            }
        }
        let body = RowsRequest {
            tenant: tenant.to_string(),
            segment: 0,
            after: String::new(),
            budget: 1,
            flush: true,
            wake: true,
        };
        self.remote(range, ROWS_ROUTE, &body).await.map(|_| ())
    }

    /// One lot on range `range`: the local engine if this node leads it, otherwise the
    /// leader through the internal port. The authority (`TenantPurgeGrant`) is always built
    /// on the leading node, right before the lot.
    async fn lot(
        &self,
        tenant: &TenantId,
        range: PartitionId,
        from: Option<TenantPurgeCursor>,
    ) -> Result<TenantRowsStep, String> {
        if let Some(h) = self.partitions.get(range) {
            let grant = tenant_purge_grant(&self.stores, tenant, self.keep, now_ms())
                .await
                .map_err(|e| e.to_string())??;
            match h.purge_tenant_rows(grant, from.clone(), ROW_BUDGET).await {
                Ok(step) => return Ok(step),
                Err(ActorError::NotLeader | ActorError::WrongRange) => {}
                Err(e) => return Err(e.to_string()),
            }
        }
        let (segment, after) = from.map_or((0, String::new()), |c| (c.segment, hex(&c.after)));
        let body = RowsRequest {
            tenant: tenant.to_string(),
            segment,
            after,
            budget: ROW_BUDGET,
            flush: false,
            wake: false,
        };
        let reply = self.remote(range, ROWS_ROUTE, &body).await?;
        Ok(TenantRowsStep {
            deleted: reply.deleted,
            scanned: reply.scanned,
            unattributed: reply.unattributed,
            found: reply.found,
            next: match reply.next {
                None => None,
                Some((segment, after)) => Some(TenantPurgeCursor {
                    segment,
                    after: unhex(&after).ok_or("corrupt cursor")?,
                }),
            },
        })
    }

    /// One check lot on range `range`: the local engine if this node leads it,
    /// otherwise the leader through [`CHECK_ROUTE`]. The authority is built on the leading node.
    async fn check_lot(
        &self,
        tenant: &TenantId,
        range: PartitionId,
        from: Option<TenantPurgeCursor>,
    ) -> Result<TenantRowsStep, String> {
        if let Some(h) = self.partitions.get(range) {
            let grant = tenant_purge_grant(&self.stores, tenant, self.keep, now_ms())
                .await
                .map_err(|e| e.to_string())??;
            match h.find_tenant_rows(grant, from.clone(), ROW_BUDGET).await {
                Ok(step) => return Ok(step),
                Err(ActorError::NotLeader | ActorError::WrongRange) => {}
                Err(e) => return Err(e.to_string()),
            }
        }
        let (segment, after) = from.map_or((0, String::new()), |c| (c.segment, hex(&c.after)));
        let body = RowsRequest {
            tenant: tenant.to_string(),
            segment,
            after,
            budget: ROW_BUDGET,
            flush: false,
            wake: false,
        };
        let reply = self.remote(range, CHECK_ROUTE, &body).await?;
        Ok(TenantRowsStep {
            deleted: reply.deleted,
            scanned: reply.scanned,
            unattributed: reply.unattributed,
            found: reply.found,
            next: match reply.next {
                None => None,
                Some((segment, after)) => Some(TenantPurgeCursor {
                    segment,
                    after: unhex(&after).ok_or("corrupt cursor")?,
                }),
            },
        })
    }

    /// A check lot requested by another node (`POST /internal/ranges/{id}/tenant-purge-check`):
    /// the authority is built here, outside the engine (D16). Deletes nothing.
    pub async fn serve_check(
        &self,
        range: PartitionId,
        req: RowsRequest,
    ) -> Result<RowsReply, String> {
        let tenant = TenantId::new(&req.tenant).map_err(|e| e.to_string())?;
        let from = req.cursor().ok_or("corrupt cursor")?;
        let h = self
            .partitions
            .get(range)
            .ok_or_else(|| format!("this node does not run range {}", range.0))?;
        let grant = tenant_purge_grant(&self.stores, &tenant, self.keep, now_ms())
            .await
            .map_err(|e| e.to_string())??;
        h.find_tenant_rows(grant, from, req.budget.clamp(1, 10 * ROW_BUDGET))
            .await
            .map(RowsReply::from)
            .map_err(|e| e.to_string())
    }

    /// Leftover progress entries of a tenant already `purged`: two runners at once during a
    /// leader change (the loser writes a proof after the tombstone), or a tombstone written by
    /// a binary older than per-range proofs. Hold in place, then delete, each lot with a
    /// [`DeleteProof::purged_tenant_progress`] rebuilt at that moment. Returns the number of
    /// objects deleted.
    async fn sweep_progress_residue(
        &self,
        tenant: &TenantId,
        mut ids: Vec<EntryId>,
    ) -> Result<usize, StoreError> {
        let main = self.stores.main();
        let left =
            tp::objects(main.store(), main.prefix(), tenant, PrefixKind::Progress, 0).await?;
        if ids.is_empty() && left.is_empty() {
            return Ok(0);
        }
        let bin = self.bin();
        let mut deleted = 0;
        if ids.is_empty() {
            let proof = match DeleteProof::purged_tenant_progress(&self.stores, tenant).await? {
                Ok(p) => p,
                Err(why) => {
                    tracing::debug!(tenant = %tenant, reason = %why, "tenant purge: keeping leftover progress entries");
                    return Ok(0);
                }
            };
            let id = EntryId::new(now_ms());
            bin.hold_in_place(&id, &proof, "tenant purged: leftover progress entries")
                .await?;
            ids.push(id);
        }
        for id in &ids {
            loop {
                let listed = tp::objects(
                    main.store(),
                    main.prefix(),
                    tenant,
                    PrefixKind::Progress,
                    OBJECT_BATCH,
                )
                .await?;
                let more = listed.len() > OBJECT_BATCH;
                let batch = &listed[..listed.len().min(OBJECT_BATCH)];
                let proof = match DeleteProof::purged_tenant_progress(&self.stores, tenant).await? {
                    Ok(p) => p,
                    Err(why) => {
                        tracing::debug!(tenant = %tenant, reason = %why, "tenant purge: keeping leftover progress entries");
                        return Ok(deleted);
                    }
                };
                if more {
                    let Some(n) = bin.purge_objects_in_place(id, proof, batch).await? else {
                        break;
                    };
                    deleted += n;
                    continue;
                }
                deleted += bin.purge_in_place(id, proof, batch).await?;
                break;
            }
        }
        if deleted > 0 {
            tracing::warn!(tenant = %tenant, object = deleted, "tenant purge: leftover progress entries cleaned after the tombstone");
        }
        Ok(deleted)
    }

    /// Send `body` to the leader of `range` through the internal port.
    async fn remote(
        &self,
        range: PartitionId,
        route: &str,
        body: &RowsRequest,
    ) -> Result<RowsReply, String> {
        let addr = self
            .routing
            .leader_of(range)
            .and_then(|l| l.address)
            .ok_or_else(|| format!("range {} has no leader with an address yet", range.0))?;
        let secret = self
            .secret
            .as_ref()
            .ok_or("no cluster secret to call the leader")?;
        let url = format!(
            "{}/internal/ranges/{}/{route}",
            addr.trim_end_matches('/'),
            range.0
        );
        let req = self
            .client
            .post(&url)
            .timeout(REMOTE_LOT_TIMEOUT)
            .json(body);
        let resp = secret
            .send(&self.client, req)
            .await
            .map_err(|e| e.to_string())?;
        if !resp.status().is_success() {
            return Err(format!("{url}: {}", resp.status()));
        }
        resp.json().await.map_err(|e| e.to_string())
    }

    /// B3: the tenant's keys, OIDC clients and rule lines — configuration, not run data: the
    /// existing write paths, not through the trash.
    async fn forget_config(&self, tenant: &TenantId) -> Result<(), StoreError> {
        if let Some(keys) = &self.keys {
            keys.refresh().await.map_err(StoreError::Io)?;
            for k in keys.list().into_iter().filter(|k| &k.tenant == tenant) {
                keys.delete(&k.id)
                    .await
                    .map_err(|e| StoreError::Io(e.to_string()))?;
            }
            let book = keys.oidc();
            let (config, _) = book.read().await.map_err(StoreError::Io)?;
            if config.clients.iter().any(|c| &c.tenant == tenant) {
                book.update_config(|c| {
                    let before = c.clients.len();
                    c.clients.retain(|cl| &cl.tenant != tenant);
                    Ok::<_, String>(((), c.clients.len() != before))
                })
                .await
                .map_err(StoreError::Io)?;
            }
        }
        if let Some(rules) = &self.rules {
            if rules
                .load()
                .await?
                .book
                .get_tenant(tenant.as_str())
                .is_some()
            {
                rules
                    .update(|b| {
                        b.remove_tenant(tenant.as_str())
                            .map_err(StoreError::from)
                            .map(|_| ())
                    })
                    .await?;
            }
        }
        Ok(())
    }

    /// A lot requested by another node (`POST /internal/ranges/{id}/tenant-purge`): the
    /// authority is built here, outside the engine (D16).
    pub async fn serve_lot(
        &self,
        range: PartitionId,
        req: RowsRequest,
    ) -> Result<RowsReply, String> {
        let tenant = TenantId::new(&req.tenant).map_err(|e| e.to_string())?;
        let from = req.cursor().ok_or("corrupt cursor")?;
        let h = self
            .partitions
            .get(range)
            .ok_or_else(|| format!("this node does not run range {}", range.0))?;
        let grant = tenant_purge_grant(&self.stores, &tenant, self.keep, now_ms())
            .await
            .map_err(|e| e.to_string())??;
        if req.wake {
            return h
                .wake_compactor()
                .await
                .map(|()| RowsReply::default())
                .map_err(|e| e.to_string());
        }
        if req.flush {
            return h
                .flush_memtable_fresh()
                .await
                .map(|()| RowsReply::default())
                .map_err(|e| e.to_string());
        }
        h.purge_tenant_rows(grant, from, req.budget.clamp(1, 10 * ROW_BUDGET))
            .await
            .map(RowsReply::from)
            .map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fake range of `keys` keys, each lot looking at `per_lot` keys; keys belonging to the
    /// tenant (`mine`) are deleted the first time a lot passes over them.
    struct FakeRange {
        mine: std::collections::BTreeSet<u64>,
        keys: u64,
        per_lot: u64,
    }

    impl FakeRange {
        fn lot(&mut self, from: Option<TenantPurgeCursor>) -> TenantRowsStep {
            let start = from.map_or(0, |c| u64::from_be_bytes(c.after.try_into().unwrap()) + 1);
            let end = (start + self.per_lot).min(self.keys);
            let deleted = (start..end).filter(|k| self.mine.remove(k)).count() as u64;
            TenantRowsStep {
                deleted,
                scanned: end - start,
                unattributed: 0,
                found: 0,
                next: (end < self.keys).then(|| TenantPurgeCursor {
                    segment: 0,
                    after: (end - 1).to_be_bytes().to_vec(),
                }),
            }
        }
    }

    impl FakeRange {
        /// A fake range of `lots` lots per pass (10 keys per lot), no tenant rows.
        fn of_lots(lots: u64) -> Self {
            FakeRange {
                mine: Default::default(),
                keys: lots * 10,
                per_lot: 10,
            }
        }

        /// A **check** lot: like [`Self::lot`] but deleting nothing, stopping at the tenant's
        /// first row.
        fn check(&mut self, from: Option<TenantPurgeCursor>) -> TenantRowsStep {
            let start = from.map_or(0, |c| u64::from_be_bytes(c.after.try_into().unwrap()) + 1);
            let end = (start + self.per_lot).min(self.keys);
            let at = |k: u64| TenantPurgeCursor {
                segment: 0,
                after: k.to_be_bytes().to_vec(),
            };
            if let Some(&k) = self.mine.range(start..end).next() {
                return TenantRowsStep {
                    scanned: k + 1 - start,
                    found: 1,
                    next: Some(at(k)),
                    ..Default::default()
                };
            }
            TenantRowsStep {
                scanned: end - start,
                next: (end < self.keys).then(|| at(end - 1)),
                ..Default::default()
            }
        }
    }

    /// A fake compaction: each request gets a new id; the compaction finishes at the
    /// `polls`-th ask about that id.
    #[derive(Default)]
    struct FakeCompactor {
        submitted: Vec<String>,
        polled: BTreeMap<String, u32>,
        polls: u32,
    }

    /// Walk [`settle_rows`] on `range` for `rounds` runner rounds, limit `cap` lots per
    /// round; `late(round, range)` slips new tenant rows in at the start of a round. Returns
    /// the round (counted from 1) where the range reached `Compacted`, and the compaction ids requested.
    async fn settle_until(
        range: FakeRange,
        cap: u32,
        polls: u32,
        rounds: u32,
        late: impl Fn(u32, &mut FakeRange),
    ) -> (Option<u32>, Vec<String>) {
        let range = std::sync::Mutex::new(range);
        let comp = std::sync::Mutex::new(FakeCompactor {
            polls,
            ..Default::default()
        });
        let (sweeps, compactions) = (Sweeps::default(), Compactions::default());
        for round in 1..=rounds {
            late(round, &mut range.lock().unwrap());
            let r = settle_rows(
                &sweeps,
                &compactions,
                ("acme".into(), 0),
                cap,
                |c| {
                    let step = range.lock().unwrap().lot(c);
                    async move { Ok(step) }
                },
                || {
                    let mut c = comp.lock().unwrap();
                    let id = format!("c{}", c.submitted.len());
                    c.submitted.push(id.clone());
                    async move { Ok(Some(id)) }
                },
                |id| {
                    let mut c = comp.lock().unwrap();
                    let polls = c.polls;
                    let n = c.polled.entry(id).or_default();
                    *n += 1;
                    let done = (*n >= polls).then_some(true);
                    async move { Ok(done) }
                },
            )
            .await
            .unwrap();
            if let Ok(Settle::Compacted(_)) = r.settle {
                return (Some(round), comp.into_inner().unwrap().submitted);
            }
        }
        (None, comp.into_inner().unwrap().submitted)
    }

    /// **The scan–delete–compact path harness**: a range whose pass fits one round (2 lots,
    /// limit 10), with tenant rows. Must see: round 1 deletes everything and then a clean
    /// pass ⇒ requests a compaction; round 2 asks about the compaction ⇒ `Compacted`;
    /// exactly one request.
    #[tokio::test]
    async fn settle_rows_compacts_a_small_range_once() {
        let range = FakeRange {
            mine: [3, 15].into(),
            ..FakeRange::of_lots(2)
        };
        let (at, submitted) = settle_until(range, 10, 1, 10, |_, _| {}).await;
        assert_eq!((at, submitted.len()), (Some(2), 1), "{submitted:?}");
    }

    /// **A large range never got a proof.** The range needs 2 lots per pass, the limit is
    /// 1 lot per runner round (one whole pass = 2 rounds). Order:
    ///
    /// 1. Round 1: the first lot of the pass. Round 2: the last lot, a clean pass ⇒ request
    ///    compaction `c0`.
    /// 2. Round 3: the compaction finishes at the first ask.
    ///
    /// Must see: `Compacted` by round ≤ 4, **exactly one** id requested. With tenant rows in
    /// the first pass ⇒ the second pass is clean in round 4, `Compacted` in round 5, one id. A
    /// slow compaction (finishing at the third ask) with a late row in round 3
    /// ⇒ the lot deletes it, drops `c0`, the next clean pass requests `c1`, then `Compacted` —
    /// exactly two ids.
    ///
    /// Before the fix: round 3 starts a new pass, unfinished ⇒ `!clean` drops `c0`;
    /// round 4 the pass finishes ⇒ requests `c1`; … never `Compacted`, one id every 2 rounds.
    #[tokio::test]
    async fn a_range_needing_several_rounds_per_pass_gets_compacted_once() {
        let (at, submitted) = settle_until(FakeRange::of_lots(2), 1, 1, 10, |_, _| {}).await;
        assert!(
            at.is_some_and(|r| r <= 4) && submitted.len() == 1,
            "round {at:?}, ids {submitted:?}"
        );
        let rows = FakeRange {
            mine: [3, 15].into(),
            ..FakeRange::of_lots(2)
        };
        let (at, submitted) = settle_until(rows, 1, 1, 10, |_, _| {}).await;
        assert_eq!((at, submitted.len()), (Some(5), 1), "{submitted:?}");
        let late = |round: u32, range: &mut FakeRange| {
            if round == 3 {
                range.mine.insert(5);
            }
        };
        let (at, submitted) = settle_until(FakeRange::of_lots(2), 1, 3, 12, late).await;
        assert!(
            at.is_some() && submitted.len() == 2,
            "late row: round {at:?}, ids {submitted:?}"
        );
    }

    /// Walk [`final_check`] over the fake ranges `ranges` for `rounds` rounds, limit
    /// `cap` lots per range per round. Returns the first round (counted from 1) in which the
    /// check concluded clean, and the outcome of each round.
    async fn final_until(
        ranges: Vec<FakeRange>,
        cap: u32,
        rounds: u32,
    ) -> (Option<u32>, Vec<FinalCheck>) {
        let ranges: Vec<_> = ranges.into_iter().map(std::sync::Mutex::new).collect();
        let live: Vec<u32> = (0..ranges.len() as u32).collect();
        let checks = Checks::default();
        let mut seen = Vec::new();
        for round in 1..=rounds {
            let out = final_check(&checks, "acme", &live, cap, |r, c| {
                let step = ranges[r as usize].lock().unwrap().check(c);
                async move { Ok(step) }
            })
            .await;
            let clean = out.clean();
            seen.push(out);
            if clean {
                return (Some(round), seen);
            }
        }
        (None, seen)
    }

    /// **The final check harness**: range 0 one lot per pass,
    /// range 1 two lots, range 2 with one tenant row; limit 1 lot. Must see: in round
    /// 1 range 1 is `checking`, range 2 `late`; without range 2, ranges 0 and 1 are clean
    /// together in round 2.
    #[tokio::test]
    async fn final_check_sorts_ranges_into_late_and_checking() {
        let found = FakeRange {
            mine: [4].into(),
            ..FakeRange::of_lots(1)
        };
        let (at, seen) = final_until(
            vec![FakeRange::of_lots(1), FakeRange::of_lots(2), found],
            1,
            1,
        )
        .await;
        assert_eq!(at, None);
        assert_eq!(
            seen[0],
            FinalCheck {
                late: vec![2],
                checking: vec![1],
            }
        );
        let (at, _) = final_until(vec![FakeRange::of_lots(1), FakeRange::of_lots(2)], 1, 10).await;
        assert_eq!(at, Some(2));
    }

    /// **The final check only closed at the LCM of the round counts.** Limit 1 lot; a range
    /// needing k lots per check pass takes k rounds for one whole check pass.
    ///
    /// Must see: two ranges of 2 and 3 rounds ⇒ the check concludes clean by round ≤ 3; adding
    /// a range of 5 rounds ⇒ ≤ 5. The window for late rows does not widen: a 1-round range
    /// already clean in round 1 that receives a tenant row in round 2 is still caught in round 2
    /// (clean ranges are still re-checked every round), and keeps being caught until the row
    /// is deleted.
    ///
    /// Before the fix: it only concluded when every range was `Clean` in the same round —
    /// round 6 and round 30.
    #[tokio::test]
    async fn final_check_concludes_after_the_slowest_range() {
        let lots = |ks: &[u64]| ks.iter().map(|&k| FakeRange::of_lots(k)).collect();
        let (at, _) = final_until(lots(&[2, 3]), 1, 40).await;
        assert!(
            at.is_some_and(|r| r <= 3),
            "2 and 3 rounds: concluded at {at:?}"
        );
        let (at, _) = final_until(lots(&[2, 3, 5]), 1, 40).await;
        assert!(
            at.is_some_and(|r| r <= 5),
            "2, 3 and 5 rounds: concluded at {at:?}"
        );
        let ranges = [
            std::sync::Mutex::new(FakeRange::of_lots(1)),
            std::sync::Mutex::new(FakeRange::of_lots(3)),
        ];
        let checks = Checks::default();
        let mut seen = Vec::new();
        for round in 1..=4 {
            if round == 2 {
                ranges[0].lock().unwrap().mine.insert(4);
            }
            seen.push(
                final_check(&checks, "acme", &[0, 1], 1, |r, c| {
                    let step = ranges[r as usize].lock().unwrap().check(c);
                    async move { Ok(step) }
                })
                .await,
            );
        }
        assert_eq!(seen[1].late, vec![0], "late row in round 2: {seen:?}");
        assert!(
            seen[2..].iter().all(|s| s.late == vec![0] && !s.clean()),
            "rows not deleted yet but the check concluded: {seen:?}"
        );
    }

    /// **One failed check lot blocked the whole final check.** Under chaos (a node killed
    /// every 10 s, injected store faults) a check lot of some range fails in most rounds:
    /// the remote leader just died, a GET of the purge grant hit a fault. Every range sits
    /// in `checking` for that round, so the batch concludes only in a round where all ~100
    /// ranges answer at the first try — rare, and the purge chases auto-splits for tens of
    /// minutes.
    ///
    /// 1. 50 ranges, one lot per pass; every range fails its first lot attempt of round 1.
    /// 2. A check lot deletes nothing: trying it again within the round is safe.
    ///
    /// Must see: the check concludes clean in round 1. Before the fix: round 1 had all 50
    /// ranges in `checking`.
    #[tokio::test(start_paused = true)]
    async fn final_check_rides_out_a_failed_lot() {
        let ranges: Vec<_> = (0..50)
            .map(|_| std::sync::Mutex::new((FakeRange::of_lots(1), 0u32)))
            .collect();
        let live: Vec<u32> = (0..50).collect();
        let checks = Checks::default();
        let out = final_check(&checks, "acme", &live, 1, |r, c| {
            let mut g = ranges[r as usize].lock().unwrap();
            g.1 += 1;
            let step = if g.1 == 1 {
                Err("injected failure".to_string())
            } else {
                Ok(g.0.check(c))
            };
            async move { step }
        })
        .await;
        assert!(
            out.clean(),
            "one failed attempt per range blocked the round: {out:?}"
        );
    }

    /// **A range larger than one round's limit**: 100 keys, 10 per lot, limit
    /// 3 lots per runner round. Must see: runner rounds continue one after another from the
    /// previous round's cursor, delete every tenant row and eventually report clean — not
    /// rescanning from the start forever.
    #[tokio::test]
    async fn sweep_resumes_across_runner_rounds() {
        let mut range = FakeRange {
            mine: (0..100).step_by(7).collect(),
            keys: 100,
            per_lot: 10,
        };
        let range = std::sync::Mutex::new(&mut range);
        let sweeps = Sweeps::default();
        let mut total = 0;
        for round in 0..20 {
            let (deleted, _, clean) = sweep_keyed(&sweeps, ("acme".into(), 0), 3, |c| {
                let step = range.lock().unwrap().lot(c);
                async move { Ok(step) }
            })
            .await
            .unwrap();
            total += deleted;
            if clean {
                assert!(range.lock().unwrap().mine.is_empty(), "clean but rows left");
                assert_eq!(total, 15);
                assert!(round >= 3, "must take several runner rounds: {round}");
                return;
            }
        }
        panic!(
            "20 runner rounds and still not clean: {:?} left",
            range.lock().unwrap().mine
        );
    }
}
